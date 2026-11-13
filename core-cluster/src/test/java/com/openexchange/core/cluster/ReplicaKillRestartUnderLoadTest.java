// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The replica process killed and restarted under order load (nightly): the production replica in a child JVM
 * ({@code com.openexchange.core.Main replica}, embedded driver) follows three in-process members in {@code shadow}
 * mode while the generator runs; after its ALIGNED at P_cm is verified it is killed ({@code destroyForcibly}, kill -9)
 * mid-follow and restarted on the same {@code BASE_DIR}. Pass: {@code restore_path=LOCAL_SHADOW}, {@code /ready} reads
 * 503 {@code unverified continuation} until the next P_cm and 200 after it, within
 * {@code core.lag.restart.ready.bound.s} of the restart; the members' floors and log starts are the same before the
 * kill and after the restart (the replica's absence moves nothing on the members). The lag is sampled and recorded (it
 * grows with the load while the replica is down).
 *
 * <p>Control in the row (the local restore switched off: a third life on a fresh {@code BASE_DIR}):
 * {@code restarted replica did not restore from its local shadow}.</p>
 */
public class ReplicaKillRestartUnderLoadTest {

    private static final int PORT_BASE = 58800; // members 58800..59000, the replica slot 59100

    @Test(timeout = 1_800_000)
    public void aKilledReplicaRestoresItsLocalShadowAndIsReadyAfterTheNextSnapshot() throws Exception {
        final List<ReplicaChild> children = new ArrayList<>();
        try (ReplicaLoadRig x = new ReplicaLoadRig(PORT_BASE, "replica-kill-restart", 3, "1m", 0)) {
            try {
                x.bootMembers();
                final Path base = x.rig.tmp.toPath();
                final Path state = base.resolve("replica-state");
                final String aeronDir = base.resolve("replica-aeron").toString();
                final int port = ReplicaRig.freeTcp();
                final ReplicaChild first = ReplicaChild.start(base, "replica-1", x.childEnv(state, aeronDir, port,
                        "embedded"), List.of("replica"));
                children.add(first);
                x.awaitChildLine(first, "[REPLICA] bootstrap done: restore_path=GENESIS", 120_000);
                x.awaitChildReady(first, port, 120_000);
                x.sampleChild(port);
                x.startLoad();
                x.rig.pumpMs(5_000);

                final long p1 = x.snapshot();
                x.rig.await(() -> x.childMetric("shadow_align_verified_position") >= p1, 120_000,
                        () -> "the first life never verified an ALIGNED at " + p1 + ReplicaLoadRig.tails(first));
                x.rig.pumpMs(3_000);
                final Map<Integer, long[]> before = floors(x);
                final long commitAtKill = x.commit();
                first.destroyForcibly(); // kill -9 mid-follow
                System.out.println("[lag] replica killed at commit " + commitAtKill + "; members " + text(before));

                // the members' backup window and the dead driver's directory both age out first
                x.r.windowGap();
                final ReplicaChild second = ReplicaChild.start(base, "replica-2", x.childEnv(state, aeronDir, port,
                        "embedded"), List.of("replica"));
                children.add(second);
                final long t0 = System.nanoTime();
                final String restored = x.awaitChildLine(second, "[REPLICA] bootstrap done: restore_path=", 120_000);
                assertLocal(restored);
                final boolean[] unverified = {false};
                final long[] nextAt = {0};
                x.rig.await(() -> {
                    if (System.currentTimeMillis() < nextAt[0]) {
                        return false;
                    }
                    nextAt[0] = System.currentTimeMillis() + 200;
                    final String[] ready = ReplicaLoadRig.childReady(port);
                    unverified[0] |= "503".equals(ready[0]) && ready[1].contains("unverified continuation");
                    return unverified[0] || "200".equals(ready[0]);
                }, 60_000, "no /ready answer from the restarted replica" + ReplicaLoadRig.tails(second));
                assertTrue("the restarted replica reads 503 unverified continuation before the next P_cm",
                        unverified[0]);
                final Map<Integer, long[]> after = floors(x);
                assertEquals("the members' floors and log starts across the replica's kill and restart", text(before),
                        text(after));
                final long p2 = x.snapshot();
                x.awaitChildLine(second, "[REPLICA] alignment: the state at " + p2 + " equals the member snapshot",
                        120_000);
                x.awaitChildReady(second, port, 120_000);
                final long readyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                System.out.println("[lag] restarted replica: " + restored + "; /ready 200 " + readyMs + " ms after the"
                        + " start, the unverified continuation cleared at P_cm=" + p2);
                if (readyMs > ReplicaLoadRig.RESTART_READY_BOUND_S * 1_000) {
                    throw new AssertionError("the restarted replica was ready " + readyMs + " ms after its start, above"
                            + " the bound " + ReplicaLoadRig.RESTART_READY_BOUND_S + " s");
                }
                x.rig.pumpMs(5_000);
                // the lag across the replica's absence grows with the load (recorded; the row's bound is the ready time)
                x.sampler.assertObserved();
                final Map<String, Object> extra = new LinkedHashMap<>();
                extra.put("restored", restored);
                extra.put("ready_ms_after_start", readyMs);
                extra.put("floors", text(after));
                extra.put("steady_window", x.sampler.summary());

                // control: the third life on a fresh BASE_DIR has no local shadow
                x.sampler.close();
                second.destroyForcibly();
                x.r.windowGap();
                final ReplicaChild third = ReplicaChild.start(base, "replica-3", x.childEnv(base.resolve(
                        "replica-state-fresh"), base.resolve("replica-aeron-fresh").toString(), port, "embedded"),
                        List.of("replica"));
                children.add(third);
                final String fresh = x.awaitChildLine(third, "[REPLICA] bootstrap done: restore_path=", 120_000);
                RestoreDrillDryRun.nc("RESTORE_LOCAL_OFF", "restarted replica did not restore from its local shadow",
                        () -> assertLocal(fresh));
                third.destroyForcibly();

                extra.put("generator", x.endLoad());
                x.sampler.writeSummary(extra);
                System.out.println("GREEN replica kill and restart under load: " + restored + ", ready " + readyMs
                        + " ms");
            } finally {
                for (final ReplicaChild c : children) {
                    c.destroyForcibly();
                }
            }
        }
    }

    private static void assertLocal(final String restoredLine) {
        if (restoredLine == null || !restoredLine.contains("restore_path=LOCAL_SHADOW")) {
            throw new AssertionError("restarted replica did not restore from its local shadow: " + restoredLine);
        }
    }

    /** Each live member's {@code {floor watermark, log start}}. */
    private static Map<Integer, long[]> floors(final ReplicaLoadRig x) {
        final Map<Integer, long[]> out = new java.util.TreeMap<>();
        for (final Member m : x.rig.live()) {
            // the floor the pruner last applied (read only: getAsLong is a purge pass)
            final com.openexchange.cluster.shadow.ShadowFloor f = ServiceProbe.shadowFloor(m.service);
            out.put(m.id, new long[] {f == null ? -1 : f.lastApplied(), x.rig.logStart(m)});
        }
        return out;
    }

    private static String text(final Map<Integer, long[]> floors) {
        final StringBuilder sb = new StringBuilder();
        floors.forEach((id, v) -> sb.append(sb.length() == 0 ? "" : ", ").append("member ").append(id)
                .append(" floor ").append(v[0]).append(" log start ").append(v[1]));
        return sb.toString();
    }
}

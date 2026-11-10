// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.NodeReadiness;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Flap row: {@code REPLICA_BACKUP_QUERY_INTERVAL_MS=12000} (the minimum), {@code /ready}
 * sampled every 10 ms (the required 50 ms, tightened) for 60 s, the first 30 s under light load with a member
 * snapshot every 12 s (so queries also retrieve member sets) and the last 30 s idle: zero not-ready samples while
 * the live recording is alive, across real backup query cycles (the backup state leaves BACKING_UP at every query,
 * CBA:1002-1007; the live recording survives it, CBA:760, :900-904).
 *
 * <p>Control: readiness computed from {@code state == BACKING_UP} (a rule that flaps at every query) ->
 * {@code ready flapped <n> times}.</p>
 */
public class ReplicaReadyDoesNotFlapTest {

    private static final int PORT_BASE = 31600;
    /** The requirement samples every 50 ms; 10 ms is stricter and lets the control see the short query states. */
    private static final long SAMPLE_MS = 10;
    /** A member snapshot every ~12 s under load: the next query retrieves the set (SNAPSHOT_RETRIEVE, not BACKING_UP). */
    private static final long SNAPSHOT_EVERY_MS = 12_000;

    @Test(timeout = 600_000)
    public void readyDoesNotFlapAcrossBackupQueries() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-flap")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").backupQueryIntervalMs(12_000).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            r.rig.await(() -> r.probe().ready(), 30_000, () -> "never ready: " + r.probe().detail());

            final long queries0 = r.replica.events().queries();
            final Samples loaded = sample(r, 30_000, true);
            final Samples idle = sample(r, 30_000, false);
            final long queries = r.replica.events().queries() - queries0;
            System.out.println("[flap] flap: " + loaded + " (light load), " + idle + " (idle), backup queries "
                    + queries + ", resets " + r.replica.events().resets());
            assertTrue("the sampling must span real query cycles (queries " + queries + ")", queries >= 4);
            assertNoFlap(loaded);
            assertNoFlap(idle);
            assertTrue("the backup state left BACKING_UP during the sampling (otherwise the row proves nothing)",
                    loaded.backupStateOther + idle.backupStateOther > 0);
            System.out.println("GREEN flap samples=" + (loaded.total + idle.total) + " queries=" + queries);

            r.stopReplica();
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-nc").backupQueryIntervalMs(12_000)
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.READINESS_FROM_BACKUP_STATE)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            r.rig.await(() -> r.probe().ready(), 30_000, () -> "the control replica never ready: "
                    + r.probe().detail());
            final Samples nc = sample(r, 60_000, true);
            System.out.println("[flap] flap control: " + nc);
            RestoreDrillDryRun.nc("READINESS_FROM_BACKUP_STATE", "ready flapped", () -> assertNoFlap(nc));
        }
    }

    /** Readiness samples: totals, not-ready samples while the live recording was alive, ready->not-ready edges. */
    static final class Samples {
        int total;
        int notReadyWhileAlive;
        int flaps;
        int backupStateOther;
        String firstBad;

        @Override
        public String toString() {
            return total + " samples, " + notReadyWhileAlive + " not ready while L alive, " + flaps + " flaps, "
                    + backupStateOther + " outside BACKING_UP";
        }
    }

    static Samples sample(final ReplicaRig r, final long durationMs, final boolean load) {
        final Samples s = new Samples();
        final Encoders enc = new Encoders();
        final long start = System.currentTimeMillis();
        final long end = start + durationMs;
        boolean lastReady = true;
        long corr = 50_000;
        long nextSnapshot = start + 2_000;
        while (System.currentTimeMillis() < end) {
            if (load && s.total % 20 == 0) {
                r.tool.offer(enc.buf, enc.deposit(corr++, 1, CoreRestoreRig.usd(), ShadowClusterRig.ONE));
            }
            if (load && System.currentTimeMillis() >= nextSnapshot) {
                nextSnapshot += SNAPSHOT_EVERY_MS;
                r.toggle(io.aeron.cluster.ClusterControl.ToggleState.SNAPSHOT);
            }
            final NodeReadiness.Probe p = r.probe();
            final boolean alive = p.detail().contains("liveRecording=alive");
            s.total++;
            if (!p.detail().contains("backupState=5")) {
                s.backupStateOther++;
            }
            if (!p.ready() && alive) {
                s.notReadyWhileAlive++;
                if (s.firstBad == null) {
                    s.firstBad = p.detail();
                }
            }
            if (lastReady && !p.ready()) {
                s.flaps++;
            }
            lastReady = p.ready();
            r.rig.pumpMs(SAMPLE_MS);
        }
        return s;
    }

    static void assertNoFlap(final Samples s) {
        if (s.notReadyWhileAlive > 0 || s.flaps > 0) {
            throw new AssertionError("ready flapped " + s.flaps + " times (" + s + "; first " + s.firstBad + ")");
        }
        assertEquals(0, s.notReadyWhileAlive);
    }
}

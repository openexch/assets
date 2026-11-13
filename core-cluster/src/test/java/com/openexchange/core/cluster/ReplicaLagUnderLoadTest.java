// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * The replica's lag under order load (nightly): three members in {@code shadow} mode (1m log term), the real replica
 * in-JVM, the order load generator's session at {@code core.lag.rate} for {@code core.lag.seconds} with two P_cm. Pass:
 * {@code max(commit - applied) <= TRANSPORT_LOG_TERM_LENGTH} at {@code core.lag.hz}, the gauge proxy {@code
 * max(core_log_position - applied)} recorded alongside, each ALIGNED listed by the replica's catalog within {@code
 * core.lag.catalog.bound.s} plus one backup query interval of P_cm (the replica holds an ALIGNED until its backup
 * retrieved the member snapshot at that P_cm, which it learns of at its next query; the listing times are recorded) and
 * VERIFIED with the members' image. The first ALIGNED after the replica's genesis bootstrap waits for its alignment
 * trust (the backup names the member snapshot at that P_cm, one backup query later): it is taken before the window and
 * its time recorded, not bounded. A functional guard: the time values here are never evidence of a rate.
 *
 * <p>Controls in the row: (b) the sampler pause: the replica's apply held for {@code core.lag.pause.s}; the lag must
 * rise above its pre-pause maximum and recover below the term within {@code core.lag.pause.recovery.s}; the same
 * window without the hold must fail with {@code sampler saw no lag during a <n> s pause}. (a) a second replica with
 * {@code REPLICA_READY_MAX_LAG_BYTES=1} reads {@code /ready} 503 {@code apply-lag} under the load. Vacuity: no lag in
 * the whole window fails with {@code no lag observed: the load did not reach the replica}.</p>
 */
public class ReplicaLagUnderLoadTest {

    private static final int PORT_BASE = 57000; // members 57000..57200, the replica slots 57300, 57400

    @Test(timeout = 1_800_000)
    public void theReplicaStaysInsideTheTermUnderLoadAndTheSamplerSeesAPause() throws Exception {
        try (ReplicaLoadRig x = new ReplicaLoadRig(PORT_BASE, "replica-lag", ReplicaLoadRig.MEMBERS, "1m", 0)) {
            x.bootMembers();
            x.startReplica(x.builder(ReplicaRig.SLOT, "replica").build());
            x.startLoad();

            // the first ALIGNED after a genesis bootstrap is held until the backup names the member snapshot at its
            // P_cm (the replica's alignment trust, one backup query): recorded, outside the measured window
            final Map<String, Object> extra = new LinkedHashMap<>();
            final long p0 = x.snapshot();
            final long trustMs = x.msToCatalog(p0, 120_000);
            x.assertAlignedEqual(p0);
            extra.put("aligned_0_position_trust", p0);
            extra.put("aligned_0_catalog_ms_trust", trustMs);
            System.out.println("[lag] the first ALIGNED (alignment trust) listed " + trustMs + " ms after P_cm=" + p0);

            // the steady window with two P_cm. The replica holds every ALIGNED until its backup has retrieved the
            // member snapshot at the same P_cm (the alignment compare), and the backup learns of a new member snapshot
            // at its next query: the listing time is the catalog bound plus one backup query interval at most
            final long queryMs = x.r.replica.config().backupQueryIntervalMs();
            final long catalogBoundMs = ReplicaLoadRig.CATALOG_BOUND_S * 1_000 + queryMs;
            extra.put("catalog_bound_ms_applied", catalogBoundMs);
            final long windowMs = ReplicaLoadRig.SECONDS * 1_000L;
            final long t0 = System.currentTimeMillis();
            for (int i = 1; i <= 2; i++) {
                x.rig.pumpMs(Math.max(0, t0 + i * windowMs / 3 - System.currentTimeMillis()));
                final long p = x.snapshot();
                final long ms = x.msToCatalog(p, 120_000);
                extra.put("aligned_" + i + "_position", p);
                extra.put("aligned_" + i + "_catalog_ms", ms);
                if (ms > catalogBoundMs) {
                    throw new AssertionError("ALIGNED at " + p + " listed " + ms + " ms after P_cm, above the catalog"
                            + " bound " + ReplicaLoadRig.CATALOG_BOUND_S + " s plus one backup query interval "
                            + queryMs + " ms");
                }
                x.assertAlignedEqual(p);
            }
            x.rig.pumpMs(Math.max(0, t0 + windowMs - System.currentTimeMillis()));
            final String steady = x.sampler.summary();
            System.out.println("[lag] steady window: " + steady);
            x.sampler.assertInsideTheTerm();
            extra.put("steady_window", steady);

            // control (b): the same window without the hold must fail; with the hold the lag rises and recovers
            RestoreDrillDryRun.nc("SAMPLER_PAUSE_WITHOUT_HOLD", "sampler saw no lag during a",
                    () -> pause(x, false, extra));
            pause(x, true, extra);

            // control (a): REPLICA_READY_MAX_LAG_BYTES=1 reads apply-lag under the load (a second replica, outside
            // the sampled window)
            x.sampler.close();
            x.r.stopReplica();
            x.r.windowGap();
            x.r.startReplica(x.builder(ReplicaRig.SLOT + 1, "replica-ready-lag").readyMaxLagBytes(1).build());
            x.r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            x.r.awaitCaughtUp(120_000); // a replica still catching up reads apply-lag at any bound: past that first
            final String[][] seen = {null};
            x.rig.await(() -> {
                final String[] ready = x.ready();
                seen[0] = ready;
                return "503".equals(ready[0]) && ready[1].contains("apply-lag");
            }, 120_000, () -> "REPLICA_READY_MAX_LAG_BYTES=1 never read /ready 503 apply-lag (last " + seen[0][0] + " "
                    + seen[0][1] + ")");
            System.out.println("NC READY_MAX_LAG_BYTES_1 -> /ready " + seen[0][0] + " " + seen[0][1].trim());
            extra.put("ready_max_lag_1", seen[0][0] + " " + seen[0][1].trim());

            extra.put("generator", x.endLoad());
            x.sampler.writeSummary(extra);
            assertTrue("no fatal: " + x.r.fatals, x.r.fatals.isEmpty());
            System.out.println("GREEN replica lag under load: " + steady);
        }
    }

    /**
     * The sampler control: the replica's apply held (or not) for the pause; the maximum inside the window must exceed
     * the maximum before it, and the lag must recover below the term within the recovery bound.
     */
    private static void pause(final ReplicaLoadRig x, final boolean hold, final Map<String, Object> extra) {
        final long before = x.sampler.maxCommitLag;
        final long pauseMs = ReplicaLoadRig.PAUSE_S * 1_000;
        x.sampler.mark();
        if (hold) {
            x.r.faults.holdApplyUntilMs = System.currentTimeMillis() + pauseMs;
        }
        x.rig.pumpMs(pauseMs);
        final long during = x.sampler.windowMax;
        if (during <= before) {
            throw new AssertionError("sampler saw no lag during a " + ReplicaLoadRig.PAUSE_S + " s pause (max "
                    + during + " B, before the pause " + before + " B)");
        }
        final long t0 = System.currentTimeMillis();
        x.rig.await(() -> x.sampler.lastCommitLag < x.termBytes && System.currentTimeMillis() > t0 + 200,
                ReplicaLoadRig.PAUSE_RECOVERY_S * 1_000, () -> "the lag did not recover below the term " + x.termBytes
                        + " B within " + ReplicaLoadRig.PAUSE_RECOVERY_S + " s of the pause (last "
                        + x.sampler.lastCommitLag + " B)");
        final long recoveredMs = System.currentTimeMillis() - t0;
        System.out.println("[lag] pause " + ReplicaLoadRig.PAUSE_S + " s: max " + during + " B (before " + before
                + " B), recovered below the term in " + recoveredMs + " ms");
        extra.put("pause_max_lag", during);
        extra.put("pause_before_max_lag", before);
        extra.put("pause_recovered_ms", recoveredMs);
    }
}

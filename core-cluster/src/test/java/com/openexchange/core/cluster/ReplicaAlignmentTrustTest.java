// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.replica.ReplicaTestBindings;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowKind;
import io.aeron.cluster.ClusterControl;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Trust needs a byte-equal alignment, on a real three-member core with the in-JVM replica.
 *
 * <ul>
 *   <li>Restart from a local shadow: {@code /ready} reads 503 {@code unverified continuation} and no STANDBY shadow
 *       is written until the first ALIGNED compare with the retrieved member snapshot passes, then 200. Run at the
 *       members' DEFAULT cadence (5 min, {@code SnapshotCadence.DEFAULT_INTERVAL_MINUTES}) and the replica's default
 *       query interval (60 s), so the not-ready window after a restart is measured, not assumed.</li>
 *   <li>Re-genesis: every re-genesis row is in one matrix, {@code ReplicaReGenesisMatrixTest}.</li>
 *   <li>A mutated replica state (test seam: one session message dropped on the replica only) latches an oracle
 *       mismatch at the next ALIGNED compare: the replica holds ({@code fenced:oracle mismatch at <P>: ...}), it does
 *       not exit, and writes no shadow at P.</li>
 * </ul>
 * <p>Control {@code ALIGN_COMPARE_OFF} (the rule before the alignment trust): every row fails with its named text.</p>
 */
public class ReplicaAlignmentTrustTest {

    private static final int PORT_BASE_RESTART = 36600;
    private static final int PORT_BASE_MUTATED = 38400;
    private static final int PORT_BASE_LAGGING = 39600;
    /**
     * Lagging row: the stall bound (a setting; the smallest one the rig's backup cycle allows, 2 x 12 000 +
     * 12 000) and the slow apply that makes the catch-up outlast it twice.
     */
    private static final long LAGGING_VERIFY_WAIT_MS = 2 * 12_000 + ReplicaConfig.DEFAULT_BACKUP_COOLDOWN_MS;
    private static final long LAGGING_SLEEP_PER_FRAME_MS = 20;
    /** The alignment fence family: a compare that differs, a position passed without an action, the bounded wait. */
    static final String ALIGNMENT_FENCE = "history mismatch or";

    /** The stall text a re-genesis fences with, in full. */
    static String stallText(final long applied) {
        return "FATAL replica: unverified continuation at " + applied + " (history mismatch or a stalled backup);"
                + " wipe BASE_DIR";
    }

    // ---------------------------------------------------------------------------- restart from a local shadow

    @Test(timeout = 1_500_000)
    public void aRestartIsUnverifiedUntilTheFirstAlignedCompareAtTheDefaultMemberCadence() throws Exception {
        final String before = System.getProperty("snapshot.interval.minutes");
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_RESTART, "replica-alignment-restart")) {
            // the members' DEFAULT cadence (the rig constructor turned it off; the services read it at construction)
            System.setProperty("snapshot.interval.minutes",
                    Long.toString(com.openexchange.cluster.SnapshotCadence.DEFAULT_INTERVAL_MINUTES));
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.GENESIS, r.replica.readiness().restorePath());
            r.awaitCaughtUp(60_000);
            final long p1 = r.rig.snapshotAll(); // also resets every member's cadence baseline
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            final long snapshotAtMs = System.currentTimeMillis();
            r.stopReplica();
            r.windowGap();

            // restore from a local shadow with the replica's DEFAULT backup query interval
            final long startMs = System.currentTimeMillis();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica")
                    .backupQueryIntervalMs(ReplicaConfig.DEFAULT_BACKUP_QUERY_INTERVAL_MS).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            r.awaitCaughtUp(60_000);
            assertUnverifiedFor(r, 20_000);
            // no STANDBY shadow while UNVERIFIED
            final java.util.List<Long> shadowsBefore = r.allShadowPositions();
            final long standbyTriggers = r.replica.follower().policy().triggerTotal(ShadowKind.STANDBY);
            r.toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
            r.rig.await(() -> r.replica.follower().policy().triggerTotal(ShadowKind.STANDBY) > standbyTriggers,
                    30_000, "the STANDBY action must reach the replica");
            r.rig.pumpMs(3_000);
            assertEquals("no STANDBY shadow while UNVERIFIED", shadowsBefore, r.allShadowPositions());
            assertUnverifiedFor(r, 1_000);

            // the first member snapshot of the default cadence, retrieved at the default query interval
            final long[] readyAt = {-1};
            r.rig.await(() -> {
                // in-process probe for the long wait (minutes of HTTP polling exhausts ephemeral ports on a busy box)
                if (readyAt[0] < 0 && r.probe().ready()) {
                    readyAt[0] = System.currentTimeMillis();
                }
                return readyAt[0] > 0 || !r.fatals.isEmpty();
            }, 9 * 60_000L, () -> "/ready never 200 after the restart: " + r.probe().detail());
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            assertEquals("/ready over HTTP after the first ALIGNED compare", "200", r.httpReady()[0]);
            final long verifiedAt = r.replica.follower().trust().verifiedAt();
            assertTrue("verified by an ALIGNED compare past the restore point " + p1 + ": " + verifiedAt,
                    verifiedAt > p1);
            r.awaitVerified(verifiedAt, ShadowKind.ALIGNED, 60_000);
            // A snapshot still in retrieval is the healthy path; a normal restart logs no named-copy CRITICAL
            final long namedCopyCritical = r.replica.follower().trust().namedCopyCriticalTotal();
            if (namedCopyCritical != 0) {
                throw new AssertionError("a normal restart logged the no-usable-named-copy CRITICAL " + namedCopyCritical
                        + " time(s) while the named member snapshot was in retrieval");
            }
            System.out.println("[alignment] MEASURED not-ready window after a restart from a local shadow at the default member cadence ("
                    + com.openexchange.cluster.SnapshotCadence.DEFAULT_INTERVAL_MINUTES + " min) and the default query"
                    + " interval (" + ReplicaConfig.DEFAULT_BACKUP_QUERY_INTERVAL_MS + " ms): "
                    + (readyAt[0] - startMs) + " ms from replica start (" + (readyAt[0] - snapshotAtMs)
                    + " ms after the previous member snapshot), verified at " + verifiedAt);
            r.stopReplica();

            // control: without the alignment gate the replica is trusted at once and /ready reads 200 before any
            // ALIGNED compare. The control also switches the cluster genesis check off (GENESIS_CHECK_OFF), so the
            // pending genesis compare (a second, independent gate: with cluster/ kept the restarted backup names no
            // member snapshot it already holds, ClusterBackupAgent:502-506) cannot hold /ready at 503 for it; the
            // alignment gate stays falsifiable on its own and the control fails with the /ready text.
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica")
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.ALIGN_COMPARE_OFF,
                            ReplicaNegativeControl.GENESIS_CHECK_OFF)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            RestoreDrillDryRun.nc("restart ALIGN_COMPARE_OFF+GENESIS_CHECK_OFF",
                    "/ready 200 before the first ALIGNED compare", () -> assertUnverifiedFor(r, 20_000));
            System.out.println("GREEN restart unverified until the first ALIGNED compare; p1=" + p1);
        } finally {
            if (before == null) {
                System.clearProperty("snapshot.interval.minutes");
            } else {
                System.setProperty("snapshot.interval.minutes", before);
            }
        }
    }

    /** For {@code ms}: every {@code /ready} is 503 {@code unverified continuation}. */
    static void assertUnverifiedFor(final ReplicaRig r, final long ms) {
        final long end = System.currentTimeMillis() + ms;
        int unverified = 0;
        while (System.currentTimeMillis() < end) {
            final String[] ready = r.httpReady();
            if ("200".equals(ready[0])) {
                throw new AssertionError("/ready 200 before the first ALIGNED compare: " + ready[1]);
            }
            if (ready[1].contains("unverified continuation")) {
                unverified++;
            }
            r.rig.pumpMs(500);
        }
        if (unverified == 0) {
            throw new AssertionError("/ready never read 'unverified continuation': " + r.httpReady()[1]);
        }
    }

    // ---------------------------------------------------------------------------------------- lagging restart

    /**
     * A lagging restart. The replica is stopped while the members advance and snapshot twice; on
     * restart its catch-up to the newest member snapshot is slowed (a test seam, {@code sleepPerFrameMs}) so it takes
     * several times {@code REPLICA_VERIFY_WAIT_MS}, its applied position advancing all the while. It must not fence,
     * and it becomes ready (200) once the ALIGNED at the newest member snapshot compares EQUAL.
     */
    @Test(timeout = 1_200_000)
    public void aLaggingRestartWhoseCatchUpOutlastsTheBoundBecomesReady() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_LAGGING, "replica-alignment-lagging")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            final long stoppedAt = r.replica.follower().appliedPosition();
            r.stopReplica();

            // the members advance and snapshot while the replica is down (the drain belt ticks the log meanwhile)
            for (int i = 0; i < 4; i++) {
                r.traffic(30, 6);
                r.rig.pumpMs(5_000);
            }
            final long older = r.rig.snapshotAll();
            for (int i = 0; i < 4; i++) {
                r.traffic(30, 6);
                r.rig.pumpMs(5_000);
            }
            final long newest = r.rig.snapshotAll();
            r.windowGap();
            System.out.println("[alignment] lagging: stopped at " + stoppedAt + ", member snapshots " + older + " and "
                    + newest + " while down, commit " + r.commit());

            r.faults.sleepPerFrameMs = LAGGING_SLEEP_PER_FRAME_MS;
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(LAGGING_VERIFY_WAIT_MS).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            final long t0 = System.currentTimeMillis();
            r.rig.await(() -> r.replica.follower().appliedPosition() >= newest || !r.fatals.isEmpty(), 600_000,
                    () -> "the catch-up never reached " + newest + ": " + r.probe().detail());
            final long catchUpMs = System.currentTimeMillis() - t0;
            if (!r.fatals.isEmpty()) {
                throw new AssertionError("a lagging restart fenced during its catch-up (after " + catchUpMs
                        + " ms, REPLICA_VERIFY_WAIT_MS=" + LAGGING_VERIFY_WAIT_MS + ", applied "
                        + r.replica.follower().appliedPosition() + ", newest member snapshot " + newest + "): "
                        + r.fatals);
            }
            r.faults.sleepPerFrameMs = 0;
            assertTrue("the catch-up must outlast the bound for this row to mean anything: " + catchUpMs + " ms",
                    catchUpMs > 2 * LAGGING_VERIFY_WAIT_MS);
            final long[] readyAt = {-1};
            r.rig.await(() -> {
                if (readyAt[0] < 0 && r.probe().ready()) {
                    readyAt[0] = System.currentTimeMillis();
                }
                return readyAt[0] > 0 || !r.fatals.isEmpty();
            }, 180_000, () -> "/ready never 200 after the lagging restart: " + r.probe().detail());
            if (!r.fatals.isEmpty()) {
                throw new AssertionError("a lagging restart fenced after its catch-up: " + r.fatals);
            }
            assertEquals("/ready over HTTP after the lagging restart", "200", r.httpReady()[0]);
            final long verifiedAt = r.replica.follower().trust().verifiedAt();
            assertTrue("verified at the newest member snapshot " + newest + ": " + verifiedAt, verifiedAt >= newest);
            r.rig.pumpMs(2 * LAGGING_VERIFY_WAIT_MS);
            assertTrue("no fatal while following: " + r.fatals, r.fatals.isEmpty());
            System.out.println("GREEN lagging restart: catch-up " + catchUpMs + " ms > REPLICA_VERIFY_WAIT_MS="
                    + LAGGING_VERIFY_WAIT_MS + ", ready " + (readyAt[0] - t0) + " ms after bootstrap, verified at "
                    + verifiedAt + ", UNVERIFIABLE skipped " + r.replica.follower().trust().unverifiableTotal());
        }
    }

    // ------------------------------------------------------------------------------------------ re-genesis

    // The re-genesis rows (cluster/ kept or wiped x new history shorter or longer than the restore
    // point) are ONE matrix now, ReplicaReGenesisMatrixTest; every cell fences with the cluster-genesis text.

    // ------------------------------------------------------------------------------------------ mutated state

    @Test(timeout = 900_000)
    public void aMutatedReplicaStateFencesAtTheNextAlignedCompare() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_MUTATED, "replica-alignment-mutated")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            ReplicaTestBindings.DroppingEngine dropping = startDropping(r, "replica", ReplicaRig.off());
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            System.out.println("[alignment] mutated: VERIFIED ALIGNED at " + p1 + " (shadow_replica_verified="
                    + r.metric("shadow_replica_verified") + ")");
            dropping.armDropNext();
            r.tool.deposits(3);
            awaitDropped(r, dropping);
            final long p2 = r.rig.snapshotAll();
            assertDivergenceFenced(r, p2);
            System.out.println("[alignment] mutated: " + r.holds.get(0));
            r.stopReplica();

            r.windowGap();
            dropping = startDropping(r, "replica-nc", ReplicaRig.off(ReplicaNegativeControl.ALIGN_COMPARE_OFF));
            dropping.armDropNext();
            r.tool.deposits(3);
            awaitDropped(r, dropping);
            final long p3 = r.rig.snapshotAll();
            RestoreDrillDryRun.nc("mutated ALIGN_COMPARE_OFF", "divergence not fenced at the next ALIGNED",
                    () -> assertDivergenceFenced(r, p3));
            System.out.println("GREEN mutated state fenced at " + p2);
        }
    }

    /** The seam dropped one session message on the replica, then the replica caught up. */
    private static void awaitDropped(final ReplicaRig r, final ReplicaTestBindings.DroppingEngine dropping) {
        final ReplicaTestBindings.DroppingEngine d = dropping;
        r.rig.await(() -> d.droppedAt() > 0, 60_000, "the seam must drop the next session message");
        r.awaitCaughtUp(60_000);
    }

    private static ReplicaTestBindings.DroppingEngine startDropping(final ReplicaRig r, final String dir,
                                                                    final Set<ReplicaNegativeControl> off) {
        final ReplicaTestBindings.DroppingEngine[] holder = new ReplicaTestBindings.DroppingEngine[1];
        r.startReplica(r.builder(ReplicaRig.SLOT, dir).testHooks(r.faults, off).build(),
                ReplicaTestBindings.withEngine(
                        ReplicaTestBindings.production(() -> r.fatals.add("CoreConfig cross-check failed")),
                        e -> holder[0] = new ReplicaTestBindings.DroppingEngine(e)));
        r.awaitBootstrapped(ReplicaRig.WAIT_MS);
        r.awaitCaughtUp(60_000);
        return holder[0];
    }

    private static void assertDivergenceFenced(final ReplicaRig r, final long p) {
        final String text = "oracle mismatch at " + p + ": ";
        try {
            r.rig.await(() -> r.holds.stream().anyMatch(f -> f.startsWith(text)), 120_000, "divergence");
        } catch (final AssertionError timeout) {
            throw new AssertionError("divergence not fenced at the next ALIGNED (" + p + "): holds " + r.holds
                    + " fatals " + r.fatals + " verified " + r.verified(ShadowKind.ALIGNED));
        }
        assertTrue("a byte mismatch holds, it does not exit: " + r.fatals, r.fatals.isEmpty());
        assertEquals("mismatch counted", 1, r.metric("shadow_oracle_mismatch_total"));
        final LogicalShadow s = r.scan() == null ? null : r.scan().atPosition(p);
        assertTrue("no shadow at the diverged position " + p, s == null);
    }
}

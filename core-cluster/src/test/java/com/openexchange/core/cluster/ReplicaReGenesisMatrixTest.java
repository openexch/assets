// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import org.agrona.IoUtil;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * ONE matrix, on a real three-member core with the in-JVM replica. A re-genesis (every member wiped, a new genesis
 * ADOPTed) under a replica that restores its old-history shadow from a local shadow at the restore point p1:
 *
 * <pre>
 *                      new history SHORTER than p1     new history LONGER than p1
 *   cluster/ KEPT      cell keptShorter                cell keptLonger
 *   cluster/ WIPED     cell wipedShorter               cell wipedLonger
 * </pre>
 *
 * <p>Every cell: no fence and never ready while the new cluster has no member snapshot; then the new cluster's first
 * member snapshot, and the replica fences with the cluster-genesis text in full
 * ({@code FATAL replica: cluster genesis <g2> != local shadows <g1>; wipe BASE_DIR}) within one member cadence (the
 * test's manual snapshot) plus one query interval; nothing of the new history is written to a shadow, and
 * {@code /ready} is never 200. The genesis check compares against the member snapshot the live member's backup
 * response named, never against the local RecordingLog's newest-by-position (which,
 * with {@code cluster/} kept, is the OLD history's snapshot).</p>
 *
 * <p>Control {@code GENESIS_CHECK_OFF}: every cell fails with {@code cell <cell>: not fenced with the cluster-genesis
 * text}. The alignment trust still keeps every new-history frame out of the shadows under the control (asserted
 * first).</p>
 *
 * <p>Two more rows for a replica that keeps RUNNING across the re-genesis (VERIFIED, {@code cluster/} kept):</p>
 * <ul>
 *   <li>{@code standbyInANewHistoryIsPurgedAtTheHistoryFence}: the new history passes the old L stop, so the backup
 *       extends L and the replica applies new-history frames. A STANDBY toggle there, before the new cluster has any
 *       snapshot, is written (nothing new is named: the freshness rule allows it, and nothing can know yet). The new
 *       cluster's first snapshot is then named: the replica fences as another history (the compare reads another
 *       genesis, or the genesis check fires first), the STANDBY shadow of the window is purged BEFORE the exit
 *       ({@code shadow_poisoned_purged_total >= 1} when the fatal is seen), and it is gone from the archive. Controls:
 *       {@code GENESIS_CHECK_OFF} -> {@code a kept re-genesis was latched as a byte mismatch hold};
 *       {@code SUSPECT_PURGE_OFF} -> {@code poisoned shadow at <Q> kept after a history fence}. The backup extends L
 *       at the old stop only while that stop is a frame end of the new log (the archive refuses a replay from inside a
 *       frame: {@code replayPosition=<P> ... does not point to a valid frame}), so the STANDBY of the window is
 *       often never reached; the row then asserts what holds in every case (a history fence, never a hold, and no
 *       shadow above the last EQUAL position left) and is reported SKIPPED ({@code window A not reached in <n>
 *       attempts}), never passed, as is a row whose controls did not reach window A.</li>
 *   <li>{@code aVerifiedReplicaNotFollowingANewHistoryFencesAtTheNamedSnapshot}: the new history is SHORTER than what
 *       the replica applied, so L is not extended; the new cluster's first snapshot lands between the replica's last
 *       EQUAL position and its applied position, where it applied no SNAPSHOT action: a VERIFIED replica fences
 *       (the applied-past text, or the genesis check). Control {@code TICK_UNVERIFIED_ONLY} with
 *       {@code GENESIS_CHECK_OFF} -> {@code VERIFIED replica kept following another history}.</li>
 * </ul>
 */
public class ReplicaReGenesisMatrixTest {

    private static final int PORT_BASE_KEPT_SHORTER = 40800;
    private static final int PORT_BASE_KEPT_LONGER = 37200;
    private static final int PORT_BASE_WIPED_SHORTER = 40200;
    private static final int PORT_BASE_WIPED_LONGER = 37800;
    private static final long REGENESIS = 0x2EED5EEDL;
    /** The old history's length before p1 in a SHORTER cell, so the new history's first snapshot lies below it. */
    private static final long OLD_HISTORY_BYTES = 4L * 1024 * 1024;
    /**
     * The fence bound allows one unanswered backup query: two backup cycles (the replica's
     * query interval plus its cool-down, read from the running replica's settings) plus the retrieval time of the new
     * cluster's first snapshot; this allowance is that retrieval time (the snapshot and its decode), stated here and
     * logged with the measured value and the backup queries and responses per cell.
     */
    private static final long RETRIEVAL_ALLOWANCE_MS = 15_000;

    static long fenceBoundMs(final ReplicaRig r) {
        return 2 * (r.replica.config().backupQueryIntervalMs() + r.replica.config().backupCooldownMs())
                + RETRIEVAL_ALLOWANCE_MS;
    }
    /** The verify wait (a setting; at least 2 x 12 000 + 12 000 with the rig's query interval). */
    private static final long VERIFY_WAIT_MS = 60_000;

    static String clusterGenesisText(final long clusterGenesis, final long localGenesis) {
        return "FATAL replica: cluster genesis " + clusterGenesis + " != local shadows " + localGenesis
                + "; wipe BASE_DIR";
    }

    @Test(timeout = 1_500_000)
    public void keptShorter() throws Exception {
        cell(PORT_BASE_KEPT_SHORTER, "keptShorter", false, true);
    }

    @Test(timeout = 1_500_000)
    public void keptLonger() throws Exception {
        cell(PORT_BASE_KEPT_LONGER, "keptLonger", false, false);
    }

    @Test(timeout = 1_500_000)
    public void wipedShorter() throws Exception {
        cell(PORT_BASE_WIPED_SHORTER, "wipedShorter", true, true);
    }

    @Test(timeout = 1_500_000)
    public void wipedLonger() throws Exception {
        cell(PORT_BASE_WIPED_LONGER, "wipedLonger", true, false);
    }

    private void cell(final int portBase, final String cell, final boolean wipeClusterDir, final boolean shorter)
            throws Exception {
        try (ReplicaRig r = new ReplicaRig(portBase, "replica-regenesis-" + cell)) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            final long t0 = System.currentTimeMillis();
            if (shorter) {
                while (r.commit() < OLD_HISTORY_BYTES) {
                    r.traffic(60, 12);
                    if (System.currentTimeMillis() - t0 > 600_000) {
                        throw new AssertionError("cell " + cell + " setup: the old history never reached "
                                + OLD_HISTORY_BYTES + " B (commit " + r.commit() + ")");
                    }
                }
            }
            r.awaitCaughtUp(120_000);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            final long g1 = r.replica.follower().knownGenesis();
            final long oldStop = r.replica.follower().recordedPosition();
            final long oldApplied = r.replica.follower().appliedPosition();
            final long oldAppliedTime = r.replica.follower().appliedClusterTime();
            final Set<Long> oldShadows = shadowRecordings(r);
            r.stopReplica();
            final long stoppedAtMs = System.currentTimeMillis();
            System.out.println("[regenesis] " + cell + ": old genesis " + g1 + " VERIFIED ALIGNED at p1=" + p1 + ", L stop "
                    + oldStop + ", shadows " + oldShadows);

            // re-genesis: every member wiped and booted fresh, a new genesis ADOPTed, NO member snapshot yet
            r.rig.forgetAll();
            for (final Member m : r.rig.members) {
                r.rig.kill(m);
            }
            for (final Member m : r.rig.members) {
                IoUtil.delete(m.baseDir, false);
            }
            for (final Member m : r.rig.members) {
                r.rig.restart(m);
                r.hook(m);
            }
            r.rig.adoptGenesis = REGENESIS;
            final long regenesisAtMs = System.currentTimeMillis();
            final Member leader2 = r.rig.awaitLeader(null);
            final Member host2 = r.rig.followerOtherThan(leader2, null);
            r.story(host2, 4);
            if (!shorter) {
                while (r.commit() <= oldStop + 4096) {
                    r.traffic(30, 6);
                }
            }
            System.out.println("[regenesis] " + cell + ": re-genesised (" + REGENESIS + "), commit " + r.commit()
                    + ", no member snapshot yet, cluster/ " + (wipeClusterDir ? "WIPED" : "KEPT"));
            if (wipeClusterDir) {
                IoUtil.delete(new File(r.replicaBase("replica"), "cluster"), false);
            }
            // the control phase restarts from a copy of the dir as it is here: the first phase's backup records the
            // new history into L and rebuilds cluster/ (a WIPED cell would no longer be wiped). The history fence
            // keeps the shadows at or below the restore point p1 (asserted after the first phase)
            copyTree(r.replicaBase("replica").toPath(), r.replicaBase("replica-saved").toPath());
            r.windowGap();

            final String genesisText = clusterGenesisText(REGENESIS, g1);
            final TailEvidence tail = wipeClusterDir ? null : new TailEvidence(oldStop, oldApplied, oldAppliedTime, stoppedAtMs,
                    regenesisAtMs);
            phase(r, cell, ReplicaRig.off(), p1, shorter, oldShadows, tail);
            if (!r.fatals.contains(genesisText)) {
                throw new AssertionError("cell " + cell + ": not fenced with the cluster-genesis text '" + genesisText + "' within "
                        + "the bound of the first post-ADOPT member snapshot: fatals " + r.fatals
                        + " detail " + r.probe().detail());
            }
            assertTrue("/ready never 200", !r.probe().ready());
            if (!r.allShadowPositions().contains(p1)) {
                throw new AssertionError("cell " + cell + ": the history fence purged the restore point's shadow at "
                        + p1 + " (the last copy of the old history): positions " + r.allShadowPositions());
            }
            System.out.println("[regenesis] " + cell + ": " + r.fatals.get(0) + "; the shadow at p1=" + p1
                    + " kept");
            r.stopReplica();

            r.windowGap();
            IoUtil.delete(r.replicaBase("replica"), false);
            copyTree(r.replicaBase("replica-saved").toPath(), r.replicaBase("replica").toPath());
            RestoreDrillDryRun.nc("cell " + cell + " GENESIS_CHECK_OFF", "cell " + cell + ": not fenced with the cluster-genesis text",
                    () -> {
                        phase(r, cell + " NC", ReplicaRig.off(ReplicaNegativeControl.GENESIS_CHECK_OFF), p1, shorter,
                                oldShadows, null);
                        if (!r.fatals.contains(genesisText)) {
                            throw new AssertionError("cell " + cell + ": not fenced with the cluster-genesis text under the"
                                    + " control: fatals " + r.fatals);
                        }
                    });
            System.out.println("GREEN cell " + cell + " p1=" + p1 + ": genesis check fenced");
        }
    }

    // ------------------------------------------------------------------- a running replica across a re-genesis

    private static final int PORT_BASE_RUNNING_FOLLOWS = 44400;
    private static final int PORT_BASE_RUNNING_FOLLOWS_NC_GENESIS = 45000;
    private static final int PORT_BASE_RUNNING_FOLLOWS_NC_PURGE = 45600;
    private static final int PORT_BASE_RUNNING_PAST = 46200;
    private static final int PORT_BASE_RUNNING_PAST_NC = 46800;
    private static final long REGENESIS_RUNNING = 0x3EED5EEDL;

    /**
     * Whether the backup extends L into the new history depends on the old stop being a frame end of the new log, which
     * the cluster's timer frames make a matter of timing: each run of the window-A shape is tried up to this many
     * times (a fresh rig each), and a row that never reached it says so.
     */
    private static final int WINDOW_A_ATTEMPTS = 3;

    @Test(timeout = 5_400_000)
    public void standbyInANewHistoryIsPurgedAtTheHistoryFence() throws Exception {
        final Running main = untilWindowA(PORT_BASE_RUNNING_FOLLOWS, "follows", ReplicaRig.off());
        if (main.fatal == null || !(main.fatal.startsWith(AlignmentTrustTexts.differs(main.firstSnapshot))
                || main.fatal.equals(clusterGenesisText(REGENESIS_RUNNING, main.oldGenesis)))) {
            throw new AssertionError("follows: not fenced as another history: fatal " + main.fatal + ", holds "
                    + main.holds);
        }
        if (!main.holds.isEmpty()) {
            throw new AssertionError("a kept re-genesis was latched as a byte mismatch hold: " + main.holds);
        }
        if (main.newShadowsKept) {
            throw new AssertionError("follows: a shadow above the last EQUAL position survived the history fence");
        }
        // the backup extends L at the old stop only while that stop is a frame end of the new log: the archive
        // refuses a replay from inside a frame. The fence above holds in every case; without window A the purge
        // rows and their controls are not evaluated, and the row is reported SKIPPED, not passed.
        System.out.println("[regenesis] running follows: fenced: " + main.fatal + "; nothing of the new history"
                + " survived" + (main.standby < 0 ? "; window A not reached: " + main.notReached : ""));
        Assume.assumeTrue("window A not reached in " + WINDOW_A_ATTEMPTS + " attempts: the purge rows and their"
                + " controls were not evaluated (" + main.notReached + ")", main.standby >= 0);
        if (main.purgedAtFatal < 1) {
            throw new AssertionError("follows: the fatal came before the suspect purge (purged " + main.purgedAtFatal
                    + " when the fatal was seen)");
        }
        if (main.standbyKept) {
            throw new AssertionError("poisoned shadow at " + main.standby + " kept after a history fence");
        }
        System.out.println("GREEN running re-genesis followed: STANDBY at " + main.standby + " written in the window,"
                + " purged (" + main.purgedAtFatal + ") before the fatal: " + main.fatal + "; stale_named_total "
                + main.staleNamed + "; other_genesis_total " + main.otherGenesis);
        final List<String> notEvaluated = new java.util.ArrayList<>();
        final Running ncGenesis = untilWindowA(PORT_BASE_RUNNING_FOLLOWS_NC_GENESIS, "follows-nc-genesis",
                ReplicaRig.off(ReplicaNegativeControl.GENESIS_CHECK_OFF));
        if (ncGenesis.standby < 0) {
            System.out.println("NC follows GENESIS_CHECK_OFF NOT EVALUATED: window A not reached in "
                    + WINDOW_A_ATTEMPTS + " attempts");
            notEvaluated.add("GENESIS_CHECK_OFF");
        } else {
            RestoreDrillDryRun.nc("follows GENESIS_CHECK_OFF", "a kept re-genesis was latched as a byte mismatch hold",
                    () -> {
                        if (!ncGenesis.holds.isEmpty()) {
                            throw new AssertionError("a kept re-genesis was latched as a byte mismatch hold: "
                                    + ncGenesis.holds);
                        }
                    });
        }
        final Running ncPurge = untilWindowA(PORT_BASE_RUNNING_FOLLOWS_NC_PURGE, "follows-nc-purge",
                ReplicaRig.off(ReplicaNegativeControl.SUSPECT_PURGE_OFF));
        if (ncPurge.standby < 0) {
            System.out.println("NC follows SUSPECT_PURGE_OFF NOT EVALUATED: window A not reached in "
                    + WINDOW_A_ATTEMPTS + " attempts");
            notEvaluated.add("SUSPECT_PURGE_OFF");
        } else {
            RestoreDrillDryRun.nc("follows SUSPECT_PURGE_OFF", "kept after a history fence", () -> {
                if (ncPurge.standbyKept) {
                    throw new AssertionError("poisoned shadow at " + ncPurge.standby + " kept after a history fence");
                }
            });
        }
        Assume.assumeTrue("window A not reached in " + WINDOW_A_ATTEMPTS + " attempts by the control(s) "
                + notEvaluated + ": not evaluated", notEvaluated.isEmpty());
    }

    private static final int PORT_BASE_GENESIS_START = 51000;
    private static final int PORT_BASE_GENESIS_START_NC = 51600;
    private static final long REGENESIS_GENESIS_START = 0x4EED5EEDL;

    /**
     * A replica that started at GENESIS (no local shadow named its genesis; it learned it from the ADOPT) keeps running
     * while every member is wiped and another genesis ADOPTed, {@code cluster/} kept; the new history stays below the
     * old L stop, so L is not extended. The new cluster's first member snapshot is named: the genesis check, armed at
     * the ADOPT, compares it against the adopted genesis and fences with the cluster-genesis text (exit 1). Control
     * {@code GENESIS_CHECK_OFF} -> {@code a genesis-start replica followed another history without a fence}.
     */
    @Test(timeout = 1_800_000)
    public void genesisStartReplicaFencesOnAKeptReGenesis() throws Exception {
        final String[] fatal = genesisStart(PORT_BASE_GENESIS_START, "genesis-start", ReplicaRig.off());
        assertGenesisStartFenced(fatal);
        System.out.println("GREEN genesis-start replica fenced on a kept re-genesis: " + fatal[0]);
        RestoreDrillDryRun.nc("genesis-start GENESIS_CHECK_OFF", "a genesis-start replica followed another history"
                + " without a fence", () -> {
                    try {
                        assertGenesisStartFenced(genesisStart(PORT_BASE_GENESIS_START_NC, "genesis-start-nc",
                                ReplicaRig.off(ReplicaNegativeControl.GENESIS_CHECK_OFF)));
                    } catch (final AssertionError | RuntimeException e) {
                        throw e;
                    } catch (final Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    private static void assertGenesisStartFenced(final String[] fatal) {
        if (fatal[0] == null || !fatal[0].equals(fatal[1])) {
            throw new AssertionError("a genesis-start replica followed another history without a fence: fatal "
                    + fatal[0] + ", expected " + fatal[1] + "; " + fatal[2]);
        }
    }

    /** {fatal or null, the expected cluster-genesis text, the readiness detail at the end}. */
    private static String[] genesisStart(final int portBase, final String cell, final Set<ReplicaNegativeControl> off)
            throws Exception {
        try (ReplicaRig r = new ReplicaRig(portBase, "replica-regenesis-" + cell)) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            r.story(r.rig.followerOtherThan(leader, null), 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(VERIFY_WAIT_MS).testHooks(r.faults, off)
                    .build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.GENESIS, r.replica.readiness().restorePath());
            // the old history runs well past where the new one's first snapshot will land
            while (r.commit() < 1024 * 1024) {
                r.traffic(60, 12);
            }
            r.awaitCaughtUp(120_000);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            final long g1 = r.replica.follower().knownGenesis();
            final long oldStop = r.replica.follower().recordedPosition();
            System.out.println("[regenesis] " + cell + ": GENESIS start, VERIFIED at " + p1 + ", genesis " + g1
                    + ", L stop " + oldStop + "; re-genesis with the replica running, cluster/ kept");

            r.rig.forgetAll();
            for (final Member m : r.rig.members) {
                r.rig.kill(m);
            }
            for (final Member m : r.rig.members) {
                IoUtil.delete(m.baseDir, false);
            }
            for (final Member m : r.rig.members) {
                r.rig.restart(m);
                r.hook(m);
            }
            r.rig.adoptGenesis = REGENESIS_GENESIS_START;
            final Member leader2 = r.rig.awaitLeader(null);
            r.story(r.rig.followerOtherThan(leader2, null), 4);
            assertTrue("precondition: the new history stays below the old L stop " + oldStop + " (commit "
                    + r.commit() + ")", r.commit() < oldStop);
            final long first = r.rig.snapshotAll();
            final long bound = fenceBoundMs(r) + r.replica.config().fencePurgeWaitMs();
            try {
                r.rig.await(() -> !r.fatals.isEmpty() || !r.holds.isEmpty(), bound, "fence");
            } catch (final AssertionError timeout) {
                // judged by the caller
            }
            final String detail = "first new snapshot at " + first + ", bound " + bound + " ms, holds " + r.holds
                    + ", /ready " + r.probe().detail();
            System.out.println("[regenesis] " + cell + ": fatals " + r.fatals + "; " + detail);
            return new String[] {r.fatals.isEmpty() ? null : r.fatals.get(0),
                clusterGenesisText(REGENESIS_GENESIS_START, g1), detail};
        }
    }

    /** The window-A shape, tried until the STANDBY of the new history was written (or the attempts ran out). */
    private static Running untilWindowA(final int portBase, final String cell, final Set<ReplicaNegativeControl> off) {
        Running last = null;
        for (int attempt = 1; attempt <= WINDOW_A_ATTEMPTS; attempt++) {
            last = running(portBase, cell + (attempt == 1 ? "" : "-" + attempt), true, off);
            if (last.standby > 0) {
                return last;
            }
            System.out.println("[regenesis] running " + cell + ": attempt " + attempt + " did not reach window A");
        }
        return last;
    }

    @Test(timeout = 2_400_000)
    public void aVerifiedReplicaNotFollowingANewHistoryFencesAtTheNamedSnapshot() throws Exception {
        final Running main = running(PORT_BASE_RUNNING_PAST, "past", false, ReplicaRig.off());
        if (main.fatal == null || !(main.fatal.startsWith(AlignmentTrustTexts.differs(main.firstSnapshot))
                || main.fatal.equals(clusterGenesisText(REGENESIS_RUNNING, main.oldGenesis)))) {
            throw new AssertionError("past: not fenced as another history: fatal " + main.fatal + ", holds "
                    + main.holds);
        }
        System.out.println("GREEN running re-genesis not followed: " + main.fatal);
        RestoreDrillDryRun.nc("past TICK_UNVERIFIED_ONLY+GENESIS_CHECK_OFF",
                "VERIFIED replica kept following another history", () -> {
                    final Running nc = running(PORT_BASE_RUNNING_PAST_NC, "past-nc", false,
                            ReplicaRig.off(ReplicaNegativeControl.TICK_UNVERIFIED_ONLY,
                                    ReplicaNegativeControl.GENESIS_CHECK_OFF));
                    if (nc.fatal == null && nc.holds.isEmpty()) {
                        throw new AssertionError("VERIFIED replica kept following another history (applied "
                                + nc.applied + ", the new cluster's snapshot at " + nc.firstSnapshot + ")");
                    }
                });
    }

    /** The history fence texts (the kit's, package-private there). */
    private static final class AlignmentTrustTexts {
        static String differs(final long position) {
            return "FATAL replica: state at " + position + " differs from member snapshot (history mismatch or"
                    + " divergence)";
        }
    }

    /** What one running-replica re-genesis produced. */
    private static final class Running {
        long oldGenesis;
        long standby = -1;
        long firstSnapshot = -1;
        long applied;
        String fatal;
        List<String> holds = List.of();
        long purgedAtFatal = -1;
        boolean standbyKept;
        long staleNamed;
        long otherGenesis;
        boolean newShadowsKept;
        String notReached;
    }

    /**
     * A VERIFIED replica keeps running while every member is wiped and a new genesis adopted. {@code follows}: the
     * new history passes the old L stop (the backup extends L, the replica applies new frames) and a STANDBY toggle in
     * it is written before anything new is named; else the new history stays below what the replica applied and its
     * first snapshot lands between the replica's last EQUAL and its applied position. Then the new cluster's first
     * snapshot; waits for the fence (or a hold) within the fence bound plus the purge wait.
     */
    private static Running running(final int portBase, final String cell, final boolean follows,
                                   final Set<ReplicaNegativeControl> off) {
        try {
            return runningChecked(portBase, cell, follows, off);
        } catch (final RuntimeException | Error e) {
            throw e;
        } catch (final Exception e) {
            throw new IllegalStateException("running " + cell + ": " + e, e);
        }
    }

    private static Running runningChecked(final int portBase, final String cell, final boolean follows,
                                          final Set<ReplicaNegativeControl> off) throws Exception {
        final Running out = new Running();
        try (ReplicaRig r = new ReplicaRig(portBase, "replica-regenesis-running-" + cell)) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            r.story(r.rig.followerOtherThan(leader, null), 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(VERIFY_WAIT_MS).testHooks(r.faults, off)
                    .build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(120_000);
            long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            if (follows) {
                // a replica that knows its genesis from a local shadow (a genesis start has none to protect, so its
                // genesis check passes for good): restart it on its dir, then VERIFIED again before the re-genesis
                r.stopReplica();
                r.windowGap();
                r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(VERIFY_WAIT_MS)
                        .testHooks(r.faults, off).build());
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
                r.awaitCaughtUp(120_000);
                r.traffic(10, 2);
                p1 = r.rig.snapshotAll();
                r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            }
            if (!follows) {
                // the replica applies well past its last EQUAL, so the new cluster's first snapshot can land between
                while (r.commit() < p1 + 2L * 1024 * 1024) {
                    r.traffic(60, 12);
                }
                r.awaitCaughtUp(120_000);
            }
            out.oldGenesis = r.replica.follower().knownGenesis();
            final long oldStop = r.replica.follower().recordedPosition();
            final long oldApplied = r.replica.follower().appliedPosition();
            System.out.println("[regenesis] running " + cell + ": VERIFIED at " + p1 + ", L stop " + oldStop
                    + ", applied " + oldApplied + "; re-genesis with the replica running");

            r.rig.forgetAll();
            for (final Member m : r.rig.members) {
                r.rig.kill(m);
            }
            for (final Member m : r.rig.members) {
                IoUtil.delete(m.baseDir, false);
            }
            for (final Member m : r.rig.members) {
                r.rig.restart(m);
                r.hook(m);
            }
            r.rig.adoptGenesis = REGENESIS_RUNNING;
            final Member leader2 = r.rig.awaitLeader(null);
            final Member host2 = r.rig.followerOtherThan(leader2, null);
            r.story(host2, 4);
            if (follows) {
                while (r.commit() <= oldStop + 4096) {
                    r.traffic(30, 6);
                }
                // window A: a STANDBY in the new history before its first snapshot is named
                final int frames = r.actionFrames(host2, 1).size();
                r.toggle(io.aeron.cluster.ClusterControl.ToggleState.STANDBY_SNAPSHOT);
                final long[] q = {-1};
                r.rig.await(() -> {
                    final List<Long> f = r.actionFrames(host2, 1);
                    if (f.size() > frames) {
                        q[0] = f.get(f.size() - 1);
                    }
                    return q[0] > 0;
                }, 30_000, "no flags-1 action in the new history");
                r.traffic(10, 0);
                try {
                    r.awaitVerified(q[0], ShadowKind.STANDBY, ReplicaRig.WAIT_MS);
                    out.standby = q[0];
                    System.out.println("[regenesis] running " + cell + ": STANDBY at " + q[0] + " written in the new"
                            + " history (applied " + r.replica.follower().appliedPosition() + ")");
                } catch (final AssertionError notWritten) {
                    out.notReached = "the replica applied " + r.replica.follower().appliedPosition() + ", recorded "
                            + r.replica.follower().recordedPosition() + " of the new log (old L stop " + oldStop
                            + "), the STANDBY action at " + q[0] + " never reached it: " + r.probe().detail();
                    System.out.println("[regenesis] running " + cell + ": window A not reached: " + out.notReached);
                }
            } else {
                final long target = p1 + (oldApplied - p1) / 2;
                while (r.commit() < target) {
                    r.traffic(30, 6);
                }
                if (r.commit() >= oldApplied) {
                    throw new AssertionError("running " + cell + " precondition: the new history passed the applied "
                            + oldApplied);
                }
            }
            r.traffic(5, 0);
            out.firstSnapshot = r.rig.snapshotAll();
            System.out.println("[regenesis] running " + cell + ": the new cluster's first snapshot at "
                    + out.firstSnapshot + " (old L stop " + oldStop + ", replica applied "
                    + r.replica.follower().appliedPosition() + ")");
            final long bound = fenceBoundMs(r) + r.replica.config().fencePurgeWaitMs();
            final Set<Long> beforeFence = shadowRecordings(r);
            final long[] purged = {-1};
            final boolean[] second = {false};
            try {
                r.rig.await(() -> {
                    if (!r.fatals.isEmpty() && purged[0] < 0) {
                        purged[0] = r.metric("shadow_poisoned_purged_total");
                    }
                    if (!second[0] && follows && r.fatals.isEmpty()
                            && r.replica.events().newestReportedServiceSnapshot() >= out.firstSnapshot) {
                        // after the naming: a second STANDBY must be suspended (or the fence already latched)
                        second[0] = true;
                        r.toggle(io.aeron.cluster.ClusterControl.ToggleState.STANDBY_SNAPSHOT);
                    }
                    return !r.fatals.isEmpty() || !r.holds.isEmpty();
                }, bound, "fence");
            } catch (final AssertionError timeout) {
                // judged by the caller
            }
            out.fatal = r.fatals.isEmpty() ? null : r.fatals.get(0);
            out.holds = List.copyOf(r.holds);
            out.purgedAtFatal = purged[0];
            out.applied = r.replica.follower().appliedPosition();
            out.staleNamed = r.metric("shadow_oracle_stale_named_total");
            out.otherGenesis = r.metric("shadow_oracle_other_genesis_total");
            out.standbyKept = out.standby > 0 && r.allShadowPositions().contains(out.standby);
            for (final long pos : r.allShadowPositions()) {
                out.newShadowsKept |= pos > p1;
            }
            System.out.println("[regenesis] running " + cell + ": shadow recordings before the fence " + beforeFence
                    + ", positions after " + r.allShadowPositions());
            final String line = "[regenesis] running " + cell + ": fatal " + out.fatal + ", holds " + out.holds
                    + ", poisoned purged when the fatal was seen " + out.purgedAtFatal + ", total "
                    + r.metric("shadow_poisoned_purged_total") + ", STANDBY at " + out.standby + " still present "
                    + out.standbyKept + ", stale_named_total " + out.staleNamed + " (second STANDBY toggled "
                    + second[0] + "), other_genesis_total " + out.otherGenesis + ", bound " + bound + " ms";
            System.out.println(line);
            writeEvidence(cell, line);
        }
        return out;
    }

    private static void writeEvidence(final String cell, final String line) {
        final File dir = new File(System.getProperty("user.dir"), "target/replica-evidence/history-fence");
        if (dir.isDirectory() || dir.mkdirs()) {
            try {
                java.nio.file.Files.writeString(new File(dir, cell + ".log").toPath(), line + "\n");
            } catch (final java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
    }

    /**
     * One replica start: restored from a local shadow at {@code p1}; no fence and never ready for two query cycles
     * without a post-ADOPT member snapshot; then that snapshot (asserted below or above {@code p1} per the cell) and
     * up to the fence bound ({@link #fenceBoundMs}) for the fence; no new shadow (judged before the fence text).
     */
    /** What the kept cells read past the stop the test recorded for the old L. */
    private record TailEvidence(long oldStop, long oldApplied, long oldAppliedClusterTime, long stoppedAtMs, long regenesisAtMs) {
    }

    private static void phase(final ReplicaRig r, final String cell, final Set<ReplicaNegativeControl> off,
                              final long p1, final boolean shorter, final Set<Long> oldShadows, final TailEvidence tail) {
        r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(VERIFY_WAIT_MS).testHooks(r.faults, off)
                .build());
        r.rig.await(() -> r.replica.follower().bootstrapDone() || !r.fatals.isEmpty(), ReplicaRig.WAIT_MS,
                () -> "cell " + cell + ": no bootstrap decision: " + r.probe().detail());
        assertTrue("cell " + cell + " bootstrap: " + r.fatals, r.fatals.isEmpty());
        assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
        assertEquals(p1, r.replica.follower().restoredPosition());
        if (tail != null) {
            tailEvidence(r, cell, tail);
        }
        final long idleUntil = System.currentTimeMillis() + 2 * 12_000;
        while (System.currentTimeMillis() < idleUntil) {
            assertTrue("cell " + cell + ": fenced before any post-ADOPT member snapshot: " + r.fatals,
                    r.fatals.isEmpty());
            assertTrue("cell " + cell + ": ready before any post-ADOPT member snapshot", !r.probe().ready());
            r.rig.pumpMs(500);
        }
        final long q = r.rig.snapshotAll();
        final long snapAt = System.currentTimeMillis();
        if (shorter ? q >= p1 : q <= p1) {
            throw new AssertionError("cell " + cell + " precondition: the new history's first member snapshot at " + q
                    + " is not " + (shorter ? "below" : "above") + " the restore point " + p1);
        }
        System.out.println("[regenesis] " + cell + ": first post-ADOPT member snapshot at " + q + (shorter ? " < " : " > ")
                + "p1=" + p1 + ", replica applied " + r.replica.follower().appliedPosition() + ", "
                + r.probe().detail());
        final long bound = fenceBoundMs(r);
        final long[] seen = {-2, -2, -2, -2};
        try {
            r.rig.await(() -> {
                final long[] now = {r.replica.events().queries(), r.replica.events().responses(),
                        r.replica.events().resets(), r.replica.events().newestReportedServiceSnapshot()};
                if (!java.util.Arrays.equals(now, seen)) {
                    System.arraycopy(now, 0, seen, 0, 4);
                    System.out.println("[regenesis] " + cell + " +" + (System.currentTimeMillis() - snapAt) + " ms: backup"
                            + " queries=" + now[0] + " responses=" + now[1] + " resets=" + now[2] + " newestNamed="
                            + now[3] + " state=" + r.replica.metrics().body().lines()
                            .filter(l -> l.startsWith("shadow_replica_backup_state ")).findFirst().orElse("?"));
                }
                return !r.fatals.isEmpty() || !newShadows(r, oldShadows).isEmpty();
            }, bound, "y1");
        } catch (final AssertionError timeout) {
            // judged by the caller
        }
        final long measured = System.currentTimeMillis() - snapAt;
        System.out.println("[regenesis] " + cell + ": bound = 2 x (query interval "
                + r.replica.config().backupQueryIntervalMs() + " + cool-down " + r.replica.config().backupCooldownMs()
                + ") + retrieval " + RETRIEVAL_ALLOWANCE_MS + " = " + bound + " ms; measured "
                + (r.fatals.isEmpty() ? "no fence after " + measured : measured + " ms to the fence")
                + "; backup queries=" + r.replica.events().queries() + " responses="
                + r.replica.events().responses() + " resets=" + r.replica.events().resets());
        System.out.println("[regenesis] " + cell + ": " + (r.fatals.isEmpty() ? "no fence" : "fenced "
                + measured + " ms after the member snapshot") + ": " + r.fatals
                + " detail " + r.probe().detail());
        final Set<Long> fresh = newShadows(r, oldShadows);
        if (!fresh.isEmpty()) {
            throw new AssertionError("cell " + cell + ": a new-history frame reached a shadow: new shadow recordings "
                    + fresh + " " + r.describeScan());
        }
    }

    /**
     * The frames of L between the stop the test recorded for the old L (the first replica's last 100 ms
     * status read) and the stop the restarted replica found at bootstrap, before its own backup records anything new.
     * Each frame's kind, position, term and timestamp, against the first replica's stop and the re-genesis wall times.
     */
    private static void tailEvidence(final ReplicaRig r, final String cell, final TailEvidence tail) {
        final long l = r.replica.follower().logRecordingId();
        final long stop = r.replicaArchive().getStopPosition(l);
        final long recorded = stop >= 0 ? stop : r.replicaArchive().getRecordingPosition(l);
        System.out.println("[regenesis] " + cell + ": old L=" + l + " test-read stop " + tail.oldStop() + " (applied "
                + tail.oldApplied() + ", clusterTime " + tail.oldAppliedClusterTime() + "), stop at the restart's"
                + " bootstrap " + stop + " (recorded " + recorded + "), " + (recorded - tail.oldStop()) + " B past it;"
                + " first replica stopped at wall " + tail.stoppedAtMs() + ", re-genesis at wall " + tail.regenesisAtMs()
                + ", replica applied " + r.replica.follower().appliedPosition());
        if (recorded <= tail.oldStop()) {
            return;
        }
        final com.openexchange.cluster.testkit.RecordedLog frames =
                com.openexchange.core.infrastructure.persistence.TestShadowProducer.captureLog(r.replica.aeron(),
                        r.replicaArchive(), l, tail.oldStop(), recorded);
        long maxTs = Long.MIN_VALUE;
        for (final com.openexchange.cluster.testkit.LogFrame f : frames.frames()) {
            maxTs = Math.max(maxTs, f.timestamp());
            System.out.println("[regenesis] " + cell + ":   frame " + f.kind() + " at " + f.position() + " term "
                    + f.leadershipTermId() + " timestamp " + f.timestamp());
        }
        System.out.println("[regenesis] " + cell + ": " + frames.size() + " frame(s) past the test-read stop; newest frame"
                + " timestamp " + maxTs + " vs first replica stop wall " + tail.stoppedAtMs() + " and re-genesis wall "
                + tail.regenesisAtMs() + ": " + (maxTs <= tail.stoppedAtMs() ? "OLD history (recorded before the first"
                + " replica's backup closed)" : "NOT before the first replica stopped"));
    }

    /** A copy of a stopped replica's dir (archive, catalog, cluster/), file by file. */
    private static void copyTree(final java.nio.file.Path from, final java.nio.file.Path to) {
        try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(from)) {
            for (final java.nio.file.Path p : (Iterable<java.nio.file.Path>)walk::iterator) {
                final java.nio.file.Path target = to.resolve(from.relativize(p));
                if (java.nio.file.Files.isDirectory(p)) {
                    java.nio.file.Files.createDirectories(target);
                } else {
                    java.nio.file.Files.copy(p, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            }
        } catch (final java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static Set<Long> shadowRecordings(final ReplicaRig r) {
        final Set<Long> ids = new TreeSet<>();
        for (final ShadowEntry e : r.freshScan().all()) {
            ids.add(e.recordingId());
        }
        return ids;
    }

    private static Set<Long> newShadows(final ReplicaRig r, final Set<Long> old) {
        final Set<Long> ids = shadowRecordings(r);
        ids.removeAll(old);
        return ids;
    }
}

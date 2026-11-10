// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * ONE matrix, on a real three-member AE with the in-JVM replica. A re-genesis (every member wiped, a new genesis
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
            System.out.println("[regenesis] " + cell + ": " + r.fatals.get(0));
            r.stopReplica();

            r.windowGap();
            RcsDryRun.nc("cell " + cell + " GENESIS_CHECK_OFF", "cell " + cell + ": not fenced with the cluster-genesis text",
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
                com.openexchange.assets.infrastructure.persistence.TestShadowProducer.captureLog(r.replica.aeron(),
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

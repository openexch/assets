// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import io.aeron.cluster.RecordingLog;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Replica housekeeping on a three-member AE with 64 KiB log terms and 128 KiB replica
 * archive segments, {@code SHADOW_RETAIN_N=2}, {@code REPLICA_RETRIEVED_SETS_KEEP=2}, margin {@code previous-verified}:
 *
 * <ul>
 *   <li>retention keeps the two newest VERIFIED positions and purges older copies;</li>
 *   <li>the floor's margin pins the previous VERIFIED shadow: {@code shadow_log_purge_position <= prev.P_s} at every
 *       sample, and L is purged at all (non-vacuity);</li>
 *   <li>a CORRUPT shadow (a payload byte flipped while the replica is down, found by the restart's startup pass) is
 *       never purged by retention;</li>
 *   <li>retrieved member sets trimmed to K=2 by recordings, the backup RecordingLog untouched (its SNAPSHOT entries
 *       only grow), the agent's next query retrieves nothing ({@code snapshotsToRetrieve} empty),
 *       {@code shadow_retrieved_sets_purged_total} counts the trimmed sets.</li>
 * </ul>
 *
 * <p>Controls: {@code FLOOR_MARGIN_IGNORED} -> {@code log purged above the previous verified shadow at <p>};
 * {@code SET_TRIM_OFF} -> {@code retrieved sets grew to <n> > K}. The purge call-site scan of the replica package is the
 * kit's {@code ShadowPurgeCallSiteTest} and the assets {@code NoPurgeCallSiteInAssetsTest}.</p>
 */
public class ReplicaHousekeepingTest {

    private static final int PORT_BASE = 16600;
    private static final int REPLICA_SEGMENT = 128 * 1024;
    private static final int DEPOSITS_PER_ROUND = 2_000;

    @Test(timeout = 900_000)
    public void retentionFloorAndSetTrim() throws Exception {
        try (ReplicaRig r = rig(PORT_BASE, "replica-housekeeping")) {
            final Member host = story(r);
            r.startReplica(config(r, ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final List<Long> ps = new ArrayList<>();
            final long[] maxViolation = {Long.MIN_VALUE};
            for (int k = 0; k < 3; k++) {
                ps.add(round(r, maxViolation));
            }
            // a CORRUPT newest, found by the restart's startup pass
            final long corruptAt = ps.get(ps.size() - 1);
            final long corruptRec = r.scan().atPosition(corruptAt).representative().recordingId();
            final File archiveDir = r.replica.config().archiveDir();
            r.stopReplica();
            ReplicaRig.flipPayloadByte(archiveDir, corruptRec);
            r.windowGap();
            r.startReplica(config(r, ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            for (int k = 0; k < 3; k++) {
                ps.add(round(r, maxViolation));
            }
            final List<Long> expected = List.of(ps.get(ps.size() - 2), ps.get(ps.size() - 1));
            r.rig.await(() -> r.verified(ShadowKind.ALIGNED).equals(expected), 60_000,
                    () -> "retention N=2 must keep exactly " + expected + ": verified " + r.verified(ShadowKind.ALIGNED)
                            + " scan" + r.describeScan());
            // the housekeeping scan carries the deep-read verdicts (a fresh catalog's cheap scan reads COMPLETE)
            final ShadowCatalog.Scan scan = r.scan();
            final ShadowEntry corrupt = scan.entry(corruptRec);
            final boolean corruptKept = corrupt != null && corrupt.state() == ShadowEntry.State.CORRUPT
                    && r.freshScan().entry(corruptRec) != null;
            assertTrue("the CORRUPT shadow at " + corruptAt + " (rec " + corruptRec + ") was purged: "
                    + r.describeScan(), corruptKept);
            assertTrue("the log of L was never purged (vacuous floor check): purges="
                    + r.replica.housekeeping().logPurgesTotal(), r.replica.housekeeping().logPurgesTotal() > 0);
            assertTrue("log purged above the previous verified shadow (max excess " + maxViolation[0] + ")",
                    maxViolation[0] <= 0);

            // retrieved sets: trimmed to K by recordings, the RecordingLog untouched, nothing re-retrieved
            final File clusterDir = r.replica.config().clusterDir();
            final int sets = retrievedSetPositions(clusterDir).size();
            assertTrue("the backup retrieved member sets: " + sets, sets >= 4);
            assertSetsTrimmed(r);
            assertTrue("shadow_retrieved_sets_purged_total counts trimmed sets: "
                    + r.replica.housekeeping().retrievedSetsPurgedTotal(),
                    r.replica.housekeeping().retrievedSetsPurgedTotal() >= sets - 2 - 1);
            final long responses = r.replica.events().responses();
            r.rig.await(() -> r.replica.events().responses() > responses, 60_000, "no backup response after the trim");
            assertEquals("the agent's next query retrieves nothing (it reads only the latest set)", 0,
                    r.replica.events().snapshotsToRetrieve());
            assertEquals("RecordingLog SNAPSHOT entries are never edited by the trim", sets,
                    retrievedSetPositions(clusterDir).size());
            System.out.println("[housekeeping] shadows " + ps + " -> verified " + r.verified(ShadowKind.ALIGNED)
                    + "; CORRUPT rec " + corruptRec + " kept; L purged " + r.replica.housekeeping().logPurgesTotal()
                    + "x to " + r.replica.housekeeping().logPurgePosition() + "; retrieved sets in the log " + sets
                    + ", present " + r.replica.housekeeping().retrievedSets() + ", purged "
                    + r.replica.housekeeping().retrievedSetsPurgedTotal());
            System.out.println("GREEN housekeeping");
        }
    }

    @Test(timeout = 600_000)
    public void marginIgnoredPurgesAboveThePreviousShadow() throws Exception {
        try (ReplicaRig r = rig(PORT_BASE + 1000, "replica-housekeeping-nc-margin")) {
            story(r);
            r.startReplica(config(r, ReplicaRig.SLOT, "replica")
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.FLOOR_MARGIN_IGNORED)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long[] maxViolation = {Long.MIN_VALUE};
            for (int k = 0; k < 3; k++) {
                round(r, maxViolation);
            }
            RcsDryRun.nc("FLOOR_MARGIN_IGNORED", "log purged above the previous verified shadow at", () -> {
                if (maxViolation[0] > 0) {
                    throw new AssertionError("log purged above the previous verified shadow at " + lastPrev
                            + " by " + maxViolation[0] + " B (purge position "
                            + r.replica.housekeeping().logPurgePosition() + ")");
                }
            });
        }
    }

    @Test(timeout = 600_000)
    public void setTrimOffGrowsTheRetrievedSets() throws Exception {
        try (ReplicaRig r = rig(PORT_BASE + 2000, "replica-housekeeping-nc-trim")) {
            story(r);
            r.startReplica(config(r, ReplicaRig.SLOT, "replica")
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.SET_TRIM_OFF)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long[] ignored = {Long.MIN_VALUE};
            for (int k = 0; k < 4; k++) {
                round(r, ignored);
            }
            RcsDryRun.nc("SET_TRIM_OFF", "retrieved sets grew to", () -> assertSetsTrimmed(r));
        }
    }

    // ---------------------------------------------------------------------------------------------- helpers

    private static volatile long lastPrev = -1;

    private static ReplicaRig rig(final int base, final String name) {
        return new ReplicaRig(new ShadowClusterRig(base, name, 3, "64k", 0));
    }

    private static Member story(final ReplicaRig r) throws InterruptedException {
        r.bootAll();
        final Member leader = r.rig.awaitLeader(null);
        final Member host = r.rig.followerOtherThan(leader, null);
        r.story(host, 4);
        return host;
    }

    private static com.openexchange.cluster.replica.ReplicaConfig.Builder config(final ReplicaRig r, final int slot,
                                                                                final String dir) {
        return r.builder(slot, dir).retainN(2).retrievedSetsKeep(2).archiveSegmentLength(REPLICA_SEGMENT)
                .scanPeriodMs(300);
    }

    /**
     * Traffic of more than one replica segment, a member snapshot, the replica's VERIFIED ALIGNED shadow there and the
     * backup's retrieval of that member set; samples {@code purgePosition - prev.P_s} meanwhile.
     */
    private static long round(final ReplicaRig r, final long[] maxViolation) {
        r.tool.deposits(DEPOSITS_PER_ROUND);
        final long p = r.rig.snapshotAll();
        r.awaitVerified(p, ShadowKind.ALIGNED, 60_000);
        final File clusterDir = r.replica.config().clusterDir();
        r.rig.await(() -> {
            sampleFloor(r, maxViolation);
            return retrievedSetPositions(clusterDir).contains(p);
        }, 60_000, () -> "the backup never retrieved the member set at " + p + ": " + retrievedSetPositions(clusterDir));
        // a few housekeeping passes after the set arrived
        final long until = System.currentTimeMillis() + 2_000;
        r.rig.await(() -> {
            sampleFloor(r, maxViolation);
            return System.currentTimeMillis() > until;
        }, 5_000, "pump");
        System.out.println("[housekeeping] round: P_cm=" + p + " verified " + r.verified(ShadowKind.ALIGNED) + " L purge "
                + r.replica.housekeeping().logPurgePosition() + " sets " + retrievedSetPositions(clusterDir)
                + " present " + r.replica.housekeeping().retrievedSets());
        return p;
    }

    /** {@code shadow_log_purge_position - prev.P_s} where prev is the second-newest VERIFIED shadow. */
    private static void sampleFloor(final ReplicaRig r, final long[] maxViolation) {
        final List<Long> v = r.verified(null);
        final long purge = r.replica.housekeeping().logPurgePosition();
        if (v.size() >= 2 && purge > 0) {
            final long prev = v.get(v.size() - 2);
            if (purge - prev > maxViolation[0]) {
                maxViolation[0] = purge - prev;
                lastPrev = prev;
            }
        }
    }

    private static Set<Long> retrievedSetPositions(final File clusterDir) {
        final Set<Long> out = new TreeSet<>();
        if (!new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            return out;
        }
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid) {
                    out.add(e.logPosition);
                }
            }
        }
        return out;
    }

    private static void assertSetsTrimmed(final ReplicaRig r) {
        r.rig.pumpMs(1_500);
        final int present = r.replica.housekeeping().retrievedSets();
        if (present > 2) {
            throw new AssertionError("retrieved sets grew to " + present + " > K=2 (in the RecordingLog "
                    + retrievedSetPositions(r.replica.config().clusterDir()) + ")");
        }
    }
}

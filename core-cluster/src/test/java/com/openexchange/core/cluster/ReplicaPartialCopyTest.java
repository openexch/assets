// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import org.agrona.IoUtil;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * On a real three-member core with the in-JVM replica: a partially retrieved member snapshot copy
 * (what a backup reset during SNAPSHOT_RETRIEVE leaves: stopped, BEGIN present, END or payload missing) is never THE
 * copy, a copy whose decode fails is never cached, and while the newest named snapshot has no usable copy the
 * stall clock runs whatever its position relative to the restore point.
 *
 * <p>Rig mechanism ({@link ReplicaRig.Faults#wedgeSnapshotRetrieval}, {@link ReplicaRig#partialCopy}): the backup is
 * reset at every SNAPSHOT_RETRIEVE by a 1 ns replication progress timeout on its live context (the agent reads it when
 * each retrieval starts), and the partial copy is a message-exact prefix of the member's service snapshot recorded on
 * the snapshot stream of the replica archive and stopped. The product settings are untouched.</p>
 *
 * <pre>
 * (a) same history, restored from a local shadow at p1; the next member snapshot p2 is named while the backup is
 *     wedged, a BEGIN-only copy of p2 lands, then a healthy cycle: the replica becomes ready (the genesis and
 *     alignment compares at p2 pass)
 * (b) re-genesis with the new history SHORTER than p1, cluster/ kept, the backup wedged after naming the new cluster's
 *     first snapshot q (below p1): fence with the stall text within REPLICA_VERIFY_WAIT_MS of the naming
 * (c) as (b) with one reset mid-retrieval (a copy of q through END with a cut payload) and then a healthy cycle: fence
 *     with the cluster-genesis text once the complete copy lands
 * </pre>
 * <p>Each writes no shadow before it resolves.</p>
 */
public class ReplicaPartialCopyTest {

    private static final int PORT_BASE_A = 41400;
    private static final int PORT_BASE_B = 42000;
    private static final int PORT_BASE_C = 42600;
    private static final long REGENESIS = 0x2EED5EEDL;
    private static final long OLD_HISTORY_BYTES = 4L * 1024 * 1024;
    /** (a) and (c): above the time the test keeps the backup wedged plus one backup cycle (verify-wait floor 36 000). */
    private static final long VERIFY_WAIT_LONG_MS = 90_000;
    /** (b): the stall bound under test. */
    private static final long VERIFY_WAIT_B_MS = 60_000;
    /** The time a snapshot retrieval and its decode may take on top of one backup cycle (a test allowance). */
    private static final long RETRIEVAL_ALLOWANCE_MS = 15_000;

    static String clusterGenesisText(final long clusterGenesis, final long localGenesis) {
        return "FATAL replica: cluster genesis " + clusterGenesis + " != local shadows " + localGenesis
                + "; wipe BASE_DIR";
    }

    static String stallText(final long applied) {
        return "FATAL replica: unverified continuation at " + applied + " (history mismatch or a stalled backup);"
                + " wipe BASE_DIR";
    }

    /** One backup cycle from the replica's own settings: query interval plus cool-down. */
    static long cycleMs(final ReplicaRig r) {
        final ReplicaConfig c = r.replica.config();
        return c.backupQueryIntervalMs() + c.backupCooldownMs();
    }

    @Test(timeout = 1_200_000)
    public void aSameHistoryResetMidRetrievalThenAHealthyCycleBecomesReady() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_A, "replica-partial-copy-a")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(120_000);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            final Set<Long> oldShadows = shadowRecordings(r);
            r.stopReplica();
            r.windowGap();

            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(VERIFY_WAIT_LONG_MS).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            assertEquals(p1, r.replica.follower().restoredPosition());
            r.awaitCaughtUp(120_000);
            r.faults.wedgeSnapshotRetrieval(true);
            final long p2 = r.rig.snapshotAll();
            r.rig.await(() -> r.replica.events().newestReportedServiceSnapshot() >= p2 || !r.fatals.isEmpty(),
                    3 * cycleMs(r), () -> "(a): the backup never named p2=" + p2 + ": " + r.probe().detail());
            final long resets = r.replica.events().resets();
            r.rig.await(() -> r.replica.events().resets() > resets || !r.fatals.isEmpty(), 30_000,
                    "(a): the wedged backup must reset in SNAPSHOT_RETRIEVE");
            r.rig.pumpMs(1_000);
            final long partial = r.partialCopy(leader, p2, false, 0);
            pumpWatching(r, 3_000, oldShadows, "(a) wedged");
            assertTrue("(a): fenced while wedged: " + r.fatals, r.fatals.isEmpty());
            System.out.println("[partial-copy] (a) p1=" + p1 + " p2=" + p2 + " partial rec=" + partial + ", wedged: "
                    + r.probe().detail());

            r.faults.wedgeSnapshotRetrieval(false);
            final long healthyAt = System.currentTimeMillis();
            final long bound = cycleMs(r) + RETRIEVAL_ALLOWANCE_MS;
            final long end = healthyAt + bound;
            boolean ready = false;
            while (System.currentTimeMillis() < end && r.fatals.isEmpty()) {
                if (r.probe().ready()) {
                    ready = true;
                    break;
                }
                assertNothingWrittenWhileUntrusted(r, oldShadows, "(a)");
                r.rig.pumpMs(250);
            }
            if (!ready) {
                throw new AssertionError("(a): not ready within " + bound + " ms (query interval + cool-down + "
                        + RETRIEVAL_ALLOWANCE_MS + ") of the healthy cycle: fatals " + r.fatals + " detail "
                        + r.probe().detail());
            }
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            assertTrue("verified at p2: " + r.replica.follower().trust().verifiedAt(),
                    r.replica.follower().trust().verifiedAt() >= p2);
            System.out.println("GREEN (a) ready " + (System.currentTimeMillis() - healthyAt) + " ms after the"
                    + " healthy cycle began (bound " + bound + "), verified at "
                    + r.replica.follower().trust().verifiedAt());
        }
    }

    @Test(timeout = 1_500_000)
    public void bReGenesisShorterBackupWedgedAfterNamingFencesWithTheStallText() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_B, "replica-partial-copy-b")) {
            final Regenesis g = regenesisShorter(r, "(b)", VERIFY_WAIT_B_MS);
            final long q = r.rig.snapshotAll();
            assertTrue("(b) precondition: q=" + q + " below p1=" + g.p1, q < g.p1);
            r.rig.await(() -> r.replica.events().newestReportedServiceSnapshot() >= q || !r.fatals.isEmpty(),
                    3 * cycleMs(r), () -> "(b): the backup never named q=" + q + ": " + r.probe().detail());
            final long namedAt = System.currentTimeMillis();
            final long bound = VERIFY_WAIT_B_MS + cycleMs(r);
            final long end = namedAt + bound;
            while (System.currentTimeMillis() < end && r.fatals.isEmpty()) {
                assertTrue("(b): ready while wedged", !r.probe().ready());
                assertTrue("(b): a new-history frame reached a shadow: " + r.describeScan(),
                        newShadows(r, g.oldShadows).isEmpty());
                r.rig.pumpMs(500);
            }
            final long applied = r.replica.follower().appliedPosition();
            if (!r.fatals.contains(stallText(applied))) {
                throw new AssertionError("(b): not fenced with the stall text '" + stallText(applied)
                        + "' within " + bound + " ms (REPLICA_VERIFY_WAIT_MS + one backup cycle) of the naming of q="
                        + q + ": fatals " + r.fatals + " detail " + r.probe().detail());
            }
            assertTrue("nothing written", newShadows(r, g.oldShadows).isEmpty());
            assertTrue("/ready never 200", !r.probe().ready());
            System.out.println("GREEN (b) fenced " + (System.currentTimeMillis() - namedAt) + " ms after the"
                    + " naming of q=" + q + " (p1=" + g.p1 + ", bound " + bound + "): " + r.fatals.get(0));
        }
    }

    @Test(timeout = 1_500_000)
    public void cReGenesisShorterResetMidRetrievalThenACompleteCopyFencesWithTheGenesisText() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_C, "replica-partial-copy-c")) {
            final Regenesis g = regenesisShorter(r, "(c)", VERIFY_WAIT_LONG_MS);
            final long q = r.rig.snapshotAll();
            assertTrue("(c) precondition: q=" + q + " below p1=" + g.p1, q < g.p1);
            r.rig.await(() -> r.replica.events().newestReportedServiceSnapshot() >= q || !r.fatals.isEmpty(),
                    3 * cycleMs(r), () -> "(c): the backup never named q=" + q + ": " + r.probe().detail());
            r.rig.pumpMs(1_000);
            final Member source = r.rig.awaitLeader(null);
            final long partial = r.partialCopy(source, q, true, 64);
            pumpWatching(r, 3_000, g.oldShadows, "(c) wedged");
            assertTrue("(c): fenced before the complete copy: " + r.fatals, r.fatals.isEmpty());
            System.out.println("[partial-copy] (c) q=" + q + " partial rec=" + partial + ", wedged: " + r.probe().detail());

            r.faults.wedgeSnapshotRetrieval(false);
            final long healthyAt = System.currentTimeMillis();
            final long bound = cycleMs(r) + RETRIEVAL_ALLOWANCE_MS;
            final long end = healthyAt + bound;
            final String genesisText = clusterGenesisText(REGENESIS, g.g1);
            while (System.currentTimeMillis() < end && r.fatals.isEmpty()) {
                assertTrue("(c): ready", !r.probe().ready());
                assertTrue("(c): a new-history frame reached a shadow: " + r.describeScan(),
                        newShadows(r, g.oldShadows).isEmpty());
                r.rig.pumpMs(250);
            }
            if (!r.fatals.contains(genesisText)) {
                throw new AssertionError("(c): not fenced with the cluster-genesis text '" + genesisText + "' within " + bound
                        + " ms (query interval + cool-down + " + RETRIEVAL_ALLOWANCE_MS + ") of the healthy cycle:"
                        + " fatals " + r.fatals + " detail " + r.probe().detail());
            }
            assertTrue("nothing written", newShadows(r, g.oldShadows).isEmpty());
            System.out.println("GREEN (c) fenced " + (System.currentTimeMillis() - healthyAt) + " ms after the"
                    + " healthy cycle began (bound " + bound + "): " + r.fatals.get(0));
        }
    }

    private record Regenesis(long p1, long g1, Set<Long> oldShadows) {
    }

    /**
     * Old history past {@link #OLD_HISTORY_BYTES}, VERIFIED ALIGNED at p1, replica stopped; every member wiped and a
     * new genesis ADOPTed (cluster/ of the replica KEPT); the replica restarted (restored from a local shadow at p1)
     * with its backup WEDGED and left two query cycles without a post-ADOPT member snapshot (no fence, never ready).
     */
    private static Regenesis regenesisShorter(final ReplicaRig r, final String row, final long verifyWaitMs)
            throws Exception {
        r.bootAll();
        final Member leader = r.rig.awaitLeader(null);
        final Member host = r.rig.followerOtherThan(leader, null);
        r.story(host, 4);
        r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
        r.awaitBootstrapped(ReplicaRig.WAIT_MS);
        final long t0 = System.currentTimeMillis();
        while (r.commit() < OLD_HISTORY_BYTES) {
            r.traffic(60, 12);
            if (System.currentTimeMillis() - t0 > 600_000) {
                throw new AssertionError("row " + row + " setup: the old history never reached " + OLD_HISTORY_BYTES);
            }
        }
        r.awaitCaughtUp(120_000);
        final long p1 = r.rig.snapshotAll();
        r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
        final long g1 = r.replica.follower().knownGenesis();
        final Set<Long> oldShadows = shadowRecordings(r);
        r.stopReplica();

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
        final Member leader2 = r.rig.awaitLeader(null);
        r.story(r.rig.followerOtherThan(leader2, null), 4);
        r.windowGap();

        r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(verifyWaitMs).build());
        r.faults.wedgeSnapshotRetrieval(true);
        r.rig.await(() -> r.replica.follower().bootstrapDone() || !r.fatals.isEmpty(), ReplicaRig.WAIT_MS,
                () -> "row " + row + ": no bootstrap decision: " + r.probe().detail());
        assertTrue("row " + row + " bootstrap: " + r.fatals, r.fatals.isEmpty());
        assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
        assertEquals(p1, r.replica.follower().restoredPosition());
        final long idleUntil = System.currentTimeMillis() + 2 * r.replica.config().backupQueryIntervalMs();
        while (System.currentTimeMillis() < idleUntil) {
            assertTrue("row " + row + ": fenced before any post-ADOPT member snapshot: " + r.fatals,
                    r.fatals.isEmpty());
            assertTrue("row " + row + ": ready before any post-ADOPT member snapshot", !r.probe().ready());
            r.rig.pumpMs(500);
        }
        System.out.println("[partial-copy] " + row + ": re-genesised (" + REGENESIS + "), old genesis " + g1 + " p1=" + p1
                + ", replica restored at p1 with its backup wedged: " + r.probe().detail());
        return new Regenesis(p1, g1, oldShadows);
    }

    private static void pumpWatching(final ReplicaRig r, final long ms, final Set<Long> oldShadows,
                                     final String row) {
        final long end = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < end) {
            assertNothingWrittenWhileUntrusted(r, oldShadows, row);
            r.rig.pumpMs(250);
        }
    }

    private static void assertNothingWrittenWhileUntrusted(final ReplicaRig r, final Set<Long> oldShadows,
                                                           final String row) {
        final com.openexchange.cluster.replica.AlignmentTrust t = r.replica.follower().trust();
        if (t != null && !t.trusted() && !newShadows(r, oldShadows).isEmpty()) {
            throw new AssertionError("row " + row + ": a shadow was written while UNVERIFIED: " + r.describeScan());
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

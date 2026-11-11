// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import com.openexchange.cluster.testkit.RecordedLog;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * The replica's bootstrap decision table on a real three-member core.
 *
 * <ul>
 *   <li>Genesis: a fresh dir against a log complete from 0 starts at GENESIS; the first ALIGNED after ADOPT equals the
 *       members'.</li>
 *   <li>Local shadow: two VERIFIED shadows, stop, restart on the same dirs: LOCAL_SHADOW at the newest VERIFIED
 *       position, the tail replays, the next ALIGNED equals the members'.</li>
 *   <li>CORRUPT newest (a payload byte flipped on disk): the previous shadow is restored, {@code shadow_catalog_corrupt
 *       == 1}.</li>
 *   <li>Crashed write: a trailer-less recording of this replica's producer from before the restart (the shape a
 *       kill -9 in the middle of a write leaves) is purged by the retention owner after the restart.</li>
 *   <li>{@code cluster/} wiped, {@code archive/} kept: the backup creates a new L, the bootstrap waits for its
 *       descriptor, restores the newest local shadow and follows; the next ALIGNED equals the members'.</li>
 *   <li>After the CORRUPT restart a VERIFIED own copy is written again at the CORRUPT position. After the
 *       {@code cluster/} wipe the old L and the old retrieved sets are purged by housekeeping (orphans == 0).</li>
 *   <li>Cluster genesis: a re-genesised rig plus the old dir is refused before any apply ({@code GENESIS_CHECK_OFF}
 *       control).</li>
 *   <li>Pruned log: a fresh dir against members whose logs were purged above 0: refused with the runbook text,
 *       {@code /ready} never 200. Two geneses in the archive: refused with the two-geneses text.</li>
 * </ul>
 *
 * <p>Controls: {@code GENESIS_PRECONDITION_OFF} on the purged log -> {@code genesis on a pruned log: start=<s>};
 * {@code MATCHES_OFF} with a forged identity (a header claiming the money journal armed) ->
 * {@code IDENTITY_CROSS_CHECK_FAILED expected}; {@code TWO_RESTORES_ALLOWED} with an undecodable newest shadow ->
 * {@code two restore attempts in one process}.</p>
 */
public class ReplicaBootstrapTest {

    private static final int PORT_BASE_RESTART = 29800;
    private static final int PORT_BASE_REFUSALS = 30400;
    private static final int PORT_BASE_PRUNED = 31000;
    private static final int PORT_BASE_PULL = 49600;
    private static final int SEGMENT = 64 * 1024;

    @Test(timeout = 900_000)
    public void genesisThenRestartFromTheNewestUsableShadow() throws Exception {
        final List<String> deferred = new java.util.ArrayList<>();
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_RESTART, "replica-bootstrap-restart")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);

            // genesis
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.GENESIS, r.replica.readiness().restorePath());
            r.awaitCaughtUp(60_000);
            final long p1 = aligned(r, host);
            r.traffic(30, 6);
            final long p2 = aligned(r, host);
            System.out.println("[bootstrap] GENESIS; ALIGNED shadows at " + p1 + " and " + p2 + " equal the members'");
            final long stopBefore = r.replica.follower().recordedPosition();
            r.stopReplica();
            r.traffic(20, 4);
            r.windowGap();

            // restore from a local shadow on the same dirs
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            assertEquals("the restart restores the newest VERIFIED shadow", p2, r.replica.follower().restoredPosition());
            r.awaitCaughtUp(60_000);
            assertTrue("the tail [P_s, L.stop) replayed: applied " + r.replica.follower().appliedPosition()
                    + " < old stop " + stopBefore, r.replica.follower().appliedPosition() >= stopBefore);
            final long p3 = aligned(r, host);
            System.out.println("[bootstrap] LOCAL_SHADOW at " + p2 + ", tail replayed past " + stopBefore
                    + ", next ALIGNED at " + p3 + " equals the members'");

            // What a kill -9 in the middle of this replica's write leaves behind
            final RecordedLog log = r.rig.captureCommitted(host);
            final TestShadowProducer.Written crashed = r.producerOnReplica().produceCrashed(
                    new Who(ProducerKind.REPLICA, ReplicaRig.SLOT), log, p3, 10_000);
            assertFalse("the crashed write must not complete: " + crashed, crashed.ok());
            r.rig.await(() -> orphans(r) == 1, 30_000, () -> "the crashed write is not a trailer-less R"
                    + ReplicaRig.SLOT + " recording: " + crashed + " scan " + r.describeScan());
            final long p3rec = r.scan().atPosition(p3).representative().recordingId();
            final File archiveDir = r.replica.config().archiveDir();
            r.stopReplica();

            // CORRUPT newest: the previous VERIFIED shadow is restored
            ReplicaRig.flipPayloadByte(archiveDir, p3rec);
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            assertEquals("a CORRUPT newest falls back to the previous shadow", p2,
                    r.replica.follower().restoredPosition());
            assertEquals("shadow_catalog_corrupt", 1, r.metric("shadow_catalog_corrupt"));
            // The NOT_MONOTONE floor counts usable own copies only, so the ALIGNED at the CORRUPT
            // position P_c is written again (a VERIFIED own copy appears at P_c next to the CORRUPT one)
            r.awaitCaughtUp(60_000);
            final long pc = p3;
            try {
                r.rig.await(() -> ownVerifiedCopyAt(r, pc, p3rec) || !r.fatals.isEmpty(), 60_000,
                        () -> "no VERIFIED own copy at P_c=" + pc + " after the CORRUPT restart (the"
                                + " NOT_MONOTONE floor counted the CORRUPT copy): " + r.describeScan());
                assertTrue("no fatal while re-writing P_c: " + r.fatals, r.fatals.isEmpty());
                System.out.println("[bootstrap] a VERIFIED own copy re-written at the CORRUPT position " + pc);
            } catch (final AssertionError rewrite) {
                // reported at the end, so the orphan row below runs (and shows its own verdict) in the same rig
                System.out.println("[bootstrap] RED " + rewrite.getMessage());
                deferred.add(rewrite.getMessage());
            }
            r.rig.await(() -> orphans(r) == 0, 30_000, () -> "the trailer-less R" + ReplicaRig.SLOT
                    + " recording " + crashed.recordingId() + " from before the restart was not purged: "
                    + r.describeScan());
            r.awaitCaughtUp(60_000);
            final long p4 = aligned(r, host);
            System.out.println("[bootstrap] CORRUPT newest at " + p3 + " skipped, restored " + p2 + "; orphan rec="
                    + crashed.recordingId() + " purged; next ALIGNED at " + p4 + " equals the members'");
            r.stopReplica();

            // cluster/ wiped, archive/ kept: the old L and the old retrieved sets are orphans
            final java.util.Set<Long> beforeWipe = r.recordingLogIds("replica");
            assertFalse("the old RecordingLog references L and the retrieved sets", beforeWipe.isEmpty());
            IoUtil.delete(new File(r.replicaBase("replica"), "cluster"), false);
            r.traffic(20, 4);
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            assertEquals("the newest usable shadow is restored after the wipe", p4,
                    r.replica.follower().restoredPosition());
            assertEquals("the new L starts where the supplier's log starts", 0,
                    r.replica.follower().logStartPositionAtBootstrap());
            r.awaitCaughtUp(60_000);
            final long p5 = aligned(r, host);
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            final java.util.Set<Long> referenced = r.recordingLogIds("replica");
            final java.util.Set<Long> orphans = new java.util.TreeSet<>(beforeWipe);
            orphans.removeAll(referenced);
            orphans.remove(r.replica.follower().logRecordingId());
            assertFalse("the wipe left recordings the new RecordingLog does not reference: " + beforeWipe + " vs "
                    + referenced, orphans.isEmpty());
            try {
                r.rig.await(() -> orphans.stream().noneMatch(r::inReplicaArchive)
                        && r.metric("shadow_replica_orphan_recordings") == 0, 30_000,
                        () -> "orphan recordings " + orphans + " still in the replica archive after"
                                + " housekeeping (present " + orphans.stream().filter(r::inReplicaArchive).toList()
                                + ", shadow_replica_orphan_recordings="
                                + r.metric("shadow_replica_orphan_recordings") + ")");
            } catch (final AssertionError orphanFailure) {
                deferred.add(orphanFailure.getMessage());
                throw new AssertionError(String.join(" | ", deferred), orphanFailure);
            }
            if (!deferred.isEmpty()) {
                throw new AssertionError(String.join(" | ", deferred));
            }
            assertTrue("the new L survives", r.inReplicaArchive(r.replica.follower().logRecordingId()));
            for (final long id : referenced) {
                assertTrue("the orphan purge never purges a referenced recording: " + id, r.inReplicaArchive(id));
            }
            System.out.println("[bootstrap] orphans " + orphans + " purged after the cluster/ wipe; referenced "
                    + referenced + " kept");
            System.out.println("[bootstrap] cluster/ wiped: new L=" + r.replica.follower().logRecordingId()
                    + ", restored " + p4 + ", next ALIGNED at " + p5 + " equals the members'");
            System.out.println("GREEN restart p1=" + p1 + " p2=" + p2 + " p3=" + p3 + " p4=" + p4 + " p5=" + p5);
        }
    }

    @Test(timeout = 900_000)
    public void forgedCandidatesAreRefusedLoudly() throws Exception {
        System.setProperty(ShadowSelfCheck.UNCHECKED_PROPERTY, "true");
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_REFUSALS, "replica-bootstrap-refusals")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            final RecordedLog log = r.rig.captureCommitted(host);
            final long[] pos = log.positions();
            final long last = pos[pos.length - 1];
            final long lower = pos[pos.length - 10];

            // matches: a header claiming the money journal armed at 77; restore succeeds, the cross-check refuses
            startFresh(r, "replica-id");
            assertTrue(r.producerOnReplica().produceJournalClaim(new Who(ProducerKind.REPLICA, 11), log, last, 77)
                    .ok());
            restart(r, "replica-id", ReplicaRig.off());
            assertIdentityRefused(r);
            System.out.println("[bootstrap] forged identity refused: " + r.fatals);
            r.stopReplica();
            restart(r, "replica-id", ReplicaRig.off(ReplicaNegativeControl.MATCHES_OFF));
            RestoreDrillDryRun.nc("MATCHES_OFF", "IDENTITY_CROSS_CHECK_FAILED expected", () -> assertIdentityRefused(r));
            r.stopReplica();

            // one restore per process: an undecodable newest shadow halts after ONE attempt
            startFresh(r, "replica-two");
            assertTrue(r.producerOnReplica().produce(new Who(ProducerKind.REPLICA, 12), log, lower).ok());
            assertTrue(r.producerOnReplica().produceUndecodable(new Who(ProducerKind.REPLICA, 13), log, last).ok());
            restart(r, "replica-two", ReplicaRig.off());
            r.rig.await(() -> !r.fatals.isEmpty(), 60_000, "an undecodable newest shadow did not halt the replica");
            assertTrue("the halt names the failed restore: " + r.fatals,
                    r.fatals.get(0).contains("restore of the shadow at " + last));
            assertOneRestore(r);
            System.out.println("[bootstrap] undecodable newest: " + r.fatals.get(0));
            r.stopReplica();
            restart(r, "replica-two", ReplicaRig.off(ReplicaNegativeControl.TWO_RESTORES_ALLOWED));
            RestoreDrillDryRun.nc("TWO_RESTORES_ALLOWED", "two restore attempts in one process", () -> assertOneRestore(r));
            r.stopReplica();

            // two geneses among usable copies
            startFresh(r, "replica-b1");
            assertTrue(r.producerOnReplica().produce(new Who(ProducerKind.REPLICA, 14), log, lower).ok());
            assertTrue(r.producerOnReplica().produceForeignGenesis(new Who(ProducerKind.FOLLOWER, 1), log, last,
                    0xBAD5EEDL).ok());
            restart(r, "replica-b1", ReplicaRig.off());
            r.rig.await(() -> !r.fatals.isEmpty(), 60_000, "two geneses did not refuse the bootstrap");
            assertTrue("two-geneses text: " + r.fatals, r.fatals.get(0).contains("replica archive holds shadows of 2 geneses")
                    && r.fatals.get(0).contains("wipe BASE_DIR or pin the genesis"));
            assertEquals(ReplicaReadiness.RestorePath.REFUSED, r.replica.readiness().restorePath());
            System.out.println("[bootstrap] two geneses: " + r.fatals.get(0));
            System.out.println("GREEN refusals");
        } finally {
            System.clearProperty(ShadowSelfCheck.UNCHECKED_PROPERTY);
        }
    }

    @Test(timeout = 600_000)
    public void aPrunedLogWithoutALocalShadowIsRefused() throws Exception {
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE_PRUNED, "replica-bootstrap-pruned", 3, "64k",
                SEGMENT))) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.tool.deposits(900);
            final long pCm = r.rig.snapshotAll();
            r.tool.deposits(60);
            final long boundary = pCm / SEGMENT * SEGMENT;
            assertTrue("the log must span more than one segment before P_cm=" + pCm, boundary > 0);
            for (final Member m : r.rig.live()) {
                assertEquals("member " + m.id + " log start after the purge", boundary, r.rig.purgeLogTo(m, boundary));
            }
            System.out.println("[bootstrap] pruned log: every member's log purged to " + boundary + " below P_cm=" + pCm);

            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            final int[] readySamples = {0};
            r.rig.await(() -> {
                if (r.probe().ready()) {
                    readySamples[0]++;
                }
                return !r.fatals.isEmpty();
            }, 120_000, () -> "the pruned-log refusal did not happen: " + r.probe().detail());
            assertTrue("pruned-log runbook text: " + r.fatals, r.fatals.get(0).startsWith(
                    "FATAL replica: no local shadow and the cluster log starts at " + boundary + " > 0."));
            assertTrue("the refusal names the disabled pull: " + r.fatals, r.fatals.get(0).endsWith(" pull: disabled"));
            assertEquals("/ready was 200 during a refused bootstrap", 0, readySamples[0]);
            r.rig.pumpMs(2_000);
            assertFalse("/ready after the refusal", r.probe().ready());
            assertEquals("503", r.httpReady()[0]);
            assertEquals(ReplicaReadiness.RestorePath.REFUSED, r.replica.readiness().restorePath());
            System.out.println("[bootstrap] pruned log: " + r.fatals.get(0));
            r.stopReplica();
            r.windowGap();

            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-nc")
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.GENESIS_PRECONDITION_OFF)).build());
            r.rig.await(() -> r.replica.follower().bootstrapDone() || !r.fatals.isEmpty(), 120_000,
                    () -> "the control replica never decided: " + r.probe().detail());
            RestoreDrillDryRun.nc("GENESIS_PRECONDITION_OFF", "genesis on a pruned log: start=",
                    () -> assertNoGenesisOnPrunedLog(r));
            System.out.println("GREEN pruned log boundary=" + boundary);
        }
    }

    /**
     * Bootstrap case 3 with pull sources (the three members' archives): on a log pruned above 0 and no local shadow,
     * a corrupted copy as the only candidate is refused for its content and the bootstrap refuses with the pull named
     * ({@code pull: tried 3 sources, 1 candidates, refused 1, last: CORRUPT}); with a genuine ALIGNED at P_cm on
     * another member the replica pulls it, verifies it locally, restores from it ({@code restore_path} PULLED_SHADOW,
     * code 3), follows, and its next ALIGNED equals the members'. Before that, candidates of two geneses across the
     * sources refuse with the two-geneses text.
     */
    @Test(timeout = 900_000)
    public void aPrunedLogIsBootstrappedFromAPulledShadow() throws Exception {
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE_PULL, "replica-bootstrap-pull", 3, "64k",
                SEGMENT))) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.tool.deposits(900);
            final long pCm = r.rig.snapshotAll();
            r.tool.deposits(60);
            final long boundary = pCm / SEGMENT * SEGMENT;
            assertTrue("the log must span more than one segment before P_cm=" + pCm, boundary > 0);
            final List<Member> ms = r.rig.members;
            // the members' committed logs from 0, read before the purge (the shadows are built from them)
            final RecordedLog log0 = r.rig.captureCommitted(ms.get(0));
            final RecordedLog log1 = r.rig.captureCommitted(ms.get(1));
            final RecordedLog log2 = r.rig.captureCommitted(ms.get(2));
            for (final Member m : r.rig.live()) {
                assertEquals("member " + m.id + " log start after the purge", boundary, r.rig.purgeLogTo(m, boundary));
            }
            final java.util.Map<String, String> env = java.util.Map.of(
                    com.openexchange.cluster.shadow.ShadowPullSettings.SOURCES,
                    r.rig.sources(ms.get(0), ms.get(1), ms.get(2)),
                    com.openexchange.cluster.shadow.ShadowPullSettings.PERIOD_MS, "1000",
                    com.openexchange.cluster.shadow.ShadowPullSettings.STARTUP_WAIT_MS, "8000");
            final com.openexchange.cluster.shadow.ShadowPullSettings pull =
                    com.openexchange.cluster.shadow.ShadowPullSettings.parse(env::get, null);

            // (1) the only candidate is a corrupted copy on member 1
            final TestShadowProducer p1 = r.rig.producer(ms.get(1));
            final TestShadowProducer.Written bad = p1.produce(new Who(ProducerKind.FOLLOWER, 501), log1, pCm);
            assertTrue(bad.toString(), bad.ok());
            p1.flipPayloadByte(bad);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica-corrupt").pullSettings(pull).build());
            r.rig.await(() -> !r.fatals.isEmpty(), 120_000, () -> "the corrupt-only pull never refused: "
                    + r.probe().detail());
            if (r.replica.readiness().restorePath() == ReplicaReadiness.RestorePath.PULLED_SHADOW) {
                throw new AssertionError("a corrupt shadow restored the replica");
            }
            assertTrue("corrupt-only refusal: " + r.fatals, r.fatals.get(0).endsWith(
                    " pull: tried 3 sources, 1 candidates, refused 1, last: CORRUPT"));
            System.out.println("[bootstrap] pull corrupt only: " + r.fatals.get(0));
            r.stopReplica();
            r.windowGap();

            // (2) another genesis among the candidates: the puller's two-geneses check refuses
            final TestShadowProducer.Written foreign = r.rig.producer(ms.get(0)).produceForeignGenesis(
                    new Who(ProducerKind.FOLLOWER, 503), log0, pCm, 0x7777L);
            assertTrue(foreign.toString(), foreign.ok());
            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-geneses").pullSettings(pull).build());
            r.rig.await(() -> !r.fatals.isEmpty(), 120_000, () -> "the two-geneses pull never refused: "
                    + r.probe().detail());
            final String genesesText = r.fatals.get(0);
            assertTrue("two geneses across the sources: " + r.fatals, r.fatals.get(0).startsWith(
                    "FATAL replica: the pull sources list shadows of 2 geneses"));
            System.out.println("[bootstrap] pull two geneses: " + genesesText);
            r.stopReplica();
            r.windowGap();
            // the foreign copy goes before the genuine row (the members prune to their next snapshot after it)
            assertTrue("member 0's foreign copy purged", r.rig.purgeShadows(ms.get(0)) >= 1);

            // (3) a genuine ALIGNED at P_cm on member 2 (the corrupt copy is still listed): pulled, verified, restored, followed
            final TestShadowProducer.Written good = r.rig.producer(ms.get(2)).produce(
                    new Who(ProducerKind.FOLLOWER, 502), log2, pCm);
            assertTrue(good.toString(), good.ok());
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica-pulled").pullSettings(pull).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.PULLED_SHADOW, r.replica.readiness().restorePath());
            assertEquals("restore_path code", 3, r.metric("shadow_replica_restore_path"));
            assertEquals("restored at P_cm", pCm, r.replica.follower().restoredPosition());
            r.awaitCaughtUp(60_000);
            final long next = aligned(r, host);
            System.out.println("[bootstrap] PULLED_SHADOW at " + pCm + " (log start " + boundary
                    + "); the next ALIGNED at " + next + " equals the members'");

            System.out.println("GREEN bootstrap pull: corrupt refused, two geneses refused, PULLED_SHADOW at " + pCm);
        }
    }

    /**
     * A re-genesised rig (every member wiped, a new genesis ADOPTed, a member snapshot taken) plus the old
     * replica dir ({@code archive/} with VERIFIED shadows of the old genesis kept; {@code cluster/} wiped so the backup
     * builds a fresh L from 0 of the new history, the cluster/ wipe shape) is refused before any frame is applied:
     * {@code FATAL replica: cluster genesis <g2> != local shadows <g1>; wipe BASE_DIR}. Control
     * {@code GENESIS_CHECK_OFF} -> {@code re-genesis not refused}.
     */
    @Test(timeout = 900_000)
    public void aReGenesisedClusterWithTheOldDirIsRefused() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_REGENESIS, "replica-bootstrap-regenesis")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long p1 = aligned(r, host);
            r.stopReplica();
            System.out.println("[bootstrap] cluster genesis: old genesis " + ShadowClusterRig.GENESIS + " VERIFIED shadow at " + p1);

            // re-genesis: every member wiped and booted fresh, a different genesis ADOPTed, one member snapshot
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
            final Member host2 = r.rig.followerOtherThan(leader2, null);
            r.story(host2, 4);
            final long p2 = r.rig.snapshotAll();
            System.out.println("[bootstrap] cluster genesis: re-genesised rig, genesis " + REGENESIS + ", member snapshot at " + p2);

            IoUtil.delete(new File(r.replicaBase("replica"), "cluster"), false);
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            assertRegenesisRefused(r);
            System.out.println("[bootstrap] cluster genesis: " + r.fatals.get(0));
            r.stopReplica();

            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica")
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.GENESIS_CHECK_OFF)).build());
            RestoreDrillDryRun.nc("GENESIS_CHECK_OFF", "re-genesis not refused", () -> assertRegenesisRefused(r));
            System.out.println("GREEN regenesis p1=" + p1 + " p2=" + p2);
        }
    }

    private static final int PORT_BASE_REGENESIS = 34600;
    private static final long REGENESIS = 0x2EED5EEDL;

    /** The cluster genesis refusal, before any frame of the new history is applied. */
    private static void assertRegenesisRefused(final ReplicaRig r) {
        final String text = "FATAL replica: cluster genesis " + REGENESIS + " != local shadows "
                + ShadowClusterRig.GENESIS + "; wipe BASE_DIR";
        try {
            r.rig.await(() -> r.fatals.stream().anyMatch(f -> f.contains(text)), 90_000, "regenesis");
        } catch (final AssertionError timeout) {
            throw new AssertionError("re-genesis not refused within 90 s: restore=" + r.replica.readiness()
                    .restorePath() + " restored=" + r.replica.follower().restoredPosition() + " applied="
                    + r.replica.follower().appliedPosition() + " fatals=" + r.fatals + " " + r.probe().detail());
        }
        assertEquals("the cluster genesis check refuses before any frame of the new history is applied",
                r.replica.follower().restoredPosition(), r.replica.follower().appliedPosition());
        assertFalse("/ready after the refusal", r.probe().ready());
    }

    /** A VERIFIED copy at {@code p} written by this replica's producer, other than {@code excludeRec}. */
    private static boolean ownVerifiedCopyAt(final ReplicaRig r, final long p, final long excludeRec) {
        final com.openexchange.cluster.shadow.ShadowCatalog.Scan scan = r.scan();
        if (scan == null) {
            return false;
        }
        for (final ShadowEntry e : scan.all()) {
            if (e.recordingId() != excludeRec && e.state() == ShadowEntry.State.VERIFIED && e.header() != null
                    && e.header().logPosition() == p && e.header().producerKind() == ProducerKind.REPLICA.code()
                    && e.header().producerId() == ReplicaRig.SLOT) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------------------------------------- helpers

    /** A SNAPSHOT on the leader; the replica's VERIFIED ALIGNED shadow at P_cm equals the host member's image. */
    private static long aligned(final ReplicaRig r, final Member host) {
        final long p = r.rig.snapshotAll();
        final LogicalShadow s = r.awaitVerified(p, ShadowKind.ALIGNED, 60_000);
        ReplicaRig.assertPayloadEqualsMember(p, r.payload(s), r.memberImage(host.id, p));
        return p;
    }

    /** Trailer-less or too-short recordings of this replica's producer in a fresh scan. */
    private static int orphans(final ReplicaRig r) {
        int n = 0;
        for (final ShadowEntry e : r.freshScan().all()) {
            if ((e.state() == ShadowEntry.State.NO_TRAILER || e.state() == ShadowEntry.State.TOO_SHORT)
                    && e.alias() != null && e.alias().producerKind() == ProducerKind.REPLICA.code()
                    && e.alias().producerId() == ReplicaRig.SLOT) {
                n++;
            }
        }
        return n;
    }

    private static void startFresh(final ReplicaRig r, final String dir) {
        r.windowGap();
        r.startReplica(r.builder(ReplicaRig.SLOT, dir).build());
        r.awaitBootstrapped(ReplicaRig.WAIT_MS);
        assertEquals(ReplicaReadiness.RestorePath.GENESIS, r.replica.readiness().restorePath());
        r.awaitCaughtUp(60_000);
        assertEquals("no own shadow before the forgeries", Collections.emptyList(), r.verified(null));
    }

    private static void restart(final ReplicaRig r, final String dir,
                                final java.util.Set<ReplicaNegativeControl> off) {
        r.stopReplica();
        r.windowGap();
        r.startReplica(r.builder(ReplicaRig.SLOT, dir).testHooks(r.faults, off).build());
        r.rig.await(() -> r.replica.follower().bootstrapDone() || !r.fatals.isEmpty(), ReplicaRig.WAIT_MS,
                () -> "the replica never decided its bootstrap: " + r.probe().detail());
    }

    private static void assertIdentityRefused(final ReplicaRig r) {
        if (r.fatals.isEmpty() || !r.fatals.get(0).contains("IDENTITY_CROSS_CHECK_FAILED")) {
            throw new AssertionError("IDENTITY_CROSS_CHECK_FAILED expected, got restore_path="
                    + r.replica.readiness().restorePath() + " at " + r.replica.follower().restoredPosition()
                    + " fatals=" + r.fatals);
        }
        assertNotEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
    }

    private static void assertOneRestore(final ReplicaRig r) {
        final long n = r.replica.follower().restoreAttempts();
        if (n > 1) {
            throw new AssertionError("two restore attempts in one process: " + n + " (restore_path="
                    + r.replica.readiness().restorePath() + ")");
        }
    }

    private static void assertNoGenesisOnPrunedLog(final ReplicaRig r) {
        final long start = r.replica.follower().logStartPositionAtBootstrap();
        if (r.replica.readiness().restorePath() == ReplicaReadiness.RestorePath.GENESIS && start > 0) {
            throw new AssertionError("genesis on a pruned log: start=" + start);
        }
        assertTrue("pruned-log refusal expected: " + r.fatals, !r.fatals.isEmpty());
    }

    @SuppressWarnings("unused")
    private static List<Long> positions(final ReplicaRig r) {
        return r.allShadowPositions();
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.RecordingLog;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The refusal and recovery rows of the stub restore protocol on a real single-member cluster
 * (embedded driver, real archive, real positions; shadows written into the member's own archive from its own
 * committed log by the 11.0 producer). Each row is its own cold restart on the same directories.
 *
 * <p>Rows: pruned log without a shadow halts with the 7.3 text, never genesis (control: the 7.1 precondition faked
 * true); the operator step of the 7.3 runbook ({@code ClusterTool invalidate-latest-snapshot}) recovers through the
 * full path (C5); a foreign genesis at EQUAL is refused and TAIL is taken; a pre-ADOPT stub refuses every candidate
 * GENESIS_UNVERIFIED and takes virtual genesis on a complete log (review F9); a digest flip at EQUAL is CORRUPT and
 * TAIL is taken; TAIL iterates downwards past a CONFLICT (control: iteration off) and past a CORRUPT; a CONFLICT at the
 * SKIP position abandons SKIP; a SKIP candidate that is not skip-safe is not chosen (a forged journal claim, and D7: the
 * real journal writer agent stopped below P_s, control {@code skipSafeOff} -> {@code journal gap after SKIP}); a
 * CONFLICT at P_cm abandons EQUAL;
 * a failure after {@code applier.restore} halts with no second attempt (control: a second attempt allowed); a forged
 * RecordingLog copy whose second term names another recording refuses the tail range.</p>
 *
 * <p>The stub's {@code P_cm != recovery position} and clusterId rows are decode-level ({@code ShadowRestore.decodeStub},
 * {@code StubDispatchAndRulesTest.stubConsistencyChecks}): a real cluster cannot be made to hand {@code onStart} a stub
 * with another position or cluster id without re-sealing the archived snapshot.</p>
 */
public class ShadowRefusalTest {

    private static final int DEPOSITS_BEFORE = 900;
    private static final int DEPOSITS_AFTER = 120;

    /** The common shape: ADOPT, deposits, stub snapshot at P_cm, deposits; returns the committed log. */
    private static RecordedLog story(final StubStory s) {
        s.deposits(DEPOSITS_BEFORE);
        final long pCm = s.snapshot();
        s.deposits(DEPOSITS_AFTER);
        final RecordedLog log = s.log();
        StubStory.indexOf(log, pCm);
        lastPcm = pCm;
        return log;
    }

    private static long lastPcm;

    private static long at(final RecordedLog log, final long pCm, final int delta) {
        return log.positions()[StubStory.indexOf(log, pCm) + delta];
    }

    /** TAIL must have been taken from {@code q} (the iteration oracle). */
    private static void assertTailFrom(final StubStory s, final long q, final String abandonedAt) {
        final String line = s.pathLine();
        if (line == null || !line.contains("restored path=TAIL P_s=" + q + " ")) {
            throw new AssertionError("TAIL abandoned at " + abandonedAt + " with usable base " + q + " below; path line "
                    + line + "; stderr " + s.rig.errTee.lines);
        }
    }

    private static void restartAndAwaitEqual(final StubStory s, final byte[] pre, final String... faults) {
        s.kill();
        s.restart(faults);
        s.awaitReady();
        s.awaitImage(pre, "restored image");
        assertTrue(s.m.halts.toString(), s.m.halts.isEmpty());
    }

    // ------------------------------------------------------------------------------------------ halts and recovery

    @Test
    public void aStubWithNoShadowOnAPrunedLogHaltsWithTheRunbookTextNeverGenesis() throws Exception {
        try (StubStory s = new StubStory(24000, "rcs-s4-11.3-pruned", StubStory.shadow()).start(true)) {
            story(s);
            final long pCm = lastPcm;
            final long boundary = pCm / StubStory.SEGMENT * StubStory.SEGMENT;
            assertTrue("P_cm " + pCm + " lies past the first segment", boundary > 0);
            final long start = s.rig.purgeLogTo(s.m, boundary);
            assertEquals("the log was purged to " + boundary, boundary, start);
            s.quiesce();
            s.kill();
            s.restart();
            final String halt = s.awaitHalt("FATAL shadow restore: stub at P_cm=" + pCm + " and no restore path");
            System.out.println("GREEN 11.3 pruned log -> " + halt);
            assertTrue(halt, halt.contains("log starts at " + start + " > 0"));
            assertTrue(halt, halt.contains("Runbook: (1) ClusterTool invalidate-latest-snapshot"));
            // the startup pull (3b) is named: no source configured
            assertTrue(halt, halt.contains("(3) pull: disabled: SHADOW_PULL_SOURCES empty (a shadow with P_s >= "
                    + start + " from SHADOW_PULL_SOURCES)"));
            assertNoVirtualGenesisOnAPrunedLog(s, start);
            assertNull("no restore path line", s.pathLine());

            // control: the 7.1 precondition faked true on the pruned log
            s.kill();
            s.restart("virtualGenesisPreconditionFaked");
            s.rig.await(() -> !s.m.halts.isEmpty(), 60_000, "the faked virtual genesis must stop somewhere");
            RestoreDrillDryRun.nc("11.3-virtual-genesis-on-pruned-log", "virtual genesis taken on a pruned log: start=" + start,
                    () -> assertNoVirtualGenesisOnAPrunedLog(s, start));
            System.out.println("[11.3] faked precondition stderr: " + s.rig.errTee.lines);
        }
    }

    private static void assertNoVirtualGenesisOnAPrunedLog(final StubStory s, final long start) {
        if (s.counter("core_virtual_genesis_total") > 0 || StubStory.line(s.rig.errTee.lines, "shadow restore VIRTUAL GENESIS") != null) {
            throw new AssertionError("virtual genesis taken on a pruned log: start=" + start);
        }
    }

    @Test
    public void theRunbookStepInvalidateLatestSnapshotRecoversThroughTheFullPath() throws Exception {
        try (StubStory s = new StubStory(24100, "rcs-s4-11.3-runbook", StubStory.off()).start(true)) {
            s.deposits(DEPOSITS_BEFORE);
            final long p1 = s.snapshot(); // FULL image (SNAPSHOT_MODE=off)
            assertTrue(s.rig.tee.all("kind=full").toString(), !s.rig.tee.all("kind=full").isEmpty());
            final long boundary = p1 / StubStory.SEGMENT * StubStory.SEGMENT;
            assertTrue("P1 " + p1 + " lies past the first segment", boundary > 0);
            final long start = s.rig.purgeLogTo(s.m, boundary);
            s.quiesce();
            s.kill();
            // the node is switched to stub mode: its next snapshot is a stub at P2, and no shadow exists
            s.m.settings = StubStory.shadow();
            s.restart();
            s.awaitReady();
            assertTrue("full path from P1: " + s.pathLine(), s.pathLine() != null && s.pathLine().contains("path=FULL"));
            s.connect(false);
            s.deposits(DEPOSITS_AFTER);
            final long p2 = s.snapshot();
            assertTrue(s.rig.tee.all("kind=stub").toString(), !s.rig.tee.all("kind=stub").isEmpty());
            s.deposits(20);
            final byte[] pre = s.quiesce();
            s.kill();
            s.restart();
            final String halt = s.awaitHalt("FATAL shadow restore: stub at P_cm=" + p2 + " and no restore path");
            assertTrue(halt, halt.contains("log starts at " + start + " > 0"));
            s.kill();
            // the 7.3 runbook, step (1)
            final File clusterDir = s.m.clusterDir();
            assertTrue("invalidate-latest-snapshot", ClusterTool.invalidateLatestSnapshot(System.out, clusterDir));
            s.restart();
            s.awaitReady();
            s.awaitImage(pre, "after invalidate-latest-snapshot");
            assertTrue("full path again: " + s.pathLine(), s.pathLine() != null && s.pathLine().contains("path=FULL"));
            assertEquals("core_restore_path FULL", 1, s.counter("core_restore_path"));
            System.out.println("GREEN 11.3 runbook: halt at P2=" + p2 + ", invalidate-latest-snapshot, FULL from P1=" + p1
                    + " (log start " + start + "), image equals the pre-crash image");
        }
    }

    // ------------------------------------------------------------------------------------------ identity and digest

    @Test
    public void aForeignGenesisAtEqualIsRefusedAndTailIsTaken() throws Exception {
        try (StubStory s = new StubStory(24200, "rcs-s4-11.3-foreign", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            final Written foreign = s.producer().produceForeignGenesis(s.who(), log, pCm, 0xF00DL);
            assertTrue(foreign.toString(), foreign.ok());
            final long q = at(log, pCm, -8);
            s.produce(log, q);
            restartAndAwaitEqual(s, s.quiesce());
            assertTailFrom(s, q, "a foreign EQUAL");
            // the catalog is built with the stub's genesisId, so the foreign copy is FOREIGN at scan time and never an
            // EQUAL answer: STRICT identity (and its GENESIS_MISMATCH counter) never sees it
            final String scan = StubStory.line(s.rig.tee.lines, "[shadow restore] restore from stub");
            System.out.println("GREEN 11.3 foreign genesis: " + scan + "; refused GENESIS_MISMATCH="
                    + ServiceProbe.refused(s.m.service, "GENESIS_MISMATCH") + ", path " + s.pathLine());
            assertTrue("2 recordings, the foreign one not usable: " + scan, scan != null
                    && scan.contains("2 shadow recording(s), usable=1 conflicts=0"));
            assertEquals("STRICT identity never saw it", 0, ServiceProbe.refused(s.m.service, "GENESIS_MISMATCH"));
        }
    }

    @Test
    public void aPreAdoptStubRefusesEveryCandidateGenesisUnverifiedThenVirtualGenesis() throws Exception {
        try (StubStory s = new StubStory(24300, "rcs-s4-11.3-preadopt", StubStory.shadow()).start(false)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            // the node never ADOPTed: the only way a shadow carries a nonce is a producer that has one
            assertTrue(s.producer().produceForeignGenesis(s.who(), log, pCm, 0x5EEDL).ok());
            assertTrue(s.producer().produceForeignGenesis(s.who(), log, at(log, pCm, -8), 0x5EEDL).ok());
            restartAndAwaitEqual(s, s.quiesce());
            System.out.println("GREEN 11.3 pre-ADOPT: GENESIS_UNVERIFIED=" + ServiceProbe.refused(s.m.service,
                    "GENESIS_UNVERIFIED") + " virtual genesis=" + s.counter("core_virtual_genesis_total") + " path "
                    + s.pathLine());
            assertEquals("EQUAL and TAIL candidates refused GENESIS_UNVERIFIED", 2,
                    ServiceProbe.refused(s.m.service, "GENESIS_UNVERIFIED"));
            assertEquals(1, s.counter("core_virtual_genesis_total"));
            assertTrue(s.pathLine(), s.pathLine().contains("restored path=VIRTUAL_GENESIS"));
            assertNotNull(StubStory.line(s.rig.errTee.lines, "shadow restore VIRTUAL GENESIS: no shadow for stub at P_cm=" + pCm));
        }
    }

    @Test
    public void aDigestFlipAtEqualIsCorruptAndTheNextCandidateIsTaken() throws Exception {
        try (StubStory s = new StubStory(24400, "rcs-s4-11.3-flip", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            final Written equal = s.produce(log, pCm);
            s.producer().flipPayloadByte(equal);
            final long q = at(log, pCm, -8);
            s.produce(log, q);
            restartAndAwaitEqual(s, s.quiesce());
            assertTailFrom(s, q, "a CORRUPT EQUAL");
            assertEquals("CORRUPT counted", 1, ServiceProbe.refused(s.m.service, "CORRUPT"));
            System.out.println("GREEN 11.3 digest flip: CORRUPT=1, " + s.pathLine());

            // multi-recording (review F2), on a forged COPY of this node's RecordingLog: the restart above made term 1
            final List<long[]> terms = ServiceProbe.restoreTerms(s.m.clusterDir());
            assertTrue("two terms after the restart: " + terms.size(), terms.size() >= 2);
            final long base1 = terms.get(terms.size() - 1)[1];
            final long hi = base1 + 64;
            assertNull("the real log: one recording", ServiceProbe.singleRecording(s.m.clusterDir(), q, hi));
            // a byte-level forged COPY (Aeron's own appendTerm refuses a second recording id): the last TERM entry of
            // the copy names recording id + 1
            final File forged = new File(s.rig.tmp, "forged-recording-log");
            IoUtil.delete(forged, true);
            assertTrue(forged.mkdirs());
            final long realId = StubStory.forgeLastTermRecordingId(s.m.clusterDir(), forged);
            // Aeron 1.53.1 refuses such a RecordingLog when it is opened (RecordingLog.validateTermRecordingId on
            // load): no RecordingLog Aeron loads can name two recordings, so the precondition (6.4) is a second net
            String refused = null;
            try {
                ServiceProbe.singleRecording(forged, q, hi);
            } catch (final RuntimeException e) {
                refused = e.getMessage();
            }
            System.out.println("GREEN 11.3 forged RecordingLog copy (term " + terms.get(terms.size() - 1)[0]
                    + " names recording " + (realId + 1) + ") -> refused by Aeron at load: " + refused);
            assertEquals("ERROR - invalid TERM recordingId=" + (realId + 1) + ", expected recordingId=" + realId,
                    refused);
        }
    }

    // ------------------------------------------------------------------------------------------ TAIL iteration (F4)

    @Test
    public void tailIteratesDownwardsPastAConflict() throws Exception {
        try (StubStory s = new StubStory(24500, "rcs-s4-11.3-tailconflict", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            final long q1 = at(log, pCm, -3);
            final long q2 = at(log, pCm, -9);
            s.produce(log, q1);
            // the first frame after q1 that changes the image (a frame count can land on a timer that changes nothing)
            final long y1 = com.openexchange.core.infrastructure.persistence.TestShadowProducer.firstChangeAfter(log, q1);
            System.out.println("[11.3] different content at q1=" + q1 + ": S(" + y1 + "), P_cm-2=" + at(log, pCm, -2));
            assertTrue(s.producer().produceDifferentContent(s.who(), log, q1, y1).ok());
            s.produce(log, q2);
            final byte[] pre = s.quiesce();
            restartAndAwaitEqual(s, pre);
            assertTailFrom(s, q2, "CONFLICT " + q1);
            assertEquals("core_restore_conflict_total{TAIL}", 1, ServiceProbe.conflicts(s.m.service, "TAIL"));
            assertNotNull("CRITICAL line", StubStory.line(s.rig.errTee.lines, "CRITICAL: shadow CONFLICT at P=" + q1));
            System.out.println("GREEN 11.3 TAIL past CONFLICT " + q1 + ": " + s.pathLine());

            // control: iteration off
            restartAndAwaitEqual(s, pre, "tailIterationOff");
            RestoreDrillDryRun.nc("11.3-tail-iteration-off", "TAIL abandoned at CONFLICT " + q1 + " with usable base " + q2
                    + " below", () -> assertTailFrom(s, q2, "CONFLICT " + q1));
        }
    }

    @Test
    public void tailIteratesDownwardsPastACorruptCopy() throws Exception {
        try (StubStory s = new StubStory(24600, "rcs-s4-11.3-tailcorrupt", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            final long q1 = at(log, pCm, -3);
            final long q2 = at(log, pCm, -9);
            s.producer().flipPayloadByte(s.produce(log, q1));
            s.produce(log, q2);
            restartAndAwaitEqual(s, s.quiesce());
            assertTailFrom(s, q2, "CORRUPT " + q1);
            assertEquals(1, ServiceProbe.refused(s.m.service, "CORRUPT"));
            System.out.println("GREEN 11.3 TAIL past CORRUPT " + q1 + ": " + s.pathLine());
        }
    }

    // ------------------------------------------------------------------------------------------ SKIP and EQUAL refusals

    @Test
    public void aConflictAtTheSkipPositionAbandonsSkip() throws Exception {
        try (StubStory s = new StubStory(24700, "rcs-s4-11.3-skipconflict", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            final long s1 = at(log, pCm, 3);
            s.produce(log, s1);
            assertTrue(s.producer().produceDifferentContent(s.who(), log, s1, com.openexchange.core.infrastructure.persistence.TestShadowProducer.firstChangeAfter(log, s1)).ok());
            final long q = at(log, pCm, -8);
            s.produce(log, q);
            restartAndAwaitEqual(s, s.quiesce());
            assertTailFrom(s, q, "the SKIP CONFLICT " + s1);
            assertEquals("core_restore_conflict_total{SKIP}", 1, ServiceProbe.conflicts(s.m.service, "SKIP"));
            System.out.println("GREEN 11.3 CONFLICT at SKIP " + s1 + ": " + s.pathLine());
        }
    }

    @Test
    public void aSkipCandidateThatIsNotSkipSafeIsNotChosen() throws Exception {
        try (StubStory s = new StubStory(24800, "rcs-s4-11.3-skipunsafe", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            final long s1 = at(log, pCm, 3);
            // the header claims an armed journal at seq 1,000,000; this node has no journal recording at all
            assertTrue(s.producer().produceJournalClaim(s.who(), log, s1, 1_000_000L).ok());
            final long q = at(log, pCm, -8);
            s.produce(log, q);
            restartAndAwaitEqual(s, s.quiesce());
            assertTailFrom(s, q, "a SKIP that is not skip-safe at " + s1);
            assertEquals("core_skip_unsafe_total", 1, s.counter("core_skip_unsafe_total"));
            System.out.println("GREEN 11.3 SKIP not skip-safe: core_skip_unsafe_total=1, " + s.pathLine());
        }
    }

    // ------------------------------------------------------------------------------------------ D7: the journal gap

    /** Every {@code journalSeq} in this member's money-journal recordings (all of them, stopped or active). */
    static java.util.TreeSet<Long> journalSeqs(final StubStory s) {
        final io.aeron.archive.client.AeronArchive archive = s.rig.archive(s.m);
        final java.util.List<long[]> recs = new java.util.ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "aeron:ipc",
                com.openexchange.core.infrastructure.InfrastructureConstants.MONEY_JOURNAL_STREAM_ID,
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, m, sid, str, ch, oc, src) ->
                        recs.add(new long[] {recordingId, startPosition, stopPosition}));
        final java.util.TreeSet<Long> seqs = new java.util.TreeSet<>();
        final com.openexchange.cluster.shadow.ShadowReplay replay = new com.openexchange.cluster.shadow.ShadowReplay(
                s.rig.aeron(s.m), archive, org.agrona.concurrent.SystemNanoClock.INSTANCE);
        for (final long[] r : recs) {
            final long stop = r[2] == io.aeron.archive.client.AeronArchive.NULL_POSITION
                    ? archive.getRecordingPosition(r[0]) : r[2];
            if (stop <= r[1]) {
                continue;
            }
            replay.readAll(r[0], r[1], stop, (buffer, offset, length, h) -> seqs.add(buffer.getLong(offset
                    + com.openexchange.core.infrastructure.journal.MoneyJournalTail.JOURNAL_SEQ_OFFSET,
                    java.nio.ByteOrder.LITTLE_ENDIAN)), System.nanoTime() + 30_000_000_000L);
        }
        return seqs;
    }

    /** The 8.1 oracle: the member's journal recordings hold every seq from the first to the last, no hole. */
    static void assertNoJournalGap(final java.util.TreeSet<Long> seqs) {
        long expect = seqs.first();
        for (final long q : seqs) {
            if (q != expect) {
                throw new AssertionError("journal gap after SKIP: missing seq " + expect + ".." + (q - 1)
                        + " (recorded " + seqs.first() + ".." + seqs.last() + ", " + seqs.size() + " seqs)");
            }
            expect = q + 1;
        }
    }

    /** The real ring and writer agent (W4's AC:208-212 order), rebuilt on every boot of {@code s.m}. */
    private static org.agrona.concurrent.AgentRunner[] journalRig(final StubStory s) {
        final com.openexchange.core.infrastructure.journal.MoneyJournal[] ring = {null};
        final org.agrona.concurrent.AgentRunner[] writer = {null};
        s.m.beforeLaunch = svc -> {
            ring[0] = new com.openexchange.core.infrastructure.journal.MoneyJournal(16 * 1024 * 1024);
            svc.setMoneyJournalSink(ring[0]);
        };
        s.m.beforeContainer = m -> {
            writer[0] = new org.agrona.concurrent.AgentRunner(new org.agrona.concurrent.BackoffIdleStrategy(),
                    Throwable::printStackTrace, null,
                    new com.openexchange.core.infrastructure.journal.MoneyJournalWriterAgent(ring[0],
                            m.cfg.aeronArchiveContext().clone()::clone,
                            com.openexchange.core.infrastructure.InfrastructureConstants.MONEY_JOURNAL_CHANNEL,
                            com.openexchange.core.infrastructure.InfrastructureConstants.MONEY_JOURNAL_STREAM_ID));
            org.agrona.concurrent.AgentRunner.startOnThread(writer[0]);
        };
        return writer;
    }

    private static void awaitJournalDurable(final StubStory s, final String when) {
        s.rig.await(() -> {
            final java.util.TreeSet<Long> seqs = journalSeqs(s);
            return !seqs.isEmpty() && seqs.last() == ServiceProbe.journalSeq(s.m.service);
        }, 60_000, () -> when + ": the journal recording never reached the engine's journalSeq "
                + ServiceProbe.journalSeq(s.m.service));
    }

    /**
     * D7 (8.1, K8, G3): a SKIP candidate above P_cm on a node whose own journal recording stops BELOW the shadow's
     * journalSeq. The writer agent is stopped after the stub snapshot, money moves (its journal entries stay in the ring
     * and die with the kill), the SKIP shadow is written from the committed log with its real header journalSeq; then
     * the restart. Correct: SKIP refused ({@code core_skip_unsafe_total == 1}), TAIL from the base below P_cm re-emits
     * the lost entries, the recordings hold every seq. Control {@code skipSafeOff}: SKIP is taken and re-emits only
     * entries past P_s: {@code journal gap after SKIP: missing seq <a>..<b>}.
     */
    private static void journalGapRow(final int port, final boolean skipSafeOff) throws Exception {
        try (StubStory s = new StubStory(port, "rcs-s4-11.3-journalgap-" + skipSafeOff, StubStory.shadow())) {
            final org.agrona.concurrent.AgentRunner[] writer = journalRig(s);
            s.start(true);
            s.rig.armMoneyJournal(s.admin);
            s.deposits(40);
            final RecordedLog early = s.log();
            final long q = early.positions()[early.positions().length - 1];
            s.produce(early, q); // the TAIL base below P_cm
            s.deposits(40);
            final long pCm = s.snapshot();
            awaitJournalDurable(s, "before the writer stops");
            final long durable = journalSeqs(s).last();
            org.agrona.CloseHelper.quietClose(writer[0]); // this node's journal stops here
            s.deposits(30);
            final RecordedLog log = s.log();
            final long pS = log.positions()[log.positions().length - 1];
            assertTrue("P_s " + pS + " > P_cm " + pCm, pS > pCm);
            final Written w = s.produce(log, pS);
            final long seqAtPs = ServiceProbe.journalSeq(s.m.service);
            assertTrue("the money moved past the durable journal: " + seqAtPs + " > " + durable, seqAtPs > durable);
            System.out.println("[11.3-D7] P_cm=" + pCm + " durable journalSeq=" + durable + " P_s=" + pS
                    + " (journalSeq " + seqAtPs + ") shadow " + w);
            s.rig.forgetAll(); // the clients ride the member's driver: close them before the kill
            s.kill();
            if (skipSafeOff) {
                s.restart("skipSafeOff");
            } else {
                s.restart();
            }
            s.awaitReady();
            assertTrue(s.m.halts.toString(), s.m.halts.isEmpty());
            s.connect(false);
            s.deposits(5);
            awaitJournalDurable(s, "after the restart");
            final java.util.TreeSet<Long> seqs = journalSeqs(s);
            if (skipSafeOff) {
                assertTrue("the control took SKIP: " + s.pathLine(), s.pathLine() != null
                        && s.pathLine().contains("restored path=SKIP P_s=" + pS + " "));
                RestoreDrillDryRun.nc("11.3-skipSafeOff", "journal gap after SKIP: missing seq " + (durable + 1) + "..",
                        () -> assertNoJournalGap(seqs));
            } else {
                assertTailFrom(s, q, "a SKIP over a journal that stops at " + durable + " below P_s's " + seqAtPs);
                assertEquals("core_skip_unsafe_total", 1, s.counter("core_skip_unsafe_total"));
                assertNoJournalGap(seqs);
                System.out.println("GREEN 11.3 D7 journal stops below P_s: SKIP refused, " + s.pathLine()
                        + "; journal " + seqs.first() + ".." + seqs.last() + " with no gap");
            }
        }
    }

    @Test
    public void aSkipOverAJournalThatStopsBelowPsIsRefusedAndTheJournalHasNoGap() throws Exception {
        journalGapRow(27400, false);
    }

    @Test
    public void controlSkipSafeOffLeavesAJournalGapAfterSkip() throws Exception {
        journalGapRow(27500, true);
    }

    @Test
    public void aConflictAtPcmAbandonsEqualAndTailIsTaken() throws Exception {
        try (StubStory s = new StubStory(24900, "rcs-s4-11.3-equalconflict", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            s.produce(log, pCm);
            assertTrue(s.producer().produceDifferentContent(s.who(), log, pCm, com.openexchange.core.infrastructure.persistence.TestShadowProducer.firstChangeAfter(log, pCm)).ok());
            final long q = at(log, pCm, -8);
            s.produce(log, q);
            restartAndAwaitEqual(s, s.quiesce());
            assertTailFrom(s, q, "the EQUAL CONFLICT " + pCm);
            assertEquals("core_restore_conflict_total{EQUAL}", 1, ServiceProbe.conflicts(s.m.service, "EQUAL"));
            assertNotNull(StubStory.line(s.rig.errTee.lines, "CRITICAL: shadow CONFLICT at P=" + pCm));
            System.out.println("GREEN 11.3 CONFLICT at P_cm: " + s.pathLine());
        }
    }

    // ------------------------------------------------------------------------------------------ one attempt

    @Test
    public void aFailureAfterApplierRestoreHaltsWithNoSecondAttempt() throws Exception {
        try (StubStory s = new StubStory(25000, "rcs-s4-11.3-oneattempt", StubStory.shadow()).start(true)) {
            final RecordedLog log = story(s);
            final long pCm = lastPcm;
            s.produce(log, pCm);
            s.produce(log, at(log, pCm, -8)); // an older usable base exists: a second attempt would have a candidate
            s.quiesce();
            s.kill();
            s.restart("failAfterRestore");
            final String halt = s.awaitHalt("restore of shadow P_s=" + pCm);
            System.out.println("GREEN 11.3 one attempt -> " + halt);
            assertTrue(halt, halt.contains("EQUAL restore of shadow P_s=" + pCm)
                    && halt.contains("failed after the engine was touched") && halt.contains("no second attempt"));
            assertNull("no restore path line", s.pathLine());
            assertNull("no second attempt", StubStory.line(s.rig.errTee.lines, "two restore attempts"));

            // control: a second attempt allowed
            s.kill();
            s.restart("failAfterRestore", "secondAttemptAllowed");
            s.rig.await(() -> !s.m.halts.isEmpty(), 60_000, "the control must stop");
            RestoreDrillDryRun.nc("11.3-second-attempt", "two restore attempts in one process", () -> {
                final String l = StubStory.line(s.rig.errTee.lines, "FATAL shadow restore:");
                if (l != null && l.contains("two restore attempts")) {
                    throw new AssertionError(l);
                }
            });
        }
    }
}

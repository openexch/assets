// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FrameDescriptor;
import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S9h 11.4 (journal half): every {@code t1-journal} verb of 7.2 against an {@link AeronArchive} reader in the test
 * (listings and replayed bytes as the verb says; {@code oversize} arrives fragmented; RESTART makes the K8 shape),
 * KILL / REVIVE keeps the catalog, and REGENESIS writes a FRESH at seq 1 under the new epoch then seqs 2.. dense
 * (review FIX-7).
 */
public class T1JournalHostTest {

    static final int PORT_BASE = 18_700;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private T1Journal.Host host;
    private Path journal;
    private List<byte[]> records;

    @Before
    public void setUp() throws Exception {
        final FeedStory.Story s = FeedStoryTest.story(1);
        journal = tmp.newFolder("story").toPath().resolve("journal.fjr");
        java.nio.file.Files.write(journal, FeedFiles.encodeFjr(s.journal()));
        records = TranslatorProofs.recordsOf(s.journal());
        host = new T1Journal.Host(tmp.newFolder("t1j").toPath());
    }

    @After
    public void tearDown() {
        FeedHarnessSeams.clear();
        if (host != null) {
            host.close();
        }
    }

    String ok(final String line) {
        final String r = host.handle(line);
        assertTrue(line + " -> " + r, r.startsWith("OK"));
        return r;
    }

    /** {recordingId, start, stop} of every recording of source k. */
    List<long[]> list(final int k) {
        final List<long[]> out = new ArrayList<>();
        for (final String part : ok("LIST " + k).substring(2).split(";")) {
            final String p = part.trim();
            if (!p.isEmpty()) {
                final String[] x = p.split(" ");
                out.add(new long[] {Long.parseLong(x[0]), Long.parseLong(x[1]), Long.parseLong(x[2])});
            }
        }
        return out;
    }

    /** A replayed fragment: its header flags and payload. */
    record Fragment(int flags, byte[] bytes) { }

    /** Replays one stopped recording of source k into raw fragments (a test-side reader, its own archive client). */
    List<Fragment> replay(final int k, final long[] recording) {
        final T1Journal.Source s = host.source(k);
        final List<Fragment> out = new ArrayList<>();
        try (AeronArchive reader = AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(s.driverDir())
                .controlRequestChannel(s.controlChannel()).controlRequestStreamId(T1Journal.CONTROL_STREAM)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {
            final long length = recording[2] - recording[1];
            if (length <= 0) {
                return out;
            }
            final int stream = 3_000 + (int) recording[0];
            try (Subscription sub = reader.replay(recording[0], recording[1], length, "aeron:ipc", stream)) {
                final long deadline = System.currentTimeMillis() + 10_000;
                while (sub.imageCount() == 0) {
                    assertTrue("replay image", System.currentTimeMillis() < deadline);
                    Thread.onSpinWait();
                }
                final Image image = sub.imageAtIndex(0);
                while (image.position() < recording[2] && !image.isEndOfStream()) {
                    image.poll((buffer, offset, len, header) -> {
                        final byte[] b = new byte[len];
                        buffer.getBytes(offset, b);
                        out.add(new Fragment(header.flags() & 0xFF, b));
                    }, 100);
                    assertTrue("replay to the stop position", System.currentTimeMillis() < deadline);
                }
            }
        }
        return out;
    }

    static List<byte[]> whole(final List<Fragment> fragments) {
        final List<byte[]> out = new ArrayList<>();
        for (final Fragment f : fragments) {
            assertEquals("an unfragmented frame", FrameDescriptor.UNFRAGMENTED & 0xFF, f.flags());
            out.add(f.bytes());
        }
        return out;
    }

    @Test
    public void journalHostVerbs() {
        ok("SOURCES 2 " + PORT_BASE);
        assertTrue(ok("LOAD " + journal).contains("dense"));
        ok("WRITE 0 1 5");
        ok("STOP 0");
        List<long[]> l = list(0);
        assertEquals("one recording", 1, l.size());
        List<byte[]> got = whole(replay(0, l.get(0)));
        assertEquals(5, got.size());
        for (int i = 0; i < 5; i++) {
            assertArrayEquals("WRITE copies record " + (i + 1), records.get(i), got.get(i));
        }
        // HOLE / FILL: seq 6 held back, written later in a new recording
        ok("HOLE 0 6");
        ok("WRITE 0 6 8");
        ok("STOP 0");
        l = list(0);
        got = whole(replay(0, l.get(1)));
        assertEquals("the hole skips 6", List.of(7L, 8L), seqs(got));
        ok("FILL 0 6");
        ok("STOP 0");
        l = list(0);
        assertEquals(3, l.size());
        assertEquals("FILL writes the held seq into a new recording", List.of(6L), seqs(whole(replay(0, l.get(2)))));
        // RESTART: a new recording re-journals from fromSeq (the K8 crash shape)
        ok("RESTART 0 3");
        ok("STOP 0");
        l = list(0);
        assertEquals("RESTART re-journals 3..8", List.of(3L, 4L, 5L, 6L, 7L, 8L), seqs(whole(replay(0, l.get(3)))));
        // PAD: other frame boundaries on another source
        ok("PAD 1 96");
        ok("WRITE 1 1 3");
        ok("STOP 1");
        final List<long[]> l1 = list(1);
        final List<Fragment> padded = replay(1, l1.get(0));
        final List<byte[]> realFrames = new ArrayList<>();
        for (final Fragment f : padded) {
            realFrames.add(f.bytes());
        }
        assertEquals("the pad is not a frame", List.of(1L, 2L, 3L), seqs(realFrames));
        long unpadded = 0;
        for (int i = 0; i < 3; i++) {
            unpadded += (32 + records.get(i).length + 31) & ~31;
        }
        assertEquals("PAD moved the boundaries by one 128 B padding frame", unpadded + 128, l1.get(0)[2] - l1.get(0)[1]);
        // EPOCH: an in-epoch relabel from a seq on
        ok("EPOCH 1 5 0x99");
        ok("WRITE 1 4 6");
        ok("STOP 1");
        final List<byte[]> relabel = whole(replay(1, list(1).get(1)));
        assertEquals(List.of(4L, 5L, 6L), seqs(relabel));
        assertEquals("seq 4 keeps its epoch", JournalRecords.meEpoch(records.get(3)), JournalRecords.meEpoch(relabel.get(0)));
        assertEquals("seq 5 carries 0x99", 0x99L, JournalRecords.meEpoch(relabel.get(1)));
        // CORRUPT: refused variants instead of seq; oversize arrives fragmented by the 1408 MTU
        ok("CORRUPT 0 9 oversize");
        ok("CORRUPT 0 10 schema3");
        ok("CORRUPT 0 11 head0");
        ok("WRITE 0 9 11");
        ok("STOP 0");
        l = list(0);
        final List<Fragment> bad = replay(0, l.get(l.size() - 1));
        assertTrue("oversize: more than one fragment", bad.size() > 3);
        assertEquals("the first fragment of 2000 B begins a message", FrameDescriptor.BEGIN_FRAG_FLAG & 0xFF,
                bad.get(0).flags() & 0xFF);
        int total = 0;
        int k = 0;
        while ((bad.get(k).flags() & FrameDescriptor.END_FRAG_FLAG) == 0) {
            total += bad.get(k++).bytes().length;
        }
        total += bad.get(k++).bytes().length;
        assertEquals("the fragments make 2000 bytes", 2000, total);
        assertEquals("schema 3 frame", 3, bad.get(k).bytes()[4]);
        assertEquals("head0: journalSeq 0", 0L, JournalRecords.journalSeq(bad.get(k + 1).bytes()));
        assertEquals("the reference gate refuses each", ReferenceFrameGate.Reason.SCHEMA,
                ReferenceFrameGate.check(bad.get(k).bytes()).reason());
        assertEquals(ReferenceFrameGate.Reason.HEAD, ReferenceFrameGate.check(bad.get(k + 1).bytes()).reason());
        // EMPTY: a stopped recording with nothing in it
        ok("EMPTY 1");
        final List<long[]> l2 = list(1);
        final long[] empty = l2.get(l2.size() - 1);
        assertEquals("EMPTY", empty[1], empty[2]);
        // RECORD with a 64m term; HOLD_REPLAY / RELEASE_REPLAY; PURGE answers (a tiny recording has no whole segment)
        ok("RECORD 1 term=64m");
        assertEquals(64 << 20, host.source(1).termLength);
        ok("WRITE 1 1 1");
        ok("HOLD_REPLAY 1");
        assertEquals(1, host.source(1).replays.size());
        ok("RELEASE_REPLAY 1");
        assertEquals(0, host.source(1).replays.size());
        final String purge = host.handle("PURGE 1 0");
        assertTrue(purge, purge.startsWith("OK") || purge.startsWith("ERR"));
        assertTrue("an unknown verb is an ERR", host.handle("NOPE").startsWith("ERR"));
        assertTrue("a write outside the journal is an ERR", host.handle("WRITE 0 1 999999").startsWith("ERR"));
        assertEquals("OK bye", host.handle("QUIT"));
        host = null;
    }

    static List<Long> seqs(final List<byte[]> frames) {
        final List<Long> out = new ArrayList<>();
        for (final byte[] f : frames) {
            out.add(JournalRecords.journalSeq(f));
        }
        return out;
    }

    /**
     * The seq of the first Trade record with no Trade next to it ({@code inRun} false), or of the first Trade right behind
     * another Trade ({@code inRun} true: cut there, the run ends in it and it is the last entry of a 62).
     */
    static long tradeSeq(final List<byte[]> records, final boolean inRun) {
        for (int i = 1; i + 1 < records.size(); i++) {
            final boolean trade = JournalRecords.templateId(records.get(i)) == 1;
            final boolean before = JournalRecords.templateId(records.get(i - 1)) == 1;
            final boolean after = JournalRecords.templateId(records.get(i + 1)) == 1;
            if (trade && (inRun ? before : !before && !after)) {
                return i + 1;
            }
        }
        throw new AssertionError("no " + (inRun ? "run" : "lone") + " trade in the story");
    }

    /**
     * The clean run of the story up to the round holding {@code seq}, that round's records up to {@code seq} with the
     * written {@code copy} in its place, translated by the reference translator (its gate included) and applied on the
     * pinned BRIDGE session: what a bridge offering the poisoned copy does to the real core.
     */
    static CoreApplyRunner applyWithCopy(final FeedStory.Story s, final long seq, final byte[] copy, final List<byte[]> frames) {
        final CoreApplyRunner r = FeedStory.bootClean(s.genesis());
        for (int round = 0; round < s.rounds(); round++) {
            FeedStory.applyBlock(r, s, round);
            final List<FeedFiles.Entry> in = new ArrayList<>();
            boolean last = false;
            for (final byte[] rec : s.answers().get(round)) {
                final long q = JournalRecords.journalSeq(rec);
                if (q <= seq) {
                    in.add(FeedFiles.Entry.record(q == seq ? copy : rec));
                }
                last |= q == seq;
            }
            for (final byte[] f : MeJournalReferenceTranslator.translate(in)) {
                frames.add(f);
                r.apply(CoreApplyRunner.BRIDGE_SESSION, f, FeedStory.feedTime(s.genesis().t0(), round));
            }
            if (last) {
                return r;
            }
        }
        throw new AssertionError("seq " + seq + " is not in the story");
    }

    /**
     * S9c 14.4 / R-K12c: {@code CORRUPT <src> <seq> poison} writes a copy of the Trade at seq whose only difference is
     * {@code takerIsBuy} raw 2. The reference gate admits it, the reference translator keeps the raw byte (a 61 when the
     * trade is alone, a 62 entry inside a run), and the real core refuses it on decode and latches SOURCE_SCHEMA with
     * {@code skip = seq}, its cursor at seq - 1. A non-Trade seq answers ERR and marks nothing; {@code none} removes the
     * mark so RESTART writes the valid copy again.
     */
    @Test
    public void corruptPoisonIsGateAdmittedAndCoreRefused() {
        final FeedStory.Story s = FeedStoryTest.story(1);
        ok("SOURCES 1 " + (PORT_BASE + 20));
        assertTrue("poison needs a loaded journal", host.handle("CORRUPT 0 2 poison").startsWith("ERR"));
        ok("LOAD " + journal);
        final String notTrade = host.handle("CORRUPT 0 1 poison");
        assertTrue("seq 1 is the FRESH 68, not a Trade: " + notTrade, notTrade.startsWith("ERR") && notTrade.contains("not a Trade"));
        assertTrue("out of range", host.handle("CORRUPT 0 " + (records.size() + 1) + " poison").startsWith("ERR"));
        assertTrue("nothing was marked", host.source(0).corrupt.isEmpty());
        assertEquals("the poison byte is the journal Trade's takerIsBuy", 8 + 84, T1Journal.Host.POISON_OFFSET);
        final long lone = tradeSeq(records, false);
        final long inRun = tradeSeq(records, true);
        ok("CORRUPT 0 " + lone + " poison");
        ok("CORRUPT 0 " + inRun + " poison");
        final long upTo = Math.max(lone, inRun);
        ok("WRITE 0 1 " + upTo);
        ok("STOP 0");
        final List<byte[]> got = whole(replay(0, list(0).get(0)));
        assertEquals(upTo, got.size());
        for (final long seq : new long[] {lone, inRun}) {
            final byte[] valid = records.get((int) seq - 1);
            final byte[] copy = got.get((int) seq - 1);
            final String what = (seq == lone ? "lone" : "in-run") + " trade " + seq;
            assertEquals(what + ": same length", valid.length, copy.length);
            for (int i = 0; i < valid.length; i++) {
                if (i != T1Journal.Host.POISON_OFFSET) {
                    assertEquals(what + ": byte " + i + " unchanged", valid[i], copy[i]);
                }
            }
            assertEquals(what + ": takerIsBuy raw 2", 2, copy[T1Journal.Host.POISON_OFFSET]);
            final ReferenceFrameGate.Verdict v = ReferenceFrameGate.check(copy);
            assertTrue(what + ": the gate admits it (" + v.reason() + ")", v.admitted());
            assertEquals(what + ": seq", seq, v.journalSeq());
            assertEquals(what + ": meEpoch", JournalRecords.meEpoch(valid), v.meEpoch());
            assertEquals(what + ": egressSeq", JournalRecords.egressSeq(valid), v.egressSeq());
            final List<byte[]> frames = new ArrayList<>();
            final CoreApplyRunner r = applyWithCopy(s, seq, copy, frames);
            final byte[] last = frames.get(frames.size() - 1);
            final int template = FeedDigests.templateOf(last);
            assertEquals(what + ": translated as " + (seq == lone ? "a 61" : "a 62 entry"), seq == lone ? 61 : 62, template);
            if (template == 61) {
                assertEquals(what + ": the 61 carries takerIsBuy raw 2", 2, last[8 + 68]);
            }
            final CoreApplyRunner.Result res = r.result();
            assertEquals(what + ": the core latches SOURCE_SCHEMA", "SOURCE_SCHEMA", res.latchReason());
            assertEquals(what + ": skip = seq", seq, res.latchSkip());
            assertEquals(what + ": expected = seq", seq, res.latchExpected());
            assertEquals(what + ": the cursor stops before it", seq - 1, res.lastAppliedJournalSeq());
            assertEquals(what + ": refused on decode", 1L, (long) res.counters().get("decodeRejectCount"));
            assertEquals(what + ": no apply fault", false, res.applyFailed());
            // the valid copy reach-clears: offered next, it applies and the latch is gone
            final List<FeedFiles.Entry> again = List.of(FeedFiles.Entry.record(valid));
            for (final byte[] f : MeJournalReferenceTranslator.translate(again)) {
                r.apply(CoreApplyRunner.BRIDGE_SESSION, f, r.engine().getLastLogTime());
            }
            assertEquals(what + ": the valid copy applies", seq, r.engine().feed().lastAppliedJournalSeq());
            assertEquals(what + ": and clears the latch", "NONE", r.engine().feed().latchReason().name());
        }
        // composes with RESTART like the other modes; none removes the mark
        ok("RESTART 0 " + lone);
        ok("STOP 0");
        List<long[]> l = list(0);
        assertEquals("RESTART re-writes the poisoned copy", 2, whole(replay(0, l.get(l.size() - 1))).get(0)[T1Journal.Host.POISON_OFFSET]);
        ok("CORRUPT 0 " + lone + " none");
        ok("RESTART 0 " + lone);
        ok("STOP 0");
        l = list(0);
        assertArrayEquals("after none, RESTART writes the valid copy", records.get((int) lone - 1),
                whole(replay(0, l.get(l.size() - 1))).get(0));
    }

    @Test
    public void journalHostReviveKeepsTheCatalog() {
        ok("SOURCES 1 " + (PORT_BASE + 50));
        ok("LOAD " + journal);
        ok("WRITE 0 1 4");
        ok("RECORD 0");
        ok("WRITE 0 5 6");
        ok("STOP 0");
        final List<long[]> before = list(0);
        assertEquals(2, before.size());
        ok("KILL 0");
        assertTrue("a killed source refuses writes", host.handle("WRITE 0 7 7").startsWith("ERR"));
        ok("REVIVE 0");
        final List<long[]> after = list(0);
        assertEquals("the catalog is recovered", before.size(), after.size());
        for (int i = 0; i < before.size(); i++) {
            assertArrayEquals("recording " + i, before.get(i), after.get(i));
        }
        assertEquals(List.of(5L, 6L), seqs(whole(replay(0, after.get(1)))));
    }

    @Test
    public void regenesisWritesFreshAtSeqOneUnderTheNewEpoch() {
        regenesisShape(this);
    }

    /** The FIX-7 shape, as a check the REGENESIS_AS_RELABEL control must fail. */
    static void regenesisShape(final T1JournalHostTest t) {
        t.ok("SOURCES 1 " + (PORT_BASE + 80));
        t.ok("LOAD " + t.journal);
        t.ok("WRITE 0 1 6");
        final int before = t.list(0).size();
        // the new epoch's records: a journal of other content (records 3..10 of the story, no FRESH of their own)
        final Path sub = t.journal.resolveSibling("sub.fjr");
        final List<FeedFiles.Entry> part = new ArrayList<>();
        for (int i = 2; i < 10; i++) {
            part.add(FeedFiles.Entry.record(t.records.get(i)));
        }
        try {
            java.nio.file.Files.write(sub, FeedFiles.encodeFjr(part));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
        t.ok("REGENESIS all 0x777 " + sub);
        t.ok("STOP 0");
        final List<long[]> l = t.list(0);
        assertEquals("REGENESIS starts a new recording", before + 1, l.size());
        final List<byte[]> f = whole(t.replay(0, l.get(l.size() - 1)));
        assertEquals("the first frame is a 68", 5, JournalRecords.templateId(f.get(0)));
        assertEquals("FRESH", 0, f.get(0)[8 + 38]);
        assertEquals("seq 1", 1L, JournalRecords.journalSeq(f.get(0)));
        assertEquals("the new epoch", 0x777L, JournalRecords.meEpoch(f.get(0)));
        for (int i = 1; i < f.size(); i++) {
            assertEquals("dense from 2 under the new epoch", i + 1, JournalRecords.journalSeq(f.get(i)));
            assertEquals(0x777L, JournalRecords.meEpoch(f.get(i)));
        }
        assertEquals("the FRESH, then the 8 records renumbered", 9, f.size());
    }

    @Test
    public void regenesisAsRelabelIsCaught() {
        FeedHarnessNegativeControlsTest.red(FeedHarnessSeams.REGENESIS_AS_RELABEL, "the first frame is a 68",
                () -> regenesisShape(this));
    }

    // ------------------------------------------------------------------ RC2 S10' 4.5 items 3 and 4

    @Test
    public void holeRangeHoldsEverySeq() {
        ok("SOURCES 2 " + (PORT_BASE + 60));
        ok("LOAD " + journal);
        assertTrue(ok("HOLE 0 3 5").contains("held 3..5 (3)"));
        ok("WRITE all 1 8");
        ok("STOP all");
        assertEquals("source 0 skips every seq of the range", List.of(1L, 2L, 6L, 7L, 8L), seqs(whole(replay(0, list(0).get(0)))));
        assertEquals("source 1 is whole", List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), seqs(whole(replay(1, list(1).get(0)))));
        assertTrue("the one-seq form still holds one seq", ok("HOLE 1 9").contains("held 9"));
        ok("RECORD 1");
        ok("WRITE 1 9 10");
        ok("STOP 1");
        final List<long[]> l1 = list(1);
        assertEquals(List.of(10L), seqs(whole(replay(1, l1.get(l1.size() - 1)))));
    }

    @Test
    public void holeRangeRefusesInvertedRange() {
        ok("SOURCES 1 " + (PORT_BASE + 70));
        ok("LOAD " + journal);
        for (final String bad : new String[] {"HOLE 0 5 3", "HOLE 0 0 2", "HOLE 0 -1 1", "HOLE 0 1 2 3", "HOLE 0"}) {
            final String r = host.handle(bad);
            assertTrue(bad + " -> " + r, r.startsWith("ERR"));
        }
        ok("WRITE 0 1 6");
        ok("STOP 0");
        assertEquals("a refused HOLE held nothing", List.of(1L, 2L, 3L, 4L, 5L, 6L), seqs(whole(replay(0, list(0).get(0)))));
    }

    @Test
    public void regenesisFreshOnlyThenWriteCarriesTheNewEpoch() {
        ok("SOURCES 1 " + (PORT_BASE + 90));
        ok("LOAD " + journal);
        ok("WRITE 0 1 6");
        final int before = list(0).size();
        assertTrue(ok("REGENESIS 0 777001 --fresh-only").contains("fresh-only"));
        ok("WRITE 0 1 6"); // the loaded journal again, seq 1 included, under the new epoch
        ok("STOP 0");
        final List<long[]> l = list(0);
        assertEquals("--fresh-only starts a new recording", before + 1, l.size());
        final List<byte[]> f = whole(replay(0, l.get(l.size() - 1)));
        assertEquals("the FRESH, then seq 1 again (byte-identical), then 2..6", 7, f.size());
        assertEquals("the first frame is a 68", 5, JournalRecords.templateId(f.get(0)));
        assertEquals("FRESH", 0, f.get(0)[8 + 38]);
        assertEquals("seq 1", 1L, JournalRecords.journalSeq(f.get(0)));
        assertEquals("the new epoch", 777001L, JournalRecords.meEpoch(f.get(0)));
        assertArrayEquals("the story's FRESH relabelled, so a WRITE of seq 1 is a byte-equal duplicate", f.get(0), f.get(1));
        for (int i = 1; i < f.size(); i++) {
            final byte[] story = records.get(i - 1);
            assertEquals("seqs unchanged", JournalRecords.journalSeq(story), JournalRecords.journalSeq(f.get(i)));
            assertEquals("the new epoch on every WRITE", 777001L, JournalRecords.meEpoch(f.get(i)));
            assertEquals("egressSeqs unchanged", JournalRecords.egressSeq(story), JournalRecords.egressSeq(f.get(i)));
            assertArrayEquals("only the epoch differs", JournalRecords.withHead(story, JournalRecords.journalSeq(story), 777001L,
                    JournalRecords.egressSeq(story)), f.get(i));
        }
        final String bad = host.handle("REGENESIS 0 0 --fresh-only");
        assertTrue("epoch 0 refused: " + bad, bad.startsWith("ERR"));
    }
}

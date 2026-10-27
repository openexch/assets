// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S9h 11.3 (story half) and the story rows of 11.1: coverage and determinism of {@link FeedStory}, the reverse
 * encoder, and the 5.1 / 5.2 a proofs on coherent inputs (the core's own stored digest equals the source's).
 */
public class FeedStoryTest {

    static final int ROUNDS = 12;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @After
    public void seamsOff() {
        FeedHarnessSeams.clear();
    }

    private static final Map<Long, FeedStory.Story> CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Stories of seeds 1..8 (built once per JVM; the builder itself asserts coverage and a coherent clean run). */
    static FeedStory.Story story(final long seed) {
        return CACHE.computeIfAbsent(seed, s -> FeedStory.build(s, ROUNDS));
    }

    @Test
    public void storyCoverage() {
        for (long seed = 1; seed <= 8; seed++) {
            final FeedStory.Story s = story(seed);
            for (final String item : FeedStory.COVERAGE) {
                assertTrue("seed " + seed + " covers " + item, s.coverage().get(item));
            }
            for (int r = 0; r < s.rounds(); r++) {
                assertTrue("seed " + seed + " round " + r + " has a feed record (FIX-4)", !s.answers().get(r).isEmpty());
                for (final FeedFiles.LogEntry e : s.blocks().get(r)) {
                    assertTrue("core block " + r + " inside its window", e.timestamp() >= FeedStory.blockTime(FeedStory.T0, r)
                            && e.timestamp() <= FeedStory.blockTime(FeedStory.T0, r) + 499);
                }
            }
            final byte[] first = s.answers().get(0).get(0);
            assertEquals("seq 1 is a 68", 5, JournalRecords.templateId(first));
            assertEquals("seq 1", 1, JournalRecords.journalSeq(first));
            assertEquals("FRESH", 0, first[8 + 38]);
            assertEquals("meEpoch = genesisId", s.genesis().genesisId(), JournalRecords.meEpoch(first));
            long seq = 0;
            long egress = Long.MIN_VALUE;
            for (final FeedFiles.Entry e : s.journal()) {
                if (!e.isFlush()) {
                    assertEquals("dense journalSeq", ++seq, JournalRecords.journalSeq(e.bytes()));
                    assertTrue("egressSeq non-decreasing", JournalRecords.egressSeq(e.bytes()) >= egress);
                    egress = JournalRecords.egressSeq(e.bytes());
                }
            }
            int bumps = 0;
            for (final List<byte[]> a : s.answers()) {
                for (final byte[] r : a) {
                    if (JournalRecords.templateId(r) == 5 && r[8 + 38] == 3) {
                        bumps++;
                    }
                }
            }
            assertTrue("at most one BUMP", bumps <= 1);
        }
    }

    @Test
    public void storyIsDeterministic() throws Exception {
        final Path a = tmp.newFolder("a").toPath();
        final Path b = tmp.newFolder("b").toPath();
        FeedStory.write(FeedStory.build(3, ROUNDS), a);
        FeedStory.write(FeedStory.build(3, ROUNDS), b);
        assertArrayEquals(Files.readAllBytes(a.resolve("SHA256SUMS")), Files.readAllBytes(b.resolve("SHA256SUMS")));
        // and the CLI writes the same bytes
        final Path c = tmp.newFolder("c").toPath();
        assertEquals(0, FeedHarnessCliTest.cli("story", "3", Integer.toString(ROUNDS), c.toString()).code());
        assertArrayEquals(Files.readAllBytes(a.resolve("SHA256SUMS")), Files.readAllBytes(c.resolve("SHA256SUMS")));
        final FeedStory.Story read = FeedStory.read(c);
        assertEquals(FeedStory.build(3, ROUNDS).expectedJson(), read.expectedJson());
    }

    /** The coverage check as the negative control's target (a builder with one coverage item removed must refuse). */
    public static void coverageHolds() {
        FeedStory.build(5, ROUNDS);
    }

    @Test
    public void reverseThenTranslateIsIdentity() {
        for (long seed = 1; seed <= 8; seed++) {
            reverseIdentity(story(seed), "seed " + seed);
        }
    }

    /** translate(reverse(f)) == f for every ME frame of the story and every translated frame (62 included). */
    public static void reverseIdentity(final FeedStory.Story s, final String what) {
        final MeJournalReverseEncoder rev = new MeJournalReverseEncoder();
        final List<byte[]> frames = new ArrayList<>(s.meFrames());
        for (int r = 0; r < s.rounds(); r++) {
            frames.addAll(s.frames(r));
        }
        int n62 = 0;
        for (final byte[] f : frames) {
            final List<FeedFiles.Entry> in = new ArrayList<>();
            for (final byte[] rec : rev.reverse(f)) {
                in.add(FeedFiles.Entry.record(rec));
            }
            final List<byte[]> back = MeJournalReferenceTranslator.translate(in);
            assertEquals(what + ": one frame back", 1, back.size());
            assertArrayEquals(what + ": translate(reverse(frame)) == frame, template " + FeedDigests.templateOf(f), f, back.get(0));
            n62 += FeedDigests.templateOf(f) == 62 ? 1 : 0;
        }
        assertTrue(what + ": a 62 was reversed", n62 > 0);
    }

    @Test
    public void everyStoryFrameDecodesThroughTheS7Demuxer() {
        for (long seed = 1; seed <= 8; seed++) {
            decodesAndDigests(story(seed), "seed " + seed);
        }
    }

    /**
     * 5.1 and 5.2 a on a coherent story: the clean run through the production demuxer drops nothing, the cursor reaches N
     * with no latch, and after each frame the digest the ENGINE stored for every applied seq equals the production 3.8
     * digest of the INPUT record read with match-common's decoders (a 62 per entry).
     */
    public static void decodesAndDigests(final FeedStory.Story s, final String what) {
        final CoreApplyRunner r = FeedStory.bootClean(s.genesis());
        final long before = TranslatorProofs.drops(r);
        for (int round = 0; round < s.rounds(); round++) {
            FeedStory.applyBlock(r, s, round);
            int k = 0;
            final List<byte[]> records = s.answers().get(round);
            for (final byte[] f : s.frames(round)) {
                r.apply(CoreApplyRunner.BRIDGE_SESSION, f, FeedStory.feedTime(s.genesis().t0(), round));
                assertEquals(what + " round " + round + ": no demuxer drop", before, TranslatorProofs.drops(r));
                final int entries = FeedDigests.templateOf(f) == 62 ? TranslatorProofs.u16(f, 10) : 1;
                for (int i = 0; i < entries; i++) {
                    final byte[] rec = records.get(k++);
                    final long seq = JournalRecords.journalSeq(rec);
                    assertTrue(what + ": the core holds the digest of seq " + seq + " (cursor "
                            + r.engine().feed().lastAppliedJournalSeq() + ", latch " + r.engine().feed().latchReason() + ")",
                            r.engine().feed().digestHeld(seq));
                    assertEquals(what + ": the core's digest of seq " + seq + " equals the source digest",
                            FeedDigests.productionOfRecord(rec), r.engine().feed().digestAt(seq));
                }
            }
        }
        assertEquals(what + ": cursor = N", s.length(), r.engine().feed().lastAppliedJournalSeq());
        assertEquals(what + ": no latch", "NONE", r.engine().feed().latchReason().name());
    }

    /** Evidence: what the eight stories contain (records per template, the core's rejects, triggered stops, slices). */
    @Test
    public void storyStatistics() {
        for (long seed = 1; seed <= 8; seed++) {
            final FeedStory.Story s = story(seed);
            final int[] t = new int[6];
            for (final List<byte[]> a : s.answers()) {
                for (final byte[] r : a) {
                    t[JournalRecords.templateId(r)]++;
                }
            }
            int f62 = 0;
            for (int r = 0; r < s.rounds(); r++) {
                for (final byte[] f : s.frames(r)) {
                    f62 += FeedDigests.templateOf(f) == 62 ? 1 : 0;
                }
            }
            final CoreApplyRunner c = FeedStory.cleanRun(s);
            final java.util.Map<String, Long> ev = new java.util.TreeMap<>();
            for (final var e : c.sink().events()) {
                final String k = e.getClass().getSimpleName()
                        + (e instanceof com.openexchange.assets.determinism.AssetsEngineEvent.OrderReject o ? ":" + o.reason() : "")
                        + (e instanceof com.openexchange.assets.determinism.AssetsEngineEvent.OrderEvent o ? ":" + o.kind() : "")
                        + (e instanceof com.openexchange.assets.determinism.AssetsEngineEvent.OrderFault o ? ":" + o.kind() : "");
                ev.merge(k, 1L, Long::sum);
            }
            System.out.println("story " + seed + ": N=" + s.length() + " trades=" + t[1] + " terminals=" + t[2] + " outcomes=" + t[3]
                    + " releases=" + t[4] + " configs=" + t[5] + " 62s=" + f62 + " triggered=" + c.engine().orders().triggeredCount()
                    + " slices=" + c.engine().orders().sliceCount() + " events=" + ev);
            assertTrue("a story trades", t[1] >= 5);
            assertTrue("a story releases", t[4] >= 1);
        }
    }

    /** Review NOTE-9: {@code apply} replays a story from its own files (boot prefix + replay log) to expected.json. */
    @Test
    public void applyReplaysAStoryToItsExpectedResult() throws Exception {
        final Path d = tmp.newFolder("apply").toPath();
        final FeedStory.Story s = story(6);
        FeedStory.write(s, d);
        final FeedHarnessCliTest.Run r = FeedHarnessCliTest.cli("apply", d.resolve("blocks/boot.flog").toString(),
                d.resolve("replay.flog").toString(), d.resolve("out.json").toString());
        assertEquals(r.err(), 0, r.code());
        assertEquals("apply of the story's files is the clean run", s.expectedJson(), Files.readString(d.resolve("out.json")).trim());
    }
}

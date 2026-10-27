// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.feedharness.CoreServe;
import com.openexchange.assets.feedharness.FeedDigests;
import com.openexchange.assets.feedharness.FeedFiles;
import com.openexchange.assets.feedharness.FeedHarnessSeams;
import com.openexchange.assets.feedharness.FeedStory;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S9h 11.3: {@link CoreApplyRunner} and {@code serve}. The runner reproduces the production image of a re-homed v3
 * {@link CoreFrameStory} (D-S9h-5); BRIDGE frames need a registered session; a clean story run equals the builder's
 * expected Result; duplicates and redelivery inside a round keep the snapshot hash (the 6.3 time rule); a feedless round
 * breaks it (why the story rule exists, FIX-4); a bridge restart reuses the pinned session (FIX-5); a lost frame latches
 * GAP and heals on redelivery; and the serve protocol, byte by byte.
 */
public class CoreApplyRunnerTest {

    static final int ROUNDS = 12;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @After
    public void seamsOff() {
        FeedHarnessSeams.clear();
    }

    private static final Map<Long, Path> STORIES = new ConcurrentHashMap<>();
    private static final TemporaryFolder SHARED = new TemporaryFolder();

    public static synchronized Path storyDir(final long seed) throws Exception {
        if (STORIES.isEmpty()) {
            SHARED.create();
            Runtime.getRuntime().addShutdownHook(new Thread(SHARED::delete));
        }
        final Path cached = STORIES.get(seed);
        if (cached != null) {
            return cached;
        }
        final Path d = SHARED.newFolder("story-" + seed).toPath();
        FeedStory.write(FeedStory.build(seed, ROUNDS), d);
        STORIES.put(seed, d);
        return d;
    }

    // ------------------------------------------------------------------ a small serve client

    /** The in-process serve, request by request. */
    static final class Client {
        final CoreServe serve = new CoreServe();
        FeedStory.Story story;

        CoreServe.Reply call(final int op, final byte[] payload) {
            final CoreServe.Reply r = serve.handle(op, payload);
            if (r.op() == CoreServe.ERR) {
                throw new AssertionError("serve ERR: " + new String(r.payload(), StandardCharsets.UTF_8));
            }
            return r;
        }

        void boot(final Path dir) throws Exception {
            story = FeedStory.read(dir);
            call(CoreServe.BOOT, dir.toString().getBytes(StandardCharsets.UTF_8));
        }

        void round(final int r) {
            call(CoreServe.ROUND, ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(r).array());
        }

        CoreServe.Reply frame(final long session, final byte[] f) {
            return call(CoreServe.FRAME, ByteBuffer.allocate(8 + f.length).order(ByteOrder.LITTLE_ENDIAN).putLong(session).put(f)
                    .array());
        }

        CoreServe.Reply openBridge(final long session, final long identity) {
            return call(CoreServe.OPEN_BRIDGE, ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(session)
                    .putLong(identity).array());
        }

        String finish() {
            return new String(call(CoreServe.FINISH, new byte[0]).payload(), StandardCharsets.UTF_8);
        }
    }

    static String sha(final String json) {
        final int i = json.indexOf("\"snapshotSha256\":\"") + 18;
        return json.substring(i, i + 64);
    }

    static long counter(final String json, final String name) {
        final int i = json.indexOf("\"" + name + "\":") + name.length() + 3;
        int j = i;
        while (j < json.length() && (Character.isDigit(json.charAt(j)) || json.charAt(j) == '-')) {
            j++;
        }
        return Long.parseLong(json.substring(i, j));
    }

    static int entries(final byte[] f) {
        return FeedDigests.templateOf(f) == 62 ? (f[10] & 0xFF) | (f[11] & 0xFF) << 8 : 1;
    }

    // ------------------------------------------------------------------ D-S9h-5

    @Test
    public void reproducesTheV3CoreFrameStoryImage() {
        for (long seed = 1; seed <= 4; seed++) {
            reproduces(seed);
        }
    }

    /** The image of a re-homed v3 CoreFrameStory through the runner equals FrameReplica's of the same log. */
    public static void reproduces(final long seed) {
        final List<SessionedLog.LogStep> log = rehome(CoreFrameStory.build(seed, 600, true));
        final CoreApplyRunner runner = CoreApplyRunner.empty();
        final FrameReplica replica = new FrameReplica();
        for (final SessionedLog.LogStep s : log) {
            switch (s.kind()) {
                case OPEN -> {
                    runner.openSession(s.sessionId(), s.bytes());
                    replica.apply(new CoreFrameStory.Step(CoreFrameStory.Kind.OPEN_SESSION, s.bytes(), s.timestamp(), s.sessionId(),
                            s.logPosition()));
                }
                case MESSAGE -> {
                    runner.apply(s.sessionId(), s.bytes(), s.timestamp());
                    replica.apply(new CoreFrameStory.Step(CoreFrameStory.Kind.FRAME, s.bytes(), s.timestamp(), s.sessionId(),
                            s.logPosition()));
                }
                case TIMER -> {
                    runner.sweep(s.timestamp());
                    replica.apply(new CoreFrameStory.Step(CoreFrameStory.Kind.SWEEP, new byte[0], s.timestamp(),
                            CoreFrameStory.TRUSTED, s.logPosition()));
                }
                default -> throw new AssertionError("step " + s.kind());
            }
            assertEquals("seed " + seed + ": the runner's log position follows the SessionedLog rule", s.logPosition(),
                    runner.position());
        }
        assertTrue("seed " + seed + ": the story bound the epoch", runner.engine().feed().boundMeEpoch() == CoreFrameStory.GENESIS);
        assertEquals("seed " + seed + ": snapshotSha256 equals FrameReplica.image() of the same story",
                FeedFiles.sha256Hex(replica.image()), runner.result().snapshotSha256());
    }

    /**
     * {@link SessionedLog#from} for a v3 story: the same re-homing (one production session per class, the SessionedLog
     * prefix and position rule), with the v7 feed templates SessionedLog predates homed as the demuxer's class mask says
     * (66, 67, 68 BRIDGE; 76 ADMIN).
     */
    static List<SessionedLog.LogStep> rehome(final List<CoreFrameStory.Step> story) {
        final List<CoreFrameStory.Step> mapped = new ArrayList<>();
        for (final CoreFrameStory.Step s : story) {
            final int t = s.templateId();
            if (s.kind() == CoreFrameStory.Kind.FRAME && s.sessionId() == CoreFrameStory.TRUSTED && (t == 66 || t == 67 || t == 68
                    || t == 76)) {
                final SessionClass c = t == 76 ? SessionClass.ADMIN : SessionClass.BRIDGE;
                mapped.add(new CoreFrameStory.Step(s.kind(), s.bytes(), s.timestamp(), SessionedLog.sessionOf(c), s.logPosition()));
            } else {
                mapped.add(s);
            }
        }
        return SessionedLog.from(mapped);
    }

    @Test
    public void bridgeFramesNeedRegistration() {
        bridgeNeedsRegistration();
    }

    public static void bridgeNeedsRegistration() {
        final CoreApplyRunner r = CoreApplyRunner.boot(new CoreApplyRunner.Genesis(77L, 0, 0, FeedStory.T0));
        r.openBridge(CoreApplyRunner.BRIDGE_SESSION, CoreApplyRunner.BRIDGE_IDENTITY, FeedStory.T0);
        final com.openexchange.assets.feedharness.CoreFrames enc = new com.openexchange.assets.feedharness.CoreFrames();
        r.apply(CoreApplyRunner.BRIDGE_SESSION, enc.engineConfig(1, 1, 77L, 1, 2, 4096, 12, 0), FeedStory.T0 + 1);
        assertEquals("bound", 77L, r.engine().feed().boundMeEpoch());
        r.openSession(702L, SessionClass.BRIDGE, 3L, FeedStory.T0);
        final long before = r.engine().getUnauthorisedCount();
        r.apply(702L, enc.trade(2, 2, 77L, 1, 1, 10, 1, 11, 2, 100, 1, true, 1, 1, 2), FeedStory.T0 + 2);
        assertEquals("a 61 from an unregistered BRIDGE session is refused UNREGISTERED_SESSION", before + 1,
                r.engine().getUnauthorisedCount());
        final boolean named = r.sink().events().stream().anyMatch(e -> e.render().contains("UNREGISTERED_SESSION"));
        assertTrue("the refusal names UNREGISTERED_SESSION", named);
        assertEquals("no apply: the cursor stays at the FRESH", 1L, r.engine().feed().lastAppliedJournalSeq());
    }

    // ------------------------------------------------------------------ clean runs

    @Test
    public void cleanStoryOnceEqualsModel() throws Exception {
        for (long seed = 1; seed <= 8; seed++) {
            final Path dir = storyDir(seed);
            final Client c = new Client();
            c.boot(dir);
            for (int r = 0; r < c.story.rounds(); r++) {
                c.round(r);
                for (final byte[] f : c.story.frames(r)) {
                    c.frame(CoreApplyRunner.BRIDGE_SESSION, f);
                }
            }
            assertEquals("seed " + seed + ": the serve Result equals expected.json", c.story.expectedJson(), c.finish());
            // and through the offline path: the same story applied by FeedStory.cleanRun from the files read back
            assertEquals("seed " + seed + ": cleanRun of the read story", c.story.expectedJson(),
                    FeedStory.cleanRun(FeedStory.read(dir)).result().toJson());
        }
    }

    @Test
    public void duplicatesAndRedeliveryKeepTheSnapshot() throws Exception {
        for (final long seed : new long[] {1, 4, 7}) {
            duplicatesKeepTheSnapshot(storyDir(seed), "seed " + seed);
        }
    }

    /** Every frame sent twice and a prefix of every round re-sent: same hash, every extra entry a counted duplicate. */
    public static void duplicatesKeepTheSnapshot(final Path dir, final String what) throws Exception {
        final Client c = new Client();
        c.boot(dir);
        long extra = 0;
        for (int r = 0; r < c.story.rounds(); r++) {
            c.round(r);
            final List<byte[]> frames = c.story.frames(r);
            for (final byte[] f : frames) {
                c.frame(CoreApplyRunner.BRIDGE_SESSION, f);
                c.frame(CoreApplyRunner.BRIDGE_SESSION, f);
                extra += entries(f);
            }
            for (int i = 0; i < (frames.size() + 1) / 2; i++) {
                c.frame(CoreApplyRunner.BRIDGE_SESSION, frames.get(i));
                extra += entries(frames.get(i));
            }
        }
        final String json = c.finish();
        assertEquals(what + ": no latch", "NONE", json.replaceAll(".*\"latch\":\\{\"reason\":\"([A-Z_]+)\".*", "$1"));
        long dups = 0;
        for (final int t : new int[] {61, 62, 63, 64, 67, 68}) {
            dups += counter(json, "feedDuplicateCount" + t);
        }
        assertEquals(what + ": feedDuplicateCount = the extra entries", extra, dups);
        assertEquals(what + ": the snapshot hash of the duplicated run equals the clean run's", c.story.expectedSha256(), sha(json));
    }

    @Test
    public void crossRoundRedeliveryIntoAFeedlessRoundBreaksTheHash() throws Exception {
        final Path dir = tmp.newFolder("feedless").toPath();
        FeedHarnessSeams.set(FeedHarnessSeams.STORY_FEEDLESS_ROUND, true);
        final FeedStory.Story s;
        try {
            s = FeedStory.build(9, ROUNDS);
        } finally {
            FeedHarnessSeams.set(FeedHarnessSeams.STORY_FEEDLESS_ROUND, false);
        }
        FeedStory.write(s, dir);
        final int k = ROUNDS - 1; // the last round: lastLogTime is a monotone max, a later fed round would hide it
        assertTrue("round " + k + " carries no feed record", s.answers().get(k).isEmpty());
        final Client clean = new Client();
        clean.boot(dir);
        final Client redelivered = new Client();
        redelivered.boot(dir);
        for (int r = 0; r < s.rounds(); r++) {
            for (final Client c : List.of(clean, redelivered)) {
                c.round(r);
                for (final byte[] f : s.frames(r)) {
                    c.frame(CoreApplyRunner.BRIDGE_SESSION, f);
                }
            }
            if (r == k) {
                final List<byte[]> prev = s.frames(k - 1);
                redelivered.frame(CoreApplyRunner.BRIDGE_SESSION, prev.get(prev.size() - 1)); // a duplicate at round k's time
            }
        }
        final String a = clean.finish();
        final String b = redelivered.finish();
        assertEquals("the clean run is the builder's", s.expectedJson(), a);
        assertEquals("one duplicate", 1, counterSum(b) - counterSum(a));
        assertNotEquals("a duplicate in a feedless round raises lastLogTime", counter(a, "lastLogTime"), counter(b, "lastLogTime"));
        assertNotEquals("so the hash differs for a reason that is not money (why FIX-4's story rule exists)", sha(a), sha(b));
    }

    static long counterSum(final String json) {
        long n = 0;
        for (final int t : new int[] {61, 62, 63, 64, 67, 68}) {
            n += counter(json, "feedDuplicateCount" + t);
        }
        return n;
    }

    @Test
    public void bridgeRestartReusesThePinnedSession() throws Exception {
        restartKeepsTheHash(storyDir(2), "seed 2");
    }

    /** A restart re-registers 701 mid-run (DUPLICATE, hash unchanged); a new session 702 changes section 9. */
    public static void restartKeepsTheHash(final Path dir, final String what) throws Exception {
        final Client same = new Client();
        same.boot(dir);
        final Client other = new Client();
        other.boot(dir);
        for (int r = 0; r < same.story.rounds(); r++) {
            for (final Client c : List.of(same, other)) {
                c.round(r);
                final List<byte[]> frames = c.story.frames(r);
                for (int i = 0; i < frames.size(); i++) {
                    if (r == 3 && i == frames.size() / 2) {
                        final CoreServe.Reply ack = c == same
                                ? c.openBridge(CoreApplyRunner.BRIDGE_SESSION, CoreApplyRunner.BRIDGE_IDENTITY)
                                : c.openBridge(702L, 0xB2L);
                        if (c == same) {
                            final byte[] f = java.util.Arrays.copyOfRange(ack.payload(), 2, ack.payload().length);
                            assertEquals(what + ": the second RegisterSession on 701 answers DUPLICATE",
                                    com.openexchange.assets.infrastructure.generated.AdminResult.DUPLICATE.value(),
                                    f[8 + 10]);
                        }
                    }
                    c.frame(c == other && r > 3 || (c == other && r == 3 && i >= frames.size() / 2) ? 702L
                            : CoreApplyRunner.BRIDGE_SESSION, frames.get(i));
                }
            }
        }
        final String a = same.finish();
        assertEquals(what + ": a restart on the pinned session keeps the snapshot hash", same.story.expectedSha256(), sha(a));
        assertNotEquals(what + ": a new BRIDGE session 702 changes section 9 (and the hash)", same.story.expectedSha256(),
                sha(other.finish()));
    }

    @Test
    public void lostFrameLatchesGapAndHealsOnRedelivery() throws Exception {
        final Path dir = storyDir(3);
        final Client c = new Client();
        c.boot(dir);
        final int last = c.story.rounds() - 1;
        for (int r = 0; r < last; r++) {
            c.round(r);
            for (final byte[] f : c.story.frames(r)) {
                c.frame(CoreApplyRunner.BRIDGE_SESSION, f);
            }
        }
        c.round(last);
        final List<byte[]> frames = c.story.frames(last);
        assertTrue("the last round has three frames or more: " + frames.size(), frames.size() >= 3);
        c.frame(CoreApplyRunner.BRIDGE_SESSION, frames.get(0));
        final CoreServe.Reply refused = c.frame(CoreApplyRunner.BRIDGE_SESSION, frames.get(2)); // frame 1 lost
        final var feed = c.serve.runner().engine().feed();
        assertEquals("GAP latched", "GAP", feed.latchReason().name());
        assertTrue("the gate carries BRIDGE_HALT", (c.serve.runner().engine().gate()
                .reasons(com.openexchange.assets.application.engine.gate.OrderEntryGate.GLOBAL)
                & (1 << com.openexchange.assets.domain.GateReason.BRIDGE_HALT.code())) != 0);
        // review NOTE-10: the refused entry (whatever its template) is reported: one SettleRejected FEED_GAP naming c + 1
        final ByteBuffer rb = ByteBuffer.wrap(refused.payload()).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals("one ORIGIN event for the frame behind the hole", 1, rb.getInt());
        final byte[] rf = new byte[rb.getShort() & 0xFFFF];
        rb.get(rf);
        assertEquals("SettleRejected", 114, FeedDigests.templateOf(rf));
        final var rd = new com.openexchange.assets.infrastructure.generated.SettleRejectedDecoder()
                .wrapAndApplyHeader(new org.agrona.concurrent.UnsafeBuffer(rf), 0,
                        new com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder());
        assertEquals("FEED_GAP", com.openexchange.assets.infrastructure.generated.SettleRejectReason.FEED_GAP, rd.reason());
        assertEquals("detail = the expected seq (the hole)", feed.lastAppliedJournalSeq() + 1, rd.detail());
        for (int i = 1; i < frames.size(); i++) {
            c.frame(CoreApplyRunner.BRIDGE_SESSION, frames.get(i)); // redelivery from the hole on
        }
        assertEquals("reach-clear", "NONE", feed.latchReason().name());
        assertEquals("cursor = N", c.story.length(), feed.lastAppliedJournalSeq());
    }

    // ------------------------------------------------------------------ the wire

    static byte[] req(final int op, final byte[] payload) {
        return CoreServe.request(op, payload);
    }

    static byte[] cat(final List<byte[]> parts) {
        final ByteArrayOutputStream o = new ByteArrayOutputStream();
        parts.forEach(o::writeBytes);
        return o.toByteArray();
    }

    /** Runs the stdio loop over {@code requests}; returns {exit code, replies}. */
    static Object[] wire(final byte[] requests) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final int code = new CoreServe().loop(new ByteArrayInputStream(requests), out);
        final List<byte[]> replies = new ArrayList<>();
        final ByteBuffer b = ByteBuffer.wrap(out.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
        while (b.hasRemaining()) {
            final byte[] r = new byte[b.getInt()];
            b.get(r);
            replies.add(r);
        }
        return new Object[] {code, replies};
    }

    static byte[] le(final long... v) {
        final ByteBuffer b = ByteBuffer.allocate(8 * v.length).order(ByteOrder.LITTLE_ENDIAN);
        for (final long x : v) {
            b.putLong(x);
        }
        return b.array();
    }

    static byte[] u32(final int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    static byte[] frameReq(final byte[] f) {
        return req(CoreServe.FRAME, ByteBuffer.allocate(8 + f.length).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(CoreApplyRunner.BRIDGE_SESSION).put(f).array());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void serveProtocol() throws Exception {
        final Path dir = storyDir(5);
        final FeedStory.Story s = FeedStory.read(dir);
        final byte[] boot = req(CoreServe.BOOT, dir.toString().getBytes(StandardCharsets.UTF_8));

        // H3: a FRAME before BOOT
        Object[] w = wire(frameReq(s.frames(0).get(0)));
        assertEquals("FRAME before BOOT exits 65", 65, w[0]);
        assertEquals("ERR", 0xFF, ((List<byte[]>) w[1]).get(0)[0] & 0xFF);
        // H3: ROUND out of order
        w = wire(cat(List.of(boot, req(CoreServe.ROUND, u32(1)))));
        assertEquals("ROUND 1 first exits 65", 65, w[0]);
        // H3: barrier unmet (round 1 applied without its frames, then ROUND 2)
        final List<byte[]> unmet = new ArrayList<>(List.of(boot, req(CoreServe.ROUND, u32(0))));
        s.frames(0).forEach(f -> unmet.add(frameReq(f)));
        unmet.add(req(CoreServe.ROUND, u32(1)));
        unmet.add(req(CoreServe.ROUND, u32(2)));
        w = wire(cat(unmet));
        assertEquals("barrier unmet exits 65", 65, w[0]);
        final List<byte[]> ur = (List<byte[]>) w[1];
        assertTrue(new String(ur.get(ur.size() - 1), StandardCharsets.UTF_8).contains("barrier"));
        // malformed: a QUERY with 3 payload bytes
        w = wire(cat(List.of(boot, req(CoreServe.QUERY, new byte[3]))));
        assertEquals("malformed exits 65", 65, w[0]);

        // the happy path, every op
        final List<byte[]> all = new ArrayList<>(List.of(boot));
        for (int r = 0; r < s.rounds(); r++) {
            all.add(req(CoreServe.ROUND, u32(r)));
            for (final byte[] f : s.frames(r)) {
                all.add(frameReq(f));
            }
            all.add(req(CoreServe.QUERY, le(CoreApplyRunner.BRIDGE_SESSION, 1000 + r)));
        }
        all.add(req(CoreServe.FINISH, new byte[0]));
        all.add(req(CoreServe.QUIT, new byte[0]));
        w = wire(cat(all));
        assertEquals("QUIT exits 0", 0, w[0]);
        final List<byte[]> replies = (List<byte[]>) w[1];
        final ByteBuffer ready = ByteBuffer.wrap(replies.get(0)).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals("READY", CoreServe.BOOT | 0x80, ready.get() & 0xFF);
        assertEquals("N", s.length(), ready.getLong());
        assertEquals("rounds", s.rounds(), ready.getInt());
        for (final long b : s.barriers()) {
            assertEquals("barrier", b, ready.getLong());
        }
        final byte[] sha = new byte[64];
        ready.get(sha);
        assertEquals("the clean-run hash", s.expectedSha256(), new String(sha, StandardCharsets.US_ASCII));
        assertEquals("pinned session", CoreApplyRunner.BRIDGE_SESSION, ready.getLong());
        assertEquals("identity", CoreApplyRunner.BRIDGE_IDENTITY, ready.getLong());
        assertEquals("class BRIDGE", SessionClass.BRIDGE.value(), ready.get());
        ready.get(); // channel mask
        assertEquals("resumable", 1, ready.get());
        assertEquals("sourceEpoch 0", 0L, ready.getLong());
        assertEquals("genesisId", s.genesis().genesisId(), ready.getLong());
        assertEquals("t0", s.genesis().t0(), ready.getLong());
        int at = 1;
        for (int r = 0; r < s.rounds(); r++) {
            assertEquals("ROUND_DONE", CoreServe.ROUND | 0x80, replies.get(at++)[0] & 0xFF);
            for (int i = 0; i < s.frames(r).size(); i++) {
                final byte[] e = replies.get(at++);
                assertEquals("EGRESS", CoreServe.FRAME | 0x80, e[0] & 0xFF);
                assertEquals("a clean frame produces no ORIGIN event", 0,
                        ByteBuffer.wrap(e, 1, 4).order(ByteOrder.LITTLE_ENDIAN).getInt());
            }
            final ByteBuffer rep = ByteBuffer.wrap(replies.get(at++), 1, 1 + 8 * 3).order(ByteOrder.LITTLE_ENDIAN);
            assertEquals("REPORT corr", 1000 + r, rep.getLong());
            assertEquals("REPORT epoch", s.genesis().genesisId(), rep.getLong());
            assertEquals("a QUERY is answered after everything before it", s.barriers()[r], rep.getLong());
        }
        final byte[] result = replies.get(at++);
        assertEquals("RESULT", CoreServe.FINISH | 0x80, result[0] & 0xFF);
        assertEquals(s.expectedJson(), new String(result, 1, result.length - 1, StandardCharsets.UTF_8));
        assertEquals("BYE", CoreServe.QUIT | 0x80, replies.get(at)[0] & 0xFF);

        // a refused frame's ORIGIN egress: a trade two seqs ahead is a GAP with a SettleRejected FEED_GAP
        final Client c = new Client();
        c.boot(dir);
        c.round(0);
        c.frame(CoreApplyRunner.BRIDGE_SESSION, s.frames(0).get(0));
        final com.openexchange.assets.feedharness.CoreFrames enc = new com.openexchange.assets.feedharness.CoreFrames();
        final CoreServe.Reply e = c.frame(CoreApplyRunner.BRIDGE_SESSION, enc.trade(9, 3, s.genesis().genesisId(), 1, 1, 10, 1,
                11, 2, 100, 1, true, 1, 1, 2));
        final ByteBuffer eb = ByteBuffer.wrap(e.payload()).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals("one ORIGIN event", 1, eb.getInt());
        final int len = eb.getShort() & 0xFFFF;
        final byte[] f = new byte[len];
        eb.get(f);
        assertEquals("SettleRejected", 114, FeedDigests.templateOf(f));
        final var d = new com.openexchange.assets.infrastructure.generated.SettleRejectedDecoder()
                .wrapAndApplyHeader(new org.agrona.concurrent.UnsafeBuffer(f), 0,
                        new com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder());
        assertEquals("FEED_GAP", com.openexchange.assets.infrastructure.generated.SettleRejectReason.FEED_GAP, d.reason());
        assertEquals("detail = the expected seq", 2L, d.detail());
        // determinism (review NOTE-10): a second serve, the same requests, the same egress bytes
        final Client c2 = new Client();
        c2.boot(dir);
        c2.round(0);
        c2.frame(CoreApplyRunner.BRIDGE_SESSION, s.frames(0).get(0));
        assertArrayEquals("deterministic egress", e.payload(), c2.frame(CoreApplyRunner.BRIDGE_SESSION, enc.trade(9, 3,
                s.genesis().genesisId(), 1, 1, 10, 1, 11, 2, 100, 1, true, 1, 1, 2)).payload());
    }

    // ------------------------------------------------------------------ review FIX-1 (H4) and FIX-4 (DUMP, ADMIN_OPEN)

    /**
     * H4 with no production seam (the review's probe): an event sink that throws IllegalArgumentException inside the
     * core's apply latches the apply fence; serve must answer RESULT with applyFailed = true and exit 70, never ERR 65.
     */
    public static void h4(final Path dir) throws Exception {
        final CoreServe s = new CoreServe();
        s.handle(CoreServe.BOOT, dir.toString().getBytes(StandardCharsets.UTF_8));
        final var real = s.runner().sink();
        final com.openexchange.assets.domain.AssetsEventSink throwing = (com.openexchange.assets.domain.AssetsEventSink)
                java.lang.reflect.Proxy.newProxyInstance(com.openexchange.assets.domain.AssetsEventSink.class.getClassLoader(),
                        new Class<?>[] {com.openexchange.assets.domain.AssetsEventSink.class}, (p, m, a) -> {
                            if (m.getName().equals("onBalanceUpdate")) {
                                throw new IllegalArgumentException("H4 probe: a core-side IllegalArgumentException");
                            }
                            try {
                                return m.invoke(real, a);
                            } catch (java.lang.reflect.InvocationTargetException x) {
                                throw x.getCause();
                            }
                        });
        s.runner().engine().setEventSink(throwing);
        final CoreServe.Reply r = s.handle(CoreServe.ROUND, ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).array());
        assertTrue("the apply fence latched", s.runner().applyFailed());
        assertEquals("H4: RESULT, not ERR (" + new String(r.payload(), StandardCharsets.UTF_8) + ")", CoreServe.FINISH | 0x80,
                r.op());
        assertEquals("H4: exit 70", 70, r.exit());
        assertTrue("applyFailed in the RESULT", new String(r.payload(), StandardCharsets.UTF_8).contains("\"applyFailed\":true"));
        // and the stdio loop exits 70 with that reply
        final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        final CoreServe loop = new CoreServe();
        loop.handle(CoreServe.BOOT, dir.toString().getBytes(StandardCharsets.UTF_8));
        loop.runner().engine().setEventSink(throwing);
        assertEquals("the loop's exit", 70, loop.loop(new ByteArrayInputStream(CoreServe.request(CoreServe.ROUND,
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).array())), out));
    }

    /** The {@code L halted reason} line of a DUMP reply (the latch is reported outside the money hash, asserted here). */
    static String latchLine(final CoreServe.Reply d) {
        final String text = new String(d.payload(), 132, d.payload().length - 132, StandardCharsets.UTF_8);
        final String[] lines = text.split("\n");
        final String last = lines[lines.length - 1];
        assertTrue("the dump ends with the latch line: " + last, last.startsWith("L "));
        return last;
    }

    @Test
    public void anApplyFaultIsH4NotAMalformedRequest() throws Exception {
        h4(storyDir(2));
    }

    /**
     * Review FIX-4 (S9b 13.17's hole class): a frame lost inside a round latches GAP; DUMP at that point equals the clean
     * prefix up to e - 1; the redelivery reach-clears, ADMIN_OPEN clears BRIDGE_HALT, and the run ends equal to the clean run
     * (hash and dump). Without the ADMIN_OPEN the gate bit stays and the hash differs (the review's probe).
     */
    @Test
    public void aHealedHoleWithAdminOpenEqualsTheCleanPrefixAndTheCleanRun() throws Exception {
        final Path dir = storyDir(3);
        final FeedStory.Story st = FeedStory.read(dir);
        int k = -1;
        for (int r = 1; r < st.rounds() && k < 0; r++) {
            if (st.frames(r).size() >= 3) {
                k = r;
            }
        }
        assertTrue("a round with three frames", k > 0);
        for (final boolean open : new boolean[] {true, false}) {
            final Client c = new Client();
            c.boot(dir);
            for (int r = 0; r < st.rounds(); r++) {
                c.round(r);
                final List<byte[]> f = st.frames(r);
                if (r != k) {
                    f.forEach(x -> c.frame(CoreApplyRunner.BRIDGE_SESSION, x));
                    continue;
                }
                c.frame(CoreApplyRunner.BRIDGE_SESSION, f.get(0));
                c.frame(CoreApplyRunner.BRIDGE_SESSION, f.get(2)); // f1 lost: GAP at e
                final long e = st.barriers()[k - 1] + entries(f.get(0)) + 1;
                assertEquals("GAP at e", "GAP", c.serve.runner().engine().feed().latchReason().name());
                final CoreServe.Reply d = c.call(CoreServe.DUMP, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(e - 1).array());
                final String live = new String(d.payload(), 0, 64, StandardCharsets.US_ASCII);
                final String model = new String(d.payload(), 64, 64, StandardCharsets.US_ASCII);
                assertEquals("the money state equals the clean prefix up to e - 1", model, live);
                assertEquals("the latch is checked explicitly, never silently outside the hash: halted, GAP (1)",
                        "L true 1", latchLine(d));
                for (int i = 1; i < f.size(); i++) {
                    c.frame(CoreApplyRunner.BRIDGE_SESSION, f.get(i)); // the fill
                }
                assertEquals("reach-clear", "NONE", c.serve.runner().engine().feed().latchReason().name());
                if (open) {
                    final CoreServe.Reply ack = c.call(CoreServe.ADMIN_OPEN, new byte[0]);
                    assertEquals("ADMIN OPEN accepted", com.openexchange.assets.infrastructure.generated.AdminResult.ACCEPTED
                            .value(), ack.payload()[2 + 8 + 10]);
                }
            }
            final String json = c.finish();
            if (open) {
                assertEquals("healed and opened: the clean run's hash", st.expectedSha256(), sha(json));
                final CoreServe.Reply d = c.call(CoreServe.DUMP, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(st.length()).array());
                assertEquals("and the clean run's dump", new String(d.payload(), 64, 64, StandardCharsets.US_ASCII),
                        new String(d.payload(), 0, 64, StandardCharsets.US_ASCII));
                assertEquals("and the latch, explicitly: not halted", "L false 0", latchLine(d));
            } else {
                assertNotEquals("without ADMIN OPEN the BRIDGE_HALT bit stays in the hash", st.expectedSha256(), sha(json));
            }
        }
    }
}

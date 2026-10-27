// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * RC2 S9h 6.3: {@code serve}, the real behaviour-3 core over stdio (the E3 transport for S9b and S9c). Requests and
 * replies are length-prefixed binary messages {@code u32 length, u8 op, payload} (little-endian; {@code length} counts
 * the op byte and the payload). Entries apply strictly in request order; a QUERY is answered after everything before it.
 *
 * <pre>
 * op  request                               reply (op | 0x80, or ERR 0xFF with a UTF-8 message)
 * 1   BOOT        storyPath (UTF-8)          READY: i64 N, u32 rounds, rounds x i64 barrier, 64 B clean snapshotSha256 (hex),
 *                                                   i64 bridgeSession, i64 identity, u8 sessionClass, u8 channelMask,
 *                                                   u8 resumable, i64 sourceEpoch, i64 genesisId, i64 t0
 * 2   OPEN_BRIDGE i64 sessionId, i64 identity ACK: u16 length + the AdminAck (110) money v7 frame of the RegisterSession
 * 3   FRAME       i64 sessionId, frame bytes EGRESS: u32 count, then per ORIGIN event u16 length + its money v7 frame
 * 4   QUERY       i64 sessionId, i64 corr    REPORT: i64 corr, i64 meEpoch, i64 lastAppliedJournalSeq, u8 feedHalted,
 *                                                    u8 haltReason, i64 consumePosition, i64 lastAppliedTradeId,
 *                                                    u16 length + the FeedPositionReport (17) frame
 * 5   ROUND       u32 r                      ROUND_DONE: u32 r, u32 entries, i64 cursor (after checking cursor >= b(r - 1))
 * 6   FINISH      -                          RESULT: the 6.1 Result as UTF-8 JSON
 * 7   QUIT        -                          BYE (empty), then exit 0
 * 8   DUMP        [i64 upto]                 DUMP: 64 B live dump sha256, 64 B the clean model prefix's dump sha256 up to
 *                                                 {@code upto} (zeros without it), u32 n, the n live dump lines (UTF-8, \n)
 * 9   ADMIN_OPEN  [i32 market]               ACK: u16 length + the AdminAck (110) of OrderEntryGate OPEN (GLOBAL by default)
 * </pre>
 * The snapshot hash covers the gate bits: a healed GAP leaves BRIDGE_HALT set until an ADMIN OPEN (S7 3.3), so a run that
 * heals a hole compares by DUMP, or sends ADMIN_OPEN, before it compares hashes.
 *
 * <p>Times (6.3, review FIX-4): the entries of core block r carry the story's times; every FRAME and QUERY after
 * {@code ROUND r} (and before {@code ROUND r + 1}) carries {@code t0 + 1000 r + 500}; before ROUND 0, {@code t0}. Errors
 * (rule H3): a request out of order (FRAME before BOOT, a ROUND whose barrier is unmet, a ROUND not the next one) or a
 * malformed one answers ERR and the process exits 65; a core apply fault answers RESULT with {@code applyFailed = true}
 * and exits 70 (rule H4).</p>
 */
public final class CoreServe {

    public static final int BOOT = 1;
    public static final int OPEN_BRIDGE = 2;
    public static final int FRAME = 3;
    public static final int QUERY = 4;
    public static final int ROUND = 5;
    public static final int FINISH = 6;
    public static final int QUIT = 7;
    /** Review FIX-4: the canonical money dump of the live core, and the clean model prefix's sha when {@code upto} is given. */
    public static final int DUMP = 8;
    /** Review FIX-4: ADMIN OPEN (GLOBAL, or {@code i32 market}) on the ADMIN session at the round's time. */
    public static final int ADMIN_OPEN = 9;
    public static final int ERR = 0xFF;

    /** One reply; {@code exit >= 0} ends the loop with that status. */
    public record Reply(int op, byte[] payload, int exit) { }

    private FeedStory.Story story;
    private CoreApplyRunner runner;
    private int lastRound = -1;
    private long frameCount;
    /** RESTART_NEW_SESSION seam: the session a restart of 701 is moved to. */
    private long restartSession = 702;
    private long adminOpens;

    public CoreApplyRunner runner() {
        return runner;
    }

    private long now() {
        final long t0 = story.genesis().t0();
        if (lastRound < 0) {
            return t0;
        }
        final long t = FeedStory.feedTime(t0, lastRound);
        return FeedHarnessSeams.on(FeedHarnessSeams.ROUND_TIME_PER_FRAME) ? t + (++frameCount % 400) : t;
    }

    private static Reply err(final String message) {
        return new Reply(ERR, message.getBytes(StandardCharsets.UTF_8), FeedHarnessMain.REFUSED);
    }

    /** Handles one request (the in-process form of the stdio loop). */
    public Reply handle(final int op, final byte[] payload) {
        final ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        try {
            if (op != BOOT && op != QUIT && story == null) {
                return err("op " + op + " before BOOT");
            }
            switch (op) {
                case BOOT -> {
                    if (story != null) {
                        return err("BOOT twice");
                    }
                    story = FeedStory.read(Path.of(new String(payload, StandardCharsets.UTF_8)));
                    runner = FeedStory.bootClean(story.genesis());
                    final long[] barriers = story.barriers();
                    final ByteBuffer o = ByteBuffer.allocate(8 + 4 + 8 * barriers.length + 64 + 8 + 8 + 3 + 8 + 8 + 8)
                            .order(ByteOrder.LITTLE_ENDIAN);
                    o.putLong(story.length()).putInt(barriers.length);
                    for (final long x : barriers) {
                        o.putLong(x);
                    }
                    o.put(story.expectedSha256().getBytes(StandardCharsets.US_ASCII));
                    // the pinned session as the core registered it (review FIX-5)
                    final var a = runner.engine().sessions().get(CoreApplyRunner.BRIDGE_SESSION);
                    o.putLong(a.sessionId()).putLong(a.identity());
                    o.put((byte) a.sessionClass().value());
                    o.put((byte) a.channelMask());
                    o.put((byte) (a.isResumable() ? 1 : 0)).putLong(a.sourceEpoch()).putLong(story.genesis().genesisId())
                            .putLong(story.genesis().t0());
                    return new Reply(BOOT | 0x80, o.array(), -1);
                }
                case OPEN_BRIDGE -> {
                    long session = b.getLong();
                    final long identity = b.getLong();
                    trailing(b);
                    if (session == CoreApplyRunner.BRIDGE_SESSION && FeedHarnessSeams.on(FeedHarnessSeams.RESTART_NEW_SESSION)) {
                        session = restartSession++;
                    }
                    final CoreApplyRunner.Egress e = runner.openBridge(session, identity, now());
                    for (final CoreApplyRunner.OriginEvent ev : e.events()) {
                        if (ev.templateId() == 110) {
                            return new Reply(OPEN_BRIDGE | 0x80, framed(ev.frame()), -1);
                        }
                    }
                    return err("RegisterSession produced no AdminAck");
                }
                case FRAME -> {
                    final long session = b.getLong();
                    final byte[] frame = new byte[b.remaining()];
                    b.get(frame);
                    if (frame.length < 8) {
                        return err("FRAME shorter than a header");
                    }
                    final CoreApplyRunner.Egress e = runner.apply(session, frame, now());
                    final ByteArrayOutputStream o = new ByteArrayOutputStream();
                    u32(o, e.events().size());
                    for (final CoreApplyRunner.OriginEvent ev : e.events()) {
                        o.writeBytes(framed(ev.frame()));
                    }
                    return new Reply(FRAME | 0x80, o.toByteArray(), -1);
                }
                case QUERY -> {
                    final long session = b.getLong();
                    final long corr = b.getLong();
                    trailing(b);
                    final CoreApplyRunner.Report r = runner.query(session, corr, now());
                    final ByteBuffer o = ByteBuffer.allocate(8 + 8 + 8 + 1 + 1 + 8 + 8 + 2 + r.frame().length)
                            .order(ByteOrder.LITTLE_ENDIAN);
                    o.putLong(r.correlationId()).putLong(r.meEpoch()).putLong(r.lastAppliedJournalSeq())
                            .put((byte) (r.feedHalted() ? 1 : 0)).put((byte) r.haltReason()).putLong(r.consumePosition())
                            .putLong(r.lastAppliedTradeId()).putShort((short) r.frame().length).put(r.frame());
                    return new Reply(QUERY | 0x80, o.array(), -1);
                }
                case ROUND -> {
                    final long r = Integer.toUnsignedLong(b.getInt());
                    trailing(b);
                    if (r != lastRound + 1 || r >= story.rounds()) {
                        return err("ROUND " + r + " out of order (next is " + (lastRound + 1) + " of " + story.rounds() + ")");
                    }
                    final long cursor = runner.engine().feed().lastAppliedJournalSeq();
                    if (r >= 1 && cursor < story.barriers()[(int) r - 1]) {
                        return err("barrier: ROUND " + r + " needs cursor >= " + story.barriers()[(int) r - 1] + ", it is " + cursor);
                    }
                    FeedStory.applyBlock(runner, story, (int) r);
                    lastRound = (int) r;
                    frameCount = 0;
                    final ByteBuffer o = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                    o.putInt((int) r).putInt(story.blocks().get((int) r).size())
                            .putLong(runner.engine().feed().lastAppliedJournalSeq());
                    return new Reply(ROUND | 0x80, o.array(), -1);
                }
                case FINISH -> {
                    trailing(b);
                    return new Reply(FINISH | 0x80, runner.result().toJson().getBytes(StandardCharsets.UTF_8), -1);
                }
                case DUMP -> {
                    final long upto = b.remaining() >= 8 ? b.getLong() : -1;
                    trailing(b);
                    final java.util.List<String> live = CanonicalDump.of(runner, now());
                    final String model = upto < 0 ? "0".repeat(64)
                            : CanonicalDump.sha256(T1Core.model(story, upto, java.util.List.of()));
                    final byte[] text = String.join("\n", live).getBytes(StandardCharsets.UTF_8);
                    final ByteBuffer o = ByteBuffer.allocate(64 + 64 + 4 + text.length).order(ByteOrder.LITTLE_ENDIAN);
                    o.put(CanonicalDump.sha256(live).getBytes(StandardCharsets.US_ASCII)).put(model.getBytes(StandardCharsets.US_ASCII))
                            .putInt(live.size()).put(text);
                    return new Reply(DUMP | 0x80, o.array(), -1);
                }
                case ADMIN_OPEN -> {
                    final int market = b.remaining() >= 4 ? b.getInt() : 0;
                    trailing(b);
                    final long corr = 0xAD0000L + adminOpens++;
                    final CoreApplyRunner.Egress e = runner.apply(CoreApplyRunner.ADMIN_SESSION,
                            new CoreFrames().gate(corr, market, true), now());
                    for (final CoreApplyRunner.OriginEvent ev : e.events()) {
                        if (ev.templateId() == 110) {
                            return new Reply(ADMIN_OPEN | 0x80, framed(ev.frame()), -1);
                        }
                    }
                    return err("ADMIN OPEN produced no AdminAck");
                }
                case QUIT -> {
                    return new Reply(QUIT | 0x80, new byte[0], 0);
                }
                default -> {
                    return err("unknown op " + op);
                }
            }
        } catch (RuntimeException | Error e) {
            // H4 first (review FIX-1): a core-side fault during apply is never relabelled as a malformed request, whatever
            // its type (the demuxer's own rule: an IllegalArgumentException may come from inside the engine)
            if (runner != null && runner.applyFailed() && !FeedHarnessSeams.on(FeedHarnessSeams.H4_CLASSIFY_FIRST)) {
                return applyFault();
            }
            if (e instanceof java.nio.BufferUnderflowException || e instanceof IllegalArgumentException) {
                return err("malformed request op " + op + ": " + e);
            }
            if (e instanceof FeedFiles.Refused) {
                return err("story refused: " + e.getMessage());
            }
            if (runner != null && runner.applyFailed()) {
                return applyFault();
            }
            throw e;
        } catch (IOException e) {
            return err("story unreadable: " + e);
        }
    }

    /** H4: RESULT with {@code applyFailed = true}, exit 70. */
    private Reply applyFault() {
        return new Reply(FINISH | 0x80, runner.result().toJson().getBytes(StandardCharsets.UTF_8), FeedHarnessMain.INTERNAL);
    }

    private static void trailing(final ByteBuffer b) {
        if (b.hasRemaining()) {
            throw new IllegalArgumentException(b.remaining() + " trailing bytes");
        }
    }

    private static byte[] framed(final byte[] frame) {
        final ByteBuffer o = ByteBuffer.allocate(2 + frame.length).order(ByteOrder.LITTLE_ENDIAN);
        o.putShort((short) frame.length).put(frame);
        return o.array();
    }

    private static void u32(final ByteArrayOutputStream o, final long v) {
        for (int i = 0; i < 4; i++) {
            o.write((int) (v >>> (8 * i)) & 0xFF);
        }
    }

    /** The stdio loop: returns the exit status (0 after QUIT or at end of input, 65 after an ERR, 70 after H4). */
    public int loop(final InputStream in, final OutputStream out) throws IOException {
        final DataInputStream d = new DataInputStream(in);
        while (true) {
            final byte[] lenBytes = new byte[4];
            try {
                d.readFully(lenBytes);
            } catch (EOFException eof) {
                return FeedHarnessMain.OK;
            }
            final long length = Integer.toUnsignedLong(ByteBuffer.wrap(lenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt());
            final Reply r;
            if (length < 1 || length > 16 * 1024 * 1024) {
                r = err("request length " + length);
            } else {
                final byte[] body = new byte[(int) length];
                try {
                    d.readFully(body);
                } catch (EOFException eof) {
                    write(out, err("truncated request"));
                    return FeedHarnessMain.REFUSED;
                }
                final byte[] payload = new byte[body.length - 1];
                System.arraycopy(body, 1, payload, 0, payload.length);
                r = handle(body[0] & 0xFF, payload);
            }
            write(out, r);
            if (r.exit() >= 0) {
                return r.exit();
            }
        }
    }

    public static void write(final OutputStream out, final Reply r) throws IOException {
        final ByteBuffer h = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
        h.putInt(1 + r.payload().length).put((byte) r.op());
        out.write(h.array());
        out.write(r.payload());
        out.flush();
    }

    /** A request in the wire form (tests and clients). */
    public static byte[] request(final int op, final byte[] payload) {
        final ByteBuffer h = ByteBuffer.allocate(5 + payload.length).order(ByteOrder.LITTLE_ENDIAN);
        h.putInt(1 + payload.length).put((byte) op).put(payload);
        return h.array();
    }
}

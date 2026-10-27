// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RC2 S9h 7.3: {@code t1-core <command>}, the one-shot core client of the T1 harness (the {@code T1CoreDriver} pattern):
 * TOOL / ADMIN / GATEWAY / TREASURY / FORWARDER sessions of a real AE cluster, credentials {@code keyId:CLASS:secret} from
 * {@code T1_ADMIN_KEY}, {@code T1_TOOL_KEY}, {@code T1_GATEWAY_KEY}, {@code T1_TREASURY_KEY}, {@code T1_FORWARDER_KEY}
 * (and {@code T1_BRIDGE_KEY} for the self-test pump), the cluster from {@code T1_CORE_INGRESS} and the client driver from
 * {@code T1_AERON_DIR}. Exit codes: 0 pass, 1 an oracle or barrier failure (the last report printed), 78 configuration
 * (rule H5), 70 internal.
 *
 * <p>Log order: frames of one core block go on several sessions; every frame with a reply is awaited, and a MarketTick
 * (no reply) is followed by a QueryFeedPosition on its own session that is awaited, so the live log has the block's order
 * (a tick overtaken by the next create would trigger a stop at another point than the model).</p>
 *
 * <p>Story sessions map to live sessions by class: 801 ADMIN, 804 TREASURY, 805 GATEWAY, 806 FORWARDER; every invocation
 * registers its GATEWAY and FORWARDER sessions before their first frame (a one-shot JVM's sessions are new each time; the
 * oracle's dump carries no session fact, so this is invisible to it).</p>
 */
public final class T1Core {

    private T1Core() {
    }

    static int main(final String[] args, final PrintStream out, final PrintStream err) throws Exception {
        return run(args, System.getenv(), out, err);
    }

    /** The command loop of one invocation (sessions opened lazily, closed at the end). */
    static final class Ctx implements AutoCloseable {
        final Map<String, String> env;
        final PrintStream out;
        final Map<SessionClass, T1Session> sessions = new LinkedHashMap<>();
        ReferencePump pump;
        long corr = System.nanoTime() & 0xFFFF_FFFFL | 1L << 40;
        long request = (System.currentTimeMillis() & 0xFFFF_FFFFL) << 16;

        Ctx(final Map<String, String> env, final PrintStream out) {
            this.env = env;
            this.out = out;
        }

        T1Session session(final SessionClass c) throws InterruptedException {
            T1Session s = sessions.get(c);
            if (s == null) {
                final String key = switch (c) {
                    case ADMIN -> "T1_ADMIN_KEY";
                    case TOOL -> "T1_TOOL_KEY";
                    case GATEWAY -> "T1_GATEWAY_KEY";
                    case TREASURY -> "T1_TREASURY_KEY";
                    case FORWARDER -> "T1_FORWARDER_KEY";
                    default -> throw new IllegalArgumentException("no T1 key for " + c);
                };
                s = T1Session.connect(env, key);
                sessions.put(c, s);
                if (c == SessionClass.GATEWAY || c == SessionClass.FORWARDER) {
                    final long k = corr++;
                    s.offer(new CoreFrames().register(k, c, 0, 0));
                    final int result = s.reply(k, 110)[8 + 10] & 0xFF;
                    if (result != 0 && result != 1) {
                        throw new IllegalStateException("RegisterSession " + c + " refused (result " + result + ")");
                    }
                }
            }
            return s;
        }

        @Override
        public void close() {
            sessions.values().forEach(T1Session::close);
            if (pump != null) {
                pump.close();
            }
        }
    }

    static SessionClass classOfStorySession(final long session) {
        if (session == CoreApplyRunner.ADMIN_SESSION) {
            return SessionClass.ADMIN;
        }
        if (session == CoreApplyRunner.TREASURY_SESSION) {
            return SessionClass.TREASURY;
        }
        if (session == FeedStory.GATEWAY_SESSION) {
            return SessionClass.GATEWAY;
        }
        if (session == FeedStory.FORWARDER_SESSION) {
            return SessionClass.FORWARDER;
        }
        throw new IllegalArgumentException("story session " + session + " has no live class");
    }

    public static int run(final String[] args, final Map<String, String> env, final PrintStream out, final PrintStream err)
            throws Exception {
        if (args.length < 2) {
            throw new FeedHarnessMain.Usage("t1-core <genesis|await-bound|open|deposit|withdraw|round|barrier|run|report|gate|"
                    + "latch-clear|oracle> ...");
        }
        try (Ctx c = new Ctx(env, out)) {
            switch (args[1]) {
                case "genesis": return genesis(c, args);
                case "await-bound": return awaitBound(c, args);
                case "open": return open(c, args);
                case "deposit":
                case "withdraw": return money(c, args);
                case "round": return round(c, FeedStory.read(Path.of(args[2])), Integer.parseInt(args[3]));
                case "barrier": return barrier(c, Long.parseLong(args[2]), Long.parseLong(args[3])) ? 0 : 1;
                case "run": return runStory(c, args);
                case "report":
                    out.println(reportJson(report(c)));
                    return 0;
                case "gate": return gate(c);
                case "latch-clear": return latchClear(c, args);
                case "oracle": return oracle(c, args);
                default: throw new FeedHarnessMain.Usage("t1-core: unknown command " + args[1]);
            }
        }
    }

    static String opt(final String[] args, final String name) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ genesis

    /** CoreConfig 1 -> 2 -> 3 (ADMIN) with genesisId G; with {@code --story} also the story's TREASURY deposits. */
    static int genesis(final Ctx c, final String[] args) throws Exception {
        final String storyDir = opt(args, "--story");
        final FeedStory.Story story = storyDir == null ? null : FeedStory.read(Path.of(storyDir));
        final String g = opt(args, "--genesis-id");
        if (story == null && g == null) {
            throw new FeedHarnessMain.Usage("t1-core genesis (--genesis-id G | --story <dir>)");
        }
        final long genesisId = story != null ? story.genesis().genesisId() : Long.decode(g);
        final CoreFrames enc = new CoreFrames();
        final T1Session admin = c.session(SessionClass.ADMIN);
        long corr = 1; // CoreApplyRunner.boot's numbering, so the model and the live core see the same requests
        for (long v = 1; v <= 3; v++) {
            final long k = corr++;
            admin.offer(enc.coreConfig(k, v, v, genesisId));
            c.out.println("CoreConfig " + v + ": " + ack(admin.reply(k, 110)));
        }
        if (story != null) {
            final T1Session treasury = c.session(SessionClass.TREASURY);
            long request = 1;
            for (int u = 1; u <= story.genesis().users(); u++) {
                for (final Asset a : Asset.values()) {
                    final long k = corr++;
                    treasury.offer(enc.deposit(k, u, a.id(), story.genesis().depositEach(), 0xB007L, request++));
                    final byte[] r = treasury.reply(k, 15, 111);
                    if (T1Session.templateOf(r) != 15) {
                        throw new IllegalStateException("deposit refused for user " + u + " asset " + a);
                    }
                }
            }
            c.out.println("deposits: " + story.genesis().users() + " users x " + Asset.values().length + " assets");
        }
        return 0;
    }

    static String ack(final byte[] f) {
        final AdminAckDecoder d = new AdminAckDecoder().wrapAndApplyHeader(new UnsafeBuffer(f), 0, new MessageHeaderDecoder());
        final int reason = d.reason();
        final String reasonName = reason == 40 ? "FEED_HALTED" : reason == 41 ? "ME_LANE_INACTIVE" : Integer.toString(reason);
        return "ADMINACK ref=" + d.refTemplateId() + " result=" + d.result() + " reason=" + reasonName;
    }

    // ------------------------------------------------------------------ queries

    record Report(long meEpoch, long lastAppliedJournalSeq, boolean halted, int haltReason, long consume, long lastTradeId) { }

    static Report report(final Ctx c) throws InterruptedException {
        final T1Session admin = c.session(SessionClass.ADMIN);
        final long k = c.corr++;
        admin.offer(new CoreFrames().query(k));
        final byte[] f = admin.reply(k, 17);
        final FeedPositionReportDecoder d = new FeedPositionReportDecoder().wrapAndApplyHeader(new UnsafeBuffer(f), 0,
                new MessageHeaderDecoder());
        return new Report(d.meEpoch(), d.lastAppliedJournalSeq(), d.feedHaltedRaw() == 1, d.haltReasonRaw(), d.consumePosition(),
                d.lastAppliedTradeId());
    }

    static String reportJson(final Report r) {
        return "{\"meEpoch\":" + r.meEpoch() + ",\"lastAppliedJournalSeq\":" + r.lastAppliedJournalSeq() + ",\"feedHalted\":"
                + r.halted() + ",\"haltReason\":" + r.haltReason() + ",\"consumePosition\":" + r.consume()
                + ",\"lastAppliedTradeId\":" + r.lastTradeId() + "}";
    }

    static int awaitBound(final Ctx c, final String[] args) throws Exception {
        final long timeout = Long.parseLong(args[2]);
        final String g = opt(args, "--genesis-id");
        final long deadline = System.currentTimeMillis() + timeout;
        Report r;
        do {
            r = report(c);
            if (r.meEpoch() != 0 && (g == null || r.meEpoch() == Long.decode(g))) {
                c.out.println("BOUND " + reportJson(r));
                return 0;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);
        c.out.println("NOT BOUND " + reportJson(r));
        return 1;
    }

    static boolean barrier(final Ctx c, final long seq, final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        Report r;
        do {
            r = report(c);
            if (r.lastAppliedJournalSeq() >= seq) {
                return true;
            }
            Thread.sleep(20);
        } while (System.currentTimeMillis() < deadline);
        c.out.println("BARRIER " + seq + " not reached: " + reportJson(r));
        return false;
    }

    static int open(final Ctx c, final String[] args) throws Exception {
        final int market = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        final T1Session admin = c.session(SessionClass.ADMIN);
        final long k = c.corr++;
        admin.offer(new CoreFrames().gate(k, market, true));
        c.out.println(ack(admin.reply(k, 110)));
        return 0;
    }

    static int money(final Ctx c, final String[] args) throws Exception {
        final long user = Long.parseLong(args[2]);
        final int asset = Integer.parseInt(args[3]);
        final long amount = Long.parseLong(args[4]);
        final T1Session t = c.session(SessionClass.TREASURY);
        final long k = c.corr++;
        final CoreFrames enc = new CoreFrames();
        t.offer(args[1].equals("deposit") ? enc.deposit(k, user, asset, amount, 0xD1L, c.request++)
                : enc.withdraw(k, user, asset, amount, 0xD1L, c.request++));
        final byte[] r = t.reply(k, 15, 16, 14, 111);
        c.out.println(args[1].toUpperCase() + " template " + T1Session.templateOf(r));
        return 0;
    }

    static int gate(final Ctx c) throws Exception {
        final T1Session gw = c.session(SessionClass.GATEWAY);
        final byte[] f = gw.await(x -> T1Session.templateOf(x) == 109 && new UnsafeBuffer(x).getInt(8,
                java.nio.ByteOrder.LITTLE_ENDIAN) == 0, 5_000);
        if (f == null) {
            c.out.println("{\"market\":0,\"state\":\"unknown\"}");
            return 0;
        }
        final AdmissionStateDecoder d = new AdmissionStateDecoder().wrapAndApplyHeader(new UnsafeBuffer(f), 0,
                new MessageHeaderDecoder());
        c.out.println("{\"market\":0,\"gateState\":\"" + d.gateState() + "\",\"gateReason\":\"" + d.gateReason()
                + "\",\"gateReasons\":" + Integer.toUnsignedLong(new UnsafeBuffer(f).getInt(8
                + AdmissionStateDecoder.gateReasonsEncodingOffset(), java.nio.ByteOrder.LITTLE_ENDIAN)) + "}");
        return 0;
    }

    static int latchClear(final Ctx c, final String[] args) throws Exception {
        final int reason = Integer.parseInt(args[2]);
        final long seq = Long.parseLong(args[3]);
        final T1Session admin = c.session(SessionClass.ADMIN);
        final long k = c.corr++;
        admin.offer(new CoreFrames().feedLatchClear(k, reason, seq));
        c.out.println(ack(admin.reply(k, 110)));
        return 0;
    }

    // ------------------------------------------------------------------ the story

    /** Sends core block r on the live sessions of its classes and waits every reply. */
    static int round(final Ctx c, final FeedStory.Story s, final int r) throws Exception {
        int sent = 0;
        for (final FeedFiles.LogEntry e : s.blocks().get(r)) {
            switch (e.kind()) {
                case FeedFiles.LogEntry.SESSION_OPEN -> c.session(classOfStorySession(e.sessionId()));
                case FeedFiles.LogEntry.FRAME -> {
                    final T1Session session = c.session(classOfStorySession(e.sessionId()));
                    session.offer(e.bytes());
                    final int t = T1Session.templateOf(e.bytes());
                    final long corr = t == 60 ? -1 : T1Session.correlationOf(e.bytes());
                    switch (t) {
                        case 60 -> {
                            // MarketTick has no reply: a QueryFeedPosition behind it on the SAME session is answered only
                            // after the tick applied, so the next frame (on another session) cannot overtake it in the log
                            final long q = c.corr++;
                            session.offer(new CoreFrames().query(q));
                            session.reply(q, 17);
                        }
                        case 50, 51, 52 -> session.reply(corr, 100, 101);
                        case 1, 2 -> session.reply(corr, 15, 16, 14, 111);
                        case 73, 80, 70, 76 -> session.reply(corr, 110);
                        default -> throw new IllegalStateException("core block template " + t + " has no reply rule");
                    }
                    sent++;
                }
                case FeedFiles.LogEntry.SWEEP -> { } // the cluster's own timers sweep
                default -> throw new IllegalStateException("FLOG kind " + e.kind());
            }
        }
        c.out.println("ROUND " + r + " sent " + sent);
        return 0;
    }

    /** The story loop: round r, the journal writes (or the self-test pump), barrier b_r. */
    static int runStory(final Ctx c, final String[] args) throws Exception {
        final FeedStory.Story s = FeedStory.read(Path.of(args[2]));
        final String fifo = opt(args, "--journal-ctl");
        final boolean pump = List.of(args).contains("--pump");
        if ((fifo == null) == !pump) {
            throw new FeedHarnessMain.Usage("t1-core run <story-dir> (--journal-ctl <fifo> | --pump)");
        }
        final long timeout = opt(args, "--timeout") == null ? 30_000 : Long.parseLong(opt(args, "--timeout"));
        final long[] barriers = s.barriers();
        long prev = 0;
        for (int r = 0; r < s.rounds(); r++) {
            round(c, s, r);
            if (pump) {
                if (c.pump == null) {
                    c.pump = ReferencePump.open(c.env);
                }
                c.pump.pump(s.frames(r));
            } else {
                Files.writeString(Path.of(fifo), "WRITE all " + (prev + 1) + " " + barriers[r] + "\n", StandardCharsets.US_ASCII,
                        StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            }
            if (!barrier(c, barriers[r], timeout)) {
                return 1;
            }
            prev = barriers[r];
        }
        c.out.println("RUN done: " + s.rounds() + " rounds, cursor " + report(c).lastAppliedJournalSeq());
        return 0;
    }

    /** The model's state for the same prefix (and extra core commands): the in-process core of the story. */
    static List<String> model(final FeedStory.Story s, final long upto, final List<FeedFiles.LogEntry> extra) {
        final CoreApplyRunner r = FeedStory.bootClean(s.genesis());
        final long[] barriers = s.barriers();
        long prev = 0;
        long t = s.genesis().t0();
        for (int round = 0; round < s.rounds() && prev < upto; round++) {
            FeedStory.applyBlock(r, s, round);
            final List<FeedFiles.Entry> in = new ArrayList<>();
            for (final byte[] rec : s.answers().get(round)) {
                if (JournalRecords.journalSeq(rec) <= upto) {
                    in.add(FeedFiles.Entry.record(rec));
                }
            }
            t = FeedStory.feedTime(s.genesis().t0(), round);
            for (final byte[] f : MeJournalReferenceTranslator.translate(in)) {
                r.apply(CoreApplyRunner.BRIDGE_SESSION, f, t);
            }
            prev = barriers[round];
        }
        for (final FeedFiles.LogEntry e : extra) {
            r.apply(e);
        }
        return CanonicalDump.of(r, t);
    }

    static int oracle(final Ctx c, final String[] args) throws Exception {
        final FeedStory.Story s = FeedStory.read(Path.of(args[2]));
        final long upto = opt(args, "--upto") == null ? s.length() : Long.parseLong(opt(args, "--upto"));
        final String extraFile = opt(args, "--extra");
        final List<FeedFiles.LogEntry> extra = extraFile == null ? List.of()
                : FeedFiles.decodeFlog(Files.readAllBytes(Path.of(extraFile)));
        final List<String> expected = model(s, upto, extra);
        final List<String> liveAll = CanonicalDump.of(c.session(SessionClass.ADMIN));
        final List<String> live = CanonicalDump.money(liveAll);
        final List<String> expectedMoney = CanonicalDump.money(expected);
        if (expectedMoney.equals(live)) {
            c.out.println("PASS " + CanonicalDump.sha256(live));
            c.out.println("latch " + liveAll.get(liveAll.size() - 1));
            return 0;
        }
        for (int i = 0; i < Math.max(expectedMoney.size(), live.size()); i++) {
            final String e = i < expectedMoney.size() ? expectedMoney.get(i) : "<none>";
            final String l = i < live.size() ? live.get(i) : "<none>";
            if (!e.equals(l)) {
                c.out.println("FAIL at line " + i + ": expected " + e + " live " + l);
                c.out.println("latch " + liveAll.get(liveAll.size() - 1));
                break;
            }
        }
        return 1;
    }
}

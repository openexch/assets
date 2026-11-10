// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.feedharness;

import com.openexchange.core.domain.Asset;
import com.openexchange.core.infrastructure.generated.AdminAckDecoder;
import com.openexchange.core.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.core.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.CoreApplyRunner;
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
 * {@code t1-core <command>}, the one-shot core client of the T1 harness (the {@code T1CoreDriver} pattern):
 * TOOL / ADMIN / GATEWAY / TREASURY / FORWARDER sessions of a real core cluster, credentials {@code keyId:CLASS:secret} from
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
        /** {@code run|round --genesis G}: the story's MarketTicks carry G (the second epoch of a dual genesis). */
        long genesisOverride;
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
                final T1Session self = s;
                s.idle = () -> drainOthers(self); // a reply wait must not let the other sessions' egress back up
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

        /** Drains every session of this invocation (and the pump): see {@link T1Session#drain}. */
        void drainAll() {
            drainOthers(null);
        }

        /** Drains every session but {@code self} (the one waiting for a reply), and the pump. */
        void drainOthers(final T1Session self) {
            for (final T1Session s : sessions.values()) {
                if (s != self) {
                    s.drain();
                }
            }
            if (pump != null) {
                pump.drain();
            }
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
            throw new FeedHarnessMain.Usage("t1-core <genesis|await-bound|open|suspend|deposit|withdraw|round|barrier|run|report|"
                    + "gate|latch-clear|oracle|invariants|dump|probe-create|probe-amend|probe-cancel> ...");
        }
        try (Ctx c = new Ctx(env, out)) {
            switch (args[1]) {
                case "genesis": return genesis(c, args);
                case "await-bound": return awaitBound(c, args);
                case "open": return open(c, args);
                case "deposit":
                case "withdraw": return money(c, args);
                case "round":
                    c.genesisOverride = opt(args, "--genesis") == null ? 0L : Long.decode(opt(args, "--genesis"));
                    return round(c, FeedStory.read(Path.of(args[2])), Integer.parseInt(args[3]));
                case "barrier": return barrier(c, Long.parseLong(args[2]), Long.parseLong(args[3])) ? 0 : 1;
                case "run": return runStory(c, args);
                case "report":
                    out.println(reportJson(report(c)));
                    return 0;
                case "gate": return opt(args, "--watch") != null ? T1CoreVerbs.gateWatch(c, args) : gate(c);
                case "latch-clear": return latchClear(c, args);
                case "oracle": return oracle(c, args);
                // halt drill verbs
                case "suspend": return T1CoreVerbs.suspend(c, args);
                case "invariants": return T1CoreVerbs.invariants(c, args);
                case "dump": return T1CoreVerbs.dumpOrders(c, args);
                case "probe-create": return T1CoreVerbs.probeCreate(c, args);
                case "probe-amend": return T1CoreVerbs.probeAmend(c, args);
                case "probe-cancel": return T1CoreVerbs.probeCancel(c, args);
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

    /**
     * CoreConfig 1 -> 2 -> 3 (ADMIN) with genesisId G; with {@code --story} also the story's TREASURY deposits.
     * (4.1 rule 6, the dual genesis): {@code --story} and {@code --genesis-id} together take the deposits from the story and
     * G from the option (the second epoch of {@code rk11-regen}; {@code oracle --genesis G} models it).
     */
    static int genesis(final Ctx c, final String[] args) throws Exception {
        final String storyDir = opt(args, "--story");
        final FeedStory.Story story = storyDir == null ? null : FeedStory.read(Path.of(storyDir));
        final String g = opt(args, "--genesis-id");
        if (story == null && g == null) {
            throw new FeedHarnessMain.Usage("t1-core genesis (--genesis-id G | --story <dir> [--genesis-id G])");
        }
        final long genesisId = g != null ? Long.decode(g) : story.genesis().genesisId();
        if (genesisId < 1) {
            throw new FeedHarnessMain.Usage("ERR genesis id " + genesisId + " (a genesis id is >= 1)");
        }
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

    static int openMarket(final String[] args) {
        for (int i = 2; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                return Integer.parseInt(args[i]);
            }
        }
        return 0;
    }

    static com.openexchange.core.infrastructure.generated.GateReason openReason(final String[] args) {
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("--ack-halt")) {
                return com.openexchange.core.infrastructure.generated.GateReason.BRIDGE_HALT;
            }
            if (args[i].startsWith("--")) {
                throw new IllegalArgumentException("t1-core open [market] [--ack-halt]: unknown option " + args[i]);
            }
        }
        return com.openexchange.core.infrastructure.generated.GateReason.ADMIN;
    }

    static String ack(final byte[] f) {
        final AdminAckDecoder d = new AdminAckDecoder().wrapAndApplyHeader(new UnsafeBuffer(f), 0, new MessageHeaderDecoder());
        final int reason = d.reason();
        final String reasonName = reason == 40 ? "FEED_HALTED" : reason == 41 ? "ME_LANE_INACTIVE"
                : reason == 45 ? "HALT_HOLDER_PENDING" : Integer.toString(reason);
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
            c.drainAll(); // no frame before this report is awaited; a long round's egress must not back up (T1Session.drain)
            r = report(c);
            if (r.lastAppliedJournalSeq() >= seq) {
                c.drainAll();
                return true;
            }
            final long until = System.currentTimeMillis() + 20;
            while (System.currentTimeMillis() < until) {
                c.drainAll();
                Thread.sleep(1);
            }
        } while (System.currentTimeMillis() < deadline);
        c.out.println("BARRIER " + seq + " not reached: " + reportJson(r));
        return false;
    }

    /**
     * {@code open [market] [--ack-halt]}: (G1) the default OPEN names ADMIN; {@code --ack-halt} names
     * BRIDGE_HALT, the acknowledgement an OPEN of GLOBAL needs while a halt holder is held (else 45 HALT_HOLDER_PENDING).
     */
    static int open(final Ctx c, final String[] args) throws Exception {
        final int market = openMarket(args);
        final T1Session admin = c.session(SessionClass.ADMIN);
        final long k = c.corr++;
        admin.offer(new CoreFrames().gate(k, market, true, openReason(args)));
        c.out.println(ack(admin.reply(k, 110)));
        return 0;
    }

    /**
     * {@code deposit|withdraw <user> <asset> <amount> [--record <flog>]}: prints the reply template and
     * (4.5 item 2) the result and reason by name; {@code --record} appends the exact TREASURY frame to a FLOG for
     * {@code oracle --extra} (the model applies it after the rounds it replays).
     */
    static int money(final Ctx c, final String[] args) throws Exception {
        final long user = Long.parseLong(args[2]);
        final int asset = Integer.parseInt(args[3]);
        final long amount = Long.parseLong(args[4]);
        final String record = opt(args, "--record");
        final T1Session t = c.session(SessionClass.TREASURY);
        final long k = c.corr++;
        final CoreFrames enc = new CoreFrames();
        final byte[] frame = args[1].equals("deposit") ? enc.deposit(k, user, asset, amount, 0xD1L, c.request++)
                : enc.withdraw(k, user, asset, amount, 0xD1L, c.request++);
        t.offer(frame);
        final byte[] r = t.reply(k, 15, 16, 14, 111, 101);
        final String result = T1CoreVerbs.moneyReply(r);
        c.out.println(args[1].toUpperCase() + " template " + T1Session.templateOf(r) + " " + result);
        if (record != null && result.equals("result=ACCEPTED")) {
            T1CoreVerbs.record(Path.of(record), frame);
            c.out.println("RECORDED " + record);
        }
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

    /**
     * A core-block frame under another genesis. A MarketTick carries the ME epoch (a tick of another
     * epoch is dropped), so the second epoch of a dual genesis sends the story's ticks with G; every other block frame is
     * epoch-free. {@code genesisId 0} = unchanged.
     */
    static byte[] underGenesis(final byte[] frame, final long genesisId) {
        if (genesisId == 0 || T1Session.templateOf(frame) != 60) {
            return frame;
        }
        final byte[] f = frame.clone();
        new UnsafeBuffer(f).putLong(8 + com.openexchange.core.infrastructure.generated.MarketTickEncoder.meEpochEncodingOffset(),
                genesisId, java.nio.ByteOrder.LITTLE_ENDIAN);
        return f;
    }

    /** Sends core block r on the live sessions of its classes and waits every reply. */
    static int round(final Ctx c, final FeedStory.Story s, final int r) throws Exception {
        int sent = 0;
        for (final FeedFiles.LogEntry e : s.blocks().get(r)) {
            switch (e.kind()) {
                case FeedFiles.LogEntry.SESSION_OPEN -> c.session(classOfStorySession(e.sessionId()));
                case FeedFiles.LogEntry.FRAME -> {
                    final T1Session session = c.session(classOfStorySession(e.sessionId()));
                    session.offer(underGenesis(e.bytes(), c.genesisOverride));
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

    static final String RUN_USAGE = "t1-core run <story-dir> (--journal-ctl <fifo> | --pump) [--timeout <ms>] [--from <r>]"
            + " [--to <r>] [--genesis G]";

    /**
     * The story loop: round r, the journal writes (or the self-test pump), barrier b_r; rounds {@code --from} .. {@code --to}
     * (default all), so a runner can stop after a round (R-K7: restart the bridge behind the long round) or do one round's
     * steps itself (R-K6: {@code round}, its own WRITE, the bridge start, {@code barrier}) and resume with {@code --from}.
     */
    static int runStory(final Ctx c, final String[] args) throws Exception {
        final FeedStory.Story s = FeedStory.read(Path.of(args[2]));
        c.genesisOverride = opt(args, "--genesis") == null ? 0L : Long.decode(opt(args, "--genesis"));
        if (c.genesisOverride != 0 && List.of(args).contains("--pump")) {
            throw new FeedHarnessMain.Usage(RUN_USAGE + ": --genesis needs --journal-ctl (the pump feeds the story's epoch)");
        }
        final String fifo = opt(args, "--journal-ctl");
        final boolean pump = List.of(args).contains("--pump");
        if ((fifo == null) == !pump) {
            throw new FeedHarnessMain.Usage(RUN_USAGE);
        }
        final long timeout = opt(args, "--timeout") == null ? 30_000 : Long.parseLong(opt(args, "--timeout"));
        final int from = opt(args, "--from") == null ? 0 : Integer.parseInt(opt(args, "--from"));
        final int to = opt(args, "--to") == null ? s.rounds() - 1 : Integer.parseInt(opt(args, "--to"));
        if (from < 0 || to >= s.rounds() || from > to) {
            throw new FeedHarnessMain.Usage(RUN_USAGE + ": rounds " + from + ".." + to + " outside 0.." + (s.rounds() - 1));
        }
        final long[] barriers = s.barriers();
        long prev = from == 0 ? 0 : barriers[from - 1];
        for (int r = from; r <= to; r++) {
            round(c, s, r);
            if (pump) {
                if (c.pump == null) {
                    c.pump = ReferencePump.open(c.env);
                }
                c.pump.pump(s.frames(r), c::drainAll);
            } else {
                Files.writeString(Path.of(fifo), "WRITE all " + (prev + 1) + " " + barriers[r] + "\n", StandardCharsets.US_ASCII,
                        StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            }
            if (!barrier(c, barriers[r], timeout)) {
                return 1;
            }
            prev = barriers[r];
        }
        c.out.println("RUN done: " + (from == 0 && to == s.rounds() - 1 ? s.rounds() + " rounds"
                : "rounds " + from + ".." + to) + ", cursor " + report(c).lastAppliedJournalSeq());
        return 0;
    }

    /** The model's state for the same prefix (and extra core commands): the in-process core of the story. */
    static List<String> model(final FeedStory.Story s, final long upto, final List<FeedFiles.LogEntry> extra) {
        return model(s, upto, extra, 0L);
    }

    /**
     * {@link #model} under {@code genesisId} when it is not 0 ({@code oracle --genesis}): the CoreConfig
     * records it, and every journal record the model translates carries it as its meEpoch (seqs and egressSeqs unchanged),
     * which is what {@code REGENESIS <src> <id> --fresh-only} followed by {@code WRITE} writes.
     */
    static List<String> model(final FeedStory.Story s, final long upto, final List<FeedFiles.LogEntry> extra, final long genesisId) {
        final long[] t = new long[1];
        final CoreApplyRunner r = modelRunner(s, upto, extra, genesisId, t);
        return CanonicalDump.of(r, t[0]);
    }

    /** The model core itself (tests read its order store); {@code time[0]} receives the last feed time. */
    static CoreApplyRunner modelRunner(final FeedStory.Story s, final long upto, final List<FeedFiles.LogEntry> extra,
                                       final long genesisId, final long[] time) {
        final boolean relabel = genesisId != 0 && !FeedHarnessSeams.on(FeedHarnessSeams.GENESIS_OVERRIDE_IGNORED);
        final CoreApplyRunner.Genesis g = relabel ? new CoreApplyRunner.Genesis(genesisId, s.genesis().users(),
                s.genesis().depositEach(), s.genesis().t0()) : s.genesis();
        final CoreApplyRunner r = FeedStory.bootClean(g);
        final long[] barriers = s.barriers();
        long prev = 0;
        long t = s.genesis().t0();
        for (int round = 0; round < s.rounds() && prev < upto; round++) {
            if (relabel) {
                for (final FeedFiles.LogEntry e : s.blocks().get(round)) {
                    r.apply(e.kind() == FeedFiles.LogEntry.FRAME
                            ? new FeedFiles.LogEntry(e.kind(), e.sessionId(), e.timestamp(), underGenesis(e.bytes(), genesisId)) : e);
                }
            } else {
                FeedStory.applyBlock(r, s, round);
            }
            final List<FeedFiles.Entry> in = new ArrayList<>();
            for (final byte[] rec : s.answers().get(round)) {
                if (JournalRecords.journalSeq(rec) <= upto) {
                    in.add(FeedFiles.Entry.record(relabel ? JournalRecords.withHead(rec, JournalRecords.journalSeq(rec), genesisId,
                            JournalRecords.egressSeq(rec)) : rec));
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
        time[0] = t;
        return r;
    }

    static int oracle(final Ctx c, final String[] args) throws Exception {
        final FeedStory.Story s = FeedStory.read(Path.of(args[2]));
        final long upto = opt(args, "--upto") == null ? s.length() : Long.parseLong(opt(args, "--upto"));
        final String extraFile = opt(args, "--extra");
        final List<FeedFiles.LogEntry> extra = extraFile == null ? List.of()
                : FeedFiles.decodeFlog(Files.readAllBytes(Path.of(extraFile)));
        final String genesis = opt(args, "--genesis");
        final List<String> expected = model(s, upto, extra, genesis == null ? 0L : Long.decode(genesis));
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

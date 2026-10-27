// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.ReleaseScope;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;
import com.openexchange.cluster.auth.ClusterPrincipal;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * RC2 S9h 6.4: a coherent behaviour-3 story the core can settle, in rounds. Round r = a core block (GATEWAY CreateOrder /
 * Cancel / Amend across LIMIT, MARKET with a budget, IOC, FOK, iceberg, stop; FORWARDER ticks; TREASURY deposits and
 * withdrawals; the ADMIN open) followed by the ME's answer to it as dense schema 4 records, seq 1 being the FRESH 68 with
 * {@code meEpoch = genesisId}. The ME is a reference ME after {@code V3CoreFuzzDriver} (S4 ledger, floors and tombstones,
 * 4.2 releases, the native IOC / FOK of S5, the expectedOldRemaining check) that reads the core's MeDispatch stream from
 * its model; its answers are core-side frames applied to the model, turned into the journal by
 * {@link MeJournalReverseEncoder}. Times: core block r inside {@code [t0 + 1000 r, t0 + 1000 r + 499]}, every feed frame
 * of round r at {@code t0 + 1000 r + 500}. Every round's answer carries at least one feed record (review FIX-4). The clean
 * run applies the feed on the pinned BRIDGE session 701, identity 0xB1 (review FIX-5).
 */
public final class FeedStory {

    public static final long T0 = 1_700_000_000_000L;
    public static final long ONE = 100_000_000L;
    public static final int USERS = 6;
    public static final long DEPOSIT = 1_000_000L * ONE;
    public static final long GATEWAY_SESSION = 805L;
    public static final long FORWARDER_SESSION = 806L;
    public static final int MIN_ROUNDS = 8;
    static final int CAPACITY = 4096;
    static final long ME_BEHAVIOR = 2;
    static final int MKT = 1;
    static final int R_APPLIED = 0;
    static final int R_REJECTED = 1;
    static final int R_UNKNOWN_LEG = 4;
    static final int R_RELEASED = 7;
    static final int ME_FILLED = 2;
    static final int ME_CANCELLED = 3;

    /** A built (or read) story. {@code answers.get(r)} are the schema 4 records of round r's ME answer. */
    public record Story(long seed, CoreApplyRunner.Genesis genesis, List<List<FeedFiles.LogEntry>> blocks,
                        List<List<byte[]>> answers, String expectedJson, Map<String, Boolean> coverage, List<byte[]> meFrames) {

        public int rounds() {
            return blocks.size();
        }

        /** The last journalSeq of each round (the round barriers). */
        public long[] barriers() {
            final long[] b = new long[rounds()];
            long s = 0;
            for (int r = 0; r < rounds(); r++) {
                s += answers.get(r).size();
                b[r] = s;
            }
            return b;
        }

        /** The dense journal length N. */
        public long length() {
            final long[] b = barriers();
            return b[b.length - 1];
        }

        /** {@code journal.fjr}: every round's records, a FLUSH at each round end. */
        public List<FeedFiles.Entry> journal() {
            final List<FeedFiles.Entry> j = new ArrayList<>();
            for (final List<byte[]> a : answers) {
                for (final byte[] r : a) {
                    j.add(FeedFiles.Entry.record(r));
                }
                j.add(FeedFiles.Entry.FLUSH);
            }
            return j;
        }

        /** The translated frames of round r (the bridge's offers for that round, in order). */
        public List<byte[]> frames(final int r) {
            final List<FeedFiles.Entry> in = new ArrayList<>();
            for (final byte[] rec : answers.get(r)) {
                in.add(FeedFiles.Entry.record(rec));
            }
            return MeJournalReferenceTranslator.translate(in);
        }

        public String expectedSha256() {
            final int i = expectedJson.indexOf("\"snapshotSha256\":\"") + 18;
            return expectedJson.substring(i, i + 64);
        }
    }

    public static long blockTime(final long t0, final int r) {
        return t0 + 1000L * r;
    }

    public static long feedTime(final long t0, final int r) {
        return t0 + 1000L * r + 500;
    }

    /** The genesis of a story seed (the core's genesisId is the ME epoch the FRESH binds). */
    public static CoreApplyRunner.Genesis genesisOf(final long seed) {
        return new CoreApplyRunner.Genesis(0x51_0000_0000L + (seed & 0xFFFF_FFFFL), USERS, DEPOSIT, T0);
    }

    /** Boot plus the pinned BRIDGE session (open + RegisterSession at t0, before round 0): the clean run's prefix. */
    public static CoreApplyRunner bootClean(final CoreApplyRunner.Genesis g) {
        final CoreApplyRunner r = CoreApplyRunner.boot(g);
        r.openBridge(CoreApplyRunner.BRIDGE_SESSION, CoreApplyRunner.BRIDGE_IDENTITY, g.t0());
        return r;
    }

    /** Applies core block r (its entries with their own times). */
    public static void applyBlock(final CoreApplyRunner r, final Story s, final int round) {
        for (final FeedFiles.LogEntry e : s.blocks().get(round)) {
            r.apply(e);
        }
    }

    /** The clean run: every round once, in order, the feed on 701 at the round's feed time. */
    public static CoreApplyRunner cleanRun(final Story s) {
        final CoreApplyRunner r = bootClean(s.genesis());
        for (int round = 0; round < s.rounds(); round++) {
            applyBlock(r, s, round);
            for (final byte[] f : s.frames(round)) {
                r.apply(CoreApplyRunner.BRIDGE_SESSION, f, feedTime(s.genesis().t0(), round));
            }
        }
        return r;
    }

    // ------------------------------------------------------------------ files

    public static void write(final Story s, final Path dir) throws IOException {
        Files.createDirectories(dir.resolve("blocks"));
        final Map<String, byte[]> files = new TreeMap<>();
        files.put("journal.fjr", FeedFiles.encodeFjr(s.journal()));
        for (int r = 0; r < s.rounds(); r++) {
            files.put("blocks/round-" + r + ".flog", FeedFiles.encodeFlog(s.blocks().get(r)));
        }
        final StringBuilder b = new StringBuilder();
        final long[] barriers = s.barriers();
        for (int r = 0; r < barriers.length; r++) {
            b.append(r).append(' ').append(barriers[r]).append('\n');
        }
        files.put("barriers.txt", b.toString().getBytes(StandardCharsets.US_ASCII));
        files.put("expected.json", (s.expectedJson() + "\n").getBytes(StandardCharsets.US_ASCII));
        // review NOTE-9: the clean run as two FLOGs, so `apply blocks/boot.flog replay.flog out.json` reproduces
        // expected.json: the boot prefix (CoreConfig 1-3, the deposits, BRIDGE 701 open and registered) and every round's
        // block followed by its translated frames on 701 at the round's feed time
        files.put("blocks/boot.flog", FeedFiles.encodeFlog(bootClean(s.genesis()).log()));
        final List<FeedFiles.LogEntry> replay = new ArrayList<>();
        for (int r = 0; r < s.rounds(); r++) {
            replay.addAll(s.blocks().get(r));
            for (final byte[] f : s.frames(r)) {
                replay.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.FRAME, CoreApplyRunner.BRIDGE_SESSION,
                        feedTime(s.genesis().t0(), r), f));
            }
        }
        files.put("replay.flog", FeedFiles.encodeFlog(replay));
        final CoreApplyRunner.Genesis g = s.genesis();
        files.put("story.txt", ("seed=" + s.seed() + "\ngenesis_id=" + g.genesisId() + "\nusers=" + g.users() + "\ndeposit_each="
                + g.depositEach() + "\nt0=" + g.t0() + "\nrounds=" + s.rounds() + "\nrecords=" + s.length()
                + "\nbridge_session=" + CoreApplyRunner.BRIDGE_SESSION + "\nbridge_identity=" + CoreApplyRunner.BRIDGE_IDENTITY
                + "\n").getBytes(StandardCharsets.US_ASCII));
        for (final Map.Entry<String, byte[]> e : files.entrySet()) {
            FeedFiles.writeAtomic(dir.resolve(e.getKey()), e.getValue());
        }
        FeedFiles.writeAtomic(dir.resolve("SHA256SUMS"), FeedFiles.sums(files).getBytes(StandardCharsets.US_ASCII));
    }

    /** Reads a story directory (every file checked against its SUMS). */
    public static Story read(final Path dir) throws IOException {
        final Map<String, byte[]> files = FeedFiles.verifySums(dir, null);
        final Map<String, String> p = new LinkedHashMap<>();
        for (final String line : new String(need(files, "story.txt"), StandardCharsets.US_ASCII).split("\n")) {
            final int eq = line.indexOf('=');
            if (eq > 0) {
                p.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }
        final CoreApplyRunner.Genesis g = new CoreApplyRunner.Genesis(Long.parseLong(p.get("genesis_id")),
                Integer.parseInt(p.get("users")), Long.parseLong(p.get("deposit_each")), Long.parseLong(p.get("t0")));
        final int rounds = Integer.parseInt(p.get("rounds"));
        final List<List<FeedFiles.LogEntry>> blocks = new ArrayList<>();
        for (int r = 0; r < rounds; r++) {
            blocks.add(FeedFiles.decodeFlog(need(files, "blocks/round-" + r + ".flog")));
        }
        final List<List<byte[]>> answers = new ArrayList<>();
        List<byte[]> cur = new ArrayList<>();
        for (final FeedFiles.Entry e : FeedFiles.decodeFjr(need(files, "journal.fjr"))) {
            if (e.isFlush()) {
                answers.add(cur);
                cur = new ArrayList<>();
            } else {
                cur.add(e.bytes());
            }
        }
        if (!cur.isEmpty() || answers.size() != rounds) {
            throw new FeedFiles.Refused("journal.fjr has " + answers.size() + " rounds, story.txt says " + rounds);
        }
        final String expected = new String(need(files, "expected.json"), StandardCharsets.US_ASCII).trim();
        return new Story(Long.parseLong(p.get("seed")), g, blocks, answers, expected, Map.of(), List.of());
    }

    private static byte[] need(final Map<String, byte[]> files, final String name) {
        final byte[] b = files.get(name);
        if (b == null) {
            throw new FeedFiles.Refused("story file " + name + " missing from SHA256SUMS");
        }
        return b;
    }

    // ------------------------------------------------------------------ the builder

    public static Story build(final long seed, final int rounds) {
        if (rounds < MIN_ROUNDS) {
            throw new IllegalArgumentException("a story needs at least " + MIN_ROUNDS + " rounds for its coverage, got " + rounds);
        }
        return new Builder(seed, rounds).run();
    }

    /** The coverage items of 6.4 (each must be true; the builder refuses a story otherwise). */
    public static final List<String> COVERAGE = List.of("t61", "t63", "t64", "t67", "t68", "batchRun",
            "partialFillThenCancel", "amendApplied", "amendRejectedOldCancelled", "icebergSlice", "releaseWithEntries",
            "everyRoundHasFeed");

    private static final class Builder {
        private final long seed;
        private final int rounds;
        private final Random rnd;
        private final CoreApplyRunner.Genesis g;
        private final CoreApplyRunner model;
        private final CoreFrames enc = new CoreFrames();
        private final MeJournalReverseEncoder rev = new MeJournalReverseEncoder();
        private final List<List<FeedFiles.LogEntry>> blocks = new ArrayList<>();
        private final List<List<byte[]>> answers = new ArrayList<>();
        private final List<byte[]> meFrames = new ArrayList<>();
        private final Map<String, Boolean> cov = new LinkedHashMap<>();
        private List<FeedFiles.LogEntry> block;
        private List<byte[]> answer;
        private int round;
        private long blockClock;
        private long corr = 1_000;
        private long request = 1_000;
        private long jseq;
        private long egress;
        private long nextLeg = 10_000;
        private long nextTrade;
        private long bookVersion;
        private long mid = 100 * ONE;
        private int sinkIndex;
        private boolean bumped;
        private final int feedlessRound;

        // the reference ME
        private final ArrayDeque<AssetsEngineEvent.MeDispatch> inbox = new ArrayDeque<>();
        /** h -> {leg, remaining, price, side, user, market}. */
        private final TreeMap<Long, long[]> rest = new TreeMap<>();
        private final TreeMap<Long, TreeSet<Long>> inflight = new TreeMap<>();
        private final TreeMap<Long, Long> floor = new TreeMap<>();
        private final TreeSet<Long> tomb = new TreeSet<>();
        private final TreeSet<Long> partial = new TreeSet<>();
        private final Map<Long, Integer> icebergSlices = new TreeMap<>();
        /** corr -> orderId from the core's OrderAck. */
        private final Map<Long, Long> acked = new TreeMap<>();
        private final Map<String, Long> named = new TreeMap<>();
        private long forcedRejectOldCancelled;

        /** The scripted beat's orders: random traffic leaves them alone (they carry the coverage items). */
        private boolean scripted(final long h) {
            return named.containsValue(h);
        }

        Builder(final long seed, final int rounds) {
            this.seed = seed;
            this.rounds = rounds;
            this.rnd = new Random(seed);
            this.g = genesisOf(seed);
            this.model = bootClean(g);
            this.feedlessRound = FeedHarnessSeams.on(FeedHarnessSeams.STORY_FEEDLESS_ROUND) ? rounds - 1 : -1;
            for (final String c : COVERAGE) {
                cov.put(c, false);
            }
            collect();
        }

        Story run() {
            for (round = 0; round < rounds; round++) {
                block = new ArrayList<>();
                answer = new ArrayList<>();
                blockClock = blockTime(g.t0(), round);
                coreBlock();
                if (blockClock > blockTime(g.t0(), round) + 499) {
                    throw new IllegalStateException("core block " + round + " ran past its 500 ms window");
                }
                meAnswer();
                blocks.add(block);
                answers.add(answer);
            }
            final Story draft = new Story(seed, g, blocks, answers, "", cov, meFrames);
            // coverage from the journal as the bridge will send it (translate staging included)
            final List<byte[]> all = new ArrayList<>();
            for (int r = 0; r < rounds; r++) {
                all.addAll(draft.frames(r));
            }
            for (final byte[] f : all) {
                final int t = FeedDigests.templateOf(f);
                if (cov.containsKey("t" + t)) {
                    cov.put("t" + t, true); // 6.4: templates 61, 63, 64, 67, 68; a 62 is the batchRun item
                }
                if (t == 62) {
                    cov.put("batchRun", true); // t61 means a lone 61 frame (review NOTE-8)
                }
                if (t == 67 && TranslatorProofs.u16(f, 54) > 0) {
                    cov.put("releaseWithEntries", true);
                }
            }
            boolean everyRound = true;
            for (int r = 0; r < rounds; r++) {
                everyRound &= !answers.get(r).isEmpty();
            }
            cov.put("everyRoundHasFeed", everyRound);
            final List<String> missing = new ArrayList<>();
            for (final Map.Entry<String, Boolean> e : cov.entrySet()) {
                if (!e.getValue() && !(e.getKey().equals("everyRoundHasFeed") && feedlessRound >= 0)) {
                    missing.add(e.getKey());
                }
            }
            if (!missing.isEmpty()) {
                throw new AssertionError("story " + seed + " coverage: missing " + missing);
            }
            final CoreApplyRunner clean = cleanRun(draft);
            final CoreApplyRunner.Result res = clean.result();
            if (res.lastAppliedJournalSeq() != draft.length() || !res.latchReason().equals("NONE") || !res.invariantsHold()
                    || res.counters().get("decodeRejectCount") != 0 || res.counters().get("settleRejectCount") != 0) {
                throw new AssertionError("story " + seed + " clean run is not coherent: " + res.toJson());
            }
            return new Story(seed, g, blocks, answers, res.toJson(), cov, meFrames);
        }

        // ---------------------------------------------------------------- core block

        private long tick() {
            return blockClock++;
        }

        private void open(final long session, final SessionClass c, final int keyId) {
            final byte[] principal = ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(c), keyId).encode();
            final FeedFiles.LogEntry e = new FeedFiles.LogEntry(FeedFiles.LogEntry.SESSION_OPEN, session, tick(), principal);
            block.add(e);
            model.apply(e);
        }

        private void core(final long session, final byte[] frame) {
            final FeedFiles.LogEntry e = new FeedFiles.LogEntry(FeedFiles.LogEntry.FRAME, session, tick(), frame);
            block.add(e);
            model.apply(e);
            collect();
        }

        /** Pulls the core's new MeDispatch events into the ME inbox and the OrderAcks into the corr map. */
        private void collect() {
            final List<AssetsEngineEvent> ev = model.sink().events();
            for (; sinkIndex < ev.size(); sinkIndex++) {
                final AssetsEngineEvent e = ev.get(sinkIndex);
                if (e instanceof AssetsEngineEvent.MeDispatch d) {
                    inbox.addLast(d);
                    if (d.kind() == DispatchKind.CREATE) {
                        final CoreOrder o = model.engine().orders().orders().get(d.commandIdHigh());
                        if (o != null && o.orderType == 6) {
                            final int n = icebergSlices.merge(d.commandIdHigh(), 1, Integer::sum);
                            if (n >= 2) {
                                cov.put("icebergSlice", true);
                            }
                        }
                    }
                } else if (e instanceof AssetsEngineEvent.OrderAck a && !a.duplicate()) {
                    acked.put(a.correlationId(), a.orderId());
                }
            }
        }

        private long create(final int user, final int side, final int type, final int tif, final long price, final long qty,
                            final long budget, final long stop, final long display) {
            final long c = corr++;
            core(GATEWAY_SESSION, enc.create(c, user, 0x5EEDL, request++, MKT, side, type, tif, price, qty, budget, stop, 0,
                    display, 0));
            final Long id = acked.get(c);
            return id == null ? 0L : id;
        }

        private long step() {
            return ONE / 2;
        }

        private long q(final int tenths) {
            return tenths * ONE / 10;
        }

        private void coreTick(final long bid, final long ask, final long last) {
            core(FORWARDER_SESSION, enc.tick(MKT, ++bookVersion, bid, ask, last, g.genesisId()));
        }

        private void coreBlock() {
            if (round == 0) {
                open(GATEWAY_SESSION, SessionClass.GATEWAY, 5);
                core(GATEWAY_SESSION, enc.register(corr++, SessionClass.GATEWAY, 0, 0));
                open(FORWARDER_SESSION, SessionClass.FORWARDER, 6);
                core(FORWARDER_SESSION, enc.register(corr++, SessionClass.FORWARDER, 0, 0));
                core(CoreApplyRunner.TREASURY_SESSION, enc.deposit(corr++, 1, 0, 5 * ONE, 0xD0L, request++));
                return;
            }
            if (round == feedlessRound) {
                core(CoreApplyRunner.TREASURY_SESSION, enc.deposit(corr++, 2, 0, 3 * ONE, 0xD0L, request++));
                return;
            }
            if (round == 1) {
                core(CoreApplyRunner.ADMIN_SESSION, enc.gate(corr++, OrderEntryGate.GLOBAL, true));
                coreTick(mid - step(), mid + step(), mid);
            }
            switch (round) {
                case 2 -> {
                    named.put("S", create(1, 1, 0, 0, mid, 5 * ONE, 0, 0, 0)); // big resting sell
                    named.put("B1", create(2, 0, 0, 0, mid, 2 * ONE, 0, 0, 0));
                    named.put("B2", create(3, 0, 0, 0, mid, 2 * ONE, 0, 0, 0));
                    named.put("ICE", create(4, 1, 6, 0, mid + step(), 3 * ONE, 0, 0, ONE)); // iceberg, display 1
                    named.put("IB", create(5, 0, 0, 0, mid + step(), q(25), 0, 0, 0)); // takes the slices
                    create(5, 0, 0, 1, mid - 4 * step(), q(3), 0, 0, 0); // IOC
                    create(6, 1, 0, 2, mid + 4 * step(), q(3), 0, 0, 0); // FOK
                    core(CoreApplyRunner.TREASURY_SESSION, enc.withdraw(corr++, 6, 0, 2 * ONE, 0xD0L, request++));
                }
                case 3 -> named.put("STOP", create(6, 1, 3, 0, 0, q(5), 0, mid - 2 * step(), 0)); // stop loss sell
                case 4 -> {
                    final long b1 = named.getOrDefault("B1", 0L);
                    if (b1 != 0) {
                        core(GATEWAY_SESSION, enc.cancel(corr++, 2, b1, 0x5EEDL, request++)); // partially filled in round 3
                    }
                    final long s = named.getOrDefault("S", 0L);
                    final CoreOrder so = s == 0 ? null : model.engine().orders().orders().get(s);
                    if (so != null) {
                        core(GATEWAY_SESSION, enc.amend(corr++, 1, s, 0xA3EDL, request++, 0, so.filledQty + 4 * ONE));
                    }
                    create(2, 0, 1, 0, 0, ONE, 200 * ONE, 0, 0); // MARKET buy with a budget
                }
                case 5 -> {
                    final long b2 = named.getOrDefault("B2", 0L);
                    final CoreOrder bo = b2 == 0 ? null : model.engine().orders().orders().get(b2);
                    if (bo != null) {
                        forcedRejectOldCancelled = b2;
                        core(GATEWAY_SESSION, enc.amend(corr++, 3, b2, 0xA3EDL, request++, 0, bo.filledQty + 3 * ONE));
                    }
                    create(1, 1, 1, 0, 0, q(4), 0, 0, 0); // MARKET sell
                    coreTick(mid - 4 * step(), mid - 3 * step(), mid - 4 * step()); // the stop triggers
                }
                default -> { }
            }
            // every round from 1 on: a resting LIMIT (so the ME answers), then random traffic
            final int u = 1 + rnd.nextInt(USERS);
            final int side = rnd.nextInt(2);
            create(u, side, 0, 0, side == 0 ? mid - (1 + rnd.nextInt(3)) * step() : mid + (1 + rnd.nextInt(3)) * step(),
                    q(1 + rnd.nextInt(10)), 0, 0, 0);
            final int n = 1 + rnd.nextInt(4);
            for (int i = 0; i < n; i++) {
                randomAction();
            }
            if (round == rounds - 1) {
                core(CoreApplyRunner.TREASURY_SESSION, enc.deposit(corr++, 3, 1, ONE, 0xD0L, request++));
            }
        }

        private void randomAction() {
            final int p = rnd.nextInt(100);
            final int u = 1 + rnd.nextInt(USERS);
            if (p < 40) {
                final int side = rnd.nextInt(2);
                final int k = rnd.nextInt(5) - 1; // sometimes crossing
                create(u, side, 0, 0, side == 0 ? mid - k * step() : mid + k * step(), q(1 + rnd.nextInt(10)), 0, 0, 0);
            } else if (p < 52) {
                final List<CoreOrder> live = live();
                live.removeIf(o -> scripted(o.orderId));
                if (!live.isEmpty()) {
                    final CoreOrder o = live.get(rnd.nextInt(live.size()));
                    core(GATEWAY_SESSION, enc.cancel(corr++, o.userId, o.orderId, 0x5EEDL, request++));
                }
            } else if (p < 62) {
                final List<CoreOrder> l = live();
                l.removeIf(o -> o.currentLegId == 0 || o.has(CoreOrder.F_DISPATCH_PENDING) || o.has(CoreOrder.F_CANCEL_PENDING)
                        || o.has(CoreOrder.F_REPLACE_PENDING) || o.has(CoreOrder.F_IOC) || o.has(CoreOrder.F_FOK)
                        || o.orderType != 0 || scripted(o.orderId));
                if (!l.isEmpty()) {
                    final CoreOrder o = l.get(rnd.nextInt(l.size()));
                    core(GATEWAY_SESSION, enc.amend(corr++, o.userId, o.orderId, 0xA3EDL, request++, 0,
                            o.filledQty + q(1 + rnd.nextInt(10))));
                }
            } else if (p < 70) {
                create(u, rnd.nextInt(2), 0, 1 + rnd.nextInt(2), mid, q(1 + rnd.nextInt(5)), 0, 0, 0); // IOC / FOK
            } else if (p < 76) {
                if (rnd.nextBoolean()) {
                    create(u, 0, 1, 0, 0, q(1 + rnd.nextInt(10)), 150 * ONE, 0, 0); // market buy, budget
                } else {
                    create(u, 1, 1, 0, 0, q(1 + rnd.nextInt(5)), 0, 0, 0);
                }
            } else if (p < 82) {
                core(CoreApplyRunner.TREASURY_SESSION, enc.deposit(corr++, u, rnd.nextInt(2), q(1 + rnd.nextInt(50)), 0xD0L,
                        request++));
            } else if (p < 86) {
                core(CoreApplyRunner.TREASURY_SESSION, enc.withdraw(corr++, u, 0, q(1 + rnd.nextInt(20)), 0xD0L, request++));
            } else if (p < 92) {
                mid += (rnd.nextInt(3) - 1) * step();
                coreTick(mid - step(), mid + step(), mid);
            } else {
                create(u, 1, 6, 0, mid + 2 * step(), 2 * ONE, 0, 0, q(5)); // iceberg
            }
        }

        private List<CoreOrder> live() {
            final List<CoreOrder> out = new ArrayList<>();
            model.engine().orders().orders().forEachByOrderId(out::add);
            return out;
        }

        // ---------------------------------------------------------------- the ME answer

        private void meAnswer() {
            if (round == 0) {
                me(enc.engineConfig(++egress, ++jseq, g.genesisId(), 1, ME_BEHAVIOR, CAPACITY, 12, 0)); // FRESH
                return;
            }
            if (round == feedlessRound) {
                return; // the seam: no feed record this round (the inbox waits for the next one)
            }
            // the inbox in FIFO order (dispatches minted while the answer applies wait for the next round)
            final int pending = inbox.size();
            for (int i = 0; i < pending; i++) {
                process(inbox.pollFirst());
            }
            if (round == 3) {
                forcedBatch();
            }
            final int trades = rnd.nextInt(4);
            for (int i = 0; i < trades; i++) {
                if (!meTrade(-1, -1, 0L, true)) {
                    break;
                }
            }
            if (!bumped && round == rounds / 2) {
                bumped = true;
                me(enc.engineConfig(++egress, ++jseq, g.genesisId(), 2, ME_BEHAVIOR, CAPACITY, 12, 3)); // the one BUMP
            }
            if (answer.isEmpty()) {
                throw new AssertionError("story " + seed + " round " + round + ": the ME answer carries no feed record");
            }
        }

        /** Applies one ME answer frame to the model on the pinned BRIDGE session and journals its records. */
        private void me(final byte[] frame) {
            final CoreApplyRunner.Egress e = model.apply(CoreApplyRunner.BRIDGE_SESSION, frame, feedTime(g.t0(), round));
            if (!model.engine().feed().latchReason().name().equals("NONE") || e.settleRejected() > 0) {
                throw new AssertionError("story " + seed + " round " + round + ": the model latched "
                        + model.engine().feed().latchReason() + " on template " + FeedDigests.templateOf(frame) + " seq " + jseq
                        + " " + e.events());
            }
            answer.addAll(rev.reverse(frame));
            meFrames.add(frame);
            collect();
        }

        private long ledgerSize() {
            long n = 0;
            for (final TreeSet<Long> s : inflight.values()) {
                n += s.size();
            }
            return n;
        }

        private void outcome(final AssetsEngineEvent.MeDispatch d, final int result, final long leg, final int reason,
                             final int oldCancelled) {
            final long g1 = ++egress;
            me(enc.outcome(g1, ++jseq, g.genesisId(), d.commandIdHigh(), d.commandIdLow(), Math.max(1L, d.userId()),
                    Math.max(1L, d.omsOrderId()), d.oldOrderId(), d.price(), d.quantity(), d.budget(), Math.max(1, d.marketId()),
                    d.kind().code(), d.orderType(), d.side(), g1, feedTime(g.t0(), round), result <= 1 ? leg : 0L,
                    result == 0 ? 0 : result == 1 ? 4 : -1, reason, oldCancelled, result, d.timeInForce(),
                    d.expectedOldRemaining(), (int) ledgerSize(), CAPACITY, ME_BEHAVIOR));
            if (d.kind() == DispatchKind.AMEND && result == R_APPLIED) {
                cov.put("amendApplied", true);
            }
            if (d.kind() == DispatchKind.AMEND && result == R_REJECTED && oldCancelled == 1) {
                cov.put("amendRejectedOldCancelled", true);
            }
        }

        private void terminal(final long h, final long leg, final long user, final int status, final int reason) {
            me(enc.terminal(++egress, ++jseq, g.genesisId(), leg, user, MKT, status, feedTime(g.t0(), round), h, reason));
        }

        private void process(final AssetsEngineEvent.MeDispatch d) {
            final long h = d.commandIdHigh();
            final long low = d.commandIdLow();
            if (d.kind() == DispatchKind.RELEASE_ID) {
                release(h, low, d.releaseScope());
                return;
            }
            if (tomb.contains(h) || (floor.containsKey(h) && low <= floor.get(h))) {
                outcome(d, R_RELEASED, 0L, 0, 0);
                return;
            }
            final TreeSet<Long> rec = inflight.get(h);
            if (rec != null && rec.contains(low)) {
                return; // an equal-intent duplicate is suppressed, nothing journaled
            }
            inflight.computeIfAbsent(h, k -> new TreeSet<>()).add(low);
            final long[] leg = rest.get(h);
            switch (d.kind()) {
                case CREATE -> {
                    final long newLeg = ++nextLeg;
                    if (d.timeInForce() == 1 || d.timeInForce() == 2) {
                        terminal(h, newLeg, d.userId(), ME_CANCELLED, 12); // S5: the ME ends it inside the create
                        outcome(d, R_APPLIED, newLeg, 0, 0);
                    } else if (d.orderType() == 1) {
                        outcome(d, R_APPLIED, newLeg, 0, 0);
                        market(d, h, newLeg);
                    } else {
                        rest.put(h, new long[] {newLeg, d.quantity(), d.price(), d.side(), d.userId(), MKT});
                        outcome(d, R_APPLIED, newLeg, 0, 0);
                    }
                }
                case CANCEL -> {
                    if (leg != null && leg[0] == d.oldOrderId()) {
                        rest.remove(h);
                        terminal(h, leg[0], leg[4], ME_CANCELLED, 0);
                        outcome(d, R_APPLIED, leg[0], 0, 0);
                        if (partial.contains(h)) {
                            cov.put("partialFillThenCancel", true);
                        }
                    } else {
                        outcome(d, R_UNKNOWN_LEG, 0L, 0, 0);
                    }
                }
                default -> { // AMEND (S5: the old leg's remaining must equal expectedOldRemaining)
                    if (leg == null || leg[0] != d.oldOrderId()) {
                        outcome(d, R_UNKNOWN_LEG, 0L, 0, 0);
                    } else if (h == forcedRejectOldCancelled) {
                        rest.remove(h); // the ME cancelled the old leg and placed nothing (S7 D9a row 1)
                        outcome(d, R_REJECTED, leg[0], 0, 1);
                    } else if (leg[1] != d.expectedOldRemaining()) {
                        outcome(d, R_REJECTED, leg[0], 16, 0);
                    } else {
                        final long newLeg = ++nextLeg;
                        rest.put(h, new long[] {newLeg, d.quantity(), d.price(), leg[3], leg[4], MKT});
                        outcome(d, R_APPLIED, newLeg, 0, 0);
                    }
                }
            }
        }

        /** A MARKET leg walks the resting book of the other side (other users), bounded by quantity and budget. */
        private void market(final AssetsEngineEvent.MeDispatch d, final long h, final long leg) {
            long remaining = d.quantity();
            long budget = d.side() == 0 && d.budget() > 0 ? d.budget() : Long.MAX_VALUE;
            while (remaining > 0) {
                Map.Entry<Long, long[]> best = null;
                for (final Map.Entry<Long, long[]> e : rest.entrySet()) {
                    final long[] l = e.getValue();
                    if (l[3] == d.side() || l[4] == d.userId() || scripted(e.getKey())) {
                        continue;
                    }
                    if (best == null || (d.side() == 0 ? l[2] < best.getValue()[2] : l[2] > best.getValue()[2])) {
                        best = e;
                    }
                }
                if (best == null) {
                    break;
                }
                final long[] m = best.getValue();
                long qty = Math.min(remaining, m[1]);
                if (d.side() == 0) {
                    final long affordable = BigInteger.valueOf(budget).multiply(BigInteger.valueOf(ONE))
                            .divide(BigInteger.valueOf(m[2])).min(BigInteger.valueOf(Long.MAX_VALUE)).longValue()
                            / (ONE / 1000) * (ONE / 1000);
                    qty = Math.min(qty, affordable);
                    if (qty <= 0) {
                        break;
                    }
                    budget -= FixedPoint.multiply(m[2], qty);
                }
                if (noBatch() && answerEndsWithTrade()) {
                    break; // the seam: never two trade records back to back
                }
                tradeLegs(h, leg, d.userId(), d.side() == 0, best.getKey(), m, qty);
                remaining -= qty;
            }
            terminal(h, leg, d.userId(), remaining == 0 ? ME_FILLED : ME_CANCELLED, 0);
        }

        /** One trade: taker (h, leg) against the resting maker {@code m} of order {@code mh}. */
        private void tradeLegs(final long h, final long leg, final long user, final boolean takerIsBuy, final long mh,
                               final long[] m, final long qty) {
            me(enc.trade(++egress, ++jseq, g.genesisId(), ++nextTrade, MKT, leg, user, m[0], m[4], m[2], qty, takerIsBuy,
                    feedTime(g.t0(), round), h, mh));
            m[1] -= qty;
            if (m[1] == 0) {
                rest.remove(mh);
                terminal(mh, m[0], m[4], ME_FILLED, 0);
            } else {
                partial.add(mh);
            }
        }

        /**
         * STORY_NO_BATCH_RUN (review NOTE-3: one coverage item only): no trade record is ever journaled right behind
         * another, so no 62 can form; every other beat keeps its trades.
         */
        private boolean noBatch() {
            return FeedHarnessSeams.on(FeedHarnessSeams.STORY_NO_BATCH_RUN);
        }

        private boolean answerEndsWithTrade() {
            return !answer.isEmpty() && JournalRecords.templateId(answer.get(answer.size() - 1)) == 1;
        }

        /** Round 3: two partial fills of the big sell back to back (a 62-sized run). */
        private void forcedBatch() {
            if (noBatch()) {
                iceFill(); // its FILLED terminal separates it from the partial fill behind it
            }
            final long s = named.getOrDefault("S", 0L);
            final long b1 = named.getOrDefault("B1", 0L);
            final long b2 = named.getOrDefault("B2", 0L);
            if (rest.containsKey(s) && rest.containsKey(b1) && rest.containsKey(b2)) {
                meTrade(b1, s, q(5), false);
                meTrade(b2, s, q(5), false);
            }
            if (!noBatch()) {
                iceFill();
            }
        }

        /** The iceberg's first slice fills, so the core works a second slice. */
        private void iceFill() {
            final long ice = named.getOrDefault("ICE", 0L);
            final long ib = named.getOrDefault("IB", 0L);
            if (rest.containsKey(ice) && rest.containsKey(ib)) {
                meTrade(ib, ice, rest.get(ice)[1], false);
            }
        }

        /**
         * The ME trades two resting legs that cross (different users), the earlier leg the maker at its price; exhausted
         * legs journal FILLED. {@code buyH}/{@code sellH} force the pair, {@code forcedQty} the quantity.
         */
        private boolean meTrade(final long buyH, final long sellH, final long forcedQty, final boolean random) {
            if (noBatch() && answerEndsWithTrade()) {
                return false;
            }
            long bh = buyH;
            long sh = sellH;
            if (random) {
                bh = -1;
                sh = -1;
                final List<Long> hs = new ArrayList<>(rest.keySet());
                java.util.Collections.shuffle(hs, rnd);
                for (final long h : hs) {
                    final long[] l = rest.get(h);
                    if (scripted(h)) {
                        continue;
                    }
                    if (l[3] == 0 && bh < 0) {
                        bh = h;
                    } else if (l[3] == 1 && sh < 0) {
                        sh = h;
                    }
                }
                if (bh < 0 || sh < 0) {
                    return false;
                }
            }
            final long[] b = rest.get(bh);
            final long[] s = rest.get(sh);
            if (b == null || s == null || b[2] < s[2] || b[4] == s[4]) {
                return false;
            }
            long qty = forcedQty > 0 ? forcedQty : (1 + rnd.nextInt(5)) * (ONE / 10);
            qty = Math.min(qty, Math.min(b[1], s[1]));
            final boolean buyIsMaker = b[0] < s[0];
            final long[] maker = buyIsMaker ? b : s;
            final long mh = buyIsMaker ? bh : sh;
            final long[] taker = buyIsMaker ? s : b;
            final long th = buyIsMaker ? sh : bh;
            me(enc.trade(++egress, ++jseq, g.genesisId(), ++nextTrade, MKT, taker[0], taker[4], maker[0], maker[4], maker[2],
                    qty, !buyIsMaker, feedTime(g.t0(), round), th, mh));
            b[1] -= qty;
            s[1] -= qty;
            for (final long[] x : new long[][] {{bh}, {sh}}) {
                final long h = x[0];
                final long[] l = rest.get(h);
                if (l[1] == 0) {
                    rest.remove(h);
                    terminal(h, l[0], l[4], ME_FILLED, 0);
                } else {
                    partial.add(h);
                }
            }
            return true;
        }

        /** S4 4.2: a RELEASE_ID; an all-NOOP frame is never journaled (S4 4.3). */
        private void release(final long h, final long f, final ReleaseScope scope) {
            final int result;
            int removed = 0;
            if (scope == ReleaseScope.UP_TO) {
                if (tomb.contains(h) || (floor.containsKey(h) && f <= floor.get(h))) {
                    return;
                }
                final TreeSet<Long> s = inflight.get(h);
                if (s != null) {
                    removed = s.headSet(f, true).size();
                    s.headSet(f, true).clear();
                }
                floor.put(h, f);
                result = 0;
            } else {
                if (tomb.contains(h)) {
                    return;
                }
                if (rest.containsKey(h)) {
                    result = 2; // LEG_LIVE
                } else {
                    final TreeSet<Long> s = inflight.remove(h);
                    if (s != null) {
                        removed = s.size();
                    }
                    floor.remove(h);
                    tomb.add(h);
                    result = 0;
                }
            }
            final long js = ++jseq;
            me(enc.release(++egress, js, g.genesisId(), js, (int) ledgerSize(), CAPACITY, tomb.size(),
                    new long[] {h, f, scope == ReleaseScope.ORDER ? 1 : 0, result, removed}));
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.FixedPoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Harness B4: emits a production-shaped lifecycle scenario as DSL LINES for {@link AssetsScenarioRunner}, from a seed
 * alone (seeded {@link Random}, logical clock, no wall time). Only verbs the runner already has are used.
 *
 * <p>The generator never reads the engine. It predicts the core's decisions by mirroring its rules (S4 admission
 * order, S5 attempts and terminals, S6 trigger book and iceberg refill, S7 v2 shape). This class writes the commands;
 * {@link LifecycleFeed} with its {@link LifecycleFakeMe} plays the ME: every dispatch the generator can predict is
 * acked at once ({@code MEOUTCOME APPLIED}) and matched
 * price-time, producing {@code METRADE} with dense ids, {@code METERMINAL} and {@code MARKETTICK}. A misprediction
 * is not hidden: the engine counts it as a stale outcome and {@code ProductionShapeEquivalenceTest} asserts zero.</p>
 */
public final class LifecycleTrafficGenerator {

    /** Of seeds 1..400 all stay in step with the engine; 120 and 231 also cover every path the mix test names. */
    public static final long SEED = 231L;
    public static final int TARGET_LINES = 1500;
    public static final String NAME = "core_lifecycle_production_shape";

    static final long ONE = FixedPoint.SCALE_FACTOR;
    static final long TICK_SIZE = ONE / 100;
    static final long LOT = ONE / 10_000;
    static final int MARKETS = 5;
    private static final int USERS = 64;
    private static final int EARLY_USERS = 56;
    private static final long CLOCK0 = 1_700_000_000_000L;
    private static final long[] MID = {0, 60_000 * ONE, 3_000 * ONE, 150 * ONE, 250 * TICK_SIZE, 80 * TICK_SIZE};
    static final long[] STEP = {0, 20 * ONE, ONE, 10 * TICK_SIZE, TICK_SIZE, TICK_SIZE};
    private static final long[] UNIT = {0, ONE / 1_000, ONE / 100, ONE / 10, ONE, ONE};
    private static final long[] FUNDING = {1_000_000 * ONE, 50 * ONE, 500 * ONE, 5_000 * ONE, 200_000 * ONE, 500_000 * ONE};
    private static final long SLIP = ONE + 500L * 10_000L; // MarketRisk genesis: 500 bps market-buy slippage
    private static final String CONFIG_TAIL = " tick=0.01 min=0.01 max=1000000 lot=0.0001";

    /** What the generator emitted, by kind; the predictions the sync test holds the engine to. */
    public record Stats(int lines, int creates, int predictedAccepted, int outcomes, int trades, int duplicateTrades,
                        int journalTerminals, int earlyTerminals, int marketTicks, int staleTicks, int oneSidedTicks,
                        int cancels, int amends, int expiries, int triggers, int slices, int replays, int mismatches,
                        int withdrawals, int clockTicks, int openAtEnd) { }

    public record Generated(List<String> lines, Stats stats) { }

    private record CreateLine(String line, int market, long quantity) { }

    private final Random rnd;
    private final int target;
    private final List<String> out = new ArrayList<>();
    private final LifecycleFeed feed;
    private final List<CreateLine> acceptedCreates = new ArrayList<>();
    private final List<Long> users = new ArrayList<>();
    private final Map<Long, boolean[]> deposited = new HashMap<>();
    private final int[] collarBps = {2_000, 2_000, 2_000, 2_000, 2_000, 2_000};
    private final boolean[] milestoneDone = new boolean[8];
    private long now = CLOCK0;
    private int lines;
    private int sinceTick;
    private long nextOrderId = 1;
    private long ridSeq;
    private long cidSeq;
    private long corrSeq = 100;
    private boolean gateOpen = true;
    private int behavior = 1;
    private int creates, predictedAccepted, cancels, amends, expiries, replays, mismatches, withdrawals, clockTicks;

    private LifecycleTrafficGenerator(final long seed, final int target) {
        this.rnd = new Random(seed);
        this.target = target;
        this.feed = new LifecycleFeed(rnd, this::emit);
    }

    public static Generated generate(final long seed, final int targetLines) {
        final LifecycleTrafficGenerator g = new LifecycleTrafficGenerator(seed, targetLines);
        g.run(seed);
        final LifecycleFeed f = g.feed;
        return new Generated(List.copyOf(g.out), new Stats(g.lines, g.creates, g.predictedAccepted, f.outcomes, f.trades,
                f.duplicateTrades, f.journalTerminals, f.earlyTerminals, f.marketTicks, f.staleTicks, f.oneSidedTicks,
                g.cancels, g.amends, g.expiries, f.triggers, f.slices, g.replays, g.mismatches, g.withdrawals, g.clockTicks,
                f.live.size()));
    }

    /** The committed corpus file: seed {@link #SEED}, about {@link #TARGET_LINES} executable lines. */
    public static Generated production() {
        return generate(SEED, TARGET_LINES);
    }

    private void run(final long seed) {
        header(seed);
        emit("CLOCK " + CLOCK0);
        emit("GATE market=0 open=1");
        emit("CORECONFIG ver=1 behavior=1" + CONFIG_TAIL + " corr=1");
        for (int i = 0; i < USERS; i++) {
            users.add(100L + 7L * i);
        }
        Collections.shuffle(users, rnd); // first touch in a scrambled order, never ascending userId
        for (int i = 0; i < EARLY_USERS; i++) {
            deposit(users.get(i), 0);
            deposit(users.get(i), 1 + rnd.nextInt(MARKETS));
            maybeTick();
        }
        while (lines < target) {
            milestones();
            step();
            maybeTick();
        }
        tick();
    }

    private void header(final long seed) {
        out.add("# generated by LifecycleTrafficGenerator seed " + seed + ", regenerate do not edit");
        out.add("#");
        out.add("# RC1 S8 harness B4: the production-shaped lifecycle corpus. 64 users first touched in a scrambled order,");
        out.add("# deposits in all six assets, markets 1..5, CoreConfig v1 at the start and the v2 behaviour switch mid-stream");
        out.add("# (tick 0.01, band [0.01, 1000000], lot 0.0001), a gate close/open pair, two risk config changes, three");
        out.add("# SNAPSHOT restarts, and an order mix of LIMIT GTC/GTD, MARKET, LIMIT_MAKER, STOP_LOSS/STOP_LIMIT, ICEBERG and");
        out.add("# TRAILING_STOP with clientOrderId reuse after terminal, requestId replays and payload mismatches. A deterministic");
        out.add("# fake ME inside the generator acks every dispatch at once, matches price-time per market and journals trades");
        out.add("# (dense ids, 1% duplicated), terminals (a few before the last trade) and top-of-book ticks (2% stale, 2% one-");
        out.add("# sided). Regenerate: -Dtest=ProductionShapeEquivalenceTest -Dcore.scenario.regenerate=true, then refresh the");
        out.add("# golden: -Dtest=DeterminismCorpusTest -Dupdate.golden=true; review both diffs.");
        out.add("");
    }

    private void milestones() {
        final double f = (double) lines / target;
        milestone(0, f >= 0.20, "SNAPSHOT");
        milestone(1, f >= 0.30, "RISKCFG market=3 ver=1 maxOpen=400");
        if (milestone(2, f >= 0.40, "GATE market=0 open=0")) {
            gateOpen = false;
        }
        if (milestone(3, f >= 0.42, "GATE market=0 open=1")) {
            gateOpen = true;
        }
        if (milestone(4, f >= 0.48, "CORECONFIG ver=2 behavior=2" + CONFIG_TAIL + " corr=2")) {
            behavior = 2;
        }
        milestone(5, f >= 0.55, "SNAPSHOT");
        if (milestone(6, f >= 0.62, "RISKCFG market=1 ver=1 collarBps=2500")) {
            collarBps[1] = 2_500;
        }
        milestone(7, f >= 0.80, "SNAPSHOT");
    }

    private boolean milestone(final int i, final boolean due, final String line) {
        if (!due || milestoneDone[i]) {
            return false;
        }
        milestoneDone[i] = true;
        emit(line);
        return true;
    }

    private void step() {
        final int r = rnd.nextInt(1000);
        if (r < 12) {
            replay(true);
        } else if (r < 18) {
            replay(false);
        } else if (r < 168) {
            cancel();
        } else if (r < 208) {
            amend();
        } else if (r < 278) {
            depositOp();
        } else if (r < 328) {
            withdrawOp();
        } else if (r < 338) {
            query();
        } else {
            create();
        }
    }

    // ---------------------------------------------------------------- money and queries

    private void deposit(final long user, final int asset) {
        deposited.computeIfAbsent(user, k -> new boolean[6])[asset] = true;
        emit("DEPOSIT u=" + user + " asset=" + asset + " amt=" + fmt(FUNDING[asset]));
    }

    private void depositOp() {
        final List<Long> untouched = new ArrayList<>();
        for (final long u : users) {
            if (!deposited.containsKey(u)) {
                untouched.add(u);
            }
        }
        final long user = !untouched.isEmpty() && rnd.nextInt(3) == 0 ? untouched.get(0) : users.get(rnd.nextInt(USERS));
        deposit(user, rnd.nextInt(6));
    }

    private void withdrawOp() {
        final List<Long> touched = touched();
        final long user = touched.get(rnd.nextInt(touched.size()));
        final boolean[] d = deposited.get(user);
        int asset = rnd.nextInt(6);
        while (!d[asset]) {
            asset = (asset + 1) % 6;
        }
        final boolean tooLarge = rnd.nextInt(10) < 3;
        withdrawals++;
        emit("WITHDRAW u=" + user + " asset=" + asset + " amt=" + fmt(tooLarge ? FUNDING[asset] * 50 : FUNDING[asset] / 1_000));
    }

    private void query() {
        final List<Long> touched = touched();
        if (rnd.nextInt(10) < 7) {
            emit("ORDERSNAP u=" + touched.get(rnd.nextInt(touched.size())) + " corr=" + (++corrSeq));
        } else {
            emit("PENDINGDISPATCH corr=" + (++corrSeq));
        }
    }

    // ---------------------------------------------------------------- CreateOrder

    private void create() {
        final int m = 1 + rnd.nextInt(MARKETS);
        final int side = rnd.nextInt(2);
        final boolean buy = side == 0;
        final int t = rnd.nextInt(100);
        final int type = t < 65 ? LifecycleModelOrder.LIMIT : t < 75 ? LifecycleModelOrder.MARKET
                : t < 83 ? LifecycleModelOrder.LIMIT_MAKER : t < 90 ? (rnd.nextBoolean() ? LifecycleModelOrder.STOP_LOSS
                : LifecycleModelOrder.STOP_LIMIT) : t < 95 ? LifecycleModelOrder.ICEBERG : LifecycleModelOrder.TRAILING;
        final boolean gtd = t >= 55 && t < 65;
        final int d = rnd.nextInt(1000);
        final boolean missingRid = d < 5;
        final boolean fok = d >= 5 && d < 8 && type == LifecycleModelOrder.LIMIT;
        final boolean offTick = d >= 8 && d < 28 && type == LifecycleModelOrder.LIMIT;
        final boolean collar = d >= 28 && d < 38 && type == LifecycleModelOrder.LIMIT && feed.lastPx[m] > 0;
        final boolean huge = d >= 38 && d < 43 && type == LifecycleModelOrder.LIMIT && buy && m <= 2;
        final boolean unfundedSell = d >= 43 && d < 48 && !buy;
        final boolean dupCid = d >= 48 && d < 51;
        final boolean unknownUser = d >= 51 && d < 54 && buy;
        final boolean offLot = d >= 54 && d < 64 && type == LifecycleModelOrder.LIMIT;
        final Long picked = unknownUser ? untouchedUser() : pickUser(m, buy, unfundedSell);
        if (picked == null) {
            depositOp();
            return;
        }
        final long user = picked;
        long qty = UNIT[m] * (1 + rnd.nextInt(40));
        long px = 0;
        long stop = 0;
        long delta = 0;
        long display = 0;
        long budget = 0;
        switch (type) {
            case LifecycleModelOrder.LIMIT:
                px = collar ? roundTick(feed.lastPx[m] * 13 / 10) : randomPrice(m) + (offTick ? TICK_SIZE / 2 : 0);
                if (huge) {
                    qty = UNIT[m] * 20_000_000L / (m == 1 ? 1 : 10);
                } else if (offLot) {
                    qty += LOT / 2;
                }
                break;
            case LifecycleModelOrder.LIMIT_MAKER:
                px = rnd.nextInt(5) == 0 ? randomPrice(m) : MID[m] + (buy ? -1 : 1) * STEP[m] * (1 + rnd.nextInt(5));
                break;
            case LifecycleModelOrder.MARKET:
                budget = buy && rnd.nextBoolean() ? FixedPoint.multiply(MID[m] + STEP[m] * rnd.nextInt(6), qty) : 0;
                break;
            case LifecycleModelOrder.STOP_LOSS:
            case LifecycleModelOrder.STOP_LIMIT: {
                final long ref = buy ? (feed.ask[m] > 0 ? feed.ask[m] : MID[m]) : (feed.bid[m] > 0 ? feed.bid[m] : MID[m]);
                final long away = STEP[m] * (rnd.nextInt(10) == 0 ? -1 : 1 + rnd.nextInt(4)); // one in ten already crossed
                stop = buy ? ref + away : ref - away;
                if (type == LifecycleModelOrder.STOP_LIMIT) {
                    px = buy ? stop + STEP[m] * rnd.nextInt(3) : stop - STEP[m] * rnd.nextInt(3);
                } else if (buy && rnd.nextBoolean()) {
                    budget = FixedPoint.multiply(stop + 2 * STEP[m], qty);
                }
                break;
            }
            case LifecycleModelOrder.TRAILING:
                delta = STEP[m] * (1 + rnd.nextInt(3));
                budget = buy && rnd.nextBoolean() ? FixedPoint.multiply(MID[m] + 10 * STEP[m], qty) : 0;
                break;
            default: { // ICEBERG
                final int units = 4 + rnd.nextInt(37);
                qty = UNIT[m] * units;
                display = UNIT[m] * (1 + rnd.nextInt(units / 2));
                px = randomPrice(m);
                break;
            }
        }
        // log-uniform over 100 ms .. 30 s, so short deadlines expire mid-stream and long ones outlive it
        final long exp = gtd ? now + Math.min(30_000L, Math.round(100.0 * Math.pow(300.0, rnd.nextDouble()))) : 0L;
        long cid = 0;
        if (dupCid && !feed.activeCids.getOrDefault(user, Set.of()).isEmpty()) {
            cid = new TreeSet<>(feed.activeCids.get(user)).first();
        } else if (rnd.nextInt(10) == 0) {
            final List<Long> free = feed.freeCids.getOrDefault(user, List.of());
            cid = !free.isEmpty() && rnd.nextBoolean() ? free.get(rnd.nextInt(free.size())) : ++cidSeq;
        }
        final long rid = missingRid ? 0 : ++ridSeq;
        final String line = LifecycleFeed.createLine(user, m, side, type, px, qty, budget, fok ? 2 : gtd ? 3 : 0, exp, stop, delta,
                display, cid, rid);
        creates++;
        emit(line);

        // the core's admission, in its own order (S4 chain, S6 section 1, S7 section 5)
        if (fok || missingRid || !gateOpen || shapeRefused(px, stop, qty)
                || (cid != 0 && feed.activeCids.getOrDefault(user, Set.of()).contains(cid))) {
            return; // structural or pre-rate: nothing stored, no order
        }
        long hold = buy ? (px > 0 ? FixedPoint.multiply(px, qty) : budget) : qty;
        if (buy && px == 0 && budget == 0) {
            final long base;
            if (type == LifecycleModelOrder.STOP_LOSS) {
                base = stop;
            } else if (feed.ask[m] <= 0) {
                return; // NO_REFERENCE_PRICE (stored)
            } else {
                base = type == LifecycleModelOrder.TRAILING ? feed.ask[m] + delta : feed.ask[m];
            }
            hold = FixedPoint.multiply(FixedPoint.multiply(base, SLIP), qty);
        }
        if (px > 0 && feed.lastPx[m] > 0 && Math.abs(px - feed.lastPx[m]) > bps(feed.lastPx[m], collarBps[m])) {
            return; // PRICE_COLLAR
        }
        if ((type == LifecycleModelOrder.STOP_LOSS || type == LifecycleModelOrder.STOP_LIMIT)
                && (buy ? feed.ask[m] > 0 && feed.ask[m] >= stop : feed.bid[m] > 0 && feed.bid[m] <= stop)) {
            return; // STOP_WOULD_TRIGGER_IMMEDIATELY
        }
        if (unknownUser || huge || (!buy && !has(user, m))) {
            return; // INSUFFICIENT_FUNDS: no account, or a hold the balance cannot cover
        }
        final LifecycleModelOrder o = new LifecycleModelOrder(nextOrderId++, user, m, side, type);
        predictedAccepted++;
        o.price = px;
        o.quantity = qty;
        o.stop = stop;
        o.delta = delta;
        o.hold = hold;
        o.exp = exp;
        o.cid = cid;
        feed.live.put(o.orderId, o);
        if (cid != 0) {
            feed.activeCids.computeIfAbsent(user, k -> new HashSet<>()).add(cid);
            feed.freeCids.getOrDefault(user, new ArrayList<>()).remove(Long.valueOf(cid));
        }
        if (rid > 0) {
            acceptedCreates.add(new CreateLine(line, m, qty));
        }
        if (type == LifecycleModelOrder.STOP_LOSS || type == LifecycleModelOrder.STOP_LIMIT
                || type == LifecycleModelOrder.TRAILING) {
            o.armed = true;
            final long quote = buy ? feed.ask[m] : feed.bid[m];
            o.trailArm = type == LifecycleModelOrder.TRAILING && quote > 0 ? quote : 0L;
            return;
        }
        o.meType = type == LifecycleModelOrder.ICEBERG ? LifecycleModelOrder.LIMIT : type;
        o.legPrice = px;
        o.legBudget = buy && type == LifecycleModelOrder.MARKET ? hold : 0L;
        if (type == LifecycleModelOrder.ICEBERG) {
            o.display = display;
            o.sliceRemaining = display;
            o.hidden = qty - display;
            o.legRemaining = display;
        } else {
            o.legRemaining = qty;
        }
        if (rnd.nextInt(200) == 0 || (type == LifecycleModelOrder.LIMIT_MAKER && feed.me.wouldCross(o))) {
            feed.outcomes++;
            emit("MEOUTCOME order=" + o.orderId + " kind=0 attempt=0 result=1"); // the ME refused the create
            feed.terminal(o);
            return;
        }
        feed.ack(o);
        feed.work(o);
        feed.publish(m);
    }

    /** 2% exact requestId replays and 1% same requestId with another payload: never a new order. */
    private void replay(final boolean exact) {
        if (acceptedCreates.isEmpty()) {
            create();
            return;
        }
        final CreateLine c = acceptedCreates.get(rnd.nextInt(acceptedCreates.size()));
        if (exact) {
            replays++;
            emit(c.line());
        } else {
            mismatches++;
            emit(c.line().replace(" qty=" + fmt(c.quantity()) + " ", " qty=" + fmt(c.quantity() + UNIT[c.market()]) + " "));
        }
    }

    // ---------------------------------------------------------------- CancelOrder, AmendOrder

    private void cancel() {
        final String rid = rnd.nextBoolean() ? " rid=" + (++ridSeq) : "";
        if (feed.live.isEmpty() || rnd.nextInt(10) == 0) {
            if (!feed.terminated.isEmpty()) {
                final LifecycleModelOrder gone = feed.terminated.get(rnd.nextInt(feed.terminated.size()));
                emit("CANCEL u=" + gone.user + " order=" + gone.orderId + rid); // UNKNOWN_ORDER
            }
            return;
        }
        final LifecycleModelOrder o = randomLive(null);
        if (rnd.nextInt(50) == 0) {
            final List<Long> touched = touched();
            final long other = touched.get(rnd.nextInt(touched.size()));
            if (other != o.user) {
                emit("CANCEL u=" + other + " order=" + o.orderId + rid); // NOT_OWNER
                return;
            }
        }
        cancels++;
        emit(o.cid != 0 && rnd.nextInt(3) == 0 ? "CANCEL u=" + o.user + " cid=" + o.cid + rid
                : "CANCEL u=" + o.user + " order=" + o.orderId + rid);
        if (o.armed) {
            feed.terminal(o); // an armed order ends in the cancel apply, no ME command
            return;
        }
        o.attempt++;
        final long leg = o.leg;
        feed.outcomes++;
        emit("MEOUTCOME order=" + o.orderId + " kind=1 attempt=" + o.attempt + " result=0");
        feed.terminal(o);
        if (rnd.nextBoolean()) {
            feed.journalTerminals++;
            emit("METERMINAL order=" + o.orderId + " leg=" + leg + " status=3"); // the ME's own journal: stale by now
        }
        feed.publish(o.market);
    }

    private void amend() {
        final LifecycleModelOrder o = randomLive(c -> !c.armed
                && (c.type == LifecycleModelOrder.LIMIT || c.type == LifecycleModelOrder.LIMIT_MAKER));
        if (o == null) {
            create();
            return;
        }
        final int m = o.market;
        final long newQty = o.filled + UNIT[m] * (1 + rnd.nextInt(40));
        final long newPx = o.type == LifecycleModelOrder.LIMIT && rnd.nextInt(10) < 6 ? randomPrice(m) : 0L;
        final long effPx = newPx == 0 ? o.price : newPx;
        amends++;
        emit("AMEND u=" + o.user + " order=" + o.orderId + (newPx > 0 ? " px=" + fmt(newPx) : "") + " qty=" + fmt(newQty)
                + " rid=" + (++ridSeq));
        if (!gateOpen || shapeRefused(effPx, 0L, newQty)
                || (feed.lastPx[m] > 0 && Math.abs(effPx - feed.lastPx[m]) > bps(feed.lastPx[m], collarBps[m]))) {
            return; // GATE_CLOSED, v2 shape, PRICE_COLLAR: the order stays as it was
        }
        o.attempt++;
        feed.me.remove(o);
        o.price = effPx;
        o.quantity = newQty;
        o.leg = ++feed.legSeq;
        o.legPrice = effPx;
        o.legRemaining = newQty - o.filled;
        feed.outcomes++;
        emit("MEOUTCOME order=" + o.orderId + " kind=2 attempt=" + o.attempt + " result=0 leg=" + o.leg);
        feed.work(o);
        feed.publish(m);
    }

    // ---------------------------------------------------------------- time

    private void maybeTick() {
        if (sinceTick >= 7) {
            tick();
        }
    }

    /** Advance the logical clock (the sweep runs in the TICK apply) and resolve what it expired. */
    private void tick() {
        final long dt = 13 + rnd.nextInt(238);
        now += dt;
        clockTicks++;
        emit("TICK " + dt);
        sinceTick = 0;
        final List<LifecycleModelOrder> due = new ArrayList<>();
        for (final LifecycleModelOrder o : feed.live.values()) {
            if (o.exp > 0 && o.exp <= now) {
                due.add(o);
            }
        }
        due.sort((x, y) -> x.exp != y.exp ? Long.compare(x.exp, y.exp) : Long.compare(x.orderId, y.orderId));
        final Set<Integer> touchedMarkets = new TreeSet<>();
        for (final LifecycleModelOrder o : due) {
            expiries++;
            if (!o.armed) {
                o.attempt++;
                feed.outcomes++;
                emit("MEOUTCOME order=" + o.orderId + " kind=1 attempt=" + o.attempt + " result=0"); // the expiry's cancel
                touchedMarkets.add(o.market);
            }
            feed.terminal(o);
        }
        for (final int m : touchedMarkets) {
            feed.publish(m);
        }
    }

    // ---------------------------------------------------------------- helpers

    private boolean shapeRefused(final long px, final long stop, final long qty) {
        if (behavior < 2) {
            return false;
        }
        return (px > 0 && (px < TICK_SIZE || px > 1_000_000 * ONE || px % TICK_SIZE != 0))
                || (stop > 0 && stop % TICK_SIZE != 0) || qty % LOT != 0;
    }

    private boolean has(final long user, final int asset) {
        return deposited.getOrDefault(user, new boolean[6])[asset] || feed.received.getOrDefault(user, new boolean[6])[asset];
    }

    /** A funded user for the side (USD for a BUY, the base for a SELL), or for {@code unfunded} a seller without it. */
    private Long pickUser(final int m, final boolean buy, final boolean unfunded) {
        final List<Long> c = new ArrayList<>();
        for (final long u : users) {
            final boolean[] d = deposited.get(u);
            if (d == null) {
                continue;
            }
            if (unfunded ? !has(u, m) : buy ? d[0] : d[m]) {
                c.add(u);
            }
        }
        return c.isEmpty() ? null : c.get(rnd.nextInt(c.size()));
    }

    private Long untouchedUser() {
        for (final long u : users) {
            if (!deposited.containsKey(u)) {
                return u;
            }
        }
        return null;
    }

    private List<Long> touched() {
        final List<Long> t = new ArrayList<>();
        for (final long u : users) {
            if (deposited.containsKey(u)) {
                t.add(u);
            }
        }
        return t;
    }

    private LifecycleModelOrder randomLive(final java.util.function.Predicate<LifecycleModelOrder> filter) {
        final List<LifecycleModelOrder> c = new ArrayList<>();
        for (final LifecycleModelOrder o : feed.live.values()) {
            if (filter == null || filter.test(o)) {
                c.add(o);
            }
        }
        return c.isEmpty() ? null : c.get(rnd.nextInt(c.size()));
    }

    private long randomPrice(final int m) {
        return MID[m] + STEP[m] * (rnd.nextInt(11) - 5);
    }

    private static long roundTick(final long px) {
        return px - px % TICK_SIZE;
    }

    /** floor(value * bps / 10_000) without overflow (OrderEngine.bps). */
    static long bps(final long value, final int bps) {
        return (value / 10_000L) * bps + ((value % 10_000L) * bps) / 10_000L;
    }

    /** A fixed-point amount as the shortest decimal that parses back to it exactly. */
    static String fmt(final long raw) {
        final String s = FixedPoint.format(raw);
        int end = s.length();
        while (s.charAt(end - 1) == '0' && s.charAt(end - 2) != '.') {
            end--;
        }
        return s.substring(0, end);
    }

    private void emit(final String line) {
        out.add(line);
        lines++;
        sinceTick++;
    }
}

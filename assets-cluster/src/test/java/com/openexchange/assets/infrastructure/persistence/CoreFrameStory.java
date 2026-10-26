// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.auth.CorePrincipal;
import com.openexchange.assets.infrastructure.generated.SessionClass;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * RC1 S8 (harness B4 test 3): a seeded, coherent lifecycle story as a list of LOG ENTRIES, each one encoded once
 * with the production encoders ({@link CoreFrameEncoders}) and stamped with an advancing log time and position.
 *
 * <p>The builder drives its own {@link FrameReplica} while it writes: a fake ME reads that replica's state (pending
 * dispatches with their attempt and kind, learned legs, remaining quantities, hold remaining) to answer every
 * dispatch with a well-formed outcome, journal trade or terminal, so the story stays coherent (fills hit live legs,
 * outcomes name the pending attempt). Coverage: CoreConfig adopt / duplicate / v2 switch / stale; gates; risk
 * config; breaker; registered GATEWAY and FORWARDER sessions plus unauthorised traffic; deposits and withdrawals
 * with requestIds (replay and mismatch); CreateOrder of every type and TIF; cancel; amend (and its replay); ticks
 * (stale, one-sided, triggering); outcomes APPLIED / REJECTED / terminal-first; single and batched trades with a
 * re-delivered tradeId; stale terminals; quarantine; both queries; sweeps that cross GTD deadlines.</p>
 */
final class CoreFrameStory {

    static final long TRUSTED = -1L;
    static final long ONE = 100_000_000L;
    static final long T0 = 1_700_000_000_000L;
    static final long GATEWAY_SESSION = 500L;
    static final long FORWARDER_SESSION = 501L;
    static final long UNREGISTERED_BRIDGE = 502L;
    static final int USERS = 10;

    enum Kind { FRAME, SWEEP, OPEN_SESSION }

    /** One log entry: a session message (frame), a TimerEvent (sweep) or a SessionOpen (principal bytes). */
    record Step(Kind kind, byte[] bytes, long timestamp, long sessionId, long logPosition) {
        int templateId() {
            return kind == Kind.FRAME && bytes.length >= 4 ? (bytes[2] & 0xFF) | (bytes[3] & 0xFF) << 8 : -1;
        }
    }

    static List<Step> build(final long seed, final int frames) {
        return new CoreFrameStory(seed).run(frames);
    }

    private final Random rnd;
    private final FrameReplica model = new FrameReplica();
    private final CoreFrameEncoders enc = new CoreFrameEncoders();
    private final List<Step> steps = new ArrayList<>();
    private long now = T0;
    private long position;
    private int frameCount;
    private long corr = 1;
    private long request = 1;
    private long legSeq = 10_000;
    private long tradeId;
    private long egress;
    private final long[] mid = new long[6];
    private final long[] mid0 = new long[6];
    private final long[] bookVersion = new long[6];
    private byte[] lastCreate;
    private byte[] lastAmend;
    private CoreFrameEncoders.Trade lastTrade;
    private int scheduled;

    private CoreFrameStory(final long seed) {
        this.rnd = new Random(seed);
        for (int m = 1; m <= 5; m++) {
            mid0[m] = mid[m] = 100L * m * ONE;
        }
    }

    // ---------------------------------------------------------------- emission

    private void frame(final byte[] bytes, final long session) {
        position += 64 + ((bytes.length + 31) & ~31);
        final Step s = new Step(Kind.FRAME, bytes, now, session, position);
        model.apply(s);
        steps.add(s);
        frameCount++;
    }

    private void frame(final byte[] bytes) {
        frame(bytes, TRUSTED);
    }

    private void sweep() {
        position += 64;
        final Step s = new Step(Kind.SWEEP, new byte[0], now, TRUSTED, position);
        model.apply(s);
        steps.add(s);
    }

    private void open(final long session, final CorePrincipal principal) {
        position += 64;
        final Step s = new Step(Kind.OPEN_SESSION, principal.encode(), now, session, position);
        model.apply(s);
        steps.add(s);
    }

    private void advance(final long ms) {
        now += ms;
    }

    // ---------------------------------------------------------------- the story

    private List<Step> run(final int target) {
        setup();
        while (frameCount < target) {
            schedule();
            advance(1 + rnd.nextInt(30));
            final int r = rnd.nextInt(100);
            if (r < 24) {
                create(0, 0, false);
            } else if (r < 46) {
                meRespond();
            } else if (r < 60) {
                trade();
            } else if (r < 65) {
                cancel();
            } else if (r < 69) {
                amend();
            } else if (r < 78) {
                tick(1 + rnd.nextInt(5), 0);
            } else if (r < 82) {
                advance(rnd.nextInt(4) == 0 ? 800 + rnd.nextInt(2500) : 50 + rnd.nextInt(350));
                sweep();
            } else if (r < 87) {
                money();
            } else if (r < 90) {
                query();
            } else if (r < 91) {
                quarantine(false);
            } else if (r < 95) {
                meTerminal();
            } else {
                unauthorised();
            }
        }
        advance(5_000);
        sweep(); // every GTD deadline still open is crossed
        for (int i = 0; i < 12; i++) {
            advance(5);
            meRespond();
        }
        return steps;
    }

    private void setup() {
        frame(enc.coreConfig(corr++, 1, 1, 1, 1, 1_000_000_000_000_000L, 0)); // ADOPT
        frame(enc.coreConfig(corr++, 1, 1, 1, 1, 1_000_000_000_000_000L, 0)); // DUPLICATE
        frame(enc.gate(corr++, 0, true));
        for (int m = 1; m <= 5; m++) {
            frame(enc.riskConfig(corr++, 1, m, 200, 40, 2_000));
        }
        frame(enc.riskConfig(corr++, 1, 9, 200, 40, 2_000)); // unknown market: REJECTED
        open(GATEWAY_SESSION, CorePrincipal.forUsers(SessionClass.GATEWAY, 7, 1, 2, 3, 4, 5, 6));
        open(FORWARDER_SESSION, CorePrincipal.anyUser(SessionClass.FORWARDER, 8));
        open(UNREGISTERED_BRIDGE, CorePrincipal.anyUser(SessionClass.BRIDGE, 9));
        frame(enc.register(corr++, SessionClass.GATEWAY), GATEWAY_SESSION);
        frame(enc.register(corr++, SessionClass.FORWARDER), FORWARDER_SESSION);
        for (int u = 1; u <= USERS; u++) {
            advance(1);
            frame(enc.deposit(corr++, u, 0, 1_000_000L * ONE, 0xD0L, request++));
            frame(enc.deposit(corr++, u, baseFor(u), 10_000L * ONE, 0xD0L, request++));
        }
        final byte[] replay = enc.deposit(corr, 3, 0, 5L * ONE, 0xD0L, request);
        frame(replay);
        frame(replay); // same requestId, same payload: duplicate ack
        frame(enc.deposit(corr++, 3, 0, 6L * ONE, 0xD0L, request++)); // same requestId, other payload: mismatch
        for (int m = 1; m <= 5; m++) {
            tick(m, 0);
        }
    }

    /** One forced story beat per threshold, so every lifecycle path appears whatever the seed. */
    private void schedule() {
        final int[] at = {40, 55, 70, 90, 100, 104, 120, 125, 140, 141, 150, 165, 170, 185, 200, 205, 215, 230};
        if (scheduled >= at.length || frameCount < at[scheduled]) {
            return;
        }
        switch (scheduled++) {
            case 0 -> create(0, 1, true);                   // short GTD on market 1
            case 1 -> create(6, 1, false);                  // iceberg
            case 2 -> { create(0, 1, false); for (int i = 0; i < 3; i++) { meRespond(); trade(); } }
            case 3 -> create(6, 2, false);
            case 4 -> create(3, 2, false);                  // SELL or BUY stop on market 2
            case 5 -> { tick(2, -6); tick(2, 12); staleTick(2); } // a big move each way (the stop fires), then a stale copy
            case 6 -> frame(enc.coreConfig(corr++, 2, 2, 1, 1, 1_000_000_000_000_000L, 0)); // v2 switch
            case 7 -> frame(enc.coreConfig(corr++, 1, 1, 1, 1, 1_000_000_000_000_000L, 0)); // STALE
            case 8 -> quarantine(false);
            case 9 -> quarantine(true);                     // unknown order: REJECTED
            case 10 -> frame(enc.riskConfig(corr++, 2, 2, 150, 40, 2_500));
            case 11 -> frame(enc.breaker(corr++, 3, true));
            case 12 -> create(0, 3, false);                 // CIRCUIT_BREAKER
            case 13 -> frame(enc.breaker(corr++, 3, false));
            case 14 -> frame(enc.gate(corr++, 4, false));
            case 15 -> create(0, 4, false);                 // GATE_CLOSED
            case 16 -> frame(enc.gate(corr++, 4, true));
            case 17 -> { create(5, 1, false); tick(1, 3); tick(1, -8); }
            default -> { }
        }
    }

    // ---------------------------------------------------------------- order entry

    private static int baseFor(final int user) {
        return (user - 1) % 5 + 1;
    }

    private long step(final int m) {
        return mid0[m] / 200;
    }

    private long units(final long tenths) {
        return tenths * ONE / 10;
    }

    /** CreateOrder; {@code type <= 0} draws one; {@code market == 0} draws one; {@code gtd} forces a short GTD LIMIT. */
    private void create(final int forcedType, final int market, final boolean gtd) {
        final int m = market > 0 ? market : 1 + rnd.nextInt(5);
        final int side = rnd.nextInt(2);
        final int user = side == 1 ? (rnd.nextBoolean() ? m : m + 5) : 1 + rnd.nextInt(USERS);
        int type = forcedType;
        if (type <= 0 && !gtd) {
            final int w = rnd.nextInt(100);
            type = w < 45 ? 0 : w < 55 ? 1 : w < 63 ? 2 : w < 73 ? 3 : w < 81 ? 4 : w < 89 ? 5 : 6;
        }
        final long st = step(m);
        final long bid = mid[m] - st;
        final long ask = mid[m] + st;
        long price = 0, budget = 0, stop = 0, delta = 0, display = 0, exp = 0;
        long qty = units(1 + rnd.nextInt(20));
        int tif = 0;
        switch (type) {
            case 0, 2 -> {
                final int k = rnd.nextInt(6) - (type == 2 ? 0 : 3);
                price = side == 0 ? mid[m] - k * st : mid[m] + k * st;
                if (type == 2) {
                    price = side == 0 ? bid - (1 + k) * st : ask + (1 + k) * st; // never crossing
                }
                final int t = rnd.nextInt(10);
                if (gtd || (type == 0 && t < 2)) {
                    tif = 3;
                    exp = now + 200 + rnd.nextInt(2_500);
                } else if (type == 0 && t == 2) {
                    tif = 1; // IOC
                }
            }
            case 1 -> budget = side == 0 && rnd.nextBoolean() ? mul(mul(qty, ask), ONE + ONE / 10) : 0;
            case 3, 4 -> {
                stop = side == 1 ? bid - (1 + rnd.nextInt(3)) * st : ask + (1 + rnd.nextInt(3)) * st;
                if (type == 4) {
                    price = side == 1 ? stop - st : stop + st;
                } else if (side == 0 && rnd.nextBoolean()) {
                    budget = mul(mul(qty, stop), ONE + ONE / 10);
                }
            }
            case 5 -> {
                delta = (1 + rnd.nextInt(3)) * st;
                if (side == 0 && rnd.nextBoolean()) {
                    budget = mul(mul(qty, ask + delta), ONE + ONE / 10);
                }
            }
            default -> {
                price = side == 0 ? mid[m] - rnd.nextInt(3) * st : mid[m] + rnd.nextInt(3) * st;
                display = units(1 + rnd.nextInt(3));
                qty = display * (2 + rnd.nextInt(3));
            }
        }
        final long cid = rnd.nextInt(10) == 0 ? 77_000 + request : 0;
        if (lastCreate != null && rnd.nextInt(30) == 0) {
            frame(lastCreate); // exact replay: same requestId, same payload
            return;
        }
        final long rl = rnd.nextInt(100) == 0 && request > 2 ? request - 1 : request++; // 1%: requestId reused, other payload
        final byte[] bytes = enc.create(corr++, user, cid, 0x5EEDL, rl, m, side, type, tif, price, qty, budget, stop, delta,
                display, exp);
        lastCreate = bytes;
        frame(bytes, user <= 6 && rnd.nextInt(3) == 0 ? GATEWAY_SESSION : TRUSTED);
    }

    private void cancel() {
        final List<CoreOrder> live = live(o -> true);
        if (live.isEmpty() || rnd.nextInt(20) == 0) {
            frame(enc.cancel(corr++, 1 + rnd.nextInt(USERS), 900_000 + rnd.nextInt(10), 0x5EEDL, request++)); // unknown
            return;
        }
        final CoreOrder o = live.get(rnd.nextInt(live.size()));
        frame(enc.cancel(corr++, o.userId, o.orderId, 0x5EEDL, request++), o.userId <= 6 && rnd.nextBoolean() ? GATEWAY_SESSION : TRUSTED);
    }

    private void amend() {
        if (lastAmend != null && rnd.nextInt(8) == 0) {
            frame(lastAmend); // requestId replay
            return;
        }
        final List<CoreOrder> c = live(o -> (o.orderType == 0 || o.orderType == 2) && o.currentLegId != 0
                && !o.has(CoreOrder.F_DISPATCH_PENDING) && !o.has(CoreOrder.F_CANCEL_PENDING)
                && !o.has(CoreOrder.F_REPLACE_PENDING) && !o.has(CoreOrder.F_QUARANTINED));
        if (c.isEmpty()) {
            create(0, 0, false);
            return;
        }
        final CoreOrder o = c.get(rnd.nextInt(c.size()));
        final long st = step(o.marketId);
        long px = rnd.nextBoolean() ? 0 : o.price + (rnd.nextBoolean() ? st : -st);
        long qty = rnd.nextBoolean() ? 0 : o.filledQty + units(1 + rnd.nextInt(20));
        if (px == 0 && qty == 0) {
            qty = o.filledQty + units(1 + rnd.nextInt(20));
        }
        lastAmend = enc.amend(corr++, o.userId, o.orderId, 0xA3EDL, request++, px, qty);
        frame(lastAmend);
    }

    // ---------------------------------------------------------------- the fake ME

    private void meRespond() {
        final List<CoreOrder> pending = live(o -> o.has(CoreOrder.F_DISPATCH_PENDING));
        if (pending.isEmpty()) {
            create(0, 0, false);
            return;
        }
        pending.sort(Comparator.comparingLong(o -> o.pendingDispatchSeq));
        final CoreOrder o = rnd.nextInt(4) == 0 ? pending.get(rnd.nextInt(pending.size())) : pending.get(0);
        final int kind = o.pendingKind - 1; // PK_CREATE 1 / PK_CANCEL 2 / PK_AMEND 3 -> wire CREATE 0 / CANCEL 1 / AMEND 2
        final long cmdLow = ((long) o.attempt << 2) | kind;
        final int p = rnd.nextInt(100);
        if (kind == 0) {
            if (p < 88) {
                final byte[] b = outcome(o, cmdLow, kind, 0L, ++legSeq, 0, 0);
                frame(b);
                if (rnd.nextInt(10) == 0) {
                    frame(b); // re-delivery: a counted no-op
                }
            } else if (p < 95) {
                frame(outcome(o, cmdLow, kind, 0L, 0L, 3, 1)); // REJECTED by the ME
            } else {
                frame(enc.terminal(++egress, ++legSeq, o.userId, o.marketId, 4, now, o.orderId)); // terminal first (G9)
            }
        } else if (kind == 1) {
            if (p < 70) {
                frame(outcome(o, cmdLow, kind, o.currentLegId, o.currentLegId, 0, 0));
            } else if (p < 90) {
                frame(enc.terminal(++egress, o.currentLegId, o.userId, o.marketId, 3, now, o.orderId));
            } else {
                frame(outcome(o, cmdLow, kind, o.currentLegId, o.currentLegId, 2, 1)); // refused: flag kept
            }
        } else {
            frame(p < 80 ? outcome(o, cmdLow, kind, o.oldLegId, ++legSeq, 0, 0) : outcome(o, cmdLow, kind, o.oldLegId, 0L, 2, 1));
        }
    }

    private byte[] outcome(final CoreOrder o, final long cmdLow, final int kind, final long old, final long leg, final int reason,
                           final int result) {
        return enc.outcome(++egress, o.orderId, cmdLow, o.userId, o.orderId, old, o.price, o.remainingQty, o.budget, o.marketId,
                kind, o.orderType, o.side, now, leg, reason, result);
    }

    /** A cancel the ME settles by its journal terminal (after a refused cancel), or a stale terminal for a gone leg. */
    private void meTerminal() {
        final List<CoreOrder> c = live(o -> o.has(CoreOrder.F_CANCEL_PENDING) && !o.has(CoreOrder.F_DISPATCH_PENDING)
                && o.currentLegId != 0);
        if (!c.isEmpty()) {
            final CoreOrder o = c.get(0);
            frame(enc.terminal(++egress, o.currentLegId, o.userId, o.marketId, 3, now, o.orderId));
        } else {
            frame(enc.terminal(++egress, 10_000 + rnd.nextInt(50), 1, 1, 2, now, 800_000 + rnd.nextInt(50)));
        }
    }

    private static boolean tradable(final CoreOrder o) {
        return o.currentLegId != 0 && o.pendingKind != CoreOrder.PK_CREATE && (o.status == 0 || o.status == 1 || o.status == 9);
    }

    private static long limitOf(final CoreOrder o) {
        return o.orderType == 0 || o.orderType == 2 || o.orderType == 4 || o.orderType == 6 ? o.price : 0L;
    }

    private void trade() {
        final int m0 = 1 + rnd.nextInt(5);
        CoreFrameEncoders.Trade t = null;
        for (int i = 0; i < 5 && t == null; i++) {
            t = pickTrade(1 + (m0 + i - 1) % 5);
        }
        if (t == null) {
            meRespond();
            return;
        }
        if (lastTrade != null && rnd.nextInt(4) == 0) {
            frame(enc.tradeBatch(List.of(lastTrade, t))); // the first entry is a re-delivery: a no-op
        } else {
            frame(enc.trade(t));
        }
        lastTrade = t;
        for (final long oms : new long[] {t.takerOms(), t.makerOms()}) {
            final CoreOrder o = model.engine.orders().orders().get(oms);
            final long leg = oms == t.takerOms() ? t.takerLeg() : t.makerLeg();
            if (o == null && rnd.nextInt(3) == 0) {
                frame(enc.terminal(++egress, leg, 1, t.marketId(), 2, now, oms)); // stale FILLED: the record is gone
            } else if (o != null && limitOf(o) == 0 && o.currentLegId == leg && rnd.nextBoolean()) {
                frame(enc.terminal(++egress, leg, o.userId, o.marketId, 3, now, oms)); // ME cancels the market remainder
            }
        }
    }

    private CoreFrameEncoders.Trade pickTrade(final int m) {
        final List<CoreOrder> buys = live(o -> o.marketId == m && o.side == 0 && tradable(o));
        final List<CoreOrder> sells = live(o -> o.marketId == m && o.side == 1 && tradable(o));
        if (buys.isEmpty() || sells.isEmpty()) {
            return null;
        }
        for (int i = 0; i < 4; i++) {
            final CoreOrder b = buys.get(rnd.nextInt(buys.size()));
            final CoreOrder s = sells.get(rnd.nextInt(sells.size()));
            final long bp = limitOf(b);
            final long sp = limitOf(s);
            if (bp > 0 && sp > 0 && bp < sp) {
                continue;
            }
            final CoreOrder maker = b.orderId < s.orderId ? b : s;
            final CoreOrder taker = maker == b ? s : b;
            final long px = limitOf(maker) > 0 ? limitOf(maker) : limitOf(taker) > 0 ? limitOf(taker) : mid[m];
            long q = Math.min(avail(b), avail(s));
            if (rnd.nextBoolean()) {
                q = q * (1 + rnd.nextInt(3)) / 4 / (ONE / 1000) * (ONE / 1000);
            }
            if (bp == 0) {
                final long hold = model.engine.account(b.userId).holdRemaining(b.orderId);
                final long cap = BigInteger.valueOf(Math.max(0, hold)).multiply(BigInteger.valueOf(ONE))
                        .divide(BigInteger.valueOf(px)).longValue() / (ONE / 1000) * (ONE / 1000);
                q = Math.min(q, cap);
            }
            if (q <= 0) {
                continue;
            }
            return new CoreFrameEncoders.Trade(++egress, ++tradeId, m, taker.currentLegId, taker.userId, maker.currentLegId,
                    maker.userId, px, q, taker == b, now, taker.orderId, maker.orderId);
        }
        return null;
    }

    private static long avail(final CoreOrder o) {
        return o.orderType == 6 ? o.sliceRemainingQty : o.remainingQty;
    }

    // ---------------------------------------------------------------- market data, money, admin, queries

    /** MarketTick; {@code move != 0} forces a move of that many steps, otherwise a small random walk (stale, one-sided). */
    private void tick(final int m, final int move) {
        final long st = step(m);
        mid[m] += (move != 0 ? move : rnd.nextInt(5) - 2) * st;
        mid[m] = Math.max(mid0[m] - 16 * st, Math.min(mid0[m] + 16 * st, mid[m]));
        final int p = move != 0 ? 99 : rnd.nextInt(100);
        final long session = rnd.nextInt(10) < 7 ? FORWARDER_SESSION : TRUSTED;
        if (p < 5 && bookVersion[m] > 0) {
            frame(enc.tick(m, bookVersion[m], mid[m] - st, mid[m] + st, 0, 0), session); // same version: stale
        } else if (p < 8) {
            frame(enc.tick(m, ++bookVersion[m], mid[m] - st, 0, 0, 0), session); // one-sided
        } else {
            frame(enc.tick(m, ++bookVersion[m], mid[m] - st, mid[m] + st, mid[m], 0), session);
        }
    }

    /** The last tick's bookVersion again (a second forwarder, or a replay): a counted drop. */
    private void staleTick(final int m) {
        frame(enc.tick(m, bookVersion[m], mid[m] - step(m), mid[m] + step(m), 0, 0), FORWARDER_SESSION);
    }

    private void money() {
        final int u = 1 + rnd.nextInt(USERS);
        final int asset = rnd.nextBoolean() ? 0 : baseFor(u);
        if (rnd.nextInt(5) < 3) {
            frame(enc.deposit(corr++, u, asset, units(1 + rnd.nextInt(5_000)), 0xD0L, request++));
        } else {
            final long amount = rnd.nextInt(6) == 0 ? 50_000_000L * ONE : units(1 + rnd.nextInt(500)); // sometimes too big
            frame(enc.withdraw(corr++, u, asset, amount, 0xD0L, request++));
        }
    }

    private void query() {
        if (rnd.nextBoolean()) {
            frame(enc.orderSnapshot(corr++, rnd.nextInt(3) == 0 ? 0 : 1 + rnd.nextInt(USERS), rnd.nextInt(3) == 0 ? 5 : 0));
        } else {
            frame(enc.pendingDispatch(corr++, 0, rnd.nextBoolean() ? 0 : 2));
        }
    }

    private void quarantine(final boolean unknown) {
        final List<CoreOrder> live = live(o -> o.status != 7);
        if (unknown || live.isEmpty()) {
            frame(enc.quarantine(corr++, 999_999));
            return;
        }
        frame(enc.quarantine(corr++, live.get(rnd.nextInt(live.size())).orderId));
    }

    /** Class and scope checks: a GATEWAY tick, a GATEWAY create outside its users, an unregistered BRIDGE terminal. */
    private void unauthorised() {
        switch (rnd.nextInt(3)) {
            case 0 -> frame(enc.tick(1, bookVersion[1] + 1, mid[1] - step(1), mid[1] + step(1), 0, 0), GATEWAY_SESSION);
            case 1 -> frame(enc.create(corr++, 9, 0, 0x5EEDL, request++, 1, 0, 0, 0, mid[1], ONE, 0, 0, 0, 0, 0), GATEWAY_SESSION);
            default -> frame(enc.terminal(++egress, 10_001, 1, 1, 3, now, 1), UNREGISTERED_BRIDGE);
        }
    }

    private List<CoreOrder> live(final java.util.function.Predicate<CoreOrder> filter) {
        final List<CoreOrder> out = new ArrayList<>();
        model.engine.orders().orders().forEachByOrderId(o -> {
            if (filter.test(o)) {
                out.add(o);
            }
        });
        return out;
    }

    /** Exact 8 dp product, truncated like FixedPoint.multiply (test-side arithmetic for budgets). */
    private static long mul(final long a, final long b) {
        return BigInteger.valueOf(a).multiply(BigInteger.valueOf(b)).divide(BigInteger.valueOf(ONE)).longValueExact();
    }

    /** The builder's own replica after the story (tests read its counters to prove coverage). */
    static FrameReplica replay(final List<Step> story) {
        final FrameReplica r = new FrameReplica();
        for (final Step s : story) {
            r.apply(s);
        }
        return r;
    }
}

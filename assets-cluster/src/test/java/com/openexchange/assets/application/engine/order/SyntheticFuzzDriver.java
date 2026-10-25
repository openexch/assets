// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;

/**
 * Seeded generator of LEGAL operations for {@link SyntheticFuzzTest} (contract S6 sections 1-3 on the S5
 * machinery). Each operation predicts from the records what the contract says must happen, applies it, checks
 * the events it owes, and then {@link #verify()} runs {@link LifecycleInvariants} (I1-I8, I11, I13) and the S6
 * invariants: armed records carry no leg and no dispatch, every live non-armed order has DISPATCH_PENDING or a
 * leg, the books count exactly the live non-quarantined PENDING_TRIGGER records, icebergs keep remaining =
 * slice + hidden, and filled + remaining = quantity. Deterministic: Random on the seed, ascending orderId.
 *
 * <p>Generator rules that keep every operation inside the contract: stops sit at least half a unit beyond the
 * quote on their trigger side (never crossed at admission); all prices are multiples of 0.5 and quantities of
 * 0.01, so every product is exact; a fill stays within the order's remaining and an iceberg's current slice; a
 * MARKET BUY child trades at the ask (or the seller's limit) and never spends its whole budget; a BUY with a
 * limit trades at or below it, so its hold always covers the fill.</p>
 */
final class SyntheticFuzzDriver extends SyntheticFuzzHarness {

    SyntheticFuzzDriver(final long seed) {
        super(seed);
    }

    void run(final int ops) {
        for (int i = 0; i < ops; i++) {
            step(i);
        }
    }

    private void step(final int i) {
        op = i;
        justSwept = false;
        r.markNow();
        final int roll = rng.nextInt(100);
        final boolean done;
        if (roll < 4) {
            opText = "DEPOSIT";
            deposit(1 + rng.nextInt(USERS), rng.nextBoolean() ? USD : BTC, (1 + rng.nextInt(1_000)) * ONE);
            done = true;
        } else if (roll < 16) {
            done = opLimit();
        } else if (roll < 36) {
            done = opStop();
        } else if (roll < 41) {
            done = opIceberg();
        } else if (roll < 55) {
            done = opTick();
        } else if (roll < 70) {
            done = opCreateOutcome();
        } else if (roll < 84) {
            done = opTrade();
        } else if (roll < 90) {
            done = opCancel();
        } else if (roll < 95) {
            done = opResolve();
        } else if (roll < 97) {
            done = opSweep();
        } else if (roll < 98) {
            done = opQuarantine();
        } else {
            done = opReplay();
        }
        if (!done) {
            opLimit(); // nothing applicable for that roll: a LIMIT create is always legal
        }
        for (final AssetsEngineEvent.MeDispatch d : all(AssetsEngineEvent.MeDispatch.class)) {
            if (dispatched.put(d.dispatchSeq(), d) != null) {
                throw fail("dispatchSeq " + d.dispatchSeq() + " minted twice");
            }
        }
        verify();
    }

    private boolean opLimit() {
        final long u = 1 + rng.nextInt(USERS);
        final int side = rng.nextInt(2);
        final long px = price();
        final long qty = (1 + rng.nextInt(200)) * CENT;
        final CreateOrderCommand c = r.limit(u, MKT, side, px, qty);
        opText = "LIMIT u=" + u + " side=" + side + " px=" + px + " qty=" + qty;
        final long hold = side == 0 ? FixedPoint.multiply(px, qty) : qty;
        admit(c, side == 0 ? USD : BTC, hold, OrderStatus.PENDING_NEW);
        return true;
    }

    private boolean opStop() {
        final long u = 1 + rng.nextInt(USERS);
        final int side = rng.nextInt(2);
        final int type = OrderEngine.TYPE_STOP_LOSS + rng.nextInt(3);
        final long qty = (1 + rng.nextInt(200)) * CENT;
        final long away = (1 + rng.nextInt(8)) * HALF;
        final CreateOrderCommand c = r.limit(u, MKT, side, 0L, qty);
        c.orderType = type;
        long hold = qty;
        long arm = 0L;
        if (type == OrderEngine.TYPE_TRAILING_STOP) {
            c.trailingDelta = away;
            arm = side == 0 ? ask : bid;
            if (side == 0) {
                if (rng.nextBoolean()) {
                    c.budget = FixedPoint.multiply(ask + away + 6 * ONE, qty);
                    hold = c.budget;
                } else {
                    hold = FixedPoint.multiply(FixedPoint.multiply(ask + away, SLIP), qty); // (ask + delta) x slip x qty
                }
            }
        } else {
            c.stopPrice = side == 0 ? ask + away : bid - away;
            if (type == OrderEngine.TYPE_STOP_LIMIT) {
                c.price = side == 0 ? c.stopPrice + rng.nextInt(3) * HALF : c.stopPrice - rng.nextInt(3) * HALF;
                if (side == 0) {
                    hold = FixedPoint.multiply(c.price, qty);
                }
            } else if (side == 0) {
                if (rng.nextBoolean()) {
                    c.budget = FixedPoint.multiply(c.stopPrice + 6 * ONE, qty);
                    hold = c.budget;
                } else {
                    hold = FixedPoint.multiply(FixedPoint.multiply(c.stopPrice, SLIP), qty); // stop x (1 + 500 bps) x qty
                }
            }
        }
        if (rng.nextInt(4) == 0) {
            c.timeInForce = OrderEngine.TIF_GTD;
            c.expiresAtMs = r.now + 500 + rng.nextInt(3_000);
        }
        opText = "STOP type=" + type + " u=" + u + " side=" + side + " qty=" + qty + " stop=" + c.stopPrice + " px=" + c.price
                + " delta=" + c.trailingDelta + " budget=" + c.budget + " tif=" + c.timeInForce + " quote=" + bid + "/" + ask;
        final CoreOrder o = admit(c, side == 0 ? USD : BTC, hold, OrderStatus.PENDING_TRIGGER);
        if (o == null) {
            return true;
        }
        if (o.status != ST_PENDING_TRIGGER || o.has(CoreOrder.F_DISPATCH_PENDING) || o.currentLegId != 0) {
            throw fail("an admitted stop rests PENDING_TRIGGER with no dispatch and no leg (contract 1): status " + o.status
                    + " flags " + o.stateFlags + " leg " + o.currentLegId);
        }
        if (count(AssetsEngineEvent.MeDispatch.class) != 0) {
            throw fail("an armed stop sends nothing to the ME (contract 1), got " + r.events());
        }
        if (o.stopPrice != c.stopPrice || o.trailingDelta != c.trailingDelta || o.trailingArmPrice != arm) {
            throw fail("record keeps stop " + c.stopPrice + " delta " + c.trailingDelta + " and arm " + arm
                    + " (the quote of its side at admission), got " + o.stopPrice + "/" + o.trailingDelta + "/" + o.trailingArmPrice);
        }
        return true;
    }

    private boolean opIceberg() {
        final long u = 1 + rng.nextInt(USERS);
        final int side = rng.nextInt(2);
        final long px = price();
        final long qty = (100 + rng.nextInt(400)) * CENT;
        final long display = Math.max(CENT, qty / (2 + rng.nextInt(3)) / CENT * CENT);
        final CreateOrderCommand c = r.limit(u, MKT, side, px, qty);
        c.orderType = OrderEngine.TYPE_ICEBERG;
        c.displayQty = display;
        opText = "ICEBERG u=" + u + " side=" + side + " px=" + px + " qty=" + qty + " display=" + display;
        final CoreOrder o = admit(c, side == 0 ? USD : BTC, side == 0 ? FixedPoint.multiply(px, qty) : qty, OrderStatus.PENDING_NEW);
        if (o == null) {
            return true;
        }
        final AssetsEngineEvent.MeDispatch d = dispatchFor(o.orderId);
        if (d.orderType() != OrderEngine.TYPE_LIMIT || d.quantity() != display || d.price() != px || d.commandIdLow() != 0) {
            throw fail("slice 1 goes out as a LIMIT of displayQty at the price, attempt 0 (3.4), got " + d.render());
        }
        if (o.sliceRemainingQty != display || o.hiddenQty != qty - display || o.remainingQty != qty) {
            throw fail("iceberg admission: slice " + display + " hidden " + (qty - display) + " (3.4), got slice "
                    + o.sliceRemainingQty + " hidden " + o.hiddenQty + " remaining " + o.remainingQty);
        }
        return true;
    }

    // ---------------------------------------------------------------- MarketTick (3.1, 3.2)

    private boolean opTick() {
        final MarketRisk m = r.orders.market(MKT);
        final int kind = rng.nextInt(100);
        if (kind < 10) {
            final boolean stale = kind < 6;
            final long b = 90 * ONE + rng.nextInt(40) * HALF;
            final long a = stale ? b + HALF : b - rng.nextInt(2) * HALF; // stale: same version; crossed: ask <= bid
            final long v = stale ? ver : ver + 1;
            opText = (stale ? "STALE" : "CROSSED") + " TICK ver=" + v + " bid=" + b + " ask=" + a;
            final long staleBefore = r.orders.staleTicks();
            final long invalidBefore = r.orders.invalidTickCount();
            r.tick(MKT, b, a, v);
            if (!r.events().isEmpty() || m.bestBid != bid || m.bestAsk != ask || m.bookVersion != ver) {
                throw fail("a " + (stale ? "stale" : "crossed") + " tick is a counted drop: no event, market unchanged (3.1), got "
                        + r.events() + " market " + m.bestBid + "/" + m.bestAsk + " v" + m.bookVersion);
            }
            if (stale ? r.orders.staleTicks() != staleBefore + 1 : r.orders.invalidTickCount() != invalidBefore + 1) {
                throw fail((stale ? "staleTicks" : "invalidTickCount") + " must count the dropped tick (3.1)");
            }
            return true;
        }
        final long b = Math.max(90 * ONE, Math.min(110 * ONE, bid + (rng.nextInt(5) - 2) * HALF));
        final long a = b + (1 + rng.nextInt(4)) * HALF;
        final long v = ver + 1;
        opText = "TICK ver=" + v + " bid=" + b + " ask=" + a;
        final List<Long> fire = new ArrayList<>();
        final Map<Long, Long> arm = new HashMap<>();
        final Map<Long, Long> budget = new HashMap<>();
        final Map<Long, Integer> quarantinedStatus = new HashMap<>();
        r.orders.orders().forEachByOrderId(o -> {
            if (o.status != ST_PENDING_TRIGGER || o.marketId != MKT) {
                return;
            }
            if (o.has(CoreOrder.F_QUARANTINED)) {
                quarantinedStatus.put(o.orderId, (int) o.status);
                return;
            }
            final boolean buy = o.side == OrderEngine.SIDE_BUY;
            final long q = buy ? a : b;
            boolean fires = false;
            long newArm = o.trailingArmPrice;
            if (o.orderType == OrderEngine.TYPE_TRAILING_STOP) {
                if (o.trailingArmPrice == 0) {
                    newArm = q;
                } else if (buy ? q < o.trailingArmPrice : q > o.trailingArmPrice) {
                    newArm = q; // the advance consumes the tick
                } else {
                    fires = (buy ? q - o.trailingArmPrice : o.trailingArmPrice - q) >= o.trailingDelta;
                }
            } else {
                fires = buy ? q >= o.stopPrice : q <= o.stopPrice;
            }
            if (fires) {
                fire.add(o.orderId);
                budget.put(o.orderId, r.engine.account(o.userId).holdRemaining(o.orderId));
            } else {
                arm.put(o.orderId, newArm);
            }
        });
        final long triggeredBefore = r.orders.triggeredCount();
        r.tick(MKT, b, a, v);
        bid = b;
        ask = a;
        ver = v;
        if (m.bestBid != b || m.bestAsk != a || m.bookVersion != v) {
            throw fail("a valid tick updates the market state, got " + m.bestBid + "/" + m.bestAsk + " v" + m.bookVersion);
        }
        final List<Long> triggered = new ArrayList<>();
        for (final AssetsEngineEvent.OrderEvent e : all(AssetsEngineEvent.OrderEvent.class)) {
            if (e.kind() == EventKind.TRIGGERED) {
                triggered.add(e.orderId());
            }
        }
        final List<Long> sent = new ArrayList<>();
        for (final AssetsEngineEvent.MeDispatch d : all(AssetsEngineEvent.MeDispatch.class)) {
            sent.add(d.omsOrderId());
        }
        if (!triggered.equals(fire) || !sent.equals(fire)) {
            throw fail("orders crossed by the tick fire once each, in ascending orderId (3.1.4-5): expected " + fire
                    + ", TRIGGERED " + triggered + ", dispatched " + sent);
        }
        if (r.orders.triggeredCount() != triggeredBefore + fire.size()) {
            throw fail("triggeredCount must grow by " + fire.size());
        }
        for (final long id : fire) {
            checkChild(record(id), dispatchFor(id), budget.get(id));
        }
        for (final Map.Entry<Long, Long> e : arm.entrySet()) {
            final CoreOrder o = record(e.getKey());
            if (o.status != ST_PENDING_TRIGGER || o.trailingArmPrice != e.getValue()) {
                throw fail("order " + o.orderId + " stays armed with arm " + e.getValue() + " (3.1.4), got status " + o.status
                        + " arm " + o.trailingArmPrice);
            }
        }
        for (final long id : quarantinedStatus.keySet()) {
            if (record(id).status != ST_PENDING_TRIGGER) {
                throw fail("a quarantined armed order never fires (3.2), order " + id);
            }
        }
        return true;
    }

    // ---------------------------------------------------------------- ME feed

    private boolean opCreateOutcome() {
        final List<CoreOrder> pending = live(o -> o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CREATE);
        if (pending.isEmpty()) {
            return false;
        }
        final CoreOrder o = pending.get(rng.nextInt(pending.size()));
        final long id = o.orderId;
        final boolean applied = rng.nextInt(100) < 88;
        final long leg = nextLeg++;
        final boolean cancelPending = o.has(CoreOrder.F_CANCEL_PENDING);
        opText = "OUTCOME CREATE order=" + id + " attempt=" + o.attempt + (applied ? " APPLIED leg=" + leg : " REJECTED");
        OutcomeFeed.outcome(r, id, o.attempt, OutcomeFeed.KIND_CREATE, applied ? OutcomeFeed.APPLIED : OutcomeFeed.REJECTED, leg, 0L);
        if (!applied) {
            gone(id, EventKind.REJECTED);
            return true;
        }
        final CoreOrder after = record(id);
        orderEvent(id, EventKind.ACK_NEW);
        if (after.currentLegId != leg || (after.has(CoreOrder.F_DISPATCH_PENDING) && after.pendingKind == CoreOrder.PK_CREATE)) {
            throw fail("CREATE APPLIED learns leg " + leg + " and clears the pending create (S5 3.3), got leg " + after.currentLegId);
        }
        if (cancelPending && !after.has(CoreOrder.F_QUARANTINED) && after.pendingKind != CoreOrder.PK_CANCEL) {
            throw fail("a cancel asked for meanwhile is minted at the ACK (S5 3.3, 3.4 R11)");
        }
        return true;
    }

    /** What a fill must leave behind (S5 3.1 + S6 3.4); captured before the trade. */
    private record Pre(long id, int type, long remaining, long slice, long hidden, long display, int attempt, long leg,
                       boolean cancelPending, long price, int side) {
        static Pre of(final CoreOrder o) {
            return new Pre(o.orderId, o.orderType, o.remainingQty, o.sliceRemainingQty, o.hiddenQty, o.displayQty, o.attempt,
                    o.currentLegId, o.has(CoreOrder.F_CANCEL_PENDING), o.price, o.side);
        }
    }

    private boolean opTrade() {
        final List<CoreOrder> buys = live(o -> o.currentLegId != 0 && o.side == OrderEngine.SIDE_BUY && tradeable(o) > 0);
        final List<CoreOrder> sells = live(o -> o.currentLegId != 0 && o.side == OrderEngine.SIDE_SELL && tradeable(o) > 0);
        if (buys.isEmpty() || sells.isEmpty()) {
            return false;
        }
        for (int attemptPair = 0; attemptPair < 8; attemptPair++) {
            final CoreOrder b = buys.get(rng.nextInt(buys.size()));
            final CoreOrder s = sells.get(rng.nextInt(sells.size()));
            if (b.userId == s.userId) {
                continue;
            }
            final long px;
            if (marketChild(b) && marketChild(s)) {
                px = ask;
            } else if (marketChild(b)) {
                px = Math.max(ask, s.price);
            } else if (marketChild(s)) {
                px = Math.min(bid, b.price);
            } else {
                if (s.price > b.price) {
                    continue;
                }
                px = s.price + rng.nextInt((int) ((b.price - s.price) / HALF) + 1) * HALF;
            }
            long qmax = Math.min(tradeable(b), tradeable(s));
            if (marketChild(b)) {
                final long unit = FixedPoint.multiply(px, CENT);
                qmax = Math.min(qmax, (r.engine.account(b.userId).holdRemaining(b.orderId) - 1) / unit * CENT);
            }
            if (qmax < CENT) {
                continue;
            }
            final long qty = CENT * (1 + rng.nextInt((int) (qmax / CENT)));
            final boolean takerIsBuy = rng.nextBoolean();
            final long tradeId = r.engine.getLastAppliedTradeId() + 1;
            final Pre pb = Pre.of(b);
            final Pre ps = Pre.of(s);
            opText = "TRADE id=" + tradeId + " buy=" + b.orderId + " sell=" + s.orderId + " px=" + px + " qty=" + qty;
            if (takerIsBuy) {
                OutcomeFeed.trade(r, tradeId, MKT, b.orderId, b.currentLegId, b.userId, s.orderId, s.currentLegId, s.userId, px, qty, true);
            } else {
                OutcomeFeed.trade(r, tradeId, MKT, s.orderId, s.currentLegId, s.userId, b.orderId, b.currentLegId, b.userId, px, qty, false);
            }
            if (count(AssetsEngineEvent.SettleFault.class) + count(AssetsEngineEvent.OrderFault.class)
                    + count(AssetsEngineEvent.SettleRejected.class) != 0) {
                throw fail("a fill inside both holds and both slices settles cleanly, got " + r.events());
            }
            afterFill(pb, qty);
            afterFill(ps, qty);
            return true;
        }
        return false;
    }

    private void afterFill(final Pre p, final long qty) {
        final CoreOrder o = r.orders.orders().get(p.id);
        final long remaining = p.remaining - qty;
        if (remaining == 0) {
            if (o != null) {
                throw fail("order " + p.id + " filled its quantity: terminal FILLED, record removed (S5 3.1)");
            }
            return;
        }
        if (p.type == OrderEngine.TYPE_ICEBERG && p.slice - qty == 0) {
            if (p.cancelPending) {
                if (o != null) {
                    throw fail("iceberg " + p.id + " slice exhausted while CANCEL_PENDING: CANCELLED now (3.4)");
                }
                orderEvent(p.id, EventKind.CANCELLED);
                return;
            }
            final long next = Math.min(p.display, p.hidden);
            if (o == null || o.sliceRemainingQty != next || o.hiddenQty != p.hidden - next || o.attempt != p.attempt + 1
                    || o.currentLegId != 0 || o.oldLegId != p.leg || o.pendingKind != CoreOrder.PK_CREATE) {
                throw fail("iceberg " + p.id + " refills in the fill apply: slice " + next + " hidden " + (p.hidden - next)
                        + " attempt " + (p.attempt + 1) + " oldLegId " + p.leg + " leg 0 (3.4), got "
                        + (o == null ? "no record" : "slice " + o.sliceRemainingQty + " hidden " + o.hiddenQty + " attempt "
                        + o.attempt + " oldLegId " + o.oldLegId + " leg " + o.currentLegId));
            }
            orderEvent(p.id, EventKind.SLICE_SUBMITTED);
            final AssetsEngineEvent.MeDispatch d = dispatchFor(p.id);
            if (d.kind() != DispatchKind.CREATE || d.orderType() != OrderEngine.TYPE_LIMIT || d.quantity() != next
                    || d.price() != p.price || d.commandIdLow() != ((long) (p.attempt + 1) << 2)) {
                throw fail("the next slice goes out as a LIMIT of " + next + " at " + p.price + " (3.4), got " + d.render());
            }
        } else if (o == null) {
            throw fail("order " + p.id + " keeps " + remaining + " after the fill and must stay live");
        } else if (p.type == OrderEngine.TYPE_ICEBERG && (o.sliceRemainingQty != p.slice - qty || o.hiddenQty != p.hidden)) {
            throw fail("iceberg " + p.id + " fill drains the slice only (3.4): slice " + (p.slice - qty) + ", got " + o.sliceRemainingQty);
        }
        if (o != null && o.remainingQty != remaining) {
            throw fail("order " + p.id + " remaining " + remaining + " after the fill, got " + o.remainingQty);
        }
    }

    private boolean opCancel() {
        final List<CoreOrder> open = live(o -> !o.has(CoreOrder.F_QUARANTINED) && !o.has(CoreOrder.F_CANCEL_PENDING));
        if (open.isEmpty()) {
            return false;
        }
        final CoreOrder o = open.get(rng.nextInt(open.size()));
        final long id = o.orderId;
        final boolean armed = o.status == ST_PENDING_TRIGGER;
        final boolean mint = o.currentLegId != 0 && !o.has(CoreOrder.F_DISPATCH_PENDING);
        opText = "CANCEL order=" + id + (armed ? " (armed)" : "");
        r.cancel(o.userId, id);
        if (armed) {
            gone(id, EventKind.CANCELLED);
            if (count(AssetsEngineEvent.MeDispatch.class) != 0) {
                throw fail("cancel of an armed order sends nothing to the ME (3.3)");
            }
            return true;
        }
        final CoreOrder after = record(id);
        if (!after.has(CoreOrder.F_CANCEL_PENDING) || count(AssetsEngineEvent.MeDispatch.class) != (mint ? 1 : 0)) {
            throw fail("cancel sets CANCEL_PENDING and mints only on a resting leg (S5), got " + r.events());
        }
        return true;
    }

    private boolean opResolve() {
        final List<CoreOrder> cancels = live(o -> o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CANCEL);
        final List<CoreOrder> children = live(o -> marketChild(o) && o.status != ST_PENDING_TRIGGER && o.currentLegId != 0
                && !o.has(CoreOrder.F_DISPATCH_PENDING));
        if (cancels.isEmpty() && children.isEmpty()) {
            return false;
        }
        final CoreOrder o;
        final boolean byOutcome;
        if (!cancels.isEmpty() && (children.isEmpty() || rng.nextInt(10) < 7)) {
            o = cancels.get(rng.nextInt(cancels.size()));
            byOutcome = rng.nextInt(10) < 6;
        } else {
            o = children.get(rng.nextInt(children.size())); // the ME cancels a MARKET child's unfilled remainder
            byOutcome = false;
        }
        final long id = o.orderId;
        final EventKind kind = o.has(CoreOrder.F_EXPIRY_REQUESTED) ? EventKind.EXPIRED : EventKind.CANCELLED;
        opText = (byOutcome ? "OUTCOME CANCEL APPLIED order=" : "JOURNAL CANCELLED order=") + id;
        if (byOutcome) {
            OutcomeFeed.outcome(r, id, o.attempt, OutcomeFeed.KIND_CANCEL, OutcomeFeed.APPLIED, 0L, o.currentLegId);
        } else {
            OutcomeFeed.terminal(r, id, o.currentLegId, OutcomeFeed.ME_CANCELLED);
        }
        gone(id, kind);
        return true;
    }

    private boolean opSweep() {
        final long dt = 100 + rng.nextInt(2_900);
        final long at = r.now + dt;
        final List<Long> expiring = new ArrayList<>();
        r.orders.orders().forEachByOrderId(o -> {
            if (o.status == ST_PENDING_TRIGGER && !o.has(CoreOrder.F_QUARANTINED) && o.expiresAtMs > 0 && o.expiresAtMs <= at) { // quarantine freezes (S6 3.7)
                expiring.add(o.orderId);
            }
        });
        opText = "SWEEP +" + dt + " (armed expiring " + expiring + ")";
        r.advance(dt);
        r.sweep();
        justSwept = true;
        for (final long id : expiring) {
            gone(id, EventKind.EXPIRED);
            for (final AssetsEngineEvent.MeDispatch d : all(AssetsEngineEvent.MeDispatch.class)) {
                if (d.omsOrderId() == id) {
                    throw fail("GTD expiry of an armed order sends nothing to the ME (3.3, R12)");
                }
            }
        }
        return true;
    }

    private boolean opQuarantine() {
        final List<CoreOrder> armed = live(o -> o.status == ST_PENDING_TRIGGER && !o.has(CoreOrder.F_QUARANTINED));
        if (armed.isEmpty()) {
            return false;
        }
        final long id = armed.get(rng.nextInt(armed.size())).orderId;
        opText = "QUARANTINE order=" + id;
        r.orders.applyQuarantine(corr++, id, r.now);
        final AssetsEngineEvent.AdminAck ack = find(AssetsEngineEvent.AdminAck.class, a -> true, "ADMINACK");
        if (ack.result() != AdminResult.ACCEPTED || !record(id).has(CoreOrder.F_QUARANTINED)) {
            throw fail("quarantine of a live armed order is accepted (S5 3.5), got " + ack.render());
        }
        return true;
    }

    /** RequestPendingDispatch re-emits every pending command byte-identically to its original MeDispatch (3.2, S5 3.6). */
    private boolean opReplay() {
        opText = "PENDINGDISPATCH";
        final int pending = live(o -> o.has(CoreOrder.F_DISPATCH_PENDING)).size();
        r.orders.applyRequestPendingDispatch(corr++, 0L, 0);
        final List<AssetsEngineEvent.MeDispatchReply> replies = all(AssetsEngineEvent.MeDispatchReply.class);
        if (replies.size() != Math.min(pending, OrderEngine.PENDING_DISPATCH_DEFAULT)) {
            throw fail("one reply per pending dispatch: " + pending + ", got " + replies.size());
        }
        for (final AssetsEngineEvent.MeDispatchReply x : replies) {
            final AssetsEngineEvent.MeDispatch d = dispatched.get(x.dispatchSeq());
            if (d == null || d.commandIdHigh() != x.commandIdHigh() || d.commandIdLow() != x.commandIdLow() || d.kind() != x.kind()
                    || d.oldOrderId() != x.oldOrderId() || d.orderType() != x.orderType() || d.side() != x.side()
                    || d.price() != x.price() || d.quantity() != x.quantity() || d.budget() != x.budget()
                    || d.timeInForce() != x.timeInForce() || d.userId() != x.userId()) {
                throw fail("the replay must equal the original dispatch " + (d == null ? "<none>" : d.render()) + ", got " + x.render());
            }
        }
        return true;
    }

}

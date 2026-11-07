// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;

/**
 * Seeded generator of LEGAL order-lifecycle operations over three users and market 1 (contract S5 sections
 * 3.1-3.4 and 3.7) on top of {@link OrderFuzzHarness}. Every operation updates the model the way the contract
 * says the engine must, asserts the events that operation owes, then {@link #verify()} runs. Deterministic:
 * {@link java.util.Random} on the seed, orders visited in ascending id, no wall clock.
 *
 * <p>Generator choices that keep every operation inside the contract: fill prices lie between the two limits,
 * and for a BUY inside a replace window at or below the lower of its old and pending limit, so a hold always
 * covers a non-overfill trade before and after the window resolves; amends go only to GTC orders (a GTD expiry
 * inside a replace window is not specified); a trade on an order in a replace window never carries filled to or
 * past the pending quantity; OVERFILL is generated only for orders outside a window. All prices are whole units
 * and all quantities multiples of 0.01, so every FixedPoint product is exact.</p>
 */
final class OrderFuzzDriver extends OrderFuzzHarness {

    private static final int T_CREATE = 50;
    private static final int T_AMEND = 52;

    OrderFuzzDriver(final long seed) {
        super(seed);
    }

    void run(final int ops) {
        for (int i = 0; i < ops; i++) {
            step(i);
        }
    }

    void step(final int i) {
        op = i;
        justSwept = false;
        r.markNow();
        final int roll = rng.nextInt(100);
        boolean done;
        if (roll < 6) {
            done = opDeposit();
        } else if (roll < 30) {
            done = opCreate();
        } else if (roll < 48) {
            done = opCreateOutcome();
        } else if (roll < 66) {
            done = opTrade();
        } else if (roll < 74) {
            done = opCancel();
        } else if (roll < 82) {
            done = opResolveCancel();
        } else if (roll < 90) {
            done = opAmend();
        } else if (roll < 96) {
            done = opResolveAmend();
        } else {
            done = opSweep();
        }
        if (!done) {
            opCreate(); // nothing applicable for that roll: a create is always legal
        }
        verify();
    }

    // ---------------------------------------------------------------- money

    private boolean opDeposit() {
        final long u = 1 + rng.nextInt(USERS);
        final int asset = rng.nextBoolean() ? USD : BTC;
        final long amt = (1 + rng.nextInt(1_000)) * ONE;
        opText = "DEPOSIT u=" + u + " asset=" + asset + " amt=" + amt;
        deposit(u, asset, amt);
        if (r.count(AssetsEngineEvent.Balance.class) != 1) {
            throw fail("one BALANCE from a deposit, got " + events());
        }
        return true;
    }

    // ---------------------------------------------------------------- create and its outcome (C2, C4, C13, C15)

    private boolean opCreate() {
        final long u = 1 + rng.nextInt(USERS);
        final int side = rng.nextInt(2);
        final long price = (90 + rng.nextInt(21)) * ONE;
        final long qty = (1 + rng.nextInt(500)) * CENT;
        final int tifRoll = rng.nextInt(100);
        final int tif = tifRoll < 60 ? OrderEngine.TIF_GTC : tifRoll < 85 ? OrderEngine.TIF_GTD : OrderEngine.TIF_IOC;
        final long exp = tif == OrderEngine.TIF_GTD ? r.now + 500 + rng.nextInt(3_000) : 0L;
        final int holdAsset = side == OrderEngine.SIDE_BUY ? USD : BTC;
        final long holdAmount = side == OrderEngine.SIDE_BUY ? FixedPoint.multiply(price, qty) : qty;
        final CreateOrderCommand c = r.limit(u, MARKET, side, price, qty);
        c.timeInForce = tif;
        c.expiresAtMs = exp;
        opText = "CREATE u=" + u + " " + (side == 0 ? "BUY" : "SELL") + " px=" + price + " qty=" + qty + " tif=" + tif
                + (exp > 0 ? " exp=" + exp : "");
        final CoreRejectReason expectedReject = r.orders.requests().countFor(u) >= RequestTable.PER_USER_CAP
                ? CoreRejectReason.TOO_MANY_PENDING_REQUESTS
                : r.engine.account(u).available(holdAsset) < holdAmount ? CoreRejectReason.INSUFFICIENT_FUNDS : null;
        r.create(c);
        if (expectedReject != null) {
            expectReject(expectedReject, T_CREATE);
            return true;
        }
        final MOrder o = new MOrder();
        o.id = nextOrderId++;
        o.user = u;
        o.side = side;
        o.tif = tif;
        o.price = price;
        o.qty = qty;
        o.holdAsset = holdAsset;
        o.holdAmount = holdAmount;
        o.residual = holdAmount;
        o.expiresAtMs = exp;
        o.pendingKind = CoreOrder.PK_CREATE;
        o.dispatchPending = true;
        o.pendingDispatchSeq = ++dispatchSeq;
        o.status = OrderStatus.PENDING_NEW.code();
        live.put(o.id, o);
        final AssetsEngineEvent.OrderAck ack = find(AssetsEngineEvent.OrderAck.class, a -> a.orderId() == o.id, "ORDERACK for order " + o.id);
        if (ack.status() != OrderStatus.PENDING_NEW || ack.duplicate()) {
            throw fail("ack of a fresh create must be PENDING_NEW and not a duplicate: " + ack.render());
        }
        expectDispatch(DispatchKind.CREATE, o);
        orderEvent(o.id, EventKind.ACCEPTED);
        return true;
    }

    private boolean opCreateOutcome() {
        final List<MOrder> cands = select(o -> o.dispatchPending && o.pendingKind == CoreOrder.PK_CREATE);
        if (cands.isEmpty()) {
            return opStaleOutcome();
        }
        final MOrder o = pick(cands);
        final boolean applied = rng.nextInt(100) < 85;
        final long leg = applied ? nextLeg++ : 0L;
        opText = "OUTCOME CREATE order=" + o.id + " attempt=" + o.attempt + (applied ? " APPLIED leg=" + leg : " REJECTED");
        r.orders.applyOutcome(++egressSeq, o.id, (long) o.attempt << 2 | DispatchKind.CREATE.code(), o.user, o.id, 0L, o.price, o.qty, 0L,
                MARKET, DispatchKind.CREATE.code(), applied ? 0 : 1, leg, 0, r.now, 0L, r.now);
        if (applied) {
            o.leg = leg;
            o.dispatchPending = false;
            o.pendingKind = CoreOrder.PK_NONE;
            o.pendingDispatchSeq = 0;
            o.status = OrderStatus.NEW.code();
            final AssetsEngineEvent.OrderEvent ev = orderEvent(o.id, EventKind.ACK_NEW);
            if (ev.meOrderId() != leg) {
                throw fail("ACK_NEW must carry the learned leg " + leg + ": " + ev.render());
            }
            if (o.cancelPending || (o.tif == OrderEngine.TIF_IOC && o.remaining() > 0)) {
                o.cancelPending = true; // C8b at ACK, or C13 (IOC rests)
                mintCancel(o);
                expectDispatch(DispatchKind.CANCEL, o);
                orderEvent(o.id, EventKind.DISPATCHED);
            } else if (r.count(AssetsEngineEvent.MeDispatch.class) != 0) {
                throw fail("nothing to mint after a plain ACK, got " + events());
            }
        } else {
            final AssetsEngineEvent.OrderEvent ev = orderEvent(o.id, EventKind.REJECTED);
            if (ev.status() != OrderStatus.REJECTED || ev.reason() != CoreRejectReason.ME_REJECTED) {
                throw fail("ME reject of a create is terminal REJECTED with reason ME_REJECTED: " + ev.render());
            }
            terminal(o);
        }
        return true;
    }

    /** Re-deliver the resolved create outcome of an acked order: a counted no-op (3.3). */
    private boolean opStaleOutcome() {
        final List<MOrder> cands = select(o -> o.leg != 0);
        if (cands.isEmpty()) {
            return false;
        }
        final MOrder o = pick(cands);
        opText = "OUTCOME CREATE replay order=" + o.id + " leg=" + o.leg;
        r.orders.applyOutcome(++egressSeq, o.id, 0L, o.user, o.id, 0L, o.price, o.qty, 0L, MARKET, DispatchKind.CREATE.code(), 0, o.leg, 0,
                r.now, 0L, r.now);
        expectedRedundantOutcomes++; // contract 3.8 #4: APPLIED naming the current leg is redundant, not stale
        if (!r.events().isEmpty()) {
            throw fail("a re-delivered outcome emits nothing, got " + events());
        }
        return true;
    }

    // ---------------------------------------------------------------- trades (C5, C6, R-C5b, R-C5c)

    private boolean opTrade() {
        if (lastTrade != null && rng.nextInt(100) < 5) {
            return opDuplicateTrade();
        }
        final List<MOrder> buys = select(o -> o.side == OrderEngine.SIDE_BUY && o.leg != 0 && o.tradeable() >= CENT);
        final List<MOrder> sells = select(o -> o.side == OrderEngine.SIDE_SELL && o.leg != 0 && o.tradeable() >= CENT);
        final List<MOrder[]> pairs = new ArrayList<>();
        for (final MOrder b : buys) {
            for (final MOrder s : sells) {
                if (b.user != s.user && s.price <= b.buyLimit()) {
                    pairs.add(new MOrder[]{b, s});
                }
            }
        }
        if (pairs.isEmpty()) {
            return false;
        }
        final MOrder[] pair = pairs.get(rng.nextInt(pairs.size()));
        final MOrder b = pair[0];
        final MOrder s = pair[1];
        final int ticks = (int) ((b.buyLimit() - s.price) / ONE);
        final long price = s.price + rng.nextInt(ticks + 1) * ONE;
        final long remB = b.remaining();
        final long remS = s.remaining();
        final long small = Math.min(b.tradeable(), s.tradeable());
        final boolean mayOverfill = !b.replacePending && !s.replacePending;
        final long qty = mayOverfill && rng.nextInt(100) < 15
                ? small + CENT * (1 + rng.nextInt(50))
                : CENT * (1 + rng.nextInt((int) (small / CENT)));
        final boolean takerIsBuy = rng.nextBoolean();
        final MOrder taker = takerIsBuy ? b : s;
        final MOrder maker = takerIsBuy ? s : b;
        final long id = ++tradeId;
        lastTrade = new long[]{id, taker.leg, taker.user, maker.leg, maker.user, price, qty, takerIsBuy ? 1 : 0, taker.id, maker.id};
        opText = "TRADE #" + id + " buy=" + b.id + " sell=" + s.id + " px=" + price + " qty=" + qty + " takerBuy=" + takerIsBuy
                + (qty > Math.min(remB, remS) ? " OVERFILL" : "");
        r.orders.applyTrade(++egressSeq, id, MARKET, taker.leg, taker.user, maker.leg, maker.user, price, qty, takerIsBuy, r.now,
                taker.id, maker.id, 0L, r.now);
        // money: the full trade quantity is drawn from each hold (3.1.2), the shortfall of an overfill from available
        final long cost = FixedPoint.multiply(price, qty);
        b.residual -= Math.min(b.residual, cost);
        s.residual -= Math.min(s.residual, qty);
        final long fillB = Math.min(qty, remB);
        final long fillS = Math.min(qty, remS);
        position.get(b.user)[MARKET] += fillB;
        position.get(s.user)[MARKET] -= fillS;
        if (fillB < qty || fillS < qty) {
            overfilled = true;
        }
        find(AssetsEngineEvent.Settled.class, e -> e.tradeId() == id, "SETTLED tradeId=" + id);
        if (r.engine.getLastAppliedTradeId() != id) {
            throw fail("lastAppliedTradeId must advance to " + id + ", is " + r.engine.getLastAppliedTradeId());
        }
        fill(b, price, fillB, qty);
        fill(s, price, fillS, qty);
        return true;
    }

    private void fill(final MOrder o, final long price, final long fillQty, final long tradeQty) {
        o.filled += fillQty;
        o.cum += FixedPoint.multiply(price, fillQty);
        if (fillQty < tradeQty) {
            final AssetsEngineEvent.OrderFault f = find(AssetsEngineEvent.OrderFault.class,
                    e -> e.kind() == OrderFaultKind.OVERFILL && e.orderId() == o.id, "ORDERFAULT OVERFILL for order " + o.id);
            if (f.amount() != tradeQty - fillQty || f.tradeId() != tradeId) {
                throw fail("OVERFILL fault carries the excess " + (tradeQty - fillQty) + " and trade " + tradeId + ": " + f.render());
            }
        }
        final AssetsEngineEvent.OrderEvent ev = orderEvent(o.id, EventKind.FILL);
        if (ev.lastQty() != fillQty || ev.lastPrice() != price || ev.tradeId() != tradeId) {
            throw fail("FILL event carries lastPrice " + price + " lastQty " + fillQty + " trade " + tradeId + ": " + ev.render());
        }
        if (o.filled == o.qty) {
            if (ev.status() != OrderStatus.FILLED) {
                throw fail("the FILL that completes order " + o.id + " carries status FILLED: " + ev.render());
            }
            terminal(o);
        } else if (o.replacePending) {
            o.status = NO_STATUS; // a fill inside a window: the contract names both PARTIALLY_FILLED and PENDING_REPLACE
        } else {
            o.status = OrderStatus.PARTIALLY_FILLED.code();
            if (ev.status() != OrderStatus.PARTIALLY_FILLED) {
                throw fail("a partial fill carries status PARTIALLY_FILLED: " + ev.render());
            }
        }
    }

    private boolean opDuplicateTrade() {
        final long[] t = lastTrade;
        opText = "TRADE replay #" + t[0];
        r.orders.applyTrade(++egressSeq, t[0], MARKET, t[1], t[2], t[3], t[4], t[5], t[6], t[7] == 1, r.now, t[8], t[9], 0L, r.now);
        if (!r.events().isEmpty()) {
            throw fail("a replayed tradeId is a no-op with no event (R-C5b), got " + events());
        }
        if (r.engine.getLastAppliedTradeId() != tradeId) {
            throw fail("a replayed tradeId leaves lastAppliedTradeId at " + tradeId + ", is " + r.engine.getLastAppliedTradeId());
        }
        return true;
    }

    // ---------------------------------------------------------------- cancel and its resolution (C7, C8b, 3.2, 3.3)

    private boolean opCancel() {
        final List<MOrder> cands = select(o -> !o.cancelPending && !o.replacePending);
        if (cands.isEmpty()) {
            return false;
        }
        final MOrder o = pick(cands);
        opText = "CANCEL order=" + o.id + (o.leg == 0 ? " (create unresolved)" : "");
        r.cancel(o.user, o.id);
        o.cancelPending = true;
        final AssetsEngineEvent.OrderAck ack = find(AssetsEngineEvent.OrderAck.class, a -> a.orderId() == o.id, "ORDERACK for order " + o.id);
        if (ack.status() != OrderStatus.PENDING_CANCEL || ack.duplicate()) {
            throw fail("a first cancel acks PENDING_CANCEL, not a duplicate: " + ack.render());
        }
        if (!o.dispatchPending && o.leg != 0) {
            mintCancel(o);
            expectDispatch(DispatchKind.CANCEL, o);
        } else if (r.count(AssetsEngineEvent.MeDispatch.class) != 0) {
            throw fail("no cancel may be minted while a command is in flight (C8b): " + events());
        }
        return true;
    }

    private boolean opResolveCancel() {
        if (!gone.isEmpty() && rng.nextInt(100) < 15) {
            return opStaleTerminal();
        }
        final List<MOrder> cands = select(o -> o.cancelPending && o.leg != 0 && (!o.dispatchPending || o.pendingKind == CoreOrder.PK_CANCEL));
        if (cands.isEmpty()) {
            return false;
        }
        final MOrder o = pick(cands);
        final boolean viaOutcome = o.dispatchPending && rng.nextInt(100) < 70;
        final EventKind kind = o.expiryRequested ? EventKind.EXPIRED : EventKind.CANCELLED;
        if (viaOutcome) {
            final boolean applied = rng.nextInt(100) < 75;
            opText = "OUTCOME CANCEL order=" + o.id + " attempt=" + o.attempt + (applied ? " APPLIED" : " REJECTED");
            r.orders.applyOutcome(++egressSeq, o.id, (long) o.attempt << 2 | DispatchKind.CANCEL.code(), o.user, o.id, o.leg, 0L, 0L, 0L,
                    MARKET, DispatchKind.CANCEL.code(), applied ? 0 : 1, o.leg, 0, r.now, 0L, r.now);
            if (applied) {
                expectTerminalEvent(o, kind);
                terminal(o);
            } else {
                o.dispatchPending = false;
                o.pendingKind = CoreOrder.PK_NONE;
                o.pendingDispatchSeq = 0;
                expectedCancelRefused++;
                if (r.count(AssetsEngineEvent.OrderEvent.class) != 0 || r.count(AssetsEngineEvent.MeDispatch.class) != 0) {
                    throw fail("a refused cancel keeps the flag, emits no event and mints nothing: " + events());
                }
            }
        } else {
            opText = "TERMINAL 3 order=" + o.id + " leg=" + o.leg;
            r.orders.applyTerminal(++egressSeq, o.leg, o.user, MARKET, 3, r.now, o.id, 0L, r.now);
            expectTerminalEvent(o, kind);
            terminal(o);
        }
        return true;
    }

    private boolean opStaleTerminal() {
        final Gone g = gone.get(rng.nextInt(gone.size()));
        opText = "TERMINAL 3 stale order=" + g.id();
        r.orders.applyTerminal(++egressSeq, g.leg(), g.user(), MARKET, 3, r.now, g.id(), 0L, r.now);
        expectedStaleTerminals++;
        if (!r.events().isEmpty()) {
            throw fail("a terminal for an unknown order is a counted no-op, got " + events());
        }
        return true;
    }

    // ---------------------------------------------------------------- amend and its resolution (B7, 3.4, 3.3)

    private boolean opAmend() {
        final List<MOrder> cands = select(o -> o.leg != 0 && !o.dispatchPending && !o.cancelPending && !o.replacePending
                && o.tif == OrderEngine.TIF_GTC);
        if (cands.isEmpty()) {
            return false;
        }
        final MOrder o = pick(cands);
        final int mode = rng.nextInt(3); // 0 price only, 1 quantity only, 2 both
        final long newPrice = mode == 1 ? 0L : (90 + rng.nextInt(21)) * ONE;
        final long newQty = mode == 0 ? 0L : o.filled + (1 + rng.nextInt(500)) * CENT;
        final long ePrice = newPrice == 0 ? o.price : newPrice;
        final long eQty = newQty == 0 ? o.qty : newQty;
        final long legQty = eQty - o.filled;
        final long target = o.side == OrderEngine.SIDE_BUY ? FixedPoint.multiply(ePrice, eQty) : eQty;
        final long delta = o.side == OrderEngine.SIDE_BUY
                ? FixedPoint.multiply(ePrice, legQty) - FixedPoint.multiply(o.price, o.remaining())
                : legQty - o.remaining();
        final AmendOrderCommand c = new AmendOrderCommand();
        c.reset();
        c.correlationId = 11_000 + nextAmendRequest;
        c.userId = o.user;
        c.orderId = o.id;
        c.requestIdHigh = 0xA3E7DL;
        c.requestIdLow = nextAmendRequest++;
        c.newPrice = newPrice;
        c.newQuantity = newQty;
        final CoreRejectReason expectedReject = r.orders.requests().countFor(o.user) >= RequestTable.PER_USER_CAP
                ? CoreRejectReason.TOO_MANY_PENDING_REQUESTS
                : delta > 0 && r.engine.account(o.user).available(o.holdAsset) < delta ? CoreRejectReason.INSUFFICIENT_FUNDS : null;
        opText = "AMEND order=" + o.id + " newPx=" + newPrice + " newQty=" + newQty + " delta=" + delta;
        r.orders.applyAmend(c, r.now);
        if (expectedReject != null) {
            expectReject(expectedReject, T_AMEND);
            return true;
        }
        o.replacePending = true;
        o.pPrice = ePrice;
        o.pQty = eQty;
        o.pDelta = delta;
        o.pTarget = target;
        o.oldLeg = o.leg;
        o.status = OrderStatus.PENDING_REPLACE.code();
        o.attempt++;
        o.pendingKind = CoreOrder.PK_AMEND;
        o.dispatchPending = true;
        o.pendingDispatchSeq = ++dispatchSeq;
        if (delta > 0) {
            o.residual += delta;
        }
        final AssetsEngineEvent.OrderAck ack = find(AssetsEngineEvent.OrderAck.class, a -> a.orderId() == o.id, "ORDERACK for order " + o.id);
        if (ack.status() != OrderStatus.PENDING_REPLACE || ack.duplicate()) {
            throw fail("an accepted amend acks PENDING_REPLACE: " + ack.render());
        }
        final AssetsEngineEvent.MeDispatch d = expectDispatch(DispatchKind.AMEND, o);
        if (d.quantity() != legQty || d.oldOrderId() != o.oldLeg || (newPrice != 0 && d.price() != newPrice)) {
            throw fail("the AMEND dispatch carries legQty " + legQty + ", oldOrderId " + o.oldLeg + " and the new price: " + d.render());
        }
        orderEvent(o.id, EventKind.DISPATCHED);
        return true;
    }

    private boolean opResolveAmend() {
        final List<MOrder> cands = select(o -> o.replacePending && o.dispatchPending && o.pendingKind == CoreOrder.PK_AMEND);
        if (cands.isEmpty()) {
            return false;
        }
        final MOrder o = pick(cands);
        final boolean applied = rng.nextInt(100) < 75;
        final long newLeg = applied ? nextLeg++ : 0L;
        final long delta = o.pDelta;
        opText = "OUTCOME AMEND order=" + o.id + " attempt=" + o.attempt + (applied ? " APPLIED leg=" + newLeg : " REJECTED") + " delta=" + delta;
        r.orders.applyOutcome(++egressSeq, o.id, (long) o.attempt << 2 | DispatchKind.AMEND.code(), o.user, o.id, o.oldLeg, o.pPrice,
                o.pQty - o.filled, 0L, MARKET, DispatchKind.AMEND.code(), applied ? 0 : 1, newLeg, 0, r.now, 0L, r.now);
        if (applied) {
            if (delta < 0) {
                o.residual -= -delta;
            }
            o.holdAmount = o.pTarget;
            o.price = o.pPrice;
            o.qty = o.pQty;
            o.leg = newLeg;
        } else if (delta > 0) {
            o.residual -= delta;
        }
        o.replacePending = false;
        o.dispatchPending = false;
        o.pendingKind = CoreOrder.PK_NONE;
        o.pendingDispatchSeq = 0;
        o.oldLeg = 0;
        o.pPrice = 0;
        o.pQty = 0;
        o.pDelta = 0;
        o.pTarget = 0;
        o.status = o.filled > 0 ? OrderStatus.PARTIALLY_FILLED.code() : OrderStatus.NEW.code();
        final AssetsEngineEvent.OrderEvent ev = orderEvent(o.id, EventKind.REPLACED);
        final CoreRejectReason reason = applied ? CoreRejectReason.NONE : CoreRejectReason.ME_REJECTED;
        if (ev.reason() != reason || ev.status() != OrderStatus.fromCode(o.status)) {
            throw fail("REPLACED carries reason " + reason + " and status " + OrderStatus.fromCode(o.status) + ": " + ev.render());
        }
        if ((applied && delta < 0) || (!applied && delta > 0)) {
            balanceEvent(o.user, o.holdAsset, "the hold change of the resolved window");
        } else if (r.count(AssetsEngineEvent.Balance.class) != 0) {
            throw fail("no money moves when the resolution releases nothing, got " + events());
        }
        return true;
    }

    // ---------------------------------------------------------------- time (C9, B5)

    private boolean opSweep() {
        final long ms = 100 + rng.nextInt(1_500);
        r.advance(ms);
        opText = "ADVANCE " + ms + " + SWEEP at " + r.now;
        r.sweep();
        justSwept = true;
        final List<MOrder> due = select(o -> o.expiresAtMs > 0 && o.expiresAtMs <= r.now && !o.expiryRequested);
        due.sort((a, b) -> a.expiresAtMs != b.expiresAtMs ? Long.compare(a.expiresAtMs, b.expiresAtMs) : Long.compare(a.id, b.id));
        for (final MOrder o : due) {
            o.expiryRequested = true;
            o.cancelPending = true;
            if (!o.dispatchPending && o.leg != 0) {
                mintCancel(o);
                expectDispatch(DispatchKind.CANCEL, o);
                orderEvent(o.id, EventKind.DISPATCHED);
            }
        }
        return true;
    }
}

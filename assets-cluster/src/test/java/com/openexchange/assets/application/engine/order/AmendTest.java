// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.AmendSupport.APPLIED;
import static com.openexchange.assets.application.engine.order.AmendSupport.KIND_AMEND;
import static com.openexchange.assets.application.engine.order.AmendSupport.LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.ME_REJECTED;
import static com.openexchange.assets.application.engine.order.AmendSupport.NEW_LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.PRICE;
import static com.openexchange.assets.application.engine.order.AmendSupport.REPLACE_WINDOW_MS;
import static com.openexchange.assets.application.engine.order.AmendSupport.SELL_LEG;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 G3 (contract 3.4 = lifecycle B7, rows R-C12a..e, critic G12): the replace window. The hold delta
 * is placed in the amend apply (top-up) or released at resolution (negative delta); the ME outcome resolves
 * or aborts; a new-leg terminal resolves first; the replace deadline is observability only.
 */
public class AmendTest {

    // contract 3.4 / R-C12a: amend up with insufficient funds is rejected atomically (no window, no hold change)
    @Test
    public void rC12a_amendUpWithInsufficientFundsIsRejectedWithoutAWindow() {
        final AmendSupport s = AmendSupport.base(); // user 1: 900 available, 100 held on order 1
        final OrderRig r = s.r;
        r.markNow();
        s.amend(1, 1, 0, 20 * ONE); // delta = 100 x 20 - 100 x 1 = 1900 > 900 available
        assertEquals(CoreRejectReason.INSUFFICIENT_FUNDS, s.rejectOnly("amend up beyond available"));
        final CoreOrder o = s.order(1);
        assertFalse("no window on a reject", o.has(CoreOrder.F_REPLACE_PENDING));
        assertFalse(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertEquals(0, o.attempt);
        assertEquals(0L, o.pendingHoldDelta);
        assertEquals(0L, o.pendingQuantity);
        assertEquals(ONE, o.quantity);
        final Account a = s.acct(1);
        assertEquals("hold unchanged", 100 * ONE, a.holdRemaining(1));
        assertEquals(100 * ONE, a.locked(USD));
        assertEquals(900 * ONE, a.available(USD));
        assertEquals("no dispatch", 1, r.engine.getDispatchSeq());
        assertEquals(0, s.replaceDeadlines());
    }

    // contract 3.4: amend up with funds opens the window, tops up the hold now, mints one AMEND, arms the replace deadline
    @Test
    public void amendUpWithFundsOpensTheWindowAndMintsOneAmend() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        final long t0 = r.now;
        final long seqBefore = r.engine.getEventSeq();
        r.markNow();
        s.amend(1, 1, PRICE, 2 * ONE); // delta = 100 x (2 - 0) - 100 x (1 - 0) = +100
        final Account a = s.acct(1);
        assertEquals("the top-up is visible in holdRemaining in the same apply", 200 * ONE, a.holdRemaining(1));
        assertEquals(200 * ONE, a.locked(USD));
        assertEquals(800 * ONE, a.available(USD));
        final CoreOrder o = s.order(1);
        assertTrue("F_REPLACE_PENDING", o.has(CoreOrder.F_REPLACE_PENDING));
        assertTrue("F_DISPATCH_PENDING for the AMEND command", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(OrderStatus.PENDING_REPLACE.code(), o.status);
        assertEquals(PRICE, o.pendingPrice);
        assertEquals(2 * ONE, o.pendingQuantity);
        assertEquals(100 * ONE, o.pendingHoldDelta);
        assertEquals(200 * ONE, o.pendingHoldTarget);
        assertEquals("oldLegId = currentLegId at window open", LEG, o.oldLegId);
        assertEquals(LEG, o.currentLegId);
        assertEquals(1, o.attempt);
        assertEquals(CoreOrder.PK_AMEND, o.pendingKind);
        assertEquals("holdAmount installs only at resolution", 100 * ONE, o.holdAmount);
        assertEquals("price installs only at resolution", PRICE, o.price);
        assertEquals(ONE, o.quantity);
        assertEquals(ONE, o.remainingQty);
        assertEquals(2, r.engine.getDispatchSeq());
        assertEquals("pendingDispatchSeq is the AMEND's dispatchSeq", 2, o.pendingDispatchSeq);
        assertEquals(1, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.AMEND, d.kind());
        assertEquals(1, d.commandIdHigh());
        assertEquals("attempt 1, kind AMEND", (1L << 2) | 2, d.commandIdLow());
        assertEquals(LEG, d.oldOrderId());
        assertEquals(PRICE, d.price());
        assertEquals("quantity = newQty - filled", 2 * ONE, d.quantity());
        assertEquals(0L, d.budget());
        assertEquals(1, d.omsOrderId());
        assertEquals(1, d.userId());
        assertEquals("replace deadline kind 2 at now + 10 s", t0 + REPLACE_WINDOW_MS, s.replaceDeadlineOf(1));
        assertEquals(1, s.replaceDeadlines());
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals(OrderStatus.PENDING_REPLACE, ack.status());
        assertFalse(ack.duplicate());
        assertEquals(1, ack.orderId());
        assertEquals(USD, ack.holdAssetId());
        assertEquals("the ack mirrors the record's holdAmount (the target installs at resolution)", 100 * ONE, ack.holdAmount());
        assertEquals(1, r.count(AssetsEngineEvent.OrderEvent.class));
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.DISPATCHED, ev.kind());
        assertEquals(OrderStatus.PENDING_REPLACE, ev.status());
        assertEquals(CoreRejectReason.NONE, ev.reason());
        assertEquals("holdRemaining shows the top-up now", 200 * ONE, ev.holdRemaining());
        assertEquals(100 * ONE, ev.holdAmount());
        assertEquals(LEG, ev.meOrderId());
        assertEquals("one OrderEvent per row", seqBefore + 1, r.engine.getEventSeq());
        assertEquals(seqBefore + 1, ev.eventSeq());
    }

    // contract 3.3 AMEND APPLIED: resolve installs price/quantity, recomputes remaining, holdAmount = target, new leg
    @Test
    public void amendOutcomeAppliedResolvesTheWindow() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.amend(1, 1, PRICE, 2 * ONE);
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        final CoreOrder o = s.order(1);
        assertNotNull("order 1 stays live after a resolved amend", o);
        assertEquals("new leg installed", NEW_LEG, o.currentLegId);
        assertEquals(PRICE, o.price);
        assertEquals(2 * ONE, o.quantity);
        assertEquals("remainingQty = quantity - filledQty", 2 * ONE, o.remainingQty);
        assertEquals(0L, o.filledQty);
        assertEquals("holdAmount = pendingHoldTarget", 200 * ONE, o.holdAmount);
        assertEquals(0L, o.oldLegId);
        assertFalse(o.has(CoreOrder.F_REPLACE_PENDING));
        assertFalse(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0L, o.pendingDispatchSeq);
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertEquals("replace deadline removed", 0, s.replaceDeadlines());
        final Account a = s.acct(1);
        assertEquals(200 * ONE, a.holdRemaining(1));
        assertEquals(200 * ONE, a.locked(USD));
        assertEquals(800 * ONE, a.available(USD));
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.REPLACED, ev.kind());
        assertEquals(CoreRejectReason.NONE, ev.reason());
        assertEquals(OrderStatus.NEW, ev.status());
        assertEquals(PRICE, ev.price());
        assertEquals(2 * ONE, ev.quantity());
        assertEquals(2 * ONE, ev.remainingQty());
        assertEquals(200 * ONE, ev.holdAmount());
        assertEquals(200 * ONE, ev.holdRemaining());
        assertEquals(NEW_LEG, ev.meOrderId());
        assertEquals(1, r.count(AssetsEngineEvent.OrderEvent.class));
        assertEquals("nothing minted at resolution", 0, r.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals("a positive delta moved at the amend; nothing moves at resolution", 0, r.count(AssetsEngineEvent.Balance.class));
        assertEquals(2, r.engine.getDispatchSeq());
        assertEquals(0, r.orders.staleOutcomeCount());
    }

    // R-C12b: amend down after a partial fill at the old price, then cancel: locked 0, available = deposit - exact fill cost
    @Test
    public void rC12b_amendDownAfterAPartialFillThenCancelLeavesNothingLocked() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.sellSide();
        s.fillBase(40_000_000L); // 0.4 at 100: user 1 pays 40 from the order's hold
        final Account a = s.acct(1);
        assertEquals("precondition (contract 3.1): the fill drew 40 from hold 1", 60 * ONE, a.holdRemaining(1));
        CoreOrder o = s.order(1);
        assertEquals(40_000_000L, o.filledQty);
        assertEquals(60_000_000L, o.remainingQty);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        r.markNow();
        s.amend(1, 1, 0, 80_000_000L); // total 0.8: legQty 0.4, delta = 100 x 0.4 - 100 x 0.6 = -20
        o = s.order(1);
        assertEquals(-20 * ONE, o.pendingHoldDelta);
        assertEquals(80 * ONE, o.pendingHoldTarget);
        assertEquals(PRICE, o.pendingPrice);
        assertEquals(80_000_000L, o.pendingQuantity);
        assertEquals("a negative delta is released at resolution, not now", 60 * ONE, a.holdRemaining(1));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.AMEND, d.kind());
        assertEquals("legQty = newQty - filled", 40_000_000L, d.quantity());
        assertEquals(LEG, d.oldOrderId());
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        assertEquals("resolution releases -delta = 20", 40 * ONE, a.holdRemaining(1));
        assertEquals(40 * ONE, a.locked(USD));
        assertEquals(920 * ONE, a.available(USD));
        o = s.order(1);
        assertEquals(80 * ONE, o.holdAmount);
        assertEquals(80_000_000L, o.quantity);
        assertEquals(40_000_000L, o.remainingQty);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        assertEquals(1, r.count(AssetsEngineEvent.Balance.class));
        final AssetsEngineEvent.OrderEvent replaced = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.REPLACED, replaced.kind());
        assertEquals(OrderStatus.PARTIALLY_FILLED, replaced.status());
        r.markNow();
        r.cancel(1, 1);
        assertEquals(OrderStatus.PENDING_CANCEL, r.first(AssetsEngineEvent.OrderAck.class).status());
        final AssetsEngineEvent.MeDispatch cancel = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, cancel.kind());
        assertEquals("attempt 2, kind CANCEL, against the new leg", (2L << 2) | 1, cancel.commandIdLow());
        assertEquals(NEW_LEG, cancel.oldOrderId());
        r.markNow();
        s.terminal(NEW_LEG, 1, 1, 3);
        assertNull("terminal CANCELLED removes the record", s.order(1));
        assertEquals("locked == 0", 0L, a.locked(USD));
        assertEquals("available = deposit - exact fill cost (100 x 0.4)", 960 * ONE, a.available(USD));
        assertFalse(a.hasHold(1));
        assertEquals(40_000_000L, a.available(BTC));
        assertEquals(0, r.orders.orders().openCount(1));
        final AssetsEngineEvent.OrderEvent cancelled = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.CANCELLED, cancelled.kind());
        assertEquals(OrderStatus.CANCELLED, cancelled.status());
        final Account b = s.acct(2);
        assertEquals("counterparty credited 40 USD", 40 * ONE, b.available(USD));
        assertEquals("counterparty's own hold drawn by 0.4 BTC", 60_000_000L, b.locked(BTC));
    }

    // R-C12c / contract 3.3 AMEND result 1: abort releases the pending delta, old leg and price stay, REPLACED reason ME_REJECTED
    @Test
    public void rC12c_meRejectsTheAmend_abortReleasesTheDeltaAndKeepsTheOldLeg() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals("precondition: the top-up is in place", 200 * ONE, s.acct(1).holdRemaining(1));
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, ME_REJECTED, 0L, LEG);
        final Account a = s.acct(1);
        assertEquals("abort releases pendingHoldDelta", 100 * ONE, a.holdRemaining(1));
        assertEquals(100 * ONE, a.locked(USD));
        assertEquals(900 * ONE, a.available(USD));
        final CoreOrder o = s.order(1);
        assertNotNull("the order stays live", o);
        assertEquals("still on the old leg", LEG, o.currentLegId);
        assertEquals("old price kept", PRICE, o.price);
        assertEquals(ONE, o.quantity);
        assertEquals(ONE, o.remainingQty);
        assertEquals(100 * ONE, o.holdAmount);
        assertEquals(0L, o.oldLegId);
        assertFalse(o.has(CoreOrder.F_REPLACE_PENDING));
        assertFalse(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0L, o.pendingDispatchSeq);
        assertEquals("status back to NEW", OrderStatus.NEW.code(), o.status);
        assertEquals("no reject reason on the order", 0, o.rejectReason);
        assertEquals("deadline removed", 0, s.replaceDeadlines());
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.REPLACED, ev.kind());
        assertEquals("the reason marks the abort", CoreRejectReason.ME_REJECTED, ev.reason());
        assertEquals(OrderStatus.NEW, ev.status());
        assertEquals(LEG, ev.meOrderId());
        assertEquals(100 * ONE, ev.holdRemaining());
        assertEquals("the release is a balance line", 1, r.count(AssetsEngineEvent.Balance.class));
        assertEquals(0, r.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals(1, r.orders.orders().openCount(1));
    }

    // R-C12d / B7: a terminal 3 on the NEW leg before the outcome: resolve first (3.3), then apply the terminal (3.2)
    @Test
    public void rC12d_terminalOnTheNewLegBeforeTheOutcomeResolvesThenCancels() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.amend(1, 1, PRICE, 2 * ONE);
        r.markNow();
        s.terminal(NEW_LEG, 1, 1, 3);
        assertNull("final state CANCELLED: the record is removed", s.order(1));
        final Account a = s.acct(1);
        assertFalse("no hold", a.hasHold(1));
        assertEquals(0L, a.locked(USD));
        assertEquals(1_000 * ONE, a.available(USD));
        assertEquals(0, s.replaceDeadlines());
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals("not stale: it named the new leg of an open window", 0, r.orders.staleTerminalCount());
        // contract 3.8 #1: the new leg speaks first, so the window resolves (REPLACED) and then the terminal applies
        final java.util.List<AssetsEngineEvent.OrderEvent> evs = r.events().stream()
                .filter(e -> e instanceof AssetsEngineEvent.OrderEvent).map(e -> (AssetsEngineEvent.OrderEvent) e).toList();
        assertEquals("REPLACED then CANCELLED, got " + evs, 2, evs.size());
        assertEquals(EventKind.REPLACED, evs.get(0).kind());
        assertEquals(CoreRejectReason.NONE, evs.get(0).reason());
        final AssetsEngineEvent.OrderEvent ev = evs.get(1);
        assertEquals(EventKind.CANCELLED, ev.kind());
        assertEquals(OrderStatus.CANCELLED, ev.status());
        assertEquals("resolved before the terminal: the event names the new leg", NEW_LEG, ev.meOrderId());
        assertEquals("resolved before the terminal: holdAmount is the target", 200 * ONE, ev.holdAmount());
        assertEquals("one total release of the 200 held", 1, r.count(AssetsEngineEvent.Balance.class));
        // the late AMEND outcome finds no live order: counted no-op
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        assertEquals(1, r.orders.staleOutcomeCount());
        assertEquals("nothing emitted for a stale outcome", 0, r.events().size());
    }

    // R-C12e: a second amend while one is pending is rejected AMEND_PENDING with no top-up
    @Test
    public void rC12e_secondAmendWhileOneIsPendingIsRejectedWithoutATopUp() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.amend(1, 1, PRICE, 2 * ONE);
        final int secBefore = r.orders.user(1).secCount;
        r.markNow();
        s.amend(1, 1, PRICE, 3 * ONE);
        assertEquals(CoreRejectReason.AMEND_PENDING, s.rejectOnly("second amend while one is pending"));
        final Account a = s.acct(1);
        assertEquals("no second top-up", 200 * ONE, a.holdRemaining(1));
        assertEquals(200 * ONE, a.locked(USD));
        assertEquals("no second dispatch", 2, r.engine.getDispatchSeq());
        final CoreOrder o = s.order(1);
        assertEquals(1, o.attempt);
        assertEquals("the first window stands", 2 * ONE, o.pendingQuantity);
        assertEquals(100 * ONE, o.pendingHoldDelta);
        assertEquals("AMEND_PENDING is decided before the rate check: no unit consumed", secBefore, r.orders.user(1).secCount);
    }

    // G12: the same AmendOrder requestId replayed after APPLIED returns a duplicate ack and changes nothing
    @Test
    public void g12_sameAmendRequestIdReplayedAfterAppliedIsADuplicateAckAndChangesNothing() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        final AmendOrderCommand cmd = s.amend(1, 1, PRICE, 2 * ONE);
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        final Account a = s.acct(1);
        final long hold = a.holdRemaining(1);
        final long locked = a.locked(USD);
        final long available = a.available(USD);
        final long dispatchSeq = r.engine.getDispatchSeq();
        final long eventSeq = r.engine.getEventSeq();
        final int sec = r.orders.user(1).secCount;
        r.markNow();
        s.amend(cmd); // byte-equal retry: same requestId, same payload
        final AssetsEngineEvent.OrderAck ack = r.only(AssetsEngineEvent.OrderAck.class);
        assertTrue("replayed decision", ack.duplicate());
        assertEquals(1, ack.orderId());
        assertEquals("the duplicate ack carries the CURRENT status", OrderStatus.NEW, ack.status());
        assertEquals("holdRemaining identical", hold, a.holdRemaining(1));
        assertEquals(locked, a.locked(USD));
        assertEquals(available, a.available(USD));
        assertEquals("no dispatch", dispatchSeq, r.engine.getDispatchSeq());
        assertEquals("no event", eventSeq, r.engine.getEventSeq());
        assertEquals("no rate unit", sec, r.orders.user(1).secCount);
        final CoreOrder o = s.order(1);
        assertFalse("no new window", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(1, o.attempt);
        assertEquals(NEW_LEG, o.currentLegId);
        assertEquals(0, s.replaceDeadlines());
    }

    // contract 3.1 + 3.3 (B7 "trade on the old leg during the window"): amend, fill on the old leg, then resolve
    @Test
    public void amendThenFillOnTheOldLegThenResolve_remainingIsRecomputedFromFilled() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.sellSide();
        s.amend(1, 1, PRICE, 2 * ONE); // hold 200
        s.fillBase(50_000_000L); // 0.5 at 100 on leg 77 while the window is open
        final Account a = s.acct(1);
        assertEquals("the fill settles from the (topped-up) hold", 150 * ONE, a.holdRemaining(1));
        CoreOrder o = s.order(1);
        assertEquals(50_000_000L, o.filledQty);
        assertEquals("remaining against the OLD quantity until resolution", 50_000_000L, o.remainingQty);
        assertTrue("the window survives a partial fill", o.has(CoreOrder.F_REPLACE_PENDING));
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        o = s.order(1);
        assertNotNull(o);
        assertEquals(2 * ONE, o.quantity);
        assertEquals(50_000_000L, o.filledQty);
        assertEquals("remainingQty = 2 - 0.5", 150_000_000L, o.remainingQty);
        assertEquals(200 * ONE, o.holdAmount);
        assertEquals(NEW_LEG, o.currentLegId);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        assertEquals(150 * ONE, a.holdRemaining(1));
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.REPLACED, ev.kind());
        assertEquals(OrderStatus.PARTIALLY_FILLED, ev.status());
        assertEquals(150_000_000L, ev.remainingQty());
        assertEquals(50_000_000L, ev.filledQty());
    }

    // contract 3.4: a SELL amend changes the base-asset hold by the quantity delta (up, then down)
    @Test
    public void aSellAmendChangesTheBaseAssetHoldByTheQuantityDelta() {
        final AmendSupport s = new AmendSupport(OrderRig.open());
        final OrderRig r = s.r;
        r.deposit(2, BTC, 3 * ONE);
        r.create(r.limit(2, 1, 1, PRICE, ONE)); // orderId 1: SELL 100 x 1, hold 1 BTC
        s.createOutcome(2, 1, SELL_LEG);
        s.assertLeg(1, SELL_LEG);
        final Account a = s.acct(2);
        r.markNow();
        s.amend(2, 1, 0, 250_000_000L); // 2.5: delta = (2.5 - 0) - (1 - 0) = +1.5 BTC
        assertEquals("base-asset top-up of 1.5 BTC", 250_000_000L, a.holdRemaining(1));
        assertEquals(BTC, a.holdAssetId(1));
        assertEquals(250_000_000L, a.locked(BTC));
        assertEquals(50_000_000L, a.available(BTC));
        assertEquals("quote untouched", 0L, a.locked(USD));
        CoreOrder o = s.order(1);
        assertEquals(150_000_000L, o.pendingHoldDelta);
        assertEquals(250_000_000L, o.pendingHoldTarget);
        assertEquals(PRICE, o.pendingPrice);
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(250_000_000L, d.quantity());
        assertEquals(PRICE, d.price());
        assertEquals(1, d.side());
        s.outcome(2, 1, 1, KIND_AMEND, APPLIED, 89L, SELL_LEG);
        o = s.order(1);
        assertEquals(250_000_000L, o.holdAmount);
        assertEquals(250_000_000L, o.quantity);
        assertEquals(89L, o.currentLegId);
        r.markNow();
        s.amend(2, 1, 0, 2 * ONE); // down: delta = 2 - 2.5 = -0.5 BTC
        o = s.order(1);
        assertEquals(-50_000_000L, o.pendingHoldDelta);
        assertEquals("nothing released until resolution", 250_000_000L, a.holdRemaining(1));
        assertEquals("attempt 2, kind AMEND", (2L << 2) | 2, r.first(AssetsEngineEvent.MeDispatch.class).commandIdLow());
        s.outcome(2, 1, 2, KIND_AMEND, APPLIED, 90L, 89L);
        assertEquals("0.5 BTC released at resolution", 2 * ONE, a.holdRemaining(1));
        assertEquals(2 * ONE, a.locked(BTC));
        assertEquals(ONE, a.available(BTC));
        assertEquals(2 * ONE, s.order(1).holdAmount);
    }

    // contract 3.4 (D6/B5): the replace deadline fires once, counts replaceSlowCount, changes no state; the outcome still resolves
    @Test
    public void replaceDeadlineFiringIsObservabilityOnlyAndOneShot() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals(1, s.replaceDeadlines());
        r.advance(REPLACE_WINDOW_MS - 1);
        r.markNow();
        r.sweep();
        assertEquals("not due yet", 0, r.orders.replaceSlowCount());
        assertEquals(1, s.replaceDeadlines());
        r.advance(1);
        r.markNow();
        r.sweep();
        assertEquals("the replace deadline counts once", 1, r.orders.replaceSlowCount());
        assertEquals("observability only: nothing emitted", 0, r.events().size());
        assertEquals("re-armed while the window stays open (contract 3.8 #14)", 1, s.replaceDeadlines());
        final CoreOrder o = s.order(1);
        assertTrue("no timeout abort: the window stays open", o.has(CoreOrder.F_REPLACE_PENDING));
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(OrderStatus.PENDING_REPLACE.code(), o.status);
        assertEquals(100 * ONE, o.pendingHoldDelta);
        assertEquals(LEG, o.currentLegId);
        assertEquals(1, o.attempt);
        assertEquals("hold untouched", 200 * ONE, s.acct(1).holdRemaining(1));
        assertEquals(2, r.engine.getDispatchSeq());
        r.markNow();
        r.sweep();
        assertEquals("a sweep before the next deadline does not fire", 1, r.orders.replaceSlowCount());
        r.advance(REPLACE_WINDOW_MS);
        r.markNow();
        r.sweep();
        assertEquals("a stuck replace keeps ticking (contract 3.8 #14)", 2, r.orders.replaceSlowCount());
        assertEquals(0, r.events().size());
        // the durable outcome resolves the window regardless of the elapsed time
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        assertEquals(EventKind.REPLACED, r.first(AssetsEngineEvent.OrderEvent.class).kind());
        assertEquals(200 * ONE, s.order(1).holdAmount);
        assertEquals(OrderStatus.NEW.code(), s.order(1).status);
    }
}

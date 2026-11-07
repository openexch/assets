// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.AmendSupport.LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.NEW_LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.PRICE;
import static com.openexchange.assets.application.engine.order.AmendSupport.SELL_LEG;
import static com.openexchange.assets.application.engine.order.EdgeSupport.baseWithWideSell;
import static com.openexchange.assets.application.engine.order.EdgeSupport.eventsOf;
import static com.openexchange.assets.application.engine.order.EdgeSupport.gtdExpiredInsideTheWindow;
import static com.openexchange.assets.application.engine.order.EdgeSupport.kinds;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.APPLIED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.BUY;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.CAPACITY;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.CONFLICT;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_AMEND;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_CANCEL;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.LEG_A;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ME_CANCELLED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.NOT_OWNER;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.REJECTED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.UNKNOWN_LEG;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.UNKNOWN_MARKET;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ack;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.event;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.live;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.outcome;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.terminal;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.trade;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 GAP group, outcome half (contract 3.3 outcomes on pending CANCEL/AMEND commands, 3.7 GTD inside a
 * window, and the RED-review decisions 3.8 #9, #10, #13): edge cases the G2/G3 suites leave open. One contract
 * line per test, pinned through the recorded events and the record. A failure here is a finding for the
 * implementer (engine or contract), never a reason to weaken the assertion. The feed half is {@link FeedEdgeCasesTest}.
 */
public class OutcomeEdgeCasesTest {

    /** 3.3 result 2 on a pending CANCEL quarantines (flag kept, no retry); result 3 on a pending AMEND closes the gate and keeps the AMEND pending. */
    @Test
    public void conflictOnAPendingCancelQuarantinesAndCapacityOnAPendingAmendClosesTheGate() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, BUY, 100 * ONE, 2 * ONE));
        ack(r, 1, LEG_A);
        r.cancel(1, 1);
        final CoreOrder o = live(r, 1);
        assertEquals("precondition (3.3): the cancel of the resting leg is dispatch 2", 2, o.pendingDispatchSeq);
        r.markNow();
        outcome(r, 1, 1, KIND_CANCEL, CONFLICT, LEG_A, LEG_A);
        assertEquals("CONFLICT: one QUARANTINED event and nothing else, got " + r.events(), 1, r.events().size());
        final AssetsEngineEvent.OrderEvent q = event(r, 1, EventKind.QUARANTINED);
        assertEquals(CoreRejectReason.ME_CONFLICT, q.reason());
        assertEquals("quarantine is a flag, never a status", OrderStatus.NEW, q.status());
        assertTrue(o.has(CoreOrder.F_QUARANTINED));
        assertTrue("CANCEL_PENDING kept", o.has(CoreOrder.F_CANCEL_PENDING));
        assertFalse("no retry", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o.pendingDispatchSeq);
        assertEquals(CoreOrder.PK_NONE, o.pendingKind);
        assertEquals(1, o.attempt);
        assertEquals(LEG_A, o.currentLegId);
        assertEquals(1, r.orders.conflictCount());
        assertEquals("the hold is untouched", 200 * ONE, r.engine.account(1).locked(USD));
        assertEquals("the fence is not tripped", GateState.OPEN, r.engine.gate().state(OrderEntryGate.GLOBAL));
        assertEquals(2, r.engine.getDispatchSeq());
        r.markNow();
        r.cancel(1, 1);
        assertEquals(CoreRejectReason.NOT_OPEN, r.reject().reason());
        assertEquals("nothing minted after the quarantine", 2, r.engine.getDispatchSeq());
        r.markNow();
        terminal(r, 1, LEG_A, ME_CANCELLED);
        assertEquals("the ME's terminal still settles the order", OrderStatus.CANCELLED, event(r, 1, EventKind.CANCELLED).status());
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.engine.account(1).locked(USD));

        final AmendSupport s = AmendSupport.base();
        final OrderRig r2 = s.r;
        s.amend(1, 1, PRICE, 2 * ONE);
        final CoreOrder a = s.order(1);
        assertEquals("precondition (3.4): the AMEND is dispatch 2", 2, a.pendingDispatchSeq);
        final long deadline = s.replaceDeadlineOf(1);
        r2.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, CAPACITY, 0L, LEG);
        assertEquals("ME capacity closes the GLOBAL gate", GateState.SUSPENDED, r2.engine.gate().state(OrderEntryGate.GLOBAL));
        assertEquals(GateReason.ME_CAPACITY, r2.engine.gate().reason(OrderEntryGate.GLOBAL));
        final AssetsEngineEvent.AdmissionState st = r2.only(AssetsEngineEvent.AdmissionState.class);
        assertEquals(OrderEntryGate.GLOBAL, st.marketId());
        assertEquals(GateState.SUSPENDED, st.gateState());
        assertEquals(GateReason.ME_CAPACITY, st.gateReason());
        assertFalse("BROADCAST", st.toOrigin());
        assertTrue("the AMEND stays pending", a.has(CoreOrder.F_DISPATCH_PENDING));
        assertTrue("REPLACE_PENDING kept", a.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(CoreOrder.PK_AMEND, a.pendingKind);
        assertEquals("pendingDispatchSeq unchanged", 2, a.pendingDispatchSeq);
        assertEquals(1, a.attempt);
        assertEquals(OrderStatus.PENDING_REPLACE.code(), a.status);
        assertEquals(PRICE, a.pendingPrice);
        assertEquals(2 * ONE, a.pendingQuantity);
        assertEquals(100 * ONE, a.pendingHoldDelta);
        assertEquals(LEG, a.oldLegId);
        assertEquals(LEG, a.currentLegId);
        assertEquals("the replace deadline is untouched", deadline, s.replaceDeadlineOf(1));
        assertEquals(1, s.replaceDeadlines());
        assertEquals("the top-up stays locked", 200 * ONE, s.acct(1).locked(USD));
        assertEquals(0, r2.orders.staleOutcomeCount());
        r2.markNow();
        r2.orders.applyRequestPendingDispatch(0xCB, 0L, 1024);
        final AssetsEngineEvent.MeDispatchReply d = r2.first(AssetsEngineEvent.MeDispatchReply.class);
        assertEquals("the resync re-offers the AMEND", DispatchKind.AMEND, d.kind());
        assertEquals(2, d.dispatchSeq());
        assertEquals((1L << 2) | 2, d.commandIdLow());
        assertEquals(LEG, d.oldOrderId());
        assertEquals(PRICE, d.price());
        assertEquals(2 * ONE, d.quantity());
    }

    /** 3.3 results 4, 5, 6 on a pending AMEND abort the window with reasons ME_UNKNOWN_LEG, NOT_OWNER, UNKNOWN_MARKET. */
    @Test
    public void amendOutcomeResults4To6AbortTheWindowWithTheMeReason() {
        final int[] results = {UNKNOWN_LEG, NOT_OWNER, UNKNOWN_MARKET};
        final CoreRejectReason[] reasons = {CoreRejectReason.ME_UNKNOWN_LEG, CoreRejectReason.NOT_OWNER, CoreRejectReason.UNKNOWN_MARKET};
        for (int i = 0; i < results.length; i++) {
            final String c = "result " + results[i] + ": ";
            final AmendSupport s = AmendSupport.base();
            final OrderRig r = s.r;
            s.amend(1, 1, PRICE, 2 * ONE); // +100 top-up, AMEND = dispatch 2
            assertEquals(c + "precondition (3.4): top-up locked", 200 * ONE, s.acct(1).locked(USD));
            r.markNow();
            s.outcome(1, 1, 1, KIND_AMEND, results[i], 0L, LEG);
            assertEquals(c + "the top-up release then REPLACED, got " + r.events(), 2, r.events().size());
            assertEquals(c, 1, r.count(AssetsEngineEvent.Balance.class));
            final AssetsEngineEvent.OrderEvent ev = event(r, 1, EventKind.REPLACED);
            assertEquals(c + "abort reason", reasons[i], ev.reason());
            assertEquals(c + "status unchanged", OrderStatus.NEW, ev.status());
            assertEquals(c, PRICE, ev.price());
            assertEquals(c, ONE, ev.quantity());
            assertEquals(c, ONE, ev.remainingQty());
            assertEquals(c, LEG, ev.meOrderId());
            assertEquals(c, 100 * ONE, ev.holdAmount());
            assertEquals(c, 100 * ONE, ev.holdRemaining());
            final CoreOrder o = s.order(1);
            assertNotNull(c + "the order stays live", o);
            assertFalse(c, o.has(CoreOrder.F_REPLACE_PENDING));
            assertFalse(c, o.has(CoreOrder.F_DISPATCH_PENDING));
            assertEquals(c, 0, o.pendingDispatchSeq);
            assertEquals(c, OrderStatus.NEW.code(), o.status);
            assertEquals(c, LEG, o.currentLegId);
            assertEquals(c, 0, o.oldLegId);
            assertEquals(c, 0, o.pendingHoldDelta);
            assertEquals(c, 0, o.pendingHoldTarget);
            assertEquals(c, PRICE, o.price);
            assertEquals(c, ONE, o.quantity);
            assertEquals(c + "top-up released", 100 * ONE, s.acct(1).locked(USD));
            assertEquals(c, 900 * ONE, s.acct(1).available(USD));
            assertEquals(c, 100 * ONE, s.acct(1).holdRemaining(1));
            assertEquals(c, 0, s.replaceDeadlines());
            assertEquals(c + "no retry", 2, r.engine.getDispatchSeq());
            assertEquals(c, 0, r.orders.staleOutcomeCount());
        }
    }

    /** 3.3 CREATE APPLIED on a QUARANTINED order: ACK_NEW, leg learned, no cancel minted (CANCEL_PENDING case and IOC case). */
    @Test
    public void createAppliedOnAQuarantinedOrderAcksWithoutMintingTheCancel() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, BUY, 100 * ONE, 2 * ONE));
        r.cancel(1, 1);
        r.orders.applyQuarantine(0x61, 1L, r.now);
        final CoreOrder o = live(r, 1);
        assertTrue("precondition (S4): cancel on an unresolved create is a flag", o.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue("precondition (3.5): quarantined", o.has(CoreOrder.F_QUARANTINED));
        assertEquals(1, r.engine.getDispatchSeq());
        r.markNow();
        ack(r, 1, LEG_A);
        assertEquals("ACK_NEW only: no cancel minted for a quarantined order (3.3), got " + r.events(), 1, r.events().size());
        final AssetsEngineEvent.OrderEvent ev = r.only(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.ACK_NEW, ev.kind());
        assertEquals(OrderStatus.NEW, ev.status());
        assertEquals(LEG_A, ev.meOrderId());
        assertEquals("the leg is learned", LEG_A, o.currentLegId);
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertFalse(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o.pendingDispatchSeq);
        assertEquals("no mint: attempt stays 0", 0, o.attempt);
        assertEquals(CoreOrder.PK_NONE, o.pendingKind);
        assertTrue("the user's intent stays recorded", o.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue(o.has(CoreOrder.F_QUARANTINED));
        assertEquals("dispatchSeq unchanged", 1, r.engine.getDispatchSeq());
        assertEquals(0, r.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals(200 * ONE, r.engine.account(1).locked(USD));

        final OrderRig r2 = OrderRig.open();
        r2.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand ioc = r2.limit(1, 1, BUY, 100 * ONE, 2 * ONE);
        ioc.timeInForce = OrderEngine.TIF_IOC;
        r2.create(ioc);
        r2.orders.applyQuarantine(0x62, 1L, r2.now);
        final CoreOrder o2 = live(r2, 1);
        assertTrue(o2.has(CoreOrder.F_IOC));
        assertTrue("precondition (3.5): quarantined", o2.has(CoreOrder.F_QUARANTINED));
        r2.markNow();
        ack(r2, 1, LEG_A);
        assertEquals("IOC quarantined: ACK_NEW only, got " + r2.events(), 1, r2.events().size());
        assertEquals(EventKind.ACK_NEW, r2.only(AssetsEngineEvent.OrderEvent.class).kind());
        assertEquals(LEG_A, o2.currentLegId);
        assertEquals(OrderStatus.NEW.code(), o2.status);
        assertFalse("the IOC emulation is skipped entirely: no flag, no mint", o2.has(CoreOrder.F_CANCEL_PENDING));
        assertFalse(o2.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o2.attempt);
        assertEquals("dispatchSeq unchanged", 1, r2.engine.getDispatchSeq());
        assertEquals(0, r2.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals(200 * ONE, r2.engine.account(1).locked(USD));
    }

    /** 3.8 #9: GTD expiry while REPLACE_PENDING sets the flags; the one cancel is minted when the window closes, on the then-current leg. */
    @Test
    public void gtdExpiryWhileReplacePendingMintsTheCancelWhenTheWindowCloses() {
        final AmendSupport s = gtdExpiredInsideTheWindow();
        final OrderRig r = s.r;
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        final List<AssetsEngineEvent.OrderEvent> e = eventsOf(r, 1);
        assertEquals("APPLIED: REPLACED then the cancel's DISPATCHED (3.8 #9), got " + e, List.of(EventKind.REPLACED, EventKind.DISPATCHED), kinds(e));
        assertEquals(CoreRejectReason.NONE, e.get(0).reason());
        assertEquals(NEW_LEG, e.get(0).meOrderId());
        assertEquals("exactly one cancel", 1, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d.kind());
        assertEquals("the cancel names the NEW leg", NEW_LEG, d.oldOrderId());
        assertEquals((2L << 2) | 1, d.commandIdLow());
        assertEquals(3, d.dispatchSeq());
        CoreOrder o = s.order(1);
        assertEquals(NEW_LEG, o.currentLegId);
        assertTrue(o.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue(o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(CoreOrder.PK_CANCEL, o.pendingKind);
        assertEquals(3, o.pendingDispatchSeq);
        assertEquals(2, o.attempt);
        assertFalse(o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(2 * ONE, o.quantity);
        assertEquals(200 * ONE, s.acct(1).locked(USD));
        r.markNow();
        s.terminal(NEW_LEG, 1, 1, ME_CANCELLED);
        assertEquals("the journal terminal 3 surfaces as EXPIRED", OrderStatus.EXPIRED, event(r, 1, EventKind.EXPIRED).status());
        assertNull(r.orders.orders().get(1));
        assertEquals(0, s.acct(1).locked(USD));
        assertEquals(1_000 * ONE, s.acct(1).available(USD));
        assertEquals(3, r.engine.getDispatchSeq());

        final AmendSupport s2 = gtdExpiredInsideTheWindow();
        final OrderRig r2 = s2.r;
        r2.markNow();
        s2.outcome(1, 1, 1, KIND_AMEND, REJECTED, 0L, LEG);
        final List<AssetsEngineEvent.OrderEvent> e2 = eventsOf(r2, 1);
        assertEquals("abort: REPLACED(ME_REJECTED) then the cancel's DISPATCHED, got " + e2, List.of(EventKind.REPLACED, EventKind.DISPATCHED), kinds(e2));
        assertEquals(CoreRejectReason.ME_REJECTED, e2.get(0).reason());
        assertEquals(1, r2.count(AssetsEngineEvent.Balance.class));
        assertEquals("the top-up came back", 100 * ONE, s2.acct(1).locked(USD));
        final AssetsEngineEvent.MeDispatch d2 = r2.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d2.kind());
        assertEquals("the cancel names the OLD leg", LEG, d2.oldOrderId());
        assertEquals((2L << 2) | 1, d2.commandIdLow());
        assertEquals(3, d2.dispatchSeq());
        o = s2.order(1);
        assertEquals(LEG, o.currentLegId);
        assertEquals(ONE, o.quantity);
        assertTrue(o.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue(o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertEquals(3, o.pendingDispatchSeq);
        r2.markNow();
        s2.terminal(LEG, 1, 1, ME_CANCELLED);
        assertEquals(OrderStatus.EXPIRED, event(r2, 1, EventKind.EXPIRED).status());
        assertNull(r2.orders.orders().get(1));
        assertEquals(0, s2.acct(1).locked(USD));
        assertEquals(1_000 * ONE, s2.acct(1).available(USD));
    }

    /** 3.8 #10: fills inside the window reaching the new (lower) quantity: REPLACED then FILL/FILLED in the one apply, everything released. */
    @Test
    public void fillsInsideTheWindowReachingTheNewQuantityTerminalizeFilledAtResolution() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.sellSide();                  // order 2 = dispatch 2
        s.fillBase(60_000_000L);       // trade 1: 0.6 at 100
        s.amend(1, 1, 0, 80_000_000L); // total 0.8: legQty 0.2, delta = 100 x 0.2 - 100 x 0.4 = -20; AMEND = dispatch 3
        CoreOrder o = s.order(1);
        assertEquals("precondition (3.4)", -20 * ONE, o.pendingHoldDelta);
        s.fillBase(20_000_000L);       // trade 2 inside the window, on the old leg
        o = s.order(1);
        assertEquals(80_000_000L, o.filledQty);
        assertEquals("remaining still against the OLD quantity 1.0", 20_000_000L, o.remainingQty);
        assertEquals("3.8 #7: a fill inside the window keeps PENDING_REPLACE", OrderStatus.PENDING_REPLACE.code(), o.status);
        assertEquals(20 * ONE, s.acct(1).holdRemaining(1));
        final long seqBefore = r.engine.getDispatchSeq();
        assertEquals("precondition: create 1, create 2, amend", 3, seqBefore);
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        final List<AssetsEngineEvent.OrderEvent> e = eventsOf(r, 1);
        assertEquals("REPLACED then FILL in the same apply (3.8 #10), got " + e, List.of(EventKind.REPLACED, EventKind.FILL), kinds(e));
        final AssetsEngineEvent.OrderEvent replaced = e.get(0);
        assertEquals(CoreRejectReason.NONE, replaced.reason());
        assertEquals(OrderStatus.PARTIALLY_FILLED, replaced.status());
        assertEquals(80_000_000L, replaced.quantity());
        assertEquals(80_000_000L, replaced.filledQty());
        assertEquals(0, replaced.remainingQty());
        assertEquals(NEW_LEG, replaced.meOrderId());
        assertEquals(80 * ONE, replaced.holdAmount());
        assertEquals("the -delta release ran before the event", 0, replaced.holdRemaining());
        final AssetsEngineEvent.OrderEvent fill = e.get(1);
        assertEquals(OrderStatus.FILLED, fill.status());
        assertEquals(80_000_000L, fill.filledQty());
        assertEquals(0, fill.remainingQty());
        assertEquals(0, fill.holdRemaining());
        assertEquals(NEW_LEG, fill.meOrderId());
        assertNull("record gone", r.orders.orders().get(1));
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals("hold fully released", 0, s.acct(1).locked(USD));
        assertEquals("available = 1000 - 0.8 x 100", 920 * ONE, s.acct(1).available(USD));
        assertEquals(80_000_000L, s.acct(1).available(BTC));
        assertFalse(s.acct(1).hasHold(1));
        assertEquals(0, s.replaceDeadlines());
        assertEquals("no cancel minted for a filled order", 0, r.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals(seqBefore, r.engine.getDispatchSeq());
        new FeedRig(r).assertLockedEqualsHolds(1);
        final CoreOrder sell = s.order(2);
        assertNotNull(sell);
        assertEquals(80_000_000L, sell.filledQty);
        assertEquals(20_000_000L, sell.remainingQty);
    }

    /**
     * 3.8 #13 with #1: a cancel flagged while REPLACE_PENDING is minted "when the window closes"; a trade on the new
     * leg closes the window (3.8 #1), so the flagged cancel must go out on the new leg in that same apply. Without it
     * the order rests at the ME with CANCEL_PENDING set and no command in flight, and a repeated CancelOrder is
     * acknowledged as already pending (R-C8c) without ever minting.
     */
    @Test
    public void cancelFlaggedInsideTheWindowIsMintedWhenANewLegTradeClosesIt() {
        final AmendSupport s = baseWithWideSell();
        final OrderRig r = s.r;
        s.amend(1, 1, PRICE, 2 * ONE); // AMEND = dispatch 3
        final long seqBefore = r.engine.getDispatchSeq();
        assertEquals("precondition: create 1, create 2, amend", 3, seqBefore);
        r.cancel(1, 1);
        final CoreOrder o = s.order(1);
        assertTrue("3.8 #13: cancel while REPLACE_PENDING is a flag", o.has(CoreOrder.F_CANCEL_PENDING));
        assertEquals("no mint while the amend is in flight", seqBefore, r.engine.getDispatchSeq());
        r.markNow();
        trade(r, 1, 1, 2, SELL_LEG, 2, 1, NEW_LEG, 1, PRICE, ONE, false); // 1 of the new 2 fills on the new leg
        final List<AssetsEngineEvent.OrderEvent> e = eventsOf(r, 1);
        assertEquals("REPLACED, FILL, then the flagged cancel's DISPATCHED (3.8 #1 + #13), got " + e,
                List.of(EventKind.REPLACED, EventKind.FILL, EventKind.DISPATCHED), kinds(e));
        assertEquals("the window closed, so the flagged cancel is minted on the then-current leg (3.8 #9/#13)",
                1, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d.kind());
        assertEquals(NEW_LEG, d.oldOrderId());
        assertEquals((2L << 2) | 1, d.commandIdLow());
        assertEquals(seqBefore + 1, d.dispatchSeq());
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(CoreOrder.PK_CANCEL, o.pendingKind);
        assertEquals(seqBefore + 1, o.pendingDispatchSeq);
        assertEquals(2, o.attempt);
        assertEquals(NEW_LEG, o.currentLegId);
        assertEquals(ONE, o.remainingQty);
    }
}

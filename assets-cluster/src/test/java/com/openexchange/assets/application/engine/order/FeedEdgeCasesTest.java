// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.AmendSupport.LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.NEW_LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.PRICE;
import static com.openexchange.assets.application.engine.order.AmendSupport.SELL_LEG;
import static com.openexchange.assets.application.engine.order.EdgeSupport.ME_EXPIRED;
import static com.openexchange.assets.application.engine.order.EdgeSupport.baseWithWideSell;
import static com.openexchange.assets.application.engine.order.EdgeSupport.eventsOf;
import static com.openexchange.assets.application.engine.order.EdgeSupport.kinds;
import static com.openexchange.assets.application.engine.order.EdgeSupport.withdraw;
import static com.openexchange.assets.application.engine.order.FeedRig.LEG1;
import static com.openexchange.assets.application.engine.order.FeedRig.MKT;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.APPLIED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.BUY;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_AMEND;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.LEG_A;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.LEG_B;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ME_CANCELLED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.REJECTED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.SELL;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ack;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.event;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.live;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.trade;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 GAP group, feed half (contract 3.1 trades, 3.2 terminals, and the RED-review decisions 3.8 #1 and #8):
 * edge cases the G1 suite leaves open. One contract line per test, pinned through the recorded events and the
 * record. A failure here is a finding for the implementer (engine or contract), never a reason to weaken the
 * assertion. The outcome half is {@link OutcomeEdgeCasesTest}.
 */
public class FeedEdgeCasesTest {

    /** 3.2: terminal EXPIRED (5) on the current leg: status EXPIRED, kind EXPIRED, hold released once, record gone. */
    @Test
    public void terminalExpiredOnTheCurrentLegReleasesTheHoldAndRemovesTheRecord() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        final long cancelledBefore = r.orders.cancelledCount();
        r.markNow();
        f.terminal(1, 1, LEG1, ME_EXPIRED);
        assertEquals("terminal EXPIRED: the release BALANCE then the terminal event (3.8 #12), got " + r.events(), 2, r.events().size());
        assertEquals("BALANCE u=1 asset=0 avail=100000000000 locked=0", r.events().get(0).render());
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals(1, e1.size());
        final AssetsEngineEvent.OrderEvent ev = e1.get(0);
        assertEquals(EventKind.EXPIRED, ev.kind());
        assertEquals(OrderStatus.EXPIRED, ev.status());
        assertEquals(CoreRejectReason.NONE, ev.reason());
        assertEquals("holdRemaining is 0 on every terminal event", 0, ev.holdRemaining());
        assertEquals(LEG1, ev.meOrderId());
        assertEquals(10 * ONE, ev.remainingQty());
        assertEquals(0, ev.filledQty());
        assertNull("terminal records leave the store in the same apply", r.orders.orders().get(1));
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertFalse(r.engine.account(1).hasHold(1));
        assertEquals("an EXPIRED terminal on the current leg is not stale", 0, r.orders.staleTerminalCount());
        assertEquals("EXPIRED counts with the cancellations", cancelledBefore + 1, r.orders.cancelledCount());
        assertNotNull("the counterparty is untouched", r.orders.orders().get(2));
        assertEquals(10 * ONE, r.engine.account(2).locked(BTC));
    }

    /** 3.8 #1: the old leg's CANCELLED inside the window is a counted no-op; its REJECTED is the abort. */
    @Test
    public void oldLegCancelledInsideTheWindowIsCountedAndOldLegRejectedAbortsTheWindow() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        final FeedRig f = new FeedRig(r);
        s.amend(1, 1, PRICE, 2 * ONE); // +100 top-up, AMEND = dispatch 2
        CoreOrder o = s.order(1);
        assertTrue("precondition (3.4): window open", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(200 * ONE, s.acct(1).locked(USD));
        final long stale = r.orders.staleTerminalCount();
        final byte[] before = f.image();
        r.markNow();
        s.terminal(LEG, 1, 1, ME_CANCELLED);
        assertEquals("3.8 #1: the old leg's CANCELLED inside the window emits nothing, got " + r.events(), 0, r.events().size());
        assertEquals("counted as a stale terminal", stale + 1, r.orders.staleTerminalCount());
        assertArrayEquals("byte-level no-op", before, f.image());
        o = s.order(1);
        assertNotNull("the order is live", o);
        assertTrue("window intact", o.has(CoreOrder.F_REPLACE_PENDING));
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(CoreOrder.PK_AMEND, o.pendingKind);
        assertEquals(2, o.pendingDispatchSeq);
        assertEquals(OrderStatus.PENDING_REPLACE.code(), o.status);
        assertEquals(100 * ONE, o.pendingHoldDelta);
        assertEquals(LEG, o.oldLegId);
        assertEquals(LEG, o.currentLegId);
        assertEquals("hold intact", 200 * ONE, s.acct(1).locked(USD));
        assertEquals(1, s.replaceDeadlines());

        r.markNow();
        s.terminal(LEG, 1, 1, FeedRig.ME_REJECTED);
        assertEquals("abort: the top-up release then REPLACED, got " + r.events(), 2, r.events().size());
        assertEquals(1, r.count(AssetsEngineEvent.Balance.class));
        final AssetsEngineEvent.OrderEvent ev = event(r, 1, EventKind.REPLACED);
        assertEquals("REPLACED with reason ME_REJECTED is the abort", CoreRejectReason.ME_REJECTED, ev.reason());
        assertEquals("status unchanged tells the gateway it was an abort", OrderStatus.NEW, ev.status());
        assertEquals(PRICE, ev.price());
        assertEquals(ONE, ev.quantity());
        assertEquals(LEG, ev.meOrderId());
        assertEquals(100 * ONE, ev.holdRemaining());
        o = s.order(1);
        assertNotNull("the order stays live on the old leg", o);
        assertFalse(o.has(CoreOrder.F_REPLACE_PENDING));
        assertFalse(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o.pendingDispatchSeq);
        assertEquals(CoreOrder.PK_NONE, o.pendingKind);
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertEquals(LEG, o.currentLegId);
        assertEquals(0, o.oldLegId);
        assertEquals(0, o.pendingHoldDelta);
        assertEquals(PRICE, o.price);
        assertEquals(ONE, o.quantity);
        assertEquals(100 * ONE, o.holdAmount);
        assertEquals("top-up released", 100 * ONE, s.acct(1).locked(USD));
        assertEquals(900 * ONE, s.acct(1).available(USD));
        assertEquals(100 * ONE, s.acct(1).holdRemaining(1));
        assertEquals(0, s.replaceDeadlines());
        assertEquals("the REJECTED terminal is the abort, not a stale", stale + 1, r.orders.staleTerminalCount());
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, REJECTED, 0L, LEG);
        assertEquals("the ME's late AMEND outcome matches no pending dispatch", 1, r.orders.staleOutcomeCount());
        assertEquals(0, r.events().size());
    }

    /** 3.8 #1: a TRADE naming the new leg while the AMEND is pending resolves the window first, then fills on the new leg. */
    @Test
    public void tradeOnTheNewLegWhileTheAmendIsPendingResolvesTheWindowThenFills() {
        final AmendSupport s = baseWithWideSell();
        final OrderRig r = s.r;
        s.amend(1, 1, PRICE, 2 * ONE); // qty 1 -> 2, +100 top-up, AMEND = dispatch 3
        assertTrue("precondition (3.4): window open", s.order(1).has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(200 * ONE, s.acct(1).locked(USD));
        r.markNow();
        trade(r, 1, 1, 2, SELL_LEG, 2, 1, NEW_LEG, 1, PRICE, 150_000_000L, false); // 1.5 on leg 78, unknown to the core
        final List<AssetsEngineEvent.OrderEvent> e1 = eventsOf(r, 1);
        assertEquals("resolve first, then fill: REPLACED then FILL (3.8 #1), got " + e1, List.of(EventKind.REPLACED, EventKind.FILL), kinds(e1));
        final AssetsEngineEvent.OrderEvent replaced = e1.get(0);
        assertEquals(CoreRejectReason.NONE, replaced.reason());
        assertEquals(OrderStatus.NEW, replaced.status());
        assertEquals(2 * ONE, replaced.quantity());
        assertEquals(2 * ONE, replaced.remainingQty());
        assertEquals(NEW_LEG, replaced.meOrderId());
        assertEquals(200 * ONE, replaced.holdAmount());
        final AssetsEngineEvent.OrderEvent fill = e1.get(1);
        assertEquals(OrderStatus.PARTIALLY_FILLED, fill.status());
        assertEquals("the FILL names the new leg", NEW_LEG, fill.meOrderId());
        assertEquals(150_000_000L, fill.filledQty());
        assertEquals("remaining from the NEW quantity 2", 50_000_000L, fill.remainingQty());
        assertEquals(150_000_000L, fill.lastQty());
        assertEquals(1, fill.tradeId());
        assertEquals(50 * ONE, fill.holdRemaining());
        final CoreOrder o = s.order(1);
        assertEquals(NEW_LEG, o.currentLegId);
        assertEquals(0, o.oldLegId);
        assertEquals(2 * ONE, o.quantity);
        assertEquals(150_000_000L, o.filledQty);
        assertEquals(50_000_000L, o.remainingQty);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        assertFalse(o.has(CoreOrder.F_REPLACE_PENDING));
        assertFalse(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o.pendingDispatchSeq);
        assertEquals(200 * ONE, o.holdAmount);
        assertEquals(0, s.replaceDeadlines());
        assertEquals(1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(0, r.count(AssetsEngineEvent.OrderFault.class));
        assertEquals(50 * ONE, s.acct(1).locked(USD));
        assertEquals(800 * ONE, s.acct(1).available(USD));
        assertEquals(150_000_000L, s.acct(1).available(BTC));
        assertEquals(150 * ONE, s.acct(2).available(USD));
        assertEquals(50_000_000L, s.acct(2).locked(BTC));
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        assertEquals("the AMEND outcome after the trade names the current leg: redundant (3.8 #4)", 1, r.orders.redundantOutcomeCount());
        assertEquals(0, r.orders.staleOutcomeCount());
        assertEquals(0, r.events().size());
    }

    /**
     * 3.1: a fill that wraps the user's net position is ORDERFAULT POSITION_OVERFLOW with the fill quantity, the
     * position clamped and the money settled. The SELL side cannot wrap (its hold is the base quantity, so two
     * sells are capped at the balance); the BUY side can, because at price 1 raw unit a 2^62 BUY locks only
     * 461 USD, and two such BUYs admitted at position 0 wrap on the second fill.
     */
    @Test
    public void positionOverflowOnTheSecondFillFaultsClampsAndStillSettles() {
        final long q = 1L << 62;
        final long notional = FixedPoint.multiply(1L, q); // 46116860184 raw USD per order
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, q); // Long.MAX_VALUE / 2 + 1 base each
        r.deposit(3, BTC, q);
        r.markNow();
        r.create(r.limit(1, 1, BUY, 1L, q));  // order 1
        r.create(r.limit(1, 1, BUY, 1L, q));  // order 2: admitted at position 0, no projected wrap
        r.create(r.limit(2, 1, SELL, 1L, q)); // order 3
        r.create(r.limit(3, 1, SELL, 1L, q)); // order 4
        assertEquals("precondition (S4): all four admitted, got " + r.events(), 4, r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals(2 * notional, r.engine.account(1).locked(USD));
        ack(r, 1, 101L);
        ack(r, 2, 102L);
        ack(r, 3, 303L);
        ack(r, 4, 404L);
        r.markNow();
        trade(r, 1, 1, 3, 303L, 2, 1, 101L, 1, 1L, q, false);
        assertEquals("the first fill fits: no fault", 0, r.count(AssetsEngineEvent.OrderFault.class));
        assertEquals(q, r.orders.user(1).position[1]);
        assertEquals(-q, r.orders.user(2).position[1]);
        assertNull(r.orders.orders().get(1));
        assertEquals(q, r.engine.account(1).available(BTC));
        withdraw(r, 1, BTC, q); // the position is net filled, not inventory: shed the base so the next credit fits
        assertEquals(0, r.engine.account(1).available(BTC));
        r.markNow();
        trade(r, 2, 1, 4, 404L, 3, 2, 102L, 1, 1L, q, false);
        assertEquals("a wrapping fill is loud: one ORDERFAULT POSITION_OVERFLOW (3.1)", 1, r.count(AssetsEngineEvent.OrderFault.class));
        final AssetsEngineEvent.OrderFault fault = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals(OrderFaultKind.POSITION_OVERFLOW, fault.kind());
        assertEquals("ORDERFAULT kind=POSITION_OVERFLOW order=2 u=1 market=1 trade=2 amount=" + q, fault.render());
        assertEquals("the position is clamped, never wrapped", Long.MAX_VALUE, r.orders.user(1).position[1]);
        assertEquals(-q, r.orders.user(3).position[1]);
        assertEquals(1, r.orders.orderFaultCount());
        assertEquals("money still settled", 1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(0, r.count(AssetsEngineEvent.SettleFault.class));
        assertEquals(4, r.count(AssetsEngineEvent.Balance.class));
        assertEquals(2, r.engine.getLastAppliedTradeId());
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE - 2 * notional, r.engine.account(1).available(USD));
        assertEquals(q, r.engine.account(1).available(BTC));
        assertEquals(notional, r.engine.account(3).available(USD));
        assertEquals(0, r.engine.account(3).locked(BTC));
        final List<AssetsEngineEvent.OrderEvent> e2 = eventsOf(r, 2);
        assertEquals(1, e2.size());
        assertEquals(EventKind.FILL, e2.get(0).kind());
        assertEquals("the order accounting still completes the order", OrderStatus.FILLED, e2.get(0).status());
        assertEquals(q, e2.get(0).lastQty());
        assertNull(r.orders.orders().get(2));
        assertNull(r.orders.orders().get(4));
        // S4 guard: at the clamped position a further BUY projects a wrap and is INVALID at admission
        r.markNow();
        r.create(r.limit(1, 1, BUY, 1L, ONE));
        assertEquals(CoreRejectReason.INVALID, r.reject().reason());
    }

    /** 3.1 step 2: a trade refused by the money settle applies no order accounting; the same tradeId then applies with a valid price. */
    @Test
    public void settleRefusalAppliesNoOrderAccountingAndTheSameTradeIdThenAppliesNormally() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        final byte[] before = f.image();
        r.markNow();
        f.sellHits(1, 0L, 6 * ONE); // tradeId 1 = lastApplied + 1, price 0
        assertEquals("one SETTLEREJECTED and nothing else, got " + r.events(), 1, r.events().size());
        final AssetsEngineEvent.SettleRejected rej = r.only(AssetsEngineEvent.SettleRejected.class);
        assertEquals(SettleRejectReason.INVALID_PRICE, rej.reason());
        assertEquals(1, rej.tradeId());
        assertEquals(0, rej.detail());
        assertEquals(MKT, rej.marketId());
        assertEquals("lastAppliedTradeId unchanged", 0, r.engine.getLastAppliedTradeId());
        assertEquals("a refusal is not a gap", 0, r.orders.tradeGapCount());
        assertEquals(0, r.orders.orderFaultCount());
        assertArrayEquals("no mutation at all", before, f.image());
        for (final long id : new long[] {1, 2}) {
            final CoreOrder o = r.orders.orders().get(id);
            assertNotNull(o);
            assertEquals("filled unchanged", 0, o.filledQty);
            assertEquals(10 * ONE, o.remainingQty);
            assertEquals(0, o.cumQuoteQty);
            assertEquals(OrderStatus.NEW.code(), o.status);
        }
        assertTrue("no FILL event", f.orderEvents(1).isEmpty() && f.orderEvents(2).isEmpty());
        assertEquals(1_000 * ONE, r.engine.account(1).locked(USD));
        assertEquals(10 * ONE, r.engine.account(2).locked(BTC));
        r.markNow();
        f.sellHits(1, 100 * ONE, 6 * ONE);
        assertEquals("the same tradeId with a valid price applies", 1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(1, r.engine.getLastAppliedTradeId());
        assertEquals(OrderStatus.PARTIALLY_FILLED, event(r, 1, EventKind.FILL).status());
        assertEquals(OrderStatus.PARTIALLY_FILLED, event(r, 2, EventKind.FILL).status());
        assertEquals(6 * ONE, r.orders.orders().get(1).filledQty);
        assertEquals(6 * ONE, r.orders.orders().get(2).filledQty);
        assertEquals(400 * ONE, r.engine.account(1).locked(USD));
        assertEquals(4 * ONE, r.engine.account(2).locked(BTC));
    }

    /** 3.8 #8: an IOC's first trade before the create outcome: ACK_NEW, FILL, then one CANCEL (remaining > 0); fully filled mints nothing. */
    @Test
    public void iocFirstTradeBeforeTheCreateOutcomeAcksFillsThenMintsOneCancelOnlyWhenRemaining() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 5 * ONE);
        final CreateOrderCommand ioc = r.limit(1, 1, BUY, 100 * ONE, 2 * ONE);
        ioc.timeInForce = OrderEngine.TIF_IOC;
        r.create(ioc);                                 // order 1, dispatch 1, create unresolved
        r.create(r.limit(2, 1, SELL, 100 * ONE, ONE)); // order 2, dispatch 2
        ack(r, 2, LEG_B);
        assertTrue("precondition: the IOC create is unresolved", live(r, 1).has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, live(r, 1).currentLegId);
        r.markNow();
        trade(r, 1, 1, 2, LEG_B, 2, 1, LEG_A, 1, 100 * ONE, ONE, false);
        final List<AssetsEngineEvent.OrderEvent> e1 = eventsOf(r, 1);
        assertEquals("implicit ack, fill, then the IOC cancel (3.8 #8), got " + e1,
                List.of(EventKind.ACK_NEW, EventKind.FILL, EventKind.DISPATCHED), kinds(e1));
        assertEquals(OrderStatus.NEW, e1.get(0).status());
        assertEquals(LEG_A, e1.get(0).meOrderId());
        assertEquals(OrderStatus.PARTIALLY_FILLED, e1.get(1).status());
        assertEquals(ONE, e1.get(1).filledQty());
        assertEquals(ONE, e1.get(1).remainingQty());
        assertEquals(LEG_A, e1.get(2).meOrderId());
        assertEquals("exactly one ME command minted", 1, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d.kind());
        assertEquals(1, d.commandIdHigh());
        assertEquals((1L << 2) | 1, d.commandIdLow());
        assertEquals(LEG_A, d.oldOrderId());
        assertEquals(3, d.dispatchSeq());
        final CoreOrder o = live(r, 1);
        assertTrue("CANCEL_PENDING set by the IOC emulation", o.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(CoreOrder.PK_CANCEL, o.pendingKind);
        assertEquals(3, o.pendingDispatchSeq);
        assertEquals(1, o.attempt);
        assertEquals(LEG_A, o.currentLegId);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        assertEquals(1, r.orders.implicitAckCount());
        assertEquals(1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(100 * ONE, r.engine.account(1).locked(USD));
        assertEquals(800 * ONE, r.engine.account(1).available(USD));
        assertEquals(ONE, r.engine.account(1).available(BTC));

        final OrderRig r2 = OrderRig.open();
        r2.deposit(1, USD, 1_000 * ONE);
        r2.deposit(2, BTC, 5 * ONE);
        final CreateOrderCommand full = r2.limit(1, 1, BUY, 100 * ONE, ONE);
        full.timeInForce = OrderEngine.TIF_IOC;
        r2.create(full);
        r2.create(r2.limit(2, 1, SELL, 100 * ONE, ONE));
        ack(r2, 2, LEG_B);
        r2.markNow();
        trade(r2, 1, 1, 2, LEG_B, 2, 1, LEG_A, 1, 100 * ONE, ONE, false);
        final List<AssetsEngineEvent.OrderEvent> e2 = eventsOf(r2, 1);
        assertEquals("fully filled by the implicit-ack trade: ACK_NEW then FILL/FILLED, no cancel (3.8 #8), got " + e2,
                List.of(EventKind.ACK_NEW, EventKind.FILL), kinds(e2));
        assertEquals(OrderStatus.FILLED, e2.get(1).status());
        assertEquals(0, e2.get(1).holdRemaining());
        assertEquals("no cancel minted", 0, r2.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals(2, r2.engine.getDispatchSeq());
        assertNull(r2.orders.orders().get(1));
        assertEquals(0, r2.engine.account(1).locked(USD));
        assertEquals(900 * ONE, r2.engine.account(1).available(USD));
        assertEquals(ONE, r2.engine.account(1).available(BTC));
        assertFalse(r2.engine.account(1).hasHold(1));
    }
}

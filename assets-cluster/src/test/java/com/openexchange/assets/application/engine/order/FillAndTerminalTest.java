// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.FeedRig.LEG1;
import static com.openexchange.assets.application.engine.order.FeedRig.LEG2;
import static com.openexchange.assets.application.engine.order.FeedRig.MKT;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 G1, contract 3.1 (MeJournalTrade; lifecycle C5, C6, R-C5a..c, R-C6a shape, R-C6c): money settles first
 * from the order's hold for the FULL trade quantity, then the order accounting follows the leg; a fill that
 * reaches remaining 0 is the terminal in the same apply; duplicates are byte-level no-ops, gaps are refused,
 * overfills and leg mismatches are loud faults, never a throw and never a silent drop. The terminal half of
 * the group (3.2) and the invariants are in {@link TerminalAndInvariantsTest}.
 */
public class FillAndTerminalTest {

    /** 3.1 steps 2-3, R-C5a: trade qty 6 on qty 10: money from the order's hold, counterparty credited, both PARTIALLY_FILLED. */
    @Test
    public void rC5a_partialFillSettlesFromTheOrdersHoldAndCreditsTheCounterparty() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        final long seqBefore = r.engine.getEventSeq();
        r.markNow();
        f.sellHits(1, 100 * ONE, 6 * ONE);
        assertEquals("money first: four BALANCE lines from the settle (3.1 step 2)", 4, r.count(AssetsEngineEvent.Balance.class));
        final List<String> lines = f.renders();
        assertTrue("balances, SETTLED, then the order events: " + lines, lines.size() >= 5);
        assertEquals(List.of(
                "BALANCE u=1 asset=0 avail=0 locked=40000000000",
                "BALANCE u=1 asset=1 avail=600000000 locked=0",
                "BALANCE u=2 asset=1 avail=0 locked=400000000",
                "BALANCE u=2 asset=0 avail=60000000000 locked=0",
                "SETTLED tradeId=1 buyer=1 seller=2"), lines.subList(0, 5));
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        final List<AssetsEngineEvent.OrderEvent> e2 = f.orderEvents(2);
        assertEquals("one FILL event for the buy", 1, e1.size());
        assertEquals("one FILL event for the sell", 1, e2.size());
        for (final AssetsEngineEvent.OrderEvent e : List.of(e1.get(0), e2.get(0))) {
            assertEquals(EventKind.FILL, e.kind());
            assertEquals(OrderStatus.PARTIALLY_FILLED, e.status());
            assertEquals(CoreRejectReason.NONE, e.reason());
            assertEquals(6 * ONE, e.filledQty());
            assertEquals(4 * ONE, e.remainingQty());
            assertEquals(100 * ONE, e.lastPrice());
            assertEquals(6 * ONE, e.lastQty());
            assertEquals(1, e.tradeId());
        }
        assertEquals(LEG1, e1.get(0).meOrderId());
        assertEquals(400 * ONE, e1.get(0).holdRemaining());
        assertEquals(LEG2, e2.get(0).meOrderId());
        assertEquals(4 * ONE, e2.get(0).holdRemaining());
        assertEquals("one eventSeq per OrderEvent", seqBefore + 2, r.engine.getEventSeq());
        for (final long id : new long[] {1, 2}) {
            final CoreOrder o = r.orders.orders().get(id);
            assertNotNull(o);
            assertEquals(6 * ONE, o.filledQty);
            assertEquals(4 * ONE, o.remainingQty);
            assertEquals("cumQuoteQty = price x fillQty, exact fixed point", 600 * ONE, o.cumQuoteQty);
            assertEquals(100 * ONE, o.lastPrice);
            assertEquals(6 * ONE, o.lastQty);
            assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        }
        assertEquals(400 * ONE, r.engine.account(1).locked(USD));
        assertEquals(0, r.engine.account(1).available(USD));
        assertEquals(6 * ONE, r.engine.account(1).available(BTC));
        assertEquals(400 * ONE, r.engine.account(1).holdRemaining(1));
        assertEquals(4 * ONE, r.engine.account(2).locked(BTC));
        assertEquals(600 * ONE, r.engine.account(2).available(USD));
        assertEquals(4 * ONE, r.engine.account(2).holdRemaining(2));
        assertEquals(1, r.engine.getLastAppliedTradeId());
    }

    /** 3.1 step 1, R-C5b: a tradeId at or below lastAppliedTradeId is a byte-level no-op: no event, no state change. */
    @Test
    public void rC5b_sameTradeIdTwiceIsAByteLevelNoOp() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        f.sellHits(1, 100 * ONE, 6 * ONE);
        assertEquals("the first apply of tradeId 1 advances the resume key (3.1 step 4)", 1, r.engine.getLastAppliedTradeId());
        assertEquals(6 * ONE, r.orders.orders().get(1).filledQty);
        final byte[] before = f.image();
        r.markNow();
        f.sellHits(1, 100 * ONE, 6 * ONE);
        f.sellHits(1, 100 * ONE, ONE); // same id, different payload: the id alone decides
        assertEquals("a replayed tradeId emits nothing: " + r.events(), 0, r.events().size());
        assertArrayEquals("snapshot bytes identical after the replay", before, f.image());
        assertEquals(1, r.engine.getLastAppliedTradeId());
        assertEquals(6 * ONE, r.orders.orders().get(1).filledQty);
        assertEquals(6 * ONE, r.orders.orders().get(2).filledQty);
        assertEquals(400 * ONE, r.engine.account(1).locked(USD));
        assertEquals(4 * ONE, r.engine.account(2).locked(BTC));
        assertEquals("a replay is not a gap", 0, r.orders.tradeGapCount());
    }

    /** 3.1 step 3, R-C5c: qty 11 on remaining 10 fills 10, ORDERFAULT OVERFILL 1; money settled the full 11; FILLED in the same apply. */
    @Test
    public void rC5c_overfillFillsToRemainingAndEmitsTheExcessAsAnOrderFault() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 20 * ONE, 100 * ONE, 10 * ONE, 90 * ONE, 20 * ONE);
        final OrderRig r = f.r;
        r.markNow();
        f.buyHits(1, 90 * ONE, 11 * ONE);
        assertEquals("an ME overfill is never silent: ORDERFAULT OVERFILL with the excess (3.1 step 3)",
                1, r.count(AssetsEngineEvent.OrderFault.class));
        final AssetsEngineEvent.OrderFault fault = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals("ORDERFAULT kind=OVERFILL order=1 u=1 market=1 trade=1 amount=" + ONE, fault.render());
        assertEquals(OrderFaultKind.OVERFILL, fault.kind());
        assertEquals(1, r.orders.orderFaultCount());
        // money settled the full 11 @ 90 = 990 out of order 1's hold of 1000: no SettleFault
        assertEquals(0, r.count(AssetsEngineEvent.SettleFault.class));
        assertEquals(1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(11 * ONE, r.engine.account(1).available(BTC));
        assertEquals(990 * ONE, r.engine.account(2).available(USD));
        assertEquals(9 * ONE, r.engine.account(2).locked(BTC));
        // the order filled to its remaining and went terminal in the same apply; the residual 10 came back
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals("one FILL event for the overfilled order", 1, e1.size());
        assertEquals(EventKind.FILL, e1.get(0).kind());
        assertEquals(OrderStatus.FILLED, e1.get(0).status());
        assertEquals(10 * ONE, e1.get(0).filledQty());
        assertEquals(0, e1.get(0).remainingQty());
        assertNull("terminal record removed from the store", r.orders.orders().get(1));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(10 * ONE, r.engine.account(1).available(USD));
        assertFalse(r.engine.account(1).hasHold(1));
        // the counterparty's accounting took the real 11
        final CoreOrder o2 = r.orders.orders().get(2);
        assertNotNull(o2);
        assertEquals(11 * ONE, o2.filledQty);
        assertEquals(9 * ONE, o2.remainingQty);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o2.status);
    }

    /** 3.1 step 3: a trade on an order whose create outcome has not arrived is the implicit ack: leg learned, ACK_NEW then FILL. */
    @Test
    public void implicitAckByTradeLearnsTheLegBeforeTheFill() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE));
        r.create(r.limit(2, MKT, 1, 100 * ONE, 10 * ONE));
        f.ack(2, 2, LEG2); // only the sell is acked; order 1 is still DISPATCH_PENDING for its create
        final CoreOrder o1 = r.orders.orders().get(1);
        assertTrue(o1.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o1.currentLegId);
        r.markNow();
        f.sellHits(1, 100 * ONE, 6 * ONE);
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        final List<EventKind> kinds = new ArrayList<>();
        for (final AssetsEngineEvent.OrderEvent e : e1) {
            kinds.add(e.kind());
        }
        assertEquals("implicit ack: ACK_NEW then FILL for order 1 (3.1 step 3)", List.of(EventKind.ACK_NEW, EventKind.FILL), kinds);
        assertEquals(OrderStatus.NEW, e1.get(0).status());
        assertEquals(LEG1, e1.get(0).meOrderId());
        assertEquals(OrderStatus.PARTIALLY_FILLED, e1.get(1).status());
        assertEquals(LEG1, o1.currentLegId);
        assertFalse("DISPATCH_PENDING cleared by the implicit ack", o1.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o1.pendingDispatchSeq);
        assertEquals(6 * ONE, o1.filledQty);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o1.status);
        assertEquals(1, r.orders.implicitAckCount());
        assertEquals(400 * ONE, r.engine.account(1).locked(USD));
        // the create outcome that arrives afterwards with the same leg is a counted no-op
        r.markNow();
        f.ack(1, 1, LEG1);
        assertEquals("a late create outcome after the implicit ack emits nothing: " + r.events(), 0, r.events().size());
        assertEquals(LEG1, o1.currentLegId);
        assertEquals(6 * ONE, o1.filledQty);
        assertEquals("the late outcome is counted, not applied", 1, r.orders.redundantOutcomeCount() + r.orders.staleOutcomeCount());
    }

    /** 3.1 step 3: a trade naming a leg that is not the order's current leg: money stands, ORDERFAULT SETTLE, order accounting skipped. */
    @Test
    public void legMismatchFaultsAndSkipsTheOrderAccountingWhileMoneySettles() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        r.markNow();
        f.trade(1, 2, LEG2, 2, 1, 999L, 1, 100 * ONE, 6 * ONE, false); // maker leg 999 is not order 1's leg 101
        assertEquals("a leg mismatch is loud: ORDERFAULT SETTLE (3.1 step 3)", 1, r.count(AssetsEngineEvent.OrderFault.class));
        assertEquals("ORDERFAULT kind=SETTLE order=1 u=1 market=1 trade=1 amount=" + 6 * ONE,
                r.first(AssetsEngineEvent.OrderFault.class).render());
        assertEquals(1, r.orders.orderFaultCount());
        // money stood: the settle ran for the full quantity against the hold keys
        assertEquals(4, r.count(AssetsEngineEvent.Balance.class));
        assertEquals(1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(400 * ONE, r.engine.account(1).locked(USD));
        assertEquals(6 * ONE, r.engine.account(1).available(BTC));
        assertEquals(600 * ONE, r.engine.account(2).available(USD));
        assertEquals(1, r.engine.getLastAppliedTradeId());
        // order 1's accounting was skipped; order 2 (correct leg) filled
        final CoreOrder o1 = r.orders.orders().get(1);
        assertNotNull(o1);
        assertEquals(0, o1.filledQty);
        assertEquals(10 * ONE, o1.remainingQty);
        assertEquals(0, o1.cumQuoteQty);
        assertEquals(OrderStatus.NEW.code(), o1.status);
        assertTrue("no OrderEvent for the mismatched side", f.orderEvents(1).isEmpty());
        assertEquals(1, f.orderEvents(2).size());
        assertEquals(6 * ONE, r.orders.orders().get(2).filledQty);
    }

    /** 3.1 step 1: tradeId lastApplied + 2 is refused with SETTLEREJECTED TRADE_GAP and nothing moves. */
    @Test
    public void tradeIdGapIsRefusedAndNothingMoves() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        r.markNow();
        f.sellHits(2, 100 * ONE, 6 * ONE);
        assertEquals("a gap is refused: SETTLEREJECTED TRADE_GAP (3.1 step 1)", 1, r.count(AssetsEngineEvent.SettleRejected.class));
        final AssetsEngineEvent.SettleRejected rej = r.first(AssetsEngineEvent.SettleRejected.class);
        assertEquals(SettleRejectReason.TRADE_GAP, rej.reason());
        assertEquals("detail = the expected id", 1, rej.detail());
        assertEquals(2, rej.tradeId());
        assertEquals(MKT, rej.marketId());
        assertEquals("nothing else emitted: " + r.events(), 1, r.events().size());
        assertEquals("cursor not advanced", 0, r.engine.getLastAppliedTradeId());
        assertEquals(1, r.orders.tradeGapCount());
        assertEquals(1_000 * ONE, r.engine.account(1).locked(USD));
        assertEquals(10 * ONE, r.engine.account(2).locked(BTC));
        assertEquals(0, r.orders.orders().get(1).filledQty);
        assertEquals(0, r.orders.orders().get(2).filledQty);
        // the dense sequence then applies in order
        r.markNow();
        f.sellHits(1, 100 * ONE, 6 * ONE);
        f.sellHits(2, 100 * ONE, 4 * ONE);
        assertEquals(2, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(2, r.engine.getLastAppliedTradeId());
        assertNull("both orders filled to zero and left the store", r.orders.orders().get(1));
        assertNull(r.orders.orders().get(2));
    }

    /** 3.1 step 3 (R-C6a shape with LIMIT): a fill at 95 completing a BUY limited at 100 is terminal FILLED in the same apply. */
    @Test
    public void aFillCompletingTheOrderIsTerminalFilledInTheSameApply() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        final CreateOrderCommand buy = r.limit(1, MKT, 0, 100 * ONE, 10 * ONE);
        buy.clientOrderIdLow = 77;
        buy.timeInForce = OrderEngine.TIF_GTD;
        buy.expiresAtMs = r.now + 60_000;
        r.create(buy);
        r.create(r.limit(2, MKT, 1, 95 * ONE, 10 * ONE));
        f.ack(1, 1, LEG1);
        f.ack(2, 2, LEG2);
        assertEquals(1, f.deadlines(DeadlineIndex.KIND_GTD, 1));
        r.markNow();
        f.buyHits(1, 95 * ONE, 10 * ONE);
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals("remaining 0: one FILL event carrying status FILLED (3.1 step 3)", 1, e1.size());
        assertEquals(EventKind.FILL, e1.get(0).kind());
        assertEquals(OrderStatus.FILLED, e1.get(0).status());
        assertEquals(10 * ONE, e1.get(0).filledQty());
        assertEquals(0, e1.get(0).remainingQty());
        assertEquals(95 * ONE, e1.get(0).lastPrice());
        // residual hold (100 - 95) x 10 = 50 released in the same apply: locked back to 0, available = the exact cost
        assertEquals("locked back to 0 for the filled BUY", 0, r.engine.account(1).locked(USD));
        assertEquals("available reflects the exact fill cost 1000 - 95 x 10", 50 * ONE, r.engine.account(1).available(USD));
        assertEquals(10 * ONE, r.engine.account(1).available(BTC));
        assertFalse(r.engine.account(1).hasHold(1));
        final List<String> lines = f.renders();
        final int settled = lines.indexOf("SETTLED tradeId=1 buyer=1 seller=2");
        final int release = lines.indexOf("BALANCE u=1 asset=0 avail=5000000000 locked=0");
        assertTrue("the release BALANCE line follows the settle: " + lines, settled >= 0 && release > settled);
        // record gone with its indexes and deadline; position updated for both parties (I7)
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals(0, r.orders.orders().activeByClientId(1, 0, 77));
        assertEquals("GTD deadline removed with the record", 0, f.deadlines(DeadlineIndex.KIND_GTD, 1));
        assertEquals(10 * ONE, r.orders.user(1).position[MKT]);
        assertEquals(-10 * ONE, r.orders.user(2).position[MKT]);
        assertNull("the sell filled to zero too", r.orders.orders().get(2));
        assertEquals(0, r.engine.account(2).locked(BTC));
        assertEquals(950 * ONE, r.engine.account(2).available(USD));
        // the clientOrderId is reusable once the order is gone (R-C2b)
        r.markNow();
        final CreateOrderCommand again = r.limit(1, MKT, 0, 95 * ONE, ONE / 2);
        again.clientOrderIdLow = 77;
        r.create(again);
        assertEquals("clientOrderId 77 reusable after FILLED: " + r.events(), 1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    /** 3.1 step 3 + 3.4, R-C6c: a fill completing the order while a replace window is open releases everything. */
    @Test
    public void rC6c_fillCompletingAnOrderWithAnOpenAmendWindowReleasesEverything() {
        final FeedRig f = FeedRig.pair(2_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        final AmendOrderCommand a = new AmendOrderCommand();
        a.reset();
        a.correlationId = 0xA11;
        a.userId = 1;
        a.orderId = 1;
        a.requestIdHigh = 0xABCDL;
        a.requestIdLow = 0xA11;
        a.newPrice = 110 * ONE; // newQuantity 0 keeps the 10: top-up (110 - 100) x 10 = 100
        r.orders.applyAmend(a, r.now);
        final CoreOrder o1 = r.orders.orders().get(1);
        assertNotNull(o1);
        assertTrue("precondition: the amend opened a replace window (3.4)", o1.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(OrderStatus.PENDING_REPLACE.code(), o1.status);
        assertEquals(100 * ONE, o1.pendingHoldDelta);
        assertEquals(LEG1, o1.oldLegId);
        assertEquals("the top-up is locked now", 1_100 * ONE, r.engine.account(1).locked(USD));
        r.markNow();
        f.sellHits(1, 100 * ONE, 10 * ONE); // the old leg 101 fills completely before the ME answers the amend
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals("one FILL event carrying status FILLED", 1, e1.size());
        assertEquals(EventKind.FILL, e1.get(0).kind());
        assertEquals(OrderStatus.FILLED, e1.get(0).status());
        assertEquals("window aborted implicitly: the release is total, no hold is left", 0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertEquals(10 * ONE, r.engine.account(1).available(BTC));
        assertFalse(r.engine.account(1).hasHold(1));
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals("no replace deadline survives the record", 0, f.deadlines(DeadlineIndex.KIND_REPLACE, -1));
    }
}

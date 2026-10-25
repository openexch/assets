// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.FeedRig.LEG1;
import static com.openexchange.assets.application.engine.order.FeedRig.LEG2;
import static com.openexchange.assets.application.engine.order.FeedRig.LEG3;
import static com.openexchange.assets.application.engine.order.FeedRig.LEG4;
import static com.openexchange.assets.application.engine.order.FeedRig.ME_CANCELLED;
import static com.openexchange.assets.application.engine.order.FeedRig.ME_FILLED;
import static com.openexchange.assets.application.engine.order.FeedRig.ME_REJECTED;
import static com.openexchange.assets.application.engine.order.FeedRig.MKT;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 G1, contract 3.2 (MeJournalTerminal; lifecycle C7, C9, R-C7a/b) and the invariants I7 (position),
 * I8 (slot symmetric) and locked == sum of hold remaining (K11 #11, single release owner): a terminal for the
 * current leg releases the residual exactly once, emits one event with the record still present and removes
 * the record; stale, old-leg and unknown terminals are counted no-ops. The trade half (3.1) is in
 * {@link FillAndTerminalTest}.
 */
public class TerminalAndInvariantsTest {

    /** 3.2, R-C7a: terminal CANCELLED (3) for the current leg after a cancel: CANCELLED event, residual released once, slot 0. */
    @Test
    public void rC7a_terminalCancelledForTheCurrentLegReleasesTheResidualOnce() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE));
        f.ack(1, 1, LEG1);
        r.cancel(1, 1);
        assertEquals(1, r.orders.orders().openCount(1));
        r.markNow();
        f.terminal(1, 1, LEG1, ME_CANCELLED);
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals("the journal CANCELLED for the current leg is the terminal: one CANCELLED event (3.2)", 1, e1.size());
        assertEquals(EventKind.CANCELLED, e1.get(0).kind());
        assertEquals(OrderStatus.CANCELLED, e1.get(0).status());
        assertEquals(CoreRejectReason.NONE, e1.get(0).reason());
        assertEquals(10 * ONE, e1.get(0).remainingQty());
        assertEquals("release BALANCE line, then the event with the record still present", 2, r.events().size());
        assertEquals("BALANCE u=1 asset=0 avail=100000000000 locked=0", r.events().get(0).render());
        assertTrue(r.events().get(1) instanceof AssetsEngineEvent.OrderEvent);
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertFalse("hasHold false after the terminal", r.engine.account(1).hasHold(1));
        assertNull(r.orders.orders().get(1));
        assertEquals("slot back to 0 (I8)", 0, r.orders.orders().openCount(1));
        // a re-delivered terminal for the gone order is counted and releases nothing twice
        r.markNow();
        f.terminal(1, 1, LEG1, ME_CANCELLED);
        assertEquals(0, r.events().size());
        assertEquals(1, r.orders.staleTerminalCount());
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
    }

    /** 3.2, R-C7b: a terminal for a leg that is not the order's current leg is ignored and counted. */
    @Test
    public void rC7b_terminalForALegThatIsNotCurrentIsIgnoredAndCounted() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE));
        f.ack(1, 1, LEG1);
        r.markNow();
        f.terminal(1, 1, 999L, ME_CANCELLED);
        assertEquals("a stale-leg terminal is counted (3.2)", 1, r.orders.staleTerminalCount());
        assertEquals("and emits nothing: " + r.events(), 0, r.events().size());
        final CoreOrder o1 = r.orders.orders().get(1);
        assertNotNull("the record stays live", o1);
        assertFalse(o1.isTerminal());
        assertEquals(LEG1, o1.currentLegId);
        assertEquals(1, r.orders.orders().openCount(1));
        assertEquals(1_000 * ONE, r.engine.account(1).locked(USD));
        assertTrue(r.engine.account(1).hasHold(1));
    }

    /** 3.2: terminal FILLED (2) arriving with remaining > 0 emits ORDERFAULT SETTLE with the remaining and still terminalizes. */
    @Test
    public void terminalFilledWithRemainingGreaterThanZeroFaultsAndStillTerminalizes() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 100 * ONE, 10 * ONE, 100 * ONE, 10 * ONE);
        final OrderRig r = f.r;
        f.sellHits(1, 100 * ONE, 6 * ONE);
        r.markNow();
        f.terminal(1, 1, LEG1, ME_FILLED);
        assertEquals("FILLED terminal with remaining 4: ORDERFAULT SETTLE carrying the remaining (3.2)",
                1, r.count(AssetsEngineEvent.OrderFault.class));
        assertEquals("ORDERFAULT kind=SETTLE order=1 u=1 market=1 trade=0 amount=" + 4 * ONE,
                r.first(AssetsEngineEvent.OrderFault.class).render());
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals("still terminal: one event with status FILLED", 1, e1.size());
        assertEquals(OrderStatus.FILLED, e1.get(0).status());
        assertEquals(EventKind.FILL, e1.get(0).kind());
        assertEquals(6 * ONE, e1.get(0).filledQty());
        assertEquals("the residual 400 is released by the terminal", 0, r.engine.account(1).locked(USD));
        assertEquals(400 * ONE, r.engine.account(1).available(USD));
        assertFalse(r.engine.account(1).hasHold(1));
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals(1, r.orders.orderFaultCount());
        assertNotNull("the counterparty is untouched by order 1's terminal", r.orders.orders().get(2));
        assertEquals(4 * ONE, r.engine.account(2).locked(BTC));
    }

    /** 3.2: terminal REJECTED (4) for the current leg gives status REJECTED reason ME_REJECTED and releases the hold. */
    @Test
    public void terminalRejectedGivesStatusRejectedWithReasonMeRejectedAndReleasesTheHold() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE));
        f.ack(1, 1, LEG1);
        r.markNow();
        f.terminal(1, 1, LEG1, ME_REJECTED);
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals("one REJECTED event (3.2)", 1, e1.size());
        assertEquals(EventKind.REJECTED, e1.get(0).kind());
        assertEquals(OrderStatus.REJECTED, e1.get(0).status());
        assertEquals(CoreRejectReason.ME_REJECTED, e1.get(0).reason());
        assertEquals(1, r.count(AssetsEngineEvent.Balance.class));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertFalse(r.engine.account(1).hasHold(1));
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.orders.orders().openCount(1));
    }

    /** 3.2 + C9: terminal CANCELLED on an EXPIRY_REQUESTED order (GTD swept past its deadline) surfaces as EXPIRED. */
    @Test
    public void terminalCancelledWhenExpiryRequestedSurfacesAsExpired() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand gtd = r.limit(1, MKT, 0, 100 * ONE, 10 * ONE);
        gtd.timeInForce = OrderEngine.TIF_GTD;
        gtd.expiresAtMs = r.now + 5_000;
        r.create(gtd);
        f.ack(1, 1, LEG1);
        r.advance(5_000);
        r.sweep();
        final CoreOrder o1 = r.orders.orders().get(1);
        assertNotNull(o1);
        assertTrue("precondition: the sweep requested the expiry (C9)", o1.has(CoreOrder.F_EXPIRY_REQUESTED));
        r.markNow();
        f.terminal(1, 1, LEG1, ME_CANCELLED);
        final List<AssetsEngineEvent.OrderEvent> e1 = f.orderEvents(1);
        assertEquals("CANCELLED for an EXPIRY_REQUESTED order surfaces as one EXPIRED event (3.2)", 1, e1.size());
        assertEquals(EventKind.EXPIRED, e1.get(0).kind());
        assertEquals(OrderStatus.EXPIRED, e1.get(0).status());
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertFalse(r.engine.account(1).hasHold(1));
        assertNull(r.orders.orders().get(1));
        assertEquals(0, f.deadlines(DeadlineIndex.KIND_GTD, 1));
    }

    /** 3.2: a terminal naming an unknown omsOrderId is a counted no-op. */
    @Test
    public void unknownOmsOrderIdTerminalIsACountedNoOp() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE));
        f.ack(1, 1, LEG1);
        r.markNow();
        f.terminal(42, 1, 777L, ME_CANCELLED);
        assertEquals("unknown omsOrderId: counted (3.2)", 1, r.orders.staleTerminalCount());
        assertEquals("and nothing emitted: " + r.events(), 0, r.events().size());
        assertEquals(1, r.orders.orders().size());
        assertEquals(1_000 * ONE, r.engine.account(1).locked(USD));
        assertEquals(1, r.orders.orders().openCount(1));
    }

    // ---------------------------------------------------------------- invariants I7, I8, locked == sum holds

    /** I7: a user's position per market is buys minus sells of filled quantity, and the two parties net to zero. */
    @Test
    public void i7_positionPerMarketIsBuysMinusSellsAndNetsToZero() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(1, BTC, 3 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        r.deposit(2, USD, 300 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE)); // 1: user 1 buys 10
        r.create(r.limit(2, MKT, 1, 100 * ONE, 10 * ONE)); // 2: user 2 sells 10
        r.create(r.limit(1, MKT, 1, 100 * ONE, 3 * ONE));  // 3: user 1 sells 3 back
        r.create(r.limit(2, MKT, 0, 100 * ONE, 3 * ONE));  // 4: user 2 buys 3
        f.ack(1, 1, LEG1);
        f.ack(2, 2, LEG2);
        f.ack(3, 1, LEG3);
        f.ack(4, 2, LEG4);
        f.sellHits(1, 100 * ONE, 6 * ONE);
        f.sellHits(2, 100 * ONE, 4 * ONE);
        assertNotNull(r.orders.user(1));
        assertNotNull(r.orders.user(2));
        assertEquals("after two buys of 6 and 4 the position is +10", 10 * ONE, r.orders.user(1).position[MKT]);
        assertEquals(-10 * ONE, r.orders.user(2).position[MKT]);
        f.trade(3, 4, LEG4, 2, 3, LEG3, 1, 100 * ONE, 3 * ONE, true); // user 2 buys 3 from user 1
        assertEquals("I7: position = buys - sells = 10 - 3", 7 * ONE, r.orders.user(1).position[MKT]);
        assertEquals(-7 * ONE, r.orders.user(2).position[MKT]);
        assertEquals("the two parties net to zero", 0, r.orders.user(1).position[MKT] + r.orders.user(2).position[MKT]);
        assertEquals("other markets untouched", 0, r.orders.user(1).position[2]);
        assertEquals(0, r.orders.user(2).position[2]);
        assertNull("orders 1-4 all filled to zero", r.orders.orders().get(1));
        assertNull(r.orders.orders().get(3));
        assertNull(r.orders.orders().get(4));
        assertEquals("the position survives the records", 7 * ONE, r.orders.user(1).position[MKT]);
    }

    /** I8 (slot symmetric): the slot taken at accept is returned by terminal FILLED, so a maxOpenOrders=1 user trades again. */
    @Test
    public void i8_theSlotTakenAtAcceptIsReturnedByTheTerminal() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        final RiskConfigSetCommand cfg = r.riskConfig(MKT, 1);
        cfg.maxOpenOrders = 1;
        r.setRisk(cfg);
        r.deposit(1, USD, 1_100 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE));
        r.create(r.limit(2, MKT, 1, 100 * ONE, 10 * ONE));
        f.ack(1, 1, LEG1);
        f.ack(2, 2, LEG2);
        r.markNow();
        r.create(r.limit(1, MKT, 0, 100 * ONE, ONE));
        assertEquals("precondition: the slot is taken", CoreRejectReason.OPEN_ORDER_LIMIT, r.reject().reason());
        f.buyHits(1, 100 * ONE, 10 * ONE);
        assertEquals("terminal FILLED returned the slot (I8)", 0, r.orders.orders().openCount(1));
        assertEquals(0, r.orders.orders().openCount(2));
        r.markNow();
        r.create(r.limit(1, MKT, 0, 100 * ONE, ONE));
        assertEquals("a new order is admitted into the freed slot: " + r.events(), 1, r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals(1, r.orders.orders().openCount(1));
    }

    /** After any terminal: for both users locked == sum of hold remaining over the live orders (single release owner, K11 #11). */
    @Test
    public void afterAnyTerminalLockedEqualsTheSumOfHoldRemainingForBothUsers() {
        final FeedRig f = FeedRig.open();
        final OrderRig r = f.r;
        r.deposit(1, USD, 1_200 * ONE);
        r.deposit(2, BTC, 11 * ONE);
        r.create(r.limit(1, MKT, 0, 100 * ONE, 10 * ONE)); // 1: hold 1000 USD
        r.create(r.limit(2, MKT, 1, 100 * ONE, 10 * ONE)); // 2: hold 10 BTC
        r.create(r.limit(1, MKT, 0, 50 * ONE, 4 * ONE));   // 3: hold 200 USD
        r.create(r.limit(2, MKT, 1, 100 * ONE, ONE));      // 4: hold 1 BTC
        f.ack(1, 1, LEG1);
        f.ack(2, 2, LEG2);
        f.ack(3, 1, LEG3);
        f.ack(4, 2, LEG4);
        assertEquals(1_200 * ONE, r.engine.account(1).locked(USD));
        assertEquals(11 * ONE, r.engine.account(2).locked(BTC));
        f.sellHits(1, 100 * ONE, 6 * ONE);
        f.terminal(1, 1, LEG1, ME_CANCELLED); // CANCELLED: residual 400 back
        assertEquals("terminal CANCELLED removed order 1 from the open set", 1, r.orders.orders().openCount(1));
        f.assertLockedEqualsHolds(1);
        f.assertLockedEqualsHolds(2);
        f.terminal(2, 2, LEG2, ME_FILLED); // FILLED with remaining 4: fault + residual 4 BTC back
        assertEquals("terminal FILLED removed order 2 from the open set", 1, r.orders.orders().openCount(2));
        f.assertLockedEqualsHolds(1);
        f.assertLockedEqualsHolds(2);
        assertFalse(r.engine.account(1).hasHold(1));
        assertFalse(r.engine.account(2).hasHold(2));
        assertEquals("only order 3's hold is left for user 1", 200 * ONE, r.engine.account(1).locked(USD));
        assertEquals(200 * ONE, r.engine.account(1).holdRemaining(3));
        assertEquals("only order 4's hold is left for user 2", ONE, r.engine.account(2).locked(BTC));
        assertEquals(ONE, r.engine.account(2).holdRemaining(4));
        assertEquals(0, r.engine.account(1).locked(BTC));
        assertEquals(0, r.engine.account(2).locked(USD));
    }
}

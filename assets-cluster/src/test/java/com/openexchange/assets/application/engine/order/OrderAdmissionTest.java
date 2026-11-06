// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S4 (lifecycle C1/C2/C8b, K11 rows 4, 6, 8): admission is ONE apply (validate, risk, hold, record,
 * dispatch, egress); a reject mutates nothing but the rate counters; the hold IS the balance check; the
 * slot is membership in the user's open set; cancel of a dispatch-pending order is a flag, not a command.
 */
public class OrderAdmissionTest {

    @Test
    public void genesisGateRefusesOrdersWithoutAnyUserState() {
        final OrderRig r = new OrderRig(); // gate SUSPENDED/GENESIS
        r.deposit(1, USD, 1_000 * ONE);
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        final AssetsEngineEvent.OrderReject rej = r.reject();
        assertEquals(CoreRejectReason.GATE_CLOSED, rej.reason());
        assertEquals(50, rej.refTemplateId());
        assertEquals("no UserRisk record from a gate reject (risk C19)", 0, r.orders.userCount());
        assertEquals(0, r.orders.orders().size());
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
    }

    @Test
    public void structuralRejectsCreateNothingAndConsumeNoBudget() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand badMarket = r.limit(1, 9, 0, 100 * ONE, ONE);
        final CreateOrderCommand badQty = r.limit(1, 1, 0, 100 * ONE, 0);
        final CreateOrderCommand badSide = r.limit(1, 1, 7, 100 * ONE, ONE);
        final CreateOrderCommand fok = r.limit(1, 1, 0, 100 * ONE, ONE);
        fok.timeInForce = 2;
        final CreateOrderCommand flags = r.limit(1, 1, 0, 100 * ONE, ONE);
        flags.flags = 1;
        final CreateOrderCommand pastGtd = r.limit(1, 1, 0, 100 * ONE, ONE);
        pastGtd.timeInForce = 3;
        pastGtd.expiresAtMs = r.now;
        final CreateOrderCommand noRequest = r.limit(1, 1, 0, 100 * ONE, ONE);
        noRequest.requestIdHigh = 0;
        noRequest.requestIdLow = 0;
        final CoreRejectReason[] expected = {CoreRejectReason.UNKNOWN_MARKET, CoreRejectReason.INVALID, CoreRejectReason.INVALID,
                CoreRejectReason.UNSUPPORTED_TIF, CoreRejectReason.UNSUPPORTED_FLAGS, CoreRejectReason.EXPIRED, CoreRejectReason.MISSING_REQUEST_ID};
        final CreateOrderCommand[] cmds = {badMarket, badQty, badSide, fok, flags, pastGtd, noRequest};
        for (int i = 0; i < cmds.length; i++) {
            r.markNow();
            r.create(cmds[i]);
            assertEquals("case " + i, expected[i], r.reject().reason());
        }
        assertEquals(0, r.orders.orders().size());
        assertNull("structural rejects create no UserRisk", r.orders.user(1));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
    }

    @Test
    public void redC2a_limitBuyWithExactFundsIsAcceptedInOneApply() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 100 * ONE);
        r.markNow();
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        c.clientOrderIdLow = 77;
        r.create(c);
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals(OrderStatus.PENDING_NEW, ack.status());
        assertFalse(ack.duplicate());
        assertEquals(USD, ack.holdAssetId());
        assertEquals(100 * ONE, ack.holdAmount());
        assertEquals(1, ack.orderId());
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.ACCEPTED, ev.kind());
        assertEquals(OrderStatus.PENDING_NEW, ev.status());
        assertEquals(ONE, ev.remainingQty());
        assertEquals(1, ev.eventSeq());
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CREATE, d.kind());
        assertEquals(1, d.commandIdHigh());
        assertEquals("attempt 0, kind CREATE", 0, d.commandIdLow());
        assertEquals(1, d.dispatchSeq());
        assertEquals(100 * ONE, d.price());
        assertEquals(ONE, d.quantity());
        assertEquals(0, d.oldOrderId());
        assertEquals("hold placed in the same apply", 100 * ONE, r.engine.account(1).locked(USD));
        assertEquals(0, r.engine.account(1).available(USD));
        assertEquals(1, r.orders.orders().openCount(1));
        final CoreOrder o = r.orders.orders().get(1);
        assertNotNull(o);
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(CoreOrder.PK_CREATE, o.pendingKind);
        assertEquals(1, r.orders.orders().activeByClientId(1, 0, 77));
        assertEquals(2, r.engine.getNextOrderId());
        assertEquals(1, r.engine.getEventSeq());
        assertEquals(1, r.engine.getDispatchSeq());
    }

    @Test
    public void sellLimitHoldsTheBaseQuantityAndBalanceCountsAsOneEvent() {
        final OrderRig r = OrderRig.open();
        r.deposit(2, BTC, 3 * ONE);
        r.markNow();
        r.create(r.limit(2, 1, 1, 100 * ONE, 2 * ONE));
        assertEquals(BTC, r.first(AssetsEngineEvent.OrderAck.class).holdAssetId());
        assertEquals(2 * ONE, r.engine.account(2).locked(BTC));
        assertEquals(ONE, r.engine.account(2).available(BTC));
        assertEquals(1, r.count(AssetsEngineEvent.Balance.class));
    }

    @Test
    public void theHoldIsTheBalanceCheck_insufficientFundsLeavesOnlyTheRateCounter() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 99 * ONE);
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.INSUFFICIENT_FUNDS, r.reject().reason());
        assertEquals(0, r.orders.orders().size());
        assertEquals(99 * ONE, r.engine.account(1).available(USD));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals("a later-check reject consumed one rate unit (K16)", 1, r.orders.user(1).secCount);
        // a user with NO account: rejected without creating one (K7)
        r.markNow();
        r.create(r.limit(5, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.INSUFFICIENT_FUNDS, r.reject().reason());
        assertNull(r.engine.account(5));
    }

    @Test
    public void redC2b_anActiveClientOrderIdIsRejectedAndReusableAfterTheOrderIsGone() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand a = r.limit(1, 1, 0, 100 * ONE, ONE);
        a.clientOrderIdHigh = 5;
        a.clientOrderIdLow = 6;
        r.create(a);
        r.markNow();
        final CreateOrderCommand b = r.limit(1, 1, 0, 100 * ONE, ONE);
        b.clientOrderIdHigh = 5;
        b.clientOrderIdLow = 6;
        r.create(b);
        assertEquals(CoreRejectReason.DUPLICATE_CLIENT_ORDER_ID, r.reject().reason());
        assertEquals(1, r.orders.orders().size());
        assertEquals("the duplicate placed no hold", 100 * ONE, r.engine.account(1).locked(USD));
        // a different high word is a different client id
        r.markNow();
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        c.clientOrderIdHigh = 6;
        c.clientOrderIdLow = 6;
        r.create(c);
        assertEquals(1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    @Test
    public void slotIsMembershipInTheOpenSet() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 10_000 * ONE);
        final var cfg = r.riskConfig(1, 1);
        cfg.maxOpenOrders = 2;
        r.setRisk(cfg);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.OPEN_ORDER_LIMIT, r.reject().reason());
        assertEquals(2, r.orders.orders().openCount(1));
        assertEquals("market 2 has its own limit", 1, countAccepted(r, r.limit(1, 2, 0, 100 * ONE, ONE)));
    }

    private static long countAccepted(final OrderRig r, final CreateOrderCommand c) {
        r.markNow();
        r.create(c);
        return r.count(AssetsEngineEvent.OrderAck.class);
    }

    @Test
    public void marketBuyUsesTheIntegerBudgetPriceOrTheClientBudget() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.markNow();
        r.create(r.market(1, 1, 0, 2 * ONE, 0));
        assertEquals("no ask seen yet", CoreRejectReason.NO_REFERENCE_PRICE, r.reject().reason());
        assertEquals(0, r.engine.account(1).locked(USD));
        r.tick(1, 99 * ONE, 100 * ONE, 10);
        r.markNow();
        r.create(r.market(1, 1, 0, 2 * ONE, 0));
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals("bestAsk 100 x 1.05 x qty 2, integer math", 210 * ONE, ack.holdAmount());
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals("quantity AND budget travel to the ME", 2 * ONE, d.quantity());
        assertEquals(210 * ONE, d.budget());
        assertEquals(1, d.orderType());
        r.markNow();
        r.create(r.market(1, 1, 0, ONE, 150 * ONE));
        assertEquals("an explicit client budget is the hold", 150 * ONE, r.first(AssetsEngineEvent.OrderAck.class).holdAmount());
        assertEquals(360 * ONE, r.engine.account(1).locked(USD));
        // a SELL market order needs no reference price: the hold is the base quantity
        r.deposit(1, BTC, ONE);
        r.markNow();
        r.create(r.market(1, 1, 1, ONE, 0));
        assertEquals(BTC, r.first(AssetsEngineEvent.OrderAck.class).holdAssetId());
    }

    @Test
    public void cancelSemantics_ownerUnknownPendingAndDuplicate() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.markNow();
        r.cancel(2, 1);
        assertEquals(CoreRejectReason.NOT_OWNER, r.reject().reason());
        r.markNow();
        r.cancel(1, 42);
        assertEquals(CoreRejectReason.UNKNOWN_ORDER, r.reject().reason());
        // the create is still dispatch-pending: the cancel is recorded as a flag, no ME command yet (C8b)
        r.markNow();
        r.cancel(1, 1);
        final AssetsEngineEvent.OrderAck ack = r.only(AssetsEngineEvent.OrderAck.class);
        assertEquals(OrderStatus.PENDING_CANCEL, ack.status());
        assertFalse(ack.duplicate());
        assertTrue(r.orders.orders().get(1).has(CoreOrder.F_CANCEL_PENDING));
        assertEquals("no second MeDispatch while the create is unresolved", 1, r.engine.getDispatchSeq());
        assertEquals("the hold stays until the terminal", 100 * ONE, r.engine.account(1).locked(USD));
        r.markNow();
        r.cancel(1, 1);
        assertTrue("a second cancel is acknowledged as already pending", r.only(AssetsEngineEvent.OrderAck.class).duplicate());
        assertEquals(1, r.engine.getDispatchSeq());
    }

    @Test
    public void cancelByClientOrderId() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        c.clientOrderIdLow = 99;
        r.create(c);
        final var cancel = new com.openexchange.assets.domain.commands.CancelOrderCommand();
        cancel.reset();
        cancel.userId = 1;
        cancel.orderId = 0;
        cancel.origClientOrderIdLow = 99;
        r.markNow();
        r.orders.applyCancel(cancel, r.now);
        assertEquals(1, r.only(AssetsEngineEvent.OrderAck.class).orderId());
        cancel.origClientOrderIdLow = 98;
        r.markNow();
        r.orders.applyCancel(cancel, r.now);
        assertEquals(CoreRejectReason.UNKNOWN_ORDER, r.reject().reason());
    }

    @Test
    public void gtdEntersTheDeadlineIndexAndExpiryIsOneShot() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        c.timeInForce = 3;
        c.expiresAtMs = r.now + 5_000;
        r.create(c);
        assertEquals("one GTD deadline (the requestId TTL rides the same index)", 1, gtdDeadlines(r));
        assertEquals(r.now + 5_000, r.engine.deadlines().firstDeadline());
        r.advance(4_999);
        r.sweep();
        assertEquals(1, gtdDeadlines(r));
        r.advance(1);
        r.markNow();
        r.sweep();
        final CoreOrder o = r.orders.orders().get(1);
        assertTrue("ACCEPTED with the create unresolved: expiry is requested, the cancel is minted on ACK (C9)",
                o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertEquals("the GTD entry popped once; the request TTL stays", 0, gtdDeadlines(r));
        assertEquals(1, r.engine.getDispatchSeq());
        r.markNow();
        r.sweep();
        assertEquals("a second sweep applies nothing", 0, r.events().size());
    }

    @Test
    public void breakerBlocksCreateButNotCancel() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.markNow();
        r.orders.applyCircuitBreaker(0xC1L, 1, true, r.now);
        assertEquals(com.openexchange.assets.domain.AdminResult.ACCEPTED, r.first(AssetsEngineEvent.AdminAck.class).result());
        assertTrue(r.orders.market(1).breakerTripped);
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.CIRCUIT_BREAKER, r.reject().reason());
        assertEquals("the breaker is a risk check: it consumes budget", 2, r.orders.user(1).secCount);
        r.markNow();
        r.create(r.limit(1, 2, 0, 100 * ONE, ONE));
        assertEquals("other markets flow", 1, r.count(AssetsEngineEvent.OrderAck.class));
        r.markNow();
        r.cancel(1, 1);
        assertEquals(OrderStatus.PENDING_CANCEL, r.only(AssetsEngineEvent.OrderAck.class).status());
        r.markNow();
        r.orders.applyCircuitBreaker(0xC2L, 1, true, r.now);
        assertEquals(com.openexchange.assets.domain.AdminResult.DUPLICATE, r.only(AssetsEngineEvent.AdminAck.class).result());
        r.orders.applyCircuitBreaker(0xC3L, 1, false, r.now);
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    @Test
    public void aMarketGateBlocksOnlyItsMarket() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.engine.gate().set(2, GateState.SUSPENDED, GateReason.BRIDGE_HALT);
        r.markNow();
        r.create(r.limit(1, 2, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.GATE_CLOSED, r.reject().reason());
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(1, r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals(OrderEntryGate.GLOBAL, 0);
    }

    @Test
    public void gtdDeadlineKindIsTheOrderKind() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        c.timeInForce = 3;
        c.expiresAtMs = r.now + 1;
        r.create(c);
        final int[] kinds = new int[1];
        r.engine.deadlines().forEach((d, id, k) -> { if (d == c.expiresAtMs) kinds[0] = k; });
        assertEquals(DeadlineIndex.KIND_GTD, kinds[0]);
    }

    private static int gtdDeadlines(final OrderRig r) {
        final int[] n = new int[1];
        r.engine.deadlines().forEach((d, id, k) -> { if (k == DeadlineIndex.KIND_GTD) n[0]++; });
        return n[0];
    }
}

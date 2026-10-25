// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.AmendSupport.LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.PRICE;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 G3 (contract 3.4 check order = lifecycle B7 preconditions): the keep rules for newPrice/newQuantity,
 * the reject vocabulary in contract order, what consumes the rate bucket, the collar on the new price, the
 * gate, and the requestId payload mismatch. Every reject mutates nothing but the rate counter (and only from
 * the rate check on).
 */
public class AmendAdmissionTest {

    private static void assertUntouched(final AmendSupport s, final String what) {
        final CoreOrder o = s.order(1);
        assertFalse(what + ": no window", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(what + ": attempt unchanged", 0, o.attempt);
        assertEquals(what + ": no pending delta", 0L, o.pendingHoldDelta);
        assertEquals(what + ": hold unchanged", 100 * ONE, s.acct(1).holdRemaining(1));
        assertEquals(what + ": locked unchanged", 100 * ONE, s.acct(1).locked(USD));
        assertEquals(what + ": no dispatch", 1, s.r.engine.getDispatchSeq());
        assertEquals(what + ": no replace deadline", 0, s.replaceDeadlines());
    }

    // contract 3.4: newPrice = 0 keeps the price
    @Test
    public void newPriceZeroKeepsThePrice() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.markNow();
        s.amend(1, 1, 0, 2 * ONE);
        final CoreOrder o = s.order(1);
        assertTrue("accepted into a window", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals("pendingPrice = the current price", PRICE, o.pendingPrice);
        assertEquals(2 * ONE, o.pendingQuantity);
        assertEquals("delta computed at the kept price: 100 x 2 - 100 x 1", 100 * ONE, o.pendingHoldDelta);
        assertEquals(200 * ONE, s.acct(1).holdRemaining(1));
        assertEquals("the dispatch carries the effective price", PRICE, r.first(AssetsEngineEvent.MeDispatch.class).price());
    }

    // contract 3.4: newQuantity = 0 keeps the TOTAL quantity (incl. fills)
    @Test
    public void newQuantityZeroKeepsTheTotalQuantity() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.markNow();
        s.amend(1, 1, 110 * ONE, 0);
        final CoreOrder o = s.order(1);
        assertTrue("accepted into a window", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals("pendingQuantity = the current total quantity", ONE, o.pendingQuantity);
        assertEquals(110 * ONE, o.pendingPrice);
        assertEquals("delta = 110 x 1 - 100 x 1", 10 * ONE, o.pendingHoldDelta);
        assertEquals(110 * ONE, o.pendingHoldTarget);
        assertEquals(110 * ONE, s.acct(1).holdRemaining(1));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(ONE, d.quantity());
        assertEquals(110 * ONE, d.price());
    }

    // contract 3.4 INVALID: both zero
    @Test
    public void bothZeroIsInvalid() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        final int secBefore = r.orders.user(1).secCount;
        r.markNow();
        s.amend(1, 1, 0, 0);
        assertEquals(CoreRejectReason.INVALID, s.rejectOnly("both zero"));
        assertUntouched(s, "both zero");
        assertEquals("INVALID is decided after the rate check: one unit consumed", secBefore + 1, r.orders.user(1).secCount);
    }

    // contract 3.4 INVALID: negative values
    @Test
    public void negativeNewPriceOrNewQuantityIsInvalid() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.markNow();
        s.amend(1, 1, -1, 2 * ONE);
        assertEquals(CoreRejectReason.INVALID, s.rejectOnly("negative newPrice"));
        r.markNow();
        s.amend(1, 1, 0, -1);
        assertEquals(CoreRejectReason.INVALID, s.rejectOnly("negative newQuantity"));
        assertUntouched(s, "negative values");
    }

    // contract 3.4 INVALID: legQty = newQty - filledQty <= 0
    @Test
    public void newQuantityAtOrBelowFilledQtyIsInvalid() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.sellSide();
        s.fillBase(40_000_000L); // filled 0.4
        assertEquals("precondition (contract 3.1): filledQty 0.4", 40_000_000L, s.order(1).filledQty);
        r.markNow();
        s.amend(1, 1, 0, 40_000_000L); // legQty 0
        assertEquals(CoreRejectReason.INVALID, s.rejectOnly("newQuantity == filledQty"));
        r.markNow();
        s.amend(1, 1, 0, 30_000_000L); // legQty < 0
        assertEquals(CoreRejectReason.INVALID, s.rejectOnly("newQuantity < filledQty"));
        final CoreOrder o = s.order(1);
        assertFalse(o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        assertEquals(ONE, o.quantity);
        assertEquals("hold stays at 100 - 40", 60 * ONE, s.acct(1).holdRemaining(1));
        assertEquals(2, s.r.engine.getDispatchSeq()); // the two creates, nothing else
    }

    // contract 3.4: MISSING_REQUEST_ID is the first check (D5)
    @Test
    public void missingRequestIdIsRejectedFirst() {
        final AmendSupport s = AmendSupport.baseWithoutLeg(); // decided before NOT_OPEN: the leg is irrelevant
        final OrderRig r = s.r;
        final AmendOrderCommand c = s.amendCmd(1, 1, PRICE, 2 * ONE);
        c.requestIdHigh = 0;
        c.requestIdLow = 0;
        r.markNow();
        s.amend(c);
        assertEquals(CoreRejectReason.MISSING_REQUEST_ID, s.rejectOnly("amend without a requestId"));
        assertUntouched(s, "missing requestId");
        assertEquals("decided before the rate check: no unit consumed", 1, r.orders.user(1).secCount);
    }

    // contract 3.4: UNKNOWN_ORDER
    @Test
    public void unknownOrderIsRejected() {
        final AmendSupport s = AmendSupport.baseWithoutLeg();
        final OrderRig r = s.r;
        r.markNow();
        s.amend(1, 42, PRICE, 2 * ONE);
        assertEquals(CoreRejectReason.UNKNOWN_ORDER, s.rejectOnly("amend of orderId 42"));
        final AssetsEngineEvent.OrderReject rej = r.first(AssetsEngineEvent.OrderReject.class);
        assertEquals(42, rej.orderId());
        assertUntouched(s, "unknown order");
    }

    // contract 3.4: NOT_OWNER
    @Test
    public void notOwnerIsRejected() {
        final AmendSupport s = AmendSupport.baseWithoutLeg();
        final OrderRig r = s.r;
        r.markNow();
        s.amend(2, 1, PRICE, 2 * ONE);
        assertEquals(CoreRejectReason.NOT_OWNER, s.rejectOnly("user 2 amends user 1's order"));
        assertUntouched(s, "not owner");
    }

    // contract 3.4: NOT_OPEN for a cancel-pending order
    @Test
    public void notOpen_cancelPendingOrder() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.cancel(1, 1); // leg resting: the cancel is minted (dispatchSeq 2)
        assertTrue("precondition", s.order(1).has(CoreOrder.F_CANCEL_PENDING));
        final long dispatchSeq = r.engine.getDispatchSeq();
        r.markNow();
        s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals(CoreRejectReason.NOT_OPEN, s.rejectOnly("amend of a cancel-pending order"));
        assertFalse(s.order(1).has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(100 * ONE, s.acct(1).holdRemaining(1));
        assertEquals(dispatchSeq, r.engine.getDispatchSeq());
        assertEquals(0, s.replaceDeadlines());
    }

    // contract 3.4: NOT_OPEN while the create is dispatch-pending (no leg yet)
    @Test
    public void notOpen_dispatchPendingCreate() {
        final AmendSupport s = AmendSupport.baseWithoutLeg();
        final OrderRig r = s.r;
        assertTrue("precondition", s.order(1).has(CoreOrder.F_DISPATCH_PENDING));
        r.markNow();
        s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals(CoreRejectReason.NOT_OPEN, s.rejectOnly("amend before the create outcome"));
        assertUntouched(s, "dispatch-pending create");
    }

    // contract 3.4: NOT_OPEN for a type that is not LIMIT / LIMIT_MAKER
    @Test
    public void notOpen_marketOrder() {
        final AmendSupport s = new AmendSupport(OrderRig.open());
        final OrderRig r = s.r;
        r.deposit(1, USD, 1_000 * ONE);
        r.tick(1, 99 * ONE, 100 * ONE, 10);
        r.create(r.market(1, 1, 0, ONE, 0)); // orderId 1: BUY MARKET, hold 105
        s.createOutcome(1, 1, 79L);
        s.assertLeg(1, 79L);
        final long held = s.acct(1).holdRemaining(1);
        r.markNow();
        s.amend(1, 1, 0, 2 * ONE);
        assertEquals(CoreRejectReason.NOT_OPEN, s.rejectOnly("amend of a MARKET order"));
        assertFalse(s.order(1).has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(held, s.acct(1).holdRemaining(1));
        assertEquals(1, r.engine.getDispatchSeq());
    }

    // contract 3.4 "rate (consumes)" + risk C20: an amend consumes the bucket, a cancel does not
    @Test
    public void amendConsumesTheRateBucketAndCancelDoesNot() {
        final OrderRig r = OrderRig.open();
        final RiskConfigSetCommand cfg = r.riskConfig(1, 1);
        cfg.maxOrdersPerSec = 2;
        r.setRisk(cfg);
        final AmendSupport s = new AmendSupport(r).buyBase(true); // the create is unit 1
        assertEquals("precondition", 1, r.orders.user(1).secCount);
        r.markNow();
        s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals("the amend is accepted", 1, r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals(0, r.count(AssetsEngineEvent.OrderReject.class));
        assertEquals("the amend consumed unit 2", 2, r.orders.user(1).secCount);
        r.markNow();
        r.create(r.limit(1, 1, 0, PRICE, ONE));
        assertEquals("the third action in the second is refused", CoreRejectReason.RATE_LIMIT, r.reject().reason());
        assertEquals(2, r.orders.user(1).secCount);
        r.markNow();
        r.cancel(1, 1);
        assertEquals("a cancel is free of rate budget", OrderStatus.PENDING_CANCEL, r.only(AssetsEngineEvent.OrderAck.class).status());
        assertEquals(2, r.orders.user(1).secCount);
    }

    // contract 3.4: PRICE_COLLAR on the new price against the last applied trade (exact bps, inclusive)
    @Test
    public void priceCollarAppliesToTheNewPrice() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.orders.market(1).lastTradePrice = PRICE; // default collar 2000 bps = 20 -> [80, 120]
        final int secBefore = r.orders.user(1).secCount;
        r.markNow();
        s.amend(1, 1, 121 * ONE, 0);
        assertEquals(CoreRejectReason.PRICE_COLLAR, s.rejectOnly("new price 121 vs last trade 100"));
        assertUntouched(s, "collar breach");
        assertEquals("the collar is a risk check: it consumes budget", secBefore + 1, r.orders.user(1).secCount);
        r.markNow();
        s.amend(1, 1, 120 * ONE, 0); // |120 - 100| = 20 <= 20
        assertEquals("the boundary is inclusive", 1, r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals("delta = 120 x 1 - 100 x 1", 120 * ONE, s.acct(1).holdRemaining(1));
    }

    // contract 3.4: GATE_CLOSED when the global gate is suspended (decided before the rate check)
    @Test
    public void gateClosedWhenTheGlobalGateIsSuspended() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.engine.gate().set(OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.ADMIN);
        final int secBefore = r.orders.user(1).secCount;
        r.markNow();
        s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals(CoreRejectReason.GATE_CLOSED, s.rejectOnly("amend through a suspended gate"));
        assertUntouched(s, "gate closed");
        assertEquals("decided before the rate check: no unit consumed", secBefore, r.orders.user(1).secCount);
        assertEquals(OrderStatus.NEW.code(), s.order(1).status);
        assertEquals(LEG, s.order(1).currentLegId);
    }

    // contract 3.4 requestId replay: same requestId, different payload is a mismatch and tops up nothing
    @Test
    public void sameRequestIdWithADifferentPayloadIsAMismatch() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        final AmendOrderCommand first = s.amend(1, 1, PRICE, 2 * ONE);
        final Account a = s.acct(1);
        assertEquals("precondition: the first amend topped up to 200", 200 * ONE, a.holdRemaining(1));
        final AmendOrderCommand again = s.amendCmd(1, 1, PRICE, 3 * ONE);
        again.requestIdHigh = first.requestIdHigh;
        again.requestIdLow = first.requestIdLow;
        r.markNow();
        s.amend(again);
        assertEquals(CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH, s.rejectOnly("same requestId, newQuantity 3 instead of 2"));
        assertEquals("no second top-up", 200 * ONE, a.holdRemaining(1));
        assertEquals(2, r.engine.getDispatchSeq());
        assertEquals("the first window stands", 2 * ONE, s.order(1).pendingQuantity);
    }
}

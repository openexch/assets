// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 G2 fixture around {@link OrderRig}: synthetic creates (STOP_LOSS 3, STOP_LIMIT 4, TRAILING_STOP 5), ticks
 * with a monotonic bookVersion, and the child-shape reads of contract 3.2. Market 1 (BTC-USD): BUY locks USD,
 * SELL locks BTC. User 1 is the buyer (10 000 USD), user 2 the seller (100 BTC). Package-private; shared by
 * {@link TriggerTest} and {@link TriggerLifecycleTest}.
 */
final class TriggerRig {

    static final int MKT = 1;
    static final int BUY = OrderEngine.SIDE_BUY;
    static final int SELL = OrderEngine.SIDE_SELL;
    static final int WIRE_LIMIT = OrderEngine.TYPE_LIMIT;
    static final int WIRE_MARKET = OrderEngine.TYPE_MARKET;
    static final long CHILD_CREATE_ID_LOW = (1L << 2) | DispatchKind.CREATE.code(); // attempt 1, kind CREATE
    /** Market 1 default {@code marketBuySlippageBps} (MarketRisk genesis). */
    static final int SLIP_BPS = 500;

    final OrderRig r;
    long version;

    TriggerRig(final OrderRig r) {
        this.r = r;
    }

    /** Gate open, user 1 holds 10 000 USD, user 2 holds 100 BTC, no tick applied yet. */
    static TriggerRig funded() {
        final TriggerRig t = new TriggerRig(OrderRig.open());
        t.r.deposit(1, USD, 10_000 * ONE);
        t.r.deposit(2, BTC, 100 * ONE);
        return t;
    }

    /** {@link #funded()} plus a first tick (bid, ask) so the market has a top of book before admission. */
    static TriggerRig quoted(final long bid, final long ask) {
        final TriggerRig t = funded();
        t.tick(bid, ask);
        return t;
    }

    // ---------------------------------------------------------------- inputs

    /** Next valid tick on market 1 (bookVersion + 1), no last trade. */
    void tick(final long bid, final long ask) {
        r.tick(MKT, bid, ask, ++version);
    }

    CreateOrderCommand stopLoss(final long user, final int side, final long stopPrice, final long qty) {
        final CreateOrderCommand c = r.limit(user, MKT, side, 0L, qty);
        c.orderType = OrderEngine.TYPE_STOP_LOSS;
        c.stopPrice = stopPrice;
        return c;
    }

    CreateOrderCommand stopLimit(final long user, final int side, final long stopPrice, final long price, final long qty) {
        final CreateOrderCommand c = r.limit(user, MKT, side, price, qty);
        c.orderType = OrderEngine.TYPE_STOP_LIMIT;
        c.stopPrice = stopPrice;
        return c;
    }

    CreateOrderCommand trailing(final long user, final int side, final long delta, final long qty, final long budget) {
        final CreateOrderCommand c = r.limit(user, MKT, side, 0L, qty);
        c.orderType = OrderEngine.TYPE_TRAILING_STOP;
        c.trailingDelta = delta;
        c.budget = budget;
        return c;
    }

    /**
     * Create and require the contract 1 outcome for a valid stop or trailing order: accepted PENDING_TRIGGER, an
     * armed record, no ME command. Returns the orderId.
     */
    long admit(final CreateOrderCommand c) {
        r.markNow();
        r.create(c);
        final List<AssetsEngineEvent> e = r.events();
        for (final AssetsEngineEvent ev : e) {
            assertTrue("contract 1: a valid synthetic create (type " + c.orderType + ") is accepted PENDING_TRIGGER, "
                    + "never refused; got " + e, !(ev instanceof AssetsEngineEvent.OrderReject));
            assertTrue("contract 1: an armed synthetic order sends no ME command at admission; got " + e,
                    !(ev instanceof AssetsEngineEvent.MeDispatch));
        }
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals("contract 1: the OrderAck of a stop/trailing create says PENDING_TRIGGER", OrderStatus.PENDING_TRIGGER,
                ack.status());
        final CoreOrder o = r.orders.orders().get(ack.orderId());
        assertNotNull("contract 1: the accepted synthetic order has a live record", o);
        assertEquals("contract 1: the record status is PENDING_TRIGGER (7)", OrderStatus.PENDING_TRIGGER.code(), o.status);
        assertEquals("contract 1: the record keeps its synthetic type", c.orderType, o.orderType);
        return ack.orderId();
    }

    // ---------------------------------------------------------------- reads

    CoreOrder order(final long orderId) {
        return r.orders.orders().get(orderId);
    }

    long holdRemaining(final long user, final long orderId) {
        return r.engine.account(user).holdRemaining(orderId);
    }

    List<AssetsEngineEvent.MeDispatch> dispatches() {
        final List<AssetsEngineEvent.MeDispatch> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatch d) {
                out.add(d);
            }
        }
        return out;
    }

    List<AssetsEngineEvent.MeDispatchReply> replies() {
        final List<AssetsEngineEvent.MeDispatchReply> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatchReply d) {
                out.add(d);
            }
        }
        return out;
    }

    /** Exactly one MeDispatch since the mark, else an assertion failure naming the scenario. */
    AssetsEngineEvent.MeDispatch onlyDispatch(final String what) {
        final List<AssetsEngineEvent.MeDispatch> d = dispatches();
        assertEquals(what + ": exactly one MeDispatch CREATE for the child (contract 3.2), got " + r.events(), 1, d.size());
        return d.get(0);
    }

    void assertNoDispatch(final String what) {
        assertEquals(what + ": no MeDispatch, got " + r.events(), 0, dispatches().size());
    }

    /** The contract 3.2 child shape: a durable CREATE (idHigh = orderId, idLow = 1<<2|0) with the table's payload. */
    void assertChild(final String what, final AssetsEngineEvent.MeDispatch d, final long orderId, final long user, final int side,
                     final int wireType, final long price, final long qty, final long budget) {
        assertEquals(what + ": the child is a CREATE", DispatchKind.CREATE, d.kind());
        assertEquals(what + ": idHigh = parent orderId", orderId, d.commandIdHigh());
        assertEquals(what + ": idLow = attempt 1 << 2 | CREATE", CHILD_CREATE_ID_LOW, d.commandIdLow());
        assertEquals(what + ": omsOrderId = parent orderId (the hold key)", orderId, d.omsOrderId());
        assertEquals(what + ": no old leg on a child create", 0L, d.oldOrderId());
        assertEquals(what + ": user", user, d.userId());
        assertEquals(what + ": market", MKT, d.marketId());
        assertEquals(what + ": side", side, d.side());
        assertEquals(what + ": child orderType on the wire (MARKET 1 / LIMIT 0), never the synthetic code", wireType, d.orderType());
        assertEquals(what + ": child price", price, d.price());
        assertEquals(what + ": child quantity = remainingQty", qty, d.quantity());
        assertEquals(what + ": child budget", budget, d.budget());
    }

    /** The parent after the trigger apply: PENDING_NEW, attempt 1, create pending on the dispatched seq, one TRIGGERED. */
    void assertTriggered(final String what, final long orderId, final AssetsEngineEvent.MeDispatch d) {
        final CoreOrder o = order(orderId);
        assertNotNull(what + ": the triggered parent stays live", o);
        assertEquals(what + ": PENDING_TRIGGER -> PENDING_NEW at the trigger (3.2)", OrderStatus.PENDING_NEW.code(), o.status);
        assertEquals(what + ": attempt 1 at the trigger", 1, o.attempt);
        assertTrue(what + ": the child create is DISPATCH_PENDING", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(what + ": pendingKind CREATE", CoreOrder.PK_CREATE, o.pendingKind);
        assertEquals(what + ": pendingDispatchSeq = the child's dispatchSeq", d.dispatchSeq(), o.pendingDispatchSeq);
        assertEquals(what + ": the child consumed the engine's dispatchSeq", r.engine.getDispatchSeq(), d.dispatchSeq());
        assertEquals(what + ": exactly one TRIGGERED event", 1L, OutcomeFeed.kindCount(r, orderId, EventKind.TRIGGERED));
        assertEquals(what + ": the TRIGGERED event reports PENDING_NEW", OrderStatus.PENDING_NEW,
                OutcomeFeed.event(r, orderId, EventKind.TRIGGERED).status());
    }

    /** {@code stopPrice x (1 + slipBps / 1e4) x qty} in exact 8 dp, the contract 1 BUY STOP_LOSS estimate. */
    static long buyStopEstimate(final long stopPrice, final long qty) {
        final long slip = ONE + (long) SLIP_BPS * (ONE / 10_000L);
        return FixedPoint.multiply(FixedPoint.multiply(stopPrice, slip), qty);
    }
}

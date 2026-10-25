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
 * RC1 S6 G3 fixture (contract 3.4): user 1 works a BUY ICEBERG on market 1 (orderId 1, price 100) against SELL
 * LIMITs of user 2 whose legs are learned by CREATE APPLIED. Slice legs are named by the test; seller legs are
 * {@code 600 + orderId}. Trades get dense tradeIds from 1. The command id is the S5 encoding
 * {@code idHigh = orderId, idLow = attempt << 2 | kind}.
 */
final class IcebergRig {

    static final long ICE = 1L;
    static final long ICE_USER = 1L;
    static final long SELLER = 2L;
    static final long PX = 100 * ONE;
    static final long S1 = 501L;
    static final long S2 = 502L;
    static final long S3 = 503L;
    static final long USD_DEPOSIT = 1_000 * ONE;
    static final int MKT = 1;
    static final int BUY = 0;
    static final int SELL = 1;

    final OrderRig r;
    final FeedRig f;
    private long nextTradeId = 1;

    private IcebergRig(final OrderRig r) {
        this.r = r;
        this.f = new FeedRig(r);
    }

    /** Funded rig, nothing created yet: user 1 has 1000 USD, user 2 has 10 BTC. */
    static IcebergRig funded() {
        final IcebergRig k = new IcebergRig(OrderRig.open());
        k.r.deposit(ICE_USER, USD, USD_DEPOSIT);
        k.r.deposit(SELLER, BTC, 10 * ONE);
        return k;
    }

    /** The iceberg CreateOrder (type 6) of user 1: BUY at 100, total {@code qty}, display {@code display}. */
    CreateOrderCommand icebergCmd(final long qty, final long display) {
        final CreateOrderCommand c = r.limit(ICE_USER, MKT, BUY, PX, qty);
        c.orderType = OrderEngine.TYPE_ICEBERG;
        c.displayQty = display;
        return c;
    }

    /** Funded rig with the iceberg accepted as orderId 1 (asserted, so a refused iceberg fails here by assertion). */
    static IcebergRig buy(final long qty, final long display) {
        final IcebergRig k = funded();
        k.r.create(k.icebergCmd(qty, display));
        k.assertAccepted(qty, display);
        return k;
    }

    void assertAccepted(final long qty, final long display) {
        final CoreOrder o = r.orders.orders().get(ICE);
        assertNotNull("contract 3.4 admission: a BUY ICEBERG 100 x " + qty + " display " + display
                + " is accepted (orderId 1, live record, first slice dispatched); got " + r.sink.events(), o);
        assertEquals("contract 1: an accepted iceberg is PENDING_NEW", OrderStatus.PENDING_NEW.code(), o.status);
        assertEquals("contract 3.4: hiddenQty = qty - displayQty at admission", qty - display, o.hiddenQty);
        assertEquals("contract 3.4: slice 1 = displayQty", display, o.sliceRemainingQty);
    }

    /** SELL LIMIT of user 2 at {@code px} x {@code qty}, leg learned as 600 + orderId; returns its orderId. */
    long seller(final long px, final long qty) {
        r.create(r.limit(SELLER, MKT, SELL, px, qty));
        final long id = r.engine.getNextOrderId() - 1;
        final CoreOrder o = r.orders.orders().get(id);
        assertTrue("precondition: the counterparty SELL LIMIT is live", o != null && o.side == SELL && o.userId == SELLER);
        OutcomeFeed.ack(r, id, legOf(id));
        return id;
    }

    static long legOf(final long sellerOrderId) {
        return 600L + sellerOrderId;
    }

    /** CREATE outcome APPLIED for slice {@code attempt} of the iceberg: the core learns {@code leg}. */
    void ackSlice(final int attempt, final long leg) {
        OutcomeFeed.outcome(r, ICE, attempt, OutcomeFeed.KIND_CREATE, OutcomeFeed.APPLIED, leg, 0L);
    }

    /** The seller order takes {@code qty} out of the resting slice leg (seller is taker, iceberg is maker). */
    void sellerTakes(final long sellerOrderId, final long sliceLeg, final long px, final long qty) {
        f.trade(nextTradeId++, sellerOrderId, legOf(sellerOrderId), SELLER, ICE, sliceLeg, ICE_USER, px, qty, false);
    }

    /** The slice leg takes {@code qty} from a resting seller order (iceberg is taker at the seller's price). */
    void sliceTakes(final long sellerOrderId, final long sliceLeg, final long px, final long qty) {
        f.trade(nextTradeId++, ICE, sliceLeg, ICE_USER, sellerOrderId, legOf(sellerOrderId), SELLER, px, qty, true);
    }

    CoreOrder ice() {
        return OutcomeFeed.live(r, ICE);
    }

    boolean iceLive() {
        return r.orders.orders().get(ICE) != null;
    }

    long holdRemaining() {
        return r.engine.account(ICE_USER).holdRemaining(ICE);
    }

    static long notional(final long px, final long qty) {
        return FixedPoint.multiply(px, qty);
    }

    /** MeDispatch events since the mark that name the iceberg. */
    List<AssetsEngineEvent.MeDispatch> iceDispatches() {
        final List<AssetsEngineEvent.MeDispatch> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatch d && d.omsOrderId() == ICE) {
                out.add(d);
            }
        }
        return out;
    }

    /** The one MeDispatch for the iceberg since the mark. */
    AssetsEngineEvent.MeDispatch onlyIceDispatch(final String what) {
        final List<AssetsEngineEvent.MeDispatch> d = iceDispatches();
        assertEquals(what + ": exactly one MeDispatch for the iceberg in this apply, got " + r.events(), 1, d.size());
        return d.get(0);
    }

    /** Assert a slice CREATE on the wire: LIMIT at 100, the slice quantity, attempt {@code attempt}. */
    static void assertSliceCreate(final String what, final AssetsEngineEvent.MeDispatch d, final int attempt, final long qty) {
        assertEquals(what + ": kind CREATE", DispatchKind.CREATE, d.kind());
        assertEquals(what + ": idHigh = orderId", ICE, d.commandIdHigh());
        assertEquals(what + ": idLow = attempt << 2 | CREATE with attempt " + attempt, ((long) attempt << 2), d.commandIdLow());
        assertEquals(what + ": a slice goes to the ME as a LIMIT (wire type 0)", OrderEngine.TYPE_LIMIT, d.orderType());
        assertEquals(what + ": BUY", BUY, d.side());
        assertEquals(what + ": slice price = the iceberg price", PX, d.price());
        assertEquals(what + ": slice quantity", qty, d.quantity());
        assertEquals(what + ": a LIMIT slice has no budget", 0L, d.budget());
        assertEquals(what + ": no old leg on a create", 0L, d.oldOrderId());
    }

    /** Record state after a refill or a fill: (hidden, sliceRemaining, filled, remaining, currentLeg, oldLeg, attempt). */
    void assertSliceState(final String what, final long hidden, final long sliceRemaining, final long filled, final long remaining,
                          final long currentLeg, final long oldLeg, final int attempt) {
        final CoreOrder o = ice();
        assertEquals(what + ": hiddenQty", hidden, o.hiddenQty);
        assertEquals(what + ": sliceRemainingQty", sliceRemaining, o.sliceRemainingQty);
        assertEquals(what + ": filledQty", filled, o.filledQty);
        assertEquals(what + ": remainingQty", remaining, o.remainingQty);
        assertEquals(what + ": currentLegId", currentLeg, o.currentLegId);
        assertEquals(what + ": oldLegId", oldLeg, o.oldLegId);
        assertEquals(what + ": attempt", attempt, o.attempt);
        assertEquals(what + ": remaining = sliceRemaining + hidden", o.remainingQty, o.sliceRemainingQty + o.hiddenQty);
    }

    /** Assert the refill that follows an exhausted slice happened in this apply (contract 3.4). */
    void assertRefilled(final String what, final int attempt, final long sliceQty) {
        assertSliceCreate(what, onlyIceDispatch(what), attempt, sliceQty);
        assertEquals(what + ": one SLICE_SUBMITTED event", 1, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        final CoreOrder o = ice();
        assertTrue(what + ": the slice create is pending", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(what + ": pending kind CREATE", CoreOrder.PK_CREATE, o.pendingKind);
    }

    /** Ordered kinds of the iceberg's OrderEvents since the mark. */
    List<EventKind> iceKinds() {
        final List<EventKind> out = new ArrayList<>();
        for (final AssetsEngineEvent.OrderEvent e : f.orderEvents(ICE)) {
            out.add(e.kind());
        }
        return out;
    }

    /** The last OrderEvent of the iceberg since the mark. */
    AssetsEngineEvent.OrderEvent lastIceEvent() {
        final List<AssetsEngineEvent.OrderEvent> e = f.orderEvents(ICE);
        assertTrue("expected an OrderEvent for the iceberg, got " + r.events(), !e.isEmpty());
        return e.get(e.size() - 1);
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.IcebergRig.ICE;
import static com.openexchange.assets.application.engine.order.IcebergRig.ICE_USER;
import static com.openexchange.assets.application.engine.order.IcebergRig.MKT;
import static com.openexchange.assets.application.engine.order.IcebergRig.PX;
import static com.openexchange.assets.application.engine.order.IcebergRig.S1;
import static com.openexchange.assets.application.engine.order.IcebergRig.S2;
import static com.openexchange.assets.application.engine.order.IcebergRig.S3;
import static com.openexchange.assets.application.engine.order.IcebergRig.SELLER;
import static com.openexchange.assets.application.engine.order.IcebergRig.USD_DEPOSIT;
import static com.openexchange.assets.application.engine.order.IcebergRig.notional;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 gap tests for contract 3.4 (iceberg) not pinned by {@link IcebergTest}: a fill past the slice, a
 * quarantined iceberg at slice exhaustion, the new slice leg learned by implicit ack, GTD on an iceberg (live slice,
 * slice create pending, slice exhausted under the expiry cancel), and the pending-dispatch replay of a refilled slice.
 */
public class IcebergGapTest {

    private static final long HALF = ONE / 2;
    private static final long T4 = 40_000_000L; // 0.4
    private static final long T6 = 60_000_000L; // 0.6
    private static final long GTD_MS = 5_000L;

    // ---------------------------------------------------------------- fixtures

    /** A GTD BUY ICEBERG 100 x qty display, expiring GTD_MS after admission, accepted as orderId 1. */
    private static IcebergRig gtd(final long qty, final long display) {
        final IcebergRig k = IcebergRig.funded();
        final CreateOrderCommand c = k.icebergCmd(qty, display);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = k.r.now + GTD_MS;
        k.r.create(c);
        k.assertAccepted(qty, display);
        assertEquals("precondition: the GTD iceberg has one GTD deadline", 1, OutcomeFeed.gtdDeadlines(k.r));
        return k;
    }

    private static void expire(final IcebergRig k) {
        k.r.advance(GTD_MS);
        k.r.markNow();
        k.r.sweep();
    }

    private static List<AssetsEngineEvent.OrderFault> faults(final OrderRig r) {
        final List<AssetsEngineEvent.OrderFault> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.OrderFault f) {
                out.add(f);
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- (1) fill past the slice

    /** 3.4: slice 1 (1.0) gets a 1.4 trade: 0.4 comes out of hidden, ORDERFAULT SETTLE 0.4, refill = min(1, 1.6). */
    @Test
    public void fillPastTheSliceTakesTheExcessOutOfHiddenAndFaultsSettle() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, 2 * ONE);
        k.ackSlice(0, S1);
        final long faultsBefore = r.orders.orderFaultCount();
        r.markNow();
        k.sellerTakes(a, S1, PX, ONE + T4);
        final List<AssetsEngineEvent.OrderFault> f = faults(r);
        assertEquals("3.4: one ORDERFAULT for the fill past the slice, got " + r.events(), 1, f.size());
        assertEquals("3.4: kind SETTLE", OrderFaultKind.SETTLE, f.get(0).kind());
        assertEquals("3.4: the fault names the iceberg", ICE, f.get(0).orderId());
        assertEquals("3.4: amount = the excess over the slice (0.4)", T4, f.get(0).amount());
        assertEquals("3.4: the fault carries the trade id", 1L, f.get(0).tradeId());
        assertEquals("the fault is counted", faultsBefore + 1, r.orders.orderFaultCount());
        assertEquals("not an OVERFILL: the trade is within remainingQty", 0,
                f.stream().filter(x -> x.kind() == OrderFaultKind.OVERFILL).count());
        k.assertRefilled("3.4: the exhausted slice refills in the fill apply", 1, ONE);
        k.assertSliceState("3.4: hidden 2 - 0.4 - 1 (refill) = 0.6, slice 1, filled 1.4, remaining 1.6",
                T6, ONE, ONE + T4, ONE + T6, 0L, S1, 1);
        assertEquals("the FILL event reports the whole 1.4", ONE + T4, OutcomeFeed.event(r, ICE, EventKind.FILL).lastQty());
        assertEquals("cumQuote = 100 x 1.4 exact", notional(PX, ONE + T4), k.ice().cumQuoteQty);
        assertEquals("hold remaining = 300 - 140", notional(PX, 3 * ONE) - notional(PX, ONE + T4), k.holdRemaining());
        assertEquals("bought 1.4 BTC", ONE + T4, r.engine.account(ICE_USER).available(BTC));
        k.f.assertLockedEqualsHolds(ICE_USER);
    }

    /** 3.4: after a fill past slice 1 the reduced hidden drives the next refill: slices 1.4 (filled), 1, then 0.6. */
    @Test
    public void nextRefillAfterAFillPastTheSliceUsesTheReducedHidden() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, 2 * ONE);
        final long b = k.seller(PX, ONE);
        final long c = k.seller(PX, T6);
        k.ackSlice(0, S1);
        k.sellerTakes(a, S1, PX, ONE + T4);
        k.ackSlice(1, S2);
        r.markNow();
        k.sellerTakes(b, S2, PX, ONE);
        assertTrue("an exact slice fill raises no fault, got " + r.events(), faults(r).isEmpty());
        k.assertRefilled("3.4: slice 3 = min(display 1, hidden 0.6) = 0.6", 2, T6);
        k.assertSliceState("after slice 2", 0L, T6, 2 * ONE + T4, T6, 0L, S2, 2);
        assertEquals("two refills", 2L, r.orders.sliceCount());
        k.ackSlice(2, S3);
        r.markNow();
        k.sellerTakes(c, S3, PX, T6);
        assertTrue("no fourth slice, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("the parent is FILLED on the 0.6 slice", OrderStatus.FILLED, end.status());
        assertEquals("filled = 3", 3 * ONE, end.filledQty());
        assertNull("FILLED parent removed", r.orders.orders().get(ICE));
        assertEquals("nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("available = 1000 - 300", USD_DEPOSIT - notional(PX, 3 * ONE), r.engine.account(ICE_USER).available(USD));
        assertEquals("bought 3 BTC", 3 * ONE, r.engine.account(ICE_USER).available(BTC));
    }

    /** 3.4: a fill past the slice that completes the order: SETTLE fault for the hidden part, FILLED, no refill. */
    @Test
    public void fillPastTheSliceThatCompletesTheOrderFaultsAndFills() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, 3 * ONE);
        k.ackSlice(0, S1);
        r.markNow();
        k.sellerTakes(a, S1, PX, 3 * ONE);
        final List<AssetsEngineEvent.OrderFault> f = faults(r);
        assertEquals("3.4: one ORDERFAULT, got " + r.events(), 1, f.size());
        assertEquals("3.4: SETTLE", OrderFaultKind.SETTLE, f.get(0).kind());
        assertEquals("3.4: excess over the slice = 2", 2 * ONE, f.get(0).amount());
        assertTrue("no refill when nothing is left, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("no SLICE_SUBMITTED", 0, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("FILLED on the FILL event", EventKind.FILL, end.kind());
        assertEquals(OrderStatus.FILLED, end.status());
        assertEquals("filled 3", 3 * ONE, end.filledQty());
        assertNull("parent removed", r.orders.orders().get(ICE));
        assertEquals("nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("no refill counted", 0L, r.orders.sliceCount());
    }

    // ---------------------------------------------------------------- (2) quarantined iceberg

    /** 3.4: a quarantined iceberg whose slice is exhausted releases no next slice and stays live. */
    @Test
    public void quarantinedIcebergWithAnExhaustedSliceReleasesNoNextSlice() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        r.orders.applyQuarantine(0xA1L, ICE, r.now);
        assertTrue("precondition: quarantined", k.ice().has(CoreOrder.F_QUARANTINED));
        r.markNow();
        k.sellerTakes(a, S1, PX, ONE);
        assertTrue("3.4: no MeDispatch for a quarantined iceberg, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("3.4: no SLICE_SUBMITTED", 0, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        assertEquals("S5 3.5: the trade still applies (one FILL)", 1, OutcomeFeed.kindCount(r, ICE, EventKind.FILL));
        assertEquals("sliceCount unchanged", 0L, r.orders.sliceCount());
        assertTrue("the parent stays live", k.iceLive());
        k.assertSliceState("3.4 quarantined: slice 0, hidden kept, leg kept", 2 * ONE, 0L, ONE, 2 * ONE, S1, 0L, 0);
        assertFalse("no create pending", k.ice().has(CoreOrder.F_DISPATCH_PENDING));
        assertTrue("still quarantined", k.ice().has(CoreOrder.F_QUARANTINED));
        assertEquals("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED.code(), k.ice().status);
        assertEquals("hold remaining = 300 - 100", notional(PX, 2 * ONE), k.holdRemaining());
        k.f.assertLockedEqualsHolds(ICE_USER);
    }

    /** 3.4: the exhausted slice's journal FILLED on a quarantined iceberg also releases nothing; the parent stays live. */
    @Test
    public void quarantinedIcebergSliceFilledTerminalReleasesNothing() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        r.orders.applyQuarantine(0xA2L, ICE, r.now);
        k.sellerTakes(a, S1, PX, T4);
        r.markNow();
        k.f.terminal(ICE, ICE_USER, S1, FeedRig.ME_FILLED);
        assertTrue("no MeDispatch, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("no SLICE_SUBMITTED", 0, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        assertTrue("the parent stays live", k.iceLive());
        assertEquals("3.4: SETTLE fault for the unfilled 0.6", T6, r.first(AssetsEngineEvent.OrderFault.class).amount());
        k.assertSliceState("0.6 back to hidden, nothing released", 2 * ONE + T6, 0L, T4, 2 * ONE + T6, S1, 0L, 0);
        assertEquals("sliceCount unchanged", 0L, r.orders.sliceCount());
    }

    // ---------------------------------------------------------------- (3) implicit ack of the new slice

    private static IcebergRig sliceOneFilled() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        k.sellerTakes(a, S1, PX, ONE);
        assertEquals("precondition: slice 2 create pending (attempt 1)", 1, k.ice().attempt);
        assertEquals("precondition: slice 2 leg unknown", 0L, k.ice().currentLegId);
        return k;
    }

    /** S5 3.1 + 3.4: a trade naming slice 2's leg before its CREATE outcome learns the leg; the outcome is redundant. */
    @Test
    public void newSliceLegLearnedByImplicitAckAndTheLateOutcomeIsRedundant() {
        final IcebergRig k = sliceOneFilled();
        final OrderRig r = k.r;
        final long b = k.seller(PX, ONE);
        final long acks = r.orders.implicitAckCount();
        r.markNow();
        k.sellerTakes(b, S2, PX, T4);
        assertEquals("S5 3.1: ACK_NEW then FILL", List.of(EventKind.ACK_NEW, EventKind.FILL), k.iceKinds());
        assertEquals("implicit ack counted", acks + 1, r.orders.implicitAckCount());
        assertTrue("no dispatch on the implicit ack, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertFalse("the slice create is no longer pending", k.ice().has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals("pendingDispatchSeq cleared", 0L, k.ice().pendingDispatchSeq);
        k.assertSliceState("slice 2 leg learned and filled 0.4", ONE, T6, ONE + T4, ONE + T6, S2, S1, 1);
        final byte[] before = k.f.image();
        final long redundant = r.orders.redundantOutcomeCount();
        final long stale = r.orders.staleOutcomeCount();
        r.markNow();
        k.ackSlice(1, S2);
        assertEquals("the late CREATE outcome emits nothing, got " + r.events(), 0, r.events().size());
        assertArrayEquals("the late CREATE outcome changes no state", before, k.f.image());
        assertEquals("S5 3.8 #4: counted redundantOutcomeCount (APPLIED naming the current leg)", redundant + 1,
                r.orders.redundantOutcomeCount());
        assertEquals("S5 3.8 #4: never both counters", stale, r.orders.staleOutcomeCount());
    }

    /**
     * The implicit-ack trade exhausts slice 2: ack, fill and slice 3 refill in one apply; the late slice-2 outcome now
     * names the OLD leg, so by the S5 3.8 #4 current-leg rule it is stale (not redundant) and leaves slice 3 pending.
     */
    @Test
    public void implicitAckTradeExhaustingTheNewSliceRefillsAndTheLateOutcomeIsStale() {
        final IcebergRig k = sliceOneFilled();
        final OrderRig r = k.r;
        final long b = k.seller(PX, ONE);
        r.markNow();
        k.sellerTakes(b, S2, PX, ONE);
        assertEquals("ACK_NEW, FILL, SLICE_SUBMITTED", List.of(EventKind.ACK_NEW, EventKind.FILL, EventKind.SLICE_SUBMITTED),
                k.iceKinds());
        k.assertRefilled("slice 3 in the implicit-ack apply", 2, ONE);
        k.assertSliceState("after slice 2", 0L, ONE, 2 * ONE, ONE, 0L, S2, 2);
        final long seq = k.ice().pendingDispatchSeq;
        final byte[] before = k.f.image();
        final long redundant = r.orders.redundantOutcomeCount();
        final long stale = r.orders.staleOutcomeCount();
        r.markNow();
        k.ackSlice(1, S2);
        assertEquals("the late slice-2 outcome emits nothing, got " + r.events(), 0, r.events().size());
        assertArrayEquals("no state change", before, k.f.image());
        assertEquals("counted stale (the leg is no longer current)", stale + 1, r.orders.staleOutcomeCount());
        assertEquals("not redundant", redundant, r.orders.redundantOutcomeCount());
        assertTrue("slice 3 create still pending", k.ice().has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals("slice 3 dispatchSeq kept", seq, k.ice().pendingDispatchSeq);
    }

    // ---------------------------------------------------------------- (4) GTD on an iceberg

    /** S5 3.7 + 3.4: GTD expiry with a live slice mints one CANCEL on it; terminal CANCELLED(3) gives EXPIRED, true filled. */
    @Test
    public void gtdIcebergWithALiveSliceMintsOneCancelAndExpiresWithTheTrueFilled() {
        final IcebergRig k = gtd(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        k.sellerTakes(a, S1, PX, T4);
        expire(k);
        final AssetsEngineEvent.MeDispatch cancel = k.onlyIceDispatch("GTD sweep on a live slice");
        assertEquals("one CANCEL", DispatchKind.CANCEL, cancel.kind());
        assertEquals("CANCEL attempt 1 (idLow = 1 << 2 | 1)", (1L << 2) | 1L, cancel.commandIdLow());
        assertEquals("the cancel names the live slice leg", S1, cancel.oldOrderId());
        assertEquals("DISPATCHED event", List.of(EventKind.DISPATCHED), k.iceKinds());
        assertTrue("EXPIRY_REQUESTED", k.ice().has(CoreOrder.F_EXPIRY_REQUESTED));
        assertTrue("CANCEL_PENDING", k.ice().has(CoreOrder.F_CANCEL_PENDING));
        r.markNow();
        k.f.terminal(ICE, ICE_USER, S1, FeedRig.ME_CANCELLED);
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("terminal 3 under EXPIRY_REQUESTED = EXPIRED", EventKind.EXPIRED, end.kind());
        assertEquals(OrderStatus.EXPIRED, end.status());
        assertEquals("true filled 0.4", T4, end.filledQty());
        assertEquals("remaining 2.6", 2 * ONE + T6, end.remainingQty());
        assertEquals("hold released on the terminal event", 0L, end.holdRemaining());
        assertTrue("no refill, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertNull("parent removed", r.orders.orders().get(ICE));
        assertEquals("nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("available = 1000 - 40", USD_DEPOSIT - notional(PX, T4), r.engine.account(ICE_USER).available(USD));
        assertEquals("no GTD deadline left", 0, OutcomeFeed.gtdDeadlines(r));
    }

    /** S5 3.7 + 3.4: GTD expiry while slice 2's create is pending flags only; the slice ACK mints the cancel; APPLIED expires. */
    @Test
    public void gtdIcebergWithASliceCreatePendingMintsTheCancelAtTheSliceAck() {
        final IcebergRig k = gtd(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        k.sellerTakes(a, S1, PX, ONE);
        assertEquals("precondition: slice 2 create pending", CoreOrder.PK_CREATE, k.ice().pendingKind);
        expire(k);
        assertTrue("no cancel while the slice create is pending, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertTrue("EXPIRY_REQUESTED", k.ice().has(CoreOrder.F_EXPIRY_REQUESTED));
        assertTrue("CANCEL_PENDING", k.ice().has(CoreOrder.F_CANCEL_PENDING));
        assertEquals("the slice create stays pending", CoreOrder.PK_CREATE, k.ice().pendingKind);
        r.markNow();
        k.ackSlice(1, S2);
        final AssetsEngineEvent.MeDispatch cancel = k.onlyIceDispatch("slice 2 ACK under expiry");
        assertEquals("CANCEL", DispatchKind.CANCEL, cancel.kind());
        assertEquals("CANCEL attempt 2 (idLow = 2 << 2 | 1)", (2L << 2) | 1L, cancel.commandIdLow());
        assertEquals("the cancel names slice 2's leg", S2, cancel.oldOrderId());
        assertEquals("ACK_NEW then DISPATCHED", List.of(EventKind.ACK_NEW, EventKind.DISPATCHED), k.iceKinds());
        r.markNow();
        OutcomeFeed.outcome(r, ICE, 2, OutcomeFeed.KIND_CANCEL, OutcomeFeed.APPLIED, S2, S2);
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("CANCEL APPLIED under EXPIRY_REQUESTED = EXPIRED", EventKind.EXPIRED, end.kind());
        assertEquals(OrderStatus.EXPIRED, end.status());
        assertEquals("true filled = slice 1", ONE, end.filledQty());
        assertEquals("hold released", 0L, end.holdRemaining());
        assertNull("parent removed", r.orders.orders().get(ICE));
        assertEquals("nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("available = 1000 - 100", USD_DEPOSIT - notional(PX, ONE), r.engine.account(ICE_USER).available(USD));
    }

    /** 3.4: slice exhausted while the expiry cancel is in flight: FILL then EXPIRED in that apply, no refill. */
    @Test
    public void gtdIcebergSliceExhaustedWhileTheExpiryCancelIsInFlightExpiresNow() {
        final IcebergRig k = gtd(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        expire(k);
        assertEquals("precondition: the expiry cancel is in flight", DispatchKind.CANCEL,
                k.onlyIceDispatch("expiry cancel").kind());
        r.markNow();
        k.sellerTakes(a, S1, PX, ONE);
        assertTrue("no refill, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("no SLICE_SUBMITTED", 0, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        assertEquals("FILL then EXPIRED in the trade apply", List.of(EventKind.FILL, EventKind.EXPIRED), k.iceKinds());
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals(OrderStatus.EXPIRED, end.status());
        assertEquals("true filled 1", ONE, end.filledQty());
        assertEquals("remaining 2", 2 * ONE, end.remainingQty());
        assertEquals("hold released", 0L, end.holdRemaining());
        assertNull("parent removed", r.orders.orders().get(ICE));
        assertEquals("nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("available = 1000 - 100", USD_DEPOSIT - notional(PX, ONE), r.engine.account(ICE_USER).available(USD));
        assertEquals("no refill counted", 0L, r.orders.sliceCount());
        final long stale = r.orders.staleOutcomeCount();
        r.markNow();
        OutcomeFeed.outcome(r, ICE, ICE_USER, MKT, 1, OutcomeFeed.KIND_CANCEL, OutcomeFeed.APPLIED, S1, S1, 0);
        assertEquals("the late cancel outcome emits nothing", 0, r.events().size());
        assertEquals("the late cancel outcome is stale", stale + 1, r.orders.staleOutcomeCount());
    }

    // ---------------------------------------------------------------- (6) pending-dispatch replay after a refill

    /** 3.2 + S5 3.6: after two refills the 0.5 slice CREATE replays as LIMIT 0, quantity 0.5, its original dispatchSeq. */
    @Test
    public void pendingDispatchReplayAfterARefillReemitsTheSliceCreateWithItsOriginalSeq() {
        final IcebergRig k = IcebergRig.buy(2 * ONE + HALF, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        final long b = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        k.sellerTakes(a, S1, PX, ONE);
        k.ackSlice(1, S2);
        r.markNow();
        k.sellerTakes(b, S2, PX, ONE);
        final AssetsEngineEvent.MeDispatch slice = k.onlyIceDispatch("slice 3");
        assertEquals("precondition: slice 3 = 0.5", HALF, slice.quantity());
        // a later pending command of another order, so the slice seq is not the head
        r.create(r.limit(SELLER, MKT, IcebergRig.SELL, 101 * ONE, ONE));
        final long other = r.engine.getNextOrderId() - 1;
        assertNotEquals("precondition: the slice seq is not the head", r.engine.getDispatchSeq(), slice.dispatchSeq());
        final byte[] before = k.f.image();
        r.markNow();
        r.orders.applyRequestPendingDispatch(0x51L, 0L, 1024);
        final List<AssetsEngineEvent.MeDispatchReply> replies = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatchReply x) {
                replies.add(x);
            }
        }
        assertEquals("two pending commands re-served, got " + r.events(), 2, replies.size());
        final AssetsEngineEvent.MeDispatchReply rep = replies.get(0);
        assertEquals("ordered by dispatchSeq: the slice first", ICE, rep.omsOrderId());
        assertEquals("the other order second", other, replies.get(1).omsOrderId());
        assertEquals("original dispatchSeq", slice.dispatchSeq(), rep.dispatchSeq());
        assertEquals("CREATE", DispatchKind.CREATE, rep.kind());
        assertEquals("idHigh = orderId", ICE, rep.commandIdHigh());
        assertEquals("idLow = attempt 2 << 2 | CREATE", 2L << 2, rep.commandIdLow());
        assertEquals("wire type LIMIT 0", OrderEngine.TYPE_LIMIT, rep.orderType());
        assertEquals("slice quantity 0.5", HALF, rep.quantity());
        assertEquals("iceberg price", PX, rep.price());
        assertEquals("no budget", 0L, rep.budget());
        assertEquals("no old leg", 0L, rep.oldOrderId());
        assertEquals("byte-identical to the original slice create",
                new AssetsEngineEvent.MeDispatchReply(slice.dispatchSeq(), slice.commandIdHigh(), slice.commandIdLow(), slice.kind(),
                        slice.userId(), slice.omsOrderId(), slice.oldOrderId(), slice.marketId(), slice.orderType(), slice.side(),
                        slice.price(), slice.quantity(), slice.budget(), slice.timeInForce()), rep);
        final AssetsEngineEvent.PendingDispatchEnd end = r.first(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals("end count 2", 2, end.count());
        assertEquals("end head = engine dispatchSeq", r.engine.getDispatchSeq(), end.headDispatchSeq());
        assertArrayEquals("the replay changes no state", before, k.f.image());
        r.markNow();
        r.orders.applyRequestPendingDispatch(0x52L, slice.dispatchSeq(), 1024);
        for (final AssetsEngineEvent e : r.events()) {
            assertFalse("fromDispatchSeq = the slice seq excludes it", e instanceof AssetsEngineEvent.MeDispatchReply x
                    && x.omsOrderId() == ICE);
        }
        assertNotNull("the slice is still pending", r.orders.orders().get(ICE));
    }
}

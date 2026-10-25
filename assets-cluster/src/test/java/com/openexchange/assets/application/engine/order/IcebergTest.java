// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.IcebergRig.ICE;
import static com.openexchange.assets.application.engine.order.IcebergRig.ICE_USER;
import static com.openexchange.assets.application.engine.order.IcebergRig.MKT;
import static com.openexchange.assets.application.engine.order.IcebergRig.PX;
import static com.openexchange.assets.application.engine.order.IcebergRig.S1;
import static com.openexchange.assets.application.engine.order.IcebergRig.S2;
import static com.openexchange.assets.application.engine.order.IcebergRig.S3;
import static com.openexchange.assets.application.engine.order.IcebergRig.USD_DEPOSIT;
import static com.openexchange.assets.application.engine.order.IcebergRig.notional;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 G3, contract 3.4 (spec R10, R11; lifecycle C10, B8): the iceberg is one record with one hold; slices are
 * LIMIT legs minted from it. A slice exhausted by a fill is refilled in the SAME apply (attempt + 1, the old leg
 * remembered, the new leg learned from its outcome); the parent ends on cumulative fills (FILLED, residual released),
 * on a cancel (minted on the live slice, or at the slice ACK), or with the slice when the ME ends it.
 */
public class IcebergTest {

    private static final long HALF = ONE / 2;
    private static final long TENTHS_4 = 40_000_000L; // 0.4
    private static final long TENTHS_6 = 60_000_000L; // 0.6

    /** BUY ICEBERG 100 x 3 display 1, seller A (SELL 100 x 1) takes all of slice 1: slice 2 is pending. */
    private static IcebergRig sliceOneFilled() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        k.sellerTakes(a, S1, PX, ONE);
        assertTrue("precondition (contract 3.4): the iceberg survives slice 1 with slice 2 pending", k.iceLive());
        assertEquals("precondition (contract 3.4): slice 2 is being created (attempt 1)", 1, k.ice().attempt);
        return k;
    }

    /** Contract 3.4 admission: hold price x qty for the whole iceberg, slice 1 = a LIMIT create of displayQty, attempt 0. */
    @Test
    public void c10_admissionHoldsTheWholeQuantityAndDispatchesSliceOneAsALimit() {
        final IcebergRig k = IcebergRig.funded();
        final OrderRig r = k.r;
        r.markNow();
        r.create(k.icebergCmd(3 * ONE, ONE));
        k.assertAccepted(3 * ONE, ONE);
        IcebergRig.assertSliceCreate("contract 3.4 slice 1", k.onlyIceDispatch("admission"), 0, ONE);
        final AssetsEngineEvent.OrderAck ack = OutcomeFeed.lastAck(r);
        assertEquals("contract 1: the iceberg ack is PENDING_NEW", OrderStatus.PENDING_NEW, ack.status());
        assertEquals("contract 1: BUY iceberg hold = price x qty (whole order, not the slice)", notional(PX, 3 * ONE), ack.holdAmount());
        assertEquals("contract 1: BUY locks quote", USD, ack.holdAssetId());
        final AssetsEngineEvent.OrderEvent accepted = OutcomeFeed.event(r, ICE, EventKind.ACCEPTED);
        assertEquals("contract 1: ACCEPTED with status PENDING_NEW", OrderStatus.PENDING_NEW, accepted.status());
        final CoreOrder o = k.ice();
        assertEquals("the record keeps the iceberg type", OrderEngine.TYPE_ICEBERG, o.orderType);
        assertEquals("quantity = total", 3 * ONE, o.quantity);
        assertEquals("displayQty stored", ONE, o.displayQty);
        assertEquals("one hold for the whole iceberg", notional(PX, 3 * ONE), o.holdAmount);
        k.assertSliceState("admission", 2 * ONE, ONE, 0L, 3 * ONE, 0L, 0L, 0);
        assertTrue("slice 1 create is pending", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals("the whole hold is locked", notional(PX, 3 * ONE), r.engine.account(ICE_USER).locked(USD));
        assertEquals("no refill yet: sliceCount counts refills", 0L, r.orders.sliceCount());
    }

    /** Contract 3.4 / C10: a trade that exhausts slice 1 refills slice 2 in the same apply (attempt 1, old leg kept, leg 0). */
    @Test
    public void c10_sliceOneExhaustedRefillsSliceTwoInTheFillApply() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        k.assertSliceState("slice 1 learned", 2 * ONE, ONE, 0L, 3 * ONE, S1, 0L, 0);
        assertEquals("slice 1 ACK moves the parent to NEW", OrderStatus.NEW.code(), k.ice().status);
        r.markNow();
        k.sellerTakes(a, S1, PX, ONE);
        k.assertRefilled("contract 3.4 refill after slice 1", 1, ONE);
        k.assertSliceState("contract 3.4 refill: next = min(display, hidden), hidden -= next, oldLegId = slice 1 leg",
                ONE, ONE, ONE, 2 * ONE, 0L, S1, 1);
        assertEquals("the parent is PARTIALLY_FILLED after slice 1", OrderStatus.PARTIALLY_FILLED.code(), k.ice().status);
        assertEquals("contract 4: sliceCount counts the refill", 1L, r.orders.sliceCount());
        final List<EventKind> kinds = k.iceKinds();
        assertTrue("the fill is reported before the refill: " + kinds,
                kinds.indexOf(EventKind.FILL) >= 0 && kinds.indexOf(EventKind.FILL) < kinds.indexOf(EventKind.SLICE_SUBMITTED));
        final AssetsEngineEvent.OrderEvent slice = OutcomeFeed.event(r, ICE, EventKind.SLICE_SUBMITTED);
        assertEquals("SLICE_SUBMITTED carries the parent status", OrderStatus.PARTIALLY_FILLED, slice.status());
        assertEquals("SLICE_SUBMITTED: no leg known for slice 2 yet", 0L, slice.meOrderId());
        assertEquals("SLICE_SUBMITTED: filled 1", ONE, slice.filledQty());
        assertEquals("SLICE_SUBMITTED: remaining 2", 2 * ONE, slice.remainingQty());
        assertEquals("hold remaining = 300 - 100 spent", notional(PX, 2 * ONE), k.holdRemaining());
        assertEquals("locked follows the hold", notional(PX, 2 * ONE), r.engine.account(ICE_USER).locked(USD));
        k.f.assertLockedEqualsHolds(ICE_USER);
    }

    /** Contract 3.4: the previous slice's journal FILLED terminal (legId == oldLegId) is a counted no-op that changes nothing. */
    @Test
    public void c10_previousSliceFilledTerminalIsACountedNoOp() {
        final IcebergRig k = sliceOneFilled();
        final OrderRig r = k.r;
        assertEquals("precondition: oldLegId names slice 1", S1, k.ice().oldLegId);
        final byte[] before = k.f.image();
        final long stale = r.orders.staleTerminalCount();
        r.markNow();
        k.f.terminal(ICE, ICE_USER, S1, FeedRig.ME_FILLED);
        assertEquals("contract 3.4: the old slice's FILLED terminal emits nothing, got " + r.events(), 0, r.events().size());
        assertArrayEquals("contract 3.4: snapshot bytes identical (no state change)", before, k.f.image());
        assertEquals("contract 3.4: the old slice terminal is counted (staleTerminalCount)", stale + 1, r.orders.staleTerminalCount());
        assertTrue("the parent stays live", k.iceLive());
    }

    /** R10: slice 2 crosses and fills at 99; slice 3 goes out in that fill apply; the last fill makes the parent FILLED, residual released. */
    @Test
    public void r10_sliceTwoCrossesBelowTheLimitAndTheParentFillsWithTheResidualReleased() {
        final IcebergRig k = sliceOneFilled();
        final OrderRig r = k.r;
        final long b = k.seller(99 * ONE, ONE);
        final long c = k.seller(PX, ONE);
        k.ackSlice(1, S2);
        assertEquals("slice 2 leg learned from its CREATE outcome", S2, k.ice().currentLegId);
        r.markNow();
        k.sliceTakes(b, S2, 99 * ONE, ONE);
        k.assertRefilled("R10: slice 3 is dispatched in the apply of slice 2's last trade", 2, ONE);
        k.assertSliceState("R10 after slice 2", 0L, ONE, 2 * ONE, ONE, 0L, S2, 2);
        assertEquals("cumQuote = 100 + 99, exact", notional(PX, ONE) + notional(99 * ONE, ONE), k.ice().cumQuoteQty);
        assertEquals("hold remaining = 300 - 100 - 99", notional(PX, 3 * ONE) - notional(PX, ONE) - notional(99 * ONE, ONE),
                k.holdRemaining());
        assertEquals("contract 4: two refills", 2L, r.orders.sliceCount());
        k.ackSlice(2, S3);
        r.markNow();
        k.sellerTakes(c, S3, PX, ONE);
        assertTrue("R10: the last fill dispatches nothing, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("R10: no slice after the last one", 0, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        final AssetsEngineEvent.OrderEvent last = k.lastIceEvent();
        assertEquals("R10: the parent ends on its FILL event", EventKind.FILL, last.kind());
        assertEquals("R10: the parent is FILLED", OrderStatus.FILLED, last.status());
        assertEquals("R10: filled = 3", 3 * ONE, last.filledQty());
        assertEquals("R10: the terminal event shows no hold left (S5 3.8 #12)", 0L, last.holdRemaining());
        assertNull("R10: the FILLED parent is removed", r.orders.orders().get(ICE));
        assertEquals("R10: nothing stays locked (the 1 USD residual is released)", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("R10: available = 1000 - 299 actually spent", USD_DEPOSIT - notional(PX, 2 * ONE) - notional(99 * ONE, ONE),
                r.engine.account(ICE_USER).available(USD));
        assertEquals("R10: bought 3 BTC", 3 * ONE, r.engine.account(ICE_USER).available(BTC));
        assertEquals("R10: no hold left for the user", 0, r.engine.account(ICE_USER).holdCount());
    }

    /** Contract 3.4 refill arithmetic: qty 2.5, display 1 gives slices 1, 1, 0.5 (the last slice is the hidden rest). */
    @Test
    public void c10_refillArithmeticWhenTheLastSliceIsSmaller() {
        final long qty = 2 * ONE + HALF;
        final IcebergRig k = IcebergRig.buy(qty, ONE);
        final OrderRig r = k.r;
        k.assertSliceState("admission", ONE + HALF, ONE, 0L, qty, 0L, 0L, 0);
        assertEquals("hold = 100 x 2.5", notional(PX, qty), k.ice().holdAmount);
        final long a = k.seller(PX, ONE);
        final long b = k.seller(PX, ONE);
        final long c = k.seller(PX, HALF);
        k.ackSlice(0, S1);
        r.markNow();
        k.sellerTakes(a, S1, PX, ONE);
        k.assertRefilled("slice 2 = min(1, 1.5)", 1, ONE);
        k.assertSliceState("after slice 1", HALF, ONE, ONE, ONE + HALF, 0L, S1, 1);
        k.ackSlice(1, S2);
        r.markNow();
        k.sellerTakes(b, S2, PX, ONE);
        k.assertRefilled("slice 3 = min(1, 0.5) = 0.5", 2, HALF);
        k.assertSliceState("after slice 2", 0L, HALF, 2 * ONE, HALF, 0L, S2, 2);
        k.ackSlice(2, S3);
        r.markNow();
        k.sellerTakes(c, S3, PX, HALF);
        assertTrue("no fourth slice, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("the parent is FILLED on the 0.5 slice", OrderStatus.FILLED, k.lastIceEvent().status());
        assertNull("FILLED parent removed", r.orders.orders().get(ICE));
        assertEquals("nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("available = 1000 - 250", USD_DEPOSIT - notional(PX, qty), r.engine.account(ICE_USER).available(USD));
        assertEquals("two refills", 2L, r.orders.sliceCount());
    }

    /** R11a: cancel while slice 2's create is pending flags only; the slice ACK mints the one CANCEL; its terminal ends the parent. */
    @Test
    public void r11a_cancelWhileASliceCreateIsPendingIsMintedAtTheSliceAck() {
        final IcebergRig k = sliceOneFilled();
        final OrderRig r = k.r;
        r.markNow();
        r.cancel(ICE_USER, ICE);
        assertEquals("R11a: the cancel is acknowledged PENDING_CANCEL", OrderStatus.PENDING_CANCEL, OutcomeFeed.lastAck(r).status());
        assertTrue("R11a: no cancel dispatch while slice 2's create is pending, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertTrue("R11a: CANCEL_PENDING recorded", k.ice().has(CoreOrder.F_CANCEL_PENDING));
        assertEquals("R11a: the slice 2 create stays the pending command", CoreOrder.PK_CREATE, k.ice().pendingKind);
        r.markNow();
        k.ackSlice(1, S2);
        final AssetsEngineEvent.MeDispatch cancel = k.onlyIceDispatch("R11a slice 2 ACK");
        assertEquals("R11a: the ACK mints a CANCEL", DispatchKind.CANCEL, cancel.kind());
        assertEquals("R11a: CANCEL attempt 2 (idLow = 2 << 2 | 1)", (2L << 2) | 1L, cancel.commandIdLow());
        assertEquals("R11a: the cancel names the new slice leg", S2, cancel.oldOrderId());
        assertEquals("R11a: ACK_NEW then DISPATCHED", List.of(EventKind.ACK_NEW, EventKind.DISPATCHED), k.iceKinds());
        assertEquals("R11a: slice 2 leg learned", S2, k.ice().currentLegId);
        r.markNow();
        k.f.terminal(ICE, ICE_USER, S2, FeedRig.ME_CANCELLED);
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("R11a: the parent ends CANCELLED", EventKind.CANCELLED, end.kind());
        assertEquals(OrderStatus.CANCELLED, end.status());
        assertEquals("R11a: true filled quantity = slice 1", ONE, end.filledQty());
        assertEquals("R11a: remaining 2", 2 * ONE, end.remainingQty());
        assertEquals("R11a: hold released on the terminal event", 0L, end.holdRemaining());
        assertTrue("R11a: the terminal dispatches nothing", k.iceDispatches().isEmpty());
        assertNull("R11a: the cancelled parent is removed", r.orders.orders().get(ICE));
        assertEquals("R11a: nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("R11a: available = 1000 - 100 spent", USD_DEPOSIT - notional(PX, ONE), r.engine.account(ICE_USER).available(USD));
        final long stale = r.orders.staleOutcomeCount();
        r.markNow();
        OutcomeFeed.outcome(r, ICE, ICE_USER, MKT, 2, OutcomeFeed.KIND_CANCEL, OutcomeFeed.APPLIED, S2, S2, 0);
        assertEquals("R11a: the late CANCEL outcome emits nothing", 0, r.events().size());
        assertEquals("R11a: the late CANCEL outcome is counted stale", stale + 1, r.orders.staleOutcomeCount());
    }

    /** R11b: a cancel in flight on slice 1 and a trade exhausting slice 1: parent CANCELLED in that apply, no refill. */
    @Test
    public void r11b_cancelInFlightWhenTheSliceExhaustsEndsTheParentCancelledNow() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        r.markNow();
        r.cancel(ICE_USER, ICE);
        final AssetsEngineEvent.MeDispatch cancel = k.onlyIceDispatch("R11b cancel on the live slice");
        assertEquals("R11b: the cancel is minted on the live slice leg", DispatchKind.CANCEL, cancel.kind());
        assertEquals("R11b: CANCEL attempt 1 (idLow = 1 << 2 | 1)", (1L << 2) | 1L, cancel.commandIdLow());
        assertEquals(S1, cancel.oldOrderId());
        r.markNow();
        k.sellerTakes(a, S1, PX, ONE);
        assertTrue("R11b: no refill and no new dispatch, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("R11b: no SLICE_SUBMITTED", 0, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        assertEquals("R11b: FILL then CANCELLED in the trade apply", List.of(EventKind.FILL, EventKind.CANCELLED), k.iceKinds());
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("R11b: parent CANCELLED", OrderStatus.CANCELLED, end.status());
        assertEquals("R11b: filled 1", ONE, end.filledQty());
        assertEquals("R11b: remaining 2", 2 * ONE, end.remainingQty());
        assertEquals("R11b: hold residual released", 0L, end.holdRemaining());
        assertNull("R11b: parent removed", r.orders.orders().get(ICE));
        assertEquals("R11b: nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("R11b: available = 1000 - 100", USD_DEPOSIT - notional(PX, ONE), r.engine.account(ICE_USER).available(USD));
        assertEquals("R11b: bought 1 BTC", ONE, r.engine.account(ICE_USER).available(BTC));
        assertEquals("R11b: no refill counted", 0L, r.orders.sliceCount());
        final long stale = r.orders.staleOutcomeCount();
        r.markNow();
        OutcomeFeed.outcome(r, ICE, ICE_USER, MKT, 1, OutcomeFeed.KIND_CANCEL, OutcomeFeed.APPLIED, S1, S1, 0);
        assertEquals("R11b: the late CANCEL outcome emits nothing", 0, r.events().size());
        assertEquals("R11b: the late CANCEL outcome is counted stale", stale + 1, r.orders.staleOutcomeCount());
    }

    /** Contract 3.4 / D8: the ME cancels the current slice (no cancel asked): the parent ends CANCELLED with the true filled. */
    @Test
    public void d8_sliceCancelledByTheMeEndsTheParentCancelledWithTheTrueFilled() {
        final IcebergRig k = sliceOneFilled();
        final OrderRig r = k.r;
        final long b = k.seller(PX, ONE);
        k.ackSlice(1, S2);
        k.sellerTakes(b, S2, PX, TENTHS_4);
        k.assertSliceState("slice 2 partly filled", ONE, TENTHS_6, ONE + TENTHS_4, ONE + TENTHS_6, S2, S1, 1);
        assertFalse("precondition: no cancel requested", k.ice().has(CoreOrder.F_CANCEL_PENDING));
        r.markNow();
        k.f.terminal(ICE, ICE_USER, S2, FeedRig.ME_CANCELLED);
        assertTrue("D8: no refill after an ME cancel, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        assertEquals("D8: no SLICE_SUBMITTED", 0, OutcomeFeed.kindCount(r, ICE, EventKind.SLICE_SUBMITTED));
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("D8: the parent ends CANCELLED", EventKind.CANCELLED, end.kind());
        assertEquals(OrderStatus.CANCELLED, end.status());
        assertEquals("D8: true filled = 1 + 0.4", ONE + TENTHS_4, end.filledQty());
        assertEquals("D8: remaining = 1.6", ONE + TENTHS_6, end.remainingQty());
        assertEquals("D8: hold released", 0L, end.holdRemaining());
        assertNull("D8: parent removed", r.orders.orders().get(ICE));
        assertEquals("D8: nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("D8: available = 1000 - 140", USD_DEPOSIT - notional(PX, ONE + TENTHS_4), r.engine.account(ICE_USER).available(USD));
    }

    /** Contract 3.4 / D8: slice 2's CREATE outcome REJECTED ends the parent REJECTED (ME_REJECTED) with the true filled. */
    @Test
    public void d8_sliceRejectedByItsCreateOutcomeEndsTheParentRejected() {
        final IcebergRig k = sliceOneFilled();
        final OrderRig r = k.r;
        r.markNow();
        OutcomeFeed.outcome(r, ICE, 1, OutcomeFeed.KIND_CREATE, OutcomeFeed.REJECTED, 0L, 0L);
        assertTrue("D8: no new slice after a refusal, got " + k.iceDispatches(), k.iceDispatches().isEmpty());
        final AssetsEngineEvent.OrderEvent end = k.lastIceEvent();
        assertEquals("D8: the parent ends REJECTED", EventKind.REJECTED, end.kind());
        assertEquals(OrderStatus.REJECTED, end.status());
        assertEquals("D8: reason ME_REJECTED", CoreRejectReason.ME_REJECTED, end.reason());
        assertEquals("D8: true filled = slice 1", ONE, end.filledQty());
        assertEquals("D8: hold released", 0L, end.holdRemaining());
        assertNull("D8: parent removed", r.orders.orders().get(ICE));
        assertEquals("D8: nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
        assertEquals("D8: available = 1000 - 100", USD_DEPOSIT - notional(PX, ONE), r.engine.account(ICE_USER).available(USD));
    }

    /** Contract 3.4: current-slice journal FILLED with sliceRemaining 0.6: ORDERFAULT SETTLE 0.6, back to hidden, refill. */
    @Test
    public void c10_currentSliceFilledTerminalWithSliceRemainingFaultsAndRefills() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        k.sellerTakes(a, S1, PX, TENTHS_4);
        k.assertSliceState("slice 1 partly filled", 2 * ONE, TENTHS_6, TENTHS_4, 2 * ONE + TENTHS_6, S1, 0L, 0);
        final long faults = r.orders.orderFaultCount();
        r.markNow();
        k.f.terminal(ICE, ICE_USER, S1, FeedRig.ME_FILLED);
        assertEquals("contract 3.4: one ORDERFAULT", 1, r.count(AssetsEngineEvent.OrderFault.class));
        final AssetsEngineEvent.OrderFault fault = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals("contract 3.4: ORDERFAULT kind SETTLE", OrderFaultKind.SETTLE, fault.kind());
        assertEquals("contract 3.4: the fault names the iceberg", ICE, fault.orderId());
        assertEquals("contract 3.4: amount = sliceRemaining 0.6", TENTHS_6, fault.amount());
        assertEquals("the fault is counted", faults + 1, r.orders.orderFaultCount());
        k.assertRefilled("contract 3.4: the slice is treated as exhausted and refilled", 1, ONE);
        k.assertSliceState("contract 3.4: 0.6 back to hidden (2.6), then next = 1", ONE + TENTHS_6, ONE, TENTHS_4,
                2 * ONE + TENTHS_6, 0L, S1, 1);
        assertTrue("contract 3.4: the parent is not terminal", k.iceLive());
        assertEquals("contract 3.4: the parent stays PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED.code(), k.ice().status);
        for (final EventKind kind : k.iceKinds()) {
            assertTrue("contract 3.4: no terminal event for the parent, got " + k.iceKinds(),
                    kind != EventKind.CANCELLED && kind != EventKind.REJECTED && kind != EventKind.EXPIRED && kind != EventKind.FILL);
        }
        assertEquals("hold remaining = 300 - 40", notional(PX, 3 * ONE) - notional(PX, TENTHS_4), k.holdRemaining());
        assertEquals("contract 4: one refill", 1L, r.orders.sliceCount());
        k.f.assertLockedEqualsHolds(ICE_USER);
    }

    /** Contract 3.2: the refilled slice create is a function of the record, so RequestPendingDispatch re-emits it identically. */
    @Test
    public void c11_pendingDispatchReplaysTheRefilledSliceIdentically() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        final long a = k.seller(PX, ONE);
        k.ackSlice(0, S1);
        r.markNow();
        k.sellerTakes(a, S1, PX, ONE);
        final AssetsEngineEvent.MeDispatch d = k.onlyIceDispatch("refill");
        r.markNow();
        r.orders.applyRequestPendingDispatch(4_242L, 0L, 0);
        AssetsEngineEvent.MeDispatchReply reply = null;
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatchReply x && x.omsOrderId() == ICE) {
                reply = x;
            }
        }
        assertTrue("contract 3.2: the pending slice create is re-served, got " + r.events(), reply != null);
        assertEquals("contract 3.2: the replay is byte-identical to the slice create",
                new AssetsEngineEvent.MeDispatchReply(d.dispatchSeq(), d.commandIdHigh(), d.commandIdLow(), d.kind(), d.userId(),
                        d.omsOrderId(), d.oldOrderId(), d.marketId(), d.orderType(), d.side(), d.price(), d.quantity(), d.budget(),
                        d.timeInForce()), reply);
    }

    /** Contract 3.3 / S5: AmendOrder on an iceberg is NOT_OPEN, nothing changes. */
    @Test
    public void c10_amendOfAnIcebergIsNotOpen() {
        final IcebergRig k = IcebergRig.buy(3 * ONE, ONE);
        final OrderRig r = k.r;
        k.ackSlice(0, S1);
        final byte[] before = k.f.image();
        r.markNow();
        new AmendSupport(r).amend(ICE_USER, ICE, 101 * ONE, 0L);
        final CoreRejectReason why = new AmendSupport(r).rejectOnly("amend of an iceberg");
        assertEquals("contract 3.3: amend of a synthetic type is NOT_OPEN", CoreRejectReason.NOT_OPEN, why);
        assertArrayEquals("contract 3.3: nothing changes", before, k.f.image());
    }

    /** Contract 1: an iceberg cannot be IOC (UNSUPPORTED_TIF), no state created. */
    @Test
    public void c10_icebergCannotBeIoc() {
        final IcebergRig k = IcebergRig.funded();
        final OrderRig r = k.r;
        final CreateOrderCommand c = k.icebergCmd(3 * ONE, ONE);
        c.timeInForce = OrderEngine.TIF_IOC;
        final long nextId = r.engine.getNextOrderId();
        r.markNow();
        r.create(c);
        final AssetsEngineEvent.OrderReject rej = r.reject();
        assertEquals("contract 1: IOC on a synthetic type is UNSUPPORTED_TIF", CoreRejectReason.UNSUPPORTED_TIF, rej.reason());
        assertEquals("the reject names CreateOrder", 50, rej.refTemplateId());
        assertEquals("no record", 0, r.orders.orders().size());
        assertEquals("no orderId consumed", nextId, r.engine.getNextOrderId());
        assertEquals("nothing locked", 0L, r.engine.account(ICE_USER).locked(USD));
    }
}

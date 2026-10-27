// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.BUY;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_CREATE;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.LEG_A;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.LEG_B;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ME_CANCELLED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.REJECTED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.SELL;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ack;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.event;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.gtdDeadlines;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.live;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.outcome;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.terminal;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.trade;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 group G2 (contract 3.6 second bullet, 3.5; lifecycle C14, C16): the forwarder resync replays exactly the
 * commands still in flight, in mint order and frame-for-frame; a quarantine freezes the core's side of an order
 * without touching money truth.
 */
public class PendingDispatchAndQuarantineTest {

    /** Orders 1..4 as BUY LIMIT (100+i) x (i+1), dispatch seqs 1..4, all creates unresolved. */
    private static OrderRig fourCreates() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 10_000 * ONE);
        for (int i = 0; i < 4; i++) {
            r.create(r.limit(1, 1, BUY, (100 + i) * ONE, (i + 1) * ONE));
        }
        assertEquals(4, r.engine.getDispatchSeq());
        return r;
    }

    /** {@link #fourCreates()} then order 1 acked and cancelled: pending = creates 2, 3, 4 (seqs 2..4) + cancel (seq 5). */
    private static OrderRig threeCreatesAndACancel() {
        final OrderRig r = fourCreates();
        ack(r, 1, LEG_A);
        assertEquals("precondition: the CREATE outcome learned the leg (3.3)", LEG_A, live(r, 1).currentLegId);
        r.cancel(1, 1);
        assertEquals("precondition: the cancel of the resting leg was minted as dispatch 5", 5, r.engine.getDispatchSeq());
        return r;
    }

    private static List<AssetsEngineEvent.MeDispatchReply> replies(final OrderRig r) {
        final List<AssetsEngineEvent.MeDispatchReply> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatchReply d) {
                out.add(d);
            }
        }
        return out;
    }

    private static List<Long> seqs(final OrderRig r) {
        final List<Long> out = new ArrayList<>();
        for (final AssetsEngineEvent.MeDispatchReply d : replies(r)) {
            out.add(d.dispatchSeq());
        }
        return out;
    }

    /** Every MEDISPATCH frame ever minted on this rig, rendered as the DISPATCHREPLY it must be replayed as. */
    private static Map<Long, String> mintedFrames(final OrderRig r) {
        final Map<Long, String> out = new HashMap<>();
        for (final AssetsEngineEvent e : r.sink.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatch d) {
                out.put(d.dispatchSeq(), d.render().replace("MEDISPATCH", "DISPATCHREPLY"));
            }
        }
        return out;
    }

    @Test
    public void pendingDispatchReplaysInFlightCommandsInMintOrderWithTheOriginalFramesAndAnEndFrame() {
        // contract 3.6 RequestPendingDispatch (65): ordered by pendingDispatchSeq, original payload, then PENDINGEND
        final OrderRig r = threeCreatesAndACancel();
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xC0, 0L, 1024);
        assertEquals("4 DISPATCHREPLY frames + PENDINGEND, got " + r.events(), 5, r.events().size());
        final List<AssetsEngineEvent.MeDispatchReply> p = replies(r);
        assertEquals("ordered by dispatchSeq", List.of(2L, 3L, 4L, 5L), seqs(r));
        final Map<Long, String> minted = mintedFrames(r);
        for (final AssetsEngineEvent.MeDispatchReply d : p) {
            assertEquals("the reply is the original frame, field for field", minted.get(d.dispatchSeq()), d.render());
        }
        for (int i = 0; i < 3; i++) {
            final AssetsEngineEvent.MeDispatchReply d = p.get(i);
            final long orderId = i + 2;
            assertEquals(DispatchKind.CREATE, d.kind());
            assertEquals(orderId, d.commandIdHigh());
            assertEquals("attempt 0 CREATE", 0, d.commandIdLow());
            assertEquals(orderId, d.omsOrderId());
            assertEquals(1, d.userId());
            assertEquals(1, d.marketId());
            assertEquals(BUY, d.side());
            assertEquals(OrderEngine.TYPE_LIMIT, d.orderType());
            assertEquals("original price", (100 + i + 1) * ONE, d.price());
            assertEquals("original quantity", (i + 2) * ONE, d.quantity());
            assertEquals(0, d.budget());
            assertEquals(0, d.oldOrderId());
            assertEquals(OrderEngine.TIF_GTC, d.timeInForce());
        }
        final AssetsEngineEvent.MeDispatchReply c = p.get(3);
        assertEquals(DispatchKind.CANCEL, c.kind());
        assertEquals(1, c.commandIdHigh());
        assertEquals((1L << 2) | 1, c.commandIdLow());
        assertEquals("cancel: oldOrderId = currentLegId", LEG_A, c.oldOrderId());
        assertEquals(0, c.price());
        assertEquals(0, c.quantity());
        final AssetsEngineEvent.PendingDispatchEnd end = r.first(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals(0xC0, end.correlationId());
        assertEquals(4, end.count());
        assertEquals("head = the engine's dispatchSeq", r.engine.getDispatchSeq(), end.headDispatchSeq());
        assertEquals(5, end.headDispatchSeq());
        assertTrue("PENDINGEND is the last frame", r.events().get(r.events().size() - 1) instanceof AssetsEngineEvent.PendingDispatchEnd);
        assertEquals("a query mutates nothing", 5, r.engine.getDispatchSeq());
        assertEquals(4, r.orders.orders().size());
    }

    @Test
    public void pendingDispatchCursorSkipsSeqsAtOrBelowFromDispatchSeq() {
        // contract 3.6: pending = DISPATCH_PENDING and pendingDispatchSeq > fromDispatchSeq
        final OrderRig r = fourCreates();
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xC1, 2L, 1024);
        assertEquals("seqs 3 and 4 + PENDINGEND, got " + r.events(), 3, r.events().size());
        assertEquals(List.of(3L, 4L), seqs(r));
        final AssetsEngineEvent.PendingDispatchEnd end = r.first(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals(2, end.count());
        assertEquals("head is the engine's seq, not the page's last", 4, end.headDispatchSeq());
        assertEquals(0xC1, end.correlationId());
        // a cursor at the head: an empty page, still an end frame
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xC2, 4L, 1024);
        final AssetsEngineEvent.PendingDispatchEnd empty = r.only(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals(0, empty.count());
        assertEquals(4, empty.headDispatchSeq());
    }

    @Test
    public void pendingDispatchMaxEntriesCapsThePageAndZeroMeansTheDefault() {
        // contract 3.6: maxEntries 0 is the 1024 default (cap 4096); a page continues from the last seq seen
        final OrderRig r = fourCreates();
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xC3, 0L, 2);
        assertEquals("a page of 2 + PENDINGEND, got " + r.events(), 3, r.events().size());
        assertEquals(List.of(1L, 2L), seqs(r));
        assertEquals(2, r.first(AssetsEngineEvent.PendingDispatchEnd.class).count());
        assertEquals("head is the engine's seq even when the page is capped", 4,
                r.first(AssetsEngineEvent.PendingDispatchEnd.class).headDispatchSeq());
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xC4, 2L, 2);
        assertEquals("the next page continues from the last seq seen", List.of(3L, 4L), seqs(r));
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xC5, 0L, 1024);
        assertEquals("maxEntries 0 is the default page (1024), not an empty page", List.of(1L, 2L, 3L, 4L), seqs(r));
        assertEquals(4, r.first(AssetsEngineEvent.PendingDispatchEnd.class).count());
    }

    @Test
    public void pendingDispatchSkipsResolvedCommands() {
        // contract 3.6: only live orders with DISPATCH_PENDING are replayed
        final OrderRig r = fourCreates();
        ack(r, 2, LEG_B);
        assertEquals("precondition: order 2 learned its leg (3.3)", LEG_B, live(r, 2).currentLegId);
        outcome(r, 3, 0, KIND_CREATE, REJECTED, 0L, 0L);
        assertNull("precondition: a refused create left the store (3.3)", r.orders.orders().get(3));
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xC6, 0L, 1024);
        assertEquals("only the unresolved creates 1 and 4 are replayed", List.of(1L, 4L), seqs(r));
        final AssetsEngineEvent.PendingDispatchEnd end = r.first(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals(2, end.count());
        assertEquals(4, end.headDispatchSeq());
        assertEquals(3, r.events().size());
    }

    @Test
    public void quarantine_unknownOrderIsRejected() {
        // contract 3.5: Unknown -> AdminAck REJECTED(UNKNOWN_ORDER)
        final OrderRig r = OrderRig.open();
        r.markNow();
        r.orders.applyQuarantine(0x51, 42L, r.now);
        assertEquals("one ADMINACK REJECTED and nothing else, got " + r.events(), 1, r.events().size());
        final AssetsEngineEvent.AdminAck a = r.only(AssetsEngineEvent.AdminAck.class);
        assertEquals(AdminResult.REJECTED, a.result());
        assertEquals(CoreRejectReason.UNKNOWN_ORDER.code(), a.reason());
        assertEquals("QuarantineOrder is template 74", 74, a.refTemplateId());
        assertEquals(0x51, a.correlationId());
    }

    @Test
    public void quarantine_firstCallIsAcceptedWithAnEventAndTheSecondIsADuplicate() {
        // contract 3.5: set QUARANTINED, OrderEvent{QUARANTINED}, AdminAck ACCEPTED; again -> DUPLICATE
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, BUY, 100 * ONE, ONE));
        r.markNow();
        r.orders.applyQuarantine(0x52, 1L, r.now);
        assertEquals("ADMINACK ACCEPTED + ORDEREVENT QUARANTINED, got " + r.events(), 2, r.events().size());
        final AssetsEngineEvent.AdminAck a = r.first(AssetsEngineEvent.AdminAck.class);
        assertEquals(AdminResult.ACCEPTED, a.result());
        assertEquals(74, a.refTemplateId());
        assertEquals(0x52, a.correlationId());
        final AssetsEngineEvent.OrderEvent ev = event(r, 1, EventKind.QUARANTINED);
        assertEquals("quarantine is a flag, never a status", OrderStatus.PENDING_NEW, ev.status());
        final CoreOrder o = live(r, 1);
        assertTrue(o.has(CoreOrder.F_QUARANTINED));
        assertTrue("the pending create is left as it is", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals("the hold is untouched by the quarantine itself", 100 * ONE, r.engine.account(1).locked(USD));
        assertEquals(1, r.orders.orders().openCount(1));
        r.markNow();
        r.orders.applyQuarantine(0x53, 1L, r.now);
        final AssetsEngineEvent.AdminAck dup = r.only(AssetsEngineEvent.AdminAck.class);
        assertEquals(AdminResult.DUPLICATE, dup.result());
        assertEquals(0x53, dup.correlationId());
        assertTrue(live(r, 1).has(CoreOrder.F_QUARANTINED));
    }

    @Test
    public void quarantine_cancelAndAmendAreRefusedNotOpenAndMintNothing() {
        // contract 3.5 effects: CancelOrder and AmendOrder -> NOT_OPEN; no further ME command
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, BUY, 100 * ONE, ONE));
        ack(r, 1, LEG_A);
        r.orders.applyQuarantine(0x54, 1L, r.now);
        assertTrue("precondition: quarantined (3.5)", live(r, 1).has(CoreOrder.F_QUARANTINED));
        r.markNow();
        r.cancel(1, 1);
        final AssetsEngineEvent.OrderReject cancel = r.reject();
        assertEquals(CoreRejectReason.NOT_OPEN, cancel.reason());
        assertEquals(51, cancel.refTemplateId());
        assertFalse("not even the flag", live(r, 1).has(CoreOrder.F_CANCEL_PENDING));
        final AmendOrderCommand amend = new AmendOrderCommand();
        amend.reset();
        amend.correlationId = 0x55;
        amend.userId = 1;
        amend.orderId = 1;
        amend.requestIdHigh = 0xABCDL;
        amend.requestIdLow = 0x5555L;
        amend.newPrice = 101 * ONE;
        amend.newQuantity = 0;
        r.markNow();
        r.orders.applyAmend(amend, r.now);
        final AssetsEngineEvent.OrderReject rej = r.reject();
        assertEquals(CoreRejectReason.NOT_OPEN, rej.reason());
        assertEquals(52, rej.refTemplateId());
        final CoreOrder o = live(r, 1);
        assertFalse(o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(100 * ONE, o.price);
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertEquals("no top-up", 100 * ONE, r.engine.account(1).locked(USD));
        assertEquals("no ME command minted for a quarantined order", 1, r.engine.getDispatchSeq());
    }

    @Test
    public void quarantine_gtdExpirySetsTheFlagsWithoutMinting() {
        // contract 3.7: QUARANTINED -> flags only, no mint
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, BUY, 100 * ONE, ONE);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = r.now + 1_000;
        r.create(c);
        ack(r, 1, LEG_A);
        assertEquals("precondition: the leg rests (3.3)", LEG_A, live(r, 1).currentLegId);
        r.orders.applyQuarantine(0x56, 1L, r.now);
        assertTrue("precondition: quarantined (3.5)", live(r, 1).has(CoreOrder.F_QUARANTINED));
        r.advance(1_000);
        r.markNow();
        r.sweep();
        final CoreOrder o = live(r, 1);
        assertTrue(o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertTrue(o.has(CoreOrder.F_CANCEL_PENDING));
        assertEquals("no cancel minted for a quarantined order", 0, r.count(AssetsEngineEvent.MeDispatch.class));
        assertFalse(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(1, r.engine.getDispatchSeq());
        assertEquals(0, o.attempt);
        assertEquals("the GTD entry popped once", 0, gtdDeadlines(r));
        assertEquals("the hold stays: money truth comes from the ME", 100 * ONE, r.engine.account(1).locked(USD));
        r.markNow();
        r.sweep();
        assertEquals("a second sweep applies nothing", 0, r.events().size());
        // the ME's own terminal still applies and, with EXPIRY_REQUESTED, surfaces as EXPIRED
        r.markNow();
        terminal(r, 1, LEG_A, ME_CANCELLED);
        assertEquals(OrderStatus.EXPIRED, event(r, 1, EventKind.EXPIRED).status());
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.engine.account(1).locked(USD));
    }

    @Test
    public void quarantine_tradesAndTerminalsFromTheMeStillSettleAndCount() {
        // contract 3.5 effects and assumption 8: money truth is the ME's; the quarantine only stops new ME commands
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 5 * ONE);
        r.create(r.limit(1, 1, BUY, 100 * ONE, 2 * ONE)); // order 1, hold 200 USD
        r.create(r.limit(2, 1, SELL, 100 * ONE, ONE));    // order 2, hold 1 BTC
        ack(r, 1, LEG_A);
        ack(r, 2, LEG_B);
        assertEquals("precondition: the legs rest (3.3)", LEG_A, live(r, 1).currentLegId);
        r.orders.applyQuarantine(0x57, 1L, r.now);
        assertTrue("precondition: quarantined (3.5)", live(r, 1).has(CoreOrder.F_QUARANTINED));
        r.markNow();
        trade(r, 1, 1, 2, LEG_B, 2, 1, LEG_A, 1, 100 * ONE, ONE, false); // taker = order 2 SELL, maker = order 1 BUY
        assertEquals("the trade settles", 1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(1, r.engine.getLastAppliedTradeId());
        assertEquals(OrderStatus.PARTIALLY_FILLED, event(r, 1, EventKind.FILL).status());
        final CoreOrder o = live(r, 1);
        assertEquals("the fill counts on the quarantined record", ONE, o.filledQty);
        assertEquals(ONE, o.remainingQty);
        assertTrue("still quarantined", o.has(CoreOrder.F_QUARANTINED));
        assertEquals(100 * ONE, r.engine.account(1).locked(USD));
        assertEquals(800 * ONE, r.engine.account(1).available(USD));
        assertEquals(ONE, r.engine.account(1).available(BTC));
        assertEquals(100 * ONE, r.engine.account(2).available(USD));
        assertEquals(4 * ONE, r.engine.account(2).available(BTC));
        assertEquals(ONE, r.orders.user(1).position[1]);
        assertEquals(0, r.orders.orderFaultCount());
        r.markNow();
        terminal(r, 1, LEG_A, ME_CANCELLED);
        assertEquals(OrderStatus.CANCELLED, event(r, 1, EventKind.CANCELLED).status());
        assertNull(r.orders.orders().get(1));
        assertEquals("the residual hold is released by the terminal", 0, r.engine.account(1).locked(USD));
        assertEquals(900 * ONE, r.engine.account(1).available(USD));
        assertEquals("no ME command was ever minted after the quarantine", 2, r.engine.getDispatchSeq());
    }
}

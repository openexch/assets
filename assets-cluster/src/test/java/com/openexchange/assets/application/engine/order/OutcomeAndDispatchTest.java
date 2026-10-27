// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
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
import static com.openexchange.assets.application.engine.order.OutcomeFeed.APPLIED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.BUY;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.CAPACITY;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.CONFLICT;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_CANCEL;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_CREATE;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.LEG_A;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.LEG_B;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ME_CANCELLED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.NOT_OWNER;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.REJECTED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.SELL;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.UNKNOWN_LEG;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.UNKNOWN_MARKET;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.ack;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.event;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.gtdDeadlines;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.kindCount;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.lastAck;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.live;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.outcome;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.terminal;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.trade;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 group G2 (contract 3.3 outcomes, 3.7 GTD; lifecycle C4, C8b, C9, C13, C15): the durable ME outcome is
 * the only thing that resolves a pending command; a cancel is minted exactly once, at ACK when the create was
 * still unresolved; GTD and IOC ride that same single cancel; a refusal terminalizes, counts or closes the gate,
 * never retries blindly.
 */
public class OutcomeAndDispatchTest {

    /** User 1 with 1000 USD and one BUY LIMIT 100 x 2 on market 1 (order 1, hold 200 USD, create dispatch 1). */
    private static OrderRig buyer() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, BUY, 100 * ONE, 2 * ONE));
        return r;
    }

    /** Like {@link #buyer()} but GTD with the given lifetime, and the leg already learned from the create outcome. */
    private static OrderRig gtdBuyerWithLeg(final long ttlMs) {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, BUY, 100 * ONE, 2 * ONE);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = r.now + ttlMs;
        r.create(c);
        ack(r, 1, LEG_A);
        assertEquals("precondition: the CREATE outcome learned the leg (3.3)", LEG_A, live(r, 1).currentLegId);
        return r;
    }

    @Test
    public void rC4a_createOutcomeAppliedLearnsTheLegAndAcksNew() {
        // contract 3.3, result 0 APPLIED on a CREATE
        final OrderRig r = buyer();
        r.markNow();
        ack(r, 1, LEG_A);
        assertEquals("CREATE outcome APPLIED emits exactly one event, the ACK_NEW OrderEvent, got " + r.events(), 1, r.events().size());
        final AssetsEngineEvent.OrderEvent ev = r.only(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.ACK_NEW, ev.kind());
        assertEquals(OrderStatus.NEW, ev.status());
        assertEquals(CoreRejectReason.NONE, ev.reason());
        assertEquals("the event carries the learned leg", LEG_A, ev.meOrderId());
        assertEquals(2 * ONE, ev.remainingQty());
        assertEquals("second event of this order (ACCEPTED was 1)", 2, ev.eventSeq());
        assertEquals(2, r.engine.getEventSeq());
        final CoreOrder o = live(r, 1);
        assertEquals(LEG_A, o.currentLegId);
        assertFalse("DISPATCH_PENDING cleared", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals("pendingDispatchSeq cleared", 0, o.pendingDispatchSeq);
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertEquals("attempt stays 0 until the next mint", 0, o.attempt);
        assertEquals("the ack moves no money", 200 * ONE, r.engine.account(1).locked(USD));
        assertEquals(1, r.engine.getDispatchSeq());
        assertEquals(0, r.orders.staleOutcomeCount());
    }

    @Test
    public void everyMintRecordsItsDispatchSeqAndTheResolvingOutcomeClearsIt() {
        // contract 1 (pendingDispatchSeq) as 3.3 resolves it
        final OrderRig r = buyer();
        final CoreOrder o = live(r, 1);
        assertEquals("the create mint stamps dispatchSeq 1 on the record", 1, o.pendingDispatchSeq);
        assertEquals(r.engine.getDispatchSeq(), o.pendingDispatchSeq);
        ack(r, 1, LEG_A);
        assertEquals("resolved: no pending ME command", 0, o.pendingDispatchSeq);
        r.cancel(1, 1);
        assertEquals("the cancel mint stamps dispatchSeq 2", 2, o.pendingDispatchSeq);
        assertEquals(2, r.engine.getDispatchSeq());
        outcome(r, 1, 1, KIND_CANCEL, APPLIED, LEG_A, LEG_A);
        assertNull("terminal: the record is gone with its pending dispatch", r.orders.orders().get(1));
    }

    @Test
    public void rC4b_staleAttemptOrKindIsIgnoredAndCounted() {
        // contract 3.3 match rule: commandIdLow must equal attempt << 2 | kind of the pending command
        final OrderRig r = buyer();
        r.markNow();
        outcome(r, 1, 1, KIND_CREATE, APPLIED, LEG_A, 0L);            // attempt 1 was never minted
        outcome(r, 1, 0, KIND_CANCEL, APPLIED, LEG_A, LEG_A);         // attempt 0 is a CREATE, not a CANCEL
        outcome(r, 9, 1, 1, 0, KIND_CREATE, APPLIED, LEG_B, 0L, 0);   // no such order
        assertEquals("three stale outcomes, three counted no-ops", 3, r.orders.staleOutcomeCount());
        assertEquals("a stale outcome emits nothing, got " + r.events(), 0, r.events().size());
        final CoreOrder o = live(r, 1);
        assertEquals("no leg learned from a stale outcome", 0, o.currentLegId);
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(OrderStatus.PENDING_NEW.code(), o.status);
        assertEquals(0, o.attempt);
        assertEquals("the create is still the pending command", 1, o.pendingDispatchSeq);
        assertEquals(200 * ONE, r.engine.account(1).locked(USD));
        assertEquals(0, r.orders.cancelRefusedCount());
    }

    @Test
    public void rC4b_redeliveryAfterResolutionIsCountedNotReapplied() {
        // contract 3.3: "anything else is a counted no-op, including a re-delivery after resolution"
        final OrderRig r = buyer();
        ack(r, 1, LEG_A);
        r.markNow();
        ack(r, 1, LEG_A); // the bridge replays the same outcome
        assertEquals("a second delivery of a resolved APPLIED outcome naming the current leg is counted as redundant (contract 3.8 #4)",
                1, r.orders.redundantOutcomeCount());
        assertEquals(0, r.orders.staleOutcomeCount());
        assertEquals("and emits nothing, got " + r.events(), 0, r.events().size());
        r.cancel(1, 1); // attempt 1, kind CANCEL is now the pending command
        r.markNow();
        ack(r, 1, LEG_A); // attempt 0 CREATE again, after attempt 1 was minted (R-C4b shape): still the current leg, APPLIED
        assertEquals(2, r.orders.redundantOutcomeCount());
        assertEquals(0, r.orders.staleOutcomeCount());
        assertEquals(0, r.events().size());
        final CoreOrder o = live(r, 1);
        assertEquals("the pending cancel is untouched", 1, o.attempt);
        assertEquals(CoreOrder.PK_CANCEL, o.pendingKind);
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertTrue(o.has(CoreOrder.F_CANCEL_PENDING));
        assertEquals(LEG_A, o.currentLegId);
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertEquals(2, r.engine.getDispatchSeq());
    }

    @Test
    public void rC8b_cancelWhileCreateUnresolvedIsMintedExactlyOnceAtAck() {
        // contract 3.3 APPLIED/CREATE with CANCEL_PENDING, then APPLIED/CANCEL; lifecycle C8b
        final OrderRig r = buyer();
        r.cancel(1, 1);
        final CoreOrder o = live(r, 1);
        assertTrue("S4: a cancel on an unresolved create is a flag", o.has(CoreOrder.F_CANCEL_PENDING));
        assertEquals(1, r.engine.getDispatchSeq());
        r.markNow();
        ack(r, 1, LEG_A);
        assertEquals("at ACK: ACK_NEW, one CANCEL dispatch, DISPATCHED (3 events), got " + r.events(), 3, r.events().size());
        assertTrue("the first event is the ACK_NEW OrderEvent, got " + r.events().get(0),
                r.events().get(0) instanceof AssetsEngineEvent.OrderEvent first && first.kind() == EventKind.ACK_NEW);
        assertEquals("exactly one ME command minted at ACK", 1, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d.kind());
        assertEquals(1, d.commandIdHigh());
        assertEquals("attempt 1, kind CANCEL", (1L << 2) | 1, d.commandIdLow());
        assertEquals("oldOrderId names the leg to cancel", LEG_A, d.oldOrderId());
        assertEquals(2, d.dispatchSeq());
        assertEquals(1, d.omsOrderId());
        assertEquals("the mint is announced on the ORDERS channel", LEG_A, event(r, 1, EventKind.DISPATCHED).meOrderId());
        assertEquals(1, o.attempt);
        assertEquals(CoreOrder.PK_CANCEL, o.pendingKind);
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertTrue(o.has(CoreOrder.F_CANCEL_PENDING));
        assertEquals(2, o.pendingDispatchSeq);
        assertEquals(LEG_A, o.currentLegId);
        assertEquals("the hold stays until the terminal", 200 * ONE, r.engine.account(1).locked(USD));

        r.markNow();
        outcome(r, 1, 1, KIND_CANCEL, APPLIED, LEG_A, LEG_A);
        assertEquals("CANCEL APPLIED: the release (Balance) then the terminal OrderEvent, got " + r.events(), 2, r.events().size());
        assertEquals(1, r.count(AssetsEngineEvent.Balance.class));
        final AssetsEngineEvent.OrderEvent term = event(r, 1, EventKind.CANCELLED);
        assertEquals(OrderStatus.CANCELLED, term.status());
        assertEquals("the terminal event sees the hold already released", 0, term.holdRemaining());
        assertEquals(2 * ONE, term.remainingQty());
        assertNull("terminal records leave the store in the same apply", r.orders.orders().get(1));
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertFalse(r.engine.account(1).hasHold(1));
        assertEquals("no further dispatch", 2, r.engine.getDispatchSeq());
        assertEquals(0, r.orders.staleOutcomeCount());
    }

    @Test
    public void rC8c_twoCancelsOnARestingLegProduceOneDispatch() {
        // lifecycle C8b guard (!CANCEL_PENDING) once the S5 outcome has learned the leg
        final OrderRig r = buyer();
        ack(r, 1, LEG_A);
        assertEquals("precondition: the CREATE outcome learned the leg (3.3)", LEG_A, live(r, 1).currentLegId);
        r.markNow();
        r.cancel(1, 1);
        r.cancel(1, 1);
        assertEquals("two cancels, one MeDispatch{CANCEL}", 1, r.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals(DispatchKind.CANCEL, r.first(AssetsEngineEvent.MeDispatch.class).kind());
        assertEquals(2, r.count(AssetsEngineEvent.OrderAck.class));
        assertTrue("the second cancel is acknowledged as already pending", lastAck(r).duplicate());
        assertEquals(2, r.engine.getDispatchSeq());
        assertEquals(1, live(r, 1).attempt);
        r.markNow();
        outcome(r, 1, 1, KIND_CANCEL, APPLIED, LEG_A, LEG_A);
        assertEquals(OrderStatus.CANCELLED, event(r, 1, EventKind.CANCELLED).status());
        assertEquals(2, r.engine.getDispatchSeq());
    }

    @Test
    public void rC9a_gtdTenSweepsMintOneCancelAndTheCancelOutcomeYieldsExpired() {
        // contract 3.7 with 3.3 APPLIED/CANCEL when EXPIRY_REQUESTED; lifecycle C9
        final OrderRig r = gtdBuyerWithLeg(5_000);
        r.advance(5_000);
        r.markNow();
        for (int i = 0; i < 10; i++) {
            r.sweep();
            r.advance(100);
        }
        assertEquals("exactly one CANCEL across 10 sweeps past the deadline", 1, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d.kind());
        assertEquals((1L << 2) | 1, d.commandIdLow());
        assertEquals(LEG_A, d.oldOrderId());
        assertEquals("the mint is announced once (DISPATCHED)", 1, kindCount(r, 1, EventKind.DISPATCHED));
        assertEquals("the sweep mints, it does not terminalize", 0, kindCount(r, 1, EventKind.EXPIRED));
        final CoreOrder o = live(r, 1);
        assertTrue(o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertTrue(o.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(2, o.pendingDispatchSeq);
        assertEquals("the GTD entry popped once", 0, gtdDeadlines(r));
        assertEquals(2, r.engine.getDispatchSeq());
        r.markNow();
        outcome(r, 1, 1, KIND_CANCEL, APPLIED, LEG_A, LEG_A);
        final AssetsEngineEvent.OrderEvent ev = event(r, 1, EventKind.EXPIRED);
        assertEquals("EXPIRY_REQUESTED makes the ME cancel surface as EXPIRED", OrderStatus.EXPIRED, ev.status());
        assertEquals(1, r.count(AssetsEngineEvent.Balance.class));
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertEquals(0, gtdDeadlines(r));
    }

    @Test
    public void rC9a_gtdJournalTerminalCancelledYieldsExpiredAndTheLateOutcomeIsStale() {
        // contract 3.2 CANCELLED(3) with EXPIRY_REQUESTED after 3.7; "the journal terminal that follows is stale" reversed
        final OrderRig r = gtdBuyerWithLeg(1_000);
        r.advance(1_000);
        r.sweep();
        assertEquals("precondition: the deadline minted the single cancel", 2, r.engine.getDispatchSeq());
        r.markNow();
        terminal(r, 1, LEG_A, ME_CANCELLED);
        final AssetsEngineEvent.OrderEvent ev = event(r, 1, EventKind.EXPIRED);
        assertEquals(OrderStatus.EXPIRED, ev.status());
        assertEquals(0, ev.holdRemaining());
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertEquals(0, gtdDeadlines(r));
        r.markNow();
        outcome(r, 1, 1, 1, 1, KIND_CANCEL, APPLIED, LEG_A, LEG_A, 0);
        assertEquals("the CANCEL outcome after the journal terminal is a counted no-op", 1, r.orders.staleOutcomeCount());
        assertEquals(0, r.events().size());
        assertEquals(0, r.orders.staleTerminalCount());
    }

    @Test
    public void rC9a_gtdExpiryBeforeAckMintsTheCancelAtAckOnly() {
        // contract 3.7 "create unresolved: the cancel is minted by 3.3 at ACK"
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, BUY, 100 * ONE, 2 * ONE);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = r.now + 1_000;
        r.create(c);
        r.advance(1_000);
        r.sweep();
        final CoreOrder o = live(r, 1);
        assertTrue(o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertEquals("S4: nothing minted while the create is unresolved", 1, r.engine.getDispatchSeq());
        r.markNow();
        ack(r, 1, LEG_A);
        assertEquals("the ACK mints the one cancel", 1, r.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals((1L << 2) | 1, r.first(AssetsEngineEvent.MeDispatch.class).commandIdLow());
        assertEquals(1, kindCount(r, 1, EventKind.ACK_NEW));
        assertEquals(1, kindCount(r, 1, EventKind.DISPATCHED));
        for (int i = 0; i < 3; i++) {
            r.advance(100);
            r.sweep();
        }
        assertEquals("later sweeps mint nothing more", 2, r.engine.getDispatchSeq());
        r.markNow();
        outcome(r, 1, 1, KIND_CANCEL, APPLIED, LEG_A, LEG_A);
        assertEquals(OrderStatus.EXPIRED, event(r, 1, EventKind.EXPIRED).status());
        assertEquals(0, r.engine.account(1).locked(USD));
    }

    @Test
    public void rC13a_iocRestingPartiallyIsCancelledOnceAndTheResidualReleased() {
        // contract 3.3 APPLIED/CREATE with IOC and remainingQty > 0, then a 3.1 fill and a 3.2 terminal; lifecycle C13
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 5 * ONE);
        final CreateOrderCommand ioc = r.limit(1, 1, BUY, 100 * ONE, 2 * ONE);
        ioc.timeInForce = OrderEngine.TIF_IOC;
        r.create(ioc);                                  // order 1, hold 200 USD
        r.create(r.limit(2, 1, SELL, 100 * ONE, ONE));  // order 2, hold 1 BTC
        final CoreOrder o = live(r, 1);
        assertTrue(o.has(CoreOrder.F_IOC));
        r.markNow();
        ack(r, 1, LEG_A);
        assertEquals("IOC at ACK: ACK_NEW, one CANCEL dispatch, DISPATCHED, got " + r.events(), 3, r.events().size());
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d.kind());
        assertEquals((1L << 2) | 1, d.commandIdLow());
        assertEquals(LEG_A, d.oldOrderId());
        assertTrue("the emulated IOC sets CANCEL_PENDING itself", o.has(CoreOrder.F_CANCEL_PENDING));
        assertEquals(OrderStatus.NEW.code(), o.status);
        ack(r, 2, LEG_B);
        r.markNow();
        trade(r, 1, 1, 2, LEG_B, 2, 1, LEG_A, 1, 100 * ONE, ONE, false); // taker = order 2 SELL, maker = order 1 BUY
        assertEquals(1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(OrderStatus.PARTIALLY_FILLED, event(r, 1, EventKind.FILL).status());
        assertEquals(ONE, o.filledQty);
        assertEquals(ONE, o.remainingQty);
        assertEquals(OrderStatus.PARTIALLY_FILLED.code(), o.status);
        assertEquals("100 USD settled from the order's hold", 100 * ONE, r.engine.account(1).locked(USD));
        assertEquals(800 * ONE, r.engine.account(1).available(USD));
        assertEquals(ONE, r.engine.account(1).available(BTC));
        assertNull("the SELL filled completely and left the store", r.orders.orders().get(2));
        assertEquals(1, r.engine.getLastAppliedTradeId());
        r.markNow();
        terminal(r, 1, LEG_A, ME_CANCELLED);
        final AssetsEngineEvent.OrderEvent term = event(r, 1, EventKind.CANCELLED);
        assertEquals("no EXPIRY_REQUESTED: CANCELLED, not EXPIRED", OrderStatus.CANCELLED, term.status());
        assertEquals(ONE, term.filledQty());
        assertEquals(ONE, term.remainingQty());
        assertNull(r.orders.orders().get(1));
        assertEquals("the residual 100 USD is back", 900 * ONE, r.engine.account(1).available(USD));
        assertEquals(0, r.engine.account(1).locked(USD));
        assertFalse(r.engine.account(1).hasHold(1));
        assertEquals("two creates + one CANCEL in the whole life of the IOC", 3, r.engine.getDispatchSeq());
        assertEquals("exactly one CANCEL dispatch for order 1", 1, r.sink.events().stream()
                .filter(e -> e instanceof AssetsEngineEvent.MeDispatch md && md.kind() == DispatchKind.CANCEL && md.omsOrderId() == 1).count());
    }

    @Test
    public void createOutcomeRefusalsTerminalizeRejectedWithTheMeReasonAndReleaseTheHold() {
        // contract 3.3 results 1 / 4 / 5 / 6 on a CREATE; lifecycle C15
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        for (int i = 0; i < 4; i++) {
            r.create(r.limit(1, 1, BUY, 100 * ONE, ONE)); // orders 1..4, 100 USD each
        }
        assertEquals(400 * ONE, r.engine.account(1).locked(USD));
        final int[] results = {REJECTED, UNKNOWN_LEG, NOT_OWNER, UNKNOWN_MARKET};
        final CoreRejectReason[] reasons = {CoreRejectReason.ME_REJECTED, CoreRejectReason.ME_UNKNOWN_LEG,
                CoreRejectReason.NOT_OWNER, CoreRejectReason.UNKNOWN_MARKET};
        for (int i = 0; i < results.length; i++) {
            final long id = i + 1;
            r.markNow();
            outcome(r, id, 0, KIND_CREATE, results[i], 0L, 0L);
            assertEquals("case " + i + ": a refused CREATE terminalizes REJECTED: one Balance (release) + one OrderEvent, got "
                    + r.events(), 2, r.events().size());
            final AssetsEngineEvent.OrderEvent ev = event(r, id, EventKind.REJECTED);
            assertEquals("case " + i, OrderStatus.REJECTED, ev.status());
            assertEquals("case " + i, reasons[i], ev.reason());
            assertEquals("case " + i, 0, ev.holdRemaining());
            assertNull("case " + i + ": removed in the same apply", r.orders.orders().get(id));
            assertEquals("case " + i + ": hold released", (400 - 100 * (i + 1)) * ONE, r.engine.account(1).locked(USD));
            assertFalse(r.engine.account(1).hasHold(id));
        }
        assertEquals(0, r.orders.orders().openCount(1));
        assertEquals(1_000 * ONE, r.engine.account(1).available(USD));
        assertEquals("no retry minted", 4, r.engine.getDispatchSeq());
        assertEquals(0, r.orders.staleOutcomeCount());
    }

    @Test
    public void cancelOutcomeRefusedClearsTheDispatchKeepsCancelPendingAndCounts() {
        // contract 3.3 result 1 on a CANCEL: "the journal terminal settles it"
        final OrderRig r = buyer();
        ack(r, 1, LEG_A);
        r.cancel(1, 1);
        assertEquals("precondition: the cancel of the resting leg was minted (3.3 learned the leg)", 2, r.engine.getDispatchSeq());
        r.markNow();
        outcome(r, 1, 1, KIND_CANCEL, REJECTED, LEG_A, LEG_A);
        assertEquals("a refused cancel is counted", 1, r.orders.cancelRefusedCount());
        final CoreOrder o = live(r, 1);
        assertFalse("no command in flight any more", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o.pendingDispatchSeq);
        assertTrue("the user's intent stays recorded", o.has(CoreOrder.F_CANCEL_PENDING));
        assertEquals(OrderStatus.NEW.code(), o.status);
        assertEquals(LEG_A, o.currentLegId);
        assertEquals("no blind retry", 2, r.engine.getDispatchSeq());
        assertEquals("nothing emitted for a refused cancel, got " + r.events(), 0, r.events().size());
        assertEquals(200 * ONE, r.engine.account(1).locked(USD));
        r.markNow();
        terminal(r, 1, LEG_A, ME_CANCELLED);
        assertEquals(OrderStatus.CANCELLED, event(r, 1, EventKind.CANCELLED).status());
        assertNull(r.orders.orders().get(1));
        assertEquals(0, r.engine.account(1).locked(USD));
    }

    @Test
    public void conflictOutcomeQuarantinesTheOrderAndNoFurtherCommandIsMinted() {
        // contract 3.3 result 2 CONFLICT, then the 3.5 effects
        final OrderRig r = buyer();
        r.markNow();
        outcome(r, 1, 0, KIND_CREATE, CONFLICT, 0L, 0L);
        assertEquals("CONFLICT emits exactly one event, the QUARANTINED OrderEvent, got " + r.events(), 1, r.events().size());
        assertEquals(CoreRejectReason.ME_CONFLICT, event(r, 1, EventKind.QUARANTINED).reason());
        assertEquals(1, r.orders.conflictCount());
        final CoreOrder o = live(r, 1);
        assertTrue(o.has(CoreOrder.F_QUARANTINED));
        assertFalse("no retry: the dispatch is dropped", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(0, o.pendingDispatchSeq);
        assertEquals("the record stays live", 1, r.orders.orders().openCount(1));
        assertEquals("the hold is untouched", 200 * ONE, r.engine.account(1).locked(USD));
        assertEquals("alarm-grade, but the fence is not tripped", GateState.OPEN, r.engine.gate().state(OrderEntryGate.GLOBAL));
        r.markNow();
        r.cancel(1, 1);
        assertEquals("a quarantined order refuses user commands", CoreRejectReason.NOT_OPEN, r.reject().reason());
        assertEquals("and mints nothing", 1, r.engine.getDispatchSeq());
    }

    @Test
    public void capacityOutcomeClosesTheGlobalGateAndKeepsTheDispatchPending() {
        // contract 3.3 result 3 CAPACITY
        final OrderRig r = buyer();
        r.markNow();
        outcome(r, 1, 0, KIND_CREATE, CAPACITY, 0L, 0L);
        assertEquals("ME capacity closes the GLOBAL gate", GateState.SUSPENDED, r.engine.gate().state(OrderEntryGate.GLOBAL));
        assertEquals(GateReason.ME_CAPACITY, r.engine.gate().reason(OrderEntryGate.GLOBAL));
        final AssetsEngineEvent.AdmissionState a = r.only(AssetsEngineEvent.AdmissionState.class);
        assertEquals(OrderEntryGate.GLOBAL, a.marketId());
        assertEquals(GateState.SUSPENDED, a.gateState());
        assertEquals(GateReason.ME_CAPACITY, a.gateReason());
        assertFalse("BROADCAST, not to an origin", a.toOrigin());
        assertEquals("no AdminAck: nobody asked", 0, r.count(AssetsEngineEvent.AdminAck.class));
        final CoreOrder o = live(r, 1);
        assertTrue("the dispatch stays pending for the resync", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals("pendingDispatchSeq unchanged", 1, o.pendingDispatchSeq);
        assertEquals(0, o.attempt);
        assertEquals(OrderStatus.PENDING_NEW.code(), o.status);
        assertEquals(0, r.orders.staleOutcomeCount());
        assertEquals(200 * ONE, r.engine.account(1).locked(USD));
        r.markNow();
        r.create(r.limit(1, 1, BUY, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.GATE_CLOSED, r.reject().reason());
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xCA, 0L, 1024);
        assertEquals("the resync re-offers the pending create", 1, r.count(AssetsEngineEvent.MeDispatchReply.class));
        assertEquals(1, r.first(AssetsEngineEvent.MeDispatchReply.class).dispatchSeq());
    }
}

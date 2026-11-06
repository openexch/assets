// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.ReleaseScope;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G3 (Tables A-D of 3.3, step 0) and the G11 S12 carrier rows. */
public class OutcomeV3Test {

    private static final long PX = 100 * ONE;

    private static OrderFaultKind faultKind(final V3Rig r) {
        final List<AssetsEngineEvent.OrderFault> f = r.all(AssetsEngineEvent.OrderFault.class);
        return f.isEmpty() ? null : f.get(0).kind();
    }

    private static void dead(final V3Rig r, final String what, final long h, final CoreRejectReason why, final long hold) {
        assertNull(what + ": terminal", r.order(h));
        final AssetsEngineEvent.OrderEvent t = r.all(AssetsEngineEvent.OrderEvent.class).get(r.all(AssetsEngineEvent.OrderEvent.class).size() - 1);
        assertEquals(what + ": REJECTED", EventKind.REJECTED, t.kind());
        assertEquals(what + ": reason", why, t.reason());
        assertEquals(what + ": every hold released", hold, r.locked(1, V3Rig.USD));
    }

    @Test
    public void everyResultAgainstEveryPendingKind() {
        // ---- Table A, pending CREATE
        for (int result = 0; result <= 8; result++) {
            final V3Rig r = funded();
            final long h = r.create(r.limit(1, 0, PX, ONE));
            r.mark();
            assertTrue(r.outcome(h, 0, CREATE, result, result <= 1 ? 101 : 0, 0, false));
            final String w = "A row " + result;
            switch (result) {
                case APPLIED:
                    assertEquals(w, OrderStatus.NEW.code(), r.order(h).status);
                    oneRelease(r, w, h, 0, ReleaseScope.UP_TO);
                    break;
                case CONFLICT:
                    assertTrue(w, r.order(h).has(CoreOrder.F_QUARANTINED));
                    assertEquals(w + ": hold unchanged", PX, r.locked(1, V3Rig.USD));
                    noRelease(r, w);
                    break;
                case CAPACITY:
                    assertTrue(w + ": stays pending", r.order(h).has(CoreOrder.F_DISPATCH_PENDING));
                    assertTrue(w + ": wait flag", r.order(h).has(CoreOrder.F_ME_CAPACITY_WAIT));
                    assertTrue(w, globalHas(r, GateReason.ME_CAPACITY));
                    assertEquals(w, OrderFaultKind.ME_CAPACITY_WHILE_OPEN, faultKind(r));
                    noRelease(r, w);
                    break;
                case RELEASED:
                    dead(r, w, h, CoreRejectReason.ME_RELEASED, 0);
                    assertEquals(w, OrderFaultKind.ME_RELEASE_EARLY, faultKind(r));
                    noRelease(r, w);
                    break;
                case INVALID:
                    dead(r, w, h, CoreRejectReason.ME_INVALID, 0);
                    assertEquals(w, OrderFaultKind.ME_INVALID, faultKind(r));
                    oneRelease(r, w, h, 3, ReleaseScope.ORDER);
                    break;
                default:
                    dead(r, w, h, result == REJECTED ? CoreRejectReason.ME_REJECTED : result == UNKNOWN_LEG
                            ? CoreRejectReason.ME_UNKNOWN_LEG : result == NOT_OWNER ? CoreRejectReason.NOT_OWNER
                            : CoreRejectReason.UNKNOWN_MARKET, 0);
                    oneRelease(r, w, h, 3, ReleaseScope.ORDER);
                    break;
            }
            checkInvariants(r, w);
        }
        // ---- Table B, pending CANCEL (attempt 1)
        for (int result = 0; result <= 8; result++) {
            final V3Rig r = funded();
            final long h = restingBuy(r, 101);
            cancel(r, 1, h);
            r.mark();
            assertTrue(r.outcome(h, 1, CANCEL, result, result <= 1 ? 101 : 0, 0, false));
            final String w = "B row " + result;
            if (result == APPLIED) {
                assertNull(w, r.order(h));
                assertEquals(OrderStatus.CANCELLED, r.first(AssetsEngineEvent.OrderEvent.class).status());
                oneRelease(r, w, h, 7, ReleaseScope.ORDER);
            } else if (result == CONFLICT) {
                assertTrue(w, r.order(h).has(CoreOrder.F_QUARANTINED));
                noRelease(r, w);
            } else if (result == CAPACITY) {
                assertTrue(w, r.order(h).has(CoreOrder.F_ME_CAPACITY_WAIT));
                noRelease(r, w);
            } else {
                final CoreOrder o = r.order(h);
                assertNotNull(w + ": the order stays live", o);
                assertTrue(w + ": flags stay", o.has(CoreOrder.F_CANCEL_PENDING));
                assertFalse(w + ": nothing pending, no automatic re-mint", o.has(CoreOrder.F_DISPATCH_PENDING));
                assertEquals(w + ": the hold stands", PX, r.locked(1, V3Rig.USD));
                if (result == RELEASED) {
                    assertTrue(w, o.has(CoreOrder.F_ME_RELEASED));
                    assertEquals(w, OrderFaultKind.ME_RELEASE_EARLY, faultKind(r));
                    assertTrue(w + ": gate m", marketHas(r, V3Rig.MKT, GateReason.ME_LEG_LIVE));
                    noRelease(r, w);
                } else if (result == INVALID) {
                    assertEquals(w, OrderFaultKind.ME_INVALID, faultKind(r));
                    assertTrue(w + ": gate m", marketHas(r, V3Rig.MKT, GateReason.ME_LEG_LIVE));
                    oneRelease(r, w + " (M10)", h, 5, ReleaseScope.UP_TO);
                } else {
                    oneRelease(r, w + " (M8)", h, 5, ReleaseScope.UP_TO);
                }
            }
            checkInvariants(r, w);
        }
        // ---- Table C, pending AMEND (attempt 1, top-up 100 USD)
        for (int result = 0; result <= 9; result++) {
            final V3Rig r = funded();
            final long h = restingBuy(r, 101);
            r.amend(1, h, 0, 2 * ONE);
            assertEquals("precondition: the top-up is held", 2 * PX, r.locked(1, V3Rig.USD));
            final boolean oldCancelled = result == 9; // row "1, oldCancelled 1"
            final int res = oldCancelled ? REJECTED : result;
            r.mark();
            assertTrue(r.outcome(h, 1, AMEND, res, res <= 1 ? (res == 0 ? 303 : 101) : 0, 0, oldCancelled));
            final String w = "C row " + res + (oldCancelled ? " oldCancelled" : "");
            final CoreOrder o = r.order(h);
            if (res == APPLIED) {
                assertEquals(w, 303L, o.currentLegId);
                assertEquals(w, 2 * ONE, o.quantity);
                oneRelease(r, w, h, 6, ReleaseScope.UP_TO);
            } else if (oldCancelled) {
                assertNull(w, o);
                assertEquals(w + ": all released", 0L, r.locked(1, V3Rig.USD));
                oneRelease(r, w, h, 7, ReleaseScope.ORDER);
            } else if (res == CONFLICT || res == CAPACITY) {
                assertTrue(w + ": window open", o.has(CoreOrder.F_REPLACE_PENDING));
                assertEquals(w + ": hold unchanged", 2 * PX, r.locked(1, V3Rig.USD));
                noRelease(r, w);
            } else {
                assertNotNull(w, o);
                assertFalse(w + ": aborted", o.has(CoreOrder.F_REPLACE_PENDING));
                assertEquals(w + ": live on the old leg", 101L, o.currentLegId);
                assertEquals(w + ": top-up released", PX, r.locked(1, V3Rig.USD));
                final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
                assertEquals(w, EventKind.REPLACED, ev.kind());
                if (res == RELEASED) {
                    assertEquals(w, CoreRejectReason.ME_RELEASED, ev.reason());
                    assertTrue(w, o.has(CoreOrder.F_ME_RELEASED));
                    assertTrue(w, marketHas(r, V3Rig.MKT, GateReason.ME_LEG_LIVE));
                    noRelease(r, w);
                } else if (res == INVALID) {
                    assertEquals(w, CoreRejectReason.ME_INVALID, ev.reason());
                    assertEquals(w, OrderFaultKind.ME_INVALID, faultKind(r));
                    assertTrue(w, marketHas(r, V3Rig.MKT, GateReason.ME_LEG_LIVE));
                    oneRelease(r, w + " (M10)", h, 6, ReleaseScope.UP_TO);
                } else {
                    oneRelease(r, w + " (M9)", h, 6, ReleaseScope.UP_TO);
                }
            }
            checkInvariants(r, w);
        }
        // ---- Table D
        V3Rig r = funded();
        long h = restingBuy(r, 101);
        assertTrue(r.outcome(h, 0, CREATE, RELEASED, 0, 0, false)); // U1: a RELEASED copy for an id no longer pending
        assertEquals("U1", 1L, r.engine.orders().releasedLateCount());
        assertTrue(r.outcome(h, 0, CREATE, APPLIED, 101, 0, false));
        assertEquals("U2", 1L, r.engine.orders().redundantOutcomeCount());
        final long stale = r.engine.orders().staleOutcomeCount();
        assertTrue(r.outcome(h, 0, CREATE, REJECTED, 909, 0, false));
        assertEquals("U3", stale + 1, r.engine.orders().staleOutcomeCount());
        cancel(r, 1, h);
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertTrue(r.outcome(h, 0, CREATE, APPLIED, 101, 0, false));
        assertEquals("U6: the outcome after the ME's own terminal", 2L, r.engine.orders().redundantOutcomeCount());
        assertTrue(r.outcome(h, 1, CANCEL, APPLIED, 101, 0, false));
        assertEquals("an APPLIED CANCEL naming the frozen leg is U6 too (first matching row)", 3L,
                r.engine.orders().redundantOutcomeCount());
        assertTrue(r.outcome(h, 1, CANCEL, REJECTED, 101, 0, false));
        assertEquals("U7", stale + 2, r.engine.orders().staleOutcomeCount());
        assertEquals("no fault from U1-U3, U6, U7", 0L, r.engine.orders().lateApplyOnDeadOrderCount());
    }

    @Test
    public void releasedAbandonsBacklogAndRestores() {
        // (A) an iceberg slice acked (its M6 UP_TO is in the backlog), the next slice's CREATE comes back RELEASED
        V3Rig r = funded();
        long buy = r.create(r.limit(1, 0, PX, ONE));
        r.ack(buy, 0, 101);
        long ice = r.create(r.iceberg(2, 1, PX, ONE, ONE / 2));
        r.ack(ice, 0, 401);
        assertNotNull("precondition: the slice's UP_TO", entry(r, ice));
        assertTrue(r.trade(1, buy, 101, 1, ice, 401, 2, PX, ONE / 2, true)); // slice 1 exhausted -> slice 2 (attempt 1)
        assertTrue(r.order(ice).has(CoreOrder.F_DISPATCH_PENDING));
        r.mark();
        assertTrue(r.outcome(ice, 1, CREATE, RELEASED, 0, 0, false));
        assertNull(r.order(ice));
        assertNull("abandoned", entry(r, ice));
        noRelease(r, "(A) no M11");
        checkInvariants(r, "(A)");
        AssetsEngineFactory.roundTrip(r);
        assertTrue(r.outcome(ice, 1, CANCEL, APPLIED, 401, 0, false));
        assertEquals("(A) a late APPLIED of a non-create id is no U5 fault", 0L, r.engine.orders().lateApplyOnDeadOrderCount());

        // (B) CANCEL RELEASED, later fills end the order FILLED
        r = funded();
        final long h = restingBuy(r, 101);
        final long sell = r.create(r.limit(2, 1, PX, ONE));
        r.ack(sell, 0, 202);
        cancel(r, 1, h);
        assertTrue(r.outcome(h, 1, CANCEL, RELEASED, 0, 0, false));
        assertNull(entry(r, h));
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE, false));
        assertNull("FILLED", r.order(h));
        final List<AssetsEngineEvent.MeDispatch> rel = releases(r);
        // review Q1 option (d): a 61 kills the flagged order, so its ORDER is minted too (no tombstone while its leg rested)
        assertEquals("both ORDERs, the flagged order's included", 2, rel.size());
        assertEquals(ORDER, entry(r, h).scope);
        assertEquals(0L, r.engine.orders().releaseSkippedCount());
        checkInvariants(r, "(B)");
        AssetsEngineFactory.roundTrip(r);
    }

    /** Tiny indirection so both rows read the same. */
    private static final class AssetsEngineFactory {
        static void roundTrip(final V3Rig r) {
            S8Support.roundTrip(r.engine, "round trip");
        }
    }

    @Test
    public void releasedFlagClearedByALaterJudgedCommand() {
        // review 2 FIX-1. The flag source is an AMEND RELEASED: after a CANCEL RELESED the order keeps F_CANCEL_PENDING,
        // which makes AmendOrder NOT_OPEN (OE amend admission), so a later amend cannot follow a released cancel
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, RELEASED, 0, 0, false));
        assertTrue(r.order(h).has(CoreOrder.F_ME_RELEASED));
        // a RELEASED or INVALID answer keeps the flag (B0/B1 precede B2); the deferred UP_TO is not flushed while flagged
        r.adminGate(V3Rig.MKT, com.openexchange.assets.domain.GateState.OPEN, GateReason.ADMIN); // the RELEASED row gated m
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.outcome(h, 2, AMEND, INVALID, 0, 0, false));
        assertTrue("INVALID keeps the flag", r.order(h).has(CoreOrder.F_ME_RELEASED));
        noRelease(r, "flagged: no UP_TO flush");
        r.adminGate(V3Rig.MKT, com.openexchange.assets.domain.GateState.OPEN, GateReason.ADMIN);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.outcome(h, 3, AMEND, APPLIED, 303, 0, false));
        assertFalse("a judged command clears the flag", r.order(h).has(CoreOrder.F_ME_RELEASED));
        oneRelease(r, "M7 after the clear", h, (3L << 2) | 2, ReleaseScope.UP_TO);
        final long sell = r.create(r.limit(2, 1, PX, 2 * ONE));
        r.ack(sell, 0, 202);
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 303, 1, PX, 2 * ONE, false));
        assertNull(r.order(h));
        assertNotNull("M11 for the cleared order", entry(r, h));
        assertEquals(ORDER, entry(r, h).scope);
        checkInvariants(r, "cleared");
    }

    @Test
    public void invalidOrReleasedOnPendingCancelKeepsOrderLiveWithHold() {
        for (final int result : new int[] {INVALID, RELEASED}) {
            final V3Rig r = funded();
            final long h = restingBuy(r, 101);
            final long sell = r.create(r.limit(2, 1, PX, ONE));
            r.ack(sell, 0, 202);
            cancel(r, 1, h);
            assertTrue(r.outcome(h, 1, CANCEL, result, 0, 0, false));
            assertEquals("result " + result + ": the hold stands", PX, r.locked(1, V3Rig.USD));
            assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE / 2, false));
            assertEquals("the leg traded and settled from the hold", PX / 2, r.locked(1, V3Rig.USD));
            assertEquals("no SETTLE fault", 0L, r.engine.getSettleFaultCount());
            checkInvariants(r, "result " + result);
        }
    }

    @Test
    public void icebergCancelInvalidEndsAtSliceExhaustion() {
        final V3Rig r = funded();
        final long buy = r.create(r.limit(1, 0, PX, ONE));
        r.ack(buy, 0, 101);
        final long ice = r.create(r.iceberg(2, 1, PX, ONE, ONE / 2));
        r.ack(ice, 0, 401);
        cancel(r, 2, ice);
        assertTrue(r.outcome(ice, 1, CANCEL, INVALID, 0, 0, false));
        assertTrue(r.order(ice).has(CoreOrder.F_CANCEL_PENDING));
        assertTrue(r.trade(1, buy, 101, 1, ice, 401, 2, PX, ONE / 2, true));
        assertNull("the next slice exhaustion ends it CANCELLED", r.order(ice));
        assertEquals(0L, r.locked(2, V3Rig.BTC));
        assertEquals(ORDER, entry(r, ice).scope);
        checkInvariants(r, "iceberg");
    }

    @Test
    public void invalidOrReleasedOnPendingAmendAbortsOnOldLeg() {
        for (final int result : new int[] {INVALID, RELEASED}) {
            final V3Rig r = funded();
            final long h = restingBuy(r, 101);
            r.amend(1, h, 110 * ONE, 2 * ONE);
            assertTrue(r.outcome(h, 1, AMEND, result, 0, 0, false));
            final CoreOrder o = r.order(h);
            assertEquals(101L, o.currentLegId);
            assertEquals("old price", PX, o.price);
            assertEquals("old quantity", ONE, o.quantity);
            assertEquals("old hold only", PX, r.locked(1, V3Rig.USD));
            checkInvariants(r, "amend result " + result);
        }
    }

    @Test
    public void amendMismatchReason16FaultCarriesTheReference() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        final long sell = r.create(r.limit(2, 1, PX, ONE));
        r.ack(sell, 0, 202);
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE / 4, false));
        r.amend(1, h, 0, 2 * ONE);
        assertEquals("expectedOldRemaining at mint", 3 * ONE / 4, r.order(h).pendingExpectedOldRemaining);
        r.mark();
        assertTrue(r.outcome(h, 1, AMEND, REJECTED, 101, 16, false));
        final AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals(OrderFaultKind.ME_AMEND_MISMATCH, f.kind());
        assertEquals("amount = the reference", 3 * ONE / 4, f.amount());
        final List<AssetsEngineEvent> ev = r.events();
        int fault = -1, abort = -1;
        for (int i = 0; i < ev.size(); i++) {
            if (ev.get(i) instanceof AssetsEngineEvent.OrderFault) {
                fault = i;
            }
            if (ev.get(i) instanceof AssetsEngineEvent.OrderEvent o && o.kind() == EventKind.REPLACED) {
                abort = i;
            }
        }
        assertTrue("N-4: the fault precedes the abort", fault < abort);
        assertEquals(0L, r.order(h).pendingExpectedOldRemaining);
        assertEquals(1L, r.engine.orders().meAmendMismatchCount());
    }

    @Test
    public void meLinkMismatchSetsMeBehaviorAndClamps() {
        // a 64 reporting another capacity
        V3Rig r = funded();
        long h = r.create(r.limit(1, 0, PX, ONE));
        MeOutcomeView v = r.outcomeView(h, 0, CREATE, APPLIED, 101, 0);
        v.ledgerCapacity = 99;
        assertTrue(r.apply(v));
        assertTrue("capacity mismatch", globalHas(r, GateReason.ME_BEHAVIOR));
        assertEquals("the entry still applies", OrderStatus.NEW.code(), r.order(h).status);
        // another behaviour
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        v = r.outcomeView(h, 0, CREATE, APPLIED, 101, 0);
        v.behaviorVersion = 3;
        assertTrue(r.apply(v));
        assertTrue("behaviour mismatch", globalHas(r, GateReason.ME_BEHAVIOR));
        // a size above the capacity: clamped to C
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        v = r.outcomeView(h, 0, CREATE, APPLIED, 101, 0);
        v.ledgerSize = 5_000;
        assertTrue(r.apply(v));
        assertTrue(globalHas(r, GateReason.ME_BEHAVIOR));
        assertEquals("BLOCKER-1: clamped at apply", 4096L, r.engine.orders().meLedgerSize());
        assertEquals(1L, r.engine.orders().meLedgerSizeClampedCount());
        checkInvariants(r, "clamped");
        roundTrip(r.engine, "a clamped meLedgerSize == C restores");
        // a 67 with a size above the capacity, and one with another capacity
        r = funded();
        final com.openexchange.assets.application.engine.order.MeReleaseView rv = r.releaseView();
        rv.ledgerSize = 9_999;
        assertTrue(r.apply(rv));
        assertTrue(globalHas(r, GateReason.ME_BEHAVIOR));
        assertEquals(4096L, r.engine.orders().meLedgerSize());
        final com.openexchange.assets.application.engine.order.MeReleaseView rc = r.releaseView();
        rc.ledgerCapacity = 1;
        rc.ledgerSize = 1;
        assertTrue(r.apply(rc));
        assertEquals(2L, r.engine.orders().meLinkMismatchCount());
        roundTrip(r.engine, "after 67 mismatches");
    }

    // ---------------------------------------------------------------- G11: the S12 INVALID carrier (S12 5.5, S8 3.3)

    /** The S12 carrier frame for a minted dispatch: substitutes as S12 5.5, the ME's result-8 rejection shape. */
    private static int carrier(final V3Rig r, final long h, final long low, final int kind, final long old, final long eor) {
        final long g = ++r.egress;
        final MeCommandOutcomeEncoder e = new MeCommandOutcomeEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(g).commandIdHigh(h).commandIdLow(low).userId(1).omsOrderId(h).oldOrderId(kind == 0 ? 0 : Math.max(1, old))
                .price(PX).quantity(ONE).budget(0).marketId(1).commandKind((short) kind).orderType((short) 2).orderSide((short) 0)
                .appliedPosition(g).meTimestamp(r.now).orderId(0).status(-1).reason(0).oldCancelled((short) 0).result(8)
                .meEpoch(V3Rig.EPOCH).journalSeq(r.nextJseq()).timeInForce((short) 1).expectedOldRemaining(kind == 2 ? eor : 0)
                .ledgerSize((int) r.engine.orders().meLedgerSize()).ledgerCapacity((int) com.openexchange.assets.determinism.V3Genesis.ME_CAPACITY)
                .behaviorVersion(com.openexchange.assets.determinism.V3Genesis.ME_BEHAVIOR);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    /** User 2's order in market 2 (ETH-USD), so the carrier's substituted user 1 and market 1 are visibly not used. */
    private static long user2Market2(final V3Rig r, final boolean ack) {
        r.deposit(2, V3Rig.USD, 1_000_000 * ONE);
        final CreateOrderCommand c = r.limit(2, 0, PX, ONE);
        c.marketId = 2;
        final long h = r.create(c);
        if (ack) {
            r.ack(h, 0, 101);
        }
        return h;
    }

    @Test
    public void s12CarrierResolvesByThePendingKind() {
        // CREATE: REJECTED ME_INVALID, hold released, M11
        V3Rig r = funded();
        long h = user2Market2(r, false);
        r.mark();
        r.dispatchTrusted(carrier(r, h, 0, 0, 0, 0));
        assertEquals("the carrier passes 3.9", FeedHaltReason.NONE, r.feed().latchReason());
        assertNull(r.order(h));
        assertEquals(CoreRejectReason.ME_INVALID, r.first(AssetsEngineEvent.OrderEvent.class).reason());
        assertEquals(0L, r.locked(2, V3Rig.USD));
        oneRelease(r, "carrier CREATE", h, 3, ReleaseScope.ORDER);
        assertEquals("the record's user, not the substitute", 2L, entry(r, h).userId);
        assertEquals("the record's market", 2, entry(r, h).marketId);
        assertEquals("meReason 0", 0, r.first(AssetsEngineEvent.OrderEvent.class).meReason());
        // CANCEL: live with its hold, fault, the order's own market gated, M10
        r = funded();
        h = user2Market2(r, true);
        cancel(r, 2, h);
        r.mark();
        r.dispatchTrusted(carrier(r, h, (1L << 2) | 1, 1, 101, 0));
        assertNotNull(r.order(h));
        assertEquals(PX, r.locked(2, V3Rig.USD));
        final AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals(OrderFaultKind.ME_INVALID, f.kind());
        assertEquals("the record's user", 2L, f.userId());
        assertTrue("market 2 gated", marketHas(r, 2, GateReason.ME_LEG_LIVE));
        assertFalse("market 1 (the substitute) untouched", marketHas(r, 1, GateReason.ME_LEG_LIVE));
        oneRelease(r, "carrier CANCEL", h, 5, ReleaseScope.UP_TO);
        // AMEND: abort, live on the old leg, top-up released, fault, gate, M10
        r = funded();
        h = user2Market2(r, true);
        final CoreOrder o = r.order(h);
        r.amend(2, h, 0, 2 * ONE);
        r.mark();
        r.dispatchTrusted(carrier(r, h, (1L << 2) | 2, 2, 101, o.pendingExpectedOldRemaining));
        assertEquals(101L, r.order(h).currentLegId);
        assertFalse(r.order(h).has(CoreOrder.F_REPLACE_PENDING));
        assertEquals("top-up released", PX, r.locked(2, V3Rig.USD));
        assertTrue(marketHas(r, 2, GateReason.ME_LEG_LIVE));
        oneRelease(r, "carrier AMEND", h, 6, ReleaseScope.UP_TO);
        // an unmatched carrier (an id the core did not mint) stays pending; pendingOldestSeq does not move
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        final long oldest = r.engine.orders().pendingOldestSeq();
        assertTrue(oldest > 0);
        r.dispatchTrusted(carrier(r, h, (5L << 2), 0, 0, 0));
        assertTrue("still pending", r.order(h).has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(oldest, r.engine.orders().pendingOldestSeq());
        assertEquals(FeedHaltReason.NONE, r.feed().latchReason());
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.ReleaseScope;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G1: where release ids are minted (3.1 M1-M12, rules 1-5). Every row boots behaviour 3 through V3Genesis. */
public class CoreReleaseTest {

    private static final long PX = 100 * ONE;

    /** Seller 2 at 100 for {@code qty}, acked at leg 202 (its own M6 UP_TO is minted before the caller's mark). */
    private static long restingSell(final V3Rig r, final long qty) {
        final long h = r.create(r.limit(2, 1, PX, qty));
        r.ack(h, 0, 202);
        return h;
    }

    @Test
    public void upToMintedAtEveryResolutionSite() {
        // M6: CREATE APPLIED
        V3Rig r = funded();
        long h = r.create(r.limit(1, 0, PX, ONE));
        r.mark();
        r.ack(h, 0, 101);
        oneRelease(r, "M6", h, 0, ReleaseScope.UP_TO);

        // M1: an implicit ack by trade (the buyer's create is pending, the seller's leg trades against leg 101)
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        long sell = restingSell(r, ONE);
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE / 2, false));
        oneRelease(r, "M1", h, 0, ReleaseScope.UP_TO);

        // M2: a trade on the NEW leg resolves the replace window (attempt 1, kind AMEND)
        r = funded();
        h = restingBuy(r, 101);
        sell = restingSell(r, ONE);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 303, 1, PX, ONE / 2, false));
        assertEquals("M2: the window resolved on the new leg", 303L, r.order(h).currentLegId);
        oneRelease(r, "M2", h, (1L << 2) | 2, ReleaseScope.UP_TO);

        // M4 (G9): a terminal learns the create's leg; the iceberg lives on with its next slice, so the UP_TO is flushed
        r = funded();
        h = r.create(r.iceberg(2, 1, PX, ONE, ONE / 2));
        r.mark();
        assertTrue(r.terminal(h, 404, 2, V3Rig.ME_FILLED, 0));
        assertNotNull("M4: the iceberg works its next slice", r.order(h));
        oneRelease(r, "M4", h, 0, ReleaseScope.UP_TO);
        final List<AssetsEngineEvent.MeDispatch> creates = dispatches(r, com.openexchange.assets.domain.DispatchKind.CREATE);
        assertEquals("M4: the next slice was minted in the same apply", 1, creates.size());
        assertTrue("rule 1: the flushed UP_TO takes a higher dispatchSeq than the follow-up mint of the same apply",
                releases(r).get(0).dispatchSeq() > creates.get(0).dispatchSeq());

        // M5: the old leg's REJECTED terminal aborts the window
        r = funded();
        h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_REJECTED, 9));
        assertFalse("M5: the window is closed", r.order(h).has(CoreOrder.F_REPLACE_PENDING));
        oneRelease(r, "M5", h, (1L << 2) | 2, ReleaseScope.UP_TO);

        // M7: AMEND APPLIED
        r = funded();
        h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 303, 0, false));
        oneRelease(r, "M7", h, (1L << 2) | 2, ReleaseScope.UP_TO);

        // M8: CANCEL refused (REJECTED; 4, 5, 6 take the same row)
        for (final int result : new int[] {REJECTED, UNKNOWN_LEG, NOT_OWNER, UNKNOWN_MARKET}) {
            r = funded();
            h = restingBuy(r, 101);
            cancel(r, 1, h);
            r.mark();
            assertTrue(r.outcome(h, 1, CANCEL, result, result == REJECTED ? 101 : 0, 0, false));
            oneRelease(r, "M8 result " + result, h, (1L << 2) | 1, ReleaseScope.UP_TO);
        }

        // M9: AMEND aborted (REJECTED with oldCancelled 0, reason 16 included; 4, 5, 6)
        for (final int[] row : new int[][] {{REJECTED, 3}, {REJECTED, 16}, {UNKNOWN_LEG, 0}, {NOT_OWNER, 0}, {UNKNOWN_MARKET, 0}}) {
            r = funded();
            h = restingBuy(r, 101);
            r.amend(1, h, 0, 2 * ONE);
            r.mark();
            assertTrue(r.outcome(h, 1, AMEND, row[0], row[0] == REJECTED ? 101 : 0, row[1], false));
            oneRelease(r, "M9 result " + row[0] + " reason " + row[1], h, (1L << 2) | 2, ReleaseScope.UP_TO);
        }

        // M10: INVALID on a pending CANCEL and on a pending AMEND
        r = funded();
        h = restingBuy(r, 101);
        cancel(r, 1, h);
        r.mark();
        assertTrue(r.outcome(h, 1, CANCEL, INVALID, 0, 0, false));
        oneRelease(r, "M10 cancel", h, (1L << 2) | 1, ReleaseScope.UP_TO);
        r = funded();
        h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.outcome(h, 1, AMEND, INVALID, 0, 0, false));
        oneRelease(r, "M10 amend", h, (1L << 2) | 2, ReleaseScope.UP_TO);

        // M3: the NEW leg's terminal resolves the window and ends the order in the same apply: the ORDER of M11 instead
        r = funded();
        h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.terminal(h, 303, 1, V3Rig.ME_CANCELLED, 0));
        assertNull(r.order(h));
        oneRelease(r, "M3 then the terminal (rule 1: one release, not two)", h, (1L << 2) | 3, ReleaseScope.ORDER);
    }

    private static void assertOrderEntry(final V3Rig r, final String what, final long h, final long user, final int side,
                                         final int meType, final long leg, final int attempt) {
        final ReleaseBacklog.Entry b = entry(r, h);
        assertNotNull(what + ": ORDER entry", b);
        assertEquals(what + ": scope", ORDER, b.scope);
        assertEquals(what + ": stage RELEASE", ReleaseBacklog.STAGE_RELEASE, b.stage);
        assertEquals(what + ": userId", user, b.userId);
        assertEquals(what + ": marketId", V3Rig.MKT, b.marketId);
        assertEquals(what + ": side", side, b.side);
        assertEquals(what + ": meOrderType", meType, b.meOrderType);
        assertEquals(what + ": legId", leg, b.legId);
        assertEquals(what + ": attempt", attempt, b.attempt);
        assertEquals(what + ": floor = attempt << 2 | 3", ((long) attempt << 2) | 3, b.floor);
    }

    /** The terminal OrderEvent of {@code h} precedes its ORDER release (S7 rule 12 stands: holdRemaining 0 on it). */
    private static void terminalBeforeRelease(final V3Rig r, final String what, final long h) {
        int terminal = -1, release = -1;
        final List<AssetsEngineEvent> ev = r.events();
        for (int i = 0; i < ev.size(); i++) {
            if (ev.get(i) instanceof AssetsEngineEvent.OrderEvent o && o.orderId() == h && o.status() != null
                    && (o.status() == OrderStatus.FILLED || o.status() == OrderStatus.CANCELLED || o.status() == OrderStatus.REJECTED
                    || o.status() == OrderStatus.EXPIRED)) {
                terminal = i;
                assertEquals(what + ": the terminal event carries holdRemaining 0", 0L, o.holdRemaining());
            }
            if (ev.get(i) instanceof AssetsEngineEvent.MeDispatch d && d.kind() == com.openexchange.assets.domain.DispatchKind.RELEASE_ID
                    && d.commandIdHigh() == h) {
                release = i;
            }
        }
        assertTrue(what + ": terminal event and release present", terminal >= 0 && release >= 0);
        assertTrue(what + ": the terminal OrderEvent precedes the ORDER mint", terminal < release);
    }

    @Test
    public void orderReleaseAtEveryTerminal() {
        // FILLED (both sides)
        V3Rig r = funded();
        long h = restingBuy(r, 101);
        long sell = restingSell(r, ONE);
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE, false));
        assertEquals("FILLED: two ORDER releases", 2, releases(r).size());
        assertOrderEntry(r, "FILLED buyer", h, 1, 0, 0, 101, 0);
        assertOrderEntry(r, "FILLED seller", sell, 2, 1, 0, 202, 0);
        terminalBeforeRelease(r, "FILLED", h);

        // CANCELLED (attempt 1)
        r = funded();
        h = restingBuy(r, 101);
        cancel(r, 1, h);
        r.mark();
        assertTrue(r.outcome(h, 1, CANCEL, APPLIED, 101, 0, false));
        oneRelease(r, "CANCELLED", h, (1L << 2) | 3, ReleaseScope.ORDER);
        assertOrderEntry(r, "CANCELLED", h, 1, 0, 0, 101, 1);
        terminalBeforeRelease(r, "CANCELLED", h);

        // EXPIRED (GTD cancel, attempt 1)
        r = funded();
        final CreateOrderCommand gtd = tif(r.limit(1, 0, PX, ONE), OrderEngine.TIF_GTD);
        gtd.expiresAtMs = r.now + 1_000;
        h = r.create(gtd);
        r.ack(h, 0, 101);
        r.now += 2_000;
        r.engine.sweep(r.now);
        r.mark();
        assertTrue(r.outcome(h, 1, CANCEL, APPLIED, 101, 0, false));
        oneRelease(r, "EXPIRED", h, (1L << 2) | 3, ReleaseScope.ORDER);
        terminalBeforeRelease(r, "EXPIRED", h);

        // REJECTED CREATE (no leg rested: legId 0)
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        r.mark();
        assertTrue(r.outcome(h, 0, CREATE, REJECTED, 101, 5, false));
        oneRelease(r, "REJECTED", h, 3, ReleaseScope.ORDER);
        assertOrderEntry(r, "REJECTED", h, 1, 0, 0, 0, 0);

        // INVALID CREATE (W8): terminal REJECTED ME_INVALID and the ORDER
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        r.mark();
        assertTrue(r.outcome(h, 0, CREATE, INVALID, 0, 0, false));
        oneRelease(r, "INVALID CREATE", h, 3, ReleaseScope.ORDER);

        // an armed STOP_LIMIT cancelled (never dispatched, attempt 0) and one expired by its GTD
        for (final boolean expire : new boolean[] {false, true}) {
            r = funded();
            final CreateOrderCommand stop = r.limit(1, 0, 110 * ONE, ONE);
            stop.orderType = OrderEngine.TYPE_STOP_LIMIT;
            stop.stopPrice = 105 * ONE;
            if (expire) {
                stop.timeInForce = OrderEngine.TIF_GTD;
                stop.expiresAtMs = r.now + 1_000;
            }
            h = r.create(stop);
            r.mark();
            if (expire) {
                r.now += 2_000;
                r.engine.sweep(r.now);
            } else {
                cancel(r, 1, h);
            }
            assertNull("armed stop ended", r.order(h));
            oneRelease(r, "armed stop " + (expire ? "expired" : "cancelled"), h, 3, ReleaseScope.ORDER);
            assertOrderEntry(r, "armed stop", h, 1, 0, 0, 0, 0); // a STOP_LIMIT child is a LIMIT at the ME
        }

        // IOC EXPIRED (63 CANCELLED on an IOC create, reason 12): the G9 UP_TO is superseded by the ORDER
        r = funded();
        h = r.create(tif(r.limit(1, 0, PX, ONE), OrderEngine.TIF_IOC));
        r.mark();
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, 12));
        oneRelease(r, "IOC EXPIRED", h, 3, ReleaseScope.ORDER);
        assertEquals(OrderStatus.EXPIRED, r.first(AssetsEngineEvent.OrderEvent.class).status());
    }

    @Test
    public void oneReleasePerApplyTerminalWins() {
        // an injected AMEND APPLIED whose in-window fills reached the new quantity: afterWindow ends it FILLED (a correct S5
        // ME answers reason 15 instead); one ORDER, no UP_TO
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        final long sell = restingSell(r, ONE);
        r.amend(1, h, 0, ONE / 2);
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE / 2, false)); // half on the OLD leg inside the window
        r.mark();
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 303, 0, false));
        assertNull("FILLED at the window's close", r.order(h));
        oneRelease(r, "terminal wins", h, (1L << 2) | 3, ReleaseScope.ORDER);
        checkInvariants(r, "terminal wins");
    }

    @Test
    public void releasedOnPendingCreateEndsWithoutAnyRelease() {
        // Table A row 7 (D-S8-2): REJECTED ME_RELEASED, the hold released, no ORDER, the backlog entry abandoned
        final V3Rig r = funded();
        final long h = r.create(r.limit(1, 0, PX, ONE));
        assertEquals("precondition: the create's hold", PX, r.locked(1, V3Rig.USD));
        r.mark();
        assertTrue(r.outcome(h, 0, CREATE, RELEASED, 0, 0, false));
        assertNull("the order ended", r.order(h));
        final AssetsEngineEvent.OrderEvent t = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.REJECTED, t.kind());
        assertEquals(CoreRejectReason.ME_RELEASED, t.reason());
        assertEquals("the hold is released", 0L, r.locked(1, V3Rig.USD));
        noRelease(r, "RELEASED on a pending CREATE");
        assertNull("no backlog entry", entry(r, h));
        assertEquals(1L, r.engine.orders().releaseSkippedCount());
        final AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals(OrderFaultKind.ME_RELEASE_EARLY, f.kind());
        assertEquals("amount = the command's idLow", 0L, f.amount());
        checkInvariants(r, "released create");
    }

    @Test
    public void noReleaseOnConflictOrCapacity() {
        final V3Rig r = funded();
        final long a = r.create(r.limit(1, 0, PX, ONE));
        final long b = r.create(r.limit(1, 0, PX, ONE));
        r.mark();
        assertTrue(r.outcome(a, 0, CREATE, CONFLICT, 0, 0, false));
        assertTrue(r.outcome(b, 0, CREATE, CAPACITY, 0, 0, false));
        noRelease(r, "CONFLICT and CAPACITY (rule 2)");
    }

    @Test
    public void quarantinedOrderStillGetsOrderRelease() {
        // rule 4 (D-S8-7): quarantine does not stop releases
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        r.engine.orders().applyQuarantine(7_400L, h, r.now);
        assertTrue(r.order(h).has(CoreOrder.F_QUARANTINED));
        r.mark();
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, 0));
        oneRelease(r, "quarantined order's terminal", h, 3, ReleaseScope.ORDER);
    }

    @Test
    public void everyResolvedIdReleasedOnceNeverEarly() {
        // the reference model over seeded runs (V3CoreFuzzDriver): every id of h is released only after its resolution was
        // applied, at most one backlog entry per order, and once the backlog drains no ME ledger entry of a dead order
        // remains that is not PARKED or abandoned
        for (long seed = 11; seed <= 14; seed++) {
            final V3CoreFuzzDriver d = new V3CoreFuzzDriver(seed, false);
            d.run(250);
            d.drainAndCheckLedger();
        }
    }

    // ---------------------------------------------------------------- R1 (rule 5), both callers

    private static int terminalFrame(final V3Rig r, final long js, final long leg, final long oms) {
        final MeJournalTerminalEncoder e = new MeJournalTerminalEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(++r.egress).orderId(leg).userId(1).marketId(V3Rig.MKT)
                .status(com.openexchange.assets.infrastructure.generated.OrderStatus.CANCELLED).rejectReason((short) 0)
                .meTimestamp(r.now).omsOrderId(oms).meEpoch(V3Rig.EPOCH).journalSeq(js);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int tradeFrame(final V3Rig r, final long js, final long makerLeg, final long makerOms) {
        final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(++r.egress).tradeId(r.engine.getLastAppliedTradeId() + 1).marketId(V3Rig.MKT).takerOrderId(202)
                .takerUserId(2).makerOrderId(makerLeg).makerUserId(1).price(PX).quantity(ONE / 4).takerIsBuy(BoolFlag.FALSE)
                .meTimestamp(r.now).takerOmsOrderId(2).makerOmsOrderId(makerOms).meEpoch(V3Rig.EPOCH).journalSeq(js);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static void assertStillPending(final V3Rig r, final String what, final long h, final long hold) {
        final CoreOrder o = r.order(h);
        assertNotNull(what + ": the order is untouched", o);
        assertEquals(what + ": PENDING_NEW", OrderStatus.PENDING_NEW.code(), o.status);
        assertTrue(what + ": the CREATE is still pending", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(what + ": the hold", hold, r.locked(1, V3Rig.USD));
    }

    @Test
    public void noPathTerminalizesWithAPendingCreate() {
        // (a) through the engine API: a 63 and a 61 naming leg 0 are refused by the engine guard (3.4, FIX-4)
        V3Rig r = funded();
        long h = r.create(r.limit(1, 0, PX, ONE));
        final long stale = r.engine.orders().staleTerminalCount();
        assertTrue(r.terminal(h, 0, 1, V3Rig.ME_CANCELLED, 0));
        assertStillPending(r, "63 leg 0 (engine API)", h, PX);
        assertEquals("counted", 1L, r.engine.orders().legZeroRefusedCount());
        assertEquals(stale + 1, r.engine.orders().staleTerminalCount());
        final long sell = r.create(r.limit(2, 1, PX, ONE));
        r.ack(sell, 0, 202);
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 0, 1, PX, ONE / 4, false));
        assertStillPending(r, "61 maker leg 0 (engine API)", h, PX - 25 * ONE); // the settle drew the hold
        assertEquals(2L, r.engine.orders().legZeroRefusedCount());
        final AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals("the trade's money stands with a SETTLE fault", OrderFaultKind.SETTLE, f.kind());
        assertEquals(ONE / 4, f.amount());
        assertEquals(0L, r.engine.orders().terminalWithPendingCreateCount());

        // (b) through the demuxer: the 3.9 decode rule latches SOURCE_SCHEMA before the engine sees leg 0
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        long js = r.nextJseq();
        r.dispatchTrusted(terminalFrame(r, js, 0, h));
        assertStillPending(r, "63 leg 0 (frame)", h, PX);
        assertEquals(FeedHaltReason.SOURCE_SCHEMA, r.feed().latchReason());
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        js = r.nextJseq();
        r.dispatchTrusted(tradeFrame(r, js, 0, h));
        assertStillPending(r, "61 maker leg 0 (frame)", h, PX);
        assertEquals(FeedHaltReason.SOURCE_SCHEMA, r.feed().latchReason());
    }

    // ---------------------------------------------------------------- review FIX-1 / Q1 + Q3, option (d) (coordinator decision 2)

    /** Probe {@code flaggedOrderEndedByItsOwnCancelGetsNoOrderRelease} inverted: a 63 kills a flagged order, M11 is minted. */
    @Test
    public void flaggedOrderEndedByItsOwnCancelGetsTheOrderRelease() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, RELEASED, 0, 0, false));
        assertTrue(r.order(h).has(CoreOrder.F_ME_RELEASED));
        cancel(r, 1, h); // attempt 2, id 2 << 2 | 1
        r.mark();
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertNull(r.order(h));
        oneRelease(r, "the ORDER of a flagged order killed by a 63", h, (2L << 2) | 3, ReleaseScope.ORDER);
        assertTrue(r.outcome(h, 2, CANCEL, APPLIED, 101, 0, false));
        assertEquals(0L, r.engine.orders().releaseSkippedCount());
        assertTrue("the ORDER covers id 9 at the ME", r.releaseEntry(h, (2L << 2) | 3, ORDER, R_APPLIED));
        assertNull(entry(r, h));
        checkInvariants(r, "flagged, killed by its cancel's 63");
    }

    /** Probe {@code flaggedOrderFilledWithCancelInFlightGetsNoOrderRelease} inverted: a 61 fills a flagged order, M11 is minted. */
    @Test
    public void flaggedOrderFilledWithCancelInFlightGetsTheOrderRelease() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        final long sell = r.create(r.limit(2, 1, PX, ONE));
        r.ack(sell, 0, 202);
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, RELEASED, 0, 0, false));
        cancel(r, 1, h);
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE, false));
        assertNull("FILLED", r.order(h));
        assertEquals("both ORDERs, the flagged buyer's included", 2, releases(r).size());
        assertEquals(ORDER, entry(r, h).scope);
        assertTrue(r.outcome(h, 2, CANCEL, UNKNOWN_LEG, 0, 0, false));
        checkInvariants(r, "flagged, filled with its cancel in flight");
    }

    /** Q3: the journal resolution sites M1-M5 clear F_ME_RELEASED (here M2 and M5), so their UP_TO is flushed. */
    @Test
    public void journalResolutionSitesClearTheFlag() {
        // M5: the old leg's REJECTED terminal aborts the window of a later amend
        V3Rig r = funded();
        long h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, RELEASED, 0, 0, false));
        r.adminGate(V3Rig.MKT, com.openexchange.assets.domain.GateState.OPEN, com.openexchange.assets.domain.GateReason.ADMIN);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_REJECTED, 9));
        assertFalse("M5 cleared the flag", r.order(h).has(CoreOrder.F_ME_RELEASED));
        oneRelease(r, "M5 after the clear", h, (2L << 2) | 2, ReleaseScope.UP_TO);
        // M2: a trade on the new leg of a later amend
        r = funded();
        h = restingBuy(r, 101);
        final long sell = r.create(r.limit(2, 1, PX, ONE));
        r.ack(sell, 0, 202);
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, RELEASED, 0, 0, false));
        r.adminGate(V3Rig.MKT, com.openexchange.assets.domain.GateState.OPEN, com.openexchange.assets.domain.GateReason.ADMIN);
        r.amend(1, h, 0, 2 * ONE);
        r.mark();
        assertTrue(r.trade(1, sell, 202, 2, h, 303, 1, PX, ONE / 2, false));
        assertFalse("M2 cleared the flag", r.order(h).has(CoreOrder.F_ME_RELEASED));
        oneRelease(r, "M2 after the clear", h, (2L << 2) | 2, ReleaseScope.UP_TO);
        checkInvariants(r, "M2");
    }
}

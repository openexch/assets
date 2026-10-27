// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.ReleaseScope;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G2: the release backlog, its life table (1.2) and the 67 rows C1-C6, the orphan rows E1-E4, Table D U5/U5b. */
public class ReleaseBacklogTest {

    private static final long PX = 100 * ONE;

    /** Order h (user 1) acked at 101, cancelled and ended by the ME: an ORDER entry at stage RELEASE, attempt 1, leg 101. */
    private static long deadWithOrderEntry(final V3Rig r) {
        final long h = restingBuy(r, 101);
        cancel(r, 1, h);
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertNull(r.order(h));
        final ReleaseBacklog.Entry b = entry(r, h);
        assertEquals(ORDER, b.scope);
        assertEquals(ReleaseBacklog.STAGE_RELEASE, b.stage);
        assertEquals((1L << 2) | 3, b.floor);
        return h;
    }

    @Test
    public void confirmationDropsOnlyWhenDominating() {
        // UP_TO: f below the entry's floor is stale (C3), f at it drops (C2)
        V3Rig r = funded();
        long h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 303, 0, false));
        assertEquals("M7 superseded M6", (1L << 2) | 2, entry(r, h).floor);
        assertTrue(r.releaseEntry(h, 0, UP_TO, R_APPLIED));
        assertNotNull("C3: an older confirmation dominates nothing", entry(r, h));
        assertEquals(1L, r.engine.orders().releaseConfirmStaleCount());
        assertTrue(r.releaseEntry(h, (1L << 2) | 2, UP_TO, R_APPLIED));
        assertNull("C2", entry(r, h));

        // S4 NOTE-9: a mixed frame, NOOP of the older UP_TO then the newer ORDER APPLIED: only the ORDER drops the entry
        r = funded();
        h = deadWithOrderEntry(r);
        r.mark();
        assertTrue(r.apply(r.releaseView(new long[] {h, 0, UP_TO, R_NOOP}, new long[] {h, (1L << 2) | 3, ORDER, R_APPLIED})));
        assertNull("C1 drops it", entry(r, h));
        final List<AssetsEngineEvent.ReleaseEffect> fx = r.all(AssetsEngineEvent.ReleaseEffect.class);
        assertEquals(2, fx.size());
        assertEquals("the NOOP of the older UP_TO is C3", OrderEngine.ROW_C3, fx.get(0).row());
        assertEquals(OrderEngine.ROW_C1, fx.get(1).row());
        assertEquals("nothing on the wire", 0, r.count(AssetsEngineEvent.MeDispatch.class));
        checkInvariants(r, "mixed frame");
    }

    @Test
    public void orderAppliedDropsEntryInAnyStage() {
        // RELEASE
        V3Rig r = funded();
        long h = deadWithOrderEntry(r);
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_APPLIED));
        assertNull(entry(r, h));
        // ORPHAN_CANCEL (the orphan's index entry leaves too)
        r = funded();
        h = deadWithOrderEntry(r);
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        assertEquals(ReleaseBacklog.STAGE_ORPHAN_CANCEL, entry(r, h).stage);
        assertEquals(1, r.engine.orders().dispatchIndex().live(PendingDispatchIndex.TAG_ORPHAN));
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_NOOP));
        assertNull("NOOP proves h tombstoned", entry(r, h));
        assertEquals(0, r.engine.orders().dispatchIndex().live());
        // PARKED (an older copy still in flight confirms it)
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        assertTrue(r.outcome(h, 0, CREATE, REJECTED, 101, 0, false)); // ORDER with legId 0
        assertTrue(r.releaseEntry(h, 3, ORDER, R_LEG_LIVE));
        assertEquals("C4 with leg 0 parks", ReleaseBacklog.STAGE_PARKED, entry(r, h).stage);
        assertTrue(r.releaseEntry(h, 3, ORDER, R_APPLIED));
        assertNull(entry(r, h));
        checkInvariants(r, "any stage");
    }

    @Test
    public void supersedeKeepsOneEntryOrderWins() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        final long first = entry(r, h).dispatchSeq;
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 303, 0, false));
        final ReleaseBacklog.Entry b = entry(r, h);
        assertTrue("a newer UP_TO replaces the entry", b.dispatchSeq > first);
        assertFalse("its old dispatchSeq left the index", r.engine.orders().dispatchIndex().contains(first));
        cancel(r, 1, h);
        assertTrue(r.terminal(h, 303, 1, V3Rig.ME_CANCELLED, 0));
        assertEquals("ORDER replaces UP_TO", ORDER, entry(r, h).scope);
        assertEquals("one entry per order", 1, r.engine.orders().backlog().size());
        r.mark();
        r.engine.orders().applyRequestPendingDispatch(9, 0L, 1024);
        assertEquals("the reply carries one release for h", 1, r.all(AssetsEngineEvent.MeDispatchReply.class).size());
        checkInvariants(r, "supersede");
    }

    @Test
    public void lifeTableEveryTransition() {
        // none -> RELEASE (UP_TO) -> abandoned by F_ME_RELEASED (BLOCKER-2)
        V3Rig r = funded();
        long h = restingBuy(r, 101);
        assertNotNull(entry(r, h));
        cancel(r, 1, h);
        r.mark();
        assertTrue(r.outcome(h, 1, CANCEL, RELEASED, 0, 0, false));
        assertNull("ABANDONED in the same apply", entry(r, h));
        assertEquals(1L, r.engine.orders().releaseAbandonedCount());
        assertEquals(OrderEngine.ROW_ABANDON, r.first(AssetsEngineEvent.ReleaseEffect.class).row());
        assertTrue(r.order(h).has(CoreOrder.F_ME_RELEASED));
        checkInvariants(r, "abandoned");

        // RELEASE ORDER -> LEG_LIVE C4 -> ORPHAN_CANCEL -> E1 -> RELEASE (orphanTries 1) -> LEG_LIVE C5 -> PARKED
        r = funded();
        h = deadWithOrderEntry(r);
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        ReleaseBacklog.Entry b = entry(r, h);
        assertEquals(ReleaseBacklog.STAGE_ORPHAN_CANCEL, b.stage);
        assertEquals("C4 raised the attempt", 2, b.attempt);
        assertTrue(r.outcome(h, 2, CANCEL, APPLIED, 101, 0, false));
        b = entry(r, h);
        assertEquals("E1: M12", ReleaseBacklog.STAGE_RELEASE, b.stage);
        assertEquals(1, b.orphanTries);
        assertEquals("M12 floor = B.attempt << 2 | 3", (2L << 2) | 3, b.floor);
        checkInvariants(r, "after M12");
        assertTrue(r.releaseEntry(h, (2L << 2) | 3, ORDER, R_LEG_LIVE));
        assertEquals("C5", ReleaseBacklog.STAGE_PARKED, entry(r, h).stage);
        assertEquals(0, r.engine.orders().dispatchIndex().live());
        assertEquals(1L, r.engine.orders().backlogParked());
        checkInvariants(r, "parked C5");
        roundTrip(r.engine, "C5 parked entry restores");

        // ORPHAN_CANCEL -> E3 -> PARKED (the second floor form)
        r = funded();
        h = deadWithOrderEntry(r);
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        assertTrue(r.outcome(h, 2, CANCEL, CONFLICT, 0, 0, false));
        assertEquals("E3", ReleaseBacklog.STAGE_PARKED, entry(r, h).stage);
        assertEquals("the floor of the release, attempt raised", (1L << 2) | 3, entry(r, h).floor);
        roundTrip(r.engine, "E3 parked entry restores");
    }

    @Test
    public void legLiveMintsOneOrphanCancelAndGatesTheMarketOnly() {
        final V3Rig r = funded();
        final long h = deadWithOrderEntry(r);
        r.mark();
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        assertTrue("the second forwarder copy journals again (LEG_LIVE is never NOOP)",
                r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        final List<AssetsEngineEvent.MeDispatch> cancels = dispatches(r, DispatchKind.CANCEL);
        assertEquals("one orphan cancel", 1, cancels.size());
        final AssetsEngineEvent.MeDispatch c = cancels.get(0);
        assertEquals(h, c.commandIdHigh());
        assertEquals("idLow = attempt << 2 | CANCEL", (2L << 2) | 1, c.commandIdLow());
        assertEquals(1, c.userId());
        assertEquals(h, c.omsOrderId());
        assertEquals("the leg to cancel", 101L, c.oldOrderId());
        assertEquals(V3Rig.MKT, c.marketId());
        assertEquals(0L, c.price() + c.quantity() + c.budget() + c.timeInForce() + c.expectedOldRemaining());
        assertEquals(1L, r.engine.orders().legLiveDuplicateCount());
        assertEquals(1L, r.engine.orders().orphanCancelCount());
        final AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals(OrderFaultKind.ME_LEG_LIVE, f.kind());
        assertEquals("amount = B.legId", 101L, f.amount());
        assertTrue("the market is gated", marketHas(r, V3Rig.MKT, GateReason.ME_LEG_LIVE));
        assertFalse("GLOBAL untouched", globalHas(r, GateReason.ME_LEG_LIVE));
        assertEquals(0, r.engine.gate().reasons(OrderEntryGate.GLOBAL));
        assertEquals("the orphan cancel counts in P", 1L, r.engine.orders().pendingCommands());
        checkInvariants(r, "leg live");
    }

    @Test
    public void orphanRowsE1toE4AndParking() {
        for (final int result : new int[] {APPLIED, REJECTED, UNKNOWN_LEG, NOT_OWNER, UNKNOWN_MARKET}) {
            final V3Rig r = funded();
            final long h = deadWithOrderEntry(r);
            assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
            r.mark();
            assertTrue(r.outcome(h, 2, CANCEL, result, result <= 1 ? 101 : 0, 0, false));
            final ReleaseBacklog.Entry b = entry(r, h);
            assertEquals("E1/E2 result " + result, ReleaseBacklog.STAGE_RELEASE, b.stage);
            assertEquals(1, b.orphanTries);
            oneRelease(r, "M12 result " + result, h, (2L << 2) | 3, ReleaseScope.ORDER);
            checkInvariants(r, "E1/E2");
        }
        for (final int result : new int[] {CONFLICT, RELEASED, INVALID}) {
            final V3Rig r = funded();
            final long h = deadWithOrderEntry(r);
            assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
            r.mark();
            assertTrue(r.outcome(h, 2, CANCEL, result, 0, 0, false));
            assertEquals("E3 result " + result, ReleaseBacklog.STAGE_PARKED, entry(r, h).stage);
            assertEquals(OrderFaultKind.ME_LEG_LIVE, r.first(AssetsEngineEvent.OrderFault.class).kind());
            assertEquals(101L, r.first(AssetsEngineEvent.OrderFault.class).amount());
            noRelease(r, "E3 mints nothing");
            assertEquals(0L, r.engine.orders().pendingCommands());
            checkInvariants(r, "E3");
        }
        // E4: CAPACITY keeps the orphan cancel pending (the resync re-offers it) and runs the capacity rule
        final V3Rig r = funded();
        final long h = deadWithOrderEntry(r);
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        assertTrue(r.outcome(h, 2, CANCEL, CAPACITY, 0, 0, false));
        assertEquals("E4 stays", ReleaseBacklog.STAGE_ORPHAN_CANCEL, entry(r, h).stage);
        assertTrue(globalHas(r, GateReason.ME_CAPACITY));
        assertEquals(1L, r.engine.orders().capacityOutcomeCount());
        // review NOTE-5: the orphan waiting on capacity counts in capacityParked (MeCapacityParked, not DispatchStuck)
        assertEquals(1L, r.engine.orders().capacityParked());
        assertTrue(r.outcome(h, 2, CANCEL, CAPACITY, 0, 0, false));
        assertEquals("counted once", 1L, r.engine.orders().capacityParked());
        checkInvariants(r, "E4");
        assertTrue(r.outcome(h, 2, CANCEL, UNKNOWN_LEG, 0, 0, false)); // E2: the wait ends
        assertEquals(0L, r.engine.orders().capacityParked());
    }

    @Test
    public void orphanCancelReleasedParks() {
        // FIX-1: U0 precedes U1, so a RELEASED orphan cancel reaches E3 instead of being a late copy
        final V3Rig r = funded();
        final long h = deadWithOrderEntry(r);
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        final long late = r.engine.orders().releasedLateCount();
        assertTrue(r.outcome(h, 2, CANCEL, RELEASED, 0, 0, false));
        assertEquals(ReleaseBacklog.STAGE_PARKED, entry(r, h).stage);
        assertEquals("not counted as a late copy", late, r.engine.orders().releasedLateCount());
        assertEquals(1L, r.engine.orders().parkedCount());
    }

    @Test
    public void lateApplyOnDeadOrderFaultsAndGates() {
        final V3Rig r = funded();
        final long h = deadWithOrderEntry(r);
        r.mark();
        // U5 at stage RELEASE: a late APPLIED CREATE copy installed leg 505 at the ME after the core ended the order
        assertTrue(r.outcome(h, 0, CREATE, APPLIED, 505, 0, false));
        assertEquals(1L, r.engine.orders().lateApplyOnDeadOrderCount());
        final AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals(OrderFaultKind.ME_LEG_LIVE, f.kind());
        assertEquals(505L, f.amount());
        assertTrue(marketHas(r, V3Rig.MKT, GateReason.ME_LEG_LIVE));
        assertEquals("B.legId follows at stage RELEASE", 505L, entry(r, h).legId);
        noRelease(r, "never a second release");
        // the coming LEG_LIVE cancels this leg; at ORPHAN_CANCEL a later late copy does not touch B (FIX-2)
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        final AssetsEngineEvent.MeDispatch orphan = dispatches(r, DispatchKind.CANCEL).get(0);
        assertEquals(505L, orphan.oldOrderId());
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 606, 0, false));
        assertEquals(2L, r.engine.orders().lateApplyOnDeadOrderCount());
        assertEquals("frozen at ORPHAN_CANCEL", 505L, entry(r, h).legId);
        r.mark();
        r.engine.orders().applyRequestPendingDispatch(1, 0L, 1024);
        final AssetsEngineEvent.MeDispatchReply reply = r.first(AssetsEngineEvent.MeDispatchReply.class);
        assertEquals("the reply is the minted orphan frame", orphan.render().replace("MEDISPATCH", "DISPATCHREPLY"), reply.render());
        checkInvariants(r, "late apply");
    }

    @Test
    public void lateApplyOnAbandonedOrderFaults() {
        // U5b (review 2 FIX-2): a dead order with no backlog entry (its CREATE came back RELEASED) gets a late APPLIED CREATE
        V3Rig r = funded();
        long h = r.create(r.limit(1, 0, PX, ONE));
        assertTrue(r.outcome(h, 0, CREATE, RELEASED, 0, 0, false));
        assertNull(entry(r, h));
        r.mark();
        MeOutcomeView v = r.outcomeView(h, 0, CREATE, APPLIED, 707, 0);
        v.userId = 1;
        v.marketId = V3Rig.MKT;
        assertTrue(r.apply(v));
        assertEquals(1L, r.engine.orders().lateApplyOnDeadOrderCount());
        AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals(OrderFaultKind.ME_LEG_LIVE, f.kind());
        assertEquals(h, f.orderId());
        assertEquals(1L, f.userId());
        assertEquals(V3Rig.MKT, f.marketId());
        assertEquals(707L, f.amount());
        assertTrue(marketHas(r, V3Rig.MKT, GateReason.ME_LEG_LIVE));
        assertNull("no backlog entry, no mint", entry(r, h));
        noRelease(r, "U5b");
        // a marketId above MARKETS gates GLOBAL ME_BEHAVIOR instead of indexing past the gate table
        r = funded();
        h = r.create(r.limit(1, 0, PX, ONE));
        assertTrue(r.outcome(h, 0, CREATE, RELEASED, 0, 0, false));
        v = r.outcomeView(h, 0, CREATE, APPLIED, 708, 0);
        v.marketId = OrderEngine.MARKETS + 1;
        assertTrue(r.apply(v));
        assertTrue(globalHas(r, GateReason.ME_BEHAVIOR));
        checkInvariants(r, "U5b");
    }

    @Test
    public void pendingReplyIncludesReleaseBacklog() {
        // G32: trading dispatches and releases interleave by seq in one page, counted
        final V3Rig r = funded();
        final long a = restingBuy(r, 101);            // seq 1 CREATE (resolved), seq 2 UP_TO a
        final long b = r.create(r.limit(1, 0, PX, ONE)); // seq 3 CREATE b pending
        final long c = deadWithOrderEntry(r);           // c: CREATE, UP_TO, CANCEL, ORDER
        cancel(r, 1, a);                                // a: CANCEL pending
        r.mark();
        r.engine.orders().applyRequestPendingDispatch(7, 0L, 1024);
        final List<AssetsEngineEvent.MeDispatchReply> rep = r.all(AssetsEngineEvent.MeDispatchReply.class);
        long previous = 0;
        for (final AssetsEngineEvent.MeDispatchReply d : rep) {
            assertTrue("ascending by seq", d.dispatchSeq() > previous);
            previous = d.dispatchSeq();
        }
        assertEquals("a's UP_TO, b's CREATE, c's ORDER, a's CANCEL", 4, rep.size());
        assertEquals(DispatchKind.RELEASE_ID, rep.get(0).kind());
        assertEquals(a, rep.get(0).commandIdHigh());
        assertEquals(ReleaseScope.UP_TO, rep.get(0).releaseScope());
        assertEquals(DispatchKind.CREATE, rep.get(1).kind());
        assertEquals(b, rep.get(1).commandIdHigh());
        assertEquals(DispatchKind.RELEASE_ID, rep.get(2).kind());
        assertEquals(c, rep.get(2).commandIdHigh());
        assertEquals(ReleaseScope.ORDER, rep.get(2).releaseScope());
        assertEquals((1L << 2) | 3, rep.get(2).commandIdLow());
        assertEquals(DispatchKind.CANCEL, rep.get(3).kind());
        assertEquals("the end frame counts every entry", 4, r.first(AssetsEngineEvent.PendingDispatchEnd.class).count());
        // a page of 2 from the second entry continues in seq order
        r.mark();
        r.engine.orders().applyRequestPendingDispatch(8, rep.get(0).dispatchSeq(), 2);
        final List<AssetsEngineEvent.MeDispatchReply> page = r.all(AssetsEngineEvent.MeDispatchReply.class);
        assertEquals(2, page.size());
        assertEquals(rep.get(1).dispatchSeq(), page.get(0).dispatchSeq());
        assertEquals(rep.get(2).dispatchSeq(), page.get(1).dispatchSeq());
    }
}

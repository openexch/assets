// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 5.1-5.5 and 3.3: FeedHalt 66 (D3a), the one latch slot (precedence, replace, merge, suppress), the latch event
 * shape and the reach-clear per latch kind.
 */
public class FeedHaltTest {

    private static final int T66 = 66;

    private static V3Rig book() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        // (the cursor stands at 3 here under v3; asserted by the rows themselves, so a base run fails at the rule)
        return r;
    }

    private static void assertAck(final V3Rig r, final String what, final AdminResult result, final int reason) {
        final AssetsEngineEvent.AdminAck ack = r.onlyAck();
        assertEquals(what + ": AdminAck names FeedHalt (66)", T66, ack.refTemplateId());
        assertEquals(what + ": AdminAck result", result, ack.result());
        assertEquals(what + ": AdminAck reason", reason, ack.reason());
        assertEquals(what + ": correlationId echoed", 6_600L, ack.correlationId());
    }

    @Test
    public void appliedStaleDuplicatePerReason() {
        // field rules first: REJECTED INVALID, no latch
        final Object[][] invalid = {
                {"GAP expected 0", FeedHaltReason.GAP, 0L, 0L, 9L},
                {"GAP expected >= seen", FeedHaltReason.GAP, 0L, 9L, 9L},
                {"EPOCH_MISMATCH meEpochSeen 0", FeedHaltReason.EPOCH_MISMATCH, 0L, 4L, 4L},
                {"SOURCE_SCHEMA expected 0", FeedHaltReason.SOURCE_SCHEMA, 0L, 0L, 4L},
        };
        for (final Object[] row : invalid) {
            final V3Rig r = book();
            r.mark();
            r.halt((FeedHaltReason) row[1], (Long) row[2], (Long) row[3], (Long) row[4], 0);
            assertAck(r, (String) row[0], AdminResult.REJECTED, CoreRejectReason.INVALID.code());
            r.assertNotLatched((String) row[0]);
        }

        // GAP: applied, then the same halt again is DUPLICATE (the reason is already latched)
        final V3Rig g = book();
        g.mark();
        g.halt(FeedHaltReason.GAP, 0, 4, 9, 0x77);
        assertAck(g, "GAP{4, 9}", AdminResult.ACCEPTED, 0);
        g.assertLatch("GAP{4, 9}", FeedHaltReason.GAP, 4, 9, 0, 0);
        g.assertGloballyHalted("GAP{4, 9}");
        assertEquals("bridgeIdentity recorded, never enforced", 0x77L, g.feed().latchBridgeIdentity());
        assertEquals(1L, g.feed().haltAppliedCount());
        final AssetsEngineEvent.OrderFault f = g.feedHaltFaults().get(0);
        assertEquals("FeedHaltApplied tradeId = seen", 9L, f.tradeId());
        assertEquals("FeedHaltApplied amount = expected", 4L, f.amount());
        g.mark();
        g.halt(FeedHaltReason.GAP, 0, 4, 9, 0x77);
        assertAck(g, "GAP re-sent on reconnect", AdminResult.DUPLICATE, 0);
        assertEquals("a duplicate halt emits no latch event", 0, g.feedHaltFaults().size());

        // GAP stale: another lane delivered past it (c >= seen - 1)
        final V3Rig gs = book();
        gs.mark();
        gs.halt(FeedHaltReason.GAP, 0, 2, 4, 0);
        assertAck(gs, "GAP{2, 4} with cursor 3", AdminResult.DUPLICATE, 0);
        gs.assertNotLatched("stale GAP");
        assertEquals(1L, gs.feed().haltStaleCount());

        // EPOCH_MISMATCH: stale while unbound and when the bridge names the bound epoch; else latched
        final V3Rig eu = V3Rig.unbound();
        eu.mark();
        eu.halt(FeedHaltReason.EPOCH_MISMATCH, 0x99, 1, 1, 0);
        assertAck(eu, "EPOCH_MISMATCH while unbound", AdminResult.DUPLICATE, 0);
        eu.assertNotLatched("binding is the core's judgment");
        final V3Rig es = book();
        es.mark();
        es.halt(FeedHaltReason.EPOCH_MISMATCH, EPOCH, 4, 4, 0);
        assertAck(es, "EPOCH_MISMATCH naming the bound epoch", AdminResult.DUPLICATE, 0);
        es.assertNotLatched("stale epoch report");
        final V3Rig ea = book();
        ea.mark();
        ea.halt(FeedHaltReason.EPOCH_MISMATCH, 0x99, 4, 4, 0);
        assertAck(ea, "EPOCH_MISMATCH foreign", AdminResult.ACCEPTED, 0);
        ea.assertLatch("EPOCH_MISMATCH foreign", FeedHaltReason.EPOCH_MISMATCH, 4, 4, 0, 0);
        assertEquals(0x99L, ea.feed().latchMeEpochSeen());

        // SOURCE_SCHEMA: stale when c >= expected; else latched skip 0
        final V3Rig ss = book();
        ss.mark();
        ss.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 3, 3, 0);
        assertAck(ss, "SOURCE_SCHEMA{3} with cursor 3", AdminResult.DUPLICATE, 0);
        ss.assertNotLatched("stale SOURCE_SCHEMA");
        final V3Rig sa = book();
        sa.mark();
        sa.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 4, 4, 0);
        assertAck(sa, "SOURCE_SCHEMA{4}", AdminResult.ACCEPTED, 0);
        sa.assertLatch("SOURCE_SCHEMA{4}", FeedHaltReason.SOURCE_SCHEMA, 4, 4, 0, 0);

        // SOURCE_UNBOUND and SETTLE_REFUSED are the core's judgment only: always DUPLICATE, never a latch
        for (final FeedHaltReason reason : new FeedHaltReason[] {FeedHaltReason.SOURCE_UNBOUND, FeedHaltReason.SETTLE_REFUSED}) {
            final V3Rig r = book();
            r.mark();
            r.halt(reason, 0, 4, 9, 0);
            assertAck(r, reason.name(), AdminResult.DUPLICATE, 0);
            r.assertNotLatched(reason.name());
            assertEquals(1L, r.feed().haltDuplicateCount());
        }
    }

    @Test
    public void latchPrecedenceReplaceMergeSuppress() {
        // class 1 GAP, replaced by class 2 SETTLE_REFUSED, replaced by class 3 EPOCH_MISMATCH (which zeroes latchTradeId)
        final V3Rig r = book();
        assertTrue(r.sellHits(1, ONE / 4));
        r.halt(FeedHaltReason.GAP, 0, 5, 9, 0x1);
        r.assertLatch("GAP", FeedHaltReason.GAP, 5, 9, 0, 0);
        assertFalse(r.sellHitsAt(5, EPOCH, r.egress + 1, 7, ONE / 4));
        r.assertLatch("SETTLE_REFUSED replaces GAP", FeedHaltReason.SETTLE_REFUSED, 5, 5, 5, 2L);
        assertEquals("a replacing latch zeroes every field first", 0L, r.feed().latchBridgeIdentity());
        assertFalse(r.sellHitsAt(5, EPOCH + 1, r.egress + 1, 2, ONE / 4));
        r.assertLatch("EPOCH_MISMATCH replaces SETTLE_REFUSED", FeedHaltReason.EPOCH_MISMATCH, 5, 5, 0, 0L);
        // a class-2 latch under a class-3 latch is suppressed
        r.mark();
        r.poison(61, 5, EPOCH, 0);
        r.assertLatch("suppressed", FeedHaltReason.EPOCH_MISMATCH, 5, 5, 0, 0L);
        assertEquals(1L, r.feed().latchSuppressedCount());
        assertEquals("a suppressed latch emits nothing", 0, r.feedHaltFaults().size());

        // same class: a skippable latch replaces a skip-0 one (review NOTE-1)
        final V3Rig s = book();
        s.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 4, 4, 0);
        s.assertLatch("skip-0 SOURCE_SCHEMA", FeedHaltReason.SOURCE_SCHEMA, 4, 4, 0, 0);
        s.poison(63, 4, EPOCH, 0);
        s.assertLatch("skippable replaces skip-0", FeedHaltReason.SOURCE_SCHEMA, 4, 4, 4, 0);

        // same skip: merge, nothing changes, nothing emitted (review 2 BLOCKER-A item 4)
        final V3Rig m = book();
        assertFalse(m.sellHitsAt(4, EPOCH, m.egress + 1, 5, ONE / 4));
        m.assertLatch("SETTLE_REFUSED at 4", FeedHaltReason.SETTLE_REFUSED, 4, 4, 4, 1L);
        m.mark();
        m.poison(61, 4, EPOCH, 123);
        m.assertLatch("merged", FeedHaltReason.SETTLE_REFUSED, 4, 4, 4, 1L);
        assertEquals("a merge emits no latch event", 0, m.feedHaltFaults().size());
        assertEquals(0, m.count(AssetsEngineEvent.FeedLatch.class));

        // a second GAP under a GAP is suppressed (it re-latches on the next out-of-order entry after a clear)
        final V3Rig gg = book();
        assertFalse(gg.sellHitsAt(7, EPOCH, gg.egress + 1, 1, ONE / 4));
        gg.assertLatch("GAP 7", FeedHaltReason.GAP, 4, 7, 0, 0);
        assertFalse(gg.sellHitsAt(9, EPOCH, gg.egress + 1, 1, ONE / 4));
        gg.assertLatch("GAP 9 suppressed", FeedHaltReason.GAP, 4, 7, 0, 0);
        assertEquals(1L, gg.feed().latchSuppressedCount());
    }

    @Test
    public void latchEventAmountIsNeverZero() {
        final String[] names = {"D1 row 1", "D1 row 3", "D1 row 5", "D1 row 6", "D2 row 2", "D2 row 3", "3.4 density",
            "4.2 breach", "5.4 row 1", "5.4 row 2", "66 GAP", "66 EPOCH_MISMATCH expected 0", "66 SOURCE_SCHEMA"};
        for (final String name : names) {
            final V3Rig r = name.startsWith("D2") || name.equals("5.4 row 1") ? V3Rig.unbound() : book();
            r.mark();
            switch (name) {
                case "D1 row 1": r.sellHitsAt(0, EPOCH, 9, 1, ONE); break;
                case "D1 row 3": r.sellHitsAt(4, EPOCH + 1, 9, 1, ONE); break;
                case "D1 row 5": r.sellHitsAt(8, EPOCH, 9, 1, ONE); break;
                case "D1 row 6": r.sellHitsAt(4, EPOCH, 1, 1, ONE); break;
                case "D2 row 2": r.sellHitsAt(1, EPOCH, 9, 1, ONE); break;
                case "D2 row 3": r.sellHitsAt(5, EPOCH, 9, 1, ONE); break;
                case "3.4 density": r.sellHitsAt(4, EPOCH, 9, 4, ONE); break;
                case "4.2 breach": r.configAt(4, EPOCH, 9, 1, 2, 4096, 12, ConfigDecision.BUMP); break;
                case "5.4 row 1": r.poison(68, 1, EPOCH, 0); break;
                case "5.4 row 2": r.poison(61, 4, EPOCH, 0); break;
                case "66 GAP": r.halt(FeedHaltReason.GAP, 0, 4, 8, 0); break;
                case "66 EPOCH_MISMATCH expected 0": r.halt(FeedHaltReason.EPOCH_MISMATCH, 0x42, 0, 0, 0); break;
                case "66 SOURCE_SCHEMA": r.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 4, 4, 0); break;
                default: throw new IllegalStateException(name);
            }
            final List<AssetsEngineEvent.OrderFault> faults = r.feedHaltFaults();
            assertEquals(name + ": exactly one latch event", 1, faults.size());
            assertTrue(name + ": latch event amount >= 1 (a clear has 0), got " + faults.get(0), faults.get(0).amount() >= 1);
            assertEquals(name + ": amount = latchExpectedSeq", r.feed().latchExpectedSeq(), faults.get(0).amount());
            assertEquals(name + ": tradeId = latchSeenSeq", r.feed().latchSeenSeq(), faults.get(0).tradeId());
        }
    }

    @Test
    public void reachClearPerLatchKind() {
        // GAP: clears when the missing range applied (c >= seen - 1)
        final V3Rig g = book();
        assertFalse(g.sellHitsAt(6, EPOCH, 20, 3, ONE / 8));
        g.assertLatch("GAP{4, 6}", FeedHaltReason.GAP, 4, 6, 0, 0);
        assertTrue(g.sellHitsAt(4, EPOCH, 10, 1, ONE / 8));
        g.assertLatch("still GAP at c = 4", FeedHaltReason.GAP, 4, 6, 0, 0);
        g.mark();
        assertTrue(g.sellHitsAt(5, EPOCH, 11, 2, ONE / 8));
        g.assertNotLatched("GAP cleared at c = 5");
        assertEquals(new AssetsEngineEvent.FeedLatchCleared(FeedHaltReason.GAP, 0L, true),
                g.first(AssetsEngineEvent.FeedLatchCleared.class));
        assertEquals("audit tradeId = c", 5L, g.feedHaltFaults().get(0).tradeId());
        assertEquals("audit amount 0", 0L, g.feedHaltFaults().get(0).amount());
        assertTrue("the gate keeps BRIDGE_HALT until ADMIN OPEN", g.globalHas(com.openexchange.assets.domain.GateReason.BRIDGE_HALT));
        assertTrue(g.sellHitsAt(6, EPOCH, 20, 3, ONE / 8));
        assertEquals(6L, g.cursor());

        // skippable (SETTLE_REFUSED skip s): the refused entry applies after all, from a fixed lane
        final V3Rig s = book();
        assertFalse(s.sellHitsAt(4, EPOCH, 10, 9, ONE / 8));
        s.assertLatch("SETTLE_REFUSED", FeedHaltReason.SETTLE_REFUSED, 4, 4, 4, 1L);
        assertTrue(s.sellHitsAt(4, EPOCH, 10, 1, ONE / 8));
        s.assertNotLatched("skippable cleared at c >= skip");

        // skip-0 SOURCE_SCHEMA: another lane delivered the entry the bridge could not read (c >= expected)
        final V3Rig z = book();
        z.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 4, 4, 0);
        assertTrue(z.terminal(1, 101, 1, V3Rig.ME_CANCELLED, 0));
        z.assertNotLatched("skip-0 cleared at c >= expected");

        // EPOCH_MISMATCH never auto-clears, though exactly-next entries keep applying
        final V3Rig e = book();
        assertFalse(e.sellHitsAt(4, EPOCH + 1, 10, 1, ONE / 8));
        assertTrue(e.sellHitsAt(4, EPOCH, 10, 1, ONE / 8));
        assertTrue(e.sellHitsAt(5, EPOCH, 11, 2, ONE / 8));
        assertEquals(5L, e.cursor());
        e.assertLatch("EPOCH_MISMATCH stays", FeedHaltReason.EPOCH_MISMATCH, 4, 4, 0, 0);

        // SOURCE_UNBOUND never auto-clears, not even at a later bind
        final V3Rig u = V3Rig.unbound();
        assertFalse(u.terminalAt(1, EPOCH, 3, 1, 101, 1, V3Rig.ME_CANCELLED, 0));
        u.assertLatch("SOURCE_UNBOUND", FeedHaltReason.SOURCE_UNBOUND, 1, 1, 0, 0);
        assertTrue(u.bindFresh());
        u.assertLatch("SOURCE_UNBOUND stays", FeedHaltReason.SOURCE_UNBOUND, 1, 1, 0, 0);
    }
}

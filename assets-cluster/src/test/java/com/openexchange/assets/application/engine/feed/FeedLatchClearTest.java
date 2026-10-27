// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 5.6 (D3b, total) and decisions D-2 / review 2 BLOCKER-A: FeedLatchClear 76 clears a latch, or skips exactly the
 * one entry the core itself refused or found poisoned at {@code c + 1}, consuming exactly one tradeId ({@code L + 1})
 * whatever the refused frame carried. Nothing skips an entry the core never saw.
 */
public class FeedLatchClearTest {

    private static final int T76 = 76;

    private static V3Rig book() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        // (the cursor stands at 3 here under v3; asserted by the rows themselves, so a base run fails at the rule)
        return r;
    }

    private static AssetsEngineEvent.AdminAck clear(final V3Rig r, final FeedHaltReason reason, final long js) {
        r.mark();
        r.clear(reason, js);
        final AssetsEngineEvent.AdminAck ack = r.onlyAck();
        assertEquals("AdminAck names FeedLatchClear (76)", T76, ack.refTemplateId());
        return ack;
    }

    private static void assertAccepted(final V3Rig r, final AssetsEngineEvent.AdminAck ack, final String what, final long js) {
        assertEquals(what + ": ACCEPTED", AdminResult.ACCEPTED, ack.result());
        r.assertNotLatched(what);
        final List<AssetsEngineEvent.OrderFault> f = r.feedHaltFaults();
        assertEquals(what + ": one audit OrderFault", 1, f.size());
        assertEquals(what + ": audit tradeId = journalSeq", js, f.get(0).tradeId());
        assertEquals(what + ": audit amount 0", 0L, f.get(0).amount());
        r.assertGloballyHalted(what + " (the gate stays until ADMIN OPEN)");
    }

    private static void assertInvalid(final AssetsEngineEvent.AdminAck ack, final String what) {
        assertEquals(what + ": REJECTED", AdminResult.REJECTED, ack.result());
        assertEquals(what + ": reason INVALID", CoreRejectReason.INVALID.code(), ack.reason());
    }

    @Test
    public void everyRowOfD3b() {
        // row 1: genesis-only reasons are never cleared by 76
        final V3Rig e = book();
        assertFalse(e.sellHitsAt(4, EPOCH + 1, 9, 1, ONE));
        assertInvalid(clear(e, FeedHaltReason.EPOCH_MISMATCH, 0), "row 1 EPOCH_MISMATCH");
        assertInvalid(clear(e, FeedHaltReason.SOURCE_UNBOUND, 0), "row 1 SOURCE_UNBOUND (not latched either)");
        e.assertLatch("row 1 leaves the latch", FeedHaltReason.EPOCH_MISMATCH, 4, 4, 0, 0);

        // row 2: not latched, or another reason
        final V3Rig n = book();
        assertEquals("row 2 not latched", AdminResult.DUPLICATE, clear(n, FeedHaltReason.GAP, 0).result());
        assertFalse(n.sellHitsAt(6, EPOCH, 9, 1, ONE));
        assertEquals("row 2 other reason", AdminResult.DUPLICATE, clear(n, FeedHaltReason.SOURCE_SCHEMA, 0).result());
        n.assertLatch("row 2 leaves the latch", FeedHaltReason.GAP, 4, 6, 0, 0);

        // row 3: GAP with journalSeq 0 clears, cursor unchanged
        assertAccepted(n, clear(n, FeedHaltReason.GAP, 0), "row 3", 0);
        assertEquals("row 3: cursor unchanged", 3L, n.cursor());
        assertEquals(new AssetsEngineEvent.FeedLatchCleared(FeedHaltReason.GAP, 0L, false),
                n.first(AssetsEngineEvent.FeedLatchCleared.class));
        assertEquals(1L, n.feed().latchClearCount());

        // row 4: a skip-0 SOURCE_SCHEMA with journalSeq 0 clears, cursor unchanged
        final V3Rig z = book();
        z.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 4, 4, 0);
        assertAccepted(z, clear(z, FeedHaltReason.SOURCE_SCHEMA, 0), "row 4", 0);
        assertEquals(3L, z.cursor());

        // row 5: the skippable entry, journalSeq == latchSkipSeq (== c + 1)
        final V3Rig s = book();
        assertFalse(s.sellHitsAt(4, EPOCH, 9, 5, ONE));
        assertAccepted(s, clear(s, FeedHaltReason.SETTLE_REFUSED, 4), "row 5", 4);
        assertEquals("row 5: c := journalSeq", 4L, s.cursor());
        assertEquals("row 5: L := latchTradeId", 1L, s.engine.getLastAppliedTradeId());
        assertEquals("row 5: P is not moved by a skip", 3L, s.engine.getConsumePosition());
        assertEquals(4L, s.feed().lastSkippedJournalSeq());

        // row 6: every "otherwise" shape is REJECTED INVALID and changes nothing
        final V3Rig g6 = book();
        assertFalse(g6.sellHitsAt(6, EPOCH, 9, 1, ONE));
        assertInvalid(clear(g6, FeedHaltReason.GAP, 5), "row 6 GAP with journalSeq != 0");
        assertEquals(3L, g6.cursor());
        final V3Rig k6 = book();
        assertFalse(k6.sellHitsAt(4, EPOCH, 9, 5, ONE));
        assertInvalid(clear(k6, FeedHaltReason.SETTLE_REFUSED, 5), "row 6 skippable named with another seq");
        assertInvalid(clear(k6, FeedHaltReason.SETTLE_REFUSED, 0), "row 6 skippable named with 0");
        k6.assertLatch("row 6 leaves the latch", FeedHaltReason.SETTLE_REFUSED, 4, 4, 4, 1L);
        final V3Rig z6 = book();
        z6.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 4, 4, 0);
        assertInvalid(clear(z6, FeedHaltReason.SOURCE_SCHEMA, 4), "row 6 skip-0 named with journalSeq != 0");
        assertEquals(3L, z6.cursor());

        // FIX-G: a skip followed by a valid resend of the same entry is a DUPLICATE, counted
        final V3Rig v = book();
        v.poison(63, 4, EPOCH, 0);
        assertAccepted(v, clear(v, FeedHaltReason.SOURCE_SCHEMA, 4), "skip of a poisoned 63", 4);
        assertTrue("the valid resend is a DUPLICATE", v.terminalAt(4, EPOCH, 9, 1, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertEquals(1L, v.feed().skippedEntryRedeliveredCount());
        assertNotNull("the skipped terminal never applied (the runbook's quarantine case)", v.order(1));
    }

    @Test
    public void gapClearNeverMovesTheCursor() {
        final V3Rig r = book();
        assertFalse(r.sellHitsAt(9, EPOCH, 9, 1, ONE));
        r.assertLatch("GAP{4, 9}", FeedHaltReason.GAP, 4, 9, 0, 0);
        assertInvalid(clear(r, FeedHaltReason.GAP, 8), "a 76 never moves the cursor over a GAP");
        assertEquals(3L, r.cursor());
        assertEquals(AdminResult.ACCEPTED, clear(r, FeedHaltReason.GAP, 0).result());
        assertEquals("the GAP clear leaves the cursor where it is", 3L, r.cursor());
        // the lost entries were not recovered: the entry past the hole still refuses (and re-latches); only the
        // missing entries applying, or a dual genesis, can move past it (D-2)
        r.mark();
        assertFalse(r.sellHitsAt(9, EPOCH, 9, 1, ONE));
        r.assertLatch("re-latched", FeedHaltReason.GAP, 4, 9, 0, 0);
        assertEquals(0, r.count(AssetsEngineEvent.Settled.class));
    }

    @Test
    public void skipConsumesTheRefusedTradeIdOnly() {
        final V3Rig r = book();
        assertTrue(r.sellHits(1, ONE / 8));
        assertFalse("density refusal", r.sellHitsAt(5, EPOCH, r.egress + 1, 3, ONE / 8));
        r.assertLatch("SETTLE_REFUSED", FeedHaltReason.SETTLE_REFUSED, 5, 5, 5, 2L);
        assertEquals(AdminResult.ACCEPTED, clear(r, FeedHaltReason.SETTLE_REFUSED, 5).result());
        assertEquals(5L, r.cursor());
        assertEquals("exactly L + 1 consumed", 2L, r.engine.getLastAppliedTradeId());
        r.mark();
        assertTrue("the next valid trade L + 2 applies", r.sellHitsAt(6, EPOCH, r.egress + 1, 3, ONE / 8));
        assertEquals(1, r.count(AssetsEngineEvent.Settled.class));
        r.assertNotLatched("after the skip");
    }

    @Test
    public void skipConsumesExactlyOneTradeIdWhateverTheFrameSays() {
        final String[] kinds = {"poisoned 61", "poisoned 62 entry", "density-refused 61"};
        for (final String kind : kinds) {
            for (final int rawCase : new int[] {0, 1, 2, 3}) {
                final V3Rig r = book();
                assertTrue(r.sellHits(1, ONE / 8));
                assertTrue(r.sellHits(2, ONE / 8));
                final long l = r.engine.getLastAppliedTradeId();
                final long raw = rawCase == 0 ? 0L : rawCase == 1 ? l : rawCase == 2 ? l + 2 : 1L << 62;
                final long s = r.cursor() + 1;
                final String what = kind + " raw tradeId " + raw;
                switch (kind) {
                    case "poisoned 61": r.poison(61, s, EPOCH, raw); break;
                    case "poisoned 62 entry": r.poison(62, s, EPOCH, raw); break;
                    default: assertFalse(what, r.sellHitsAt(s, EPOCH, r.egress + 1, raw, ONE / 8)); break;
                }
                assertEquals(what + ": latchTradeId = L + 1, never the frame's", l + 1, r.feed().latchTradeId());
                assertEquals(what + ": skip", s, r.feed().latchSkipSeq());
                assertEquals(what + ": ACCEPTED", AdminResult.ACCEPTED, clear(r, r.feed().latchReason(), s).result());
                assertEquals(what + ": L equals old L + 1", l + 1, r.engine.getLastAppliedTradeId());
                r.mark();
                assertTrue(what + ": the next valid trade L + 2 applies", r.sellHitsAt(s + 1, EPOCH, r.egress + 1, l + 2, ONE / 8));
                r.assertNotLatched(what + ": no latch after the next valid trade");
                assertEquals(what, 1, r.count(AssetsEngineEvent.Settled.class));
            }
        }
    }

    @Test
    public void skippableEntryAppliedLaterClearsLatch() {
        final V3Rig r = book();
        // poisoned s from bridge A
        r.poison(63, 4, EPOCH, 0);
        r.assertLatch("poisoned 63 from lane A", FeedHaltReason.SOURCE_SCHEMA, 4, 4, 4, 0);
        // valid s from bridge B
        assertTrue(r.terminalAt(4, EPOCH, 9, 1, 101, 1, V3Rig.ME_CANCELLED, 0));
        r.assertNotLatched("the skippable entry applied: reach-clear");
        assertEquals(4L, r.cursor());
        // the snapshot restores (every 2.1 invariant holds), and a 76 naming s is DUPLICATE
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(r.engine, buf, 0);
        final AssetsEngine fresh = new AssetsEngine();
        V3Genesis.widen(fresh);
        fresh.setEventSink(new RecordingAssetsSink());
        BalanceSnapshotCodec.deserialize(buf, 0, len, fresh);
        fresh.verifyRestoredInvariants();
        assertEquals(4L, fresh.feed().lastAppliedJournalSeq());
        assertEquals(EPOCH, fresh.feed().boundMeEpoch());
        assertEquals(FeedHaltReason.NONE, fresh.feed().latchReason());
        assertEquals(AdminResult.DUPLICATE, clear(r, FeedHaltReason.SOURCE_SCHEMA, 4).result());
        assertEquals(4L, r.cursor());
    }
}

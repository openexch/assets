// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.BTC;
import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static com.openexchange.assets.application.engine.feed.V3Rig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 contract 3.8 (review N-1, the duplicate belt): an entry at or below the cursor is never swallowed silently. It
 * must byte-match what was applied at that journalSeq (the FRESH record for the epoch's life, the last
 * {@link FeedState#DIGEST_WINDOW} entries in a ring); otherwise it is refused and latched with a reason: EPOCH_MISMATCH
 * for another FRESH under the bound epoch (an ME re-genesised or restored reusing the epoch, genesis only), SOURCE_SCHEMA
 * skip 0 for any other entry (an in-epoch ME rewind or a corrupt copy). The cursor, the money and every order stay put.
 */
public class FeedDuplicateBeltTest {

    private static V3Rig book() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        assertTrue(r.sellHitsAt(4, EPOCH, 40, 1, ONE / 4));
        assertEquals(4L, r.cursor());
        return r;
    }

    private static long[] money(final V3Rig r) {
        return new long[] {r.total(1, USD), r.locked(1, USD), r.total(2, BTC), r.locked(2, BTC), r.engine.getLastAppliedTradeId(),
            r.engine.getConsumePosition(), r.cursor()};
    }

    private static void assertRefusedLoudly(final V3Rig r, final String what, final long[] before, final FeedHaltReason reason,
                                            final long seen) {
        assertEquals(what + ": mismatch counted", 1L, r.feed().duplicateMismatchCount());
        r.assertLatch(what, reason, 5, seen, 0, 0);
        r.assertGloballyHalted(what);
        assertEquals(what + ": one latch event", 1, r.feedHaltFaults().size());
        assertEquals(what + ": no settle", 0, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(what + ": no order event", 0, r.count(AssetsEngineEvent.OrderEvent.class));
        org.junit.Assert.assertArrayEquals(what + ": cursor, money, high-water and watermark unchanged", before, money(r));
    }

    @Test
    public void aDuplicateWithOtherContentIsRefusedAndLatched() {
        // the same tradeId re-sent with another quantity at its own journalSeq (61, then as a 62 entry)
        final V3Rig a = book();
        final long[] before = money(a);
        a.mark();
        assertFalse("a mismatching duplicate is a refusal", a.sellHitsAt(4, EPOCH, 40, 1, ONE / 2));
        assertRefusedLoudly(a, "61 other quantity", before, FeedHaltReason.SOURCE_SCHEMA, 4);
        assertEquals(new AssetsEngineEvent.FeedLatch(FeedHaltReason.SOURCE_SCHEMA, 5, 4, 0, 1),
                a.first(AssetsEngineEvent.FeedLatch.class));
        // the next exactly-next entry from a sound lane applies and reach-clears; the BRIDGE_HALT bit waits for ADMIN OPEN
        assertTrue(a.sellHitsAt(5, EPOCH, 50, 2, ONE / 4));
        a.assertNotLatched("reach-clear at the next applied entry");

        final V3Rig b = book();
        final long[] beforeB = money(b);
        b.mark();
        b.dispatchTrusted(b.tradeBatch(List.of(new V3Rig.BatchEntry(4, EPOCH, 40, 1, 0, ONE / 3),
                new V3Rig.BatchEntry(5, EPOCH, 50, 2, 0, ONE / 4))));
        assertRefusedLoudly(b, "62 entry other quantity", beforeB, FeedHaltReason.SOURCE_SCHEMA, 4);
        assertEquals("the batch stops at the refused duplicate", 4L, b.cursor());

        // one row per other template: 63, 64, 67, 68 at the journalSeq where something else was applied
        for (final String t : new String[] {"63", "64", "67", "68", "64 other egressSeq"}) {
            final V3Rig r = book();
            final long[] beforeR = money(r);
            r.mark();
            final boolean verdict = switch (t) {
                case "63" -> r.terminalAt(3, EPOCH, 3, 2, 202, 2, V3Rig.ME_CANCELLED, 0);
                case "64" -> r.outcomeAt(3, EPOCH, 3, 2, 0, V3Rig.KIND_CREATE, 0, 999, 0, false);
                case "67" -> r.releaseAt(2, EPOCH, 2);
                case "68" -> r.configAt(3, EPOCH, 3, 2, 2, 4096, 12, ConfigDecision.BUMP);
                default -> r.outcomeAt(3, EPOCH, 33, 2, 0, V3Rig.KIND_CREATE, 0, 202, 0, false);
            };
            assertFalse(t + ": refused", verdict);
            assertRefusedLoudly(r, t, beforeR, FeedHaltReason.SOURCE_SCHEMA, t.equals("67") ? 2 : 3);
            assertNotNull(t + ": the order keeps its leg", r.order(2));
            assertEquals(202L, r.order(2).currentLegId);
        }
    }

    /** Belt review D-1: at seq 1 only a mismatching 68 FRESH is genesis-only; any other template is SOURCE_SCHEMA skip 0. */
    @Test
    public void aNonFreshEntryLabelledSeqOneIsSourceSchemaNotGenesis() {
        for (final String t : new String[] {"61", "63", "64", "67", "68 BUMP"}) {
            final V3Rig r = book();
            final long[] before = money(r);
            r.mark();
            final boolean verdict = switch (t) {
                case "61" -> r.sellHitsAt(1, EPOCH, 1, 1, ONE / 4);
                case "63" -> r.terminalAt(1, EPOCH, 1, 1, 101, 1, V3Rig.ME_CANCELLED, 0);
                case "64" -> r.outcomeAt(1, EPOCH, 1, 1, 0, V3Rig.KIND_CREATE, 0, 101, 0, false);
                case "67" -> r.releaseAt(1, EPOCH, 1);
                default -> r.configAt(1, EPOCH, 1, 2, 2, 4096, 12, ConfigDecision.BUMP);
            };
            assertFalse(t + " at seq 1: refused", verdict);
            assertRefusedLoudly(r, t + " at seq 1", before, FeedHaltReason.SOURCE_SCHEMA, 1);
            assertTrue(t + ": the next exactly-next entry applies", r.sellHitsAt(5, EPOCH, 50, 2, ONE / 4));
            r.assertNotLatched(t + ": reach-cleared, no genesis needed");
        }
    }

    @Test
    public void anIdenticalDuplicateIsStillANoOp() {
        final V3Rig r = book();
        final long[] before = money(r);
        r.mark();
        assertTrue(r.sellHitsAt(4, EPOCH, 40, 1, ONE / 4));                                    // as 61
        r.dispatchTrusted(r.tradeBatch(List.of(new V3Rig.BatchEntry(4, EPOCH, 40, 1, 0, ONE / 4)))); // as a 62 entry
        assertTrue(r.outcomeAt(3, EPOCH, 3, 2, 0, V3Rig.KIND_CREATE, 0, 202, 0, false));     // the ack, verbatim
        assertTrue(r.configAt(1, EPOCH, 1, 1, 2, 4096, 12, ConfigDecision.FRESH));         // the FRESH record, verbatim
        assertEquals("no event", 0, r.events().size());
        r.assertNotLatched("verbatim duplicates");
        assertEquals(0L, r.feed().duplicateMismatchCount());
        org.junit.Assert.assertArrayEquals(before, money(r));
    }

    @Test
    public void aMeReGenesisReusingTheEpochIsNotSwallowed() {
        final V3Rig r = book();
        final long[] before = money(r);
        r.mark();
        // a new FRESH under the same epoch (the journal restarted at 1, another egressSeq): genesis only
        assertFalse(r.configAt(1, EPOCH, 99, 1, 2, 4096, 12, ConfigDecision.FRESH));
        assertEquals(1L, r.feed().duplicateMismatchCount());
        r.assertLatch("re-genesis reusing the epoch", FeedHaltReason.EPOCH_MISMATCH, 5, 1, 0, 0);
        org.junit.Assert.assertArrayEquals(before, money(r));
        // its next records reuse applied seqs with other content: refused too (suppressed under the class-3 latch)
        assertFalse(r.terminalAt(2, EPOCH, 100, 1, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertFalse(r.sellHitsAt(3, EPOCH, 101, 1, ONE));
        assertEquals(3L, r.feed().duplicateMismatchCount());
        assertNotNull("the live order is not terminated", r.order(1));
        org.junit.Assert.assertArrayEquals(before, money(r));
        r.assertLatch("still genesis only", FeedHaltReason.EPOCH_MISMATCH, 5, 1, 0, 0);
    }

    @Test
    public void theFreshRecordIsCheckedForTheEpochsLifeAndTheWindowSurvivesARestore() {
        final V3Rig r = book();
        // a snapshot round trip: the FRESH digest and the window come back
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(r.engine, buf, 0);
        final AssetsEngine back = new AssetsEngine();
        V3Genesis.widen(back);
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        back.setEventSink(sink);
        BalanceSnapshotCodec.deserialize(buf, 0, len, back);
        back.verifyRestoredInvariants();
        assertFalse("the restored window still compares", back.onMeTrade(40, 4, 1, V3Rig.MKT, 202, 2, 101, 1, 100 * ONE, ONE,
                false, r.now, 2, 1, EPOCH, r.now));
        assertEquals(FeedHaltReason.SOURCE_SCHEMA, back.feed().latchReason());
        assertFalse("the FRESH record too", back.onMeEngineConfig(77, 1, EPOCH, 1, 2, 4096, 12, ConfigDecision.FRESH, r.now,
                id -> { }));
        assertEquals("EPOCH_MISMATCH replaces the class-2 latch", FeedHaltReason.EPOCH_MISMATCH, back.feed().latchReason());
    }

    @Test
    public void aSkippedEntryIsUnknownAndAnEntryOlderThanTheWindowIsCounted() {
        // a 76-skipped entry has no known content: a later copy is the FIX-G case (counted), never a mismatch latch
        final V3Rig s = book();
        s.poison(63, 5, EPOCH, 0);
        s.clear(FeedHaltReason.SOURCE_SCHEMA, 5);
        assertTrue(s.terminalAt(5, EPOCH, 50, 1, 101, 1, V3Rig.ME_CANCELLED, 0));
        s.assertNotLatched("a copy of a skipped entry");
        assertEquals(1L, s.feed().skippedEntryRedeliveredCount());
        assertEquals(0L, s.feed().duplicateMismatchCount());

        // past the window (DIGEST_WINDOW entries later), an old journalSeq can no longer be compared: counted, not applied
        final V3Rig w = book();
        for (long js = 5; js <= 4 + FeedState.DIGEST_WINDOW; js++) {
            assertTrue(w.releaseAt(js, EPOCH, js * 10));
        }
        assertEquals((long) FeedState.DIGEST_WINDOW, w.feed().digestCount());
        assertFalse("journalSeq 4 left the window", w.feed().digestHeld(4));
        w.mark();
        assertTrue(w.sellHitsAt(4, EPOCH, 40, 1, ONE));
        assertEquals(1L, w.feed().duplicateUnverifiedCount());
        assertEquals("never applied twice", 0, w.count(AssetsEngineEvent.Settled.class));
        w.assertNotLatched("an unverifiable duplicate");
        // the window's oldest held entry still compares
        final long oldest = w.cursor() - FeedState.DIGEST_WINDOW + 1;
        assertFalse(w.releaseAt(oldest, EPOCH, oldest * 10 + 1));
        assertEquals(1L, w.feed().duplicateMismatchCount());
    }

    // RC2 S8 7 G8 (3.10 item 2): the widened digests: a copy at an applied journalSeq that differs only in the 64 tail
    // (ledgerSize, status) or in one 67 entry is a mismatch, latched SOURCE_SCHEMA, no longer a silent DUPLICATE
    @Test
    public void widenedDigestRefusesTailAndGroupCopies() {
        for (final String field : new String[] {"ledgerSize", "status", "tif", "eor", "behavior"}) {
            final V3Rig r = V3Rig.open();
            r.deposit(1, USD, 1_000_000 * ONE);
            final long h = r.create(r.limit(1, 0, 100 * ONE, ONE));
            final com.openexchange.assets.application.engine.order.MeOutcomeView v = r.outcomeView(h, 0, V3Rig.KIND_CREATE, 0, 101, 0);
            assertTrue(r.apply(v));
            final long applied = v.journalSeq;
            final com.openexchange.assets.application.engine.order.MeOutcomeView copy = r.outcomeView(h, 0, V3Rig.KIND_CREATE, 0, 101, 0);
            copy.journalSeq = applied;
            copy.egressSeq = v.egressSeq;
            copy.appliedPosition = v.appliedPosition;
            copy.orderType = v.orderType;
            copy.orderSide = v.orderSide;
            copy.timeInForce = v.timeInForce;
            copy.expectedOldRemaining = v.expectedOldRemaining;
            copy.ledgerSize = v.ledgerSize;
            copy.status = v.status;
            copy.behaviorVersion = v.behaviorVersion;
            switch (field) {
                case "ledgerSize": copy.ledgerSize = v.ledgerSize + 1; break;
                case "status": copy.status = 3; break;
                case "tif": copy.timeInForce = 1; break;
                case "eor": copy.expectedOldRemaining = 1; break;
                default: copy.behaviorVersion = 3; break;
            }
            assertFalse(field + ": refused", r.apply(copy));
            assertEquals(field + ": a mismatch", 1L, r.feed().duplicateMismatchCount());
            assertEquals(field + ": latched", FeedHaltReason.SOURCE_SCHEMA, r.feed().latchReason());
            // the byte-equal copy stays a DUPLICATE
            final V3Rig q = V3Rig.open();
            q.deposit(1, USD, 1_000_000 * ONE);
            final long g = q.create(q.limit(1, 0, 100 * ONE, ONE));
            final com.openexchange.assets.application.engine.order.MeOutcomeView w = q.outcomeView(g, 0, V3Rig.KIND_CREATE, 0, 101, 0);
            assertTrue(q.apply(w));
            assertTrue(field + ": an equal copy is a DUPLICATE", q.apply(w));
            assertEquals(0L, q.feed().duplicateMismatchCount());
        }
        // a 67 copy differing in one group entry
        final V3Rig r = V3Rig.open();
        final com.openexchange.assets.application.engine.order.MeReleaseView v = r.releaseView(new long[] {5, 0, 0, 0}, new long[] {6, 0, 0, 1});
        assertTrue(r.apply(v));
        final com.openexchange.assets.application.engine.order.MeReleaseView copy = new com.openexchange.assets.application.engine.order.MeReleaseView();
        copy.egressSeq = v.egressSeq;
        copy.journalSeq = v.journalSeq;
        copy.meEpoch = v.meEpoch;
        copy.releaseSeq = v.releaseSeq;
        copy.ledgerSize = v.ledgerSize;
        copy.ledgerCapacity = v.ledgerCapacity;
        copy.add(5, 0, 0, 0, 1).add(6, 0, 0, 1, 2); // the second entry's removed differs
        assertFalse(r.apply(copy));
        assertEquals(1L, r.feed().duplicateMismatchCount());
        assertEquals(FeedHaltReason.SOURCE_SCHEMA, r.feed().latchReason());
    }
}

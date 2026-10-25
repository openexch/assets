// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Round-trip fidelity for the v4 snapshot payload (RC1 S2, layout page
 * {@code RC1-core/S2/snapshot-v4-layout.md}): {@code deserialize(serialize(state))} restores an
 * identical engine, every scalar rides, sections are tagged and length-delimited, and anything this
 * build did not write (older layouts, unknown sections, foreign flags) is refused rather than guessed.
 */
public class BalanceSnapshotCodecTest {

    private static final int N = Asset.count();

    private static AssetsEngine engineWithState() {
        AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(List.of(
                "CLOCK 5000",
                "DEPOSIT u=100 asset=1 amt=2.0",
                "DEPOSIT u=200 asset=0 amt=200000.0",
                "DEPOSIT u=300 asset=2 amt=10.0",
                "HOLD order=1 u=100 asset=1 amt=1.5",
                "HOLD order=2 u=200 asset=0 amt=120000.0",
                // partial settle so a residual hold + a nonzero high-water both exist in the snapshot
                "SETTLE tradeId=7 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=1.0 takerBuy=1"));
        return r.engine();
    }

    private static int serialize(AssetsEngine e, ExpandableArrayBuffer buf) {
        return BalanceSnapshotCodec.serialize(e, buf);
    }

    @Test
    public void roundTripPreservesFullState() {
        AssetsEngine orig = engineWithState();
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = serialize(orig, buf);
        assertEquals("v4 tag leads the payload", BalanceSnapshotCodec.LAYOUT_V4_TAG, buf.getLong(0));

        AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(buf, 0, len, restored);
        assertEquals("bytesConsumed", len, d.bytesConsumed);
        assertEquals("accountCount", orig.accountCount(), restored.accountCount());
        assertSameState(orig, restored);
    }

    @Test
    public void everyScalarRides() {
        AssetsEngine orig = engineWithState();
        orig.setConsumePosition(123456789L);
        orig.setJournalSeq(4242L);
        orig.setMoneyJournalEnabled(true);
        orig.setLastLogTime(777_000L);
        orig.setNextOrderId(91L);
        orig.setEventSeq(92L);
        orig.setDispatchSeq(93L);

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = serialize(orig, buf);
        AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(buf, 0, len, restored);

        assertEquals(7L, restored.getLastAppliedTradeId());
        assertEquals(123456789L, restored.getConsumePosition());
        assertEquals(4242L, restored.getJournalSeq());
        assertTrue(restored.isMoneyJournalEnabled());
        assertEquals(777_000L, restored.getLastLogTime());
        assertEquals(91L, restored.getNextOrderId());
        assertEquals(92L, restored.getEventSeq());
        assertEquals(93L, restored.getDispatchSeq());
        assertEquals(4242L, d.journalSeq);
        assertEquals(len, d.bytesConsumed);
    }

    @Test
    public void journalEnabledSurvivesRoundTripBothWays() {
        for (final boolean enabled : new boolean[] {true, false}) {
            AssetsEngine orig = engineWithState();
            orig.setMoneyJournalEnabled(enabled);
            ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
            int len = serialize(orig, buf);
            AssetsEngine restored = new AssetsEngine();
            BalanceSnapshotCodec.deserialize(buf, 0, len, restored);
            assertEquals("journalEnabled=" + enabled, enabled, restored.isMoneyJournalEnabled());
        }
    }

    @Test
    public void deserializeHonoursNonZeroOffsetAndSerializeWritesAtOffset() {
        AssetsEngine orig = engineWithState();
        ExpandableArrayBuffer at0 = new ExpandableArrayBuffer();
        int len = serialize(orig, at0);
        ExpandableArrayBuffer at64 = new ExpandableArrayBuffer();
        int len64 = BalanceSnapshotCodec.serialize(orig, at64, 64);
        assertEquals("payload length is offset-independent", len, len64);
        for (int i = 0; i < len; i++) {
            assertEquals("byte " + i, at0.getByte(i), at64.getByte(64 + i));
        }
        AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(at64, 64, len, restored);
        assertEquals(len, d.bytesConsumed);
        assertSameState(orig, restored);
    }

    // ---- refusals: this build reads exactly what it writes ----

    @Test
    public void olderLayoutTagsAreRefused() {
        for (final long tag : new long[] {-3L, -2L}) {
            ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
            int p = 0;
            buf.putLong(p, tag); p += 8;
            buf.putLong(p, 7L); p += 8;            // lastAppliedTradeId
            buf.putLong(p, 1L); p += 8;            // consumePosition
            buf.putLong(p, 0L); p += 8;            // journalSeq
            if (tag == -3L) { buf.putByte(p, (byte) 0); p += 1; }
            buf.putInt(p, 0); p += 4;              // numAccounts
            final int length = p;
            final IllegalStateException e = assertThrows("tag " + tag, IllegalStateException.class,
                    () -> BalanceSnapshotCodec.deserialize(buf, 0, length, new AssetsEngine()));
            assertTrue(e.getMessage(), e.getMessage().contains("layout"));
        }
    }

    @Test
    public void anUntaggedV1ImageIsRefusedNotGuessed() {
        // v1 started with lastAppliedTradeId (never negative); it used to be accepted with journalSeq=0.
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putLong(0, 7L);
        buf.putLong(8, 123L);
        buf.putInt(16, 0);
        assertThrows(IllegalStateException.class,
                () -> BalanceSnapshotCodec.deserialize(buf, 0, 20, new AssetsEngine()));
    }

    @Test
    public void anUnknownSectionIsRefused() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = serialize(engineWithState(), buf);
        // Append a section id this build does not know, with a well-formed length.
        final int sectionCountOffset = sectionCountOffset();
        final int count = buf.getInt(sectionCountOffset);
        buf.putInt(sectionCountOffset, count + 1);
        buf.putShort(len, (short) 99);
        buf.putInt(len + 2, 8);
        buf.putLong(len + 6, 0xDEADBEEFL);
        final int total = len + 6 + 8;
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> BalanceSnapshotCodec.deserialize(buf, 0, total, new AssetsEngine()));
        assertTrue(e.getMessage(), e.getMessage().contains("section"));
    }

    @Test
    public void aSectionWhoseLengthDoesNotMatchItsContentIsRefused() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = serialize(engineWithState(), buf);
        final int firstSectionLengthOffset = sectionCountOffset() + 4 + 2;
        buf.putInt(firstSectionLengthOffset, buf.getInt(firstSectionLengthOffset) + 1);
        assertThrows(IllegalStateException.class,
                () -> BalanceSnapshotCodec.deserialize(buf, 0, len, new AssetsEngine()));
    }

    @Test
    public void anUnknownFlagBitIsRefused() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = serialize(engineWithState(), buf);
        final int flagsOffset = 8 + 7 * 8;
        buf.putByte(flagsOffset, (byte) (buf.getByte(flagsOffset) | 0x40));
        assertThrows(IllegalStateException.class,
                () -> BalanceSnapshotCodec.deserialize(buf, 0, len, new AssetsEngine()));
    }

    @Test
    public void anImageWithNoSectionsIsAnEmptyLedger() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int p = 0;
        buf.putLong(p, BalanceSnapshotCodec.LAYOUT_V4_TAG); p += 8;
        for (int i = 0; i < 7; i++) { buf.putLong(p, 0L); p += 8; }
        buf.putByte(p, (byte) 0); p += 1;
        buf.putInt(p, 0); p += 4;
        AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(buf, 0, p, restored);
        assertEquals(0, restored.accountCount());
        assertEquals(p, d.bytesConsumed);
    }

    /** Offset of {@code sectionCount}: tag (8) + 7 scalars (56) + flags (1). */
    private static int sectionCountOffset() {
        return 8 + 7 * 8 + 1;
    }

    private static void assertSameState(AssetsEngine a, AssetsEngine b) {
        assertEquals(a.getLastAppliedTradeId(), b.getLastAppliedTradeId());
        assertEquals(a.getConsumePosition(), b.getConsumePosition());
        assertEquals(a.getJournalSeq(), b.getJournalSeq());
        assertEquals(a.getLastLogTime(), b.getLastLogTime());
        assertEquals(a.accountCount(), b.accountCount());
        a.forEachAccount(acc -> {
            Account bacc = b.account(acc.userId());
            assertNotNull("missing account " + acc.userId(), bacc);
            for (int as = 0; as < N; as++) {
                assertEquals("available user " + acc.userId() + " asset " + as, acc.available(as), bacc.available(as));
                assertEquals("locked user " + acc.userId() + " asset " + as, acc.locked(as), bacc.locked(as));
            }
            assertEquals("holdCount user " + acc.userId(), acc.holdCount(), bacc.holdCount());
            acc.forEachHold((orderId, assetId, remaining, oms) -> {
                assertEquals("hold asset " + orderId, assetId, bacc.holdAssetId(orderId));
                assertEquals("hold remaining " + orderId, remaining, bacc.holdRemaining(orderId));
                assertEquals("hold omsManagedRelease " + orderId, oms, bacc.isOmsManagedRelease(orderId));
            });
        });
    }
}

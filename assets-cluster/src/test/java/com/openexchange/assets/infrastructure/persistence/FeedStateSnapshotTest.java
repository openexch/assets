// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.feed.FeedState;
import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Arrays;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S7 snapshot rules (3.7, 6.3, 7.3, 14; R-B2-10): section 10 FEED_STATE (92 bytes, written iff v3, mandatory under v3,
 * refused below 3), section 5 layout v2 (10 bytes per gate), section 8 layout v2 (genesisId), and every 2.1 / 6.3 restore
 * invariant refused on breach.
 */
public class FeedStateSnapshotTest {

    /** Payload header: tag, 7 scalars, flags, sectionCount. */
    private static final int HEADER = 8 + 7 * 8 + 1 + 4;

    static byte[] image(final AssetsEngine engine) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(engine, buf, 0);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    static AssetsEngine restore(final byte[] image) {
        final AssetsEngine fresh = new AssetsEngine();
        V3Genesis.widen(fresh);
        fresh.setEventSink(new RecordingAssetsSink());
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, fresh);
        assertEquals("the whole payload is consumed", image.length, d.bytesConsumed);
        fresh.verifyRestoredInvariants();
        return fresh;
    }

    /** Offset of section {@code id}'s header in {@code image}, or -1. */
    static int section(final byte[] image, final int id) {
        final UnsafeBuffer b = new UnsafeBuffer(image);
        final int count = b.getInt(HEADER - 4);
        int pos = HEADER;
        for (int s = 0; s < count; s++) {
            final int sid = b.getShort(pos);
            final int len = b.getInt(pos + 2);
            if (sid == id) {
                return pos;
            }
            pos += 6 + len;
        }
        return -1;
    }

    static int sectionLength(final byte[] image, final int id) {
        return new UnsafeBuffer(image).getInt(section(image, id) + 2);
    }

    static byte[] removeSection(final byte[] image, final int id) {
        final int at = section(image, id);
        final int len = 6 + sectionLength(image, id);
        final byte[] out = new byte[image.length - len];
        System.arraycopy(image, 0, out, 0, at);
        System.arraycopy(image, at + len, out, at, image.length - at - len);
        final UnsafeBuffer b = new UnsafeBuffer(out);
        b.putInt(HEADER - 4, b.getInt(HEADER - 4) - 1);
        return out;
    }

    static byte[] appendSection(final byte[] image, final int id, final byte[] body) {
        final byte[] out = Arrays.copyOf(image, image.length + 6 + body.length);
        final UnsafeBuffer b = new UnsafeBuffer(out);
        b.putShort(image.length, (short) id);
        b.putInt(image.length + 2, body.length);
        b.putBytes(image.length + 6, body);
        b.putInt(HEADER - 4, b.getInt(HEADER - 4) + 1);
        return out;
    }

    /** A copy of {@code image} with section {@code id}'s body patched at {@code offset}. */
    static byte[] patch(final byte[] image, final int id, final java.util.function.BiConsumer<UnsafeBuffer, Integer> edit) {
        final byte[] out = image.clone();
        edit.accept(new UnsafeBuffer(out), section(out, id) + 6);
        return out;
    }

    static void assertRefused(final String what, final byte[] image) {
        try {
            restore(image);
            fail(what + ": restored, expected a refusal");
        } catch (final IllegalStateException expected) {
            // refused by the codec or by verifyRestoredInvariants
        }
    }

    /** V3, the two-party book, one trade and a density refusal: a SETTLE_REFUSED latch with latchTradeId. */
    private static V3Rig latched() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        assertTrue(r.sellHits(1, ONE / 4));
        r.sellHitsAt(5, EPOCH, r.egress + 1, 7, ONE / 4);
        return r;
    }

    @Test
    public void roundTripAndInvariants() {
        final V3Rig r = latched();
        final FeedState f = r.feed();
        assertEquals("precondition: latched", FeedHaltReason.SETTLE_REFUSED, f.latchReason());
        final byte[] img = image(r.engine);
        assertEquals("section 10 layout v2: the 92-byte head, freshDigest, digestCount and one digest per applied entry",
                FeedState.DIGEST_HEAD_BYTES + 8 * f.digestCount(), sectionLength(img, BalanceSnapshotCodec.SECTION_FEED_STATE));
        assertEquals("FRESH, two acks, one trade", 4, f.digestCount());
        final AssetsEngine back = restore(img);
        final FeedState g = back.feed();
        assertEquals(f.boundMeEpoch(), g.boundMeEpoch());
        assertEquals(f.lastAppliedJournalSeq(), g.lastAppliedJournalSeq());
        assertEquals(f.latchReason(), g.latchReason());
        assertEquals(f.latchExpectedSeq(), g.latchExpectedSeq());
        assertEquals(f.latchSeenSeq(), g.latchSeenSeq());
        assertEquals(f.latchSkipSeq(), g.latchSkipSeq());
        assertEquals(f.latchTradeId(), g.latchTradeId());
        assertEquals(f.latchMeEpochSeen(), g.latchMeEpochSeen());
        assertEquals(f.latchBridgeIdentity(), g.latchBridgeIdentity());
        assertEquals(f.latchLogPosition(), g.latchLogPosition());
        assertEquals(f.meConfigVersion(), g.meConfigVersion());
        assertEquals(f.meBehaviorVersion(), g.meBehaviorVersion());
        assertEquals(f.meLedgerCapacity(), g.meLedgerCapacity());
        assertEquals(f.meMaxWireVersion(), g.meMaxWireVersion());
        assertEquals(f.freshDigest(), g.freshDigest());
        for (long q = 1; q <= f.lastAppliedJournalSeq(); q++) {
            assertEquals("digest of journalSeq " + q, f.digestAt(q), g.digestAt(q));
        }
        for (int m = 0; m <= OrderEntryGate.MARKETS; m++) {
            assertEquals("gate " + m + " reasons", r.engine.gate().reasons(m), back.gate().reasons(m));
        }
        assertEquals(V3Genesis.GENESIS_ID, back.coreConfig().recorded().genesisId);
        assertArrayEquals("the restored engine re-serializes byte-identical", img, image(back));

        final int s10 = BalanceSnapshotCodec.SECTION_FEED_STATE;
        // codec-level refusals: reserved bytes, latchReason above 5, a section of the wrong length
        assertRefused("reserved u8 @17", patch(img, s10, (b, o) -> b.putByte(o + 17, (byte) 1)));
        assertRefused("reserved u16 @18", patch(img, s10, (b, o) -> b.putShort(o + 18, (short) 1)));
        assertRefused("reserved u16 @90", patch(img, s10, (b, o) -> b.putShort(o + 90, (short) 1)));
        assertRefused("latchReason 6", patch(img, s10, (b, o) -> b.putByte(o + 16, (byte) 6)));
        assertRefused("length 91", appendSection(removeSection(img, s10), s10, new byte[91]));
        assertRefused("length 93", appendSection(removeSection(img, s10), s10, new byte[93]));
        assertRefused("a digest count that is not min(cursor, window)", patch(img, s10, (b, o) -> b.putInt(o + 100, 3)));
        assertRefused("a bound feed without its FRESH digest", patch(img, s10, (b, o) -> b.putLong(o + 92, 0L)));
        // the 2.1 invariants
        assertRefused("latchSkipSeq != cursor + 1", patch(img, s10, (b, o) -> b.putLong(o + 8, b.getLong(o + 8) + 3)));
        assertRefused("NONE with latch fields", patch(img, s10, (b, o) -> b.putByte(o + 16, (byte) 0)));
        assertRefused("GAP with a skip", patch(img, s10, (b, o) -> b.putByte(o + 16, (byte) FeedHaltReason.GAP.code())));
        assertRefused("EPOCH_MISMATCH with a skip", patch(img, s10, (b, o) -> b.putByte(o + 16, (byte) 2)));
        assertRefused("latchTradeId != L + 1", patch(img, s10, (b, o) -> b.putLong(o + 44, b.getLong(o + 44) + 5)));
        assertRefused("latchTradeId without a skip", patch(img, s10, (b, o) -> b.putLong(o + 36, 0L)));
        assertRefused("meBehaviorVersion 1 while bound", patch(img, s10, (b, o) -> b.putInt(o + 80, 1)));
        assertRefused("unbound with a cursor", patch(img, s10, (b, o) -> {
            b.putLong(o, 0L);
            b.putByte(o + 16, (byte) 0);
            for (int at = 20; at < 76; at += 8) {
                b.putLong(o + at, 0L);
            }
        }));

        // 6.3: no bit above 8; v3 state and reason are the derivation of reasons; unbound implies GLOBAL SUSPENDED
        final int s5 = BalanceSnapshotCodec.SECTION_GATES;
        assertRefused("gate bit 9", patch(img, s5, (b, o) -> b.putInt(o + 4 + 10 + 6, 1 << 9)));
        assertRefused("v3 market 1 OPEN with a reason bit", patch(img, s5, (b, o) -> b.putInt(o + 4 + 10 + 6, 1)));
        // review F-3: a latch without BRIDGE_HALT on GLOBAL (it would admit orders over a known hole), and an
        // EPOCH_MISMATCH latch on an unbound feed
        assertRefused("latched without BRIDGE_HALT on GLOBAL", patch(img, s5, (b, o) -> {
            b.putByte(o + 4 + 4, (byte) GateState.OPEN.code());
            b.putByte(o + 4 + 5, (byte) GateReason.ADMIN.code());
            b.putInt(o + 4 + 6, 0);
        }));
        final V3Rig gap = V3Rig.unbound();
        gap.sellHitsAt(4, EPOCH, 9, 1, ONE); // D2 row 3: GAP while unbound (bound 0)
        final byte[] gapImg = image(gap.engine);
        restore(gapImg);
        assertRefused("EPOCH_MISMATCH on an unbound feed", patch(gapImg, s10, (b, o) -> b.putByte(o + 16,
                (byte) FeedHaltReason.EPOCH_MISMATCH.code())));
        final byte[] unbound = image(V3Rig.unbound().engine);
        restore(unbound);
        assertRefused("v3 unbound with GLOBAL OPEN", patch(unbound, s5, (b, o) -> {
            b.putByte(o + 4 + 4, (byte) GateState.OPEN.code());
            b.putByte(o + 4 + 5, (byte) GateReason.ADMIN.code());
            b.putInt(o + 4 + 6, 0);
        }));
    }

    @Test
    public void v3ImageWithoutSection10Refused() {
        final int s10 = BalanceSnapshotCodec.SECTION_FEED_STATE;
        final byte[] bound = image(latched().engine);
        assertTrue("section 10 is written under v3", section(bound, s10) > 0);
        restore(bound);
        assertRefused("a bound v3 image without section 10", removeSection(bound, s10));
        // the image taken between the switch and the bind: its section 10 is present and all zero, and restores ...
        final byte[] between = image(V3Rig.unbound().engine);
        assertTrue("written iff v3, also while unbound", section(between, s10) > 0);
        final AssetsEngine back = restore(between);
        assertEquals(0L, back.feed().boundMeEpoch());
        // ... and the same image with that all-zero section removed by hand is refused (review 2 FIX-B)
        assertRefused("the switch-to-bind image without section 10", removeSection(between, s10));
    }

    @Test
    public void section10UnderV2Refused() {
        final byte[] v2 = image(V3Rig.v2().engine);
        assertEquals("no section 10 under v2", -1, section(v2, BalanceSnapshotCodec.SECTION_FEED_STATE));
        restore(v2);
        assertRefused("a v2 image with a section 10", appendSection(v2, BalanceSnapshotCodec.SECTION_FEED_STATE,
                new byte[FeedState.SNAPSHOT_BYTES]));
        final byte[] v1 = image(new AssetsEngine());
        assertRefused("a v1 image (no config) with a section 10", appendSection(v1, BalanceSnapshotCodec.SECTION_FEED_STATE,
                new byte[FeedState.SNAPSHOT_BYTES]));
        assertRefused("section 11 stays unknown", appendSection(v2, BalanceSnapshotCodec.SECTION_ME_LINK, new byte[4]));
    }

    @Test
    public void gatesV2AndCoreConfigV2() {
        final V3Rig r = V3Rig.v2();
        r.adminGate(2, GateState.SUSPENDED, GateReason.RECOVERY);
        final byte[] img = image(r.engine);
        assertEquals("section 5 layout v2: count + 6 gates x 10 bytes", 4 + 6 * 10,
                sectionLength(img, BalanceSnapshotCodec.SECTION_GATES));
        final UnsafeBuffer b = new UnsafeBuffer(img);
        final int g = section(img, BalanceSnapshotCodec.SECTION_GATES) + 6;
        assertEquals(6, b.getInt(g));
        assertEquals("market 2 reasons (derived under v2)", 1 << GateReason.RECOVERY.code(), b.getInt(g + 4 + 2 * 10 + 6));
        assertEquals("GLOBAL reasons (genesis)", 1 << GateReason.GENESIS.code(), b.getInt(g + 4 + 6));
        final int c = section(img, BalanceSnapshotCodec.SECTION_CORE_CONFIG) + 6;
        assertEquals("section 8 layout v2: configVersion", 2L, b.getLong(c));
        assertEquals("behaviorVersion", 2L, b.getLong(c + 8));
        assertEquals("genesisId after behaviorVersion", V3Genesis.GENESIS_ID, b.getLong(c + 16));
        final AssetsEngine back = restore(img);
        assertEquals(V3Genesis.GENESIS_ID, back.coreConfig().recorded().genesisId);
        assertEquals(GateReason.RECOVERY, back.gate().reason(2));
        assertArrayEquals(img, image(back));
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.HexFormat;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S8 finding, pinned (found while building {@link AdversarialFrameFuzzTest}). The frame gate admits any root
 * blockLength at or above the generated minimum and any group entry block at or above the generated one, and the
 * generated decoders honour the frame's own dimensions. The MeJournalTradeBatch (62) handler validates the raw
 * {@code takerIsBuy} bytes at the GENERATED offsets ({@code BLOCK_LENGTH} + group header + i x generated entry block)
 * instead of the frame's, so for a frame with a wider block the check reads other bytes than the decoder applies:
 * a poisoned flag can pass (and is applied as SELL, money moving the other way), and a valid batch can be dropped
 * because an unrelated price or quantity byte is not 0/1. SettleBatch (33) reads the frame's dimensions and is right.
 *
 * <p>Both tests state the fence contract (K5: an invalid raw enum byte is a decode reject; the raw check covers
 * exactly the bytes the decoder will read). They FAIL at 182c86c.</p>
 */
public class TradeBatchWideBlockFenceTest {

    private static final long ONE = 100_000_000L;

    /** Two trades whose price and quantity low bytes are 0x00, so a misplaced bool check reads valid bytes. */
    private static byte[] canonicalBatch() {
        return new CoreFrameEncoders().tradeBatch(List.of(
                new CoreFrameEncoders.Trade(1, 1, 1, 101, 1, 202, 2, 100 * ONE, 256L * 1_000_000L, true, 1, 1, 2),
                new CoreFrameEncoders.Trade(2, 2, 1, 101, 1, 202, 2, 100 * ONE, 256L * 1_000_000L, true, 1, 1, 2)));
    }

    private static int u16(final byte[] b, final int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8;
    }

    private static FrameReplica dispatched(final byte[] frame) {
        final FrameReplica r = new FrameReplica();
        r.dispatchRaw(new UnsafeBuffer(frame), 0, frame.length, 0, 1_000L);
        return r;
    }

    @Test
    public void poisonedTakerIsBuyInAWiderBlockIsADecodeReject() {
        final byte[] wide = FrameMutator.widen(canonicalBatch(), 8); // root block 0 -> 8, entries 101 -> 109
        final int group = FrameMutator.HEADER + u16(wide, 0);
        final int entry = u16(wide, group);
        final int flag = com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder.TradesDecoder.takerIsBuyEncodingOffset();
        wide[group + 4 + flag] = 7;          // trades[0].takerIsBuy = 7
        wide[group + 4 + entry + flag] = 9;  // trades[1].takerIsBuy = 9
        final FrameReplica r = dispatched(wide);
        final String hex = HexFormat.of().formatHex(wide);
        assertEquals("no apply fault", 0, r.demuxer.applyErrorCount());
        assertEquals("invalid takerIsBuy bytes 7 and 9 must be a decode reject; frame " + hex + " applied as " + r.rendered(0),
                1, r.demuxer.decodeRejectCount());
        assertEquals("no trade of a poisoned batch is applied", 0, r.engine.getLastAppliedTradeId());
        assertTrue("nothing is emitted", r.rendered(0).isEmpty());
    }

    @Test
    public void validBatchInAWiderBlockAppliesLikeTheCanonicalOne() {
        final byte[] canonical = canonicalBatch();
        final byte[] wide = FrameMutator.widen(canonical, 16);
        final FrameReplica plain = dispatched(canonical);
        final FrameReplica widened = dispatched(wide);
        assertEquals("the canonical batch applies", 0, plain.demuxer.decodeRejectCount());
        assertEquals("the gate admitted the wider block (block >= minimum); the raw check must read the same bytes the "
                + "decoder applies, so the valid batch is not dropped; frame " + HexFormat.of().formatHex(wide),
                0, widened.demuxer.decodeRejectCount());
        assertEquals(plain.rendered(0), widened.rendered(0));
        assertArrayEquals(plain.image(), widened.image());
    }
}

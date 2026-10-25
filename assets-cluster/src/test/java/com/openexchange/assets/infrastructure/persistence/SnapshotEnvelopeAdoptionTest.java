// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.cluster.SnapshotEnvelope;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S2 (RED-F15/F16, hardening B1.3): the service writes every snapshot inside the kit envelope and
 * refuses to restore from anything else. A bare v4 payload, a flipped bit and an image that fails the
 * ledger invariant each stop the node (failFast) instead of becoming a ledger.
 */
public class SnapshotEnvelopeAdoptionTest {

    private static void fund(final AssetsEngine engine) {
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setUserId(7);
        d.setAssetId(0);
        d.setAmount(1_000);
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, 10);
        final HoldCommand h = new HoldCommand();
        h.reset();
        h.setOrderId(1);
        h.setUserId(7);
        h.setAssetId(0);
        h.setAmount(400);
        engine.applyCommand(AssetsEngine.CMD_HOLD, h, 11);
    }

    private static AssetsClusteredService serviceWithRecorder(final int[] stops) {
        final AssetsClusteredService service = new AssetsClusteredService();
        service.failFast = () -> stops[0]++;
        return service;
    }

    @Test
    public void theServiceWritesAnEnvelopedImageAndRestoresIt() {
        final AssetsClusteredService writer = serviceWithRecorder(new int[1]);
        writer.engine().setEventSink(new NoopSink());
        fund(writer.engine());
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final int length = AssetsClusteredService.encodeSnapshot(writer.engine(), image, new SnapshotEnvelope());
        assertTrue("the offered image starts with the envelope", SnapshotEnvelope.isEnveloped(image, length));
        assertEquals(SnapshotEnvelope.envelopedLength(length - SnapshotEnvelope.HEADER_LENGTH - SnapshotEnvelope.TRAILER_LENGTH), length);

        final int[] stops = new int[1];
        final AssetsClusteredService reader = serviceWithRecorder(stops);
        reader.restoreFromImage(image, length);
        assertEquals(0, stops[0]);
        assertEquals(600, reader.engine().account(7).available(0));
        assertEquals(400, reader.engine().account(7).locked(0));
    }

    @Test
    public void aBarePayloadIsRefusedAndStopsTheNode() {
        final AssetsEngine source = new AssetsEngine();
        source.setEventSink(new NoopSink());
        fund(source);
        final ExpandableArrayBuffer bare = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(source, bare);
        final int[] stops = new int[1];
        final AssetsClusteredService reader = serviceWithRecorder(stops);
        assertThrows(IllegalStateException.class, () -> reader.restoreFromImage(bare, length));
        assertEquals("failFast fires once on an unverifiable snapshot", 1, stops[0]);
        assertEquals("the engine is untouched", 0, reader.engine().accountCount());
    }

    @Test
    public void aFlippedBitIsRefusedAndStopsTheNode() {
        final AssetsEngine source = new AssetsEngine();
        source.setEventSink(new NoopSink());
        fund(source);
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final int length = AssetsClusteredService.encodeSnapshot(source, image, new SnapshotEnvelope());
        final int victim = SnapshotEnvelope.payloadOffset() + 8 + 7 * 8 + 1 + 4 + 2 + 4 + 4 + 8 + 4; // account 7, available asset 0
        image.putLong(victim, image.getLong(victim) ^ 1L);
        final int[] stops = new int[1];
        final AssetsClusteredService reader = serviceWithRecorder(stops);
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> reader.restoreFromImage(image, length));
        assertTrue(e.getMessage(), e.getMessage().contains("CRC32C"));
        assertEquals(1, stops[0]);
        assertEquals(0, reader.engine().accountCount());
    }

    @Test
    public void anImageThatBreaksTheLedgerInvariantIsRefusedAndStopsTheNode() {
        final AssetsEngine source = new AssetsEngine();
        source.setEventSink(new NoopSink());
        fund(source);
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer();
        final int payloadLength = BalanceSnapshotCodec.serialize(source, payload, SnapshotEnvelope.payloadOffset());
        final int lockedOffset = SnapshotEnvelope.payloadOffset() + 8 + 7 * 8 + 1 + 4 + 2 + 4 + 4 + 8 + 4 + 8;
        payload.putLong(lockedOffset, payload.getLong(lockedOffset) + 1); // locked != sum of holds
        final int length = new SnapshotEnvelope().seal(payload, payloadLength); // CRC is valid over the lie
        final int[] stops = new int[1];
        final AssetsClusteredService reader = serviceWithRecorder(stops);
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> reader.restoreFromImage(payload, length));
        assertTrue(e.getMessage(), e.getMessage().contains("locked"));
        assertEquals(1, stops[0]);
    }

    @Test
    public void anEmptyImageIsRefused() {
        final int[] stops = new int[1];
        final AssetsClusteredService reader = serviceWithRecorder(stops);
        assertThrows(IllegalStateException.class, () -> reader.restoreFromImage(new ExpandableArrayBuffer(), 0));
        assertEquals(1, stops[0]);
    }

    /** No-op sink for engines that only need to hold state. */
    static final class NoopSink implements com.openexchange.assets.domain.AssetsEventSink {
        public void onDepositAck(long c, long u, int a, long amt, long n) { }
        public void onWithdrawAck(long c, long u, int a, long amt, long n) { }
        public void onHoldAck(long c, long o, long u, int a, long amt) { }
        public void onHoldReject(long c, long o, long u, int a, long amt, com.openexchange.assets.domain.RejectReason r) { }
        public void onBalanceUpdate(long u, int a, long av, long l) { }
        public void onSettlementApplied(long t, long b, long s) { }
        public void onWithdrawReject(long c, long u, int a, long amt, com.openexchange.assets.domain.RejectReason r) { }
        public void onBalanceSnapshotEnd(long c, int n) { }
        public void onHoldSnapshotEntry(long o, long u, int a, long rem) { }
        public void onHoldSnapshotEnd(long c, int n) { }
        public void onFeedPositionReport(long c, long p, long t, boolean j) { }
        public void onSettleFault(long t, long o, long u, int a, long d, long un) { }
        public void onSettleRejected(long t, int m, long p, com.openexchange.assets.domain.SettleRejectReason r, long d) { }
    }
}

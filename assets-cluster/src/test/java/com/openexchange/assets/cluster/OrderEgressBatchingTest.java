// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.infrastructure.generated.DepositAckEncoder;
import com.openexchange.assets.infrastructure.generated.DispatchKind;
import com.openexchange.assets.infrastructure.generated.MeDispatchBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEventBatchDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEventEncoder;
import com.openexchange.assets.infrastructure.publisher.SessionEgressQueue;
import io.aeron.DirectBufferVector;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S4 (critic G8): consecutive OrderEvent / MeDispatch frames of one drain leave as ONE batch frame
 * (102 -> 103 cap 48, 104 -> 105 cap 64) with byte-identical entry bodies, so four gateways at 10k
 * lifecycles/s do not cost a frame per event. A non-batchable frame between them flushes the open batch.
 */
public class OrderEgressBatchingTest {

    private static final class Session implements ClientSession {
        final List<byte[]> delivered = new ArrayList<>();
        public long id() { return 1; }
        public int responseStreamId() { return 0; }
        public String responseChannel() { return "aeron:ipc"; }
        public byte[] encodedPrincipal() { return new byte[0]; }
        public void close() { }
        public boolean isClosing() { return false; }
        public long offer(final DirectBuffer b, final int o, final int l) {
            final byte[] c = new byte[l];
            b.getBytes(o, c);
            delivered.add(c);
            return 1;
        }
        public long offer(final DirectBufferVector[] v) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int l, final BufferClaim c) { throw new UnsupportedOperationException(); }
    }

    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    private int orderEvent(final long seq) {
        final OrderEventEncoder e = new OrderEventEncoder().wrapAndApplyHeader(buf, 0, header)
                .eventSeq(seq).logPosition(seq * 10).orderId(seq).userId(7).marketId(1)
                .eventKind(com.openexchange.assets.infrastructure.generated.EventKind.ACCEPTED)
                .status(com.openexchange.assets.infrastructure.generated.OrderStatus.PENDING_NEW)
                .reason(com.openexchange.assets.infrastructure.generated.CoreRejectReason.NONE)
                .side(com.openexchange.assets.infrastructure.generated.Side.BUY)
                .orderType(com.openexchange.assets.infrastructure.generated.OrderType.LIMIT)
                .timeInForce(com.openexchange.assets.infrastructure.generated.TimeInForce.GTC)
                .price(100).stopPrice(0).quantity(5).filledQty(0).remainingQty(5).lastPrice(0).lastQty(0).tradeId(0)
                .meOrderId(0).holdAssetId(0).holdAmount(500).holdRemaining(500).clientOrderIdHigh(0).clientOrderIdLow(seq)
                .createdAtMs(1).updatedAtMs(2);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int meDispatch(final long seq) {
        final MeDispatchEncoder e = new MeDispatchEncoder().wrapAndApplyHeader(buf, 0, header)
                .dispatchSeq(seq).commandIdHigh(seq).commandIdLow(0).kind(DispatchKind.CREATE).userId(7).omsOrderId(seq)
                .oldOrderId(0).marketId(1).orderType((short) 0).side((short) 0).price(100).quantity(5).budget(0).timeInForce((short) 0);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int depositAck() {
        final DepositAckEncoder e = new DepositAckEncoder().wrapAndApplyHeader(buf, 0, header)
                .correlationId(1).userId(1).assetId(0).amount(1).newAvailable(1);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void consecutiveOrderEventsLeaveAsOneBatchWithIdenticalBodies() {
        final Session s = new Session();
        final SessionEgressQueue q = new SessionEgressQueue(s);
        final List<byte[]> bodies = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            final int len = orderEvent(i);
            final byte[] body = new byte[len - MessageHeaderEncoder.ENCODED_LENGTH];
            buf.getBytes(MessageHeaderEncoder.ENCODED_LENGTH, body);
            bodies.add(body);
            assertTrue(q.append(buf, 0, len));
        }
        q.drainAll();
        assertEquals("one frame", 1, s.delivered.size());
        final UnsafeBuffer frame = new UnsafeBuffer(s.delivered.get(0));
        final MessageHeaderDecoder h = new MessageHeaderDecoder().wrap(frame, 0);
        assertEquals(OrderEventBatchDecoder.TEMPLATE_ID, h.templateId());
        final OrderEventBatchDecoder d = new OrderEventBatchDecoder().wrapAndApplyHeader(frame, 0, h);
        int i = 0;
        for (final OrderEventBatchDecoder.EventsDecoder ev : d.events()) {
            assertEquals(i + 1, ev.eventSeq());
            assertEquals(i + 1, ev.clientOrderIdLow());
            assertEquals(100, ev.price());
            assertEquals(500, ev.holdAmount());
            assertEquals(7, ev.userId());
            assertEquals(OrderEventBatchDecoder.EventsDecoder.sbeBlockLength(), bodies.get(i).length);
            i++;
        }
        assertEquals(3, i);
    }

    @Test
    public void meDispatchBatchesCapAtSixtyFourAndANonBatchableFrameFlushes() {
        final Session s = new Session();
        final SessionEgressQueue q = new SessionEgressQueue(s);
        for (int i = 1; i <= 70; i++) {
            assertTrue(q.append(buf, 0, meDispatch(i)));
        }
        assertTrue(q.append(buf, 0, depositAck()));
        assertTrue(q.append(buf, 0, meDispatch(71)));
        q.drainAll();
        assertEquals("64 + 6 dispatches, the ack, then 1 dispatch", 4, s.delivered.size());
        final MessageHeaderDecoder h = new MessageHeaderDecoder();
        final UnsafeBuffer f0 = new UnsafeBuffer(s.delivered.get(0));
        h.wrap(f0, 0);
        assertEquals(MeDispatchBatchDecoder.TEMPLATE_ID, h.templateId());
        assertEquals(64, new MeDispatchBatchDecoder().wrapAndApplyHeader(f0, 0, h).dispatches().count());
        final UnsafeBuffer f1 = new UnsafeBuffer(s.delivered.get(1));
        h.wrap(f1, 0);
        assertEquals(6, new MeDispatchBatchDecoder().wrapAndApplyHeader(f1, 0, h).dispatches().count());
        h.wrap(new UnsafeBuffer(s.delivered.get(2)), 0);
        assertEquals(DepositAckEncoder.TEMPLATE_ID, h.templateId());
        final UnsafeBuffer f3 = new UnsafeBuffer(s.delivered.get(3));
        h.wrap(f3, 0);
        assertEquals(1, new MeDispatchBatchDecoder().wrapAndApplyHeader(f3, 0, h).dispatches().count());
    }
}

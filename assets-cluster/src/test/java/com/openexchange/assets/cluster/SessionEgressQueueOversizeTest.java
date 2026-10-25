// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.infrastructure.generated.DepositAckEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.publisher.SessionEgressQueue;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import io.aeron.DirectBufferVector;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S1 (RED-F14, port of ME 202789c): an egress frame larger than the session publication's max
 * message length makes {@code ClientSession.offer} throw {@code IllegalArgumentException}. That offer
 * runs inside the apply callback, so without a catch the IAE would reach the apply-failure fence and
 * halt the leader, and the next leader would re-form the same frame. The frame is lost for that
 * session, counted, and the queue keeps flowing.
 */
public class SessionEgressQueueOversizeTest {

    private static final class CappedSession implements ClientSession {
        final int maxLength;
        final List<Integer> delivered = new ArrayList<>();

        CappedSession(final int maxLength) {
            this.maxLength = maxLength;
        }

        @Override public long id() { return 42; }
        @Override public int responseStreamId() { return 0; }
        @Override public String responseChannel() { return "aeron:ipc"; }
        @Override public byte[] encodedPrincipal() { return new byte[0]; }
        @Override public void close() { }
        @Override public boolean isClosing() { return false; }

        @Override
        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            if (length > maxLength) {
                throw new IllegalArgumentException("message exceeds maxMessageLength of " + maxLength
                        + ", length=" + length);
            }
            delivered.add(length);
            return 1;
        }

        @Override public long offer(final DirectBufferVector[] vectors) { throw new UnsupportedOperationException(); }
        @Override public long tryClaim(final int length, final BufferClaim bufferClaim) { throw new UnsupportedOperationException(); }
    }

    private static int singleFrame(final UnsafeBuffer buf, final int padding) {
        // A DepositAck is not batchable, so it leaves the queue as the single frame it entered as.
        final DepositAckEncoder e = new DepositAckEncoder().wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .correlationId(1).userId(1).assetId(0).amount(1).newAvailable(1);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength() + padding;
    }

    @Test
    public void oversizeFrameIsDroppedCountedAndTheQueueKeepsFlowing() {
        final CappedSession session = new CappedSession(64);
        final SessionEgressQueue queue = new SessionEgressQueue(session);
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
        assertTrue(queue.append(buf, 0, singleFrame(buf, 100))); // 145 B > cap
        assertTrue(queue.append(buf, 0, singleFrame(buf, 0)));   // 45 B, fits
        queue.drainAll();
        assertEquals("the oversize frame is lost for this session, loudly", 1, queue.oversizeDrops());
        assertEquals("the frame behind it is delivered", 1, session.delivered.size());
        assertEquals(45, (int) session.delivered.get(0));
        assertTrue("nothing is parked or pending", queue.isEmpty());
    }
}

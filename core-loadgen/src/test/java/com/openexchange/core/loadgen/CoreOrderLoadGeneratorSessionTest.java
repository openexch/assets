// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

import com.openexchange.core.infrastructure.generated.BoolFlag;
import com.openexchange.core.infrastructure.generated.CancelOrderDecoder;
import com.openexchange.core.infrastructure.generated.CreateOrderDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.OrderAckEncoder;
import com.openexchange.core.infrastructure.generated.OrderStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The session's identity and re-offer rules against a fake ingress: every create and cancel carries a fresh requestId
 * (the clientOrderId is the requestId, the correlationId the slot); a NewLeader re-offers the unanswered commands once
 * with byte-identical frames; a replayed answer counts as {@code replayed}, never as a new ack; a second answer of the
 * same request is ignored; with re-offer off the unanswered tail stays unanswered.
 */
public class CoreOrderLoadGeneratorSessionTest {

    private static final long MS = 1_000_000L;

    private static final class Wire implements CoreOrderLoadGenerator.Ingress {
        final List<byte[]> frames = new ArrayList<>();
        boolean accept = true;

        @Override
        public long offer(final org.agrona.DirectBuffer buffer, final int offset, final int length) {
            if (!accept) {
                return -1;
            }
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b);
            frames.add(b);
            return 64L * frames.size();
        }
    }

    private static CoreOrderLoadGenerator.Settings settings() {
        final CoreOrderLoadGenerator.Settings s = new CoreOrderLoadGenerator.Settings();
        s.rate = 10;
        s.users = 100;
        s.openOrders = 2;
        s.durationSec = 1;
        s.warmupSec = 0;
        s.register = false;
        s.reofferAfterMs = 600_000;
        s.requestIdHigh = 0x77L;
        s.drainMs = 1_000;
        return s;
    }

    private static void ack(final CoreOrderLoadGenerator.Session session, final long requestLow, final boolean duplicate) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[256]);
        final OrderAckEncoder e = new OrderAckEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .correlationId(0).requestIdHigh(0x77L).requestIdLow(requestLow).orderId(1000 + requestLow).userId(1)
                .marketId(1).status(OrderStatus.PENDING_NEW).duplicate(duplicate ? BoolFlag.TRUE : BoolFlag.FALSE)
                .holdAssetId(0).holdAmount(0).acceptedAtMs(0).eventSeq(0);
        session.onMessage(1, 0, b, 0, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength(), null);
    }

    private static long requestLowOf(final byte[] frame) {
        final UnsafeBuffer b = new UnsafeBuffer(frame);
        final MessageHeaderDecoder h = new MessageHeaderDecoder().wrap(b, 0);
        if (h.templateId() == CreateOrderDecoder.TEMPLATE_ID) {
            return new CreateOrderDecoder().wrapAndApplyHeader(b, 0, h).requestIdLow();
        }
        return new CancelOrderDecoder().wrapAndApplyHeader(b, 0, h).requestIdLow();
    }

    @Test
    public void everyCommandCarriesItsOwnRequestIdAndTheReofferKeepsIt() {
        final Wire gw = new Wire();
        final CoreOrderLoadGenerator.Session s = new CoreOrderLoadGenerator.Session(settings(), gw, null, null);
        long now = 0;
        s.tick(now);
        now += 100 * MS;
        s.tick(now);
        assertEquals("the ramp placed both open orders", 2, gw.frames.size());
        final UnsafeBuffer first = new UnsafeBuffer(gw.frames.get(0));
        final CreateOrderDecoder c = new CreateOrderDecoder().wrapAndApplyHeader(first, 0,
                new MessageHeaderDecoder().wrap(first, 0));
        assertEquals(0x77L, c.requestIdHigh());
        assertEquals(1L, c.requestIdLow());
        assertEquals("clientOrderId = requestId", c.requestIdLow(), c.clientOrderIdLow());
        assertEquals("correlationId = slot", 0L, c.correlationId());
        assertEquals(2L, requestLowOf(gw.frames.get(1)));

        // an election: both unanswered commands go out again, byte for byte
        s.onNewLeader(1, 2, 1, "");
        s.tick(now);
        assertEquals(4, gw.frames.size());
        assertArrayEquals(gw.frames.get(0), gw.frames.get(2));
        assertArrayEquals(gw.frames.get(1), gw.frames.get(3));
        // the same election announced again does not re-offer again
        s.onNewLeader(1, 2, 1, "");
        s.tick(now);
        assertEquals(4, gw.frames.size());
        assertEquals(1, s.result().elections());

        ack(s, 1, false);
        ack(s, 1, true); // the replay's answer after the original's: ignored as an answer, counted as replayed
        ack(s, 2, true); // only the replay's answer arrived
        final CoreOrderLoadGenerator.Result r = s.result();
        assertEquals(2, r.creates());
        assertEquals("each request answered once", 2, r.acks());
        assertEquals(3, r.ackFrames());
        assertEquals(2, r.replayed());
        assertEquals(2, r.reoffered());
        assertEquals(0, s.inFlight());

        // steady: a slot is a create plus a cancel of the oldest open order, by its clientOrderId
        now += 100 * MS;
        s.tick(now);
        assertEquals("STEADY", s.phase());
        s.tick(now + 1);
        final byte[] cancel = gw.frames.get(gw.frames.size() - 1);
        final UnsafeBuffer cb = new UnsafeBuffer(cancel);
        final CancelOrderDecoder x = new CancelOrderDecoder().wrapAndApplyHeader(cb, 0,
                new MessageHeaderDecoder().wrap(cb, 0));
        assertEquals("the cancel names the oldest open order", 1L, x.origClientOrderIdLow());
        assertEquals(0L, x.orderId());
        assertTrue("a fresh requestId", x.requestIdLow() > 2);
        final long create = requestLowOf(gw.frames.get(gw.frames.size() - 2));
        assertEquals(create + 1, x.requestIdLow());
        assertEquals(1, s.result().cancels());
    }

    @Test
    public void withTheReofferOffTheUnansweredTailStays() {
        final Wire gw = new Wire();
        final CoreOrderLoadGenerator.Settings st = settings();
        st.reoffer = false;
        st.openOrders = 1;
        st.createOnly = true;
        final CoreOrderLoadGenerator.Session s = new CoreOrderLoadGenerator.Session(st, gw, null, null);
        s.tick(0);
        assertEquals(1, gw.frames.size());
        s.onNewLeader(1, 2, 1, "");
        s.tick(1);
        assertEquals("nothing re-offered", 1, gw.frames.size());
        assertEquals(1, s.inFlight());
        assertEquals(0, s.result().reoffered());
    }

    @Test
    public void aRefusedOfferKeepsTheSlotOwedAndTheIdUnused() {
        final Wire gw = new Wire();
        gw.accept = false;
        final CoreOrderLoadGenerator.Settings st = settings();
        st.openOrders = 1;
        final CoreOrderLoadGenerator.Session s = new CoreOrderLoadGenerator.Session(st, gw, null, null);
        s.tick(0);
        s.tick(1);
        assertEquals(0, gw.frames.size());
        gw.accept = true;
        s.tick(2);
        assertEquals(1, gw.frames.size());
        assertEquals("the first id is used by the accepted offer", 1L, requestLowOf(gw.frames.get(0)));
        assertEquals(1, s.result().creates());
    }

    @Test
    public void anEarlyEndOfTheSteadyPhaseStopsNewSlotsAndDrains() {
        final Wire gw = new Wire();
        final CoreOrderLoadGenerator.Settings st = settings();
        st.openOrders = 1;
        st.createOnly = true;
        st.durationSec = 3_600;
        final CoreOrderLoadGenerator.Session s = new CoreOrderLoadGenerator.Session(st, gw, null, null);
        s.tick(0);
        ack(s, 1, false);
        s.tick(MS);
        assertEquals("STEADY", s.phase());
        long now = MS;
        for (int i = 0; i < 5; i++) { // 10 slots/s: one slot per 100 ms
            now += 100 * MS;
            s.tick(now);
        }
        final int sent = gw.frames.size();
        assertTrue("slots went out: " + sent, sent > 1);
        s.endSteady(now);
        s.tick(now + 500 * MS);
        assertEquals("no slot after the end", sent, gw.frames.size());
        assertEquals("DRAIN", s.phase());
        for (long low = 2; low <= sent; low++) {
            ack(s, low, false);
        }
        s.tick(now + 600 * MS);
        assertTrue("drained: " + s.phase(), s.done());
        assertEquals(0, s.result().unanswered());
    }
}

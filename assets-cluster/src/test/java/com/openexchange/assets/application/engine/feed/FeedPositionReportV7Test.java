// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** RC2 S7 3.6: FeedPositionReport's v7 tail is the FeedState (and the S1 values under v1/v2), on the wire. */
public class FeedPositionReportV7Test {

    /** Every egress frame the publisher emits, copied. */
    private static final class Capture implements AssetsEventPublisher.Egress {
        final List<byte[]> frames = new ArrayList<>();

        @Override
        public void emit(final org.agrona.MutableDirectBuffer buffer, final int offset, final int length, final int channel,
                         final AssetsEventPublisher.Routing routing) {
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b);
            frames.add(b);
        }

        <T> T last(final int templateId, final java.util.function.Function<UnsafeBuffer, T> decode) {
            for (int i = frames.size() - 1; i >= 0; i--) {
                final UnsafeBuffer b = new UnsafeBuffer(frames.get(i));
                if (new MessageHeaderDecoder().wrap(b, 0).templateId() == templateId) {
                    return decode.apply(b);
                }
            }
            throw new AssertionError("no frame " + templateId + " in " + frames.size() + " frames");
        }
    }

    private static FeedPositionReportDecoder report(final UnsafeBuffer b) {
        return new FeedPositionReportDecoder().wrapAndApplyHeader(b, 0, new MessageHeaderDecoder());
    }

    @Test
    public void tailFromFeedState() {
        // the recorded stream (what the goldens render)
        final V3Rig r = V3Rig.open();
        r.twoParty();
        r.sellHitsAt(9, EPOCH, r.egress + 1, 1, ONE);
        r.mark();
        r.engine.reportFeedPosition(77L);
        final var rep = r.first(com.openexchange.assets.determinism.AssetsEngineEvent.FeedPositionReport.class);
        assertEquals("meEpoch = the bound epoch", EPOCH, rep.meEpoch());
        assertEquals("lastAppliedJournalSeq = the cursor", 3L, rep.lastAppliedJournalSeq());
        assertTrue("feedHalted", rep.feedHalted());
        assertEquals(FeedHaltReason.GAP, rep.haltReason());

        // the wire: the publisher writes the same tail
        final Capture wire = new Capture();
        final AssetsEngine engine = new AssetsEngine();
        engine.setEventSink(new AssetsEventPublisher(wire, () -> false));
        V3Genesis.boot(engine, 1_000L);
        engine.onMeTerminal(9, 5, 101, 1, 1, 3, 0, 1_000L, 1, EPOCH, 1_000L);
        engine.reportFeedPosition(78L);
        final FeedPositionReportDecoder d = wire.last(FeedPositionReportDecoder.TEMPLATE_ID, FeedPositionReportV7Test::report);
        assertEquals(78L, d.correlationId());
        assertEquals(EPOCH, d.meEpoch());
        assertEquals(1L, d.lastAppliedJournalSeq());
        assertEquals(BoolFlag.TRUE, d.feedHalted());
        assertEquals(com.openexchange.assets.infrastructure.generated.FeedHaltReason.GAP, d.haltReason());

        // the global gate's reason set on the wire after the latch: GENESIS (the switch) | BRIDGE_HALT (the latch)
        final AdmissionStateDecoder a = wire.last(AdmissionStateDecoder.TEMPLATE_ID,
                b -> new AdmissionStateDecoder().wrapAndApplyHeader(b, 0, new MessageHeaderDecoder()));
        assertTrue("bridgeHalt bit", a.gateReasons().bridgeHalt());
        assertTrue("genesis bit", a.gateReasons().genesis());

        // v1: the S1 tail, byte for byte
        final Capture v1 = new Capture();
        final AssetsEngine e1 = new AssetsEngine();
        e1.setEventSink(new AssetsEventPublisher(v1, () -> false));
        e1.reportFeedPosition(79L);
        final FeedPositionReportDecoder d1 = v1.last(FeedPositionReportDecoder.TEMPLATE_ID, FeedPositionReportV7Test::report);
        assertEquals(0L, d1.meEpoch());
        assertEquals(0L, d1.lastAppliedJournalSeq());
        assertEquals(BoolFlag.FALSE, d1.feedHalted());
        assertEquals(com.openexchange.assets.infrastructure.generated.FeedHaltReason.NONE, d1.haltReason());
    }
}

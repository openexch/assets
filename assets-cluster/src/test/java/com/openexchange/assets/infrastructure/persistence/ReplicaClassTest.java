// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.auth.CorePrincipal;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.CoreConfigDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.RequestOrderSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.SettleBatchDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S1 (contract 4.1, 4.5, 8.3): SessionClass REPLICA (7) for the RCS replica process. RegisterSession accepts the
 * class byte 7 and leaves the judgement to the registry (REJECTED unless the principal is a REPLICA, ACCEPTED when it
 * is); 8 is decode poison. No ingress template carries the REPLICA bit, so every frame but RegisterSession from a
 * REPLICA session is UNAUTHORIZED_CLASS. REPLICA is not a registration-first class.
 */
public class ReplicaClassTest {

    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final long TS = 1_000L;

    private static final class Rig {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[2048]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final List<Long> rejectedSessions = new ArrayList<>();
        int mark;

        Rig() {
            engine.setEventSink(sink);
            demuxer.setTransportHooks(new AssetsSbeDemuxer.TransportHooks() {
                public void onSessionRegistered(long sessionId, int channelMask, boolean resumable) { }
                public void onSessionRejected(long sessionId) { rejectedSessions.add(sessionId); }
            });
        }

        long open(final long sessionId, final SessionClass principalClass) {
            engine.sessions().open(sessionId, CorePrincipal.anyUser(principalClass, (int) sessionId).encode());
            return sessionId;
        }

        int register(final long corr, final int classByte) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(corr).sessionClass(SessionClass.TOOL).readerKind((short) 0).resumable(BoolFlag.TRUE)
                    .identity(0).sourceEpoch(0);
            e.channels().clear().acks(true);
            buf.putByte(HEADER + RegisterSessionEncoder.sessionClassEncodingOffset(), (byte) classByte);
            return HEADER + e.encodedLength();
        }

        /** A minimal well-formed frame of a known template: zero root of its minimum block, empty groups. */
        int minimal(final int templateId) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            final int block = AssetsSbeDemuxer.minimumBlockLength(templateId);
            header.wrap(buf, 0).blockLength(block).templateId(templateId)
                    .schemaId(MessageHeaderEncoder.SCHEMA_ID).version(MessageHeaderEncoder.SCHEMA_VERSION);
            int at = HEADER + block;
            for (final int entryBlock : groupEntryBlocks(templateId)) {
                buf.putShort(at, (short) entryBlock, ByteOrder.LITTLE_ENDIAN);
                buf.putShort(at + 2, (short) 0, ByteOrder.LITTLE_ENDIAN);
                at += 4;
            }
            return at;
        }

        void dispatch(final long sessionId, final int length) {
            demuxer.dispatch(sessionId, buf, 0, length, TS);
        }

        void markNow() {
            mark = sink.events().size();
        }

        List<AssetsEngineEvent> events() {
            return new ArrayList<>(sink.events().subList(mark, sink.events().size()));
        }
    }

    private static int[] groupEntryBlocks(final int templateId) {
        switch (templateId) {
            case SettleBatchDecoder.TEMPLATE_ID: return new int[] {SettleBatchDecoder.SettlesDecoder.sbeBlockLength()};
            case MeJournalTradeBatchDecoder.TEMPLATE_ID: return new int[] {MeJournalTradeBatchDecoder.TradesDecoder.sbeBlockLength()};
            case MeCommandReleaseDecoder.TEMPLATE_ID: return new int[] {MeCommandReleaseDecoder.ReleasesDecoder.sbeBlockLength()};
            case CoreConfigDecoder.TEMPLATE_ID:
                return new int[] {CoreConfigDecoder.MarketsDecoder.sbeBlockLength(), CoreConfigDecoder.AssetsDecoder.sbeBlockLength()};
            default: return new int[0];
        }
    }

    @Test
    public void classSevenIsJudgedByTheRegistry() {
        final Rig r = new Rig();
        final long tool = r.open(10, SessionClass.TOOL);
        r.markNow();
        r.dispatch(tool, r.register(1, 7));
        assertEquals("class 7 is not poison", 0, r.demuxer.decodeRejectCount());
        AssetsEngineEvent.AdminAck ack = (AssetsEngineEvent.AdminAck) r.events().get(0);
        assertEquals("without a REPLICA principal", AdminResult.REJECTED, ack.result());
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS.code(), ack.reason());
        assertEquals(List.of(10L), r.rejectedSessions);

        final long replica = r.open(11, SessionClass.REPLICA);
        r.markNow();
        r.dispatch(replica, r.register(2, 7));
        ack = (AssetsEngineEvent.AdminAck) r.events().get(0);
        assertEquals("with a REPLICA principal", AdminResult.ACCEPTED, ack.result());
        assertEquals(80, ack.refTemplateId());
        assertTrue(r.engine.sessions().get(11).isRegistered());
        assertEquals(SessionClass.REPLICA, r.engine.sessions().get(11).sessionClass());
    }

    @Test
    public void classEightIsADecodeReject() {
        final Rig r = new Rig();
        final long replica = r.open(20, SessionClass.REPLICA);
        r.markNow();
        r.dispatch(replica, r.register(1, 8));
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(List.of(), r.events());
        assertFalse(r.engine.sessions().get(20).isRegistered());
    }

    @Test
    public void aReplicaSessionSendingRequestOrderSnapshotIsUnauthorizedClass() {
        final Rig r = new Rig();
        final long replica = r.open(30, SessionClass.REPLICA);
        r.dispatch(replica, r.register(1, 7));
        r.markNow();
        r.buf.setMemory(0, r.buf.capacity(), (byte) 0);
        final RequestOrderSnapshotEncoder e = new RequestOrderSnapshotEncoder().wrapAndApplyHeader(r.buf, 0, r.header)
                .correlationId(9).userId(1).fromOrderId(0);
        assertEquals(53, RequestOrderSnapshotEncoder.TEMPLATE_ID);
        r.dispatch(replica, HEADER + e.encodedLength());
        assertEquals(1, r.events().size());
        final AssetsEngineEvent.OrderReject rej = (AssetsEngineEvent.OrderReject) r.events().get(0);
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
        assertEquals(53, rej.refTemplateId());
        assertEquals(9, rej.correlationId());
    }

    @Test
    public void noIngressTemplateButRegisterSessionAdmitsAReplicaSession() {
        final Rig r = new Rig();
        final long replica = r.open(40, SessionClass.REPLICA);
        r.dispatch(replica, r.register(1, 7));
        int probed = 0;
        for (int t = 0; t < 256; t++) {
            if (AssetsSbeDemuxer.minimumBlockLength(t) < 0 || t == RegisterSessionDecoder.TEMPLATE_ID) {
                continue;
            }
            r.markNow();
            r.dispatch(replica, r.minimal(t));
            assertEquals("template " + t + ": " + r.events(), 1, r.events().size());
            final AssetsEngineEvent.OrderReject rej = (AssetsEngineEvent.OrderReject) r.events().get(0);
            assertEquals("template " + t, CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
            assertEquals(t, rej.refTemplateId());
            probed++;
        }
        assertEquals("every known template but 80 was probed", probed, r.demuxer.unauthorisedCount());
        assertEquals(0, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.unhandledTemplateCount());
        assertTrue(probed > 30);
    }
}

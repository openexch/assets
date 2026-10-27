// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.ConfigDecision;
import com.openexchange.assets.infrastructure.generated.FeedHaltEncoder;
import com.openexchange.assets.infrastructure.generated.FeedHaltReason;
import com.openexchange.assets.infrastructure.generated.FeedLatchClearEncoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.ReleaseResult;
import com.openexchange.assets.infrastructure.generated.ReleaseScope;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * RC2 S1 (contract 4.3, 4.3a, 4.5, 8.3): FeedHalt 66, MeCommandRelease 67 and MeEngineConfig 68 (BRIDGE, registration
 * required) and FeedLatchClear 76 (ADMIN) are defined in money-schema v7 and served by RC2 S7/S8. In S1 each passes the
 * frame gate, the session authority and its raw-range checks, and is then counted unhandled: no mutation, no egress.
 * Authority failures are the usual ORIGIN OrderReject (66 and 76 echo their correlationId, 67 and 68 carry none).
 */
public class UnservedFeedTemplatesTest {

    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final long TS = 1_000L;

    private static final class Rig {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[2048]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        int mark;

        Rig() {
            engine.setEventSink(sink);
        }

        /** Open a session with {@code cls}'s principal and register it with the same class (accepted). */
        long registered(final long sessionId, final SessionClass cls) {
            engine.sessions().open(sessionId, ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(cls), (int) sessionId).encode());
            final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(zeroed(), 0, header)
                    .correlationId(sessionId).sessionClass(cls).readerKind((short) 0).resumable(BoolFlag.TRUE)
                    .identity(0).sourceEpoch(0);
            e.channels().clear().acks(true);
            dispatch(sessionId, HEADER + e.encodedLength());
            markNow();
            final AssetsEngineEvent.AdminAck ack = (AssetsEngineEvent.AdminAck) sink.events().stream()
                    .filter(ev -> ev instanceof AssetsEngineEvent.AdminAck a && a.correlationId() == sessionId)
                    .findFirst().orElseThrow();
            assertEquals(AdminResult.ACCEPTED, ack.result());
            return sessionId;
        }

        long unregistered(final long sessionId, final SessionClass cls) {
            engine.sessions().open(sessionId, ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(cls), (int) sessionId).encode());
            return sessionId;
        }

        UnsafeBuffer zeroed() {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            return buf;
        }

        int feedHalt(final long corr, final int reason) {
            final FeedHaltEncoder e = new FeedHaltEncoder().wrapAndApplyHeader(zeroed(), 0, header).correlationId(corr)
                    .reason(FeedHaltReason.GAP).meEpochSeen(1).expectedJournalSeq(5).seenJournalSeq(7).bridgeIdentity(3);
            buf.putByte(HEADER + FeedHaltEncoder.reasonEncodingOffset(), (byte) reason);
            return HEADER + e.encodedLength();
        }

        int release(final int n, final int scope, final int result) {
            final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(zeroed(), 0, header)
                    .egressSeq(1).journalSeq(2).meEpoch(1).releaseSeq(3).ledgerSize(4).ledgerCapacity(5).tombstoneRanges(0);
            final MeCommandReleaseEncoder.ReleasesEncoder g = e.releasesCount(n);
            for (int i = 0; i < n; i++) {
                g.next().commandIdHigh(10 + i).idLowFloor(4).scope(ReleaseScope.UP_TO).result(ReleaseResult.NOOP).removed(0);
            }
            if (n > 0) {
                final int last = HEADER + MeCommandReleaseEncoder.BLOCK_LENGTH + 4
                        + (n - 1) * MeCommandReleaseEncoder.ReleasesEncoder.sbeBlockLength();
                buf.putByte(last + MeCommandReleaseEncoder.ReleasesEncoder.scopeEncodingOffset(), (byte) scope);
                buf.putByte(last + MeCommandReleaseEncoder.ReleasesEncoder.resultEncodingOffset(), (byte) result);
            }
            return HEADER + e.encodedLength();
        }

        int engineConfig(final int decision) {
            final MeEngineConfigEncoder e = new MeEngineConfigEncoder().wrapAndApplyHeader(zeroed(), 0, header)
                    .egressSeq(1).journalSeq(1).meEpoch(1).configVersion(1).behaviorVersion(1).ledgerCapacity(100_000)
                    .maxWireVersion(12).decision(ConfigDecision.FRESH);
            buf.putByte(HEADER + MeEngineConfigEncoder.decisionEncodingOffset(), (byte) decision);
            return HEADER + e.encodedLength();
        }

        int latchClear(final long corr, final int reason) {
            final FeedLatchClearEncoder e = new FeedLatchClearEncoder().wrapAndApplyHeader(zeroed(), 0, header)
                    .correlationId(corr).haltReason(FeedHaltReason.GAP).journalSeq(0);
            buf.putByte(HEADER + FeedLatchClearEncoder.haltReasonEncodingOffset(), (byte) reason);
            return HEADER + e.encodedLength();
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

        AssetsEngineEvent.OrderReject onlyReject() {
            assertEquals("exactly one event: " + events(), 1, events().size());
            return (AssetsEngineEvent.OrderReject) events().get(0);
        }

        byte[] snapshot() {
            final ExpandableArrayBuffer b = new ExpandableArrayBuffer();
            final int len = BalanceSnapshotCodec.serialize(engine, b);
            final byte[] out = new byte[len];
            b.getBytes(0, out);
            return out;
        }
    }

    @Test
    public void wellFormedFramesFromTheirClassAreCountedUnhandledWithNoMutationAndNoEgress() {
        final Rig r = new Rig();
        final long bridge = r.registered(10, SessionClass.BRIDGE);
        final long admin = r.registered(11, SessionClass.ADMIN);
        final byte[] before = r.snapshot();
        r.markNow();
        for (final int reason : new int[] {1, 2, 3, 4, 5}) {
            r.dispatch(bridge, r.feedHalt(reason, reason));
        }
        r.dispatch(bridge, r.release(0, 0, 0));
        r.dispatch(bridge, r.release(1, 1, 2));
        r.dispatch(bridge, r.release(48, 0, 1)); // the frozen cap
        for (final int decision : new int[] {0, 1, 2, 3}) {
            r.dispatch(bridge, r.engineConfig(decision));
        }
        for (final int reason : new int[] {1, 2, 3, 4, 5}) {
            r.dispatch(admin, r.latchClear(reason, reason));
        }
        assertEquals(5 + 3 + 4 + 5, r.demuxer.unhandledTemplateCount());
        assertEquals(0, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(0, r.demuxer.unauthorisedCount());
        assertEquals("no egress", List.of(), r.events());
        assertArrayEquals("no mutation", before, r.snapshot());
    }

    @Test
    public void aSessionOutsideTheClassGetsUnauthorizedClassWithTheCorrelationIdWhereThereIsOne() {
        final Rig r = new Rig();
        final long gateway = r.registered(20, SessionClass.GATEWAY);
        final long bridge = r.registered(21, SessionClass.BRIDGE);
        final long admin = r.registered(22, SessionClass.ADMIN);

        r.markNow();
        r.dispatch(gateway, r.feedHalt(77, 1));
        AssetsEngineEvent.OrderReject rej = r.onlyReject();
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
        assertEquals(66, rej.refTemplateId());
        assertEquals("66 echoes its correlationId", 77, rej.correlationId());

        r.markNow();
        r.dispatch(bridge, r.latchClear(78, 1));
        rej = r.onlyReject();
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
        assertEquals(76, rej.refTemplateId());
        assertEquals("76 echoes its correlationId", 78, rej.correlationId());

        r.markNow();
        r.dispatch(admin, r.release(1, 0, 0));
        rej = r.onlyReject();
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
        assertEquals(67, rej.refTemplateId());
        assertEquals("67 carries no correlationId", 0, rej.correlationId());

        r.markNow();
        r.dispatch(gateway, r.engineConfig(0));
        rej = r.onlyReject();
        assertEquals(68, rej.refTemplateId());
        assertEquals("68 carries no correlationId", 0, rej.correlationId());

        assertEquals(4, r.demuxer.unauthorisedCount());
        assertEquals(0, r.demuxer.unhandledTemplateCount());
    }

    @Test
    public void anUnregisteredBridgeSessionIsRefusedBeforeTheClassCheck() {
        final Rig r = new Rig();
        final long bridge = r.unregistered(30, SessionClass.BRIDGE);
        r.markNow();
        r.dispatch(bridge, r.feedHalt(5, 1));
        AssetsEngineEvent.OrderReject rej = r.onlyReject();
        assertEquals(CoreRejectReason.UNREGISTERED_SESSION, rej.reason());
        assertEquals(5, rej.correlationId());
        for (final int len : new int[] {r.release(1, 0, 0), r.engineConfig(0)}) {
            r.markNow();
            r.dispatch(bridge, len);
            assertEquals(CoreRejectReason.UNREGISTERED_SESSION, r.onlyReject().reason());
        }
        assertEquals(0, r.demuxer.unhandledTemplateCount());
    }

    @Test
    public void releaseGroupAboveTheCapOrPastTheFrameIsAFrameBoundsReject() {
        final Rig r = new Rig();
        final long bridge = r.registered(40, SessionClass.BRIDGE);
        r.markNow();
        final UnsafeBuffer big = new UnsafeBuffer(new byte[4096]);
        final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(big, 0, r.header)
                .egressSeq(1).journalSeq(2).meEpoch(1).releaseSeq(3).ledgerSize(4).ledgerCapacity(5).tombstoneRanges(0);
        final MeCommandReleaseEncoder.ReleasesEncoder g = e.releasesCount(49);
        for (int i = 0; i < 49; i++) {
            g.next().commandIdHigh(i).idLowFloor(0).scope(ReleaseScope.ORDER).result(ReleaseResult.APPLIED).removed(1);
        }
        r.demuxer.dispatch(bridge, big, 0, HEADER + e.encodedLength(), TS);
        assertEquals("49 entries: above the frozen cap of 48", 1, r.demuxer.decodeRejectCount());
        // numInGroup past the frame: a 2-entry frame claiming 3
        final int len = r.release(2, 0, 0);
        r.buf.putShort(HEADER + MeCommandReleaseEncoder.BLOCK_LENGTH + 2, (short) 3);
        r.dispatch(bridge, len);
        assertEquals(2, r.demuxer.decodeRejectCount());
        // an entry block below the generated one
        final int len2 = r.release(2, 0, 0);
        r.buf.putShort(HEADER + MeCommandReleaseEncoder.BLOCK_LENGTH, (short) 21);
        r.dispatch(bridge, len2);
        assertEquals(3, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.unhandledTemplateCount());
        assertEquals(List.of(), r.events());
    }

    /**
     * Verifier addition (S1 review): the adversarial fuzz corpus (CoreFrameStory) carries no 67 frame, so the wide-block
     * variants FrameMutator would derive are pinned here. A 67 re-encoded with a wider root block and wider entries is
     * judged by the frame's own dimensions: a valid one is counted unhandled, a poisoned entry in the wide layout is a
     * decode reject, never a read at the generated offsets.
     */
    @Test
    public void wideBlockReleaseFramesAreJudgedByTheirOwnDimensions() {
        final Rig r = new Rig();
        final long bridge = r.registered(60, SessionClass.BRIDGE);
        final byte[] before = r.snapshot();
        r.markNow();
        final int len = r.release(3, 1, 2);
        final byte[] canonical = new byte[len];
        r.buf.getBytes(0, canonical);
        final byte[] wide = widen(canonical, 16, 8); // root 44 -> 60, entries 22 -> 30
        final UnsafeBuffer w = new UnsafeBuffer(new byte[wide.length + 64]);
        w.putBytes(0, wide);
        r.demuxer.dispatch(bridge, w, 0, wide.length, TS);
        assertEquals("a valid wide 67 is unhandled", 1, r.demuxer.unhandledTemplateCount());
        assertEquals(0, r.demuxer.decodeRejectCount());
        // poison the LAST entry's result byte at the wide layout's offset (root 60, entry 30)
        final int lastEntry = HEADER + 60 + 4 + 2 * 30;
        w.putByte(lastEntry + MeCommandReleaseEncoder.ReleasesEncoder.resultEncodingOffset(), (byte) 3);
        r.demuxer.dispatch(bridge, w, 0, wide.length, TS);
        assertEquals("poison in the wide layout is seen", 1, r.demuxer.decodeRejectCount());
        assertEquals(1, r.demuxer.unhandledTemplateCount());
        // the same bytes with the count claiming one more entry than the frame holds: frame-bounds reject
        w.putByte(lastEntry + MeCommandReleaseEncoder.ReleasesEncoder.resultEncodingOffset(), (byte) 0);
        w.putShort(HEADER + 60 + 2, (short) 4, java.nio.ByteOrder.LITTLE_ENDIAN);
        r.demuxer.dispatch(bridge, w, 0, wide.length, TS);
        assertEquals(2, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(List.of(), r.events());
        assertArrayEquals(before, r.snapshot());
    }

    /** The frame with the root block wider by {@code wRoot} and every group entry wider by {@code w} zero bytes. */
    private static byte[] widen(final byte[] f, final int wRoot, final int w) {
        final int block = (f[0] & 0xFF) | (f[1] & 0xFF) << 8;
        final int groupAt = HEADER + block;
        final int entry = (f[groupAt] & 0xFF) | (f[groupAt + 1] & 0xFF) << 8;
        final int count = (f[groupAt + 2] & 0xFF) | (f[groupAt + 3] & 0xFF) << 8;
        final byte[] out = new byte[HEADER + block + wRoot + 4 + count * (entry + w)];
        System.arraycopy(f, 0, out, 0, HEADER + block);
        out[0] = (byte) (block + wRoot);
        out[1] = (byte) ((block + wRoot) >>> 8);
        int at = HEADER + block + wRoot;
        out[at] = (byte) (entry + w);
        out[at + 1] = (byte) ((entry + w) >>> 8);
        out[at + 2] = (byte) count;
        out[at + 3] = (byte) (count >>> 8);
        at += 4;
        for (int i = 0; i < count; i++) {
            System.arraycopy(f, groupAt + 4 + i * entry, out, at, entry);
            at += entry + w;
        }
        return out;
    }

    @Test
    public void outOfRangeEnumBytesAreDecodeRejects() {
        final Rig r = new Rig();
        final long bridge = r.registered(50, SessionClass.BRIDGE);
        final long admin = r.registered(51, SessionClass.ADMIN);
        final byte[] before = r.snapshot();
        r.markNow();
        r.dispatch(bridge, r.feedHalt(1, 0));      // NONE is not a halt
        r.dispatch(bridge, r.feedHalt(1, 6));
        r.dispatch(admin, r.latchClear(1, 0));     // nothing to clear
        r.dispatch(admin, r.latchClear(1, 6));
        r.dispatch(bridge, r.engineConfig(4));
        r.dispatch(bridge, r.engineConfig(255));
        r.dispatch(bridge, r.release(3, 2, 0));    // scope 0..1
        r.dispatch(bridge, r.release(3, 0, 3));    // result 0..2
        assertEquals(8, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.unhandledTemplateCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(List.of(), r.events());
        assertArrayEquals(before, r.snapshot());
    }
}

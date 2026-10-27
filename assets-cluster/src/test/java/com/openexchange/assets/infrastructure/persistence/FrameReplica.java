// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.cluster.SnapshotEnvelope;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * RC1 S8 (harness B4 test 3, B5 adversarial): one replica at the FRAME level, the in-process shape of a cluster
 * member: {@link AssetsEngine} + {@link SettlementProjector} + {@link AssetsSbeDemuxer} + a
 * {@link RecordingAssetsSink}. It applies {@link CoreFrameStory.Step}s exactly as the clustered service does
 * (log position set, then the session's dispatch or the trusted dispatch; a TimerEvent is the sweep; a
 * SessionOpen is the registry's open) and exposes the production snapshot image (v4 payload sealed in the
 * kit envelope) and the rendered event stream.
 */
final class FrameReplica {

    final AssetsEngine engine;
    final RecordingAssetsSink sink = new RecordingAssetsSink();
    final AssetsSbeDemuxer demuxer;
    /** Node-local cross-check failures (S7 section 4); every replica of a sane story must keep this at 0. */
    int failFastCalls;
    private final UnsafeBuffer frame = new UnsafeBuffer(new byte[0]);

    FrameReplica() {
        this(new AssetsEngine());
    }

    private FrameReplica(final AssetsEngine engine) {
        this.engine = engine;
        // RC2 S7: the v3 story runs behaviour 3, which the S7 jar supports only through this test seam (node-local)
        engine.coreConfig().setSupportedBehaviorVersions(1, 2, 3);
        engine.setEventSink(sink);
        engine.setConfigFailFast(() -> failFastCalls++);
        demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
    }

    /** A cold restart: open the envelope, restore into a FRESH engine (cross-check seam installed first), verify. */
    static FrameReplica restore(final byte[] image) {
        final UnsafeBuffer buffer = new UnsafeBuffer(image);
        final int payloadLength = new SnapshotEnvelope().open(buffer, image.length);
        final AssetsEngine fresh = new AssetsEngine();
        fresh.coreConfig().setSupportedBehaviorVersions(1, 2, 3); // before the codec's cross-check of section 8
        final int[] failFast = new int[1];
        fresh.setConfigFailFast(() -> failFast[0]++);
        final BalanceSnapshotCodec.Decoded decoded =
                BalanceSnapshotCodec.deserialize(buffer, SnapshotEnvelope.payloadOffset(), payloadLength, fresh);
        if (decoded.bytesConsumed != payloadLength) {
            throw new IllegalStateException("snapshot payload length mismatch: " + decoded.bytesConsumed + " of " + payloadLength);
        }
        fresh.verifyRestoredInvariants();
        final FrameReplica r = new FrameReplica(fresh);
        r.failFastCalls = failFast[0];
        return r;
    }

    /** Apply one log entry of the story. */
    void apply(final CoreFrameStory.Step s) {
        engine.setLogPosition(s.logPosition());
        switch (s.kind()) {
            case FRAME:
                frame.wrap(s.bytes());
                if (s.sessionId() == CoreFrameStory.TRUSTED) {
                    demuxer.dispatchTrusted(frame, 0, s.bytes().length, s.timestamp());
                } else {
                    demuxer.dispatch(s.sessionId(), frame, 0, s.bytes().length, s.timestamp());
                }
                break;
            case SWEEP:
                engine.sweep(s.timestamp());
                break;
            case OPEN_SESSION:
                engine.sessions().open(s.sessionId(), s.bytes());
                break;
            default:
                throw new IllegalStateException("step kind " + s.kind());
        }
    }

    /** A raw (possibly malformed) frame through the trusted dispatch, at the given log position and time. */
    void dispatchRaw(final DirectBuffer buffer, final int offset, final int length, final long logPosition, final long timestamp) {
        engine.setLogPosition(logPosition);
        demuxer.dispatchTrusted(buffer, offset, length, timestamp);
    }

    /** The production snapshot image: the v4 payload sealed in the kit envelope (what onTakeSnapshot offers). */
    byte[] image() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64 * 1024);
        final int payloadLength = BalanceSnapshotCodec.serialize(engine, buffer, SnapshotEnvelope.payloadOffset());
        final int length = new SnapshotEnvelope().seal(buffer, payloadLength);
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    int eventCount() {
        return sink.events().size();
    }

    /** Rendered events from index {@code from} (inclusive) to the end. */
    List<String> rendered(final int from) {
        final List<AssetsEngineEvent> events = sink.events();
        final List<String> out = new ArrayList<>(events.size() - from);
        for (int i = from; i < events.size(); i++) {
            out.add(events.get(i).render());
        }
        return out;
    }

    /** Every ingress drop counter of the demuxer, summed: a frame that moved this total was dropped. */
    long dropTotal() {
        return demuxer.schemaRejectCount() + demuxer.decodeRejectCount() + demuxer.unknownTemplateCount()
                + demuxer.unhandledTemplateCount();
    }
}

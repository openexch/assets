// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Aeron {@link ClusteredService} for the Assets Engine — the boundary that turns the deterministic
 * {@link AssetsEngine} into a replicated cluster service. Thin: ingress is decoded by
 * {@link AssetsSbeDemuxer} and applied to the engine (on every replica, deterministically); egress is
 * emitted by {@link AssetsEventPublisher} to client sessions (leader only); snapshots go through the
 * unit-tested {@link BalanceSnapshotCodec}.
 *
 * <p><b>Phase 0 status:</b> the ingress path and the snapshot byte format are exercised by the
 * determinism + codec test suites. The Aeron snapshot <em>transport</em> (offer/poll/reassemble) and
 * live leader egress are exercised when the node is first booted (Phase 2 shadow mode) — that is where
 * the two-cluster-on-one-box capacity is validated on appropriate hardware. Money egress is reliable
 * (never shed): {@link #broadcast} backpressures rather than drops.</p>
 */
public final class AssetsClusteredService implements ClusteredService {

    private final AssetsEngine engine = new AssetsEngine();
    // The projector owns the feed-forward translation (Settle/TerminalRelease -> money mutation +
    // consumePosition advance). It wraps the same engine instance for the life of the service:
    // loadSnapshot restores INTO that instance, so the projector needs no re-wiring on recovery.
    private final SettlementProjector projector = new SettlementProjector(engine);
    private final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, projector);

    private Cluster cluster;
    private IdleStrategy idleStrategy;
    private boolean isLeader;

    // Reusable egress staging buffer for the leader-only broadcast to client sessions.
    private final MutableDirectBuffer egressBuffer = new UnsafeBuffer(new byte[256]);

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
        this.isLeader = cluster.role() == Cluster.Role.LEADER;
        engine.setEventSink(new AssetsEventPublisher(this::broadcast));
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        // No per-session state in Phase 0; egress is broadcast to all sessions on the leader.
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        // No per-session state to release.
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer,
                                 int offset, int length, Header header) {
        // Deterministic on every replica: decode -> domain command -> engine mutation.
        demuxer.dispatch(buffer, offset, length, timestamp);
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        // Holds have no TTL yet (auto-expiry is a later phase).
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(engine, buffer);
        idleStrategy.reset();
        while (true) {
            final long result = snapshotPublication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (result == ExclusivePublication.CLOSED
                    || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("failed to offer assets snapshot: " + result);
            }
            idleStrategy.idle();
        }
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        this.isLeader = newRole == Cluster.Role.LEADER;
    }

    @Override
    public void onTerminate(Cluster cluster) {
        // Nothing to close in Phase 0.
    }

    private void loadSnapshot(Image snapshotImage) {
        final ExpandableArrayBuffer collector = new ExpandableArrayBuffer();
        final MutableInteger length = new MutableInteger(0);
        // A snapshot is one logical message (possibly fragmented); reassemble it, then decode.
        final FragmentHandler handler = (buffer, offset, len, header) -> {
            collector.putBytes(0, buffer, offset, len);
            length.value = len;
        };
        final io.aeron.FragmentAssembler assembler = new io.aeron.FragmentAssembler(handler);
        idleStrategy.reset();
        while (true) {
            final int fragments = snapshotImage.poll(assembler, 1);
            if (length.value > 0) {
                break;
            }
            if (snapshotImage.isEndOfStream()) {
                break;
            }
            idleStrategy.idle(fragments);
        }
        if (length.value > 0) {
            BalanceSnapshotCodec.deserialize(collector, 0, length.value, engine);
        }
    }

    /** Reliable leader-only egress: backpressure (retry), never shed — a dropped money event is unrecoverable. */
    private void broadcast(MutableDirectBuffer buffer, int offset, int length) {
        if (!isLeader || cluster == null) {
            return; // followers replicate state but do not emit client egress
        }
        for (final ClientSession session : cluster.clientSessions()) {
            idleStrategy.reset();
            while (true) {
                final long result = session.offer(buffer, offset, length);
                if (result > 0) {
                    break;
                }
                if (result == io.aeron.Publication.CLOSED
                        || result == io.aeron.Publication.MAX_POSITION_EXCEEDED
                        || result == io.aeron.Publication.NOT_CONNECTED) {
                    break; // session gone — skip it (state is safe; the consumer will resnapshot)
                }
                idleStrategy.idle();
            }
        }
    }

    /** Exposed for tests / diagnostics. */
    public AssetsEngine engine() {
        return engine;
    }
}

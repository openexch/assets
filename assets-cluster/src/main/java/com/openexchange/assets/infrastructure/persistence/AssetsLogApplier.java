// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.MoneyJournalSink;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import com.openexchange.cluster.SnapshotEnvelope;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * RCS S1 (G6/G7): the ONE place where the Assets Engine's replicated state is mutated from log entries. It owns the
 * engine, its projector, its demuxer and the snapshot envelope for its whole life and is addressed by cluster session
 * id, never by a {@code ClientSession}, so a live service, a log reader and a shadow apply the same entries through
 * the same code. The live {@link AssetsClusteredService} is a thin transport wrapper around one instance.
 *
 * <p><b>Hard rules.</b></p>
 * <ol>
 *   <li>Anything that mutates replicated state is reached through this class, never from a service wrapper.</li>
 *   <li>Inputs come from the log only: cluster session id, timestamp, buffer, frame-end position, principal bytes. No
 *       clock, environment, role or {@code Cluster} reaches the applier.</li>
 *   <li>The applier knows nothing about shadows, stubs or skip windows; callers gate calls in front of it.</li>
 * </ol>
 *
 * <p><b>Position rule (kept from a9a865d).</b> Only a non-Subscribe session message stamps
 * {@code engine.setLogPosition(logPosition)}. Open, close, timer and term events receive their frame's position too,
 * but ignore it until RCS S4 (RF-3) changes the bodies; the signatures do not change then.</p>
 *
 * <p><b>Refusals.</b> The applier never calls a fail-fast, never logs a FATAL line, never fences and never touches
 * readiness. Any exception out of an entry latches the instance ({@link #failed()}): every later entry throws
 * {@link IllegalStateException} and the owner discards the instance. The one exception is a snapshot candidate the
 * envelope refuses (magic, version, length, CRC32C): nothing was decoded, so the instance stays pristine and a later
 * {@link #restore} may try another candidate.</p>
 *
 * <p>Not thread-safe: the owner's apply thread only. Zero allocation on the message, timer, open (beyond what the
 * session registry allocates) and close paths.</p>
 */
public final class AssetsLogApplier {

    public static final int ENGINE_ID_ASSETS = 1;
    /** Timer ids are {@code BASE + leadershipTermId * TIMER_ID_TERM_STRIDE}; anything at or above a base is that kind. */
    public static final long EGRESS_DRAIN_TIMER_BASE = 1_000_000_000_000L;
    public static final long SWEEP_TIMER_BASE = 2_000_000_000_000_000L;
    public static final long TIMER_ID_TERM_STRIDE = 1_000_000L;
    public static final int TIMER_NONE = 0;
    public static final int TIMER_SWEEP = 1;
    public static final int TIMER_DRAIN = 2;

    private final AssetsEngine engine = new AssetsEngine();
    // The projector owns the feed-forward translation (Settle/TerminalRelease -> money mutation + consumePosition
    // advance). It wraps the same engine instance for the life of the applier: restore decodes INTO that instance.
    private final SettlementProjector projector = new SettlementProjector(engine);
    private final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, projector);
    private final SnapshotEnvelope envelope = new SnapshotEnvelope();
    /** Classification only (Subscribe split); never shared with a transport peek. */
    private final MessageHeaderDecoder classifyHeader = new MessageHeaderDecoder();
    private BalanceSnapshotCodec.Decoded lastRestored;
    /** No {@code on*}, {@code restore} or {@code genesis} call yet: the engine is genesis state. */
    private boolean pristine = true;
    /** An entry threw: the engine is in an unspecified state and this instance must be discarded. */
    private boolean failed;

    /**
     * @param configFailFast the node-local stop for a CoreConfig cross-check failure (A-9 rule); required
     */
    public AssetsLogApplier(final Runnable configFailFast) {
        engine.setConfigFailFast(Objects.requireNonNull(configFailFast, "configFailFast"));
        engine.setEventSink(NoopAssetsEventSink.INSTANCE);
    }

    // ---------------------------------------------------------------- node-local wiring, never replicated state

    public void setEventSink(final AssetsEventSink sink) {
        engine.setEventSink(Objects.requireNonNull(sink, "sink"));
    }

    public void setMoneyJournalSink(final MoneyJournalSink sink) {
        engine.setMoneyJournalSink(sink);
    }

    /** Leader-local transport effects of RegisterSession; {@code null} installs {@code TransportHooks.NONE}. */
    public void setTransportHooks(final AssetsSbeDemuxer.TransportHooks hooks) {
        demuxer.setTransportHooks(hooks);
    }

    // ---------------------------------------------------------------- the shadow-engine surface

    public int engineId() {
        return ENGINE_ID_ASSETS;
    }

    /** Start from genesis: a fresh engine already is genesis state. Only legal on a pristine instance. */
    public void genesis() {
        ensureNotFailed();
        if (!pristine) {
            throw new IllegalStateException("assets applier genesis requires a pristine instance");
        }
        pristine = false;
    }

    /**
     * Restore from a reassembled snapshot image: open the kit envelope (magic, version, length, CRC32C), decode the
     * v4 payload into this instance's engine, require it to end exactly where the envelope says, verify the ledger
     * invariants. Only legal on a pristine instance: the codec restores INTO the engine and is additive, so a second
     * restore (or a restore after any apply) would merge two states silently.
     */
    public void restore(final DirectBuffer image, final int length) {
        ensureNotFailed();
        if (!pristine) {
            throw new IllegalStateException("assets applier restore requires a pristine instance");
        }
        if (length <= 0) {
            throw new IllegalStateException("assets snapshot image is empty");
        }
        // A candidate the envelope refuses decoded nothing: the instance stays pristine and retryable.
        final int payloadLength = envelope.open(image, length);
        pristine = false;
        try {
            final BalanceSnapshotCodec.Decoded decoded = BalanceSnapshotCodec.deserialize(
                    image, SnapshotEnvelope.payloadOffset(), payloadLength, engine);
            if (decoded.bytesConsumed != payloadLength) {
                // The codec read fewer or more bytes than the envelope carried: a mis-framed payload.
                // Refusing to start beats serving a ledger assembled from part of a snapshot.
                throw new IllegalStateException("assets snapshot payload length mismatch: envelope=" + payloadLength
                        + " decoded=" + decoded.bytesConsumed + " accounts=" + decoded.accountCount
                        + " holds=" + decoded.holdCount);
            }
            engine.verifyRestoredInvariants();
            lastRestored = decoded;
        } catch (final RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /** The snapshot image: the v4 payload at {@link SnapshotEnvelope#payloadOffset()}, sealed; the image starts at 0. */
    public int serialize(final MutableDirectBuffer dst) {
        ensureNotFailed();
        return encode(engine, dst, envelope);
    }

    /**
     * SessionOpen: parse the principal into the session's authority. A clashing id is refused: the container never
     * delivers one (it throws before calling the service), so a caller that does has lost track of its sessions.
     *
     * @param logPosition the entry's frame-end position; ignored until RCS S4 (RF-3)
     */
    public void onSessionOpen(final long clusterSessionId, final long logPosition, final long timestamp,
                              final byte[] encodedPrincipal) {
        enter();
        try {
            if (engine.sessions().get(clusterSessionId) != null) {
                throw new IllegalStateException("clashing open clusterSessionId=" + clusterSessionId);
            }
            engine.sessions().open(clusterSessionId, encodedPrincipal);
        } catch (final RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /**
     * SessionClose: drop the session's authority (an unknown id is a no-op).
     *
     * @param logPosition the entry's frame-end position; ignored until RCS S4 (RF-3)
     */
    public void onSessionClose(final long clusterSessionId, final long logPosition, final long timestamp,
                               final CloseReason closeReason) {
        enter();
        try {
            engine.sessions().close(clusterSessionId);
        } catch (final RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /**
     * A session message. A well-formed Subscribe is a transport concern and changes nothing here (returns false).
     * Anything else stamps the position and is dispatched under the authority of {@code clusterSessionId}
     * (an id unknown to the registry dispatches with no authority).
     *
     * @return false for a well-formed Subscribe, true when the frame went to the engine
     */
    public boolean onSessionMessage(final long clusterSessionId, final long logPosition, final long timestamp,
                                    final DirectBuffer buffer, final int offset, final int length) {
        enter();
        try {
            if (isTransportSubscribe(buffer, offset, length)) {
                return false;
            }
            engine.setLogPosition(logPosition);
            demuxer.dispatch(clusterSessionId, buffer, offset, length, timestamp);
            return true;
        } catch (final RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /**
     * A TimerEvent: the deadline sweep is replicated state; the drain belt and any other id change nothing here.
     *
     * @param logPosition the entry's frame-end position; ignored until RCS S4 (RF-3)
     * @return {@link #timerKind(long)} of the id
     */
    public int onTimerEvent(final long correlationId, final long logPosition, final long timestamp) {
        enter();
        try {
            final int kind = timerKind(correlationId);
            if (kind == TIMER_SWEEP) {
                engine.sweep(timestamp);
            }
            return kind;
        } catch (final RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /**
     * Nothing in S1: the term id is the owner's (timer ids); RCS S4 (G28) drives the owner's term event, not this. Like
     * every {@code on*} entry it ends the pristine state, so a replayer restores or declares genesis before the first
     * term entry.
     */
    public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                         final TimeUnit timeUnit) {
        enter();
    }

    // ---------------------------------------------------------------- pure classification

    /**
     * True iff the frame is a well-formed Subscribe: a full header, Subscribe's template and schema ids, this
     * build's schema version, and a frame and declared block of at least Subscribe's block length. Anything else is
     * an engine frame (a malformed Subscribe reaches the demuxer, which drops and counts it).
     */
    public boolean isTransportSubscribe(final DirectBuffer buffer, final int offset, final int length) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return false;
        }
        classifyHeader.wrap(buffer, offset);
        if (classifyHeader.templateId() != SubscribeDecoder.TEMPLATE_ID
                || classifyHeader.schemaId() != SubscribeDecoder.SCHEMA_ID) {
            return false;
        }
        return classifyHeader.version() == MessageHeaderDecoder.SCHEMA_VERSION
                && length >= MessageHeaderDecoder.ENCODED_LENGTH + SubscribeDecoder.BLOCK_LENGTH
                && classifyHeader.blockLength() >= SubscribeDecoder.BLOCK_LENGTH;
    }

    public static int timerKind(final long correlationId) {
        if (isSweepTimerId(correlationId)) {
            return TIMER_SWEEP;
        }
        return isDrainTimerId(correlationId) ? TIMER_DRAIN : TIMER_NONE;
    }

    public static boolean isSweepTimerId(final long correlationId) {
        return correlationId >= SWEEP_TIMER_BASE;
    }

    public static boolean isDrainTimerId(final long correlationId) {
        return correlationId >= EGRESS_DRAIN_TIMER_BASE && correlationId < SWEEP_TIMER_BASE;
    }

    // ---------------------------------------------------------------- read access and the static codec path

    public AssetsEngine engine() {
        return engine;
    }

    public AssetsSbeDemuxer demuxer() {
        return demuxer;
    }

    /** The decode summary of the last successful {@link #restore}; null until one succeeds. */
    public BalanceSnapshotCodec.Decoded lastRestored() {
        return lastRestored;
    }

    /** True once an entry threw; every later entry refuses. */
    public boolean failed() {
        return failed;
    }

    /** The snapshot image as offered: the v4 payload written at the envelope's payload offset, then sealed. */
    static int encode(final AssetsEngine engine, final MutableDirectBuffer dst, final SnapshotEnvelope envelope) {
        final int payloadLength = BalanceSnapshotCodec.serialize(engine, dst, SnapshotEnvelope.payloadOffset());
        return envelope.seal(dst, payloadLength);
    }

    private void ensureNotFailed() {
        if (failed) {
            throw new IllegalStateException("assets applier failed; discard the instance");
        }
    }

    private void enter() {
        ensureNotFailed();
        pristine = false;
    }
}

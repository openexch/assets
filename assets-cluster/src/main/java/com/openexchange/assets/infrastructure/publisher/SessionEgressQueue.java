// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.publisher;

import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.HoldAckBatchEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotBatchEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedBatchEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder;
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableRingBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * One egress queue per client session: encoded frames are appended by the engine's apply and offered
 * later, in bulk, by the service thread's drain.
 *
 * <p><b>Why a queue at all.</b> The engine used to call {@link ClientSession#offer} inline, inside the
 * command apply, and spin on back-pressure. One slow consumer therefore parked the whole deterministic
 * thread: ingress stopped and every other session stopped with it. Appending here and draining at the
 * end of the duty cycle decouples the two.</p>
 *
 * <p><b>Why one queue per session, not one shared queue.</b> Back-pressure is per session. A shared
 * queue would either head-of-line block every consumer behind the slowest one, or re-deliver frames to
 * sessions that already took them. Per-session queues are also where subscription filtering plugs in.</p>
 *
 * <p><b>Why {@link ExpandableRingBuffer}.</b> Its {@code consume} contract is exactly what reliable
 * money egress needs: the consumer returns false and that frame, plus every frame after it, stays in
 * place and in order for the next drain. Back-pressure becomes "retry next cycle" instead of a spin,
 * with nothing dropped. It is the same primitive Aeron uses for its own pending cluster messages, and
 * it copies on {@code append}, which matters because {@link AssetsEventPublisher} hands out a buffer it
 * immediately reuses for the next event.</p>
 *
 * <p><b>v5 batch coalescing.</b> The drain no longer offers every queued frame as-is: a run of
 * consecutive same-type frames of the four high-rate egress types (BalanceUpdate, HoldAck,
 * SettlementApplied, HoldSnapshotEntry) leaves as ONE batch frame (BalanceUpdateBatch and friends),
 * capped at {@link #BATCH_CAP} entries. A batch flushes when the type changes, the cap is hit, or the
 * drain cycle ends — so cross-type ORDER is exactly the queue order, and latency is never held waiting
 * for a batch to fill. Framing was ~40B per 28-69B message; a settle burst that used to be hundreds of
 * offers on the deterministic thread becomes a handful. Everything else (rejects, acks with a money
 * consequence to shout about, snapshot terminators, feed reports) passes through as singles.
 * Subscription filtering is untouched: it happened at append time, before frames ever reach this
 * drain. A batch whose offer is back-pressured parks, encoded, in {@link #batchScratch} and is
 * re-offered FIRST on the next drain — nothing is dropped and nothing overtakes it.</p>
 *
 * <p>Single-threaded by contract: only the cluster service thread touches an instance.</p>
 */
public final class SessionEgressQueue {

    /**
     * Pre-sized well past the steady state (one command's events are a few hundred bytes), but small
     * enough that a session reconnect storm cannot pile up direct memory. Growth only happens once a
     * consumer is already falling behind, which is exactly when the capacity is worth paying for.
     */
    static final int INITIAL_CAPACITY = 1 << 16;   // 64 KB
    /** Growth ceiling per session. Past this the caller must decide rather than shed money events. */
    // Property-overridable as transport-level tuning (and so tests can reach the ceiling without 16MB
    // of traffic): per-node, never replicated state, so a system property is legitimate here.
    static final int MAX_CAPACITY = Integer.getInteger("assets.egress.max.capacity.bytes", 1 << 24);
    /** Frames offered per session per drain, so one session cannot monopolise a duty cycle. */
    static final int DRAIN_LIMIT = 256;
    /** Max entries coalesced into one egress batch frame. */
    public static final int BATCH_CAP = 128;
    /** Widest batch entry is HoldAck (36B); header 8 + group dimension 4 + slack, rounded up. */
    private static final int BATCH_SCRATCH_CAPACITY = 8192;
    /** Max fields any batchable entry carries (HoldAck: correlationId, orderId, userId, assetId, amount). */
    private static final int MAX_ENTRY_FIELDS = 5;

    private final ClientSession session;
    private final ExpandableRingBuffer pending =
            new ExpandableRingBuffer(INITIAL_CAPACITY, MAX_CAPACITY, true);
    // Bound once: consume() is called on the money path, and a fresh lambda there would allocate.
    private final ExpandableRingBuffer.MessageConsumer offerConsumer = this::onQueuedFrame;

    // ---- batch coalescing state (service thread only, like everything else here) ----
    // Staged entries of the currently-open batch, decoded from their single frames: column-major,
    // stagedFields[f][i] = field f of entry i. One generic staging block serves all four batchable
    // types because none has more than MAX_ENTRY_FIELDS fields; assetId (int32) rides in a long.
    private final long[][] stagedFields = new long[MAX_ENTRY_FIELDS][BATCH_CAP];
    /** Single-form templateId of the open batch; 0 = no batch open. */
    private int openBatchTemplateId;
    private int openBatchCount;
    /** An encoded batch frame the session back-pressured: length in {@link #batchScratch}; 0 = none. */
    private int parkedBatchLength;
    private final UnsafeBuffer batchScratch = new UnsafeBuffer(new byte[BATCH_SCRATCH_CAPACITY]);

    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final BalanceUpdateDecoder balanceUpdateDecoder = new BalanceUpdateDecoder();
    private final HoldAckDecoder holdAckDecoder = new HoldAckDecoder();
    private final SettlementAppliedDecoder settlementAppliedDecoder = new SettlementAppliedDecoder();
    private final HoldSnapshotEntryDecoder holdSnapshotEntryDecoder = new HoldSnapshotEntryDecoder();
    private final MessageHeaderEncoder batchHeaderEncoder = new MessageHeaderEncoder();
    private final BalanceUpdateBatchEncoder balanceUpdateBatchEncoder = new BalanceUpdateBatchEncoder();
    private final HoldAckBatchEncoder holdAckBatchEncoder = new HoldAckBatchEncoder();
    private final SettlementAppliedBatchEncoder settlementAppliedBatchEncoder = new SettlementAppliedBatchEncoder();
    private final HoldSnapshotBatchEncoder holdSnapshotBatchEncoder = new HoldSnapshotBatchEncoder();

    private long backPressureCount;
    private int peakPendingBytes;
    /**
     * Which BROADCAST egress channels this session wants — replies to its own commands bypass this
     * mask entirely. Defaults to everything, so a client that never sends Subscribe behaves exactly
     * as it did before subscriptions existed. Transport state, never snapshotted: after a leader
     * change or a snapshot restore it falls back to this default, which is the safe direction (more
     * traffic, never missing traffic).
     */
    private int channelMask = AssetsEventPublisher.CH_ALL;
    /**
     * Whether this session's consumer has PROVEN it can survive losing the session: set when it sends
     * QueryFeedPosition, the first step of the resume protocol (reconnect -> re-query -> skip-filter,
     * with AE-side idempotency underneath). The engine's full-queue handling keys on this: a resumable
     * consumer's session may be closed instead of blocking the engine, because resync replays
     * everything from positions. Transport state exactly like {@link #channelMask} — never
     * snapshotted. After a leader change the fresh queue defaults to NOT resumable, which is the safe
     * direction: an unproven consumer blocks exactly as today, and the bridge re-identifies itself
     * with a fresh QueryFeedPosition at every epoch start.
     */
    private boolean resumable;

    public SessionEgressQueue(final ClientSession session) {
        this.session = session;
    }

    public ClientSession session() {
        return session;
    }

    public long sessionId() {
        return session.id();
    }

    public boolean isEmpty() {
        return pending.isEmpty() && openBatchCount == 0 && parkedBatchLength == 0;
    }

    public int pendingBytes() {
        return pending.size() + parkedBatchLength;
    }

    public long backPressureCount() {
        return backPressureCount;
    }

    public int peakPendingBytes() {
        return peakPendingBytes;
    }

    public int channelMask() {
        return channelMask;
    }

    public boolean wants(final int channel) {
        return (channelMask & channel) != 0;
    }

    /** The consumer sent QueryFeedPosition: it owns a resume protocol. See {@link #resumable}. */
    public void markResumable() {
        this.resumable = true;
    }

    public boolean isResumable() {
        return resumable;
    }

    /**
     * The mask is taken exactly as asked: it narrows BROADCAST streams only, because request-scoped
     * replies are routed to their origin unconditionally (see
     * {@code AssetsClusteredService#enqueueToOrigin}). SNAPSHOTS used to imply BALANCES here — a
     * snapshot reply streams its entries as ordinary BalanceUpdate frames, and a mask that could
     * silence them would turn the terminator's entry COUNT into a silently wrong answer — with a note
     * that a cleaner fix belonged later. Origin-unconditional replies are that cleaner fix: the
     * entries reach the asker regardless of its mask, so the coupling is gone with the hazard it
     * guarded.
     */
    public void subscribe(final int channels) {
        this.channelMask = channels;
    }

    /**
     * Copy an encoded frame into this session's queue.
     *
     * @return false only when {@link #MAX_CAPACITY} is reached, which the caller must resolve by
     *         draining rather than by dropping the frame.
     */
    public boolean append(final DirectBuffer buffer, final int offset, final int length) {
        if (!pending.append(buffer, offset, length)) {
            return false;
        }
        final int size = pending.size();
        if (size > peakPendingBytes) {
            peakPendingBytes = size;
        }
        return true;
    }

    /**
     * Offer queued frames to the session, stopping at the first back-pressure.
     *
     * @return ring-buffer bytes consumed, 0 when nothing could be taken off the queue — the value the
     *         engine's free-space stall loop keys on, so a parked batch (which frees no queue space)
     *         deliberately does not count.
     */
    public int drain() {
        if (!offerParkedBatch()) {
            return 0; // the parked batch still blocks the line; nothing may overtake it
        }
        final int consumed = pending.consume(offerConsumer, DRAIN_LIMIT);
        flushOpenBatch(); // the drain cycle is over for this pass; a batch never waits for more events
        return consumed;
    }

    /**
     * Drain until the queue is empty or the session back-pressures. {@link #DRAIN_LIMIT} bounds a single
     * {@code consume} pass, not the drain: a command that emits thousands of frames (a balance snapshot
     * reply) must not need thousands of duty cycles to leave.
     *
     * @return ring-buffer bytes consumed across all passes.
     */
    public int drainAll() {
        if (!offerParkedBatch()) {
            return 0;
        }
        int total = 0;
        int consumed;
        while ((consumed = pending.consume(offerConsumer, DRAIN_LIMIT)) > 0) {
            total += consumed;
        }
        // End of the drain cycle: whatever is still coalescing goes out now (or parks on back-pressure).
        flushOpenBatch();
        return total;
    }

    /**
     * Discard everything queued, for example when this node stops being the leader. {@link #resumable}
     * survives deliberately: it describes the consumer on this session, not the queued frames.
     */
    public void reset() {
        pending.reset(INITIAL_CAPACITY);
        openBatchTemplateId = 0;
        openBatchCount = 0;
        parkedBatchLength = 0;
    }

    /**
     * One frame off the ring. Batchable types are decoded into the staging columns and leave later as
     * one batch frame; everything else must first flush the open batch (order!) and then goes out as
     * the original single frame. Returning false keeps THIS frame and everything after it queued.
     */
    private boolean onQueuedFrame(final MutableDirectBuffer buffer, final int offset, final int length,
                                  final int headOffset) {
        final int batchable = batchableTemplateId(buffer, offset, length);
        if (batchable != 0) {
            if (openBatchTemplateId != 0 && openBatchTemplateId != batchable && !flushOpenBatch()) {
                return false;
            }
            if (openBatchCount == BATCH_CAP && !flushOpenBatch()) {
                return false;
            }
            stage(batchable, buffer, offset);
            return true;
        }
        if (openBatchTemplateId != 0 && !flushOpenBatch()) {
            return false;
        }
        return offerFrame(buffer, offset, length);
    }

    /**
     * The single-form templateId if this frame is one of the four batch-coalesced egress types on OUR
     * schema, else 0. The schemaId check is not paranoia: this queue also carries whatever bytes a test
     * or a future foreign frame appends, and a foreign frame that aliased a batchable templateId would
     * be silently re-encoded into garbage.
     */
    private int batchableTemplateId(final DirectBuffer buffer, final int offset, final int length) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return 0;
        }
        frameHeaderDecoder.wrap(buffer, offset);
        if (frameHeaderDecoder.schemaId() != BalanceUpdateDecoder.SCHEMA_ID) {
            return 0;
        }
        final int templateId = frameHeaderDecoder.templateId();
        switch (templateId) {
            case BalanceUpdateDecoder.TEMPLATE_ID:
            case HoldAckDecoder.TEMPLATE_ID:
            case SettlementAppliedDecoder.TEMPLATE_ID:
            case HoldSnapshotEntryDecoder.TEMPLATE_ID:
                return templateId;
            default:
                return 0;
        }
    }

    /** Decode one single frame into the staging columns of the (possibly just-opened) batch. */
    private void stage(final int templateId, final DirectBuffer buffer, final int offset) {
        openBatchTemplateId = templateId;
        final int i = openBatchCount++;
        switch (templateId) {
            case BalanceUpdateDecoder.TEMPLATE_ID -> {
                balanceUpdateDecoder.wrapAndApplyHeader(buffer, offset, frameHeaderDecoder);
                stagedFields[0][i] = balanceUpdateDecoder.userId();
                stagedFields[1][i] = balanceUpdateDecoder.assetId();
                stagedFields[2][i] = balanceUpdateDecoder.available();
                stagedFields[3][i] = balanceUpdateDecoder.locked();
            }
            case HoldAckDecoder.TEMPLATE_ID -> {
                holdAckDecoder.wrapAndApplyHeader(buffer, offset, frameHeaderDecoder);
                stagedFields[0][i] = holdAckDecoder.correlationId();
                stagedFields[1][i] = holdAckDecoder.orderId();
                stagedFields[2][i] = holdAckDecoder.userId();
                stagedFields[3][i] = holdAckDecoder.assetId();
                stagedFields[4][i] = holdAckDecoder.amount();
            }
            case SettlementAppliedDecoder.TEMPLATE_ID -> {
                settlementAppliedDecoder.wrapAndApplyHeader(buffer, offset, frameHeaderDecoder);
                stagedFields[0][i] = settlementAppliedDecoder.tradeId();
                stagedFields[1][i] = settlementAppliedDecoder.buyerUserId();
                stagedFields[2][i] = settlementAppliedDecoder.sellerUserId();
            }
            case HoldSnapshotEntryDecoder.TEMPLATE_ID -> {
                holdSnapshotEntryDecoder.wrapAndApplyHeader(buffer, offset, frameHeaderDecoder);
                stagedFields[0][i] = holdSnapshotEntryDecoder.orderId();
                stagedFields[1][i] = holdSnapshotEntryDecoder.userId();
                stagedFields[2][i] = holdSnapshotEntryDecoder.assetId();
                stagedFields[3][i] = holdSnapshotEntryDecoder.remaining();
            }
            default -> throw new IllegalStateException("not a batchable templateId: " + templateId);
        }
    }

    /**
     * Encode the open batch into {@link #batchScratch} and offer it. On back-pressure the encoded frame
     * PARKS in the scratch (nothing lost, nothing reordered) and blocks the line until delivered.
     *
     * @return true when the line is clear (delivered, discarded to a gone session, or nothing to flush).
     */
    private boolean flushOpenBatch() {
        if (openBatchCount == 0) {
            return parkedBatchLength == 0;
        }
        if (parkedBatchLength != 0) {
            return false; // scratch is occupied by an undelivered batch; it must leave first
        }
        parkedBatchLength = encodeOpenBatch();
        openBatchTemplateId = 0;
        openBatchCount = 0;
        return offerParkedBatch();
    }

    /** @return the encoded frame length of the open batch, written at offset 0 of {@link #batchScratch}. */
    private int encodeOpenBatch() {
        final int count = openBatchCount;
        switch (openBatchTemplateId) {
            case BalanceUpdateDecoder.TEMPLATE_ID -> {
                final BalanceUpdateBatchEncoder.UpdatesEncoder g = balanceUpdateBatchEncoder
                        .wrapAndApplyHeader(batchScratch, 0, batchHeaderEncoder)
                        .updatesCount(count);
                for (int i = 0; i < count; i++) {
                    g.next()
                            .userId(stagedFields[0][i])
                            .assetId((int) stagedFields[1][i])
                            .available(stagedFields[2][i])
                            .locked(stagedFields[3][i]);
                }
                return MessageHeaderEncoder.ENCODED_LENGTH + balanceUpdateBatchEncoder.encodedLength();
            }
            case HoldAckDecoder.TEMPLATE_ID -> {
                final HoldAckBatchEncoder.AcksEncoder g = holdAckBatchEncoder
                        .wrapAndApplyHeader(batchScratch, 0, batchHeaderEncoder)
                        .acksCount(count);
                for (int i = 0; i < count; i++) {
                    g.next()
                            .correlationId(stagedFields[0][i])
                            .orderId(stagedFields[1][i])
                            .userId(stagedFields[2][i])
                            .assetId((int) stagedFields[3][i])
                            .amount(stagedFields[4][i]);
                }
                return MessageHeaderEncoder.ENCODED_LENGTH + holdAckBatchEncoder.encodedLength();
            }
            case SettlementAppliedDecoder.TEMPLATE_ID -> {
                final SettlementAppliedBatchEncoder.SettlementsEncoder g = settlementAppliedBatchEncoder
                        .wrapAndApplyHeader(batchScratch, 0, batchHeaderEncoder)
                        .settlementsCount(count);
                for (int i = 0; i < count; i++) {
                    g.next()
                            .tradeId(stagedFields[0][i])
                            .buyerUserId(stagedFields[1][i])
                            .sellerUserId(stagedFields[2][i]);
                }
                return MessageHeaderEncoder.ENCODED_LENGTH + settlementAppliedBatchEncoder.encodedLength();
            }
            case HoldSnapshotEntryDecoder.TEMPLATE_ID -> {
                final HoldSnapshotBatchEncoder.HoldsEncoder g = holdSnapshotBatchEncoder
                        .wrapAndApplyHeader(batchScratch, 0, batchHeaderEncoder)
                        .holdsCount(count);
                for (int i = 0; i < count; i++) {
                    g.next()
                            .orderId(stagedFields[0][i])
                            .userId(stagedFields[1][i])
                            .assetId((int) stagedFields[2][i])
                            .remaining(stagedFields[3][i]);
                }
                return MessageHeaderEncoder.ENCODED_LENGTH + holdSnapshotBatchEncoder.encodedLength();
            }
            default -> throw new IllegalStateException(
                    "open batch with unknown templateId: " + openBatchTemplateId);
        }
    }

    /** @return true when no parked batch remains in the way (delivered, discarded, or none). */
    private boolean offerParkedBatch() {
        if (parkedBatchLength == 0) {
            return true;
        }
        if (offerFrame(batchScratch, 0, parkedBatchLength)) {
            parkedBatchLength = 0;
            return true;
        }
        return false;
    }

    private boolean offerFrame(final DirectBuffer buffer, final int offset, final int length) {
        final long result = session.offer(buffer, offset, length);
        if (result > 0) {
            return true; // delivered (also covers ClientSession.MOCKED_OFFER on a follower)
        }
        if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
            backPressureCount++;
            return false; // keep this frame and everything after it for the next drain
        }
        // CLOSED / MAX_POSITION_EXCEEDED / NOT_CONNECTED: the session is gone. Discard, exactly as the
        // inline path did — the state is safe and the consumer resynchronises through the snapshot
        // request messages.
        return true;
    }
}

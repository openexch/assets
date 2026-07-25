// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.publisher;

import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableRingBuffer;
import org.agrona.MutableDirectBuffer;

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
 * sessions that already took them. Per-session queues are also where subscription filtering will plug
 * in later.</p>
 *
 * <p><b>Why {@link ExpandableRingBuffer}.</b> Its {@code consume} contract is exactly what reliable
 * money egress needs: the consumer returns false and that frame, plus every frame after it, stays in
 * place and in order for the next drain. Back-pressure becomes "retry next cycle" instead of a spin,
 * with nothing dropped. It is the same primitive Aeron uses for its own pending cluster messages, and
 * it copies on {@code append}, which matters because {@link AssetsEventPublisher} hands out a buffer it
 * immediately reuses for the next event.</p>
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
    /** Growth ceiling per session. Past this the caller must free space rather than shed money events. */
    static final int MAX_CAPACITY = 1 << 24;       // 16 MB
    /** Frames offered per session per drain, so one session cannot monopolise a duty cycle. */
    static final int DRAIN_LIMIT = 256;

    private final ClientSession session;
    private final ExpandableRingBuffer pending =
            new ExpandableRingBuffer(INITIAL_CAPACITY, MAX_CAPACITY, true);
    // Bound once: consume() is called on the money path, and a fresh lambda there would allocate.
    private final ExpandableRingBuffer.MessageConsumer offerConsumer = this::offerFrame;

    private long backPressureCount;
    private int peakPendingBytes;
    /**
     * Which egress channels this session wants. Defaults to everything, so a client that never sends
     * Subscribe behaves exactly as it did before subscriptions existed. Transport state, never
     * snapshotted: after a leader change or a snapshot restore it falls back to this default, which is
     * the safe direction (more traffic, never missing traffic).
     */
    private int channelMask = AssetsEventPublisher.CH_ALL;

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
        return pending.isEmpty();
    }

    public int pendingBytes() {
        return pending.size();
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

    /**
     * A snapshot reply streams its entries as ordinary BalanceUpdate frames and then a terminator, so a
     * session subscribed to SNAPSHOTS but not BALANCES would receive an entry COUNT with no entries: a
     * silently wrong answer. Rather than let that be expressible, SNAPSHOTS implies BALANCES here.
     * (The cleaner fix is a dedicated BalanceSnapshotEntry message, which belongs with the next schema
     * change; until then this coupling is enforced rather than documented and hoped for.)
     */
    public void subscribe(final int channels) {
        int mask = channels;
        if ((mask & AssetsEventPublisher.CH_SNAPSHOTS) != 0) {
            mask |= AssetsEventPublisher.CH_BALANCES;
        }
        this.channelMask = mask;
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
     * Offer queued frames to the session, stopping at the first one it back-pressures.
     *
     * @return bytes consumed, 0 when nothing could be delivered.
     */
    public int drain() {
        return pending.consume(offerConsumer, DRAIN_LIMIT);
    }

    /**
     * Drain until the queue is empty or the session back-pressures. {@link #DRAIN_LIMIT} bounds a single
     * {@code consume} pass, not the drain: a command that emits thousands of frames (a balance snapshot
     * reply) must not need thousands of duty cycles to leave.
     *
     * @return bytes consumed across all passes.
     */
    public int drainAll() {
        int total = 0;
        int consumed;
        while ((consumed = pending.consume(offerConsumer, DRAIN_LIMIT)) > 0) {
            total += consumed;
        }
        return total;
    }

    /** Discard everything queued, for example when this node stops being the leader. */
    public void reset() {
        pending.reset(INITIAL_CAPACITY);
    }

    private boolean offerFrame(final MutableDirectBuffer buffer, final int offset, final int length,
                               final int headOffset) {
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

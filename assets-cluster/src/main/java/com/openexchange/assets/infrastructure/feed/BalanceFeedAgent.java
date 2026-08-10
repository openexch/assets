// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.feed;

import com.openexchange.assets.infrastructure.Logger;
import com.openexchange.assets.infrastructure.feed.BalanceConflationStore.BalanceSlot;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.publisher.SessionEgressQueue;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Drains the {@link BalanceConflationStore}'s dirty slots and publishes their LATEST pairs as
 * BalanceUpdate / BalanceUpdateBatch frames on a plain Aeron publication — the balance feed
 * side-channel. Runs on its own AgentRunner thread: the cluster service thread never touches this
 * publication, it only writes the store.
 *
 * <p><b>Delivery contract: conflated, not lossless.</b> A slow consumer misses INTERMEDIATE values,
 * never the final truth: on back-pressure the batch's slots are re-marked pending agent-side and
 * re-read FRESH on the retry, and any value written meanwhile re-enters the dirty queue — either
 * way what goes out next is the latest. A consumer that needs a baseline (it connected late, or a
 * failover happened) takes it from the cluster (RequestBalanceSnapshot); the feed also replays a
 * full-store sweep whenever this node GAINS leadership, which re-baselines connected consumers
 * without a round trip.</p>
 *
 * <p>Publishes only while this node leads. As a follower it still drains the dirty queue (keeping
 * flags and queue empty — the store itself is always current on every node) and discards; the
 * leadership-gain sweep is what makes those discards safe.</p>
 *
 * <p>Batching reuses the wire's v5 shape: one dirty slot leaves as a single BalanceUpdate, a run of
 * them as one BalanceUpdateBatch capped at {@link SessionEgressQueue#BATCH_CAP} — the same cap the
 * cluster egress drain uses, so consumers see one frame vocabulary. No schema of its own.</p>
 */
public final class BalanceFeedAgent implements Agent {

    private static final Logger log = Logger.getLogger(BalanceFeedAgent.class);

    private static final int BATCH_CAP = SessionEgressQueue.BATCH_CAP;
    /** Batch frame: header 8 + block 0 + group header 4 + 128 * 28B entries = 3596; rounded up. */
    private static final int ENCODE_BUFFER_CAPACITY = 4096;
    private static final long CONNECT_RETRY_BACKOFF_MS = 1_000;
    private static final long STATS_INTERVAL_MS = 10_000;

    private final BalanceConflationStore store;
    private final String aeronDirectoryName;
    private final String channel;
    private final int streamId;

    private Aeron aeron;
    private Publication publication;
    private long lastConnectAttemptMs;

    private final UnsafeBuffer encodeBuffer = new UnsafeBuffer(new byte[ENCODE_BUFFER_CAPACITY]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final BalanceUpdateEncoder singleEncoder = new BalanceUpdateEncoder();
    private final BalanceUpdateBatchEncoder batchEncoder = new BalanceUpdateBatchEncoder();
    private final long[] pair = new long[2];

    /** Slots taken off the queue this cycle, and — after back-pressure — the ones still owed. */
    private final BalanceSlot[] batch = new BalanceSlot[BATCH_CAP];
    private int pendingCount;

    /** Full-store sweep in progress: next flat index to publish; -1 = no sweep. */
    private int sweepCursor = -1;

    private long lastStatsMs;
    private long lastStatsFrames = -1;

    private volatile long publishedFrames;
    private volatile long publishedEntries;
    private volatile long backPressureEvents;
    private volatile long droppedNoSubscriber;
    private volatile long sweepsCompleted;

    public BalanceFeedAgent(
            final BalanceConflationStore store,
            final String aeronDirectoryName,
            final String channel,
            final int streamId) {
        this.store = store;
        this.aeronDirectoryName = aeronDirectoryName;
        this.channel = channel;
        this.streamId = streamId;
    }

    @Override
    public int doWork() {
        if (publication == null && !tryConnect()) {
            return 0;
        }
        final boolean leader = store.isLeader();

        // A batch the publication back-pressured: re-read those slots FRESH and re-offer before
        // touching anything else — cheap and correct, because conflation means the latest pair is
        // all a consumer is owed. A demotion abandons them: the next leader's gain-sweep re-baselines.
        if (pendingCount > 0) {
            if (!leader) {
                pendingCount = 0;
            } else if (!publishPending()) {
                return 0; // still back-pressured; idle and retry
            }
        }

        int work = 0;
        if (store.consumeSweepRequest() && leader) {
            sweepCursor = 0; // (re)start: a sweep interrupted by re-election restarts from 0, harmless
        }
        if (sweepCursor >= 0) {
            work += sweepStep(leader);
            if (pendingCount > 0) {
                return work; // sweep batch back-pressured; finish it first
            }
        }

        // Dirty queue: drained on EVERY node so flags clear and the queue never sits full on a
        // follower; published only on the leader.
        int taken = 0;
        BalanceSlot slot;
        while (taken < BATCH_CAP && (slot = store.pollDirty()) != null) {
            batch[taken++] = slot;
        }
        if (taken > 0 && leader) {
            pendingCount = taken;
            publishPending();
        }
        work += taken;
        maybeLogStats();
        return work;
    }

    /**
     * Publish up to one batch of the full-store sweep — the feed-side snapshot on leadership gain.
     * Interleaved with the dirty queue so live updates keep flowing during a large sweep.
     */
    private int sweepStep(final boolean leader) {
        if (!leader) {
            sweepCursor = -1; // demoted mid-sweep: abandon; the next gain requests a fresh one
            return 0;
        }
        final int live = store.liveSlots();
        if (sweepCursor >= live) {
            sweepCursor = -1;
            sweepsCompleted = sweepsCompleted + 1;
            log.info("Balance feed sweep complete: %d slots (leadership baseline)", live);
            return 0;
        }
        int taken = 0;
        while (taken < BATCH_CAP && sweepCursor < live) {
            batch[taken++] = store.slotAt(sweepCursor++);
        }
        pendingCount = taken;
        publishPending();
        return taken;
    }

    /**
     * Encode {@code batch[0..pendingCount)} — each slot seqlock-read at THIS moment, so a retry
     * republishes fresh values — and offer the frame.
     *
     * @return true when the line is clear (delivered, or dropped because no consumer is connected);
     *         false on back-pressure, with the slots kept in {@code batch} for the next cycle.
     */
    private boolean publishPending() {
        final int count = pendingCount;
        final int length = encode(count);
        final long result = publication.offer(encodeBuffer, 0, length);
        if (result > 0) {
            publishedFrames = publishedFrames + 1;
            publishedEntries = publishedEntries + count;
            pendingCount = 0;
            return true;
        }
        if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
            backPressureEvents = backPressureEvents + 1;
            return false; // slots stay pending; the retry re-reads them fresh
        }
        if (result == Publication.NOT_CONNECTED) {
            // No subscriber: the feed is a latest-value convenience, not the record. Drop — a
            // consumer that connects later baselines from the cluster and rides updates from there.
            droppedNoSubscriber = droppedNoSubscriber + 1;
            pendingCount = 0;
            return true;
        }
        // CLOSED / MAX_POSITION_EXCEEDED: this publication is finished. Recreate on the next cycle;
        // the slots stay pending so nothing conflates away silently while we do.
        log.error("Balance feed publication unusable (%d); recreating", result);
        CloseHelper.quietClose(publication);
        publication = null;
        return false;
    }

    private int encode(final int count) {
        if (count == 1) {
            final BalanceSlot slot = batch[0];
            slot.readInto(pair);
            singleEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
                    .userId(slot.userId()).assetId(slot.assetId()).available(pair[0]).locked(pair[1]);
            return MessageHeaderEncoder.ENCODED_LENGTH + singleEncoder.encodedLength();
        }
        final BalanceUpdateBatchEncoder.UpdatesEncoder g = batchEncoder
                .wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
                .updatesCount(count);
        for (int i = 0; i < count; i++) {
            final BalanceSlot slot = batch[i];
            slot.readInto(pair);
            g.next().userId(slot.userId()).assetId(slot.assetId()).available(pair[0]).locked(pair[1]);
        }
        return MessageHeaderEncoder.ENCODED_LENGTH + batchEncoder.encodedLength();
    }

    /**
     * Connect lazily with a retry cadence, like the money-journal writer: a slow-starting media
     * driver never blocks node boot, and the store conflates in the meantime.
     */
    private boolean tryConnect() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastConnectAttemptMs < CONNECT_RETRY_BACKOFF_MS) {
            return false;
        }
        lastConnectAttemptMs = nowMs;
        try {
            if (aeron == null) {
                aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
            }
            publication = aeron.addPublication(channel, streamId);
            log.info("Balance feed connected: channel=%s stream=%d session=%d",
                    channel, streamId, publication.sessionId());
            return true;
        } catch (final Exception e) {
            CloseHelper.quietClose(aeron);
            aeron = null;
            publication = null;
            log.error("Balance feed connect failed: %s", e.getMessage());
            return false;
        }
    }

    /** Periodic, change-gated stats line, mirroring the money-journal writer's. */
    private void maybeLogStats() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastStatsMs < STATS_INTERVAL_MS) {
            return;
        }
        lastStatsMs = nowMs;
        final long frames = publishedFrames;
        if (frames != lastStatsFrames) {
            lastStatsFrames = frames;
            log.info("BALANCE FEED STATS: frames=%d entries=%d backPressure=%d droppedNoSub=%d sweeps=%d",
                    frames, publishedEntries, backPressureEvents, droppedNoSubscriber, sweepsCompleted);
        }
    }

    /** Frames offered successfully since boot. */
    public long publishedFrames() {
        return publishedFrames;
    }

    /** BalanceUpdate entries (singles + batch members) offered successfully since boot. */
    public long publishedEntries() {
        return publishedEntries;
    }

    /** Full-store sweeps completed (one per leadership gain, plus any overflow fallback). */
    public long sweepsCompleted() {
        return sweepsCompleted;
    }

    @Override
    public void onClose() {
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(aeron);
    }

    @Override
    public String roleName() {
        return "balance-feed";
    }
}

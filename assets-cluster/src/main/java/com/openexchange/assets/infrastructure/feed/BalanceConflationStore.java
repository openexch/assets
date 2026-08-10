// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.feed;

import com.openexchange.assets.domain.Asset;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Latest-value store for the balance side-channel feed: one slot per (userId, assetId) holding the
 * most recent (available, locked) pair, written by the cluster service thread during apply and read
 * by the {@link BalanceFeedAgent} thread.
 *
 * <p><b>Semantics: conflation, not a log.</b> A slot only ever holds the latest pair; a reader that
 * falls behind misses INTERMEDIATE values, never the final truth. The authoritative balance record
 * remains the cluster itself (RequestBalanceSnapshot + acks) — this store exists so the live feed can
 * leave the deterministic thread without the cluster egress fanout multiplying it per session.</p>
 *
 * <p><b>Threading.</b> Exactly two threads by contract:</p>
 * <ul>
 *   <li><b>Writer</b> — the cluster service thread, the only caller of {@link #onBalanceUpdate},
 *       {@link #onRoleChange} and the only mutator of the key map. Zero allocation steady-state:
 *       allocation happens only when a (userId, assetId) is first seen, the same discipline as the
 *       engine's account map ({@code Long2ObjectHashMap} of per-user dense per-asset arrays,
 *       mirroring {@code Account}'s {@code long[2 * Asset.count()]}).</li>
 *   <li><b>Reader</b> — the feed agent thread, the only caller of {@link #pollDirty},
 *       {@link #consumeSweepRequest} and {@link BalanceSlot#readInto}. It never touches the key map
 *       (the map resizes under the writer); full-store iteration goes through the flat
 *       {@link #slotAt}/{@link #liveSlots} view, which is append-only and release-published.</li>
 * </ul>
 *
 * <p><b>Torn pairs are impossible</b>: each slot is a seqlock (writer bumps the sequence to odd,
 * writes the pair, bumps to even; the reader retries on an odd or changed sequence — the same
 * discipline as the matching engine's top-of-book reads).</p>
 *
 * <p><b>Dirty tracking.</b> An SPSC queue of slots with a per-slot {@code scheduled} flag: a hot key
 * conflates in place instead of flooding the queue (it is enqueued at most once until the agent takes
 * it). Both flag operations are atomic read-modify-writes on purpose — the full-fence semantics are
 * what guarantee the writer's pair is visible to the agent's subsequent seqlock read, or failing
 * that, that the writer re-enqueues; either way the FINAL value is always delivered. If the queue is
 * ever full (more distinct dirty keys than {@link #DIRTY_QUEUE_CAPACITY} between agent drains) the
 * writer clears the flag and requests a full sweep instead: coarser, never lossy.</p>
 *
 * <p><b>Leadership.</b> Maintained on EVERY node — followers apply the same deterministic events, so
 * their store is always current. The agent publishes only while this node leads; on GAINING
 * leadership {@link #onRoleChange} requests a full-store sweep, which is a natural feed snapshot for
 * consumers across a failover. None of this is replicated or snapshotted state: it is derived, on
 * each node, from the replicated event stream.</p>
 */
public final class BalanceConflationStore {

    /**
     * Bounded dirty queue: covers this many DISTINCT concurrently-dirty keys between agent drains
     * (a hot key occupies one entry however often it changes). Overflow degrades to a full sweep,
     * so the bound is a performance knob, never a loss.
     */
    static final int DIRTY_QUEUE_CAPACITY = 64 * 1024;

    private static final int INITIAL_FLAT_CAPACITY = 1024;
    private static final int ASSETS = Asset.count();

    /**
     * One (userId, assetId) line: the latest (available, locked) pair under a seqlock, plus the
     * dirty-queue {@code scheduled} flag. Allocated once, on first sight of the key; identity is
     * stable for the life of the store, which is what lets both the queue and the flat view hand
     * out references instead of copies.
     */
    public static final class BalanceSlot {

        private static final VarHandle SEQ;
        private static final VarHandle SCHEDULED;
        private static final VarHandle AVAILABLE;
        private static final VarHandle LOCKED;

        static {
            try {
                final MethodHandles.Lookup l = MethodHandles.lookup();
                SEQ = l.findVarHandle(BalanceSlot.class, "seq", int.class);
                SCHEDULED = l.findVarHandle(BalanceSlot.class, "scheduled", boolean.class);
                AVAILABLE = l.findVarHandle(BalanceSlot.class, "available", long.class);
                LOCKED = l.findVarHandle(BalanceSlot.class, "locked", long.class);
            } catch (final ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final long userId;
        private final int assetId;

        @SuppressWarnings("unused") private int seq;
        @SuppressWarnings("unused") private boolean scheduled;
        @SuppressWarnings("unused") private long available;
        @SuppressWarnings("unused") private long locked;

        BalanceSlot(final long userId, final int assetId) {
            this.userId = userId;
            this.assetId = assetId;
        }

        public long userId() {
            return userId;
        }

        public int assetId() {
            return assetId;
        }

        /** Seqlock write. Writer thread only. */
        void write(final long newAvailable, final long newLocked) {
            final int s = (int) SEQ.getOpaque(this);
            SEQ.setOpaque(this, s + 1);              // odd: write in progress
            VarHandle.storeStoreFence();             // pair stores may not float above the odd seq
            AVAILABLE.setOpaque(this, newAvailable);
            LOCKED.setOpaque(this, newLocked);
            SEQ.setRelease(this, s + 2);             // even: pair stores may not float below
        }

        /**
         * Seqlock read into {@code pair[0]=available, pair[1]=locked}. Retries until it observes a
         * consistent pair — the two values are NEVER torn against each other. Safe from any thread.
         */
        public void readInto(final long[] pair) {
            while (true) {
                final int begin = (int) SEQ.getAcquire(this);
                if ((begin & 1) == 0) {
                    final long a = (long) AVAILABLE.getOpaque(this);
                    final long l = (long) LOCKED.getOpaque(this);
                    VarHandle.loadLoadFence();       // pair loads may not float below the re-check
                    if ((int) SEQ.getOpaque(this) == begin) {
                        pair[0] = a;
                        pair[1] = l;
                        return;
                    }
                }
                Thread.onSpinWait();                 // writer mid-flight: a store, not a wait
            }
        }

        /**
         * Writer side of the dirty protocol: claim the right to enqueue. A CAS even on the hot
         * (already-scheduled) path, deliberately: the failed CAS is still a full fence, which
         * guarantees the pair written just before it is globally visible before the agent — who
         * cleared the flag with an equally fenced {@link #acknowledge} — can seqlock-read the slot.
         * A plain read of a stale {@code true} here could strand a final value unpublished.
         */
        boolean trySchedule() {
            return SCHEDULED.compareAndSet(this, false, true);
        }

        /** Agent side: clear the flag BEFORE reading, so a concurrent write re-enqueues the slot. */
        void acknowledge() {
            SCHEDULED.getAndSet(this, false);
        }

        /** Queue-overflow path only (writer thread): un-claim so future writes can re-schedule. */
        void unschedule() {
            SCHEDULED.getAndSet(this, false);
        }
    }

    // ---- writer-side key map: userId -> dense per-asset slot array (mirrors Account.bal) ----
    private final Long2ObjectHashMap<BalanceSlot[]> slotsByUser = new Long2ObjectHashMap<>();

    // ---- shared flat view for full-store sweeps: append-only, grow-by-copy, release-published ----
    private static final VarHandle FLAT;
    private static final VarHandle FLAT_COUNT;

    static {
        try {
            final MethodHandles.Lookup l = MethodHandles.lookup();
            FLAT = l.findVarHandle(BalanceConflationStore.class, "flat", BalanceSlot[].class);
            FLAT_COUNT = l.findVarHandle(BalanceConflationStore.class, "flatCount", int.class);
        } catch (final ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private BalanceSlot[] flat = new BalanceSlot[INITIAL_FLAT_CAPACITY];
    @SuppressWarnings("unused") private int flatCount;

    private final OneToOneConcurrentArrayQueue<BalanceSlot> dirty =
            new OneToOneConcurrentArrayQueue<>(DIRTY_QUEUE_CAPACITY);

    private volatile boolean sweepRequested;
    private volatile boolean leader;
    /** Writer-thread-only shadow of {@link #leader}, to detect the gain edge. */
    private boolean wasLeader;

    // ---- writer API (cluster service thread) ----

    /**
     * Record the latest pair for (userId, assetId) and schedule it for the agent. Allocation only on
     * a first-seen key; a repeat write to a still-scheduled key conflates in place (no queue entry).
     */
    public void onBalanceUpdate(final long userId, final int assetId, final long available, final long locked) {
        BalanceSlot[] userSlots = slotsByUser.get(userId);
        if (userSlots == null) {
            userSlots = new BalanceSlot[ASSETS];
            slotsByUser.put(userId, userSlots);
        }
        BalanceSlot slot = userSlots[assetId];
        if (slot == null) {
            slot = new BalanceSlot(userId, assetId);
            userSlots[assetId] = slot;
            appendToFlat(slot);
        }
        slot.write(available, locked);
        if (slot.trySchedule() && !dirty.offer(slot)) {
            // Full queue: more distinct dirty keys than the queue holds. Un-claim (so later writes
            // can re-schedule once there is room) and fall back to a full sweep — coarser, never lossy.
            slot.unschedule();
            sweepRequested = true;
        }
    }

    /**
     * Leadership transition, from the service's {@code onRoleChange}. On a GAIN, request a full-store
     * sweep: the agent publishes every key once — a natural feed snapshot for consumers across the
     * failover. On a loss the agent simply stops publishing (it checks {@link #isLeader} per batch).
     */
    public void onRoleChange(final boolean isLeader) {
        this.leader = isLeader;
        if (isLeader && !wasLeader) {
            sweepRequested = true;
        }
        wasLeader = isLeader;
    }

    private void appendToFlat(final BalanceSlot slot) {
        BalanceSlot[] array = (BalanceSlot[]) FLAT.getOpaque(this);
        final int count = (int) FLAT_COUNT.getOpaque(this);
        if (count == array.length) {
            final BalanceSlot[] grown = new BalanceSlot[array.length * 2];
            System.arraycopy(array, 0, grown, 0, count);
            array = grown;
            FLAT.setRelease(this, grown); // published before the count that licenses reading it
        }
        array[count] = slot;
        FLAT_COUNT.setRelease(this, count + 1);
    }

    // ---- reader API (feed agent thread; readInto is safe from any thread) ----

    /** Whether this node currently leads — the only state in which the agent publishes. */
    public boolean isLeader() {
        return leader;
    }

    /**
     * Take the next dirty slot, clearing its scheduled flag (so a concurrent write re-enqueues it),
     * or {@code null} when nothing is pending. The caller then {@link BalanceSlot#readInto}s the
     * LATEST pair — which may already be newer than the write that scheduled the slot: conflation.
     */
    public BalanceSlot pollDirty() {
        final BalanceSlot slot = dirty.poll();
        if (slot != null) {
            slot.acknowledge();
        }
        return slot;
    }

    /** Consume a pending full-sweep request (leadership gain or dirty-queue overflow). One-shot. */
    public boolean consumeSweepRequest() {
        if (sweepRequested) {
            sweepRequested = false;
            return true;
        }
        return false;
    }

    /** Number of slots visible in the flat view. Read this BEFORE {@link #slotAt} per pass. */
    public int liveSlots() {
        return (int) FLAT_COUNT.getAcquire(this);
    }

    /**
     * Slot at flat index {@code i < }{@link #liveSlots()}. Append-only: an index never remaps to a
     * different key, so a sweep that iterates {@code [0, liveSlots())} visits every key exactly once.
     */
    public BalanceSlot slotAt(final int i) {
        final BalanceSlot[] array = (BalanceSlot[]) FLAT.getAcquire(this);
        return array[i];
    }
}

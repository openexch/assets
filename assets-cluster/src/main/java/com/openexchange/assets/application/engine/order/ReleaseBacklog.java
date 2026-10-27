// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * RC2 S8 1.2: the release backlog, at most one entry per core orderId, from a release mint until a dominating
 * confirmation, its abandonment or its parking. Replicated (snapshot section 11, ascending orderId); the per-stage counts
 * are maintained gauges (1.6) the scrape thread reads as plain values. Service thread only; pooled entries.
 */
public final class ReleaseBacklog {

    public static final int SCOPE_UP_TO = 0;
    public static final int SCOPE_ORDER = 1;
    public static final int STAGE_RELEASE = 0;
    public static final int STAGE_ORPHAN_CANCEL = 1;
    public static final int STAGE_PARKED = 2;

    /** One backlog entry (section 11 record, 56 B). UP_TO entries carry 0 in every identity field. */
    public static final class Entry {
        public long orderId;
        /** The minted {@code idLowFloor}. */
        public long floor;
        /** dispatchSeq of the dispatch the index replays (the release, or the orphan cancel at ORPHAN_CANCEL). */
        public long dispatchSeq;
        public long userId;
        /** The last known leg (currentLegId, else oldLegId) frozen at the terminal; U5 may raise it at stage RELEASE. */
        public long legId;
        public int marketId;
        /** The last attempt used for the order (C4 raises it by one for the orphan cancel). */
        public int attempt;
        public int scope;
        public int stage;
        public int side;
        public int meOrderType;
        public int orphanTries;

        void clear() {
            orderId = 0; floor = 0; dispatchSeq = 0; userId = 0; legId = 0; marketId = 0; attempt = 0; scope = 0; stage = 0;
            side = 0; meOrderType = 0; orphanTries = 0;
        }

        @Override
        public String toString() {
            return "Entry{order=" + orderId + " floor=" + floor + " seq=" + dispatchSeq + " scope=" + scope + " stage=" + stage
                    + " leg=" + legId + " attempt=" + attempt + " tries=" + orphanTries + "}";
        }
    }

    private final Long2ObjectHashMap<Entry> byId = new Long2ObjectHashMap<>();
    private final ArrayDeque<Entry> pool = new ArrayDeque<>();
    private final int[] byStage = new int[3];
    private long[] scratch = new long[64];

    /** The entry for {@code orderId}, or null. */
    public Entry get(final long orderId) {
        return byId.get(orderId);
    }

    public int size() {
        return byId.size();
    }

    /** Entries at {@code stage} (maintained, plain int). */
    public int count(final int stage) {
        return byStage[stage];
    }

    /** A zeroed entry for {@code orderId}, inserted at stage RELEASE. Must not exist yet. */
    public Entry insert(final long orderId) {
        if (byId.containsKey(orderId)) {
            throw new IllegalStateException("release backlog already holds order " + orderId);
        }
        final Entry e = pool.pollFirst();
        final Entry entry = e == null ? new Entry() : e;
        entry.clear();
        entry.orderId = orderId;
        entry.stage = STAGE_RELEASE;
        byId.put(orderId, entry);
        byStage[STAGE_RELEASE]++;
        return entry;
    }

    /** Restore one entry exactly as read (section 11). */
    public Entry restore(final Entry from) {
        if (byId.containsKey(from.orderId)) {
            throw new IllegalStateException("release backlog section 11 repeats order " + from.orderId);
        }
        if (from.stage < STAGE_RELEASE || from.stage > STAGE_PARKED) {
            throw new IllegalStateException("release backlog section 11 entry " + from.orderId + " stage " + from.stage);
        }
        final Entry e = pool.pollFirst();
        final Entry entry = e == null ? new Entry() : e;
        entry.orderId = from.orderId; entry.floor = from.floor; entry.dispatchSeq = from.dispatchSeq; entry.userId = from.userId;
        entry.legId = from.legId; entry.marketId = from.marketId; entry.attempt = from.attempt; entry.scope = from.scope;
        entry.stage = from.stage; entry.side = from.side; entry.meOrderType = from.meOrderType; entry.orphanTries = from.orphanTries;
        byId.put(entry.orderId, entry);
        byStage[entry.stage]++;
        return entry;
    }

    /** Move {@code e} to {@code stage}, keeping the per-stage counts. */
    public void setStage(final Entry e, final int stage) {
        byStage[e.stage]--;
        e.stage = stage;
        byStage[stage]++;
    }

    /** Remove and recycle the entry of {@code orderId}; returns false when there is none. */
    public boolean remove(final long orderId) {
        final Entry e = byId.remove(orderId);
        if (e == null) {
            return false;
        }
        byStage[e.stage]--;
        e.clear();
        pool.offerFirst(e);
        return true;
    }

    /** Visit entries in ascending orderId (canonical, section 11). Not reentrant. */
    public void forEachByOrderId(final Consumer<Entry> visitor) {
        final int n = byId.size();
        if (scratch.length < n) {
            scratch = new long[Math.max(n, scratch.length * 2)];
        }
        int i = 0;
        for (final Entry e : byId.values()) {
            scratch[i++] = e.orderId;
        }
        Arrays.sort(scratch, 0, n);
        for (int k = 0; k < n; k++) {
            visitor.accept(byId.get(scratch[k]));
        }
    }

    /** Drop everything (restore into a fresh engine only). */
    public void clear() {
        for (final Entry e : byId.values()) {
            e.clear();
            pool.offerFirst(e);
        }
        byId.clear();
        Arrays.fill(byStage, 0);
    }
}

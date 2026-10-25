// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The replicated requestId table (lifecycle B6, critic D5): (userId, requestId) → the decision the core
 * took, kept for a TTL on the single deadline index and capped per user. A replay with an equal payload
 * fingerprint returns the stored decision; a different fingerprint is REQUEST_ID_PAYLOAD_MISMATCH. Layout
 * section 6, ascending (userId, high, low).
 */
public final class RequestTable {

    public static final long TTL_MS = 15 * 60_000L;
    public static final int PER_USER_CAP = 256;
    public static final int ENTRY_BYTES = 8 * 3 + 8 + 8 + 8 + 8 + 1 + 1;

    /** Kind of command the decision belongs to (byte). */
    public static final int KIND_CREATE = 1;
    public static final int KIND_CANCEL = 2;
    public static final int KIND_AMEND = 3;
    public static final int KIND_DEPOSIT = 4;
    public static final int KIND_WITHDRAW = 5;

    public static final class Entry {
        public long userId;
        public long high;
        public long low;
        public long requestSeq;    // the deadline-index id
        public long fingerprint;
        public long expiresAtMs;
        public long orderId;       // decision payload (order id, or the money amount for deposits)
        public byte kind;
        public byte reason;        // CoreRejectReason / RejectReason code; 0 = accepted
    }

    private record Key(long userId, long high, long low) { }

    private final Map<Key, Entry> entries = new HashMap<>();
    private final Map<Long, Entry> bySeq = new HashMap<>();
    private final Map<Long, Integer> perUser = new HashMap<>();
    private long nextRequestSeq = 1L;
    private final ArrayList<Entry> scratch = new ArrayList<>();

    public Entry get(final long userId, final long high, final long low) {
        return entries.get(new Key(userId, high, low));
    }

    public int countFor(final long userId) {
        return perUser.getOrDefault(userId, 0);
    }

    public int size() {
        return entries.size();
    }

    public long nextRequestSeq() {
        return nextRequestSeq;
    }

    /** Restore only. */
    public void setNextRequestSeq(final long v) {
        nextRequestSeq = v;
    }

    /** Record a decision; returns the entry (its requestSeq is the deadline-index id for the TTL). */
    public Entry put(final long userId, final long high, final long low, final long fingerprint, final long expiresAtMs,
                     final int kind, final long orderId, final int reason) {
        final Entry e = new Entry();
        e.userId = userId; e.high = high; e.low = low; e.requestSeq = nextRequestSeq++;
        e.fingerprint = fingerprint; e.expiresAtMs = expiresAtMs; e.kind = (byte) kind; e.orderId = orderId; e.reason = (byte) reason;
        insert(e);
        return e;
    }

    /** Restore one row (snapshot section 6). */
    public void restore(final Entry e) {
        insert(e);
    }

    private void insert(final Entry e) {
        entries.put(new Key(e.userId, e.high, e.low), e);
        bySeq.put(e.requestSeq, e);
        perUser.merge(e.userId, 1, Integer::sum);
    }

    /** The TTL fired for {@code requestSeq}: drop the entry. Returns the removed entry or null. */
    public Entry expire(final long requestSeq) {
        final Entry e = bySeq.remove(requestSeq);
        if (e == null) {
            return null;
        }
        entries.remove(new Key(e.userId, e.high, e.low));
        perUser.merge(e.userId, -1, Integer::sum);
        if (perUser.get(e.userId) <= 0) {
            perUser.remove(e.userId);
        }
        return e;
    }

    /** Visit in ascending (userId, high, low). Not reentrant. */
    public void forEachOrdered(final Consumer<Entry> visitor) {
        scratch.clear();
        scratch.addAll(entries.values());
        scratch.sort(Comparator.<Entry>comparingLong(e -> e.userId).thenComparingLong(e -> e.high).thenComparingLong(e -> e.low));
        for (int i = 0; i < scratch.size(); i++) {
            visitor.accept(scratch.get(i));
        }
        scratch.clear();
    }

    /** Every entry (unordered) for index rebuilds. */
    public List<Entry> all() {
        return new ArrayList<>(entries.values());
    }
}

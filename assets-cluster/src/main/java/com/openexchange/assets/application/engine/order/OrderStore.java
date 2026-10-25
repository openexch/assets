// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The live order records and their DERIVED indexes: per-user open set (the one slot definition, lifecycle
 * B9#4) and the (userId, clientOrderId) index of ACTIVE orders. Only the records are serialized (layout
 * section 2, ascending orderId); every index is rebuilt from them on restore, so the bytes are a function
 * of state. Service-thread only; pooled records; sorted scratch for canonical traversal.
 */
public final class OrderStore {

    /** Composite key for the active clientOrderId index (allocated per lookup; off the µs-critical path for now). */
    private record ClientKey(long userId, long high, long low) { }

    private final Long2ObjectHashMap<CoreOrder> byId = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<LongHashSet> openByUser = new Long2ObjectHashMap<>();
    private final Map<ClientKey, Long> byClientId = new HashMap<>();
    private final ArrayDeque<CoreOrder> pool = new ArrayDeque<>();
    private long[] scratch = new long[256];

    public CoreOrder acquire() {
        final CoreOrder o = pool.pollFirst();
        return o == null ? new CoreOrder() : o;
    }

    /** Insert a live record and index it. */
    public void add(final CoreOrder o) {
        byId.put(o.orderId, o);
        openByUser.computeIfAbsent(o.userId, k -> new LongHashSet()).add(o.orderId);
        if (o.hasClientOrderId()) {
            byClientId.put(new ClientKey(o.userId, o.clientOrderIdHigh, o.clientOrderIdLow), o.orderId);
        }
    }

    /** Remove a terminal record from every index and recycle it. */
    public void remove(final CoreOrder o) {
        byId.remove(o.orderId);
        final LongHashSet open = openByUser.get(o.userId);
        if (open != null) {
            open.remove(o.orderId);
            if (open.isEmpty()) {
                openByUser.remove(o.userId);
            }
        }
        if (o.hasClientOrderId()) {
            byClientId.remove(new ClientKey(o.userId, o.clientOrderIdHigh, o.clientOrderIdLow));
        }
        o.clear();
        pool.offerFirst(o);
    }

    public CoreOrder get(final long orderId) {
        return byId.get(orderId);
    }

    public int size() {
        return byId.size();
    }

    /** Number of live (non-terminal) orders of a user: the slot count. */
    public int openCount(final long userId) {
        final LongHashSet open = openByUser.get(userId);
        return open == null ? 0 : open.size();
    }

    /** The live order with this (userId, clientOrderId), or 0. */
    public long activeByClientId(final long userId, final long high, final long low) {
        final Long id = byClientId.get(new ClientKey(userId, high, low));
        return id == null ? 0L : id;
    }

    /** Visit live orders in ascending orderId (canonical). Not reentrant. */
    public void forEachByOrderId(final Consumer<CoreOrder> visitor) {
        int n = 0;
        if (scratch.length < byId.size()) {
            scratch = new long[Math.max(byId.size(), scratch.length * 2)];
        }
        for (final CoreOrder o : byId.values()) {
            scratch[n++] = o.orderId;
        }
        Arrays.sort(scratch, 0, n);
        for (int i = 0; i < n; i++) {
            visitor.accept(byId.get(scratch[i]));
        }
    }

    /** Drop everything (restore into a fresh engine only). */
    public void clear() {
        byId.clear();
        openByUser.clear();
        byClientId.clear();
    }
}

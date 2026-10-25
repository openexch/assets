// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.proto;

import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;

/**
 * RC0 spike: per-user replicated risk state. Rate limits are FIXED one-second and one-minute buckets
 * keyed on cluster time (not the OMS's sliding rings): four longs that snapshot exactly, so a member
 * restored from a snapshot and a member that replayed the log make the same admission decision on
 * the next order. The two indexes are rebuilt from the orders on restore and never serialized.
 */
public final class ProtoUserState {
    /** Serialized size: userId, secBucket, minBucket, openNotional, 6 positions = 10 longs; openCount, secCount, minCount = 3 ints. */
    public static final int SNAPSHOT_BYTES = 10 * 8 + 3 * 4;
    public static final int MARKET_SLOTS = 6; // marketId 1..5 today; slot 0 unused

    public long userId;
    public int openOrderCount;
    public long secBucket = -1;
    public int secCount;
    public long minBucket = -1;
    public int minCount;
    public long openNotional;
    public final long[] position = new long[MARKET_SLOTS];

    public final LongHashSet openOrders = new LongHashSet();
    public final Long2LongHashMap clientOrderIds = new Long2LongHashMap(-1L);

    ProtoUserState(final long userId) {
        this.userId = userId;
    }
}

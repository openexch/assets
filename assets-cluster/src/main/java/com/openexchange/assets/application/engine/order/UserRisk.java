// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

/**
 * Per-user replicated risk state (risk spec B2; layout section 3, 80 B): fixed-window rate counters keyed
 * on log time and the net filled position per market. Open-order and clientOrderId indexes are derived
 * (see {@link OrderStore}).
 */
public final class UserRisk {

    public static final int MARKET_SLOTS = 6; // marketId 1..5, slot 0 unused
    public static final int SNAPSHOT_BYTES = 8 + 8 + 4 + 8 + 4 + MARKET_SLOTS * 8;

    public final long userId;
    public long secBucket;
    public int secCount;
    public long minBucket;
    public int minCount;
    public final long[] position = new long[MARKET_SLOTS];

    public UserRisk(final long userId) {
        this.userId = userId;
    }

    /** Roll the windows to the buckets of {@code logTime}; counts reset when their bucket changes. */
    public void roll(final long logTime) {
        final long sec = logTime / 1_000L;
        if (sec != secBucket) {
            secBucket = sec;
            secCount = 0;
        }
        final long min = logTime / 60_000L;
        if (min != minBucket) {
            minBucket = min;
            minCount = 0;
        }
    }
}

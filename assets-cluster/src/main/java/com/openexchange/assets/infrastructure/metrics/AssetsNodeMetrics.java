// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.metrics;

import java.math.BigDecimal;

/**
 * Hot-path-safe metrics state for an Assets Engine cluster node: the per-message cost of the
 * service thread, split into the deterministic apply ({@code demuxer.dispatch}) and the egress
 * drain that follows it, plus the last snapshot's size and timings.
 *
 * <p>Same shape as the matching engine's {@code NodeMetrics} (match#33): every record/stamp method
 * is called ONLY on the cluster service thread and writes plain fields (no volatiles, no
 * allocation, no locks on the per-command path). Cross-thread visibility for the /metrics scraper is
 * the piggyback pattern: the service thread bumps the volatile {@link #publish()} sequence (every
 * {@value #PUBLISH_EVERY} messages and from the drain timer), and the scraper reads
 * {@link #acquire()} FIRST, which makes every plain write that happened before the corresponding
 * publish visible.</p>
 *
 * <p>Buckets are a 1-2-5 ladder from 250 ns to 10 s, so the RC0 thresholds (5 us, 50 us, 1 ms)
 * fall on exact bucket boundaries instead of inside a power-of-two bin. The exact maximum is kept
 * as well: a histogram bin says "somewhere under 2 ms", the leader's worst cycle wants a number.</p>
 *
 * <p>Sampling: {@code ASSETS_APPLY_SAMPLE_SHIFT} (env) or {@code assets.apply.sample.shift}
 * (property) records 1 in 2^shift messages; the default 0 records every message. The ledger's
 * per-command work is microseconds, two {@code System.nanoTime()} calls are tens of nanoseconds,
 * so the default is exact and the knob exists for the day it is not. The effective value is
 * exported as {@code assets_apply_sample_shift}.</p>
 */
public final class AssetsNodeMetrics {

    /** Upper bounds in nanoseconds; the last bucket is +Inf. */
    static final long[] BUCKET_UPPER_NS = {
            250L, 500L, 1_000L, 2_000L, 5_000L, 10_000L, 20_000L, 50_000L, 100_000L, 200_000L, 500_000L,
            1_000_000L, 2_000_000L, 5_000_000L, 10_000_000L, 20_000_000L, 50_000_000L, 100_000_000L,
            200_000_000L, 500_000_000L, 1_000_000_000L, 2_000_000_000L, 5_000_000_000L, 10_000_000_000L,
    };
    static final int BUCKETS = BUCKET_UPPER_NS.length + 1;
    /** Prometheus {@code le} labels, plain decimal seconds (no exponent notation). */
    static final String[] BUCKET_LE = new String[BUCKET_UPPER_NS.length];
    static {
        for (int i = 0; i < BUCKET_UPPER_NS.length; i++) {
            BUCKET_LE[i] = BigDecimal.valueOf(BUCKET_UPPER_NS[i]).movePointLeft(9)
                    .stripTrailingZeros().toPlainString();
        }
    }
    /** money-schema templateIds are small (1..33 today); anything larger shares the last slot. */
    public static final int TEMPLATE_SLOTS = 64;
    static final int PUBLISH_EVERY = 64;

    /** One single-writer latency distribution: cumulative-free buckets, sum, count, exact max. */
    public static final class Histogram {
        private final long[] buckets = new long[BUCKETS];
        private long sumNanos;
        private long count;
        private long maxNanos;

        void record(final long nanos) {
            int i = 0;
            while (i < BUCKET_UPPER_NS.length && nanos > BUCKET_UPPER_NS[i]) {
                i++;
            }
            buckets[i]++;
            sumNanos += nanos;
            count++;
            if (nanos > maxNanos) {
                maxNanos = nanos;
            }
        }

        public long bucketCount(final int idx) {
            return buckets[idx];
        }

        public long sumNanos() {
            return sumNanos;
        }

        public long count() {
            return count;
        }

        public long maxNanos() {
            return maxNanos;
        }
    }

    private final int sampleShift;
    private final int sampleMask;
    private int sampleTick;
    private long messagesSeen;

    // Written only on the service thread.
    private final Histogram dispatch = new Histogram();
    private final Histogram drain = new Histogram();
    private final Histogram timerDrain = new Histogram();
    private final long[] templateCount = new long[TEMPLATE_SLOTS];
    private final long[] templateSumNanos = new long[TEMPLATE_SLOTS];
    private final long[] templateMaxNanos = new long[TEMPLATE_SLOTS];
    private long lastLogPosition = -1;
    private long lastSnapshotMs;
    private long lastSnapshotBytes = -1;
    private long lastSnapshotChunks = -1;
    private long lastSnapshotSerializeNanos = -1;
    private long lastSnapshotOfferNanos = -1;
    private long lastSnapshotRecordNanos = -1;
    private long lastSnapshotLogPosition = -1;

    // Rare writes (role change / startup): volatile is fine here.
    private volatile int role = -1;
    private volatile int memberId = -1;

    private volatile long published;

    public AssetsNodeMetrics() {
        this(sampleShiftFromEnv());
    }

    AssetsNodeMetrics(final int sampleShift) {
        if (sampleShift < 0 || sampleShift > 16) {
            throw new IllegalArgumentException("sample shift must be 0..16, got " + sampleShift);
        }
        this.sampleShift = sampleShift;
        this.sampleMask = (1 << sampleShift) - 1;
    }

    private static int sampleShiftFromEnv() {
        final String env = System.getenv("ASSETS_APPLY_SAMPLE_SHIFT");
        final String raw = env != null && !env.isEmpty() ? env : System.getProperty("assets.apply.sample.shift", "0");
        try {
            return Integer.parseInt(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("ASSETS_APPLY_SAMPLE_SHIFT / assets.apply.sample.shift must be an "
                    + "integer 0..16, got '" + raw + "'", e);
        }
    }

    // ---- service-thread writes ----

    /** Whether the next message should be timed (call before the first nanoTime). */
    public boolean shouldSample() {
        return (sampleTick++ & sampleMask) == 0;
    }

    /**
     * One ingress message: apply cost by templateId and the drain that followed. Bumps the publish
     * sequence every {@value #PUBLISH_EVERY} messages so follower nodes (which never run the drain
     * timer) still become visible to the scraper.
     */
    public void recordMessage(final int templateId, final long dispatchNanos, final long drainNanos,
                              final long logPosition) {
        dispatch.record(dispatchNanos);
        drain.record(drainNanos);
        final int slot = templateId >= 0 && templateId < TEMPLATE_SLOTS ? templateId : TEMPLATE_SLOTS - 1;
        templateCount[slot]++;
        templateSumNanos[slot] += dispatchNanos;
        if (dispatchNanos > templateMaxNanos[slot]) {
            templateMaxNanos[slot] = dispatchNanos;
        }
        lastLogPosition = logPosition;
        if ((++messagesSeen & (PUBLISH_EVERY - 1)) == 0) {
            publish();
        }
    }

    /** The idle-path drain belt fired: how long its drain took. Publishes (leader cadence, 10 ms). */
    public void recordTimerDrain(final long drainNanos) {
        timerDrain.record(drainNanos);
        publish();
    }

    public void stampSnapshot(final long epochMs, final long bytes, final long chunks, final long serializeNanos,
                              final long offerNanos, final long recordNanos, final long logPosition) {
        lastSnapshotMs = epochMs;
        lastSnapshotBytes = bytes;
        lastSnapshotChunks = chunks;
        lastSnapshotSerializeNanos = serializeNanos;
        lastSnapshotOfferNanos = offerNanos;
        lastSnapshotRecordNanos = recordNanos;
        lastSnapshotLogPosition = logPosition;
        publish();
    }

    public void setRole(final int roleCode) {
        this.role = roleCode;
    }

    public void setMemberId(final int memberId) {
        this.memberId = memberId;
    }

    /** Service thread: publish plain writes to scraper threads. */
    public void publish() {
        published++;
    }

    // ---- scraper-side reads (after acquire()) ----

    /** Scraper thread: MUST be called before reading any getter below. */
    public long acquire() {
        return published;
    }

    public int sampleShift() {
        return sampleShift;
    }

    public Histogram dispatch() {
        return dispatch;
    }

    public Histogram drain() {
        return drain;
    }

    public Histogram timerDrain() {
        return timerDrain;
    }

    public long templateCount(final int slot) {
        return templateCount[slot];
    }

    public long templateSumNanos(final int slot) {
        return templateSumNanos[slot];
    }

    public long templateMaxNanos(final int slot) {
        return templateMaxNanos[slot];
    }

    public long lastLogPosition() {
        return lastLogPosition;
    }

    public long lastSnapshotMs() {
        return lastSnapshotMs;
    }

    public long lastSnapshotBytes() {
        return lastSnapshotBytes;
    }

    public long lastSnapshotChunks() {
        return lastSnapshotChunks;
    }

    public long lastSnapshotSerializeNanos() {
        return lastSnapshotSerializeNanos;
    }

    public long lastSnapshotOfferNanos() {
        return lastSnapshotOfferNanos;
    }

    public long lastSnapshotRecordNanos() {
        return lastSnapshotRecordNanos;
    }

    public long lastSnapshotLogPosition() {
        return lastSnapshotLogPosition;
    }

    public int role() {
        return role;
    }

    public int memberId() {
        return memberId;
    }
}

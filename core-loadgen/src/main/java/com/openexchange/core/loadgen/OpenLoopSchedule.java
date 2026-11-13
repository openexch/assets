// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

/**
 * A fixed-rate open-loop send timeline: slot {@code k} is due at {@code start + k / rate}, computed from {@code k} (no
 * accumulated rounding). The schedule is never reset when the sender falls behind: a slot sent late keeps its
 * scheduled time as its t0, so delay caused by a slow system lands in the tail percentiles instead of being hidden
 * (coordinated omission). Single-threaded; the caller owns the clock.
 */
public final class OpenLoopSchedule {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final long rate;
    /** Negative-control seam only: re-anchor the timeline at the late send time (hides a stall). */
    private final boolean resetOnLag;
    private long baseNs;
    private long baseSlot;
    private long next;
    private long maxBacklog;

    /**
     * @param startNs the scheduled time of slot 0
     * @param ratePerSecond slots per second, at least 1
     */
    public OpenLoopSchedule(final long startNs, final long ratePerSecond) {
        this(startNs, ratePerSecond, false);
    }

    OpenLoopSchedule(final long startNs, final long ratePerSecond, final boolean resetOnLag) {
        if (ratePerSecond < 1) {
            throw new IllegalArgumentException("rate must be at least 1 slot/s: " + ratePerSecond);
        }
        this.rate = ratePerSecond;
        this.resetOnLag = resetOnLag;
        this.baseNs = startNs;
        this.baseSlot = 0;
    }

    /** The scheduled send time of slot {@code k}. */
    public long t0(final long k) {
        return baseNs + (k - baseSlot) * NANOS_PER_SECOND / rate;
    }

    /** The next slot not yet taken. */
    public long nextSlot() {
        return next;
    }

    /** True when the next slot's scheduled time has come. */
    public boolean due(final long nowNs) {
        return nowNs >= t0(next);
    }

    /** Slots owed at {@code nowNs} beyond the next one (0 when on time). */
    public long backlog(final long nowNs) {
        final long late = nowNs - t0(next);
        return late <= 0 ? 0 : late * rate / NANOS_PER_SECOND;
    }

    /** The largest backlog seen by {@link #take}. */
    public long maxBacklog() {
        return maxBacklog;
    }

    /**
     * Takes the next slot at {@code nowNs} (the actual send time) and returns its index; its t0 stays
     * {@link #t0(long)} of that index whatever {@code nowNs} is.
     */
    public long take(final long nowNs) {
        final long backlog = backlog(nowNs);
        if (backlog > maxBacklog) {
            maxBacklog = backlog;
        }
        final long k = next++;
        if (resetOnLag && nowNs > t0(k)) {
            baseNs = nowNs;
            baseSlot = k;
        }
        return k;
    }
}

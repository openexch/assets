// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

/**
 * Deterministic, injectable timestamp source for scenarios. The value handed to
 * {@code AssetsEngine.applyCommand} is fully controlled here — never wall-clock — so replays see an
 * identical timestamp sequence. The scenario {@code CLOCK <n>} verb sets the absolute value.
 */
public final class LogicalClock {

    public static final long DEFAULT_START = 1000L;

    private long current;

    public LogicalClock() {
        this(DEFAULT_START);
    }

    public LogicalClock(long start) {
        this.current = start;
    }

    public long now() {
        return current;
    }

    public void set(long value) {
        this.current = value;
    }
}

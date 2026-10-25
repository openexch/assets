// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.loadgen;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.io.PrintStream;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * One latency distribution with a single writer (the duty thread, wait-free {@code recordValue}) and
 * serialized readers (the interval logger and the final report). Same contract as the inner Track of
 * {@link MoneyLoadGenerator}, lifted out for the RC0 prototype driver.
 */
final class LatencyTrack {
    static final long MAX_NS = TimeUnit.MINUTES.toNanos(2);

    final String name;
    private final SingleWriterRecorder recorder = new SingleWriterRecorder(3);
    private final Histogram cumulative = new Histogram(MAX_NS, 3);
    private Histogram recycled;

    LatencyTrack(final String name) {
        this.name = name;
    }

    void record(final long ns) {
        recorder.recordValue(Math.max(0, Math.min(ns, MAX_NS - 1)));
    }

    private void flip() {
        recycled = recorder.getIntervalHistogram(recycled);
        cumulative.add(recycled);
    }

    synchronized Histogram cumulative() {
        flip();
        return cumulative;
    }

    synchronized void printIntervalLine(final PrintStream out, final String stampPrefix) {
        flip();
        out.printf(Locale.ROOT, "%strack=%s n=%d p50_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f%n",
                stampPrefix, name, recycled.getTotalCount(),
                recycled.getValueAtPercentile(50) / 1e3,
                recycled.getValueAtPercentile(99) / 1e3,
                recycled.getValueAtPercentile(99.9) / 1e3,
                recycled.getMaxValue() / 1e3);
    }
}

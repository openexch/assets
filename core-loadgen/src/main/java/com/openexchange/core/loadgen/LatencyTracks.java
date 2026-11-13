// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Named latency tracks with a warm-up window and per-second maxima. Each {@link Track} has one writer (the duty
 * thread, wait-free {@code recordValue}) and readers serialized by its synchronized flip methods; the cumulative
 * histogram keeps the {@link #newHist()} bounds. {@link #record} drops a value observed before the measurement window
 * opens and folds the rest into the track and into the per-second row of the observation time.
 */
public final class LatencyTracks {

    /** The largest recordable value (two minutes); larger values are clamped. */
    public static final long HIGHEST_NS = TimeUnit.MINUTES.toNanos(2);

    /** A histogram of nanoseconds up to {@link #HIGHEST_NS}, three significant digits. */
    public static Histogram newHist() {
        return new Histogram(HIGHEST_NS, 3);
    }

    /** {@code ns} clamped into {@code [0, HIGHEST_NS)}. */
    public static long clamp(final long ns) {
        return Math.max(0, Math.min(ns, HIGHEST_NS - 1));
    }

    /** One latency track. */
    public static final class Track {
        private final String name;
        private final SingleWriterRecorder recorder = new SingleWriterRecorder(3);
        private final Histogram cumulative = newHist();
        private Histogram recycled;

        public Track(final String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        /** Records one value (clamped); the duty thread only. */
        public void recordValue(final long ns) {
            recorder.recordValue(clamp(ns));
        }

        /** Fold the interval since the last flip into cumulative; recycled IS that interval. */
        private void flip() {
            recycled = recorder.getIntervalHistogram(recycled);
            cumulative.add(recycled);
        }

        /** Everything recorded so far. */
        public synchronized Histogram cumulative() {
            flip();
            return cumulative;
        }

        /** One line for the interval since the last flip: n, p50, p99, max in ms. */
        public synchronized void printIntervalLine(final PrintStream out, final String stampPrefix) {
            flip();
            out.printf(Locale.ROOT, "%strack=%s n=%d p50_ms=%.2f p99_ms=%.2f max_ms=%.2f%n",
                    stampPrefix, name, recycled.getTotalCount(),
                    recycled.getValueAtPercentile(50) / 1e6,
                    recycled.getValueAtPercentile(99) / 1e6,
                    recycled.getMaxValue() / 1e6);
        }
    }

    /** The maximum and the count of one track in one second of the run. */
    public record SecondRow(long second, String track, long count, long maxNs) {
    }

    private final long startNs;
    private final long measureStartNs;
    private final List<Track> tracks = new ArrayList<>();
    private final List<List<long[]>> perTrack = new ArrayList<>();

    /**
     * @param startNs the run's start (second 0)
     * @param measureStartNs the end of the warm-up: values observed earlier are not recorded
     */
    public LatencyTracks(final long startNs, final long measureStartNs) {
        this.startNs = startNs;
        this.measureStartNs = measureStartNs;
    }

    /** A new track of this set. */
    public Track track(final String name) {
        final Track t = new Track(name);
        tracks.add(t);
        perTrack.add(new ArrayList<>());
        return t;
    }

    public List<Track> tracks() {
        return List.copyOf(tracks);
    }

    /** True once {@code nowNs} is inside the measurement window. */
    public boolean measuring(final long nowNs) {
        return nowNs >= measureStartNs;
    }

    /**
     * Records {@code valueNs} observed at {@code nowNs} into {@code t} when the window is open; returns whether it was
     * recorded. The per-second row is the observation's second since the start.
     */
    public boolean record(final Track t, final long nowNs, final long valueNs) {
        if (!measuring(nowNs)) {
            return false;
        }
        final long v = clamp(valueNs);
        t.recordValue(v);
        final List<long[]> rows = perTrack.get(tracks.indexOf(t));
        final long second = (nowNs - startNs) / 1_000_000_000L;
        long[] row = rows.isEmpty() ? null : rows.get(rows.size() - 1);
        if (row == null || row[0] != second) {
            row = new long[] {second, 0, 0};
            rows.add(row);
        }
        row[1]++;
        if (v > row[2]) {
            row[2] = v;
        }
        return true;
    }

    /** Every per-second row, by track then second. */
    public List<SecondRow> secondRows() {
        final List<SecondRow> out = new ArrayList<>();
        for (int i = 0; i < tracks.size(); i++) {
            for (final long[] r : perTrack.get(i)) {
                out.add(new SecondRow(r[0], tracks.get(i).name(), r[1], r[2]));
            }
        }
        return out;
    }

    /** The per-second rows as TSV with a header: {@code second, track, n, max_us}. */
    public void writeSecondsTsv(final PrintStream out) {
        out.println("second\ttrack\tn\tmax_us");
        for (final SecondRow r : secondRows()) {
            out.printf(Locale.ROOT, "%d\t%s\t%d\t%.1f%n", r.second(), r.track(), r.count(), r.maxNs() / 1e3);
        }
    }
}

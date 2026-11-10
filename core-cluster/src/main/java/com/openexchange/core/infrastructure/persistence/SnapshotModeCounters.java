// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.log.FrameKind;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The member's {@code core_*} counters, plain longs written on the service thread (and, for the
 * journal tail on the housekeeping thread) and read by the node endpoint's exporter. The {@code shadow_*} floor,
 * retention and housekeeping counters are the kit's ({@code ShadowHousekeeping.Counters}, {@code ShadowCounters});
 * {@code CoreClusteredService.metricsBody()} exports all of them. Counting only: nothing here decides anything.
 */
public final class SnapshotModeCounters {

    /** Unlabelled counters and gauges. */
    public enum C {
        SNAPSHOT_STUB_TOTAL("core_snapshot_stub_total"),
        SNAPSHOT_FULL_TOTAL("core_snapshot_full_total"),
        /** Shadow mode wrote a FULL image because the term id was unknown (-1, the full-path hole N2; D3). */
        SNAPSHOT_FULL_TERM_UNKNOWN_TOTAL("core_snapshot_full_term_unknown_total"),
        SNAPSHOT_IN_SKIP_WINDOW_TOTAL("core_snapshot_in_skip_window_total"),
        AWAIT_RECORDED_SKIPPED_TOTAL("core_await_recorded_skipped_total"),
        /** 0 none, 1 FULL, 2 EQUAL, 3 SKIP, 4 TAIL, 5 VIRTUAL_GENESIS; set after the post-restore checks passed (C3). */
        RESTORE_PATH("core_restore_path"),
        RESTORE_CANDIDATES_SCANNED("core_restore_candidates_scanned"),
        SKIP_UNSAFE_TOTAL("core_skip_unsafe_total"),
        SKIP_WINDOW_CLOSED_TOTAL("core_skip_window_closed_total"),
        TAIL_FRAMES_TOTAL("core_tail_frames_total"),
        TAIL_EVENTS_DISCARDED_TOTAL("core_tail_events_discarded_total"),
        TAIL_MS("core_tail_ms"),
        VIRTUAL_GENESIS_TOTAL("core_virtual_genesis_total"),
        WITNESS_FAILED_TOTAL("core_witness_failed_total"),
        JOURNAL_TAIL_SEQ("core_journal_tail_seq"),
        JOURNAL_TAIL_ACTIVE_TOTAL("core_journal_tail_active_total"),
        /** Housekeeping journal-tail reads (only with a STUB newest group and a VERIFIED shadow above P_cm). */
        JOURNAL_TAIL_READS_TOTAL("core_journal_tail_reads_total"),
        /** Housekeeping wakes that did not read the journal tail because the floor would not consult it. */
        JOURNAL_TAIL_READS_SKIPPED_TOTAL("core_journal_tail_reads_skipped_total");

        private final String metricName;

        C(final String metricName) {
            this.metricName = metricName;
        }

        public String metricName() {
            return metricName;
        }
    }

    /** {@code core_restore_candidates_refused_total{reason}}. */
    public enum Refusal {
        GENESIS_MISMATCH, GENESIS_UNVERIFIED, HISTORY_MISMATCH, HISTORY_UNVERIFIED, ENGINE_MISMATCH, CLUSTER_MISMATCH,
        UNIDENTIFIED, CORRUPT, SCAN_ERROR, REFUSED, TOO_LARGE, MULTI_RECORDING
    }

    /** {@code core_restore_conflict_total{path}}. */
    public enum ConflictAt { EQUAL, SKIP, TAIL }

    /** {@code core_restore_path} values. */
    public static final int PATH_NONE = 0;
    public static final int PATH_FULL = 1;
    public static final int PATH_EQUAL = 2;
    public static final int PATH_SKIP = 3;
    public static final int PATH_TAIL = 4;
    public static final int PATH_VIRTUAL_GENESIS = 5;

    /** The kinds a skip window drops ({@code core_skip_dropped_total{kind}}). */
    static final FrameKind[] DROPPABLE = {FrameKind.SESSION_MESSAGE, FrameKind.TIMER_EVENT, FrameKind.SESSION_OPEN,
            FrameKind.SESSION_CLOSE};

    private final AtomicLongArray values = new AtomicLongArray(C.values().length);
    private final AtomicLongArray refused = new AtomicLongArray(Refusal.values().length);
    private final AtomicLongArray conflicts = new AtomicLongArray(ConflictAt.values().length);
    private final AtomicLongArray dropped = new AtomicLongArray(FrameKind.values().length);

    public SnapshotModeCounters() {
        values.set(C.JOURNAL_TAIL_SEQ.ordinal(), -1);
    }

    public long get(final C c) {
        return values.get(c.ordinal());
    }

    void increment(final C c) {
        values.incrementAndGet(c.ordinal());
    }

    void add(final C c, final long delta) {
        values.addAndGet(c.ordinal(), delta);
    }

    void set(final C c, final long v) {
        values.set(c.ordinal(), v);
    }

    public long refused(final Refusal r) {
        return refused.get(r.ordinal());
    }

    void refuse(final Refusal r) {
        refused.incrementAndGet(r.ordinal());
    }

    public long conflicts(final ConflictAt at) {
        return conflicts.get(at.ordinal());
    }

    void conflict(final ConflictAt at) {
        conflicts.incrementAndGet(at.ordinal());
    }

    public long dropped(final FrameKind kind) {
        return dropped.get(kind.ordinal());
    }

    void drop(final FrameKind kind) {
        dropped.incrementAndGet(kind.ordinal());
    }
}

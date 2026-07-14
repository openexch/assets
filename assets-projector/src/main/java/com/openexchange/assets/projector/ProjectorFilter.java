// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

/**
 * The projector's money-critical projection decisions, kept PURE (no Aeron, no JDBC) so every
 * boundary case is unit-testable. Simpler than the settlement bridge's filter: the AE money
 * journal carries ONE dense sequence, {@code journalSeq} (starts at 1, no gaps, no dupes), so
 * there is a single rule instead of the bridge's trade/terminal split.
 *
 * One instance per resume epoch: on startup and on every epoch the projector reads the Postgres
 * high-water ({@code MAX(journal_seq)}, 0 on an empty ledger) and builds a fresh filter from it.
 * The filter itself never needs persistence.
 *
 * Rules (in decision order):
 *  1. HALTED is latched: after a detected gap nothing is ever projected again by this filter
 *     (correctness over liveness). A dense gap is a lost money movement; projecting past it would
 *     silently mis-state the ledger forever.
 *  2. {@code journalSeq <= highWater} was already projected (startup overlap, replay overlap across
 *     recordings, restart-duplicate) -> SKIP.
 *  3. A projected record must be EXACTLY {@code highWater + 1}. Anything greater is a GAP: HALT
 *     (or, with haltOnGap=false, PROJECT anyway after counting -> an explicit operator override).
 */
public final class ProjectorFilter {

    public enum Action { PROJECT, SKIP, HALT }

    private final boolean haltOnGap;

    private long highWater;
    private boolean halted;
    private long skipped;
    private long gapsDetected;

    /**
     * @param highWater the Postgres MAX(journal_seq) at epoch start (0 on an empty ledger)
     * @param haltOnGap latch HALT on a dense journalSeq gap (production default true)
     */
    public ProjectorFilter(final long highWater, final boolean haltOnGap) {
        this.highWater = highWater;
        this.haltOnGap = haltOnGap;
    }

    public Action onRecord(final long journalSeq) {
        if (halted) {
            return Action.HALT;
        }
        if (journalSeq <= highWater) {
            skipped++;
            return Action.SKIP;
        }
        if (journalSeq != highWater + 1) {
            gapsDetected++;
            if (haltOnGap) {
                halted = true;
                return Action.HALT;
            }
            // Operator override: project past the gap, loudly counted.
        }
        highWater = journalSeq;
        return Action.PROJECT;
    }

    public boolean halted() {
        return halted;
    }

    public long highWater() {
        return highWater;
    }

    public long skipped() {
        return skipped;
    }

    public long gapsDetected() {
        return gapsDetected;
    }
}

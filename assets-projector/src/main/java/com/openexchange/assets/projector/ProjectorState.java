// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

/**
 * Cross-thread observable projector state: written by the projector agent thread, read by the
 * metrics server and the periodic status log. Volatile single-writer fields, no locks needed
 * (mirrors the settlement bridge's BridgeState).
 */
public final class ProjectorState {

    public volatile boolean halted;
    /** True while a Postgres connection is usable (flips false on a flush failure / connect error). */
    public volatile boolean connectedToPg;
    public volatile String journalSource = "";
    /** Postgres high-water (MAX(journal_seq)) as read at the start of the current epoch. */
    public volatile long epochHighWater;
    /** journalSeq of the last record durably committed to Postgres. */
    public volatile long lastJournalSeq;
    public volatile long rowsProjected;
    public volatile long skippedRecords;
    public volatile long gapsDetected;
    public volatile long epochs;
    public volatile long errors;
    /** Journal-source stalls/losses detected (image lost, never connected, stopped-and-stuck). */
    public volatile long sourceStalls;
    /** Bytes recorded in the followed journal recording not yet delivered to the projector. */
    public volatile long sourceBacklogBytes;
    /** Postgres flush failures (a flush that threw); each keeps the batch pending for retry. */
    public volatile long pgFlushFailures;

    public String render() {
        return "projector{halted=" + halted + " pgConnected=" + connectedToPg
                + " source=" + journalSource
                + " epochHwm=" + epochHighWater
                + " lastJournalSeq=" + lastJournalSeq
                + " rowsProjected=" + rowsProjected + " skipped=" + skippedRecords
                + " gaps=" + gapsDetected
                + " epochs=" + epochs + " errors=" + errors
                + " stalls=" + sourceStalls + " backlog=" + sourceBacklogBytes
                + " pgFlushFailures=" + pgFlushFailures + "}";
    }
}

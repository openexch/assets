// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

/**
 * Cross-thread observable bridge state: written by the bridge thread, read by the metrics
 * server and the periodic status log. Volatile single-writer fields — no locks needed.
 */
public final class BridgeState {

    public volatile boolean halted;
    public volatile boolean connectedToAe;
    public volatile String journalSource = "";
    public volatile long epochConsumePosition;
    public volatile long epochLastAppliedTradeId;
    public volatile long forwardedTrades;
    public volatile long forwardedTerminals;
    public volatile long skippedEntries;
    public volatile long gapsDetected;
    public volatile long lastForwardedTradeId;
    public volatile long lastForwardedEgressSeq;
    public volatile long epochs;
    public volatile long errors;
    /** Journal-source stalls/losses detected (image lost, never connected, stopped-and-stuck). */
    public volatile long sourceStalls;
    /** Bytes the followed recording holds beyond what the replay has delivered (0 = caught up). */
    public volatile long sourceBacklogBytes;

    public String render() {
        return "bridge{halted=" + halted + " aeConnected=" + connectedToAe
                + " source=" + journalSource
                + " epoch(W=" + epochConsumePosition + ",T=" + epochLastAppliedTradeId + ")"
                + " fwdTrades=" + forwardedTrades + " fwdTerminals=" + forwardedTerminals
                + " skipped=" + skippedEntries + " gaps=" + gapsDetected
                + " lastTradeId=" + lastForwardedTradeId + " lastEgressSeq=" + lastForwardedEgressSeq
                + " epochs=" + epochs + " errors=" + errors
                + " stalls=" + sourceStalls + " backlog=" + sourceBacklogBytes + "}";
    }
}

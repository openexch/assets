// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Cross-thread observable bridge state: written by the bridge thread, read by the metrics
 * server and the periodic status log. Volatile single-writer fields — no locks needed.
 */
public final class BridgeState {

    /**
     * Fixed-bucket latency histogram, single-writer (the bridge agent thread), read by the
     * metrics HTTP thread. Buckets live in an {@link AtomicLongArray} purely for cross-thread
     * visibility (a plain {@code long[]} write could be seen stale by the reader) — there is
     * no contention, so no locks and no new dependency. {@code sumMs}/{@code count} are
     * volatile longs whose read-modify-write is safe because only the agent thread writes.
     * A scrape may observe a momentarily inconsistent (buckets, sum, count) triple; the
     * renderer derives {@code _count} from the bucket total so {@code le="+Inf"} always
     * equals {@code _count}.
     */
    public static final class LatencyHistogram {
        /** Bucket upper bounds in milliseconds (inclusive); one extra implicit bucket is +Inf. */
        public static final long[] BUCKET_UPPER_MS = {1, 2, 5, 10, 25, 50, 100, 250, 1000};

        /** Per-bucket sample counts (NOT cumulative); index {@code BUCKET_UPPER_MS.length} is +Inf. */
        public final AtomicLongArray buckets = new AtomicLongArray(BUCKET_UPPER_MS.length + 1);
        /** Sum of recorded values in milliseconds. Agent-thread writes only. */
        public volatile long sumMs;
        /** Number of recorded samples. Agent-thread writes only. */
        public volatile long count;

        /** Record one sample in milliseconds. Agent thread only. */
        public void record(final long valueMs) {
            int i = 0;
            while (i < BUCKET_UPPER_MS.length && valueMs > BUCKET_UPPER_MS[i]) {
                i++;
            }
            buckets.incrementAndGet(i);
            sumMs += valueMs;
            count++;
        }
    }

    /**
     * Settle forward latency: ME journal trade timestamp (epoch ms, written by the ME leader's
     * clock) -> the bridge's blocking offer for that Settle returning. CROSS-HOST: two machines'
     * clocks are involved, so this is supporting evidence only, at ms resolution — never a
     * precision claim.
     */
    public final LatencyHistogram settleForwardLatency = new LatencyHistogram();
    /**
     * Settle ack latency: blocking-offer return -> SettlementApplied egress observed for that
     * tradeId. Single host (bridge clock only) — the clean measurement.
     */
    public final LatencyHistogram settleAckLatency = new LatencyHistogram();

    /** Recorded position of the journal recording currently being replayed/followed. */
    public volatile long replayRecordingPosition;
    /** Replay position the bridge has consumed of that recording (lag = recording - consumed). */
    public volatile long replayConsumedPosition;
    /** Journal trade timestamps rejected as garbage (<= 0 or in the future); sample skipped. */
    public volatile long settleForwardClockAnomalies;
    /** Ack-tracking inserts skipped because the in-flight map hit its bound. */
    public volatile long settleAckMapSkips;

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

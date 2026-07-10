// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

/**
 * The bridge's money-critical forwarding decisions, kept PURE (no Aeron, no I/O) so every
 * boundary case is unit-testable. One instance per AE sync epoch: whenever the bridge
 * (re)connects to the AE it re-queries the feed position and builds a fresh filter from
 * the answer — the filter itself never needs persistence (the bridge is stateless).
 *
 * Inputs per journal entry: whether it is a trade or a terminal, its egressSeq (the ME
 * cluster-log position: monotonic per incarnation, SPARSE, NON-UNIQUE — an order key,
 * never a dense sequence) and, for trades, the GLOBAL DENSE gap-free tradeId.
 *
 * Rules (in decision order):
 *  1. HALTED is latched: after a detected trade gap nothing is ever forwarded again by
 *     this filter (correctness over liveness — a tradeId gap means a lost settlement;
 *     forwarding past it would silently mis-state balances forever).
 *  2. Trades dedupe on the dense tradeId: {@code tradeId <= lastForwardedTradeId} is a
 *     duplicate (startup overlap, replay overlap, restart-duplicate recordings) -> SKIP.
 *  3. A forwarded trade must be EXACTLY {@code lastForwardedTradeId + 1}. Anything
 *     greater is a GAP: HALT (or, with haltOnGap=false, FORWARD anyway after counting —
 *     an explicit operator override for recovery procedures).
 *  4. Terminals have no dense id; they dedupe on the egressSeq watermark alone:
 *     {@code egressSeq < initialWatermark} was applied before this epoch -> SKIP;
 *     {@code egressSeq >= initialWatermark} is forwarded (the == boundary re-delivers
 *     the watermark command's whole event group; the AE's hold-scoped release makes
 *     re-delivery a no-op).
 *  5. Trades below the watermark that survive rule 2 (possible only if the AE's tradeId
 *     high-water lags its consumePosition, which a snapshot makes atomic — so in
 *     practice never) still forward: the AE's settle high-water absorbs them.
 */
public final class BridgeFilter {

    public enum Action { FORWARD, SKIP, HALT }

    private final long initialWatermark;
    private final boolean haltOnGap;

    private long lastForwardedTradeId;
    private boolean halted;
    private long skippedTrades;
    private long skippedTerminals;
    private long gapsDetected;

    /**
     * @param aeConsumePosition   the AE's consumePosition (egressSeq watermark) at sync time
     * @param aeLastAppliedTradeId the AE's settle high-water at sync time
     * @param haltOnGap           latch HALT on a dense-tradeId gap (production default true)
     */
    public BridgeFilter(final long aeConsumePosition, final long aeLastAppliedTradeId, final boolean haltOnGap) {
        this.initialWatermark = aeConsumePosition;
        this.lastForwardedTradeId = aeLastAppliedTradeId;
        this.haltOnGap = haltOnGap;
    }

    public Action onTrade(final long egressSeq, final long tradeId) {
        if (halted) {
            return Action.HALT;
        }
        if (tradeId <= lastForwardedTradeId) {
            skippedTrades++;
            return Action.SKIP;
        }
        if (tradeId != lastForwardedTradeId + 1) {
            gapsDetected++;
            if (haltOnGap) {
                halted = true;
                return Action.HALT;
            }
            // Operator override: forward past the gap, loudly counted.
        }
        lastForwardedTradeId = tradeId;
        return Action.FORWARD;
    }

    public Action onTerminal(final long egressSeq) {
        if (halted) {
            return Action.HALT;
        }
        if (egressSeq < initialWatermark) {
            skippedTerminals++;
            return Action.SKIP;
        }
        return Action.FORWARD;
    }

    public boolean halted() {
        return halted;
    }

    public long lastForwardedTradeId() {
        return lastForwardedTradeId;
    }

    public long skippedTrades() {
        return skippedTrades;
    }

    public long skippedTerminals() {
        return skippedTerminals;
    }

    public long gapsDetected() {
        return gapsDetected;
    }
}

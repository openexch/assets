// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.gate.OrderEntryGate;

/**
 * The read-only longs (feed + order) the {@code OrderEngineMetrics} registrar exports. Every method
 * reads a plain value the service thread maintains at each transition (1.6), after the {@code CoreNodeMetrics} volatile
 * acquire of the scrape; none walks the backlog, the index or the order store.
 */
public final class CoreMetricsView {

    private final CoreEngine engine;

    public CoreMetricsView(final CoreEngine engine) {
        this.engine = engine;
    }

    private OrderEngine o() {
        return engine.orders();
    }

    // Order counters and the quarantine gauge
    public long staleOutcomes() { return o().staleOutcomeCount(); }
    public long redundantOutcomes() { return o().redundantOutcomeCount(); }
    public long staleTerminals() { return o().staleTerminalCount(); }
    public long implicitAcks() { return o().implicitAckCount(); }
    public long conflicts() { return o().conflictCount(); }
    public long cancelRefused() { return o().cancelRefusedCount(); }
    public long replaceSlow() { return o().replaceSlowCount(); }
    public long tradeGaps() { return o().tradeGapCount(); }
    public long faults() { return o().orderFaultCount(); }
    public long legRegressions() { return o().legRegressionCount(); }
    public long quarantinedLive() { return o().quarantinedLive(); }

    // Feed
    public long feedDuplicateUnverified() { return engine.feed().duplicateUnverifiedCount(); }
    public long feedDuplicateMismatch() { return engine.feed().duplicateMismatchCount(); }
    public long feedCursor() { return engine.feed().lastAppliedJournalSeq(); }
    public long feedLatchReason() { return engine.feed().haltReason().code(); }
    public long gateReasonsGlobal() { return engine.gate().reasons(OrderEntryGate.GLOBAL); }
    public long feedLanesBehind() { return engine.feed().lanesBehind(); }
    public long feedLaneOverflow() { return engine.feed().feedLaneOverflowCount(); }

    // (the counters)
    public long feedDuplicateTotal() { return engine.feed().duplicateTotal(); }
    public long feedHaltApplied() { return engine.feed().haltAppliedCount(); }
    public long feedHaltStale() { return engine.feed().haltStaleCount(); }
    public long feedHaltDuplicate() { return engine.feed().haltDuplicateCount(); }
    public long feedLatchAutoClear() { return engine.feed().latchAutoClearCount(); }
    public long feedGapRejects() { return engine.feed().gapCount(); }
    public long gateAutoReopen() { return engine.feed().gateAutoReopenCount(); }
    public long feedLaneHalts() { return engine.feed().laneHaltCount(); }
    public long feedHaltTableOverflow() { return engine.feed().haltOverflow() ? 1L : 0L; }
    /** Registered live sessions of {@code cls} in the replicated session table. */
    public long sessionsRegistered(final com.openexchange.core.infrastructure.generated.SessionClass cls) {
        return engine.sessions().registeredCount(cls);
    }

    // Counters
    public long releaseMintedUpTo() { return o().releaseMintedUpToCount(); }
    public long releaseMintedOrder() { return o().releaseMintedOrderCount(); }
    public long releaseConfirmed() { return o().releaseConfirmedCount(); }
    public long releaseConfirmStale() { return o().releaseConfirmStaleCount(); }
    public long releaseAbandoned() { return o().releaseAbandonedCount(); }
    public long releaseSkipped() { return o().releaseSkippedCount(); }
    public long legLive() { return o().legLiveCount(); }
    public long legLiveDuplicate() { return o().legLiveDuplicateCount(); }
    public long orphanCancels() { return o().orphanCancelCount(); }
    public long parked() { return o().parkedCount(); }
    public long releasedLate() { return o().releasedLateCount(); }
    public long meInvalid() { return o().meInvalidCount(); }
    public long meReleaseEarly() { return o().meReleaseEarlyCount(); }
    public long meAmendMismatch() { return o().meAmendMismatchCount(); }
    public long capacityWhileOpen() { return o().capacityWhileOpenCount(); }
    public long capacityOutcomes() { return o().capacityOutcomeCount(); }
    public long meLinkMismatch() { return o().meLinkMismatchCount(); }
    public long meLedgerSizeClamped() { return o().meLedgerSizeClampedCount(); }
    public long lateApplyOnDeadOrder() { return o().lateApplyOnDeadOrderCount(); }
    public long legZeroRefused() { return o().legZeroRefusedCount(); }
    public long terminalWithPendingCreate() { return o().terminalWithPendingCreateCount(); }
    public long coreOrderHoldRefused() { return engine.coreOrderHoldRefusedCount(); }
    public long coreOrderReleaseRefused() { return engine.coreOrderReleaseRefusedCount(); }

    // Gauges
    public long meLedgerSize() { return o().meLedgerSize(); }
    public long meLedgerCapacity() { return engine.feed().meLedgerCapacity(); }
    public long pendingCommands() { return o().pendingCommands(); }
    public long capacityEstimate() { return o().meLedgerSize() + o().pendingCommands(); }
    public long backlog() { return o().backlogRelease() + o().backlogOrphan() + o().backlogParked(); }
    public long backlogParked() { return o().backlogParked(); }
    public long backlogOldestSeq() { return o().oldestReleaseSeq(); }
    public long capacityParked() { return o().capacityParked(); }
    public long pendingOldestSeq() { return o().pendingOldestSeq(); }
    public long coreBehaviorVersion() { return engine.coreConfig().behaviorVersion(); }

    // Tick counters and the per-market quote the core holds. Plain reads of node-local counters and of
    // MarketRisk fields the service thread maintains (marketId 1..5; slot 0 is unused).
    public long ticksApplied() { return o().tickAppliedCount(); }
    public long ticksStale() { return o().staleTicks(); }
    public long ticksInvalid() { return o().invalidTickCount(); }
    public long ticksEpochDrop() { return engine.feed().tickEpochDropCount(); }
    public long ticksUnbound() { return engine.feed().tickUnboundCount(); }
    public long marketBookVersion(final int marketId) { return o().market(marketId).bookVersion; }
    public long marketBestBid(final int marketId) { return o().market(marketId).bestBid; }
    public long marketBestAsk(final int marketId) { return o().market(marketId).bestAsk; }
    public long marketLastTradeId(final int marketId) { return o().market(marketId).lastTradeId; }
    public long marketTickClusterMs(final int marketId) { return o().market(marketId).lastTickTs; }
}

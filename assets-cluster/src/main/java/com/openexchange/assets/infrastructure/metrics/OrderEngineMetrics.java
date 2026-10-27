// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.metrics;

import com.openexchange.assets.application.engine.order.CoreMetricsView;

import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * RC2 S8 3.10 item 3 (G6): the order-core and feed series of an Assets Engine node, registered on its
 * {@link AssetsMetricsBody} by one appended {@code include} call (critic 1.2 seam 1). Every supplier reads a plain long of
 * {@link CoreMetricsView} that the service thread maintains (S8 1.6); no supplier walks a table. The names are the ones
 * {@code assets/grafana/alerts.yml} uses ({@code AlertsFileTest} checks them against this registrar).
 */
public final class OrderEngineMetrics implements Consumer<AssetsMetricsBody> {

    private final CoreMetricsView v;
    private final LongSupplier discardCloses;

    private OrderEngineMetrics(final CoreMetricsView view, final LongSupplier discardCloses) {
        this.v = view;
        this.discardCloses = discardCloses;
    }

    /** The registrar for {@code view}; {@code discardCloses} is the SEQ {@code discardCloseCount} summed over the queues. */
    public static OrderEngineMetrics of(final CoreMetricsView view, final LongSupplier discardCloses) {
        return new OrderEngineMetrics(view, discardCloses);
    }

    /** The registrar without an egress discard-close source (in-process tests). */
    public static OrderEngineMetrics of(final CoreMetricsView view) {
        return new OrderEngineMetrics(view, () -> 0L);
    }

    @Override
    public void accept(final AssetsMetricsBody b) {
        // RC1 order counters and the quarantine gauge
        b.counter("assets_order_stale_outcomes_total", "ME outcomes that matched no pending command (Table D U3/U7)", v::staleOutcomes)
         .counter("assets_order_redundant_outcomes_total", "ME outcomes for an already learned leg (U2/U6)", v::redundantOutcomes)
         .counter("assets_order_stale_terminals_total", "ME terminals that changed nothing", v::staleTerminals)
         .counter("assets_order_implicit_acks_total", "Creates resolved by a trade or a terminal before their outcome", v::implicitAcks)
         .counter("assets_order_conflicts_total", "CONFLICT outcomes (the order is quarantined)", v::conflicts)
         .counter("assets_order_cancel_refused_total", "Cancels the ME refused (1, 4, 5, 6)", v::cancelRefused)
         .counter("assets_order_replace_slow_total", "Replace windows past their observability deadline", v::replaceSlow)
         .counter("assets_order_trade_gaps_total", "Trades refused for a tradeId gap", v::tradeGaps)
         .counter("assets_order_faults_total", "OrderFault events emitted by the order core", v::faults)
         .counter("assets_order_leg_regressions_total", "Leg installs refused by the S7 monotonicity belt", v::legRegressions)
         .gauge("assets_order_quarantined", "Live quarantined core orders", v::quarantinedLive)
        // S7 feed
         .counter("assets_feed_duplicate_unverified_total", "Feed duplicates older than the digest window (not byte-checked)",
                 v::feedDuplicateUnverified)
         .counter("assets_feed_duplicate_mismatch_total", "Feed duplicates whose bytes differ from the applied entry",
                 v::feedDuplicateMismatch)
         .gauge("assets_feed_cursor", "lastAppliedJournalSeq of the v3 feed", v::feedCursor)
         .gauge("assets_feed_latch_reason", "0 when the feed is not latched, else the FeedHaltReason code", v::feedLatchReason)
         .gauge("assets_order_entry_gate_reasons", "GLOBAL order-entry gate reason bitmask (0 = open)", v::gateReasonsGlobal)
         .gauge("assets_feed_lanes_behind", "Registered BRIDGE lanes whose highest delivered journalSeq is below the cursor",
                 v::feedLanesBehind)
         .counter("assets_feed_lane_overflow_total", "BRIDGE sessions beyond the 8-lane table", v::feedLaneOverflow)
        // S8 counters (3.10 item 3 table)
         .counter("assets_release_minted_up_to_total", "UP_TO releases minted (M1-M10)", v::releaseMintedUpTo)
         .counter("assets_release_minted_order_total", "ORDER releases minted (M11, M12)", v::releaseMintedOrder)
         .counter("assets_release_confirmed_total", "Release backlog entries dropped by a dominating confirmation (C1, C2)",
                 v::releaseConfirmed)
         .counter("assets_release_confirm_stale_total", "Release confirmations that dominated nothing (C3)", v::releaseConfirmStale)
         .counter("assets_release_abandoned_total", "Release backlog entries abandoned by RELEASED 7", v::releaseAbandoned)
         .counter("assets_release_skipped_total", "ORDER releases skipped on an F_ME_RELEASED order", v::releaseSkipped)
         .counter("assets_me_leg_live_total", "LEG_LIVE release results acted on (C4, C5)", v::legLive)
         .counter("assets_me_leg_live_duplicate_total", "LEG_LIVE copies deduplicated (C6)", v::legLiveDuplicate)
         .counter("assets_orphan_cancel_total", "Orphan cancels minted (C4)", v::orphanCancels)
         .counter("assets_backlog_parked_total", "Release backlog entries parked (C4 leg 0, C5, E3)", v::parked)
         .counter("assets_released_late_total", "RELEASED copies arriving after the core's own release (U1)", v::releasedLate)
         .counter("assets_me_invalid_total", "INVALID 8 on a pending command", v::meInvalid)
         .counter("assets_me_release_early_total", "RELEASED 7 on a pending command", v::meReleaseEarly)
         .counter("assets_me_amend_mismatch_total", "AMEND REJECTED reason 16 (the ME leg disagreed with the reference)",
                 v::meAmendMismatch)
         .counter("assets_me_capacity_while_open_total", "CAPACITY outcomes while ME_CAPACITY was clear", v::capacityWhileOpen)
         .counter("assets_me_capacity_outcome_total", "CAPACITY outcomes", v::capacityOutcomes)
         .counter("assets_me_link_mismatch_total", "64/67 entries whose capacity, behaviour or size broke the bound ME link",
                 v::meLinkMismatch)
         .counter("assets_me_ledger_size_clamped_total", "ledgerSize reports above the bound capacity (clamped)",
                 v::meLedgerSizeClamped)
         .counter("assets_late_apply_on_dead_order_total", "APPLIED outcomes for an order the core had ended (U5, U5b)",
                 v::lateApplyOnDeadOrder)
         .counter("assets_leg_zero_refused_total", "Fills or terminals naming leg 0 refused by the engine guard", v::legZeroRefused)
         .counter("assets_terminal_with_pending_create_total", "Terminals with a pending CREATE (R1 assertion, must stay 0)",
                 v::terminalWithPendingCreate)
         .counter("assets_core_order_hold_refused_total", "TOOL/ADMIN Hold 3 naming a core orderId refused under v3",
                 v::coreOrderHoldRefused)
         .counter("assets_core_order_release_refused_total", "TOOL/ADMIN Release 4 naming a core orderId refused under v3",
                 v::coreOrderReleaseRefused)
        // S8 gauges
         .gauge("assets_me_ledger_size", "Last ME command-ledger size the core applied (clamped to the capacity)", v::meLedgerSize)
         .gauge("assets_me_ledger_capacity", "Bound ME command-ledger capacity (0 while unbound)", v::meLedgerCapacity)
         .gauge("assets_me_capacity_estimate", "meLedgerSize + pending commands (the 3.5 capacity estimate e)", v::capacityEstimate)
         .gauge("assets_pending_commands", "Pending ME commands (trading and orphan cancels)", v::pendingCommands)
         .gauge("assets_release_backlog", "Release backlog entries (every stage)", v::backlog)
         .gauge("assets_release_backlog_parked", "Release backlog entries parked (operator exit D-S8-3)", v::backlogParked)
         .gauge("assets_release_backlog_oldest_seq", "Lowest dispatchSeq of an unconfirmed release (0 = none)", v::backlogOldestSeq)
         .gauge("assets_me_capacity_parked", "Pending commands waiting after a CAPACITY outcome", v::capacityParked)
         .gauge("assets_core_behavior_version", "Recorded core behaviour version", v::coreBehaviorVersion)
         .gauge("assets_pending_oldest_seq", "Lowest pending dispatchSeq of a trading command or orphan cancel (0 = none)",
                 v::pendingOldestSeq)
         .counter("assets_egress_discard_closes_total",
                 "Resumable sessions closed on a NOT_CONNECTED or MAX_POSITION_EXCEEDED discard after a delivery (S8 3.13)",
                 discardCloses);
    }
}

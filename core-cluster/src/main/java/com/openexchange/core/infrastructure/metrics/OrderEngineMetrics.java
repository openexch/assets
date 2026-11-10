// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.metrics;

import com.openexchange.core.application.engine.order.CoreMetricsView;

import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The order-core and feed series of a core node, registered on its
 * {@link CoreMetricsBody} by one appended {@code include} call (critic 1.2 seam 1). Every supplier reads a plain long of
 * {@link CoreMetricsView} that the service thread maintains; no supplier walks a table. The names are the ones
 * {@code grafana/alerts.yml} uses ({@code AlertsFileTest} checks them against this registrar).
 */
public final class OrderEngineMetrics implements Consumer<CoreMetricsBody> {

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

    /** The {@code class} label values of {@code core_sessions} (4R.1 rule 7). */
    static final String[] SESSION_CLASS_LABELS = {"ADMIN", "TOOL", "BRIDGE", "GATEWAY", "TREASURY", "FORWARDER", "REPLICA"};

    private static LongSupplier[] sessionSuppliers(final CoreMetricsView view) {
        final LongSupplier[] out = new LongSupplier[SESSION_CLASS_LABELS.length];
        for (int i = 0; i < out.length; i++) {
            final com.openexchange.core.infrastructure.generated.SessionClass cls =
                    com.openexchange.core.infrastructure.generated.SessionClass.valueOf(SESSION_CLASS_LABELS[i]);
            out[i] = () -> view.sessionsRegistered(cls);
        }
        return out;
    }

    /** The {@code market} label values of the per-market quote gauges: marketId 1..5. */
    static final String[] MARKET_LABELS = marketLabels();

    private static String[] marketLabels() {
        final String[] out = new String[com.openexchange.core.application.engine.order.OrderEngine.MARKETS];
        for (int i = 0; i < out.length; i++) {
            out[i] = Integer.toString(i + 1);
        }
        return out;
    }

    private static LongSupplier[] marketSuppliers(final java.util.function.IntToLongFunction read) {
        final LongSupplier[] out = new LongSupplier[MARKET_LABELS.length];
        for (int i = 0; i < out.length; i++) {
            final int marketId = i + 1;
            out[i] = () -> read.applyAsLong(marketId);
        }
        return out;
    }

    @Override
    public void accept(final CoreMetricsBody b) {
        // Order counters and the quarantine gauge
        b.counter("core_order_stale_outcomes_total", "ME outcomes that matched no pending command (Table D U3/U7)", v::staleOutcomes)
         .counter("core_order_redundant_outcomes_total", "ME outcomes for an already learned leg (U2/U6)", v::redundantOutcomes)
         .counter("core_order_stale_terminals_total", "ME terminals that changed nothing", v::staleTerminals)
         .counter("core_order_implicit_acks_total", "Creates resolved by a trade or a terminal before their outcome", v::implicitAcks)
         .counter("core_order_conflicts_total", "CONFLICT outcomes (the order is quarantined)", v::conflicts)
         .counter("core_order_cancel_refused_total", "Cancels the ME refused (1, 4, 5, 6)", v::cancelRefused)
         .counter("core_order_replace_slow_total", "Replace windows past their observability deadline", v::replaceSlow)
         .counter("core_order_trade_gaps_total", "Trades refused for a tradeId gap", v::tradeGaps)
         .counter("core_order_faults_total", "OrderFault events emitted by the order core", v::faults)
         .counter("core_order_leg_regressions_total", "Leg installs refused by the monotonicity belt", v::legRegressions)
         .gauge("core_order_quarantined", "Live quarantined core orders", v::quarantinedLive)
        // Feed
         .counter("core_feed_duplicate_unverified_total", "Feed duplicates older than the digest window (not byte-checked)",
                 v::feedDuplicateUnverified)
         .counter("core_feed_duplicate_mismatch_total", "Feed duplicates whose bytes differ from the applied entry",
                 v::feedDuplicateMismatch)
         .gauge("core_feed_cursor", "lastAppliedJournalSeq of the v3 feed", v::feedCursor)
         .gauge("core_feed_latch_reason", "0 when the feed is not latched, else the FeedHaltReason code", v::feedLatchReason)
         .gauge("core_order_entry_gate_reasons", "GLOBAL order-entry gate reason bitmask (0 = open)", v::gateReasonsGlobal)
         .gauge("core_feed_lanes_behind", "Registered BRIDGE lanes whose highest delivered journalSeq is below the cursor",
                 v::feedLanesBehind)
         .counter("core_feed_lane_overflow_total", "BRIDGE sessions beyond the 8-lane table", v::feedLaneOverflow)
        // (the counters and gauges)
         .counter("core_feed_duplicate_total", "Feed entries at or below the cursor that byte-matched (every second copy)",
                 v::feedDuplicateTotal)
         .counter("core_feed_halt_applied_total", "FeedHalts that wrote a lane-halt record or the latch slot",
                 v::feedHaltApplied)
         .counter("core_feed_halt_stale_total", "FeedHalts acked DUPLICATE because the cursor had passed them",
                 v::feedHaltStale)
         .counter("core_feed_halt_duplicate_total", "FeedHalts acked DUPLICATE because the core already held them",
                 v::feedHaltDuplicate)
         .counter("core_feed_latch_auto_clear_total", "Latch slots and lane-halt records cleared by their reach condition",
                 v::feedLatchAutoClear)
         .counter("core_feed_gap_rejects_total", "SettleRejected FEED_GAP emitted (an entry above cursor + 1)",
                 v::feedGapRejects)
         .counter("core_gate_auto_reopen_total", "GLOBAL order-entry gate OPENed by a GAP reach-clear (opens, not bit clears)",
                 v::gateAutoReopen)
         .gauge("core_feed_lane_halts", "Open GAP records in the replicated lane-halt table", v::feedLaneHalts)
         .gauge("core_feed_halt_table_overflow", "1 while the lane-halt overflow record is set (an operator latch)",
                 v::feedHaltTableOverflow)
         .labelledGauge("core_sessions", "Registered live sessions in the replicated session table, by class", "class",
                 SESSION_CLASS_LABELS, sessionSuppliers(v))
        // Counters (3.10 item 3 table)
         .counter("core_release_minted_up_to_total", "UP_TO releases minted (M1-M10)", v::releaseMintedUpTo)
         .counter("core_release_minted_order_total", "ORDER releases minted (M11, M12)", v::releaseMintedOrder)
         .counter("core_release_confirmed_total", "Release backlog entries dropped by a dominating confirmation (C1, C2)",
                 v::releaseConfirmed)
         .counter("core_release_confirm_stale_total", "Release confirmations that dominated nothing (C3)", v::releaseConfirmStale)
         .counter("core_release_abandoned_total", "Release backlog entries abandoned by RELEASED 7", v::releaseAbandoned)
         .counter("core_release_skipped_total", "ORDER releases skipped on an F_ME_RELEASED order", v::releaseSkipped)
         .counter("core_me_leg_live_total", "LEG_LIVE release results acted on (C4, C5)", v::legLive)
         .counter("core_me_leg_live_duplicate_total", "LEG_LIVE copies deduplicated (C6)", v::legLiveDuplicate)
         .counter("core_orphan_cancel_total", "Orphan cancels minted (C4)", v::orphanCancels)
         .counter("core_backlog_parked_total", "Release backlog entries parked (C4 leg 0, C5, E3)", v::parked)
         .counter("core_released_late_total", "RELEASED copies arriving after the core's own release (U1)", v::releasedLate)
         .counter("core_me_invalid_total", "INVALID 8 on a pending command", v::meInvalid)
         .counter("core_me_release_early_total", "RELEASED 7 on a pending command", v::meReleaseEarly)
         .counter("core_me_amend_mismatch_total", "AMEND REJECTED reason 16 (the ME leg disagreed with the reference)",
                 v::meAmendMismatch)
         .counter("core_me_capacity_while_open_total", "CAPACITY outcomes while ME_CAPACITY was clear", v::capacityWhileOpen)
         .counter("core_me_capacity_outcome_total", "CAPACITY outcomes", v::capacityOutcomes)
         .counter("core_me_link_mismatch_total", "64/67 entries whose capacity, behaviour or size broke the bound ME link",
                 v::meLinkMismatch)
         .counter("core_me_ledger_size_clamped_total", "ledgerSize reports above the bound capacity (clamped)",
                 v::meLedgerSizeClamped)
         .counter("core_late_apply_on_dead_order_total", "APPLIED outcomes for an order the core had ended (U5, U5b)",
                 v::lateApplyOnDeadOrder)
         .counter("core_leg_zero_refused_total", "Fills or terminals naming leg 0 refused by the engine guard", v::legZeroRefused)
         .counter("core_terminal_with_pending_create_total", "Terminals with a pending CREATE (R1 assertion, must stay 0)",
                 v::terminalWithPendingCreate)
         .counter("core_order_hold_refused_total", "TOOL/ADMIN Hold 3 naming a core orderId refused under v3",
                 v::coreOrderHoldRefused)
         .counter("core_order_release_refused_total", "TOOL/ADMIN Release 4 naming a core orderId refused under v3",
                 v::coreOrderReleaseRefused)
        // Gauges
         .gauge("core_me_ledger_size", "Last ME command-ledger size the core applied (clamped to the capacity)", v::meLedgerSize)
         .gauge("core_me_ledger_capacity", "Bound ME command-ledger capacity (0 while unbound)", v::meLedgerCapacity)
         .gauge("core_me_capacity_estimate", "meLedgerSize + pending commands (the 3.5 capacity estimate e)", v::capacityEstimate)
         .gauge("core_pending_commands", "Pending ME commands (trading and orphan cancels)", v::pendingCommands)
         .gauge("core_release_backlog", "Release backlog entries (every stage)", v::backlog)
         .gauge("core_release_backlog_parked", "Release backlog entries parked (operator exit)", v::backlogParked)
         .gauge("core_release_backlog_oldest_seq", "Lowest dispatchSeq of an unconfirmed release (0 = none)", v::backlogOldestSeq)
         .gauge("core_me_capacity_parked", "Pending commands waiting after a CAPACITY outcome", v::capacityParked)
         .gauge("core_behavior_version", "Recorded core behaviour version", v::coreBehaviorVersion)
         .gauge("core_pending_oldest_seq", "Lowest pending dispatchSeq of a trading command or orphan cancel (0 = none)",
                 v::pendingOldestSeq)
         .counter("core_egress_discard_closes_total",
                 "Resumable sessions closed on a NOT_CONNECTED or MAX_POSITION_EXCEEDED discard after a delivery",
                 discardCloses)
        // The K4 MarketTick counters and the per-market quote
         .counter("core_ticks_applied_total", "MarketTicks applied (version above the stored one)", v::ticksApplied)
         .counter("core_ticks_stale_total", "MarketTicks dropped at or below the stored bookVersion", v::ticksStale)
         .counter("core_ticks_invalid_total", "MarketTicks dropped as malformed, crossed or for an unknown market (a forwarder defect)",
                 v::ticksInvalid)
         .counter("core_ticks_epoch_drop_total", "MarketTicks dropped for a foreign meEpoch (a forwarder defect)", v::ticksEpochDrop)
         .counter("core_ticks_unbound_total", "MarketTicks dropped before the core bound an ME epoch (expected at genesis)",
                 v::ticksUnbound)
         .labelledGauge("core_market_book_version", "bookVersion of the last applied tick, by market", "market", MARKET_LABELS,
                 marketSuppliers(v::marketBookVersion))
         .labelledGauge("core_market_best_bid", "Best bid (8 dp) of the last applied tick, by market", "market", MARKET_LABELS,
                 marketSuppliers(v::marketBestBid))
         .labelledGauge("core_market_best_ask", "Best ask (8 dp) of the last applied tick, by market", "market", MARKET_LABELS,
                 marketSuppliers(v::marketBestAsk))
         .labelledGauge("core_market_last_trade_id", "Last trade id the core holds, by market", "market", MARKET_LABELS,
                 marketSuppliers(v::marketLastTradeId))
         .labelledGauge("core_market_tick_cluster_ms", "Cluster time (ms) of the last applied tick, by market (0 = none)", "market",
                 MARKET_LABELS, marketSuppliers(v::marketTickClusterMs));
    }
}

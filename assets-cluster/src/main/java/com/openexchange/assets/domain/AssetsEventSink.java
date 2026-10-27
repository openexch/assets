// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Outbound port: the money domain emits its events through this interface, in money terms
 * (primitives only — zero allocation). The production adapter ({@code AssetsEventPublisher})
 * encodes these to SBE egress; the test adapter ({@code RecordingAssetsSink}) captures them for
 * golden rendering. The domain and the engine depend only on this port, never on an adapter — which
 * is exactly what lets the determinism harness swap the adapter with no engine change.
 *
 * <p>All events are invoked synchronously on the single engine thread within {@code applyCommand},
 * so the captured stream is the complete, globally-ordered output the moment the call returns.</p>
 */
public interface AssetsEventSink {

    /**
     * A deposit was applied: {@code amount} of {@code assetId} credited to the user's available balance.
     * {@code correlationId} echoes the request's client-chosen id (0 = fire-and-forget). Emitted before
     * the resulting {@link #onBalanceUpdate}. {@code newAvailable} is the post-credit available balance.
     */
    void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate);

    /**
     * A withdrawal was applied: {@code amount} of {@code assetId} debited from the user's available
     * balance. {@code correlationId} echoes the request's client-chosen id. Emitted before the resulting
     * {@link #onBalanceUpdate}. {@code newAvailable} is the post-debit available balance.
     */
    void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate);

    /** A hold was accepted: {@code amount} of {@code assetId} moved available -> locked for the user. */
    void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount);

    /** A hold was rejected (no state change). */
    void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason);

    /** A user's balance for one asset changed to these new (available, locked) values. */
    void onBalanceUpdate(long userId, int assetId, long available, long locked);

    /** A trade settled (idempotency confirmation), after the balance updates for both parties. */
    void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId);

    /** A withdrawal was rejected (no state change). */
    void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason);

    // ---- read-only snapshot queries (a stream of entries terminated by an *End with a count) ----

    /**
     * Terminates a balance snapshot: the {@code entryCount} preceding {@link #onBalanceUpdate}s answered
     * the {@code RequestBalanceSnapshot} carrying this {@code correlationId}.
     */
    void onBalanceSnapshotEnd(long correlationId, int entryCount);

    /** One outstanding hold, streamed in answer to a {@code RequestHoldSnapshot}. */
    void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining);

    /**
     * Terminates a hold snapshot: the {@code entryCount} preceding {@link #onHoldSnapshotEntry}s answered
     * the {@code RequestHoldSnapshot} carrying this {@code correlationId}.
     */
    void onHoldSnapshotEnd(long correlationId, int entryCount);

    /**
     * Answers a {@code QueryFeedPosition}: how far into the ME settlement journal this ledger has
     * applied ({@code consumePosition} = journal egressSeq watermark) and the settlement high-water
     * ({@code lastAppliedTradeId}). The settlement bridge resumes the journal from these.
     */
    void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId,
                              boolean journalEnabled);

    /**
     * EXCEPTIONAL: a settle leg could not draw fully from its order hold (reconciler race or a
     * genuine breach). {@code drawnFromAvailable} was recovered from the payer's available;
     * {@code uncovered} could not be moved at all (the counterparty was credited only what was
     * debited, so conservation holds). MUST be alerted on.
     */
    void onSettleFault(long tradeId, long orderId, long userId, int assetId,
                       long drawnFromAvailable, long uncovered);

    /**
     * EXCEPTIONAL (RC1 S1): a feed settle failed pre-validation and was refused with NO mutation and NO
     * journal-cursor advance ({@code journalPosition} is the refused entry's position so the feed can
     * see exactly where it stopped). {@code detail} is the offending value (marketId, price, quantity).
     * MUST be alerted on: the feed is delivering trades this ledger cannot book.
     */
    void onSettleRejected(long tradeId, int marketId, long journalPosition, SettleRejectReason reason, long detail);

    // ---- RC1 S3: session authority, admin and gate outcomes ----

    /** ORIGIN reject of an ingress command (wire OrderReject). refTemplateId names the refused template. */
    void onOrderReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, long orderId,
                       int marketId, int refTemplateId, CoreRejectReason reason, int meReason, long detail,
                       long rejectedAtMs);

    /** ORIGIN reply to an admin command or RegisterSession (wire AdminAck). */
    void onAdminAck(long correlationId, int refTemplateId, AdminResult result, int reason, long configVersion);

    /** Gate state for one market (0 = global); BROADCAST on control, or ORIGIN when answering RegisterSession. */
    void onAdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                          long atMs, long logPosition, boolean toOrigin);

    // ---- RC1 S4: order admission, dispatch, money requestId outcomes, config ----

    /** ORIGIN reply to CreateOrder/CancelOrder/AmendOrder (wire OrderAck). */
    void onOrderAck(long correlationId, long requestIdHigh, long requestIdLow, long orderId, long userId, int marketId,
                    OrderStatus status, boolean duplicate, int holdAssetId, long holdAmount, long acceptedAtMs, long eventSeq);

    /** BROADCAST orders channel (wire OrderEvent): one per transition, dense eventSeq. */
    void onOrderEvent(long eventSeq, long logPosition, CoreOrderView order, EventKind kind, CoreRejectReason reason,
                      long lastPrice, long lastQty, long tradeId, long holdRemaining, long updatedAtMs);

    /** BROADCAST dispatch channel (wire MeDispatch): one durable ME command the forwarder relays verbatim. */
    void onMeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                      long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                      long quantity, long budget, int timeInForceCode);

    /** ORIGIN: a deposit was refused (wire DepositReject); v5 emitted nothing here (critic D13). */
    void onDepositReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, int assetId, long amount,
                         RejectReason reason);

    /** BROADCAST control: a config record was applied (wire ConfigApplied). */
    void onConfigApplied(ConfigKind kind, int marketId, long configVersion, long logPosition);

    // ---- RC1 S5 (defaults are no-ops so that test fakes outside the order scope need not change) ----

    /** BROADCAST control: an order-level anomaly, settled and reported (wire OrderFault 113). */
    default void onOrderFault(OrderFaultKind kind, long orderId, long userId, int marketId, long tradeId, long amount,
                              long logPosition) { }

    /** ORIGIN reply page for RequestOrderSnapshot (wire OrderSnapshotBatch 106, framed by the publisher). */
    default void onOrderSnapshotBatch(long correlationId, CoreOrderView[] page, int count) { }

    /** ORIGIN terminator for RequestOrderSnapshot (wire OrderSnapshotEnd 107). */
    default void onOrderSnapshotEnd(long correlationId, int entryCount, long nextOrderId, long asOfLogPosition) { }

    /** ORIGIN terminator for RequestPendingDispatch (wire PendingDispatchEnd 108). */
    default void onPendingDispatchEnd(long correlationId, int count, long headDispatchSeq) { }

    /** ORIGIN re-emission of a pending MeDispatch for RequestPendingDispatch (same wire frame 104). */
    default void onMeDispatchReply(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                                   long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                                   long quantity, long budget, int timeInForceCode) { }

    // ---- RC2 S7 (contract section 15). Defaults delegate to the RC1 methods, so every existing sink compiles unchanged;
    //      the engine calls these, the publisher overrides them and writes the v7 fields. ----

    /** {@link #onFeedPositionReport(long, long, long, boolean)} with the v7 tail (3.6): all zero under v1/v2. */
    default void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId, boolean journalEnabled,
                                      long meEpoch, long lastAppliedJournalSeq, boolean feedHalted, FeedHaltReason haltReason) {
        onFeedPositionReport(correlationId, consumePosition, lastAppliedTradeId, journalEnabled);
    }

    /** {@link #onAdmissionState} with the D4 bitmask (6.2); equal to S1's derived value under v1/v2. */
    default void onAdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                                  long atMs, long logPosition, boolean toOrigin, int gateReasons) {
        onAdmissionState(marketId, gateState, gateReason, circuitBreaker, atMs, logPosition, toOrigin);
    }

    /** {@link #onOrderEvent} with the ME reason code (D9b); 255 = not ME-sourced, always 255 under v1/v2. */
    default void onOrderEvent(long eventSeq, long logPosition, CoreOrderView order, EventKind kind, CoreRejectReason reason,
                              long lastPrice, long lastQty, long tradeId, long holdRemaining, long updatedAtMs, int meReason) {
        onOrderEvent(eventSeq, logPosition, order, kind, reason, lastPrice, lastQty, tradeId, holdRemaining, updatedAtMs);
    }

    /**
     * Node-local observability (review 2 FIX-F): a feed latch was raised or replaced (5.2). Never wire bytes (production
     * alerting reads the OrderFault{FEED_HALT}); {@code rawTradeId} is the refused frame's own tradeId, diagnostic only.
     */
    default void onFeedLatch(FeedHaltReason reason, long expected, long seen, long skip, long rawTradeId) { }

    /** Node-local observability: a latch was cleared by the reach-clear ({@code auto}) or by FeedLatchClear 76. */
    default void onFeedLatchCleared(FeedHaltReason reason, long journalSeq, boolean auto) { }

    // ---- RC2 S8 (contract section 4). Defaults delegate to the RC1 forms, so every existing sink compiles unchanged; the
    //      engine calls these, the publisher overrides them and writes MeDispatch fields 15 and 16. ----

    /**
     * {@link #onMeDispatch} with the v7 fields: {@code releaseScope} (read only on RELEASE_ID, UP_TO otherwise) and
     * {@code expectedOldRemaining} (the old leg's remainingQty at an AMEND mint under behaviour v3, else 0).
     */
    default void onMeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                              long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                              long quantity, long budget, int timeInForceCode, ReleaseScope releaseScope,
                              long expectedOldRemaining) {
        onMeDispatch(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId, oldOrderId, marketId, orderTypeCode,
                sideCode, price, quantity, budget, timeInForceCode);
    }

    /** {@link #onMeDispatchReply} with the v7 fields, equal to the minted frame field for field (S8 3.8). */
    default void onMeDispatchReply(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                                   long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                                   long quantity, long budget, int timeInForceCode, ReleaseScope releaseScope,
                                   long expectedOldRemaining) {
        onMeDispatchReply(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId, oldOrderId, marketId, orderTypeCode,
                sideCode, price, quantity, budget, timeInForceCode);
    }

    /**
     * Node-local observability (no wire bytes, like {@link #onFeedLatch}): one release-backlog effect of S8 3.2, its row
     * code ({@code 1..6} = C1..C6, {@code 11..14} = E1..E4, {@code 21} = abandonment). {@code result} is null for the
     * orphan rows and the abandonment, which are not release results.
     */
    default void onReleaseEffect(long orderId, long floor, ReleaseScope scope, ReleaseResult result, int row) { }
}

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
    void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable);

    /**
     * A withdrawal was applied: {@code amount} of {@code assetId} debited from the user's available
     * balance. {@code correlationId} echoes the request's client-chosen id. Emitted before the resulting
     * {@link #onBalanceUpdate}. {@code newAvailable} is the post-debit available balance.
     */
    void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable);

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
}

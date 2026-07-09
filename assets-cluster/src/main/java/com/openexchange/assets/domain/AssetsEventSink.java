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

    /** A hold was accepted: {@code amount} of {@code assetId} moved available -> locked for the user. */
    void onHoldAck(long orderId, long userId, int assetId, long amount);

    /** A hold was rejected (no state change). */
    void onHoldReject(long orderId, long userId, int assetId, long amount, RejectReason reason);

    /** A user's balance for one asset changed to these new (available, locked) values. */
    void onBalanceUpdate(long userId, int assetId, long available, long locked);

    /** A trade settled (idempotency confirmation), after the balance updates for both parties. */
    void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId);

    /** A withdrawal was rejected (no state change). */
    void onWithdrawReject(long userId, int assetId, long amount, RejectReason reason);
}

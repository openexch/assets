// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.RejectReason;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Synchronous {@link AssetsEventSink} that records every engine event inline on the calling thread —
 * no Disruptor, no threads. The engine calls these methods within {@code applyCommand}, so the
 * captured list is the complete, globally-ordered output stream the moment the call returns. This is
 * the test-side adapter of the outbound port; the determinism harness works precisely because the
 * engine depends on the port, not on the production publisher.
 */
public final class RecordingAssetsSink implements AssetsEventSink {

    private final List<AssetsEngineEvent> events = new ArrayList<>();

    @Override
    public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable) {
        events.add(new AssetsEngineEvent.DepositAck(correlationId, userId, assetId, amount, newAvailable));
    }

    @Override
    public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable) {
        events.add(new AssetsEngineEvent.WithdrawAck(correlationId, userId, assetId, amount, newAvailable));
    }

    @Override
    public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) {
        events.add(new AssetsEngineEvent.HoldAck(correlationId, orderId, userId, assetId, amount));
    }

    @Override
    public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason) {
        events.add(new AssetsEngineEvent.HoldReject(correlationId, orderId, userId, assetId, amount, reason));
    }

    @Override
    public void onBalanceUpdate(long userId, int assetId, long available, long locked) {
        events.add(new AssetsEngineEvent.Balance(userId, assetId, available, locked));
    }

    @Override
    public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) {
        events.add(new AssetsEngineEvent.Settled(tradeId, buyerUserId, sellerUserId));
    }

    @Override
    public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) {
        events.add(new AssetsEngineEvent.WithdrawReject(correlationId, userId, assetId, amount, reason));
    }

    @Override
    public void onBalanceSnapshotEnd(long correlationId, int entryCount) {
        events.add(new AssetsEngineEvent.BalanceSnapshotEnd(correlationId, entryCount));
    }

    @Override
    public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) {
        events.add(new AssetsEngineEvent.HoldSnapshotEntry(orderId, userId, assetId, remaining));
    }

    @Override
    public void onHoldSnapshotEnd(long correlationId, int entryCount) {
        events.add(new AssetsEngineEvent.HoldSnapshotEnd(correlationId, entryCount));
    }

    public List<AssetsEngineEvent> events() {
        return Collections.unmodifiableList(events);
    }

    /** Clear the captured stream — for tests that assert only a later phase's output (e.g. a query answer). */
    public void reset() {
        events.clear();
    }

    /** Render the full captured stream to canonical text, one event per line (trailing newline). */
    public String render() {
        StringBuilder sb = new StringBuilder();
        for (AssetsEngineEvent e : events) {
            sb.append(e.render()).append('\n');
        }
        return sb.toString();
    }
}

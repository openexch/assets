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
    public void onHoldAck(long orderId, long userId, int assetId, long amount) {
        events.add(new AssetsEngineEvent.HoldAck(orderId, userId, assetId, amount));
    }

    @Override
    public void onHoldReject(long orderId, long userId, int assetId, long amount, RejectReason reason) {
        events.add(new AssetsEngineEvent.HoldReject(orderId, userId, assetId, amount, reason));
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
    public void onWithdrawReject(long userId, int assetId, long amount, RejectReason reason) {
        events.add(new AssetsEngineEvent.WithdrawReject(userId, assetId, amount, reason));
    }

    public List<AssetsEngineEvent> events() {
        return Collections.unmodifiableList(events);
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

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreOrderView;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettleRejectReason;

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
    public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) {
        events.add(new AssetsEngineEvent.DepositAck(correlationId, userId, assetId, amount, newAvailable, duplicate));
    }

    @Override
    public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) {
        events.add(new AssetsEngineEvent.WithdrawAck(correlationId, userId, assetId, amount, newAvailable, duplicate));
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

    @Override
    public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId,
                                     boolean journalEnabled) {
        events.add(new AssetsEngineEvent.FeedPositionReport(
                correlationId, consumePosition, lastAppliedTradeId, journalEnabled));
    }

    @Override
    public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId, boolean journalEnabled,
                                     long meEpoch, long lastAppliedJournalSeq, boolean feedHalted, FeedHaltReason haltReason) {
        events.add(new AssetsEngineEvent.FeedPositionReport(correlationId, consumePosition, lastAppliedTradeId, journalEnabled,
                meEpoch, lastAppliedJournalSeq, feedHalted, haltReason));
    }

    @Override
    public void onFeedLatch(FeedHaltReason reason, long expected, long seen, long skip, long rawTradeId) {
        events.add(new AssetsEngineEvent.FeedLatch(reason, expected, seen, skip, rawTradeId));
    }

    @Override
    public void onFeedLatchCleared(FeedHaltReason reason, long journalSeq, boolean auto) {
        events.add(new AssetsEngineEvent.FeedLatchCleared(reason, journalSeq, auto));
    }

    @Override
    public void onSettleFault(long tradeId, long orderId, long userId, int assetId,
                              long drawnFromAvailable, long uncovered) {
        events.add(new AssetsEngineEvent.SettleFault(tradeId, orderId, userId, assetId,
                drawnFromAvailable, uncovered));
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

    @Override
    public void onSettleRejected(long tradeId, int marketId, long journalPosition, SettleRejectReason reason, long detail) {
        events.add(new AssetsEngineEvent.SettleRejected(tradeId, marketId, journalPosition, reason, detail));
    }

    @Override
    public void onOrderReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, long orderId,
                              int marketId, int refTemplateId, CoreRejectReason reason, int meReason, long detail,
                              long rejectedAtMs) {
        events.add(new AssetsEngineEvent.OrderReject(correlationId, userId, orderId, marketId, refTemplateId, reason, detail));
    }

    @Override
    public void onAdminAck(long correlationId, int refTemplateId, AdminResult result, int reason, long configVersion) {
        events.add(new AssetsEngineEvent.AdminAck(correlationId, refTemplateId, result, reason, configVersion));
    }

    @Override
    public void onAdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                                 long atMs, long logPosition, boolean toOrigin) {
        events.add(new AssetsEngineEvent.AdmissionState(marketId, gateState, gateReason, circuitBreaker, toOrigin));
    }

    @Override
    public void onAdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                                 long atMs, long logPosition, boolean toOrigin, int gateReasons) {
        events.add(new AssetsEngineEvent.AdmissionState(marketId, gateState, gateReason, circuitBreaker, toOrigin, gateReasons));
    }

    @Override
    public void onOrderAck(long correlationId, long requestIdHigh, long requestIdLow, long orderId, long userId, int marketId,
                           OrderStatus status, boolean duplicate, int holdAssetId, long holdAmount, long acceptedAtMs, long eventSeq) {
        events.add(new AssetsEngineEvent.OrderAck(correlationId, orderId, userId, marketId, status, duplicate, holdAssetId, holdAmount, eventSeq));
    }

    @Override
    public void onOrderEvent(long eventSeq, long logPosition, CoreOrderView o, EventKind kind, CoreRejectReason reason,
                             long lastPrice, long lastQty, long tradeId, long holdRemaining, long updatedAtMs) {
        events.add(new AssetsEngineEvent.OrderEvent(eventSeq, o.orderId(), o.userId(), o.marketId(), kind,
                OrderStatus.fromCode(o.statusCode()), reason, o.price(), o.quantity(), o.filledQty(), o.remainingQty(),
                lastPrice, lastQty, tradeId, o.meOrderId(), o.holdAssetId(), o.holdAmount(), holdRemaining));
    }

    @Override
    public void onOrderEvent(long eventSeq, long logPosition, CoreOrderView o, EventKind kind, CoreRejectReason reason,
                             long lastPrice, long lastQty, long tradeId, long holdRemaining, long updatedAtMs, int meReason) {
        events.add(new AssetsEngineEvent.OrderEvent(eventSeq, o.orderId(), o.userId(), o.marketId(), kind,
                OrderStatus.fromCode(o.statusCode()), reason, o.price(), o.quantity(), o.filledQty(), o.remainingQty(),
                lastPrice, lastQty, tradeId, o.meOrderId(), o.holdAssetId(), o.holdAmount(), holdRemaining, meReason));
    }

    @Override
    public void onMeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                             long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                             long quantity, long budget, int timeInForceCode) {
        onMeDispatch(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId, oldOrderId, marketId, orderTypeCode,
                sideCode, price, quantity, budget, timeInForceCode, com.openexchange.assets.domain.ReleaseScope.UP_TO, 0L);
    }

    @Override
    public void onMeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                             long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                             long quantity, long budget, int timeInForceCode, com.openexchange.assets.domain.ReleaseScope releaseScope,
                             long expectedOldRemaining) {
        events.add(new AssetsEngineEvent.MeDispatch(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId,
                oldOrderId, marketId, orderTypeCode, sideCode, price, quantity, budget, timeInForceCode, releaseScope,
                expectedOldRemaining));
    }

    @Override
    public void onReleaseEffect(long orderId, long floor, com.openexchange.assets.domain.ReleaseScope scope,
                                com.openexchange.assets.domain.ReleaseResult result, int row) {
        events.add(new AssetsEngineEvent.ReleaseEffect(orderId, floor, scope, result, row));
    }

    @Override
    public void onDepositReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, int assetId, long amount,
                                RejectReason reason) {
        events.add(new AssetsEngineEvent.DepositReject(correlationId, userId, assetId, amount, reason));
    }

    @Override
    public void onConfigApplied(ConfigKind kind, int marketId, long configVersion, long logPosition) {
        events.add(new AssetsEngineEvent.ConfigApplied(kind, marketId, configVersion));
    }

    // ---- RC1 S5 ----

    @Override
    public void onOrderFault(OrderFaultKind kind, long orderId, long userId, int marketId, long tradeId, long amount, long logPosition) {
        events.add(new AssetsEngineEvent.OrderFault(kind, orderId, userId, marketId, tradeId, amount));
    }

    @Override
    public void onOrderSnapshotBatch(long correlationId, CoreOrderView[] page, int count) {
        for (int i = 0; i < count; i++) {
            final CoreOrderView o = page[i];
            events.add(new AssetsEngineEvent.OrderSnapshotEntry(correlationId, o.orderId(), o.userId(), o.marketId(),
                    OrderStatus.fromCode(o.statusCode()), o.price(), o.quantity(), o.filledQty(), o.remainingQty(), o.holdAssetId(),
                    o.holdAmount(), o.meOrderId(), o.sliceSeq()));
        }
    }

    @Override
    public void onOrderSnapshotEnd(long correlationId, int entryCount, long nextOrderId, long asOfLogPosition) {
        events.add(new AssetsEngineEvent.OrderSnapshotEnd(correlationId, entryCount, nextOrderId));
    }

    @Override
    public void onPendingDispatchEnd(long correlationId, int count, long headDispatchSeq) {
        events.add(new AssetsEngineEvent.PendingDispatchEnd(correlationId, count, headDispatchSeq));
    }

    @Override
    public void onMeDispatchReply(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                                  long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                                  long quantity, long budget, int timeInForceCode) {
        onMeDispatchReply(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId, oldOrderId, marketId, orderTypeCode,
                sideCode, price, quantity, budget, timeInForceCode, com.openexchange.assets.domain.ReleaseScope.UP_TO, 0L);
    }

    @Override
    public void onMeDispatchReply(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                                  long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                                  long quantity, long budget, int timeInForceCode,
                                  com.openexchange.assets.domain.ReleaseScope releaseScope, long expectedOldRemaining) {
        events.add(new AssetsEngineEvent.MeDispatchReply(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId,
                oldOrderId, marketId, orderTypeCode, sideCode, price, quantity, budget, timeInForceCode, releaseScope,
                expectedOldRemaining));
    }
}

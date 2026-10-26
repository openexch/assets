// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreOrderView;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettleRejectReason;

/**
 * RCS S1 (review FIX-3): the event sink an {@link AssetsLogApplier} starts with, so an owner that never wires one (a
 * replayer, a shadow, a test fixture) applies the log instead of throwing a NullPointerException on the first emitting
 * command. Every abstract method of {@link AssetsEventSink} is implemented here on purpose: an abstract method added to
 * the port later is a compile error in this class, never a silent gap. The service replaces it in {@code onStart}.
 */
final class NoopAssetsEventSink implements AssetsEventSink {

    static final NoopAssetsEventSink INSTANCE = new NoopAssetsEventSink();

    private NoopAssetsEventSink() {
    }

    public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) { }

    public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) { }

    public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) { }

    public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason) { }

    public void onBalanceUpdate(long userId, int assetId, long available, long locked) { }

    public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) { }

    public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) { }

    public void onBalanceSnapshotEnd(long correlationId, int entryCount) { }

    public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) { }

    public void onHoldSnapshotEnd(long correlationId, int entryCount) { }

    public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId,
                                     boolean journalEnabled) { }

    public void onSettleFault(long tradeId, long orderId, long userId, int assetId, long drawnFromAvailable,
                              long uncovered) { }

    public void onSettleRejected(long tradeId, int marketId, long journalPosition, SettleRejectReason reason, long detail) { }

    public void onOrderReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, long orderId,
                              int marketId, int refTemplateId, CoreRejectReason reason, int meReason, long detail,
                              long rejectedAtMs) { }

    public void onAdminAck(long correlationId, int refTemplateId, AdminResult result, int reason, long configVersion) { }

    public void onAdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                                 long atMs, long logPosition, boolean toOrigin) { }

    public void onOrderAck(long correlationId, long requestIdHigh, long requestIdLow, long orderId, long userId,
                           int marketId, OrderStatus status, boolean duplicate, int holdAssetId, long holdAmount,
                           long acceptedAtMs, long eventSeq) { }

    public void onOrderEvent(long eventSeq, long logPosition, CoreOrderView order, EventKind kind, CoreRejectReason reason,
                             long lastPrice, long lastQty, long tradeId, long holdRemaining, long updatedAtMs) { }

    public void onMeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                             long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                             long quantity, long budget, int timeInForceCode) { }

    public void onDepositReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, int assetId,
                                long amount, RejectReason reason) { }

    public void onConfigApplied(ConfigKind kind, int marketId, long configVersion, long logPosition) { }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.domain.AdminResult;
import com.openexchange.core.domain.CoreEventSink;
import com.openexchange.core.domain.ConfigKind;
import com.openexchange.core.domain.CoreOrderView;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.DispatchKind;
import com.openexchange.core.domain.EventKind;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import com.openexchange.core.domain.OrderStatus;
import com.openexchange.core.domain.RejectReason;
import com.openexchange.core.domain.SettleRejectReason;

/**
 * The event sink an {@link CoreLogApplier} starts with, so an owner that never wires one (a
 * replayer, a shadow, a test fixture) applies the log instead of throwing a NullPointerException on the first emitting
 * command. Every abstract method of {@link CoreEventSink} is implemented here on purpose: an abstract method added to
 * the port later is a compile error in this class, never a silent gap. The service replaces it in {@code onStart}.
 */
final class NoopCoreEventSink implements CoreEventSink {

    static final NoopCoreEventSink INSTANCE = new NoopCoreEventSink();

    private NoopCoreEventSink() {
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

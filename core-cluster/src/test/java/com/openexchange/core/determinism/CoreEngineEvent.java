// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import com.openexchange.core.domain.AdminResult;
import com.openexchange.core.domain.ConfigKind;
import com.openexchange.core.domain.DispatchKind;
import com.openexchange.core.domain.EventKind;
import com.openexchange.core.domain.FeedHaltReason;
import com.openexchange.core.domain.OrderFaultKind;
import com.openexchange.core.domain.OrderStatus;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import com.openexchange.core.domain.RejectReason;
import com.openexchange.core.domain.SettleRejectReason;

/**
 * An immutable core output event captured by {@link RecordingCoreSink}. Each renders to a
 * single canonical line for golden-master comparison; record equality also backs the run-twice /
 * wall-clock-invariance determinism checks. Values are raw fixed-point longs (implementation-agnostic;
 * no transport fields). Acks echo the request's {@code correlationId} (0 = fire-and-forget).
 */
public sealed interface CoreEngineEvent
        permits CoreEngineEvent.Balance, CoreEngineEvent.DepositAck, CoreEngineEvent.WithdrawAck,
                CoreEngineEvent.HoldAck, CoreEngineEvent.HoldReject, CoreEngineEvent.WithdrawReject,
                CoreEngineEvent.Settled, CoreEngineEvent.BalanceSnapshotEnd,
                CoreEngineEvent.HoldSnapshotEntry, CoreEngineEvent.HoldSnapshotEnd,
                CoreEngineEvent.FeedPositionReport, CoreEngineEvent.SettleFault,
                CoreEngineEvent.SettleRejected, CoreEngineEvent.OrderReject, CoreEngineEvent.AdminAck,
                CoreEngineEvent.AdmissionState, CoreEngineEvent.OrderAck, CoreEngineEvent.OrderEvent,
                CoreEngineEvent.MeDispatch, CoreEngineEvent.DepositReject, CoreEngineEvent.ConfigApplied,
        CoreEngineEvent.OrderFault, CoreEngineEvent.OrderSnapshotEntry, CoreEngineEvent.OrderSnapshotEnd,
        CoreEngineEvent.PendingDispatchEnd, CoreEngineEvent.MeDispatchReply,
        CoreEngineEvent.FeedLatch, CoreEngineEvent.FeedLatchCleared, CoreEngineEvent.ReleaseEffect,
        CoreEngineEvent.GateAutoReopen {

    String render();

    record DepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("DEPOSITACK corr=%d u=%d asset=%d amt=%d newAvail=%d%s",
                    correlationId, userId, assetId, amount, newAvailable, duplicate ? " dup=1" : "");
        }
    }

    record WithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("WITHDRAWACK corr=%d u=%d asset=%d amt=%d newAvail=%d%s",
                    correlationId, userId, assetId, amount, newAvailable, duplicate ? " dup=1" : "");
        }
    }

    record Balance(long userId, int assetId, long available, long locked) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("BALANCE u=%d asset=%d avail=%d locked=%d", userId, assetId, available, locked);
        }
    }

    record HoldAck(long correlationId, long orderId, long userId, int assetId, long amount)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDACK corr=%d order=%d u=%d asset=%d amt=%d",
                    correlationId, orderId, userId, assetId, amount);
        }
    }

    record HoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDREJECT corr=%d order=%d u=%d asset=%d amt=%d reason=%s",
                    correlationId, orderId, userId, assetId, amount, reason.name());
        }
    }

    record WithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("WITHDRAWREJECT corr=%d u=%d asset=%d amt=%d reason=%s",
                    correlationId, userId, assetId, amount, reason.name());
        }
    }

    record Settled(long tradeId, long buyerUserId, long sellerUserId) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("SETTLED tradeId=%d buyer=%d seller=%d", tradeId, buyerUserId, sellerUserId);
        }
    }

    record BalanceSnapshotEnd(long correlationId, int entryCount) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("BALSNAPEND corr=%d count=%d", correlationId, entryCount);
        }
    }

    record HoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDSNAPENTRY order=%d u=%d asset=%d remaining=%d",
                    orderId, userId, assetId, remaining);
        }
    }

    record HoldSnapshotEnd(long correlationId, int entryCount) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDSNAPEND corr=%d count=%d", correlationId, entryCount);
        }
    }

    /** The v7 tail renders only when it is not {@code 0 0 FALSE NONE} (so the v1/v2 goldens hold). */
    record FeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId,
                              boolean journalEnabled, long meEpoch, long lastAppliedJournalSeq, boolean feedHalted,
                              FeedHaltReason haltReason)
            implements CoreEngineEvent {
        public FeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId, boolean journalEnabled) {
            this(correlationId, consumePosition, lastAppliedTradeId, journalEnabled, 0L, 0L, false, FeedHaltReason.NONE);
        }

        @Override
        public String render() {
            final String base = String.format("FEEDPOS corr=%d consumePos=%d lastTradeId=%d journal=%s",
                    correlationId, consumePosition, lastAppliedTradeId, journalEnabled ? "on" : "off");
            if (meEpoch == 0L && lastAppliedJournalSeq == 0L && !feedHalted && haltReason == FeedHaltReason.NONE) {
                return base;
            }
            return base + String.format(" epoch=%d jseq=%d halted=%s haltReason=%s", meEpoch, lastAppliedJournalSeq,
                    feedHalted ? "TRUE" : "FALSE", haltReason.name());
        }
    }

    record SettleFault(long tradeId, long orderId, long userId, int assetId,
                       long drawnFromAvailable, long uncovered) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("SETTLEFAULT trade=%d order=%d u=%d asset=%d fromAvail=%d uncovered=%d",
                    tradeId, orderId, userId, assetId, drawnFromAvailable, uncovered);
        }
    }

    record SettleRejected(long tradeId, int marketId, long journalPosition, SettleRejectReason reason, long detail)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("SETTLEREJECTED trade=%d market=%d pos=%d reason=%s detail=%d",
                    tradeId, marketId, journalPosition, reason.name(), detail);
        }
    }

    record OrderReject(long correlationId, long userId, long orderId, int marketId, int refTemplateId,
                       CoreRejectReason reason, long detail) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERREJECT corr=%d u=%d order=%d market=%d ref=%d reason=%s detail=%d",
                    correlationId, userId, orderId, marketId, refTemplateId, reason.name(), detail);
        }
    }

    record AdminAck(long correlationId, int refTemplateId, AdminResult result, int reason, long configVersion)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("ADMINACK corr=%d ref=%d result=%s reason=%d cfg=%d",
                    correlationId, refTemplateId, result.name(), reason, configVersion);
        }
    }

    /** {@code reasons=} renders only when the bitmask is not single-reason derivation. */
    record AdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                          boolean toOrigin, int gateReasons) implements CoreEngineEvent {
        public AdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                              boolean toOrigin) {
            this(marketId, gateState, gateReason, circuitBreaker, toOrigin, derivedReasons(gateState, gateReason));
        }

        static int derivedReasons(final GateState gateState, final GateReason gateReason) {
            return gateState == GateState.OPEN ? 0 : 1 << gateReason.code();
        }

        @Override
        public String render() {
            final String base = String.format("ADMISSION market=%d gate=%s reason=%s breaker=%s %s",
                    marketId, gateState.name(), gateReason.name(), circuitBreaker ? "on" : "off",
                    toOrigin ? "origin" : "broadcast");
            return gateReasons == derivedReasons(gateState, gateReason) ? base
                    : base + String.format(" reasons=0x%x", gateReasons);
        }
    }

    record OrderAck(long correlationId, long orderId, long userId, int marketId, OrderStatus status, boolean duplicate,
                    int holdAssetId, long holdAmount, long eventSeq) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERACK corr=%d order=%d u=%d market=%d status=%s dup=%s holdAsset=%d hold=%d seq=%d",
                    correlationId, orderId, userId, marketId, status.name(), duplicate ? "1" : "0", holdAssetId, holdAmount, eventSeq);
        }
    }

    /** {@code meReason=} renders only when it is not 255 (never under v1/v2). */
    record OrderEvent(long eventSeq, long orderId, long userId, int marketId, EventKind kind, OrderStatus status,
                      CoreRejectReason reason, long price, long quantity, long filledQty, long remainingQty, long lastPrice,
                      long lastQty, long tradeId, long meOrderId, int holdAssetId, long holdAmount, long holdRemaining,
                      int meReason)
            implements CoreEngineEvent {
        public OrderEvent(long eventSeq, long orderId, long userId, int marketId, EventKind kind, OrderStatus status,
                          CoreRejectReason reason, long price, long quantity, long filledQty, long remainingQty, long lastPrice,
                          long lastQty, long tradeId, long meOrderId, int holdAssetId, long holdAmount, long holdRemaining) {
            this(eventSeq, orderId, userId, marketId, kind, status, reason, price, quantity, filledQty, remainingQty, lastPrice,
                    lastQty, tradeId, meOrderId, holdAssetId, holdAmount, holdRemaining, 255);
        }

        @Override
        public String render() {
            return renderBase() + (meReason == 255 ? "" : " meReason=" + meReason);
        }

        private String renderBase() {
            return String.format("ORDEREVENT seq=%d order=%d u=%d market=%d kind=%s status=%s reason=%s px=%d qty=%d filled=%d remaining=%d lastPx=%d lastQty=%d trade=%d leg=%d holdAsset=%d hold=%d holdRemaining=%d",
                    eventSeq, orderId, userId, marketId, kind.name(), status == null ? "?" : status.name(), reason.name(), price, quantity,
                    filledQty, remainingQty, lastPrice, lastQty, tradeId, meOrderId, holdAssetId, holdAmount, holdRemaining);
        }
    }

    record MeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId, long omsOrderId,
                      long oldOrderId, int marketId, int orderType, int side, long price, long quantity, long budget, int timeInForce,
                      com.openexchange.core.domain.ReleaseScope releaseScope, long expectedOldRemaining)
            implements CoreEngineEvent {
        /** The shape (fields UP_TO, 0). */
        public MeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId, long omsOrderId,
                          long oldOrderId, int marketId, int orderType, int side, long price, long quantity, long budget, int timeInForce) {
            this(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId, oldOrderId, marketId, orderType, side, price,
                    quantity, budget, timeInForce, com.openexchange.core.domain.ReleaseScope.UP_TO, 0L);
        }

        @Override
        public String render() {
            return String.format("MEDISPATCH seq=%d id=%d/%d kind=%s u=%d order=%d old=%d market=%d type=%d side=%d px=%d qty=%d budget=%d tif=%d",
                    dispatchSeq, commandIdHigh, commandIdLow, kind.name(), userId, omsOrderId, oldOrderId, marketId, orderType, side,
                    price, quantity, budget, timeInForce) + s8Tail(kind, releaseScope, expectedOldRemaining);
        }
    }

    /** {@code scope=} on RELEASE_ID and {@code eor=} when non-zero; neither appears under v1/v2. */
    static String s8Tail(final DispatchKind kind, final com.openexchange.core.domain.ReleaseScope scope, final long eor) {
        final StringBuilder sb = new StringBuilder();
        if (kind == DispatchKind.RELEASE_ID) {
            sb.append(" scope=").append(scope == null ? "UP_TO" : scope.name());
        }
        if (eor != 0) {
            sb.append(" eor=").append(eor);
        }
        return sb.toString();
    }

    /** One release-backlog effect (node-local {@code onReleaseEffect}). */
    record ReleaseEffect(long orderId, long floor, com.openexchange.core.domain.ReleaseScope scope,
                         com.openexchange.core.domain.ReleaseResult result, int row) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("RELEASEFX order=%d floor=%d scope=%s result=%s row=%d", orderId, floor,
                    scope == null ? "-" : scope.name(), result == null ? "-" : result.name(), row);
        }
    }

    record DepositReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("DEPOSITREJECT corr=%d u=%d asset=%d amt=%d reason=%s", correlationId, userId, assetId, amount, reason.name());
        }
    }

    record ConfigApplied(ConfigKind kind, int marketId, long configVersion) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("CONFIGAPPLIED kind=%s market=%d version=%d", kind.name(), marketId, configVersion);
        }
    }

    // ---- ----

    record OrderFault(OrderFaultKind kind, long orderId, long userId, int marketId, long tradeId, long amount) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERFAULT kind=%s order=%d u=%d market=%d trade=%d amount=%d", kind.name(), orderId, userId, marketId, tradeId, amount);
        }
    }

    record OrderSnapshotEntry(long correlationId, long orderId, long userId, int marketId, OrderStatus status, long price, long quantity,
                              long filledQty, long remainingQty, int holdAssetId, long holdAmount, long meOrderId, int sliceSeq)
            implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERSNAP corr=%d order=%d u=%d market=%d status=%s px=%d qty=%d filled=%d remaining=%d holdAsset=%d hold=%d leg=%d attempt=%d",
                    correlationId, orderId, userId, marketId, status == null ? "?" : status.name(), price, quantity, filledQty, remainingQty,
                    holdAssetId, holdAmount, meOrderId, sliceSeq);
        }
    }

    record OrderSnapshotEnd(long correlationId, int entryCount, long nextOrderId) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERSNAPEND corr=%d n=%d next=%d", correlationId, entryCount, nextOrderId);
        }
    }

    record PendingDispatchEnd(long correlationId, int count, long headDispatchSeq) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("PENDINGEND corr=%d n=%d head=%d", correlationId, count, headDispatchSeq);
        }
    }

    record MeDispatchReply(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId, long omsOrderId,
                           long oldOrderId, int marketId, int orderType, int side, long price, long quantity, long budget, int timeInForce,
                           com.openexchange.core.domain.ReleaseScope releaseScope, long expectedOldRemaining)
            implements CoreEngineEvent {
        /** The shape (fields UP_TO, 0). */
        public MeDispatchReply(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId, long omsOrderId,
                               long oldOrderId, int marketId, int orderType, int side, long price, long quantity, long budget,
                               int timeInForce) {
            this(dispatchSeq, commandIdHigh, commandIdLow, kind, userId, omsOrderId, oldOrderId, marketId, orderType, side, price,
                    quantity, budget, timeInForce, com.openexchange.core.domain.ReleaseScope.UP_TO, 0L);
        }

        @Override
        public String render() {
            return String.format("DISPATCHREPLY seq=%d id=%d/%d kind=%s u=%d order=%d old=%d market=%d type=%d side=%d px=%d qty=%d budget=%d tif=%d",
                    dispatchSeq, commandIdHigh, commandIdLow, kind.name(), userId, omsOrderId, oldOrderId, marketId, orderType, side, price,
                    quantity, budget, timeInForce) + s8Tail(kind, releaseScope, expectedOldRemaining);
        }
    }

    // ---- (node-local sink methods, rendered by the recording sink only; never wire bytes) ----

    record FeedLatch(FeedHaltReason reason, long expected, long seen, long skip, long rawTradeId) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("FEEDLATCH reason=%s expected=%d seen=%d skip=%d rawTradeId=%d", reason.name(), expected, seen,
                    skip, rawTradeId);
        }
    }

    record FeedLatchCleared(FeedHaltReason reason, long journalSeq, boolean auto) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("FEEDCLEAR reason=%s jseq=%d auto=%d", reason.name(), journalSeq, auto ? 1 : 0);
        }
    }

    /** The node-local "auto-reopen" control event (the BRIDGE_HALT bit cleared by a GAP reach). */
    record GateAutoReopen(long journalSeq, boolean opened, int gateReasons, int laneHalts) implements CoreEngineEvent {
        @Override
        public String render() {
            return String.format("AUTOREOPEN jseq=%d opened=%d reasons=0x%x laneHalts=%d", journalSeq, opened ? 1 : 0, gateReasons,
                    laneHalts);
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
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

/**
 * An immutable Assets Engine output event captured by {@link RecordingAssetsSink}. Each renders to a
 * single canonical line for golden-master comparison; record equality also backs the run-twice /
 * wall-clock-invariance determinism checks. Values are raw fixed-point longs (implementation-agnostic;
 * no transport fields). Acks echo the request's {@code correlationId} (0 = fire-and-forget).
 */
public sealed interface AssetsEngineEvent
        permits AssetsEngineEvent.Balance, AssetsEngineEvent.DepositAck, AssetsEngineEvent.WithdrawAck,
                AssetsEngineEvent.HoldAck, AssetsEngineEvent.HoldReject, AssetsEngineEvent.WithdrawReject,
                AssetsEngineEvent.Settled, AssetsEngineEvent.BalanceSnapshotEnd,
                AssetsEngineEvent.HoldSnapshotEntry, AssetsEngineEvent.HoldSnapshotEnd,
                AssetsEngineEvent.FeedPositionReport, AssetsEngineEvent.SettleFault,
                AssetsEngineEvent.SettleRejected, AssetsEngineEvent.OrderReject, AssetsEngineEvent.AdminAck,
                AssetsEngineEvent.AdmissionState, AssetsEngineEvent.OrderAck, AssetsEngineEvent.OrderEvent,
                AssetsEngineEvent.MeDispatch, AssetsEngineEvent.DepositReject, AssetsEngineEvent.ConfigApplied,
        AssetsEngineEvent.OrderFault, AssetsEngineEvent.OrderSnapshotEntry, AssetsEngineEvent.OrderSnapshotEnd,
        AssetsEngineEvent.PendingDispatchEnd, AssetsEngineEvent.MeDispatchReply,
        AssetsEngineEvent.FeedLatch, AssetsEngineEvent.FeedLatchCleared {

    String render();

    record DepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("DEPOSITACK corr=%d u=%d asset=%d amt=%d newAvail=%d%s",
                    correlationId, userId, assetId, amount, newAvailable, duplicate ? " dup=1" : "");
        }
    }

    record WithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("WITHDRAWACK corr=%d u=%d asset=%d amt=%d newAvail=%d%s",
                    correlationId, userId, assetId, amount, newAvailable, duplicate ? " dup=1" : "");
        }
    }

    record Balance(long userId, int assetId, long available, long locked) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("BALANCE u=%d asset=%d avail=%d locked=%d", userId, assetId, available, locked);
        }
    }

    record HoldAck(long correlationId, long orderId, long userId, int assetId, long amount)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDACK corr=%d order=%d u=%d asset=%d amt=%d",
                    correlationId, orderId, userId, assetId, amount);
        }
    }

    record HoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDREJECT corr=%d order=%d u=%d asset=%d amt=%d reason=%s",
                    correlationId, orderId, userId, assetId, amount, reason.name());
        }
    }

    record WithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("WITHDRAWREJECT corr=%d u=%d asset=%d amt=%d reason=%s",
                    correlationId, userId, assetId, amount, reason.name());
        }
    }

    record Settled(long tradeId, long buyerUserId, long sellerUserId) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("SETTLED tradeId=%d buyer=%d seller=%d", tradeId, buyerUserId, sellerUserId);
        }
    }

    record BalanceSnapshotEnd(long correlationId, int entryCount) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("BALSNAPEND corr=%d count=%d", correlationId, entryCount);
        }
    }

    record HoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDSNAPENTRY order=%d u=%d asset=%d remaining=%d",
                    orderId, userId, assetId, remaining);
        }
    }

    record HoldSnapshotEnd(long correlationId, int entryCount) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDSNAPEND corr=%d count=%d", correlationId, entryCount);
        }
    }

    /** RC2 S7 3.6: the v7 tail renders only when it is not S1's {@code 0 0 FALSE NONE} (so the v1/v2 goldens hold). */
    record FeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId,
                              boolean journalEnabled, long meEpoch, long lastAppliedJournalSeq, boolean feedHalted,
                              FeedHaltReason haltReason)
            implements AssetsEngineEvent {
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
                       long drawnFromAvailable, long uncovered) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("SETTLEFAULT trade=%d order=%d u=%d asset=%d fromAvail=%d uncovered=%d",
                    tradeId, orderId, userId, assetId, drawnFromAvailable, uncovered);
        }
    }

    record SettleRejected(long tradeId, int marketId, long journalPosition, SettleRejectReason reason, long detail)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("SETTLEREJECTED trade=%d market=%d pos=%d reason=%s detail=%d",
                    tradeId, marketId, journalPosition, reason.name(), detail);
        }
    }

    record OrderReject(long correlationId, long userId, long orderId, int marketId, int refTemplateId,
                       CoreRejectReason reason, long detail) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERREJECT corr=%d u=%d order=%d market=%d ref=%d reason=%s detail=%d",
                    correlationId, userId, orderId, marketId, refTemplateId, reason.name(), detail);
        }
    }

    record AdminAck(long correlationId, int refTemplateId, AdminResult result, int reason, long configVersion)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("ADMINACK corr=%d ref=%d result=%s reason=%d cfg=%d",
                    correlationId, refTemplateId, result.name(), reason, configVersion);
        }
    }

    /** RC2 S7 6.2: {@code reasons=} renders only when the bitmask is not S1's single-reason derivation. */
    record AdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                          boolean toOrigin, int gateReasons) implements AssetsEngineEvent {
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
                    int holdAssetId, long holdAmount, long eventSeq) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERACK corr=%d order=%d u=%d market=%d status=%s dup=%s holdAsset=%d hold=%d seq=%d",
                    correlationId, orderId, userId, marketId, status.name(), duplicate ? "1" : "0", holdAssetId, holdAmount, eventSeq);
        }
    }

    /** RC2 S7 D9b: {@code meReason=} renders only when it is not 255 (never under v1/v2). */
    record OrderEvent(long eventSeq, long orderId, long userId, int marketId, EventKind kind, OrderStatus status,
                      CoreRejectReason reason, long price, long quantity, long filledQty, long remainingQty, long lastPrice,
                      long lastQty, long tradeId, long meOrderId, int holdAssetId, long holdAmount, long holdRemaining,
                      int meReason)
            implements AssetsEngineEvent {
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
                      long oldOrderId, int marketId, int orderType, int side, long price, long quantity, long budget, int timeInForce)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("MEDISPATCH seq=%d id=%d/%d kind=%s u=%d order=%d old=%d market=%d type=%d side=%d px=%d qty=%d budget=%d tif=%d",
                    dispatchSeq, commandIdHigh, commandIdLow, kind.name(), userId, omsOrderId, oldOrderId, marketId, orderType, side,
                    price, quantity, budget, timeInForce);
        }
    }

    record DepositReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("DEPOSITREJECT corr=%d u=%d asset=%d amt=%d reason=%s", correlationId, userId, assetId, amount, reason.name());
        }
    }

    record ConfigApplied(ConfigKind kind, int marketId, long configVersion) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("CONFIGAPPLIED kind=%s market=%d version=%d", kind.name(), marketId, configVersion);
        }
    }

    // ---- RC1 S5 ----

    record OrderFault(OrderFaultKind kind, long orderId, long userId, int marketId, long tradeId, long amount) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERFAULT kind=%s order=%d u=%d market=%d trade=%d amount=%d", kind.name(), orderId, userId, marketId, tradeId, amount);
        }
    }

    record OrderSnapshotEntry(long correlationId, long orderId, long userId, int marketId, OrderStatus status, long price, long quantity,
                              long filledQty, long remainingQty, int holdAssetId, long holdAmount, long meOrderId, int sliceSeq)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERSNAP corr=%d order=%d u=%d market=%d status=%s px=%d qty=%d filled=%d remaining=%d holdAsset=%d hold=%d leg=%d attempt=%d",
                    correlationId, orderId, userId, marketId, status == null ? "?" : status.name(), price, quantity, filledQty, remainingQty,
                    holdAssetId, holdAmount, meOrderId, sliceSeq);
        }
    }

    record OrderSnapshotEnd(long correlationId, int entryCount, long nextOrderId) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("ORDERSNAPEND corr=%d n=%d next=%d", correlationId, entryCount, nextOrderId);
        }
    }

    record PendingDispatchEnd(long correlationId, int count, long headDispatchSeq) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("PENDINGEND corr=%d n=%d head=%d", correlationId, count, headDispatchSeq);
        }
    }

    record MeDispatchReply(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId, long omsOrderId,
                           long oldOrderId, int marketId, int orderType, int side, long price, long quantity, long budget, int timeInForce)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("DISPATCHREPLY seq=%d id=%d/%d kind=%s u=%d order=%d old=%d market=%d type=%d side=%d px=%d qty=%d budget=%d tif=%d",
                    dispatchSeq, commandIdHigh, commandIdLow, kind.name(), userId, omsOrderId, oldOrderId, marketId, orderType, side, price,
                    quantity, budget, timeInForce);
        }
    }

    // ---- RC2 S7 (node-local sink methods, rendered by the recording sink only; never wire bytes) ----

    record FeedLatch(FeedHaltReason reason, long expected, long seen, long skip, long rawTradeId) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("FEEDLATCH reason=%s expected=%d seen=%d skip=%d rawTradeId=%d", reason.name(), expected, seen,
                    skip, rawTradeId);
        }
    }

    record FeedLatchCleared(FeedHaltReason reason, long journalSeq, boolean auto) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("FEEDCLEAR reason=%s jseq=%d auto=%d", reason.name(), journalSeq, auto ? 1 : 0);
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
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
                AssetsEngineEvent.MeDispatch, AssetsEngineEvent.DepositReject, AssetsEngineEvent.ConfigApplied {

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

    record FeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId,
                              boolean journalEnabled)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("FEEDPOS corr=%d consumePos=%d lastTradeId=%d journal=%s",
                    correlationId, consumePosition, lastAppliedTradeId, journalEnabled ? "on" : "off");
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

    record AdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                          boolean toOrigin) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("ADMISSION market=%d gate=%s reason=%s breaker=%s %s",
                    marketId, gateState.name(), gateReason.name(), circuitBreaker ? "on" : "off",
                    toOrigin ? "origin" : "broadcast");
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

    record OrderEvent(long eventSeq, long orderId, long userId, int marketId, EventKind kind, OrderStatus status,
                      CoreRejectReason reason, long price, long quantity, long filledQty, long remainingQty, long lastPrice,
                      long lastQty, long tradeId, long meOrderId, int holdAssetId, long holdAmount, long holdRemaining)
            implements AssetsEngineEvent {
        @Override
        public String render() {
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
}

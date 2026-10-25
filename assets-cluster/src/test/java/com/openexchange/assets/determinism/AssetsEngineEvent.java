// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.AdminResult;
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
                AssetsEngineEvent.AdmissionState {

    String render();

    record DepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("DEPOSITACK corr=%d u=%d asset=%d amt=%d newAvail=%d",
                    correlationId, userId, assetId, amount, newAvailable);
        }
    }

    record WithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("WITHDRAWACK corr=%d u=%d asset=%d amt=%d newAvail=%d",
                    correlationId, userId, assetId, amount, newAvailable);
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
}

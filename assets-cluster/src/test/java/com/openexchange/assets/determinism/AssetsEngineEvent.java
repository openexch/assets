// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.RejectReason;

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
                AssetsEngineEvent.FeedPositionReport, AssetsEngineEvent.SettleFault {

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

    record FeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("FEEDPOS corr=%d consumePos=%d lastTradeId=%d",
                    correlationId, consumePosition, lastAppliedTradeId);
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
}

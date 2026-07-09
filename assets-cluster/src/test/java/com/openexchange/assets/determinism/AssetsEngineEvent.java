// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.RejectReason;

/**
 * An immutable Assets Engine output event captured by {@link RecordingAssetsSink}. Each renders to a
 * single canonical line for golden-master comparison; record equality also backs the run-twice /
 * wall-clock-invariance determinism checks. Values are raw fixed-point longs (implementation-agnostic;
 * no transport fields).
 */
public sealed interface AssetsEngineEvent
        permits AssetsEngineEvent.Balance, AssetsEngineEvent.HoldAck, AssetsEngineEvent.HoldReject,
                AssetsEngineEvent.WithdrawReject, AssetsEngineEvent.Settled {

    String render();

    record Balance(long userId, int assetId, long available, long locked) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("BALANCE u=%d asset=%d avail=%d locked=%d", userId, assetId, available, locked);
        }
    }

    record HoldAck(long orderId, long userId, int assetId, long amount) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDACK order=%d u=%d asset=%d amt=%d", orderId, userId, assetId, amount);
        }
    }

    record HoldReject(long orderId, long userId, int assetId, long amount, RejectReason reason)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("HOLDREJECT order=%d u=%d asset=%d amt=%d reason=%s",
                    orderId, userId, assetId, amount, reason.name());
        }
    }

    record WithdrawReject(long userId, int assetId, long amount, RejectReason reason)
            implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("WITHDRAWREJECT u=%d asset=%d amt=%d reason=%s",
                    userId, assetId, amount, reason.name());
        }
    }

    record Settled(long tradeId, long buyerUserId, long sellerUserId) implements AssetsEngineEvent {
        @Override
        public String render() {
            return String.format("SETTLED tradeId=%d buyer=%d seller=%d", tradeId, buyerUserId, sellerUserId);
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.proto;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.ProtoOrderCancelCommand;
import com.openexchange.assets.domain.commands.ProtoOrderCreateCommand;
import com.openexchange.assets.domain.commands.ProtoOrderExpireCommand;
import com.openexchange.assets.domain.commands.ProtoOrderFillCommand;

import java.util.ArrayList;
import java.util.List;

/** Test scaffolding for the prototype order engine: a recording sink and command helpers. */
final class ProtoHarness {
    static final int BTC_USD = 1;
    static final long USD = Asset.USD.id();
    static final long BTC = Asset.BTC.id();

    /** {corr, orderId, userId, market, status, reason, filled, remaining, lastPrice, lastQty, hold, ts, seq}. */
    final List<long[]> events = new ArrayList<>();
    int balanceUpdates;
    int settlements;
    int settleFaults;
    final AssetsEngine engine = new AssetsEngine();
    final List<long[]> timers = new ArrayList<>();

    private final DepositCommand deposit = new DepositCommand();
    private final ProtoOrderCreateCommand create = new ProtoOrderCreateCommand();
    private final ProtoOrderCancelCommand cancel = new ProtoOrderCancelCommand();
    private final ProtoOrderFillCommand fill = new ProtoOrderFillCommand();
    private final ProtoOrderExpireCommand expire = new ProtoOrderExpireCommand();

    ProtoHarness() {
        engine.setEventSink(new AssetsEventSink() {
            public void onDepositAck(long c, long u, int a, long amt, long n) { }
            public void onWithdrawAck(long c, long u, int a, long amt, long n) { }
            public void onHoldAck(long c, long o, long u, int a, long amt) { }
            public void onHoldReject(long c, long o, long u, int a, long amt, RejectReason r) { }
            public void onBalanceUpdate(long u, int a, long av, long lk) { balanceUpdates++; }
            public void onSettlementApplied(long t, long b, long s) { settlements++; }
            public void onWithdrawReject(long c, long u, int a, long amt, RejectReason r) { }
            public void onBalanceSnapshotEnd(long c, int n) { }
            public void onHoldSnapshotEntry(long o, long u, int a, long r) { }
            public void onHoldSnapshotEnd(long c, int n) { }
            public void onFeedPositionReport(long c, long p, long t, boolean j) { }
            public void onSettleFault(long t, long o, long u, int a, long d, long un) { settleFaults++; }
            @Override
            public void onProtoOrderEvent(long correlationId, long orderId, long userId, int marketId, int status,
                                          int reason, long filledQty, long remainingQty, long lastPrice, long lastQty,
                                          long holdAmount, long updatedAtMs, long eventSeq) {
                events.add(new long[] {correlationId, orderId, userId, marketId, status, reason, filledQty, remainingQty,
                        lastPrice, lastQty, holdAmount, updatedAtMs, eventSeq});
            }
        });
        engine.proto().setTimerPort((orderId, deadlineMs) -> timers.add(new long[] {orderId, deadlineMs}));
    }

    void deposit(long user, long asset, double amount) {
        deposit.reset();
        deposit.setCorrelationId(0L);
        deposit.setUserId(user);
        deposit.setAssetId((int) asset);
        deposit.setAmount(FixedPoint.fromDouble(amount));
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, deposit, 1L);
    }

    /** @return the event emitted for this create (NEW or REJECTED) */
    long[] create(long corr, long user, int side, double price, double qty, long clientOrderId, int tif, long expiresAtMs, long ts) {
        create.reset();
        create.correlationId = corr;
        create.userId = user;
        create.clientOrderIdLow = clientOrderId;
        create.clientOrderIdHigh = clientOrderId == 0 ? 0 : 0x5A5AL;
        create.requestIdLow = corr;
        create.marketId = BTC_USD;
        create.side = side;
        create.orderType = 0;
        create.timeInForce = tif;
        create.price = price <= 0 ? 0 : FixedPoint.fromDouble(price);
        create.quantity = FixedPoint.fromDouble(qty);
        create.expiresAtMs = expiresAtMs;
        final int before = events.size();
        engine.applyCommand(AssetsEngine.CMD_PROTO_ORDER_CREATE, create, ts);
        return events.get(before);
    }

    long[] limit(long corr, long user, int side, double price, double qty, long ts) {
        return create(corr, user, side, price, qty, 0L, 0, 0L, ts);
    }

    long[] cancel(long corr, long user, long orderId, long ts) {
        cancel.reset();
        cancel.correlationId = corr;
        cancel.userId = user;
        cancel.orderId = orderId;
        final int before = events.size();
        engine.applyCommand(AssetsEngine.CMD_PROTO_ORDER_CANCEL, cancel, ts);
        return events.get(before);
    }

    void fill(long tradeId, long buyOrderId, long sellOrderId, double price, double qty, long ts) {
        fill.reset();
        fill.tradeId = tradeId;
        fill.marketId = BTC_USD;
        fill.buyOrderId = buyOrderId;
        fill.sellOrderId = sellOrderId;
        fill.price = FixedPoint.fromDouble(price);
        fill.quantity = FixedPoint.fromDouble(qty);
        fill.takerIsBuy = true;
        engine.applyCommand(AssetsEngine.CMD_PROTO_ORDER_FILL, fill, ts);
    }

    void expire(long orderId, long ts) {
        expire.reset();
        expire.orderId = orderId;
        engine.applyCommand(AssetsEngine.CMD_PROTO_ORDER_EXPIRE, expire, ts);
    }

    long available(long user, long asset) { return engine.account(user).available((int) asset); }
    long locked(long user, long asset) { return engine.account(user).locked((int) asset); }

    /** Σ(available + locked) across every account for one asset: must be invariant under holds, cancels and fills. */
    long total(long asset) {
        final long[] sum = {0L};
        engine.forEachAccount(a -> sum[0] += a.available((int) asset) + a.locked((int) asset));
        return sum[0];
    }

    /** locked == Σ hold remaining, per account and asset. */
    boolean lockedMatchesHolds() {
        final boolean[] ok = {true};
        engine.forEachAccount(a -> {
            final long[] byAsset = new long[Asset.count()];
            a.forEachHold((orderId, assetId, remaining, oms) -> byAsset[assetId] += remaining);
            for (int i = 0; i < byAsset.length; i++) {
                if (a.locked(i) != byAsset[i]) {
                    ok[0] = false;
                }
            }
        });
        return ok[0];
    }

    static long fp(double v) { return FixedPoint.fromDouble(v); }
}

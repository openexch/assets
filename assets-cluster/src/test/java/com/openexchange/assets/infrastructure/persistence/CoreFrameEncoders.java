// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.infrastructure.generated.AmendOrderEncoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.BreakerAction;
import com.openexchange.assets.infrastructure.generated.CancelOrderEncoder;
import com.openexchange.assets.infrastructure.generated.CircuitBreakerEncoder;
import com.openexchange.assets.infrastructure.generated.CoreConfigEncoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.GateReason;
import com.openexchange.assets.infrastructure.generated.GateState;
import com.openexchange.assets.infrastructure.generated.MarketTickEncoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateEncoder;
import com.openexchange.assets.infrastructure.generated.OrderStatus;
import com.openexchange.assets.infrastructure.generated.OrderType;
import com.openexchange.assets.infrastructure.generated.QuarantineOrderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.RequestOrderSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.RequestPendingDispatchEncoder;
import com.openexchange.assets.infrastructure.generated.RiskConfigSetEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.Side;
import com.openexchange.assets.infrastructure.generated.TimeInForce;
import com.openexchange.assets.infrastructure.generated.WithdrawEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.List;

/**
 * RC1 S8: every ingress frame of the frame-level harness, encoded ONCE through the generated production encoders
 * (money-schema v6 header: schemaId 2, version 6). Each call starts from a zeroed buffer, so a field the caller
 * does not set is 0 on the wire and no stale byte of an earlier frame can leak into this one.
 */
final class CoreFrameEncoders {

    static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    /** A trade as the bridge journals it; the fields of MeJournalTrade (61) and of one MeJournalTradeBatch entry. */
    record Trade(long egressSeq, long tradeId, int marketId, long takerLeg, long takerUser, long makerLeg, long makerUser,
                 long price, long quantity, boolean takerIsBuy, long meTimestamp, long takerOms, long makerOms) { }

    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    private void zero() {
        buf.setMemory(0, buf.capacity(), (byte) 0);
    }

    private byte[] out(final int bodyLength) {
        final byte[] b = new byte[HEADER + bodyLength];
        buf.getBytes(0, b);
        return b;
    }

    byte[] deposit(long corr, long user, int asset, long amount, long rh, long rl) {
        zero();
        return out(new DepositEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).assetId(asset)
                .amount(amount).requestIdHigh(rh).requestIdLow(rl).encodedLength());
    }

    byte[] withdraw(long corr, long user, int asset, long amount, long rh, long rl) {
        zero();
        return out(new WithdrawEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).assetId(asset)
                .amount(amount).requestIdHigh(rh).requestIdLow(rl).encodedLength());
    }

    byte[] create(long corr, long user, long cidLow, long rh, long rl, int market, int side, int type, int tif, long price,
                  long qty, long budget, long stop, long delta, long display, long expiresAt) {
        zero();
        final CreateOrderEncoder e = new CreateOrderEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user)
                .clientOrderIdHigh(0).clientOrderIdLow(cidLow).requestIdHigh(rh).requestIdLow(rl).marketId(market)
                .side(Side.get((short) side)).orderType(OrderType.get((short) type)).timeInForce(TimeInForce.get((short) tif))
                .price(price).quantity(qty).budget(budget).stopPrice(stop).trailingDelta(delta).displayQty(display)
                .expiresAtMs(expiresAt);
        return out(e.encodedLength());
    }

    byte[] cancel(long corr, long user, long orderId, long rh, long rl) {
        zero();
        return out(new CancelOrderEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).orderId(orderId)
                .requestIdHigh(rh).requestIdLow(rl).origClientOrderIdHigh(0).origClientOrderIdLow(0).encodedLength());
    }

    byte[] amend(long corr, long user, long orderId, long rh, long rl, long newPrice, long newQty) {
        zero();
        return out(new AmendOrderEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).orderId(orderId)
                .requestIdHigh(rh).requestIdLow(rl).newPrice(newPrice).newQuantity(newQty).encodedLength());
    }

    byte[] orderSnapshot(long corr, long user, long fromOrderId) {
        zero();
        return out(new RequestOrderSnapshotEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user)
                .fromOrderId(fromOrderId).encodedLength());
    }

    byte[] pendingDispatch(long corr, long fromSeq, int max) {
        zero();
        return out(new RequestPendingDispatchEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .fromDispatchSeq(fromSeq).maxEntries(max).encodedLength());
    }

    byte[] tick(int market, long version, long bid, long ask, long last, long lastTradeId) {
        zero();
        return out(new MarketTickEncoder().wrapAndApplyHeader(buf, 0, header).marketId(market).bookVersion(version).bestBid(bid)
                .bestAsk(ask).lastTradePrice(last).lastTradeId(lastTradeId).meEpoch(1).encodedLength());
    }

    byte[] trade(final Trade t) {
        zero();
        return out(new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(t.egressSeq()).tradeId(t.tradeId())
                .marketId(t.marketId()).takerOrderId(t.takerLeg()).takerUserId(t.takerUser()).makerOrderId(t.makerLeg())
                .makerUserId(t.makerUser()).price(t.price()).quantity(t.quantity())
                .takerIsBuy(t.takerIsBuy() ? BoolFlag.TRUE : BoolFlag.FALSE).meTimestamp(t.meTimestamp())
                .takerOmsOrderId(t.takerOms()).makerOmsOrderId(t.makerOms()).meEpoch(1).encodedLength());
    }

    byte[] tradeBatch(final List<Trade> trades) {
        zero();
        final MeJournalTradeBatchEncoder e = new MeJournalTradeBatchEncoder().wrapAndApplyHeader(buf, 0, header);
        final MeJournalTradeBatchEncoder.TradesEncoder g = e.tradesCount(trades.size());
        for (final Trade t : trades) {
            g.next().egressSeq(t.egressSeq()).tradeId(t.tradeId()).marketId(t.marketId()).takerOrderId(t.takerLeg())
                    .takerUserId(t.takerUser()).makerOrderId(t.makerLeg()).makerUserId(t.makerUser()).price(t.price())
                    .quantity(t.quantity()).takerIsBuy(t.takerIsBuy() ? BoolFlag.TRUE : BoolFlag.FALSE)
                    .meTimestamp(t.meTimestamp()).takerOmsOrderId(t.takerOms()).makerOmsOrderId(t.makerOms()).meEpoch(1);
        }
        return out(e.encodedLength());
    }

    byte[] terminal(long egress, long leg, long user, int market, int status, long meTs, long oms) {
        zero();
        return out(new MeJournalTerminalEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress).orderId(leg).userId(user)
                .marketId(market).status(OrderStatus.get((short) status)).meTimestamp(meTs).omsOrderId(oms).meEpoch(1)
                .encodedLength());
    }

    byte[] outcome(long egress, long cmdHigh, long cmdLow, long user, long oms, long old, long price, long qty, long budget,
                   int market, int kind, int type, int side, long meTs, long leg, int reason, int result) {
        zero();
        return out(new MeCommandOutcomeEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress).commandIdHigh(cmdHigh)
                .commandIdLow(cmdLow).userId(user).omsOrderId(oms).oldOrderId(old).price(price).quantity(qty).budget(budget)
                .marketId(market).commandKind((short) kind).orderType((short) type).orderSide((short) side)
                .appliedPosition(egress).meTimestamp(meTs).orderId(leg).status(0).reason(reason).oldCancelled((short) 0)
                .result(result).meEpoch(1).encodedLength());
    }

    /** The compiled Market.ALL / Asset tables as a CoreConfig frame (the only config the S7 cross-check accepts). */
    byte[] coreConfig(long corr, long version, long behavior, long tick, long min, long max, long lot) {
        zero();
        final CoreConfigEncoder e = new CoreConfigEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .configVersion(version).behaviorVersion(behavior);
        final CoreConfigEncoder.MarketsEncoder m = e.marketsCount(Market.ALL.length);
        for (final Market d : Market.ALL) {
            m.next().marketId(d.marketId()).symbol(d.symbol()).baseAssetId(d.baseAsset().id()).quoteAssetId(d.quoteAsset().id())
                    .tickSize(tick).minPrice(min).maxPrice(max).lotSize(lot);
        }
        final CoreConfigEncoder.AssetsEncoder a = e.assetsCount(Asset.values().length);
        for (final Asset d : Asset.values()) {
            a.next().assetId(d.id()).code(d.name()).scale((short) 8);
        }
        return out(e.encodedLength());
    }

    byte[] riskConfig(long corr, long version, int market, int perSec, int maxOpen, int collarBps) {
        zero();
        return out(new RiskConfigSetEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).configVersion(version)
                .marketId(market).minQuantity(1).maxQuantity(1_000_000L * 100_000_000L).minNotional(0)
                .maxNotional(Long.MAX_VALUE).priceCollarBps(collarBps).maxOrdersPerSec(perSec).maxOrdersPerMin(perSec * 30)
                .maxOpenOrders(maxOpen).maxPositionPerMarket(Long.MAX_VALUE).marketBuySlippageBps(500).encodedLength());
    }

    byte[] breaker(long corr, int market, boolean trip) {
        zero();
        return out(new CircuitBreakerEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).marketId(market)
                .action(trip ? BreakerAction.TRIP : BreakerAction.RESET).reason((short) 0).encodedLength());
    }

    byte[] gate(long corr, int market, boolean open) {
        zero();
        return out(new OrderEntryGateEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).marketId(market)
                .state(open ? GateState.OPEN : GateState.SUSPENDED).reason(GateReason.ADMIN).encodedLength());
    }

    byte[] quarantine(long corr, long orderId) {
        zero();
        return out(new QuarantineOrderEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).orderId(orderId)
                .encodedLength());
    }

    byte[] register(long corr, SessionClass cls) {
        zero();
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .sessionClass(cls).readerKind((short) 0).resumable(BoolFlag.TRUE).identity(0).sourceEpoch(0);
        e.channels().clear().acks(true).orders(true).dispatch(true).control(true);
        return out(e.encodedLength());
    }
}

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
import com.openexchange.assets.infrastructure.generated.FeedHaltEncoder;
import com.openexchange.assets.infrastructure.generated.FeedHaltReason;
import com.openexchange.assets.infrastructure.generated.FeedLatchClearEncoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.assets.infrastructure.generated.ReleaseResult;
import com.openexchange.assets.infrastructure.generated.ReleaseScope;
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
                 long price, long quantity, boolean takerIsBuy, long meTimestamp, long takerOms, long makerOms, long journalSeq) {
        Trade(long egressSeq, long tradeId, int marketId, long takerLeg, long takerUser, long makerLeg, long makerUser,
              long price, long quantity, boolean takerIsBuy, long meTimestamp, long takerOms, long makerOms) {
            this(egressSeq, tradeId, marketId, takerLeg, takerUser, makerLeg, makerUser, price, quantity, takerIsBuy, meTimestamp,
                    takerOms, makerOms, 0L);
        }
    }

    /** RC2 S7: the producer epoch every ME-sourced frame carries (1 in the v1/v2 story; the bound genesisId at v3). */
    long meEpoch = 1L;
    /** RC2 S7: the genesisId every CoreConfig frame carries (0 in the v1/v2 story). */
    long genesisId = 0L;

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
                .bestAsk(ask).lastTradePrice(last).lastTradeId(lastTradeId).meEpoch(meEpoch).encodedLength());
    }

    byte[] trade(final Trade t) {
        zero();
        return out(new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(t.egressSeq()).tradeId(t.tradeId())
                .marketId(t.marketId()).takerOrderId(t.takerLeg()).takerUserId(t.takerUser()).makerOrderId(t.makerLeg())
                .makerUserId(t.makerUser()).price(t.price()).quantity(t.quantity())
                .takerIsBuy(t.takerIsBuy() ? BoolFlag.TRUE : BoolFlag.FALSE).meTimestamp(t.meTimestamp())
                .takerOmsOrderId(t.takerOms()).makerOmsOrderId(t.makerOms()).meEpoch(meEpoch).journalSeq(t.journalSeq())
                .encodedLength());
    }

    byte[] tradeBatch(final List<Trade> trades) {
        zero();
        final MeJournalTradeBatchEncoder e = new MeJournalTradeBatchEncoder().wrapAndApplyHeader(buf, 0, header);
        final MeJournalTradeBatchEncoder.TradesEncoder g = e.tradesCount(trades.size());
        for (final Trade t : trades) {
            g.next().egressSeq(t.egressSeq()).tradeId(t.tradeId()).marketId(t.marketId()).takerOrderId(t.takerLeg())
                    .takerUserId(t.takerUser()).makerOrderId(t.makerLeg()).makerUserId(t.makerUser()).price(t.price())
                    .quantity(t.quantity()).takerIsBuy(t.takerIsBuy() ? BoolFlag.TRUE : BoolFlag.FALSE)
                    .meTimestamp(t.meTimestamp()).takerOmsOrderId(t.takerOms()).makerOmsOrderId(t.makerOms()).meEpoch(meEpoch)
                    .journalSeq(t.journalSeq());
        }
        return out(e.encodedLength());
    }

    byte[] terminal(long egress, long leg, long user, int market, int status, long meTs, long oms) {
        return terminal(egress, leg, user, market, status, meTs, oms, 0L);
    }

    byte[] terminal(long egress, long leg, long user, int market, int status, long meTs, long oms, long journalSeq) {
        zero();
        return out(new MeJournalTerminalEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress).orderId(leg).userId(user)
                .marketId(market).status(OrderStatus.get((short) status)).meTimestamp(meTs).omsOrderId(oms).meEpoch(meEpoch)
                .journalSeq(journalSeq)
                .encodedLength());
    }

    byte[] outcome(long egress, long cmdHigh, long cmdLow, long user, long oms, long old, long price, long qty, long budget,
                   int market, int kind, int type, int side, long meTs, long leg, int reason, int result) {
        return outcome(egress, cmdHigh, cmdLow, user, oms, old, price, qty, budget, market, kind, type, side, meTs, leg, reason,
                result, 0L);
    }

    byte[] outcome(long egress, long cmdHigh, long cmdLow, long user, long oms, long old, long price, long qty, long budget,
                   int market, int kind, int type, int side, long meTs, long leg, int reason, int result, long journalSeq) {
        zero();
        // RC2 S8 section 8 (FIX-7): the shape a correct ME journals: status 0 / 4 / -1 by result and leg 0 above result 1
        // (3.9), the ME type of the command (0..2), ledgerCapacity and behaviorVersion as the bound FRESH, ledgerSize equal to
        // the capacity on CAPACITY (S4 3.3) and 0 otherwise
        final int status = result == 0 ? 0 : result == 1 ? 4 : -1;
        final int meType = type == 3 || type == 5 ? 1 : type == 4 || type == 6 ? 0 : type;
        final int capacity = (int) com.openexchange.assets.determinism.V3Genesis.ME_CAPACITY;
        return out(new MeCommandOutcomeEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress).commandIdHigh(cmdHigh)
                .commandIdLow(cmdLow).userId(user).omsOrderId(oms).oldOrderId(old).price(price).quantity(qty).budget(budget)
                .marketId(market).commandKind((short) kind).orderType((short) meType).orderSide((short) side)
                .appliedPosition(egress).meTimestamp(meTs).orderId(result <= 1 ? leg : 0L).status(status).reason(reason)
                .oldCancelled((short) 0).result(result).meEpoch(meEpoch).journalSeq(journalSeq).timeInForce((short) 0)
                .expectedOldRemaining(0L).ledgerSize(result == 3 ? capacity : 0).ledgerCapacity(capacity)
                .behaviorVersion(com.openexchange.assets.determinism.V3Genesis.ME_BEHAVIOR).encodedLength());
    }

    /** The compiled Market.ALL / Asset tables as a CoreConfig frame (the only config the S7 cross-check accepts). */
    byte[] coreConfig(long corr, long version, long behavior, long tick, long min, long max, long lot) {
        zero();
        final CoreConfigEncoder e = new CoreConfigEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .configVersion(version).behaviorVersion(behavior).genesisId(genesisId);
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

    // ---- RC2 S7 (16.4 fuzz corpus rows): the v7 feed templates 66, 67, 68 and 76 ----

    byte[] feedHalt(long corr, int reason, long meEpochSeen, long expected, long seen) {
        zero();
        return out(new FeedHaltEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .reason(FeedHaltReason.get((short) reason)).meEpochSeen(meEpochSeen).expectedJournalSeq(expected)
                .seenJournalSeq(seen).bridgeIdentity(0x77).encodedLength());
    }

    /** One release entry (the corpus row S1 review FIX-1 asked for: groupCount(67) = 1). */
    byte[] meCommandRelease(long egress, long journalSeq, long meEpoch, long commandIdHigh) {
        zero();
        final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress)
                .journalSeq(journalSeq).meEpoch(meEpoch).releaseSeq(1).ledgerSize(0)
                .ledgerCapacity((int) com.openexchange.assets.determinism.V3Genesis.ME_CAPACITY).tombstoneRanges(0);
        e.releasesCount(1).next().commandIdHigh(commandIdHigh).idLowFloor(0).scope(ReleaseScope.UP_TO).result(ReleaseResult.APPLIED)
                .removed(1);
        return out(e.encodedLength());
    }

    /** RC2 S8: one 67 entry of any scope and result (ledgerSize as given, the bound capacity). */
    byte[] release(long egress, long journalSeq, long meEpoch, long commandIdHigh, long floor, int scope, int result, int ledgerSize) {
        zero();
        final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress)
                .journalSeq(journalSeq).meEpoch(meEpoch).releaseSeq(journalSeq).ledgerSize(ledgerSize)
                .ledgerCapacity((int) com.openexchange.assets.determinism.V3Genesis.ME_CAPACITY).tombstoneRanges(0);
        e.releasesCount(1).next().commandIdHigh(commandIdHigh).idLowFloor(floor).scope(ReleaseScope.get((short) scope))
                .result(ReleaseResult.get((short) result)).removed(result == 0 ? 1 : 0);
        return out(e.encodedLength());
    }

    byte[] meEngineConfig(long egress, long journalSeq, long meEpoch, long configVersion, int decision) {
        zero();
        return out(new MeEngineConfigEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress).journalSeq(journalSeq)
                .meEpoch(meEpoch).configVersion(configVersion).behaviorVersion(2).ledgerCapacity(4096).maxWireVersion(12)
                .decision(com.openexchange.assets.infrastructure.generated.ConfigDecision.get((short) decision)).encodedLength());
    }

    byte[] feedLatchClear(long corr, int haltReason, long journalSeq) {
        zero();
        return out(new FeedLatchClearEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .haltReason(FeedHaltReason.get((short) haltReason)).journalSeq(journalSeq).encodedLength());
    }
}

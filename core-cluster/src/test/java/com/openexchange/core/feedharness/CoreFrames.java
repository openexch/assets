// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.feedharness;

import com.openexchange.core.domain.Asset;
import com.openexchange.core.domain.Market;
import com.openexchange.core.infrastructure.generated.AdminAckEncoder;
import com.openexchange.core.infrastructure.generated.AmendOrderEncoder;
import com.openexchange.core.infrastructure.generated.BoolFlag;
import com.openexchange.core.infrastructure.generated.CancelOrderEncoder;
import com.openexchange.core.infrastructure.generated.CoreConfigEncoder;
import com.openexchange.core.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.core.infrastructure.generated.DepositEncoder;
import com.openexchange.core.infrastructure.generated.FeedHaltReason;
import com.openexchange.core.infrastructure.generated.FeedLatchClearEncoder;
import com.openexchange.core.infrastructure.generated.FeedPositionReportEncoder;
import com.openexchange.core.infrastructure.generated.GateReason;
import com.openexchange.core.infrastructure.generated.GateState;
import com.openexchange.core.infrastructure.generated.MarketTickEncoder;
import com.openexchange.core.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.core.infrastructure.generated.MeCommandReleaseEncoder;
import com.openexchange.core.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.core.infrastructure.generated.MeJournalTerminalEncoder;
import com.openexchange.core.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.OrderEntryGateEncoder;
import com.openexchange.core.infrastructure.generated.OrderType;
import com.openexchange.core.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.core.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.core.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.core.infrastructure.generated.RequestHoldSnapshotEncoder;
import com.openexchange.core.infrastructure.generated.RequestOrderSnapshotEncoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.SettleRejectedEncoder;
import com.openexchange.core.infrastructure.generated.Side;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.generated.WithdrawEncoder;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The money v7 frames the harness sends to (and decodes from) the core, encoded ONCE through the generated
 * production encoders from a zeroed buffer (a field the caller does not set is 0). Public so the story builder, the
 * runner, the T1 tools and the tests share one encoder set. The feed frames here are the CORE side of the feed (what the
 * fake ME's answer looks like after translation); the journal side is {@link MeJournalReverseEncoder}.
 */
public final class CoreFrames {

    public static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[16 * 1024]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    private void zero() {
        buf.setMemory(0, buf.capacity(), (byte) 0);
    }

    private byte[] out(final int bodyLength) {
        final byte[] b = new byte[HEADER + bodyLength];
        buf.getBytes(0, b);
        return b;
    }

    // ------------------------------------------------------------------ core block (ingress)

    public byte[] coreConfig(long corr, long version, long behavior, long genesisId) {
        zero();
        final CoreConfigEncoder e = new CoreConfigEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .configVersion(version).behaviorVersion(behavior).genesisId(genesisId);
        final CoreConfigEncoder.MarketsEncoder m = e.marketsCount(Market.ALL.length);
        for (final Market d : Market.ALL) {
            m.next().marketId(d.marketId()).symbol(d.symbol()).baseAssetId(d.baseAsset().id()).quoteAssetId(d.quoteAsset().id())
                    .tickSize(1).minPrice(1).maxPrice(1_000_000_000_000_000L).lotSize(0);
        }
        final CoreConfigEncoder.AssetsEncoder a = e.assetsCount(Asset.values().length);
        for (final Asset d : Asset.values()) {
            a.next().assetId(d.id()).code(d.name()).scale((short) 8);
        }
        return out(e.encodedLength());
    }

    public byte[] deposit(long corr, long user, int asset, long amount, long rh, long rl) {
        zero();
        return out(new DepositEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).assetId(asset)
                .amount(amount).requestIdHigh(rh).requestIdLow(rl).encodedLength());
    }

    public byte[] withdraw(long corr, long user, int asset, long amount, long rh, long rl) {
        zero();
        return out(new WithdrawEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).assetId(asset)
                .amount(amount).requestIdHigh(rh).requestIdLow(rl).encodedLength());
    }

    public byte[] create(long corr, long user, long rh, long rl, int market, int side, int type, int tif, long price, long qty,
                         long budget, long stop, long delta, long display, long expiresAt) {
        zero();
        final CreateOrderEncoder e = new CreateOrderEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user)
                .clientOrderIdHigh(0).clientOrderIdLow(0).requestIdHigh(rh).requestIdLow(rl).marketId(market)
                .side(Side.get((short) side)).orderType(OrderType.get((short) type)).timeInForce(TimeInForce.get((short) tif))
                .price(price).quantity(qty).budget(budget).stopPrice(stop).trailingDelta(delta).displayQty(display)
                .expiresAtMs(expiresAt);
        return out(e.encodedLength());
    }

    public byte[] cancel(long corr, long user, long orderId, long rh, long rl) {
        zero();
        return out(new CancelOrderEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).orderId(orderId)
                .requestIdHigh(rh).requestIdLow(rl).origClientOrderIdHigh(0).origClientOrderIdLow(0).encodedLength());
    }

    public byte[] amend(long corr, long user, long orderId, long rh, long rl, long newPrice, long newQty) {
        zero();
        return out(new AmendOrderEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).orderId(orderId)
                .requestIdHigh(rh).requestIdLow(rl).newPrice(newPrice).newQuantity(newQty).encodedLength());
    }

    public byte[] gate(long corr, int market, boolean open) {
        return gate(corr, market, open, GateReason.ADMIN);
    }

    /** An OPEN of GLOBAL after a halt that needed the operator names BRIDGE_HALT. */
    public byte[] gate(long corr, int market, boolean open, GateReason reason) {
        zero();
        return out(new OrderEntryGateEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).marketId(market)
                .state(open ? GateState.OPEN : GateState.SUSPENDED).reason(reason).encodedLength());
    }

    public byte[] tick(int market, long version, long bid, long ask, long last, long meEpoch) {
        zero();
        return out(new MarketTickEncoder().wrapAndApplyHeader(buf, 0, header).marketId(market).bookVersion(version).bestBid(bid)
                .bestAsk(ask).lastTradePrice(last).lastTradeId(0).meEpoch(meEpoch).encodedLength());
    }

    /** RegisterSession; {@code bridge} picks the BRIDGE channel set {acks, settlements}, otherwise the consumer set. */
    public byte[] register(long corr, SessionClass cls, long identity, long sourceEpoch) {
        zero();
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .sessionClass(cls).readerKind((short) 0).resumable(BoolFlag.TRUE).identity(identity).sourceEpoch(sourceEpoch);
        if (cls == SessionClass.BRIDGE) {
            e.channels().clear().acks(true).settlements(true);
        } else {
            e.channels().clear().acks(true).orders(true).dispatch(true).control(true);
        }
        return out(e.encodedLength());
    }

    public byte[] query(long corr) {
        zero();
        return out(new QueryFeedPositionEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).encodedLength());
    }

    public byte[] balanceSnapshot(long corr) {
        zero();
        return out(new RequestBalanceSnapshotEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).encodedLength());
    }

    public byte[] holdSnapshot(long corr) {
        zero();
        return out(new RequestHoldSnapshotEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).encodedLength());
    }

    public byte[] orderSnapshot(long corr, long user, long fromOrderId) {
        zero();
        return out(new RequestOrderSnapshotEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user)
                .fromOrderId(fromOrderId).encodedLength());
    }

    /** A FeedHalt 66 (BRIDGE). */
    public byte[] feedHalt(long corr, int haltReason, long meEpochSeen, long expected, long seen, long bridgeIdentity) {
        zero();
        return out(new com.openexchange.core.infrastructure.generated.FeedHaltEncoder().wrapAndApplyHeader(buf, 0, header)
                .correlationId(corr).reason(FeedHaltReason.get((short) haltReason)).meEpochSeen(meEpochSeen)
                .expectedJournalSeq(expected).seenJournalSeq(seen).bridgeIdentity(bridgeIdentity).encodedLength());
    }

    public byte[] feedLatchClear(long corr, int haltReason, long journalSeq) {
        zero();
        return out(new FeedLatchClearEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .haltReason(FeedHaltReason.get((short) haltReason)).journalSeq(journalSeq).encodedLength());
    }

    // ------------------------------------------------------------------ feed (core side)

    public byte[] trade(long egress, long journalSeq, long meEpoch, long tradeId, int market, long takerLeg, long takerUser,
                        long makerLeg, long makerUser, long price, long qty, boolean takerIsBuy, long meTs, long takerOms,
                        long makerOms) {
        zero();
        return out(new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress).tradeId(tradeId)
                .marketId(market).takerOrderId(takerLeg).takerUserId(takerUser).makerOrderId(makerLeg).makerUserId(makerUser)
                .price(price).quantity(qty).takerIsBuy(takerIsBuy ? BoolFlag.TRUE : BoolFlag.FALSE).meTimestamp(meTs)
                .takerOmsOrderId(takerOms).makerOmsOrderId(makerOms).meEpoch(meEpoch).journalSeq(journalSeq).encodedLength());
    }

    public byte[] terminal(long egress, long journalSeq, long meEpoch, long leg, long user, int market, int status, long meTs,
                           long oms, int rejectReason) {
        zero();
        final MeJournalTerminalEncoder e = new MeJournalTerminalEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress)
                .orderId(leg).userId(user).marketId(market).meTimestamp(meTs).omsOrderId(oms).meEpoch(meEpoch)
                .journalSeq(journalSeq).rejectReason((short) rejectReason);
        buf.putByte(HEADER + MeJournalTerminalEncoder.statusEncodingOffset(), (byte) status);
        return out(e.encodedLength());
    }

    /** A 64 with every field (the shape). */
    public byte[] outcome(long egress, long journalSeq, long meEpoch, long cmdHigh, long cmdLow, long user, long oms, long old,
                          long price, long qty, long budget, int market, int kind, int type, int side, long applied, long meTs,
                          long leg, int status, int reason, int oldCancelled, int result, int tif, long expectedOldRemaining,
                          int ledgerSize, int ledgerCapacity, long behavior) {
        zero();
        return out(new MeCommandOutcomeEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress).commandIdHigh(cmdHigh)
                .commandIdLow(cmdLow).userId(user).omsOrderId(oms).oldOrderId(old).price(price).quantity(qty).budget(budget)
                .marketId(market).commandKind((short) kind).orderType((short) type).orderSide((short) side)
                .appliedPosition(applied).meTimestamp(meTs).orderId(leg).status(status).reason(reason)
                .oldCancelled((short) oldCancelled).result(result).meEpoch(meEpoch).journalSeq(journalSeq)
                .timeInForce((short) tif).expectedOldRemaining(expectedOldRemaining).ledgerSize(ledgerSize)
                .ledgerCapacity(ledgerCapacity).behaviorVersion(behavior).encodedLength());
    }

    /** A 67; {@code entries[i] = {commandIdHigh, idLowFloor, scope, result, removed}}. */
    public byte[] release(long egress, long journalSeq, long meEpoch, long releaseSeq, int ledgerSize, int ledgerCapacity,
                          int tombstoneRanges, long[]... entries) {
        zero();
        final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress)
                .journalSeq(journalSeq).meEpoch(meEpoch).releaseSeq(releaseSeq).ledgerSize(ledgerSize)
                .ledgerCapacity(ledgerCapacity).tombstoneRanges(tombstoneRanges);
        final MeCommandReleaseEncoder.ReleasesEncoder g = e.releasesCount(entries.length);
        for (final long[] x : entries) {
            final int base = e.limit();
            g.next().commandIdHigh(x[0]).idLowFloor(x[1]).removed((int) x[4]);
            buf.putByte(base + MeCommandReleaseEncoder.ReleasesEncoder.scopeEncodingOffset(), (byte) x[2]);
            buf.putByte(base + MeCommandReleaseEncoder.ReleasesEncoder.resultEncodingOffset(), (byte) x[3]);
        }
        return out(e.encodedLength());
    }

    public byte[] engineConfig(long egress, long journalSeq, long meEpoch, long configVersion, long behavior, long capacity,
                               int maxWire, int decision) {
        zero();
        final MeEngineConfigEncoder e = new MeEngineConfigEncoder().wrapAndApplyHeader(buf, 0, header).egressSeq(egress)
                .journalSeq(journalSeq).meEpoch(meEpoch).configVersion(configVersion).behaviorVersion(behavior)
                .ledgerCapacity(capacity).maxWireVersion(maxWire);
        buf.putByte(HEADER + MeEngineConfigEncoder.decisionEncodingOffset(), (byte) decision);
        return out(e.encodedLength());
    }

    // ------------------------------------------------------------------ egress (what serve hands back)

    public byte[] settleRejected(long tradeId, int marketId, long journalPosition, int reason, long detail) {
        zero();
        final SettleRejectedEncoder e = new SettleRejectedEncoder().wrapAndApplyHeader(buf, 0, header).tradeId(tradeId)
                .marketId(marketId).journalPosition(journalPosition).detail(detail);
        buf.putByte(HEADER + SettleRejectedEncoder.reasonEncodingOffset(), (byte) reason);
        return out(e.encodedLength());
    }

    public byte[] adminAck(long corr, int refTemplateId, int result, int reason, long configVersion) {
        zero();
        final AdminAckEncoder e = new AdminAckEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .refTemplateId(refTemplateId).reason((short) reason).configVersion(configVersion);
        buf.putByte(HEADER + AdminAckEncoder.resultEncodingOffset(), (byte) result);
        return out(e.encodedLength());
    }

    public byte[] feedPositionReport(long corr, long consumePosition, long lastAppliedTradeId, boolean journalEnabled,
                                     long meEpoch, long lastAppliedJournalSeq, boolean feedHalted, int haltReason) {
        zero();
        final FeedPositionReportEncoder e = new FeedPositionReportEncoder().wrapAndApplyHeader(buf, 0, header)
                .correlationId(corr).consumePosition(consumePosition).lastAppliedTradeId(lastAppliedTradeId)
                .journalEnabled(journalEnabled ? BoolFlag.TRUE : BoolFlag.FALSE).meEpoch(meEpoch)
                .lastAppliedJournalSeq(lastAppliedJournalSeq).feedHalted(feedHalted ? BoolFlag.TRUE : BoolFlag.FALSE);
        buf.putByte(HEADER + FeedPositionReportEncoder.haltReasonEncodingOffset(), (byte) haltReason);
        return out(e.encodedLength());
    }
}

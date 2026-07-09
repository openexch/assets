// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.HoldRejectDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryDecoder;
import com.openexchange.assets.infrastructure.generated.InitTradeHighWaterEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawAckDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawRejectDecoder;
import com.openexchange.assets.infrastructure.persistence.AssetsSbeDemuxer;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * End-to-end validation of the cluster seams (not just compilation): an SBE ingress message decoded by
 * {@link AssetsSbeDemuxer} drives the real {@link AssetsEngine}, whose events are encoded by
 * {@link AssetsEventPublisher} to SBE egress, which we decode back and assert. This proves the wire
 * codecs and the engine agree on money — including the v2 correlationId echo, the deposit/withdraw acks,
 * and the read-only snapshot queries.
 */
public class WireRoundTripTest {

    /** Capturing egress: decodes each SBE message the publisher emits into a canonical line. */
    private static final class CapturingEgress implements AssetsEventPublisher.Egress {
        final List<String> lines = new ArrayList<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final BalanceUpdateDecoder balance = new BalanceUpdateDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();
        private final WithdrawAckDecoder withdrawAck = new WithdrawAckDecoder();
        private final HoldAckDecoder holdAck = new HoldAckDecoder();
        private final HoldRejectDecoder holdReject = new HoldRejectDecoder();
        private final WithdrawRejectDecoder withdrawReject = new WithdrawRejectDecoder();
        private final SettlementAppliedDecoder settled = new SettlementAppliedDecoder();
        private final BalanceSnapshotEndDecoder balSnapEnd = new BalanceSnapshotEndDecoder();
        private final HoldSnapshotEntryDecoder holdSnapEntry = new HoldSnapshotEntryDecoder();
        private final HoldSnapshotEndDecoder holdSnapEnd = new HoldSnapshotEndDecoder();

        @Override
        public void broadcast(MutableDirectBuffer buffer, int offset, int length) {
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case BalanceUpdateDecoder.TEMPLATE_ID:
                    balance.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("BALANCE u=%d asset=%d avail=%d locked=%d",
                            balance.userId(), balance.assetId(), balance.available(), balance.locked()));
                    break;
                case DepositAckDecoder.TEMPLATE_ID:
                    depositAck.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("DEPOSITACK corr=%d u=%d asset=%d amt=%d newAvail=%d",
                            depositAck.correlationId(), depositAck.userId(), depositAck.assetId(),
                            depositAck.amount(), depositAck.newAvailable()));
                    break;
                case WithdrawAckDecoder.TEMPLATE_ID:
                    withdrawAck.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("WITHDRAWACK corr=%d u=%d asset=%d amt=%d newAvail=%d",
                            withdrawAck.correlationId(), withdrawAck.userId(), withdrawAck.assetId(),
                            withdrawAck.amount(), withdrawAck.newAvailable()));
                    break;
                case HoldAckDecoder.TEMPLATE_ID:
                    holdAck.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("HOLDACK corr=%d order=%d u=%d asset=%d amt=%d",
                            holdAck.correlationId(), holdAck.orderId(), holdAck.userId(),
                            holdAck.assetId(), holdAck.amount()));
                    break;
                case HoldRejectDecoder.TEMPLATE_ID:
                    holdReject.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("HOLDREJECT corr=%d order=%d u=%d asset=%d amt=%d reason=%s",
                            holdReject.correlationId(), holdReject.orderId(), holdReject.userId(),
                            holdReject.assetId(), holdReject.amount(), holdReject.reason().name()));
                    break;
                case WithdrawRejectDecoder.TEMPLATE_ID:
                    withdrawReject.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("WITHDRAWREJECT corr=%d u=%d asset=%d amt=%d reason=%s",
                            withdrawReject.correlationId(), withdrawReject.userId(), withdrawReject.assetId(),
                            withdrawReject.amount(), withdrawReject.reason().name()));
                    break;
                case SettlementAppliedDecoder.TEMPLATE_ID:
                    settled.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("SETTLED tradeId=%d buyer=%d seller=%d",
                            settled.tradeId(), settled.buyerUserId(), settled.sellerUserId()));
                    break;
                case BalanceSnapshotEndDecoder.TEMPLATE_ID:
                    balSnapEnd.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("BALSNAPEND corr=%d count=%d",
                            balSnapEnd.correlationId(), balSnapEnd.entryCount()));
                    break;
                case HoldSnapshotEntryDecoder.TEMPLATE_ID:
                    holdSnapEntry.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("HOLDSNAPENTRY order=%d u=%d asset=%d remaining=%d",
                            holdSnapEntry.orderId(), holdSnapEntry.userId(), holdSnapEntry.assetId(),
                            holdSnapEntry.remaining()));
                    break;
                case HoldSnapshotEndDecoder.TEMPLATE_ID:
                    holdSnapEnd.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("HOLDSNAPEND corr=%d count=%d",
                            holdSnapEnd.correlationId(), holdSnapEnd.entryCount()));
                    break;
                default:
                    lines.add("UNKNOWN template " + header.templateId());
            }
        }
    }

    private final MutableDirectBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final DepositEncoder depositEncoder = new DepositEncoder();
    private final WithdrawEncoder withdrawEncoder = new WithdrawEncoder();
    private final HoldEncoder holdEncoder = new HoldEncoder();
    private final SettleEncoder settleEncoder = new SettleEncoder();
    private final InitTradeHighWaterEncoder initEncoder = new InitTradeHighWaterEncoder();
    private final RequestBalanceSnapshotEncoder balSnapReqEncoder = new RequestBalanceSnapshotEncoder();
    private final RequestHoldSnapshotEncoder holdSnapReqEncoder = new RequestHoldSnapshotEncoder();

    @Test
    public void depositHoldSettleRoundTripThroughTheWire() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine);

        // seller (100) deposits 1 BTC, buyer (200) deposits 60000 USD (correlationIds echoed on the acks)
        deposit(demuxer, 11, 100, Asset.BTC.id(), FixedPoint.fromDouble(1.0));
        deposit(demuxer, 12, 200, Asset.USD.id(), FixedPoint.fromDouble(60000.0));
        // both place holds
        hold(demuxer, 21, 1, 100, Asset.BTC.id(), FixedPoint.fromDouble(1.0));
        hold(demuxer, 22, 2, 200, Asset.USD.id(), FixedPoint.fromDouble(60000.0));
        // a full 1 BTC @ 60000 settlement (taker=buyer)
        settle(demuxer, 1, 1, 2, 200, 1, 100, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);

        // Engine ended where the money should be:
        assertEquals(FixedPoint.fromDouble(1.0), engine.account(200).available(Asset.BTC.id()));
        assertEquals(FixedPoint.fromDouble(60000.0), engine.account(100).available(Asset.USD.id()));
        assertEquals(0L, engine.account(200).locked(Asset.USD.id()));
        assertEquals(0L, engine.account(100).locked(Asset.BTC.id()));

        // And the egress, decoded back off the wire, matches the settlement bookkeeping (acks echo corr).
        List<String> expected = List.of(
                "DEPOSITACK corr=11 u=100 asset=1 amt=100000000 newAvail=100000000",
                "BALANCE u=100 asset=1 avail=100000000 locked=0",
                "DEPOSITACK corr=12 u=200 asset=0 amt=6000000000000 newAvail=6000000000000",
                "BALANCE u=200 asset=0 avail=6000000000000 locked=0",
                "HOLDACK corr=21 order=1 u=100 asset=1 amt=100000000",
                "BALANCE u=100 asset=1 avail=0 locked=100000000",
                "HOLDACK corr=22 order=2 u=200 asset=0 amt=6000000000000",
                "BALANCE u=200 asset=0 avail=0 locked=6000000000000",
                "BALANCE u=200 asset=0 avail=0 locked=0",
                "BALANCE u=200 asset=1 avail=100000000 locked=0",
                "BALANCE u=100 asset=1 avail=0 locked=0",
                "BALANCE u=100 asset=0 avail=6000000000000 locked=0",
                "SETTLED tradeId=1 buyer=200 seller=100");
        assertEquals(expected, egress.lines);
    }

    @Test
    public void withdrawAckAndRejectRoundTrip() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine);

        deposit(demuxer, 1, 300, Asset.USD.id(), FixedPoint.fromDouble(1000.0));
        withdraw(demuxer, 2, 300, Asset.USD.id(), FixedPoint.fromDouble(300.0));   // accepted
        withdraw(demuxer, 3, 300, Asset.USD.id(), FixedPoint.fromDouble(800.0));   // rejected (only 700 left)

        List<String> expected = List.of(
                "DEPOSITACK corr=1 u=300 asset=0 amt=100000000000 newAvail=100000000000",
                "BALANCE u=300 asset=0 avail=100000000000 locked=0",
                "WITHDRAWACK corr=2 u=300 asset=0 amt=30000000000 newAvail=70000000000",
                "BALANCE u=300 asset=0 avail=70000000000 locked=0",
                "WITHDRAWREJECT corr=3 u=300 asset=0 amt=80000000000 reason=INSUFFICIENT_FUNDS");
        assertEquals(expected, egress.lines);
    }

    @Test
    public void snapshotQueriesRoundTrip() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine);

        deposit(demuxer, 0, 100, Asset.USD.id(), FixedPoint.fromDouble(1000.0));
        deposit(demuxer, 0, 200, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        hold(demuxer, 0, 1, 100, Asset.USD.id(), FixedPoint.fromDouble(600.0));
        hold(demuxer, 0, 2, 200, Asset.BTC.id(), FixedPoint.fromDouble(0.5));
        egress.lines.clear(); // drop the setup egress; assert only the query answers

        requestBalanceSnapshot(demuxer, 777);
        requestHoldSnapshot(demuxer, 888);

        List<String> expected = List.of(
                // balances streamed userId-ascending, asset-ascending; both users have exactly one asset
                "BALANCE u=100 asset=0 avail=40000000000 locked=60000000000",
                "BALANCE u=200 asset=1 avail=150000000 locked=50000000",
                "BALSNAPEND corr=777 count=2",
                // holds streamed userId-ascending then orderId-ascending
                "HOLDSNAPENTRY order=1 u=100 asset=0 remaining=60000000000",
                "HOLDSNAPENTRY order=2 u=200 asset=1 remaining=50000000",
                "HOLDSNAPEND corr=888 count=2");
        assertEquals(expected, egress.lines);
    }

    @Test
    public void initTradeHighWaterPrimesOnlyVirginLedger() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine);

        // Virgin ledger: the primer is accepted and seeds the scalars, emitting no egress.
        initTradeHighWater(demuxer, 500L, 12345L);
        assertEquals(500L, engine.getLastAppliedTradeId());
        assertEquals(12345L, engine.getConsumePosition());
        assertEquals(0, egress.lines.size());

        // After activity the ledger is no longer virgin: a second primer is a strict no-op.
        deposit(demuxer, 0, 100, Asset.USD.id(), FixedPoint.fromDouble(1.0));
        initTradeHighWater(demuxer, 999L, 99999L);
        assertEquals("high-water unchanged by refused primer", 500L, engine.getLastAppliedTradeId());
        assertEquals("consumePosition unchanged by refused primer", 12345L, engine.getConsumePosition());
    }

    // ---- ingress SBE encoding ----

    private void deposit(AssetsSbeDemuxer demuxer, long corr, long userId, int assetId, long amount) {
        depositEncoder.wrapAndApplyHeader(ingress, 0, header)
                .correlationId(corr).userId(userId).assetId(assetId).amount(amount);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + depositEncoder.encodedLength(), 1000L);
    }

    private void withdraw(AssetsSbeDemuxer demuxer, long corr, long userId, int assetId, long amount) {
        withdrawEncoder.wrapAndApplyHeader(ingress, 0, header)
                .correlationId(corr).userId(userId).assetId(assetId).amount(amount);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + withdrawEncoder.encodedLength(), 1000L);
    }

    private void hold(AssetsSbeDemuxer demuxer, long corr, long orderId, long userId, int assetId, long amount) {
        holdEncoder.wrapAndApplyHeader(ingress, 0, header)
                .correlationId(corr).orderId(orderId).userId(userId).assetId(assetId).amount(amount);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + holdEncoder.encodedLength(), 1000L);
    }

    private void settle(AssetsSbeDemuxer demuxer, long tradeId, int marketId, long takerOrder, long takerUser,
                        long makerOrder, long makerUser, long price, long qty, boolean takerIsBuy) {
        settleEncoder.wrapAndApplyHeader(ingress, 0, header)
                .tradeId(tradeId).marketId(marketId)
                .takerOrderId(takerOrder).takerUserId(takerUser)
                .makerOrderId(makerOrder).makerUserId(makerUser)
                .price(price).quantity(qty)
                .takerIsBuy(takerIsBuy ? BoolFlag.TRUE : BoolFlag.FALSE);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + settleEncoder.encodedLength(), 1000L);
    }

    private void initTradeHighWater(AssetsSbeDemuxer demuxer, long tradeId, long consumePosition) {
        initEncoder.wrapAndApplyHeader(ingress, 0, header).tradeId(tradeId).consumePosition(consumePosition);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + initEncoder.encodedLength(), 1000L);
    }

    private void requestBalanceSnapshot(AssetsSbeDemuxer demuxer, long corr) {
        balSnapReqEncoder.wrapAndApplyHeader(ingress, 0, header).correlationId(corr);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + balSnapReqEncoder.encodedLength(), 1000L);
    }

    private void requestHoldSnapshot(AssetsSbeDemuxer demuxer, long corr) {
        holdSnapReqEncoder.wrapAndApplyHeader(ingress, 0, header).correlationId(corr);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + holdSnapReqEncoder.encodedLength(), 1000L);
    }
}

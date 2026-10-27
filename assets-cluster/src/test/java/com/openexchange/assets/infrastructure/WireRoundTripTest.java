// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
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
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.SettleBatchEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseEncoder;
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
import static org.junit.Assert.assertTrue;

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
        /** Channel each emitted frame was tagged with, parallel to {@link #lines}. */
        final List<Integer> channels = new ArrayList<>();
        /** Routing each emitted frame was tagged with, parallel to {@link #lines}. */
        final List<AssetsEventPublisher.Routing> routings = new ArrayList<>();
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
        private final FeedPositionReportDecoder feedPos = new FeedPositionReportDecoder();

        @Override
        public void emit(MutableDirectBuffer buffer, int offset, int length, int channel,
                         AssetsEventPublisher.Routing routing) {
            channels.add(channel);
            routings.add(routing);
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
                case FeedPositionReportDecoder.TEMPLATE_ID:
                    feedPos.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("FEEDPOS corr=%d consumePos=%d lastTradeId=%d",
                            feedPos.correlationId(), feedPos.consumePosition(), feedPos.lastAppliedTradeId()));
                    break;
                default:
                    lines.add("UNKNOWN template " + header.templateId());
            }
        }
    }

    private final MutableDirectBuffer ingress = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final DepositEncoder depositEncoder = new DepositEncoder();
    private final WithdrawEncoder withdrawEncoder = new WithdrawEncoder();
    private final HoldEncoder holdEncoder = new HoldEncoder();
    private final SettleEncoder settleEncoder = new SettleEncoder();
    private final InitTradeHighWaterEncoder initEncoder = new InitTradeHighWaterEncoder();
    private final RequestBalanceSnapshotEncoder balSnapReqEncoder = new RequestBalanceSnapshotEncoder();
    private final RequestHoldSnapshotEncoder holdSnapReqEncoder = new RequestHoldSnapshotEncoder();
    private final TerminalReleaseEncoder terminalReleaseEncoder = new TerminalReleaseEncoder();
    private final QueryFeedPositionEncoder queryFeedPosEncoder = new QueryFeedPositionEncoder();

    @Test
    public void depositHoldSettleRoundTripThroughTheWire() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress, () -> false));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));

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
    public void feedPathAdvancesConsumePositionAndAnswersFeedQuery() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress, () -> false));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));

        deposit(demuxer, 1, 100, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        deposit(demuxer, 2, 200, Asset.USD.id(), FixedPoint.fromDouble(120000.0));
        hold(demuxer, 3, 1, 100, Asset.BTC.id(), FixedPoint.fromDouble(2.0));      // sell 2 BTC
        hold(demuxer, 4, 2, 200, Asset.USD.id(), FixedPoint.fromDouble(120000.0)); // buy budget

        // A feed-forward settle stamped with the ME journal position advances the cursor with the money.
        settleAt(demuxer, 7_777L, 1, 1, 2, 200, 1, 100,
                FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);
        assertEquals(7_777L, engine.getConsumePosition());
        assertEquals(1L, engine.getLastAppliedTradeId());

        // Terminal for the half-consumed sell releases its residual (1 BTC) and advances further.
        terminalRelease(demuxer, 7_900L, 1, 100);
        assertEquals(7_900L, engine.getConsumePosition());
        assertEquals(FixedPoint.fromDouble(1.0), engine.account(100).available(Asset.BTC.id()));
        assertEquals(0L, engine.account(100).locked(Asset.BTC.id()));

        // Terminal for an unknown order is a money no-op but still advances the cursor.
        terminalRelease(demuxer, 8_000L, 999, 100);
        assertEquals(8_000L, engine.getConsumePosition());

        // An over-replayed (stale) position never regresses the cursor.
        terminalRelease(demuxer, 7_500L, 998, 100);
        assertEquals(8_000L, engine.getConsumePosition());

        // The feed query answers cursor + settlement high-water, decoded back off the wire.
        egress.lines.clear();
        queryFeedPosition(demuxer, 42);
        assertEquals(List.of("FEEDPOS corr=42 consumePos=8000 lastTradeId=1"), egress.lines);
    }

    /**
     * The v5 SettleBatch ingress: entries apply strictly in group order with per-entry semantics
     * identical to single Settle — including the tradeId idempotency high-water and the
     * journalPosition -> consumePosition advance. A batch straddling the high-water must skip the
     * already-applied entry and apply the rest, exactly as three single Settles would.
     */
    @Test
    public void settleBatchAppliesEntriesInOrderWithSingleSettleSemantics() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress, () -> false));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));

        // Seller has 2 BTC held, buyer has 120k USD held: room for exactly two 1-BTC settles.
        deposit(demuxer, 1, 100, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        deposit(demuxer, 2, 200, Asset.USD.id(), FixedPoint.fromDouble(120000.0));
        hold(demuxer, 3, 1, 100, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        hold(demuxer, 4, 2, 200, Asset.USD.id(), FixedPoint.fromDouble(120000.0));

        // Trade 1 lands as a single Settle: the high-water is now 1.
        settleAt(demuxer, 5_000L, 1, 1, 2, 200, 1, 100,
                FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);
        egress.lines.clear();

        // Batch of [trade 1 (already applied), trade 2 (new)]: entry 1 must be an idempotent no-op,
        // entry 2 must settle, and the cursor must land on the LAST entry's journalPosition.
        SettleBatchEncoder batchEncoder = new SettleBatchEncoder();
        SettleBatchEncoder.SettlesEncoder g = batchEncoder
                .wrapAndApplyHeader(ingress, 0, header)
                .settlesCount(2);
        g.next()
                .tradeId(1).marketId(1)
                .takerOrderId(2).takerUserId(200).makerOrderId(1).makerUserId(100)
                .price(FixedPoint.fromDouble(60000.0)).quantity(FixedPoint.fromDouble(1.0))
                .takerIsBuy(BoolFlag.TRUE).journalPosition(5_000L);
        g.next()
                .tradeId(2).marketId(1)
                .takerOrderId(2).takerUserId(200).makerOrderId(1).makerUserId(100)
                .price(FixedPoint.fromDouble(60000.0)).quantity(FixedPoint.fromDouble(1.0))
                .takerIsBuy(BoolFlag.TRUE).journalPosition(6_000L);
        demuxer.dispatchTrusted(ingress, 0,
                MessageHeaderEncoder.ENCODED_LENGTH + batchEncoder.encodedLength(), 1000L);

        // Only trade 2 applied: exactly one settle's egress (4 balance lines + 1 SETTLED).
        assertEquals(List.of(
                "BALANCE u=200 asset=0 avail=0 locked=0",
                "BALANCE u=200 asset=1 avail=200000000 locked=0",
                "BALANCE u=100 asset=1 avail=0 locked=0",
                "BALANCE u=100 asset=0 avail=12000000000000 locked=0",
                "SETTLED tradeId=2 buyer=200 seller=100"), egress.lines);

        // Money ended where two settles put it, not three.
        assertEquals(FixedPoint.fromDouble(2.0), engine.account(200).available(Asset.BTC.id()));
        assertEquals(FixedPoint.fromDouble(120000.0), engine.account(100).available(Asset.USD.id()));
        assertEquals(2L, engine.getLastAppliedTradeId());
        assertEquals("cursor advances to the batch's last journalPosition",
                6_000L, engine.getConsumePosition());
    }

    @Test
    public void withdrawAckAndRejectRoundTrip() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress, () -> false));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));

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
        engine.setEventSink(new AssetsEventPublisher(egress, () -> false));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));

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
        engine.setEventSink(new AssetsEventPublisher(egress, () -> false));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));

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
                .correlationId(corr).userId(userId).assetId(assetId).amount(amount).requestIdHigh(0).requestIdLow(0);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + depositEncoder.encodedLength(), 1000L);
    }

    private void withdraw(AssetsSbeDemuxer demuxer, long corr, long userId, int assetId, long amount) {
        withdrawEncoder.wrapAndApplyHeader(ingress, 0, header)
                .correlationId(corr).userId(userId).assetId(assetId).amount(amount).requestIdHigh(0).requestIdLow(0);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + withdrawEncoder.encodedLength(), 1000L);
    }

    private void hold(AssetsSbeDemuxer demuxer, long corr, long orderId, long userId, int assetId, long amount) {
        holdEncoder.wrapAndApplyHeader(ingress, 0, header)
                .correlationId(corr).orderId(orderId).userId(userId).assetId(assetId).amount(amount);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + holdEncoder.encodedLength(), 1000L);
    }

    private void settle(AssetsSbeDemuxer demuxer, long tradeId, int marketId, long takerOrder, long takerUser,
                        long makerOrder, long makerUser, long price, long qty, boolean takerIsBuy) {
        settleAt(demuxer, 0L, tradeId, marketId, takerOrder, takerUser, makerOrder, makerUser, price, qty, takerIsBuy);
    }

    private void settleAt(AssetsSbeDemuxer demuxer, long journalPosition, long tradeId, int marketId,
                          long takerOrder, long takerUser, long makerOrder, long makerUser,
                          long price, long qty, boolean takerIsBuy) {
        settleEncoder.wrapAndApplyHeader(ingress, 0, header)
                .tradeId(tradeId).marketId(marketId)
                .takerOrderId(takerOrder).takerUserId(takerUser)
                .makerOrderId(makerOrder).makerUserId(makerUser)
                .price(price).quantity(qty)
                .takerIsBuy(takerIsBuy ? BoolFlag.TRUE : BoolFlag.FALSE)
                .journalPosition(journalPosition);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + settleEncoder.encodedLength(), 1000L);
    }

    private void terminalRelease(AssetsSbeDemuxer demuxer, long journalPosition, long orderId, long userId) {
        terminalReleaseEncoder.wrapAndApplyHeader(ingress, 0, header)
                .journalPosition(journalPosition).orderId(orderId).userId(userId).timestamp(1000L);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + terminalReleaseEncoder.encodedLength(), 1000L);
    }

    private void queryFeedPosition(AssetsSbeDemuxer demuxer, long corr) {
        queryFeedPosEncoder.wrapAndApplyHeader(ingress, 0, header).correlationId(corr);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + queryFeedPosEncoder.encodedLength(), 1000L);
    }

    private void initTradeHighWater(AssetsSbeDemuxer demuxer, long tradeId, long consumePosition) {
        initEncoder.wrapAndApplyHeader(ingress, 0, header).tradeId(tradeId).consumePosition(consumePosition);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + initEncoder.encodedLength(), 1000L);
    }

    private void requestBalanceSnapshot(AssetsSbeDemuxer demuxer, long corr) {
        balSnapReqEncoder.wrapAndApplyHeader(ingress, 0, header).correlationId(corr);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + balSnapReqEncoder.encodedLength(), 1000L);
    }

    private void requestHoldSnapshot(AssetsSbeDemuxer demuxer, long corr) {
        holdSnapReqEncoder.wrapAndApplyHeader(ingress, 0, header).correlationId(corr);
        demuxer.dispatchTrusted(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + holdSnapReqEncoder.encodedLength(), 1000L);
    }

    /**
     * Every egress message must be filed under the channel a subscriber would expect. This is not
     * cosmetic: a frame filed under the wrong channel is silently dropped for any session that did not
     * subscribe to that channel, which looks exactly like the engine never emitting it. A new message
     * added without a channel decision would land here.
     */
    @Test
    public void everyEventIsFiledUnderTheChannelSubscribersExpect() {
        final CapturingEgress egress = new CapturingEgress();
        final AssetsEventPublisher p = new AssetsEventPublisher(egress, () -> false);

        p.onHoldAck(1, 2, 3, 0, 100);
        assertChannel(egress, AssetsEventPublisher.CH_ACKS, "HoldAck");
        p.onHoldReject(1, 2, 3, 0, 100, com.openexchange.assets.domain.RejectReason.INSUFFICIENT_FUNDS);
        assertChannel(egress, AssetsEventPublisher.CH_ACKS, "HoldReject");
        p.onDepositAck(1, 2, 0, 100, 100, false);
        assertChannel(egress, AssetsEventPublisher.CH_ACKS, "DepositAck");
        p.onWithdrawAck(1, 2, 0, 100, 0, false);
        assertChannel(egress, AssetsEventPublisher.CH_ACKS, "WithdrawAck");
        p.onWithdrawReject(1, 2, 0, 100, com.openexchange.assets.domain.RejectReason.INSUFFICIENT_FUNDS);
        assertChannel(egress, AssetsEventPublisher.CH_ACKS, "WithdrawReject");
        p.onFeedPositionReport(1, 42, 7, true);
        assertChannel(egress, AssetsEventPublisher.CH_ACKS, "FeedPositionReport");

        p.onBalanceUpdate(2, 0, 100, 0);
        assertChannel(egress, AssetsEventPublisher.CH_BALANCES, "BalanceUpdate");

        p.onSettlementApplied(9, 1, 2);
        assertChannel(egress, AssetsEventPublisher.CH_SETTLEMENTS, "SettlementApplied");
        p.onSettleFault(9, 1, 2, 0, 5, 5);
        assertChannel(egress, AssetsEventPublisher.CH_SETTLEMENTS, "SettleFault");

        p.onBalanceSnapshotEnd(1, 3);
        assertChannel(egress, AssetsEventPublisher.CH_SNAPSHOTS, "BalanceSnapshotEnd");
        p.onHoldSnapshotEntry(1, 2, 0, 50);
        assertChannel(egress, AssetsEventPublisher.CH_SNAPSHOTS, "HoldSnapshotEntry");
        p.onHoldSnapshotEnd(1, 1);
        assertChannel(egress, AssetsEventPublisher.CH_SNAPSHOTS, "HoldSnapshotEnd");
    }

    /**
     * The routing table, method by method: request-scoped events go to the ORIGIN session only,
     * shared streams BROADCAST. Misrouting is silently wrong in both directions — an ORIGIN frame
     * broadcast resurrects the foreign-ack fanout this table removed; a BROADCAST frame origin-scoped
     * starves every other consumer of money events. BalanceUpdate is the context-dependent one:
     * BROADCAST live, ORIGIN while a query reply is being streamed (the balance-snapshot entries).
     */
    @Test
    public void everyEventIsRoutedToOriginOrBroadcastAsDesigned() {
        final boolean[] queryReply = {false};
        final CapturingEgress egress = new CapturingEgress();
        final AssetsEventPublisher p = new AssetsEventPublisher(egress, () -> queryReply[0]);

        p.onDepositAck(1, 2, 0, 100, 100, false);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "DepositAck");
        p.onWithdrawAck(1, 2, 0, 100, 0, false);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "WithdrawAck");
        p.onHoldAck(1, 2, 3, 0, 100);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "HoldAck");
        p.onHoldReject(1, 2, 3, 0, 100, com.openexchange.assets.domain.RejectReason.INSUFFICIENT_FUNDS);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "HoldReject");
        p.onWithdrawReject(1, 2, 0, 100, com.openexchange.assets.domain.RejectReason.INSUFFICIENT_FUNDS);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "WithdrawReject");
        p.onFeedPositionReport(1, 42, 7, true);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "FeedPositionReport");
        p.onBalanceSnapshotEnd(1, 3);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "BalanceSnapshotEnd");
        p.onHoldSnapshotEntry(1, 2, 0, 50);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "HoldSnapshotEntry");
        p.onHoldSnapshotEnd(1, 1);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "HoldSnapshotEnd");

        p.onSettlementApplied(9, 1, 2);
        assertRouting(egress, AssetsEventPublisher.Routing.BROADCAST, "SettlementApplied");
        p.onSettleFault(9, 1, 2, 0, 5, 5);
        assertRouting(egress, AssetsEventPublisher.Routing.BROADCAST, "SettleFault");

        p.onBalanceUpdate(2, 0, 100, 0);
        assertRouting(egress, AssetsEventPublisher.Routing.BROADCAST, "live BalanceUpdate");
        queryReply[0] = true;
        p.onBalanceUpdate(2, 0, 100, 0);
        assertRouting(egress, AssetsEventPublisher.Routing.ORIGIN, "snapshot-reply BalanceUpdate");
        queryReply[0] = false;
        p.onBalanceUpdate(2, 0, 100, 0);
        assertRouting(egress, AssetsEventPublisher.Routing.BROADCAST, "live BalanceUpdate after the reply");
    }

    // ---- RC2 S1 (contract 8.1 assets, 8.3): money-schema v7 templates and tails ----

    /** Copies every emitted frame (the publisher reuses one scratch buffer). */
    private static final class RawEgress implements AssetsEventPublisher.Egress {
        final List<byte[]> frames = new ArrayList<>();

        @Override
        public void emit(MutableDirectBuffer buffer, int offset, int length, int channel, AssetsEventPublisher.Routing routing) {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            frames.add(copy);
        }

        UnsafeBuffer last() {
            return new UnsafeBuffer(frames.get(frames.size() - 1));
        }
    }

    private static MessageHeaderDecoder headerOf(final UnsafeBuffer b) {
        return new MessageHeaderDecoder().wrap(b, 0);
    }

    /** An OrderEvent whose body is all 0xFF where the order carries a long: dirties the publisher scratch. */
    private static UnsafeBuffer dirtyScratch(final AssetsEventPublisher p, final RawEgress egress) {
        final com.openexchange.assets.application.engine.order.CoreOrder o = new com.openexchange.assets.application.engine.order.CoreOrder();
        o.clear();
        o.orderId = -1; o.userId = -1; o.price = -1; o.stopPrice = -1; o.quantity = -1; o.filledQty = -1; o.remainingQty = -1;
        o.holdAmount = -1; o.clientOrderIdHigh = -1; o.clientOrderIdLow = -1; o.createdAtMs = -1; o.currentLegId = -1;
        o.marketId = -1;
        o.timeInForce = 1;
        p.onOrderEvent(-1L, -1L, o, com.openexchange.assets.domain.EventKind.ACK_NEW,
                com.openexchange.assets.domain.CoreRejectReason.NONE, -1L, -1L, -1L, -1L, -1L);
        return egress.last();
    }

    @Test
    public void v7NewTemplatesRoundTrip() {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[2048]);
        final MessageHeaderEncoder he = new MessageHeaderEncoder();
        final MessageHeaderDecoder hd = new MessageHeaderDecoder();

        new com.openexchange.assets.infrastructure.generated.FeedHaltEncoder().wrapAndApplyHeader(b, 0, he).correlationId(1)
                .reason(com.openexchange.assets.infrastructure.generated.FeedHaltReason.SETTLE_REFUSED).meEpochSeen(2)
                .expectedJournalSeq(3).seenJournalSeq(4).bridgeIdentity(5);
        final var fh = new com.openexchange.assets.infrastructure.generated.FeedHaltDecoder().wrapAndApplyHeader(b, 0, hd);
        assertEquals(66, hd.templateId());
        assertEquals(7, hd.version());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), List.of(fh.correlationId(), fh.meEpochSeen(), fh.expectedJournalSeq(),
                fh.seenJournalSeq(), fh.bridgeIdentity()));
        assertEquals(com.openexchange.assets.infrastructure.generated.FeedHaltReason.SETTLE_REFUSED, fh.reason());

        for (final int n : new int[] {0, 1, 48}) {
            final var re = new com.openexchange.assets.infrastructure.generated.MeCommandReleaseEncoder().wrapAndApplyHeader(b, 0, he)
                    .egressSeq(10).journalSeq(11).meEpoch(12).releaseSeq(13).ledgerSize(14).ledgerCapacity(15).tombstoneRanges(16);
            final var g = re.releasesCount(n);
            for (int i = 0; i < n; i++) {
                g.next().commandIdHigh(100 + i).idLowFloor(4 * i)
                        .scope(i % 2 == 0 ? com.openexchange.assets.infrastructure.generated.ReleaseScope.UP_TO
                                : com.openexchange.assets.infrastructure.generated.ReleaseScope.ORDER)
                        .result(com.openexchange.assets.infrastructure.generated.ReleaseResult.get((short) (i % 3)))
                        .removed(i);
            }
            assertEquals(MessageHeaderEncoder.ENCODED_LENGTH + 44 + 4 + 22 * n, MessageHeaderEncoder.ENCODED_LENGTH + re.encodedLength());
            final var rd = new com.openexchange.assets.infrastructure.generated.MeCommandReleaseDecoder().wrapAndApplyHeader(b, 0, hd);
            assertEquals(List.of(10L, 11L, 12L, 13L), List.of(rd.egressSeq(), rd.journalSeq(), rd.meEpoch(), rd.releaseSeq()));
            assertEquals(List.of(14, 15, 16), List.of(rd.ledgerSize(), rd.ledgerCapacity(), rd.tombstoneRanges()));
            int i = 0;
            for (final var e : rd.releases()) {
                assertEquals(100 + i, e.commandIdHigh());
                assertEquals(4L * i, e.idLowFloor());
                assertEquals(i % 2, e.scopeRaw());
                assertEquals(i % 3, e.resultRaw());
                assertEquals(i, e.removed());
                i++;
            }
            assertEquals(n, i);
        }

        new com.openexchange.assets.infrastructure.generated.MeEngineConfigEncoder().wrapAndApplyHeader(b, 0, he)
                .egressSeq(20).journalSeq(1).meEpoch(21).configVersion(3).behaviorVersion(2).ledgerCapacity(100_000L)
                .maxWireVersion(12).decision(com.openexchange.assets.infrastructure.generated.ConfigDecision.BUMP);
        final var ec = new com.openexchange.assets.infrastructure.generated.MeEngineConfigDecoder().wrapAndApplyHeader(b, 0, hd);
        assertEquals(List.of(20L, 1L, 21L, 3L, 2L, 100_000L), List.of(ec.egressSeq(), ec.journalSeq(), ec.meEpoch(),
                ec.configVersion(), ec.behaviorVersion(), ec.ledgerCapacity()));
        assertEquals(12, ec.maxWireVersion());
        assertEquals(com.openexchange.assets.infrastructure.generated.ConfigDecision.BUMP, ec.decision());

        new com.openexchange.assets.infrastructure.generated.FeedLatchClearEncoder().wrapAndApplyHeader(b, 0, he).correlationId(30)
                .haltReason(com.openexchange.assets.infrastructure.generated.FeedHaltReason.GAP).journalSeq(31);
        final var lc = new com.openexchange.assets.infrastructure.generated.FeedLatchClearDecoder().wrapAndApplyHeader(b, 0, hd);
        assertEquals(76, hd.templateId());
        assertEquals(30, lc.correlationId());
        assertEquals(com.openexchange.assets.infrastructure.generated.FeedHaltReason.GAP, lc.haltReason());
        assertEquals(31, lc.journalSeq());

        new com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder().wrapAndApplyHeader(b, 0, he).correlationId(40)
                .sessionClass(com.openexchange.assets.infrastructure.generated.SessionClass.REPLICA).readerKind((short) 0)
                .resumable(BoolFlag.TRUE).identity(0).sourceEpoch(0);
        final var rs = new com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder().wrapAndApplyHeader(b, 0, hd);
        assertEquals(7, rs.sessionClassRaw());
        assertEquals(com.openexchange.assets.infrastructure.generated.SessionClass.REPLICA, rs.sessionClass());
    }

    @Test
    public void v7FeedAndBatchTailsRoundTrip() {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[8192]);
        final MessageHeaderEncoder he = new MessageHeaderEncoder();
        final MessageHeaderDecoder hd = new MessageHeaderDecoder();
        new com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder().wrapAndApplyHeader(b, 0, he)
                .result(8).meEpoch(1).journalSeq(2).timeInForce((short) 2).expectedOldRemaining(3).ledgerSize(4)
                .ledgerCapacity(5).behaviorVersion(6);
        final var oc = new com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder().wrapAndApplyHeader(b, 0, hd);
        assertEquals(List.of(8L, 1L, 2L, 2L, 3L, 4L, 5L, 6L), List.of((long) oc.result(), oc.meEpoch(), oc.journalSeq(),
                (long) oc.timeInForce(), oc.expectedOldRemaining(), (long) oc.ledgerSize(), (long) oc.ledgerCapacity(),
                oc.behaviorVersion()));

        new com.openexchange.assets.infrastructure.generated.MeJournalTerminalEncoder().wrapAndApplyHeader(b, 0, he)
                .journalSeq(9).rejectReason((short) 254);
        final var jt = new com.openexchange.assets.infrastructure.generated.MeJournalTerminalDecoder().wrapAndApplyHeader(b, 0, hd);
        assertEquals(9, jt.journalSeq());
        assertEquals(254, jt.rejectReason());

        final var tb = new com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchEncoder().wrapAndApplyHeader(b, 0, he);
        final var tg = tb.tradesCount(64);
        for (int i = 0; i < 64; i++) {
            tg.next().tradeId(i).journalSeq(1_000 + i);
        }
        assertEquals(6988, MessageHeaderEncoder.ENCODED_LENGTH + tb.encodedLength());
        int i = 0;
        for (final var t : new com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder().wrapAndApplyHeader(b, 0, hd).trades()) {
            assertEquals(1_000 + i, t.journalSeq());
            i++;
        }
        assertEquals(64, i);

        final var ob = new com.openexchange.assets.infrastructure.generated.OrderEventBatchEncoder().wrapAndApplyHeader(b, 0, he);
        final var og = ob.eventsCount(48);
        for (int k = 0; k < 48; k++) {
            og.next().orderId(k).meReason((short) (k == 0 ? 255 : k));
        }
        assertEquals(8028, MessageHeaderEncoder.ENCODED_LENGTH + ob.encodedLength());
        i = 0;
        for (final var e : new com.openexchange.assets.infrastructure.generated.OrderEventBatchDecoder().wrapAndApplyHeader(b, 0, hd).events()) {
            assertEquals(i == 0 ? 255 : i, e.meReason());
            i++;
        }
        assertEquals(48, i);

        final var db = new com.openexchange.assets.infrastructure.generated.MeDispatchBatchEncoder().wrapAndApplyHeader(b, 0, he);
        final var dg = db.dispatchesCount(64);
        for (int k = 0; k < 64; k++) {
            dg.next().dispatchSeq(k).releaseScope(com.openexchange.assets.infrastructure.generated.ReleaseScope.ORDER)
                    .expectedOldRemaining(k * 10L);
        }
        i = 0;
        for (final var d : new com.openexchange.assets.infrastructure.generated.MeDispatchBatchDecoder().wrapAndApplyHeader(b, 0, hd).dispatches()) {
            assertEquals(com.openexchange.assets.infrastructure.generated.ReleaseScope.ORDER, d.releaseScope());
            assertEquals(i * 10L, d.expectedOldRemaining());
            i++;
        }
        assertEquals(64, i);
    }

    /** 4.6: the publisher writes the S1 value of every appended egress field, over a dirty scratch buffer. */
    @Test
    public void publisherWritesTheS1ValuesOfTheV7Tails() {
        final RawEgress egress = new RawEgress();
        final AssetsEventPublisher p = new AssetsEventPublisher(egress, () -> false);

        // 17 FeedPositionReport: 0, 0, FALSE, NONE
        UnsafeBuffer dirty = dirtyScratch(p, egress);
        // precondition: every v7 field of 17 (meEpoch @25, lastAppliedJournalSeq @33, feedHalted @41, haltReason @42)
        // starts over nonzero scratch bytes
        assertTrue(dirty.getLong(MessageHeaderEncoder.ENCODED_LENGTH + 25) != 0);
        assertTrue(dirty.getLong(MessageHeaderEncoder.ENCODED_LENGTH + 33) != 0);
        assertTrue(dirty.getByte(MessageHeaderEncoder.ENCODED_LENGTH + 41) != 0);
        assertTrue(dirty.getByte(MessageHeaderEncoder.ENCODED_LENGTH + 42) != 0);
        assertTrue(dirty.getInt(MessageHeaderEncoder.ENCODED_LENGTH + 23) != 0); // 109 gateReasons @23
        p.onFeedPositionReport(1L, 2L, 3L, true);
        UnsafeBuffer f = egress.last();
        final FeedPositionReportDecoder fp = new FeedPositionReportDecoder().wrapAndApplyHeader(f, 0, headerOf(f));
        assertEquals(43, headerOf(f).blockLength());
        assertEquals(0L, fp.meEpoch());
        assertEquals(0L, fp.lastAppliedJournalSeq());
        assertEquals(BoolFlag.FALSE, fp.feedHalted());
        assertEquals(com.openexchange.assets.infrastructure.generated.FeedHaltReason.NONE, fp.haltReason());

        // 109 AdmissionState: OPEN -> 0, SUSPENDED -> 1 << gateReason
        final Object[][] gates = {
            {com.openexchange.assets.domain.GateState.OPEN, com.openexchange.assets.domain.GateReason.ADMIN, 0L},
            {com.openexchange.assets.domain.GateState.SUSPENDED, com.openexchange.assets.domain.GateReason.ADMIN, 1L},
            {com.openexchange.assets.domain.GateState.SUSPENDED, com.openexchange.assets.domain.GateReason.GENESIS, 1L << 5},
            {com.openexchange.assets.domain.GateState.SUSPENDED, com.openexchange.assets.domain.GateReason.ME_BEHAVIOR, 1L << 8},
        };
        for (final Object[] g : gates) {
            dirtyScratch(p, egress);
            p.onAdmissionState(1, (com.openexchange.assets.domain.GateState) g[0], (com.openexchange.assets.domain.GateReason) g[1],
                    false, 5L, 6L, false);
            f = egress.last();
            final var as = new com.openexchange.assets.infrastructure.generated.AdmissionStateDecoder().wrapAndApplyHeader(f, 0, headerOf(f));
            assertEquals(g[0] + "/" + g[1], (long) g[2], as.gateReasons().getRaw());
            assertEquals(((com.openexchange.assets.domain.GateReason) g[1]).code(), as.gateReasonRaw());
        }

        // 102 OrderEvent: meReason 255 (not ME-sourced)
        dirtyScratch(p, egress);
        f = egress.last();
        final OrderEventDecoderView oe = new OrderEventDecoderView(f);
        assertEquals(255, oe.meReason());

        // 104 MeDispatch (broadcast and the ORIGIN reply): releaseScope 0, expectedOldRemaining 0
        dirty = dirtyScratch(p, egress);
        assertTrue(dirty.getLong(MessageHeaderEncoder.ENCODED_LENGTH + 81) != 0);
        p.onMeDispatch(1L, 2L, 3L, com.openexchange.assets.domain.DispatchKind.CREATE, 4L, 5L, 6L, 1, 0, 0, 7L, 8L, 9L, 0);
        f = egress.last();
        var md = new com.openexchange.assets.infrastructure.generated.MeDispatchDecoder().wrapAndApplyHeader(f, 0, headerOf(f));
        assertEquals(89, headerOf(f).blockLength());
        assertEquals(com.openexchange.assets.infrastructure.generated.ReleaseScope.UP_TO, md.releaseScope());
        assertEquals(0L, md.expectedOldRemaining());
        dirtyScratch(p, egress);
        p.onMeDispatchReply(1L, 2L, 3L, com.openexchange.assets.domain.DispatchKind.CANCEL, 4L, 5L, 6L, 1, 0, 0, 7L, 8L, 9L, 0);
        f = egress.last();
        md = new com.openexchange.assets.infrastructure.generated.MeDispatchDecoder().wrapAndApplyHeader(f, 0, headerOf(f));
        assertEquals(com.openexchange.assets.infrastructure.generated.ReleaseScope.UP_TO, md.releaseScope());
        assertEquals(0L, md.expectedOldRemaining());
    }

    private static final class OrderEventDecoderView {
        private final com.openexchange.assets.infrastructure.generated.OrderEventDecoder d;

        OrderEventDecoderView(final UnsafeBuffer f) {
            d = new com.openexchange.assets.infrastructure.generated.OrderEventDecoder().wrapAndApplyHeader(f, 0, headerOf(f));
        }

        int meReason() {
            return d.meReason();
        }
    }

    /**
     * 4.4 row 70 as served by RC2 S7 (contract 7.1 rows 2 and 2b; the S1 form of this test pinned "decoded, neither
     * compared nor recorded" and said S7 turns the other-nonce resend into a re-genesis REJECT): genesisId is recorded at
     * ADOPT as sent, carried by section 8 layout v2, and any later config with another nonce is REJECTED INVALID.
     */
    @Test
    public void coreConfigGenesisIdIsRecordedAndComparedFromS7() {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[2048]);
        final MessageHeaderEncoder he = new MessageHeaderEncoder();
        final MessageHeaderDecoder hd = new MessageHeaderDecoder();
        final int len = coreConfig(b, he, 0x1234_5678_9ABCL);
        final var cd = new com.openexchange.assets.infrastructure.generated.CoreConfigDecoder().wrapAndApplyHeader(b, 0, hd);
        assertEquals("round trip", 0x1234_5678_9ABCL, cd.genesisId());

        final AssetsEngine engine = new AssetsEngine();
        final com.openexchange.assets.determinism.RecordingAssetsSink sink = new com.openexchange.assets.determinism.RecordingAssetsSink();
        engine.setEventSink(sink);
        engine.setConfigFailFast(() -> { throw new AssertionError("the compiled tables must pass the cross-check"); });
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
        demuxer.dispatchTrusted(b, 0, len, 1_000L);
        assertEquals(1, engine.coreConfig().adoptedCount());
        assertEquals("recorded as sent (S7 7.1 row 2)", 0x1234_5678_9ABCL, engine.coreConfig().recorded().genesisId);
        final org.agrona.ExpandableArrayBuffer img = new org.agrona.ExpandableArrayBuffer();
        final int imgLen = com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec.serialize(engine, img);

        // the same config under another nonce is a re-genesis attempt: REJECTED INVALID (row 2b), nothing recorded
        demuxer.dispatchTrusted(b, 0, coreConfig(b, he, 42L), 1_000L);
        assertEquals(0, engine.coreConfig().duplicateCount());
        assertEquals(1, engine.coreConfig().rejectCount());
        assertEquals(0, demuxer.decodeRejectCount());
        final var ack = (com.openexchange.assets.determinism.AssetsEngineEvent.AdminAck) sink.events().get(sink.events().size() - 1);
        assertEquals(com.openexchange.assets.domain.AdminResult.REJECTED, ack.result());
        assertEquals(com.openexchange.assets.domain.CoreRejectReason.INVALID.code(), ack.reason());
        assertEquals(0x1234_5678_9ABCL, engine.coreConfig().recorded().genesisId);
        final org.agrona.ExpandableArrayBuffer img2 = new org.agrona.ExpandableArrayBuffer();
        final int imgLen2 = com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec.serialize(engine, img2);
        final byte[] a = new byte[imgLen];
        final byte[] c = new byte[imgLen2];
        img.getBytes(0, a);
        img2.getBytes(0, c);
        org.junit.Assert.assertArrayEquals("the refused re-genesis leaves no trace in the image", a, c);
    }

    /** The compiled market and asset tables as a behaviour-1 CoreConfig (what the cross-check accepts). */
    private static int coreConfig(final UnsafeBuffer b, final MessageHeaderEncoder he, final long genesisId) {
        final var e = new com.openexchange.assets.infrastructure.generated.CoreConfigEncoder().wrapAndApplyHeader(b, 0, he)
                .correlationId(1).configVersion(1).behaviorVersion(1).genesisId(genesisId);
        final var m = e.marketsCount(com.openexchange.assets.domain.Market.ALL.length);
        for (final com.openexchange.assets.domain.Market d : com.openexchange.assets.domain.Market.ALL) {
            m.next().marketId(d.marketId()).symbol(d.symbol()).baseAssetId(d.baseAsset().id()).quoteAssetId(d.quoteAsset().id())
                    .tickSize(1).minPrice(1).maxPrice(Long.MAX_VALUE / 4).lotSize(1);
        }
        final var a = e.assetsCount(Asset.values().length);
        for (final Asset d : Asset.values()) {
            a.next().assetId(d.id()).code(d.name()).scale((short) 8);
        }
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static void assertChannel(CapturingEgress egress, int expected, String what) {
        assertEquals(what + " must be filed under channel " + expected,
                Integer.valueOf(expected), egress.channels.get(egress.channels.size() - 1));
    }

    private static void assertRouting(CapturingEgress egress, AssetsEventPublisher.Routing expected,
                                      String what) {
        assertEquals(what + " must be routed " + expected,
                expected, egress.routings.get(egress.routings.size() - 1));
    }
}

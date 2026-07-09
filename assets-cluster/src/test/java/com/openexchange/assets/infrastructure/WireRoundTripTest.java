// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.HoldRejectDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder;
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
 * codecs and the engine agree on money.
 */
public class WireRoundTripTest {

    /** Capturing egress: decodes each SBE message the publisher emits into a readable line. */
    private static final class CapturingEgress implements AssetsEventPublisher.Egress {
        final List<String> lines = new ArrayList<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final BalanceUpdateDecoder balance = new BalanceUpdateDecoder();
        private final HoldAckDecoder holdAck = new HoldAckDecoder();
        private final HoldRejectDecoder holdReject = new HoldRejectDecoder();
        private final SettlementAppliedDecoder settled = new SettlementAppliedDecoder();

        @Override
        public void broadcast(MutableDirectBuffer buffer, int offset, int length) {
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case BalanceUpdateDecoder.TEMPLATE_ID:
                    balance.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("BALANCE u=%d asset=%d avail=%d locked=%d",
                            balance.userId(), balance.assetId(), balance.available(), balance.locked()));
                    break;
                case HoldAckDecoder.TEMPLATE_ID:
                    holdAck.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("HOLDACK order=%d u=%d asset=%d amt=%d",
                            holdAck.orderId(), holdAck.userId(), holdAck.assetId(), holdAck.amount()));
                    break;
                case HoldRejectDecoder.TEMPLATE_ID:
                    holdReject.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("HOLDREJECT order=%d u=%d asset=%d amt=%d reason=%s",
                            holdReject.orderId(), holdReject.userId(), holdReject.assetId(),
                            holdReject.amount(), holdReject.reason().name()));
                    break;
                case SettlementAppliedDecoder.TEMPLATE_ID:
                    settled.wrapAndApplyHeader(buffer, offset, header);
                    lines.add(String.format("SETTLED tradeId=%d buyer=%d seller=%d",
                            settled.tradeId(), settled.buyerUserId(), settled.sellerUserId()));
                    break;
                default:
                    lines.add("UNKNOWN template " + header.templateId());
            }
        }
    }

    private final MutableDirectBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final DepositEncoder depositEncoder = new DepositEncoder();
    private final HoldEncoder holdEncoder = new HoldEncoder();
    private final SettleEncoder settleEncoder = new SettleEncoder();

    @Test
    public void depositHoldSettleRoundTripThroughTheWire() {
        AssetsEngine engine = new AssetsEngine();
        CapturingEgress egress = new CapturingEgress();
        engine.setEventSink(new AssetsEventPublisher(egress));
        AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine);

        // seller (100) deposits 1 BTC, buyer (200) deposits 60000 USD
        deposit(demuxer, 100, Asset.BTC.id(), FixedPoint.fromDouble(1.0));
        deposit(demuxer, 200, Asset.USD.id(), FixedPoint.fromDouble(60000.0));
        // both place holds
        hold(demuxer, 1, 100, Asset.BTC.id(), FixedPoint.fromDouble(1.0));
        hold(demuxer, 2, 200, Asset.USD.id(), FixedPoint.fromDouble(60000.0));
        // a full 1 BTC @ 60000 settlement (taker=buyer)
        settle(demuxer, 1, 1, 2, 200, 1, 100, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);

        // Engine ended where the money should be:
        assertEquals(FixedPoint.fromDouble(1.0), engine.account(200).available(Asset.BTC.id()));
        assertEquals(FixedPoint.fromDouble(60000.0), engine.account(100).available(Asset.USD.id()));
        assertEquals(0L, engine.account(200).locked(Asset.USD.id()));
        assertEquals(0L, engine.account(100).locked(Asset.BTC.id()));

        // And the egress, decoded back off the wire, matches the settlement bookkeeping.
        List<String> l = egress.lines;
        assertEquals("BALANCE u=100 asset=1 avail=100000000 locked=0", l.get(0));       // seller deposit
        assertEquals("BALANCE u=200 asset=0 avail=6000000000000 locked=0", l.get(1));    // buyer deposit
        assertEquals("HOLDACK order=1 u=100 asset=1 amt=100000000", l.get(2));
        assertEquals("BALANCE u=100 asset=1 avail=0 locked=100000000", l.get(3));
        assertEquals("HOLDACK order=2 u=200 asset=0 amt=6000000000000", l.get(4));
        assertEquals("BALANCE u=200 asset=0 avail=0 locked=6000000000000", l.get(5));
        // settlement: 4 balance lines + SETTLED
        assertEquals("BALANCE u=200 asset=0 avail=0 locked=0", l.get(6));
        assertEquals("BALANCE u=200 asset=1 avail=100000000 locked=0", l.get(7));
        assertEquals("BALANCE u=100 asset=1 avail=0 locked=0", l.get(8));
        assertEquals("BALANCE u=100 asset=0 avail=6000000000000 locked=0", l.get(9));
        assertEquals("SETTLED tradeId=1 buyer=200 seller=100", l.get(10));
        assertEquals(11, l.size());
    }

    private void deposit(AssetsSbeDemuxer demuxer, long userId, int assetId, long amount) {
        depositEncoder.wrapAndApplyHeader(ingress, 0, header).userId(userId).assetId(assetId).amount(amount);
        demuxer.dispatch(ingress, 0, MessageHeaderEncoder.ENCODED_LENGTH + depositEncoder.encodedLength(), 1000L);
    }

    private void hold(AssetsSbeDemuxer demuxer, long orderId, long userId, int assetId, long amount) {
        holdEncoder.wrapAndApplyHeader(ingress, 0, header)
                .orderId(orderId).userId(userId).assetId(assetId).amount(amount);
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
}

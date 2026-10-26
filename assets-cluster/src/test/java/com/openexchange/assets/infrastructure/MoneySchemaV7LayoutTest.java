// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.ReleaseResult;
import com.openexchange.assets.domain.ReleaseScope;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.CoreConfigDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.FeedHaltDecoder;
import com.openexchange.assets.infrastructure.generated.FeedLatchClearDecoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.GateReasonsDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEventBatchDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEventDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.AssetsSbeDemuxer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S1 (contract 4.1-4.4, 4.9, 8.1 assets): every block length, offset and enum value money-schema v7 freezes,
 * checked against the generated codecs; the v7 additions sit after the unchanged v6 prefix of each template; the
 * domain enums carry the wire codes; and a v6-shaped frame is refused by the exact v7 gate (schema reject, counted).
 */
public class MoneySchemaV7LayoutTest {

    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    @Test
    public void schemaIdentity() {
        assertEquals(2, MessageHeaderDecoder.SCHEMA_ID);
        assertEquals(7, MessageHeaderDecoder.SCHEMA_VERSION);
        assertEquals("0.7", MessageHeaderDecoder.SEMANTIC_VERSION);
    }

    /** 4.2: the ME feed templates, appended fields after the v6 block (101, 53, 124). */
    @Test
    public void feedIngressTails() {
        assertEquals(109, MeJournalTradeDecoder.BLOCK_LENGTH);
        assertEquals(101, MeJournalTradeDecoder.journalSeqEncodingOffset());
        assertEquals(15, MeJournalTradeDecoder.journalSeqId());
        assertEquals(7, MeJournalTradeDecoder.journalSeqSinceVersion());

        assertEquals(109, MeJournalTradeBatchDecoder.TradesDecoder.sbeBlockLength());
        assertEquals(101, MeJournalTradeBatchDecoder.TradesDecoder.journalSeqEncodingOffset());
        assertEquals("62 at cap 64 fits the 8192 MTU", 6988, HEADER + 4 + 64 * MeJournalTradeBatchDecoder.TradesDecoder.sbeBlockLength());

        assertEquals(62, MeJournalTerminalDecoder.BLOCK_LENGTH);
        assertEquals(53, MeJournalTerminalDecoder.journalSeqEncodingOffset());
        assertEquals(9, MeJournalTerminalDecoder.journalSeqId());
        assertEquals(61, MeJournalTerminalDecoder.rejectReasonEncodingOffset());
        assertEquals(10, MeJournalTerminalDecoder.rejectReasonId());
        assertEquals(1, MeJournalTerminalDecoder.rejectReasonEncodingLength());

        assertEquals(153, MeCommandOutcomeDecoder.BLOCK_LENGTH);
        assertEquals(124, MeCommandOutcomeDecoder.journalSeqEncodingOffset());
        assertEquals(132, MeCommandOutcomeDecoder.timeInForceEncodingOffset());
        assertEquals(133, MeCommandOutcomeDecoder.expectedOldRemainingEncodingOffset());
        assertEquals(141, MeCommandOutcomeDecoder.ledgerSizeEncodingOffset());
        assertEquals(145, MeCommandOutcomeDecoder.ledgerCapacityEncodingOffset());
        assertEquals(149, MeCommandOutcomeDecoder.behaviorVersionEncodingOffset());
        assertEquals(4, MeCommandOutcomeDecoder.behaviorVersionEncodingLength());
        assertEquals(22, MeCommandOutcomeDecoder.journalSeqId());
        assertEquals(27, MeCommandOutcomeDecoder.behaviorVersionId());
    }

    /** 4.3 / 4.3a: the new templates 66, 67, 68 and 76. */
    @Test
    public void newTemplates() {
        assertEquals(66, FeedHaltDecoder.TEMPLATE_ID);
        assertEquals(41, FeedHaltDecoder.BLOCK_LENGTH);
        assertEquals(0, FeedHaltDecoder.correlationIdEncodingOffset());
        assertEquals(8, FeedHaltDecoder.reasonEncodingOffset());
        assertEquals(9, FeedHaltDecoder.meEpochSeenEncodingOffset());
        assertEquals(17, FeedHaltDecoder.expectedJournalSeqEncodingOffset());
        assertEquals(25, FeedHaltDecoder.seenJournalSeqEncodingOffset());
        assertEquals(33, FeedHaltDecoder.bridgeIdentityEncodingOffset());

        assertEquals(67, MeCommandReleaseDecoder.TEMPLATE_ID);
        assertEquals(44, MeCommandReleaseDecoder.BLOCK_LENGTH);
        assertEquals(0, MeCommandReleaseDecoder.egressSeqEncodingOffset());
        assertEquals(8, MeCommandReleaseDecoder.journalSeqEncodingOffset());
        assertEquals(16, MeCommandReleaseDecoder.meEpochEncodingOffset());
        assertEquals(24, MeCommandReleaseDecoder.releaseSeqEncodingOffset());
        assertEquals(32, MeCommandReleaseDecoder.ledgerSizeEncodingOffset());
        assertEquals(36, MeCommandReleaseDecoder.ledgerCapacityEncodingOffset());
        assertEquals(40, MeCommandReleaseDecoder.tombstoneRangesEncodingOffset());
        assertEquals(20, MeCommandReleaseDecoder.releasesDecoderId());
        assertEquals(22, MeCommandReleaseDecoder.ReleasesDecoder.sbeBlockLength());
        assertEquals(0, MeCommandReleaseDecoder.ReleasesDecoder.commandIdHighEncodingOffset());
        assertEquals(8, MeCommandReleaseDecoder.ReleasesDecoder.idLowFloorEncodingOffset());
        assertEquals(16, MeCommandReleaseDecoder.ReleasesDecoder.scopeEncodingOffset());
        assertEquals(17, MeCommandReleaseDecoder.ReleasesDecoder.resultEncodingOffset());
        assertEquals(18, MeCommandReleaseDecoder.ReleasesDecoder.removedEncodingOffset());
        assertEquals("frame max at cap 48", 1112, HEADER + MeCommandReleaseDecoder.BLOCK_LENGTH + 4
                + 48 * MeCommandReleaseDecoder.ReleasesDecoder.sbeBlockLength());

        assertEquals(68, MeEngineConfigDecoder.TEMPLATE_ID);
        assertEquals(39, MeEngineConfigDecoder.BLOCK_LENGTH);
        assertEquals(0, MeEngineConfigDecoder.egressSeqEncodingOffset());
        assertEquals(8, MeEngineConfigDecoder.journalSeqEncodingOffset());
        assertEquals(16, MeEngineConfigDecoder.meEpochEncodingOffset());
        assertEquals(24, MeEngineConfigDecoder.configVersionEncodingOffset());
        assertEquals(28, MeEngineConfigDecoder.behaviorVersionEncodingOffset());
        assertEquals(32, MeEngineConfigDecoder.ledgerCapacityEncodingOffset());
        assertEquals(36, MeEngineConfigDecoder.maxWireVersionEncodingOffset());
        assertEquals(2, MeEngineConfigDecoder.maxWireVersionEncodingLength());
        assertEquals(38, MeEngineConfigDecoder.decisionEncodingOffset());

        assertEquals(76, FeedLatchClearDecoder.TEMPLATE_ID);
        assertEquals(17, FeedLatchClearDecoder.BLOCK_LENGTH);
        assertEquals(0, FeedLatchClearDecoder.correlationIdEncodingOffset());
        assertEquals(8, FeedLatchClearDecoder.haltReasonEncodingOffset());
        assertEquals(9, FeedLatchClearDecoder.journalSeqEncodingOffset());
    }

    /** 4.4: egress and admin tails. */
    @Test
    public void egressAndAdminTails() {
        assertEquals(43, FeedPositionReportDecoder.BLOCK_LENGTH);
        assertEquals(25, FeedPositionReportDecoder.meEpochEncodingOffset());
        assertEquals(33, FeedPositionReportDecoder.lastAppliedJournalSeqEncodingOffset());
        assertEquals(41, FeedPositionReportDecoder.feedHaltedEncodingOffset());
        assertEquals(42, FeedPositionReportDecoder.haltReasonEncodingOffset());

        assertEquals(24, CoreConfigDecoder.BLOCK_LENGTH);
        assertEquals(16, CoreConfigDecoder.genesisIdEncodingOffset());
        assertEquals(4, CoreConfigDecoder.genesisIdId());

        assertEquals(167, OrderEventDecoder.BLOCK_LENGTH);
        assertEquals(166, OrderEventDecoder.meReasonEncodingOffset());
        assertEquals(28, OrderEventDecoder.meReasonId());
        assertEquals(167, OrderEventBatchDecoder.EventsDecoder.sbeBlockLength());
        assertEquals(166, OrderEventBatchDecoder.EventsDecoder.meReasonEncodingOffset());
        assertEquals("103 at cap 48 inside the 8 KiB scratch", 8028, HEADER + 4 + 48 * OrderEventBatchDecoder.EventsDecoder.sbeBlockLength());

        assertEquals(89, MeDispatchDecoder.BLOCK_LENGTH);
        assertEquals(80, MeDispatchDecoder.releaseScopeEncodingOffset());
        assertEquals(81, MeDispatchDecoder.expectedOldRemainingEncodingOffset());
        assertEquals(89, MeDispatchBatchDecoder.DispatchesDecoder.sbeBlockLength());
        assertEquals(80, MeDispatchBatchDecoder.DispatchesDecoder.releaseScopeEncodingOffset());
        assertEquals(81, MeDispatchBatchDecoder.DispatchesDecoder.expectedOldRemainingEncodingOffset());

        assertEquals(27, AdmissionStateDecoder.BLOCK_LENGTH);
        assertEquals(23, AdmissionStateDecoder.gateReasonsEncodingOffset());
        assertEquals(4, GateReasonsDecoder.ENCODED_LENGTH);
    }

    /** 4.1: wire enum values, and the domain enums carry the same codes (the publisher maps by code). */
    @Test
    public void enumValuesAndDomainCodes() {
        assertEquals(7, SessionClass.REPLICA.value());
        assertEquals(7, com.openexchange.assets.infrastructure.generated.GateReason.ME_LEG_LIVE.value());
        assertEquals(8, com.openexchange.assets.infrastructure.generated.GateReason.ME_BEHAVIOR.value());
        for (final GateReason g : GateReason.values()) {
            assertEquals(g.name(), g.code(), com.openexchange.assets.infrastructure.generated.GateReason.valueOf(g.name()).value());
            assertEquals(g, GateReason.fromCode(g.code()));
        }
        assertEquals(9, GateReason.values().length);
        assertEquals(null, GateReason.fromCode(9));

        assertEquals(40, CoreRejectReason.FEED_HALTED.code());
        assertEquals(41, CoreRejectReason.ME_LANE_INACTIVE.code());
        assertEquals(42, CoreRejectReason.ME_INVALID.code());
        for (final CoreRejectReason c : CoreRejectReason.values()) {
            assertEquals(c.name(), c.code(), com.openexchange.assets.infrastructure.generated.CoreRejectReason.valueOf(c.name()).value());
        }
        for (final OrderFaultKind k : OrderFaultKind.values()) {
            assertEquals(k.name(), k.code(), com.openexchange.assets.infrastructure.generated.OrderFaultKind.valueOf(k.name()).value());
        }
        assertEquals(6, OrderFaultKind.FEED_HALT.code());
        for (final SettleRejectReason r : SettleRejectReason.values()) {
            assertEquals(r.name(), r.ordinal(), com.openexchange.assets.infrastructure.generated.SettleRejectReason.valueOf(r.name()).value());
        }
        assertEquals(8, com.openexchange.assets.infrastructure.generated.SettleRejectReason.FEED_UNBOUND.value());

        assertEquals(6, FeedHaltReason.values().length);
        for (final FeedHaltReason f : FeedHaltReason.values()) {
            assertEquals(f.name(), f.code(), com.openexchange.assets.infrastructure.generated.FeedHaltReason.valueOf(f.name()).value());
        }
        assertEquals(5, FeedHaltReason.SETTLE_REFUSED.code());
        for (final ReleaseScope v : ReleaseScope.values()) {
            assertEquals(v.name(), v.code(), com.openexchange.assets.infrastructure.generated.ReleaseScope.valueOf(v.name()).value());
        }
        assertEquals(2, ReleaseScope.values().length);
        for (final ReleaseResult v : ReleaseResult.values()) {
            assertEquals(v.name(), v.code(), com.openexchange.assets.infrastructure.generated.ReleaseResult.valueOf(v.name()).value());
        }
        assertEquals(3, ReleaseResult.values().length);
        for (final ConfigDecision v : ConfigDecision.values()) {
            assertEquals(v.name(), v.code(), com.openexchange.assets.infrastructure.generated.ConfigDecision.valueOf(v.name()).value());
        }
        assertEquals(4, ConfigDecision.values().length);
        assertEquals(null, ConfigDecision.fromCode(4));

        // GateReasons: one choice per GateReason value, bit = value
        assertTrue(GateReasonsDecoder.admin(1));
        assertTrue(GateReasonsDecoder.applyFault(1 << 6));
        assertTrue(GateReasonsDecoder.meLegLive(1 << 7));
        assertTrue(GateReasonsDecoder.meBehavior(1 << 8));
    }

    /** A v6 producer's frames (v6 header, v6 block lengths) are dropped by the exact v7 gate, counted, never decoded. */
    @Test
    public void v6ShapedFramesAreSchemaRejects() {
        final AssetsEngine engine = new AssetsEngine();
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[512]);
        // {templateId, v6 root block}
        final int[][] v6 = {{61, 101}, {63, 53}, {64, 124}, {70, 16}};
        for (final int[] t : v6) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            new MessageHeaderEncoder().wrap(buf, 0).blockLength(t[1]).templateId(t[0])
                    .schemaId(MessageHeaderEncoder.SCHEMA_ID).version(6);
            final int len = HEADER + t[1] + (t[0] == 70 ? 8 : 0);
            demuxer.dispatchTrusted(buf, 0, len, 1L);
        }
        final int len = HEADER + new DepositEncoder().wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .correlationId(1).userId(1).assetId(0).amount(1).requestIdHigh(0).requestIdLow(0).encodedLength();
        buf.putShort(6, (short) 6, ByteOrder.LITTLE_ENDIAN);
        demuxer.dispatchTrusted(buf, 0, len, 1L);
        assertEquals(5, demuxer.schemaRejectCount());
        assertEquals(0, demuxer.decodeRejectCount());
        assertEquals(0, engine.accountCount());
    }

    /** The v6 fields of a v7 frame are where a v6 decoder reads them: the prefix bytes equal the v6 layout. */
    @Test
    public void v7FramesKeepTheV6PrefixByteForByte() {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
        final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .egressSeq(1).tradeId(2).marketId(3).takerOrderId(4).takerUserId(5).makerOrderId(6).makerUserId(7)
                .price(8).quantity(9).takerIsBuy(BoolFlag.TRUE).meTimestamp(10).takerOmsOrderId(11).makerOmsOrderId(12)
                .meEpoch(13).journalSeq(14);
        assertEquals(109, e.encodedLength());
        // the v6 layout (block 101): meEpoch was the last field at 93
        assertEquals(93, MeJournalTradeDecoder.meEpochEncodingOffset());
        assertEquals(13L, buf.getLong(HEADER + 93, ByteOrder.LITTLE_ENDIAN));
        assertEquals(14L, buf.getLong(HEADER + 101, ByteOrder.LITTLE_ENDIAN));
        final MeJournalTradeDecoder d = new MeJournalTradeDecoder().wrapAndApplyHeader(buf, 0, new MessageHeaderDecoder());
        assertEquals(2L, d.tradeId());
        assertEquals(12L, d.makerOmsOrderId());
        assertEquals(13L, d.meEpoch());
        assertEquals(14L, d.journalSeq());
        // the other appended templates: the last v6 field keeps its v6 offset (taken from the a9a865d codecs) and the
        // first v7 field starts at the v6 block length
        final int[][] lastV6AndFirstV7 = {
            // {last v6 field offset (v6), its v7 offset, v6 block, first v7 field offset}
            {116, MeCommandOutcomeDecoder.meEpochEncodingOffset(), 124, MeCommandOutcomeDecoder.journalSeqEncodingOffset()},
            {45, MeJournalTerminalDecoder.meEpochEncodingOffset(), 53, MeJournalTerminalDecoder.journalSeqEncodingOffset()},
            {24, FeedPositionReportDecoder.journalEnabledEncodingOffset(), 25, FeedPositionReportDecoder.meEpochEncodingOffset()},
            {15, AdmissionStateDecoder.logPositionEncodingOffset(), 23, AdmissionStateDecoder.gateReasonsEncodingOffset()},
            {158, OrderEventDecoder.updatedAtMsEncodingOffset(), 166, OrderEventDecoder.meReasonEncodingOffset()},
            {79, MeDispatchDecoder.timeInForceEncodingOffset(), 80, MeDispatchDecoder.releaseScopeEncodingOffset()},
            {12, CoreConfigDecoder.behaviorVersionEncodingOffset(), 16, CoreConfigDecoder.genesisIdEncodingOffset()},
        };
        for (final int[] row : lastV6AndFirstV7) {
            assertEquals("v6 prefix unchanged", row[0], row[1]);
            assertEquals("v7 appended after the v6 block", row[2], row[3]);
        }
    }
}

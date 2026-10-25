// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderFaultDecoder;
import com.openexchange.assets.infrastructure.generated.OrderSnapshotBatchDecoder;
import com.openexchange.assets.infrastructure.generated.OrderSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.PendingDispatchEndDecoder;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 wire seams the engine-level suites do not pin: MeJournalTradeBatch (62) and MeJournalTrade (61) through
 * the demuxer's K5 fence into the order engine (contract 3.1), and the S5 egress frames (OrderFault 113,
 * OrderSnapshotBatch 106, OrderSnapshotEnd 107, PendingDispatchEnd 108; contract section 4) through the publisher
 * and back off the wire. Modelled on {@link IngressFenceTest}'s rig.
 */
public class MeJournalBatchDemuxerTest {

    private static final long TS = 1_700_000_000_000L;
    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final long ONE = 100_000_000L;
    private static final int USD = 0;
    private static final int BTC = 1;
    private static final int MKT = 1;
    private static final long PX = 100 * ONE;
    private static final long LEG_BUY = 101L;
    private static final long LEG_SELL = 202L;

    /** Engine + projector + demuxer + recording sink, with the order gate open and a crossed BUY/SELL pair. */
    private static final class Rig {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final SettlementProjector projector = new SettlementProjector(engine);
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, projector);
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[8192]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        private long nextRequest = 1;
        int mark;

        Rig() {
            engine.setEventSink(sink);
            engine.gate().set(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        }

        void deposit(final long user, final int asset, final long amount) {
            final DepositCommand d = new DepositCommand();
            d.reset();
            d.setCorrelationId(0);
            d.setUserId(user);
            d.setAssetId(asset);
            d.setAmount(amount);
            engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, TS);
        }

        long limit(final long user, final int side, final long price, final long qty) {
            final CreateOrderCommand c = new CreateOrderCommand();
            c.reset();
            c.correlationId = 7_000 + nextRequest;
            c.userId = user;
            c.marketId = MKT;
            c.side = side;
            c.orderType = 0;
            c.timeInForce = 0;
            c.price = price;
            c.quantity = qty;
            c.requestIdHigh = 0xABCDL;
            c.requestIdLow = nextRequest++;
            engine.orders().applyCreate(c, TS);
            return engine.getNextOrderId() - 1;
        }

        /** CREATE APPLIED outcome for attempt 0 (contract 3.3): the core learns the leg. */
        void ack(final long orderId, final long userId, final long legId) {
            engine.orders().applyOutcome(0L, orderId, 0L, userId, orderId, 0L, 0L, 0L, 0L, MKT, 0, 0, legId, 0, TS, 0L, TS);
        }

        /** One MeJournalTrade (61): order 2 (SELL, leg 202, user 2) takes against resting order 1 (BUY, leg 101, user 1). */
        int trade(final long tradeId, final long qty) {
            final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, header)
                    .egressSeq(1_000 + tradeId).tradeId(tradeId).marketId(MKT)
                    .takerOrderId(LEG_SELL).takerUserId(2).makerOrderId(LEG_BUY).makerUserId(1)
                    .price(PX).quantity(qty).takerIsBuy(BoolFlag.FALSE).meTimestamp(TS)
                    .takerOmsOrderId(2).makerOmsOrderId(1).meEpoch(1);
            return HEADER + e.encodedLength();
        }

        /** A MeJournalTradeBatch (62) of {@code n} dense trades from {@code firstTradeId}, same parties as {@link #trade}. */
        int tradeBatch(final int n, final long firstTradeId, final long qty) {
            final MeJournalTradeBatchEncoder e = new MeJournalTradeBatchEncoder().wrapAndApplyHeader(buf, 0, header);
            final MeJournalTradeBatchEncoder.TradesEncoder g = e.tradesCount(n);
            for (int i = 0; i < n; i++) {
                final long id = firstTradeId + i;
                g.next().egressSeq(1_000 + id).tradeId(id).marketId(MKT)
                        .takerOrderId(LEG_SELL).takerUserId(2).makerOrderId(LEG_BUY).makerUserId(1)
                        .price(PX).quantity(qty).takerIsBuy(BoolFlag.FALSE).meTimestamp(TS)
                        .takerOmsOrderId(2).makerOmsOrderId(1).meEpoch(1);
            }
            return HEADER + e.encodedLength();
        }

        /** Byte offset of entry {@code i}'s takerIsBuy inside a MeJournalTradeBatch frame at offset 0. */
        static int batchTakerIsBuyOffset(final int i) {
            return HEADER + MeJournalTradeBatchEncoder.BLOCK_LENGTH + MeJournalTradeBatchEncoder.TradesEncoder.sbeHeaderSize()
                    + i * MeJournalTradeBatchEncoder.TradesEncoder.sbeBlockLength()
                    + MeJournalTradeBatchEncoder.TradesEncoder.takerIsBuyEncodingOffset();
        }

        void dispatch(final int length) {
            demuxer.dispatchTrusted(buf, 0, length, TS);
        }

        void markNow() {
            mark = sink.events().size();
        }

        List<AssetsEngineEvent> events() {
            return sink.events().subList(mark, sink.events().size());
        }

        long count(final Class<? extends AssetsEngineEvent> type) {
            return events().stream().filter(type::isInstance).count();
        }

        List<AssetsEngineEvent.OrderEvent> fills(final long orderId) {
            final List<AssetsEngineEvent.OrderEvent> out = new ArrayList<>();
            for (final AssetsEngineEvent e : events()) {
                if (e instanceof AssetsEngineEvent.OrderEvent oe && oe.orderId() == orderId && oe.kind() == EventKind.FILL) {
                    out.add(oe);
                }
            }
            return out;
        }

        List<Long> settledTradeIds() {
            final List<Long> out = new ArrayList<>();
            for (final AssetsEngineEvent e : events()) {
                if (e instanceof AssetsEngineEvent.Settled s) {
                    out.add(s.tradeId());
                }
            }
            return out;
        }
    }

    /** user 1: 1000 USD, BUY LIMIT 100 x qty (order 1, leg 101); user 2: 10 BTC, SELL LIMIT 100 x qty (order 2, leg 202). */
    private static Rig crossedPair(final long qty) {
        final Rig r = new Rig();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        assertEquals(1, r.limit(1, 0, PX, qty));
        assertEquals(2, r.limit(2, 1, PX, qty));
        r.ack(1, 1, LEG_BUY);
        r.ack(2, 2, LEG_SELL);
        assertEquals("precondition (3.3): order 1 learned leg 101", LEG_BUY, r.engine.orders().orders().get(1).currentLegId);
        assertEquals("precondition (3.3): order 2 learned leg 202", LEG_SELL, r.engine.orders().orders().get(2).currentLegId);
        assertEquals(0, r.engine.getLastAppliedTradeId());
        r.markNow();
        return r;
    }

    /** Capturing egress: copies every frame (the publisher reuses its scratch) with its channel and routing. */
    private static final class CapturingEgress implements AssetsEventPublisher.Egress {
        final List<byte[]> frames = new ArrayList<>();
        final List<Integer> channels = new ArrayList<>();
        final List<AssetsEventPublisher.Routing> routings = new ArrayList<>();

        @Override
        public void emit(final MutableDirectBuffer buffer, final int offset, final int length, final int channel,
                         final AssetsEventPublisher.Routing routing) {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            frames.add(copy);
            channels.add(channel);
            routings.add(routing);
        }

        UnsafeBuffer frame(final int i) {
            return new UnsafeBuffer(frames.get(i));
        }

        int templateId(final int i) {
            return new MessageHeaderDecoder().wrap(frame(i), 0).templateId();
        }
    }

    // contract 3.1 via 62: three dense trades in one batch apply in group order, money first, then both legs
    @Test
    public void tradeBatchOfThreeDenseTradesAppliesAllThreeInGroupOrder() {
        final Rig r = crossedPair(3 * ONE);
        r.dispatch(r.tradeBatch(3, 1, ONE));
        assertEquals(0, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals("the resume key advanced through the whole group", 3, r.engine.getLastAppliedTradeId());
        assertEquals("SETTLED x3 in group order", List.of(1L, 2L, 3L), r.settledTradeIds());
        for (final long orderId : new long[] {1, 2}) {
            final List<AssetsEngineEvent.OrderEvent> fills = r.fills(orderId);
            assertEquals("one FILL per trade on order " + orderId + ", got " + r.events(), 3, fills.size());
            for (int i = 0; i < 3; i++) {
                assertEquals("trade id in group order", i + 1, fills.get(i).tradeId());
                assertEquals(ONE, fills.get(i).lastQty());
                assertEquals(PX, fills.get(i).lastPrice());
                assertEquals((i + 1) * ONE, fills.get(i).filledQty());
                assertEquals((2 - i) * ONE, fills.get(i).remainingQty());
                assertEquals(i < 2 ? OrderStatus.PARTIALLY_FILLED : OrderStatus.FILLED, fills.get(i).status());
            }
            assertEquals("the terminal FILL event carries holdRemaining 0 (3.8 #12)", 0L, fills.get(2).holdRemaining());
            assertNull("FILLED removes the record (3.1 step 3)", r.engine.orders().orders().get(orderId));
        }
        assertEquals("user 1 paid 300 USD and holds 3 BTC", 700 * ONE, r.engine.account(1).available(USD));
        assertEquals(3 * ONE, r.engine.account(1).available(BTC));
        assertEquals(0L, r.engine.account(1).locked(USD));
        assertEquals("user 2 sold 3 BTC for 300 USD", 300 * ONE, r.engine.account(2).available(USD));
        assertEquals(7 * ONE, r.engine.account(2).available(BTC));
        assertEquals(0L, r.engine.account(2).locked(BTC));
        assertEquals(3 * ONE, r.engine.orders().user(1).position[MKT]);
        assertEquals(-3 * ONE, r.engine.orders().user(2).position[MKT]);
        assertEquals(0, r.engine.orders().orderFaultCount());
    }

    // K5 at the 62 boundary: an invalid enum byte in the SECOND entry refuses the whole batch before entry 0 is applied
    @Test
    public void poisonInTheSecondEntryOfATradeBatchRefusesTheWholeBatchBeforeAnyEntryApplies() {
        final Rig r = crossedPair(3 * ONE);
        final int len = r.tradeBatch(3, 1, ONE);
        r.buf.putByte(Rig.batchTakerIsBuyOffset(1), (byte) 7);
        r.dispatch(len);
        assertEquals("a decode reject", 1, r.demuxer.decodeRejectCount());
        assertEquals("not an apply failure", 0, r.demuxer.applyErrorCount());
        assertEquals("entry 0 must NOT have been applied", 0, r.engine.getLastAppliedTradeId());
        assertEquals(0, r.count(AssetsEngineEvent.Settled.class));
        assertEquals("nothing emitted at all, got " + r.events(), 0, r.events().size());
        final CoreOrder o1 = r.engine.orders().orders().get(1);
        assertNotNull(o1);
        assertEquals(0L, o1.filledQty);
        assertEquals(3 * ONE, o1.remainingQty);
        assertEquals(OrderStatus.NEW.code(), o1.status);
        assertEquals("money untouched", 300 * ONE, r.engine.account(1).locked(USD));
        assertEquals(700 * ONE, r.engine.account(1).available(USD));
        assertEquals(3 * ONE, r.engine.account(2).locked(BTC));
        assertEquals(0L, r.engine.orders().user(1).position[MKT]);
        // the healthy engine continues: the clean batch applies in full
        r.dispatch(r.tradeBatch(3, 1, ONE));
        assertEquals(3, r.engine.getLastAppliedTradeId());
        assertEquals(3, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(1, r.demuxer.decodeRejectCount());
    }

    // K5 at the 61 boundary: an invalid takerIsBuy byte on a single trade is a decode reject, never a throw or a half-apply
    @Test
    public void singleTradeWithAnInvalidTakerIsBuyByteIsADecodeReject() {
        final Rig r = crossedPair(ONE);
        final int len = r.trade(1, ONE);
        r.buf.putByte(HEADER + MeJournalTradeEncoder.takerIsBuyEncodingOffset(), (byte) 7);
        r.dispatch(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(0, r.engine.getLastAppliedTradeId());
        assertEquals(0, r.count(AssetsEngineEvent.Settled.class));
        assertEquals("nothing emitted, got " + r.events(), 0, r.events().size());
        assertEquals(0L, r.engine.orders().orders().get(1).filledQty);
        assertEquals(100 * ONE, r.engine.account(1).locked(USD));
        // the healthy engine continues: the clean frame applies
        r.dispatch(r.trade(1, ONE));
        assertEquals(1, r.engine.getLastAppliedTradeId());
        assertEquals(List.of(1L), r.settledTradeIds());
        assertNull("1 x 1 fills order 1 completely", r.engine.orders().orders().get(1));
        assertEquals(1, r.demuxer.decodeRejectCount());
    }

    // contract section 4: OrderFault (113) on CH_CONTROL, BROADCAST, field for field
    @Test
    public void orderFaultReachesTheWireAsTemplate113OnControlBroadcast() {
        final CapturingEgress egress = new CapturingEgress();
        final AssetsEventPublisher p = new AssetsEventPublisher(egress, () -> false);
        p.onOrderFault(OrderFaultKind.SETTLE, 7L, 3L, MKT, 42L, 5 * ONE, 99L);
        assertEquals("one frame", 1, egress.frames.size());
        assertEquals(OrderFaultDecoder.TEMPLATE_ID, egress.templateId(0));
        assertEquals(113, egress.templateId(0));
        final MessageHeaderDecoder h = new MessageHeaderDecoder().wrap(egress.frame(0), 0);
        assertEquals(OrderFaultDecoder.SCHEMA_ID, h.schemaId());
        assertEquals(OrderFaultDecoder.SCHEMA_VERSION, h.version());
        final OrderFaultDecoder d = new OrderFaultDecoder().wrapAndApplyHeader(egress.frame(0), 0, h);
        assertEquals(com.openexchange.assets.infrastructure.generated.OrderFaultKind.SETTLE, d.kind());
        assertEquals(7L, d.orderId());
        assertEquals(3L, d.userId());
        assertEquals(MKT, d.marketId());
        assertEquals(42L, d.tradeId());
        assertEquals(5 * ONE, d.amount());
        assertEquals(99L, d.logPosition());
        assertEquals("CH_CONTROL", AssetsEventPublisher.CH_CONTROL, (int) egress.channels.get(0));
        assertEquals("BROADCAST", AssetsEventPublisher.Routing.BROADCAST, egress.routings.get(0));
        // every domain kind maps onto its wire code
        for (final OrderFaultKind k : OrderFaultKind.values()) {
            p.onOrderFault(k, 1L, 1L, MKT, 0L, 0L, 0L);
            final int i = egress.frames.size() - 1;
            final OrderFaultDecoder dk = new OrderFaultDecoder().wrapAndApplyHeader(egress.frame(i), 0, new MessageHeaderDecoder().wrap(egress.frame(i), 0));
            assertEquals("wire code of " + k, k.code(), dk.kind().value());
        }
    }

    private static CoreOrder[] page(final int n) {
        final CoreOrder[] page = new CoreOrder[n];
        for (int i = 0; i < n; i++) {
            final CoreOrder o = new CoreOrder();
            o.clear();
            o.orderId = i + 1;
            o.userId = 1 + (i % 3);
            o.marketId = MKT;
            o.status = (byte) (i % 2 == 0 ? OrderStatus.NEW.code() : OrderStatus.PARTIALLY_FILLED.code());
            o.side = (byte) (i % 2);
            o.price = (100 + i) * ONE;
            o.quantity = (i + 1) * ONE;
            o.filledQty = i % 2 == 0 ? 0L : ONE / 2;
            o.remainingQty = o.quantity - o.filledQty;
            o.holdAssetId = i % 2 == 0 ? USD : BTC;
            o.holdAmount = (i + 1) * 10 * ONE;
            o.currentLegId = 1_000 + i;
            o.attempt = i % 3;
            o.clientOrderIdHigh = i;
            o.clientOrderIdLow = 2L * i;
            o.createdAtMs = TS;
            o.updatedAtMs = TS + i;
            page[i] = o;
        }
        return page;
    }

    // contract 3.6 "publisher: 106 frames of <= 56" on CH_SNAPSHOTS ORIGIN: 60 entries = one frame of 56 and one of 4
    @Test
    public void orderSnapshotBatchOfSixtyEntriesIsChunkedIntoFramesOf56And4OnSnapshotsOrigin() {
        final CapturingEgress egress = new CapturingEgress();
        final AssetsEventPublisher p = new AssetsEventPublisher(egress, () -> false);
        final CoreOrder[] page = page(60);
        p.onOrderSnapshotBatch(0x5A, page, 60);
        assertEquals("two frames", 2, egress.frames.size());
        final int[] expectedCounts = {56, 4};
        int next = 0;
        for (int f = 0; f < 2; f++) {
            assertEquals(OrderSnapshotBatchDecoder.TEMPLATE_ID, egress.templateId(f));
            assertEquals("CH_SNAPSHOTS", AssetsEventPublisher.CH_SNAPSHOTS, (int) egress.channels.get(f));
            assertEquals("ORIGIN", AssetsEventPublisher.Routing.ORIGIN, egress.routings.get(f));
            final UnsafeBuffer frame = egress.frame(f);
            final OrderSnapshotBatchDecoder d = new OrderSnapshotBatchDecoder().wrapAndApplyHeader(frame, 0, new MessageHeaderDecoder().wrap(frame, 0));
            final OrderSnapshotBatchDecoder.OrdersDecoder g = d.orders();
            assertEquals("frame " + f + " entry count", expectedCounts[f], g.count());
            while (g.hasNext()) {
                g.next();
                final CoreOrder o = page[next++];
                assertEquals("orderId in page order", o.orderId, g.orderId());
                assertEquals(o.userId, g.userId());
                assertEquals(o.marketId, g.marketId());
                assertEquals(o.status, (byte) g.status().value());
                assertEquals(o.side, (byte) g.side().value());
                assertEquals(o.price, g.price());
                assertEquals(o.quantity, g.quantity());
                assertEquals(o.filledQty, g.filledQty());
                assertEquals(o.remainingQty, g.remainingQty());
                assertEquals(o.holdAssetId, g.holdAssetId());
                assertEquals(o.holdAmount, g.holdAmount());
                assertEquals(o.currentLegId, g.meOrderId());
                assertEquals(o.attempt, g.sliceSeq());
                assertEquals(o.clientOrderIdHigh, g.clientOrderIdHigh());
                assertEquals(o.clientOrderIdLow, g.clientOrderIdLow());
                assertEquals(o.updatedAtMs, g.updatedAtMs());
            }
        }
        assertEquals("all 60 entries went out", 60, next);
        // exactly 56 is one frame; 57 is two
        p.onOrderSnapshotBatch(0x5A, page, 56);
        assertEquals(3, egress.frames.size());
        p.onOrderSnapshotBatch(0x5A, page, 57);
        assertEquals(5, egress.frames.size());
        final UnsafeBuffer last = egress.frame(4);
        assertEquals(1, new OrderSnapshotBatchDecoder().wrapAndApplyHeader(last, 0, new MessageHeaderDecoder().wrap(last, 0)).orders().count());
    }

    // contract section 4: OrderSnapshotEnd (107) and PendingDispatchEnd (108) decode back exactly, ORIGIN
    @Test
    public void pendingDispatchEndAndOrderSnapshotEndDecodeBackExactly() {
        final CapturingEgress egress = new CapturingEgress();
        final AssetsEventPublisher p = new AssetsEventPublisher(egress, () -> false);
        p.onOrderSnapshotEnd(0x5B, 60, 60L, 777L);
        assertEquals(1, egress.frames.size());
        assertEquals(OrderSnapshotEndDecoder.TEMPLATE_ID, egress.templateId(0));
        assertEquals(107, egress.templateId(0));
        final OrderSnapshotEndDecoder se = new OrderSnapshotEndDecoder().wrapAndApplyHeader(egress.frame(0), 0, new MessageHeaderDecoder().wrap(egress.frame(0), 0));
        assertEquals(0x5B, se.correlationId());
        assertEquals(60, se.entryCount());
        assertEquals(60L, se.nextOrderId());
        assertEquals(777L, se.asOfLogPosition());
        assertEquals("CH_SNAPSHOTS", AssetsEventPublisher.CH_SNAPSHOTS, (int) egress.channels.get(0));
        assertEquals("ORIGIN", AssetsEventPublisher.Routing.ORIGIN, egress.routings.get(0));

        p.onMeDispatchReply(5L, 9L, 1L, DispatchKind.CANCEL, 1L, 9L, 77L, MKT, 0, 0, 0L, 0L, 0L, 0);
        p.onPendingDispatchEnd(0x5C, 7, 123L);
        assertEquals(3, egress.frames.size());
        assertEquals(PendingDispatchEndDecoder.TEMPLATE_ID, egress.templateId(2));
        assertEquals(108, egress.templateId(2));
        final PendingDispatchEndDecoder pe = new PendingDispatchEndDecoder().wrapAndApplyHeader(egress.frame(2), 0, new MessageHeaderDecoder().wrap(egress.frame(2), 0));
        assertEquals(0x5C, pe.correlationId());
        assertEquals(7, pe.count());
        assertEquals(123L, pe.headDispatchSeq());
        assertEquals("ORIGIN", AssetsEventPublisher.Routing.ORIGIN, egress.routings.get(2));
        assertEquals("the end frame rides the same channel as the DISPATCHREPLY frames it terminates",
                egress.channels.get(1), egress.channels.get(2));
        assertEquals("the DISPATCHREPLY frame is ORIGIN too", AssetsEventPublisher.Routing.ORIGIN, egress.routings.get(1));
        assertTrue("a zero page still has an end frame",
                decodesPendingEnd(p, egress, 0x5D, 0, 0L));
    }

    private static boolean decodesPendingEnd(final AssetsEventPublisher p, final CapturingEgress egress, final long corr, final int count, final long head) {
        p.onPendingDispatchEnd(corr, count, head);
        final int i = egress.frames.size() - 1;
        final PendingDispatchEndDecoder d = new PendingDispatchEndDecoder().wrapAndApplyHeader(egress.frame(i), 0, new MessageHeaderDecoder().wrap(egress.frame(i), 0));
        return d.correlationId() == corr && d.count() == count && d.headDispatchSeq() == head;
    }
}

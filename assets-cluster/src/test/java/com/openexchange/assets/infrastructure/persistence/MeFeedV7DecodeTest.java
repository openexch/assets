// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderStatus;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S1 (contract 4.2, 4.7, 8.3): the v7 tails of the ME feed templates 61-64 are decoded and range-checked, and
 * consumed by nobody yet. MeCommandOutcome results 7 RELEASED and 8 INVALID reach {@code applyOutcome} and land in its
 * existing default branch (counted stale, no mutation, no egress) until RC2 S8; result 9 and timeInForce 3 are decode
 * poison. MeJournalTerminal.rejectReason is the ME code verbatim, 255 is poison. Any journalSeq on 61/62 applies
 * exactly as today. Behaviour-free: every comparison is against the same frame with the S1 placeholder values.
 */
public class MeFeedV7DecodeTest {

    private static final long TS = 1_700_000_000_000L;
    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final long ONE = 100_000_000L;
    private static final int USD = 0;
    private static final int BTC = 1;
    private static final int MKT = 1;
    private static final long PX = 100 * ONE;
    private static final long LEG_BUY = 101L;
    private static final long LEG_SELL = 202L;

    private static final class Rig {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
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
            d.setUserId(user);
            d.setAssetId(asset);
            d.setAmount(amount);
            engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, TS);
        }

        long limit(final long user, final int side, final long qty) {
            final CreateOrderCommand c = new CreateOrderCommand();
            c.reset();
            c.correlationId = 7_000 + nextRequest;
            c.userId = user;
            c.marketId = MKT;
            c.side = side;
            c.price = PX;
            c.quantity = qty;
            c.requestIdHigh = 0xABCDL;
            c.requestIdLow = nextRequest++;
            engine.orders().applyCreate(c, TS);
            return engine.getNextOrderId() - 1;
        }

        /** A MeCommandOutcome (64) for (orderId, attempt 0, CREATE) with every v7 tail field set. */
        int outcome(final long orderId, final long userId, final long legId, final int result, final int tif) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            final MeCommandOutcomeEncoder e = new MeCommandOutcomeEncoder().wrapAndApplyHeader(buf, 0, header)
                    .egressSeq(5).commandIdHigh(orderId).commandIdLow(0).userId(userId).omsOrderId(orderId).oldOrderId(0)
                    .price(PX).quantity(ONE).budget(0).marketId(MKT).commandKind((short) 0).orderType((short) 0)
                    .orderSide((short) 0).appliedPosition(5).meTimestamp(TS).orderId(legId).status(0).reason(0)
                    .oldCancelled((short) 0).result(result).meEpoch(1)
                    .journalSeq(9).timeInForce((short) tif).expectedOldRemaining(0).ledgerSize(1).ledgerCapacity(100_000)
                    .behaviorVersion(2);
            return HEADER + e.encodedLength();
        }

        int terminal(final long leg, final long user, final int status, final int rejectReason) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            final MeJournalTerminalEncoder e = new MeJournalTerminalEncoder().wrapAndApplyHeader(buf, 0, header)
                    .egressSeq(7).orderId(leg).userId(user).marketId(MKT).status(OrderStatus.get((short) status))
                    .meTimestamp(TS).omsOrderId(1).meEpoch(1).journalSeq(11).rejectReason((short) rejectReason);
            return HEADER + e.encodedLength();
        }

        int trade(final long tradeId, final long journalSeq) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, header)
                    .egressSeq(1_000 + tradeId).tradeId(tradeId).marketId(MKT)
                    .takerOrderId(LEG_SELL).takerUserId(2).makerOrderId(LEG_BUY).makerUserId(1)
                    .price(PX).quantity(ONE).takerIsBuy(BoolFlag.FALSE).meTimestamp(TS)
                    .takerOmsOrderId(2).makerOmsOrderId(1).meEpoch(1).journalSeq(journalSeq);
            return HEADER + e.encodedLength();
        }

        int tradeBatch(final int n, final long firstTradeId, final long journalSeq) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            final MeJournalTradeBatchEncoder e = new MeJournalTradeBatchEncoder().wrapAndApplyHeader(buf, 0, header);
            final MeJournalTradeBatchEncoder.TradesEncoder g = e.tradesCount(n);
            for (int i = 0; i < n; i++) {
                final long id = firstTradeId + i;
                g.next().egressSeq(1_000 + id).tradeId(id).marketId(MKT)
                        .takerOrderId(LEG_SELL).takerUserId(2).makerOrderId(LEG_BUY).makerUserId(1)
                        .price(PX).quantity(ONE).takerIsBuy(BoolFlag.FALSE).meTimestamp(TS)
                        .takerOmsOrderId(2).makerOmsOrderId(1).meEpoch(1).journalSeq(journalSeq + i);
            }
            return HEADER + e.encodedLength();
        }

        void dispatch(final int length) {
            demuxer.dispatchTrusted(buf, 0, length, TS);
        }

        void markNow() {
            mark = sink.events().size();
        }

        List<AssetsEngineEvent> events() {
            return new ArrayList<>(sink.events().subList(mark, sink.events().size()));
        }

        byte[] snapshot() {
            final ExpandableArrayBuffer b = new ExpandableArrayBuffer();
            final int len = BalanceSnapshotCodec.serialize(engine, b);
            final byte[] out = new byte[len];
            b.getBytes(0, out);
            return out;
        }
    }

    /** user 1 BUY order 1 still PENDING_NEW with its CREATE dispatch outstanding (attempt 0). */
    private static Rig pendingCreate() {
        final Rig r = new Rig();
        r.deposit(1, USD, 1_000 * ONE);
        assertEquals(1, r.limit(1, 0, ONE));
        assertTrue("precondition: the CREATE dispatch is pending",
                r.engine.orders().orders().get(1).has(CoreOrder.F_DISPATCH_PENDING));
        r.markNow();
        return r;
    }

    /** Orders 1 (BUY, leg 101) and 2 (SELL, leg 202) acknowledged, crossing at PX. */
    private static Rig crossedPair(final long qty) {
        final Rig r = new Rig();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        assertEquals(1, r.limit(1, 0, qty));
        assertEquals(2, r.limit(2, 1, qty));
        r.engine.orders().applyOutcome(0L, 1, 0L, 1, 1, 0L, 0L, 0L, 0L, MKT, 0, 0, LEG_BUY, 0, TS, 0L, TS);
        r.engine.orders().applyOutcome(0L, 2, 0L, 2, 2, 0L, 0L, 0L, 0L, MKT, 0, 0, LEG_SELL, 0, TS, 0L, TS);
        r.markNow();
        return r;
    }

    @Test
    public void resultsSevenAndEightOnAPendingDispatchAreCountedStaleWithNoMutation() {
        for (final int result : new int[] {7, 8}) {
            final Rig r = pendingCreate();
            final byte[] before = r.snapshot();
            final long stale = r.engine.orders().staleOutcomeCount();
            r.dispatch(r.outcome(1, 1, LEG_BUY, result, 0));
            assertEquals("result " + result + " is not poison", 0, r.demuxer.decodeRejectCount());
            assertEquals(0, r.demuxer.applyErrorCount());
            assertEquals("result " + result + " lands in applyOutcome's default branch", stale + 1,
                    r.engine.orders().staleOutcomeCount());
            assertEquals("no egress", List.of(), r.events());
            assertArrayEquals("no state change", before, r.snapshot());
            assertTrue("still pending", r.engine.orders().orders().get(1).has(CoreOrder.F_DISPATCH_PENDING));
        }
        // control: the same frame with result 0 matches the pending dispatch and acknowledges it
        final Rig r = pendingCreate();
        r.dispatch(r.outcome(1, 1, LEG_BUY, 0, 0));
        assertEquals(LEG_BUY, r.engine.orders().orders().get(1).currentLegId);
        assertTrue(r.events().stream().anyMatch(e -> e instanceof AssetsEngineEvent.OrderEvent oe && oe.kind() == EventKind.ACK_NEW));
    }

    @Test
    public void resultNineAndTimeInForceThreeAreDecodeRejects() {
        final Rig r = pendingCreate();
        final byte[] before = r.snapshot();
        final long stale = r.engine.orders().staleOutcomeCount();
        r.dispatch(r.outcome(1, 1, LEG_BUY, 9, 0));
        assertEquals(1, r.demuxer.decodeRejectCount());
        r.dispatch(r.outcome(1, 1, LEG_BUY, 0, 3));
        assertEquals(2, r.demuxer.decodeRejectCount());
        r.dispatch(r.outcome(1, 1, LEG_BUY, -1, 0));
        assertEquals(3, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals("never reached the engine", stale, r.engine.orders().staleOutcomeCount());
        assertEquals(List.of(), r.events());
        assertArrayEquals(before, r.snapshot());
        // timeInForce 0..2 all decode
        for (final int tif : new int[] {1, 2}) {
            r.dispatch(r.outcome(1, 1, LEG_BUY, 7, tif));
        }
        assertEquals(3, r.demuxer.decodeRejectCount());
        assertEquals(stale + 2, r.engine.orders().staleOutcomeCount());
    }

    @Test
    public void terminalRejectReasonIsNotConsumedAnd255IsPoison() {
        final Rig a = crossedPair(ONE);
        final Rig b = crossedPair(ONE);
        a.dispatch(a.terminal(LEG_BUY, 1, OrderStatus.CANCELLED.value(), 0));
        b.dispatch(b.terminal(LEG_BUY, 1, OrderStatus.CANCELLED.value(), 7));
        assertEquals(0, a.demuxer.decodeRejectCount());
        assertEquals(0, b.demuxer.decodeRejectCount());
        assertTrue("the terminal applied: " + a.events(), a.events().stream()
                .anyMatch(e -> e instanceof AssetsEngineEvent.OrderEvent oe && oe.kind() == EventKind.CANCELLED));
        assertEquals("rejectReason 0 and 7 apply identically", a.events(), b.events());
        assertArrayEquals(a.snapshot(), b.snapshot());

        final Rig c = crossedPair(ONE);
        final byte[] before = c.snapshot();
        c.dispatch(c.terminal(LEG_BUY, 1, OrderStatus.CANCELLED.value(), 255));
        assertEquals(1, c.demuxer.decodeRejectCount());
        assertEquals(0, c.demuxer.applyErrorCount());
        assertEquals(List.of(), c.events());
        assertArrayEquals(before, c.snapshot());
        c.dispatch(c.terminal(LEG_BUY, 1, OrderStatus.CANCELLED.value(), 254));
        assertEquals("254 is a legal ME code", 1, c.demuxer.decodeRejectCount());
        assertFalse(c.events().isEmpty());
    }

    @Test
    public void tradesApplyAsTodayWhateverTheirJournalSeq() {
        final Rig a = crossedPair(4 * ONE);
        final Rig b = crossedPair(4 * ONE);
        a.dispatch(a.trade(1, 0));
        b.dispatch(b.trade(1, Long.MAX_VALUE));
        a.dispatch(a.tradeBatch(3, 2, 0));
        b.dispatch(b.tradeBatch(3, 2, -5));
        assertEquals(0, a.demuxer.decodeRejectCount() + b.demuxer.decodeRejectCount());
        assertEquals(4, a.engine.getLastAppliedTradeId());
        assertEquals("journalSeq is not consumed in S1", a.events(), b.events());
        assertArrayEquals(a.snapshot(), b.snapshot());
    }
}

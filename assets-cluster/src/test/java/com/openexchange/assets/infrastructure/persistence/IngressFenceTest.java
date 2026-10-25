// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderType;
import com.openexchange.assets.infrastructure.generated.SetMoneyJournalEncoder;
import com.openexchange.assets.infrastructure.generated.SettleBatchEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.generated.Side;
import com.openexchange.assets.infrastructure.generated.TimeInForce;
import com.openexchange.assets.infrastructure.generated.WithdrawEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S1 (K5 fence, RED first): the ME's ingress fence (23090c6) ported to the core. Malformed wire
 * input is dropped deterministically BEFORE any application callback and counted; an exception after a
 * mutation latches the demuxer so a partially applied command can never become an applied prefix; a
 * rejected command creates no account; a Settle that would have thrown is a loud deterministic outcome.
 * Scenario ids follow RC1-core/spec/hardening-k5-k6.md B5 (RED-F1..F12).
 */
public class IngressFenceTest {

    private static final long TS = 1_000L;
    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    /** Engine + projector + demuxer + recording sink, the in-process shape every corpus test uses. */
    private static final class Rig {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final SettlementProjector projector = new SettlementProjector(engine);
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, projector);
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[1024]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();

        Rig() {
            engine.setEventSink(sink);
        }

        int deposit(long user, int asset, long amount) {
            final DepositEncoder e = new DepositEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(1).userId(user).assetId(asset).amount(amount).requestIdHigh(0).requestIdLow(0);
            return HEADER + e.encodedLength();
        }

        int hold(long user, long order, int asset, long amount) {
            final HoldEncoder e = new HoldEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(2).orderId(order).userId(user).assetId(asset).amount(amount)
                    .omsManagedRelease(BoolFlag.FALSE);
            return HEADER + e.encodedLength();
        }

        int settle(long tradeId, int market, long price, long qty, long journalPosition) {
            final SettleEncoder e = new SettleEncoder().wrapAndApplyHeader(buf, 0, header)
                    .tradeId(tradeId).marketId(market).takerOrderId(11).takerUserId(1).makerOrderId(22).makerUserId(2)
                    .price(price).quantity(qty).takerIsBuy(BoolFlag.TRUE).journalPosition(journalPosition);
            return HEADER + e.encodedLength();
        }

        void dispatch(int length) {
            demuxer.dispatch(buf, 0, length, TS);
        }

        /** Events recorded since the last {@link #markNow()}: the recorder's list is read-only. */
        int mark;

        void markNow() {
            mark = sink.events().size();
        }

        List<AssetsEngineEvent> events() {
            return sink.events().subList(mark, sink.events().size());
        }

        long count(Class<? extends AssetsEngineEvent> type) {
            return events().stream().filter(type::isInstance).count();
        }
    }

    private static Rig funded() {
        final Rig r = new Rig();
        r.dispatch(r.deposit(1, 0, 1_000_000_000L)); // user 1: 10.0 USD
        r.dispatch(r.deposit(2, 1, 1_000_000_000L)); // user 2: 10.0 BTC
        assertEquals(2, r.engine.accountCount());
        r.markNow();
        return r;
    }

    // ---- RED-F1 / F2 / bounds ----

    @Test
    public void truncatedFrameCannotReadStaleBackingBytes() {
        final Rig r = funded();
        final int full = r.hold(1, 7, 0, 100);
        r.dispatch(full); // control: the full frame places the hold
        assertEquals(1, r.count(AssetsEngineEvent.HoldAck.class));
        r.markNow();
        final int before = r.engine.accountCount();
        r.hold(1, 8, 0, 100);   // the backing buffer holds a complete Hold...
        r.dispatch(10);         // ...but THIS frame is 10 bytes long
        assertEquals("truncated frame is a decode reject", 1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(0, r.count(AssetsEngineEvent.HoldAck.class));
        assertEquals(before, r.engine.accountCount());
        assertTrue("order 8 must not be held", r.engine.account(1).holdAssetId(8) < 0);
    }

    @Test
    public void shortDeclaredBlockCannotHideFullStalePayload() {
        final Rig r = new Rig();
        final int len = r.deposit(1, 0, 100);
        r.buf.putShort(0, (short) 1, ByteOrder.LITTLE_ENDIAN); // blockLength = 1
        r.dispatch(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.engine.accountCount());
        assertTrue(r.events().isEmpty());
    }

    @Test
    public void invalidPhysicalBoundsAreDroppedBeforeHeaderAccess() {
        final Rig r = new Rig();
        final UnsafeBuffer tiny = new UnsafeBuffer(new byte[4]);
        r.demuxer.dispatch(tiny, 0, 8, TS);
        r.demuxer.dispatch(tiny, Integer.MAX_VALUE, 8, TS);
        assertEquals(2, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        r.dispatch(r.deposit(1, 0, 100)); // the healthy engine continues
        assertEquals(1, r.engine.accountCount());
    }

    @Test
    public void validFrameAtNonzeroOffsetStillApplies() {
        final Rig r = new Rig();
        final int len = r.deposit(1, 0, 100);
        final UnsafeBuffer shifted = new UnsafeBuffer(new byte[512]);
        shifted.putBytes(17, r.buf, 0, len);
        r.demuxer.dispatch(shifted, 17, len, TS);
        assertEquals(1, r.engine.accountCount());
        assertEquals(0, r.demuxer.decodeRejectCount());
    }

    // ---- RED-F3 / F4: the header gate ----

    @Test
    public void wrongSchemaIdIsDroppedBeforeAnyBodyDecode() {
        final Rig r = new Rig();
        final int len = r.deposit(1, 0, 100);
        r.buf.putShort(4, (short) 1, ByteOrder.LITTLE_ENDIAN); // schemaId = 1 (the ME's)
        r.dispatch(len);
        assertEquals(1, r.demuxer.schemaRejectCount());
        assertEquals(0, r.demuxer.decodeRejectCount());
        assertEquals(0, r.engine.accountCount());
    }

    @Test
    public void versionOutsideExactSixIsDropped() {
        // D12: the core speaks exactly v6 from RC1; a v5 producer (the v0.5.0-beta bridge/tools) is not
        // silently decoded with absent fields, it is dropped and counted.
        final Rig r = new Rig();
        for (final int version : new int[] {0, 5, 7}) {
            final int len = r.deposit(1, 0, 100);
            r.buf.putShort(6, (short) version, ByteOrder.LITTLE_ENDIAN);
            r.dispatch(len);
        }
        assertEquals(3, r.demuxer.schemaRejectCount());
        assertEquals(0, r.engine.accountCount());
        r.dispatch(r.deposit(1, 0, 100)); // version 6 applies
        assertEquals(1, r.engine.accountCount());
    }

    // ---- RED-F5: invalid enum bytes are poison, never a throw ----

    @Test
    public void invalidBoolFlagByteIsDecodePoisonNotAThrow() {
        final Rig r = funded();
        // Hold.omsManagedRelease = 2
        int len = r.hold(1, 9, 0, 100);
        r.buf.putByte(HEADER + HoldEncoder.omsManagedReleaseEncodingOffset(), (byte) 2);
        r.dispatch(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(0, r.count(AssetsEngineEvent.HoldAck.class));
        assertEquals(1_000_000_000L, r.engine.account(1).available(0));
        // SetMoneyJournal.enabled = 2
        final SetMoneyJournalEncoder sj = new SetMoneyJournalEncoder().wrapAndApplyHeader(r.buf, 0, r.header)
                .correlationId(3).enabled(BoolFlag.TRUE);
        r.buf.putByte(HEADER + SetMoneyJournalEncoder.enabledEncodingOffset(), (byte) 2);
        r.dispatch(HEADER + sj.encodedLength());
        assertEquals(2, r.demuxer.decodeRejectCount());
        assertFalse(r.engine.isMoneyJournalEnabled());
        // Settle.takerIsBuy = 2
        len = r.settle(1, 1, 100_000_000L, 100_000_000L, 5);
        r.buf.putByte(HEADER + SettleEncoder.takerIsBuyEncodingOffset(), (byte) 2);
        r.dispatch(len);
        assertEquals(3, r.demuxer.decodeRejectCount());
        assertEquals(0, r.engine.getLastAppliedTradeId());
        assertEquals(0, r.engine.getConsumePosition());
        // the healthy engine continues
        r.dispatch(r.hold(1, 9, 0, 100));
        assertEquals(1, r.count(AssetsEngineEvent.HoldAck.class));
    }

    // ---- RED-F6 / F7: SettleBatch is atomic at the decode boundary ----

    private int settleBatch(Rig r, int entries, int poisonIndex) {
        final SettleBatchEncoder e = new SettleBatchEncoder().wrapAndApplyHeader(r.buf, 0, r.header);
        final SettleBatchEncoder.SettlesEncoder g = e.settlesCount(entries);
        for (int i = 0; i < entries; i++) {
            g.next().tradeId(i + 1).marketId(1).takerOrderId(11).takerUserId(1).makerOrderId(22).makerUserId(2)
                    .price(100_000_000L).quantity(10_000_000L).takerIsBuy(BoolFlag.TRUE).journalPosition(10 + i);
        }
        final int len = HEADER + e.encodedLength();
        if (poisonIndex >= 0) {
            final int entryOffset = HEADER + SettleBatchEncoder.BLOCK_LENGTH + 4
                    + poisonIndex * SettleBatchEncoder.SettlesEncoder.sbeBlockLength();
            r.buf.putByte(entryOffset + 8 + 4 + 8 * 6, (byte) 2); // takerIsBuy follows tradeId..quantity
        }
        return len;
    }

    @Test
    public void poisonInsideASettleBatchDropsTheWholeBatch() {
        final Rig r = funded();
        r.dispatch(settleBatch(r, 3, 2));
        assertEquals("entries 0..1 must NOT have been applied", 0, r.engine.getLastAppliedTradeId());
        assertEquals(0, r.engine.getConsumePosition());
        assertEquals(0, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        r.dispatch(settleBatch(r, 3, -1)); // the clean batch applies in full
        assertEquals(3, r.engine.getLastAppliedTradeId());
        assertEquals(12, r.engine.getConsumePosition());
    }

    @Test
    public void settleBatchCountBeyondTheFrameIsAFrameBoundsReject() {
        final Rig r = funded();
        final int len = settleBatch(r, 2, -1);
        r.buf.putShort(HEADER + SettleBatchEncoder.BLOCK_LENGTH + 2, (short) 0xFFFF, ByteOrder.LITTLE_ENDIAN);
        r.dispatch(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.engine.getLastAppliedTradeId());
        assertEquals(0, r.count(AssetsEngineEvent.Settled.class));
    }

    // ---- RED-F8: a Settle that used to throw is a loud deterministic outcome ----

    @Test
    public void settleWithUnknownMarketIsRejectedLoudlyNotThrown() {
        final Rig r = funded();
        r.dispatch(r.settle(1, 9, 100_000_000L, 100_000_000L, 77));
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(0, r.demuxer.decodeRejectCount());
        assertEquals(1, r.engine.getSettleRejectCount());
        assertEquals("a refused settle must not advance the journal cursor", 0, r.engine.getConsumePosition());
        assertEquals(0, r.engine.getLastAppliedTradeId());
        final AssetsEngineEvent.SettleRejected rejected = (AssetsEngineEvent.SettleRejected) r.events().get(0);
        assertEquals(SettleRejectReason.UNKNOWN_MARKET, rejected.reason());
        assertEquals(1, rejected.tradeId());
        assertEquals(77, rejected.journalPosition());
    }

    @Test
    public void settleNotionalOverflowAndBadAmountsAreRejectedLoudly() {
        final Rig r = funded();
        r.dispatch(r.settle(1, 1, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2, 1));
        r.dispatch(r.settle(2, 1, 100_000_000L, 0, 2));
        r.dispatch(r.settle(3, 1, -5, 100_000_000L, 3));
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(3, r.engine.getSettleRejectCount());
        assertEquals(0, r.engine.getConsumePosition());
        assertEquals(SettleRejectReason.NOTIONAL_OVERFLOW, ((AssetsEngineEvent.SettleRejected) r.events().get(0)).reason());
        assertEquals(SettleRejectReason.INVALID_QUANTITY, ((AssetsEngineEvent.SettleRejected) r.events().get(1)).reason());
        assertEquals(SettleRejectReason.INVALID_PRICE, ((AssetsEngineEvent.SettleRejected) r.events().get(2)).reason());
        assertEquals(2, r.engine.accountCount()); // no account created for the refused trades' parties
    }

    // ---- RED-F9: a rejected command creates no account ----

    @Test
    public void outOfRangeAssetIdIsRejectedAtBothLayersAndCreatesNoAccount() {
        final Rig r = new Rig();
        r.dispatch(r.deposit(5, 6, 1)); // demuxer: assetId 6 is outside [0, Asset.count())
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(0, r.engine.accountCount());
        // engine level, demuxer bypassed: a reject, never an AIOOBE, and still no account
        final DepositCommand c = new DepositCommand();
        c.reset();
        c.setUserId(5);
        c.setAssetId(6);
        c.setAmount(1);
        r.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, c, TS);
        assertEquals(0, r.engine.accountCount());
        assertTrue(r.events().isEmpty());
    }

    @Test
    public void rejectedDepositWithdrawAndHoldCreateNoAccount() {
        final Rig r = new Rig();
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setUserId(5);
        d.setAssetId(0);
        d.setAmount(0);
        r.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, TS);
        assertEquals("rejected deposit must not create an account", 0, r.engine.accountCount());

        final WithdrawCommand w = new WithdrawCommand();
        w.reset();
        w.setCorrelationId(9);
        w.setUserId(6);
        w.setAssetId(0);
        w.setAmount(100);
        r.engine.applyCommand(AssetsEngine.CMD_WITHDRAW, w, TS);
        assertEquals(0, r.engine.accountCount());
        assertEquals(RejectReason.INSUFFICIENT_FUNDS, ((AssetsEngineEvent.WithdrawReject) r.events().get(0)).reason());

        final HoldCommand h = new HoldCommand();
        h.reset();
        h.setCorrelationId(10);
        h.setOrderId(1);
        h.setUserId(7);
        h.setAssetId(0);
        h.setAmount(100);
        r.engine.applyCommand(AssetsEngine.CMD_HOLD, h, TS);
        assertEquals(0, r.engine.accountCount());
        assertEquals(RejectReason.INSUFFICIENT_FUNDS, ((AssetsEngineEvent.HoldReject) r.events().get(1)).reason());
        assertNull(r.engine.account(5));
        assertNull(r.engine.account(6));
        assertNull(r.engine.account(7));
    }

    // ---- RED-F10: unknown and unhandled templates are visible ----

    @Test
    public void unknownTemplateIsCountedNotSilent() {
        final Rig r = new Rig();
        final int len = r.deposit(1, 0, 100);
        r.buf.putShort(2, (short) 200, ByteOrder.LITTLE_ENDIAN); // templateId = 200
        r.dispatch(len);
        assertEquals(1, r.demuxer.unknownTemplateCount());
        assertEquals(0, r.demuxer.decodeRejectCount());
        assertEquals(0, r.engine.accountCount());
    }

    @Test
    public void knownButUnservedV6TemplateIsCountedNotApplied() {
        final Rig r = new Rig();
        final CreateOrderEncoder e = new CreateOrderEncoder().wrapAndApplyHeader(r.buf, 0, r.header)
                .correlationId(1).userId(1).clientOrderIdHigh(0).clientOrderIdLow(1).requestIdHigh(0).requestIdLow(1)
                .marketId(1).side(Side.BUY).orderType(OrderType.LIMIT).timeInForce(TimeInForce.GTC)
                .price(100).quantity(100).budget(0).stopPrice(0).trailingDelta(0).displayQty(0).expiresAtMs(0);
        e.flags().clear();
        r.dispatch(HEADER + e.encodedLength());
        assertEquals(1, r.demuxer.unhandledTemplateCount());
        assertEquals(0, r.demuxer.unknownTemplateCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals(0, r.engine.accountCount());
    }

    // ---- RED-F11 / F12: mutation then throw is an apply failure, never poison ----

    private static AssetsEventSink failingSink(final Throwable failure) {
        return new AssetsEventSink() {
            private void fail() {
                if (failure instanceof RuntimeException e) {
                    throw e;
                }
                throw (Error) failure;
            }
            public void onDepositAck(long c, long u, int a, long amt, long n) { }
            public void onWithdrawAck(long c, long u, int a, long amt, long n) { }
            public void onHoldAck(long c, long o, long u, int a, long amt) { }
            public void onHoldReject(long c, long o, long u, int a, long amt, RejectReason r) { }
            public void onBalanceUpdate(long u, int a, long av, long l) { fail(); }
            public void onSettlementApplied(long t, long b, long s) { }
            public void onWithdrawReject(long c, long u, int a, long amt, RejectReason r) { }
            public void onBalanceSnapshotEnd(long c, int n) { }
            public void onHoldSnapshotEntry(long o, long u, int a, long rem) { }
            public void onHoldSnapshotEnd(long c, int n) { }
            public void onFeedPositionReport(long c, long p, long t, boolean j) { }
            public void onSettleFault(long t, long o, long u, int a, long d, long un) { }
            public void onSettleRejected(long t, int m, long p, SettleRejectReason r, long d) { }
        };
    }

    private static void assertApplyFailure(final Throwable failure) {
        final Rig r = new Rig();
        r.engine.setEventSink(failingSink(failure));
        final int len = r.deposit(1, 0, 100);
        assertSame(failure, assertThrows(failure.getClass(), () -> r.dispatch(len)));
        assertEquals("the failure happened AFTER the credit", 100, r.engine.account(1).available(0));
        assertEquals(1, r.demuxer.applyErrorCount());
        assertEquals("apply exceptions are not poison input", 0, r.demuxer.decodeRejectCount());
        r.engine.setEventSink(new RecordingAssetsSink());
        final int again = r.deposit(2, 0, 100);
        assertThrows(IllegalStateException.class, () -> r.dispatch(again));
        assertEquals("a failed instance must never apply another command", 1, r.engine.accountCount());
        assertEquals("one underlying failure, not one per blocked retry", 1, r.demuxer.applyErrorCount());
    }

    @Test public void publisherRuntimeFailureEscapes() { assertApplyFailure(new IllegalStateException("injected")); }
    @Test public void applyIllegalArgumentIsNotDecodePoison() { assertApplyFailure(new IllegalArgumentException("injected")); }
    @Test public void applyBoundsFailureIsNotDecodePoison() { assertApplyFailure(new IndexOutOfBoundsException("injected")); }
    @Test public void applyErrorAlsoLatches() { assertApplyFailure(new AssertionError("injected")); }

    @Test
    public void poisonInputDoesNotFenceTheHealthyEngine() {
        final Rig r = funded();
        final int len = r.hold(1, 9, 0, 100);
        r.buf.putByte(HEADER + HoldEncoder.omsManagedReleaseEncodingOffset(), (byte) 99);
        r.dispatch(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        r.dispatch(r.hold(1, 9, 0, 100));
        assertEquals(1, r.count(AssetsEngineEvent.HoldAck.class));
        // a withdraw frame still decodes and applies after the poison
        final WithdrawEncoder w = new WithdrawEncoder().wrapAndApplyHeader(r.buf, 0, r.header)
                .correlationId(4).userId(1).assetId(0).amount(100).requestIdHigh(0).requestIdLow(0);
        r.dispatch(HEADER + w.encodedLength());
        assertEquals(1, r.count(AssetsEngineEvent.WithdrawAck.class));
    }

    @Test
    public void queryClassificationCoversTheV6Queries() {
        assertTrue(AssetsSbeDemuxer.isQuery(com.openexchange.assets.infrastructure.generated.RequestOrderSnapshotDecoder.TEMPLATE_ID));
        assertTrue(AssetsSbeDemuxer.isQuery(com.openexchange.assets.infrastructure.generated.RequestPendingDispatchDecoder.TEMPLATE_ID));
        assertTrue(AssetsSbeDemuxer.isQuery(com.openexchange.assets.infrastructure.generated.RequestRiskConfigDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(CreateOrderEncoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder.TEMPLATE_ID));
    }
}

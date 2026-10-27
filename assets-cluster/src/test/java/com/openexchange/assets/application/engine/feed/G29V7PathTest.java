// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SettleBatchEncoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseEncoder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.BTC;
import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static com.openexchange.assets.application.engine.feed.V3Rig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * RC2 S7 section 13: the G29 class of silent skip (drill {@code RC2-me-link/G29/}, {@code
 * SettleBatchLossG29DefectCharacterizationTest}) is impossible on the behaviour-v3 path.
 *
 * <p>Same parameters as the drill: the demo BTC-USD anchor (100,000 USD, 0.1 BTC per trade), three batches of 64 (the
 * bridge's cap), batch 2 lost between batch 1 and batch 3, then both FILLED terminals. On the 62 path under v3 the loss is
 * loud and gated and nothing past the hole applies; a late redelivery heals it exactly. And the drill's own v5 frame
 * sequence (SettleBatch, TerminalRelease, through the trusted path) cannot move a cent on a v3 core (D-1).</p>
 */
public class G29V7PathTest {

    private static final long PRICE = 100_000L * FixedPoint.SCALE_FACTOR;
    private static final long QTY = FixedPoint.SCALE_FACTOR / 10;
    private static final long NOTIONAL = FixedPoint.multiply(PRICE, QTY);
    private static final int BATCH = 64;
    private static final int TRADES = 3 * BATCH;
    private static final long BUYER = 1;
    private static final long SELLER = 2;
    /** journalSeq of the last ack: FRESH 1, acks 2 and 3. */
    private static final long ACKED = 3;

    /** Buyer and seller funded and their core orders accepted, acked, holding exactly what 192 trades need. */
    private static V3Rig book() {
        final V3Rig r = V3Rig.open();
        r.deposit(BUYER, USD, NOTIONAL * TRADES + 1_000 * ONE);
        r.deposit(SELLER, BTC, QTY * TRADES + ONE);
        assertEquals(1L, r.create(r.limit(BUYER, 0, PRICE, QTY * TRADES)));
        assertEquals(2L, r.create(r.limit(SELLER, 1, PRICE, QTY * TRADES)));
        r.ack(1, 0, 101);
        r.ack(2, 0, 202);
        assertEquals(ACKED, r.cursor());
        return r;
    }

    /** Batch k (1-based): tradeIds 64(k-1)+1 .. 64k at journalSeqs ACKED + tradeId, sparse increasing egressSeqs. */
    private static void batch(final V3Rig r, final int k) {
        final List<V3Rig.BatchEntry> entries = new ArrayList<>();
        for (int i = 0; i < BATCH; i++) {
            final long tradeId = (long) (k - 1) * BATCH + i + 1;
            final long js = ACKED + tradeId;
            entries.add(new V3Rig.BatchEntry(js, EPOCH, 10_000L + js * 10L, tradeId, 0, QTY));
        }
        r.dispatchTrusted(r.tradeBatch(entries, PRICE));
    }

    /** Both FILLED terminals at the journalSeqs after the last trade. */
    private static void terminals(final V3Rig r) {
        final long js = ACKED + TRADES;
        r.terminalAt(js + 1, EPOCH, 10_000L + (js + 1) * 10L, 1, 101, BUYER, V3Rig.ME_FILLED, 0);
        r.terminalAt(js + 2, EPOCH, 10_000L + (js + 2) * 10L, 2, 202, SELLER, V3Rig.ME_FILLED, 0);
    }

    private static V3Rig oracle() {
        final V3Rig r = book();
        for (int k = 1; k <= 3; k++) {
            batch(r, k);
        }
        terminals(r);
        return r;
    }

    private static void assertMoneyEquals(final String what, final V3Rig expected, final V3Rig actual) {
        for (final long user : new long[] {BUYER, SELLER}) {
            for (final int asset : new int[] {USD, BTC}) {
                assertEquals(what + ": user " + user + " asset " + asset + " total", expected.total(user, asset),
                        actual.total(user, asset));
                assertEquals(what + ": user " + user + " asset " + asset + " locked", expected.locked(user, asset),
                        actual.locked(user, asset));
            }
        }
    }

    @Test
    public void lostBatchLatchesNeverSkips() {
        final V3Rig victim = book();
        batch(victim, 1);
        victim.mark();
        batch(victim, 3);
        terminals(victim);

        // the hole is loud: FEED_GAP naming the first missing journalSeq, a GAP latch, the gate shut
        final List<AssetsEngineEvent.SettleRejected> rejected = victim.all(AssetsEngineEvent.SettleRejected.class);
        assertEquals("batch 3 stops at its first entry; each terminal refuses too", 3, rejected.size());
        for (final AssetsEngineEvent.SettleRejected sr : rejected) {
            assertEquals(SettleRejectReason.FEED_GAP, sr.reason());
            assertEquals("detail = the first missing journalSeq", ACKED + BATCH + 1, sr.detail());
        }
        assertEquals("the first refusal names batch 3's first journalSeq", ACKED + 2 * BATCH + 1, rejected.get(0).journalPosition());
        victim.assertLatch("lost batch 2", FeedHaltReason.GAP, ACKED + BATCH + 1, ACKED + 2 * BATCH + 1, 0, 0);
        victim.assertGloballyHalted("lost batch 2");

        // nothing past the hole applied: 64 settled, the high-water and the cursor stay at the end of batch 1
        assertEquals("not one trade of batch 3 settles", 0, victim.count(AssetsEngineEvent.Settled.class));
        assertEquals(BATCH, victim.engine.getLastAppliedTradeId());
        assertEquals(ACKED + BATCH, victim.cursor());
        assertNotNull("the FILLED terminals did not apply over the hole", victim.order(1));
        assertNotNull(victim.order(2));

        // money = the oracle prefix (exactly batch 1): holds still lock what the unapplied trades owe
        final V3Rig prefix = book();
        batch(prefix, 1);
        assertMoneyEquals("victim vs the batch-1 prefix", prefix, victim);

        // the resume key certifies nothing: the report names the cursor before the hole, and halted
        victim.mark();
        victim.engine.reportFeedPosition(77L);
        final AssetsEngineEvent.FeedPositionReport report = victim.first(AssetsEngineEvent.FeedPositionReport.class);
        assertEquals(ACKED + BATCH, report.lastAppliedJournalSeq());
        assertEquals(BATCH, report.lastAppliedTradeId());
        assertEquals(true, report.feedHalted());
        assertEquals(FeedHaltReason.GAP, report.haltReason());

        // and no ADMIN can open order entry over the hole
        victim.mark();
        victim.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        final AssetsEngineEvent.AdminAck ack = victim.onlyAck();
        assertEquals(AdminResult.REJECTED, ack.result());
        assertEquals(CoreRejectReason.FEED_HALTED.code(), ack.reason());
    }

    @Test
    public void lateRedeliveryAppliesAndClears() {
        final V3Rig victim = book();
        batch(victim, 1);
        batch(victim, 3);
        terminals(victim);
        // the lost frame finally arrives: exactly next, so it applies, and the GAP reach point is passed
        victim.mark();
        batch(victim, 2);
        assertEquals(BATCH, victim.count(AssetsEngineEvent.Settled.class));
        assertEquals(2L * BATCH, victim.engine.getLastAppliedTradeId());
        victim.assertNotLatched("batch 2 healed the hole");
        assertEquals(new AssetsEngineEvent.FeedLatchCleared(FeedHaltReason.GAP, 0L, true),
                victim.first(AssetsEngineEvent.FeedLatchCleared.class));
        // the bridge resends from the reported cursor: batch 3 and the terminals now apply in order
        batch(victim, 3);
        terminals(victim);
        assertEquals(TRADES, victim.engine.getLastAppliedTradeId());
        assertEquals(ACKED + TRADES + 2, victim.cursor());
        assertNull(victim.order(1));
        assertNull(victim.order(2));
        assertMoneyEquals("healed victim vs oracle", oracle(), victim);
        assertEquals(0L, victim.locked(BUYER, USD));
        assertEquals(0L, victim.locked(SELLER, BTC));
        victim.engine.verifyRestoredInvariants();
    }

    // ---------------------------------------------------------------- the drill's own v5 frames on a v3 core

    private static int hold(final V3Rig r, final long user, final long order, final int asset, final long amount) {
        final HoldEncoder e = new HoldEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .correlationId(order).orderId(order).userId(user).assetId(asset).amount(amount).omsManagedRelease(BoolFlag.FALSE);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int settleBatch(final V3Rig r, final int k) {
        final SettleBatchEncoder e = new SettleBatchEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header());
        final SettleBatchEncoder.SettlesEncoder g = e.settlesCount(BATCH);
        for (int i = 0; i < BATCH; i++) {
            final long tradeId = (long) (k - 1) * BATCH + i + 1;
            g.next().tradeId(tradeId).marketId(V3Rig.MKT).takerOrderId(5_011L).takerUserId(900_001L).makerOrderId(5_022L)
                    .makerUserId(900_002L).price(PRICE).quantity(QTY).takerIsBuy(BoolFlag.TRUE).journalPosition(10_000L + tradeId * 10L);
        }
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int terminalRelease(final V3Rig r, final long user, final long order) {
        final TerminalReleaseEncoder e = new TerminalReleaseEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .journalPosition(10_000L + TRADES * 10L + 5L).orderId(order).userId(user).timestamp(r.now);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    /**
     * The G29 drill's exact frame sequence (holds, SettleBatch 1, SettleBatch 3, both TerminalReleases, then the late
     * SettleBatch 2), dispatched through the TRUSTED path that the drill uses, on a v3 core: every money frame is refused
     * before it can apply, so no lost batch can be applied over and no high-water can certify a hole.
     */
    @Test
    public void g29DrillSequenceIsImpossibleUnderV3() {
        final V3Rig r = V3Rig.open();
        r.deposit(900_001L, USD, NOTIONAL * TRADES + 1_000 * ONE);
        r.deposit(900_002L, BTC, QTY * TRADES + ONE);
        // RC2 S8 3.12 (D-S8-10): the drill's own holds name orderIds 5011/5022, inside the core's id space: refused on a v3
        // core like the v5 money frames below (TRUSTED included), nothing locked
        r.mark();
        r.dispatchTrusted(hold(r, 900_001L, 5_011L, USD, NOTIONAL * TRADES));
        r.dispatchTrusted(hold(r, 900_002L, 5_022L, BTC, QTY * TRADES));
        assertEquals("both drill holds refused (S8 3.12)", 2, r.all(AssetsEngineEvent.OrderReject.class).size());
        assertEquals(3, r.first(AssetsEngineEvent.OrderReject.class).refTemplateId());
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.first(AssetsEngineEvent.OrderReject.class).reason());
        assertEquals(0L, r.locked(900_001L, USD));
        // the precondition holds are placed outside the core's id space (orderId <= 0 keeps today's handling)
        r.dispatchTrusted(hold(r, 900_001L, -5_011L, USD, NOTIONAL * TRADES));
        r.dispatchTrusted(hold(r, 900_002L, -5_022L, BTC, QTY * TRADES));
        final long buyerUsd = r.total(900_001L, USD);
        final long buyerLocked = r.locked(900_001L, USD);
        assertEquals("precondition: the drill's buyer hold is placed", NOTIONAL * TRADES, buyerLocked);
        assertEquals("precondition: the drill's seller hold is placed", QTY * TRADES, r.locked(900_002L, BTC));
        final long sellerBtc = r.total(900_002L, BTC);
        final long cursor = r.cursor();
        final long position = r.engine.getConsumePosition();
        r.mark();
        r.dispatchTrusted(settleBatch(r, 1));
        r.dispatchTrusted(settleBatch(r, 3));
        r.dispatchTrusted(terminalRelease(r, 900_001L, -5_011L));
        r.dispatchTrusted(terminalRelease(r, 900_002L, -5_022L));
        r.dispatchTrusted(settleBatch(r, 2));

        final List<AssetsEngineEvent.OrderReject> refused = r.all(AssetsEngineEvent.OrderReject.class);
        assertEquals("all five v5 money frames refused (D-1, TRUSTED included)", 5, refused.size());
        for (final AssetsEngineEvent.OrderReject o : refused) {
            assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, o.reason());
        }
        assertEquals(33, refused.get(0).refTemplateId());
        assertEquals(6, refused.get(2).refTemplateId());
        assertEquals(5L, r.engine.v5MoneyRefusedCount());
        assertEquals("nothing settled", 0, r.count(AssetsEngineEvent.Settled.class));
        assertEquals("the high-water never moved", 0L, r.engine.getLastAppliedTradeId());
        assertEquals("the retention watermark never moved", position, r.engine.getConsumePosition());
        assertEquals("the cursor never moved", cursor, r.cursor());
        assertEquals(buyerUsd, r.total(900_001L, USD));
        assertEquals("the hold is not released by TerminalRelease", buyerLocked, r.locked(900_001L, USD));
        assertEquals(sellerBtc, r.total(900_002L, BTC));
        assertEquals("the seller's hold stands too", QTY * TRADES, r.locked(900_002L, BTC));
    }
}

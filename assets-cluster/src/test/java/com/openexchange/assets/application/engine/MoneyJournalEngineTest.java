// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine;

import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.MoneyJournalSink;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Engine-level contract of the money journal port: exact record sequence (opening-balance epoch on a
 * pre-funded ledger, then movements), journalSeq density (no gaps, no dupes), and the negative space
 * (rejects, deduped settles, holds/releases, and the dark default journal NOTHING).
 */
public class MoneyJournalEngineTest {

    private static final int USD = Asset.USD.id(); // 0
    private static final int BTC = Asset.BTC.id(); // 1
    private static final long TS = 1000L;

    /** Captures journal emissions inline, rendering one canonical line per record. */
    private static final class RecordingJournal implements MoneyJournalSink {
        final List<String> lines = new ArrayList<>();
        final List<Long> seqs = new ArrayList<>();

        @Override
        public void onOpeningBalance(long journalSeq, long userId, int assetId, long amount) {
            seqs.add(journalSeq);
            lines.add("OPENING seq=" + journalSeq + " u=" + userId + " asset=" + assetId + " amt=" + amount);
        }

        @Override
        public void onDeposit(long journalSeq, long userId, int assetId, long amount,
                              long balanceAfter, long clusterTimeMs) {
            seqs.add(journalSeq);
            lines.add("DEPOSIT seq=" + journalSeq + " u=" + userId + " asset=" + assetId + " amt=" + amount
                    + " after=" + balanceAfter + " t=" + clusterTimeMs);
        }

        @Override
        public void onWithdraw(long journalSeq, long userId, int assetId, long amount,
                               long balanceAfter, long clusterTimeMs) {
            seqs.add(journalSeq);
            lines.add("WITHDRAW seq=" + journalSeq + " u=" + userId + " asset=" + assetId + " amt=" + amount
                    + " after=" + balanceAfter + " t=" + clusterTimeMs);
        }

        @Override
        public void onSettle(long journalSeq, long tradeId, int marketId, long price, long quantity,
                             long buyerUserId, long sellerUserId, boolean takerIsBuy,
                             long buyerBaseAfter, long buyerQuoteAfter,
                             long sellerBaseAfter, long sellerQuoteAfter, long clusterTimeMs) {
            seqs.add(journalSeq);
            lines.add("SETTLE seq=" + journalSeq + " trade=" + tradeId + " market=" + marketId
                    + " px=" + price + " qty=" + quantity
                    + " buyer=" + buyerUserId + " seller=" + sellerUserId + " takerBuy=" + takerIsBuy
                    + " buyerBase=" + buyerBaseAfter + " buyerQuote=" + buyerQuoteAfter
                    + " sellerBase=" + sellerBaseAfter + " sellerQuote=" + sellerQuoteAfter
                    + " t=" + clusterTimeMs);
        }

        /** journalSeq density: strictly 1..n in emission order. */
        void assertDense() {
            for (int i = 0; i < seqs.size(); i++) {
                assertEquals("journalSeq at record " + i, i + 1L, (long) seqs.get(i));
            }
        }
    }

    private AssetsEngine engine;
    private RecordingJournal journal;

    private void newEngine() {
        engine = new AssetsEngine();
        engine.setEventSink(new RecordingAssetsSink());
        journal = new RecordingJournal();
    }

    // ---- the approved scenario: epoch on a pre-funded ledger, then deposit -> settle -> withdraw ----

    @Test
    public void openingEpochThenDepositSettleWithdrawInExactOrder() {
        newEngine();
        // Pre-fund BEFORE journaling is armed: this history must surface only as the epoch.
        deposit(100, BTC, FixedPoint.fromDouble(1.0));
        deposit(200, USD, FixedPoint.fromDouble(60000.0));
        deposit(100, USD, FixedPoint.fromDouble(5.0));
        assertEquals("dark engine journals nothing", 0L, engine.getJournalSeq());

        engine.setMoneyJournal(journal);

        // Holds journal nothing and do not trigger the epoch (they do not change totals).
        hold(1L, 100, BTC, FixedPoint.fromDouble(1.0));
        hold(2L, 200, USD, FixedPoint.fromDouble(60000.0));
        assertEquals(0, journal.lines.size());

        // First journaled event: the epoch (PRE-event totals, ascending userId then assetId) precedes it.
        deposit(300, USD, FixedPoint.fromDouble(100.0));
        // Taker 200 buys 1 BTC @ 60000 from maker 100.
        settle(1L, 1, 2L, 200, 1L, 100, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);
        withdraw(200, BTC, FixedPoint.fromDouble(0.4));

        assertEquals(List.of(
                "OPENING seq=1 u=100 asset=0 amt=" + FixedPoint.fromDouble(5.0),
                "OPENING seq=2 u=100 asset=1 amt=" + FixedPoint.fromDouble(1.0),
                "OPENING seq=3 u=200 asset=0 amt=" + FixedPoint.fromDouble(60000.0),
                "DEPOSIT seq=4 u=300 asset=0 amt=" + FixedPoint.fromDouble(100.0)
                        + " after=" + FixedPoint.fromDouble(100.0) + " t=" + TS,
                "SETTLE seq=5 trade=1 market=1 px=" + FixedPoint.fromDouble(60000.0)
                        + " qty=" + FixedPoint.fromDouble(1.0)
                        + " buyer=200 seller=100 takerBuy=true"
                        + " buyerBase=" + FixedPoint.fromDouble(1.0) + " buyerQuote=0"
                        + " sellerBase=0 sellerQuote=" + FixedPoint.fromDouble(60005.0) + " t=" + TS,
                "WITHDRAW seq=6 u=200 asset=1 amt=" + FixedPoint.fromDouble(0.4)
                        + " after=" + FixedPoint.fromDouble(0.6) + " t=" + TS),
                journal.lines);
        journal.assertDense();
        assertEquals(6L, engine.getJournalSeq());
    }

    // ---- implicit epoch on an empty ledger ----

    @Test
    public void emptyLedgerEpochIsImplicit() {
        newEngine();
        engine.setMoneyJournal(journal);
        deposit(1, USD, FixedPoint.fromDouble(10.0));
        assertEquals(List.of(
                "DEPOSIT seq=1 u=1 asset=0 amt=" + FixedPoint.fromDouble(10.0)
                        + " after=" + FixedPoint.fromDouble(10.0) + " t=" + TS),
                journal.lines);
        journal.assertDense();
    }

    // ---- negative space: rejects, dedupes, holds/releases journal NOTHING ----

    @Test
    public void rejectsJournalNothingAndConsumeNoSeq() {
        newEngine();
        engine.setMoneyJournal(journal);
        withdraw(1, USD, FixedPoint.fromDouble(1.0)); // overdraft reject: not even the epoch
        deposit(1, USD, -5L); // invalid amount reject
        assertEquals(0, journal.lines.size());
        assertEquals(0L, engine.getJournalSeq());

        deposit(1, USD, FixedPoint.fromDouble(10.0));
        withdraw(1, USD, FixedPoint.fromDouble(999.0)); // overdraft reject after a journaled event
        assertEquals(1, journal.lines.size());
        assertEquals(1L, engine.getJournalSeq());
    }

    @Test
    public void dedupedSettleJournalsNothingAndConsumesNoSeq() {
        newEngine();
        engine.setMoneyJournal(journal);
        deposit(100, BTC, FixedPoint.fromDouble(1.0));
        deposit(200, USD, FixedPoint.fromDouble(60000.0));
        hold(1L, 100, BTC, FixedPoint.fromDouble(1.0));
        hold(2L, 200, USD, FixedPoint.fromDouble(60000.0));
        settle(5L, 1, 2L, 200, 1L, 100, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);
        assertEquals(3, journal.lines.size()); // 2 deposits + 1 settle
        final long seqAfter = engine.getJournalSeq();

        // Re-delivery of the same trade and an older tradeId: idempotent no-ops.
        settle(5L, 1, 2L, 200, 1L, 100, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);
        settle(4L, 1, 2L, 200, 1L, 100, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0), true);
        assertEquals(3, journal.lines.size());
        assertEquals(seqAfter, engine.getJournalSeq());
        journal.assertDense();
    }

    @Test
    public void holdAndReleaseJournalNothing() {
        newEngine();
        deposit(7, USD, FixedPoint.fromDouble(50.0));
        engine.setMoneyJournal(journal);
        hold(9L, 7, USD, FixedPoint.fromDouble(20.0));
        release(9L, 7);
        assertEquals(0, journal.lines.size());
        assertEquals(0L, engine.getJournalSeq());

        // The epoch still fires intact on the first real movement (totals unchanged by hold cycles).
        deposit(7, USD, FixedPoint.fromDouble(1.0));
        assertEquals(List.of(
                "OPENING seq=1 u=7 asset=0 amt=" + FixedPoint.fromDouble(50.0),
                "DEPOSIT seq=2 u=7 asset=0 amt=" + FixedPoint.fromDouble(1.0)
                        + " after=" + FixedPoint.fromDouble(51.0) + " t=" + TS),
                journal.lines);
        journal.assertDense();
    }

    @Test
    public void darkByDefaultAdvancesNothing() {
        newEngine();
        deposit(1, USD, FixedPoint.fromDouble(10.0));
        withdraw(1, USD, FixedPoint.fromDouble(5.0));
        assertEquals(0L, engine.getJournalSeq());
        assertTrue(journal.lines.isEmpty());
    }

    // ---- command helpers ----

    private void deposit(long userId, int assetId, long amount) {
        DepositCommand c = new DepositCommand();
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, c, TS);
    }

    private void withdraw(long userId, int assetId, long amount) {
        WithdrawCommand c = new WithdrawCommand();
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_WITHDRAW, c, TS);
    }

    private void hold(long orderId, long userId, int assetId, long amount) {
        HoldCommand c = new HoldCommand();
        c.setOrderId(orderId);
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_HOLD, c, TS);
    }

    private void release(long orderId, long userId) {
        ReleaseCommand c = new ReleaseCommand();
        c.setOrderId(orderId);
        c.setUserId(userId);
        c.setAmount(-1L);
        engine.applyCommand(AssetsEngine.CMD_RELEASE, c, TS);
    }

    private void settle(long tradeId, int marketId, long takerOrder, long takerUser,
                        long makerOrder, long makerUser, long price, long quantity, boolean takerIsBuy) {
        SettleCommand c = new SettleCommand();
        c.setTradeId(tradeId);
        c.setMarketId(marketId);
        c.setTakerOrderId(takerOrder);
        c.setTakerUserId(takerUser);
        c.setMakerOrderId(makerOrder);
        c.setMakerUserId(makerUser);
        c.setPrice(price);
        c.setQuantity(quantity);
        c.setTakerIsBuy(takerIsBuy);
        engine.applyCommand(AssetsEngine.CMD_SETTLE, c, TS);
    }
}

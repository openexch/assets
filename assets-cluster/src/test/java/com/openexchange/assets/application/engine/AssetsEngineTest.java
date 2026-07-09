// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine;

import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.InitTradeHighWaterCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Engine-level tests for the v2 behaviour that the wire/golden suites do not isolate: the deposit and
 * withdraw acks (with the correlationId echo), the {@code InitTradeHighWater} cutover primer's virgin-
 * ledger guard, and the read-only snapshot queries. Uses the in-process {@link RecordingAssetsSink} so
 * the exact emitted event stream (order + values) is asserted.
 */
public class AssetsEngineTest {

    private static final int USD = Asset.USD.id(); // 0
    private static final int BTC = Asset.BTC.id(); // 1
    private static final long TS = 1000L;

    private AssetsEngine engine;
    private RecordingAssetsSink sink;

    private void newEngine() {
        engine = new AssetsEngine();
        sink = new RecordingAssetsSink();
        engine.setEventSink(sink);
    }

    // ---- deposit / withdraw acks with correlationId echo ----

    @Test
    public void depositEmitsDepositAckThenBalanceWithCorrelationIdEchoed() {
        newEngine();
        deposit(42L, 100, USD, FixedPoint.fromDouble(1000.0));
        assertEquals(""
                + "DEPOSITACK corr=42 u=100 asset=0 amt=100000000000 newAvail=100000000000\n"
                + "BALANCE u=100 asset=0 avail=100000000000 locked=0\n", sink.render());
    }

    @Test
    public void withdrawAcceptedEmitsWithdrawAckThenBalance() {
        newEngine();
        deposit(0L, 100, USD, FixedPoint.fromDouble(1000.0));
        withdraw(7L, 100, USD, FixedPoint.fromDouble(300.0));
        assertEquals(""
                + "DEPOSITACK corr=0 u=100 asset=0 amt=100000000000 newAvail=100000000000\n"
                + "BALANCE u=100 asset=0 avail=100000000000 locked=0\n"
                + "WITHDRAWACK corr=7 u=100 asset=0 amt=30000000000 newAvail=70000000000\n"
                + "BALANCE u=100 asset=0 avail=70000000000 locked=0\n", sink.render());
    }

    @Test
    public void withdrawRejectedEmitsWithdrawRejectWithCorrelationId() {
        newEngine();
        deposit(0L, 100, USD, FixedPoint.fromDouble(100.0));
        withdraw(9L, 100, USD, FixedPoint.fromDouble(500.0)); // overdraft
        assertEquals(""
                + "DEPOSITACK corr=0 u=100 asset=0 amt=10000000000 newAvail=10000000000\n"
                + "BALANCE u=100 asset=0 avail=10000000000 locked=0\n"
                + "WITHDRAWREJECT corr=9 u=100 asset=0 amt=50000000000 reason=INSUFFICIENT_FUNDS\n",
                sink.render());
    }

    // ---- InitTradeHighWater cutover primer ----

    @Test
    public void initHighWaterAcceptedOnVirginLedgerAndEmitsNothing() {
        newEngine();
        initHighWater(500L, 12345L);
        assertEquals(500L, engine.getLastAppliedTradeId());
        assertEquals(12345L, engine.getConsumePosition());
        assertEquals("primer emits no egress", "", sink.render());
    }

    @Test
    public void initHighWaterRefusedAfterAnyDeposit() {
        newEngine();
        deposit(0L, 100, USD, FixedPoint.fromDouble(1.0)); // ledger is no longer virgin (an account exists)
        initHighWater(999L, 88888L);
        assertEquals("refused: high-water untouched", 0L, engine.getLastAppliedTradeId());
        assertEquals("refused: consumePosition untouched", 0L, engine.getConsumePosition());
    }

    @Test
    public void initHighWaterRefusedAfterSettle() {
        newEngine();
        // Fund + hold both sides, then settle a full 1 BTC @ 60000 so lastAppliedTradeId advances to 1.
        deposit(0L, 100, BTC, FixedPoint.fromDouble(1.0));
        deposit(0L, 200, USD, FixedPoint.fromDouble(60000.0));
        hold(0L, 1L, 100, BTC, FixedPoint.fromDouble(1.0));
        hold(0L, 2L, 200, USD, FixedPoint.fromDouble(60000.0));
        settleFullBuy(1L, 1, 2L, 200, 1L, 100, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0));
        assertEquals(1L, engine.getLastAppliedTradeId());

        initHighWater(999L, 88888L);
        assertEquals("refused: settle high-water not overwritten", 1L, engine.getLastAppliedTradeId());
        assertEquals("refused: consumePosition untouched", 0L, engine.getConsumePosition());
    }

    // ---- snapshot queries ----

    @Test
    public void balanceSnapshotStreamsNonZeroEntriesSortedByUserThenEnd() {
        newEngine();
        deposit(0L, 100, USD, FixedPoint.fromDouble(1000.0));
        deposit(0L, 200, BTC, FixedPoint.fromDouble(2.0));
        hold(0L, 1L, 100, USD, FixedPoint.fromDouble(600.0));
        hold(0L, 2L, 200, BTC, FixedPoint.fromDouble(0.5));
        sink.reset();

        engine.requestBalanceSnapshot(777L);
        assertEquals(""
                + "BALANCE u=100 asset=0 avail=40000000000 locked=60000000000\n"
                + "BALANCE u=200 asset=1 avail=150000000 locked=50000000\n"
                + "BALSNAPEND corr=777 count=2\n", sink.render());
    }

    @Test
    public void holdSnapshotStreamsEntriesSortedByUserThenOrderThenEnd() {
        newEngine();
        deposit(0L, 100, USD, FixedPoint.fromDouble(1000.0));
        deposit(0L, 200, BTC, FixedPoint.fromDouble(2.0));
        hold(0L, 2L, 100, USD, FixedPoint.fromDouble(100.0)); // out-of-order insertion within u=100
        hold(0L, 1L, 100, USD, FixedPoint.fromDouble(200.0));
        hold(0L, 5L, 200, BTC, FixedPoint.fromDouble(0.5));
        sink.reset();

        engine.requestHoldSnapshot(888L);
        assertEquals(""
                + "HOLDSNAPENTRY order=1 u=100 asset=0 remaining=20000000000\n"
                + "HOLDSNAPENTRY order=2 u=100 asset=0 remaining=10000000000\n"
                + "HOLDSNAPENTRY order=5 u=200 asset=1 remaining=50000000\n"
                + "HOLDSNAPEND corr=888 count=3\n", sink.render());
    }

    @Test
    public void emptyLedgerSnapshotsEmitOnlyTheEndMarker() {
        newEngine();
        engine.requestBalanceSnapshot(1L);
        engine.requestHoldSnapshot(2L);
        assertEquals(""
                + "BALSNAPEND corr=1 count=0\n"
                + "HOLDSNAPEND corr=2 count=0\n", sink.render());
    }

    // ---- command builders ----

    private void deposit(long corr, long userId, int assetId, long amount) {
        DepositCommand c = new DepositCommand();
        c.setCorrelationId(corr);
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, c, TS);
    }

    private void withdraw(long corr, long userId, int assetId, long amount) {
        WithdrawCommand c = new WithdrawCommand();
        c.setCorrelationId(corr);
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_WITHDRAW, c, TS);
    }

    private void hold(long corr, long orderId, long userId, int assetId, long amount) {
        HoldCommand c = new HoldCommand();
        c.setCorrelationId(corr);
        c.setOrderId(orderId);
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_HOLD, c, TS);
    }

    private void settleFullBuy(long tradeId, int marketId, long takerOrder, long takerUser,
                               long makerOrder, long makerUser, long price, long qty) {
        SettleCommand c = new SettleCommand();
        c.setTradeId(tradeId);
        c.setMarketId(marketId);
        c.setTakerOrderId(takerOrder);
        c.setTakerUserId(takerUser);
        c.setMakerOrderId(makerOrder);
        c.setMakerUserId(makerUser);
        c.setPrice(price);
        c.setQuantity(qty);
        c.setTakerIsBuy(true);
        engine.applyCommand(AssetsEngine.CMD_SETTLE, c, TS);
    }

    private void initHighWater(long tradeId, long consumePosition) {
        InitTradeHighWaterCommand c = new InitTradeHighWaterCommand();
        c.setTradeId(tradeId);
        c.setConsumePosition(consumePosition);
        engine.applyCommand(AssetsEngine.CMD_INIT_HIGH_WATER, c, TS);
    }
}

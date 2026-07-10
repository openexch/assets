// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projection;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Adversarial property test: a seeded, reproducible randomized market simulation hammers the engine
 * with thousands of interleaved deposits, withdrawals, hold+cancels, and full trade cycles (hold both
 * sides → settle a random fraction → release residuals), and asserts <b>every money invariant after
 * every single operation</b>:
 *
 * <ul>
 *   <li><b>Conservation of external flow</b> — per asset, Σ(available + locked) across all accounts
 *       equals exactly (Σ deposits − Σ withdrawals). Holds, settlements, and releases move money but
 *       never create or destroy it.</li>
 *   <li><b>Locked fully accounted</b> — per (account, asset), {@code locked == Σ remaining} over that
 *       asset's open holds.</li>
 *   <li><b>No negative balances.</b></li>
 * </ul>
 *
 * This is the "structural conservation" bar the TigerBeetle benchmark set, proven under adversarial
 * input rather than hand-picked scenarios. Deterministic (fixed seeds) so a failure reproduces exactly.
 */
public class ConservationFuzzTest {

    private static final int N = Asset.count();
    private static final long[] SEEDS = {1L, 42L, 1337L, 2718281828L};
    private static final int OPS_PER_SEED = 3000;
    private static final int USERS = 8;

    @Test
    public void invariantsHoldUnderRandomizedLoad() {
        for (long seed : SEEDS) {
            runOne(seed);
        }
    }

    private void runOne(long seed) {
        Random rng = new Random(seed);
        AssetsEngine e = new AssetsEngine();
        e.setEventSink(NOOP);
        SettlementProjector proj = new SettlementProjector(e);

        long[] external = new long[N]; // expected Σ(avail+locked) per asset = Σdeposits − Σwithdrawals
        long nextOrderId = 1, nextTradeId = 1, nextPos = 1, ts = 1;
        // Standalone holds we can later cancel: (orderId, userId, assetId).
        List<long[]> cancelable = new ArrayList<>();

        for (int i = 0; i < OPS_PER_SEED; i++) {
            int roll = rng.nextInt(100);
            if (roll < 35) {
                // DEPOSIT (always succeeds for a positive amount)
                long user = user(rng);
                int asset = rng.nextInt(N);
                long amt = amount(rng);
                deposit(e, user, asset, amt, ts++);
                external[asset] += amt;
            } else if (roll < 50) {
                // WITHDRAW (succeeds iff funded; predict from available)
                long user = user(rng);
                int asset = rng.nextInt(N);
                long amt = amount(rng);
                long avail = e.account(user) == null ? 0 : e.account(user).available(asset);
                withdraw(e, user, asset, amt, ts++);
                if (amt > 0 && avail >= amt) {
                    external[asset] -= amt;
                }
            } else if (roll < 65) {
                // Standalone HOLD (may reject; conservative either way)
                long user = user(rng);
                int asset = rng.nextInt(N);
                long amt = amount(rng);
                long avail = e.account(user) == null ? 0 : e.account(user).available(asset);
                long orderId = nextOrderId++;
                hold(e, orderId, user, asset, amt, ts++);
                if (amt > 0 && avail >= amt) {
                    cancelable.add(new long[]{orderId, user, asset});
                }
            } else if (roll < 78) {
                // CANCEL a standalone hold (release full residual) — conservative
                if (!cancelable.isEmpty()) {
                    long[] h = cancelable.remove(rng.nextInt(cancelable.size()));
                    release(e, h[0], h[1], -1L, ts++);
                }
            } else {
                // TRADE CYCLE: fund + hold both sides, settle a random fraction, release residuals.
                tradeCycle(e, proj, rng, external, nextOrderId, nextTradeId, nextPos, ts);
                nextOrderId += 2;
                nextTradeId += 1;
                nextPos += 3;
                ts += 6;
            }
            check(e, external, seed, i);
        }
    }

    /** A realistic hold→settle→release cycle on a random market, generating a VALID settlement. */
    private void tradeCycle(AssetsEngine e, SettlementProjector proj, Random rng, long[] external,
                            long baseOrderId, long tradeId, long pos, long ts) {
        Market m = Market.ALL[rng.nextInt(Market.ALL.length)];
        int base = m.baseAsset().id();
        int quote = m.quoteAsset().id();
        long buyer = user(rng);
        long seller = differentUser(rng, buyer);

        long price = 1 + rng.nextInt(100_000); // whole-unit-ish fixed-point price
        long qtyFull = 1 + (long) rng.nextInt(1_000_000); // fixed-point base qty
        long quoteFull = FixedPoint.multiply(price, qtyFull);
        if (quoteFull <= 0) {
            return; // degenerate (price*qty rounded to 0) — skip
        }

        long buyOrder = baseOrderId;
        long sellOrder = baseOrderId + 1;

        // Fund exactly what each side needs, then hold it (external grows by the deposits).
        deposit(e, buyer, quote, quoteFull, ts++);
        external[quote] += quoteFull;
        hold(e, buyOrder, buyer, quote, quoteFull, ts++);

        deposit(e, seller, base, qtyFull, ts++);
        external[base] += qtyFull;
        hold(e, sellOrder, seller, base, qtyFull, ts++);

        // Settle a random fraction (1..qtyFull) of the order.
        long qtyFill = 1 + (Math.floorMod(rng.nextLong(), qtyFull));
        proj.onTrade(pos, tradeId, m.marketId(), buyOrder, buyer, sellOrder, seller,
                price, qtyFill, true, ts++);

        // Terminal on both orders — release whatever residual remains (conservative).
        proj.onTerminal(pos + 1, buyOrder, buyer, ts++);
        proj.onTerminal(pos + 2, sellOrder, seller, ts++);
    }

    /** Assert every invariant on the current engine state. */
    private void check(AssetsEngine e, long[] external, long seed, int step) {
        long[] total = new long[N];
        // structural, per account
        e.forEachAccount(acc -> {
            long[] sumHold = new long[N];
            acc.forEachHold((orderId, assetId, remaining) -> sumHold[assetId] += remaining);
            for (int a = 0; a < N; a++) {
                assertTrue("seed " + seed + " step " + step + ": available>=0 (u" + acc.userId() + " a" + a + ")",
                        acc.available(a) >= 0);
                assertTrue("seed " + seed + " step " + step + ": locked>=0 (u" + acc.userId() + " a" + a + ")",
                        acc.locked(a) >= 0);
                assertTrue("seed " + seed + " step " + step + ": locked==Σholds (u" + acc.userId() + " a" + a + ")",
                        acc.locked(a) == sumHold[a]);
                total[a] += acc.available(a) + acc.locked(a);
            }
        });
        for (int a = 0; a < N; a++) {
            assertTrue("seed " + seed + " step " + step + ": conservation asset " + a
                            + " expected " + external[a] + " got " + total[a],
                    total[a] == external[a]);
        }
    }

    // ---- op helpers ----

    private static void deposit(AssetsEngine e, long user, int asset, long amt, long ts) {
        DepositCommand c = new DepositCommand();
        c.setUserId(user);
        c.setAssetId(asset);
        c.setAmount(amt);
        e.applyCommand(AssetsEngine.CMD_DEPOSIT, c, ts);
    }

    private static void withdraw(AssetsEngine e, long user, int asset, long amt, long ts) {
        WithdrawCommand c = new WithdrawCommand();
        c.setUserId(user);
        c.setAssetId(asset);
        c.setAmount(amt);
        e.applyCommand(AssetsEngine.CMD_WITHDRAW, c, ts);
    }

    private static void hold(AssetsEngine e, long orderId, long user, int asset, long amt, long ts) {
        HoldCommand c = new HoldCommand();
        c.setOrderId(orderId);
        c.setUserId(user);
        c.setAssetId(asset);
        c.setAmount(amt);
        e.applyCommand(AssetsEngine.CMD_HOLD, c, ts);
    }

    private static void release(AssetsEngine e, long orderId, long user, long amt, long ts) {
        ReleaseCommand c = new ReleaseCommand();
        c.setOrderId(orderId);
        c.setUserId(user);
        c.setAmount(amt);
        e.applyCommand(AssetsEngine.CMD_RELEASE, c, ts);
    }

    private static long user(Random rng) {
        return 1 + rng.nextInt(USERS);
    }

    private static long differentUser(Random rng, long other) {
        long u;
        do {
            u = user(rng);
        } while (u == other);
        return u;
    }

    /** A positive fixed-point amount in a sane range. */
    private static long amount(Random rng) {
        return 1 + (long) rng.nextInt(10_000_000);
    }

    private static final AssetsEventSink NOOP = new AssetsEventSink() {
        @Override public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable) { }
        @Override public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable) { }
        @Override public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) { }
        @Override public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason) { }
        @Override public void onBalanceUpdate(long userId, int assetId, long available, long locked) { }
        @Override public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) { }
        @Override public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) { }
        @Override public void onBalanceSnapshotEnd(long correlationId, int entryCount) { }
        @Override public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) { }
        @Override public void onHoldSnapshotEnd(long correlationId, int entryCount) { }
        @Override public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId) { }
        @Override public void onSettleFault(long tradeId, long orderId, long userId, int assetId, long drawnFromAvailable, long uncovered) { }
    };
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The AE-specific correctness gate: money is conserved and fully accounted, on top of determinism.
 *
 * <p>Two kinds of check:</p>
 * <ul>
 *   <li><b>Structural invariants</b> (hold for <em>any</em> engine state): no negative balances, and
 *       for every (account, asset) {@code locked == Σ remaining} over that asset's open holds.
 *       Asserted at the end of every scenario in the determinism corpus.</li>
 *   <li><b>Conservation</b>: DEPOSIT/WITHDRAW move a per-asset total by exactly their amount; HOLD,
 *       RELEASE, and SETTLE never change a per-asset total (money is moved, never created/destroyed).</li>
 * </ul>
 */
public class ConservationInvariantTest {

    private static final int N = Asset.count();

    // ---- structural invariant across the whole determinism corpus ----

    @Test
    public void structuralInvariantsHoldAcrossCorpus() throws IOException {
        Path dir = Path.of("src", "test", "resources", "determinism");
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> scenarios = s.filter(p -> p.toString().endsWith(".scenario")).sorted().toList();
            assertTrue("corpus must not be empty", !scenarios.isEmpty());
            for (Path p : scenarios) {
                AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
                r.execAll(Files.readAllLines(p));
                assertStructural(r.engine(), p.getFileName().toString());
            }
        }
    }

    // ---- conservation of external flow and internal moves ----

    @Test
    public void depositsAndWithdrawalsMoveTotalsExactly() {
        AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(List.of(
                "DEPOSIT u=1 asset=0 amt=1000.0",
                "DEPOSIT u=2 asset=1 amt=5.0",
                "WITHDRAW u=1 asset=0 amt=200.0"));
        long[] total = perAssetTotal(r.engine());
        assertEquals(FixedPoint.fromDouble(800.0), total[Asset.USD.id()]);
        assertEquals(FixedPoint.fromDouble(5.0), total[Asset.BTC.id()]);
        assertStructural(r.engine(), "deposit/withdraw");
    }

    @Test
    public void holdDoesNotChangePerAssetTotal() {
        AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(List.of("DEPOSIT u=1 asset=0 amt=1000.0"));
        long[] before = perAssetTotal(r.engine());
        r.execAll(List.of("HOLD order=1 u=1 asset=0 amt=600.0"));
        assertArrayEquals("hold must move avail->locked, not change the total", before, perAssetTotal(r.engine()));
        assertStructural(r.engine(), "hold");
    }

    @Test
    public void settlementConservesEveryAsset() {
        AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(List.of(
                "DEPOSIT u=100 asset=1 amt=1.0",          // seller: 1 BTC
                "DEPOSIT u=200 asset=0 amt=60000.0",      // buyer: 60000 USD
                "HOLD order=1 u=100 asset=1 amt=1.0",
                "HOLD order=2 u=200 asset=0 amt=60000.0"));
        long[] before = perAssetTotal(r.engine());
        r.execAll(List.of(
                "SETTLE tradeId=1 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=1.0 takerBuy=1"));
        assertArrayEquals("settlement must conserve every asset", before, perAssetTotal(r.engine()));
        // And the money actually landed where it should:
        assertEquals(FixedPoint.fromDouble(1.0), r.engine().account(200).available(Asset.BTC.id()));
        assertEquals(FixedPoint.fromDouble(60000.0), r.engine().account(100).available(Asset.USD.id()));
        assertStructural(r.engine(), "settle");
    }

    @Test
    public void buyPriceImprovementResidualIsConservedAndReleased() {
        // Buyer holds 60000 USD for 1 BTC but fills at 59000; the 1000 residual is released on terminal.
        AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(List.of(
                "DEPOSIT u=100 asset=1 amt=1.0",
                "DEPOSIT u=200 asset=0 amt=60000.0",
                "HOLD order=1 u=100 asset=1 amt=1.0",
                "HOLD order=2 u=200 asset=0 amt=60000.0"));
        long[] before = perAssetTotal(r.engine());
        r.execAll(List.of(
                "SETTLE tradeId=1 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=59000.0 qty=1.0 takerBuy=1",
                "RELEASE order=2 u=200",
                "RELEASE order=1 u=100"));
        assertArrayEquals("price-improvement settle+release must conserve every asset", before, perAssetTotal(r.engine()));
        // Buyer keeps the 1000 USD improvement as available; seller receives 59000 USD.
        assertEquals(FixedPoint.fromDouble(1000.0), r.engine().account(200).available(Asset.USD.id()));
        assertEquals(FixedPoint.fromDouble(59000.0), r.engine().account(100).available(Asset.USD.id()));
        assertEquals(0L, r.engine().account(200).locked(Asset.USD.id()));
        assertStructural(r.engine(), "price-improvement");
    }

    // ---- helpers ----

    /** Σ(available + locked) over all accounts, per asset. */
    private static long[] perAssetTotal(AssetsEngine e) {
        long[] t = new long[N];
        e.forEachAccount(acc -> {
            for (int a = 0; a < N; a++) {
                t[a] += acc.available(a) + acc.locked(a);
            }
        });
        return t;
    }

    /** No negative balances, and for each (account, asset) locked == Σ remaining over that asset's holds. */
    private static void assertStructural(AssetsEngine e, String ctx) {
        e.forEachAccount(acc -> {
            long[] sumHold = new long[N];
            acc.forEachHold((orderId, assetId, remaining, omsManagedRelease) -> sumHold[assetId] += remaining);
            for (int a = 0; a < N; a++) {
                assertTrue(ctx + ": available>=0 (user " + acc.userId() + " asset " + a + ")", acc.available(a) >= 0);
                assertTrue(ctx + ": locked>=0 (user " + acc.userId() + " asset " + a + ")", acc.locked(a) >= 0);
                assertEquals(ctx + ": locked==Σholds (user " + acc.userId() + " asset " + a + ")",
                        acc.locked(a), sumHold[a]);
            }
        });
    }
}

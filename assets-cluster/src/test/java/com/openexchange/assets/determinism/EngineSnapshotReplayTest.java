// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Determinism ∩ durability centerpiece: a snapshot/restore woven into a money-command stream must be
 * invisible to the output. For each case we run {@code A + B} and {@code A + SNAPSHOT + B} (where
 * {@code SNAPSHOT} serializes the engine and restores into a brand-new engine — an in-process
 * "restart") and assert the captured event streams are byte-identical. Balances, holds, and the
 * settlement high-water must flow continuously across the restart.
 */
public class EngineSnapshotReplayTest {

    @Test
    public void holdSurvivesRestartThenReleases() {
        List<String> a = List.of(
                "CLOCK 1000",
                "DEPOSIT u=100 asset=0 amt=1000.0",
                "HOLD order=1 u=100 asset=0 amt=600.0");
        List<String> b = List.of(
                "RELEASE order=1 u=100");
        assertSnapshotTransparent(a, b);
    }

    @Test
    public void partialSettleAcrossRestart() {
        // Seller holds 1.0 BTC, buyer holds quote for 1.0 BTC; a 0.5 fill settles, restart, then the
        // residual is released on terminal.
        List<String> a = List.of(
                "CLOCK 1000",
                "DEPOSIT u=100 asset=1 amt=2.0",              // seller BTC
                "DEPOSIT u=200 asset=0 amt=200000.0",         // buyer USD
                "HOLD order=1 u=100 asset=1 amt=1.0",         // seller holds 1 BTC
                "HOLD order=2 u=200 asset=0 amt=60000.0");    // buyer holds 60000 USD (1 BTC @ 60000)
        List<String> b = List.of(
                "SETTLE tradeId=1 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=0.5 takerBuy=1",
                "RELEASE order=1 u=100",
                "RELEASE order=2 u=200");
        assertSnapshotTransparent(a, b);
    }

    @Test
    public void settleIdempotencyHighWaterSurvivesRestart() {
        // The same tradeId re-delivered AFTER a restart must still be a no-op — the high-water must
        // ride through the snapshot.
        List<String> a = List.of(
                "CLOCK 1000",
                "DEPOSIT u=100 asset=1 amt=2.0",
                "DEPOSIT u=200 asset=0 amt=200000.0",
                "HOLD order=1 u=100 asset=1 amt=1.0",
                "HOLD order=2 u=200 asset=0 amt=60000.0",
                "SETTLE tradeId=1 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=0.5 takerBuy=1");
        List<String> b = List.of(
                // duplicate of tradeId=1 — must be ignored post-restart
                "SETTLE tradeId=1 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=0.5 takerBuy=1");
        assertSnapshotTransparent(a, b);
    }

    @Test
    public void doubleSnapshotIsAlsoTransparent() {
        List<String> a = List.of(
                "CLOCK 1000",
                "DEPOSIT u=100 asset=0 amt=1000.0",
                "HOLD order=1 u=100 asset=0 amt=400.0");
        List<String> b = List.of(
                "HOLD order=2 u=100 asset=0 amt=200.0");

        String noSnap = AssetsScenarioRunner.runLines(concat(a, b));
        String withSnaps = AssetsScenarioRunner.runLines(concat(a, Arrays.asList("SNAPSHOT", "SNAPSHOT"), b));
        assertEquals(noSnap, withSnaps);
    }

    @Test
    public void snapshotOnEmptyStateThenActivityIsTransparent() {
        List<String> a = List.of("CLOCK 1000");
        List<String> b = List.of(
                "DEPOSIT u=100 asset=0 amt=1000.0",
                "HOLD order=1 u=100 asset=0 amt=600.0");
        assertSnapshotTransparent(a, b);
    }

    private static void assertSnapshotTransparent(List<String> a, List<String> b) {
        String noSnap = AssetsScenarioRunner.runLines(concat(a, b));
        String withSnap = AssetsScenarioRunner.runLines(concat(a, List.of("SNAPSHOT"), b));
        assertEquals("snapshot/restore must be invisible to subsequent output", noSnap, withSnap);
    }

    @SafeVarargs
    private static List<String> concat(List<String>... parts) {
        List<String> out = new ArrayList<>();
        for (List<String> p : parts) {
            out.addAll(p);
        }
        return out;
    }
}

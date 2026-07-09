// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Round-trip fidelity for the snapshot format: {@code deserialize(serialize(state))} restores an
 * identical engine — balances, holds, and both engine scalars ({@code lastAppliedTradeId},
 * {@code consumePosition}). This is the durability contract every restart and failover depends on.
 */
public class BalanceSnapshotCodecTest {

    private static final int N = Asset.count();

    private static AssetsEngine engineWithState() {
        AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(List.of(
                "DEPOSIT u=100 asset=1 amt=2.0",
                "DEPOSIT u=200 asset=0 amt=200000.0",
                "DEPOSIT u=300 asset=2 amt=10.0",
                "HOLD order=1 u=100 asset=1 amt=1.5",
                "HOLD order=2 u=200 asset=0 amt=120000.0",
                // partial settle so a residual hold + a nonzero high-water both exist in the snapshot
                "SETTLE tradeId=7 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=1.0 takerBuy=1"));
        return r.engine();
    }

    @Test
    public void roundTripPreservesFullState() {
        AssetsEngine orig = engineWithState();

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = BalanceSnapshotCodec.serialize(orig, buf);

        AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(buf, 0, len, restored);

        assertEquals("bytesConsumed", len, d.bytesConsumed);
        assertEquals("accountCount", orig.accountCount(), restored.accountCount());
        assertSameState(orig, restored);
    }

    @Test
    public void highWaterAndConsumePositionSurvive() {
        AssetsEngine orig = engineWithState();
        orig.setConsumePosition(123456789L);

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = BalanceSnapshotCodec.serialize(orig, buf);
        AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.deserialize(buf, 0, len, restored);

        assertEquals("lastAppliedTradeId", 7L, restored.getLastAppliedTradeId());
        assertEquals("consumePosition", 123456789L, restored.getConsumePosition());
    }

    @Test
    public void deserializeHonoursNonZeroOffset() {
        AssetsEngine orig = engineWithState();
        ExpandableArrayBuffer src = new ExpandableArrayBuffer();
        int len = BalanceSnapshotCodec.serialize(orig, src);

        // Place the payload at a nonzero offset and decode from there.
        final int offset = 64;
        ExpandableArrayBuffer shifted = new ExpandableArrayBuffer();
        shifted.putBytes(offset, src, 0, len);

        AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(shifted, offset, len, restored);
        assertEquals(len, d.bytesConsumed);
        assertSameState(orig, restored);
    }

    private static void assertSameState(AssetsEngine a, AssetsEngine b) {
        assertEquals(a.getLastAppliedTradeId(), b.getLastAppliedTradeId());
        assertEquals(a.getConsumePosition(), b.getConsumePosition());
        assertEquals(a.accountCount(), b.accountCount());
        a.forEachAccount(acc -> {
            Account bacc = b.account(acc.userId());
            assertNotNull("missing account " + acc.userId(), bacc);
            for (int as = 0; as < N; as++) {
                assertEquals("available user " + acc.userId() + " asset " + as, acc.available(as), bacc.available(as));
                assertEquals("locked user " + acc.userId() + " asset " + as, acc.locked(as), bacc.locked(as));
            }
            assertEquals("holdCount user " + acc.userId(), acc.holdCount(), bacc.holdCount());
            acc.forEachHold((orderId, assetId, remaining) -> {
                assertEquals("hold asset user " + acc.userId() + " order " + orderId, assetId, bacc.holdAssetId(orderId));
                assertEquals("hold remaining user " + acc.userId() + " order " + orderId, remaining, bacc.holdRemaining(orderId));
            });
        });
    }
}

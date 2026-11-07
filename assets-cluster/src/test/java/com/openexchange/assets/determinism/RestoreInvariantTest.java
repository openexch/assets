// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S2 (layout page §3): a restored ledger is verified before it serves. {@code locked == Σ remaining}
 * per (account, asset) is the invariant every hold path preserves; bytes that break it are a corrupt
 * or foreign snapshot, and the node must refuse to start on them rather than settle against them.
 */
public class RestoreInvariantTest {

    private static ExpandableArrayBuffer image(final int[] lengthOut) {
        final AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(List.of(
                "DEPOSIT u=100 asset=0 amt=1000.0",
                "HOLD order=1 u=100 asset=0 amt=400.0",
                "HOLD order=2 u=100 asset=0 amt=100.0"));
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        lengthOut[0] = BalanceSnapshotCodec.serialize(r.engine(), buf);
        return buf;
    }

    @Test
    public void aConsistentImagePassesVerification() {
        final int[] len = new int[1];
        final ExpandableArrayBuffer buf = image(len);
        final AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.deserialize(buf, 0, len[0], restored);
        restored.verifyRestoredInvariants();
    }

    @Test
    public void lockedThatDoesNotMatchTheHoldsIsRefused() {
        final int[] len = new int[1];
        final ExpandableArrayBuffer buf = image(len);
        // Account 100 lies right after [tag][7 scalars][flags][sectionCount][id][len][numAccounts]:
        // userId(8), numAssets(4), then (available, locked) for asset 0.
        final int accountOffset = 8 + 7 * 8 + 1 + 4 + 2 + 4 + 4;
        final int lockedOffset = accountOffset + 8 + 4 + 8;
        buf.putLong(lockedOffset, buf.getLong(lockedOffset) + 1);
        final AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.deserialize(buf, 0, len[0], restored);
        final IllegalStateException e = assertThrows(IllegalStateException.class, restored::verifyRestoredInvariants);
        assertTrue(e.getMessage(), e.getMessage().contains("locked"));
    }

    @Test
    public void aNegativeBalanceIsRefused() {
        final int[] len = new int[1];
        final ExpandableArrayBuffer buf = image(len);
        final int accountOffset = 8 + 7 * 8 + 1 + 4 + 2 + 4 + 4;
        final int availableOffset = accountOffset + 8 + 4;
        buf.putLong(availableOffset, -5L);
        final AssetsEngine restored = new AssetsEngine();
        BalanceSnapshotCodec.deserialize(buf, 0, len[0], restored);
        assertThrows(IllegalStateException.class, restored::verifyRestoredInvariants);
    }
}

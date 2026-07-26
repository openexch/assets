// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * A snapshot must be a pure function of the engine's <em>state</em>, never of the <em>history</em>
 * that produced it. Two engines holding the same balances and the same holds must serialize to the
 * same bytes, whether they got there by replaying the log from genesis or by restoring an earlier
 * snapshot and replaying the tail.
 *
 * <p>This is the property bundle replay is checked against: without it a byte comparison between a
 * from-genesis node and a from-bundle node reports divergence on a ledger that has not diverged,
 * which is a false alarm on the money path and therefore worse than no check at all.</p>
 *
 * <p>Not covered by {@link EngineSnapshotReplayTest}, which asserts a snapshot is invisible to
 * subsequent <em>output</em>. Both engines below emit identical events; only their bytes differ.</p>
 */
public class SnapshotCanonicalOrderTest {

    @Test
    public void snapshotBytesAreAFunctionOfStateNotHistory() {
        // A: reaches the state the long way — 40 holds placed, 36 released. Removals leave the hold
        // map compacted and grown, exactly as a long-lived account looks in production.
        final AssetsEngine a = freshEngine();
        deposit(a, 100L, 0, 10_000_000L);
        for (long order = 1; order <= 40; order++) {
            hold(a, 100L, order, 0, 1_000L);
        }
        for (long order = 1; order <= 40; order++) {
            if (order % 10 != 0) {
                release(a, 100L, order);
            }
        }

        // B: the same state, reached by restoring A's snapshot into a fresh engine — the shape a node
        // recovering from a bundle is in.
        final byte[] fromA = snapshot(a);
        final AssetsEngine b = freshEngine();
        BalanceSnapshotCodec.deserialize(new UnsafeBuffer(fromA), 0, fromA.length, b);

        assertArrayEquals("snapshot bytes must depend on state alone, not on how the state was reached",
                fromA, snapshot(b));
    }

    @Test
    public void accountOrderDoesNotDependOnFirstTouchOrder() {
        // The same ledger, built by touching users in opposite orders. Same state, same bytes.
        final long[] users = {7L, 3L, 11L, 5L, 19L, 2L, 23L, 13L, 17L};

        final AssetsEngine forward = freshEngine();
        for (final long user : users) {
            deposit(forward, user, 0, 1_000L);
        }

        final AssetsEngine reverse = freshEngine();
        for (int i = users.length - 1; i >= 0; i--) {
            deposit(reverse, users[i], 0, 1_000L);
        }

        assertArrayEquals("account order must not depend on which user was seen first",
                snapshot(forward), snapshot(reverse));
    }

    // ---- helpers ----

    private static AssetsEngine freshEngine() {
        final AssetsEngine engine = new AssetsEngine();
        engine.setEventSink(new RecordingAssetsSink());
        return engine;
    }

    private static byte[] snapshot(AssetsEngine engine) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(engine, buffer);
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    private static void deposit(AssetsEngine engine, long userId, int assetId, long amount) {
        final DepositCommand c = new DepositCommand();
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, c, 1_000L);
    }

    private static void hold(AssetsEngine engine, long userId, long orderId, int assetId, long amount) {
        final HoldCommand c = new HoldCommand();
        c.setUserId(userId);
        c.setOrderId(orderId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_HOLD, c, 1_000L);
    }

    private static void release(AssetsEngine engine, long userId, long orderId) {
        final ReleaseCommand c = new ReleaseCommand();
        c.setUserId(userId);
        c.setOrderId(orderId);
        c.setAmount(-1L);
        engine.applyCommand(AssetsEngine.CMD_RELEASE, c, 1_000L);
    }
}

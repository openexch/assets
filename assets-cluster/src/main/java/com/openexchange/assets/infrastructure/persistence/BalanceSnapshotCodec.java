// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Pure serialize / deserialize of the Assets Engine's whole state to and from a byte buffer, with NO
 * Aeron dependency. An infrastructure adapter: it reads the {@link Account} aggregates through their
 * public accessors and writes bytes — the domain never knows about serialization.
 *
 * <p>The engine scalars ({@code lastAppliedTradeId}, {@code consumePosition}) are written first, so the
 * consume-position is restored <em>atomically</em> with the balances it produced — recovery has no
 * skew between "which trades are reflected" and "the balances that reflect them".</p>
 *
 * <p><b>BYTE FORMAT (must remain stable — recovery depends on it):</b></p>
 * <pre>
 *   [lastAppliedTradeId : long]
 *   [consumePosition    : long]
 *   [numAccounts        : int]
 *   repeat numAccounts:
 *     [userId    : long]
 *     [numAssets : int]                                  // = Asset.count()
 *     repeat numAssets: [available : long][locked : long]   // dense, assetId order 0..numAssets-1
 *     [numHolds  : int]
 *     repeat numHolds:  [orderId : long][assetId : int][remaining : long]
 * </pre>
 *
 * <p>All scalars use the buffer's native byte order (Agrona default).</p>
 */
public final class BalanceSnapshotCodec {

    private BalanceSnapshotCodec() {
    }

    /** Diagnostics from a restore (the engine's scalars are applied in-place by {@link #deserialize}). */
    public static final class Decoded {
        public final long lastAppliedTradeId;
        public final long consumePosition;
        public final int accountCount;
        public final int holdCount;
        public final int bytesConsumed;

        Decoded(long lastAppliedTradeId, long consumePosition, int accountCount, int holdCount, int bytesConsumed) {
            this.lastAppliedTradeId = lastAppliedTradeId;
            this.consumePosition = consumePosition;
            this.accountCount = accountCount;
            this.holdCount = holdCount;
            this.bytesConsumed = bytesConsumed;
        }
    }

    /**
     * Serialize the engine's full state into {@code dst}.
     *
     * @return number of bytes written — offer {@code dst, 0, length}
     */
    public static int serialize(AssetsEngine engine, MutableDirectBuffer dst) {
        final int assets = Asset.count();
        final int[] pos = {0};

        dst.putLong(pos[0], engine.getLastAppliedTradeId());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getConsumePosition());
        pos[0] += 8;
        dst.putInt(pos[0], engine.accountCount());
        pos[0] += 4;

        engine.forEachAccount(account -> {
            dst.putLong(pos[0], account.userId());
            pos[0] += 8;
            dst.putInt(pos[0], assets);
            pos[0] += 4;
            for (int a = 0; a < assets; a++) {
                dst.putLong(pos[0], account.available(a));
                pos[0] += 8;
                dst.putLong(pos[0], account.locked(a));
                pos[0] += 8;
            }
            dst.putInt(pos[0], account.holdCount());
            pos[0] += 4;
            account.forEachHold((orderId, assetId, remaining) -> {
                dst.putLong(pos[0], orderId);
                pos[0] += 8;
                dst.putInt(pos[0], assetId);
                pos[0] += 4;
                dst.putLong(pos[0], remaining);
                pos[0] += 8;
            });
        });

        return pos[0];
    }

    /**
     * Decode a snapshot payload into {@code engine}, restoring its accounts, holds, and scalars.
     * The engine is assumed fresh (empty); recovery constructs a new engine and restores into it.
     */
    public static Decoded deserialize(DirectBuffer src, int offset, int length, AssetsEngine engine) {
        int pos = offset;

        final long lastAppliedTradeId = src.getLong(pos);
        pos += 8;
        final long consumePosition = src.getLong(pos);
        pos += 8;
        engine.setLastAppliedTradeId(lastAppliedTradeId);
        engine.setConsumePosition(consumePosition);

        final int numAccounts = src.getInt(pos);
        pos += 4;

        int holdTotal = 0;
        for (int i = 0; i < numAccounts; i++) {
            final long userId = src.getLong(pos);
            pos += 8;
            final Account account = engine.getOrCreateAccount(userId);

            final int numAssets = src.getInt(pos);
            pos += 4;
            for (int a = 0; a < numAssets; a++) {
                final long available = src.getLong(pos);
                pos += 8;
                final long locked = src.getLong(pos);
                pos += 8;
                account.restoreBalance(a, available, locked);
            }

            final int numHolds = src.getInt(pos);
            pos += 4;
            for (int h = 0; h < numHolds; h++) {
                final long orderId = src.getLong(pos);
                pos += 8;
                final int assetId = src.getInt(pos);
                pos += 4;
                final long remaining = src.getLong(pos);
                pos += 8;
                account.restoreHold(orderId, assetId, remaining);
            }
            holdTotal += numHolds;
        }

        return new Decoded(lastAppliedTradeId, consumePosition, numAccounts, holdTotal, pos - offset);
    }
}

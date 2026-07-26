// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.infrastructure.Logger;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Pure serialize / deserialize of the Assets Engine's whole state to and from a byte buffer, with NO
 * Aeron dependency. An infrastructure adapter: it reads the {@link Account} aggregates through their
 * public accessors and writes bytes — the domain never knows about serialization.
 *
 * <p>The engine scalars ({@code lastAppliedTradeId}, {@code consumePosition}, {@code journalSeq}) are
 * written first, so the consume-position and the journal sequence are restored <em>atomically</em>
 * with the balances they produced: recovery has no skew between "which trades are reflected" and
 * "the balances that reflect them", and the money journal resumes without gaps or reuse.</p>
 *
 * <p><b>BYTE FORMAT v3 (must remain stable; recovery depends on it):</b></p>
 * <pre>
 *   [layoutTag          : long]   // LAYOUT_V3_TAG (-3)
 *   [lastAppliedTradeId : long]
 *   [consumePosition    : long]
 *   [journalSeq         : long]
 *   [journalEnabled     : byte]   // v3+; whether the cluster's money journal is armed
 *   [numAccounts        : int]
 *   repeat numAccounts:
 *     [userId    : long]
 *     [numAssets : int]                                  // = Asset.count()
 *     repeat numAssets: [available : long][locked : long]   // dense, assetId order 0..numAssets-1
 *     [numHolds  : int]
 *     repeat numHolds:  [orderId : long][assetId : int][remaining : long][omsManagedRelease : byte]
 * </pre>
 *
 * <p><b>Versioning.</b> The v1 layout had no tag: it began directly with {@code lastAppliedTradeId}
 * (and had no {@code journalSeq}). Trade ids are never negative, so a negative first long
 * unambiguously identifies a tagged layout: {@code first >= 0} decodes as v1 (with a WARN and
 * {@code journalSeq = 0}), {@code first == -2} decodes as v2, {@code first == -3} as v3, anything else
 * fails loudly. Chosen over a leading version byte because a byte cannot be distinguished from v1's
 * untagged first long.</p>
 *
 * <p><b>Restoring a pre-v3 snapshot leaves the money journal DISARMED</b>, and says so at WARN. Before
 * v3 the setting lived in a node's environment and is simply not present in those bytes; guessing it —
 * by reading the environment during a restore, say — would make recovery depend on where it ran, which
 * is the whole class of defect v3 exists to remove. A cluster that was journaling before the upgrade
 * resumes with one {@code SetMoneyJournal} command, and resumes at its existing {@code journalSeq}, so
 * nothing is gapped or reused.</p>
 *
 * <p><b>CANONICAL ORDER (load-bearing).</b> Accounts are written in ascending {@code userId} and each
 * account's holds in ascending {@code orderId}, so the bytes are a function of the engine's STATE and
 * not of the history that produced it. Iterating the underlying hash maps instead would make the
 * snapshot depend on the order users were first touched and on which holds had been released — so a
 * node that replayed the log from genesis and a node that restored an earlier snapshot and replayed
 * the tail would write different bytes for an identical ledger. That difference is harmless to the
 * ledger but fatal to verification: comparing snapshots is how a bundle replay is checked, and a false
 * divergence alarm on the money path is worse than no check at all.</p>
 *
 * <p>Order is a serialize-side property only: {@link #deserialize} reads whatever order it is given,
 * so snapshots written before this was canonical still restore.</p>
 *
 * <p>All scalars use the buffer's native byte order (Agrona default).</p>
 */
public final class BalanceSnapshotCodec {

    private static final Logger LOG = Logger.getLogger(BalanceSnapshotCodec.class);

    /** v2 layout tag (negative: impossible as v1's leading lastAppliedTradeId). */
    static final long LAYOUT_V2_TAG = -2L;

    /** v3 layout tag: v2 plus the replicated journalEnabled flag. */
    static final long LAYOUT_V3_TAG = -3L;

    private BalanceSnapshotCodec() {
    }

    /** Diagnostics from a restore (the engine's scalars are applied in-place by {@link #deserialize}). */
    public static final class Decoded {
        public final long lastAppliedTradeId;
        public final long consumePosition;
        public final long journalSeq;
        public final int accountCount;
        public final int holdCount;
        public final int bytesConsumed;

        Decoded(long lastAppliedTradeId, long consumePosition, long journalSeq,
                int accountCount, int holdCount, int bytesConsumed) {
            this.lastAppliedTradeId = lastAppliedTradeId;
            this.consumePosition = consumePosition;
            this.journalSeq = journalSeq;
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

        dst.putLong(pos[0], LAYOUT_V3_TAG);
        pos[0] += 8;
        dst.putLong(pos[0], engine.getLastAppliedTradeId());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getConsumePosition());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getJournalSeq());
        pos[0] += 8;
        dst.putByte(pos[0], (byte) (engine.isMoneyJournalEnabled() ? 1 : 0));
        pos[0] += 1;
        dst.putInt(pos[0], engine.accountCount());
        pos[0] += 4;

        // Accounts ascending by userId, holds ascending by orderId — see CANONICAL ORDER above.
        engine.forEachAccountByUserId(account -> {
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
            account.forEachHoldByOrderId((orderId, assetId, remaining, omsManagedRelease) -> {
                dst.putLong(pos[0], orderId);
                pos[0] += 8;
                dst.putInt(pos[0], assetId);
                pos[0] += 4;
                dst.putLong(pos[0], remaining);
                pos[0] += 8;
                dst.putByte(pos[0], (byte) (omsManagedRelease ? 1 : 0));
                pos[0] += 1;
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

        // Layout detection: v1 snapshots are untagged and start with lastAppliedTradeId (never
        // negative); v2 starts with the negative LAYOUT_V2_TAG. See the class doc.
        final long first = src.getLong(pos);
        final long lastAppliedTradeId;
        final long consumePosition;
        final long journalSeq;
        final boolean journalEnabled;
        if (first >= 0) {
            LOG.warn("old-format (v1) balance snapshot: no journalSeq field, defaulting journalSeq=0");
            lastAppliedTradeId = first;
            pos += 8;
            consumePosition = src.getLong(pos);
            pos += 8;
            journalSeq = 0L;
            journalEnabled = false;
        } else if (first == LAYOUT_V2_TAG || first == LAYOUT_V3_TAG) {
            pos += 8;
            lastAppliedTradeId = src.getLong(pos);
            pos += 8;
            consumePosition = src.getLong(pos);
            pos += 8;
            journalSeq = src.getLong(pos);
            pos += 8;
            if (first == LAYOUT_V3_TAG) {
                journalEnabled = src.getByte(pos) != 0;
                pos += 1;
            } else {
                // Pre-v3: the setting was a node environment variable and is not in these bytes. Restore
                // DISARMED and say so — see the class doc on why this is not guessed.
                LOG.warn("pre-v3 balance snapshot: money journal restored DISARMED (journalSeq=%d kept); "
                        + "send SetMoneyJournal to re-arm if this cluster was journaling", journalSeq);
                journalEnabled = false;
            }
        } else {
            throw new IllegalStateException("unknown balance snapshot layout tag: " + first);
        }
        engine.setLastAppliedTradeId(lastAppliedTradeId);
        engine.setConsumePosition(consumePosition);
        engine.setJournalSeq(journalSeq);
        engine.setMoneyJournalEnabled(journalEnabled);

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
                final boolean omsManagedRelease = src.getByte(pos) != 0;
                pos += 1;
                account.restoreHold(orderId, assetId, remaining, omsManagedRelease);
            }
            holdTotal += numHolds;
        }

        return new Decoded(lastAppliedTradeId, consumePosition, journalSeq, numAccounts, holdTotal, pos - offset);
    }
}

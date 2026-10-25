// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Pure serialize / deserialize of the core's replicated state to and from a byte buffer, with NO Aeron
 * dependency. An infrastructure adapter: it reads the {@link Account} aggregates through their public
 * accessors and writes bytes; the domain never knows about serialization.
 *
 * <p><b>PAYLOAD v4 (RC1 S2; the contract is {@code notebook/.../RC1-core/S2/snapshot-v4-layout.md}):</b></p>
 * <pre>
 *   [layoutTag          : long]   // LAYOUT_V4_TAG (-4); anything else is refused
 *   [lastAppliedTradeId : long]
 *   [consumePosition    : long]
 *   [journalSeq         : long]
 *   [lastLogTime        : long]   // monotone log time
 *   [nextOrderId        : long]   // RC1 S4; 0 until then
 *   [eventSeq           : long]   // RC1 S4; 0 until then
 *   [dispatchSeq        : long]   // RC1 S4; 0 until then
 *   [flags              : byte]   // bit0 journalEnabled; any other bit set is refused
 *   [sectionCount       : int]
 *   repeat sectionCount, ids strictly ascending:
 *     [sectionId : short][sectionLength : int][payload : sectionLength bytes]
 * </pre>
 * Section 1 ACCOUNTS: {@code [numAccounts:int]} then per account {@code userId:long, numAssets:int,
 * (available:long, locked:long) x numAssets, numHolds:int, (orderId:long, assetId:int, remaining:long,
 * omsManagedRelease:byte) x numHolds}. Sections 2..8 are reserved by the layout page and land with their
 * slices; a reader refuses an id it does not know and treats an absent section as empty.
 *
 * <p>The engine scalars are written first, so the consume position and the journal sequence are restored
 * <em>atomically</em> with the balances they produced: recovery has no skew between "which trades are
 * reflected" and "the balances that reflect them".</p>
 *
 * <p><b>No legacy read paths.</b> v1 (untagged), v2 (-2) and v3 (-3) images are refused, not decoded
 * (clean genesis, K10): every silent "it looked like an old layout" restore is a way to serve a ledger
 * nobody verified. The payload travels inside the kit {@code SnapshotEnvelope} (magic, length, CRC32C);
 * this codec neither writes nor checks the envelope, the service does.</p>
 *
 * <p><b>CANONICAL ORDER (load-bearing).</b> Accounts are written in ascending {@code userId} and each
 * account's holds in ascending {@code orderId}, so the bytes are a function of the engine's STATE and not
 * of the history that produced it. Comparing snapshots is how a bundle replay is verified, and a false
 * divergence alarm on the money path is worse than no check at all.</p>
 *
 * <p>All scalars use the buffer's native byte order (Agrona default).</p>
 */
public final class BalanceSnapshotCodec {

    public static final long LAYOUT_V4_TAG = -4L;

    public static final short SECTION_ACCOUNTS = 1;
    public static final short SECTION_ORDERS = 2;
    public static final short SECTION_USER_RISK = 3;
    public static final short SECTION_MARKET_STATE = 4;
    public static final short SECTION_GATES = 5;
    public static final short SECTION_REQUEST_IDS = 6;
    public static final short SECTION_READER_REGISTRY = 7;
    public static final short SECTION_CORE_CONFIG = 8;

    private static final int FLAG_JOURNAL_ENABLED = 1;
    private static final int KNOWN_FLAGS = FLAG_JOURNAL_ENABLED;
    private static final int SECTION_HEADER = 2 + 4;

    private BalanceSnapshotCodec() {
    }

    /** Diagnostics from a restore (the engine's scalars are applied in-place by {@link #deserialize}). */
    public static final class Decoded {
        public final long lastAppliedTradeId;
        public final long consumePosition;
        public final long journalSeq;
        public final long lastLogTime;
        public final int accountCount;
        public final int holdCount;
        public final int sectionCount;
        /** Total bytes consumed from the source buffer (so callers can verify the payload boundary). */
        public final int bytesConsumed;

        Decoded(long lastAppliedTradeId, long consumePosition, long journalSeq, long lastLogTime,
                int accountCount, int holdCount, int sectionCount, int bytesConsumed) {
            this.lastAppliedTradeId = lastAppliedTradeId;
            this.consumePosition = consumePosition;
            this.journalSeq = journalSeq;
            this.lastLogTime = lastLogTime;
            this.accountCount = accountCount;
            this.holdCount = holdCount;
            this.sectionCount = sectionCount;
            this.bytesConsumed = bytesConsumed;
        }
    }

    /** Serialize the engine's full state into {@code dst} at offset 0; returns the payload length. */
    public static int serialize(AssetsEngine engine, MutableDirectBuffer dst) {
        return serialize(engine, dst, 0);
    }

    /**
     * Serialize the engine's full state into {@code dst} starting at {@code offset} (the service writes
     * at the envelope's payload offset so a large ledger is never copied).
     *
     * @return number of payload bytes written
     */
    public static int serialize(AssetsEngine engine, MutableDirectBuffer dst, int offset) {
        final int[] pos = {offset};

        dst.putLong(pos[0], LAYOUT_V4_TAG);
        pos[0] += 8;
        dst.putLong(pos[0], engine.getLastAppliedTradeId());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getConsumePosition());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getJournalSeq());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getLastLogTime());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getNextOrderId());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getEventSeq());
        pos[0] += 8;
        dst.putLong(pos[0], engine.getDispatchSeq());
        pos[0] += 8;
        dst.putByte(pos[0], (byte) (engine.isMoneyJournalEnabled() ? FLAG_JOURNAL_ENABLED : 0));
        pos[0] += 1;

        // Sections, ascending by id. Only ACCOUNTS exists on this build; later slices append theirs.
        dst.putInt(pos[0], 1);
        pos[0] += 4;
        pos[0] = writeAccountsSection(engine, dst, pos[0]);

        return pos[0] - offset;
    }

    private static int writeAccountsSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        final int assets = Asset.count();
        final int[] pos = {start};
        dst.putShort(pos[0], SECTION_ACCOUNTS);
        pos[0] += 2;
        final int lengthSlot = pos[0];
        pos[0] += 4;
        final int bodyStart = pos[0];

        dst.putInt(pos[0], engine.accountCount());
        pos[0] += 4;
        // Accounts ascending by userId, holds ascending by orderId: see CANONICAL ORDER above.
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
        dst.putInt(lengthSlot, pos[0] - bodyStart);
        return pos[0];
    }

    /**
     * Decode a v4 payload into {@code engine}, restoring its accounts, holds and scalars. The engine is
     * assumed fresh (empty); recovery constructs a new engine and restores into it. Refuses, with
     * {@link IllegalStateException}, any layout tag other than v4, unknown flag bits, unknown or
     * out-of-order sections, and a section whose declared length does not match what it decodes to.
     */
    public static Decoded deserialize(DirectBuffer src, int offset, int length, AssetsEngine engine) {
        final int end = offset + length;
        int pos = offset;
        if (length < 8 + 7 * 8 + 1 + 4) {
            throw new IllegalStateException("balance snapshot payload too short for the v4 layout: " + length);
        }
        final long tag = src.getLong(pos);
        pos += 8;
        if (tag != LAYOUT_V4_TAG) {
            throw new IllegalStateException("balance snapshot layout tag " + tag + " refused: this build reads only v4 ("
                    + LAYOUT_V4_TAG + "); pre-v4 images are not restored (clean genesis)");
        }
        final long lastAppliedTradeId = src.getLong(pos);
        pos += 8;
        final long consumePosition = src.getLong(pos);
        pos += 8;
        final long journalSeq = src.getLong(pos);
        pos += 8;
        final long lastLogTime = src.getLong(pos);
        pos += 8;
        final long nextOrderId = src.getLong(pos);
        pos += 8;
        final long eventSeq = src.getLong(pos);
        pos += 8;
        final long dispatchSeq = src.getLong(pos);
        pos += 8;
        final int flags = src.getByte(pos) & 0xFF;
        pos += 1;
        if ((flags & ~KNOWN_FLAGS) != 0) {
            throw new IllegalStateException("balance snapshot flags 0x" + Integer.toHexString(flags)
                    + " carry bits this build does not know");
        }
        engine.setLastAppliedTradeId(lastAppliedTradeId);
        engine.setConsumePosition(consumePosition);
        engine.setJournalSeq(journalSeq);
        engine.setLastLogTime(lastLogTime);
        engine.setNextOrderId(nextOrderId);
        engine.setEventSeq(eventSeq);
        engine.setDispatchSeq(dispatchSeq);
        engine.setMoneyJournalEnabled((flags & FLAG_JOURNAL_ENABLED) != 0);

        final int sectionCount = src.getInt(pos);
        pos += 4;
        if (sectionCount < 0) {
            throw new IllegalStateException("balance snapshot section count " + sectionCount);
        }
        int accounts = 0;
        int holds = 0;
        int previousId = 0;
        for (int s = 0; s < sectionCount; s++) {
            if (end - pos < SECTION_HEADER) {
                throw new IllegalStateException("balance snapshot section " + s + " header runs past the payload");
            }
            final int id = src.getShort(pos);
            pos += 2;
            final int sectionLength = src.getInt(pos);
            pos += 4;
            if (id <= previousId) {
                throw new IllegalStateException("balance snapshot section id " + id + " after " + previousId
                        + ": ids must be strictly ascending and unique");
            }
            if (sectionLength < 0 || sectionLength > end - pos) {
                throw new IllegalStateException("balance snapshot section " + id + " length " + sectionLength
                        + " runs past the payload (" + (end - pos) + " left)");
            }
            final int sectionEnd = pos + sectionLength;
            switch (id) {
                case SECTION_ACCOUNTS: {
                    final int[] counts = readAccountsSection(src, pos, sectionEnd, engine);
                    accounts = counts[0];
                    holds = counts[1];
                    if (counts[2] != sectionEnd) {
                        throw new IllegalStateException("balance snapshot ACCOUNTS section declares " + sectionLength
                                + " bytes but decodes " + (counts[2] - pos));
                    }
                    break;
                }
                default:
                    throw new IllegalStateException("balance snapshot section id " + id
                            + " is not known to this build (layout page sections 2..8 land with their slices)");
            }
            pos = sectionEnd;
            previousId = id;
        }
        return new Decoded(lastAppliedTradeId, consumePosition, journalSeq, lastLogTime, accounts, holds,
                sectionCount, pos - offset);
    }

    /** @return {accounts, holds, endPosition} */
    private static int[] readAccountsSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        final int numAccounts = src.getInt(pos);
        pos += 4;
        int holdTotal = 0;
        for (int i = 0; i < numAccounts; i++) {
            if (limit - pos < 8 + 4) {
                throw new IllegalStateException("balance snapshot ACCOUNTS section truncated at account " + i);
            }
            final long userId = src.getLong(pos);
            pos += 8;
            final Account account = engine.getOrCreateAccount(userId);
            final int numAssets = src.getInt(pos);
            pos += 4;
            if (numAssets < 0 || numAssets > Asset.count() || limit - pos < numAssets * 16 + 4) {
                throw new IllegalStateException("balance snapshot ACCOUNTS section: account " + userId
                        + " declares " + numAssets + " assets");
            }
            for (int a = 0; a < numAssets; a++) {
                final long available = src.getLong(pos);
                pos += 8;
                final long locked = src.getLong(pos);
                pos += 8;
                account.restoreBalance(a, available, locked);
            }
            final int numHolds = src.getInt(pos);
            pos += 4;
            if (numHolds < 0 || limit - pos < numHolds * 21) {
                throw new IllegalStateException("balance snapshot ACCOUNTS section: account " + userId
                        + " declares " + numHolds + " holds");
            }
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
        return new int[] {numAccounts, holdTotal, pos};
    }
}

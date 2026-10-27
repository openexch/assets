// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.feed.FeedState;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.application.engine.order.MarketRisk;
import com.openexchange.assets.application.engine.order.OrderEngine;
import com.openexchange.assets.application.engine.order.RequestTable;
import com.openexchange.assets.application.engine.order.UserRisk;
import com.openexchange.assets.application.engine.session.SessionAuthority;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
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
    /** Live session authorities (RC1 S3, K6): replicated-derived, so a restored member agrees with its peers. */
    public static final short SECTION_SESSIONS = 9;
    /**
     * RC2 S7 (contract 3.7, 14): the feed state ({@code FeedState}), exactly {@link FeedState#SNAPSHOT_BYTES} bytes. Written
     * iff the recorded behaviour is 3 or more, and mandatory then; an image below behaviour 3 that carries it is refused.
     * Presence is tracked by the codec, because an absent section and a present all-zero one restore the same state.
     */
    public static final short SECTION_FEED_STATE = 10;
    /**
     * RESERVED (RC2 S1 4.9): the ME link (release backlog, meBehaviorVersion, meLedgerSize, meLedgerCapacity), content
     * owned by RC2 S8. Neither written nor read by this build: refused as an unknown section until S8 lands.
     */
    public static final short SECTION_ME_LINK = 11;

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

        // Sections, ascending by id: 1 ACCOUNTS, 2 ORDERS, 3 USER_RISK, 4 MARKET_STATE, 5 GATES, 6 REQUEST_IDS,
        // 9 SESSIONS on this build (RC1 S4); 7 and 8 land with their slices.
        final boolean withConfig = engine.coreConfig().recorded() != null;
        final boolean withFeed = engine.behaviorV3(); // RC2 S7 3.7: section 10 iff behaviour >= 3
        dst.putInt(pos[0], (withConfig ? 8 : 7) + (withFeed ? 1 : 0));
        pos[0] += 4;
        pos[0] = writeAccountsSection(engine, dst, pos[0]);
        pos[0] = writeOrdersSection(engine, dst, pos[0]);
        pos[0] = writeUserRiskSection(engine, dst, pos[0]);
        pos[0] = writeMarketStateSection(engine, dst, pos[0]);
        pos[0] = writeGatesSection(engine, dst, pos[0]);
        pos[0] = writeRequestsSection(engine, dst, pos[0]);
        if (withConfig) {
            pos[0] = writeCoreConfigSection(engine, dst, pos[0]); // RC1 S7: only when a config is recorded
        }
        pos[0] = writeSessionsSection(engine, dst, pos[0]);
        if (withFeed) {
            pos[0] = writeFeedStateSection(engine, dst, pos[0]);
        }

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

    /** Section 2: live order records, ascending orderId, CoreOrder.SNAPSHOT_BYTES each (layout page). */
    private static int writeOrdersSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        final int[] pos = {start};
        dst.putShort(pos[0], SECTION_ORDERS);
        pos[0] += 2;
        final int lengthSlot = pos[0];
        pos[0] += 4;
        final int bodyStart = pos[0];
        dst.putInt(pos[0], engine.orders().orders().size());
        pos[0] += 4;
        engine.orders().orders().forEachByOrderId(o -> pos[0] = writeOrder(dst, pos[0], o));
        dst.putInt(lengthSlot, pos[0] - bodyStart);
        return pos[0];
    }

    private static int writeOrder(MutableDirectBuffer dst, int p, CoreOrder o) {
        final long[] longs = {o.orderId, o.userId, o.clientOrderIdHigh, o.clientOrderIdLow, o.requestIdHigh, o.requestIdLow,
                o.price, o.quantity, o.budget, o.stopPrice, o.trailingDelta, o.trailingArmPrice, o.displayQty, o.hiddenQty,
                o.sliceRemainingQty, o.expiresAtMs, o.filledQty, o.remainingQty, o.cumQuoteQty, o.lastPrice, o.lastQty,
                o.holdAmount, o.createdAtMs, o.updatedAtMs, o.eventSeq, o.currentLegId, o.pendingPrice, o.pendingQuantity,
                o.pendingHoldDelta, o.pendingHoldTarget, o.oldLegId, o.pendingDispatchSeq, o.replaceDeadlineMs};
        for (final long v : longs) {
            dst.putLong(p, v);
            p += 8;
        }
        dst.putInt(p, o.marketId); p += 4;
        dst.putInt(p, o.flags); p += 4;
        dst.putInt(p, o.holdAssetId); p += 4;
        dst.putInt(p, o.attempt); p += 4;
        dst.putShort(p, (short) o.stateFlags); p += 2;
        dst.putByte(p, o.side); p += 1;
        dst.putByte(p, o.orderType); p += 1;
        dst.putByte(p, o.timeInForce); p += 1;
        dst.putByte(p, o.status); p += 1;
        dst.putByte(p, o.rejectReason); p += 1;
        dst.putByte(p, o.pendingKind); p += 1;
        return p;
    }

    private static int readOrder(DirectBuffer src, int p, CoreOrder o) {
        final long[] longs = new long[33];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = src.getLong(p);
            p += 8;
        }
        o.orderId = longs[0]; o.userId = longs[1]; o.clientOrderIdHigh = longs[2]; o.clientOrderIdLow = longs[3];
        o.requestIdHigh = longs[4]; o.requestIdLow = longs[5]; o.price = longs[6]; o.quantity = longs[7]; o.budget = longs[8];
        o.stopPrice = longs[9]; o.trailingDelta = longs[10]; o.trailingArmPrice = longs[11]; o.displayQty = longs[12];
        o.hiddenQty = longs[13]; o.sliceRemainingQty = longs[14]; o.expiresAtMs = longs[15]; o.filledQty = longs[16];
        o.remainingQty = longs[17]; o.cumQuoteQty = longs[18]; o.lastPrice = longs[19]; o.lastQty = longs[20];
        o.holdAmount = longs[21]; o.createdAtMs = longs[22]; o.updatedAtMs = longs[23]; o.eventSeq = longs[24];
        o.currentLegId = longs[25]; o.pendingPrice = longs[26]; o.pendingQuantity = longs[27]; o.pendingHoldDelta = longs[28];
        o.pendingHoldTarget = longs[29]; o.oldLegId = longs[30]; o.pendingDispatchSeq = longs[31]; o.replaceDeadlineMs = longs[32];
        o.marketId = src.getInt(p); p += 4;
        o.flags = src.getInt(p); p += 4;
        o.holdAssetId = src.getInt(p); p += 4;
        o.attempt = src.getInt(p); p += 4;
        o.stateFlags = src.getShort(p) & 0xFFFF; p += 2;
        o.side = src.getByte(p); p += 1;
        o.orderType = src.getByte(p); p += 1;
        o.timeInForce = src.getByte(p); p += 1;
        o.status = src.getByte(p); p += 1;
        o.rejectReason = src.getByte(p); p += 1;
        o.pendingKind = src.getByte(p); p += 1;
        return p;
    }

    /** Section 3: per-user risk, ascending userId, UserRisk.SNAPSHOT_BYTES each. */
    private static int writeUserRiskSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        final int[] pos = {start};
        dst.putShort(pos[0], SECTION_USER_RISK);
        pos[0] += 2;
        final int lengthSlot = pos[0];
        pos[0] += 4;
        final int bodyStart = pos[0];
        dst.putInt(pos[0], engine.orders().userCount());
        pos[0] += 4;
        engine.orders().forEachUserByUserId(u -> {
            dst.putLong(pos[0], u.userId); pos[0] += 8;
            dst.putLong(pos[0], u.secBucket); pos[0] += 8;
            dst.putInt(pos[0], u.secCount); pos[0] += 4;
            dst.putLong(pos[0], u.minBucket); pos[0] += 8;
            dst.putInt(pos[0], u.minCount); pos[0] += 4;
            for (int m = 0; m < UserRisk.MARKET_SLOTS; m++) {
                dst.putLong(pos[0], u.position[m]); pos[0] += 8;
            }
        });
        dst.putInt(lengthSlot, pos[0] - bodyStart);
        return pos[0];
    }

    /** Section 4: per-market state for marketId 0..MARKETS (slot 0 carries the defaults), MarketRisk.SNAPSHOT_BYTES each. */
    private static int writeMarketStateSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        int p = start;
        dst.putShort(p, SECTION_MARKET_STATE); p += 2;
        final int lengthSlot = p; p += 4;
        final int bodyStart = p;
        dst.putInt(p, UserRisk.MARKET_SLOTS); p += 4;
        for (int id = 0; id < UserRisk.MARKET_SLOTS; id++) {
            final MarketRisk m = engine.orders().market(id);
            dst.putLong(p, m.configVersion); p += 8;
            dst.putLong(p, m.minQuantity); p += 8;
            dst.putLong(p, m.maxQuantity); p += 8;
            dst.putLong(p, m.minNotional); p += 8;
            dst.putLong(p, m.maxNotional); p += 8;
            dst.putLong(p, m.maxPositionPerMarket); p += 8;
            dst.putLong(p, m.lastTradePrice); p += 8;
            dst.putLong(p, m.lastTradeId); p += 8;
            dst.putLong(p, m.bestBid); p += 8;
            dst.putLong(p, m.bestAsk); p += 8;
            dst.putLong(p, m.bookVersion); p += 8;
            dst.putLong(p, m.lastTickTs); p += 8;
            dst.putInt(p, m.priceCollarBps); p += 4;
            dst.putInt(p, m.maxOrdersPerSec); p += 4;
            dst.putInt(p, m.maxOrdersPerMin); p += 4;
            dst.putInt(p, m.maxOpenOrders); p += 4;
            dst.putInt(p, m.marketBuySlippageBps); p += 4;
            dst.putByte(p, (byte) (m.breakerTripped ? 1 : 0)); p += 1;
        }
        dst.putInt(lengthSlot, p - bodyStart);
        return p;
    }

    /** Section 6: the request table, [nextRequestSeq] then rows ascending (userId, high, low). */
    private static int writeRequestsSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        final int[] pos = {start};
        dst.putShort(pos[0], SECTION_REQUEST_IDS);
        pos[0] += 2;
        final int lengthSlot = pos[0];
        pos[0] += 4;
        final int bodyStart = pos[0];
        final RequestTable t = engine.orders().requests();
        dst.putLong(pos[0], t.nextRequestSeq()); pos[0] += 8;
        dst.putInt(pos[0], t.size()); pos[0] += 4;
        t.forEachOrdered(e -> {
            dst.putLong(pos[0], e.userId); pos[0] += 8;
            dst.putLong(pos[0], e.high); pos[0] += 8;
            dst.putLong(pos[0], e.low); pos[0] += 8;
            dst.putLong(pos[0], e.requestSeq); pos[0] += 8;
            dst.putLong(pos[0], e.fingerprint); pos[0] += 8;
            dst.putLong(pos[0], e.expiresAtMs); pos[0] += 8;
            dst.putLong(pos[0], e.orderId); pos[0] += 8;
            dst.putByte(pos[0], e.kind); pos[0] += 1;
            dst.putByte(pos[0], e.reason); pos[0] += 1;
        });
        dst.putInt(lengthSlot, pos[0] - bodyStart);
        return pos[0];
    }

    private static int readOrdersSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        final int count = src.getInt(pos);
        pos += 4;
        if (count < 0 || (long) count * CoreOrder.SNAPSHOT_BYTES != limit - pos) {
            throw new IllegalStateException("balance snapshot ORDERS section declares " + count + " orders");
        }
        final OrderEngine oe = engine.orders();
        for (int i = 0; i < count; i++) {
            final CoreOrder o = oe.orders().acquire();
            pos = readOrder(src, pos, o);
            oe.restoreOrder(o);
        }
        return pos;
    }

    private static int readUserRiskSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        final int count = src.getInt(pos);
        pos += 4;
        if (count < 0 || (long) count * UserRisk.SNAPSHOT_BYTES != limit - pos) {
            throw new IllegalStateException("balance snapshot USER_RISK section declares " + count + " users");
        }
        for (int i = 0; i < count; i++) {
            final UserRisk u = engine.orders().userOrCreate(src.getLong(pos)); pos += 8;
            u.secBucket = src.getLong(pos); pos += 8;
            u.secCount = src.getInt(pos); pos += 4;
            u.minBucket = src.getLong(pos); pos += 8;
            u.minCount = src.getInt(pos); pos += 4;
            for (int m = 0; m < UserRisk.MARKET_SLOTS; m++) {
                u.position[m] = src.getLong(pos); pos += 8;
            }
        }
        return pos;
    }

    private static int readMarketStateSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        final int count = src.getInt(pos);
        pos += 4;
        if (count != UserRisk.MARKET_SLOTS || (long) count * MarketRisk.SNAPSHOT_BYTES != limit - pos) {
            throw new IllegalStateException("balance snapshot MARKET_STATE section declares " + count + " markets");
        }
        for (int id = 0; id < count; id++) {
            final MarketRisk m = engine.orders().market(id);
            m.configVersion = src.getLong(pos); pos += 8;
            m.minQuantity = src.getLong(pos); pos += 8;
            m.maxQuantity = src.getLong(pos); pos += 8;
            m.minNotional = src.getLong(pos); pos += 8;
            m.maxNotional = src.getLong(pos); pos += 8;
            m.maxPositionPerMarket = src.getLong(pos); pos += 8;
            m.lastTradePrice = src.getLong(pos); pos += 8;
            m.lastTradeId = src.getLong(pos); pos += 8;
            m.bestBid = src.getLong(pos); pos += 8;
            m.bestAsk = src.getLong(pos); pos += 8;
            m.bookVersion = src.getLong(pos); pos += 8;
            m.lastTickTs = src.getLong(pos); pos += 8;
            m.priceCollarBps = src.getInt(pos); pos += 4;
            m.maxOrdersPerSec = src.getInt(pos); pos += 4;
            m.maxOrdersPerMin = src.getInt(pos); pos += 4;
            m.maxOpenOrders = src.getInt(pos); pos += 4;
            m.marketBuySlippageBps = src.getInt(pos); pos += 4;
            m.breakerTripped = src.getByte(pos) != 0; pos += 1;
        }
        return pos;
    }

    private static int readRequestsSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        engine.orders().requests().setNextRequestSeq(src.getLong(pos)); pos += 8;
        final int count = src.getInt(pos); pos += 4;
        if (count < 0 || (long) count * RequestTable.ENTRY_BYTES != limit - pos) {
            throw new IllegalStateException("balance snapshot REQUEST_IDS section declares " + count + " rows");
        }
        for (int i = 0; i < count; i++) {
            final RequestTable.Entry e = new RequestTable.Entry();
            e.userId = src.getLong(pos); pos += 8;
            e.high = src.getLong(pos); pos += 8;
            e.low = src.getLong(pos); pos += 8;
            e.requestSeq = src.getLong(pos); pos += 8;
            e.fingerprint = src.getLong(pos); pos += 8;
            e.expiresAtMs = src.getLong(pos); pos += 8;
            e.orderId = src.getLong(pos); pos += 8;
            e.kind = src.getByte(pos); pos += 1;
            e.reason = src.getByte(pos); pos += 1;
            engine.orders().restoreRequest(e);
        }
        return pos;
    }

    /**
     * Section 5 (layout v2, RC2 S7 6.3): the order-entry gate, marketId 0 (global) .. MARKETS,
     * {marketId:int, state:u8, reason:u8, reasons:u32}. Every behaviour (v7 is a clean genesis).
     */
    private static int writeGatesSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        int pos = start;
        dst.putShort(pos, SECTION_GATES);
        pos += 2;
        final int lengthSlot = pos;
        pos += 4;
        final int bodyStart = pos;
        final OrderEntryGate gate = engine.gate();
        dst.putInt(pos, OrderEntryGate.MARKETS + 1);
        pos += 4;
        for (int m = 0; m <= OrderEntryGate.MARKETS; m++) {
            dst.putInt(pos, m);
            pos += 4;
            dst.putByte(pos, (byte) gate.state(m).code());
            pos += 1;
            dst.putByte(pos, (byte) gate.reason(m).code());
            pos += 1;
            dst.putInt(pos, gate.reasons(m));
            pos += 4;
        }
        dst.putInt(lengthSlot, pos - bodyStart);
        return pos;
    }

    /**
     * Section 10 FEED_STATE (contract 14, layout v2 with 3.8): the 92-byte head, native order, reserved bytes zero; then
     * {@code freshDigest i64 @92}, {@code digestCount u32 @100} ({@code = min(cursor, DIGEST_WINDOW)}) and the digests of
     * journalSeq {@code (cursor - digestCount, cursor]} in order, 8 bytes each.
     */
    private static int writeFeedStateSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        final FeedState f = engine.feed();
        final int count = f.digestCount();
        int p = start;
        dst.putShort(p, SECTION_FEED_STATE); p += 2;
        dst.putInt(p, FeedState.DIGEST_HEAD_BYTES + 8 * count); p += 4;
        final int body = p;
        dst.putLong(body, f.boundMeEpoch());
        dst.putLong(body + 8, f.lastAppliedJournalSeq());
        dst.putByte(body + 16, (byte) f.latchReason().code());
        dst.putByte(body + 17, (byte) 0);
        dst.putShort(body + 18, (short) 0);
        dst.putLong(body + 20, f.latchExpectedSeq());
        dst.putLong(body + 28, f.latchSeenSeq());
        dst.putLong(body + 36, f.latchSkipSeq());
        dst.putLong(body + 44, f.latchTradeId());
        dst.putLong(body + 52, f.latchMeEpochSeen());
        dst.putLong(body + 60, f.latchBridgeIdentity());
        dst.putLong(body + 68, f.latchLogPosition());
        dst.putInt(body + 76, (int) f.meConfigVersion());
        dst.putInt(body + 80, (int) f.meBehaviorVersion());
        dst.putInt(body + 84, (int) f.meLedgerCapacity());
        dst.putShort(body + 88, (short) f.meMaxWireVersion());
        dst.putShort(body + 90, (short) 0);
        dst.putLong(body + 92, f.freshDigest());
        dst.putInt(body + 100, count);
        final long c = f.lastAppliedJournalSeq();
        int q = body + FeedState.DIGEST_HEAD_BYTES;
        for (long seq = c - count + 1; seq <= c; seq++) {
            dst.putLong(q, f.digestAt(seq));
            q += 8;
        }
        return q;
    }

    private static void readFeedStateSection(DirectBuffer src, int start, int length, AssetsEngine engine) {
        if (length < FeedState.DIGEST_HEAD_BYTES) {
            throw new IllegalStateException("balance snapshot FEED_STATE section is " + length + " bytes, below its "
                    + FeedState.DIGEST_HEAD_BYTES + "-byte head");
        }
        final long cursor = src.getLong(start + 8);
        final int count = src.getInt(start + 100);
        if (cursor < 0 || count != Math.min(cursor, FeedState.DIGEST_WINDOW)
                || length != FeedState.DIGEST_HEAD_BYTES + 8L * count) {
            throw new IllegalStateException("balance snapshot FEED_STATE section: " + count + " digests for cursor " + cursor
                    + " in " + length + " bytes");
        }
        final int reasonCode = src.getByte(start + 16) & 0xFF;
        final FeedHaltReason reason = FeedHaltReason.fromCode(reasonCode);
        if (reason == null || src.getByte(start + 17) != 0 || src.getShort(start + 18) != 0 || src.getShort(start + 90) != 0) {
            throw new IllegalStateException("balance snapshot FEED_STATE section: latchReason " + reasonCode
                    + " or a reserved byte this build does not know");
        }
        engine.feed().restore(src.getLong(start), src.getLong(start + 8), reason, src.getLong(start + 20), src.getLong(start + 28),
                src.getLong(start + 36), src.getLong(start + 44), src.getLong(start + 52), src.getLong(start + 60),
                src.getLong(start + 68), src.getInt(start + 76) & 0xFFFFFFFFL, src.getInt(start + 80) & 0xFFFFFFFFL,
                src.getInt(start + 84) & 0xFFFFFFFFL, src.getShort(start + 88) & 0xFFFF);
        final long[] window = new long[count];
        for (int i = 0; i < count; i++) {
            window[i] = src.getLong(start + FeedState.DIGEST_HEAD_BYTES + 8 * i);
        }
        engine.feed().restoreDigests(src.getLong(start + 92), window);
    }

    /**
     * Section 9: every live session authority, ascending by session id:
     * {sessionId:long, registered:u8, resumable:u8, readerKind:u8, reserved:u8, channelMask:int,
     *  identity:long, sourceEpoch:long, principalLength:short, principal bytes}.
     */
    private static final int CORE_MARKET_BYTES = 4 + 16 + 4 + 4 + 8 * 4;
    private static final int CORE_ASSET_BYTES = 4 + 8 + 1;

    /**
     * Section 8 CORE_CONFIG (layout v2, RC2 S7 7.3): configVersion, behaviorVersion, genesisId, then markets and assets
     * ascending by id, fixed-size rows.
     */
    private static int writeCoreConfigSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        final com.openexchange.assets.domain.commands.CoreConfigCommand c = engine.coreConfig().recorded();
        int p = start;
        dst.putShort(p, SECTION_CORE_CONFIG); p += 2;
        final int lengthSlot = p; p += 4;
        final int bodyStart = p;
        dst.putLong(p, c.configVersion); p += 8;
        dst.putLong(p, c.behaviorVersion); p += 8;
        dst.putLong(p, c.genesisId); p += 8; // layout v2 (RC2 S7 7.3)
        dst.putInt(p, c.markets.length); p += 4;
        for (final com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef m : c.markets) {
            dst.putInt(p, m.marketId()); p += 4;
            p = putAscii(dst, p, m.symbol(), 16);
            dst.putInt(p, m.baseAssetId()); p += 4;
            dst.putInt(p, m.quoteAssetId()); p += 4;
            dst.putLong(p, m.tickSize()); p += 8;
            dst.putLong(p, m.minPrice()); p += 8;
            dst.putLong(p, m.maxPrice()); p += 8;
            dst.putLong(p, m.lotSize()); p += 8;
        }
        dst.putInt(p, c.assets.length); p += 4;
        for (final com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef a : c.assets) {
            dst.putInt(p, a.assetId()); p += 4;
            p = putAscii(dst, p, a.code(), 8);
            dst.putByte(p, (byte) a.scale()); p += 1;
        }
        dst.putInt(lengthSlot, p - bodyStart);
        return p;
    }

    private static int putAscii(MutableDirectBuffer dst, int p, String s, int width) {
        for (int i = 0; i < width; i++) {
            dst.putByte(p + i, i < s.length() ? (byte) s.charAt(i) : 0);
        }
        return p + width;
    }

    private static String getAscii(DirectBuffer src, int p, int width) {
        final StringBuilder sb = new StringBuilder(width);
        for (int i = 0; i < width; i++) {
            final byte b = src.getByte(p + i);
            if (b == 0) {
                break;
            }
            sb.append((char) b);
        }
        return sb.toString();
    }

    private static int readCoreConfigSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        final com.openexchange.assets.domain.commands.CoreConfigCommand c = new com.openexchange.assets.domain.commands.CoreConfigCommand();
        c.reset();
        c.configVersion = src.getLong(pos); pos += 8;
        c.behaviorVersion = src.getLong(pos); pos += 8;
        c.genesisId = src.getLong(pos); pos += 8;
        final int nm = src.getInt(pos); pos += 4;
        if (nm < 0 || (long) nm * CORE_MARKET_BYTES > limit - pos) {
            throw new IllegalStateException("balance snapshot CORE_CONFIG declares " + nm + " markets");
        }
        c.markets = new com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef[nm];
        for (int i = 0; i < nm; i++) {
            final int id = src.getInt(pos); pos += 4;
            final String symbol = getAscii(src, pos, 16); pos += 16;
            final int base = src.getInt(pos); pos += 4;
            final int quote = src.getInt(pos); pos += 4;
            final long tick = src.getLong(pos); pos += 8;
            final long min = src.getLong(pos); pos += 8;
            final long max = src.getLong(pos); pos += 8;
            final long lot = src.getLong(pos); pos += 8;
            c.markets[i] = new com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef(id, symbol, base, quote, tick, min, max, lot);
        }
        final int na = src.getInt(pos); pos += 4;
        if (na < 0 || (long) na * CORE_ASSET_BYTES != limit - pos) {
            throw new IllegalStateException("balance snapshot CORE_CONFIG declares " + na + " assets");
        }
        c.assets = new com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef[na];
        for (int i = 0; i < na; i++) {
            final int id = src.getInt(pos); pos += 4;
            final String code = getAscii(src, pos, 8); pos += 8;
            final int scale = src.getByte(pos) & 0xFF; pos += 1;
            c.assets[i] = new com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef(id, code, scale);
        }
        engine.coreConfig().restore(c);
        engine.crossCheckConfigOrFail("restore"); // section 4: a restored config this node cannot run stops it
        return pos;
    }

    private static int writeSessionsSection(AssetsEngine engine, MutableDirectBuffer dst, int start) {
        final int[] pos = {start};
        dst.putShort(pos[0], SECTION_SESSIONS);
        pos[0] += 2;
        final int lengthSlot = pos[0];
        pos[0] += 4;
        final int bodyStart = pos[0];
        dst.putInt(pos[0], engine.sessions().size());
        pos[0] += 4;
        engine.sessions().forEachBySessionId(a -> {
            dst.putLong(pos[0], a.sessionId());
            pos[0] += 8;
            dst.putByte(pos[0], (byte) (a.isRegistered() ? 1 : 0));
            pos[0] += 1;
            dst.putByte(pos[0], (byte) (a.isResumable() ? 1 : 0));
            pos[0] += 1;
            dst.putByte(pos[0], (byte) a.readerKind());
            pos[0] += 1;
            dst.putByte(pos[0], (byte) 0);
            pos[0] += 1;
            dst.putInt(pos[0], a.channelMask());
            pos[0] += 4;
            dst.putLong(pos[0], a.identity());
            pos[0] += 8;
            dst.putLong(pos[0], a.sourceEpoch());
            pos[0] += 8;
            final byte[] principal = a.principal().classCode() == 0 // UNSET
                    ? new byte[0] : a.principal().encode();
            dst.putShort(pos[0], (short) principal.length);
            pos[0] += 2;
            dst.putBytes(pos[0], principal);
            pos[0] += principal.length;
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
        boolean feedSection = false;
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
                case SECTION_ORDERS: {
                    final int endPos = readOrdersSection(src, pos, sectionEnd, engine);
                    if (endPos != sectionEnd) {
                        throw new IllegalStateException("balance snapshot ORDERS section declares " + sectionLength
                                + " bytes but decodes " + (endPos - pos));
                    }
                    break;
                }
                case SECTION_USER_RISK: {
                    final int endPos = readUserRiskSection(src, pos, sectionEnd, engine);
                    if (endPos != sectionEnd) {
                        throw new IllegalStateException("balance snapshot USER_RISK section length mismatch");
                    }
                    break;
                }
                case SECTION_MARKET_STATE: {
                    final int endPos = readMarketStateSection(src, pos, sectionEnd, engine);
                    if (endPos != sectionEnd) {
                        throw new IllegalStateException("balance snapshot MARKET_STATE section length mismatch");
                    }
                    break;
                }
                case SECTION_REQUEST_IDS: {
                    final int endPos = readRequestsSection(src, pos, sectionEnd, engine);
                    if (endPos != sectionEnd) {
                        throw new IllegalStateException("balance snapshot REQUEST_IDS section length mismatch");
                    }
                    break;
                }
                case SECTION_GATES: {
                    final int endPos = readGatesSection(src, pos, sectionEnd, engine);
                    if (endPos != sectionEnd) {
                        throw new IllegalStateException("balance snapshot GATES section declares " + sectionLength
                                + " bytes but decodes " + (endPos - pos));
                    }
                    break;
                }
                case SECTION_CORE_CONFIG: {
                    final int endPos = readCoreConfigSection(src, pos, sectionEnd, engine);
                    if (endPos != sectionEnd) {
                        throw new IllegalStateException("balance snapshot CORE_CONFIG section length mismatch");
                    }
                    break;
                }
                case SECTION_SESSIONS: {
                    final int endPos = readSessionsSection(src, pos, sectionEnd, engine);
                    if (endPos != sectionEnd) {
                        throw new IllegalStateException("balance snapshot SESSIONS section declares " + sectionLength
                                + " bytes but decodes " + (endPos - pos));
                    }
                    break;
                }
                case SECTION_FEED_STATE:
                    readFeedStateSection(src, pos, sectionLength, engine);
                    feedSection = true;
                    break;
                default:
                    throw new IllegalStateException("balance snapshot section id " + id
                            + " is not known to this build (layout page sections 2..8 land with their slices)");
            }
            pos = sectionEnd;
            previousId = id;
        }
        // RC2 S7 3.7 (review 2 FIX-B): section 10 is mandatory under a recorded behaviour of 3 and refused below it
        final boolean v3 = engine.behaviorV3();
        if (v3 && !feedSection) {
            throw new IllegalStateException("balance snapshot of a behaviour-" + engine.coreConfig().behaviorVersion()
                    + " core has no FEED_STATE section (10)");
        }
        if (!v3 && feedSection) {
            throw new IllegalStateException("balance snapshot below behaviour 3 carries a FEED_STATE section (10)");
        }
        return new Decoded(lastAppliedTradeId, consumePosition, journalSeq, lastLogTime, accounts, holds,
                sectionCount, pos - offset);
    }

    private static int readGatesSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        final int count = src.getInt(pos);
        pos += 4;
        if (count < 0 || limit - pos < count * 10) {
            throw new IllegalStateException("balance snapshot GATES section declares " + count + " gates");
        }
        engine.gate().reset();
        for (int i = 0; i < count; i++) {
            final int marketId = src.getInt(pos);
            pos += 4;
            final GateState state = GateState.fromCode(src.getByte(pos) & 0xFF);
            pos += 1;
            final GateReason reason = GateReason.fromCode(src.getByte(pos) & 0xFF);
            pos += 1;
            final int reasons = src.getInt(pos);
            pos += 4;
            if (!OrderEntryGate.validMarket(marketId) || state == null || reason == null) {
                throw new IllegalStateException("balance snapshot GATES section: market " + marketId
                        + " state/reason bytes this build does not know");
            }
            engine.gate().restore(marketId, state, reason, reasons); // validated after section 8 (verifyRestoredInvariants)
        }
        return pos;
    }

    private static int readSessionsSection(DirectBuffer src, int start, int limit, AssetsEngine engine) {
        int pos = start;
        final int count = src.getInt(pos);
        pos += 4;
        if (count < 0) {
            throw new IllegalStateException("balance snapshot SESSIONS section declares " + count + " sessions");
        }
        for (int i = 0; i < count; i++) {
            if (limit - pos < 8 + 4 + 4 + 8 + 8 + 2) {
                throw new IllegalStateException("balance snapshot SESSIONS section truncated at session " + i);
            }
            final long sessionId = src.getLong(pos);
            pos += 8;
            final boolean registered = src.getByte(pos) != 0;
            pos += 1;
            final boolean resumable = src.getByte(pos) != 0;
            pos += 1;
            final int readerKind = src.getByte(pos) & 0xFF;
            pos += 1;
            pos += 1; // reserved
            final int channelMask = src.getInt(pos);
            pos += 4;
            final long identity = src.getLong(pos);
            pos += 8;
            final long sourceEpoch = src.getLong(pos);
            pos += 8;
            final int principalLength = src.getShort(pos) & 0xFFFF;
            pos += 2;
            if (limit - pos < principalLength) {
                throw new IllegalStateException("balance snapshot SESSIONS section: principal runs past the section");
            }
            final byte[] principal = new byte[principalLength];
            src.getBytes(pos, principal);
            pos += principalLength;
            engine.sessions().restore(sessionId, principal, registered, channelMask, resumable, readerKind, identity, sourceEpoch);
        }
        return pos;
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

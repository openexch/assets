// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.proto;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettlementService;
import com.openexchange.assets.domain.commands.ProtoOrderCancelCommand;
import com.openexchange.assets.domain.commands.ProtoOrderCreateCommand;
import com.openexchange.assets.domain.commands.ProtoOrderExpireCommand;
import com.openexchange.assets.domain.commands.ProtoOrderFillCommand;
import com.openexchange.assets.domain.commands.ProtoRiskConfigCommand;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayDeque;
import java.util.Arrays;

/**
 * RC0 spike: the order lifecycle as the OMS + ledger core would run it, inside the Assets Engine's
 * deterministic apply. Measurement scaffold, not the product design: it exists so the per-order
 * service-thread cost, the snapshot size at 100k / 1M open orders and the egress bytes per lifecycle
 * can be measured on the real cluster before RC1 commits to the shape (plan RC0 step b).
 *
 * <p>What it does carry faithfully, because the cost lives there: the seven admission checks in the
 * OMS's order (rate limit first and consuming budget even on reject, circuit breaker, size and
 * notional, price collar, open-order slot, duplicate clientOrderId, position), the hold placed in the
 * same apply as the admission (the balance check IS the hold: no projection, no round trip), a
 * realistic ~30-field order record with three indexes, settlement of a fill through the production
 * {@link SettlementService} with the residual hold released at FILLED, GTD as a cluster timer, and a
 * snapshot block ordered by id so the bytes are a function of state.</p>
 *
 * <p>What it deliberately leaves out: the ME link (fills arrive as {@code ProtoOrderFill} from the
 * load generator), synthetic order types (stop/trailing/iceberg are accepted as plain records),
 * amend/replace, IOC/FOK semantics, session authorisation.</p>
 */
public final class ProtoOrderEngine {

    // Wire codes (money-schema ProtoOrderStatus / ProtoRejectReason / ProtoSide / ProtoTimeInForce).
    public static final int ST_NEW = 0, ST_PARTIALLY_FILLED = 1, ST_FILLED = 2, ST_CANCELLED = 3, ST_REJECTED = 4, ST_EXPIRED = 5;
    public static final int RR_NONE = 0, RR_RATE_LIMIT = 1, RR_CIRCUIT_BREAKER = 2, RR_SIZE = 3, RR_NOTIONAL = 4,
            RR_PRICE_COLLAR = 5, RR_OPEN_ORDERS = 6, RR_INSUFFICIENT_FUNDS = 7, RR_POSITION = 8,
            RR_DUPLICATE_CLIENT_ORDER_ID = 9, RR_UNKNOWN_ORDER = 10, RR_NOT_OWNER = 11, RR_INVALID = 12, RR_EXPIRED = 13,
            RR_NOT_OPEN = 14;
    public static final int SIDE_BUY = 0, SIDE_SELL = 1;
    public static final int TIF_GTD = 3;
    /** marketId 1..5 today (see {@link Market}); slot 0 unused. */
    public static final int MARKET_SLOTS = ProtoUserState.MARKET_SLOTS;
    /** FixedPoint scale: a product whose high 64 bits reach it cannot be represented after scaling. */
    private static final long SCALE = 100_000_000L;

    /** How the engine asks the transport to arm a GTD deadline (a cluster timer keyed by orderId). */
    public interface TimerPort {
        TimerPort NONE = (orderId, deadlineMs) -> { };

        void schedule(long orderId, long deadlineMs);
    }

    private final AssetsEngine ledger;
    private final SettlementService settlement = new SettlementService();
    private final Long2ObjectHashMap<ProtoOrder> orders = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<ProtoUserState> users = new Long2ObjectHashMap<>();
    private final ProtoMarketState[] markets = new ProtoMarketState[MARKET_SLOTS];
    private final ArrayDeque<ProtoOrder> pool = new ArrayDeque<>();
    private TimerPort timerPort = TimerPort.NONE;

    // Replicated scalars (snapshotted).
    private long nextOrderId = 1L;
    private long eventSeq;
    private long lastFillTradeId;

    // Diagnostics (node-local counters; not snapshotted, not replicated truth).
    private long acceptedCount, rejectedCount, cancelledCount, expiredCount, fillCount, fillsIgnored, settleFaultCount;

    private long[] sortScratch = new long[1024];

    public ProtoOrderEngine(final AssetsEngine ledger) {
        this.ledger = ledger;
        for (int i = 0; i < MARKET_SLOTS; i++) {
            markets[i] = new ProtoMarketState();
        }
    }

    public void setTimerPort(final TimerPort port) {
        this.timerPort = port == null ? TimerPort.NONE : port;
    }

    // ---- apply ----

    public void applyCreate(final ProtoOrderCreateCommand c, final long ts) {
        final AssetsEventSink sink = ledger.eventSink();
        if (c.marketId <= 0 || c.marketId >= MARKET_SLOTS || c.quantity <= 0 || c.price < 0
                || (c.side != SIDE_BUY && c.side != SIDE_SELL)) {
            reject(sink, c, RR_INVALID, ts);
            return;
        }
        final ProtoMarketState m = markets[c.marketId];
        final ProtoUserState u = userState(c.userId);

        // 1. Rate limit: fixed buckets on cluster time; budget is consumed before the other checks,
        //    so a rejected order still counts (today's OMS semantics).
        final long secBucket = ts / 1_000L;
        if (u.secBucket != secBucket) {
            u.secBucket = secBucket;
            u.secCount = 0;
        }
        u.secCount++;
        final long minBucket = ts / 60_000L;
        if (u.minBucket != minBucket) {
            u.minBucket = minBucket;
            u.minCount = 0;
        }
        u.minCount++;
        if (u.secCount > m.maxOrdersPerSec || u.minCount > m.maxOrdersPerMin) {
            reject(sink, c, RR_RATE_LIMIT, ts);
            return;
        }
        // 2. Circuit breaker.
        if (m.circuitBreaker) {
            reject(sink, c, RR_CIRCUIT_BREAKER, ts);
            return;
        }
        // 3. Size and notional (notional at the limit price, else the last trade, else skipped).
        if (c.quantity < m.minQty || c.quantity > m.maxQty) {
            reject(sink, c, RR_SIZE, ts);
            return;
        }
        final long refPrice = c.price > 0 ? c.price : m.lastTradePrice;
        long notional = 0L;
        if (refPrice > 0) {
            if (Math.multiplyHigh(refPrice, c.quantity) >= SCALE) {
                reject(sink, c, RR_INVALID, ts); // FixedPoint.multiply would overflow: never throw in apply
                return;
            }
            notional = FixedPoint.multiply(refPrice, c.quantity);
            if (notional > m.maxNotional) {
                reject(sink, c, RR_NOTIONAL, ts);
                return;
            }
        }
        // 4. Price collar (priced orders, once there is a last trade).
        if (c.price > 0 && m.lastTradePrice > 0) {
            final long maxDev = m.lastTradePrice / 100 * m.collarPct;
            if (Math.abs(c.price - m.lastTradePrice) > maxDev) {
                reject(sink, c, RR_PRICE_COLLAR, ts);
                return;
            }
        }
        // 5. Open-order slot.
        if (u.openOrderCount >= m.maxOpenOrders) {
            reject(sink, c, RR_OPEN_ORDERS, ts);
            return;
        }
        // 6. Duplicate clientOrderId (0 = none supplied).
        if (c.clientOrderIdLow != 0 && u.clientOrderIds.containsKey(c.clientOrderIdLow)) {
            reject(sink, c, RR_DUPLICATE_CLIENT_ORDER_ID, ts);
            return;
        }
        // 7. Position.
        if (m.maxPositionPerMarket != Long.MAX_VALUE) {
            final long projected = u.position[c.marketId] + (c.side == SIDE_BUY ? c.quantity : -c.quantity);
            if (Math.abs(projected) > m.maxPositionPerMarket) {
                reject(sink, c, RR_POSITION, ts);
                return;
            }
        }
        // The balance check IS the hold, in the same apply: no projection read, no round trip.
        final Market mk = Market.fromId(c.marketId);
        final int holdAsset;
        final long holdAmount;
        if (c.side == SIDE_BUY) {
            if (c.price <= 0) {
                reject(sink, c, RR_INVALID, ts); // a price-less buy needs a budget price from the gateway
                return;
            }
            holdAsset = mk.quoteAsset().id();
            holdAmount = notional;
        } else {
            holdAsset = mk.baseAsset().id();
            holdAmount = c.quantity;
        }
        final long orderId = nextOrderId++;
        final Account a = ledger.getOrCreateAccount(c.userId);
        final RejectReason r = a.hold(orderId, holdAsset, holdAmount, false);
        if (!r.accepted()) {
            reject(sink, c, r == RejectReason.INSUFFICIENT_FUNDS ? RR_INSUFFICIENT_FUNDS : RR_INVALID, ts);
            return;
        }

        final ProtoOrder o = acquire();
        o.orderId = orderId;
        o.userId = c.userId;
        o.clientOrderIdHigh = c.clientOrderIdHigh;
        o.clientOrderIdLow = c.clientOrderIdLow;
        o.requestIdHigh = c.requestIdHigh;
        o.requestIdLow = c.requestIdLow;
        o.marketId = c.marketId;
        o.side = (byte) c.side;
        o.orderType = (byte) c.orderType;
        o.timeInForce = (byte) c.timeInForce;
        o.flags = c.flags;
        o.price = c.price;
        o.quantity = c.quantity;
        o.stopPrice = c.stopPrice;
        o.displayQty = c.displayQty;
        o.expiresAtMs = c.expiresAtMs;
        o.status = ST_NEW;
        o.remainingQty = c.quantity;
        o.holdAssetId = holdAsset;
        o.holdAmount = holdAmount;
        o.createdAtMs = ts;
        o.updatedAtMs = ts;
        o.eventSeq = ++eventSeq;
        o.sliceRemainingQty = c.displayQty > 0 ? Math.min(c.displayQty, c.quantity) : 0L;

        orders.put(orderId, o);
        u.openOrders.add(orderId);
        if (c.clientOrderIdLow != 0) {
            u.clientOrderIds.put(c.clientOrderIdLow, orderId);
        }
        u.openOrderCount++;
        u.openNotional += notional;
        m.openOrders++;
        if (c.timeInForce == TIF_GTD && c.expiresAtMs > 0) {
            timerPort.schedule(orderId, c.expiresAtMs);
        }
        acceptedCount++;

        sink.onBalanceUpdate(c.userId, holdAsset, a.available(holdAsset), a.locked(holdAsset));
        sink.onProtoOrderEvent(c.correlationId, orderId, c.userId, c.marketId, ST_NEW, RR_NONE,
                0L, c.quantity, 0L, 0L, holdAmount, ts, o.eventSeq);
    }

    public void applyCancel(final ProtoOrderCancelCommand c, final long ts) {
        final AssetsEventSink sink = ledger.eventSink();
        final ProtoOrder o = orders.get(c.orderId);
        if (o == null) {
            sink.onProtoOrderEvent(c.correlationId, c.orderId, c.userId, 0, ST_REJECTED, RR_UNKNOWN_ORDER,
                    0L, 0L, 0L, 0L, 0L, ts, ++eventSeq);
            return;
        }
        if (o.userId != c.userId) {
            sink.onProtoOrderEvent(c.correlationId, c.orderId, c.userId, o.marketId, ST_REJECTED, RR_NOT_OWNER,
                    0L, 0L, 0L, 0L, 0L, ts, ++eventSeq);
            return;
        }
        cancelledCount++;
        close(sink, o, ST_CANCELLED, RR_NONE, c.correlationId, ts);
    }

    /** A GTD deadline fired (cluster TimerEvent). Idempotent: an order already closed is a no-op. */
    public void applyExpire(final ProtoOrderExpireCommand c, final long ts) {
        final ProtoOrder o = orders.get(c.orderId);
        if (o == null) {
            return;
        }
        expiredCount++;
        close(ledger.eventSink(), o, ST_EXPIRED, RR_EXPIRED, 0L, ts);
    }

    public void applyFill(final ProtoOrderFillCommand c, final long ts) {
        if (c.tradeId <= lastFillTradeId) {
            return; // already applied: idempotent no-op
        }
        final AssetsEventSink sink = ledger.eventSink();
        final ProtoOrder buy = orders.get(c.buyOrderId);
        final ProtoOrder sell = orders.get(c.sellOrderId);
        if (buy == null || sell == null || c.quantity <= 0 || c.price <= 0
                || buy.marketId != c.marketId || sell.marketId != c.marketId
                || buy.side != SIDE_BUY || sell.side != SIDE_SELL
                || Math.multiplyHigh(c.price, c.quantity) >= SCALE) {
            fillsIgnored++;
            lastFillTradeId = c.tradeId;
            return;
        }
        final long qty = Math.min(c.quantity, Math.min(buy.remainingQty, sell.remainingQty));
        if (qty <= 0) {
            fillsIgnored++;
            lastFillTradeId = c.tradeId;
            return;
        }
        final Market mk = Market.fromId(c.marketId);
        final int base = mk.baseAsset().id();
        final int quote = mk.quoteAsset().id();
        final long quoteAmt = FixedPoint.multiply(c.price, qty);
        final Account buyer = ledger.getOrCreateAccount(buy.userId);
        final Account seller = ledger.getOrCreateAccount(sell.userId);
        settlement.settle(buyer, seller, buy.orderId, sell.orderId, base, quote, qty, quoteAmt);
        lastFillTradeId = c.tradeId;
        fillCount++;

        if (settlement.buyerLeg().debitFaulted()) {
            settleFaultCount++;
            sink.onSettleFault(c.tradeId, buy.orderId, buy.userId, quote,
                    settlement.buyerLeg().drawnFromAvailable, settlement.buyerLeg().uncovered);
        }
        if (settlement.sellerLeg().debitFaulted()) {
            settleFaultCount++;
            sink.onSettleFault(c.tradeId, sell.orderId, sell.userId, base,
                    settlement.sellerLeg().drawnFromAvailable, settlement.sellerLeg().uncovered);
        }
        if (settlement.buyerLeg().uncredited != 0) {
            settleFaultCount++;
            sink.onSettleFault(c.tradeId, sell.orderId, sell.userId, quote, 0L, settlement.buyerLeg().uncredited);
        }
        if (settlement.sellerLeg().uncredited != 0) {
            settleFaultCount++;
            sink.onSettleFault(c.tradeId, buy.orderId, buy.userId, base, 0L, settlement.sellerLeg().uncredited);
        }

        final ProtoMarketState m = markets[c.marketId];
        m.lastTradePrice = c.price;
        userState(buy.userId).position[c.marketId] += qty;
        userState(sell.userId).position[c.marketId] -= qty;

        sink.onBalanceUpdate(buy.userId, quote, buyer.available(quote), buyer.locked(quote));
        sink.onBalanceUpdate(buy.userId, base, buyer.available(base), buyer.locked(base));
        sink.onBalanceUpdate(sell.userId, base, seller.available(base), seller.locked(base));
        sink.onBalanceUpdate(sell.userId, quote, seller.available(quote), seller.locked(quote));
        sink.onSettlementApplied(c.tradeId, buy.userId, sell.userId);

        applyFillTo(sink, buy, c.price, qty, quoteAmt, ts);
        applyFillTo(sink, sell, c.price, qty, quoteAmt, ts);
    }

    public void applyRiskConfig(final ProtoRiskConfigCommand c) {
        if (c.marketId <= 0 || c.marketId >= MARKET_SLOTS) {
            return;
        }
        final ProtoMarketState m = markets[c.marketId];
        m.maxOrdersPerSec = c.maxOrdersPerSec;
        m.maxOrdersPerMin = c.maxOrdersPerMin;
        m.minQty = c.minQty;
        m.maxQty = c.maxQty;
        m.maxNotional = c.maxNotional;
        m.collarPct = c.collarPct;
        m.maxOpenOrders = c.maxOpenOrders;
        m.maxPositionPerMarket = c.maxPositionPerMarket;
        m.circuitBreaker = c.circuitBreaker;
    }

    // ---- internals ----

    private void applyFillTo(final AssetsEventSink sink, final ProtoOrder o, final long price, final long qty,
                             final long quoteAmt, final long ts) {
        o.filledQty += qty;
        o.remainingQty -= qty;
        o.cumQuoteQty += quoteAmt;
        o.lastPrice = price;
        o.lastQty = qty;
        o.updatedAtMs = ts;
        if (o.price > 0) {
            userState(o.userId).openNotional -= FixedPoint.multiply(o.price, qty);
        }
        if (o.remainingQty == 0) {
            // FILLED: the hold's residual (a buy filled below its limit) goes back to available here,
            // the K11 fix for the OMS's "FILLED never releases" leak.
            close(sink, o, ST_FILLED, RR_NONE, 0L, ts);
        } else {
            o.status = ST_PARTIALLY_FILLED;
            o.eventSeq = ++eventSeq;
            sink.onProtoOrderEvent(0L, o.orderId, o.userId, o.marketId, ST_PARTIALLY_FILLED, RR_NONE,
                    o.filledQty, o.remainingQty, price, qty, o.holdAmount, ts, o.eventSeq);
        }
    }

    private void close(final AssetsEventSink sink, final ProtoOrder o, final int status, final int reason,
                       final long correlationId, final long ts) {
        final Account a = ledger.account(o.userId);
        if (a != null) {
            final long released = a.release(o.orderId, -1L);
            if (released > 0) {
                sink.onBalanceUpdate(o.userId, o.holdAssetId, a.available(o.holdAssetId), a.locked(o.holdAssetId));
            }
        }
        final ProtoUserState u = userState(o.userId);
        orders.remove(o.orderId);
        u.openOrders.remove(o.orderId);
        if (o.clientOrderIdLow != 0 && u.clientOrderIds.get(o.clientOrderIdLow) == o.orderId) {
            u.clientOrderIds.remove(o.clientOrderIdLow);
        }
        u.openOrderCount--;
        u.openNotional -= o.notional();
        markets[o.marketId].openOrders--;
        o.status = (byte) status;
        o.rejectReason = (byte) reason;
        o.updatedAtMs = ts;
        o.eventSeq = ++eventSeq;
        sink.onProtoOrderEvent(correlationId, o.orderId, o.userId, o.marketId, status, reason,
                o.filledQty, o.remainingQty, o.lastPrice, o.lastQty, o.holdAmount, ts, o.eventSeq);
        recycle(o);
    }

    private void reject(final AssetsEventSink sink, final ProtoOrderCreateCommand c, final int reason, final long ts) {
        rejectedCount++;
        sink.onProtoOrderEvent(c.correlationId, 0L, c.userId, c.marketId, ST_REJECTED, reason,
                0L, c.quantity, 0L, 0L, 0L, ts, ++eventSeq);
    }

    private ProtoUserState userState(final long userId) {
        ProtoUserState u = users.get(userId);
        if (u == null) {
            u = new ProtoUserState(userId);
            users.put(userId, u);
        }
        return u;
    }

    private ProtoOrder acquire() {
        final ProtoOrder o = pool.pollFirst();
        return o != null ? o : new ProtoOrder();
    }

    private void recycle(final ProtoOrder o) {
        o.clear();
        if (pool.size() < 65_536) {
            pool.addFirst(o);
        }
    }

    // ---- snapshot (v4 order block) ----

    /** Bytes the order block will take: used by the sizing test and by callers pre-sizing a buffer. */
    public int snapshotBytes() {
        return 3 * 8 + 4 + MARKET_SLOTS * ProtoMarketState.SNAPSHOT_BYTES
                + 4 + users.size() * ProtoUserState.SNAPSHOT_BYTES
                + 4 + orders.size() * ProtoOrder.SNAPSHOT_BYTES;
    }

    /** Append the order block at {@code pos}; users by ascending userId, orders by ascending orderId. */
    public int serialize(final MutableDirectBuffer dst, int pos) {
        dst.putLong(pos, nextOrderId); pos += 8;
        dst.putLong(pos, eventSeq); pos += 8;
        dst.putLong(pos, lastFillTradeId); pos += 8;

        dst.putInt(pos, MARKET_SLOTS); pos += 4;
        for (int i = 0; i < MARKET_SLOTS; i++) {
            final ProtoMarketState m = markets[i];
            dst.putLong(pos, m.lastTradePrice); pos += 8;
            dst.putLong(pos, m.minQty); pos += 8;
            dst.putLong(pos, m.maxQty); pos += 8;
            dst.putLong(pos, m.maxNotional); pos += 8;
            dst.putLong(pos, m.maxPositionPerMarket); pos += 8;
            dst.putInt(pos, m.openOrders); pos += 4;
            dst.putInt(pos, m.maxOrdersPerSec); pos += 4;
            dst.putInt(pos, m.maxOrdersPerMin); pos += 4;
            dst.putInt(pos, m.collarPct); pos += 4;
            dst.putInt(pos, m.maxOpenOrders); pos += 4;
            dst.putByte(pos, (byte) (m.circuitBreaker ? 1 : 0)); pos += 1;
        }

        int n = 0;
        ensureScratch(users.size());
        for (final ProtoUserState u : users.values()) {
            sortScratch[n++] = u.userId;
        }
        Arrays.sort(sortScratch, 0, n);
        dst.putInt(pos, n); pos += 4;
        for (int i = 0; i < n; i++) {
            final ProtoUserState u = users.get(sortScratch[i]);
            dst.putLong(pos, u.userId); pos += 8;
            dst.putLong(pos, u.secBucket); pos += 8;
            dst.putLong(pos, u.minBucket); pos += 8;
            dst.putLong(pos, u.openNotional); pos += 8;
            for (int k = 0; k < MARKET_SLOTS; k++) {
                dst.putLong(pos, u.position[k]); pos += 8;
            }
            dst.putInt(pos, u.openOrderCount); pos += 4;
            dst.putInt(pos, u.secCount); pos += 4;
            dst.putInt(pos, u.minCount); pos += 4;
        }

        n = 0;
        ensureScratch(orders.size());
        for (final ProtoOrder o : orders.values()) {
            sortScratch[n++] = o.orderId;
        }
        Arrays.sort(sortScratch, 0, n);
        dst.putInt(pos, n); pos += 4;
        for (int i = 0; i < n; i++) {
            final ProtoOrder o = orders.get(sortScratch[i]);
            dst.putLong(pos, o.orderId); pos += 8;
            dst.putLong(pos, o.userId); pos += 8;
            dst.putLong(pos, o.clientOrderIdHigh); pos += 8;
            dst.putLong(pos, o.clientOrderIdLow); pos += 8;
            dst.putLong(pos, o.requestIdHigh); pos += 8;
            dst.putLong(pos, o.requestIdLow); pos += 8;
            dst.putLong(pos, o.price); pos += 8;
            dst.putLong(pos, o.quantity); pos += 8;
            dst.putLong(pos, o.stopPrice); pos += 8;
            dst.putLong(pos, o.displayQty); pos += 8;
            dst.putLong(pos, o.expiresAtMs); pos += 8;
            dst.putLong(pos, o.filledQty); pos += 8;
            dst.putLong(pos, o.remainingQty); pos += 8;
            dst.putLong(pos, o.cumQuoteQty); pos += 8;
            dst.putLong(pos, o.lastPrice); pos += 8;
            dst.putLong(pos, o.lastQty); pos += 8;
            dst.putLong(pos, o.holdAmount); pos += 8;
            dst.putLong(pos, o.createdAtMs); pos += 8;
            dst.putLong(pos, o.updatedAtMs); pos += 8;
            dst.putLong(pos, o.eventSeq); pos += 8;
            dst.putLong(pos, o.parentOrderId); pos += 8;
            dst.putLong(pos, o.sliceRemainingQty); pos += 8;
            dst.putInt(pos, o.marketId); pos += 4;
            dst.putInt(pos, o.flags); pos += 4;
            dst.putInt(pos, o.holdAssetId); pos += 4;
            dst.putByte(pos, o.side); pos += 1;
            dst.putByte(pos, o.orderType); pos += 1;
            dst.putByte(pos, o.timeInForce); pos += 1;
            dst.putByte(pos, o.status); pos += 1;
            dst.putByte(pos, o.rejectReason); pos += 1;
            dst.putByte(pos, o.cancelRequested); pos += 1;
        }
        return pos;
    }

    /** Restore the order block from {@code pos} into this (empty) engine; rebuilds the indexes. */
    public int deserialize(final DirectBuffer src, int pos) {
        nextOrderId = src.getLong(pos); pos += 8;
        eventSeq = src.getLong(pos); pos += 8;
        lastFillTradeId = src.getLong(pos); pos += 8;

        final int marketCount = src.getInt(pos); pos += 4;
        if (marketCount != MARKET_SLOTS) {
            throw new IllegalStateException("proto snapshot market slots " + marketCount + " != " + MARKET_SLOTS);
        }
        for (int i = 0; i < MARKET_SLOTS; i++) {
            final ProtoMarketState m = markets[i];
            m.lastTradePrice = src.getLong(pos); pos += 8;
            m.minQty = src.getLong(pos); pos += 8;
            m.maxQty = src.getLong(pos); pos += 8;
            m.maxNotional = src.getLong(pos); pos += 8;
            m.maxPositionPerMarket = src.getLong(pos); pos += 8;
            m.openOrders = src.getInt(pos); pos += 4;
            m.maxOrdersPerSec = src.getInt(pos); pos += 4;
            m.maxOrdersPerMin = src.getInt(pos); pos += 4;
            m.collarPct = src.getInt(pos); pos += 4;
            m.maxOpenOrders = src.getInt(pos); pos += 4;
            m.circuitBreaker = src.getByte(pos) != 0; pos += 1;
        }

        final int userCount = src.getInt(pos); pos += 4;
        for (int i = 0; i < userCount; i++) {
            final long userId = src.getLong(pos); pos += 8;
            final ProtoUserState u = userState(userId);
            u.secBucket = src.getLong(pos); pos += 8;
            u.minBucket = src.getLong(pos); pos += 8;
            u.openNotional = src.getLong(pos); pos += 8;
            for (int k = 0; k < MARKET_SLOTS; k++) {
                u.position[k] = src.getLong(pos); pos += 8;
            }
            u.openOrderCount = src.getInt(pos); pos += 4;
            u.secCount = src.getInt(pos); pos += 4;
            u.minCount = src.getInt(pos); pos += 4;
        }

        final int orderCount = src.getInt(pos); pos += 4;
        for (int i = 0; i < orderCount; i++) {
            final ProtoOrder o = acquire();
            o.orderId = src.getLong(pos); pos += 8;
            o.userId = src.getLong(pos); pos += 8;
            o.clientOrderIdHigh = src.getLong(pos); pos += 8;
            o.clientOrderIdLow = src.getLong(pos); pos += 8;
            o.requestIdHigh = src.getLong(pos); pos += 8;
            o.requestIdLow = src.getLong(pos); pos += 8;
            o.price = src.getLong(pos); pos += 8;
            o.quantity = src.getLong(pos); pos += 8;
            o.stopPrice = src.getLong(pos); pos += 8;
            o.displayQty = src.getLong(pos); pos += 8;
            o.expiresAtMs = src.getLong(pos); pos += 8;
            o.filledQty = src.getLong(pos); pos += 8;
            o.remainingQty = src.getLong(pos); pos += 8;
            o.cumQuoteQty = src.getLong(pos); pos += 8;
            o.lastPrice = src.getLong(pos); pos += 8;
            o.lastQty = src.getLong(pos); pos += 8;
            o.holdAmount = src.getLong(pos); pos += 8;
            o.createdAtMs = src.getLong(pos); pos += 8;
            o.updatedAtMs = src.getLong(pos); pos += 8;
            o.eventSeq = src.getLong(pos); pos += 8;
            o.parentOrderId = src.getLong(pos); pos += 8;
            o.sliceRemainingQty = src.getLong(pos); pos += 8;
            o.marketId = src.getInt(pos); pos += 4;
            o.flags = src.getInt(pos); pos += 4;
            o.holdAssetId = src.getInt(pos); pos += 4;
            o.side = src.getByte(pos); pos += 1;
            o.orderType = src.getByte(pos); pos += 1;
            o.timeInForce = src.getByte(pos); pos += 1;
            o.status = src.getByte(pos); pos += 1;
            o.rejectReason = src.getByte(pos); pos += 1;
            o.cancelRequested = src.getByte(pos); pos += 1;
            orders.put(o.orderId, o);
            final ProtoUserState u = userState(o.userId);
            u.openOrders.add(o.orderId);
            if (o.clientOrderIdLow != 0) {
                u.clientOrderIds.put(o.clientOrderIdLow, o.orderId);
            }
        }
        return pos;
    }

    private void ensureScratch(final int n) {
        if (sortScratch.length < n) {
            sortScratch = new long[Math.max(n, sortScratch.length * 2)];
        }
    }

    // ---- reads (diagnostics, tests) ----

    public int openOrderCount() { return orders.size(); }
    public int userCount() { return users.size(); }
    public ProtoOrder order(final long orderId) { return orders.get(orderId); }
    public ProtoUserState user(final long userId) { return users.get(userId); }
    public ProtoMarketState market(final int marketId) { return markets[marketId]; }
    public long nextOrderId() { return nextOrderId; }
    public long eventSeq() { return eventSeq; }
    public long lastFillTradeId() { return lastFillTradeId; }
    public long acceptedCount() { return acceptedCount; }
    public long rejectedCount() { return rejectedCount; }
    public long cancelledCount() { return cancelledCount; }
    public long expiredCount() { return expiredCount; }
    public long fillCount() { return fillCount; }
    public long fillsIgnored() { return fillsIgnored; }
    public long settleFaultCount() { return settleFaultCount; }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.CancelOrderCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Order lifecycle admission and risk (RC1 S4; lifecycle spec B1-B7, risk spec B1-B7). One apply, one
 * decision: validate, gate, requestId, rate, breaker, size/notional, collar, slot, position, hold, then the
 * record, the indexes, the GTD deadline, the ME dispatch and the egress. A reject mutates nothing but the
 * rate counters (and only from the rate check on). The hold IS the balance check. Everything replicated here
 * is snapshotted (layout sections 2, 3, 4, 6); every index is derived from the records.
 *
 * <p>Fills, terminals, outcomes (S5) and synthetic types (S6) are not served yet: a synthetic type is refused
 * INVALID, FOK is refused UNSUPPORTED_TIF (critic D3), IOC is accepted with its flag and emulated at ACK_NEW.</p>
 */
public final class OrderEngine {

    public static final int SIDE_BUY = 0;
    public static final int SIDE_SELL = 1;
    public static final int TYPE_LIMIT = 0;
    public static final int TYPE_MARKET = 1;
    public static final int TYPE_LIMIT_MAKER = 2;
    public static final int TIF_GTC = 0;
    public static final int TIF_IOC = 1;
    public static final int TIF_FOK = 2;
    public static final int TIF_GTD = 3;
    public static final int MARKETS = UserRisk.MARKET_SLOTS - 1;
    private static final long ONE_FP = 100_000_000L;
    private static final int T_CREATE = 50;
    private static final int T_CANCEL = 51;
    private static final int T_RISK = 71;
    private static final int T_BREAKER = 72;
    private static final int T_REQUEST_RISK = 75;

    private final AssetsEngine ledger;
    private final OrderStore orders = new OrderStore();
    private final RequestTable requests = new RequestTable();
    private final MarketRisk[] markets = new MarketRisk[UserRisk.MARKET_SLOTS];
    private final Long2ObjectHashMap<UserRisk> users = new Long2ObjectHashMap<>();
    private long[] userScratch = new long[256];

    // Diagnostics (node-local, not replicated, not snapshotted).
    private long acceptedCount;
    private long rejectedCount;
    private long cancelledCount;
    private long staleTicks;

    public OrderEngine(final AssetsEngine ledger) {
        this.ledger = ledger;
        for (int m = 0; m < markets.length; m++) {
            markets[m] = new MarketRisk(m);
        }
    }

    // ---------------------------------------------------------------- CreateOrder

    public void applyCreate(final CreateOrderCommand c, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final AssetsEventSink sink = ledger.eventSink();

        // 0. structure + gate: input errors and operations create no user state and consume no budget
        if (c.marketId < 1 || c.marketId > MARKETS) {
            reject(sink, c, CoreRejectReason.UNKNOWN_MARKET, c.marketId, now, false);
            return;
        }
        if (c.quantity <= 0 || c.price < 0 || c.budget < 0 || c.stopPrice < 0 || c.trailingDelta < 0 || c.displayQty < 0
                || c.expiresAtMs < 0 || (c.side != SIDE_BUY && c.side != SIDE_SELL) || c.orderType < 0 || c.orderType > 6
                || c.timeInForce < 0 || c.timeInForce > 3 || c.userId <= 0) {
            reject(sink, c, CoreRejectReason.INVALID, c.quantity <= 0 ? c.quantity : c.side, now, false);
            return;
        }
        if (c.orderType > TYPE_LIMIT_MAKER) {
            reject(sink, c, CoreRejectReason.INVALID, c.orderType, now, false); // synthetic types land in S6
            return;
        }
        if (c.orderType == TYPE_MARKET ? c.price != 0 : c.price <= 0) {
            reject(sink, c, CoreRejectReason.INVALID, c.price, now, false);
            return;
        }
        if (c.orderType != TYPE_MARKET && c.budget != 0) {
            reject(sink, c, CoreRejectReason.INVALID, c.budget, now, false);
            return;
        }
        if (c.timeInForce == TIF_FOK) {
            reject(sink, c, CoreRejectReason.UNSUPPORTED_TIF, c.timeInForce, now, false);
            return;
        }
        if (c.flags != 0) {
            reject(sink, c, CoreRejectReason.UNSUPPORTED_FLAGS, c.flags, now, false);
            return;
        }
        if (c.timeInForce == TIF_GTD) {
            if (c.expiresAtMs <= now) {
                reject(sink, c, CoreRejectReason.EXPIRED, c.expiresAtMs, now, false);
                return;
            }
        } else if (c.expiresAtMs != 0) {
            reject(sink, c, CoreRejectReason.INVALID, c.expiresAtMs, now, false);
            return;
        }
        if (c.requestIdHigh == 0 && c.requestIdLow == 0) {
            reject(sink, c, CoreRejectReason.MISSING_REQUEST_ID, 0, now, false);
            return;
        }
        if (!ledger.gate().admits(c.marketId)) {
            reject(sink, c, CoreRejectReason.GATE_CLOSED, c.marketId, now, false);
            return;
        }

        // 0b. requestId: a replay returns the stored decision and consumes nothing
        final RequestTable.Entry seen = requests.get(c.userId, c.requestIdHigh, c.requestIdLow);
        if (seen != null) {
            if (seen.fingerprint != c.fingerprint()) {
                reject(sink, c, CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH, seen.requestSeq, now, false);
                return;
            }
            if (seen.reason != 0) {
                reject(sink, c, reasonOf(seen.reason), 0, now, false); // the stored decision stands (R-C1b)
                return;
            }
            final CoreOrder live = orders.get(seen.orderId);
            sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, seen.orderId, c.userId, c.marketId,
                    live == null ? OrderStatus.PENDING_NEW : statusOf(live), true,
                    live == null ? 0 : live.holdAssetId, live == null ? 0L : live.holdAmount, now, live == null ? 0L : live.eventSeq);
            return;
        }
        if (requests.countFor(c.userId) >= RequestTable.PER_USER_CAP) {
            reject(sink, c, CoreRejectReason.TOO_MANY_PENDING_REQUESTS, requests.countFor(c.userId), now, false);
            return;
        }
        // 0c. clientOrderId: unique among the user's ACTIVE orders (critic D4, R-C2b); reusable once the order is gone
        if ((c.clientOrderIdHigh != 0 || c.clientOrderIdLow != 0)
                && orders.activeByClientId(c.userId, c.clientOrderIdHigh, c.clientOrderIdLow) != 0) {
            reject(sink, c, CoreRejectReason.DUPLICATE_CLIENT_ORDER_ID,
                    orders.activeByClientId(c.userId, c.clientOrderIdHigh, c.clientOrderIdLow), now, false);
            return;
        }

        final MarketRisk m = markets[c.marketId];
        final UserRisk u = userOrCreate(c.userId);

        // 1. rate: fixed windows on log time; the one deliberate mutation on a reject
        u.roll(now);
        if (u.secCount >= m.maxOrdersPerSec || u.minCount >= m.maxOrdersPerMin) {
            reject(sink, c, CoreRejectReason.RATE_LIMIT, u.secCount >= m.maxOrdersPerSec ? m.maxOrdersPerSec : m.maxOrdersPerMin, now, true);
            return;
        }
        u.secCount++;
        u.minCount++;

        // 2. breaker
        if (m.breakerTripped) {
            reject(sink, c, CoreRejectReason.CIRCUIT_BREAKER, c.marketId, now, true);
            return;
        }

        // 3. size + notional (one reference price for the notional AND the hold)
        if (c.quantity < m.minQuantity) {
            reject(sink, c, CoreRejectReason.SIZE_TOO_SMALL, m.minQuantity, now, true);
            return;
        }
        if (c.quantity > m.maxQuantity) {
            reject(sink, c, CoreRejectReason.SIZE_TOO_LARGE, m.maxQuantity, now, true);
            return;
        }
        long refPrice = c.price;
        long budget = c.budget;
        if (c.orderType == TYPE_MARKET && c.side == SIDE_BUY) {
            if (budget == 0) {
                if (m.bestAsk <= 0) {
                    reject(sink, c, CoreRejectReason.NO_REFERENCE_PRICE, c.marketId, now, true);
                    return;
                }
                final long slip = ONE_FP + (long) m.marketBuySlippageBps * 10_000L;
                if (!FixedPoint.multiplyFits(m.bestAsk, slip)) {
                    reject(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, m.bestAsk, now, true);
                    return;
                }
                refPrice = FixedPoint.multiply(m.bestAsk, slip);
                if (!FixedPoint.multiplyFits(refPrice, c.quantity)) {
                    reject(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, refPrice, now, true);
                    return;
                }
                budget = FixedPoint.multiply(refPrice, c.quantity);
            }
        }
        long notional = 0L;
        if (c.orderType == TYPE_MARKET && c.side == SIDE_BUY) {
            notional = budget;
        } else if (refPrice > 0) {
            if (!FixedPoint.multiplyFits(refPrice, c.quantity)) {
                reject(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, refPrice, now, true);
                return;
            }
            notional = FixedPoint.multiply(refPrice, c.quantity);
        }
        // a BUY whose notional rounds to zero in 8dp has nothing to lock: refused here, never at the hold
        if ((c.side == SIDE_BUY && notional <= 0) || notional < m.minNotional) {
            reject(sink, c, CoreRejectReason.NOTIONAL_TOO_SMALL, notional, now, true);
            return;
        }
        if (notional > m.maxNotional) {
            reject(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, notional, now, true);
            return;
        }

        // 4. collar against the last applied trade, exact basis points
        if (c.price > 0 && m.lastTradePrice > 0 && m.priceCollarBps > 0) {
            final long dev = Math.abs(c.price - m.lastTradePrice);
            final long limit = bps(m.lastTradePrice, m.priceCollarBps);
            if (dev > limit) {
                reject(sink, c, CoreRejectReason.PRICE_COLLAR, limit, now, true);
                return;
            }
        }

        // 5. slot = membership in the open set
        if (orders.openCount(c.userId) >= m.maxOpenOrders) {
            reject(sink, c, CoreRejectReason.OPEN_ORDER_LIMIT, m.maxOpenOrders, now, true);
            return;
        }

        // 7. position (6, the balance check, is the hold): the projected net position must fit a long
        //    whatever the limit (never a wrap), and stay within the limit when one is configured
        final long pos = u.position[c.marketId];
        final long projected = c.side == SIDE_BUY ? pos + c.quantity : pos - c.quantity;
        final boolean overflow = (c.side == SIDE_BUY ? projected < pos : projected > pos) || projected == Long.MIN_VALUE;
        if (overflow) {
            reject(sink, c, CoreRejectReason.INVALID, pos, now, true);
            return;
        }
        if (m.maxPositionPerMarket != Long.MAX_VALUE && Math.abs(projected) > m.maxPositionPerMarket) {
            reject(sink, c, CoreRejectReason.POSITION_LIMIT, projected, now, true);
            return;
        }

        // 8. the hold: BUY locks quote notional/budget, SELL locks base quantity
        final Market market = Market.ALL[c.marketId - 1];
        final int holdAsset = c.side == SIDE_BUY ? market.quoteAsset().id() : market.baseAsset().id();
        final long holdAmount = c.side == SIDE_BUY ? notional : c.quantity;
        final Account account = ledger.account(c.userId);
        if (account == null) {
            reject(sink, c, CoreRejectReason.INSUFFICIENT_FUNDS, holdAmount, now, true); // never creates one (K7)
            return;
        }
        final long orderId = ledger.getNextOrderId() <= 0 ? 1L : ledger.getNextOrderId();
        final RejectReason held = account.hold(orderId, holdAsset, holdAmount, false);
        if (!held.accepted()) {
            reject(sink, c, held == RejectReason.INSUFFICIENT_FUNDS ? CoreRejectReason.INSUFFICIENT_FUNDS
                    : CoreRejectReason.NOTIONAL_TOO_SMALL, holdAmount, now, true);
            return;
        }

        // ---- accepted: record, indexes, deadline, dispatch, egress (nothing above this line mutated
        //      anything but the rate counters and the hold)
        ledger.setNextOrderId(orderId + 1);
        final CoreOrder o = orders.acquire();
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
        o.budget = budget;
        o.expiresAtMs = c.timeInForce == TIF_GTD ? c.expiresAtMs : 0L;
        o.status = (byte) OrderStatus.PENDING_NEW.code();
        o.remainingQty = c.quantity;
        o.holdAssetId = holdAsset;
        o.holdAmount = holdAmount;
        o.createdAtMs = now;
        o.updatedAtMs = now;
        o.attempt = 0;
        o.pendingKind = (byte) CoreOrder.PK_CREATE;
        o.set(CoreOrder.F_DISPATCH_PENDING);
        if (c.timeInForce == TIF_IOC) {
            o.set(CoreOrder.F_IOC);
        }
        final long eventSeq = ledger.getEventSeq() + 1;
        ledger.setEventSeq(eventSeq);
        o.eventSeq = eventSeq;
        orders.add(o);
        if (o.expiresAtMs > 0) {
            ledger.deadlines().add(o.expiresAtMs, orderId, DeadlineIndex.KIND_GTD);
        }
        remember(c.userId, c.requestIdHigh, c.requestIdLow, c.fingerprint(), now, RequestTable.KIND_CREATE, orderId, 0);
        acceptedCount++;

        sink.onBalanceUpdate(c.userId, holdAsset, account.available(holdAsset), account.locked(holdAsset));
        dispatch(sink, o, DispatchKind.CREATE, 0L, c.price, c.quantity, budget);
        sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, orderId, c.userId, c.marketId,
                OrderStatus.PENDING_NEW, false, holdAsset, holdAmount, now, eventSeq);
        sink.onOrderEvent(eventSeq, ledger.getLogPosition(), o, EventKind.ACCEPTED, CoreRejectReason.NONE, 0L, 0L, 0L,
                account.holdRemaining(orderId), now);
    }

    /** Mint the next durable ME command for {@code o}: idHigh = orderId, idLow = attempt << 2 | kind. */
    private void dispatch(final AssetsEventSink sink, final CoreOrder o, final DispatchKind kind, final long oldLegId,
                          final long price, final long quantity, final long budget) {
        final long dispatchSeq = ledger.getDispatchSeq() + 1;
        ledger.setDispatchSeq(dispatchSeq);
        final long idLow = ((long) o.attempt << 2) | kind.code();
        sink.onMeDispatch(dispatchSeq, o.orderId, idLow, kind, o.userId, o.orderId, oldLegId, o.marketId,
                o.orderType, o.side, price, quantity, budget, o.timeInForce);
    }

    private void reject(final AssetsEventSink sink, final CreateOrderCommand c, final CoreRejectReason reason, final long detail,
                        final long now, final boolean remember) {
        rejectedCount++;
        sink.onOrderReject(c.correlationId, c.requestIdHigh, c.requestIdLow, c.userId, 0L, c.marketId, T_CREATE, reason, 255,
                detail, now);
        if (remember) {
            remember(c.userId, c.requestIdHigh, c.requestIdLow, c.fingerprint(), now, RequestTable.KIND_CREATE, 0L, reason.code());
        }
    }

    private void remember(final long userId, final long high, final long low, final long fingerprint, final long now,
                          final int kind, final long orderId, final int reason) {
        final RequestTable.Entry e = requests.put(userId, high, low, fingerprint, now + RequestTable.TTL_MS, kind, orderId, reason);
        ledger.deadlines().add(e.expiresAtMs, e.requestSeq, DeadlineIndex.KIND_REQUEST_TTL);
    }

    private static CoreRejectReason reasonOf(final int code) {
        for (final CoreRejectReason r : CoreRejectReason.values()) {
            if (r.code() == code) {
                return r;
            }
        }
        return CoreRejectReason.INVALID;
    }

    private static OrderStatus statusOf(final CoreOrder o) {
        final OrderStatus s = OrderStatus.fromCode(o.status);
        return s == null ? OrderStatus.PENDING_NEW : s;
    }

    /** floor(value * bps / 10_000) without overflow. */
    static long bps(final long value, final int bps) {
        return (value / 10_000L) * bps + ((value % 10_000L) * bps) / 10_000L;
    }

    // ---------------------------------------------------------------- CancelOrder

    public void applyCancel(final CancelOrderCommand c, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final AssetsEventSink sink = ledger.eventSink();
        final boolean hasRequest = c.requestIdHigh != 0 || c.requestIdLow != 0;
        if (hasRequest) {
            final RequestTable.Entry seen = requests.get(c.userId, c.requestIdHigh, c.requestIdLow);
            if (seen != null) {
                if (seen.fingerprint != c.fingerprint()) {
                    rejectCancel(sink, c, c.orderId, CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH, now);
                    return;
                }
                if (seen.reason != 0) {
                    rejectCancel(sink, c, seen.orderId, reasonOf(seen.reason), now);
                } else {
                    final CoreOrder live = orders.get(seen.orderId);
                    sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, seen.orderId, c.userId,
                            live == null ? 0 : live.marketId, OrderStatus.PENDING_CANCEL, true,
                            live == null ? 0 : live.holdAssetId, live == null ? 0L : live.holdAmount, now, live == null ? 0L : live.eventSeq);
                }
                return;
            }
        }
        long orderId = c.orderId;
        if (orderId == 0) {
            orderId = orders.activeByClientId(c.userId, c.origClientOrderIdHigh, c.origClientOrderIdLow);
        }
        final CoreOrder o = orderId == 0 ? null : orders.get(orderId);
        if (o == null) {
            rejectCancel(sink, c, orderId, CoreRejectReason.UNKNOWN_ORDER, now);
            return;
        }
        if (o.userId != c.userId) {
            rejectCancel(sink, c, orderId, CoreRejectReason.NOT_OWNER, now);
            return;
        }
        if (o.isTerminal()) {
            rejectCancel(sink, c, orderId, CoreRejectReason.NOT_OPEN, now);
            return;
        }
        if (o.has(CoreOrder.F_CANCEL_PENDING)) {
            // already pending: acknowledged, never a second ME command (R-C8c)
            sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, orderId, c.userId, o.marketId,
                    OrderStatus.PENDING_CANCEL, true, o.holdAssetId, o.holdAmount, now, o.eventSeq);
            return;
        }
        o.set(CoreOrder.F_CANCEL_PENDING);
        o.updatedAtMs = now;
        if (!o.has(CoreOrder.F_DISPATCH_PENDING) && o.currentLegId != 0) {
            // a leg is resting at the ME: mint the cancel now (C8b); while the create is unresolved the flag
            // alone is recorded and the cancel is minted when the ACK lands (S5)
            o.attempt++;
            o.pendingKind = (byte) CoreOrder.PK_CANCEL;
            o.set(CoreOrder.F_DISPATCH_PENDING);
            dispatch(sink, o, DispatchKind.CANCEL, o.currentLegId, 0L, 0L, 0L);
        }
        cancelledCount++;
        if (hasRequest) {
            remember(c.userId, c.requestIdHigh, c.requestIdLow, c.fingerprint(), now, RequestTable.KIND_CANCEL, orderId, 0);
        }
        sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, orderId, c.userId, o.marketId,
                OrderStatus.PENDING_CANCEL, false, o.holdAssetId, o.holdAmount, now, o.eventSeq);
    }

    private void rejectCancel(final AssetsEventSink sink, final CancelOrderCommand c, final long orderId,
                              final CoreRejectReason reason, final long now) {
        rejectedCount++;
        sink.onOrderReject(c.correlationId, c.requestIdHigh, c.requestIdLow, c.userId, orderId, 0, T_CANCEL, reason, 255,
                orderId, now);
    }

    // ---------------------------------------------------------------- admin: risk config, breaker, tick

    public void applyRiskConfigSet(final RiskConfigSetCommand c, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final AssetsEventSink sink = ledger.eventSink();
        if (c.marketId < 1 || c.marketId > MARKETS) {
            sink.onAdminAck(c.correlationId, T_RISK, AdminResult.REJECTED, CoreRejectReason.UNKNOWN_MARKET.code(), c.configVersion);
            return;
        }
        if (!MarketRisk.valid(c.minQuantity, c.maxQuantity, c.minNotional, c.maxNotional, c.priceCollarBps, c.maxOrdersPerSec,
                c.maxOrdersPerMin, c.maxOpenOrders, c.maxPositionPerMarket, c.marketBuySlippageBps)) {
            sink.onAdminAck(c.correlationId, T_RISK, AdminResult.REJECTED, CoreRejectReason.INVALID.code(), c.configVersion);
            return;
        }
        final MarketRisk m = markets[c.marketId];
        final boolean same = m.sameConfig(c.minQuantity, c.maxQuantity, c.minNotional, c.maxNotional, c.priceCollarBps,
                c.maxOrdersPerSec, c.maxOrdersPerMin, c.maxOpenOrders, c.maxPositionPerMarket, c.marketBuySlippageBps);
        if (c.configVersion == m.configVersion && same && c.configVersion != 0) {
            sink.onAdminAck(c.correlationId, T_RISK, AdminResult.DUPLICATE, 0, m.configVersion);
            return;
        }
        if (c.configVersion <= m.configVersion) {
            sink.onAdminAck(c.correlationId, T_RISK, AdminResult.STALE, 0, m.configVersion);
            return;
        }
        m.applyConfig(c.configVersion, c.minQuantity, c.maxQuantity, c.minNotional, c.maxNotional, c.priceCollarBps,
                c.maxOrdersPerSec, c.maxOrdersPerMin, c.maxOpenOrders, c.maxPositionPerMarket, c.marketBuySlippageBps);
        sink.onAdminAck(c.correlationId, T_RISK, AdminResult.ACCEPTED, 0, m.configVersion);
        sink.onConfigApplied(ConfigKind.RISK, c.marketId, m.configVersion, ledger.getLogPosition());
    }

    public void applyCircuitBreaker(final long correlationId, final int marketId, final boolean trip, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final AssetsEventSink sink = ledger.eventSink();
        if (marketId < 1 || marketId > MARKETS) {
            sink.onAdminAck(correlationId, T_BREAKER, AdminResult.REJECTED, CoreRejectReason.UNKNOWN_MARKET.code(), 0L);
            return;
        }
        final MarketRisk m = markets[marketId];
        if (m.breakerTripped == trip) {
            sink.onAdminAck(correlationId, T_BREAKER, AdminResult.DUPLICATE, 0, 0L);
            return;
        }
        m.breakerTripped = trip;
        sink.onAdminAck(correlationId, T_BREAKER, AdminResult.ACCEPTED, 0, 0L);
        sink.onAdmissionState(marketId, ledger.gate().state(marketId), ledger.gate().reason(marketId), trip,
                ledger.getLastLogTime(), ledger.getLogPosition(), false);
    }

    /** A forwarder top-of-book tick: state only; a bookVersion at or below the stored one is stale (K4). */
    public void applyMarketTick(final int marketId, final long bookVersion, final long bestBid, final long bestAsk,
                                final long lastTradePrice, final long lastTradeId, final long meEpoch, final long timestamp) {
        ledger.observeLogTime(timestamp);
        if (marketId < 1 || marketId > MARKETS) {
            return;
        }
        final MarketRisk m = markets[marketId];
        if (bookVersion <= m.bookVersion) {
            staleTicks++;
            return;
        }
        m.bookVersion = bookVersion;
        m.bestBid = Math.max(0L, bestBid);
        m.bestAsk = Math.max(0L, bestAsk);
        m.lastTickTs = ledger.getLastLogTime();
        if (lastTradeId > m.lastTradeId && lastTradePrice > 0) {
            m.lastTradeId = lastTradeId;
            m.lastTradePrice = lastTradePrice;
        }
    }

    /** Read-only: the effective version for a market (the record itself has no reply template yet). */
    public void applyRequestRiskConfig(final long correlationId, final int marketId) {
        final AssetsEventSink sink = ledger.eventSink();
        if (marketId < 1 || marketId > MARKETS) {
            sink.onAdminAck(correlationId, T_REQUEST_RISK, AdminResult.REJECTED, CoreRejectReason.UNKNOWN_MARKET.code(), 0L);
            return;
        }
        sink.onAdminAck(correlationId, T_REQUEST_RISK, AdminResult.ACCEPTED, 0, markets[marketId].configVersion);
    }

    // ---------------------------------------------------------------- the sweep

    /** A deadline popped by the single sweep (one-shot): GTD expiry of an order, or a requestId TTL. */
    public void onDeadline(final long deadlineMs, final long id, final int kind, final long now) {
        switch (kind) {
            case DeadlineIndex.KIND_GTD: {
                final CoreOrder o = orders.get(id);
                if (o == null || o.isTerminal() || o.expiresAtMs != deadlineMs || o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
                    return;
                }
                o.set(CoreOrder.F_EXPIRY_REQUESTED);
                o.set(CoreOrder.F_CANCEL_PENDING);
                o.updatedAtMs = now;
                if (!o.has(CoreOrder.F_DISPATCH_PENDING) && o.currentLegId != 0) {
                    o.attempt++;
                    o.pendingKind = (byte) CoreOrder.PK_CANCEL;
                    o.set(CoreOrder.F_DISPATCH_PENDING);
                    dispatch(ledger.eventSink(), o, DispatchKind.CANCEL, o.currentLegId, 0L, 0L, 0L);
                }
                // the create is unresolved: the single cancel is minted when the ACK lands (C9 + C8b)
                break;
            }
            case DeadlineIndex.KIND_REQUEST_TTL:
                requests.expire(id);
                break;
            default:
                break;
        }
    }

    // ---------------------------------------------------------------- money requestIds (Deposit/Withdraw)

    /** Fingerprint of a money command for replay detection. */
    public static long moneyFingerprint(final long userId, final int assetId, final long amount) {
        long h = 0x4d6f6e6579L;
        h = CreateOrderCommand.mix(h, userId);
        h = CreateOrderCommand.mix(h, assetId);
        h = CreateOrderCommand.mix(h, amount);
        return h;
    }

    /** Remember an accepted money command under its requestId (no per-user cap for money, TTL applies). */
    public void rememberMoney(final long userId, final long high, final long low, final long fingerprint, final int kind,
                              final long amount, final long now) {
        remember(userId, high, low, fingerprint, now, kind, amount, 0);
    }

    // ---------------------------------------------------------------- restore

    /** Restore one record (section 2) and rebuild its indexes and GTD deadline. */
    public void restoreOrder(final CoreOrder o) {
        orders.add(o);
        if (o.expiresAtMs > 0 && !o.isTerminal() && !o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
            ledger.deadlines().add(o.expiresAtMs, o.orderId, DeadlineIndex.KIND_GTD);
        }
    }

    /** Restore one request row (section 6) and its TTL deadline. */
    public void restoreRequest(final RequestTable.Entry e) {
        requests.restore(e);
        ledger.deadlines().add(e.expiresAtMs, e.requestSeq, DeadlineIndex.KIND_REQUEST_TTL);
    }

    // ---------------------------------------------------------------- reads

    public OrderStore orders() { return orders; }
    public RequestTable requests() { return requests; }
    public MarketRisk market(final int marketId) { return markets[marketId]; }
    public UserRisk user(final long userId) { return users.get(userId); }

    public UserRisk userOrCreate(final long userId) {
        UserRisk u = users.get(userId);
        if (u == null) {
            u = new UserRisk(userId);
            users.put(userId, u);
        }
        return u;
    }

    public int userCount() { return users.size(); }

    /** Visit users in ascending userId (canonical). Not reentrant. */
    public void forEachUserByUserId(final Consumer<UserRisk> visitor) {
        int n = 0;
        if (userScratch.length < users.size()) {
            userScratch = new long[Math.max(users.size(), userScratch.length * 2)];
        }
        for (final UserRisk u : users.values()) {
            userScratch[n++] = u.userId;
        }
        Arrays.sort(userScratch, 0, n);
        for (int i = 0; i < n; i++) {
            visitor.accept(users.get(userScratch[i]));
        }
    }

    public long acceptedCount() { return acceptedCount; }
    public long rejectedCount() { return rejectedCount; }
    public long cancelledCount() { return cancelledCount; }
    public long staleTicks() { return staleTicks; }
}

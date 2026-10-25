// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreOrderView;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.domain.commands.SettleCommand;
import org.agrona.collections.Long2LongHashMap;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
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
    public static final int TYPE_STOP_LOSS = 3;
    public static final int TYPE_STOP_LIMIT = 4;
    public static final int TYPE_TRAILING_STOP = 5;
    public static final int TYPE_ICEBERG = 6;
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
    private static final int T_AMEND = 52;
    private static final int T_ORDER_SNAPSHOT = 53;
    private static final int T_QUARANTINE = 74;
    /** Replace window observability deadline (lifecycle B5, D6): no state change when it fires. */
    public static final long REPLACE_DEADLINE_MS = 10_000L;
    /** RequestOrderSnapshot page cap (contract 3.6). */
    public static final int ORDER_SNAPSHOT_PAGE = 512;
    public static final int PENDING_DISPATCH_DEFAULT = 1024;
    public static final int PENDING_DISPATCH_CAP = 4096;
    private static final int ST_NEW = 0;
    private static final int ST_PARTIALLY_FILLED = 1;
    private static final int ST_FILLED = 2;
    private static final int ST_CANCELLED = 3;
    private static final int ST_REJECTED = 4;
    private static final int ST_EXPIRED = 5;
    private static final int ST_PENDING_NEW = 6;
    private static final int ST_PENDING_REPLACE = 9;
    private static final int RESULT_APPLIED = 0;
    private static final int RESULT_REJECTED = 1;
    private static final int RESULT_CONFLICT = 2;
    private static final int RESULT_CAPACITY = 3;
    private static final int RESULT_UNKNOWN_LEG = 4;
    private static final int RESULT_NOT_OWNER = 5;
    private static final int RESULT_UNKNOWN_MARKET = 6;

    private static final int ST_PENDING_TRIGGER = 7;

    private final SettleCommand settleCommand = new SettleCommand();
    private final TriggerBook triggers = new TriggerBook();
    private final org.agrona.collections.LongArrayList firedScratch = new org.agrona.collections.LongArrayList();
    private long invalidTickCount;
    private long triggeredCount;
    private long sliceCount;
    private final CoreOrderView[] snapshotPage = new CoreOrderView[ORDER_SNAPSHOT_PAGE];

    private final AssetsEngine ledger;
    private final OrderStore orders = new OrderStore();
    private final RequestTable requests = new RequestTable();
    private final MarketRisk[] markets = new MarketRisk[UserRisk.MARKET_SLOTS];
    private final Long2ObjectHashMap<UserRisk> users = new Long2ObjectHashMap<>();
    private long[] userScratch = new long[256];

    // Diagnostics (node-local, not replicated, not snapshotted).
    private long staleTerminalCount;
    private long staleOutcomeCount;
    private long redundantOutcomeCount;
    private long implicitAckCount;
    private long conflictCount;
    private long cancelRefusedCount;
    private long replaceSlowCount;
    private long tradeGapCount;
    private long orderFaultCount;
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
        // field shape per type (S6 contract section 1): price-less types carry no price, only they may carry a budget
        final boolean priceLess = isPriceLess(c.orderType);
        if (priceLess ? c.price != 0 : c.price <= 0) {
            reject(sink, c, CoreRejectReason.INVALID, c.price, now, false);
            return;
        }
        if (!priceLess && c.budget != 0) {
            reject(sink, c, CoreRejectReason.INVALID, c.budget, now, false);
            return;
        }
        if (!syntheticFieldsValid(c)) {
            reject(sink, c, CoreRejectReason.INVALID, c.orderType, now, false);
            return;
        }
        if (c.timeInForce == TIF_FOK || (c.orderType >= TYPE_STOP_LOSS && c.timeInForce == TIF_IOC)) {
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
        final boolean priceLessBuy = priceLess && c.side == SIDE_BUY;
        if (priceLessBuy && budget == 0) {
            // the deterministic estimate (S6 contract section 1): MARKET on the ask, STOP_LOSS on its stop price,
            // TRAILING_STOP on ask + delta (its trigger point if the ask never falls), times (1 + slippage bps)
            final long base;
            if (c.orderType == TYPE_STOP_LOSS) {
                base = c.stopPrice;
            } else {
                if (m.bestAsk <= 0) {
                    reject(sink, c, CoreRejectReason.NO_REFERENCE_PRICE, c.marketId, now, true);
                    return;
                }
                if (c.orderType == TYPE_TRAILING_STOP && m.bestAsk > Long.MAX_VALUE - c.trailingDelta) {
                    reject(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, m.bestAsk, now, true);
                    return;
                }
                base = c.orderType == TYPE_TRAILING_STOP ? m.bestAsk + c.trailingDelta : m.bestAsk;
            }
            final long slip = ONE_FP + (long) m.marketBuySlippageBps * 10_000L;
            if (!FixedPoint.multiplyFits(base, slip)) {
                reject(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, base, now, true);
                return;
            }
            refPrice = FixedPoint.multiply(base, slip);
            if (!FixedPoint.multiplyFits(refPrice, c.quantity)) {
                reject(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, refPrice, now, true);
                return;
            }
            budget = FixedPoint.multiply(refPrice, c.quantity);
        }
        long notional = 0L;
        if (priceLessBuy) {
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

        // 4b. a stop that is already crossed would fire on the next tick: refused (S6 contract section 1, D8)
        if (c.orderType == TYPE_STOP_LOSS || c.orderType == TYPE_STOP_LIMIT) {
            final boolean crossed = c.side == SIDE_SELL ? m.bestBid > 0 && m.bestBid <= c.stopPrice
                    : m.bestAsk > 0 && m.bestAsk >= c.stopPrice;
            if (crossed) {
                reject(sink, c, CoreRejectReason.STOP_WOULD_TRIGGER_IMMEDIATELY, c.side == SIDE_SELL ? m.bestBid : m.bestAsk, now, true);
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
        o.stopPrice = c.stopPrice;
        o.trailingDelta = c.trailingDelta;
        o.displayQty = c.displayQty;
        o.expiresAtMs = c.timeInForce == TIF_GTD ? c.expiresAtMs : 0L;
        final boolean armed = isTriggered(c.orderType);
        o.status = (byte) (armed ? OrderStatus.PENDING_TRIGGER.code() : OrderStatus.PENDING_NEW.code());
        o.remainingQty = c.quantity;
        o.holdAssetId = holdAsset;
        o.holdAmount = holdAmount;
        o.createdAtMs = now;
        o.updatedAtMs = now;
        o.attempt = 0;
        if (c.orderType == TYPE_TRAILING_STOP) {
            final long quote = c.side == SIDE_SELL ? m.bestBid : m.bestAsk;
            o.trailingArmPrice = quote > 0 ? quote : 0L;
        }
        if (c.orderType == TYPE_ICEBERG) {
            o.sliceRemainingQty = c.displayQty;
            o.hiddenQty = c.quantity - c.displayQty;
        }
        if (!armed) {
            o.pendingKind = (byte) CoreOrder.PK_CREATE;
            o.set(CoreOrder.F_DISPATCH_PENDING);
        }
        if (c.timeInForce == TIF_IOC) {
            o.set(CoreOrder.F_IOC);
        }
        final long eventSeq = ledger.getEventSeq() + 1;
        ledger.setEventSeq(eventSeq);
        o.eventSeq = eventSeq;
        orders.add(o);
        if (armed) {
            triggers.add(o);
        }
        if (o.expiresAtMs > 0) {
            ledger.deadlines().add(o.expiresAtMs, orderId, DeadlineIndex.KIND_GTD);
        }
        remember(c.userId, c.requestIdHigh, c.requestIdLow, c.fingerprint(), now, RequestTable.KIND_CREATE, orderId, 0);
        acceptedCount++;

        sink.onBalanceUpdate(c.userId, holdAsset, account.available(holdAsset), account.locked(holdAsset));
        if (!armed) {
            dispatch(sink, o, DispatchKind.CREATE, 0L, c.price, createQuantity(o), budget);
        }
        sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, orderId, c.userId, c.marketId,
                armed ? OrderStatus.PENDING_TRIGGER : OrderStatus.PENDING_NEW, false, holdAsset, holdAmount, now, eventSeq);
        sink.onOrderEvent(eventSeq, ledger.getLogPosition(), o, EventKind.ACCEPTED, CoreRejectReason.NONE, 0L, 0L, 0L,
                account.holdRemaining(orderId), now);
    }

    // ---------------------------------------------------------------- RC1 S6: synthetic shape helpers

    /** MARKET, STOP_LOSS and TRAILING_STOP carry no limit price (the ME child is a MARKET order). */
    static boolean isPriceLess(final int orderType) {
        return orderType == TYPE_MARKET || orderType == TYPE_STOP_LOSS || orderType == TYPE_TRAILING_STOP;
    }

    /** A stop or trailing order waits in a trigger book; an iceberg works slices at once. */
    static boolean isTriggered(final int orderType) {
        return orderType == TYPE_STOP_LOSS || orderType == TYPE_STOP_LIMIT || orderType == TYPE_TRAILING_STOP;
    }

    /** S6 contract section 1 table: required and forbidden fields per type (the price/budget rules are checked apart). */
    private static boolean syntheticFieldsValid(final CreateOrderCommand c) {
        switch (c.orderType) {
            case TYPE_STOP_LOSS:
                return c.stopPrice > 0 && c.trailingDelta == 0 && c.displayQty == 0;
            case TYPE_STOP_LIMIT:
                return c.stopPrice > 0 && c.trailingDelta == 0 && c.displayQty == 0;
            case TYPE_TRAILING_STOP:
                return c.trailingDelta > 0 && c.stopPrice == 0 && c.displayQty == 0;
            case TYPE_ICEBERG:
                return c.displayQty > 0 && c.displayQty < c.quantity && c.stopPrice == 0 && c.trailingDelta == 0;
            default:
                return c.stopPrice == 0 && c.trailingDelta == 0 && c.displayQty == 0;
        }
    }

    /** The order type the ME sees for the current command of {@code o} (children of stops are MARKET or LIMIT). */
    static int meOrderType(final CoreOrder o) {
        switch (o.orderType) {
            case TYPE_STOP_LOSS:
            case TYPE_TRAILING_STOP:
                return TYPE_MARKET;
            case TYPE_STOP_LIMIT:
            case TYPE_ICEBERG:
                return TYPE_LIMIT;
            default:
                return o.orderType;
        }
    }

    /** The quantity of the current CREATE: an iceberg slice works {@code sliceRemainingQty}, every other create the rest. */
    static long createQuantity(final CoreOrder o) {
        return o.orderType == TYPE_ICEBERG ? o.sliceRemainingQty : o.remainingQty;
    }

    /** Mint the next durable ME command for {@code o}: idHigh = orderId, idLow = attempt << 2 | kind. */
    private void dispatch(final AssetsEventSink sink, final CoreOrder o, final DispatchKind kind, final long oldLegId,
                          final long price, final long quantity, final long budget) {
        final long dispatchSeq = ledger.getDispatchSeq() + 1;
        ledger.setDispatchSeq(dispatchSeq);
        o.pendingDispatchSeq = dispatchSeq;
        o.set(CoreOrder.F_DISPATCH_PENDING);
        final long idLow = ((long) o.attempt << 2) | kind.code();
        sink.onMeDispatch(dispatchSeq, o.orderId, idLow, kind, o.userId, o.orderId, oldLegId, o.marketId,
                meOrderType(o), o.side, price, quantity, budget, o.timeInForce);
    }

    /** Mint the single cancel for a resting leg (attempt+1, kind CANCEL) and say so on the orders channel. */
    private void mintCancel(final CoreOrder o, final long now) {
        o.attempt++;
        o.pendingKind = (byte) CoreOrder.PK_CANCEL;
        o.updatedAtMs = now;
        dispatch(ledger.eventSink(), o, DispatchKind.CANCEL, o.currentLegId, 0L, 0L, 0L);
        emitEvent(o, EventKind.DISPATCHED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
    }

    private void clearPending(final CoreOrder o) {
        o.unset(CoreOrder.F_DISPATCH_PENDING);
        o.pendingDispatchSeq = 0L;
        o.pendingKind = (byte) CoreOrder.PK_NONE;
    }

    private static int kindCodeOf(final int pendingKind) {
        switch (pendingKind) {
            case CoreOrder.PK_CREATE: return DispatchKind.CREATE.code();
            case CoreOrder.PK_CANCEL: return DispatchKind.CANCEL.code();
            case CoreOrder.PK_AMEND:  return DispatchKind.AMEND.code();
            default: return -1;
        }
    }

    private long holdRemaining(final CoreOrder o) {
        final Account a = ledger.account(o.userId);
        return a == null ? 0L : a.holdRemaining(o.orderId);
    }

    /** One OrderEvent per transition with a fresh eventSeq (projector idempotency key). */
    private void emitEvent(final CoreOrder o, final EventKind kind, final CoreRejectReason reason, final long lastPrice,
                           final long lastQty, final long tradeId, final long now) {
        final long eventSeq = ledger.getEventSeq() + 1;
        ledger.setEventSeq(eventSeq);
        o.eventSeq = eventSeq;
        ledger.eventSink().onOrderEvent(eventSeq, ledger.getLogPosition(), o, kind, reason, lastPrice, lastQty, tradeId,
                holdRemaining(o), now);
    }

    private void fault(final OrderFaultKind kind, final CoreOrder o, final long tradeId, final long amount) {
        orderFaultCount++;
        ledger.eventSink().onOrderFault(kind, o.orderId, o.userId, o.marketId, tradeId, amount, ledger.getLogPosition());
    }

    /**
     * The single terminal apply (contract 3.2): release whatever the order still holds, drop its deadlines and pending
     * dispatch, emit the terminal event with the record still visible, then remove the record from every index.
     */
    private void terminalize(final CoreOrder o, final int statusCode, final EventKind kind, final CoreRejectReason reason,
                             final long lastPrice, final long lastQty, final long tradeId, final long now) {
        if (o.status == ST_PENDING_TRIGGER) {
            triggers.remove(o); // an armed order ends without ever reaching the ME (S6 3.3)
        }
        if (o.expiresAtMs > 0 && !o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
            ledger.deadlines().remove(o.expiresAtMs, o.orderId, DeadlineIndex.KIND_GTD);
        }
        if (o.replaceDeadlineMs > 0) {
            ledger.deadlines().remove(o.replaceDeadlineMs, o.orderId, DeadlineIndex.KIND_REPLACE);
            o.replaceDeadlineMs = 0L;
        }
        clearPending(o);
        o.unset(CoreOrder.F_REPLACE_PENDING);
        o.status = (byte) statusCode;
        o.rejectReason = (byte) reason.code();
        o.updatedAtMs = now;
        ledger.releaseOrderHold(o.userId, o.orderId, -1L);
        emitEvent(o, kind, reason, lastPrice, lastQty, tradeId, now);
        if (statusCode == ST_CANCELLED || statusCode == ST_EXPIRED) {
            cancelledCount++;
        }
        orders.remove(o);
    }

    /** B7 resolution: the ME replaced the leg; install the window and settle the hold to its target. */
    private void resolveReplace(final CoreOrder o, final long newLegId, final long now) {
        if (o.pendingHoldDelta < 0) {
            ledger.releaseOrderHold(o.userId, o.orderId, -o.pendingHoldDelta);
        }
        o.holdAmount = o.pendingHoldTarget;
        o.price = o.pendingPrice;
        o.quantity = o.pendingQuantity;
        o.remainingQty = Math.max(0L, o.quantity - o.filledQty);
        o.currentLegId = newLegId;
        closeReplaceWindow(o, now);
        emitEvent(o, EventKind.REPLACED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
    }

    /** B7 abort: the ME refused the amend; give back the top-up, the order stays live on the old leg. */
    private void abortReplace(final CoreOrder o, final CoreRejectReason reason, final long now) {
        if (o.pendingHoldDelta > 0) {
            ledger.releaseOrderHold(o.userId, o.orderId, o.pendingHoldDelta);
        }
        closeReplaceWindow(o, now);
        emitEvent(o, EventKind.REPLACED, reason, 0L, 0L, 0L, now);
    }

    private void closeReplaceWindow(final CoreOrder o, final long now) {
        if (o.replaceDeadlineMs > 0) {
            ledger.deadlines().remove(o.replaceDeadlineMs, o.orderId, DeadlineIndex.KIND_REPLACE);
            o.replaceDeadlineMs = 0L;
        }
        o.pendingPrice = 0L;
        o.pendingQuantity = 0L;
        o.pendingHoldDelta = 0L;
        o.pendingHoldTarget = 0L;
        o.oldLegId = 0L;
        o.unset(CoreOrder.F_REPLACE_PENDING);
        o.status = (byte) (o.filledQty > 0 ? ST_PARTIALLY_FILLED : ST_NEW);
        o.updatedAtMs = now;
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
        if (o.isTerminal() || o.has(CoreOrder.F_QUARANTINED)) {
            rejectCancel(sink, c, orderId, CoreRejectReason.NOT_OPEN, now);
            return;
        }
        if (o.has(CoreOrder.F_CANCEL_PENDING)) {
            // already pending: acknowledged, never a second ME command (R-C8c)
            sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, orderId, c.userId, o.marketId,
                    OrderStatus.PENDING_CANCEL, true, o.holdAssetId, o.holdAmount, now, o.eventSeq);
            return;
        }
        final boolean armed = o.status == ST_PENDING_TRIGGER;
        o.set(CoreOrder.F_CANCEL_PENDING);
        o.updatedAtMs = now;
        final boolean mint = !armed && !o.has(CoreOrder.F_DISPATCH_PENDING) && o.currentLegId != 0;
        if (hasRequest) {
            remember(c.userId, c.requestIdHigh, c.requestIdLow, c.fingerprint(), now, RequestTable.KIND_CANCEL, orderId, 0);
        }
        sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, orderId, c.userId, o.marketId,
                OrderStatus.PENDING_CANCEL, false, o.holdAssetId, o.holdAmount, now, o.eventSeq);
        if (armed) {
            // C8: an armed synthetic order has no ME leg; it ends here (S6 3.3)
            terminalize(o, ST_CANCELLED, EventKind.CANCELLED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
        } else if (mint) {
            // a leg is resting at the ME: mint the cancel now (C8b); while the create is unresolved the flag
            // alone is recorded and the cancel is minted when the ACK lands (3.3)
            mintCancel(o, now);
        }
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
            invalidTickCount++; // S6 3.1 #1: an unknown market is part of the validation, counted like the rest
            return;
        }
        if (bestBid < 0 || bestAsk < 0 || lastTradePrice < 0 || lastTradeId < 0 || (bestBid > 0 && bestAsk > 0 && bestBid >= bestAsk)) {
            invalidTickCount++; // S6 3.1: a malformed or crossed quote is a counted drop, never applied
            return;
        }
        final MarketRisk m = markets[marketId];
        if (bookVersion <= m.bookVersion) {
            staleTicks++;
            return;
        }
        m.bookVersion = bookVersion;
        m.bestBid = bestBid;
        m.bestAsk = bestAsk;
        m.lastTickTs = ledger.getLastLogTime();
        if (lastTradeId > m.lastTradeId && lastTradePrice > 0) {
            m.lastTradeId = lastTradeId;
            m.lastTradePrice = lastTradePrice;
        }
        evaluateTriggers(marketId, m.bestBid, m.bestAsk, ledger.getLastLogTime());
    }

    /** S6 3.1 step 4-5: stops and trailing orders of one market against the new quote; fire in ascending orderId. */
    private void evaluateTriggers(final int marketId, final long bid, final long ask, final long now) {
        final org.agrona.collections.LongArrayList fired = firedScratch;
        fired.clear();
        triggers.collectFiredStops(marketId, bid, ask, fired);
        final org.agrona.collections.LongArrayList trailing = triggers.trailing(marketId);
        for (int i = 0; i < trailing.size(); i++) {
            final CoreOrder o = orders.get(trailing.getLong(i));
            if (o == null) {
                continue;
            }
            final long quote = o.side == SIDE_SELL ? bid : ask;
            if (quote <= 0) {
                continue; // no quote on the side this order watches
            }
            if (o.trailingArmPrice == 0) {
                o.trailingArmPrice = quote; // first valid quote arms it; no trigger on the arming tick
                o.updatedAtMs = now;
            } else if (o.side == SIDE_SELL ? quote > o.trailingArmPrice : quote < o.trailingArmPrice) {
                o.trailingArmPrice = quote; // a new extreme: the advance consumes the tick
                o.updatedAtMs = now;
            } else if ((o.side == SIDE_SELL ? o.trailingArmPrice - quote : quote - o.trailingArmPrice) >= o.trailingDelta) {
                fired.addLong(o.orderId);
            }
        }
        if (fired.isEmpty()) {
            return;
        }
        final long[] ids = new long[fired.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = fired.getLong(i);
        }
        Arrays.sort(ids);
        for (final long id : ids) {
            final CoreOrder o = orders.get(id);
            if (o != null && o.status == ST_PENDING_TRIGGER) {
                fire(o, now);
            }
        }
    }

    /** S6 3.2: the armed parent becomes the working order of its child; one CREATE with a payload derived from the record. */
    private void fire(final CoreOrder o, final long now) {
        triggers.remove(o);
        o.status = (byte) ST_PENDING_NEW;
        o.attempt++;
        o.pendingKind = (byte) CoreOrder.PK_CREATE;
        o.updatedAtMs = now;
        final boolean marketChild = meOrderType(o) == TYPE_MARKET;
        o.budget = marketChild && o.side == SIDE_BUY ? holdRemaining(o) : 0L; // K11 #1: the BUY child spends the hold
        triggeredCount++;
        dispatch(ledger.eventSink(), o, DispatchKind.CREATE, 0L, marketChild ? 0L : o.price, createQuantity(o), o.budget);
        emitEvent(o, EventKind.TRIGGERED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
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
                if (o.status == ST_PENDING_TRIGGER && !o.has(CoreOrder.F_QUARANTINED)) {
                    o.set(CoreOrder.F_EXPIRY_REQUESTED);
                    terminalize(o, ST_EXPIRED, EventKind.EXPIRED, CoreRejectReason.NONE, 0L, 0L, 0L, now); // S6 3.3, R12
                    break;
                }
                o.set(CoreOrder.F_EXPIRY_REQUESTED);
                o.set(CoreOrder.F_CANCEL_PENDING);
                o.updatedAtMs = now;
                if (!o.has(CoreOrder.F_DISPATCH_PENDING) && o.currentLegId != 0 && !o.has(CoreOrder.F_QUARANTINED)) {
                    mintCancel(o, now);
                }
                // the create is unresolved: the single cancel is minted when the ACK lands (C9 + C8b)
                break;
            }
            case DeadlineIndex.KIND_REPLACE: {
                final CoreOrder o = orders.get(id);
                if (o != null && o.has(CoreOrder.F_REPLACE_PENDING) && o.replaceDeadlineMs == deadlineMs) {
                    // observability only (B5, D6): the durable outcome resolves the window. Re-armed while the window
                    // stays open, so a stuck replace keeps ticking (one entry per open window, derivable from the record).
                    replaceSlowCount++;
                    o.replaceDeadlineMs = deadlineMs + REPLACE_DEADLINE_MS;
                    ledger.deadlines().add(o.replaceDeadlineMs, o.orderId, DeadlineIndex.KIND_REPLACE);
                }
                break;
            }
            case DeadlineIndex.KIND_REQUEST_TTL:
                requests.expire(id);
                break;
            default:
                break;
        }
    }

    // ---------------------------------------------------------------- RC1 S5: feed (bridge) events

    /** MeJournalTrade (61) / one entry of MeJournalTradeBatch (62). Contract S5 §3.1. */
    public void applyTrade(final long egressSeq, final long tradeId, final int marketId, final long takerLegId,
                           final long takerUserId, final long makerLegId, final long makerUserId, final long price,
                           final long quantity, final boolean takerIsBuy, final long meTimestamp, final long takerOmsOrderId,
                           final long makerOmsOrderId, final long meEpoch, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final long last = ledger.getLastAppliedTradeId();
        if (tradeId <= last) {
            return; // already applied: byte-level no-op (R-C5b)
        }
        if (tradeId != last + 1) {
            tradeGapCount++;
            ledger.eventSink().onSettleRejected(tradeId, marketId, egressSeq, SettleRejectReason.TRADE_GAP, last + 1);
            return;
        }
        // 1. money, for the full trade quantity, through the ledger's settle (hold-scoped, faults never silent)
        final SettleCommand sc = settleCommand;
        sc.reset();
        sc.setJournalPosition(egressSeq);
        sc.setTradeId(tradeId);
        sc.setMarketId(marketId);
        sc.setTakerOrderId(takerOmsOrderId);
        sc.setTakerUserId(takerUserId);
        sc.setMakerOrderId(makerOmsOrderId);
        sc.setMakerUserId(makerUserId);
        sc.setPrice(price);
        sc.setQuantity(quantity);
        sc.setTakerIsBuy(takerIsBuy);
        if (!ledger.settle(sc, timestamp)) {
            return; // refused before any mutation (SettleRejected emitted); the order accounting stays untouched too
        }
        if (marketId >= 1 && marketId <= MARKETS && tradeId > markets[marketId].lastTradeId) {
            markets[marketId].lastTradeId = tradeId; // S6 3.5: the collar reference follows settled trades
            markets[marketId].lastTradePrice = price;
        }
        // 2. the two core orders (either may be a legacy hold the core does not own)
        applyFill(takerOmsOrderId, takerLegId, takerUserId, price, quantity, tradeId, now);
        applyFill(makerOmsOrderId, makerLegId, makerUserId, price, quantity, tradeId, now);
    }

    private void applyFill(final long omsOrderId, final long legId, final long userId, final long price, final long quantity,
                           final long tradeId, final long now) {
        final CoreOrder o = orders.get(omsOrderId);
        if (o == null) {
            return;
        }
        if (o.userId != userId) {
            fault(OrderFaultKind.SETTLE, o, tradeId, quantity);
            return;
        }
        boolean implicitAck = false;
        boolean windowClosedByTrade = false;
        if (legId != 0 && o.currentLegId == 0 && o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CREATE) {
            // implicit ack: the leg exists at the ME (contract 3.1); the post-ack rules run after the fill below
            o.currentLegId = legId;
            clearPending(o);
            o.status = (byte) ST_NEW;
            o.updatedAtMs = now;
            implicitAckCount++;
            implicitAck = true;
            emitEvent(o, EventKind.ACK_NEW, CoreRejectReason.NONE, 0L, 0L, 0L, now);
        } else if (legId != 0 && legId != o.currentLegId) {
            if (o.has(CoreOrder.F_REPLACE_PENDING) && o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_AMEND) {
                // a trade on the NEW leg before the amend outcome: resolve first (B7), then fill; the window is closed
                // by this trade, so the post-window rules (3.8 #9/#13: a cancel flagged meanwhile) run after the fill
                clearPending(o);
                resolveReplace(o, legId, now);
                windowClosedByTrade = true;
            } else {
                fault(OrderFaultKind.SETTLE, o, tradeId, quantity);
                return;
            }
        }
        final long fillQty = Math.min(quantity, o.remainingQty);
        final long excess = quantity - fillQty;
        if (excess > 0) {
            fault(OrderFaultKind.OVERFILL, o, tradeId, excess);
        }
        if (fillQty <= 0) {
            if (implicitAck) {
                afterAck(o, now);
            }
            return;
        }
        o.filledQty += fillQty;
        o.remainingQty -= fillQty;
        if (o.orderType == TYPE_ICEBERG) {
            if (fillQty > o.sliceRemainingQty) {
                // the ME filled past the slice (within the order): keep remaining == slice + hidden, say so
                final long past = fillQty - o.sliceRemainingQty;
                o.hiddenQty -= Math.min(past, o.hiddenQty);
                o.sliceRemainingQty = 0L;
                fault(OrderFaultKind.SETTLE, o, tradeId, past);
            } else {
                o.sliceRemainingQty -= fillQty;
            }
        }
        o.cumQuoteQty += FixedPoint.multiply(price, fillQty);
        o.lastPrice = price;
        o.lastQty = fillQty;
        o.updatedAtMs = now;
        final UserRisk u = userOrCreate(o.userId);
        final long pos = u.position[o.marketId];
        final long projected = o.side == SIDE_BUY ? pos + fillQty : pos - fillQty;
        final boolean wrap = o.side == SIDE_BUY ? projected < pos : projected > pos;
        if (wrap) {
            fault(OrderFaultKind.POSITION_OVERFLOW, o, tradeId, fillQty);
            u.position[o.marketId] = o.side == SIDE_BUY ? Long.MAX_VALUE : Long.MIN_VALUE + 1;
        } else {
            u.position[o.marketId] = projected;
        }
        if (o.remainingQty == 0) {
            terminalize(o, ST_FILLED, EventKind.FILL, CoreRejectReason.NONE, price, fillQty, tradeId, now);
            return;
        }
        if (o.status == ST_PENDING_NEW || o.status == ST_NEW) {
            o.status = (byte) ST_PARTIALLY_FILLED; // an open replace window keeps PENDING_REPLACE until it resolves
        }
        emitEvent(o, EventKind.FILL, CoreRejectReason.NONE, price, fillQty, tradeId, now);
        if (implicitAck) {
            afterAck(o, now);
        }
        if (windowClosedByTrade && orders.get(o.orderId) == o) {
            afterWindow(o, now);
        }
        if (o.orderType == TYPE_ICEBERG && o.sliceRemainingQty == 0 && orders.get(o.orderId) == o) {
            onSliceExhausted(o, now);
        }
    }

    /**
     * S6 3.4: the current slice has nothing left. A cancel in flight ends the parent now (no live slice remains to cancel);
     * a quarantined parent waits; otherwise the next slice is released in this same apply.
     */
    private void onSliceExhausted(final CoreOrder o, final long now) {
        if (o.hiddenQty <= 0 || o.isTerminal()) {
            return;
        }
        if (o.has(CoreOrder.F_CANCEL_PENDING)) {
            final boolean expired = o.has(CoreOrder.F_EXPIRY_REQUESTED);
            terminalize(o, expired ? ST_EXPIRED : ST_CANCELLED, expired ? EventKind.EXPIRED : EventKind.CANCELLED,
                    CoreRejectReason.NONE, 0L, 0L, 0L, now);
            return;
        }
        if (o.has(CoreOrder.F_QUARANTINED) || o.has(CoreOrder.F_DISPATCH_PENDING)) {
            return;
        }
        final long next = Math.min(o.displayQty, o.hiddenQty);
        o.hiddenQty -= next;
        o.sliceRemainingQty = next;
        o.oldLegId = o.currentLegId;
        o.currentLegId = 0L;
        o.attempt++;
        o.pendingKind = (byte) CoreOrder.PK_CREATE;
        o.updatedAtMs = now;
        sliceCount++;
        dispatch(ledger.eventSink(), o, DispatchKind.CREATE, 0L, o.price, next, 0L);
        emitEvent(o, EventKind.SLICE_SUBMITTED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
    }

    /** What a create ACK triggers once the leg is known: the cancel asked for meanwhile (C8b) or the IOC cancel (C13). */
    private void afterAck(final CoreOrder o, final long now) {
        if (o.has(CoreOrder.F_QUARANTINED) || o.has(CoreOrder.F_DISPATCH_PENDING)) {
            return;
        }
        if (o.has(CoreOrder.F_CANCEL_PENDING)) {
            mintCancel(o, now);
        } else if (o.has(CoreOrder.F_IOC) && o.remainingQty > 0) {
            o.set(CoreOrder.F_CANCEL_PENDING);
            mintCancel(o, now);
        }
    }

    /** A replace window closed (resolved or aborted) with the order still live: a cancel asked for meanwhile goes out now. */
    private void afterWindow(final CoreOrder o, final long now) {
        if (o.remainingQty == 0) {
            terminalize(o, ST_FILLED, EventKind.FILL, CoreRejectReason.NONE, o.lastPrice, 0L, 0L, now); // fills inside the window reached the new quantity
            return;
        }
        if (o.has(CoreOrder.F_CANCEL_PENDING) && !o.has(CoreOrder.F_QUARANTINED) && !o.has(CoreOrder.F_DISPATCH_PENDING)) {
            mintCancel(o, now);
        }
    }

    /** MeJournalTerminal (63). Contract S5 §3.2. */
    public void applyTerminal(final long egressSeq, final long legId, final long userId, final int marketId, final int statusCode,
                              final long meTimestamp, final long omsOrderId, final long meEpoch, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final CoreOrder o = orders.get(omsOrderId);
        if (o == null) {
            staleTerminalCount++;
            return;
        }
        if (o.has(CoreOrder.F_REPLACE_PENDING) && legId != 0 && legId != o.currentLegId) {
            // the NEW leg speaks before the amend outcome: resolve first, then apply (B7)
            clearPending(o);
            resolveReplace(o, legId, now);
        }
        if (!o.has(CoreOrder.F_REPLACE_PENDING) && legId != 0 && o.currentLegId == 0 && legId != o.oldLegId
                && o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CREATE) {
            // G9 (S6 3.6): the ME journals a terminal for a create it refused or ended before its outcome reached us.
            // Learn the leg and apply the terminal; the later CREATE outcome is redundant. A refused leg never rested,
            // so no ACK_NEW is announced for it.
            o.currentLegId = legId;
            clearPending(o);
            implicitAckCount++;
            if (statusCode != ST_REJECTED) {
                if (o.status == ST_PENDING_NEW) {
                    o.status = (byte) ST_NEW;
                }
                emitEvent(o, EventKind.ACK_NEW, CoreRejectReason.NONE, 0L, 0L, 0L, now);
            }
        }
        if (legId != o.currentLegId) {
            staleTerminalCount++; // includes the previous iceberg slice (legId == oldLegId)
            return;
        }
        if (o.has(CoreOrder.F_REPLACE_PENDING)) {
            if (statusCode == ST_REJECTED) {
                clearPending(o); // the ME named the OLD leg when it refused the amend (A6): abort, order stays live
                abortReplace(o, CoreRejectReason.ME_REJECTED, now);
                afterWindow(o, now);
            } else {
                staleTerminalCount++; // the replace's own cancel of the old leg: bookkeeping only
            }
            return;
        }
        switch (statusCode) {
            case ST_FILLED:
                if (o.orderType == TYPE_ICEBERG && o.hiddenQty > 0) {
                    // the current slice ended at the ME while the parent has more to work (S6 3.4)
                    if (o.sliceRemainingQty > 0) {
                        fault(OrderFaultKind.SETTLE, o, 0L, o.sliceRemainingQty);
                        o.hiddenQty += o.sliceRemainingQty; // the unfilled part goes back to hidden: remaining == slice + hidden
                        o.sliceRemainingQty = 0L;
                    }
                    onSliceExhausted(o, now);
                    break;
                }
                if (o.remainingQty > 0) {
                    fault(OrderFaultKind.SETTLE, o, 0L, o.remainingQty);
                }
                terminalize(o, ST_FILLED, EventKind.FILL, CoreRejectReason.NONE, o.lastPrice, 0L, 0L, now);
                break;
            case ST_CANCELLED:
                if (o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
                    terminalize(o, ST_EXPIRED, EventKind.EXPIRED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                } else {
                    terminalize(o, ST_CANCELLED, EventKind.CANCELLED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                }
                break;
            case ST_REJECTED:
                terminalize(o, ST_REJECTED, EventKind.REJECTED, CoreRejectReason.ME_REJECTED, 0L, 0L, 0L, now);
                break;
            case ST_EXPIRED:
                terminalize(o, ST_EXPIRED, EventKind.EXPIRED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                break;
            default:
                staleTerminalCount++;
                break;
        }
    }

    /** MeCommandOutcome (64). Contract S5 §3.3. */
    public void applyOutcome(final long egressSeq, final long commandIdHigh, final long commandIdLow, final long userId,
                             final long omsOrderId, final long oldOrderId, final long price, final long quantity, final long budget,
                             final int marketId, final int commandKind, final int result, final long legId, final int reason,
                             final long meTimestamp, final long meEpoch, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final CoreOrder o = orders.get(commandIdHigh);
        if (o == null || !o.has(CoreOrder.F_DISPATCH_PENDING)
                || commandIdLow != (((long) o.attempt << 2) | kindCodeOf(o.pendingKind))) {
            if (o != null && legId != 0 && legId == o.currentLegId && result == RESULT_APPLIED) {
                redundantOutcomeCount++; // the leg was already learned (implicit ack by trade) or delivered twice
            } else {
                staleOutcomeCount++;
            }
            return;
        }
        final int pending = o.pendingKind;
        switch (result) {
            case RESULT_APPLIED:
                if (pending == CoreOrder.PK_CREATE) {
                    o.currentLegId = legId;
                    clearPending(o);
                    if (o.status == ST_PENDING_NEW) {
                        o.status = (byte) ST_NEW;
                    }
                    o.updatedAtMs = now;
                    emitEvent(o, EventKind.ACK_NEW, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                    afterAck(o, now); // C8b cancel asked for meanwhile, or C13 IOC: one cancel
                } else if (pending == CoreOrder.PK_CANCEL) {
                    clearPending(o);
                    if (o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
                        terminalize(o, ST_EXPIRED, EventKind.EXPIRED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                    } else {
                        terminalize(o, ST_CANCELLED, EventKind.CANCELLED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                    }
                } else {
                    clearPending(o);
                    resolveReplace(o, legId, now);
                    afterWindow(o, now);
                }
                break;
            case RESULT_REJECTED:
            case RESULT_UNKNOWN_LEG:
            case RESULT_NOT_OWNER:
            case RESULT_UNKNOWN_MARKET: {
                final CoreRejectReason why = result == RESULT_REJECTED ? CoreRejectReason.ME_REJECTED
                        : result == RESULT_UNKNOWN_LEG ? CoreRejectReason.ME_UNKNOWN_LEG
                        : result == RESULT_NOT_OWNER ? CoreRejectReason.NOT_OWNER : CoreRejectReason.UNKNOWN_MARKET;
                if (pending == CoreOrder.PK_CREATE) {
                    clearPending(o);
                    terminalize(o, ST_REJECTED, EventKind.REJECTED, why, 0L, 0L, 0L, now);
                } else if (pending == CoreOrder.PK_CANCEL) {
                    clearPending(o); // the leg is gone or refused: the journal terminal settles it; the flag stays
                    cancelRefusedCount++;
                } else {
                    clearPending(o);
                    abortReplace(o, why, now);
                    afterWindow(o, now);
                }
                break;
            }
            case RESULT_CONFLICT:
                clearPending(o);
                o.set(CoreOrder.F_QUARANTINED);
                o.rejectReason = (byte) CoreRejectReason.ME_CONFLICT.code();
                o.updatedAtMs = now;
                conflictCount++;
                emitEvent(o, EventKind.QUARANTINED, CoreRejectReason.ME_CONFLICT, 0L, 0L, 0L, now);
                break;
            case RESULT_CAPACITY:
                // the ME command ledger is full: close order entry, keep the dispatch pending for the resync (2.6)
                ledger.suspendGateInternally(OrderEntryGate.GLOBAL, GateReason.ME_CAPACITY);
                break;
            default:
                staleOutcomeCount++;
                break;
        }
    }

    // ---------------------------------------------------------------- RC1 S5: AmendOrder, QuarantineOrder, queries

    /** AmendOrder (52). Contract S5 §3.4 (lifecycle B7). */
    public void applyAmend(final AmendOrderCommand c, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final AssetsEventSink sink = ledger.eventSink();
        if (!c.hasRequestId()) {
            rejectAmend(sink, c, CoreRejectReason.MISSING_REQUEST_ID, 0, now, false);
            return;
        }
        final RequestTable.Entry seen = requests.get(c.userId, c.requestIdHigh, c.requestIdLow);
        if (seen != null) {
            if (seen.fingerprint != c.fingerprint()) {
                rejectAmend(sink, c, CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH, seen.requestSeq, now, false);
                return;
            }
            if (seen.reason != 0) {
                rejectAmend(sink, c, reasonOf(seen.reason), 0, now, false);
                return;
            }
            final CoreOrder live = orders.get(seen.orderId);
            sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, seen.orderId, c.userId,
                    live == null ? 0 : live.marketId, live == null ? OrderStatus.PENDING_REPLACE : statusOf(live), true,
                    live == null ? 0 : live.holdAssetId, live == null ? 0L : live.holdAmount, now, live == null ? 0L : live.eventSeq);
            return;
        }
        if (requests.countFor(c.userId) >= RequestTable.PER_USER_CAP) {
            rejectAmend(sink, c, CoreRejectReason.TOO_MANY_PENDING_REQUESTS, requests.countFor(c.userId), now, false);
            return;
        }
        final CoreOrder o = orders.get(c.orderId);
        if (o == null) {
            rejectAmend(sink, c, CoreRejectReason.UNKNOWN_ORDER, c.orderId, now, false);
            return;
        }
        if (o.userId != c.userId) {
            rejectAmend(sink, c, CoreRejectReason.NOT_OWNER, c.orderId, now, false);
            return;
        }
        if (o.isTerminal() || o.has(CoreOrder.F_QUARANTINED) || o.has(CoreOrder.F_CANCEL_PENDING)
                || (o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CREATE)
                || (o.orderType != TYPE_LIMIT && o.orderType != TYPE_LIMIT_MAKER)) {
            rejectAmend(sink, c, CoreRejectReason.NOT_OPEN, c.orderId, now, false);
            return;
        }
        if (o.has(CoreOrder.F_REPLACE_PENDING)) {
            rejectAmend(sink, c, CoreRejectReason.AMEND_PENDING, c.orderId, now, false);
            return;
        }
        if (!ledger.gate().admits(o.marketId)) {
            rejectAmend(sink, c, CoreRejectReason.GATE_CLOSED, o.marketId, now, false);
            return;
        }
        final MarketRisk m = markets[o.marketId];
        final UserRisk u = userOrCreate(c.userId);
        u.roll(now);
        if (u.secCount >= m.maxOrdersPerSec || u.minCount >= m.maxOrdersPerMin) {
            rejectAmend(sink, c, CoreRejectReason.RATE_LIMIT, u.secCount >= m.maxOrdersPerSec ? m.maxOrdersPerSec : m.maxOrdersPerMin, now, true);
            return;
        }
        u.secCount++;
        u.minCount++;
        if (m.breakerTripped) {
            rejectAmend(sink, c, CoreRejectReason.CIRCUIT_BREAKER, o.marketId, now, true);
            return;
        }
        final long newPrice = c.newPrice == 0 ? o.price : c.newPrice;
        final long newQty = c.newQuantity == 0 ? o.quantity : c.newQuantity;
        final long legQty = newQty - o.filledQty;
        if (c.newPrice < 0 || c.newQuantity < 0 || (c.newPrice == 0 && c.newQuantity == 0) || legQty <= 0) {
            rejectAmend(sink, c, CoreRejectReason.INVALID, legQty, now, true);
            return;
        }
        if (newQty < m.minQuantity) {
            rejectAmend(sink, c, CoreRejectReason.SIZE_TOO_SMALL, m.minQuantity, now, true);
            return;
        }
        if (newQty > m.maxQuantity) {
            rejectAmend(sink, c, CoreRejectReason.SIZE_TOO_LARGE, m.maxQuantity, now, true);
            return;
        }
        if (!FixedPoint.multiplyFits(newPrice, legQty) || !FixedPoint.multiplyFits(newPrice, newQty)) {
            rejectAmend(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, newPrice, now, true);
            return;
        }
        final long legNotional = FixedPoint.multiply(newPrice, legQty);
        if ((o.side == SIDE_BUY && legNotional <= 0) || legNotional < m.minNotional) {
            rejectAmend(sink, c, CoreRejectReason.NOTIONAL_TOO_SMALL, legNotional, now, true);
            return;
        }
        if (legNotional > m.maxNotional) {
            rejectAmend(sink, c, CoreRejectReason.NOTIONAL_TOO_LARGE, legNotional, now, true);
            return;
        }
        if (m.lastTradePrice > 0 && m.priceCollarBps > 0) {
            final long dev = Math.abs(newPrice - m.lastTradePrice);
            final long limit = bps(m.lastTradePrice, m.priceCollarBps);
            if (dev > limit) {
                rejectAmend(sink, c, CoreRejectReason.PRICE_COLLAR, limit, now, true);
                return;
            }
        }
        // hold target and delta (LS:93-103, 162-174 formulas, exact fixed point)
        final long holdTarget;
        final long holdDelta;
        if (o.side == SIDE_BUY) {
            holdTarget = FixedPoint.multiply(newPrice, newQty);
            final long oldLeg = FixedPoint.multiply(o.price, o.quantity - o.filledQty);
            holdDelta = legNotional - oldLeg;
        } else {
            holdTarget = newQty;
            holdDelta = legQty - (o.quantity - o.filledQty);
        }
        final Account account = ledger.account(c.userId);
        if (holdDelta > 0) {
            final RejectReason held = account == null ? RejectReason.INSUFFICIENT_FUNDS
                    : account.hold(o.orderId, o.holdAssetId, holdDelta, false);
            if (!held.accepted()) {
                rejectAmend(sink, c, CoreRejectReason.INSUFFICIENT_FUNDS, holdDelta, now, true);
                return;
            }
            sink.onBalanceUpdate(c.userId, o.holdAssetId, account.available(o.holdAssetId), account.locked(o.holdAssetId));
        }
        // ---- accepted: window, dispatch, deadline, requestId, ack, event
        o.pendingPrice = newPrice;
        o.pendingQuantity = newQty;
        o.pendingHoldDelta = holdDelta;
        o.pendingHoldTarget = holdTarget;
        o.oldLegId = o.currentLegId;
        o.set(CoreOrder.F_REPLACE_PENDING);
        o.status = (byte) ST_PENDING_REPLACE;
        o.attempt++;
        o.pendingKind = (byte) CoreOrder.PK_AMEND;
        o.updatedAtMs = now;
        o.replaceDeadlineMs = now + REPLACE_DEADLINE_MS;
        ledger.deadlines().add(o.replaceDeadlineMs, o.orderId, DeadlineIndex.KIND_REPLACE);
        dispatch(sink, o, DispatchKind.AMEND, o.currentLegId, newPrice, legQty, 0L);
        remember(c.userId, c.requestIdHigh, c.requestIdLow, c.fingerprint(), now, RequestTable.KIND_AMEND, o.orderId, 0);
        sink.onOrderAck(c.correlationId, c.requestIdHigh, c.requestIdLow, o.orderId, c.userId, o.marketId,
                OrderStatus.PENDING_REPLACE, false, o.holdAssetId, o.holdAmount, now, o.eventSeq);
        emitEvent(o, EventKind.DISPATCHED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
    }

    private void rejectAmend(final AssetsEventSink sink, final AmendOrderCommand c, final CoreRejectReason reason, final long detail,
                             final long now, final boolean remember) {
        rejectedCount++;
        sink.onOrderReject(c.correlationId, c.requestIdHigh, c.requestIdLow, c.userId, c.orderId, 0, T_AMEND, reason, 255, detail, now);
        if (remember && c.hasRequestId()) {
            remember(c.userId, c.requestIdHigh, c.requestIdLow, c.fingerprint(), now, RequestTable.KIND_AMEND, c.orderId, reason.code());
        }
    }

    /** QuarantineOrder (74). Contract S5 §3.5 (lifecycle C14). */
    public void applyQuarantine(final long correlationId, final long orderId, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final AssetsEventSink sink = ledger.eventSink();
        final CoreOrder o = orders.get(orderId);
        if (o == null) {
            sink.onAdminAck(correlationId, T_QUARANTINE, AdminResult.REJECTED, CoreRejectReason.UNKNOWN_ORDER.code(), 0L);
            return;
        }
        if (o.has(CoreOrder.F_QUARANTINED)) {
            sink.onAdminAck(correlationId, T_QUARANTINE, AdminResult.DUPLICATE, 0, 0L);
            return;
        }
        o.set(CoreOrder.F_QUARANTINED);
        o.updatedAtMs = now;
        if (o.status == ST_PENDING_TRIGGER) {
            triggers.remove(o); // a quarantined armed order never fires
        }
        sink.onAdminAck(correlationId, T_QUARANTINE, AdminResult.ACCEPTED, 0, 0L);
        emitEvent(o, EventKind.QUARANTINED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
    }

    /** RequestOrderSnapshot (53), ORIGIN-only reply. Contract S5 §3.6. */
    public void applyRequestOrderSnapshot(final long correlationId, final long userId, final long fromOrderId,
                                          final boolean allUsersAllowed) {
        final AssetsEventSink sink = ledger.eventSink();
        if (userId == 0 && !allUsersAllowed) {
            sink.onOrderReject(correlationId, 0L, 0L, userId, 0L, 0, T_ORDER_SNAPSHOT, CoreRejectReason.INVALID, 255, 0L,
                    ledger.getLastLogTime());
            return;
        }
        final int[] n = {0};
        final long[] lastId = {0L};
        final boolean[] more = {false};
        orders.forEachByOrderId(o -> {
            if (o.orderId <= fromOrderId || (userId != 0 && o.userId != userId)) {
                return;
            }
            if (n[0] == ORDER_SNAPSHOT_PAGE) {
                more[0] = true;
                return;
            }
            snapshotPage[n[0]++] = o;
            lastId[0] = o.orderId;
        });
        if (n[0] > 0) {
            sink.onOrderSnapshotBatch(correlationId, snapshotPage, n[0]);
        }
        sink.onOrderSnapshotEnd(correlationId, n[0], more[0] ? lastId[0] : 0L, ledger.getLogPosition());
        Arrays.fill(snapshotPage, 0, n[0], null);
    }

    /** RequestPendingDispatch (65), ORIGIN-only reply. Contract S5 §3.6. */
    public void applyRequestPendingDispatch(final long correlationId, final long fromDispatchSeq, final int maxEntries) {
        final AssetsEventSink sink = ledger.eventSink();
        final int max = maxEntries <= 0 ? PENDING_DISPATCH_DEFAULT : Math.min(maxEntries, PENDING_DISPATCH_CAP);
        // a resync is rare and leader-only in effect: an allocation per call is acceptable (not a hot path)
        final Long2LongHashMap seqToOrder = new Long2LongHashMap(-1L);
        orders.forEachByOrderId(o -> {
            if (o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingDispatchSeq > fromDispatchSeq) {
                seqToOrder.put(o.pendingDispatchSeq, o.orderId);
            }
        });
        final long[] seqs = new long[seqToOrder.size()];
        int i = 0;
        for (final long seq : seqToOrder.keySet()) {
            seqs[i++] = seq;
        }
        Arrays.sort(seqs);
        final int count = Math.min(max, seqs.length);
        for (int k = 0; k < count; k++) {
            final CoreOrder o = orders.get(seqToOrder.get(seqs[k]));
            final DispatchKind kind;
            long oldLeg = 0L, price = 0L, qty = 0L, budget = 0L;
            switch (o.pendingKind) {
                case CoreOrder.PK_CANCEL:
                    kind = DispatchKind.CANCEL;
                    oldLeg = o.currentLegId;
                    break;
                case CoreOrder.PK_AMEND:
                    kind = DispatchKind.AMEND;
                    oldLeg = o.oldLegId;
                    price = o.pendingPrice;
                    qty = o.pendingQuantity - o.filledQty;
                    break;
                default:
                    kind = DispatchKind.CREATE;
                    price = meOrderType(o) == TYPE_MARKET ? 0L : o.price;
                    qty = createQuantity(o);
                    budget = o.budget;
                    break;
            }
            sink.onMeDispatchReply(o.pendingDispatchSeq, o.orderId, ((long) o.attempt << 2) | kind.code(), kind, o.userId, o.orderId,
                    oldLeg, o.marketId, meOrderType(o), o.side, price, qty, budget, o.timeInForce);
        }
        sink.onPendingDispatchEnd(correlationId, count, ledger.getDispatchSeq());
    }

    public long invalidTickCount() { return invalidTickCount; }
    public long triggeredCount() { return triggeredCount; }
    public long sliceCount() { return sliceCount; }
    public int armedStopCount(final int marketId) { return triggers.stopCount(marketId); }
    public int armedTrailingCount(final int marketId) { return triggers.trailingCount(marketId); }
    public long staleTerminalCount() { return staleTerminalCount; }
    public long staleOutcomeCount() { return staleOutcomeCount; }
    public long redundantOutcomeCount() { return redundantOutcomeCount; }
    public long implicitAckCount() { return implicitAckCount; }
    public long conflictCount() { return conflictCount; }
    public long cancelRefusedCount() { return cancelRefusedCount; }
    public long replaceSlowCount() { return replaceSlowCount; }
    public long tradeGapCount() { return tradeGapCount; }
    public long orderFaultCount() { return orderFaultCount; }

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
        if (o.replaceDeadlineMs > 0 && o.has(CoreOrder.F_REPLACE_PENDING)) {
            ledger.deadlines().add(o.replaceDeadlineMs, o.orderId, DeadlineIndex.KIND_REPLACE);
        }
        if (o.status == ST_PENDING_TRIGGER && !o.has(CoreOrder.F_QUARANTINED)) {
            triggers.add(o); // derived from the record (S6 section 2)
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

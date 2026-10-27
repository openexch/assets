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
import com.openexchange.assets.domain.ReleaseResult;
import com.openexchange.assets.domain.ReleaseScope;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.domain.commands.SettleCommand;
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
    private static final int T_PENDING_DISPATCH = 65;
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
    private static final int RESULT_RELEASED = 7;
    private static final int RESULT_INVALID = 8;
    private static final int RESULT_LEG_LIVE_RELEASE = 2; // ReleaseResult.LEG_LIVE
    /** RC2 S8 3.1 rule 1: the deferred UP_TO list (capacity 8); a fifth entry in one apply is a bug (IllegalStateException). */
    private static final int DEFERRED_CAPACITY = 8;
    private static final int DEFERRED_LIMIT = 4;
    /** {@link AssetsEventSink#onReleaseEffect} row codes (S8 3.2): C1..C6, E1..E4, abandonment. */
    public static final int ROW_C1 = 1, ROW_C2 = 2, ROW_C3 = 3, ROW_C4 = 4, ROW_C5 = 5, ROW_C6 = 6;
    public static final int ROW_E1 = 11, ROW_E2 = 12, ROW_E3 = 13, ROW_E4 = 14, ROW_ABANDON = 21;

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
    // RC2 S7 (node-local)
    private long legRegressionCount;
    private long oldCancelledIgnoredCount;
    /**
     * RC2 S7 D9b: the ME reason code of the feed entry being applied (255 = none). Set by the 63/64 v3 entry, consumed once
     * by {@link #terminalize} or {@link #abortReplace} in that apply, reset in a finally. Transient, never replicated.
     */
    private int pendingMeReason = NOT_ME_SOURCED;
    private static final int NOT_ME_SOURCED = 255;

    // ---- RC2 S8: the ME link (replicated, section 11), the derived index and the deferred release list ----
    /** S8 1.2: the release backlog (section 11). */
    private final ReleaseBacklog backlog = new ReleaseBacklog();
    /** S8 3.8: the pending-dispatch index (derived; rebuilt by {@link #rebuildDerived}). Behaviour-independent. */
    private final PendingDispatchIndex index = new PendingDispatchIndex();
    /** S8 1.3: the last applied {@code ledgerSize}, clamped to {@code [0, meLedgerCapacity]} at apply (section 11). */
    private long meLedgerSize;
    /** S8 3.5: the Schmitt-trigger state of the capacity gate (section 11). */
    private boolean capacityHigh;
    /** S8 3.1 rule 1: UP_TO releases recorded in this apply, flushed by {@link #endApply}. Transient. */
    private final long[] deferredOrder = new long[DEFERRED_CAPACITY];
    private final long[] deferredFloor = new long[DEFERRED_CAPACITY];
    private int deferredCount;
    private final MeOutcomeView defaultOutcome = new MeOutcomeView();
    /**
     * Review Q1 option (d), transient: the 64 being applied is the one that set or kept F_ME_RELEASED on a pending CREATE
     * (Table A rows 7 and 8); only a terminal reached from it skips M11. Reset after every 64.
     */
    private boolean skipOrderRelease;
    /**
     * Review NOTE-5, node-local: orphan cancels whose last outcome was CAPACITY (E4); counted in {@link #capacityParked} so
     * MeCapacityParked fires instead of DispatchStuck. Not snapshotted: after a restore it is empty until the orphan's next
     * outcome (the forwarder's resync re-offers it either way).
     */
    private final org.agrona.collections.LongHashSet orphanCapacityWait = new org.agrona.collections.LongHashSet();

    // S8 1.6 node-local counters
    private long releaseMintedUpToCount;
    private long releaseMintedOrderCount;
    private long releaseConfirmedCount;
    private long releaseConfirmStaleCount;
    private long releaseAbandonedCount;
    private long releaseSkippedCount;
    private long legLiveCount;
    private long legLiveDuplicateCount;
    private long orphanCancelCount;
    private long parkedCount;
    private long releasedLateCount;
    private long meInvalidCount;
    private long meReleaseEarlyCount;
    private long meAmendMismatchCount;
    private long capacityWhileOpenCount;
    private long capacityOutcomeCount;
    private long meLinkMismatchCount;
    private long meLedgerSizeClampedCount;
    private long lateApplyOnDeadOrderCount;
    private long legZeroRefusedCount;
    private long terminalWithPendingCreateCount;
    private long restoreCapacityBitWithoutHighCount;
    private long pendingReplyVisited;
    // S8 1.6 maintained gauges (plain longs written on the service thread at each transition)
    private long capacityParked;
    private long quarantinedLive;
    private long pendingOldestSeq;
    private long oldestReleaseSeq;

    public OrderEngine(final AssetsEngine ledger) {
        this.ledger = ledger;
        for (int m = 0; m < markets.length; m++) {
            markets[m] = new MarketRisk(m);
        }
    }

    // ---------------------------------------------------------------- CreateOrder

    public void applyCreate(final CreateOrderCommand c, final long timestamp) {
        createBody(c, timestamp);
        endApply();
    }

    private void createBody(final CreateOrderCommand c, final long timestamp) {
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
        if (v3() ? unsupportedTifV3(c.orderType, c.timeInForce)
                : (c.timeInForce == TIF_FOK || (c.orderType >= TYPE_STOP_LOSS && c.timeInForce == TIF_IOC))) {
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
        // 0a. behaviour v2 (RC1 S7 section 5): tick, lot and price band from the recorded CoreConfig; structural
        final CoreRejectReason shape = behaviourShape(c.marketId, c.price, c.stopPrice, c.quantity);
        if (shape != null) {
            reject(sink, c, shape, c.price, now, false);
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
        if (c.timeInForce == TIF_FOK) {
            o.set(CoreOrder.F_FOK); // RC2 S8 3.6: native FOK, a plain LIMIT create only (admission above)
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

    /**
     * RC2 S8 3.6 admission table under v3: IOC native on LIMIT and MARKET, FOK native on LIMIT only, every other IOC/FOK
     * cell UNSUPPORTED_TIF (LIMIT_MAKER IOC is new; stops, trailing and iceberg keep GTC/GTD only).
     */
    private static boolean unsupportedTifV3(final int orderType, final int tif) {
        if (tif == TIF_FOK) {
            return orderType != TYPE_LIMIT;
        }
        if (tif == TIF_IOC) {
            return orderType != TYPE_LIMIT && orderType != TYPE_MARKET;
        }
        return false;
    }

    // ---------------------------------------------------------------- RC1 S7: behaviour v2 admission shape

    /**
     * Behaviour v2 (S7 contract section 5): with a recorded CoreConfig, a priced order must sit on the tick and inside
     * [minPrice, maxPrice], a stop price on the tick, a quantity on the lot (lotSize > 0). v1 checks nothing. One field
     * read on the hot path; the strategy is the version itself.
     */
    private CoreRejectReason behaviourShape(final int marketId, final long price, final long stopPrice, final long quantity) {
        if (ledger.coreConfig().behaviorVersion() < 2) {
            return null;
        }
        final com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef d = ledger.coreConfig().market(marketId);
        if (d == null) {
            return null;
        }
        if (price > 0) {
            if (price < d.minPrice() || price > d.maxPrice()) {
                return CoreRejectReason.PRICE_OUT_OF_BAND;
            }
            if (price % d.tickSize() != 0) {
                return CoreRejectReason.PRICE_OFF_TICK;
            }
        }
        if (stopPrice > 0 && stopPrice % d.tickSize() != 0) {
            return CoreRejectReason.PRICE_OFF_TICK;
        }
        if (d.lotSize() > 0 && quantity % d.lotSize() != 0) {
            return CoreRejectReason.QTY_OFF_LOT;
        }
        return null;
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
        if (o.pendingDispatchSeq != 0) {
            index.remove(o.pendingDispatchSeq); // defensive: one pending command per order (never reached by the live paths)
        }
        final long dispatchSeq = ledger.getDispatchSeq() + 1;
        ledger.setDispatchSeq(dispatchSeq);
        o.pendingDispatchSeq = dispatchSeq;
        o.set(CoreOrder.F_DISPATCH_PENDING);
        index.add(dispatchSeq, o.orderId, PendingDispatchIndex.TAG_TRADING);
        final long idLow = ((long) o.attempt << 2) | kind.code();
        final boolean v3 = v3();
        sink.onMeDispatch(dispatchSeq, o.orderId, idLow, kind, o.userId, o.orderId, oldLegId, o.marketId,
                meOrderType(o), o.side, price, quantity, budget, wireTif(o, kind, v3), ReleaseScope.UP_TO,
                kind == DispatchKind.AMEND && v3 ? o.pendingExpectedOldRemaining : 0L);
    }

    /**
     * RC2 S8 3.6: the wire timeInForce, mint and reply alike. Under v3 a CREATE carries GTC 0, IOC 1, FOK 2 (GTD 0) and
     * every other kind 0; under v1/v2 the record's code on every kind (the base bytes).
     */
    private static int wireTif(final CoreOrder o, final DispatchKind kind, final boolean v3) {
        if (!v3) {
            return o.timeInForce;
        }
        if (kind != DispatchKind.CREATE) {
            return 0;
        }
        return o.timeInForce == TIF_IOC ? TIF_IOC : o.timeInForce == TIF_FOK ? TIF_FOK : TIF_GTC;
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
        if (o.pendingDispatchSeq != 0) {
            index.remove(o.pendingDispatchSeq);
        }
        if (o.has(CoreOrder.F_ME_CAPACITY_WAIT)) {
            o.unset(CoreOrder.F_ME_CAPACITY_WAIT);
            capacityParked--;
        }
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
        emitEvent(o, kind, reason, lastPrice, lastQty, tradeId, now, NOT_ME_SOURCED);
    }

    private void emitEvent(final CoreOrder o, final EventKind kind, final CoreRejectReason reason, final long lastPrice,
                           final long lastQty, final long tradeId, final long now, final int meReason) {
        final long eventSeq = ledger.getEventSeq() + 1;
        ledger.setEventSeq(eventSeq);
        o.eventSeq = eventSeq;
        ledger.eventSink().onOrderEvent(eventSeq, ledger.getLogPosition(), o, kind, reason, lastPrice, lastQty, tradeId,
                holdRemaining(o), now, meReason);
    }

    /** D9b: the pending ME reason, once. */
    private int takeMeReason() {
        final int r = pendingMeReason;
        pendingMeReason = NOT_ME_SOURCED;
        return r;
    }

    /** RC2 S7: the v3 rules read the recorded behaviour, one field read per apply. */
    private boolean v3() {
        return ledger.coreConfig().behaviorVersion() >= 3;
    }

    /** 12.2: under v3 a leg may be installed only when it is newer than every leg the order has had. */
    private boolean beltRefuses(final CoreOrder o, final long legId) {
        return v3() && legId <= Math.max(o.currentLegId, o.oldLegId);
    }

    private void fault(final OrderFaultKind kind, final CoreOrder o, final long tradeId, final long amount) {
        fault(kind, o.orderId, o.userId, o.marketId, tradeId, amount);
    }

    private void fault(final OrderFaultKind kind, final long orderId, final long userId, final int marketId, final long tradeId,
                       final long amount) {
        orderFaultCount++;
        ledger.eventSink().onOrderFault(kind, orderId, userId, marketId, tradeId, amount, ledger.getLogPosition());
    }

    /**
     * The single terminal apply (contract 3.2): release whatever the order still holds, drop its deadlines and pending
     * dispatch, emit the terminal event with the record still visible, then remove the record from every index.
     */
    private void terminalize(final CoreOrder o, final int statusCode, final EventKind kind, final CoreRejectReason reason,
                             final long lastPrice, final long lastQty, final long tradeId, final long now) {
        final boolean v3 = v3();
        if (v3 && o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CREATE) {
            terminalWithPendingCreateCount++; // S8 3.1 rule 5 (R1 as an assertion): must stay 0; M11 still runs
        }
        if (o.has(CoreOrder.F_QUARANTINED)) {
            quarantinedLive--;
        }
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
        emitEvent(o, kind, reason, lastPrice, lastQty, tradeId, now, takeMeReason());
        if (statusCode == ST_CANCELLED || statusCode == ST_EXPIRED) {
            cancelledCount++;
        }
        if (v3) {
            // S8 3.1 M11: after the terminal OrderEvent, before the record leaves; one ORDER release for every terminal (an
            // armed cancel or expiry included, attempt 0), unless F_ME_RELEASED (rule 2: the floor of the order is unknown)
            // Review Q1 option (d): M11 is skipped only when the dying apply is the 64 that set or kept the flag (Table A rows
            // 7 and 8): the ME may hold a tombstone of h there, which would make the ORDER an unjournaled NOOP. Any other
            // death of a flagged order follows a journal event naming its live leg (or ending its pending command): no
            // tombstone can exist while a leg of h rests (S4 4.2 LEG_LIVE, B0), so the ORDER is APPLIED or LEG_LIVE.
            if (o.has(CoreOrder.F_ME_RELEASED) && skipOrderRelease) {
                releaseSkippedCount++;
            } else {
                mintRelease(o.orderId, ((long) o.attempt << 2) | 3, ReleaseBacklog.SCOPE_ORDER, o);
            }
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
        emitEvent(o, EventKind.REPLACED, reason, 0L, 0L, 0L, now, takeMeReason());
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
        o.pendingLegQty = 0L;
        o.pendingExpectedOldRemaining = 0L;
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
        cancelBody(c, timestamp);
        endApply();
    }

    private void cancelBody(final CancelOrderCommand c, final long timestamp) {
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
                ledger.getLastLogTime(), ledger.getLogPosition(), false, ledger.gate().reasons(marketId));
    }

    /** A forwarder top-of-book tick: state only; a bookVersion at or below the stored one is stale (K4). */
    public void applyMarketTick(final int marketId, final long bookVersion, final long bestBid, final long bestAsk,
                                final long lastTradePrice, final long lastTradeId, final long meEpoch, final long timestamp) {
        tickBody(marketId, bookVersion, bestBid, bestAsk, lastTradePrice, lastTradeId, meEpoch, timestamp);
        endApply();
    }

    private void tickBody(final int marketId, final long bookVersion, final long bestBid, final long bestAsk,
                          final long lastTradePrice, final long lastTradeId, final long meEpoch, final long timestamp) {
        ledger.observeLogTime(timestamp);
        if (v3()) {
            // RC2 S7 D6 (W17): a tick counts only from the bound ME epoch; nothing is live before the bind
            final long bound = ledger.feed().boundMeEpoch();
            if (bound == 0) {
                ledger.feed().countTickUnbound();
                return;
            }
            if (meEpoch != bound) {
                ledger.feed().countTickEpochDrop();
                return;
            }
        }
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
        deadlineBody(deadlineMs, id, kind, now);
        endApply();
    }

    private void deadlineBody(final long deadlineMs, final long id, final int kind, final long now) {
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
        final long last = ledger.getLastAppliedTradeId();
        if (tradeId <= last) {
            return; // already applied: byte-level no-op (R-C5b)
        }
        if (tradeId != last + 1) {
            tradeGapCount++;
            ledger.eventSink().onSettleRejected(tradeId, marketId, egressSeq, SettleRejectReason.TRADE_GAP, last + 1);
            return;
        }
        tradeBody(egressSeq, tradeId, marketId, takerLegId, takerUserId, makerLegId, makerUserId, price, quantity, takerIsBuy,
                takerOmsOrderId, makerOmsOrderId, timestamp);
        endApply();
    }

    /**
     * RC2 S7 3.4: one exactly-next 61 or 62 entry under behaviour v3. The tradeId must be {@code lastAppliedTradeId + 1}
     * (an exactly-next entry can never have been applied already, so {@code tradeId <= last} is refused loudly too), and
     * every SettleRejected carries {@code journalPosition = journalSeq}. Returns false on a refusal (nothing mutated).
     */
    public boolean applyTradeV3(final long journalSeq, final long egressSeq, final long tradeId, final int marketId,
                                final long takerLegId, final long takerUserId, final long makerLegId, final long makerUserId,
                                final long price, final long quantity, final boolean takerIsBuy, final long meTimestamp,
                                final long takerOmsOrderId, final long makerOmsOrderId, final long meEpoch, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long last = ledger.getLastAppliedTradeId();
        if (tradeId != last + 1) {
            tradeGapCount++;
            ledger.eventSink().onSettleRejected(tradeId, marketId, journalSeq, SettleRejectReason.TRADE_GAP, last + 1);
            return false;
        }
        final boolean applied = tradeBody(journalSeq, tradeId, marketId, takerLegId, takerUserId, makerLegId, makerUserId, price,
                quantity, takerIsBuy, takerOmsOrderId, makerOmsOrderId, timestamp);
        if (applied) {
            endApply();
        }
        return applied;
    }

    /** The S5 3.1 apply of a dense trade; {@code journalPosition} is what a settle refusal echoes. */
    private boolean tradeBody(final long journalPosition, final long tradeId, final int marketId, final long takerLegId,
                              final long takerUserId, final long makerLegId, final long makerUserId, final long price,
                              final long quantity, final boolean takerIsBuy, final long takerOmsOrderId, final long makerOmsOrderId,
                              final long timestamp) {
        final long now = ledger.getLastLogTime();
        // 1. money, for the full trade quantity, through the ledger's settle (hold-scoped, faults never silent)
        final SettleCommand sc = settleCommand;
        sc.reset();
        sc.setJournalPosition(journalPosition);
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
            return false; // refused before any mutation (SettleRejected emitted); the order accounting stays untouched too
        }
        if (marketId >= 1 && marketId <= MARKETS && tradeId > markets[marketId].lastTradeId) {
            markets[marketId].lastTradeId = tradeId; // S6 3.5: the collar reference follows settled trades
            markets[marketId].lastTradePrice = price;
        }
        // 2. the two core orders (either may be a legacy hold the core does not own)
        applyFill(takerOmsOrderId, takerLegId, takerUserId, price, quantity, tradeId, now);
        applyFill(makerOmsOrderId, makerLegId, makerUserId, price, quantity, tradeId, now);
        return true;
    }

    private void applyFill(final long omsOrderId, final long legId, final long userId, final long price, final long quantity,
                           final long tradeId, final long now) {
        final CoreOrder o = orders.get(omsOrderId);
        if (o == null) {
            return;
        }
        if (legId <= 0 && v3()) {
            // RC2 S8 3.4 (FIX-4): the leg-0 engine guard for every caller; the trade's money already stands (S5 3.1)
            legZeroRefusedCount++;
            fault(OrderFaultKind.SETTLE, o, tradeId, quantity);
            return;
        }
        if (o.userId != userId) {
            fault(OrderFaultKind.SETTLE, o, tradeId, quantity);
            return;
        }
        boolean implicitAck = false;
        boolean windowClosedByTrade = false;
        if (legId != 0 && o.currentLegId == 0 && o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CREATE) {
            if (beltRefuses(o, legId)) {
                // RC2 S7 12.2 site 1: e.g. a late trade on the just-exhausted slice; the money stands, no leg is learned
                legRegressionCount++;
                fault(OrderFaultKind.SETTLE, o, tradeId, quantity);
                return;
            }
            // implicit ack: the leg exists at the ME (contract 3.1); the post-ack rules run after the fill below
            o.unset(CoreOrder.F_ME_RELEASED); // review Q3: the ME judged this id above its floor
            deferUpTo(o, DispatchKind.CREATE.code()); // RC2 S8 M1
            o.currentLegId = legId;
            clearPending(o);
            o.status = (byte) ST_NEW;
            o.updatedAtMs = now;
            implicitAckCount++;
            implicitAck = true;
            emitEvent(o, EventKind.ACK_NEW, CoreRejectReason.NONE, 0L, 0L, 0L, now);
        } else if (legId != 0 && legId != o.currentLegId) {
            if (o.has(CoreOrder.F_REPLACE_PENDING) && o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_AMEND) {
                if (beltRefuses(o, legId)) {
                    legRegressionCount++; // RC2 S7 12.2 site 2: an older leg cannot be the replace's new leg
                    fault(OrderFaultKind.SETTLE, o, tradeId, quantity);
                    return;
                }
                // a trade on the NEW leg before the amend outcome: resolve first (B7), then fill; the window is closed
                // by this trade, so the post-window rules (3.8 #9/#13: a cancel flagged meanwhile) run after the fill
                o.unset(CoreOrder.F_ME_RELEASED); // review Q3
                deferUpTo(o, DispatchKind.AMEND.code()); // RC2 S8 M2
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
        if (v3() && (o.has(CoreOrder.F_IOC) || o.has(CoreOrder.F_FOK) || meOrderType(o) == TYPE_MARKET)) {
            return; // RC2 S8 3.6 (D-S8-6): the ME ends such an order inside its create; a cancel would only meet UNKNOWN_LEG
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

    /**
     * RC2 S7 2.4: MeJournalTerminal (63) with the ME reject code. Under v3 the code reaches the terminal event this entry
     * makes (D9b); under v1/v2 it is ignored (the RC1 signature below is this with no code).
     */
    public void applyTerminal(final long egressSeq, final long legId, final long userId, final int marketId, final int statusCode,
                              final long meTimestamp, final long omsOrderId, final long meEpoch, final long timestamp,
                              final int meReason) {
        pendingMeReason = v3() ? meReason : NOT_ME_SOURCED;
        try {
            applyTerminal(egressSeq, legId, userId, marketId, statusCode, meTimestamp, omsOrderId, meEpoch, timestamp);
        } finally {
            pendingMeReason = NOT_ME_SOURCED;
        }
    }

    /**
     * RC2 S7 2.4: MeCommandOutcome (64) with {@code oldCancelled} (D9a) and, on result 1, the ME reason code (D9b). Both
     * are v3 only; under v1/v2 this is the RC1 apply.
     */
    public void applyOutcome(final long egressSeq, final long commandIdHigh, final long commandIdLow, final long userId,
                             final long omsOrderId, final long oldOrderId, final long price, final long quantity, final long budget,
                             final int marketId, final int commandKind, final int result, final long legId, final int reason,
                             final long meTimestamp, final long meEpoch, final long timestamp, final boolean oldCancelled) {
        if (v3()) {
            // RC2 S8 section 2: every caller under v3 reaches the S8 tables; the 64 tail takes the section 2 defaults
            applyOutcomeV3(outcomeWithDefaults(defaultOutcome, egressSeq, 0L, commandIdHigh, commandIdLow, userId, omsOrderId,
                    oldOrderId, price, quantity, budget, marketId, commandKind, result, legId, reason, oldCancelled, meTimestamp,
                    meEpoch), timestamp);
            return;
        }
        outcome(egressSeq, commandIdHigh, commandIdLow, userId, omsOrderId, oldOrderId, price, quantity, budget, marketId,
                commandKind, result, legId, reason, meTimestamp, meEpoch, timestamp, false);
        endApply();
    }

    public long legRegressionCount() { return legRegressionCount; }
    public long oldCancelledIgnoredCount() { return oldCancelledIgnoredCount; }

    /** MeJournalTerminal (63). Contract S5 §3.2. */
    public void applyTerminal(final long egressSeq, final long legId, final long userId, final int marketId, final int statusCode,
                              final long meTimestamp, final long omsOrderId, final long meEpoch, final long timestamp) {
        terminalBody(legId, statusCode, omsOrderId, timestamp);
        endApply();
    }

    private void terminalBody(final long legId, final int statusCode, final long omsOrderId, final long timestamp) {
        ledger.observeLogTime(timestamp);
        final long now = ledger.getLastLogTime();
        final CoreOrder o = orders.get(omsOrderId);
        if (o == null) {
            staleTerminalCount++;
            return;
        }
        final boolean v3 = v3();
        if (v3 && legId <= 0) {
            // RC2 S8 3.4 (FIX-4): a terminal naming leg 0 changes nothing (the R1 hole: it would terminalize a pending CREATE)
            legZeroRefusedCount++;
            staleTerminalCount++;
            return;
        }
        if (o.has(CoreOrder.F_REPLACE_PENDING) && legId != 0 && legId != o.currentLegId) {
            if (beltRefuses(o, legId)) {
                legRegressionCount++; // RC2 S7 12.2 site 3: an older leg never resolves the replace
                staleTerminalCount++;
                return;
            }
            // the NEW leg speaks before the amend outcome: resolve first, then apply (B7)
            if (o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_AMEND) {
                o.unset(CoreOrder.F_ME_RELEASED); // review Q3
                deferUpTo(o, DispatchKind.AMEND.code()); // RC2 S8 M3
            }
            clearPending(o);
            resolveReplace(o, legId, now);
        }
        if (!o.has(CoreOrder.F_REPLACE_PENDING) && legId != 0 && o.currentLegId == 0 && legId != o.oldLegId
                && o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_CREATE) {
            if (beltRefuses(o, legId)) {
                legRegressionCount++; // RC2 S7 12.2 site 4 (G9, H1): no leg learned, no slice minted
                staleTerminalCount++;
                return;
            }
            // G9 (S6 3.6): the ME journals a terminal for a create it refused or ended before its outcome reached us.
            // Learn the leg and apply the terminal; the later CREATE outcome is redundant. A refused leg never rested,
            // so no ACK_NEW is announced for it.
            o.unset(CoreOrder.F_ME_RELEASED); // review Q3
            deferUpTo(o, DispatchKind.CREATE.code()); // RC2 S8 M4
            o.currentLegId = legId;
            clearPending(o);
            implicitAckCount++;
            // RC2 S8 3.4: an IOC/FOK leg the ME killed (CANCELLED) never rested: no ACK_NEW under v3
            if (statusCode != ST_REJECTED && !(v3 && statusCode == ST_CANCELLED && iocOrFok(o))) {
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
                if (o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingKind == CoreOrder.PK_AMEND) {
                    o.unset(CoreOrder.F_ME_RELEASED); // review Q3
                    deferUpTo(o, DispatchKind.AMEND.code()); // RC2 S8 M5
                }
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
                if (o.has(CoreOrder.F_EXPIRY_REQUESTED) || (v3 && iocOrFok(o))) {
                    // RC2 S8 3.4: an IOC/FOK leg the ME cancelled (reasons 12, 13, 14, 7 in meReason) ends EXPIRED
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
        applyOutcome(egressSeq, commandIdHigh, commandIdLow, userId, omsOrderId, oldOrderId, price, quantity, budget, marketId,
                commandKind, result, legId, reason, meTimestamp, meEpoch, timestamp, false);
    }

    private void outcome(final long egressSeq, final long commandIdHigh, final long commandIdLow, final long userId,
                         final long omsOrderId, final long oldOrderId, final long price, final long quantity, final long budget,
                         final int marketId, final int commandKind, final int result, final long legId, final int reason,
                         final long meTimestamp, final long meEpoch, final long timestamp, final boolean oldCancelled) {
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
        if (oldCancelled && pending != CoreOrder.PK_AMEND) {
            oldCancelledIgnoredCount++; // D9a: only an AMEND can cancel an old leg
        }
        if (result == RESULT_APPLIED && pending != CoreOrder.PK_CANCEL && beltRefuses(o, legId)) {
            // RC2 S7 12.2 sites 5 (CREATE APPLIED) and 6 (AMEND APPLIED): the pending command is left as it is
            legRegressionCount++;
            staleOutcomeCount++;
            return;
        }
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
                } else if (pending == CoreOrder.PK_AMEND && result == RESULT_REJECTED && oldCancelled) {
                    // RC2 S7 D9a row 1 (H3/X14): the ME cancelled the old leg and placed nothing. The order ends REJECTED
                    // with its true filledQty; terminalize releases the whole residual (top-up, hidden remainder included)
                    clearPending(o);
                    terminalize(o, ST_REJECTED, EventKind.REJECTED, CoreRejectReason.ME_REJECTED, 0L, 0L, 0L, now);
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
                if (!o.has(CoreOrder.F_QUARANTINED)) {
                    quarantinedLive++;
                }
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

    // ================================================================ RC2 S8: releases, Tables A-D, the 67, capacity

    private static boolean iocOrFok(final CoreOrder o) {
        return o.has(CoreOrder.F_IOC) || o.has(CoreOrder.F_FOK);
    }

    /** S8 3.1 rule 1: record the UP_TO of a resolution site (floor captured now, before clearPending); flushed by endApply. */
    private void deferUpTo(final CoreOrder o, final int kindCode) {
        if (!v3()) {
            return;
        }
        if (deferredCount == DEFERRED_LIMIT) {
            throw new IllegalStateException("S8 3.1 rule 1: a fifth deferred UP_TO in one apply (order " + o.orderId + ")");
        }
        deferredOrder[deferredCount] = o.orderId;
        deferredFloor[deferredCount] = ((long) o.attempt << 2) | kindCode;
        deferredCount++;
    }

    /**
     * The end of every mutating public apply (S8 section 2): under v3 the deferred UP_TO releases (recording order; only for
     * an order still live and without {@code F_ME_RELEASED}), then the capacity rule of 3.5; on every behaviour the
     * maintained gauges of the index (1.6). A refused apply recorded nothing, so it flushes nothing.
     */
    private void endApply() {
        if (deferredCount > 0) {
            final int n = deferredCount;
            deferredCount = 0;
            for (int i = 0; i < n; i++) {
                final CoreOrder o = orders.get(deferredOrder[i]);
                if (o != null && !o.has(CoreOrder.F_ME_RELEASED)) {
                    mintRelease(deferredOrder[i], deferredFloor[i], ReleaseBacklog.SCOPE_UP_TO, null);
                }
            }
        }
        if (v3()) {
            capacityRule();
        }
        refreshGauges();
    }

    private void refreshGauges() {
        final long trading = index.headSeq(PendingDispatchIndex.TAG_TRADING);
        final long orphan = index.headSeq(PendingDispatchIndex.TAG_ORPHAN);
        pendingOldestSeq = trading == 0 ? orphan : orphan == 0 ? trading : Math.min(trading, orphan);
        oldestReleaseSeq = index.headSeq(PendingDispatchIndex.TAG_RELEASE);
    }

    /**
     * S8 3.1: one release mint. It advances the replicated dispatchSeq, indexes the seq (tag RELEASE), writes the backlog
     * entry (supersede: the old seq leaves the index; ORDER replaces UP_TO, an UP_TO never replaces ORDER) and emits the
     * frozen S1 4.4 RELEASE_ID mapping. It touches no CoreOrder ({@code dying} is only read, for the M11 identity).
     */
    private long mintRelease(final long h, final long floor, final int scope, final CoreOrder dying) {
        ReleaseBacklog.Entry b = backlog.get(h);
        if (b != null) {
            if (b.scope == ReleaseBacklog.SCOPE_ORDER && scope == ReleaseBacklog.SCOPE_UP_TO) {
                releaseSkippedCount++; // unreachable (UP_TO flushes only for a live order, ORDER exists only for a dead one)
                return 0L;
            }
            if (b.stage != ReleaseBacklog.STAGE_PARKED) {
                index.remove(b.dispatchSeq);
            }
            if (b.stage != ReleaseBacklog.STAGE_RELEASE) {
                backlog.setStage(b, ReleaseBacklog.STAGE_RELEASE);
            }
        } else {
            b = backlog.insert(h);
        }
        final long seq = ledger.getDispatchSeq() + 1;
        ledger.setDispatchSeq(seq);
        index.add(seq, h, PendingDispatchIndex.TAG_RELEASE);
        b.floor = floor;
        b.dispatchSeq = seq;
        b.scope = scope;
        if (dying != null) {
            b.userId = dying.userId;
            b.marketId = dying.marketId;
            b.side = dying.side;
            b.meOrderType = meOrderType(dying);
            b.legId = dying.currentLegId != 0 ? dying.currentLegId : dying.oldLegId;
            b.attempt = dying.attempt;
            b.orphanTries = 0;
        }
        if (scope == ReleaseBacklog.SCOPE_ORDER) {
            releaseMintedOrderCount++;
        } else {
            releaseMintedUpToCount++;
        }
        ledger.eventSink().onMeDispatch(seq, h, floor, DispatchKind.RELEASE_ID, 0L, 0L, 0L, 0, 0, 0, 0L, 0L, 0L, 0, scopeOf(scope), 0L);
        return seq;
    }

    /** S8 3.2 C4: the orphan cancel of {@code b.legId} for {@code b.attempt}; its seq replaces the release in the index. */
    private long mintOrphanCancel(final ReleaseBacklog.Entry b) {
        index.remove(b.dispatchSeq);
        final long seq = ledger.getDispatchSeq() + 1;
        ledger.setDispatchSeq(seq);
        index.add(seq, b.orderId, PendingDispatchIndex.TAG_ORPHAN);
        b.dispatchSeq = seq;
        orphanCancelCount++;
        ledger.eventSink().onMeDispatch(seq, b.orderId, ((long) b.attempt << 2) | DispatchKind.CANCEL.code(), DispatchKind.CANCEL,
                b.userId, b.orderId, b.legId, b.marketId, b.meOrderType, b.side, 0L, 0L, 0L, 0, ReleaseScope.UP_TO, 0L);
        return seq;
    }

    /** S8 3.1 rule 2 (BLOCKER-2): set F_ME_RELEASED and abandon the order's backlog entry in the same apply. */
    private void markReleased(final CoreOrder o) {
        o.set(CoreOrder.F_ME_RELEASED);
        final ReleaseBacklog.Entry b = backlog.get(o.orderId);
        if (b != null) {
            if (b.stage != ReleaseBacklog.STAGE_PARKED) {
                index.remove(b.dispatchSeq);
            }
            final long floor = b.floor;
            final int scope = b.scope;
            backlog.remove(o.orderId);
            releaseAbandonedCount++;
            ledger.eventSink().onReleaseEffect(o.orderId, floor, scopeOf(scope), null, ROW_ABANDON);
        }
    }

    private void clearOrphanWait(final long h) {
        if (orphanCapacityWait.remove(h)) {
            capacityParked--;
        }
    }

    /** Drop {@code b} (a dominating confirmation, C1/C2). */
    private void dropEntry(final ReleaseBacklog.Entry b) {
        clearOrphanWait(b.orderId);
        if (b.stage != ReleaseBacklog.STAGE_PARKED) {
            index.remove(b.dispatchSeq);
        }
        backlog.remove(b.orderId);
    }

    private void park(final ReleaseBacklog.Entry b) {
        index.remove(b.dispatchSeq);
        backlog.setStage(b, ReleaseBacklog.STAGE_PARKED);
        parkedCount++;
    }

    private boolean globalHas(final GateReason r) {
        return (ledger.gate().reasons(OrderEntryGate.GLOBAL) & (1 << r.code())) != 0;
    }

    /** S8 3.5: the pending command count {@code P} (index entries of the trading and orphan tags). */
    public long pendingCommands() {
        return index.live(PendingDispatchIndex.TAG_TRADING) + index.live(PendingDispatchIndex.TAG_ORPHAN);
    }

    /** S8 3.5 Schmitt trigger, integer only; unbound or {@code C == 0} never gates (W6). */
    private void capacityRule() {
        final long c = ledger.feed().meLedgerCapacity();
        if (ledger.feed().boundMeEpoch() == 0 || c < 1) {
            return;
        }
        final long e = meLedgerSize + pendingCommands();
        if (!capacityHigh && 5 * e >= 4 * c) {
            capacityHigh = true;
            ledger.suspendGateInternally(OrderEntryGate.GLOBAL, GateReason.ME_CAPACITY);
        } else if (capacityHigh && 5 * e <= 3 * c) {
            capacityHigh = false;
            ledger.clearGateInternally(OrderEntryGate.GLOBAL, GateReason.ME_CAPACITY);
        }
    }

    /** S8 3.5 CAPACITY outcome rule (Tables A-C row 3, E4); {@code L = C} was already set by step 0. */
    private void capacityOutcome(final long orderId, final long userId, final int marketId) {
        capacityOutcomeCount++;
        if (!globalHas(GateReason.ME_CAPACITY)) {
            capacityWhileOpenCount++;
            fault(OrderFaultKind.ME_CAPACITY_WHILE_OPEN, orderId, userId, marketId, 0L, meLedgerSize + pendingCommands());
        }
        ledger.suspendGateInternally(OrderEntryGate.GLOBAL, GateReason.ME_CAPACITY);
        capacityHigh = true;
    }

    /** Step 0 of 3.3 and the root of 3.2 (BLOCKER-1, FIX-3): decided at apply, never left to the restore. */
    private void linkCheck(final long ledgerSize, final long ledgerCapacity, final boolean behaviourMismatch,
                           final boolean capacityResult) {
        final long c = ledger.feed().meLedgerCapacity();
        final boolean clamped = ledgerSize > c;
        // ledgerSize < 0 cannot pass the 3.9 decode; an engine-API caller that sends one is clamped to 0 and named (P1)
        final boolean mismatch = ledgerCapacity != c || behaviourMismatch || clamped || ledgerSize < 0
                || (capacityResult && ledgerSize != ledgerCapacity);
        meLedgerSize = capacityResult ? c : Math.max(0L, Math.min(ledgerSize, c));
        if (mismatch) {
            meLinkMismatchCount++;
            if (clamped) {
                meLedgerSizeClampedCount++;
            }
            ledger.suspendGateInternally(OrderEntryGate.GLOBAL, GateReason.ME_BEHAVIOR);
        }
    }

    private void gateMarket(final int marketId) {
        ledger.suspendGateInternally(marketId, GateReason.ME_LEG_LIVE);
    }

    private void setCapacityWait(final CoreOrder o) {
        if (!o.has(CoreOrder.F_ME_CAPACITY_WAIT)) {
            o.set(CoreOrder.F_ME_CAPACITY_WAIT);
            capacityParked++;
        }
    }

    private void quarantine(final CoreOrder o, final long now) {
        clearPending(o);
        if (!o.has(CoreOrder.F_QUARANTINED)) {
            quarantinedLive++;
        }
        o.set(CoreOrder.F_QUARANTINED);
        o.rejectReason = (byte) CoreRejectReason.ME_CONFLICT.code();
        o.updatedAtMs = now;
        conflictCount++;
        emitEvent(o, EventKind.QUARANTINED, CoreRejectReason.ME_CONFLICT, 0L, 0L, 0L, now);
    }

    /**
     * MeCommandOutcome 64 under behaviour v3 (S8 3.3): step 0 (link check and clamp), the RC1 match, the S7 belt, then Tables
     * A (pending CREATE), B (pending CANCEL), C (pending AMEND) or D (unmatched). The engine guard for every caller.
     */
    public void applyOutcomeV3(final MeOutcomeView v, final long timestamp) {
        ledger.observeLogTime(timestamp);
        // S7 11.2 for result 1, and the S8 3.3 rows 7 and 8 (RELEASED and INVALID carry the 64's reason, 0 on the wire)
        pendingMeReason = (v.result == RESULT_REJECTED || v.result == RESULT_RELEASED || v.result == RESULT_INVALID)
                && v.reason >= 0 && v.reason <= 254 ? v.reason : NOT_ME_SOURCED;
        try {
            outcomeV3(v, ledger.getLastLogTime());
        } finally {
            pendingMeReason = NOT_ME_SOURCED;
            skipOrderRelease = false;
        }
        endApply();
    }

    private void outcomeV3(final MeOutcomeView v, final long now) {
        linkCheck(v.ledgerSize, v.ledgerCapacity, v.behaviorVersion != ledger.feed().meBehaviorVersion(),
                v.result == RESULT_CAPACITY);
        final long h = v.commandIdHigh;
        final CoreOrder o = orders.get(h);
        if (o == null || !o.has(CoreOrder.F_DISPATCH_PENDING)
                || v.commandIdLow != (((long) o.attempt << 2) | kindCodeOf(o.pendingKind))) {
            unmatchedV3(v, o, now);
            return;
        }
        final int pending = o.pendingKind;
        final long legId = v.orderId;
        if (v.oldCancelled && pending != CoreOrder.PK_AMEND) {
            oldCancelledIgnoredCount++; // D9a: only an AMEND can cancel an old leg
        }
        if (v.result == RESULT_APPLIED && pending != CoreOrder.PK_CANCEL && beltRefuses(o, legId)) {
            // RC2 S7 12.2 sites 5 and 6 (legId <= 0 included: max(cur, old) >= 0): the pending command is left as it is
            legRegressionCount++;
            staleOutcomeCount++;
            return;
        }
        if (v.result <= RESULT_UNKNOWN_MARKET) {
            o.unset(CoreOrder.F_ME_RELEASED); // 3.1 rule 2 (review 2 FIX-1): the ME judged this id above its coverage
        }
        switch (pending) {
            case CoreOrder.PK_CREATE: outcomeCreate(o, v, legId, now); break;
            case CoreOrder.PK_CANCEL: outcomeCancel(o, v, now); break;
            default: outcomeAmend(o, v, legId, now); break;
        }
    }

    /** Table A, pending CREATE. */
    private void outcomeCreate(final CoreOrder o, final MeOutcomeView v, final long legId, final long now) {
        switch (v.result) {
            case RESULT_APPLIED:
                deferUpTo(o, DispatchKind.CREATE.code()); // M6
                o.currentLegId = legId;
                clearPending(o);
                if (o.status == ST_PENDING_NEW) {
                    o.status = (byte) ST_NEW;
                }
                o.updatedAtMs = now;
                emitEvent(o, EventKind.ACK_NEW, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                afterAck(o, now);
                break;
            case RESULT_REJECTED:
            case RESULT_UNKNOWN_LEG:
            case RESULT_NOT_OWNER:
            case RESULT_UNKNOWN_MARKET:
                clearPending(o);
                terminalize(o, ST_REJECTED, EventKind.REJECTED, meWhy(v.result), 0L, 0L, 0L, now); // M11
                break;
            case RESULT_CONFLICT:
                quarantine(o, now);
                break;
            case RESULT_CAPACITY:
                setCapacityWait(o);
                capacityOutcome(o.orderId, o.userId, o.marketId);
                break;
            case RESULT_RELEASED:
                // D-S8-2: no leg exists (the dense cursor would have resolved an applied original first); no ORDER (rule 2)
                meReleaseEarlyCount++;
                fault(OrderFaultKind.ME_RELEASE_EARLY, o, 0L, v.commandIdLow);
                markReleased(o);
                clearPending(o);
                skipOrderRelease = true; // Table A row 7: the 64 that set the flag kills the order
                terminalize(o, ST_REJECTED, EventKind.REJECTED, CoreRejectReason.ME_RELEASED, 0L, 0L, 0L, now);
                break;
            default: // RESULT_INVALID (W8)
                meInvalidCount++;
                fault(OrderFaultKind.ME_INVALID, o, 0L, v.commandIdLow);
                clearPending(o);
                skipOrderRelease = o.has(CoreOrder.F_ME_RELEASED); // Table A row 8 on a flagged order: the 64 kept the flag
                terminalize(o, ST_REJECTED, EventKind.REJECTED, CoreRejectReason.ME_INVALID, 0L, 0L, 0L, now); // M11
                break;
        }
    }

    /** Table B, pending CANCEL. */
    private void outcomeCancel(final CoreOrder o, final MeOutcomeView v, final long now) {
        switch (v.result) {
            case RESULT_APPLIED:
                clearPending(o);
                if (o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
                    terminalize(o, ST_EXPIRED, EventKind.EXPIRED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                } else {
                    terminalize(o, ST_CANCELLED, EventKind.CANCELLED, CoreRejectReason.NONE, 0L, 0L, 0L, now);
                }
                break;
            case RESULT_REJECTED:
            case RESULT_UNKNOWN_LEG:
            case RESULT_NOT_OWNER:
            case RESULT_UNKNOWN_MARKET:
                deferUpTo(o, DispatchKind.CANCEL.code()); // M8
                clearPending(o); // the flags stay (RC1)
                cancelRefusedCount++;
                break;
            case RESULT_CONFLICT:
                quarantine(o, now);
                break;
            case RESULT_CAPACITY:
                setCapacityWait(o);
                capacityOutcome(o.orderId, o.userId, o.marketId);
                break;
            case RESULT_RELEASED:
                meReleaseEarlyCount++;
                fault(OrderFaultKind.ME_RELEASE_EARLY, o, 0L, v.commandIdLow);
                markReleased(o);
                clearPending(o); // flags stay, the order stays live on its resting leg; no automatic re-mint
                gateMarket(o.marketId);
                break;
            default: // RESULT_INVALID
                meInvalidCount++;
                fault(OrderFaultKind.ME_INVALID, o, 0L, v.commandIdLow);
                deferUpTo(o, DispatchKind.CANCEL.code()); // M10
                clearPending(o);
                gateMarket(o.marketId);
                break;
        }
    }

    /** Table C, pending AMEND. */
    private void outcomeAmend(final CoreOrder o, final MeOutcomeView v, final long legId, final long now) {
        switch (v.result) {
            case RESULT_APPLIED:
                deferUpTo(o, DispatchKind.AMEND.code()); // M7 (M11 instead if afterWindow terminalizes)
                clearPending(o);
                resolveReplace(o, legId, now);
                afterWindow(o, now);
                break;
            case RESULT_REJECTED:
                if (v.oldCancelled) {
                    // S7 D9a row 1: the ME cancelled the old leg and placed nothing; terminalize releases the whole residual
                    clearPending(o);
                    terminalize(o, ST_REJECTED, EventKind.REJECTED, CoreRejectReason.ME_REJECTED, 0L, 0L, 0L, now); // M11
                    break;
                }
                if (v.reason == 16) {
                    // N-4: the fault first, it reads the reference closeReplaceWindow zeroes
                    meAmendMismatchCount++;
                    fault(OrderFaultKind.ME_AMEND_MISMATCH, o, 0L, o.pendingExpectedOldRemaining);
                }
                deferUpTo(o, DispatchKind.AMEND.code()); // M9
                clearPending(o);
                abortReplace(o, CoreRejectReason.ME_REJECTED, now);
                afterWindow(o, now);
                break;
            case RESULT_UNKNOWN_LEG:
            case RESULT_NOT_OWNER:
            case RESULT_UNKNOWN_MARKET:
                deferUpTo(o, DispatchKind.AMEND.code()); // M9
                clearPending(o);
                abortReplace(o, meWhy(v.result), now);
                afterWindow(o, now);
                break;
            case RESULT_CONFLICT:
                quarantine(o, now); // RC1: the window stays open (finding F-3)
                break;
            case RESULT_CAPACITY:
                setCapacityWait(o);
                capacityOutcome(o.orderId, o.userId, o.marketId);
                break;
            case RESULT_RELEASED:
                meReleaseEarlyCount++;
                fault(OrderFaultKind.ME_RELEASE_EARLY, o, 0L, v.commandIdLow);
                markReleased(o);
                clearPending(o);
                gateMarket(o.marketId);
                abortReplace(o, CoreRejectReason.ME_RELEASED, now); // live on the old leg with its old hold
                afterWindow(o, now);
                break;
            default: // RESULT_INVALID
                meInvalidCount++;
                fault(OrderFaultKind.ME_INVALID, o, 0L, v.commandIdLow);
                gateMarket(o.marketId);
                deferUpTo(o, DispatchKind.AMEND.code()); // M10
                clearPending(o);
                abortReplace(o, CoreRejectReason.ME_INVALID, now);
                afterWindow(o, now);
                break;
        }
    }

    private static CoreRejectReason meWhy(final int result) {
        return result == RESULT_REJECTED ? CoreRejectReason.ME_REJECTED
                : result == RESULT_UNKNOWN_LEG ? CoreRejectReason.ME_UNKNOWN_LEG
                : result == RESULT_NOT_OWNER ? CoreRejectReason.NOT_OWNER : CoreRejectReason.UNKNOWN_MARKET;
    }

    /** Table D, an unmatched outcome (first matching row). */
    private void unmatchedV3(final MeOutcomeView v, final CoreOrder o, final long now) {
        final long h = v.commandIdHigh;
        final boolean dead = o == null && h >= 1 && h < ledger.getNextOrderId();
        final ReleaseBacklog.Entry b = dead ? backlog.get(h) : null;
        final long legId = v.orderId;
        final boolean lateApply = v.result == RESULT_APPLIED
                && (v.commandKind == DispatchKind.CREATE.code() || v.commandKind == DispatchKind.AMEND.code());
        if (b != null && b.stage == ReleaseBacklog.STAGE_ORPHAN_CANCEL
                && v.commandIdLow == (((long) b.attempt << 2) | DispatchKind.CANCEL.code())) {
            orphanOutcome(b, v); // U0, first (FIX-1)
            return;
        }
        if (v.result == RESULT_RELEASED) {
            releasedLateCount++; // U1
            return;
        }
        if (o != null) {
            if (v.result == RESULT_APPLIED && legId != 0 && legId == o.currentLegId) {
                redundantOutcomeCount++; // U2
            } else {
                staleOutcomeCount++; // U3
            }
            return;
        }
        if (dead && b != null && b.scope == ReleaseBacklog.SCOPE_ORDER && lateApply && legId > b.legId) {
            // U5 (S2 handoff): a late copy applied after the core ended the order; never a second release
            lateApplyOnDeadOrderCount++;
            fault(OrderFaultKind.ME_LEG_LIVE, h, b.userId, b.marketId, 0L, legId);
            gateMarket(b.marketId);
            if (b.stage == ReleaseBacklog.STAGE_RELEASE) {
                b.legId = legId; // the coming LEG_LIVE (C4) cancels this leg; the minted orphan frame stays frozen (FIX-2)
            }
            return;
        }
        if (dead && b == null && lateApply && legId > 0) {
            // U5b (review 2 FIX-2): no backlog entry (abandoned or confirmed); cannot come from a correct ME
            lateApplyOnDeadOrderCount++;
            fault(OrderFaultKind.ME_LEG_LIVE, h, v.userId, v.marketId, 0L, legId);
            if (v.marketId >= 1 && v.marketId <= MARKETS) {
                gateMarket(v.marketId);
            } else {
                ledger.suspendGateInternally(OrderEntryGate.GLOBAL, GateReason.ME_BEHAVIOR);
            }
            return;
        }
        if (dead && b != null && v.result == RESULT_APPLIED && b.scope == ReleaseBacklog.SCOPE_ORDER && legId <= b.legId) {
            redundantOutcomeCount++; // U6: the outcome after the ME's own terminal (IOC and G9 shapes)
            return;
        }
        staleOutcomeCount++; // U7
    }

    /** Table D row U0: the outcome of an orphan cancel (rows E1-E4). */
    private void orphanOutcome(final ReleaseBacklog.Entry b, final MeOutcomeView v) {
        if (v.result != RESULT_CAPACITY) {
            clearOrphanWait(b.orderId);
        }
        switch (v.result) {
            case RESULT_APPLIED:
            case RESULT_REJECTED:
            case RESULT_UNKNOWN_LEG:
            case RESULT_NOT_OWNER:
            case RESULT_UNKNOWN_MARKET: {
                // E1 / E2: the leg was cancelled (its CANCELLED terminal preceded, stale) or is not live: M12 ORDER
                final int row = v.result == RESULT_APPLIED ? ROW_E1 : ROW_E2;
                mintRelease(b.orderId, ((long) b.attempt << 2) | 3, ReleaseBacklog.SCOPE_ORDER, null);
                b.orphanTries = 1;
                ledger.eventSink().onReleaseEffect(b.orderId, b.floor, ReleaseScope.ORDER, null, row);
                break;
            }
            case RESULT_CAPACITY:
                capacityOutcome(b.orderId, b.userId, b.marketId); // E4: stays, the resync re-offers it
                if (orphanCapacityWait.add(b.orderId)) {
                    capacityParked++; // review NOTE-5
                }
                ledger.eventSink().onReleaseEffect(b.orderId, b.floor, ReleaseScope.ORDER, null, ROW_E4);
                break;
            default:
                // E3 (CONFLICT, RELEASED, INVALID): parked, alarmed; the market stays gated until ADMIN OPEN (D-S8-3)
                park(b);
                fault(OrderFaultKind.ME_LEG_LIVE, b.orderId, b.userId, b.marketId, 0L, b.legId);
                ledger.eventSink().onReleaseEffect(b.orderId, b.floor, ReleaseScope.ORDER, null, ROW_E3);
                break;
        }
    }

    /**
     * MeCommandRelease 67 content under behaviour v3 (S8 3.2): the root link check and clamp, then each group entry in
     * order against the backlog, first matching row of C1-C6. Releases nothing from any hold.
     */
    public void applyReleaseV3(final MeReleaseView v, final long timestamp) {
        ledger.observeLogTime(timestamp);
        linkCheck(v.ledgerSize, v.ledgerCapacity, false, false);
        for (int i = 0; i < v.count; i++) {
            releaseEntry(v.commandIdHigh[i], v.idLowFloor[i], v.scope[i], v.result[i]);
        }
        endApply();
    }

    private void releaseEntry(final long h, final long f, final int sc, final int r) {
        final ReleaseBacklog.Entry b = backlog.get(h);
        final ReleaseScope scope = scopeOf(sc);
        final ReleaseResult result = ReleaseResult.fromCode(r);
        int row;
        if (r != RESULT_LEG_LIVE_RELEASE) {
            if (sc == ReleaseBacklog.SCOPE_ORDER && b != null) {
                dropEntry(b); // C1: ORDER APPLIED proves no leg rests, NOOP that h was tombstoned
                releaseConfirmedCount++;
                row = ROW_C1;
            } else if (sc == ReleaseBacklog.SCOPE_UP_TO && b != null && b.scope == ReleaseBacklog.SCOPE_UP_TO && f >= b.floor) {
                dropEntry(b); // C2: S4 NOTE-9 dominance
                releaseConfirmedCount++;
                row = ROW_C2;
            } else {
                releaseConfirmStaleCount++; // C3
                row = ROW_C3;
            }
        } else if (b != null && b.scope == ReleaseBacklog.SCOPE_ORDER && b.stage == ReleaseBacklog.STAGE_RELEASE && f == b.floor) {
            // W4: a leg of h still rests at the ME
            legLiveCount++;
            fault(OrderFaultKind.ME_LEG_LIVE, h, b.userId, b.marketId, 0L, b.legId);
            gateMarket(b.marketId);
            if (b.orphanTries == 0) {
                if (b.legId == 0) {
                    park(b); // C4 with no known leg
                } else {
                    b.attempt++;
                    backlog.setStage(b, ReleaseBacklog.STAGE_ORPHAN_CANCEL);
                    mintOrphanCancel(b); // C4; from here the frame's fields are frozen (FIX-2)
                }
                row = ROW_C4;
            } else {
                park(b); // C5: the second LEG_LIVE after an orphan round
                row = ROW_C5;
            }
        } else {
            legLiveDuplicateCount++; // C6: dedupe per order across forwarder copies
            row = ROW_C6;
        }
        ledger.eventSink().onReleaseEffect(h, f, scope, result, row);
    }

    /**
     * Section 2 defaults (FIX-7) for the S7 signatures and every RC1-shaped caller under v3: orderType and side from the
     * matched record (0 when none), appliedPosition = egressSeq, status 0 / 4 / -1 by result, timeInForce and
     * expectedOldRemaining as the matched pending dispatch was minted (0 when none), ledgerSize = meLedgerSize (the bound
     * meLedgerCapacity for a CAPACITY result, S4 3.3), ledgerCapacity = meLedgerCapacity, behaviorVersion =
     * meBehaviorVersion.
     */
    public MeOutcomeView outcomeWithDefaults(final MeOutcomeView v, final long egressSeq, final long journalSeq,
                                             final long commandIdHigh, final long commandIdLow, final long userId,
                                             final long omsOrderId, final long oldOrderId, final long price, final long quantity,
                                             final long budget, final int marketId, final int commandKind, final int result,
                                             final long legId, final int reason, final boolean oldCancelled,
                                             final long meTimestamp, final long meEpoch) {
        v.reset();
        v.egressSeq = egressSeq;
        v.journalSeq = journalSeq;
        v.meEpoch = meEpoch;
        v.commandIdHigh = commandIdHigh;
        v.commandIdLow = commandIdLow;
        v.userId = userId;
        v.omsOrderId = omsOrderId;
        v.oldOrderId = oldOrderId;
        v.price = price;
        v.quantity = quantity;
        v.budget = budget;
        v.marketId = marketId;
        v.commandKind = commandKind;
        v.result = result;
        v.orderId = legId;
        v.reason = reason;
        v.oldCancelled = oldCancelled;
        v.meTimestamp = meTimestamp;
        v.appliedPosition = egressSeq;
        v.status = result == RESULT_APPLIED ? 0 : result == RESULT_REJECTED ? ST_REJECTED : -1;
        final CoreOrder o = orders.get(commandIdHigh);
        if (o != null) {
            v.orderType = meOrderType(o);
            v.orderSide = o.side;
            if (o.has(CoreOrder.F_DISPATCH_PENDING) && commandIdLow == (((long) o.attempt << 2) | kindCodeOf(o.pendingKind))) {
                final DispatchKind kind = o.pendingKind == CoreOrder.PK_CANCEL ? DispatchKind.CANCEL
                        : o.pendingKind == CoreOrder.PK_AMEND ? DispatchKind.AMEND : DispatchKind.CREATE;
                v.timeInForce = wireTif(o, kind, v3());
                v.expectedOldRemaining = kind == DispatchKind.AMEND && v3() ? o.pendingExpectedOldRemaining : 0L;
            }
        }
        v.ledgerCapacity = ledger.feed().meLedgerCapacity();
        v.ledgerSize = result == RESULT_CAPACITY ? v.ledgerCapacity : meLedgerSize;
        v.behaviorVersion = ledger.feed().meBehaviorVersion();
        return v;
    }

    // ---------------------------------------------------------------- RC2 S8: restore (sections 2 and 11) and derived state

    /** Section 11 restore: one backlog entry exactly as read (checked by {@link #meLinkRestoreBreach}). */
    public void restoreBacklogEntry(final ReleaseBacklog.Entry e) {
        backlog.restore(e);
    }

    /** Section 11 restore: the ME link head. */
    public void restoreMeLink(final long ledgerSize, final boolean high) {
        this.meLedgerSize = ledgerSize;
        this.capacityHigh = high;
    }

    /**
     * Rebuild the derived state of S8 1.5/1.6 after sections 2 and 11: the index (live {@code F_DISPATCH_PENDING} records
     * and the backlog's RELEASE and ORPHAN_CANCEL entries, sorted by seq), and the maintained gauges.
     */
    public void rebuildDerived() {
        final int n = orders.size() + backlog.size();
        final long[] seqs = new long[n];
        final long[] refs = new long[n];
        final int[] tags = new int[n];
        final int[] k = {0};
        final long[] parked = {0L};
        final long[] quarantined = {0L};
        orders.forEachByOrderId(o -> {
            if (o.has(CoreOrder.F_DISPATCH_PENDING)) {
                seqs[k[0]] = o.pendingDispatchSeq;
                refs[k[0]] = o.orderId;
                tags[k[0]] = PendingDispatchIndex.TAG_TRADING;
                k[0]++;
            }
            if (o.has(CoreOrder.F_ME_CAPACITY_WAIT)) {
                parked[0]++;
            }
            if (o.has(CoreOrder.F_QUARANTINED)) {
                quarantined[0]++;
            }
        });
        backlog.forEachByOrderId(b -> {
            if (b.stage != ReleaseBacklog.STAGE_PARKED) {
                seqs[k[0]] = b.dispatchSeq;
                refs[k[0]] = b.orderId;
                tags[k[0]] = b.stage == ReleaseBacklog.STAGE_RELEASE ? PendingDispatchIndex.TAG_RELEASE : PendingDispatchIndex.TAG_ORPHAN;
                k[0]++;
            }
        });
        index.rebuild(seqs, refs, tags, k[0]);
        orphanCapacityWait.clear(); // NOTE-5: node-local, not snapshotted
        capacityParked = parked[0];
        quarantinedLive = quarantined[0];
        deferredCount = 0;
        refreshGauges();
    }

    /**
     * S8 5.1 a-d on every restored record (5.0: each with its live rule); null when they hold, else the first breach.
     */
    public String orderRestoreBreach(final boolean v3) {
        final String[] breach = new String[1];
        orders.forEachByOrderId(o -> {
            if (breach[0] != null) {
                return;
            }
            if (v3 && (o.userId < 1 || o.marketId < 1 || o.marketId > MARKETS || o.side < 0 || o.side > 1 || o.orderType < 0
                    || o.orderType > 6 || o.timeInForce < 0 || o.timeInForce > 3 || o.attempt < 0 || o.currentLegId < 0
                    || o.oldLegId < 0 || o.orderId < 1)) {
                // review Q4: the identity M11 copies into an ORDER entry, range-checked as admission (applyCreate) checks it,
                // the live rule of the 5.0 FIX-4 row; refused here instead of at the snapshot after M11 copies it
                breach[0] = "order " + o.orderId + " has an identity outside the admission ranges (user " + o.userId + ", market "
                        + o.marketId + ", side " + o.side + ", type " + o.orderType + ", tif " + o.timeInForce + ", attempt "
                        + o.attempt + ", legs " + o.currentLegId + "/" + o.oldLegId + ")";
            } else if ((o.stateFlags & ~CoreOrder.KNOWN_STATE_FLAGS) != 0) {
                breach[0] = "order " + o.orderId + " carries state flag bits 0x" + Integer.toHexString(o.stateFlags);
            } else if (o.has(CoreOrder.F_ME_CAPACITY_WAIT) && !o.has(CoreOrder.F_DISPATCH_PENDING)) {
                breach[0] = "order " + o.orderId + " is ME_CAPACITY_WAIT without a pending dispatch";
            } else if ((o.pendingLegQty != 0 || o.pendingExpectedOldRemaining != 0) && !o.has(CoreOrder.F_REPLACE_PENDING)) {
                breach[0] = "order " + o.orderId + " carries an amend reference outside a replace window";
            } else if (v3 && o.has(CoreOrder.F_REPLACE_PENDING) && (o.pendingLegQty < 1 || o.pendingExpectedOldRemaining < 1
                    || o.pendingExpectedOldRemaining != o.quantity - o.pendingQuantity + o.pendingLegQty)) {
                breach[0] = "order " + o.orderId + " has an amend reference that breaks 3.7 (leg " + o.pendingLegQty + ", eor "
                        + o.pendingExpectedOldRemaining + ", qty " + o.quantity + ", pendingQty " + o.pendingQuantity + ")";
            }
        });
        return breach[0];
    }

    /** S8 5.2 rules 1-7 against the restored sections 2, 8, 10 and 11; null when they hold, else the first breach. */
    public String meLinkRestoreBreach() {
        final long cap = ledger.feed().meLedgerCapacity();
        if (meLedgerSize < 0 || meLedgerSize > cap) {
            return "meLedgerSize " + meLedgerSize + " outside [0, " + cap + "]";
        }
        final long next = ledger.getNextOrderId();
        final long head = ledger.getDispatchSeq();
        final String[] breach = new String[1];
        final long[] previous = {0L};
        final org.agrona.collections.LongHashSet seqs = new org.agrona.collections.LongHashSet();
        orders.forEachByOrderId(o -> {
            if (o.has(CoreOrder.F_DISPATCH_PENDING)) {
                seqs.add(o.pendingDispatchSeq);
            }
        });
        backlog.forEachByOrderId(b -> {
            if (breach[0] != null) {
                return;
            }
            final String what = "backlog entry " + b.orderId + " " + b;
            if (b.orderId <= previous[0] || b.orderId < 1 || b.orderId >= next) {
                breach[0] = what + ": orderId outside [1, " + next + ") or not ascending";
                return;
            }
            previous[0] = b.orderId;
            if (b.scope < 0 || b.scope > 1 || b.stage < 0 || b.stage > 2 || b.orphanTries < 0 || b.orphanTries > 1) {
                breach[0] = what + ": scope, stage or orphanTries out of range";
                return;
            }
            final CoreOrder o = orders.get(b.orderId);
            if (b.scope == ReleaseBacklog.SCOPE_UP_TO) {
                if (o == null || o.has(CoreOrder.F_ME_RELEASED) || b.stage != ReleaseBacklog.STAGE_RELEASE || b.orphanTries != 0
                        || b.userId != 0 || b.legId != 0 || b.marketId != 0 || b.attempt != 0 || b.side != 0 || b.meOrderType != 0
                        || b.floor < 0 || b.floor >= ((((long) o.attempt) << 2) | 3)) {
                    breach[0] = what + ": an UP_TO entry breaks rule 3";
                    return;
                }
            } else {
                final boolean identity = b.userId >= 1 && b.marketId >= 1 && b.marketId <= MARKETS && b.side >= 0 && b.side <= 1
                        && b.meOrderType >= 0 && b.meOrderType <= 2 && b.legId >= 0 && b.attempt >= 0 && b.floor >= 3;
                final long first = (((long) b.attempt) << 2) | 3;
                final long second = ((((long) b.attempt) - 1) << 2) | 3;
                final boolean stageOk;
                switch (b.stage) {
                    case ReleaseBacklog.STAGE_RELEASE: stageOk = b.floor == first; break;
                    case ReleaseBacklog.STAGE_ORPHAN_CANCEL:
                        stageOk = b.attempt >= 1 && b.floor == second && b.legId >= 1 && b.orphanTries == 0;
                        break;
                    default: stageOk = b.floor == first || (b.attempt >= 1 && b.floor == second); break;
                }
                if (o != null || !identity || !stageOk
                        || (b.orphanTries == 1 && b.stage == ReleaseBacklog.STAGE_ORPHAN_CANCEL)) {
                    breach[0] = what + ": an ORDER entry breaks rule 4 or 6";
                    return;
                }
                final com.openexchange.assets.domain.Account acct = ledger.account(b.userId);
                if (acct != null && acct.holdAssetId(b.orderId) >= 0) {
                    breach[0] = what + ": the account of user " + b.userId + " still holds under the ORDER entry (rule 7)";
                    return;
                }
            }
            if (b.dispatchSeq < 1 || b.dispatchSeq > head || !seqs.add(b.dispatchSeq)) {
                breach[0] = what + ": dispatchSeq outside [1, " + head + "] or not unique (rule 5)";
            }
        });
        return breach[0];
    }

    /** S8 5.2 diagnostic D1 (counted, never a refusal): ME_CAPACITY on GLOBAL without capacityHigh. */
    public void countRestoreDiagnostics() {
        if (globalHas(GateReason.ME_CAPACITY) && !capacityHigh) {
            restoreCapacityBitWithoutHighCount++;
        }
    }

    // ---------------------------------------------------------------- RC2 S8: reads (metrics, tests)

    public ReleaseBacklog backlog() { return backlog; }
    public PendingDispatchIndex dispatchIndex() { return index; }
    public long meLedgerSize() { return meLedgerSize; }
    public boolean capacityHigh() { return capacityHigh; }
    public long releaseMintedUpToCount() { return releaseMintedUpToCount; }
    public long releaseMintedOrderCount() { return releaseMintedOrderCount; }
    public long releaseConfirmedCount() { return releaseConfirmedCount; }
    public long releaseConfirmStaleCount() { return releaseConfirmStaleCount; }
    public long releaseAbandonedCount() { return releaseAbandonedCount; }
    public long releaseSkippedCount() { return releaseSkippedCount; }
    public long legLiveCount() { return legLiveCount; }
    public long legLiveDuplicateCount() { return legLiveDuplicateCount; }
    public long orphanCancelCount() { return orphanCancelCount; }
    public long parkedCount() { return parkedCount; }
    public long releasedLateCount() { return releasedLateCount; }
    public long meInvalidCount() { return meInvalidCount; }
    public long meReleaseEarlyCount() { return meReleaseEarlyCount; }
    public long meAmendMismatchCount() { return meAmendMismatchCount; }
    public long capacityWhileOpenCount() { return capacityWhileOpenCount; }
    public long capacityOutcomeCount() { return capacityOutcomeCount; }
    public long meLinkMismatchCount() { return meLinkMismatchCount; }
    public long meLedgerSizeClampedCount() { return meLedgerSizeClampedCount; }
    public long lateApplyOnDeadOrderCount() { return lateApplyOnDeadOrderCount; }
    public long legZeroRefusedCount() { return legZeroRefusedCount; }
    public long terminalWithPendingCreateCount() { return terminalWithPendingCreateCount; }
    public long restoreCapacityBitWithoutHighCount() { return restoreCapacityBitWithoutHighCount; }
    /** Index slots the last RequestPendingDispatch reply visited (test hook, G7). */
    public long pendingReplyVisited() { return pendingReplyVisited; }
    public long capacityParked() { return capacityParked; }
    public long quarantinedLive() { return quarantinedLive; }
    public long pendingOldestSeq() { return pendingOldestSeq; }
    public long oldestReleaseSeq() { return oldestReleaseSeq; }
    public long backlogRelease() { return backlog.count(ReleaseBacklog.STAGE_RELEASE); }
    public long backlogOrphan() { return backlog.count(ReleaseBacklog.STAGE_ORPHAN_CANCEL); }
    public long backlogParked() { return backlog.count(ReleaseBacklog.STAGE_PARKED); }

    // ---------------------------------------------------------------- RC1 S5: AmendOrder, QuarantineOrder, queries

    /** AmendOrder (52). Contract S5 §3.4 (lifecycle B7). */
    public void applyAmend(final AmendOrderCommand c, final long timestamp) {
        amendBody(c, timestamp);
        endApply();
    }

    private void amendBody(final AmendOrderCommand c, final long timestamp) {
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
                || (o.orderType != TYPE_LIMIT && o.orderType != TYPE_LIMIT_MAKER)
                || (o.remainingQty == 0 && v3())    // RC2 S8 3.7: defensive NOT_OPEN (structurally unreachable, FIX-5)
                || (v3() && (o.has(CoreOrder.F_IOC) || o.has(CoreOrder.F_FOK)))) {
            // review BLOCKER-1 (coordinator 2026-10-27): the native ME never rests an IOC/FOK, so an amend of one has no
            // meaning; admitting it let the ME's kill terminal be taken for the replace's old-leg cancel and strand the hold
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
        final CoreRejectReason amendShape = behaviourShape(o.marketId, c.newPrice == 0 ? o.price : c.newPrice, 0L,
                c.newQuantity == 0 ? o.quantity : c.newQuantity);
        if (amendShape != null) {
            rejectAmend(sink, c, amendShape, c.newPrice, now, false);
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
        o.pendingLegQty = legQty; // RC2 S8 3.7, every behaviour: the reply replays the minted bytes
        o.pendingExpectedOldRemaining = o.remainingQty;
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
        quarantinedLive++;
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

    /**
     * RequestPendingDispatch (65), ORIGIN-only reply (RC2 S8 3.8, every behaviour). {@code maxEntries < 0}: OrderReject
     * INVALID naming 65; {@code 0}: the head probe ({@code PendingDispatchEnd{0, dispatchSeq}}, no index visit); {@code 1..4096}
     * a page (above 4096 clamped). The page walks the index from the first seq above {@code fromDispatchSeq}: O(log n + k +
     * dead-in-range), zero allocation. Every reply entry equals its minted MeDispatch field for field. Read-only: no
     * endApply, no mint, no BROADCAST.
     */
    public void applyRequestPendingDispatch(final long correlationId, final long fromDispatchSeq, final int maxEntries) {
        final AssetsEventSink sink = ledger.eventSink();
        pendingReplyVisited = 0;
        if (maxEntries < 0) {
            sink.onOrderReject(correlationId, 0L, 0L, 0L, 0L, 0, T_PENDING_DISPATCH, CoreRejectReason.INVALID, 255, maxEntries,
                    ledger.getLastLogTime());
            return;
        }
        if (maxEntries == 0) {
            sink.onPendingDispatchEnd(correlationId, 0, ledger.getDispatchSeq());
            return;
        }
        final int max = Math.min(maxEntries, PENDING_DISPATCH_CAP);
        final boolean v3 = v3();
        int count = 0;
        long visited = 0;
        for (int slot = index.firstSlotAfter(fromDispatchSeq); slot < index.end() && count < max; slot = index.nextSlot(slot)) {
            visited++;
            if (index.isDead(slot)) {
                continue;
            }
            final long seq = index.seqAt(slot);
            final long h = index.refAt(slot);
            switch (index.tagAt(slot)) {
                case PendingDispatchIndex.TAG_TRADING: {
                    final CoreOrder o = orders.get(h);
                    if (o == null || o.pendingDispatchSeq != seq) {
                        throw new IllegalStateException("pending-dispatch index names seq " + seq + " for order " + h
                                + " but the record says " + (o == null ? "dead" : String.valueOf(o.pendingDispatchSeq)));
                    }
                    replyTrading(sink, o, v3);
                    break;
                }
                case PendingDispatchIndex.TAG_RELEASE: {
                    final ReleaseBacklog.Entry b = backlog.get(h);
                    sink.onMeDispatchReply(seq, h, b.floor, DispatchKind.RELEASE_ID, 0L, 0L, 0L, 0, 0, 0, 0L, 0L, 0L, 0,
                            scopeOf(b.scope), 0L);
                    break;
                }
                default: {
                    final ReleaseBacklog.Entry b = backlog.get(h);
                    sink.onMeDispatchReply(seq, h, ((long) b.attempt << 2) | DispatchKind.CANCEL.code(), DispatchKind.CANCEL, b.userId,
                            h, b.legId, b.marketId, b.meOrderType, b.side, 0L, 0L, 0L, 0, ReleaseScope.UP_TO, 0L);
                    break;
                }
            }
            count++;
        }
        pendingReplyVisited = visited;
        sink.onPendingDispatchEnd(correlationId, count, ledger.getDispatchSeq());
    }

    /** The reply of one trading dispatch, computed from the record exactly as it was minted (3.6, 3.7, 3.8). */
    private void replyTrading(final AssetsEventSink sink, final CoreOrder o, final boolean v3) {
        final DispatchKind kind;
        long oldLeg = 0L, price = 0L, qty = 0L, budget = 0L, eor = 0L;
        switch (o.pendingKind) {
            case CoreOrder.PK_CANCEL:
                kind = DispatchKind.CANCEL;
                oldLeg = o.currentLegId;
                break;
            case CoreOrder.PK_AMEND:
                kind = DispatchKind.AMEND;
                oldLeg = o.oldLegId;
                price = o.pendingPrice;
                qty = o.pendingLegQty; // S8 3.7: the minted quantity, never recomputed from fills inside the window
                eor = v3 ? o.pendingExpectedOldRemaining : 0L;
                break;
            default:
                kind = DispatchKind.CREATE;
                price = meOrderType(o) == TYPE_MARKET ? 0L : o.price;
                qty = createQuantity(o);
                budget = o.budget;
                break;
        }
        sink.onMeDispatchReply(o.pendingDispatchSeq, o.orderId, ((long) o.attempt << 2) | kind.code(), kind, o.userId, o.orderId,
                oldLeg, o.marketId, meOrderType(o), o.side, price, qty, budget, wireTif(o, kind, v3), ReleaseScope.UP_TO, eor);
    }

    private static ReleaseScope scopeOf(final int scope) {
        return scope == ReleaseBacklog.SCOPE_ORDER ? ReleaseScope.ORDER : ReleaseScope.UP_TO;
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

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.OrderEngine.TYPE_ICEBERG;
import static com.openexchange.assets.application.engine.order.OrderEngine.TYPE_STOP_LIMIT;
import static com.openexchange.assets.application.engine.order.OrderEngine.TYPE_STOP_LOSS;
import static com.openexchange.assets.application.engine.order.OrderEngine.TYPE_TRAILING_STOP;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * RC1 S6 G1 (contract section 1; spec synthetic-marketdata 2.5, R13, R14, R16): admission of STOP_LOSS (3),
 * STOP_LIMIT (4), TRAILING_STOP (5) and ICEBERG (6). Structural violations are INVALID and consume no rate
 * budget; IOC/FOK are UNSUPPORTED_TIF; an already crossed stop is STOP_WOULD_TRIGGER_IMMEDIATELY (stored); the
 * hold follows the section 1 table exactly; an armed order is PENDING_TRIGGER with no ME command and holds a slot.
 * Market 1 = BTC-USD (BUY locks USD, SELL locks BTC).
 */
public class SyntheticAdmissionTest {

    private static final int MKT = 1;
    private static final int BUY = 0;
    private static final int SELL = 1;
    private static final int GTC = 0;
    private static final int IOC = 1;
    private static final int FOK = 2;
    private static final int GTD = 3;
    private static final long USD_FUNDS = 1_000_000 * ONE;
    private static final long BTC_FUNDS = 1_000 * ONE;

    // ---------------------------------------------------------------- fixtures

    private static OrderRig funded() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, USD_FUNDS);
        r.deposit(1, BTC, BTC_FUNDS);
        return r;
    }

    /** A funded rig whose user 1 already has a UserRisk record (one resting LIMIT, rate secCount 1). */
    private static OrderRig seeded() {
        final OrderRig r = funded();
        r.create(r.limit(1, MKT, BUY, ONE, ONE));
        assertNotNull("seed LIMIT admitted", r.orders.orders().get(1));
        assertEquals(1, r.orders.user(1).secCount);
        return r;
    }

    private static CreateOrderCommand stopLoss(final OrderRig r, final int side, final long stop, final long qty) {
        final CreateOrderCommand c = r.limit(1, MKT, side, 0L, qty);
        c.orderType = TYPE_STOP_LOSS;
        c.stopPrice = stop;
        return c;
    }

    private static CreateOrderCommand stopLimit(final OrderRig r, final int side, final long stop, final long price, final long qty) {
        final CreateOrderCommand c = r.limit(1, MKT, side, price, qty);
        c.orderType = TYPE_STOP_LIMIT;
        c.stopPrice = stop;
        return c;
    }

    private static CreateOrderCommand trailing(final OrderRig r, final int side, final long delta, final long qty) {
        final CreateOrderCommand c = r.limit(1, MKT, side, 0L, qty);
        c.orderType = TYPE_TRAILING_STOP;
        c.trailingDelta = delta;
        return c;
    }

    private static CreateOrderCommand iceberg(final OrderRig r, final int side, final long price, final long qty, final long display) {
        final CreateOrderCommand c = r.limit(1, MKT, side, price, qty);
        c.orderType = TYPE_ICEBERG;
        c.displayQty = display;
        return c;
    }

    /** {@code x (1 + bps/1e4)} on a price, left to right in 8dp, exactly as the MARKET BUY estimate is built. */
    private static long slip(final long price, final int bps) {
        return FixedPoint.multiply(price, ONE + (long) bps * 10_000L);
    }

    /** Create {@code c} and require an admission; returns the ack. */
    private static AssetsEngineEvent.OrderAck admit(final OrderRig r, final CreateOrderCommand c, final String what) {
        r.markNow();
        r.create(c);
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.OrderReject rej) {
                fail(what + " must be admitted (S6 contract section 1), got " + rej.render());
            }
        }
        return r.first(AssetsEngineEvent.OrderAck.class);
    }

    /** Create {@code c} and require exactly the reject {@code reason}, with no order, hold or event beyond the reject. */
    private static void refused(final OrderRig r, final CreateOrderCommand c, final CoreRejectReason reason, final String what) {
        final int ordersBefore = r.orders.orders().size();
        final long usdLocked = r.engine.account(1).locked(USD);
        final long btcLocked = r.engine.account(1).locked(BTC);
        r.markNow();
        r.create(c);
        final AssetsEngineEvent.OrderReject rej = r.reject();
        assertEquals(what + " must be refused " + reason + " (S6 contract section 1)", reason, rej.reason());
        assertEquals(what + ": the reject names CreateOrder", 50, rej.refTemplateId());
        assertEquals(what + ": no record", ordersBefore, r.orders.orders().size());
        assertEquals(what + ": no USD hold", usdLocked, r.engine.account(1).locked(USD));
        assertEquals(what + ": no BTC hold", btcLocked, r.engine.account(1).locked(BTC));
    }

    /** Structural INVALID: refused, and the rate budget (secCount/minCount) is untouched. */
    private static void invalid(final OrderRig r, final CreateOrderCommand c, final String what) {
        final int sec = r.orders.user(1).secCount;
        final int min = r.orders.user(1).minCount;
        refused(r, c, CoreRejectReason.INVALID, what);
        assertEquals(what + ": a structural INVALID consumes no rate budget (secCount)", sec, r.orders.user(1).secCount);
        assertEquals(what + ": a structural INVALID consumes no rate budget (minCount)", min, r.orders.user(1).minCount);
    }

    private static void assertArmed(final OrderRig r, final long orderId, final String what) {
        final CoreOrder o = r.orders.orders().get(orderId);
        assertNotNull(what + ": record exists", o);
        assertEquals(what + ": status PENDING_TRIGGER (code 7)", OrderStatus.PENDING_TRIGGER.code(), o.status);
        assertFalse(what + ": an armed order has no ME command in flight", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(what + ": no pending kind", CoreOrder.PK_NONE, o.pendingKind);
        assertEquals(what + ": no MeDispatch at admission", 0, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals(what + ": OrderAck status PENDING_TRIGGER", OrderStatus.PENDING_TRIGGER, ack.status());
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(what + ": ACCEPTED event", EventKind.ACCEPTED, ev.kind());
        assertEquals(what + ": ACCEPTED event carries PENDING_TRIGGER", OrderStatus.PENDING_TRIGGER, ev.status());
    }

    private static int gtdEntries(final OrderRig r, final long orderId) {
        final int[] n = new int[1];
        r.engine.deadlines().forEach((d, id, kind) -> {
            if (kind == DeadlineIndex.KIND_GTD && id == orderId) {
                n[0]++;
            }
        });
        return n[0];
    }

    // ---------------------------------------------------------------- section 1 table: required / forbidden

    @Test
    public void section1_stopLossRequiresStopPriceAndForbidsPriceDeltaDisplay() {
        final OrderRig r = seeded();
        invalid(r, stopLoss(r, SELL, 0L, ONE), "STOP_LOSS stopPrice 0");
        CreateOrderCommand c = stopLoss(r, SELL, 90 * ONE, ONE);
        c.price = 90 * ONE;
        invalid(r, c, "STOP_LOSS price != 0");
        c = stopLoss(r, SELL, 90 * ONE, ONE);
        c.trailingDelta = ONE;
        invalid(r, c, "STOP_LOSS trailingDelta != 0");
        c = stopLoss(r, SELL, 90 * ONE, ONE);
        c.displayQty = ONE / 2;
        invalid(r, c, "STOP_LOSS displayQty != 0");
        admit(r, stopLoss(r, SELL, 90 * ONE, ONE), "STOP_LOSS SELL stop 90");
        assertEquals("the admitted stop consumed one rate unit", 2, r.orders.user(1).secCount);
    }

    @Test
    public void section1_stopLimitRequiresStopAndPriceAndForbidsDeltaDisplayBudget() {
        final OrderRig r = seeded();
        invalid(r, stopLimit(r, SELL, 0L, 89 * ONE, ONE), "STOP_LIMIT stopPrice 0");
        invalid(r, stopLimit(r, SELL, 90 * ONE, 0L, ONE), "STOP_LIMIT price 0");
        CreateOrderCommand c = stopLimit(r, SELL, 90 * ONE, 89 * ONE, ONE);
        c.trailingDelta = ONE;
        invalid(r, c, "STOP_LIMIT trailingDelta != 0");
        c = stopLimit(r, SELL, 90 * ONE, 89 * ONE, ONE);
        c.displayQty = ONE / 2;
        invalid(r, c, "STOP_LIMIT displayQty != 0");
        c = stopLimit(r, BUY, 110 * ONE, 111 * ONE, ONE);
        c.budget = 200 * ONE;
        invalid(r, c, "STOP_LIMIT budget != 0");
        admit(r, stopLimit(r, SELL, 90 * ONE, 89 * ONE, ONE), "STOP_LIMIT SELL stop 90 price 89");
        assertEquals(2, r.orders.user(1).secCount);
    }

    @Test
    public void section1_trailingStopRequiresDeltaAndForbidsPriceStopDisplay() {
        final OrderRig r = seeded();
        invalid(r, trailing(r, SELL, 0L, ONE), "TRAILING_STOP trailingDelta 0");
        CreateOrderCommand c = trailing(r, SELL, 5 * ONE, ONE);
        c.price = 90 * ONE;
        invalid(r, c, "TRAILING_STOP price != 0");
        c = trailing(r, SELL, 5 * ONE, ONE);
        c.stopPrice = 90 * ONE;
        invalid(r, c, "TRAILING_STOP stopPrice != 0");
        c = trailing(r, SELL, 5 * ONE, ONE);
        c.displayQty = ONE / 2;
        invalid(r, c, "TRAILING_STOP displayQty != 0");
        admit(r, trailing(r, SELL, 5 * ONE, ONE), "TRAILING_STOP SELL delta 5");
        assertEquals(2, r.orders.user(1).secCount);
    }

    @Test
    public void section1_icebergRequiresPriceAndDisplayAndForbidsStopDeltaBudget() {
        final OrderRig r = seeded();
        invalid(r, iceberg(r, SELL, 0L, 10 * ONE, 3 * ONE), "ICEBERG price 0");
        invalid(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 0L), "ICEBERG displayQty 0");
        invalid(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 10 * ONE), "ICEBERG displayQty == qty");
        CreateOrderCommand c = iceberg(r, SELL, 100 * ONE, 10 * ONE, 3 * ONE);
        c.stopPrice = 90 * ONE;
        invalid(r, c, "ICEBERG stopPrice != 0");
        c = iceberg(r, SELL, 100 * ONE, 10 * ONE, 3 * ONE);
        c.trailingDelta = ONE;
        invalid(r, c, "ICEBERG trailingDelta != 0");
        c = iceberg(r, BUY, 100 * ONE, 10 * ONE, 3 * ONE);
        c.budget = 2_000 * ONE;
        invalid(r, c, "ICEBERG budget != 0");
        admit(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 3 * ONE), "ICEBERG SELL 10 display 3");
        assertEquals(2, r.orders.user(1).secCount);
    }

    // ---------------------------------------------------------------- TIF

    @Test
    public void section1_iocAndFokOnEverySyntheticTypeAreUnsupportedTif() {
        final OrderRig r = seeded();
        for (final int tif : new int[] {IOC, FOK}) {
            final CreateOrderCommand[] cmds = {stopLoss(r, SELL, 90 * ONE, ONE), stopLimit(r, SELL, 90 * ONE, 89 * ONE, ONE),
                    trailing(r, SELL, 5 * ONE, ONE), iceberg(r, SELL, 100 * ONE, 10 * ONE, 3 * ONE)};
            for (final CreateOrderCommand c : cmds) {
                c.timeInForce = tif;
                final int sec = r.orders.user(1).secCount;
                refused(r, c, CoreRejectReason.UNSUPPORTED_TIF, "type " + c.orderType + " tif " + tif);
                assertEquals("UNSUPPORTED_TIF is structural: no rate budget consumed", sec, r.orders.user(1).secCount);
            }
        }
    }

    @Test
    public void section1_gtdIsAllowedOnSyntheticTypes() {
        final OrderRig r = funded();
        final long expiry = r.now + 60_000L;
        CreateOrderCommand c = stopLoss(r, SELL, 90 * ONE, ONE);
        c.timeInForce = GTD;
        c.expiresAtMs = expiry;
        final long stopId = admit(r, c, "GTD STOP_LOSS SELL").orderId();
        assertEquals("GTD stop keeps its expiry", expiry, r.orders.orders().get(stopId).expiresAtMs);
        assertEquals("GTD stop is in the deadline index", 1, gtdEntries(r, stopId));
        c = iceberg(r, SELL, 100 * ONE, 10 * ONE, 3 * ONE);
        c.timeInForce = GTD;
        c.expiresAtMs = expiry;
        final long iceId = admit(r, c, "GTD ICEBERG SELL").orderId();
        assertEquals("GTD iceberg is in the deadline index", 1, gtdEntries(r, iceId));
    }

    // ---------------------------------------------------------------- R13 / D8: crossed at admission

    @Test
    public void R13_alreadyCrossedSellStopIsRejectedAfterTheRateCheckAndStored() {
        final OrderRig r = seeded();
        r.tick(MKT, 100 * ONE, 101 * ONE, 1);
        final CreateOrderCommand atBid = stopLoss(r, SELL, 100 * ONE, ONE);
        refused(r, atBid, CoreRejectReason.STOP_WOULD_TRIGGER_IMMEDIATELY, "SELL STOP_LOSS stop == bestBid");
        assertEquals("the crossed reject comes after the rate check (consumes one unit)", 2, r.orders.user(1).secCount);
        refused(r, atBid, CoreRejectReason.STOP_WOULD_TRIGGER_IMMEDIATELY, "replay of the same requestId");
        assertEquals("a replay returns the stored decision and consumes nothing", 2, r.orders.user(1).secCount);
        refused(r, stopLimit(r, SELL, 105 * ONE, 104 * ONE, ONE), CoreRejectReason.STOP_WOULD_TRIGGER_IMMEDIATELY,
                "SELL STOP_LIMIT stop above bestBid");
        admit(r, stopLoss(r, SELL, 100 * ONE - 1, ONE), "SELL STOP_LOSS one unit below bestBid");
        assertEquals(1, r.orders.armedStopCount(MKT));
    }

    @Test
    public void R13_alreadyCrossedBuyStopIsRejectedAfterTheRateCheckAndStored() {
        final OrderRig r = seeded();
        r.tick(MKT, 100 * ONE, 101 * ONE, 1);
        final CreateOrderCommand atAsk = stopLoss(r, BUY, 101 * ONE, ONE);
        refused(r, atAsk, CoreRejectReason.STOP_WOULD_TRIGGER_IMMEDIATELY, "BUY STOP_LOSS stop == bestAsk");
        assertEquals("the crossed reject comes after the rate check (consumes one unit)", 2, r.orders.user(1).secCount);
        refused(r, atAsk, CoreRejectReason.STOP_WOULD_TRIGGER_IMMEDIATELY, "replay of the same requestId");
        assertEquals("a replay returns the stored decision and consumes nothing", 2, r.orders.user(1).secCount);
        refused(r, stopLimit(r, BUY, 95 * ONE, 96 * ONE, ONE), CoreRejectReason.STOP_WOULD_TRIGGER_IMMEDIATELY,
                "BUY STOP_LIMIT stop below bestAsk");
        admit(r, stopLoss(r, BUY, 101 * ONE + 1, ONE), "BUY STOP_LOSS one unit above bestAsk");
        assertEquals(1, r.orders.armedStopCount(MKT));
    }

    @Test
    public void R13_anEmptyReferenceSideIsNotCrossed() {
        final OrderRig r = funded();
        // critic fix: a later tick must not cross the first stop, or 3.1 #4 fires it (bid 100 <= SELL stop 500 would)
        r.tick(MKT, 0L, 101 * ONE, 1);
        admit(r, stopLoss(r, SELL, 50 * ONE, ONE), "SELL stop with bestBid 0");
        r.tick(MKT, 100 * ONE, 0L, 2);
        admit(r, stopLoss(r, BUY, 500 * ONE, ONE), "BUY stop with bestAsk 0");
        assertEquals("both armed", 2, r.orders.armedStopCount(MKT));
    }

    // ---------------------------------------------------------------- holds

    @Test
    public void section1_buyStopLossBudgetZeroHoldsStopPriceTimesSlippageTimesQtyExactly() {
        final OrderRig r = funded();
        final long stop = 12_345_678_901L; // 123.45678901
        final long qty = 33_333_333L;      // 0.33333333
        final long expected = FixedPoint.multiply(slip(stop, 500), qty);
        final AssetsEngineEvent.OrderAck ack = admit(r, stopLoss(r, BUY, stop, qty), "BUY STOP_LOSS budget 0");
        assertEquals("hold asset is the quote", USD, ack.holdAssetId());
        assertEquals("hold = stopPrice x (1 + 500 bps) x qty, exact 8dp", expected, ack.holdAmount());
        assertEquals(expected, r.engine.account(1).locked(USD));
        assertEquals(expected, r.engine.account(1).holdRemaining(ack.orderId()));
        assertEquals(USD_FUNDS - expected, r.engine.account(1).available(USD));
    }

    @Test
    public void section1_buyStopLossEstimateUsesTheMarketSlippageBps() {
        final OrderRig r = funded();
        final var cfg = r.riskConfig(MKT, 1);
        cfg.marketBuySlippageBps = 250;
        r.setRisk(cfg);
        final long stop = 110 * ONE;
        final long qty = 3 * ONE;
        final long expected = FixedPoint.multiply(slip(stop, 250), qty); // 110 x 1.025 x 3 = 338.25
        assertEquals(33_825_000_000L, expected);
        assertEquals("hold follows the configured bps", expected,
                admit(r, stopLoss(r, BUY, stop, qty), "BUY STOP_LOSS at 250 bps").holdAmount());
    }

    @Test
    public void section1_buyStopLossExplicitBudgetIsTheHold() {
        final OrderRig r = funded();
        final CreateOrderCommand c = stopLoss(r, BUY, 110 * ONE, 3 * ONE);
        c.budget = 50 * ONE;
        final AssetsEngineEvent.OrderAck ack = admit(r, c, "BUY STOP_LOSS budget 50");
        assertEquals("an explicit budget is the hold", 50 * ONE, ack.holdAmount());
        assertEquals(50 * ONE, r.engine.account(1).locked(USD));
    }

    @Test
    public void section1_buyTrailingBudgetZeroHoldsAskPlusDeltaTimesSlippageTimesQty() {
        final OrderRig r = funded();
        r.tick(MKT, 99 * ONE, 100 * ONE, 1);
        final long delta = 5 * ONE;
        final long qty = 2 * ONE;
        final long expected = FixedPoint.multiply(slip(100 * ONE + delta, 500), qty); // 105 x 1.05 x 2 = 220.5
        assertEquals(22_050_000_000L, expected);
        final AssetsEngineEvent.OrderAck ack = admit(r, trailing(r, BUY, delta, qty), "BUY TRAILING_STOP budget 0");
        assertEquals("hold = (bestAsk + delta) x (1 + slip) x qty", expected, ack.holdAmount());
        assertEquals(expected, r.engine.account(1).locked(USD));
        assertEquals(1, r.orders.armedTrailingCount(MKT));
    }

    @Test
    public void section1_buyTrailingWithoutAskIsNoReferencePriceButAnExplicitBudgetNeedsNone() {
        final OrderRig r = seeded();
        r.tick(MKT, 99 * ONE, 0L, 1);
        refused(r, trailing(r, BUY, 5 * ONE, ONE), CoreRejectReason.NO_REFERENCE_PRICE, "BUY TRAILING budget 0, no ask");
        final CreateOrderCommand c = trailing(r, BUY, 5 * ONE, ONE);
        c.budget = 80 * ONE;
        final AssetsEngineEvent.OrderAck ack = admit(r, c, "BUY TRAILING explicit budget, no ask");
        assertEquals(80 * ONE, ack.holdAmount());
        assertEquals("no ask at admission: arm 0", 0L, r.orders.orders().get(ack.orderId()).trailingArmPrice);
    }

    @Test
    public void section1_sellStopsLockTheBaseQuantity() {
        final OrderRig r = funded();
        final long[] qty = {2 * ONE, 3 * ONE, 5 * ONE};
        final CreateOrderCommand[] cmds = {stopLoss(r, SELL, 90 * ONE, qty[0]), stopLimit(r, SELL, 90 * ONE, 89 * ONE, qty[1]),
                trailing(r, SELL, 5 * ONE, qty[2])};
        long locked = 0L;
        for (int i = 0; i < cmds.length; i++) {
            final AssetsEngineEvent.OrderAck ack = admit(r, cmds[i], "SELL type " + cmds[i].orderType);
            assertEquals("SELL locks base", BTC, ack.holdAssetId());
            assertEquals("SELL hold = qty", qty[i], ack.holdAmount());
            locked += qty[i];
            assertEquals(locked, r.engine.account(1).locked(BTC));
        }
        assertEquals("no quote locked by SELL stops", 0L, r.engine.account(1).locked(USD));
    }

    @Test
    public void section1_stopLimitBuyLocksPriceTimesQty() {
        final OrderRig r = funded();
        final AssetsEngineEvent.OrderAck ack = admit(r, stopLimit(r, BUY, 110 * ONE, 112 * ONE, 3 * ONE), "BUY STOP_LIMIT");
        assertEquals(USD, ack.holdAssetId());
        assertEquals("BUY STOP_LIMIT hold = price x qty (not the stop)", 336 * ONE, ack.holdAmount());
        assertEquals(336 * ONE, r.engine.account(1).locked(USD));
    }

    @Test
    public void section1_icebergBuyLocksPriceTimesQtyAndDispatchesOneDisplaySlice() {
        final OrderRig r = funded();
        final AssetsEngineEvent.OrderAck ack = admit(r, iceberg(r, BUY, 100 * ONE, 10 * ONE, 3 * ONE), "ICEBERG BUY 10/3");
        assertEquals("ICEBERG acks PENDING_NEW (first slice dispatched)", OrderStatus.PENDING_NEW, ack.status());
        assertEquals(USD, ack.holdAssetId());
        assertEquals("hold = price x total qty", 1_000 * ONE, ack.holdAmount());
        assertEquals(1_000 * ONE, r.engine.account(1).locked(USD));
        assertEquals("exactly one MeDispatch", 1, r.count(AssetsEngineEvent.MeDispatch.class));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CREATE, d.kind());
        assertEquals("the slice is a LIMIT on the wire (orderType 0)", 0, d.orderType());
        assertEquals(100 * ONE, d.price());
        assertEquals("slice quantity = displayQty", 3 * ONE, d.quantity());
        assertEquals(0L, d.budget());
        assertEquals(ack.orderId(), d.commandIdHigh());
        assertEquals("attempt 0, kind CREATE", 0L, d.commandIdLow());
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals(EventKind.ACCEPTED, ev.kind());
        assertEquals(OrderStatus.PENDING_NEW, ev.status());
        final CoreOrder o = r.orders.orders().get(ack.orderId());
        assertEquals("hiddenQty = qty - display", 7 * ONE, o.hiddenQty);
        assertEquals("sliceRemainingQty = display", 3 * ONE, o.sliceRemainingQty);
        assertEquals(3 * ONE, o.displayQty);
        assertEquals("remainingQty is the total", 10 * ONE, o.remainingQty);
        assertEquals(OrderStatus.PENDING_NEW.code(), o.status);
        assertEquals("icebergs are not in the trigger books", 0, r.orders.armedStopCount(MKT));
    }

    @Test
    public void section1_icebergSellLocksQtyAndDispatchesTheDisplaySlice() {
        final OrderRig r = funded();
        final AssetsEngineEvent.OrderAck ack = admit(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 4 * ONE), "ICEBERG SELL 10/4");
        assertEquals(BTC, ack.holdAssetId());
        assertEquals("SELL iceberg holds the total quantity", 10 * ONE, ack.holdAmount());
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(0, d.orderType());
        assertEquals(1, d.side());
        assertEquals(4 * ONE, d.quantity());
        assertEquals(6 * ONE, r.orders.orders().get(ack.orderId()).hiddenQty);
    }

    // ---------------------------------------------------------------- armed state, slot (R14), arm

    @Test
    public void section1_armedStopIsPendingTriggerWithoutDispatchAndTakesASlot() {
        final OrderRig r = funded();
        final long id = admit(r, stopLoss(r, SELL, 90 * ONE, ONE), "SELL STOP_LOSS").orderId();
        assertArmed(r, id, "SELL STOP_LOSS");
        assertEquals("one armed stop on market 1", 1, r.orders.armedStopCount(MKT));
        assertEquals("a stop is not a trailing order", 0, r.orders.armedTrailingCount(MKT));
        assertEquals("R14: the armed stop holds a slot", 1, r.orders.orders().openCount(1));
        assertEquals("no ME command minted", 0L, r.engine.getDispatchSeq());
        final long id2 = admit(r, stopLimit(r, BUY, 110 * ONE, 112 * ONE, ONE), "BUY STOP_LIMIT").orderId();
        assertArmed(r, id2, "BUY STOP_LIMIT");
        assertEquals(2, r.orders.armedStopCount(MKT));
        assertEquals(2, r.orders.orders().openCount(1));
    }

    @Test
    public void section1_armedTrailingIsPendingTriggerWithoutDispatch() {
        final OrderRig r = funded();
        final long id = admit(r, trailing(r, SELL, 5 * ONE, ONE), "SELL TRAILING").orderId();
        assertArmed(r, id, "SELL TRAILING");
        assertEquals(1, r.orders.armedTrailingCount(MKT));
        assertEquals("a trailing order is not in the stop books", 0, r.orders.armedStopCount(MKT));
        assertEquals(1, r.orders.orders().openCount(1));
    }

    @Test
    public void R14_stopTakesASlotAndItsCancelFreesExactlyOne() {
        final OrderRig r = funded();
        final var cfg = r.riskConfig(MKT, 1);
        cfg.maxOpenOrders = 3;
        r.setRisk(cfg);
        r.create(r.limit(1, MKT, BUY, ONE, ONE));
        r.create(r.limit(1, MKT, BUY, ONE, ONE));
        assertEquals("user at maxOpenOrders - 1", 2, r.orders.orders().openCount(1));
        final long stopId = admit(r, stopLoss(r, SELL, 90 * ONE, ONE), "SELL STOP_LOSS at max-1").orderId();
        assertEquals("the stop takes the last slot (+1)", 3, r.orders.orders().openCount(1));
        refused(r, r.limit(1, MKT, BUY, ONE, ONE), CoreRejectReason.OPEN_ORDER_LIMIT, "LIMIT while the stop holds the slot");
        r.markNow();
        r.cancel(1, stopId);
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals(OrderStatus.PENDING_CANCEL, ack.status());
        final AssetsEngineEvent.OrderEvent ev = r.first(AssetsEngineEvent.OrderEvent.class);
        assertEquals("cancel of an armed stop is terminal in the same apply", EventKind.CANCELLED, ev.kind());
        assertEquals(OrderStatus.CANCELLED, ev.status());
        assertEquals("residual released before the terminal event", 0L, ev.holdRemaining());
        assertEquals("no ME command for an armed cancel", 0, r.count(AssetsEngineEvent.MeDispatch.class));
        assertNull("record removed", r.orders.orders().get(stopId));
        assertEquals("BTC hold released", 0L, r.engine.account(1).locked(BTC));
        assertEquals(BTC_FUNDS, r.engine.account(1).available(BTC));
        assertEquals("out of the stop book", 0, r.orders.armedStopCount(MKT));
        assertEquals("the cancel frees exactly one slot (-1)", 2, r.orders.orders().openCount(1));
        admit(r, r.limit(1, MKT, BUY, ONE, ONE), "LIMIT after the stop's cancel");
        assertEquals(3, r.orders.orders().openCount(1));
    }

    @Test
    public void section1_trailingArmAtAdmissionIsTheReferenceSideOrZero() {
        final OrderRig r = funded();
        r.tick(MKT, 100 * ONE, 102 * ONE, 1);
        final long sell = admit(r, trailing(r, SELL, 5 * ONE, ONE), "SELL TRAILING with bid").orderId();
        final long buy = admit(r, trailing(r, BUY, 5 * ONE, ONE), "BUY TRAILING with ask").orderId();
        assertEquals("SELL arm = bestBid", 100 * ONE, r.orders.orders().get(sell).trailingArmPrice);
        assertEquals("BUY arm = bestAsk", 102 * ONE, r.orders.orders().get(buy).trailingArmPrice);
        r.tick(MKT, 0L, 102 * ONE, 2);
        final long sellNoBid = admit(r, trailing(r, SELL, 5 * ONE, ONE), "SELL TRAILING without bid").orderId();
        assertEquals("SELL arm = 0 when bestBid is 0", 0L, r.orders.orders().get(sellNoBid).trailingArmPrice);
        assertEquals(3, r.orders.armedTrailingCount(MKT));
    }

    // ---------------------------------------------------------------- R16

    @Test
    public void R16_degenerateShapesAreRejectedInvalid() {
        final OrderRig r = seeded();
        invalid(r, stopLoss(r, SELL, 0L, ONE), "R16 STOP_LOSS stopPrice 0");
        invalid(r, stopLimit(r, SELL, 0L, 89 * ONE, ONE), "R16 STOP_LIMIT stopPrice 0");
        invalid(r, trailing(r, SELL, 0L, ONE), "R16 TRAILING_STOP trailingDelta 0");
        invalid(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 10 * ONE), "R16 ICEBERG displayQty == qty");
        invalid(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 11 * ONE), "R16 ICEBERG displayQty > qty");
        invalid(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 0L), "R16 ICEBERG displayQty 0");
        // the nearest legal shapes are admitted (reject, never degrade: Q7)
        admit(r, stopLoss(r, SELL, 1L, ONE), "STOP_LOSS stopPrice 1 unit");
        admit(r, trailing(r, SELL, 1L, ONE), "TRAILING delta 1 unit");
        admit(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 10 * ONE - 1), "ICEBERG displayQty qty - 1 unit");
        admit(r, iceberg(r, SELL, 100 * ONE, 10 * ONE, 1L), "ICEBERG displayQty 1 unit");
    }

    @Test
    public void section1_gtcStopHasNoDeadline() {
        final OrderRig r = funded();
        final CreateOrderCommand c = stopLoss(r, SELL, 90 * ONE, ONE);
        assertEquals("rig default TIF is GTC", GTC, c.timeInForce);
        final long id = admit(r, c, "GTC STOP_LOSS").orderId();
        assertEquals("GTC stop has no deadline", 0, gtdEntries(r, id));
        assertEquals(0L, r.orders.orders().get(id).expiresAtMs);
    }
}

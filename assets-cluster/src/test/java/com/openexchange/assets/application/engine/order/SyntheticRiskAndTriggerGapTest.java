// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.APPLIED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_CANCEL;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_CREATE;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.event;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.gtdDeadlines;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.kindCount;
import static com.openexchange.assets.application.engine.order.TriggerRig.BUY;
import static com.openexchange.assets.application.engine.order.TriggerRig.MKT;
import static com.openexchange.assets.application.engine.order.TriggerRig.SELL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 gap tests: the S4 admission chain on the synthetic types (contract 1), no risk re-check at the trigger
 * (3.2), R12 for TRAILING_STOP, quarantine freezing an armed order (3.7 #6), GTD expiry of a triggered child
 * through the S5 path, and the non-REJECTED branch of the unlearned-leg terminal (3.6).
 */
public class SyntheticRiskAndTriggerGapTest {

    private static final long PX = 100 * ONE;
    private static final long CHILD_LEG = 501L;
    private static final long MAKER_LEG = 602L;
    private static final int ME_FILLED = 2;
    private static final int ME_CANCELLED = 3;

    // ---------------------------------------------------------------- helpers

    private static void setRisk(final TriggerRig t, final RiskConfigSetCommand c) {
        t.r.setRisk(c);
        assertEquals("fixture: the RiskConfigSet applied", c.configVersion, t.r.orders.market(MKT).configVersion);
    }

    /** Next tick with a last trade (the collar reference). */
    private static void tickWithTrade(final TriggerRig t, final long bid, final long ask, final long lastPx, final long lastId) {
        t.r.orders.applyMarketTick(MKT, ++t.version, bid, ask, lastPx, lastId, 0L, t.r.now);
    }

    private static CreateOrderCommand iceberg(final TriggerRig t, final long user, final int side, final long price,
                                              final long qty, final long display) {
        final CreateOrderCommand c = t.r.limit(user, MKT, side, price, qty);
        c.orderType = OrderEngine.TYPE_ICEBERG;
        c.displayQty = display;
        return c;
    }

    /** Send {@code c}; require exactly one OrderReject with {@code want}, no record, no hold, no dispatch. */
    private static AssetsEngineEvent.OrderReject refused(final TriggerRig t, final String what, final CreateOrderCommand c,
                                                         final CoreRejectReason want) {
        final int liveBefore = t.r.orders.orders().size();
        final long usdLocked = t.r.engine.account(1).locked(USD);
        final long btcLocked = t.r.engine.account(2).locked(BTC);
        t.r.markNow();
        t.r.create(c);
        final List<AssetsEngineEvent> e = t.r.events();
        assertTrue(what + ": exactly one OrderReject " + want + ", got " + e,
                e.size() == 1 && e.get(0) instanceof AssetsEngineEvent.OrderReject);
        final AssetsEngineEvent.OrderReject rej = (AssetsEngineEvent.OrderReject) e.get(0);
        assertEquals(what + ": reason", want, rej.reason());
        assertEquals(what + ": no record created", liveBefore, t.r.orders.orders().size());
        assertEquals(what + ": no USD hold taken", usdLocked, t.r.engine.account(1).locked(USD));
        assertEquals(what + ": no BTC hold taken", btcLocked, t.r.engine.account(2).locked(BTC));
        return rej;
    }

    private static List<AssetsEngineEvent.OrderEvent> orderEvents(final TriggerRig t, final long orderId) {
        final List<AssetsEngineEvent.OrderEvent> out = new ArrayList<>();
        for (final AssetsEngineEvent e : t.r.events()) {
            if (e instanceof AssetsEngineEvent.OrderEvent oe && oe.orderId() == orderId) {
                out.add(oe);
            }
        }
        return out;
    }

    /** SELL STOP_LOSS 99 x qty of user 2 (orderId 1) under bid 100, fired by bid 99, child not yet acknowledged. */
    private static TriggerRig triggeredSellStop(final long qty, final boolean gtd) {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final CreateOrderCommand c = t.stopLoss(2, SELL, 99 * ONE, qty);
        if (gtd) {
            c.timeInForce = OrderEngine.TIF_GTD;
            c.expiresAtMs = t.r.now + 5_000L;
        }
        assertEquals("fixture: the SELL stop is order 1", 1L, t.admit(c));
        return t;
    }

    private static void fire(final TriggerRig t, final long id) {
        t.r.markNow();
        t.tick(99 * ONE, 101 * ONE);
        t.assertTriggered("fixture trigger", id, t.onlyDispatch("fixture trigger"));
    }

    // ---------------------------------------------------------------- (1) S4 chain on synthetic types

    @Test
    public void priceCollarRefusesAStopLimitWhoseLimitIsOutsideTheBandAndAdmitsTheEdge() {
        final TriggerRig t = TriggerRig.funded();
        tickWithTrade(t, 94 * ONE, 95 * ONE, 95 * ONE, 1L);
        final RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.priceCollarBps = 1_000; // 10% of 95 = 9.5
        setRisk(t, rc);
        final AssetsEngineEvent.OrderReject rej = refused(t, "collar STOP_LIMIT",
                t.stopLimit(1, BUY, PX, 105 * ONE, ONE), CoreRejectReason.PRICE_COLLAR);
        assertEquals("S4 collar detail = the band, floor(95 x 1000 / 1e4)", 95 * ONE / 10, rej.detail());
        final long id = t.admit(t.stopLimit(1, BUY, PX, 95 * ONE + 95 * ONE / 10, ONE));
        assertEquals("a limit exactly at the band edge (dev == band) is admitted", OrderStatus.PENDING_TRIGGER.code(),
                t.order(id).status);
    }

    @Test
    public void priceCollarRefusesAnIcebergLimitOutsideTheBandBeforeAnySliceIsDispatched() {
        final TriggerRig t = TriggerRig.funded();
        tickWithTrade(t, 94 * ONE, 95 * ONE, 95 * ONE, 1L);
        final RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.priceCollarBps = 1_000;
        setRisk(t, rc);
        refused(t, "collar ICEBERG BUY", iceberg(t, 1, BUY, 105 * ONE, 3 * ONE, ONE), CoreRejectReason.PRICE_COLLAR);
        refused(t, "collar ICEBERG SELL", iceberg(t, 2, SELL, 85 * ONE, 3 * ONE, ONE), CoreRejectReason.PRICE_COLLAR);
        assertEquals("no slice was dispatched", 0L, t.r.engine.getDispatchSeq());
    }

    @Test
    public void priceCollarDoesNotApplyToAPriceLessStopLoss() {
        final TriggerRig t = TriggerRig.funded();
        tickWithTrade(t, 94 * ONE, 95 * ONE, 95 * ONE, 1L);
        final RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.priceCollarBps = 1; // 0.01%
        setRisk(t, rc);
        final long id = t.admit(t.stopLoss(1, BUY, 200 * ONE, ONE));
        assertEquals("contract 1: the collar is on price only when price > 0; a STOP_LOSS stop far from the last trade "
                + "is admitted with its estimate hold", TriggerRig.buyStopEstimate(200 * ONE, ONE), t.holdRemaining(1, id));
    }

    @Test
    public void buyStopLossEstimateAboveMaxNotionalIsNotionalTooLargeAndTheExactCapAdmits() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long estimate = TriggerRig.buyStopEstimate(PX, ONE); // 105
        RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.maxNotional = estimate - 1;
        setRisk(t, rc);
        final AssetsEngineEvent.OrderReject rej = refused(t, "BUY STOP_LOSS estimate > maxNotional",
                t.stopLoss(1, BUY, PX, ONE), CoreRejectReason.NOTIONAL_TOO_LARGE);
        assertEquals("the notional of a price-less BUY is its budget (the estimate)", estimate, rej.detail());
        rc = t.r.riskConfig(MKT, 2);
        rc.maxNotional = estimate;
        setRisk(t, rc);
        final long id = t.admit(t.stopLoss(1, BUY, PX, ONE));
        assertEquals("estimate == maxNotional is admitted, hold = estimate", estimate, t.holdRemaining(1, id));
    }

    @Test
    public void buyStopLossExplicitBudgetAboveMaxNotionalIsNotionalTooLarge() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.maxNotional = 50 * ONE;
        setRisk(t, rc);
        final CreateOrderCommand c = t.stopLoss(1, BUY, PX, ONE);
        c.budget = 60 * ONE;
        final AssetsEngineEvent.OrderReject rej = refused(t, "explicit budget > maxNotional", c,
                CoreRejectReason.NOTIONAL_TOO_LARGE);
        assertEquals("the explicit budget is the notional", 60 * ONE, rej.detail());
    }

    @Test
    public void priceLessSellStopHasNotionalZeroAdmittedWhenMinNotionalIsZero() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        assertEquals("precondition: genesis minNotional 0", 0L, t.r.orders.market(MKT).minNotional);
        final long stop = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        final long trail = t.admit(t.trailing(2, SELL, 2 * ONE, ONE, 0L));
        assertEquals("SELL stop locks the base", ONE, t.holdRemaining(2, stop));
        assertEquals("SELL trailing locks the base", ONE, t.holdRemaining(2, trail));
    }

    @Test
    public void priceLessSellStopIsNotionalTooSmallWhenMinNotionalIsPositive() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.minNotional = 1;
        setRisk(t, rc);
        final AssetsEngineEvent.OrderReject a = refused(t, "SELL STOP_LOSS notional 0 < minNotional 1",
                t.stopLoss(2, SELL, 99 * ONE, ONE), CoreRejectReason.NOTIONAL_TOO_SMALL);
        assertEquals("contract 1: a price-less SELL has notional 0", 0L, a.detail());
        refused(t, "SELL TRAILING notional 0 < minNotional 1", t.trailing(2, SELL, 2 * ONE, ONE, 0L),
                CoreRejectReason.NOTIONAL_TOO_SMALL);
        final long sl = t.admit(t.stopLimit(2, SELL, 99 * ONE, 98 * ONE, ONE));
        assertEquals("a SELL STOP_LIMIT has notional price x qty and passes the same floor", ONE, t.holdRemaining(2, sl));
    }

    @Test
    public void circuitBreakerRefusesEverySyntheticCreateAndTheDecisionIsStored() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        t.r.orders.applyCircuitBreaker(0xB1, MKT, true, t.r.now);
        final CreateOrderCommand stop = t.stopLoss(2, SELL, 99 * ONE, ONE);
        refused(t, "breaker STOP_LOSS", stop, CoreRejectReason.CIRCUIT_BREAKER);
        refused(t, "breaker STOP_LIMIT", t.stopLimit(1, BUY, 102 * ONE, 102 * ONE, ONE), CoreRejectReason.CIRCUIT_BREAKER);
        refused(t, "breaker TRAILING", t.trailing(1, BUY, ONE, ONE, 0L), CoreRejectReason.CIRCUIT_BREAKER);
        refused(t, "breaker ICEBERG", iceberg(t, 1, BUY, PX, 3 * ONE, ONE), CoreRejectReason.CIRCUIT_BREAKER);
        t.r.orders.applyCircuitBreaker(0xB2, MKT, false, t.r.now);
        refused(t, "S4: the breaker decision is stored (after the rate check); the same requestId replays it",
                stop, CoreRejectReason.CIRCUIT_BREAKER);
        assertEquals("nothing is armed", 0, t.r.orders.armedStopCount(MKT) + t.r.orders.armedTrailingCount(MKT));
    }

    @Test
    public void gateClosedRefusesSyntheticCreatesWithoutStoringTheDecision() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        t.r.engine.applyOrderEntryGate(0xC1, MKT, GateState.SUSPENDED, GateReason.ADMIN, t.r.now);
        final CreateOrderCommand stop = t.stopLoss(2, SELL, 99 * ONE, ONE);
        refused(t, "gate STOP_LOSS", stop, CoreRejectReason.GATE_CLOSED);
        refused(t, "gate TRAILING", t.trailing(2, SELL, ONE, ONE, 0L), CoreRejectReason.GATE_CLOSED);
        refused(t, "gate ICEBERG", iceberg(t, 2, SELL, PX, 3 * ONE, ONE), CoreRejectReason.GATE_CLOSED);
        t.r.engine.applyOrderEntryGate(0xC2, MKT, GateState.OPEN, GateReason.ADMIN, t.r.now);
        final long id = t.admit(stop);
        assertEquals("S4: GATE_CLOSED is not stored, the same requestId is accepted after the gate opens",
                OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
    }

    @Test
    public void sizeTooSmallAndSizeTooLargeApplyToSyntheticQuantities() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.minQuantity = ONE;
        rc.maxQuantity = 10 * ONE;
        setRisk(t, rc);
        AssetsEngineEvent.OrderReject rej = refused(t, "STOP_LIMIT below minQuantity",
                t.stopLimit(1, BUY, 102 * ONE, 102 * ONE, ONE - 1), CoreRejectReason.SIZE_TOO_SMALL);
        assertEquals("detail = minQuantity", ONE, rej.detail());
        rej = refused(t, "TRAILING above maxQuantity", t.trailing(2, SELL, ONE, 10 * ONE + 1, 0L),
                CoreRejectReason.SIZE_TOO_LARGE);
        assertEquals("detail = maxQuantity", 10 * ONE, rej.detail());
        refused(t, "STOP_LOSS above maxQuantity", t.stopLoss(2, SELL, 99 * ONE, 11 * ONE), CoreRejectReason.SIZE_TOO_LARGE);
        refused(t, "ICEBERG total above maxQuantity (display inside)", iceberg(t, 2, SELL, PX, 11 * ONE, ONE),
                CoreRejectReason.SIZE_TOO_LARGE);
    }

    // ---------------------------------------------------------------- (2) no risk re-check at the trigger (3.2, Q8)

    @Test
    public void triggerFiresDespiteBreakerGateAndTightenedRiskConfig() {
        final TriggerRig t = TriggerRig.funded();
        tickWithTrade(t, 94 * ONE, 95 * ONE, 95 * ONE, 1L);
        final long stopLimit = t.admit(t.stopLimit(1, BUY, PX, 101 * ONE, ONE));
        final long stopLoss = t.admit(t.stopLoss(1, BUY, PX, ONE));
        final long hold = t.holdRemaining(1, stopLoss);

        t.r.orders.applyCircuitBreaker(0xB1, MKT, true, t.r.now);
        t.r.engine.applyOrderEntryGate(0xC1, MKT, GateState.SUSPENDED, GateReason.ADMIN, t.r.now);
        final RiskConfigSetCommand rc = t.r.riskConfig(MKT, 1);
        rc.priceCollarBps = 1;
        rc.maxNotional = ONE;
        rc.minQuantity = 2 * ONE;
        rc.maxOpenOrders = 1;
        setRisk(t, rc);
        assertFalse("precondition: the market gate is closed", t.r.engine.gate().admits(MKT));

        t.r.markNow();
        tickWithTrade(t, 99 * ONE, PX, 50 * ONE, 2L); // last trade 50: limit 101 is far outside a 1 bps collar
        final List<AssetsEngineEvent.MeDispatch> d = t.dispatches();
        assertEquals("3.2: no risk re-check at trigger, both children go out; got " + t.r.events(), 2, d.size());
        t.assertChild("STOP_LIMIT child", d.get(0), stopLimit, 1, BUY, TriggerRig.WIRE_LIMIT, 101 * ONE, ONE, 0L);
        t.assertChild("STOP_LOSS child", d.get(1), stopLoss, 1, BUY, TriggerRig.WIRE_MARKET, 0L, ONE, hold);
        assertEquals("no OrderReject at the trigger", 0L, t.r.count(AssetsEngineEvent.OrderReject.class));
        assertEquals("both TRIGGERED", 2L, t.r.orders.triggeredCount());
        assertEquals("STOP_LIMIT parent PENDING_NEW", OrderStatus.PENDING_NEW.code(), t.order(stopLimit).status);
        assertEquals("STOP_LOSS parent PENDING_NEW", OrderStatus.PENDING_NEW.code(), t.order(stopLoss).status);
    }

    // ---------------------------------------------------------------- (3) R12 for TRAILING_STOP

    @Test
    public void R12_gtdArmedSellTrailingExpiresInTheSweepAndALaterTickDispatchesNothing() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final CreateOrderCommand c = t.trailing(2, SELL, 2 * ONE, ONE, 0L);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = t.r.now + 5_000L;
        final long id = t.admit(c);
        assertEquals("precondition: in the trailing book", 1, t.r.orders.armedTrailingCount(MKT));

        t.r.advance(5_000L);
        t.r.markNow();
        t.r.sweep();
        assertNull("R12: EXPIRED in the sweep apply, record removed", t.order(id));
        assertEquals("R12: status EXPIRED", OrderStatus.EXPIRED, event(t.r, id, EventKind.EXPIRED).status());
        assertEquals("R12: armedTrailingCount 0", 0, t.r.orders.armedTrailingCount(MKT));
        assertEquals("R12: hold released", 0L, t.r.engine.account(2).locked(BTC));
        assertEquals("R12: the seller is whole", 100 * ONE, t.r.engine.account(2).available(BTC));
        assertEquals("R12: no GTD deadline left", 0, gtdDeadlines(t.r));
        t.assertNoDispatch("R12: no ME command for an armed trailing expiry");

        t.r.markNow();
        t.tick(90 * ONE, 101 * ONE); // arm 100, delta 2: would fire
        t.assertNoDispatch("R12: a firing tick after the expiry dispatches nothing");
        assertEquals("R12: nothing triggered", 0L, t.r.orders.triggeredCount());
    }

    @Test
    public void R12_gtdArmedBuyTrailingExpiryReleasesTheEstimateHold() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final CreateOrderCommand c = t.trailing(1, BUY, 2 * ONE, ONE, 0L);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = t.r.now + 5_000L;
        final long id = t.admit(c);
        assertTrue("precondition: an estimate hold", t.holdRemaining(1, id) > 0);
        t.r.advance(5_000L);
        t.r.markNow();
        t.r.sweep();
        assertNull("R12: EXPIRED and removed", t.order(id));
        assertEquals("R12: armedTrailingCount 0", 0, t.r.orders.armedTrailingCount(MKT));
        assertEquals("R12: USD hold released", 0L, t.r.engine.account(1).locked(USD));
        assertEquals("R12: the buyer is whole", 10_000 * ONE, t.r.engine.account(1).available(USD));
        t.r.markNow();
        t.tick(99 * ONE, 110 * ONE);
        t.assertNoDispatch("R12: nothing fires after the expiry");
    }

    // ---------------------------------------------------------------- (4), (5) quarantine freezes an armed order

    @Test
    public void quarantinedArmedGtdStopIsFrozenPastItsDeadline() {
        final TriggerRig t = triggeredSellStopArmedOnly();
        final long id = 1L;
        t.r.orders.applyQuarantine(0xF1, id, t.r.now);
        t.r.advance(6_000L);
        t.r.markNow();
        t.r.sweep();
        final CoreOrder o = t.order(id);
        assertNotNull("3.7 #6: a quarantined armed order is not expired by the sweep", o);
        assertEquals("3.7 #6: status stays PENDING_TRIGGER (7)", OrderStatus.PENDING_TRIGGER.code(), o.status);
        assertTrue("3.7 #6: the deadline sets the expiry flag", o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertEquals("3.7 #6: no EXPIRED event", 0L, kindCount(t.r, id, EventKind.EXPIRED));
        t.assertNoDispatch("3.7 #6: no ME command");
        assertEquals("3.7 #6: hold intact", ONE, t.holdRemaining(2, id));
        assertEquals("3.7 #6: locked intact", ONE, t.r.engine.account(2).locked(BTC));
        assertEquals("3.7 #6: not in the stop book", 0, t.r.orders.armedStopCount(MKT));
        t.r.markNow();
        t.tick(95 * ONE, 101 * ONE);
        t.assertNoDispatch("a crossing tick still fires nothing");
    }

    /** SELL STOP_LOSS 99 x 1 GTD (now + 5 s) of user 2, armed under bid 100, orderId 1. */
    private static TriggerRig triggeredSellStopArmedOnly() {
        return triggeredSellStop(ONE, true);
    }

    @Test
    public void quarantinedArmedSellTrailingArmDoesNotAdvanceAndNeverFires() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.trailing(2, SELL, 2 * ONE, ONE, 0L));
        assertEquals("precondition: armed at the bid", PX, t.order(id).trailingArmPrice);
        t.r.orders.applyQuarantine(0xF1, id, t.r.now);
        t.tick(105 * ONE, 106 * ONE);
        assertEquals("3.7 #6: the arm does not follow a higher bid while quarantined", PX, t.order(id).trailingArmPrice);
        t.r.markNow();
        t.tick(90 * ONE, 106 * ONE);
        t.assertNoDispatch("3.7 #6: a quarantined trailing order never fires");
        assertEquals("arm still 100", PX, t.order(id).trailingArmPrice);
        assertEquals("still armed status", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
    }

    @Test
    public void quarantinedUnarmedBuyTrailingIsNotArmedByALaterAsk() {
        final TriggerRig t = TriggerRig.funded();
        t.tick(99 * ONE, 0L); // no ask: arm 0
        final long id = t.admit(t.trailing(1, BUY, 2 * ONE, ONE, 50 * ONE));
        assertEquals("precondition: no ask, arm 0", 0L, t.order(id).trailingArmPrice);
        t.r.orders.applyQuarantine(0xF1, id, t.r.now);
        t.tick(99 * ONE, PX);
        assertEquals("3.7 #6: a quarantined trailing order is not armed by a first ask", 0L, t.order(id).trailingArmPrice);
    }

    // ---------------------------------------------------------------- (6) GTD expiry of a triggered child

    @Test
    public void gtdExpiryOfATriggeredChildMintsOneCancelAndTheJournalCancelledEndsItExpired() {
        final TriggerRig t = triggeredSellStop(ONE, true);
        fire(t, 1L);
        OutcomeFeed.outcome(t.r, 1L, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L);
        assertEquals("fixture: child leg learned", CHILD_LEG, t.order(1L).currentLegId);

        t.r.advance(5_000L);
        t.r.markNow();
        t.r.sweep();
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("GTD of the triggered child");
        assertEquals("S5 3.7: a CANCEL", DispatchKind.CANCEL, d.kind());
        assertEquals("S5 3.7: idLow = attempt 2 << 2 | CANCEL", (2L << 2) | KIND_CANCEL, d.commandIdLow());
        assertEquals("S5 3.7: the cancel names the child leg", CHILD_LEG, d.oldOrderId());
        assertEquals("S5 3.7: DISPATCHED", 1L, kindCount(t.r, 1L, EventKind.DISPATCHED));
        assertTrue("flags", t.order(1L).has(CoreOrder.F_EXPIRY_REQUESTED) && t.order(1L).has(CoreOrder.F_CANCEL_PENDING));
        assertEquals("S5 3.2: no terminal yet", 0L, kindCount(t.r, 1L, EventKind.EXPIRED));

        t.r.markNow();
        OutcomeFeed.terminal(t.r, 1L, CHILD_LEG, ME_CANCELLED);
        assertNull("S5 3.2: CANCELLED(3) with EXPIRY_REQUESTED ends the parent", t.order(1L));
        assertEquals("S5 3.2: terminal EXPIRED", OrderStatus.EXPIRED, event(t.r, 1L, EventKind.EXPIRED).status());
        assertEquals("no CANCELLED event", 0L, kindCount(t.r, 1L, EventKind.CANCELLED));
        assertEquals("hold released", 0L, t.r.engine.account(2).locked(BTC));
        assertEquals("seller whole", 100 * ONE, t.r.engine.account(2).available(BTC));
        t.assertNoDispatch("no second command");
    }

    @Test
    public void gtdExpiryOfATriggeredChildBeforeItsAckMintsTheCancelAtTheAck() {
        final TriggerRig t = triggeredSellStop(ONE, true);
        fire(t, 1L);
        t.r.advance(5_000L);
        t.r.markNow();
        t.r.sweep();
        t.assertNoDispatch("S5 3.7: the create is unresolved, the deadline only sets the flags");
        assertNotNull("still live", t.order(1L));
        t.r.markNow();
        OutcomeFeed.outcome(t.r, 1L, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L);
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("cancel at the child ACK");
        assertEquals("S5 3.3: CANCEL minted at the ACK", DispatchKind.CANCEL, d.kind());
        assertEquals("names the child leg", CHILD_LEG, d.oldOrderId());
        OutcomeFeed.outcome(t.r, 1L, 2, KIND_CANCEL, APPLIED, CHILD_LEG, CHILD_LEG);
        assertNull("CANCEL APPLIED ends it", t.order(1L));
        assertEquals("EXPIRED", 1L, kindCount(t.r, 1L, EventKind.EXPIRED));
        assertEquals("hold released", 0L, t.r.engine.account(2).locked(BTC));
    }

    // ---------------------------------------------------------------- (7) 3.6 non-REJECTED branch

    @Test
    public void G9_journalCancelledForUnlearnedChildLegLearnsAcksThenCancels() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long id = t.admit(t.stopLoss(1, BUY, PX, ONE));
        t.r.markNow();
        t.tick(99 * ONE, PX);
        t.assertTriggered("fixture", id, t.onlyDispatch("fixture"));
        final long stale = t.r.orders.staleOutcomeCount();
        final long redundant = t.r.orders.redundantOutcomeCount();

        t.r.markNow();
        t.r.orders.applyTerminal(88L, CHILD_LEG, 1, MKT, ME_CANCELLED, t.r.now, id, 0L, t.r.now);
        final List<AssetsEngineEvent.OrderEvent> ev = orderEvents(t, id);
        assertEquals("3.6: ACK_NEW then CANCELLED, got " + ev, 2, ev.size());
        assertEquals("3.6: first ACK_NEW (a cancelled leg did rest)", EventKind.ACK_NEW, ev.get(0).kind());
        assertEquals("3.6: the ACK_NEW names the learned leg", CHILD_LEG, ev.get(0).meOrderId());
        assertEquals("3.6: then CANCELLED", EventKind.CANCELLED, ev.get(1).kind());
        assertEquals("3.6: status CANCELLED", OrderStatus.CANCELLED, ev.get(1).status());
        assertEquals("S5 3.8 #12: holdRemaining 0 on the terminal event", 0L, ev.get(1).holdRemaining());
        assertNull("record removed", t.order(id));
        assertEquals("hold released", 0L, t.r.engine.account(1).locked(USD));
        assertEquals("buyer whole", 10_000 * ONE, t.r.engine.account(1).available(USD));
        t.assertNoDispatch("no ME command");

        t.r.markNow();
        OutcomeFeed.outcome(t.r, id, 1, MKT, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L, 0);
        assertEquals("3.6: the later CREATE outcome emits nothing, got " + t.r.events(), 0, t.r.events().size());
        assertEquals("3.6: the later CREATE outcome is counted once",
                stale + redundant + 1, t.r.orders.staleOutcomeCount() + t.r.orders.redundantOutcomeCount());
    }

    @Test
    public void G9_journalFilledAfterATradeThatNamedTheUnlearnedLeg() {
        final TriggerRig t = triggeredSellStop(2 * ONE, false);
        t.r.create(t.r.limit(1, MKT, BUY, 99 * ONE, 2 * ONE));
        final long buyer = t.r.lastAcceptedOrderId();
        OutcomeFeed.ack(t.r, buyer, MAKER_LEG);
        fire(t, 1L);
        final long stale = t.r.orders.staleOutcomeCount();
        final long redundant = t.r.orders.redundantOutcomeCount();

        t.r.markNow();
        OutcomeFeed.trade(t.r, 1, MKT, 1L, CHILD_LEG, 2, buyer, MAKER_LEG, 1, 99 * ONE, ONE, false);
        final List<AssetsEngineEvent.OrderEvent> ev = orderEvents(t, 1L);
        assertEquals("S5 3.1: implicit ack by trade then FILL, got " + ev, 2, ev.size());
        assertEquals("ACK_NEW first", EventKind.ACK_NEW, ev.get(0).kind());
        assertEquals("then FILL", EventKind.FILL, ev.get(1).kind());
        assertEquals("partially filled", OrderStatus.PARTIALLY_FILLED, ev.get(1).status());
        assertEquals("leg learned by the trade", CHILD_LEG, t.order(1L).currentLegId);

        t.r.markNow();
        OutcomeFeed.terminal(t.r, 1L, CHILD_LEG, ME_FILLED);
        assertNull("S5 3.2: FILLED ends the parent", t.order(1L));
        final AssetsEngineEvent.OrderFault f = t.r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals("S5 3.2: FILLED with remaining > 0 faults SETTLE", OrderFaultKind.SETTLE, f.kind());
        assertEquals("fault amount = remaining", ONE, f.amount());
        assertEquals("terminal FILLED", OrderStatus.FILLED, event(t.r, 1L, EventKind.FILL).status());
        assertEquals("no second ACK_NEW", 0L, kindCount(t.r, 1L, EventKind.ACK_NEW));
        assertEquals("seller BTC hold released", 0L, t.r.engine.account(2).locked(BTC));
        assertEquals("seller paid exactly 1 BTC", 99 * ONE, t.r.engine.account(2).available(BTC));
        assertEquals("seller received 99 USD", 99 * ONE, t.r.engine.account(2).available(USD));

        t.r.markNow();
        OutcomeFeed.outcome(t.r, 1L, 2, MKT, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L, 0);
        assertEquals("the later CREATE outcome emits nothing, got " + t.r.events(), 0, t.r.events().size());
        assertEquals("the later CREATE outcome is counted once",
                stale + redundant + 1, t.r.orders.staleOutcomeCount() + t.r.orders.redundantOutcomeCount());
    }

    @Test
    public void G9_tradeFillingTheUnlearnedChildFullyMakesTheJournalFilledAndTheOutcomeNoOps() {
        final TriggerRig t = triggeredSellStop(ONE, false);
        t.r.create(t.r.limit(1, MKT, BUY, 99 * ONE, ONE));
        final long buyer = t.r.lastAcceptedOrderId();
        OutcomeFeed.ack(t.r, buyer, MAKER_LEG);
        fire(t, 1L);
        final long staleTerm = t.r.orders.staleTerminalCount();
        final long staleOut = t.r.orders.staleOutcomeCount() + t.r.orders.redundantOutcomeCount();

        t.r.markNow();
        OutcomeFeed.trade(t.r, 1, MKT, 1L, CHILD_LEG, 2, buyer, MAKER_LEG, 1, 99 * ONE, ONE, false);
        final List<AssetsEngineEvent.OrderEvent> ev = orderEvents(t, 1L);
        assertEquals("ACK_NEW then FILL(FILLED), got " + ev, 2, ev.size());
        assertEquals("ACK_NEW", EventKind.ACK_NEW, ev.get(0).kind());
        assertEquals("FILLED", OrderStatus.FILLED, ev.get(1).status());
        assertNull("parent removed", t.order(1L));
        assertEquals("seller hold released", 0L, t.r.engine.account(2).locked(BTC));

        t.r.markNow();
        t.r.orders.applyTerminal(91L, CHILD_LEG, 2, MKT, ME_FILLED, t.r.now, 1L, 0L, t.r.now);
        OutcomeFeed.outcome(t.r, 1L, 2, MKT, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L, 0);
        assertEquals("the journal FILLED and the outcome emit nothing, got " + t.r.events(), 0, t.r.events().size());
        assertEquals("journal FILLED counted stale", staleTerm + 1, t.r.orders.staleTerminalCount());
        assertEquals("outcome counted once", staleOut + 1,
                t.r.orders.staleOutcomeCount() + t.r.orders.redundantOutcomeCount());
        assertEquals("seller balance unchanged by the late events", 99 * ONE, t.r.engine.account(2).available(BTC));
    }
}

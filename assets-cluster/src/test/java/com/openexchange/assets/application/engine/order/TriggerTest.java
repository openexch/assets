// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static com.openexchange.assets.application.engine.order.TriggerRig.BUY;
import static com.openexchange.assets.application.engine.order.TriggerRig.MKT;
import static com.openexchange.assets.application.engine.order.TriggerRig.SELL;
import static com.openexchange.assets.application.engine.order.TriggerRig.WIRE_LIMIT;
import static com.openexchange.assets.application.engine.order.TriggerRig.WIRE_MARKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 group G2, trigger half (contract 3.1, 3.2; spec R1-R4, R6): a MarketTick is validated, deduped, applied
 * and then evaluated in the same apply; a firing order mints exactly one durable child CREATE whose payload is a
 * pure function of the record; everything that fires in one tick is dispatched in ascending orderId.
 */
public class TriggerTest {

    private static final long PX = 100 * ONE;

    // ---------------------------------------------------------------- R1, R2 and the child table (3.2)

    @Test
    public void R1_buyStopLossFiresOnAskAtStopWithMarketChildBudgetEqualToHold() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long id = t.admit(t.stopLoss(1, BUY, PX, ONE));
        final long hold = t.holdRemaining(1, id);
        assertEquals("contract 1: BUY STOP_LOSS hold = stopPrice x (1 + 500 bps) x qty (exact 8 dp)",
                TriggerRig.buyStopEstimate(PX, ONE), hold);
        assertEquals("contract 2: the armed stop is in market 1's stop book", 1, t.r.orders.armedStopCount(MKT));

        t.r.markNow();
        t.tick(99 * ONE, PX); // bestAsk = 100 >= stop 100
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("R1");
        t.assertChild("R1 BUY STOP_LOSS", d, id, 1, BUY, WIRE_MARKET, 0L, ONE, hold);
        assertTrue("R1 / K11 #1: the MARKET BUY child budget is never 0", d.budget() > 0);
        t.assertTriggered("R1", id, d);
        assertEquals("3.2: the BUY budget is stored in the record at trigger", hold, t.order(id).budget);
        assertEquals("3.2: the trigger moves no money, the hold stays whole", hold, t.holdRemaining(1, id));
        assertEquals("contract 2: a triggered order leaves the stop book", 0, t.r.orders.armedStopCount(MKT));
        assertEquals("contract 4: triggeredCount", 1L, t.r.orders.triggeredCount());
    }

    @Test
    public void R1_buyStopLossDoesNotFireWhileAskBelowStop() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long id = t.admit(t.stopLoss(1, BUY, PX, ONE));
        t.r.markNow();
        t.tick(98 * ONE, 99 * ONE);
        t.tick(98 * ONE, 99_99_999_999L); // 99.99999999 < 100
        t.assertNoDispatch("3.1 #4: a BUY stop fires only when bestAsk >= stopPrice");
        assertEquals("the stop stays armed", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
        assertEquals("the stop stays in the book", 1, t.r.orders.armedStopCount(MKT));
        assertEquals("nothing triggered", 0L, t.r.orders.triggeredCount());
    }

    @Test
    public void R2_buyTrailingFollowsLowerAsksThenFiresAtDeltaWithMarketChild() {
        final TriggerRig t = TriggerRig.quoted(99 * ONE, PX);
        final long budget = 200 * ONE;
        final long id = t.admit(t.trailing(1, BUY, 5 * ONE, ONE, budget));
        assertEquals("contract 1: BUY trailing arms at admission from bestAsk", PX, t.order(id).trailingArmPrice);
        assertEquals("contract 2: the trailing order is in the trailing book", 1, t.r.orders.armedTrailingCount(MKT));

        t.r.markNow();
        t.tick(97 * ONE, 98 * ONE);
        assertEquals("3.1 #4: a lower ask advances the BUY arm", 98 * ONE, t.order(id).trailingArmPrice);
        t.tick(95 * ONE, 96 * ONE);
        assertEquals("3.1 #4: the arm follows the next lower ask", 96 * ONE, t.order(id).trailingArmPrice);
        t.tick(99 * ONE, PX); // 100 - 96 = 4 < 5
        assertEquals("3.1 #4: a higher ask below the delta leaves the arm", 96 * ONE, t.order(id).trailingArmPrice);
        t.assertNoDispatch("R2: advancing ticks and an ask within the delta never fire");
        assertEquals("R2: still armed", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);

        t.r.markNow();
        t.tick(100 * ONE, 101 * ONE); // 101 - 96 = 5 >= 5
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("R2");
        t.assertChild("R2 BUY TRAILING", d, id, 1, BUY, WIRE_MARKET, 0L, ONE, budget);
        t.assertTriggered("R2", id, d);
        assertEquals("R2: the child budget is the hold remaining", t.holdRemaining(1, id), d.budget());
        assertEquals("contract 2: a triggered trailing order leaves the book", 0, t.r.orders.armedTrailingCount(MKT));
    }

    @Test
    public void R2_buyTrailingArmingTickNeverFires() {
        final TriggerRig t = TriggerRig.funded(); // no ask at admission: arm 0, explicit budget (no reference needed)
        final long id = t.admit(t.trailing(1, BUY, 5 * ONE, ONE, 200 * ONE));
        assertEquals("contract 1: no ask at admission -> arm 0", 0L, t.order(id).trailingArmPrice);

        t.r.markNow();
        t.tick(90 * ONE, 0L); // ask side empty: no arm, no evaluation
        assertEquals("3.1 #4: an empty ask side never arms a BUY trailing order", 0L, t.order(id).trailingArmPrice);
        t.tick(119 * ONE, 120 * ONE);
        assertEquals("3.1 #4: arm 0 -> arm := ask", 120 * ONE, t.order(id).trailingArmPrice);
        t.assertNoDispatch("3.1 #4: the arming tick never fires");
        t.r.markNow();
        t.tick(125 * ONE, 126 * ONE); // 126 - 120 = 6 >= 5
        t.assertChild("R2 armed late", t.onlyDispatch("R2 armed late"), id, 1, BUY, WIRE_MARKET, 0L, ONE, 200 * ONE);
    }

    @Test
    public void sellStopLossFiresOnBidAtStopWithMarketSellChildBudgetZero() {
        final TriggerRig t = TriggerRig.quoted(101 * ONE, 102 * ONE);
        final long qty = 3 * ONE;
        final long id = t.admit(t.stopLoss(2, SELL, 99 * ONE, qty));
        assertEquals("contract 1: SELL STOP_LOSS holds qty base", qty, t.holdRemaining(2, id));

        t.r.markNow();
        t.tick(99 * ONE, 100 * ONE); // bestBid 99 <= stop 99
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("SELL STOP_LOSS");
        t.assertChild("SELL STOP_LOSS", d, id, 2, SELL, WIRE_MARKET, 0L, qty, 0L);
        t.assertTriggered("SELL STOP_LOSS", id, d);
        assertEquals("contract 2: stop book empty after the trigger", 0, t.r.orders.armedStopCount(MKT));
    }

    @Test
    public void sellTrailingMirrorsOnTheBidAndSkipsAnEmptyBid() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long qty = 2 * ONE;
        final long id = t.admit(t.trailing(2, SELL, 3 * ONE, qty, 0L));
        assertEquals("contract 1: SELL trailing arms at admission from bestBid", PX, t.order(id).trailingArmPrice);

        t.r.markNow();
        t.tick(104 * ONE, 105 * ONE);
        assertEquals("3.1 #4: a higher bid advances the SELL arm (the advance consumes the tick)", 104 * ONE,
                t.order(id).trailingArmPrice);
        t.tick(0L, 105 * ONE); // bid side empty: 104 - 0 >= 3 must NOT fire
        assertEquals("2.5: an empty bid neither advances nor evaluates", 104 * ONE, t.order(id).trailingArmPrice);
        t.tick(102 * ONE, 105 * ONE); // 104 - 102 = 2 < 3
        t.assertNoDispatch("SELL trailing: advance, empty side and a dip within delta never fire");
        assertEquals("still armed", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);

        t.r.markNow();
        t.tick(101 * ONE, 105 * ONE); // 104 - 101 = 3 >= 3
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("SELL TRAILING");
        t.assertChild("SELL TRAILING", d, id, 2, SELL, WIRE_MARKET, 0L, qty, 0L);
        t.assertTriggered("SELL TRAILING", id, d);
        assertEquals("contract 2: trailing book empty after the trigger", 0, t.r.orders.armedTrailingCount(MKT));
    }

    @Test
    public void stopLimitFiresALimitChildAtItsPrice() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long id = t.admit(t.stopLimit(1, BUY, PX, 101 * ONE, 2 * ONE));
        assertEquals("contract 1: BUY STOP_LIMIT holds price x qty", 202 * ONE, t.holdRemaining(1, id));

        t.r.markNow();
        t.tick(99 * ONE, PX);
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("STOP_LIMIT");
        t.assertChild("STOP_LIMIT", d, id, 1, BUY, WIRE_LIMIT, 101 * ONE, 2 * ONE, 0L);
        t.assertTriggered("STOP_LIMIT", id, d);
    }

    // ---------------------------------------------------------------- R3, R4: the true top of book only

    @Test
    public void R3_bidStayingAboveSellStopNeverFires() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        t.r.markNow();
        for (int i = 0; i < 50; i++) {
            t.tick(PX, (101 + i) * ONE); // deeper-level churn reaches the core only as unchanged bid 100
        }
        t.assertNoDispatch("R3: bestBid 100 above a SELL stop at 99 never fires");
        assertEquals("R3: still armed", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
        assertEquals("R3: still in the book", 1, t.r.orders.armedStopCount(MKT));
        assertEquals("R3: the ticks were applied (not stale)", t.version, t.r.orders.market(MKT).bookVersion);
    }

    @Test
    public void R4_bidFallingThroughSellStopFires() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        t.r.markNow();
        t.tick(97 * ONE, 101 * ONE); // best bid 100 deleted, next bid 97
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("R4");
        t.assertChild("R4", d, id, 2, SELL, WIRE_MARKET, 0L, ONE, 0L);
        t.assertTriggered("R4", id, d);
    }

    // ---------------------------------------------------------------- R6 and invalid ticks (3.1 #1, #2)

    @Test
    public void R6_sameOrLowerBookVersionIsDroppedAndAppliesOnce() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        final MarketRisk m = t.r.orders.market(MKT);
        final long v = t.version;
        final long stale0 = t.r.orders.staleTicks();

        t.r.markNow();
        t.r.tick(MKT, 98 * ONE, 101 * ONE, v);     // same version, a firing bid: dropped
        t.r.tick(MKT, 97 * ONE, 101 * ONE, v - 1); // lower version: dropped
        t.assertNoDispatch("R6: a stale tick is never evaluated");
        assertEquals("R6: two stale drops counted", stale0 + 2, t.r.orders.staleTicks());
        assertEquals("R6: market bestBid unchanged by stale ticks", PX, m.bestBid);
        assertEquals("R6: market bestAsk unchanged", 101 * ONE, m.bestAsk);
        assertEquals("R6: market bookVersion unchanged", v, m.bookVersion);
        assertEquals("R6: still armed after stale ticks", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);

        t.r.markNow();
        t.r.tick(MKT, 98 * ONE, 101 * ONE, v + 1); // forwarder A
        t.r.tick(MKT, 98 * ONE, 101 * ONE, v + 1); // forwarder B, same tick
        final AssetsEngineEvent.MeDispatch d = t.onlyDispatch("R6 duplicated tick");
        t.assertChild("R6", d, id, 2, SELL, WIRE_MARKET, 0L, ONE, 0L);
        assertEquals("R6: the second copy is a counted stale drop", stale0 + 3, t.r.orders.staleTicks());
        assertEquals("R6: one trigger", 1L, t.r.orders.triggeredCount());
    }

    @Test
    public void invalidTicksAreCountedAndChangeNothing() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        final MarketRisk m = t.r.orders.market(MKT);
        final long v = t.version;
        final long stale0 = t.r.orders.staleTicks();
        final long now = t.r.now;

        t.r.markNow();
        t.r.orders.applyMarketTick(MKT, v + 1, -1L, 101 * ONE, 0L, 0L, 0L, now);        // negative bid
        t.r.orders.applyMarketTick(MKT, v + 1, PX, -1L, 0L, 0L, 0L, now);               // negative ask
        t.r.orders.applyMarketTick(MKT, v + 1, 98 * ONE, 97 * ONE, 0L, 0L, 0L, now);    // crossed, the bid would fire
        t.r.orders.applyMarketTick(MKT, v + 1, 98 * ONE, 98 * ONE, 0L, 0L, 0L, now);    // locked (bid == ask)
        t.r.orders.applyMarketTick(MKT, v + 1, PX, 101 * ONE, -1L, 9L, 0L, now);        // negative lastTradePrice
        t.r.orders.applyMarketTick(0, v + 1, PX, 101 * ONE, 0L, 0L, 0L, now);           // unknown market
        t.r.orders.applyMarketTick(OrderEngine.MARKETS + 1, v + 1, PX, 101 * ONE, 0L, 0L, 0L, now);

        t.assertNoDispatch("3.1 #1: an invalid tick is never evaluated, even a crossed one whose bid would fire");
        assertEquals("3.1 #1: every invalid tick is counted in invalidTickCount", 7L, t.r.orders.invalidTickCount());
        assertEquals("3.1 #1: an invalid tick is not a stale drop", stale0, t.r.orders.staleTicks());
        assertEquals("3.1 #1: bestBid unchanged", PX, m.bestBid);
        assertEquals("3.1 #1: bestAsk unchanged", 101 * ONE, m.bestAsk);
        assertEquals("3.1 #1: bookVersion unchanged (validation runs before the dedupe)", v, m.bookVersion);
        assertEquals("3.1 #1: lastTradeId unchanged", 0L, m.lastTradeId);
        assertEquals("3.1 #1: the stop is still armed", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);

        t.r.markNow();
        t.r.tick(MKT, 98 * ONE, 101 * ONE, v + 1); // the version an invalid tick carried is still free
        t.assertChild("valid tick after invalid ones", t.onlyDispatch("valid tick after invalid ones"), id, 2, SELL,
                WIRE_MARKET, 0L, ONE, 0L);
    }

    // ---------------------------------------------------------------- firing order (3.1 #5)

    @Test
    public void allOrdersFiredByOneTickAreDispatchedInAscendingOrderId() {
        final TriggerRig t = TriggerRig.quoted(PX, 100_50_000_000L); // bid 100, ask 100.5
        final long lowSellStop = t.admit(t.stopLoss(2, SELL, 98_50_000_000L, ONE));       // 98.5
        final long buyStopLimit = t.admit(t.stopLimit(1, BUY, 101 * ONE, 102 * ONE, ONE));
        final long sellTrailing = t.admit(t.trailing(2, SELL, 2 * ONE, ONE, 0L));          // arm 100
        final long highSellStop = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        assertEquals("precondition: three stops armed", 3, t.r.orders.armedStopCount(MKT));
        assertEquals("precondition: one trailing armed", 1, t.r.orders.armedTrailingCount(MKT));

        t.r.markNow();
        t.tick(98 * ONE, 101 * ONE); // all four fire: bid 98 <= 98.5, 99; 100 - 98 >= 2; ask 101 >= 101
        final List<AssetsEngineEvent.MeDispatch> d = t.dispatches();
        assertEquals("3.1 #5: four children from one tick, got " + t.r.events(), 4, d.size());
        final long[] expected = {lowSellStop, buyStopLimit, sellTrailing, highSellStop};
        for (int i = 0; i < 4; i++) {
            assertEquals("3.1 #5: dispatch " + i + " is in ascending orderId (not price order, not side order)",
                    expected[i], d.get(i).omsOrderId());
            if (i > 0) {
                assertEquals("3.1 #5: dispatchSeq follows the same order", d.get(i - 1).dispatchSeq() + 1, d.get(i).dispatchSeq());
            }
        }
        t.assertChild("ordered BUY STOP_LIMIT", d.get(1), buyStopLimit, 1, BUY, WIRE_LIMIT, 102 * ONE, ONE, 0L);
        t.assertChild("ordered SELL TRAILING", d.get(2), sellTrailing, 2, SELL, WIRE_MARKET, 0L, ONE, 0L);
        assertEquals("contract 4: four triggered", 4L, t.r.orders.triggeredCount());
        assertEquals("both books empty", 0, t.r.orders.armedStopCount(MKT) + t.r.orders.armedTrailingCount(MKT));
        for (final long id : expected) {
            assertEquals("each parent gets one TRIGGERED", 1L, OutcomeFeed.kindCount(t.r, id, EventKind.TRIGGERED));
        }
    }

    // ---------------------------------------------------------------- quarantine (3.2 last line)

    @Test
    public void quarantinedArmedStopNeverFires() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        final long trail = t.admit(t.trailing(2, SELL, ONE, ONE, 0L));
        t.r.orders.applyQuarantine(0xF1, id, t.r.now);
        t.r.orders.applyQuarantine(0xF2, trail, t.r.now);
        assertEquals("contract 2: a quarantined order leaves the stop book", 0, t.r.orders.armedStopCount(MKT));
        assertEquals("contract 2: a quarantined order leaves the trailing book", 0, t.r.orders.armedTrailingCount(MKT));

        t.r.markNow();
        t.tick(95 * ONE, 101 * ONE);
        t.assertNoDispatch("3.2: a quarantined armed order never fires");
        assertNotNull("the quarantined record stays live", t.order(id));
        assertEquals("still PENDING_TRIGGER", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
        assertEquals("nothing triggered", 0L, t.r.orders.triggeredCount());
        assertEquals("the hold is untouched by the quarantine", ONE, t.holdRemaining(2, id));
        assertEquals("no TRIGGERED event", 0L, OutcomeFeed.kindCount(t.r, id, EventKind.TRIGGERED));
        assertEquals("the buyer's USD is untouched", 10_000 * ONE, t.r.engine.account(1).available(USD));
    }
}

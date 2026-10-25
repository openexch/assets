// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.APPLIED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.KIND_CREATE;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.REJECTED;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.event;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.gtdDeadlines;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.kindCount;
import static com.openexchange.assets.application.engine.order.TriggerRig.BUY;
import static com.openexchange.assets.application.engine.order.TriggerRig.MKT;
import static com.openexchange.assets.application.engine.order.TriggerRig.SELL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 group G2, lifecycle half (contract 3.2 tail, 3.3, 3.5, 3.6; spec R1 continued, R9, R12; critic D8; G9):
 * once triggered, the child runs the S5 machinery (replay identity, outcome, fills, terminal release); an armed
 * order is cancelled or expired without any ME command; a journal trade moves the collar reference.
 */
public class TriggerLifecycleTest {

    private static final long PX = 100 * ONE;
    private static final long CHILD_LEG = 501L;
    private static final long MAKER_LEG = 602L;
    private static final int ME_REJECTED_STATUS = 4;

    /** BUY STOP_LOSS 100 x 1 of user 1 (orderId 1) armed under ask 95, then fired by ask 100. */
    private static TriggerRig triggeredBuyStop() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long id = t.admit(t.stopLoss(1, BUY, PX, ONE));
        assertEquals("fixture: the BUY stop is order 1", 1L, id);
        t.r.markNow();
        t.tick(99 * ONE, PX);
        t.assertTriggered("fixture trigger", id, t.onlyDispatch("fixture trigger"));
        return t;
    }

    /** The resting SELL LIMIT counterparty of user 2 (next orderId) with its leg learned. */
    private static long restingSell(final TriggerRig t, final long price, final long qty) {
        t.r.markNow();
        t.r.create(t.r.limit(2, MKT, SELL, price, qty));
        final long id = t.r.lastAcceptedOrderId();
        OutcomeFeed.ack(t.r, id, MAKER_LEG);
        return id;
    }

    // ---------------------------------------------------------------- replay identity (3.2)

    @Test
    public void pendingDispatchReplaysTheChildPayloadByteIdentically() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long stop = t.admit(t.stopLoss(1, BUY, PX, ONE));
        final long stopLimit = t.admit(t.stopLimit(1, BUY, 96 * ONE, 97 * ONE, 2 * ONE));
        t.r.markNow();
        t.tick(99 * ONE, PX); // both fire
        final List<AssetsEngineEvent.MeDispatch> sent = t.dispatches();
        assertEquals("precondition: two children, got " + t.r.events(), 2, sent.size());
        t.assertChild("replay MARKET child", sent.get(0), stop, 1, BUY, TriggerRig.WIRE_MARKET, 0L, ONE,
                t.holdRemaining(1, stop));
        t.assertChild("replay LIMIT child", sent.get(1), stopLimit, 1, BUY, TriggerRig.WIRE_LIMIT, 97 * ONE, 2 * ONE, 0L);

        t.r.markNow();
        t.r.orders.applyRequestPendingDispatch(0xD1, 0L, 0);
        final List<AssetsEngineEvent.MeDispatchReply> replay = t.replies();
        assertEquals("3.2: both children are pending and replayed, got " + t.r.events(), 2, replay.size());
        for (int i = 0; i < 2; i++) {
            final AssetsEngineEvent.MeDispatch d = sent.get(i);
            final AssetsEngineEvent.MeDispatchReply p = replay.get(i);
            final String sentLine = d.render().substring("MEDISPATCH ".length());
            final String replayLine = p.render().substring("DISPATCHREPLY ".length());
            assertEquals("3.2: RequestPendingDispatch re-emits the child exactly as dispatched (the ME binds the payload "
                    + "to the id): orderType MARKET/LIMIT, price, quantity, budget, same dispatchSeq", sentLine, replayLine);
        }
        final AssetsEngineEvent.PendingDispatchEnd end = t.r.first(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals("the end frame counts both", 2, end.count());
        assertEquals("head = engine dispatchSeq", t.r.engine.getDispatchSeq(), end.headDispatchSeq());
    }

    // ---------------------------------------------------------------- R1 continued, R9: fill and residual release

    @Test
    public void R1_childAppliedFilledParentFilledResidualReleased() {
        final TriggerRig t = triggeredBuyStop();
        final long hold = t.holdRemaining(1, 1);
        assertEquals("fixture: hold = 100 x 1.05 x 1", TriggerRig.buyStopEstimate(PX, ONE), hold);
        final long maker = restingSell(t, PX, ONE);

        t.r.markNow();
        OutcomeFeed.outcome(t.r, 1, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L);
        event(t.r, 1, EventKind.ACK_NEW);
        assertEquals("S5 3.3: the child leg is learned from the attempt-1 outcome", CHILD_LEG, t.order(1).currentLegId);
        assertEquals("S5 3.3: status NEW", OrderStatus.NEW.code(), t.order(1).status);

        t.r.markNow();
        OutcomeFeed.trade(t.r, 1, MKT, 1, CHILD_LEG, 1, maker, MAKER_LEG, 2, PX, ONE, true);
        assertNull("R1: the parent is FILLED and removed", t.order(1));
        final AssetsEngineEvent.OrderEvent fill = event(t.r, 1, EventKind.FILL);
        assertEquals("R1: the FILL event carries status FILLED", OrderStatus.FILLED, fill.status());
        assertEquals("R1: true filled quantity", ONE, fill.filledQty());
        assertEquals("R1: the residual (budget above the cost) is released in the FILLED apply", 0L, fill.holdRemaining());
        assertEquals("R1: the buyer locks nothing", 0L, t.r.engine.account(1).locked(USD));
        assertEquals("R1: the buyer paid exactly the trade cost", 10_000 * ONE - PX, t.r.engine.account(1).available(USD));
        assertEquals("R1: the buyer received the base", ONE, t.r.engine.account(1).available(BTC));
        new FeedRig(t.r).assertLockedEqualsHolds(1);
    }

    @Test
    public void R9_buyStopLimitFilledBelowItsLimitReleasesTheResidual() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long qty = 2 * ONE;
        final long id = t.admit(t.stopLimit(1, BUY, PX, PX, qty));
        assertEquals("contract 1: hold = price x qty", 200 * ONE, t.holdRemaining(1, id));
        t.r.markNow();
        t.tick(99 * ONE, PX);
        t.assertTriggered("R9", id, t.onlyDispatch("R9"));
        final long maker = restingSell(t, 99 * ONE, qty);
        OutcomeFeed.outcome(t.r, id, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L);

        t.r.markNow();
        OutcomeFeed.trade(t.r, 1, MKT, id, CHILD_LEG, 1, maker, MAKER_LEG, 2, 99 * ONE, qty, true);
        assertNull("R9: parent FILLED and removed", t.order(id));
        assertEquals("R9: FILLED", OrderStatus.FILLED, event(t.r, id, EventKind.FILL).status());
        assertEquals("R9: residual qty x 1 released, nothing stays locked", 0L, t.r.engine.account(1).locked(USD));
        assertEquals("R9: the buyer paid 99 x 2", 10_000 * ONE - 198 * ONE, t.r.engine.account(1).available(USD));
        new FeedRig(t.r).assertLockedEqualsHolds(1);
    }

    // ---------------------------------------------------------------- D8 / G9: the ME refuses the child

    @Test
    public void D8_childRejectedByOutcomeEndsParentRejectedAndReleases() {
        final TriggerRig t = triggeredBuyStop();
        t.r.markNow();
        OutcomeFeed.outcome(t.r, 1, 1, KIND_CREATE, REJECTED, 0L, 0L);
        assertNull("D8: the parent is terminal and removed", t.order(1));
        final AssetsEngineEvent.OrderEvent rej = event(t.r, 1, EventKind.REJECTED);
        assertEquals("D8: status REJECTED", OrderStatus.REJECTED, rej.status());
        assertEquals("D8: reason ME_REJECTED", CoreRejectReason.ME_REJECTED, rej.reason());
        assertEquals("D8: hold released", 0L, t.r.engine.account(1).locked(USD));
        assertEquals("D8: the buyer is whole", 10_000 * ONE, t.r.engine.account(1).available(USD));
    }

    @Test
    public void G9_journalRejectedForUnlearnedChildLegBeforeTheOutcome() {
        final TriggerRig t = triggeredBuyStop();
        final long staleOutcomes = t.r.orders.staleOutcomeCount();
        t.r.markNow();
        t.r.orders.applyTerminal(77L, CHILD_LEG, 1, MKT, ME_REJECTED_STATUS, t.r.now, 1L, 0L, t.r.now);
        assertNull("3.6: the terminal for the not-yet-learned child leg ends the parent", t.order(1));
        final AssetsEngineEvent.OrderEvent rej = event(t.r, 1, EventKind.REJECTED);
        assertEquals("3.6: status REJECTED", OrderStatus.REJECTED, rej.status());
        assertEquals("3.6: reason ME_REJECTED", CoreRejectReason.ME_REJECTED, rej.reason());
        assertEquals("3.6: a refused leg never rested, so no ACK_NEW", 0L, kindCount(t.r, 1, EventKind.ACK_NEW));
        assertEquals("3.6: hold released", 0L, t.r.engine.account(1).locked(USD));
        assertEquals("3.6: the buyer is whole", 10_000 * ONE, t.r.engine.account(1).available(USD));

        t.r.markNow();
        OutcomeFeed.outcome(t.r, 1, 1, MKT, 1, KIND_CREATE, REJECTED, 0L, 0L, 0);
        assertEquals("3.6: the later CREATE outcome emits nothing, got " + t.r.events(), 0, t.r.events().size());
        assertEquals("3.6: the later CREATE outcome is a counted no-op", staleOutcomes + 1, t.r.orders.staleOutcomeCount());
    }

    // ---------------------------------------------------------------- R12, C8, C9: armed orders end without the ME

    @Test
    public void R12_gtdArmedSellStopExpiresInTheSweepAndNeverFiresAfter() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final CreateOrderCommand c = t.stopLoss(2, SELL, 99 * ONE, ONE);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = t.r.now + 5_000L;
        final long id = t.admit(c);
        assertEquals("precondition: one GTD deadline", 1, gtdDeadlines(t.r));

        t.r.advance(5_000L);
        t.r.markNow();
        t.r.sweep();
        assertNull("R12: EXPIRED in the sweep apply, record removed", t.order(id));
        final AssetsEngineEvent.OrderEvent exp = event(t.r, id, EventKind.EXPIRED);
        assertEquals("R12: status EXPIRED", OrderStatus.EXPIRED, exp.status());
        t.assertNoDispatch("R12: no ME command for an armed order's expiry");
        assertEquals("R12: hold released", 0L, t.r.engine.account(2).locked(BTC));
        assertEquals("R12: removed from the stop book", 0, t.r.orders.armedStopCount(MKT));
        assertEquals("R12: no GTD deadline left", 0, gtdDeadlines(t.r));

        t.r.markNow();
        t.tick(95 * ONE, 101 * ONE);
        t.assertNoDispatch("R12: a crossing tick after the expiry dispatches nothing");
        assertEquals("R12: nothing triggered", 0L, t.r.orders.triggeredCount());
    }

    @Test
    public void C8_cancelArmedStopIsCancelledInTheSameApplyWithoutMeCommand() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.stopLoss(2, SELL, 99 * ONE, ONE));
        assertEquals("precondition: the stop takes a slot", 1, t.r.orders.orders().openCount(2));

        t.r.markNow();
        t.r.cancel(2, id);
        final AssetsEngineEvent.OrderAck ack = t.r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals("C8: the cancel is acknowledged PENDING_CANCEL", OrderStatus.PENDING_CANCEL, ack.status());
        assertNull("C8: CANCELLED in the same apply, record removed", t.order(id));
        assertEquals("C8: status CANCELLED", OrderStatus.CANCELLED, event(t.r, id, EventKind.CANCELLED).status());
        t.assertNoDispatch("C8: no ME command for an armed order");
        assertEquals("C8: hold released", 0L, t.r.engine.account(2).locked(BTC));
        assertEquals("C8: slot released", 0, t.r.orders.orders().openCount(2));
        assertEquals("C8: out of the stop book", 0, t.r.orders.armedStopCount(MKT));

        t.r.markNow();
        t.tick(95 * ONE, 101 * ONE);
        t.assertNoDispatch("C8: a crossing tick after the cancel dispatches nothing");
    }

    @Test
    public void C8_cancelArmedTrailingLeavesTheTrailingBook() {
        final TriggerRig t = TriggerRig.quoted(PX, 101 * ONE);
        final long id = t.admit(t.trailing(2, SELL, 2 * ONE, ONE, 0L));
        t.r.markNow();
        t.r.cancel(2, id);
        assertNull("C8: trailing CANCELLED in the cancel apply", t.order(id));
        assertTrue("C8: a CANCELLED event", kindCount(t.r, id, EventKind.CANCELLED) == 1L);
        assertEquals("C8: out of the trailing book", 0, t.r.orders.armedTrailingCount(MKT));
        t.assertNoDispatch("C8: no ME command");
        t.r.markNow();
        t.tick(90 * ONE, 101 * ONE);
        t.assertNoDispatch("C8: a crossing tick after the cancel dispatches nothing");
    }

    @Test
    public void amendOfArmedSyntheticIsNotOpen() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long id = t.admit(t.stopLimit(1, BUY, PX, PX, ONE));
        final AmendSupport a = new AmendSupport(t.r);
        t.r.markNow();
        a.amend(1, id, 0L, 2 * ONE);
        assertEquals("3.3: AmendOrder on a synthetic type is NOT_OPEN", CoreRejectReason.NOT_OPEN, a.rejectOnly("amend armed"));
        assertEquals("3.3: the record is unchanged", ONE, t.order(id).quantity);
        assertEquals("3.3: still armed", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
    }

    @Test
    public void amendOfTriggeredStopLimitWithLearnedLegIsNotOpen() {
        final TriggerRig t = TriggerRig.quoted(94 * ONE, 95 * ONE);
        final long id = t.admit(t.stopLimit(1, BUY, PX, PX, ONE));
        t.r.markNow();
        t.tick(99 * ONE, PX);
        t.assertTriggered("amend fixture", id, t.onlyDispatch("amend fixture"));
        OutcomeFeed.outcome(t.r, id, 1, KIND_CREATE, APPLIED, CHILD_LEG, 0L);
        final AmendSupport a = new AmendSupport(t.r);
        t.r.markNow();
        a.amend(1, id, 99 * ONE, 0L);
        assertEquals("3.3: a triggered STOP_LIMIT is still a synthetic parent: NOT_OPEN", CoreRejectReason.NOT_OPEN,
                a.rejectOnly("amend triggered"));
        assertEquals("3.3: the price is unchanged", PX, t.order(id).price);
    }

    // ---------------------------------------------------------------- 3.5: market state from fills

    @Test
    public void journalTradeSetsTheCollarReferenceAndAnOlderTickDoesNotOverwriteIt() {
        final FeedRig f = FeedRig.pair(1_000 * ONE, 10 * ONE, 105 * ONE, 2 * ONE, PX, 2 * ONE);
        final MarketRisk m = f.r.orders.market(MKT);
        f.sellHits(1, PX, ONE);
        assertEquals("3.5: a journal trade sets market.lastTradePrice", PX, m.lastTradePrice);
        assertEquals("3.5: and market.lastTradeId", 1L, m.lastTradeId);
        f.sellHits(2, 101 * ONE, ONE);
        assertEquals("3.5: the newer trade moves the reference", 101 * ONE, m.lastTradePrice);
        assertEquals("3.5: lastTradeId 2", 2L, m.lastTradeId);

        f.r.orders.applyMarketTick(MKT, 1L, 99 * ONE, 102 * ONE, 99 * ONE, 1L, 0L, f.r.now);
        assertEquals("3.5: a tick whose lastTradeId is older keeps the trade's price", 101 * ONE, m.lastTradePrice);
        assertEquals("3.5: and its id", 2L, m.lastTradeId);
        assertEquals("3.5: the quote part of the tick still applies", 99 * ONE, m.bestBid);
        f.r.orders.applyMarketTick(MKT, 2L, 99 * ONE, 102 * ONE, 103 * ONE, 3L, 0L, f.r.now);
        assertEquals("3.5: a newer tick lastTrade applies", 103 * ONE, m.lastTradePrice);
        assertEquals("3.5: newer id", 3L, m.lastTradeId);
    }

    @Test
    public void journalTradeMovesTheCollarWithoutATick() {
        final FeedRig f = FeedRig.pair(10_000 * ONE, 10 * ONE, 105 * ONE, 2 * ONE, PX, 2 * ONE);
        f.sellHits(1, PX, ONE);
        f.r.markNow();
        f.r.create(f.r.limit(1, MKT, BUY, 150 * ONE, ONE)); // 50% away from the 100 trade, collar 20%
        final List<AssetsEngineEvent> e = f.r.events();
        assertTrue("3.5: the collar reference follows the settled trade with no tick, so BUY LIMIT 150 against a 100 "
                        + "trade (collar 20%) is one OrderReject PRICE_COLLAR; got " + e,
                e.size() == 1 && e.get(0) instanceof AssetsEngineEvent.OrderReject rej
                        && rej.reason() == CoreRejectReason.PRICE_COLLAR);
        assertNotNull("the pair's buy order is still live", f.r.orders.orders().get(1));
    }
}

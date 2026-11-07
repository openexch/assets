// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.AmendSupport.LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.PRICE;
import static com.openexchange.assets.application.engine.order.AmendSupport.SELL_LEG;
import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.BUY;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.SELL;
import static com.openexchange.assets.application.engine.order.OutcomeFeed.gtdDeadlines;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Shared fixtures and reads of the RC1 S5 GAP suites ({@link FeedEdgeCasesTest}, {@link OutcomeEdgeCasesTest}):
 * event-kind sequences per order, a withdraw, the base BUY with a wide SELL counterparty, and a GTD order whose
 * deadline popped while its replace window was open. Package-private.
 */
final class EdgeSupport {

    /** MeJournalTerminal status EXPIRED (contract 3.2). */
    static final int ME_EXPIRED = 5;

    private EdgeSupport() { }

    static List<EventKind> kinds(final List<AssetsEngineEvent.OrderEvent> events) {
        final List<EventKind> out = new ArrayList<>();
        for (final AssetsEngineEvent.OrderEvent e : events) {
            out.add(e.kind());
        }
        return out;
    }

    /** The OrderEvents of {@code orderId} since the rig's mark, in emission order. */
    static List<AssetsEngineEvent.OrderEvent> eventsOf(final OrderRig r, final long orderId) {
        return new FeedRig(r).orderEvents(orderId);
    }

    static void withdraw(final OrderRig r, final long user, final int asset, final long amount) {
        final WithdrawCommand w = new WithdrawCommand();
        w.reset();
        w.setCorrelationId(0);
        w.setUserId(user);
        w.setAssetId(asset);
        w.setAmount(amount);
        r.engine.applyCommand(AssetsEngine.CMD_WITHDRAW, w, r.now);
    }

    /**
     * {@link AmendSupport#base()} (user 1 BUY 100 x 1, orderId 1, leg 77, dispatch 1) plus user 2 SELL 100 x 2
     * (orderId 2, leg 88, dispatch 2) so a 1.5 fill against the amended BUY is possible.
     */
    static AmendSupport baseWithWideSell() {
        final AmendSupport s = AmendSupport.base();
        s.r.deposit(2, BTC, 2 * ONE);
        s.r.create(s.r.limit(2, 1, SELL, PRICE, 2 * ONE));
        s.createOutcome(2, 2, SELL_LEG);
        assertEquals("precondition: create 1, create 2", 2, s.r.engine.getDispatchSeq());
        return s;
    }

    /**
     * GTD BUY 100 x 1 (orderId 1, expires in 5 s, leg 77), amended to 100 x 2 (AMEND = dispatch 2), then the GTD
     * deadline swept while the window is open: contract 3.8 #9 says flags only, no mint, until the window closes.
     */
    static AmendSupport gtdExpiredInsideTheWindow() {
        final OrderRig r = OrderRig.open();
        final AmendSupport s = new AmendSupport(r);
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, BUY, PRICE, ONE);
        c.timeInForce = OrderEngine.TIF_GTD;
        c.expiresAtMs = r.now + 5_000;
        r.create(c);
        s.createOutcome(1, 1, LEG);
        s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals("precondition (3.4): the AMEND is dispatch 2", 2, r.engine.getDispatchSeq());
        assertEquals(1, gtdDeadlines(r));
        r.advance(5_000);
        r.markNow();
        r.sweep();
        final CoreOrder o = s.order(1);
        assertTrue("3.8 #9: the sweep sets EXPIRY_REQUESTED", o.has(CoreOrder.F_EXPIRY_REQUESTED));
        assertTrue("3.8 #9: and CANCEL_PENDING", o.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue("the window stays open", o.has(CoreOrder.F_REPLACE_PENDING));
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals("the AMEND is still the command in flight", CoreOrder.PK_AMEND, o.pendingKind);
        assertEquals(2, o.pendingDispatchSeq);
        assertEquals("3.8 #9: no mint while the window is open", 0, r.count(AssetsEngineEvent.MeDispatch.class));
        assertEquals("the sweep emits nothing in this state, got " + r.events(), 0, r.events().size());
        assertEquals("dispatchSeq unchanged by the sweep", 2, r.engine.getDispatchSeq());
        assertEquals(1, o.attempt);
        assertEquals("the GTD entry popped once", 0, gtdDeadlines(r));
        return s;
    }
}

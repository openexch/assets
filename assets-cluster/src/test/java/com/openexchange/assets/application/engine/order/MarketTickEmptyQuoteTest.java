// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.CoreRejectReason;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.TriggerRig.BUY;
import static com.openexchange.assets.application.engine.order.TriggerRig.MKT;
import static com.openexchange.assets.application.engine.order.TriggerRig.SELL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S13 11.6 (CL-N-7, C10): W-ME2 publishes an emptied book, so the forwarder can send a {@code (0, 0)} tick at a higher
 * version. The core must treat it as "no quote": no stop or trailing order fires ({@code TriggerBook.collectFiredStops} needs a
 * positive side), a market BUY is refused on {@code bestAsk <= 0}, and {@code locked == sum of holds} after each step.
 */
public class MarketTickEmptyQuoteTest {

    private static void assertLockedEqualsHolds(final TriggerRig t, final String when) {
        for (final long user : new long[] {1, 2}) {
            final var a = t.r.engine.account(user);
            final long[] sum = new long[com.openexchange.assets.domain.Asset.count()];
            a.forEachHold((orderId, assetId, remaining, oms) -> sum[assetId] += remaining);
            for (int asset = 0; asset < sum.length; asset++) {
                assertEquals(when + ": locked == sum of holds, user " + user + " asset " + asset, sum[asset], a.locked(asset));
            }
        }
    }

    @Test
    public void emptyQuoteFiresNothingAndRefusesAMarketBuy() {
        final TriggerRig t = TriggerRig.quoted(99 * ONE, 100 * ONE);
        final long buyStop = t.admit(t.stopLoss(1, BUY, 105 * ONE, ONE));
        final long sellStop = t.admit(t.stopLoss(2, SELL, 90 * ONE, ONE));
        final long trailing = t.admit(t.trailing(1, BUY, 5 * ONE, ONE, 200 * ONE));
        final long armBefore = t.order(trailing).trailingArmPrice;
        assertLockedEqualsHolds(t, "armed");

        t.r.markNow();
        t.tick(0L, 0L); // version 2 > 1: valid, an empty book
        assertEquals("the (0, 0) tick is applied at the higher version", 2L, t.r.orders.market(MKT).bookVersion);
        assertEquals(0L, t.r.orders.market(MKT).bestBid);
        assertEquals(0L, t.r.orders.market(MKT).bestAsk);
        t.assertNoDispatch("a (0, 0) tick fires no stop (TriggerBook needs a positive side)");
        assertEquals("the buy stop stays armed", OrderStatus.PENDING_TRIGGER.code(), t.order(buyStop).status);
        assertEquals("the sell stop stays armed", OrderStatus.PENDING_TRIGGER.code(), t.order(sellStop).status);
        assertEquals("the trailing arm does not move on an empty ask", armBefore, t.order(trailing).trailingArmPrice);
        assertEquals("two stops stay in the stop book", 2, t.r.orders.armedStopCount(MKT));
        assertEquals("one trailing stays in the trailing book", 1, t.r.orders.armedTrailingCount(MKT));
        assertEquals("nothing triggered", 0L, t.r.orders.triggeredCount());
        assertLockedEqualsHolds(t, "after the empty tick");

        // a market BUY without a budget is estimated on the ask; none exists (OrderEngine admission, bestAsk <= 0)
        t.r.markNow();
        t.r.create(t.r.market(1, MKT, BUY, ONE, 0L));
        final AssetsEngineEvent.OrderReject rej = t.r.first(AssetsEngineEvent.OrderReject.class);
        assertEquals(CoreRejectReason.NO_REFERENCE_PRICE, rej.reason());
        assertLockedEqualsHolds(t, "after the refused market buy");

        // the quote returns: the armed orders still react (the empty tick destroyed nothing)
        t.r.markNow();
        t.tick(105 * ONE, 106 * ONE);
        assertTrue("the buy stop fires on the returned ask", t.dispatches().size() >= 1);
        assertLockedEqualsHolds(t, "after the quote returned");
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G6: the amend reference (3.7): expectedOldRemaining at mint, the defensive NOT_OPEN, the v1/v2 wire. */
public class AmendReferenceTest {

    private static final long PX = 100 * ONE;

    @Test
    public void amendOnAMutatedLiveRecordWithZeroRemainingIsNotOpen() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        r.order(h).remainingQty = 0; // the mutated-record hook: unreachable by the live paths (3.7, FIX-5)
        r.mark();
        r.amend(1, h, 0, 2 * ONE);
        assertEquals(CoreRejectReason.NOT_OPEN, r.first(AssetsEngineEvent.OrderReject.class).reason());
        assertEquals("no AMEND minted", 0, dispatches(r, DispatchKind.AMEND).size());
    }

    @Test
    public void amendAfterTheLastFillIsUnknownOrder() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        final long sell = r.create(r.limit(2, 1, PX, ONE));
        r.ack(sell, 0, 202);
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, ONE, false));
        assertNull(r.order(h));
        r.mark();
        r.amend(1, h, 0, 2 * ONE);
        assertEquals(CoreRejectReason.UNKNOWN_ORDER, r.first(AssetsEngineEvent.OrderReject.class).reason());
    }

    @Test
    public void amendCarriesExpectedOldRemaining() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        final long sell = r.create(r.limit(2, 1, PX, ONE));
        r.ack(sell, 0, 202);
        assertTrue(r.trade(1, sell, 202, 2, h, 101, 1, PX, 3 * ONE / 10, false));
        r.mark();
        r.amend(1, h, 0, 2 * ONE);
        final AssetsEngineEvent.MeDispatch d = dispatches(r, DispatchKind.AMEND).get(0);
        assertEquals("quantity = legQty (2 - 0.3)", 17 * ONE / 10, d.quantity());
        assertEquals("expectedOldRemaining = remainingQty at mint", 7 * ONE / 10, d.expectedOldRemaining());
        final CoreOrder o = r.order(h);
        assertEquals("3.7 invariant", o.pendingExpectedOldRemaining, o.quantity - o.pendingQuantity + o.pendingLegQty);
        r.mark();
        r.engine.orders().applyRequestPendingDispatch(1, 0L, 1024);
        final AssetsEngineEvent.MeDispatchReply reply = r.all(AssetsEngineEvent.MeDispatchReply.class).stream()
                .filter(x -> x.kind() == DispatchKind.AMEND).findFirst().orElseThrow();
        assertEquals("the reply carries the minted reference", d.render().replace("MEDISPATCH", "DISPATCHREPLY"), reply.render());
    }

    @Test
    public void v2SendsZero() {
        final V3Rig r = V3Rig.v2();
        r.engine.gate().set(com.openexchange.assets.application.engine.gate.OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        r.deposit(1, V3Rig.USD, 1_000_000 * ONE);
        final long h = r.create(r.limit(1, 0, PX, ONE));
        r.engine.orders().applyOutcome(1, h, 0, 1, h, 0, 0, 0, 0, 1, 0, 0, 101, 0, r.now, 0, r.now);
        r.mark();
        r.amend(1, h, 0, 2 * ONE);
        final AssetsEngineEvent.MeDispatch d = dispatches(r, DispatchKind.AMEND).get(0);
        assertEquals("v1/v2 bytes unchanged: expectedOldRemaining 0", 0L, d.expectedOldRemaining());
        assertEquals("the record keeps the reference on every behaviour", ONE, r.order(h).pendingExpectedOldRemaining);
        assertEquals(2 * ONE, r.order(h).pendingLegQty);
    }
}

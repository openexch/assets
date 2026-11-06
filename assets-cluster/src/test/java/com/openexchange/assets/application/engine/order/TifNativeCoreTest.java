// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G5: native IOC/FOK under behaviour 3 (3.6 admission, afterAck, wire tif; 3.4 EXPIRED). */
public class TifNativeCoreTest {

    private static final long PX = 100 * ONE;

    /** A well-formed SELL of {@code type} with {@code tif} (SELL: no reference price is needed for the price-less types). */
    private static CreateOrderCommand of(final V3Rig r, final int type, final int tif) {
        final CreateOrderCommand c = r.limit(3, 1, PX, ONE);
        c.orderType = type;
        c.timeInForce = tif;
        switch (type) {
            case OrderEngine.TYPE_MARKET: c.price = 0; break;
            case OrderEngine.TYPE_STOP_LOSS: c.price = 0; c.stopPrice = 90 * ONE; break;
            case OrderEngine.TYPE_STOP_LIMIT: c.stopPrice = 95 * ONE; break;
            case OrderEngine.TYPE_TRAILING_STOP: c.price = 0; c.trailingDelta = ONE; break;
            case OrderEngine.TYPE_ICEBERG: c.displayQty = ONE / 2; break;
            default: break;
        }
        if (tif == OrderEngine.TIF_GTD) {
            c.expiresAtMs = r.now + 60_000;
        }
        return c;
    }

    private static boolean admitted(final V3Rig r, final CreateOrderCommand c) {
        r.mark();
        r.engine.orders().applyCreate(c, r.now);
        if (r.count(AssetsEngineEvent.OrderReject.class) > 0) {
            assertEquals("the only refusal of the table is UNSUPPORTED_TIF", CoreRejectReason.UNSUPPORTED_TIF,
                    r.first(AssetsEngineEvent.OrderReject.class).reason());
            return false;
        }
        return true;
    }

    @Test
    public void admissionTable() {
        // rows: LIMIT, MARKET, LIMIT_MAKER, STOP_LOSS, STOP_LIMIT, TRAILING_STOP, ICEBERG; columns GTC, IOC, FOK, GTD
        final boolean[][] yes = {
            {true, true, true, true},
            {true, true, false, true},
            {true, false, false, true},
            {true, false, false, true},
            {true, false, false, true},
            {true, false, false, true},
            {true, false, false, true},
        };
        final int[] tifs = {OrderEngine.TIF_GTC, OrderEngine.TIF_IOC, OrderEngine.TIF_FOK, OrderEngine.TIF_GTD};
        for (int type = 0; type <= 6; type++) {
            for (int t = 0; t < 4; t++) {
                final V3Rig r = funded();
                final CreateOrderCommand c = of(r, type, tifs[t]);
                assertEquals("type " + type + " tif " + tifs[t], yes[type][t], admitted(r, c));
                if (yes[type][t] && tifs[t] == OrderEngine.TIF_FOK) {
                    assertTrue("F_FOK set", r.order(1).has(CoreOrder.F_FOK));
                }
            }
        }
    }

    @Test
    public void iocFokAndMarketNotCancelledAfterAck() {
        for (final int[] row : new int[][] {{OrderEngine.TYPE_LIMIT, OrderEngine.TIF_IOC}, {OrderEngine.TYPE_LIMIT, OrderEngine.TIF_FOK},
                {OrderEngine.TYPE_MARKET, OrderEngine.TIF_GTC}, {OrderEngine.TYPE_MARKET, OrderEngine.TIF_IOC}}) {
            final V3Rig r = funded();
            final long h = r.create(of(r, row[0], row[1]));
            cancel(r, 3, h); // CANCEL_PENDING while the create is pending
            r.mark();
            assertTrue(r.ack(h, 0, 303));
            assertEquals("type " + row[0] + " tif " + row[1] + ": no cancel after the ack (D-S8-6)", 0,
                    dispatches(r, DispatchKind.CANCEL).size());
        }
        // control: a GTC LIMIT asked to cancel while pending gets its single cancel at the ack (C8b)
        final V3Rig r = funded();
        final long h = r.create(of(r, OrderEngine.TYPE_LIMIT, OrderEngine.TIF_GTC));
        cancel(r, 3, h);
        r.mark();
        assertTrue(r.ack(h, 0, 303));
        assertEquals(1, dispatches(r, DispatchKind.CANCEL).size());
    }

    @Test
    public void iocFokCancelledIsExpiredWithoutAckNew() {
        for (final int[] row : new int[][] {{OrderEngine.TIF_IOC, 12}, {OrderEngine.TIF_IOC, 13}, {OrderEngine.TIF_FOK, 7},
                {OrderEngine.TIF_IOC, 14}}) {
            final V3Rig r = funded();
            final long h = r.create(of(r, OrderEngine.TYPE_LIMIT, row[0]));
            r.mark();
            assertTrue(r.terminal(h, 303, 3, V3Rig.ME_CANCELLED, row[1]));
            assertNull(r.order(h));
            assertEquals("no ACK_NEW for a leg that never rested", 0, r.all(AssetsEngineEvent.OrderEvent.class).stream()
                    .filter(e -> e.kind() == EventKind.ACK_NEW).count());
            final AssetsEngineEvent.OrderEvent t = r.first(AssetsEngineEvent.OrderEvent.class);
            assertEquals("tif " + row[0] + " reason " + row[1], EventKind.EXPIRED, t.kind());
            assertEquals(OrderStatus.EXPIRED, t.status());
            assertEquals("meReason verbatim", row[1], t.meReason());
            assertEquals("the hold released", 0L, r.locked(3, V3Rig.BTC));
        }
    }

    @Test
    public void wireTifPerKindMintAndReply() {
        final int[][] create = {{OrderEngine.TIF_GTC, 0}, {OrderEngine.TIF_IOC, 1}, {OrderEngine.TIF_FOK, 2}, {OrderEngine.TIF_GTD, 0}};
        for (final int[] row : create) {
            final V3Rig r = funded();
            r.mark();
            final long h = r.create(of(r, OrderEngine.TYPE_LIMIT, row[0]));
            final AssetsEngineEvent.MeDispatch d = dispatches(r, DispatchKind.CREATE).get(0);
            assertEquals("CREATE tif " + row[0], row[1], d.timeInForce());
            r.mark();
            r.engine.orders().applyRequestPendingDispatch(1, 0L, 1024);
            assertEquals("reply == mint", d.render().replace("MEDISPATCH", "DISPATCHREPLY"),
                    r.first(AssetsEngineEvent.MeDispatchReply.class).render());
            if (row[0] == OrderEngine.TIF_GTD) {
                // CANCEL and AMEND carry 0 (the base wrote the record's 3 on every kind)
                r.ack(h, 0, 303);
                r.amend(3, h, 0, 2 * ONE);
                final AssetsEngineEvent.MeDispatch amend = r.all(AssetsEngineEvent.MeDispatch.class).stream()
                        .filter(x -> x.kind() == DispatchKind.AMEND).findFirst().orElseThrow();
                assertEquals("AMEND tif", 0, amend.timeInForce());
                assertTrue(r.outcome(h, 1, AMEND, APPLIED, 404, 0, false));
                r.mark();
                cancel(r, 3, h);
                assertEquals("CANCEL tif", 0, dispatches(r, DispatchKind.CANCEL).get(0).timeInForce());
            }
        }
    }

    @Test
    public void marketThinBookCancelledReleasesResidual() {
        // reasons 12 and 14 on a MARKET that is not IOC: CANCELLED, residual released, no SETTLE fault (guard)
        for (final int reason : new int[] {12, 14}) {
            final V3Rig r = funded();
            final CreateOrderCommand c = r.limit(1, 0, 0, ONE);
            c.orderType = OrderEngine.TYPE_MARKET;
            c.budget = 150 * ONE;
            final long h = r.create(c);
            final long sell = r.create(r.limit(2, 1, PX, ONE / 2));
            r.ack(sell, 0, 202);
            assertTrue(r.trade(1, h, 101, 1, sell, 202, 2, PX, ONE / 2, true)); // half fills at 100
            r.mark();
            assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, reason));
            assertNull(r.order(h));
            assertEquals(EventKind.CANCELLED, r.first(AssetsEngineEvent.OrderEvent.class).kind());
            assertEquals("residual released", 0L, r.locked(1, V3Rig.USD));
            assertEquals("no SETTLE fault", 0, r.count(AssetsEngineEvent.OrderFault.class));
        }
    }

    @Test
    public void v2KeepsEmulation() {
        final V3Rig r = V3Rig.v2();
        r.engine.gate().set(com.openexchange.assets.application.engine.gate.OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        r.deposit(3, V3Rig.BTC, 100 * ONE);
        assertFalse("FOK refused under v2", admitted(r, of(r, OrderEngine.TYPE_LIMIT, OrderEngine.TIF_FOK)));
        assertTrue("IOC on LIMIT_MAKER accepted under v2", admitted(r, of(r, OrderEngine.TYPE_LIMIT_MAKER, OrderEngine.TIF_IOC)));
        final long h = r.create(of(r, OrderEngine.TYPE_LIMIT, OrderEngine.TIF_IOC));
        r.mark();
        r.engine.orders().applyOutcome(1, h, 0, 3, h, 0, 0, 0, 0, 1, 0, 0, 303, 0, r.now, 0, r.now);
        assertEquals("the IOC cancel is emulated at the ack under v2", 1, dispatches(r, DispatchKind.CANCEL).size());
        assertEquals("v2 wire tif = the record's code", OrderEngine.TIF_IOC, dispatches(r, DispatchKind.CANCEL).get(0).timeInForce());
    }

    // ---------------------------------------------------------------- review BLOCKER-1 (coordinator decision 1)

    /** G5 row: under v3 AmendOrder on an order with F_IOC or F_FOK is NOT_OPEN (the native ME never rests such an order). */
    @Test
    public void iocOrFokAmendIsNotOpen() {
        for (final int tif : new int[] {OrderEngine.TIF_IOC, OrderEngine.TIF_FOK}) {
            final V3Rig r = funded();
            final long h = r.create(tif(r.limit(1, 0, PX, ONE), tif));
            assertTrue(r.ack(h, 0, 101)); // live with a known leg and nothing pending
            r.mark();
            r.amend(1, h, 99 * ONE, 0);
            assertEquals("tif " + tif, CoreRejectReason.NOT_OPEN, r.first(AssetsEngineEvent.OrderReject.class).reason());
            assertEquals("no AMEND minted", 0, dispatches(r, DispatchKind.AMEND).size());
            assertFalse(r.order(h).has(CoreOrder.F_REPLACE_PENDING));
        }
    }

    /**
     * Review BLOCKER-1 inverted (the probe {@code iocAmendBetweenTradeAndKillStrandsTheHold}): a native IOC partially fills
     * (61, implicit ack), a user amend sequenced before the ME's kill terminal is refused NOT_OPEN, so the 63 CANCELLED
     * reason 12 ends the order EXPIRED and releases the residual hold; locked == sum(holds).
     */
    @Test
    public void iocAmendBetweenTradeAndKillReleasesTheHold() {
        final V3Rig r = funded();
        final long sell = r.create(r.limit(2, 1, PX, ONE / 2));
        r.ack(sell, 0, 202);
        final long h = r.create(tif(r.limit(1, 0, PX, ONE), OrderEngine.TIF_IOC));
        assertEquals(PX, r.locked(1, V3Rig.USD));
        assertTrue(r.trade(1, h, 101, 1, sell, 202, 2, PX, ONE / 2, true));
        r.mark();
        r.amend(1, h, 99 * ONE, 0);
        assertEquals("the amend is refused", CoreRejectReason.NOT_OPEN, r.first(AssetsEngineEvent.OrderReject.class).reason());
        assertFalse("no replace window", r.order(h).has(CoreOrder.F_REPLACE_PENDING));
        r.mark();
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, 12));
        assertNull("the kill terminal ends the order", r.order(h));
        assertEquals(EventKind.EXPIRED, r.first(AssetsEngineEvent.OrderEvent.class).kind());
        assertEquals("the residual hold is released", 0L, r.locked(1, V3Rig.USD));
        checkInvariants(r, "IOC kill after a refused amend");
    }
}

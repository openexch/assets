// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.BTC;
import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static com.openexchange.assets.application.engine.feed.V3Rig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 11.1 (D9a; H3/X14, R-B2-7): an AMEND REJECTED whose {@code oldCancelled} is set means the ME cancelled the old leg
 * and placed nothing, so the order terminalizes REJECTED/ME_REJECTED with its true filled quantity and its whole residual
 * hold (top-up, hidden or untriggered remainder included) is released. Without the flag the abort stays as today.
 */
public class AmendOldCancelledTest {

    private static final int RESULT_REJECTED = 1;

    /** LIMIT BUY {@code qty} @ 100 (order 1, user 1) acked at leg 101. */
    private static V3Rig live(final long qty) {
        final V3Rig r = V3Rig.open();
        r.deposit(1, USD, 10_000 * ONE);
        r.deposit(2, BTC, 100 * ONE);
        r.create(r.limit(1, 0, 100 * ONE, qty));
        r.ack(1, 0, 101);
        return r;
    }

    private static int deadlines(final V3Rig r, final long orderId) {
        final int[] n = {0};
        r.engine.deadlines().forEach((d, id, kind) -> {
            if (id == orderId && kind != DeadlineIndex.KIND_REQUEST_TTL) {
                n[0]++;
            }
        });
        return n[0];
    }

    private static AssetsEngineEvent.OrderEvent terminalEvent(final V3Rig r) {
        final List<AssetsEngineEvent.OrderEvent> e = r.all(AssetsEngineEvent.OrderEvent.class);
        assertEquals("exactly one order event, got " + e, 1, e.size());
        return e.get(0);
    }

    @Test
    public void rejectedWithOldCancelledTerminalizes() {
        final V3Rig r = live(ONE);
        r.amend(1, 1, 110 * ONE, 0);
        assertEquals("fixture: the top-up is held", 110 * ONE, r.locked(1, USD));
        assertTrue(r.order(1).has(CoreOrder.F_REPLACE_PENDING));
        r.mark();
        assertTrue(r.outcome(1, 1, V3Rig.KIND_AMEND, RESULT_REJECTED, 0, 7, true));
        assertNull("terminal: the record is gone", r.order(1));
        final AssetsEngineEvent.OrderEvent ev = terminalEvent(r);
        assertEquals(EventKind.REJECTED, ev.kind());
        assertEquals(OrderStatus.REJECTED, ev.status());
        assertEquals(CoreRejectReason.ME_REJECTED, ev.reason());
        assertEquals(0L, ev.filledQty());
        assertEquals("the whole hold, top-up included, is released", 0L, r.locked(1, USD));
        assertEquals(10_000 * ONE, r.total(1, USD));
        assertEquals("window and GTD deadlines removed", 0, deadlines(r, 1));
    }

    @Test
    public void rejectedWithoutOldCancelledStillAborts() {
        final V3Rig r = live(ONE);
        r.amend(1, 1, 110 * ONE, 0);
        r.mark();
        assertTrue(r.outcome(1, 1, V3Rig.KIND_AMEND, RESULT_REJECTED, 0, 7, false));
        assertNotNull("the abort keeps the order live on the old leg", r.order(1));
        assertEquals(EventKind.REPLACED, terminalEvent(r).kind());
        assertEquals("the top-up is given back", 100 * ONE, r.locked(1, USD));
    }

    @Test
    public void noRoomLeftAfterInWindowFillsEndsRejectedWithFills() {
        final V3Rig r = live(2 * ONE);
        r.create(r.limit(2, 1, 100 * ONE, 2 * ONE));
        r.ack(2, 0, 202);
        r.amend(1, 1, 0, ONE + ONE / 2);
        // fills on the old leg inside the window reach the amended quantity: the ME has no room left
        assertTrue(r.trade(1, 2, 202, 2, 1, 101, 1, 100 * ONE, ONE + ONE / 2, false));
        assertEquals(ONE + ONE / 2, r.order(1).filledQty);
        r.mark();
        assertTrue(r.outcome(1, 1, V3Rig.KIND_AMEND, RESULT_REJECTED, 0, 3, true));
        final AssetsEngineEvent.OrderEvent ev = terminalEvent(r);
        assertEquals("REJECTED, never FILLED", EventKind.REJECTED, ev.kind());
        assertEquals(OrderStatus.REJECTED, ev.status());
        assertEquals("the true filled quantity", ONE + ONE / 2, ev.filledQty());
        assertNull(r.order(1));
        assertEquals(0L, r.locked(1, USD));
    }

    @Test
    public void icebergOrStopChildParentEndsAndReleasesRemainder() {
        // an iceberg with a hidden remainder, and a triggered STOP_LIMIT child, each in an amend window (constructed: the
        // admission path refuses AmendOrder on synthetic types, the ME outcome rule still has to hold for them)
        for (final String kind : new String[] {"iceberg", "stop child"}) {
            final V3Rig r = V3Rig.open();
            r.deposit(2, BTC, 100 * ONE);
            final long id;
            if (kind.equals("iceberg")) {
                id = r.create(r.iceberg(2, 1, 100 * ONE, 3 * ONE, ONE));
                r.ack(id, 0, 101);
            } else {
                final var c = r.limit(2, 1, 89 * ONE, ONE);
                c.orderType = 4; // STOP_LIMIT
                c.stopPrice = 90 * ONE;
                id = r.create(c);
                r.tick(1, 89 * ONE, 91 * ONE, EPOCH);
                assertEquals(kind + " fixture: triggered", CoreOrder.PK_CREATE, r.order(id).pendingKind);
                r.ack(id, 1, 101);
            }
            final CoreOrder o = r.order(id);
            assertTrue(kind + " fixture: holds the remainder", r.locked(2, BTC) > 0);
            o.oldLegId = o.currentLegId;
            o.set(CoreOrder.F_REPLACE_PENDING);
            o.set(CoreOrder.F_DISPATCH_PENDING);
            o.pendingKind = (byte) CoreOrder.PK_AMEND;
            o.attempt++;
            o.status = (byte) OrderStatus.PENDING_REPLACE.code();
            r.mark();
            assertTrue(r.outcome(id, o.attempt, V3Rig.KIND_AMEND, RESULT_REJECTED, 0, 7, true));
            assertNull(kind + ": the parent ends", r.order(id));
            assertEquals(kind + ": REJECTED", EventKind.REJECTED, terminalEvent(r).kind());
            assertEquals(kind + ": the hidden or untriggered remainder is released", 0L, r.locked(2, BTC));
        }
    }

    @Test
    public void outcomeBeforeTerminal() {
        // the ME suppresses the old-leg CANCELLED of a replace, so the outcome is the only signal; a late old-leg
        // terminal after it is stale
        final V3Rig r = live(ONE);
        r.amend(1, 1, 110 * ONE, 0);
        assertTrue(r.outcome(1, 1, V3Rig.KIND_AMEND, RESULT_REJECTED, 0, 7, true));
        assertNull(r.order(1));
        final long stale = r.engine.orders().staleTerminalCount();
        r.mark();
        assertTrue(r.terminal(1, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertEquals("nothing happens twice", 0, r.count(AssetsEngineEvent.OrderEvent.class));
        assertEquals(stale + 1, r.engine.orders().staleTerminalCount());
        assertEquals(0L, r.locked(1, USD));
    }

    @Test
    public void oldCancelledOnCreateOrCancelIsIgnored() {
        final V3Rig r = V3Rig.open();
        r.deposit(1, USD, 10_000 * ONE);
        r.create(r.limit(1, 0, 100 * ONE, ONE));
        assertTrue(r.outcome(1, 0, V3Rig.KIND_CREATE, 0, 101, 0, true));
        assertEquals(1L, r.engine.orders().oldCancelledIgnoredCount());
        assertEquals("the create resolves as today", 101L, r.order(1).currentLegId);
    }
}

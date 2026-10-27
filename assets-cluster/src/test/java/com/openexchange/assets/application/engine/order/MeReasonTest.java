// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.EventKind;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.BTC;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static com.openexchange.assets.application.engine.feed.V3Rig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** RC2 S7 11.2 (D9b; R-B2-8): {@code OrderEvent.meReason} carries the ME code on ME-caused events only, 255 elsewhere. */
public class MeReasonTest {

    private static AssetsEngineEvent.OrderEvent only(final V3Rig r, final EventKind kind) {
        final List<AssetsEngineEvent.OrderEvent> e = r.all(AssetsEngineEvent.OrderEvent.class).stream()
                .filter(x -> x.kind() == kind).toList();
        assertEquals("one " + kind + " event, got " + r.all(AssetsEngineEvent.OrderEvent.class), 1, e.size());
        return e.get(0);
    }

    private static V3Rig rig(final V3Rig r) {
        r.deposit(1, USD, 10_000 * ONE);
        r.deposit(2, BTC, 100 * ONE);
        return r;
    }

    @Test
    public void everyRowOfD9b() {
        // 63: the terminal event terminalize emits for this entry carries 63.rejectReason verbatim (0 = NONE allowed)
        final V3Rig a = rig(V3Rig.open());
        a.create(a.limit(1, 0, 100 * ONE, ONE));
        a.ack(1, 0, 101);
        a.mark();
        assertTrue(a.terminal(1, 101, 1, V3Rig.ME_REJECTED, 17));
        assertEquals("63 REJECTED", 17, only(a, EventKind.REJECTED).meReason());
        final V3Rig b = rig(V3Rig.open());
        b.create(b.limit(1, 0, 100 * ONE, ONE));
        b.ack(1, 0, 101);
        b.mark();
        assertTrue(b.terminal(1, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertEquals("63 CANCELLED with NONE", 0, only(b, EventKind.CANCELLED).meReason());

        // 63 naming the old leg while a replace is pending: the abort REPLACED carries the code
        final V3Rig c = rig(V3Rig.open());
        c.create(c.limit(1, 0, 100 * ONE, ONE));
        c.ack(1, 0, 101);
        c.amend(1, 1, 101 * ONE, 0);
        c.mark();
        assertTrue(c.terminal(1, 101, 1, V3Rig.ME_REJECTED, 9));
        assertEquals("63 abort REPLACED", 9, only(c, EventKind.REPLACED).meReason());

        // 64 result 1: the CREATE REJECTED terminal carries 64.reason (0..254), else 255
        for (final int[] row : new int[][] {{12, 12}, {254, 254}, {300, 255}, {-1, 255}}) {
            final V3Rig d = rig(V3Rig.open());
            d.create(d.limit(1, 0, 100 * ONE, ONE));
            d.mark();
            assertTrue(d.outcome(1, 0, V3Rig.KIND_CREATE, 1, 0, row[0], false));
            assertEquals("64 CREATE REJECTED reason " + row[0], row[1], only(d, EventKind.REJECTED).meReason());
        }
        // 64 result 1 on AMEND: the abort REPLACED (oldCancelled 0) and the D9a row-1 terminal (oldCancelled 1)
        for (final boolean oldCancelled : new boolean[] {false, true}) {
            final V3Rig e = rig(V3Rig.open());
            e.create(e.limit(1, 0, 100 * ONE, ONE));
            e.ack(1, 0, 101);
            e.amend(1, 1, 101 * ONE, 0);
            e.mark();
            assertTrue(e.outcome(1, 1, V3Rig.KIND_AMEND, 1, 0, 21, oldCancelled));
            assertEquals("64 AMEND REJECTED oldCancelled " + oldCancelled, 21,
                    only(e, oldCancelled ? EventKind.REJECTED : EventKind.REPLACED).meReason());
        }
        // other results that terminalize or abort (UNKNOWN_LEG 4) are not ME-reason sourced: 255
        final V3Rig f = rig(V3Rig.open());
        f.create(f.limit(1, 0, 100 * ONE, ONE));
        f.mark();
        assertTrue(f.outcome(1, 0, V3Rig.KIND_CREATE, 4, 0, 33, false));
        assertEquals("64 CREATE UNKNOWN_LEG", 255, only(f, EventKind.REJECTED).meReason());

        // everything else is 255: ACK_NEW, FILL, REPLACED resolve, SLICE_SUBMITTED, DISPATCHED, ACCEPTED
        final V3Rig g = rig(V3Rig.open());
        g.mark();
        g.create(g.iceberg(2, 1, 100 * ONE, 2 * ONE, ONE));
        g.create(g.limit(1, 0, 100 * ONE, 2 * ONE));
        g.ack(1, 0, 101);
        g.ack(2, 0, 202);
        assertTrue(g.trade(1, 2, 202, 1, 1, 101, 2, 100 * ONE, ONE, true));
        g.amend(1, 2, 101 * ONE, 0);
        g.outcome(2, 1, V3Rig.KIND_AMEND, 0, 909, 0, false);
        for (final AssetsEngineEvent.OrderEvent ev : g.all(AssetsEngineEvent.OrderEvent.class)) {
            assertEquals(ev.kind() + " is not ME-reason sourced", 255, ev.meReason());
        }
        assertTrue(g.all(AssetsEngineEvent.OrderEvent.class).stream().anyMatch(x -> x.kind() == EventKind.SLICE_SUBMITTED));
        assertTrue(g.all(AssetsEngineEvent.OrderEvent.class).stream().anyMatch(x -> x.kind() == EventKind.REPLACED));

        // v2: always 255
        final V3Rig v2 = V3Rig.v2();
        v2.engine.applyOrderEntryGate(1, 0, com.openexchange.assets.domain.GateState.OPEN,
                com.openexchange.assets.domain.GateReason.ADMIN, v2.now);
        rig(v2);
        v2.create(v2.limit(1, 0, 100 * ONE, ONE));
        v2.engine.orders().applyOutcome(1, 1, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 101, 0, v2.now, 0, v2.now);
        v2.mark();
        v2.engine.onMeTerminal(2, 0, 101, 1, 1, V3Rig.ME_REJECTED, 17, v2.now, 1, 0, v2.now);
        assertEquals("v2 never sets meReason", 255, only(v2, EventKind.REJECTED).meReason());
    }
}

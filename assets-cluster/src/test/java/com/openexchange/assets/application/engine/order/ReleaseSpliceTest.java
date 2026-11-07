// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S8 7 G9: A + B == A + SNAPSHOT + B for the release machinery: mints, confirmations, abandonment, LEG_LIVE, the
 * orphan round, parking, CAPACITY and the auto-reopen. The B events and the final image are equal on both paths.
 */
public class ReleaseSpliceTest {

    private static final long PX = 100 * ONE;

    /** Phase A: a mixed book with every backlog stage in flight. */
    private static final Consumer<V3Rig> A = r -> {
        restingBuy(r, 101);                                          // 1: UP_TO pending
        final long sell = r.create(r.limit(2, 1, PX, 2 * ONE));      // 2
        r.ack(sell, 0, 202);
        final long c = restingBuy(r, 103);                           // 3: dead, ORDER at RELEASE
        cancel(r, 1, c);
        r.terminal(c, 103, 1, V3Rig.ME_CANCELLED, 0);
        final long d = restingBuy(r, 104);                           // 4: dead, then LEG_LIVE -> ORPHAN_CANCEL
        cancel(r, 1, d);
        r.terminal(d, 104, 1, V3Rig.ME_CANCELLED, 0);
        r.releaseEntry(d, (1L << 2) | 3, ORDER, R_LEG_LIVE);
        r.adminGate(V3Rig.MKT, GateState.OPEN, GateReason.ADMIN);    //    (C4 gated the market)
        final long e = restingBuy(r, 105);                           // 5: CANCEL RELEASED -> flagged, abandoned
        cancel(r, 1, e);
        r.outcome(e, 1, CANCEL, RELEASED, 0, 0, false);
        r.adminGate(V3Rig.MKT, GateState.OPEN, GateReason.ADMIN);    //    (gate m)
        r.create(r.limit(1, 0, PX, ONE));                            // 6: CREATE pending
        r.outcome(6, 0, CREATE, CAPACITY, 0, 0, false);              //    CAPACITY: parked, the gate closed
    };

    /** Phase B: confirmations, the orphan outcome, a C5 park, the reopen, fills, terminals and late copies. */
    private static final Consumer<V3Rig> B = r -> {
        r.releaseEntry(1, 0, UP_TO, R_APPLIED);                      // C2
        r.releaseEntry(3, (1L << 2) | 3, ORDER, R_APPLIED);          // C1
        r.outcome(4, 2, CANCEL, UNKNOWN_LEG, 0, 0, false);           // E2 -> M12
        r.releaseEntry(4, (2L << 2) | 3, ORDER, R_LEG_LIVE);         // C5 -> PARKED
        final com.openexchange.assets.application.engine.order.MeReleaseView v = r.releaseView();
        v.ledgerSize = 0;
        r.apply(v);                                                  // L := 0: the auto-reopen
        r.outcome(6, 0, CREATE, APPLIED, 106, 0, false);             // the parked create applies
        r.trade(1, 2, 202, 2, 1, 101, 1, PX, ONE, false);            // 1 FILLED -> M11
        r.trade(2, 2, 202, 2, 5, 105, 1, PX, ONE, false);            // 2 FILLED, 5 (flagged) FILLED: an ORDER for 5 too (Q1 (d))
        r.outcome(3, 0, CREATE, APPLIED, 999, 0, false);             // U5b on 3 (its entry was confirmed)
        r.releaseEntry(1, 3, ORDER, R_APPLIED);
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        r.adminGate(V3Rig.MKT, GateState.OPEN, GateReason.ADMIN);
        r.engine.orders().applyRequestPendingDispatch(1, 0L, 1024);
    };

    private static List<String> renders(final V3Rig r) {
        final List<String> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            out.add(e.render());
        }
        return out;
    }

    @Test
    public void splicedRunEqualsTheStraightOne() {
        final V3Rig straight = funded();
        A.accept(straight);
        final V3Rig spliced = straight.fork();
        straight.mark();
        B.accept(straight);
        spliced.mark();
        B.accept(spliced);
        assertTrue("phase B did something", straight.events().size() > 20);
        assertEquals("phase B emits the same events after the restore", renders(straight), renders(spliced));
        assertArrayEquals("the final images are equal", image(straight.engine), image(spliced.engine));
        checkInvariants(spliced, "spliced");
        assertEquals("one parked entry (C5)", 1L, spliced.engine.orders().backlogParked());
        assertEquals("the orphan round ran", 1L, straight.engine.orders().orphanCancelCount());
        assertEquals(1L, straight.engine.orders().releaseAbandonedCount());
        assertEquals(1L, straight.engine.orders().lateApplyOnDeadOrderCount());
    }
}

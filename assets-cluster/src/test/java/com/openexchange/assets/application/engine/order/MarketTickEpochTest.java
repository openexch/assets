// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import org.junit.Test;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;

/** RC2 S7 section 8 (D6, W17): under v3 a MarketTick counts only when its meEpoch is the bound epoch. */
public class MarketTickEpochTest {

    @Test
    public void tickWithForeignEpochDropped() {
        final V3Rig r = V3Rig.open();
        final MarketRisk m = r.engine.orders().market(V3Rig.MKT);
        long version = 1;
        for (final long epoch : new long[] {EPOCH + 1, 0L, -EPOCH}) {
            r.tick(version++, 99 * ONE, 101 * ONE, epoch);
            assertEquals("meEpoch " + epoch + ": dropped, bestBid unchanged", 0L, m.bestBid);
            assertEquals("meEpoch " + epoch + ": bookVersion unchanged", 0L, m.bookVersion);
        }
        assertEquals(3L, r.feed().tickEpochDropCount());
        r.tick(version, 99 * ONE, 101 * ONE, EPOCH);
        assertEquals("the bound epoch's tick applies", 99 * ONE, m.bestBid);
        assertEquals(version, m.bookVersion);
        assertEquals(3L, r.feed().tickEpochDropCount());
    }

    @Test
    public void unboundTickDropped() {
        final V3Rig r = V3Rig.unbound();
        final MarketRisk m = r.engine.orders().market(V3Rig.MKT);
        r.tick(1, 99 * ONE, 101 * ONE, EPOCH);
        assertEquals("no ME is bound: the tick cannot be attributed", 0L, m.bestBid);
        assertEquals(1L, r.feed().tickUnboundCount());
        // v2 keeps the RC1 rule: meEpoch is not read
        final V3Rig v2 = V3Rig.v2();
        v2.tick(1, 99 * ONE, 101 * ONE, 0L);
        assertEquals(99 * ONE, v2.engine.orders().market(V3Rig.MKT).bestBid);
    }
}

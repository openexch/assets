// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static org.junit.Assert.fail;

/**
 * The tick counters ({@code tickAppliedCount}, {@code staleTicks}, {@code invalidTickCount},
 * {@code tickEpochDropCount}, {@code tickUnboundCount}) are node-local diagnostics; the snapshot is a function of the replicated
 * state alone. Run A applies 1000 ticks; run B applies the same ticks and then overwrites the five counters by reflection before
 * the snapshot; the bytes must be equal.
 *
 * <p>Negative control {@code -DTICK_COUNTERS_IN_SNAPSHOT=true} (test scope): the snapshot helper below appends the five counters
 * to the codec bytes, which is what a codec that serialised {@code staleTicks} would produce; the test must then fail with
 * "snapshot bytes differ at offset". The production {@link BalanceSnapshotCodec} carries no seam.</p>
 */
public class TickMetricsDoNotTouchSnapshotTest {

    private static final long ONE = V3Rig.ONE;

    private static void applyTicks(final V3Rig r) {
        final var oe = r.engine.orders();
        long version = 0;
        for (int i = 0; i < 1000; i++) {
            final int market = 1 + (i % 5);
            final long px = (100 + (i % 17)) * ONE;
            switch (i % 10) {
                case 3 -> oe.applyMarketTick(market, 1, px - ONE, px + ONE, 0L, 0L, V3Rig.EPOCH, r.now);          // stale (version 1 or lower)
                case 6 -> oe.applyMarketTick(market, ++version + 1_000_000, px + ONE, px - ONE, 0L, 0L, V3Rig.EPOCH, r.now); // invalid: crossed
                case 8 -> oe.applyMarketTick(market, ++version + 2_000_000, px - ONE, px + ONE, 0L, 0L, V3Rig.EPOCH + 1, r.now); // epoch drop
                default -> oe.applyMarketTick(market, ++version, px - ONE, px + ONE, px, i + 1L, V3Rig.EPOCH, r.now);
            }
        }
    }

    private static void setLong(final Object target, final String field, final long value) throws Exception {
        final Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.setLong(target, value);
    }

    private static long getLong(final Object target, final String field) throws Exception {
        final Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return f.getLong(target);
    }

    private static byte[] snapshot(final V3Rig r) throws Exception {
        final CoreEngine engine = r.engine;
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(engine, buffer);
        final boolean seam = Boolean.getBoolean("TICK_COUNTERS_IN_SNAPSHOT");
        final ByteBuffer out = ByteBuffer.allocate(length + (seam ? 5 * 8 : 0));
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        out.put(bytes);
        if (seam) {
            final Object oe = engine.orders();
            final Object feed = engine.feed();
            out.putLong(getLong(oe, "tickAppliedCount")).putLong(getLong(oe, "staleTicks")).putLong(getLong(oe, "invalidTickCount"))
                    .putLong(getLong(feed, "tickEpochDropCount")).putLong(getLong(feed, "tickUnboundCount"));
        }
        return out.array();
    }

    @Test
    public void tickCountersAreNotInTheSnapshot() throws Exception {
        final V3Rig a = V3Rig.open();
        applyTicks(a);
        final V3Rig b = V3Rig.open();
        applyTicks(b);
        final Object oe = b.engine.orders();
        final Object feed = b.engine.feed();
        // the counters really moved in this run (a vacuous test would pass with any codec)
        if (getLong(oe, "tickAppliedCount") < 500 || getLong(oe, "staleTicks") == 0 || getLong(oe, "invalidTickCount") == 0
                || getLong(feed, "tickEpochDropCount") == 0) {
            fail("the 1000 ticks did not exercise every counter");
        }
        setLong(oe, "tickAppliedCount", 111_111L);
        setLong(oe, "staleTicks", 222_222L);
        setLong(oe, "invalidTickCount", 333_333L);
        setLong(feed, "tickEpochDropCount", 444_444L);
        setLong(feed, "tickUnboundCount", 555_555L);
        final byte[] sa = snapshot(a);
        final byte[] sb = snapshot(b);
        if (sa.length != sb.length) {
            fail("snapshot bytes differ at offset " + Math.min(sa.length, sb.length) + " (length " + sa.length + " vs " + sb.length + ")");
        }
        for (int i = 0; i < sa.length; i++) {
            if (sa[i] != sb[i]) {
                fail("snapshot bytes differ at offset " + i);
            }
        }
    }
}

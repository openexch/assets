// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 contract section 2 + 3.7 #6: the trigger books are derived at restore from records that are PENDING_TRIGGER
 * and NOT quarantined. A codec round trip with a quarantined armed stop, a quarantined armed trailing order and a
 * normal armed stop rebuilds a book with only the normal stop, and a crossing tick fires only that one.
 */
public class SyntheticRestoreBooksTest {

    private static final int MKT = TriggerRig.MKT;
    private static final int ST_PENDING_TRIGGER = 7;

    private static byte[] bytes(final AssetsEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    private static AssetsEngine restore(final byte[] image, final RecordingAssetsSink sink) {
        final AssetsEngine e = new AssetsEngine();
        e.setEventSink(sink);
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, e);
        assertEquals(image.length, d.bytesConsumed);
        e.verifyRestoredInvariants();
        return e;
    }

    /** Ids 1 (SELL stop 95, quarantined), 2 (SELL trailing delta 3, arm 100, quarantined), 3 (SELL stop 96, normal). */
    private static TriggerRig fixture() {
        final TriggerRig t = TriggerRig.quoted(100 * ONE, 101 * ONE);
        final long q1 = t.admit(t.stopLoss(2, TriggerRig.SELL, 95 * ONE, ONE));
        final long q2 = t.admit(t.trailing(2, TriggerRig.SELL, 3 * ONE, ONE, 0L));
        final long n3 = t.admit(t.stopLoss(2, TriggerRig.SELL, 96 * ONE, ONE));
        assertEquals("fixture ids", List.of(1L, 2L, 3L), List.of(q1, q2, n3));
        t.r.orders.applyQuarantine(0xC1L, q1, t.r.now);
        t.r.orders.applyQuarantine(0xC2L, q2, t.r.now);
        assertEquals("precondition: one armed stop (the normal one)", 1, t.r.orders.armedStopCount(MKT));
        assertEquals("precondition: no armed trailing order", 0, t.r.orders.armedTrailingCount(MKT));
        assertEquals("precondition: the quarantined trailing arm is the admission bid", 100 * ONE, t.order(q2).trailingArmPrice);
        return t;
    }

    private static List<String> renders(final List<AssetsEngineEvent> events) {
        final List<String> out = new ArrayList<>();
        for (final AssetsEngineEvent e : events) {
            out.add(e.render());
        }
        return out;
    }

    private static List<AssetsEngineEvent.MeDispatch> dispatches(final List<AssetsEngineEvent> events) {
        final List<AssetsEngineEvent.MeDispatch> out = new ArrayList<>();
        for (final AssetsEngineEvent e : events) {
            if (e instanceof AssetsEngineEvent.MeDispatch d) {
                out.add(d);
            }
        }
        return out;
    }

    /** Section 2: quarantined armed records survive the round trip but stay out of the rebuilt books. */
    @Test
    public void quarantinedArmedRecordsStayOutOfTheBooksAfterRestore() {
        final TriggerRig t = fixture();
        final byte[] image = bytes(t.r.engine);
        final AssetsEngine b = restore(image, new RecordingAssetsSink());
        assertEquals("section 2: armedStopCount == 1 after restore", 1, b.orders().armedStopCount(MKT));
        assertEquals("section 2: armedTrailingCount == 0 after restore", 0, b.orders().armedTrailingCount(MKT));
        for (final long id : new long[] {1L, 2L, 3L}) {
            final CoreOrder o = b.orders().orders().get(id);
            assertNotNull("record " + id + " survives the round trip", o);
            assertEquals("record " + id + " is still PENDING_TRIGGER", ST_PENDING_TRIGGER, o.status);
        }
        assertTrue("1 stays quarantined", b.orders().orders().get(1L).has(CoreOrder.F_QUARANTINED));
        assertTrue("2 stays quarantined", b.orders().orders().get(2L).has(CoreOrder.F_QUARANTINED));
        assertTrue("3 is not quarantined", !b.orders().orders().get(3L).has(CoreOrder.F_QUARANTINED));
        assertArrayEquals("bytes are a function of state (the books are never serialized)", image, bytes(b));
    }

    /** 3.2: a crossing tick on the restored engine fires only the normal stop, identically to the original engine. */
    @Test
    public void crossingTickOnTheRestoredEngineFiresOnlyTheNormalStop() {
        final TriggerRig t = fixture();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final AssetsEngine b = restore(bytes(t.r.engine), sink);
        final long ver = t.version + 1;
        // bid 94: at or below both stops (95, 96) and 6 below the trailing arm (delta 3): all three would fire if armed
        b.orders().applyMarketTick(MKT, ver, 94 * ONE, 101 * ONE, 0L, 0L, 0L, t.r.now);
        final List<AssetsEngineEvent.MeDispatch> d = dispatches(sink.events());
        assertEquals("only one child dispatched on the restored engine, got " + sink.events(), 1, d.size());
        assertEquals("the child belongs to the normal stop (order 3)", 3L, d.get(0).omsOrderId());
        assertEquals("CREATE", DispatchKind.CREATE, d.get(0).kind());
        assertEquals("wire MARKET", OrderEngine.TYPE_MARKET, d.get(0).orderType());
        assertEquals("one trigger counted", 1L, b.orders().triggeredCount());
        assertEquals("order 3 left PENDING_TRIGGER", OrderEngine.TYPE_STOP_LOSS, b.orders().orders().get(3L).orderType);
        assertTrue("order 3 is PENDING_NEW", b.orders().orders().get(3L).status != ST_PENDING_TRIGGER);
        assertEquals("1 still armed-but-frozen", ST_PENDING_TRIGGER, b.orders().orders().get(1L).status);
        assertEquals("2 still armed-but-frozen", ST_PENDING_TRIGGER, b.orders().orders().get(2L).status);
        assertEquals("the book is empty after the fire", 0, b.orders().armedStopCount(MKT));
        long triggered = 0;
        for (final AssetsEngineEvent e : sink.events()) {
            if (e instanceof AssetsEngineEvent.OrderEvent oe && oe.kind() == EventKind.TRIGGERED) {
                triggered++;
                assertEquals("TRIGGERED names order 3", 3L, oe.orderId());
            }
        }
        assertEquals("one TRIGGERED event", 1L, triggered);
        t.r.markNow();
        t.r.orders.applyMarketTick(MKT, ver, 94 * ONE, 101 * ONE, 0L, 0L, 0L, t.r.now);
        assertEquals("the restored engine fires identically to the original", renders(t.r.events()), renders(sink.events()));
    }

    /** 3.7 #6: the quarantined trailing arm does not advance on the restored engine (it is out of the book). */
    @Test
    public void quarantinedTrailingArmDoesNotAdvanceOnTheRestoredEngine() {
        final TriggerRig t = fixture();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final AssetsEngine b = restore(bytes(t.r.engine), sink);
        b.orders().applyMarketTick(MKT, t.version + 1, 105 * ONE, 106 * ONE, 0L, 0L, 0L, t.r.now);
        assertEquals("the quarantined trailing arm stays at 100", 100 * ONE, b.orders().orders().get(2L).trailingArmPrice);
        assertTrue("no dispatch, got " + sink.events(), dispatches(sink.events()).isEmpty());
        t.r.orders.applyMarketTick(MKT, t.version + 1, 105 * ONE, 106 * ONE, 0L, 0L, 0L, t.r.now);
        assertArrayEquals("restored and original engines stay byte-identical", bytes(t.r.engine), bytes(b));
    }
}

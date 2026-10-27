// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.application.engine.order.OrderEngine;
import com.openexchange.assets.application.engine.order.ReleaseBacklog;
import com.openexchange.assets.application.engine.order.S8Support;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.GateReason;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G9: section 11 ME_LINK (5.2) and the 304-byte section 2 record (5.1), with every restore rule (5.0). */
public class MeLinkSnapshotTest {

    private static final long PX = 100 * ONE;
    private static final int HEADER = 8 + 7 * 8 + 1 + 4;

    /**
     * Orders 1..5: 1 live with its UP_TO; 2 dead, ORDER at RELEASE; 3 dead, ORPHAN_CANCEL; 4 REJECTED (leg 0) then C4: PARKED,
     * the first floor form; 5 dead, orphan CONFLICT (E3): PARKED, the second floor form.
     */
    public static V3Rig rich() {
        final V3Rig r = funded();
        restingBuy(r, 101);                                   // 1
        for (final long leg : new long[] {102, 103}) {        // 2, 3
            final long h = restingBuy(r, leg);
            cancel(r, 1, h);
            r.terminal(h, leg, 1, V3Rig.ME_CANCELLED, 0);
        }
        r.releaseEntry(3, (1L << 2) | 3, ORDER, R_LEG_LIVE);
        r.adminGate(V3Rig.MKT, com.openexchange.assets.domain.GateState.OPEN, GateReason.ADMIN); // C4 gated the market
        final long d = r.create(r.limit(1, 0, PX, ONE));      // 4
        r.outcome(d, 0, CREATE, REJECTED, 104, 0, false);
        r.releaseEntry(d, 3, ORDER, R_LEG_LIVE);
        r.adminGate(V3Rig.MKT, com.openexchange.assets.domain.GateState.OPEN, GateReason.ADMIN);
        final long e = restingBuy(r, 105);                    // 5
        cancel(r, 1, e);
        r.terminal(e, 105, 1, V3Rig.ME_CANCELLED, 0);
        r.releaseEntry(e, (1L << 2) | 3, ORDER, R_LEG_LIVE);
        r.outcome(e, 2, CANCEL, CONFLICT, 0, 0, false);
        final OrderEngine oe = r.engine.orders();
        assertEquals(ReleaseBacklog.STAGE_RELEASE, oe.backlog().get(1).stage);
        assertEquals(UP_TO, oe.backlog().get(1).scope);
        assertEquals(ReleaseBacklog.STAGE_RELEASE, oe.backlog().get(2).stage);
        assertEquals(ReleaseBacklog.STAGE_ORPHAN_CANCEL, oe.backlog().get(3).stage);
        assertEquals(ReleaseBacklog.STAGE_PARKED, oe.backlog().get(4).stage);
        assertEquals(ReleaseBacklog.STAGE_PARKED, oe.backlog().get(5).stage);
        assertEquals("the first PARKED form", 3L, oe.backlog().get(4).floor);
        assertEquals("the second PARKED form: floor of the old attempt", (1L << 2) | 3, oe.backlog().get(5).floor);
        assertEquals(2, oe.backlog().get(5).attempt);
        checkInvariants(r, "rich");
        return r;
    }

    /** {@code [offset of section id's body, its length]} in {@code img}, or null. */
    static int[] section(final byte[] img, final int id) {
        final UnsafeBuffer b = new UnsafeBuffer(img);
        final int count = b.getInt(HEADER - 4);
        int pos = HEADER;
        for (int i = 0; i < count; i++) {
            final int sid = b.getShort(pos);
            final int len = b.getInt(pos + 2);
            if (sid == id) {
                return new int[] {pos + 6, len};
            }
            pos += 6 + len;
        }
        return null;
    }

    private static void refused(final byte[] good, final String what, final BiConsumer<UnsafeBuffer, Integer> corrupt) {
        final byte[] img = good.clone();
        final UnsafeBuffer b = new UnsafeBuffer(img);
        corrupt.accept(b, section(img, BalanceSnapshotCodec.SECTION_ME_LINK)[0]);
        final IllegalStateException e = assertThrows(what, IllegalStateException.class, () -> S8Support.restore(img));
        assertTrue(what + ": " + e.getMessage(), e.getMessage() != null);
    }

    private static int entry(final int body, final int i) {
        return body + BalanceSnapshotCodec.ME_LINK_HEAD_BYTES + i * BalanceSnapshotCodec.ME_LINK_ENTRY_BYTES;
    }

    @Test
    public void roundTripAndEveryInvariant() {
        final V3Rig r = rich();
        final byte[] good = image(r.engine);
        roundTrip(r.engine, "the rich image");
        final int[] s11 = section(good, BalanceSnapshotCodec.SECTION_ME_LINK);
        assertEquals("16 + 5 x 56", 16 + 5 * 56, s11[1]);

        // ---- refused, one corruption per rule (5.2) ----
        refused(good, "rule 1: meLedgerSize above C", (b, s) -> b.putLong(s, 5_000L));
        refused(good, "rule 1: meLedgerSize < 0", (b, s) -> b.putLong(s, -1L));
        refused(good, "rule 1: capacityHigh 2", (b, s) -> b.putByte(s + 12, (byte) 2));
        refused(good, "rule 1: a reserved head byte", (b, s) -> b.putByte(s + 13, (byte) 1));
        refused(good, "rule 2: not ascending", (b, s) -> b.putLong(entry(s, 1), 1L));
        refused(good, "rule 2: orderId at nextOrderId", (b, s) -> b.putLong(entry(s, 4), 99L));
        refused(good, "rule 2: scope 2", (b, s) -> b.putByte(entry(s, 1) + 48, (byte) 2));
        refused(good, "rule 2: stage 3", (b, s) -> b.putByte(entry(s, 1) + 49, (byte) 3));
        refused(good, "rule 2: orphanTries 2", (b, s) -> b.putByte(entry(s, 1) + 52, (byte) 2));
        refused(good, "an entry's reserved byte", (b, s) -> b.putByte(entry(s, 1) + 53, (byte) 1));
        refused(good, "rule 3: UP_TO floor at the attempt's ORDER floor", (b, s) -> b.putLong(entry(s, 0) + 8, 3L));
        refused(good, "rule 3: UP_TO floor < 0", (b, s) -> b.putLong(entry(s, 0) + 8, -1L));
        refused(good, "rule 3: UP_TO with an identity field", (b, s) -> b.putLong(entry(s, 0) + 24, 1L));
        refused(good, "rule 3: UP_TO orphanTries 1", (b, s) -> b.putByte(entry(s, 0) + 52, (byte) 1));
        refused(good, "rule 3: UP_TO naming a dead order", (b, s) -> {
            b.putByte(entry(s, 1) + 48, (byte) 0);
            b.putLong(entry(s, 1) + 24, 0L);
            b.putLong(entry(s, 1) + 32, 0L);
            b.putInt(entry(s, 1) + 40, 0);
            b.putInt(entry(s, 1) + 44, 0);
            b.putLong(entry(s, 1) + 8, 0L);
        });
        refused(good, "rule 4: RELEASE floor mismatch", (b, s) -> b.putLong(entry(s, 1) + 8, 11L));
        refused(good, "rule 4: ORPHAN_CANCEL with leg 0", (b, s) -> b.putLong(entry(s, 2) + 32, 0L));
        refused(good, "rule 4: ORPHAN_CANCEL with orphanTries 1", (b, s) -> b.putByte(entry(s, 2) + 52, (byte) 1));
        refused(good, "rule 4: marketId 0", (b, s) -> b.putInt(entry(s, 1) + 40, 0));
        refused(good, "rule 4: marketId MARKETS + 1", (b, s) -> b.putInt(entry(s, 1) + 40, OrderEngine.MARKETS + 1));
        refused(good, "rule 4: userId 0", (b, s) -> b.putLong(entry(s, 1) + 24, 0L));
        refused(good, "rule 4: side 2", (b, s) -> b.putByte(entry(s, 1) + 50, (byte) 2));
        refused(good, "rule 4: meOrderType 3", (b, s) -> b.putByte(entry(s, 1) + 51, (byte) 3));
        refused(good, "rule 4: negative legId", (b, s) -> b.putLong(entry(s, 1) + 32, -1L));
        refused(good, "rule 4: negative attempt", (b, s) -> b.putInt(entry(s, 1) + 44, -1));
        refused(good, "rule 4: negative floor", (b, s) -> b.putLong(entry(s, 3) + 8, -1L));
        refused(good, "rule 5: dispatchSeq 0", (b, s) -> b.putLong(entry(s, 1) + 16, 0L));
        refused(good, "rule 5: dispatchSeq above the head", (b, s) -> b.putLong(entry(s, 1) + 16, 1_000L));
        refused(good, "rule 5: a dispatchSeq held twice", (b, s) -> b.putLong(entry(s, 4) + 16,
                b.getLong(entry(s, 3) + 16)));
        refused(good, "the length rule", (b, s) -> b.putInt(s + 8, 4));
        // rule 7: a hold under an ORDER entry's orderId in B.userId's account (crafted state, written through the account)
        final V3Rig h = rich();
        h.engine.account(1).hold(2L, V3Rig.USD, 7 * ONE, false);
        final byte[] withHold = image(h.engine);
        final IllegalStateException e7 = assertThrows(IllegalStateException.class, () -> S8Support.restore(withHold));
        assertTrue(e7.getMessage(), e7.getMessage().contains("rule 7"));
        // rule 6: an ORDER entry for a live order (order 1's UP_TO made an ORDER with an identity)
        refused(good, "rule 6: ORDER for a live order", (b, s) -> {
            b.putByte(entry(s, 0) + 48, (byte) 1);
            b.putLong(entry(s, 0) + 24, 1L);
            b.putInt(entry(s, 0) + 40, 1);
            b.putLong(entry(s, 0) + 8, 3L);
        });

        // ---- must restore ----
        // a clamped meLedgerSize == C
        final V3Rig c = rich();
        final com.openexchange.assets.application.engine.order.MeReleaseView over = c.releaseView();
        over.ledgerSize = 9_999;
        assertTrue(c.apply(over));
        assertEquals(4096L, c.engine.orders().meLedgerSize());
        roundTrip(c.engine, "a clamped meLedgerSize == C");
        // ME_CAPACITY on GLOBAL with capacityHigh false (diagnostic D1, counted)
        final byte[] d1 = image(c.engine);
        assertTrue("precondition: the clamp closed the gate", c.globalHas(GateReason.ME_CAPACITY));
        new UnsafeBuffer(d1).putByte(section(d1, BalanceSnapshotCodec.SECTION_ME_LINK)[0] + 12, (byte) 0);
        final AssetsEngine restored = S8Support.restore(d1);
        assertEquals("D1 counted, not refused", 1L, restored.orders().restoreCapacityBitWithoutHighCount());
    }

    @Test
    public void v3ImageWithoutSection11Refused() {
        final byte[] img = image(rich().engine);
        final int[] s11 = section(img, BalanceSnapshotCodec.SECTION_ME_LINK);
        final byte[] cut = Arrays.copyOf(img, s11[0] - 6); // section 11 is the last one
        new UnsafeBuffer(cut).putInt(HEADER - 4, new UnsafeBuffer(img).getInt(HEADER - 4) - 1);
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> S8Support.restore(cut));
        assertTrue(e.getMessage(), e.getMessage().contains("no ME_LINK section (11)"));
    }

    @Test
    public void section11BelowV3Refused() {
        final V3Rig v2 = V3Rig.v2();
        final byte[] img = image(v2.engine);
        final byte[] longer = Arrays.copyOf(img, img.length + 6 + 16);
        final UnsafeBuffer b = new UnsafeBuffer(longer);
        b.putShort(img.length, BalanceSnapshotCodec.SECTION_ME_LINK);
        b.putInt(img.length + 2, 16);
        b.putInt(HEADER - 4, b.getInt(HEADER - 4) + 1);
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> {
            final AssetsEngine x = new AssetsEngine();
            x.setEventSink(new RecordingAssetsSink());
            BalanceSnapshotCodec.deserialize(new UnsafeBuffer(longer), 0, longer.length, x);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("below behaviour 3 carries an ME_LINK section (11)"));
    }

    @Test
    public void orderRecord304() {
        assertEquals("5.1: 35 longs, 4 ints, 1 short, 6 bytes", 304, CoreOrder.SNAPSHOT_BYTES);
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        final CoreOrder o = r.order(h);
        assertEquals(2 * ONE, o.pendingLegQty);
        assertEquals(ONE, o.pendingExpectedOldRemaining);
        final byte[] img = image(r.engine);
        final int[] s2 = section(img, BalanceSnapshotCodec.SECTION_ORDERS);
        final UnsafeBuffer b = new UnsafeBuffer(img);
        assertEquals("one record", 1, b.getInt(s2[0]));
        final int rec = s2[0] + 4;
        assertEquals("replaceDeadlineMs at long 32", o.replaceDeadlineMs, b.getLong(rec + 32 * 8));
        assertEquals("pendingLegQty right after it", 2 * ONE, b.getLong(rec + 33 * 8));
        assertEquals("then pendingExpectedOldRemaining", ONE, b.getLong(rec + 34 * 8));
        final AssetsEngine back = roundTrip(r.engine, "an open window");
        assertEquals(2 * ONE, back.orders().orders().get(h).pendingLegQty);
        // 5.1 a-d refusals
        final byte[] flags = img.clone();
        new UnsafeBuffer(flags).putShort(rec + 35 * 8 + 16, (short) (o.stateFlags | (1 << 9)));
        assertThrows("5.1 a: an unknown flag bit", IllegalStateException.class, () -> S8Support.restore(flags));
        final byte[] wait = img.clone();
        new UnsafeBuffer(wait).putShort(rec + 35 * 8 + 16, (short) ((o.stateFlags | CoreOrder.F_ME_CAPACITY_WAIT)
                & ~CoreOrder.F_DISPATCH_PENDING));
        assertThrows("5.1 b: CAPACITY_WAIT without a pending dispatch", IllegalStateException.class, () -> S8Support.restore(wait));
        final byte[] outside = img.clone();
        new UnsafeBuffer(outside).putShort(rec + 35 * 8 + 16, (short) (o.stateFlags & ~CoreOrder.F_REPLACE_PENDING));
        assertThrows("5.1 c: a reference outside a window", IllegalStateException.class, () -> S8Support.restore(outside));
        final byte[] broken = img.clone();
        new UnsafeBuffer(broken).putLong(rec + 34 * 8, ONE + 1);
        assertThrows("5.1 d: the 3.7 invariant", IllegalStateException.class, () -> S8Support.restore(broken));
    }

    /**
     * Review Q4 (coordinator decision 4): under v3 a section 2 record whose identity is outside the admission ranges is
     * refused at restore (the live rule of the 5.0 FIX-4 row: M11 copies this identity into an ORDER entry).
     */
    @Test
    public void section2IdentityRangesRefused() {
        final V3Rig r = funded();
        restingBuy(r, 101);
        final byte[] img = image(r.engine);
        final int rec = section(img, BalanceSnapshotCodec.SECTION_ORDERS)[0] + 4;
        final Object[][] rows = {
            {"userId 0", 8, 8, 0L}, {"marketId 0", 280, 4, 0L}, {"marketId MARKETS + 1", 280, 4, (long) OrderEngine.MARKETS + 1},
            {"side 2", 298, 1, 2L}, {"orderType 7", 299, 1, 7L}, {"timeInForce 4", 300, 1, 4L}, {"attempt -1", 292, 4, -1L},
            {"currentLegId -1", 25 * 8, 8, -1L}, {"oldLegId -1", 30 * 8, 8, -1L},
        };
        for (final Object[] row : rows) {
            final byte[] bad = img.clone();
            final UnsafeBuffer b = new UnsafeBuffer(bad);
            final int at = rec + (int) row[1];
            switch ((int) row[2]) {
                case 8: b.putLong(at, (long) row[3]); break;
                case 4: b.putInt(at, (int) (long) row[3]); break;
                default: b.putByte(at, (byte) (long) row[3]); break;
            }
            final IllegalStateException e = assertThrows((String) row[0], IllegalStateException.class, () -> S8Support.restore(bad));
            assertTrue(row[0] + ": " + e.getMessage(), e.getMessage().contains("order 1") || e.getMessage().contains("market"));
        }
        roundTrip(r.engine, "the unmodified image restores");
    }
}

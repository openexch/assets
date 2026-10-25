// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsReplica;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.cluster.testkit.SnapshotSpliceRunner;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S6 G4 (contract section 2, 3.1, 3.4, 6): the trigger books are DERIVED from the records. A codec round
 * trip keeps every record field an armed stop, an advanced trailing arm and an iceberg mid-slice carry, the
 * restored engine rebuilds the same books, re-serializes to the same bytes and fires identically on the same
 * ticks; and the splice runner (spec R7) holds with synthetic CREATE types and MARKETTICK verbs.
 */
public class SyntheticSnapshotTest {

    private static final int MKT = 1;
    private static final int ST_PENDING_TRIGGER = 7;
    private static final long SLIP = ONE + 500L * 10_000L; // 1 + 500 bps in 8 dp (MarketRisk default)
    private static final long HALF = ONE / 2;

    private static byte[] bytes(final AssetsEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    private static AssetsEngine restore(final byte[] image) {
        final AssetsEngine e = new AssetsEngine();
        e.setEventSink(new RecordingAssetsSink());
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, e);
        assertEquals(image.length, d.bytesConsumed);
        e.verifyRestoredInvariants();
        return e;
    }

    // ---------------------------------------------------------------- fixture

    private static CreateOrderCommand synth(final OrderRig r, final long user, final int side, final int type, final long qty) {
        final CreateOrderCommand c = r.limit(user, MKT, side, 0L, qty);
        c.orderType = type;
        return c;
    }

    private static CoreOrder armed(final OrderEngine oe, final long orderId, final String what) {
        final CoreOrder o = oe.orders().get(orderId);
        if (o == null) {
            throw new AssertionError("order " + orderId + " (" + what + ") must be admitted and live (contract 1: synthetic "
                    + "types are accepted), but the store has no record for it");
        }
        return o;
    }

    /**
     * Ten orders on market 1 (ids at GREEN): 1 SELL STOP_LOSS stop 95, 2 BUY STOP_LOSS stop 105 (estimate), 3 SELL
     * STOP_LIMIT stop 96 px 95.5, 4 BUY STOP_LIMIT stop 104 px 104.5, 5 SELL TRAILING delta 3, 6 BUY TRAILING delta 3
     * budget 120, 7 BUY ICEBERG px 99 qty 3 display 1 (slice 1 filled, slice 2 create pending), 8 SELL LIMIT 99 x 1.5,
     * 9 SELL ICEBERG px 102 qty 2 display 0.5 (slice 1 filled 0.2), 10 BUY LIMIT 102 x 1; then a tick that advances
     * both trailing arms and fires nothing.
     */
    private static OrderRig fixture() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 100_000 * ONE);
        r.deposit(2, BTC, 100 * ONE);
        r.deposit(3, USD, 100_000 * ONE);
        r.deposit(3, BTC, 100 * ONE);
        r.tick(MKT, 100 * ONE, 101 * ONE, 1);
        CreateOrderCommand c = synth(r, 2, 1, OrderEngine.TYPE_STOP_LOSS, ONE);
        c.stopPrice = 95 * ONE;
        r.create(c);
        c = synth(r, 1, 0, OrderEngine.TYPE_STOP_LOSS, ONE);
        c.stopPrice = 105 * ONE;
        r.create(c);
        c = synth(r, 2, 1, OrderEngine.TYPE_STOP_LIMIT, 2 * ONE);
        c.stopPrice = 96 * ONE;
        c.price = 95 * ONE + HALF;
        r.create(c);
        c = synth(r, 1, 0, OrderEngine.TYPE_STOP_LIMIT, 2 * ONE);
        c.stopPrice = 104 * ONE;
        c.price = 104 * ONE + HALF;
        r.create(c);
        c = synth(r, 2, 1, OrderEngine.TYPE_TRAILING_STOP, ONE);
        c.trailingDelta = 3 * ONE;
        r.create(c);
        c = synth(r, 1, 0, OrderEngine.TYPE_TRAILING_STOP, ONE);
        c.trailingDelta = 3 * ONE;
        c.budget = 120 * ONE;
        r.create(c);
        c = synth(r, 3, 0, OrderEngine.TYPE_ICEBERG, 3 * ONE);
        c.price = 99 * ONE;
        c.displayQty = ONE;
        r.create(c);
        r.create(r.limit(2, MKT, 1, 99 * ONE, ONE + HALF));
        c = synth(r, 3, 1, OrderEngine.TYPE_ICEBERG, 2 * ONE);
        c.price = 102 * ONE;
        c.displayQty = HALF;
        r.create(c);
        r.create(r.limit(1, MKT, 0, 102 * ONE, ONE));
        final String[] what = {"", "SELL STOP_LOSS stop 95", "BUY STOP_LOSS stop 105", "SELL STOP_LIMIT stop 96",
            "BUY STOP_LIMIT stop 104", "SELL TRAILING delta 3", "BUY TRAILING delta 3 budget 120", "BUY ICEBERG 3 / display 1",
            "SELL LIMIT 99", "SELL ICEBERG 2 / display 0.5", "BUY LIMIT 102"};
        final int[] type = {-1, 3, 3, 4, 4, 5, 5, 6, 0, 6, 0};
        for (int id = 1; id <= 10; id++) {
            // admission first, so a refusal fails here with the contract reason
            assertEquals("order " + id + " must be the admitted " + what[id] + " (contract 1: synthetic types are accepted)",
                    type[id], armed(r.orders, id, what[id]).orderType);
        }
        OutcomeFeed.ack(r, 7, 701);
        OutcomeFeed.ack(r, 8, 801);
        OutcomeFeed.trade(r, 1, MKT, 8, 801, 2, 7, 701, 3, 99 * ONE, ONE, false); // slice 1 of 7 exhausted: slice 2 out
        OutcomeFeed.ack(r, 9, 901);
        OutcomeFeed.ack(r, 10, 1001);
        OutcomeFeed.trade(r, 2, MKT, 10, 1001, 1, 9, 901, 3, 102 * ONE, ONE / 5, true); // 9 slice 1 left with 0.3
        r.tick(MKT, 100 * ONE + HALF, 100 * ONE + 80_000_000L, 2); // arms advance: SELL 100 -> 100.5, BUY 101 -> 100.8
        return r;
    }

    /** The same tail applied to an engine: slice-2 ACK, a fill that exhausts 9's slice, then five ticks that fire all six. */
    private static void tail(final OrderEngine oe, final long now) {
        oe.applyOutcome(9_001, 7, (1L << 2), 3, 7, 0L, 0L, 0L, 0L, MKT, 0, 0, 702, 0, now, 0L, now);
        oe.applyTrade(9_002, 3, MKT, 1001, 1, 901, 3, 102 * ONE, 3 * ONE / 10, true, now, 10, 9, 0L, now);
        oe.applyMarketTick(MKT, 3, 97 * ONE, 97 * ONE + HALF, 0L, 0L, 0L, now);            // 5 fires; 6 arm -> 97.5
        oe.applyMarketTick(MKT, 4, 95 * ONE + HALF, 96 * ONE, 0L, 0L, 0L, now);            // 3 fires; 6 arm -> 96
        oe.applyMarketTick(MKT, 5, 99 * ONE, 99 * ONE + HALF, 0L, 0L, 0L, now);            // 6 fires (99.5 - 96 >= 3)
        oe.applyMarketTick(MKT, 6, 104 * ONE + HALF, 105 * ONE, 0L, 0L, 0L, now);          // 2 and 4 fire
        oe.applyMarketTick(MKT, 7, 94 * ONE, 94 * ONE + HALF, 0L, 0L, 0L, now);            // 1 fires
    }

    private static List<Field> recordFields() {
        final List<Field> out = new ArrayList<>();
        for (final Field f : CoreOrder.class.getFields()) {
            if (!Modifier.isStatic(f.getModifiers())) {
                out.add(f);
            }
        }
        return out;
    }

    private static void assertSameRecord(final CoreOrder want, final CoreOrder got) throws IllegalAccessException {
        for (final Field f : recordFields()) {
            assertEquals("order " + want.orderId + " field " + f.getName() + " must survive the codec round trip",
                    f.get(want), f.get(got));
        }
    }

    // ---------------------------------------------------------------- tests

    @Test
    public void G4_armedStopsTrailingArmsAndIcebergSliceStateSurviveTheCodecWithBooksRebuilt() throws IllegalAccessException {
        final OrderRig r = fixture();
        final OrderEngine oe = r.orders;

        // the state before the snapshot is the one the contract defines (so the round trip is not vacuous)
        final CoreOrder sellStop = armed(oe, 1, "SELL STOP_LOSS stop 95");
        assertEquals("SELL STOP_LOSS rests PENDING_TRIGGER (contract 1)", ST_PENDING_TRIGGER, sellStop.status);
        assertEquals(95 * ONE, sellStop.stopPrice);
        assertEquals("SELL stop holds its quantity in base", ONE, sellStop.holdAmount);
        final CoreOrder buyStop = armed(oe, 2, "BUY STOP_LOSS stop 105");
        assertEquals(ST_PENDING_TRIGGER, buyStop.status);
        assertEquals("BUY STOP_LOSS hold = stopPrice x (1 + 500 bps) x qty, exact",
                FixedPoint.multiply(FixedPoint.multiply(105 * ONE, SLIP), ONE), buyStop.holdAmount);
        assertEquals(ST_PENDING_TRIGGER, armed(oe, 3, "SELL STOP_LIMIT").status);
        assertEquals("BUY STOP_LIMIT hold = price x qty", FixedPoint.multiply(104 * ONE + HALF, 2 * ONE),
                armed(oe, 4, "BUY STOP_LIMIT").holdAmount);
        final CoreOrder trailSell = armed(oe, 5, "SELL TRAILING delta 3");
        assertEquals(ST_PENDING_TRIGGER, trailSell.status);
        assertEquals("SELL trailing arm follows a higher bid (3.1.4)", 100 * ONE + HALF, trailSell.trailingArmPrice);
        assertEquals(3 * ONE, trailSell.trailingDelta);
        final CoreOrder trailBuy = armed(oe, 6, "BUY TRAILING delta 3 budget 120");
        assertEquals("BUY trailing arm follows a lower ask (3.1.4)", 100 * ONE + 80_000_000L, trailBuy.trailingArmPrice);
        assertEquals("explicit budget is the BUY trailing hold", 120 * ONE, trailBuy.holdAmount);
        final CoreOrder ice = armed(oe, 7, "BUY ICEBERG 3 / display 1");
        assertEquals("slice 2 = min(display, hidden) after slice 1 exhausted (3.4)", ONE, ice.sliceRemainingQty);
        assertEquals("hidden = qty - display - next slice", ONE, ice.hiddenQty);
        assertEquals(ONE, ice.displayQty);
        assertEquals("remaining = slice + hidden", 2 * ONE, ice.remainingQty);
        assertEquals("refill bumps the attempt", 1, ice.attempt);
        assertEquals("refill keeps the exhausted slice's leg as oldLegId", 701, ice.oldLegId);
        assertEquals("the new slice's leg is not known yet", 0, ice.currentLegId);
        assertTrue("slice 2 create is pending", ice.has(CoreOrder.F_DISPATCH_PENDING));
        final CoreOrder ice2 = armed(oe, 9, "SELL ICEBERG 2 / display 0.5");
        assertEquals("slice 1 left 0.3 after a 0.2 fill", 3 * ONE / 10, ice2.sliceRemainingQty);
        assertEquals(3 * HALF, ice2.hiddenQty);
        assertEquals(901, ice2.currentLegId);
        assertEquals("four armed stops on market 1 (contract 2)", 4, oe.armedStopCount(MKT));
        assertEquals("two armed trailing orders on market 1 (contract 2)", 2, oe.armedTrailingCount(MKT));

        final byte[] image = bytes(r.engine);
        final AssetsEngine b = restore(image);
        final OrderEngine ob = b.orders();
        final List<Long> ids = new ArrayList<>();
        oe.orders().forEachByOrderId(o -> ids.add(o.orderId));
        final List<Long> restoredIds = new ArrayList<>();
        ob.orders().forEachByOrderId(o -> restoredIds.add(o.orderId));
        assertEquals("the same live orders after restore", ids, restoredIds);
        for (final long id : ids) {
            assertSameRecord(oe.orders().get(id), ob.orders().get(id));
        }
        for (int m = 1; m <= OrderEngine.MARKETS; m++) {
            assertEquals("armed stop book of market " + m + " is rebuilt from the records", oe.armedStopCount(m), ob.armedStopCount(m));
            assertEquals("trailing book of market " + m + " is rebuilt from the records", oe.armedTrailingCount(m), ob.armedTrailingCount(m));
        }
        assertArrayEquals("bytes are a function of state (books are never serialized)", image, bytes(b));

        // the restored engine fires identically on the same tail
        r.markNow();
        tail(oe, r.now);
        final List<String> original = new ArrayList<>();
        final List<Long> firedOrder = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            original.add(e.render());
            if (e instanceof AssetsEngineEvent.OrderEvent ev && ev.kind() == EventKind.TRIGGERED) {
                firedOrder.add(ev.orderId());
            }
        }
        assertEquals("the tail fires all six armed orders, by tick and ascending orderId within a tick (3.1.5)",
                List.of(5L, 3L, 6L, 2L, 4L, 1L), firedOrder);
        final RecordingAssetsSink sb = (RecordingAssetsSink) b.eventSink();
        final int from = sb.events().size();
        tail(ob, r.now);
        final List<String> restored = new ArrayList<>();
        for (final AssetsEngineEvent e : sb.events().subList(from, sb.events().size())) {
            restored.add(e.render());
        }
        assertEquals("the restored engine emits exactly the original's events on the same tail", original, restored);
        assertArrayEquals("and ends in the same bytes", bytes(r.engine), bytes(b));
        assertEquals("every armed order left the books", 0, ob.armedStopCount(MKT) + ob.armedTrailingCount(MKT));
    }

    private static final List<String> SPLICE = List.of(
            "CLOCK 1700000000000",
            "GATE market=0 open=1",
            "DEPOSIT u=1 asset=0 amt=100000.0",
            "DEPOSIT u=2 asset=1 amt=100.0",
            "DEPOSIT u=3 asset=0 amt=100000.0",
            "DEPOSIT u=3 asset=1 amt=100.0",
            "MARKETTICK market=1 bid=100.0 ask=101.0 ver=1",
            "CREATE u=2 market=1 side=1 type=3 qty=1.0 stop=95.0 rid=1",                       // 1 SELL STOP_LOSS
            "CREATE u=1 market=1 side=0 type=3 qty=1.0 stop=105.0 rid=2",                      // 2 BUY STOP_LOSS, estimate 110.25
            "CREATE u=1 market=1 side=0 type=4 px=104.5 qty=1.0 stop=104.0 rid=3",             // 3 BUY STOP_LIMIT
            "CREATE u=2 market=1 side=1 type=5 qty=1.0 delta=3.0 rid=4",                       // 4 SELL TRAILING, arm 100
            "CREATE u=1 market=1 side=0 type=5 qty=1.0 delta=3.0 budget=120.0 rid=5",          // 5 BUY TRAILING, arm 101
            "CREATE u=3 market=1 side=0 type=6 px=99.0 qty=3.0 display=1.0 rid=6",             // 6 BUY ICEBERG
            "CREATE u=2 market=1 side=1 type=0 px=99.0 qty=1.5 rid=7",                         // 7 SELL LIMIT
            "CREATE u=3 market=1 side=1 type=0 px=105.0 qty=1.0 rid=8",                        // 8 SELL LIMIT for child 2
            "CREATE u=2 market=1 side=1 type=3 qty=0.5 stop=90.0 tif=3 exp=1700000005000 rid=9", // 9 GTD armed
            "CREATE u=1 market=1 side=0 type=5 qty=0.1 delta=2.0 rid=10",                      // 10 armed, cancelled below
            "CREATE u=2 market=1 side=1 type=3 qty=1.0 stop=101.0 rid=11",                     // STOP_WOULD_TRIGGER_IMMEDIATELY
            "CREATE u=2 market=1 side=1 type=6 px=99.0 qty=1.0 display=1.0 rid=12",            // INVALID: display == qty
            "CREATE u=1 market=1 side=0 type=3 qty=1.0 stop=105.0 tif=1 rid=13",               // UNSUPPORTED_TIF
            "MEOUTCOME order=6 kind=0 attempt=0 result=0 leg=601",
            "MEOUTCOME order=7 kind=0 attempt=0 result=0 leg=701",
            "MEOUTCOME order=8 kind=0 attempt=0 result=0 leg=801",
            "METRADE id=1 market=1 taker=7 maker=6 px=99.0 qty=1.0 takerBuy=0",                // slice 1 exhausted: slice 2 out
            "MARKETTICK market=1 bid=100.5 ask=100.8 ver=2",                                   // arms advance, nothing fires
            "MEOUTCOME order=6 kind=0 attempt=1 result=0 leg=602",
            "METERMINAL order=6 leg=601 status=2",                                             // previous slice FILLED: no-op
            "METRADE id=2 market=1 taker=7 maker=6 px=99.0 qty=0.5 takerBuy=0",                // 7 FILLED; slice 2 has 0.5
            "CANCEL u=1 order=10 rid=14",                                                      // armed: CANCELLED, no ME command
            "MARKETTICK market=1 bid=97.0 ask=97.5 ver=3",                                     // 4 fires; 5 arm -> 97.5
            "MARKETTICK market=1 bid=97.0 ask=97.5 ver=3",                                     // stale duplicate
            "MARKETTICK market=1 bid=98.0 ask=97.0 ver=4",                                     // crossed: invalid
            "MARKETTICK market=1 bid=100.5 ask=101.0 ver=4",                                   // 5 fires (101 - 97.5 >= 3)
            "TICK 6000",                                                                       // 9 EXPIRED while armed
            "MARKETTICK market=1 bid=104.8 ask=105.0 ver=5",                                   // 2 and 3 fire
            "MARKETTICK market=1 bid=89.0 ask=89.5 ver=6",                                     // 1 fires; expired 9 never
            "MEOUTCOME order=4 kind=0 attempt=1 result=0 leg=401",
            "METRADE id=3 market=1 taker=4 maker=6 px=99.0 qty=0.5 takerBuy=0",                // slice 2 exhausted: slice 3 out
            "METERMINAL order=4 leg=401 status=3",                                             // ME cancels the market remainder
            "MEOUTCOME order=2 kind=0 attempt=1 result=0 leg=201",
            "METRADE id=4 market=1 taker=2 maker=8 px=105.0 qty=1.0 takerBuy=1",               // child 2 FILLED within its budget
            "MEOUTCOME order=3 kind=0 attempt=1 result=1 reason=1",                            // STOP_LIMIT child refused: REJECTED
            "METERMINAL order=1 leg=101 status=4",                                             // terminal before the outcome (3.6)
            "MEOUTCOME order=1 kind=0 attempt=1 result=1",                                     // late outcome: no-op
            "MEOUTCOME order=5 kind=0 attempt=1 result=0 leg=501",
            "CANCEL u=3 order=6 rid=15",                                                       // slice 3 create pending: flag only
            "MEOUTCOME order=6 kind=0 attempt=2 result=0 leg=603",                             // ACK mints the cancel
            "MEOUTCOME order=6 kind=1 attempt=3 result=0",                                     // CANCELLED with filled 1.5
            "PENDINGDISPATCH");

    private static int count(final String text, final String regex) {
        final Matcher m = Pattern.compile(regex, Pattern.MULTILINE).matcher(text);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    @Test
    public void R7_theSpliceRunnerHoldsWithSyntheticCreatesAndMarketTicks() {
        SnapshotSpliceRunner.run(AssetsReplica::new, SPLICE);
        // the splice is only meaningful if the scenario exercises the S6 paths: check that it does
        final String out = AssetsScenarioRunner.runLines(SPLICE);
        assertEquals("seven stop and trailing orders are admitted PENDING_TRIGGER (contract 1), got:\n" + out,
                7, count(out, "^ORDERACK .*status=PENDING_TRIGGER"));
        assertEquals("five armed orders fire (4, 5, 2, 3, 1) and nothing else (3.1), got:\n" + out,
                5, count(out, "^ORDEREVENT .*kind=TRIGGERED"));
        assertEquals("two iceberg refills (slices 2 and 3) (3.4), got:\n" + out,
                2, count(out, "^ORDEREVENT .*kind=SLICE_SUBMITTED"));
        assertEquals("the GTD armed stop expires in the sweep (3.3, R12), got:\n" + out,
                1, count(out, "^ORDEREVENT .* order=9 .*kind=EXPIRED"));
        assertEquals("an expired armed stop never dispatches (R12), got:\n" + out,
                0, count(out, "^MEDISPATCH .* order=9 "));
        assertEquals("the armed cancel ends CANCELLED in its apply, no ME command (3.3), got:\n" + out,
                0, count(out, "^MEDISPATCH .* order=10 "));
        assertEquals("one crossed-at-admission stop is refused (D8), got:\n" + out,
                1, count(out, "^ORDERREJECT .*reason=STOP_WOULD_TRIGGER_IMMEDIATELY"));
        assertEquals("IOC on a synthetic type is refused (contract 1), got:\n" + out,
                1, count(out, "^ORDERREJECT .*reason=UNSUPPORTED_TIF"));
    }
}

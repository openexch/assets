// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsReplica;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.cluster.testkit.SnapshotSpliceRunner;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.LOT;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.MAX;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.MIN;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.PX_ABOVE_MAX;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.PX_BELOW_MIN;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.PX_OFF_TICK;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.PX_ON_TICK;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.QTY_OFF_LOT;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.QTY_ON_LOT;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.admit;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.adoptV1;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.countContaining;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.funded;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.fundedV2;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.refuseStructurally;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.sm;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.switchToV2;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.withRequestId;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S7 contract section 5 (and the BehaviorSwitchTest line of section 6): behaviour v1 admits off-tick, out-of-band
 * and off-lot orders; CoreConfig v2 with behaviour 2 switches at its log position; from the next command on
 * CreateOrder and AmendOrder are refused structurally with PRICE_OFF_TICK (38), PRICE_OUT_OF_BAND (37) and
 * QTY_OFF_LOT (39); stop prices follow the tick rule; MARKET orders skip the price rules but keep the lot rule; lot 0
 * disables the lot rule; v1 orders stay live; the snapshot and the splice runner keep v2 and the config. Every market
 * carries tick 0.5, band [10, 1000], lot 0.1 (8 dp).
 */
public class BehaviorSwitchTest {

    // ---------------------------------------------------------------- v1: nothing is checked

    @Test
    public void v1AdmitsOffTickOutOfBandAndOffLotOrders() {
        final OrderRig r = funded();
        assertEquals("contract 2: behaviour 1 when nothing is recorded", 1L, sm(r).behaviorVersion());
        adoptV1(r, LOT);
        admit(r, r.limit(1, 1, 0, PX_OFF_TICK, ONE), "v1: off-tick price 100.3");
        admit(r, r.limit(1, 1, 0, PX_BELOW_MIN, ONE), "v1: out-of-band price 5");
        admit(r, r.limit(1, 1, 0, PX_ABOVE_MAX, ONE), "v1: out-of-band price 1000.5");
        admit(r, r.limit(1, 1, 0, 100 * ONE, QTY_OFF_LOT), "v1: off-lot quantity 0.15");
        assertEquals("contract 5: still behaviour 1 after v1 admissions", 1L, sm(r).behaviorVersion());
    }

    // ---------------------------------------------------------------- the switch at a log position

    @Test
    public void coreConfigV2SwitchesAtThatLogPosition() {
        final OrderRig r = funded();
        adoptV1(r, LOT);
        admit(r, r.limit(1, 1, 0, PX_OFF_TICK, ONE), "the last command before the switch runs v1");
        switchToV2(r, LOT);
        refuseStructurally(r, r.limit(1, 1, 0, PX_OFF_TICK, ONE), CoreRejectReason.PRICE_OFF_TICK,
                "the first command after the switch runs v2");
    }

    // ---------------------------------------------------------------- v2 CreateOrder refusals

    @Test
    public void v2OffTickCreateIsPriceOffTickAndStructural() {
        final OrderRig r = fundedV2();
        admit(r, r.limit(1, 1, 0, 100 * ONE, ONE), "precondition: an on-tick order (user risk exists)");
        final CreateOrderCommand off = r.limit(1, 1, 0, PX_OFF_TICK, ONE);
        refuseStructurally(r, off, CoreRejectReason.PRICE_OFF_TICK, "v2: price 100.3 is not a multiple of 0.5");
        // not stored: the same requestId with a fixed payload is a fresh decision, not a mismatch or a stored reject
        admit(r, withRequestId(r.limit(1, 1, 0, PX_ON_TICK, ONE), off.requestIdLow),
                "v2: the refused requestId resent with an on-tick price");
    }

    @Test
    public void v2OffTickCreateIsRefusedOnEveryMarket() {
        final OrderRig r = fundedV2();
        refuseStructurally(r, r.limit(1, 2, 0, PX_OFF_TICK, ONE), CoreRejectReason.PRICE_OFF_TICK,
                "v2: market 2 carries the same tick 0.5");
    }

    @Test
    public void v2PriceBelowMinIsPriceOutOfBand() {
        final OrderRig r = fundedV2();
        refuseStructurally(r, r.limit(1, 1, 0, PX_BELOW_MIN, ONE), CoreRejectReason.PRICE_OUT_OF_BAND,
                "v2: price 5 is below minPrice 10 (and on tick)");
    }

    @Test
    public void v2PriceAboveMaxIsPriceOutOfBand() {
        final OrderRig r = fundedV2();
        refuseStructurally(r, r.limit(1, 1, 0, PX_ABOVE_MAX, ONE), CoreRejectReason.PRICE_OUT_OF_BAND,
                "v2: price 1000.5 is above maxPrice 1000 (and on tick)");
    }

    @Test
    public void v2OffLotQuantityIsQtyOffLot() {
        final OrderRig r = fundedV2();
        final CreateOrderCommand off = r.limit(2, 1, 1, 100 * ONE, QTY_OFF_LOT);
        refuseStructurally(r, off, CoreRejectReason.QTY_OFF_LOT, "v2: quantity 0.15 is not a multiple of lot 0.1");
        admit(r, withRequestId(r.limit(2, 1, 1, 100 * ONE, QTY_ON_LOT), off.requestIdLow),
                "v2: the refused requestId resent with an on-lot quantity");
    }

    @Test
    public void v2OnTickInBandOnLotOrdersAreAdmitted() {
        final OrderRig r = fundedV2();
        final long id = admit(r, r.limit(1, 1, 0, PX_ON_TICK, QTY_ON_LOT), "v2: 100.5 x 0.3");
        assertEquals("the admitted BUY holds price x quantity", 3_015_000_000L, r.engine.account(1).holdRemaining(id));
        assertEquals("one create consumed one rate slot", 1, r.orders.user(1).secCount);
        admit(r, r.limit(1, 1, 0, MIN, LOT), "v2: price == minPrice is in band (inclusive)");
        admit(r, r.limit(1, 1, 0, MAX, ONE), "v2: price == maxPrice is in band (inclusive)");
        admit(r, r.limit(2, 1, 1, 100 * ONE, 7 * ONE), "v2: a whole quantity is on lot");
    }

    @Test
    public void v2LotZeroDisablesTheLotRule() {
        final OrderRig r = funded();
        adoptV1(r, 0L);
        switchToV2(r, 0L);
        admit(r, r.limit(2, 1, 1, 100 * ONE, QTY_OFF_LOT), "v2 with lot 0: quantity 0.15 has no lot to miss");
        admit(r, r.limit(2, 1, 1, 100 * ONE, 1L), "v2 with lot 0: one unit of the 8th decimal");
        refuseStructurally(r, r.limit(1, 1, 0, PX_OFF_TICK, ONE), CoreRejectReason.PRICE_OFF_TICK,
                "v2 with lot 0: the tick rule still applies");
    }

    @Test
    public void v2MarketOrderSkipsThePriceRulesButKeepsTheLotRule() {
        final OrderRig r = fundedV2();
        admit(r, r.market(2, 1, 1, QTY_ON_LOT, 0L), "v2: MARKET SELL 0.3 (price 0 is neither ticked nor banded)");
        refuseStructurally(r, r.market(2, 1, 1, QTY_OFF_LOT, 0L), CoreRejectReason.QTY_OFF_LOT,
                "v2: MARKET SELL 0.15 keeps the lot rule");
    }

    // ---------------------------------------------------------------- stop prices follow the tick rule

    @Test
    public void v2StopLimitChecksBothItsPriceAndItsStopPrice() {
        final OrderRig r = fundedV2();
        final TriggerRig t = new TriggerRig(r);
        refuseStructurally(r, t.stopLimit(1, OrderEngine.SIDE_BUY, PX_OFF_TICK, 101 * ONE, ONE), CoreRejectReason.PRICE_OFF_TICK,
                "v2: STOP_LIMIT with stopPrice 100.3 off tick (price 101 on tick)");
        refuseStructurally(r, t.stopLimit(1, OrderEngine.SIDE_BUY, PX_ON_TICK, 101 * ONE + 30_000_000L, ONE),
                CoreRejectReason.PRICE_OFF_TICK, "v2: STOP_LIMIT with price 101.3 off tick (stopPrice 100.5 on tick)");
        final long id = t.admit(t.stopLimit(1, OrderEngine.SIDE_BUY, PX_ON_TICK, 101 * ONE, ONE));
        assertEquals("v2: an on-tick STOP_LIMIT arms", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
    }

    @Test
    public void v2StopLossChecksItsStopPrice() {
        final OrderRig r = fundedV2();
        final TriggerRig t = new TriggerRig(r);
        refuseStructurally(r, t.stopLoss(2, OrderEngine.SIDE_SELL, 99 * ONE + 30_000_000L, ONE), CoreRejectReason.PRICE_OFF_TICK,
                "v2: STOP_LOSS with stopPrice 99.3 off tick");
        refuseStructurally(r, t.stopLoss(2, OrderEngine.SIDE_SELL, 99 * ONE + ONE / 2, QTY_OFF_LOT), CoreRejectReason.QTY_OFF_LOT,
                "v2: a price-less STOP_LOSS keeps the lot rule on its quantity");
        final long id = t.admit(t.stopLoss(2, OrderEngine.SIDE_SELL, 99 * ONE + ONE / 2, ONE));
        assertEquals("v2: an on-tick STOP_LOSS arms", OrderStatus.PENDING_TRIGGER.code(), t.order(id).status);
    }

    // ---------------------------------------------------------------- AmendOrder under v2

    @Test
    public void v2AmendWithAnOffTickNewPriceIsPriceOffTickAndNothingChanges() {
        final AmendSupport s = AmendSupport.base(); // BUY 100 x 1, order 1, leg 77, 100 USD held
        final OrderRig r = s.r;
        adoptV1(r, LOT);
        switchToV2(r, LOT);
        final CoreOrder o = s.order(1);
        final long price = o.price;
        final long qty = o.quantity;
        final long hold = o.holdAmount;
        final byte status = o.status;
        final long held = s.acct(1).holdRemaining(1);
        final long locked = s.acct(1).locked(USD);
        final int sec = r.orders.user(1).secCount;
        final int requests = r.orders.requests().countFor(1);
        final long dispatchSeq = r.engine.getDispatchSeq();

        r.markNow();
        final AmendOrderCommand c = s.amend(1, 1, PX_OFF_TICK, 0);
        assertEquals("contract 5: an off-tick amend price is PRICE_OFF_TICK",
                CoreRejectReason.PRICE_OFF_TICK, s.rejectOnly("v2 amend to 100.3"));
        assertEquals("nothing changes: price", price, o.price);
        assertEquals("nothing changes: quantity", qty, o.quantity);
        assertEquals("nothing changes: holdAmount", hold, o.holdAmount);
        assertEquals("nothing changes: status", status, o.status);
        assertEquals("nothing changes: leg", AmendSupport.LEG, o.currentLegId);
        assertFalse("nothing changes: no replace window", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals("nothing changes: no replace deadline", 0, s.replaceDeadlines());
        assertEquals("nothing changes: the hold", held, s.acct(1).holdRemaining(1));
        assertEquals("nothing changes: USD locked", locked, s.acct(1).locked(USD));
        assertEquals("structural: no rate budget consumed", sec, r.orders.user(1).secCount);
        assertEquals("structural: the request is not stored", requests, r.orders.requests().countFor(1));
        assertTrue("structural: no stored entry for the amend requestId",
                r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow) == null);
        assertEquals("nothing changes: no dispatch", dispatchSeq, r.engine.getDispatchSeq());

        r.markNow();
        s.amend(1, 1, PX_ON_TICK, 0);
        for (final AssetsEngineEvent e : r.events()) {
            assertFalse("v2: an on-tick amend passes the tick rule; got " + r.events(), e instanceof AssetsEngineEvent.OrderReject);
        }
        assertTrue("v2: the on-tick amend opens the replace window", o.has(CoreOrder.F_REPLACE_PENDING));
    }

    @Test
    public void v2AmendOutOfBandAndOffLotAreRefused() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        adoptV1(r, LOT);
        switchToV2(r, LOT);
        final int sec = r.orders.user(1).secCount;
        r.markNow();
        s.amend(1, 1, PX_ABOVE_MAX, 0);
        assertEquals("contract 5: an amend above maxPrice is PRICE_OUT_OF_BAND",
                CoreRejectReason.PRICE_OUT_OF_BAND, s.rejectOnly("v2 amend to 1000.5"));
        r.markNow();
        s.amend(1, 1, PX_BELOW_MIN, 0);
        assertEquals("contract 5: an amend below minPrice is PRICE_OUT_OF_BAND",
                CoreRejectReason.PRICE_OUT_OF_BAND, s.rejectOnly("v2 amend to 5"));
        r.markNow();
        s.amend(1, 1, 0, ONE + QTY_OFF_LOT);
        assertEquals("contract 5: an off-lot amend quantity is QTY_OFF_LOT",
                CoreRejectReason.QTY_OFF_LOT, s.rejectOnly("v2 amend to quantity 1.15"));
        assertEquals("structural: three refusals consumed no rate budget", sec, r.orders.user(1).secCount);
        assertEquals("nothing changes: price", 100 * ONE, s.order(1).price);
        assertEquals("nothing changes: quantity", ONE, s.order(1).quantity);
    }

    // ---------------------------------------------------------------- no retroactive action

    @Test
    public void v1OrdersThatAreOffTickStayLiveAfterTheSwitch() {
        final OrderRig r = funded();
        adoptV1(r, LOT);
        final long offTick = admit(r, r.limit(1, 1, 0, PX_OFF_TICK, ONE), "v1: off-tick");
        final long offBand = admit(r, r.limit(1, 1, 0, PX_BELOW_MIN, ONE), "v1: below min");
        final long offLot = admit(r, r.limit(2, 1, 1, 100 * ONE, QTY_OFF_LOT), "v1: off lot");
        final long held = r.engine.account(1).holdRemaining(offTick);
        switchToV2(r, LOT); // asserts no OrderEvent / OrderReject / MeDispatch at the switch
        for (final long id : new long[] {offTick, offBand, offLot}) {
            final CoreOrder o = r.orders.orders().get(id);
            assertNotNull("contract 5: no retroactive action, order " + id + " stays live", o);
            assertEquals("contract 5: order " + id + " keeps its status", OrderStatus.PENDING_NEW.code(), o.status);
        }
        assertEquals("contract 5: the off-tick order keeps its price", PX_OFF_TICK, r.orders.orders().get(offTick).price);
        assertEquals("contract 5: the off-tick order keeps its hold", held, r.engine.account(1).holdRemaining(offTick));
        r.markNow();
        r.cancel(1, offTick);
        assertEquals("the v1 off-tick order is still cancellable (cancel carries no price)", 0,
                r.count(AssetsEngineEvent.OrderReject.class));
        assertEquals("behaviour stays 2", 2L, sm(r).behaviorVersion());
    }

    // ---------------------------------------------------------------- snapshot and splice

    private static byte[] bytes(final AssetsEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    @Test
    public void snapshotRoundTripKeepsV2AndTheRecordedConfig() {
        final OrderRig r = fundedV2();
        admit(r, r.limit(1, 1, 0, PX_ON_TICK, ONE), "v2: on-tick");
        final byte[] image = bytes(r.engine);
        final AssetsEngine b = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        b.setEventSink(sink);
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, b);
        assertEquals(image.length, d.bytesConsumed);
        b.verifyRestoredInvariants();

        assertEquals("contract 5: the restored engine runs behaviour 2", 2L, b.coreConfig().behaviorVersion());
        final CoreConfigCommand rec = b.coreConfig().recorded();
        assertNotNull("contract 2: section 8 restores the recorded config", rec);
        final CoreConfigCommand src = r.engine.coreConfig().recorded();
        assertEquals("section 8: configVersion", 2L, rec.configVersion);
        assertEquals("section 8: behaviorVersion", 2L, rec.behaviorVersion);
        assertArrayEquals("section 8: markets field-equal, ascending marketId", src.markets, rec.markets);
        assertArrayEquals("section 8: assets field-equal, ascending assetId", src.assets, rec.assets);
        assertEquals("section 8: tick 0.5 survives", BehaviorSwitchSupport.TICK, rec.markets[0].tickSize());
        assertEquals("section 8: lot 0.1 survives", LOT, rec.markets[0].lotSize());
        assertArrayEquals("bytes are a function of state", image, bytes(b));

        final CreateOrderCommand off = r.limit(1, 1, 0, PX_OFF_TICK, ONE);
        final int before = sink.events().size();
        b.orders().applyCreate(off, r.now);
        assertEquals("the restored engine decides v2: one event", before + 1, sink.events().size());
        final AssetsEngineEvent e = sink.events().get(before);
        assertTrue("the restored engine refuses off-tick with PRICE_OFF_TICK; got " + e,
                e instanceof AssetsEngineEvent.OrderReject rej && rej.reason() == CoreRejectReason.PRICE_OFF_TICK);
    }

    /** The story of the corpus scenario, minus its explicit SNAPSHOT (the splice runner cuts everywhere). */
    private static final List<String> STORY = List.of(
            "CLOCK 1700000000000",
            "GATE market=0 open=1",
            "DEPOSIT u=1 asset=0 amt=100000.0",
            "DEPOSIT u=2 asset=1 amt=10.0",
            "CORECONFIG ver=1 behavior=1 tick=0.5 min=10.0 max=1000.0 lot=0.1 corr=71",
            "CREATE u=1 market=1 side=0 type=0 px=100.3 qty=1.0 rid=1",
            "CREATE u=1 market=1 side=0 type=0 px=5.0 qty=1.0 rid=2",
            "CREATE u=2 market=1 side=1 type=0 px=100.0 qty=0.15 rid=3",
            "CORECONFIG ver=2 behavior=2 tick=0.5 min=10.0 max=1000.0 lot=0.1 corr=72",
            "CREATE u=1 market=1 side=0 type=0 px=100.3 qty=1.0 rid=4",
            "CREATE u=1 market=1 side=0 type=0 px=5.0 qty=1.0 rid=5",
            "CREATE u=1 market=1 side=0 type=0 px=1000.5 qty=1.0 rid=6",
            "CREATE u=2 market=1 side=1 type=0 px=100.0 qty=0.15 rid=7",
            "CREATE u=1 market=1 side=0 type=0 px=100.5 qty=1.0 rid=4",
            "CREATE u=2 market=1 side=1 type=1 qty=0.2 rid=8",
            "CREATE u=2 market=1 side=1 type=1 qty=0.15 rid=9",
            "CREATE u=2 market=1 side=1 type=3 qty=1.0 stop=99.3 rid=10",
            "CORECONFIG ver=2 behavior=2 tick=0.5 min=10.0 max=1000.0 lot=0.1 corr=73",
            "CREATE u=1 market=1 side=0 type=0 px=100.3 qty=1.0 rid=11",
            "CANCEL u=1 order=1 rid=12");

    @Test
    public void theSpliceRunnerHoldsAcrossTheBehaviourSwitch() {
        SnapshotSpliceRunner.run(AssetsReplica::new, STORY);
        // the splice equivalence alone holds for a no-op switch too: pin the decisions it must be equivalent over
        final AssetsReplica a = new AssetsReplica();
        STORY.forEach(a::apply);
        final List<String> ev = a.events();
        assertEquals("ADOPT v1: " + ev, 1, countContaining(ev, "ADMINACK corr=71 ref=70 result=ADOPTED"));
        assertEquals("SWITCH v2: " + ev, 1, countContaining(ev, "ADMINACK corr=72 ref=70 result=ACCEPTED reason=0 cfg=2"));
        assertEquals("v2 resent: " + ev, 1, countContaining(ev, "ADMINACK corr=73 ref=70 result=DUPLICATE"));
        assertEquals("ConfigApplied for the ADOPT and the SWITCH only: " + ev, 2, countContaining(ev, "CONFIGAPPLIED kind=CORE market=0"));
        assertEquals("v2 refusals PRICE_OFF_TICK (rid 4, the stop, rid 11): " + ev, 3,
                countContaining(ev, "ref=50 reason=PRICE_OFF_TICK"));
        assertEquals("v2 refusals PRICE_OUT_OF_BAND (rid 5, 6): " + ev, 2, countContaining(ev, "ref=50 reason=PRICE_OUT_OF_BAND"));
        assertEquals("v2 refusals QTY_OFF_LOT (rid 7, 9): " + ev, 2, countContaining(ev, "ref=50 reason=QTY_OFF_LOT"));
        assertEquals("no other refusal (rid 4 resent fixed is admitted, the cancel is accepted): " + ev, 7,
                countContaining(ev, "ORDERREJECT"));
    }

    @Test
    public void theCorpusScenarioTellsTheSameStory() throws IOException {
        final String out = AssetsScenarioRunner.run(
                Path.of("src", "test", "resources", "determinism", "core_config_behavior_switch.scenario"));
        assertEquals("ADOPT v1:\n" + out, 1, countContaining(out, "ADMINACK corr=71 ref=70 result=ADOPTED"));
        assertEquals("SWITCH v2:\n" + out, 1, countContaining(out, "ADMINACK corr=72 ref=70 result=ACCEPTED reason=0 cfg=2"));
        assertEquals("v2 resent:\n" + out, 1, countContaining(out, "ADMINACK corr=73 ref=70 result=DUPLICATE"));
        assertEquals("PRICE_OFF_TICK incl. the one after the SNAPSHOT:\n" + out, 3,
                countContaining(out, "ref=50 reason=PRICE_OFF_TICK"));
        assertEquals("PRICE_OUT_OF_BAND:\n" + out, 2, countContaining(out, "ref=50 reason=PRICE_OUT_OF_BAND"));
        assertEquals("QTY_OFF_LOT:\n" + out, 2, countContaining(out, "ref=50 reason=QTY_OFF_LOT"));
        assertEquals("seven refusals in all:\n" + out, 7, countContaining(out, "ORDERREJECT"));
    }
}

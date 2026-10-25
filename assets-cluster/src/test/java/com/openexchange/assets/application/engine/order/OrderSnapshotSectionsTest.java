// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsReplica;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.cluster.testkit.SnapshotSpliceRunner;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S4 (layout sections 2 ORDERS, 3 USER_RISK, 4 MARKET_STATE, 6 REQUEST_IDS): orders, risk state and the
 * request table ride the v4 snapshot; the indexes (open sets, clientOrderId, deadline index for GTD and
 * request TTLs) are rebuilt from them; the bytes are a function of state; and the splice runner holds with
 * order verbs in the DSL.
 */
public class OrderSnapshotSectionsTest {

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

    private static List<String> deadlines(final AssetsEngine e) {
        final List<String> out = new ArrayList<>();
        e.deadlines().forEach((d, id, k) -> out.add(d + ":" + id + ":" + k));
        return out;
    }

    @Test
    public void ordersRiskAndRequestsSurviveRestoreWithIndexesRebuilt() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 10_000 * ONE);
        r.deposit(2, USD, 10_000 * ONE);
        final RiskConfigSetCommand cfg = r.riskConfig(1, 3);
        cfg.maxOpenOrders = 7;
        r.setRisk(cfg);
        r.orders.applyCircuitBreaker(1, 2, true, r.now);
        r.tick(1, 99 * ONE, 101 * ONE, 42);
        r.orders.market(1).lastTradePrice = 100 * ONE;
        final CreateOrderCommand a = r.limit(1, 1, 0, 100 * ONE, ONE);
        a.clientOrderIdHigh = 1;
        a.clientOrderIdLow = 2;
        r.create(a);
        final CreateOrderCommand gtd = r.limit(2, 1, 0, 100 * ONE, 2 * ONE);
        gtd.timeInForce = 3;
        gtd.expiresAtMs = r.now + 60_000;
        r.create(gtd);
        r.create(r.market(1, 1, 0, ONE, 0));
        r.cancel(1, 1); // CANCEL_PENDING flag on a dispatch-pending order rides the record
        final byte[] image = bytes(r.engine);

        final AssetsEngine b = restore(image);
        final OrderEngine ob = b.orders();
        assertEquals(3, ob.orders().size());
        final CoreOrder o1 = ob.orders().get(1);
        assertNotNull(o1);
        assertTrue(o1.has(CoreOrder.F_CANCEL_PENDING));
        assertTrue(o1.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(100 * ONE, o1.holdAmount);
        assertEquals(1, ob.orders().activeByClientId(1, 1, 2));
        assertEquals(2, ob.orders().openCount(1));
        assertEquals(1, ob.orders().openCount(2));
        assertEquals(7, ob.market(1).maxOpenOrders);
        assertEquals(3, ob.market(1).configVersion);
        assertTrue(ob.market(2).breakerTripped);
        assertEquals(101 * ONE, ob.market(1).bestAsk);
        assertEquals(42, ob.market(1).bookVersion);
        assertEquals(100 * ONE, ob.market(1).lastTradePrice);
        assertEquals("two creates consumed rate; the cancel is free", 2, ob.user(1).secCount);
        assertEquals(3, ob.requests().countFor(1));
        assertEquals(r.orders.requests().nextRequestSeq(), ob.requests().nextRequestSeq());
        assertEquals("GTD + 4 request TTLs rebuilt in order", deadlines(r.engine), deadlines(b));
        assertEquals(b.getNextOrderId(), r.engine.getNextOrderId());
        assertEquals(b.getEventSeq(), r.engine.getEventSeq());
        assertEquals(b.getDispatchSeq(), r.engine.getDispatchSeq());
        assertArrayEquals("bytes are a function of state", image, bytes(b));
        // the restored engine keeps deciding identically: the duplicate client id is still refused
        final OrderRig rb = new OrderRig();
        final CreateOrderCommand dup = rb.limit(1, 1, 0, 100 * ONE, ONE);
        dup.clientOrderIdHigh = 1;
        dup.clientOrderIdLow = 2;
        dup.requestIdLow = 0x999L; // a NEW request (not a replay of the original create's requestId)
        final RecordingAssetsSink sink = (RecordingAssetsSink) b.eventSink();
        final int before = sink.events().size();
        ob.applyCreate(dup, r.now);
        assertTrue(sink.events().get(before).render().contains("DUPLICATE_CLIENT_ORDER_ID"));
    }

    @Test
    public void theSpliceRunnerHoldsWithOrderVerbs() {
        SnapshotSpliceRunner.run(AssetsReplica::new, List.of(
                "CLOCK 1700000000000",
                "GATE market=0 open=1",
                "RISKCFG market=1 ver=1 maxOpen=3 perSec=100 perMin=1000 collarBps=2000 slipBps=500",
                "DEPOSIT u=1 asset=0 amt=10000.0",
                "DEPOSIT u=2 asset=1 amt=5.0",
                "MARKETTICK market=1 bid=99.0 ask=101.0 ver=7",
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 cid=11 rid=1",
                "CREATE u=1 market=1 side=0 type=1 qty=0.5 rid=2",
                "CREATE u=2 market=1 side=1 type=0 px=100.0 qty=1.0 tif=3 exp=1700000060000 rid=3",
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 cid=11 rid=4",
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 cid=11 rid=1",
                "CANCEL u=1 order=1 rid=5",
                "CANCEL u=2 order=1",
                "TICK 100",
                "CANCEL u=1 order=1",
                "TICK 60000",
                "SWEEP",
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 rid=6"));
    }
}

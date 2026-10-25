// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.LOT;
import static com.openexchange.assets.application.engine.order.BehaviorSwitchSupport.PX_OFF_TICK;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S7 gap tests for the coverage critic's list (contract sections 2, 4 and 5): restore cross-check of the tables,
 * an extra compiled-unknown asset, precedence of the v2 shape against the gate and between band and tick, the
 * trailing-stop lot rule, and "only when a config is recorded".
 */
public class CoreConfigGapTest {

    private static byte[] bytes(final AssetsEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    private static CoreConfigCommand withMarket1Symbol(final CoreConfigCommand c, final String symbol) {
        final CoreConfigCommand.MarketDef m = c.markets[0];
        c.markets = c.markets.clone();
        c.markets[0] = new CoreConfigCommand.MarketDef(m.marketId(), symbol, m.baseAssetId(), m.quoteAssetId(), m.tickSize(),
                m.minPrice(), m.maxPrice(), m.lotSize());
        return c;
    }

    @Test
    public void s4a_aRestoredImageWhoseMarketsDifferFromTheCompiledTableFailsFastAtRestore() {
        // a node that adopted a foreign table (it halted, but the image exists) is restored by a correct jar
        final AssetsEngine source = new AssetsEngine();
        source.setEventSink(new RecordingAssetsSink());
        source.applyCoreConfig(withMarket1Symbol(BehaviorSwitchSupport.config(1, 1, LOT, 1), "XBT-USD"), 1L);
        assertNotNull(source.coreConfig().recorded());
        final byte[] image = bytes(source);

        final AssetsEngine target = new AssetsEngine();
        target.setEventSink(new RecordingAssetsSink());
        final AtomicInteger halts = new AtomicInteger();
        target.setConfigFailFast(halts::incrementAndGet);
        BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, target);
        assertEquals("section 4: the restored table differs from the compiled one, the node halts", 1, halts.get());
        assertTrue(target.coreConfig().crossCheck().contains("XBT-USD"));
    }

    @Test
    public void s4b_anExtraValidAssetAtAdoptFailsFast() {
        final AssetsEngine e = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        e.setEventSink(sink);
        final AtomicInteger halts = new AtomicInteger();
        e.setConfigFailFast(halts::incrementAndGet);
        final CoreConfigCommand c = BehaviorSwitchSupport.config(1, 1, LOT, 2);
        final CoreConfigCommand.AssetDef[] assets = java.util.Arrays.copyOf(c.assets, c.assets.length + 1);
        assets[assets.length - 1] = new CoreConfigCommand.AssetDef(6, "ADA", 8);
        c.assets = assets;
        e.applyCoreConfig(c, 1L);
        assertEquals("structurally valid, so the replicated decision is ADOPTED", AdminResult.ADOPTED,
                ((AssetsEngineEvent.AdminAck) sink.events().get(0)).result());
        assertEquals("the compiled table has no asset 6: this node halts (never a reject)", 1, halts.get());
    }

    @Test
    public void s5_aClosedGateWinsOverTheV2Shape() {
        final OrderRig r = BehaviorSwitchSupport.fundedV2();
        r.engine.applyOrderEntryGate(0L, 1, GateState.SUSPENDED, GateReason.ADMIN, r.now);
        r.markNow();
        r.create(r.limit(1, 1, 0, PX_OFF_TICK, ONE));
        assertEquals("the gate is checked first (contract 5: right after the gate)", CoreRejectReason.GATE_CLOSED, r.reject().reason());
    }

    @Test
    public void s5_outOfBandIsReportedBeforeOffTick() {
        final OrderRig r = BehaviorSwitchSupport.fundedV2();
        r.markNow();
        r.create(r.limit(1, 1, 0, 5 * ONE + 30_000_000L, ONE)); // 5.3: below min AND off the 0.5 tick
        assertEquals(CoreRejectReason.PRICE_OUT_OF_BAND, r.reject().reason());
    }

    @Test
    public void s5_aTrailingStopFollowsTheLotRule() {
        final OrderRig r = BehaviorSwitchSupport.fundedV2();
        r.tick(1, 99 * ONE, 101 * ONE, 1);
        final CreateOrderCommand c = r.limit(2, 1, 1, 0L, 15_000_000L); // 0.15 off the 0.1 lot
        c.orderType = OrderEngine.TYPE_TRAILING_STOP;
        c.trailingDelta = 2 * ONE;
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.QTY_OFF_LOT, r.reject().reason());
        final CreateOrderCommand ok = r.limit(2, 1, 1, 0L, 30_000_000L);
        ok.orderType = OrderEngine.TYPE_TRAILING_STOP;
        ok.trailingDelta = 2 * ONE;
        r.markNow();
        r.create(ok);
        assertEquals("on the lot: armed", 1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    @Test
    public void s5_withNothingRecordedNothingIsChecked() {
        final OrderRig r = BehaviorSwitchSupport.funded();
        r.markNow();
        r.create(r.limit(1, 1, 0, PX_OFF_TICK, 15_000_000L));
        assertEquals("no CoreConfig recorded: behaviour 1, off-tick and off-lot admitted", 1, r.count(AssetsEngineEvent.OrderAck.class));
    }
}

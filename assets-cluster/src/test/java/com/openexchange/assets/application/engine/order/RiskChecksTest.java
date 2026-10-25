// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** RC1 S4 (risk spec C1, C4, C5, C6, C9, C10, C13, C16, C17, C20; K11 row 7): the seven checks on log time. */
public class RiskChecksTest {

    private static OrderRig funded(final long usd) {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, usd);
        return r;
    }

    private static void limitPerSec(final OrderRig r, final int market, final long version, final int perSec) {
        final RiskConfigSetCommand c = r.riskConfig(market, version);
        c.maxOrdersPerSec = perSec;
        c.maxOrdersPerMin = Math.max(perSec, 1_000);
        r.setRisk(c);
    }

    @Test
    public void c4_rateBudgetIsConsumedByLaterRejectsNotByRateRejects() {
        final OrderRig r = funded(1_000_000 * ONE);
        limitPerSec(r, 1, 1, 5);
        final RiskConfigSetCommand collar = r.riskConfig(1, 2);
        collar.maxOrdersPerSec = 5;
        collar.priceCollarBps = 1_000; // 10 %
        r.setRisk(collar);
        r.orders.market(1).lastTradePrice = 100 * ONE;
        for (int i = 0; i < 5; i++) {
            r.markNow();
            r.create(r.limit(1, 1, 0, 200 * ONE, ONE)); // collar breach
            assertEquals(CoreRejectReason.PRICE_COLLAR, r.reject().reason());
        }
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE)); // valid, 6th in the bucket
        assertEquals(CoreRejectReason.RATE_LIMIT, r.reject().reason());
        assertEquals("a rate reject does not increment", 5, r.orders.user(1).secCount);
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.RATE_LIMIT, r.reject().reason());
        assertEquals(5, r.orders.user(1).secCount);
    }

    @Test
    public void c5_fixedWindowBoundaryBurst() {
        final OrderRig r = funded(1_000_000 * ONE);
        limitPerSec(r, 1, 1, 5);
        r.now = 1_700_000_000_999L;
        for (int i = 0; i < 5; i++) {
            r.markNow();
            r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
            assertEquals(1, r.count(AssetsEngineEvent.OrderAck.class));
        }
        r.now = 1_700_000_001_000L;
        for (int i = 0; i < 5; i++) {
            r.markNow();
            r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
            assertEquals("new second, new bucket", 1, r.count(AssetsEngineEvent.OrderAck.class));
        }
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.RATE_LIMIT, r.reject().reason());
    }

    @Test
    public void c6_backwardsClusterTimeDoesNotRollTheBucket() {
        final OrderRig r = OrderRig.open();
        r.now = 1_000; // the ledger's log time starts in the bucket before the one under test
        r.deposit(1, USD, 1_000_000 * ONE);
        limitPerSec(r, 1, 1, 2);
        r.now = 2_000;
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.now = 2_005;
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.now = 1_990; // a step backwards: log time stays 2005
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.RATE_LIMIT, r.reject().reason());
        assertEquals(2, r.orders.user(1).secCount);
        assertEquals(2_005, r.engine.getLastLogTime());
    }

    @Test
    public void c1_limitRaisedAfterHistoryIsDecidedNormally() {
        final OrderRig r = funded(1_000_000 * ONE);
        limitPerSec(r, 1, 1, 2);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals(CoreRejectReason.RATE_LIMIT, r.reject().reason());
        limitPerSec(r, 1, 2, 1_000); // raised in the same bucket
        r.markNow();
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        assertEquals("no ring to resize, no exception, decided against the new bound", 1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    @Test
    public void c9_collarVectorsOnTheLastAppliedTrade() {
        final OrderRig r = funded(1_000_000 * ONE);
        r.orders.market(1).lastTradePrice = 199;
        final RiskConfigSetCommand c = r.riskConfig(1, 1);
        c.priceCollarBps = 2_000;
        c.minNotional = 0;
        r.setRisk(c);
        r.markNow();
        r.create(r.limit(1, 1, 0, 238, ONE)); // |238-199| = 39 <= 199*2000/10000 = 39 (exact bps); notional 238 units
        assertEquals(1, r.count(AssetsEngineEvent.OrderAck.class));
        r.markNow();
        r.create(r.limit(1, 1, 0, 239, ONE));
        assertEquals(CoreRejectReason.PRICE_COLLAR, r.reject().reason());
        r.orders.market(1).lastTradePrice = 0;
        r.markNow();
        r.create(r.limit(1, 1, 0, 1_000 * ONE, 1));
        assertEquals("no reference: collar skipped", 1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    @Test
    public void c10_overflowNeverThrows() {
        final OrderRig r = funded(1_000_000 * ONE);
        r.markNow();
        r.create(r.limit(1, 1, 0, 1L << 62, 3 * ONE));
        assertEquals(CoreRejectReason.NOTIONAL_TOO_LARGE, r.reject().reason());
        r.markNow();
        r.create(r.limit(1, 1, 0, Long.MAX_VALUE, 3 * ONE));
        assertEquals(CoreRejectReason.NOTIONAL_TOO_LARGE, r.reject().reason());
        assertEquals(1_000_000 * ONE, r.engine.account(1).available(USD));
    }

    @Test
    public void sizeAndNotionalBoundsKeepFourCodes() {
        final OrderRig r = funded(1_000_000 * ONE);
        final RiskConfigSetCommand c = r.riskConfig(1, 1);
        c.minQuantity = ONE;
        c.maxQuantity = 10 * ONE;
        c.minNotional = 50 * ONE;
        c.maxNotional = 500 * ONE;
        r.setRisk(c);
        final long[][] vectors = {{10 * ONE, ONE / 2}, {10 * ONE, 11 * ONE}, {10 * ONE, 2 * ONE}, {100 * ONE, 6 * ONE}};
        final CoreRejectReason[] expected = {CoreRejectReason.SIZE_TOO_SMALL, CoreRejectReason.SIZE_TOO_LARGE,
                CoreRejectReason.NOTIONAL_TOO_SMALL, CoreRejectReason.NOTIONAL_TOO_LARGE};
        for (int i = 0; i < vectors.length; i++) {
            r.markNow();
            r.create(r.limit(1, 1, 0, vectors[i][0], vectors[i][1]));
            assertEquals("vector " + i, expected[i], r.reject().reason());
        }
        r.markNow();
        r.create(r.limit(1, 1, 0, 10 * ONE, 5 * ONE)); // notional 50: inclusive bounds
        assertEquals(1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    @Test
    public void positionLimitWithOverflowGuard() {
        final OrderRig r = funded(1_000_000 * ONE);
        r.deposit(1, OrderRig.BTC, 100 * ONE);
        final RiskConfigSetCommand c = r.riskConfig(1, 1);
        c.maxPositionPerMarket = 10 * ONE;
        r.setRisk(c);
        r.orders.userOrCreate(1).position[1] = 8 * ONE;
        r.markNow();
        r.create(r.limit(1, 1, 0, ONE, 3 * ONE));
        assertEquals(CoreRejectReason.POSITION_LIMIT, r.reject().reason());
        r.markNow();
        r.create(r.limit(1, 1, 1, ONE, 3 * ONE));
        assertEquals("a sell toward zero passes", 1, r.count(AssetsEngineEvent.OrderAck.class));
        r.markNow();
        r.create(r.limit(1, 1, 1, ONE, 19 * ONE));
        assertEquals(CoreRejectReason.POSITION_LIMIT, r.reject().reason());
        r.orders.userOrCreate(1).position[1] = Long.MAX_VALUE - 1;
        c.maxPositionPerMarket = Long.MAX_VALUE;
        c.configVersion = 2;
        r.setRisk(c);
        r.markNow();
        r.create(r.limit(1, 1, 0, ONE, 3 * ONE));
        assertEquals("position overflow is a reject, never a wrap", CoreRejectReason.INVALID, r.reject().reason());
    }

    @Test
    public void c16_riskConfigStateMachine() {
        final OrderRig r = OrderRig.open();
        final RiskConfigSetCommand bad = r.riskConfig(1, 1);
        bad.minQuantity = 10;
        bad.maxQuantity = 5;
        r.markNow();
        r.setRisk(bad);
        assertEquals(AdminResult.REJECTED, r.only(AssetsEngineEvent.AdminAck.class).result());
        assertEquals(0, r.orders.market(1).configVersion);
        r.markNow();
        r.setRisk(r.riskConfig(1, 1));
        assertEquals(AdminResult.ACCEPTED, r.first(AssetsEngineEvent.AdminAck.class).result());
        final AssetsEngineEvent.ConfigApplied applied = r.first(AssetsEngineEvent.ConfigApplied.class);
        assertEquals(ConfigKind.RISK, applied.kind());
        assertEquals(1, applied.configVersion());
        r.markNow();
        r.setRisk(r.riskConfig(1, 1));
        assertEquals(AdminResult.DUPLICATE, r.only(AssetsEngineEvent.AdminAck.class).result());
        final RiskConfigSetCommand changed = r.riskConfig(1, 1);
        changed.maxOpenOrders = 7;
        r.markNow();
        r.setRisk(changed);
        assertEquals(AdminResult.STALE, r.only(AssetsEngineEvent.AdminAck.class).result());
        assertEquals(500, r.orders.market(1).maxOpenOrders);
        r.markNow();
        r.setRisk(r.riskConfig(1, 0));
        assertEquals(AdminResult.STALE, r.only(AssetsEngineEvent.AdminAck.class).result());
        r.markNow();
        r.setRisk(r.riskConfig(9, 1));
        assertEquals(AdminResult.REJECTED, r.only(AssetsEngineEvent.AdminAck.class).result());
        r.markNow();
        r.setRisk(r.riskConfig(0, 1));
        assertEquals("marketId 0 is INVALID for risk config (critic C10)", AdminResult.REJECTED, r.only(AssetsEngineEvent.AdminAck.class).result());
    }

    @Test
    public void c17_perMarketIsolationAndNoUserStateFromStructuralRejects() {
        final OrderRig r = funded(1_000 * ONE);
        final RiskConfigSetCommand c = r.riskConfig(1, 1);
        c.maxOpenOrders = 1;
        r.setRisk(c);
        assertEquals(500, r.orders.market(2).maxOpenOrders);
        r.create(r.limit(1, 9, 0, ONE, ONE));
        assertNull(r.orders.user(1));
    }

    @Test
    public void c20_cancelIsFreeOfRateBudget() {
        final OrderRig r = funded(1_000 * ONE);
        limitPerSec(r, 1, 1, 1);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.markNow();
        r.cancel(1, 1);
        assertEquals(1, r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals(1, r.orders.user(1).secCount);
    }
}

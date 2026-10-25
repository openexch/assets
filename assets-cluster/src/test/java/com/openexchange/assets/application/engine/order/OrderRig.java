// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.commands.CancelOrderCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** In-process rig for the order engine tests: ledger + order engine + recording sink + a logical clock. */
final class OrderRig {

    static final long ONE = 100_000_000L; // 1.0 in 8dp
    static final int USD = 0;
    static final int BTC = 1;

    final AssetsEngine engine = new AssetsEngine();
    final RecordingAssetsSink sink = new RecordingAssetsSink();
    final OrderEngine orders;
    long now = 1_700_000_000_000L;
    private long nextRequest = 1;
    private int mark;

    OrderRig() {
        engine.setEventSink(sink);
        orders = engine.orders();
    }

    /** A rig with the global gate OPEN (genesis is SUSPENDED). */
    static OrderRig open() {
        final OrderRig r = new OrderRig();
        r.engine.gate().set(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        return r;
    }

    void deposit(final long user, final int asset, final long amount) {
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setCorrelationId(0);
        d.setUserId(user);
        d.setAssetId(asset);
        d.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, now);
    }

    CreateOrderCommand limit(final long user, final int market, final int side, final long price, final long qty) {
        final CreateOrderCommand c = new CreateOrderCommand();
        c.reset();
        c.correlationId = 7_000 + nextRequest;
        c.userId = user;
        c.marketId = market;
        c.side = side;
        c.orderType = 0; // LIMIT
        c.timeInForce = 0; // GTC
        c.price = price;
        c.quantity = qty;
        c.requestIdHigh = 0xABCDL;
        c.requestIdLow = nextRequest++;
        return c;
    }

    CreateOrderCommand market(final long user, final int market, final int side, final long qty, final long budget) {
        final CreateOrderCommand c = limit(user, market, side, 0, qty);
        c.orderType = 1; // MARKET
        c.budget = budget;
        return c;
    }

    void create(final CreateOrderCommand c) {
        orders.applyCreate(c, now);
    }

    void cancel(final long user, final long orderId) {
        final CancelOrderCommand c = new CancelOrderCommand();
        c.reset();
        c.correlationId = 9_000 + orderId;
        c.userId = user;
        c.orderId = orderId;
        c.requestIdHigh = 0xABCDL;
        c.requestIdLow = nextRequest++;
        orders.applyCancel(c, now);
    }

    RiskConfigSetCommand riskConfig(final int market, final long version) {
        final RiskConfigSetCommand c = new RiskConfigSetCommand();
        c.reset();
        c.correlationId = 5_000 + version;
        c.marketId = market;
        c.configVersion = version;
        c.minQuantity = 1;
        c.maxQuantity = Long.MAX_VALUE;
        c.minNotional = 0;
        c.maxNotional = Long.MAX_VALUE;
        c.priceCollarBps = 2_000;
        c.maxOrdersPerSec = 100;
        c.maxOrdersPerMin = 1_000;
        c.maxOpenOrders = 500;
        c.maxPositionPerMarket = Long.MAX_VALUE;
        c.marketBuySlippageBps = 500;
        return c;
    }

    void setRisk(final RiskConfigSetCommand c) {
        orders.applyRiskConfigSet(c, now);
    }

    void tick(final int market, final long bid, final long ask, final long version) {
        orders.applyMarketTick(market, version, bid, ask, 0L, 0L, 0L, now);
    }

    void advance(final long ms) {
        now += ms;
    }

    void sweep() {
        engine.sweep(now);
    }

    void markNow() {
        mark = sink.events().size();
    }

    List<AssetsEngineEvent> events() {
        return sink.events().subList(mark, sink.events().size());
    }

    <T extends AssetsEngineEvent> T only(final Class<T> type) {
        final List<AssetsEngineEvent> e = events();
        assertEquals("one event, got " + e, 1, e.size());
        assertTrue("expected " + type.getSimpleName() + " got " + e.get(0), type.isInstance(e.get(0)));
        return type.cast(e.get(0));
    }

    <T extends AssetsEngineEvent> T first(final Class<T> type) {
        for (final AssetsEngineEvent e : events()) {
            if (type.isInstance(e)) {
                return type.cast(e);
            }
        }
        throw new AssertionError("no " + type.getSimpleName() + " in " + events());
    }

    long count(final Class<? extends AssetsEngineEvent> type) {
        return events().stream().filter(type::isInstance).count();
    }

    AssetsEngineEvent.OrderReject reject() {
        return only(AssetsEngineEvent.OrderReject.class);
    }

    long lastAcceptedOrderId() {
        return first(AssetsEngineEvent.OrderAck.class).orderId();
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.config;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.InitTradeHighWaterCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.SettleBatchEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseEncoder;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** RC2 S7 section 7: behaviour v3 (the CoreConfig rows 2b and 7, the production supported set, and 7.2 rows 3 and 5). */
public class BehaviorV3Test {

    private static final long G = V3Genesis.GENESIS_ID;
    private static final int T70 = 70;

    private static final class Box {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        int failFast;
        long now = 1_700_000_000_000L;

        Box(final boolean widen) {
            engine.setEventSink(sink);
            engine.setConfigFailFast(() -> failFast++);
            if (widen) {
                V3Genesis.widen(engine);
            }
        }

        AssetsEngineEvent.AdminAck config(final long version, final long behavior, final long genesisId) {
            final CoreConfigCommand c = AssetsScenarioRunner.coreConfigFor(version, behavior, 1L, 1L, 1_000_000_000_000_000L, 0L,
                    700 + version);
            c.genesisId = genesisId;
            final int mark = sink.events().size();
            engine.applyCoreConfig(c, now);
            for (final AssetsEngineEvent e : sink.events().subList(mark, sink.events().size())) {
                if (e instanceof AssetsEngineEvent.AdminAck a && a.refTemplateId() == T70) {
                    return a;
                }
            }
            throw new AssertionError("no CoreConfig AdminAck");
        }

        void assertInvalid(final AssetsEngineEvent.AdminAck a, final String what) {
            assertEquals(what + ": REJECTED", AdminResult.REJECTED, a.result());
            assertEquals(what + ": INVALID", CoreRejectReason.INVALID.code(), a.reason());
        }
    }

    @Test
    public void coreConfigRowsOf71() {
        // row 2: ADOPT records the genesisId as sent
        final Box b = new Box(true);
        assertEquals(AdminResult.ADOPTED, b.config(1, 1, G).result());
        assertEquals("row 2: genesisId recorded", G, b.engine.coreConfig().recorded().genesisId);
        // row 2b: any later config with another genesisId is a re-genesis attempt: INVALID (even an otherwise duplicate one)
        b.assertInvalid(b.config(1, 1, G + 1), "row 2b duplicate with another genesisId");
        b.assertInvalid(b.config(2, 2, G + 1), "row 2b switch with another genesisId");
        assertEquals(1L, b.engine.coreConfig().behaviorVersion());
        assertEquals(AdminResult.ACCEPTED, b.config(2, 2, G).result());
        assertEquals(G, b.engine.coreConfig().recorded().genesisId);

        // row 7, condition 1: no genesisId recorded
        final Box z = new Box(true);
        z.config(1, 1, 0);
        z.config(2, 2, 0);
        z.assertInvalid(z.config(3, 3, 0), "row 7 recorded genesisId 0");
        assertEquals(2L, z.engine.coreConfig().behaviorVersion());

        // row 7, condition 2: not feed-virgin (lastAppliedTradeId, consumePosition, dispatchSeq each)
        for (final String breach : new String[] {"lastAppliedTradeId", "consumePosition", "dispatchSeq"}) {
            final Box n = new Box(true);
            n.config(1, 1, G);
            n.config(2, 2, G);
            switch (breach) {
                case "lastAppliedTradeId": {
                    final SettleCommand s = new SettleCommand();
                    s.reset();
                    s.setTradeId(1);
                    s.setMarketId(1);
                    s.setTakerOrderId(1);
                    s.setTakerUserId(1);
                    s.setMakerOrderId(2);
                    s.setMakerUserId(2);
                    s.setPrice(ONE);
                    s.setQuantity(ONE);
                    s.setTakerIsBuy(true);
                    n.engine.applyCommand(AssetsEngine.CMD_SETTLE, s, n.now);
                    break;
                }
                case "consumePosition":
                    new SettlementProjector(n.engine).onTerminal(500L, 9L, 9L, n.now);
                    break;
                default: {
                    n.engine.applyOrderEntryGate(1, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, n.now);
                    final com.openexchange.assets.domain.commands.DepositCommand d = new com.openexchange.assets.domain.commands.DepositCommand();
                    d.reset();
                    d.setUserId(1);
                    d.setAssetId(0);
                    d.setAmount(1_000 * ONE);
                    n.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, n.now);
                    final com.openexchange.assets.domain.commands.CreateOrderCommand c =
                            new com.openexchange.assets.domain.commands.CreateOrderCommand();
                    c.reset();
                    c.userId = 1;
                    c.marketId = 1;
                    c.side = 0;
                    c.price = ONE;
                    c.quantity = ONE;
                    c.requestIdLow = 1;
                    n.engine.orders().applyCreate(c, n.now);
                    n.engine.applyOrderEntryGate(2, OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.ADMIN, n.now);
                    break;
                }
            }
            n.assertInvalid(n.config(3, 3, G), "row 7 not feed-virgin: " + breach);
            assertEquals(2L, n.engine.coreConfig().behaviorVersion());
        }

        // row 7, condition 3: GLOBAL OPEN at the switch
        final Box o = new Box(true);
        o.config(1, 1, G);
        o.config(2, 2, G);
        o.engine.applyOrderEntryGate(1, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, o.now);
        o.assertInvalid(o.config(3, 3, G), "row 7 GLOBAL OPEN");
        assertEquals(2L, o.engine.coreConfig().behaviorVersion());

        // row 8: every precondition holds: SWITCH to 3 (supported through the seam)
        final Box s = new Box(true);
        s.config(1, 1, G);
        s.config(2, 2, G);
        assertEquals(AdminResult.ACCEPTED, s.config(3, 3, G).result());
        assertEquals(3L, s.engine.coreConfig().behaviorVersion());
        assertEquals(0, s.failFast);
        assertEquals("deposits do not break feed-virginity", AdminResult.ADOPTED, new Box(true).config(1, 1, G).result());
    }

    // RC2 S8 3.10 item 5 (G8): behaviour 3 joins the production set; replaces S7's productionSupportedSetStaysOneTwo
    @Test
    public void productionSupportedSetIncludesThree() {
        assertArrayEquals("the S8 jar ships behaviour {1, 2, 3}", new int[] {1, 2, 3},
                CoreConfigStateMachine.SUPPORTED_BEHAVIOR_VERSIONS);
        final Box b = new Box(false);
        assertArrayEquals(new int[] {1, 2, 3}, b.engine.coreConfig().supportedBehaviorVersions());
        b.config(1, 1, G);
        b.config(2, 2, G);
        assertEquals(AdminResult.ACCEPTED, b.config(3, 3, G).result());
        assertEquals("no fail-fast at CoreConfig 3 on a production S8 jar (no test seam)", 0, b.failFast);
        assertEquals(3, b.engine.coreConfig().behaviorVersion());
    }

    // ---------------------------------------------------------------- 7.2 row 3 (D-1): v5 money refused for every class

    private static int register(final V3Rig r, final SessionClass cls) {
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .correlationId(8_000).sessionClass(cls).readerKind((short) 0).resumable(BoolFlag.TRUE).identity(0).sourceEpoch(0);
        final EgressChannelsEncoder ch = e.channels().clear();
        ch.acks(true).control(true).settlements(true);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int settle(final V3Rig r) {
        final SettleEncoder e = new SettleEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .tradeId(r.engine.getLastAppliedTradeId() + 1).marketId(1).takerOrderId(5_011).takerUserId(9).makerOrderId(5_022)
                .makerUserId(10).price(100 * ONE).quantity(ONE).takerIsBuy(BoolFlag.TRUE).journalPosition(7_777);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int settleBatch(final V3Rig r) {
        final SettleBatchEncoder e = new SettleBatchEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header());
        e.settlesCount(1).next().tradeId(r.engine.getLastAppliedTradeId() + 1).marketId(1).takerOrderId(5_011).takerUserId(9)
                .makerOrderId(5_022).makerUserId(10).price(100 * ONE).quantity(ONE).takerIsBuy(BoolFlag.TRUE).journalPosition(7_778);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int terminalRelease(final V3Rig r) {
        final TerminalReleaseEncoder e = new TerminalReleaseEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .journalPosition(7_779).orderId(-5_011).userId(9).timestamp(r.now);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void v5MoneyTemplatesRefusedForEveryClass() {
        final V3Rig r = V3Rig.open();
        r.deposit(9, V3Rig.USD, 1_000 * ONE);
        r.deposit(10, V3Rig.BTC, 10 * ONE);
        final var hold = new com.openexchange.assets.domain.commands.HoldCommand();
        hold.reset();
        hold.setOrderId(-5_011); // RC2 S8 3.12: outside the core's id space (a Hold naming orderId >= 1 is refused under v3)
        hold.setUserId(9);
        hold.setAssetId(V3Rig.USD);
        hold.setAmount(500 * ONE);
        r.engine.applyCommand(AssetsEngine.CMD_HOLD, hold, r.now);
        final long bridge = r.openSession(21, SessionClass.BRIDGE);
        r.dispatch(bridge, register(r, SessionClass.BRIDGE));
        final long tool = r.openSession(22, SessionClass.TOOL);
        final long position = r.engine.getConsumePosition();
        final long usd = r.total(9, V3Rig.USD);
        final long locked = r.locked(9, V3Rig.USD);
        long refused = 0;
        for (final String who : new String[] {"BRIDGE", "TOOL", "TRUSTED"}) {
            for (final int t : new int[] {5, 33, 6}) {
                final int len = t == 5 ? settle(r) : t == 33 ? settleBatch(r) : terminalRelease(r);
                r.mark();
                switch (who) {
                    case "BRIDGE": r.dispatch(bridge, len); break;
                    case "TOOL": r.dispatch(tool, len); break;
                    default: r.dispatchTrusted(len); break;
                }
                final String what = who + " template " + t;
                final AssetsEngineEvent.OrderReject rej = r.first(AssetsEngineEvent.OrderReject.class);
                assertEquals(what + ": UNAUTHORIZED_CLASS", CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
                assertEquals(what + ": names the template", t, rej.refTemplateId());
                assertEquals(what + ": nothing settled", 0, r.count(AssetsEngineEvent.Settled.class));
                assertEquals(what + ": no balance moved", 0, r.count(AssetsEngineEvent.Balance.class));
                assertEquals(what + ": counted", ++refused, r.engine.v5MoneyRefusedCount());
            }
        }
        assertEquals("consumePosition unchanged", position, r.engine.getConsumePosition());
        assertEquals("high-water unchanged", 0L, r.engine.getLastAppliedTradeId());
        assertEquals(usd, r.total(9, V3Rig.USD));
        assertEquals("the hold stands", locked, r.locked(9, V3Rig.USD));
    }

    @Test
    public void initTradeHighWaterRefusedUnderV3() {
        final V3Rig r = V3Rig.unbound();
        assertEquals("precondition: a virgin ledger", 0, r.engine.accountCount());
        final InitTradeHighWaterCommand c = new InitTradeHighWaterCommand();
        c.reset();
        c.setTradeId(100);
        c.setConsumePosition(5_000);
        r.engine.applyCommand(AssetsEngine.CMD_INIT_HIGH_WATER, c, r.now);
        assertEquals("7.2 row 5: the primer would break tradeId density", 0L, r.engine.getLastAppliedTradeId());
        assertEquals(0L, r.engine.getConsumePosition());
    }

    @Test
    public void v2LogReplayUnchanged() throws IOException {
        final Path dir = Path.of("src", "test", "resources", "determinism");
        final List<Path> scenarios;
        try (Stream<Path> s = Files.list(dir)) {
            scenarios = s.filter(p -> p.toString().endsWith(".scenario")).sorted().collect(Collectors.toList());
        }
        assertEquals("the corpus: 38 scenarios", 38, scenarios.size());
        for (final Path p : scenarios) {
            final String name = p.getFileName().toString().replace(".scenario", "");
            final String golden = Files.readString(dir.resolve(name + ".golden"), StandardCharsets.UTF_8);
            assertEquals("v1/v2 replay of " + name + " is byte-identical to its golden", golden, AssetsScenarioRunner.run(p));
        }
    }

    // ---------------------------------------------------------------- RC2 S8 3.12 (D-S8-10)

    private static int holdFrame(final V3Rig r, final long user, final long order, final int asset, final long amount) {
        final com.openexchange.assets.infrastructure.generated.HoldEncoder e = new com.openexchange.assets.infrastructure.generated.HoldEncoder()
                .wrapAndApplyHeader(r.buffer(), 0, r.header()).correlationId(3_300 + order).orderId(order).userId(user).assetId(asset)
                .amount(amount).omsManagedRelease(BoolFlag.FALSE);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int releaseFrame(final V3Rig r, final long user, final long order) {
        final com.openexchange.assets.infrastructure.generated.ReleaseEncoder e = new com.openexchange.assets.infrastructure.generated.ReleaseEncoder()
                .wrapAndApplyHeader(r.buffer(), 0, r.header()).orderId(order).userId(user).amount(-1L);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static com.openexchange.assets.domain.commands.HoldCommand hold(final long user, final long order, final long amount) {
        final var h = new com.openexchange.assets.domain.commands.HoldCommand();
        h.reset();
        h.setOrderId(order);
        h.setUserId(user);
        h.setAssetId(V3Rig.USD);
        h.setAmount(amount);
        return h;
    }

    @Test
    public void switchRefusedWithForeignHoldInCoreRange() {
        // 3.12 switch precondition: a v2 TOOL hold under nextOrderId + 5, and one under a live core order's id in ANOTHER account
        for (final int variant : new int[] {0, 1}) {
            final Box b = new Box(true);
            b.config(1, 1, G);
            b.config(2, 2, G);
            final var d = new com.openexchange.assets.domain.commands.DepositCommand();
            d.reset();
            d.setUserId(9);
            d.setAssetId(V3Rig.USD);
            d.setAmount(1_000 * ONE);
            b.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, b.now);
            final long foreignId;
            if (variant == 0) {
                foreignId = Math.max(1, b.engine.getNextOrderId()) + 5;
            } else {
                // a live core order of user 10 (order 1, an armed STOP_LIMIT: no dispatch, so the core stays feed-virgin), and
                // user 9's TOOL hold under id 1
                final var d10 = new com.openexchange.assets.domain.commands.DepositCommand();
                d10.reset();
                d10.setUserId(10);
                d10.setAssetId(V3Rig.USD);
                d10.setAmount(1_000 * ONE);
                b.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d10, b.now);
                b.engine.gate().set(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
                final var c = new com.openexchange.assets.domain.commands.CreateOrderCommand();
                c.reset();
                c.userId = 10; c.marketId = 1; c.side = 0; c.orderType = 4; c.timeInForce = 0; c.price = 110 * ONE; c.quantity = ONE;
                c.stopPrice = 105 * ONE;
                c.requestIdHigh = 1; c.requestIdLow = 1;
                b.engine.orders().applyCreate(c, b.now);
                foreignId = 1;
                assertEquals("precondition: the armed order is live", 10L, b.engine.orders().orders().get(1).userId);
                assertEquals("precondition: feed-virgin", 0L, b.engine.getDispatchSeq());
                b.engine.gate().set(OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.GENESIS);
            }
            b.engine.applyCommand(AssetsEngine.CMD_HOLD, hold(9, foreignId, 10 * ONE), b.now);
            assertEquals("precondition: the v2 TOOL hold stands", 10 * ONE, b.engine.account(9).locked(V3Rig.USD));
            b.assertInvalid(b.config(3, 3, G), "variant " + variant + ": a foreign hold with orderId >= 1");
            assertEquals(2L, b.engine.coreConfig().behaviorVersion());
            // migration: Release 4 at v2 (served there), then CoreConfig 3 is accepted
            final var rel = new com.openexchange.assets.domain.commands.ReleaseCommand();
            rel.reset();
            rel.setOrderId(foreignId);
            rel.setUserId(9);
            rel.setAmount(-1);
            b.engine.applyCommand(AssetsEngine.CMD_RELEASE, rel, b.now);
            assertEquals(0L, b.engine.account(9).locked(V3Rig.USD));
            assertEquals(AdminResult.ACCEPTED, b.config(3, 3, G).result());
            assertEquals(3L, b.engine.coreConfig().behaviorVersion());
        }
    }

    @Test
    public void toolOrAdminHoldOrReleaseNamingACoreOrderIdIsRefused() {
        final V3Rig r = V3Rig.open();
        r.deposit(1, V3Rig.USD, 10_000 * ONE);
        final long live = r.create(r.limit(1, 0, 100 * ONE, ONE));
        final long dead = r.create(r.limit(1, 0, 100 * ONE, ONE));
        r.outcome(dead, 0, V3Rig.KIND_CREATE, 1, 102, 0, false); // REJECTED: dead, its hold gone
        final long next = r.engine.getNextOrderId();
        final long tool = r.openSession(41, SessionClass.TOOL);
        final long admin = r.openSession(42, SessionClass.ADMIN);
        final long available = r.engine.account(1).available(V3Rig.USD);
        final long locked = r.locked(1, V3Rig.USD);
        long refused = 0;
        for (final long id : new long[] {dead, live, next, next + 5}) {
            for (final long who : new long[] {tool, admin, -1}) {
                r.mark();
                final int len = holdFrame(r, 1, id, V3Rig.USD, 50 * ONE);
                if (who < 0) {
                    r.dispatchTrusted(len);
                } else {
                    r.dispatch(who, len);
                }
                final AssetsEngineEvent.OrderReject rej = r.first(AssetsEngineEvent.OrderReject.class);
                assertEquals("Hold " + id + " from " + who, CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
                assertEquals(3, rej.refTemplateId());
                assertEquals("no HoldAck", 0, r.count(AssetsEngineEvent.HoldAck.class));
                assertEquals(++refused, r.engine.coreOrderHoldRefusedCount());
            }
        }
        assertEquals("balances unchanged", available, r.engine.account(1).available(V3Rig.USD));
        assertEquals(locked, r.locked(1, V3Rig.USD));
        // Release 4 naming a live core order: refused loudly (supersedes S7 7.2 row 4's counted no-op)
        for (final long who : new long[] {tool, admin, -1}) {
            r.mark();
            if (who < 0) {
                r.dispatchTrusted(releaseFrame(r, 1, live));
            } else {
                r.dispatch(who, releaseFrame(r, 1, live));
            }
            assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.first(AssetsEngineEvent.OrderReject.class).reason());
            assertEquals(4, r.first(AssetsEngineEvent.OrderReject.class).refTemplateId());
        }
        assertEquals(3L, r.engine.coreOrderReleaseRefusedCount());
        assertEquals("the live order's hold untouched", locked, r.locked(1, V3Rig.USD));
        // a create at nextOrderId gets a fresh hold of exactly its notional (nothing was topped up under its id)
        final long fresh = r.create(r.limit(1, 0, 100 * ONE, ONE));
        assertEquals(next, fresh);
        assertEquals(100 * ONE, r.engine.account(1).holdRemaining(fresh));
        // the engine API refuses with no reply
        r.mark();
        r.engine.applyCommand(AssetsEngine.CMD_HOLD, hold(1, next + 9, 5 * ONE), r.now);
        assertEquals("no reply", 0, r.events().size());
        assertEquals(refused + 1, r.engine.coreOrderHoldRefusedCount());
        final var rel = new com.openexchange.assets.domain.commands.ReleaseCommand();
        rel.reset();
        rel.setOrderId(live);
        rel.setUserId(1);
        rel.setAmount(-1);
        r.engine.applyCommand(AssetsEngine.CMD_RELEASE, rel, r.now);
        assertEquals(0, r.events().size());
        assertEquals(4L, r.engine.coreOrderReleaseRefusedCount());
        // orderId 0 keeps today's handling
        r.mark();
        r.dispatchTrusted(holdFrame(r, 1, 0, V3Rig.USD, 5 * ONE));
        assertEquals("orderId 0: today's hold path", 1, r.count(AssetsEngineEvent.HoldAck.class));
    }
}

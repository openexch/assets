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

    @Test
    public void productionSupportedSetStaysOneTwo() {
        assertArrayEquals("the S7 jar ships behaviour {1, 2} only; S8 adds 3", new int[] {1, 2},
                CoreConfigStateMachine.SUPPORTED_BEHAVIOR_VERSIONS);
        final Box b = new Box(false);
        assertArrayEquals(new int[] {1, 2}, b.engine.coreConfig().supportedBehaviorVersions());
        b.config(1, 1, G);
        b.config(2, 2, G);
        assertEquals("no fail-fast through behaviour 2", 0, b.failFast);
        b.config(3, 3, G);
        assertEquals("a production S7 jar takes the fail-fast path at CoreConfig 3", 1, b.failFast);
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
                .journalPosition(7_779).orderId(5_011).userId(9).timestamp(r.now);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void v5MoneyTemplatesRefusedForEveryClass() {
        final V3Rig r = V3Rig.open();
        r.deposit(9, V3Rig.USD, 1_000 * ONE);
        r.deposit(10, V3Rig.BTC, 10 * ONE);
        final var hold = new com.openexchange.assets.domain.commands.HoldCommand();
        hold.reset();
        hold.setOrderId(5_011);
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
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.config;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef;
import com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S7 contract section 4 (node-local cross-check, A-9 rule) and section 2 (snapshot section 8 CORE_CONFIG).
 * A mismatch between the recorded config and this jar's compiled tables or supported behaviour set is never a
 * reject: the replicated decision stands (recorded, AdminAck ADOPTED/ACCEPTED) and THIS node calls failFast.
 * The failFast seam is a recording Runnable installed through {@link AssetsEngine#setConfigFailFast}.
 */
public class CoreConfigCrossCheckTest {

    private static final long TS = 1_700_000_000_000L;
    private static final long ONE = 100_000_000L;
    private static final long TICK = ONE / 100;
    private static final long MIN = TICK;
    private static final long MAX = 1_000_000 * ONE;
    private static final long LOT = ONE / 1_000;
    private static final int CORE_CONFIG_TEMPLATE = 70;
    private static final int CREATE_ORDER_TEMPLATE = 50;

    /** One node: engine, recording sink, recording failFast. */
    private static final class Node {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        int failFastCalls;
        int mark;

        Node() {
            engine.setEventSink(sink);
            engine.setConfigFailFast(() -> failFastCalls++);
        }

        CoreConfigStateMachine cfg() {
            return engine.coreConfig();
        }

        void apply(final CoreConfigCommand c) {
            engine.applyCoreConfig(c, TS);
        }

        void markNow() {
            mark = sink.events().size();
        }

        List<AssetsEngineEvent> events() {
            return sink.events().subList(mark, sink.events().size());
        }

        AssetsEngineEvent.AdminAck onlyAdminAck() {
            final List<AssetsEngineEvent.AdminAck> acks = new ArrayList<>();
            for (final AssetsEngineEvent e : events()) {
                if (e instanceof AssetsEngineEvent.AdminAck a) {
                    acks.add(a);
                }
            }
            assertEquals("exactly one AdminAck in " + events(), 1, acks.size());
            assertEquals(CORE_CONFIG_TEMPLATE, acks.get(0).refTemplateId());
            return acks.get(0);
        }

        <T extends AssetsEngineEvent> T first(final Class<T> type) {
            for (final AssetsEngineEvent e : events()) {
                if (type.isInstance(e)) {
                    return type.cast(e);
                }
            }
            throw new AssertionError("no " + type.getSimpleName() + " in " + events());
        }

        void deposit(final long user, final int asset, final long amount) {
            final DepositCommand d = new DepositCommand();
            d.reset();
            d.setUserId(user);
            d.setAssetId(asset);
            d.setAmount(amount);
            engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, TS);
        }

        void openGate() {
            engine.gate().set(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        }
    }

    private static CoreConfigCommand config(final long version, final long behavior) {
        return AssetsScenarioRunner.coreConfigFor(version, behavior, TICK, MIN, MAX, LOT, 900 + version);
    }

    /** A v1 config with one market row replaced; the config stays structurally valid. */
    private static CoreConfigCommand withMarket(final int index, final MarketDef replacement) {
        final CoreConfigCommand c = config(1, 1);
        c.markets = c.markets.clone();
        c.markets[index] = replacement;
        return c;
    }

    private static void assertAdoptedAndHalted(final Node n, final CoreConfigCommand c) {
        n.markNow();
        n.apply(c);
        final AssetsEngineEvent.AdminAck ack = n.onlyAdminAck();
        assertEquals("the replicated decision is ADOPT, never a node-local reject", AdminResult.ADOPTED, ack.result());
        assertNotNull("the config is recorded on every replica", n.cfg().recorded());
        assertEquals(1, n.cfg().adoptedCount());
        assertEquals("this node halts exactly once", 1, n.failFastCalls);
        assertNotNull("crossCheck() names the mismatch", n.cfg().crossCheck());
    }

    // ---- section 4a: markets against the compiled Market.ALL ----

    @Test
    public void adoptWithAMarketSymbolThatDiffersFromTheCompiledTableIsRecordedAndFailsFast() {
        final Node n = new Node();
        final MarketDef m1 = config(1, 1).markets[0];
        assertEquals("BTC-USD", m1.symbol());
        final CoreConfigCommand c = withMarket(0, new MarketDef(1, "XBT-USD", m1.baseAssetId(), m1.quoteAssetId(),
                m1.tickSize(), m1.minPrice(), m1.maxPrice(), m1.lotSize()));
        assertAdoptedAndHalted(n, c);
        assertEquals("XBT-USD", n.cfg().recorded().markets[0].symbol());
        final String reason = n.cfg().crossCheck();
        assertTrue("names the recorded side: " + reason, reason.contains("XBT-USD"));
        assertTrue("names the compiled side: " + reason, reason.contains("BTC-USD"));
    }

    @Test
    public void adoptMissingACompiledMarketFailsFast() {
        final Node n = new Node();
        final CoreConfigCommand c = config(1, 1);
        c.markets = Arrays.copyOf(c.markets, c.markets.length - 1); // DOGE-USD (5) dropped
        assertAdoptedAndHalted(n, c);
        assertEquals(4, n.cfg().recorded().markets.length);
    }

    @Test
    public void adoptWithAnExtraMarketIdNineFailsFast() {
        final Node n = new Node();
        final CoreConfigCommand c = config(1, 1);
        c.markets = Arrays.copyOf(c.markets, c.markets.length + 1);
        c.markets[c.markets.length - 1] = new MarketDef(9, "BTC-ETH", 1, 2, TICK, MIN, MAX, LOT);
        assertAdoptedAndHalted(n, c);
        assertEquals(6, n.cfg().recorded().markets.length);
    }

    @Test
    public void adoptWithBaseAndQuoteSwappedFailsFast() {
        final Node n = new Node();
        final MarketDef m1 = config(1, 1).markets[0];
        final CoreConfigCommand c = withMarket(0, new MarketDef(1, m1.symbol(), m1.quoteAssetId(), m1.baseAssetId(),
                m1.tickSize(), m1.minPrice(), m1.maxPrice(), m1.lotSize()));
        assertAdoptedAndHalted(n, c);
    }

    // ---- section 4b: assets against the compiled Asset table ----

    @Test
    public void adoptWithAnAssetCodeThatDiffersFromTheCompiledTableFailsFast() {
        final Node n = new Node();
        final CoreConfigCommand c = config(1, 1);
        c.assets = c.assets.clone();
        final AssetDef doge = c.assets[5];
        assertEquals("DOGE", doge.code());
        c.assets[5] = new AssetDef(doge.assetId(), "DOG", doge.scale());
        assertAdoptedAndHalted(n, c);
        final String reason = n.cfg().crossCheck();
        assertTrue("names both sides: " + reason, reason.contains("DOG") && reason.contains("DOGE"));
    }

    @Test
    public void validAdoptCallsNothing() {
        final Node n = new Node();
        n.markNow();
        n.apply(config(1, 1));
        assertEquals(AdminResult.ADOPTED, n.onlyAdminAck().result());
        assertNotNull(n.cfg().recorded());
        final AssetsEngineEvent.ConfigApplied applied = n.first(AssetsEngineEvent.ConfigApplied.class);
        assertEquals(ConfigKind.CORE, applied.kind());
        assertEquals(0, applied.marketId());
        assertEquals(1, applied.configVersion());
        assertEquals("a matching node does not halt", 0, n.failFastCalls);
        assertNull(n.cfg().crossCheck());
    }

    // ---- section 4c: the supported behaviour set ----

    @Test
    public void switchToBehaviorTwoOnAJarThatSupportsOnlyOneIsRecordedAndFailsFast() {
        final Node n = new Node();
        n.cfg().setSupportedBehaviorVersions(1);
        n.apply(config(1, 1));
        assertNotNull(n.cfg().recorded());
        assertEquals("v1 is supported: the adopt does not halt", 0, n.failFastCalls);
        n.markNow();
        n.apply(config(2, 2));
        final AssetsEngineEvent.AdminAck ack = n.onlyAdminAck();
        assertEquals("the switch is the replicated decision", AdminResult.ACCEPTED, ack.result());
        assertEquals(2, ack.configVersion());
        assertEquals(2, n.cfg().recorded().behaviorVersion);
        assertEquals(2, n.cfg().behaviorVersion());
        assertEquals(1, n.cfg().switchCount());
        assertEquals("this node cannot run v2: it halts exactly once", 1, n.failFastCalls);
        final String reason = n.cfg().crossCheck();
        assertNotNull(reason);
        assertTrue("names the version: " + reason, reason.contains("2"));
    }

    @Test
    public void restoredImageWhoseBehaviorVersionIsUnsupportedFailsFastAtRestore() {
        final Node a = new Node();
        a.apply(config(1, 1));
        a.apply(config(2, 2));
        assertEquals(2, a.cfg().behaviorVersion());
        final byte[] image = bytes(a.engine);

        final Node narrowed = new Node();
        narrowed.cfg().setSupportedBehaviorVersions(1);
        restoreInto(narrowed, image);
        assertEquals("a restored v2 image on a {1} jar halts at restore", 1, narrowed.failFastCalls);
        assertNotNull(narrowed.cfg().crossCheck());

        final Node full = new Node();
        restoreInto(full, image);
        assertEquals("control: the {1, 2} jar restores the same image without halting", 0, full.failFastCalls);
        assertEquals(2, full.cfg().behaviorVersion());
        assertNull(full.cfg().crossCheck());
    }

    // ---- section 2: snapshot section 8 CORE_CONFIG ----

    @Test
    public void noConfigRecordedWritesNoSectionEightAndRestoresToNoneRecorded() {
        final Node a = new Node();
        a.deposit(1, 0, 10 * ONE);
        final byte[] image = bytes(a.engine);
        assertFalse("section 8 is written only when a config is recorded: " + sectionIds(image),
                sectionIds(image).contains((int) BalanceSnapshotCodec.SECTION_CORE_CONFIG));
        final Node b = new Node();
        restoreInto(b, image);
        assertNull(b.cfg().recorded());
        assertEquals(1, b.cfg().behaviorVersion());
        assertEquals(0, b.failFastCalls);
        // "none recorded" is a real state: the first CoreConfig after this restore is an ADOPT
        b.markNow();
        b.apply(config(1, 1));
        assertEquals(AdminResult.ADOPTED, b.onlyAdminAck().result());
        assertNotNull(b.cfg().recorded());
    }

    @Test
    public void recordedV2ConfigRoundTripsEveryFieldAndReserializesByteIdentical() {
        final Node a = new Node();
        a.apply(config(1, 1));
        a.apply(config(2, 2));
        final CoreConfigCommand recorded = a.cfg().recorded();
        assertNotNull(recorded);
        final byte[] image = bytes(a.engine);
        assertTrue("section 8 present: " + sectionIds(image),
                sectionIds(image).contains((int) BalanceSnapshotCodec.SECTION_CORE_CONFIG));

        final Node b = new Node();
        restoreInto(b, image);
        final CoreConfigCommand back = b.cfg().recorded();
        assertNotNull(back);
        assertEquals(2, back.configVersion);
        assertEquals(2, back.behaviorVersion);
        assertEquals(recorded.configVersion, back.configVersion);
        assertEquals(recorded.behaviorVersion, back.behaviorVersion);
        assertArrayEquals("markets, ascending marketId, every field", recorded.markets, back.markets);
        assertArrayEquals("assets, ascending assetId, every field", recorded.assets, back.assets);
        assertEquals(TICK, back.markets[0].tickSize());
        assertEquals(MIN, back.markets[0].minPrice());
        assertEquals(MAX, back.markets[0].maxPrice());
        assertEquals(LOT, back.markets[0].lotSize());
        assertEquals(2, b.cfg().behaviorVersion());
        assertEquals(0, b.failFastCalls);
        assertArrayEquals("bytes are a function of state", image, bytes(b.engine));
    }

    @Test
    public void restoredV2EngineEnforcesTickLotAndBandAtAdmission() {
        final Node a = new Node();
        a.openGate();
        a.deposit(1, 0, 1_000_000 * ONE);
        a.apply(config(1, 1));
        a.apply(config(2, 2));
        assertEquals(2, a.cfg().behaviorVersion());
        final byte[] image = bytes(a.engine);

        final Node b = new Node();
        restoreInto(b, image);
        assertEquals(2, b.cfg().behaviorVersion());

        b.markNow();
        b.engine.orders().applyCreate(limit(1, 100 * ONE + 1, ONE, 1), TS);
        assertReject(b, CoreRejectReason.PRICE_OFF_TICK);
        b.markNow();
        b.engine.orders().applyCreate(limit(1, MAX + TICK, ONE, 2), TS);
        assertReject(b, CoreRejectReason.PRICE_OUT_OF_BAND);
        b.markNow();
        b.engine.orders().applyCreate(limit(1, 100 * ONE, ONE + 1, 3), TS);
        assertReject(b, CoreRejectReason.QTY_OFF_LOT);
        assertEquals("structural refusals store nothing", 0, b.engine.orders().orders().size());
        b.markNow();
        b.engine.orders().applyCreate(limit(1, 100 * ONE, ONE, 4), TS);
        assertNotNull("an on-tick, on-lot, in-band order passes", b.first(AssetsEngineEvent.OrderAck.class));
        assertEquals(1, b.engine.orders().orders().size());
    }

    // ---- helpers ----

    private static void assertReject(final Node n, final CoreRejectReason expected) {
        final AssetsEngineEvent.OrderReject rej = n.first(AssetsEngineEvent.OrderReject.class);
        assertEquals(expected, rej.reason());
        assertEquals(CREATE_ORDER_TEMPLATE, rej.refTemplateId());
    }

    private static CreateOrderCommand limit(final int market, final long price, final long qty, final long request) {
        final CreateOrderCommand c = new CreateOrderCommand();
        c.reset();
        c.correlationId = 7_000 + request;
        c.userId = 1;
        c.marketId = market;
        c.side = 0; // BUY
        c.orderType = 0; // LIMIT
        c.timeInForce = 0; // GTC
        c.price = price;
        c.quantity = qty;
        c.requestIdHigh = 0xC0FEL;
        c.requestIdLow = request;
        return c;
    }

    /** Copied from OrderSnapshotSectionsTest. */
    private static byte[] bytes(final AssetsEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    /**
     * OrderSnapshotSectionsTest's restore(), into a node whose failFast (and supported set) is installed BEFORE
     * the image is read: the cross-check runs during deserialize or verifyRestoredInvariants.
     */
    private static void restoreInto(final Node n, final byte[] image) {
        final BalanceSnapshotCodec.Decoded d =
                BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, n.engine);
        assertEquals(image.length, d.bytesConsumed);
        n.engine.verifyRestoredInvariants();
    }

    /** Section ids in the image: 8 longs of scalars, the flags byte, the section count, then id/length/payload. */
    private static List<Integer> sectionIds(final byte[] image) {
        final UnsafeBuffer b = new UnsafeBuffer(image);
        int pos = 8 * 8 + 1;
        final int count = b.getInt(pos);
        pos += 4;
        final List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add((int) b.getShort(pos));
            pos += 2;
            final int len = b.getInt(pos);
            pos += 4 + len;
        }
        assertEquals("section walk ends at the image end", image.length, pos);
        return ids;
    }
}

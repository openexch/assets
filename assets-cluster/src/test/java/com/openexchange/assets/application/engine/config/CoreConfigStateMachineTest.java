// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.config;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef;
import com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S7 contract section 3: the CoreConfig (70) decision table, driven through an {@link AssetsEngine} with a
 * {@link RecordingAssetsSink}. One scenario per test, named after the contract row it pins.
 */
public final class CoreConfigStateMachineTest {

    private static final int T_CORE = 70;
    private static final long TICK = 1_000_000L;            // 0.01
    private static final long MIN = 1_000_000L;             // 0.01
    private static final long MAX = 1_000_000_000_000_000L; // 10,000,000.00 (a tick multiple)
    private static final long LOT = 10_000L;                // 0.0001
    private static final int INVALID = CoreRejectReason.INVALID.code();

    private AssetsEngine engine;
    private RecordingAssetsSink sink;
    private CoreConfigStateMachine sm;
    private int failFastCalls;
    private long now = 1_700_000_000_000L;
    private long nextCorr = 70_000L;
    private int mark;

    @Before
    public void setUp() {
        engine = new AssetsEngine();
        sink = new RecordingAssetsSink();
        engine.setEventSink(sink);
        engine.setConfigFailFast(() -> failFastCalls++);
        sm = engine.coreConfig();
    }

    // ------------------------------------------------------------------ section 3 row 1: structural validity

    @Test public void row1_configVersionZeroIsInvalid() {
        assertStructuralReject(c -> { c.configVersion = 0; return c; });
    }
    @Test public void row1_behaviorVersionZeroIsInvalid() {
        assertStructuralReject(c -> { c.behaviorVersion = 0; return c; });
    }
    @Test public void row1_duplicateMarketIdIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 1, market(c.markets[1], 1, c.markets[1].symbol())));
    }
    @Test public void row1_marketIdZeroIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, market(c.markets[0], 0, c.markets[0].symbol())));
    }
    @Test public void row1_negativeMarketIdIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, market(c.markets[0], -1, c.markets[0].symbol())));
    }
    @Test public void row1_emptySymbolIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, market(c.markets[0], c.markets[0].marketId(), "")));
    }
    @Test public void row1_seventeenCharSymbolIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, market(c.markets[0], c.markets[0].marketId(), "ABCDEFGHIJKLMNOPQ")));
    }
    @Test public void row1_nonPrintableSymbolIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, market(c.markets[0], c.markets[0].marketId(), "BTC\u0001USD")));
    }
    @Test public void row1_baseEqualsQuoteIsInvalid() {
        assertStructuralReject(c -> {
            final MarketDef m = c.markets[0];
            return withMarket(c, 0, new MarketDef(m.marketId(), m.symbol(), m.quoteAssetId(), m.quoteAssetId(), m.tickSize(),
                    m.minPrice(), m.maxPrice(), m.lotSize()));
        });
    }
    @Test public void row1_baseAssetNotInAssetsGroupIsInvalid() {
        assertStructuralReject(c -> {
            final MarketDef m = c.markets[0];
            return withMarket(c, 0, new MarketDef(m.marketId(), m.symbol(), 9, m.quoteAssetId(), m.tickSize(), m.minPrice(),
                    m.maxPrice(), m.lotSize()));
        });
    }
    @Test public void row1_quoteAssetNotInAssetsGroupIsInvalid() {
        assertStructuralReject(c -> {
            final MarketDef m = c.markets[0];
            return withMarket(c, 0, new MarketDef(m.marketId(), m.symbol(), m.baseAssetId(), 9, m.tickSize(), m.minPrice(),
                    m.maxPrice(), m.lotSize()));
        });
    }
    @Test public void row1_marketAssetDroppedFromAssetsGroupIsInvalid() {
        // DOGE (5) is still referenced by DOGE-USD but no longer in the assets group.
        assertStructuralReject(c -> { c.assets = Arrays.copyOf(c.assets, c.assets.length - 1); return c; });
    }
    @Test public void row1_tickSizeZeroIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, prices(c.markets[0], 0L, MIN, MAX, LOT)));
    }
    @Test public void row1_minPriceZeroIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, prices(c.markets[0], TICK, 0L, MAX, LOT)));
    }
    @Test public void row1_minPriceEqualToMaxPriceIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, prices(c.markets[0], TICK, MAX, MAX, LOT)));
    }
    @Test public void row1_minPriceAboveMaxPriceIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, prices(c.markets[0], TICK, MAX, MIN, LOT)));
    }
    @Test public void row1_negativeLotSizeIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, prices(c.markets[0], TICK, MIN, MAX, -1L)));
    }
    @Test public void row1_minPriceNotATickMultipleIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, prices(c.markets[0], TICK, MIN + TICK / 2, MAX, LOT)));
    }
    @Test public void row1_maxPriceNotATickMultipleIsInvalid() {
        assertStructuralReject(c -> withMarket(c, 0, prices(c.markets[0], TICK, MIN, MAX + 1L, LOT)));
    }
    @Test public void row1_duplicateAssetIdIsInvalid() {
        assertStructuralReject(c -> withAsset(c, 2, new AssetDef(1, c.assets[2].code(), 8)));
    }
    @Test public void row1_emptyAssetCodeIsInvalid() {
        assertStructuralReject(c -> withAsset(c, 1, new AssetDef(c.assets[1].assetId(), "", 8)));
    }
    @Test public void row1_nineCharAssetCodeIsInvalid() {
        assertStructuralReject(c -> withAsset(c, 1, new AssetDef(c.assets[1].assetId(), "ABCDEFGHI", 8)));
    }
    @Test public void row1_nonPrintableAssetCodeIsInvalid() {
        assertStructuralReject(c -> withAsset(c, 1, new AssetDef(c.assets[1].assetId(), "B\u0007C", 8)));
    }
    @Test public void row1_assetScaleNotEightIsInvalid() {
        assertStructuralReject(c -> withAsset(c, 1, new AssetDef(c.assets[1].assetId(), c.assets[1].code(), 6)));
    }
    @Test public void row1_structuralRejectAppliesWhenRecordedAndPrecedesStale() {
        adoptV1();
        mark();
        // configVersion 0 is both "lower" and structurally invalid: rule 1 holds in every state, so INVALID, not STALE.
        final CoreConfigCommand c = base(0, 1);
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertEquals(1L, sm.rejectCount());
        assertEquals(0L, sm.staleCount());
        assertRecordedEquals(base(1, 1));
    }

    @Test public void row1_structuralRejectAtHigherVersionLeavesRecordedUnchanged() {
        adoptV1();
        mark();
        final CoreConfigCommand c = withMarket(base(2, 1), 0, prices(base(2, 1).markets[0], 0L, MIN, MAX, LOT));
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertEquals(1L, sm.rejectCount());
        assertRecordedEquals(base(1, 1));
        assertEquals(1L, sm.behaviorVersion());
    }

    // ------------------------------------------------------------------ section 3 row 2: ADOPT

    @Test public void row2_firstValidConfigIsAdopted() {
        mark();
        final CoreConfigCommand c = base(1, 1);
        engine.applyCoreConfig(c, now);
        final List<AssetsEngineEvent> e = events();
        assertEquals("AdminAck then ConfigApplied, got " + e, 2, e.size());
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.ADOPTED, 0, 1L), e.get(0));
        assertEquals(new AssetsEngineEvent.ConfigApplied(ConfigKind.CORE, 0, 1L), e.get(1));
        assertRecordedEquals(base(1, 1));
        assertEquals(1L, sm.behaviorVersion());
        assertEquals(1L, sm.adoptedCount());
        assertCounters(1, 0, 0, 0, 0, 0);
        assertEquals("compiled tables: the cross-check holds", 0, failFastCalls);
        assertNull(sm.crossCheck());
    }

    @Test public void row2_firstRecordMayCarryAnyConfigVersionAtLeastOne() {
        mark();
        final CoreConfigCommand c = base(7, 1);
        engine.applyCoreConfig(c, now);
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.ADOPTED, 0, 7L), firstEvent());
        assertTrue(events().contains(new AssetsEngineEvent.ConfigApplied(ConfigKind.CORE, 0, 7L)));
        assertRecordedEquals(base(7, 1));
    }

    @Test public void row2_recordedIsACopyNotTheCommandInstance() {
        final CoreConfigCommand c = base(1, 1);
        engine.applyCoreConfig(c, now);
        // The decoder reuses its command object; mutating it after apply must not move replicated state.
        c.configVersion = 99;
        c.behaviorVersion = 2;
        c.markets[0] = prices(c.markets[0], 5 * TICK, 5 * MIN, MAX, LOT);
        c.assets = new AssetDef[0];
        assertRecordedEquals(base(1, 1));
        assertEquals(1L, sm.behaviorVersion());
    }

    @Test public void row2_firstRecordWithBehaviorVersionTwoIsRejected() {
        mark();
        final CoreConfigCommand c = base(1, 2);
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertNull(sm.recorded());
        assertEquals(1L, sm.behaviorVersion());
        assertCounters(0, 0, 0, 0, 1, 0);
        assertEquals(0, failFastCalls);
    }

    @Test public void row2_rejectedFirstRecordThenValidOneIsAdopted() {
        engine.applyCoreConfig(base(1, 2), now);
        mark();
        final CoreConfigCommand c = base(1, 1);
        engine.applyCoreConfig(c, now);
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.ADOPTED, 0, 1L), firstEvent());
        assertRecordedEquals(base(1, 1));
        assertCounters(1, 0, 0, 0, 1, 0);
    }

    // ------------------------------------------------------------------ RC2 S7 7.1 row 2b: genesisId equality

    @Test public void row2b_sameConfigWithAnotherGenesisIdIsInvalid() {
        final CoreConfigCommand first = base(1, 1);
        first.genesisId = 0x5EEDL;
        engine.applyCoreConfig(first, now);
        assertEquals("row 2: the genesisId is recorded as sent", 0x5EEDL, sm.recorded().genesisId);
        mark();
        final CoreConfigCommand c = base(1, 1);
        c.genesisId = 0x5EEEL;
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertEquals(0x5EEDL, sm.recorded().genesisId);
        final CoreConfigCommand none = base(2, 2); // genesisId 0 against a recorded 0x5EED: a re-genesis attempt too
        engine.applyCoreConfig(none, now);
        assertEquals(1L, sm.behaviorVersion());
    }

    // ------------------------------------------------------------------ RC2 S7 7.1 row 7: the switch to behaviour 3

    @Test public void row7_targetThreeNeedsGenesisIdFeedVirginAndGlobalSuspended() {
        sm.setSupportedBehaviorVersions(1, 2, 3);
        adoptV1();
        engine.applyCoreConfig(base(2, 2), now);
        mark();
        final CoreConfigCommand c = base(3, 3);
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertEquals("recorded genesisId 0: no switch to 3", 2L, sm.behaviorVersion());
        assertEquals("decide reads the GLOBAL state and the feed-virgin scalars as arguments",
                CoreConfigStateMachine.Decision.INVALID, sm.decide(withGenesis(base(3, 3), 0L), false, true));
        final CoreConfigStateMachine fresh = new CoreConfigStateMachine();
        fresh.setSupportedBehaviorVersions(1, 2, 3);
        fresh.apply(fresh.decide(withGenesis(base(1, 1), 9L), false, true), withGenesis(base(1, 1), 9L));
        fresh.apply(fresh.decide(withGenesis(base(2, 2), 9L), false, true), withGenesis(base(2, 2), 9L));
        assertEquals(CoreConfigStateMachine.Decision.INVALID, fresh.decide(withGenesis(base(3, 3), 9L), true, true));
        assertEquals(CoreConfigStateMachine.Decision.INVALID, fresh.decide(withGenesis(base(3, 3), 9L), false, false));
        assertEquals(CoreConfigStateMachine.Decision.SWITCH, fresh.decide(withGenesis(base(3, 3), 9L), false, true));
    }

    private static CoreConfigCommand withGenesis(final CoreConfigCommand c, final long genesisId) {
        c.genesisId = genesisId;
        return c;
    }

    // ------------------------------------------------------------------ section 3 row 3: DUPLICATE

    @Test public void row3_identicalResendIsDuplicateNoOp() {
        adoptV1();
        mark();
        final CoreConfigCommand c = base(1, 1); // fresh correlation id: an operator retry
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.DUPLICATE, 0);
        assertEquals("duplicate acks the recorded version", 1L, ((AssetsEngineEvent.AdminAck) firstEvent()).configVersion());
        assertRecordedEquals(base(1, 1));
        assertCounters(1, 0, 1, 0, 0, 0);
    }

    @Test public void row3_duplicateAfterSwitchKeepsVersionTwoAndDoesNotSwitchAgain() {
        adoptV1();
        engine.applyCoreConfig(base(2, 2), now);
        mark();
        final CoreConfigCommand c = base(2, 2);
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.DUPLICATE, 0);
        assertEquals(2L, sm.behaviorVersion());
        assertEquals(1L, sm.switchCount());
        assertEquals(1L, sm.duplicateCount());
    }

    // ------------------------------------------------------------------ section 3 row 4: STALE

    @Test public void row4_sameVersionDifferentBehaviorIsStale() {
        adoptV1();
        mark();
        final CoreConfigCommand c = base(1, 2);
        engine.applyCoreConfig(c, now);
        assertStale(c, 1L);
        assertEquals(1L, sm.behaviorVersion());
        assertEquals(0L, sm.switchCount());
    }

    @Test public void row4_sameVersionDifferentTickIsStaleNotInvalid() {
        adoptV1();
        mark();
        final CoreConfigCommand c = cfg(1, 1, 2 * TICK, 2 * MIN, MAX, LOT);
        engine.applyCoreConfig(c, now);
        assertStale(c, 1L);
        assertEquals(0L, sm.rejectCount());
    }

    @Test public void row4_lowerVersionIsStale() {
        engine.applyCoreConfig(base(5, 1), now);
        mark();
        final CoreConfigCommand c = base(4, 1);
        engine.applyCoreConfig(c, now);
        assertStale(c, 5L);
        assertRecordedEquals(base(5, 1));
    }

    @Test public void row4_olderConfigResentAfterBumpIsStaleNotDuplicate() {
        adoptV1();
        engine.applyCoreConfig(base(2, 1), now);
        mark();
        final CoreConfigCommand c = base(1, 1);
        engine.applyCoreConfig(c, now);
        assertStale(c, 2L);
        assertEquals(0L, sm.duplicateCount());
        assertRecordedEquals(base(2, 1));
    }

    @Test public void row4_lowerVersionWithLowerBehaviorAfterSwitchIsStaleAndKeepsVersionTwo() {
        adoptV1();
        engine.applyCoreConfig(base(2, 2), now);
        mark();
        final CoreConfigCommand c = base(1, 1);
        engine.applyCoreConfig(c, now);
        assertStale(c, 2L);
        assertEquals(2L, sm.behaviorVersion());
        assertEquals(0L, sm.rejectCount());
    }

    // ------------------------------------------------------------------ section 3 row 5: higher version, tables

    @Test public void row5_higherVersionWithChangedTickIsRejected() {
        assertTableChangeRejected(cfg(2, 1, 2 * TICK, 2 * MIN, MAX, LOT));
    }

    @Test public void row5_higherVersionWithChangedPriceBandIsRejected() {
        assertTableChangeRejected(cfg(2, 1, TICK, 2 * MIN, MAX, LOT));
    }

    @Test public void row5_higherVersionWithChangedLotIsRejected() {
        assertTableChangeRejected(cfg(2, 1, TICK, MIN, MAX, 2 * LOT));
    }

    @Test public void row5_higherVersionWithChangedSymbolIsRejected() {
        final CoreConfigCommand c = base(2, 1);
        assertTableChangeRejected(withMarket(c, 0, market(c.markets[0], c.markets[0].marketId(), "XBT-USD")));
    }

    @Test public void row5_higherVersionWithAMarketRemovedIsRejected() {
        final CoreConfigCommand c = base(2, 1);
        c.markets = Arrays.copyOf(c.markets, c.markets.length - 1);
        assertTableChangeRejected(c);
    }

    @Test public void row5_higherVersionWithAMarketAddedIsRejected() {
        final CoreConfigCommand c = base(2, 1);
        final MarketDef[] ms = Arrays.copyOf(c.markets, c.markets.length + 1);
        ms[ms.length - 1] = new MarketDef(6, "ETH-BTC", 2, 1, TICK, MIN, MAX, LOT);
        c.markets = ms;
        assertTableChangeRejected(c);
    }

    @Test public void row5_higherVersionWithChangedAssetCodeIsRejected() {
        final CoreConfigCommand c = base(2, 1);
        assertTableChangeRejected(withAsset(c, 1, new AssetDef(c.assets[1].assetId(), "XBT", 8)));
    }

    @Test public void row5_higherVersionWithAnAssetAddedIsRejected() {
        final CoreConfigCommand c = base(2, 1);
        final AssetDef[] as = Arrays.copyOf(c.assets, c.assets.length + 1);
        as[as.length - 1] = new AssetDef(6, "ADA", 8);
        c.assets = as;
        assertTableChangeRejected(c);
    }

    @Test public void row5_tableChangeCarryingASwitchIsRejectedAndDoesNotSwitch() {
        assertTableChangeRejected(cfg(2, 2, 2 * TICK, 2 * MIN, MAX, LOT));
        assertEquals(1L, sm.behaviorVersion());
        assertEquals(0L, sm.switchCount());
    }

    // ------------------------------------------------------------------ section 3 row 5: higher version, behaviour

    @Test public void row5_higherVersionSameTablesSameBehaviorIsAcceptedVersionBumpOnly() {
        adoptV1();
        mark();
        final CoreConfigCommand c = base(2, 1);
        engine.applyCoreConfig(c, now);
        final List<AssetsEngineEvent> e = events();
        assertEquals("AdminAck then ConfigApplied, got " + e, 2, e.size());
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.ACCEPTED, 0, 2L), e.get(0));
        assertEquals(new AssetsEngineEvent.ConfigApplied(ConfigKind.CORE, 0, 2L), e.get(1));
        assertRecordedEquals(base(2, 1));
        assertEquals(1L, sm.behaviorVersion());
        assertCounters(1, 1, 0, 0, 0, 0);
    }

    @Test public void row5_higherVersionBehaviorPlusOneSwitchesToVersionTwo() {
        adoptV1();
        mark();
        final CoreConfigCommand c = base(2, 2);
        engine.applyCoreConfig(c, now);
        final List<AssetsEngineEvent> e = events();
        assertEquals("AdminAck then ConfigApplied, got " + e, 2, e.size());
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.ACCEPTED, 0, 2L), e.get(0));
        assertEquals(new AssetsEngineEvent.ConfigApplied(ConfigKind.CORE, 0, 2L), e.get(1));
        assertEquals(2L, sm.behaviorVersion());
        assertRecordedEquals(base(2, 2));
        assertCounters(1, 1, 0, 0, 0, 1);
        assertEquals("v2 is supported: the post-switch cross-check holds", 0, failFastCalls);
        assertNull(sm.crossCheck());
    }

    @Test public void row5_switchThenVersionBumpAtSameBehaviorKeepsVersionTwo() {
        adoptV1();
        engine.applyCoreConfig(base(2, 2), now);
        mark();
        final CoreConfigCommand c = base(3, 2);
        engine.applyCoreConfig(c, now);
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.ACCEPTED, 0, 3L), firstEvent());
        assertEquals(2L, sm.behaviorVersion());
        assertEquals(1L, sm.switchCount());
        assertEquals(2L, sm.acceptedCount());
    }

    @Test public void row5_behaviorSkipIsRejected() {
        adoptV1();
        mark();
        final CoreConfigCommand c = base(2, 3);
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertEquals(1L, sm.behaviorVersion());
        assertRecordedEquals(base(1, 1));
        assertCounters(1, 0, 0, 0, 1, 0);
        assertEquals(0, failFastCalls);
    }

    @Test public void row5_behaviorDowngradeIsRejected() {
        adoptV1();
        engine.applyCoreConfig(base(2, 2), now);
        mark();
        final CoreConfigCommand c = base(3, 1);
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertEquals(2L, sm.behaviorVersion());
        assertRecordedEquals(base(2, 2));
        assertCounters(1, 1, 0, 0, 1, 1);
    }

    // ------------------------------------------------------------------ section 3 row 6: acks carry the recorded version

    @Test public void row6_rejectWhileRecordedAcksTheRecordedVersion() {
        engine.applyCoreConfig(base(3, 1), now);
        mark();
        final CoreConfigCommand c = base(4, 3);
        engine.applyCoreConfig(c, now);
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.REJECTED, INVALID, 3L), firstEvent());
    }

    @Test public void row6_rejectWithNothingRecordedAcksVersionZero() {
        mark();
        final CoreConfigCommand c = base(4, 2);
        engine.applyCoreConfig(c, now);
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.REJECTED, INVALID, 0L), firstEvent());
    }

    // ------------------------------------------------------------------ ME parity: DUPLICATE and STALE never move state

    @Test public void parity_duplicateAndStaleNeverChangeState() {
        adoptV1();
        engine.applyCoreConfig(base(2, 2), now);
        final long version = sm.recorded().configVersion;
        engine.applyCoreConfig(base(2, 2), now);           // DUPLICATE
        engine.applyCoreConfig(base(2, 1), now);           // STALE (same version, other behaviour)
        engine.applyCoreConfig(base(1, 1), now);           // STALE (lower)
        engine.applyCoreConfig(cfg(2, 2, 2 * TICK, 2 * MIN, MAX, LOT), now); // STALE (same version, other tables)
        assertEquals(version, sm.recorded().configVersion);
        assertRecordedEquals(base(2, 2));
        assertEquals(2L, sm.behaviorVersion());
        assertCounters(1, 1, 1, 3, 0, 1);
    }

    @Test public void parity_duplicateAndStaleEmitNoConfigApplied() {
        adoptV1();
        mark();
        engine.applyCoreConfig(base(1, 1), now);
        engine.applyCoreConfig(base(1, 2), now);
        for (final AssetsEngineEvent e : events()) {
            assertTrue("only acks expected, got " + e, e instanceof AssetsEngineEvent.AdminAck);
        }
        assertEquals(2, events().size());
    }

    @Test public void counters_eachPathCountsOnce() {
        engine.applyCoreConfig(base(0, 1), now);                // reject (structural)
        engine.applyCoreConfig(base(1, 2), now);                // reject (first record not v1)
        engine.applyCoreConfig(base(1, 1), now);                // adopt
        engine.applyCoreConfig(base(1, 1), now);                // duplicate
        engine.applyCoreConfig(base(1, 2), now);                // stale
        engine.applyCoreConfig(base(2, 1), now);                // accepted, bump
        engine.applyCoreConfig(cfg(3, 1, 2 * TICK, 2 * MIN, MAX, LOT), now); // reject (table change)
        engine.applyCoreConfig(base(3, 2), now);                // accepted, switch
        engine.applyCoreConfig(base(4, 1), now);                // reject (downgrade)
        assertCounters(1, 2, 1, 1, 4, 1);
        assertEquals(2L, sm.behaviorVersion());
        assertRecordedEquals(base(3, 2));
    }

    // ------------------------------------------------------------------ assertions

    private void assertStructuralReject(final UnaryOperator<CoreConfigCommand> mutate) {
        mark();
        final CoreConfigCommand c = mutate.apply(base(1, 1));
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertNull("an invalid config is never recorded", sm.recorded());
        assertEquals(1L, sm.behaviorVersion());
        assertCounters(0, 0, 0, 0, 1, 0);
        assertEquals("structural rejects never reach the cross-check", 0, failFastCalls);
    }

    private void assertTableChangeRejected(final CoreConfigCommand c) {
        adoptV1();
        mark();
        engine.applyCoreConfig(c, now);
        assertOnlyAck(c.correlationId, AdminResult.REJECTED, INVALID);
        assertRecordedEquals(base(1, 1));
        assertEquals(1L, sm.behaviorVersion());
        assertCounters(1, 0, 0, 0, 1, 0);
        assertEquals(0, failFastCalls);
    }

    private void assertStale(final CoreConfigCommand c, final long recordedVersion) {
        final List<AssetsEngineEvent> e = events();
        assertEquals("one AdminAck, got " + e, 1, e.size());
        assertEquals(new AssetsEngineEvent.AdminAck(c.correlationId, T_CORE, AdminResult.STALE, 0, recordedVersion), e.get(0));
        assertEquals(1L, sm.staleCount());
        assertEquals(recordedVersion, sm.recorded().configVersion);
    }

    /** Exactly one AdminAck for ref 70 with this result and reason; configVersion is pinned by the row 6 tests. */
    private void assertOnlyAck(final long corr, final AdminResult result, final int reason) {
        final List<AssetsEngineEvent> e = events();
        assertEquals("one AdminAck, got " + e, 1, e.size());
        assertTrue("expected AdminAck, got " + e.get(0), e.get(0) instanceof AssetsEngineEvent.AdminAck);
        final AssetsEngineEvent.AdminAck a = (AssetsEngineEvent.AdminAck) e.get(0);
        assertEquals(corr, a.correlationId());
        assertEquals(T_CORE, a.refTemplateId());
        assertEquals(result, a.result());
        assertEquals(reason, a.reason());
    }

    private void assertRecordedEquals(final CoreConfigCommand expected) {
        final CoreConfigCommand r = sm.recorded();
        assertNotNull("a config is recorded", r);
        assertEquals("configVersion", expected.configVersion, r.configVersion);
        assertEquals("behaviorVersion", expected.behaviorVersion, r.behaviorVersion);
        assertArrayEquals("markets", expected.markets, r.markets);
        assertArrayEquals("assets", expected.assets, r.assets);
    }

    private void assertCounters(final long adopted, final long accepted, final long duplicate, final long stale,
                                final long reject, final long switches) {
        assertEquals("adoptedCount", adopted, sm.adoptedCount());
        assertEquals("acceptedCount", accepted, sm.acceptedCount());
        assertEquals("duplicateCount", duplicate, sm.duplicateCount());
        assertEquals("staleCount", stale, sm.staleCount());
        assertEquals("rejectCount", reject, sm.rejectCount());
        assertEquals("switchCount", switches, sm.switchCount());
    }

    // ------------------------------------------------------------------ fixtures

    private void adoptV1() {
        engine.applyCoreConfig(base(1, 1), now);
        assertNotNull("precondition: v1 adopted", sm.recorded());
    }

    private CoreConfigCommand base(final long version, final long behavior) {
        return cfg(version, behavior, TICK, MIN, MAX, LOT);
    }

    private CoreConfigCommand cfg(final long version, final long behavior, final long tick, final long min, final long max,
                                  final long lot) {
        return AssetsScenarioRunner.coreConfigFor(version, behavior, tick, min, max, lot, nextCorr++);
    }

    private static CoreConfigCommand withMarket(final CoreConfigCommand c, final int index, final MarketDef m) {
        final MarketDef[] ms = c.markets.clone();
        ms[index] = m;
        c.markets = ms;
        return c;
    }

    private static CoreConfigCommand withAsset(final CoreConfigCommand c, final int index, final AssetDef a) {
        final AssetDef[] as = c.assets.clone();
        as[index] = a;
        c.assets = as;
        return c;
    }

    private static MarketDef market(final MarketDef m, final int marketId, final String symbol) {
        return new MarketDef(marketId, symbol, m.baseAssetId(), m.quoteAssetId(), m.tickSize(), m.minPrice(), m.maxPrice(),
                m.lotSize());
    }

    private static MarketDef prices(final MarketDef m, final long tick, final long min, final long max, final long lot) {
        return new MarketDef(m.marketId(), m.symbol(), m.baseAssetId(), m.quoteAssetId(), tick, min, max, lot);
    }

    private AssetsEngineEvent firstEvent() {
        final List<AssetsEngineEvent> e = events();
        assertTrue("expected at least one event, got none", !e.isEmpty());
        return e.get(0);
    }

    private void mark() {
        mark = sink.events().size();
    }

    private List<AssetsEngineEvent> events() {
        return sink.events().subList(mark, sink.events().size());
    }
}

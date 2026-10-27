// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.config;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef;
import com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef;

import java.util.Arrays;
import java.util.Comparator;

/**
 * RC1 S7 (contract RC1-core/S7/contract.md): the replicated CoreConfig record and the behaviour version. Every
 * {@link #decide} reads only the command and the recorded state, so all replicas decide alike; the node-local truth
 * (compiled tables, the jar's supported behaviour versions) is only ever compared in {@link #crossCheck}, whose failure
 * stops the node instead of rejecting (A-9 rule, as the ME's EngineConfigStateMachine).
 */
public final class CoreConfigStateMachine {

    /** Behaviour versions this jar can run (contract section 4c). */
    public static final int[] SUPPORTED_BEHAVIOR_VERSIONS = {1, 2};
    public static final int SCALE = 8;
    private static final int MAX_SYMBOL = 16;
    private static final int MAX_CODE = 8;

    /** The outcome of one CoreConfig against the recorded state. */
    public enum Decision { INVALID, ADOPT, DUPLICATE, STALE, ACCEPT, SWITCH }

    private int[] supported = SUPPORTED_BEHAVIOR_VERSIONS.clone();
    private CoreConfigCommand recorded;
    private MarketDef[] byMarketId = new MarketDef[0];

    private long adoptedCount;
    private long acceptedCount;
    private long duplicateCount;
    private long staleCount;
    private long rejectCount;
    private long switchCount;

    /** The recorded config, or null when none was accepted yet. */
    public CoreConfigCommand recorded() {
        return recorded;
    }

    /** 1 when nothing is recorded. */
    public long behaviorVersion() {
        return recorded == null ? 1L : recorded.behaviorVersion;
    }

    /** The recorded market row, or null (no config, or an unknown market). */
    public MarketDef market(final int marketId) {
        return marketId >= 0 && marketId < byMarketId.length ? byMarketId[marketId] : null;
    }

    /** Test seam: the jar's supported set (contract section 4c). */
    public void setSupportedBehaviorVersions(final int... versions) {
        this.supported = versions.clone();
    }

    public int[] supportedBehaviorVersions() {
        return supported.clone();
    }

    /**
     * Section 3 (RC1 S7) with the RC2 S7 rows (contract 7.1): the decision for {@code c}, without mutating anything.
     * {@code globalOpen} and {@code feedVirgin} are replicated inputs the engine passes in (review 2 NOTE-E: this method
     * never reaches into the engine); they matter only to row 7, the switch to behaviour 3.
     */
    public Decision decide(final CoreConfigCommand c, final boolean globalOpen, final boolean feedVirgin) {
        if (invalidReason(c) != null) {
            return Decision.INVALID;
        }
        if (recorded == null) {
            return c.behaviorVersion == 1 ? Decision.ADOPT : Decision.INVALID;
        }
        // row 2b: a genesisId other than the recorded one is a re-genesis attempt
        if (c.genesisId != recorded.genesisId) {
            return Decision.INVALID;
        }
        if (c.configVersion == recorded.configVersion && c.behaviorVersion == recorded.behaviorVersion && tablesEqual(c, recorded)) {
            return Decision.DUPLICATE;
        }
        if (c.configVersion <= recorded.configVersion) {
            return Decision.STALE;
        }
        if (!tablesEqual(c, recorded)) {
            return Decision.INVALID; // runtime market/asset change is not in RC1 (contract 3 row 5)
        }
        if (c.behaviorVersion == recorded.behaviorVersion) {
            return Decision.ACCEPT;
        }
        // row 7: the switch to 3 needs a recorded genesisId, a feed-virgin core and GLOBAL SUSPENDED (no order can be admitted
        // between the switch and the bind)
        if (c.behaviorVersion == 3 && recorded.behaviorVersion == 2 && (recorded.genesisId == 0 || !feedVirgin || globalOpen)) {
            return Decision.INVALID;
        }
        return c.behaviorVersion == recorded.behaviorVersion + 1 ? Decision.SWITCH : Decision.INVALID;
    }

    /** Apply a decision taken by {@link #decide}; returns the recorded configVersion afterwards. */
    public long apply(final Decision d, final CoreConfigCommand c) {
        switch (d) {
            case ADOPT: adoptedCount++; install(c); break;
            case ACCEPT: acceptedCount++; install(c); break;
            case SWITCH: acceptedCount++; switchCount++; install(c); break;
            case DUPLICATE: duplicateCount++; break;
            case STALE: staleCount++; break;
            default: rejectCount++; break;
        }
        return recorded == null ? 0L : recorded.configVersion;
    }

    /** Restore (section 8): install the recorded config without decisions; the caller then cross-checks. */
    public void restore(final CoreConfigCommand config) {
        install(config);
    }

    /**
     * Node-local cross-check (section 4) against the compiled tables and this jar's supported behaviour versions.
     * Null when it holds (or nothing is recorded), else the reason naming both sides.
     */
    public String crossCheck() {
        if (recorded == null) {
            return null;
        }
        if (Arrays.stream(supported).noneMatch(v -> v == recorded.behaviorVersion)) {
            return "behaviorVersion " + recorded.behaviorVersion + " is not supported by this jar " + Arrays.toString(supported);
        }
        if (recorded.markets.length != Market.ALL.length) {
            return "declared " + recorded.markets.length + " markets, compiled " + Market.ALL.length;
        }
        for (final Market m : Market.ALL) {
            final MarketDef d = market(m.marketId());
            if (d == null || !d.symbol().equals(m.symbol()) || d.baseAssetId() != m.baseAsset().id()
                    || d.quoteAssetId() != m.quoteAsset().id()) {
                return "market " + m.marketId() + " declared " + d + " compiled " + m;
            }
        }
        final Asset[] compiled = Asset.values();
        if (recorded.assets.length != compiled.length) {
            return "declared " + recorded.assets.length + " assets, compiled " + compiled.length;
        }
        for (final AssetDef a : recorded.assets) {
            if (a.assetId() < 0 || a.assetId() >= compiled.length || !compiled[a.assetId()].name().equals(a.code())
                    || a.scale() != SCALE) {
                return "asset " + a + " differs from the compiled " + Arrays.toString(compiled);
            }
        }
        return null;
    }

    /** Section 3 row 1: structural validity, or null. */
    public static String invalidReason(final CoreConfigCommand c) {
        if (c.configVersion < 1 || c.behaviorVersion < 1) {
            return "configVersion and behaviorVersion must be >= 1";
        }
        final java.util.Set<Integer> assetIds = new java.util.HashSet<>();
        for (final AssetDef a : c.assets) {
            if (!assetIds.add(a.assetId())) {
                return "duplicate assetId " + a.assetId();
            }
            if (!printable(a.code(), MAX_CODE)) {
                return "asset code " + a.code();
            }
            if (a.scale() != SCALE) {
                return "asset scale " + a.scale();
            }
        }
        final java.util.Set<Integer> marketIds = new java.util.HashSet<>();
        for (final MarketDef m : c.markets) {
            if (m.marketId() <= 0 || !marketIds.add(m.marketId())) {
                return "marketId " + m.marketId();
            }
            if (!printable(m.symbol(), MAX_SYMBOL)) {
                return "symbol " + m.symbol();
            }
            if (m.baseAssetId() == m.quoteAssetId() || !assetIds.contains(m.baseAssetId()) || !assetIds.contains(m.quoteAssetId())) {
                return "market " + m.marketId() + " assets";
            }
            if (m.tickSize() <= 0 || m.minPrice() <= 0 || m.minPrice() >= m.maxPrice() || m.lotSize() < 0
                    || m.minPrice() % m.tickSize() != 0 || m.maxPrice() % m.tickSize() != 0) {
                return "market " + m.marketId() + " tick/band/lot";
            }
        }
        return null;
    }

    private static boolean printable(final String s, final int max) {
        if (s == null || s.isEmpty() || s.length() > max) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            final char ch = s.charAt(i);
            if (ch < 0x21 || ch > 0x7E) {
                return false;
            }
        }
        return true;
    }

    private static boolean tablesEqual(final CoreConfigCommand a, final CoreConfigCommand b) {
        return Arrays.equals(sortedMarkets(a.markets), sortedMarkets(b.markets)) && Arrays.equals(sortedAssets(a.assets), sortedAssets(b.assets));
    }

    private static MarketDef[] sortedMarkets(final MarketDef[] in) {
        final MarketDef[] out = in.clone();
        Arrays.sort(out, Comparator.comparingInt(MarketDef::marketId));
        return out;
    }

    private static AssetDef[] sortedAssets(final AssetDef[] in) {
        final AssetDef[] out = in.clone();
        Arrays.sort(out, Comparator.comparingInt(AssetDef::assetId));
        return out;
    }

    /** Record a canonical copy (sorted by id) and index the markets. */
    private void install(final CoreConfigCommand c) {
        final CoreConfigCommand copy = new CoreConfigCommand();
        copy.correlationId = 0L;
        copy.configVersion = c.configVersion;
        copy.behaviorVersion = c.behaviorVersion;
        copy.genesisId = c.genesisId; // RC2 S7 7.1 row 2: recorded as sent (section 8 layout v2)
        copy.markets = sortedMarkets(c.markets);
        copy.assets = sortedAssets(c.assets);
        recorded = copy;
        int max = 0;
        for (final MarketDef m : copy.markets) {
            max = Math.max(max, m.marketId());
        }
        byMarketId = new MarketDef[max + 1];
        for (final MarketDef m : copy.markets) {
            byMarketId[m.marketId()] = m;
        }
    }

    public long adoptedCount() { return adoptedCount; }
    public long acceptedCount() { return acceptedCount; }
    public long duplicateCount() { return duplicateCount; }
    public long staleCount() { return staleCount; }
    public long rejectCount() { return rejectCount; }
    public long switchCount() { return switchCount; }
}

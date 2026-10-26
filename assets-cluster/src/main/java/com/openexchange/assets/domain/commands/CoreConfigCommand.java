// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

import java.util.Arrays;

/** CoreConfig (70): the declared market and asset tables plus the behaviour version (RC1 S7 contract). */
public final class CoreConfigCommand {

    /** One market row, as on the wire. */
    public record MarketDef(int marketId, String symbol, int baseAssetId, int quoteAssetId, long tickSize, long minPrice,
                            long maxPrice, long lotSize) { }

    /** One asset row, as on the wire. */
    public record AssetDef(int assetId, String code, int scale) { }

    public long correlationId;
    public long configVersion;
    public long behaviorVersion;
    /**
     * money-schema v7 (RC2 S1 4.4): the genesis nonce, decoded from the wire only. It is NOT part of the DUPLICATE
     * equality and NOT recorded ({@code CoreConfigStateMachine.install} does not copy it, the snapshot does not carry
     * it); RC2 S7 records it (section 8 layout v2) and puts the recorded value into the equality.
     */
    public long genesisId;
    public MarketDef[] markets = new MarketDef[0];
    public AssetDef[] assets = new AssetDef[0];

    public void reset() {
        correlationId = 0;
        configVersion = 0;
        behaviorVersion = 0;
        genesisId = 0;
        markets = new MarketDef[0];
        assets = new AssetDef[0];
    }

    @Override
    public String toString() {
        return "CoreConfig{v=" + configVersion + " behavior=" + behaviorVersion + " markets=" + Arrays.toString(markets)
                + " assets=" + Arrays.toString(assets) + "}";
    }
}

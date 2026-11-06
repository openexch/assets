// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** Pooled RiskConfigSet (money-schema v6 template 71): the FULL per-market record with a monotonic version. */
public final class RiskConfigSetCommand {
    public long correlationId;
    public long configVersion;
    public int marketId;
    public long minQuantity;
    public long maxQuantity;
    public long minNotional;
    public long maxNotional;
    public int priceCollarBps;
    public int maxOrdersPerSec;
    public int maxOrdersPerMin;
    public int maxOpenOrders;
    public long maxPositionPerMarket;
    public int marketBuySlippageBps;

    public void reset() {
        correlationId = 0; configVersion = 0; marketId = 0; minQuantity = 0; maxQuantity = 0; minNotional = 0; maxNotional = 0;
        priceCollarBps = 0; maxOrdersPerSec = 0; maxOrdersPerMin = 0; maxOpenOrders = 0; maxPositionPerMarket = 0; marketBuySlippageBps = 0;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** RC0 spike: per-market risk limits, replicated through the log. Pooled. */
public final class ProtoRiskConfigCommand {
    public long correlationId;
    public int marketId;
    public int maxOrdersPerSec;
    public int maxOrdersPerMin;
    public long minQty;
    public long maxQty;
    public long maxNotional;
    public int collarPct;
    public int maxOpenOrders;
    public long maxPositionPerMarket;
    public boolean circuitBreaker;

    public void reset() {
        correlationId = 0; marketId = 0; maxOrdersPerSec = 0; maxOrdersPerMin = 0; minQty = 0; maxQty = 0; maxNotional = 0;
        collarPct = 0; maxOpenOrders = 0; maxPositionPerMarket = 0; circuitBreaker = false;
    }
}

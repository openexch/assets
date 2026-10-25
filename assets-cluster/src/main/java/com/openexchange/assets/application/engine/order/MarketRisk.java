// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

/**
 * Per-market replicated state (risk spec B3; layout section 4): the full RiskConfigSet record with its
 * version, the manual circuit breaker, the collar reference (last applied trade) and the top of book from
 * the last applied MarketTick (market-buy budget; triggers in S6). Genesis = today's boot defaults for
 * marketId 1..5 with the collar and slippage in basis points.
 */
public final class MarketRisk {

    /** Section 4 record: 12 longs, 5 ints, 1 byte (layout page). */
    public static final int SNAPSHOT_BYTES = 8 * 12 + 4 * 5 + 1;

    public final int marketId;
    public long configVersion;
    public long minQuantity = 1L;
    public long maxQuantity = Long.MAX_VALUE;
    public long minNotional = 0L;
    public long maxNotional = Long.MAX_VALUE;
    public long maxPositionPerMarket = Long.MAX_VALUE;
    public int priceCollarBps = 2_000;
    public int maxOrdersPerSec = 100;
    public int maxOrdersPerMin = 1_000;
    public int maxOpenOrders = 500;
    public int marketBuySlippageBps = 500;
    public boolean breakerTripped;
    public long lastTradePrice;
    public long lastTradeId;
    public long bestBid;
    public long bestAsk;
    public long bookVersion;
    public long lastTickTs;

    public MarketRisk(final int marketId) {
        this.marketId = marketId;
    }

    /** Is {@code candidate} a structurally valid full record (risk spec B3 validation table)? */
    public static boolean valid(final long minQuantity, final long maxQuantity, final long minNotional, final long maxNotional,
                                final int priceCollarBps, final int maxOrdersPerSec, final int maxOrdersPerMin,
                                final int maxOpenOrders, final long maxPositionPerMarket, final int marketBuySlippageBps) {
        return minQuantity >= 1 && maxQuantity >= 1 && minQuantity <= maxQuantity
                && minNotional >= 0 && maxNotional >= 0 && minNotional <= maxNotional
                && priceCollarBps >= 0 && maxOrdersPerSec >= 1 && maxOrdersPerMin >= maxOrdersPerSec
                && maxOpenOrders >= 1 && maxPositionPerMarket >= 1
                && marketBuySlippageBps >= 0 && marketBuySlippageBps <= 10_000;
    }

    public boolean sameConfig(final long minQuantity, final long maxQuantity, final long minNotional, final long maxNotional,
                              final int priceCollarBps, final int maxOrdersPerSec, final int maxOrdersPerMin,
                              final int maxOpenOrders, final long maxPositionPerMarket, final int marketBuySlippageBps) {
        return this.minQuantity == minQuantity && this.maxQuantity == maxQuantity && this.minNotional == minNotional
                && this.maxNotional == maxNotional && this.priceCollarBps == priceCollarBps
                && this.maxOrdersPerSec == maxOrdersPerSec && this.maxOrdersPerMin == maxOrdersPerMin
                && this.maxOpenOrders == maxOpenOrders && this.maxPositionPerMarket == maxPositionPerMarket
                && this.marketBuySlippageBps == marketBuySlippageBps;
    }

    public void applyConfig(final long configVersion, final long minQuantity, final long maxQuantity, final long minNotional,
                            final long maxNotional, final int priceCollarBps, final int maxOrdersPerSec, final int maxOrdersPerMin,
                            final int maxOpenOrders, final long maxPositionPerMarket, final int marketBuySlippageBps) {
        this.configVersion = configVersion;
        this.minQuantity = minQuantity;
        this.maxQuantity = maxQuantity;
        this.minNotional = minNotional;
        this.maxNotional = maxNotional;
        this.priceCollarBps = priceCollarBps;
        this.maxOrdersPerSec = maxOrdersPerSec;
        this.maxOrdersPerMin = maxOrdersPerMin;
        this.maxOpenOrders = maxOpenOrders;
        this.maxPositionPerMarket = maxPositionPerMarket;
        this.marketBuySlippageBps = marketBuySlippageBps;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.proto;

/** RC0 spike: per-market replicated state: last trade (collar reference) and the risk limits. */
public final class ProtoMarketState {
    /** Serialized size: lastTradePrice, minQty, maxQty, maxNotional, maxPosition = 5 longs; 5 ints; 1 byte. */
    public static final int SNAPSHOT_BYTES = 5 * 8 + 5 * 4 + 1;

    public long lastTradePrice;
    public int openOrders;
    public boolean circuitBreaker;
    // OMS boot defaults per the 25 Oct inventory: collar 20%, 100/s, 1000/min, 500 open orders.
    public int maxOrdersPerSec = 100;
    public int maxOrdersPerMin = 1_000;
    public long minQty = 1L;
    public long maxQty = Long.MAX_VALUE / 4;
    public long maxNotional = Long.MAX_VALUE / 4;
    public int collarPct = 20;
    public int maxOpenOrders = 500;
    public long maxPositionPerMarket = Long.MAX_VALUE;
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** Pooled CreateOrder (money-schema v6 template 50), fields as on the wire; codes are wire codes. */
public final class CreateOrderCommand {
    public long correlationId;
    public long userId;
    public long clientOrderIdHigh;
    public long clientOrderIdLow;
    public long requestIdHigh;
    public long requestIdLow;
    public int marketId;
    public int side;
    public int orderType;
    public int timeInForce;
    public long price;
    public long quantity;
    public long budget;
    public long stopPrice;
    public long trailingDelta;
    public long displayQty;
    public long expiresAtMs;
    public int flags;

    public void reset() {
        correlationId = 0; userId = 0; clientOrderIdHigh = 0; clientOrderIdLow = 0; requestIdHigh = 0; requestIdLow = 0;
        marketId = 0; side = 0; orderType = 0; timeInForce = 0; price = 0; quantity = 0; budget = 0; stopPrice = 0;
        trailingDelta = 0; displayQty = 0; expiresAtMs = 0; flags = 0;
    }

    /** Payload fingerprint for requestId replay detection: every field but correlationId and the requestId itself. */
    public long fingerprint() {
        long h = 0x9E3779B97F4A7C15L;
        h = mix(h, userId); h = mix(h, clientOrderIdHigh); h = mix(h, clientOrderIdLow); h = mix(h, marketId);
        h = mix(h, side); h = mix(h, orderType); h = mix(h, timeInForce); h = mix(h, price); h = mix(h, quantity);
        h = mix(h, budget); h = mix(h, stopPrice); h = mix(h, trailingDelta); h = mix(h, displayQty); h = mix(h, expiresAtMs);
        h = mix(h, flags);
        return h;
    }

    public static long mix(long h, long v) {
        h ^= v + 0x9E3779B97F4A7C15L + (h << 6) + (h >>> 2);
        h *= 0xBF58476D1CE4E5B9L;
        return h ^ (h >>> 31);
    }
}

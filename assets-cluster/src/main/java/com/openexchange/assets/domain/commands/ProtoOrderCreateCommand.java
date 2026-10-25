// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** RC0 spike: place an order in the core. Pooled; {@link #reset()} for reuse. Field codes are the wire enums' values. */
public final class ProtoOrderCreateCommand {
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
    public long stopPrice;
    public long displayQty;
    public long expiresAtMs;
    public int flags;

    public void reset() {
        correlationId = 0; userId = 0; clientOrderIdHigh = 0; clientOrderIdLow = 0; requestIdHigh = 0; requestIdLow = 0;
        marketId = 0; side = 0; orderType = 0; timeInForce = 0; price = 0; quantity = 0; stopPrice = 0; displayQty = 0;
        expiresAtMs = 0; flags = 0;
    }
}

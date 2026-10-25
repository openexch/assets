// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.proto;

/**
 * RC0 spike: one order as the core would keep it. The field set mirrors the OMS {@code OmsOrder}
 * (27 longs, 1 int, 4 enums, 2 strings, 1 boolean per the 25 Oct inventory) with the two strings
 * carried as 128-bit ids, so the per-order snapshot cost is realistic. Pooled; plain fields.
 */
public final class ProtoOrder {
    /** Serialized size of one order in the v4 snapshot block: 22 longs, 3 ints, 6 bytes. */
    public static final int SNAPSHOT_BYTES = 22 * 8 + 3 * 4 + 6;

    public long orderId;
    public long userId;
    public long clientOrderIdHigh;
    public long clientOrderIdLow;
    public long requestIdHigh;
    public long requestIdLow;
    public int marketId;
    public byte side;
    public byte orderType;
    public byte timeInForce;
    public int flags;
    public long price;
    public long quantity;
    public long stopPrice;
    public long displayQty;
    public long expiresAtMs;
    public byte status;
    public byte rejectReason;
    public long filledQty;
    public long remainingQty;
    public long cumQuoteQty;
    public long lastPrice;
    public long lastQty;
    public int holdAssetId;
    public long holdAmount;
    public long createdAtMs;
    public long updatedAtMs;
    public long eventSeq;
    public long parentOrderId;
    public long sliceRemainingQty;
    public byte cancelRequested;

    void clear() {
        orderId = 0; userId = 0; clientOrderIdHigh = 0; clientOrderIdLow = 0; requestIdHigh = 0; requestIdLow = 0;
        marketId = 0; side = 0; orderType = 0; timeInForce = 0; flags = 0; price = 0; quantity = 0; stopPrice = 0;
        displayQty = 0; expiresAtMs = 0; status = 0; rejectReason = 0; filledQty = 0; remainingQty = 0; cumQuoteQty = 0;
        lastPrice = 0; lastQty = 0; holdAssetId = 0; holdAmount = 0; createdAtMs = 0; updatedAtMs = 0; eventSeq = 0;
        parentOrderId = 0; sliceRemainingQty = 0; cancelRequested = 0;
    }

    /** Notional at the limit price (0 for a price-less order): what the user's open-notional sum carries. */
    long notional() {
        return price > 0 ? com.openexchange.assets.domain.FixedPoint.multiply(price, remainingQty) : 0L;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** AmendOrder (52): cancel/replace with a new leg; newPrice 0 keeps the price, newQuantity 0 keeps the TOTAL quantity. */
public final class AmendOrderCommand {
    public long correlationId;
    public long userId;
    public long orderId;
    public long requestIdHigh;
    public long requestIdLow;
    public long newPrice;
    public long newQuantity;

    public void reset() {
        correlationId = 0; userId = 0; orderId = 0; requestIdHigh = 0; requestIdLow = 0; newPrice = 0; newQuantity = 0;
    }

    public boolean hasRequestId() {
        return requestIdHigh != 0 || requestIdLow != 0;
    }

    /** Payload fingerprint for requestId replay detection (userId, orderId, newPrice, newQuantity). */
    public long fingerprint() {
        long h = 0x416d656e64L;
        h = CreateOrderCommand.mix(h, userId);
        h = CreateOrderCommand.mix(h, orderId);
        h = CreateOrderCommand.mix(h, newPrice);
        h = CreateOrderCommand.mix(h, newQuantity);
        return h;
    }
}

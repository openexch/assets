// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** Pooled CancelOrder (money-schema v6 template 51). orderId 0 = cancel by origClientOrderId. */
public final class CancelOrderCommand {
    public long correlationId;
    public long userId;
    public long orderId;
    public long requestIdHigh;
    public long requestIdLow;
    public long origClientOrderIdHigh;
    public long origClientOrderIdLow;

    public void reset() {
        correlationId = 0; userId = 0; orderId = 0; requestIdHigh = 0; requestIdLow = 0;
        origClientOrderIdHigh = 0; origClientOrderIdLow = 0;
    }

    public long fingerprint() {
        long h = 0x51L;
        h = CreateOrderCommand.mix(h, userId); h = CreateOrderCommand.mix(h, orderId);
        h = CreateOrderCommand.mix(h, origClientOrderIdHigh); h = CreateOrderCommand.mix(h, origClientOrderIdLow);
        return h;
    }
}

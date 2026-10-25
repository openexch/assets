// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Read-only view of one core order as the outbound port needs it (RC1 S4). The application-layer record
 * implements it; the publisher encodes from it. Values are raw 8dp fixed-point longs and wire codes.
 */
public interface CoreOrderView {
    long orderId();
    long userId();
    int marketId();
    int statusCode();
    int sideCode();
    int orderTypeCode();
    int timeInForceCode();
    long price();
    long stopPrice();
    long quantity();
    long filledQty();
    long remainingQty();
    long meOrderId();
    int holdAssetId();
    long holdAmount();
    long clientOrderIdHigh();
    long clientOrderIdLow();
    long createdAtMs();
    long updatedAtMs();
}

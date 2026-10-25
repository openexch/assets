// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** Wire {@code OrderFaultKind} (money-schema v6, template 113): an order-level anomaly that is settled, never silent. */
public enum OrderFaultKind {
    /** The ME filled more than the order's remaining quantity (budget-only fills, K11 #2); the excess is reported. */
    OVERFILL(0),
    /** A fill or terminal that the order accounting could not reconcile with the money settlement. */
    SETTLE(1),
    /** A fill would wrap the user's net position; the value was clamped. */
    POSITION_OVERFLOW(2);

    private final int code;

    OrderFaultKind(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

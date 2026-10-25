// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** Public order state vocabulary (money-schema v6 OrderStatus; critic C1: the only wire vocabulary). Codes equal the wire values. */
public enum OrderStatus {
    NEW(0),
    PARTIALLY_FILLED(1),
    FILLED(2),
    CANCELLED(3),
    REJECTED(4),
    EXPIRED(5),
    PENDING_NEW(6),
    PENDING_TRIGGER(7),
    PENDING_CANCEL(8),
    PENDING_REPLACE(9);

    private final int code;

    OrderStatus(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static OrderStatus fromCode(final int code) {
        for (final OrderStatus v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        return null;
    }
}

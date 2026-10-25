// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** Why an order-entry gate is in its state (money-schema v6 GateReason). Codes equal the wire values, so the publisher maps by {@link #code()}. */
public enum GateReason {
    ADMIN(0),
    RETENTION_BUDGET(1),
    BRIDGE_HALT(2),
    RECOVERY(3),
    ME_CAPACITY(4),
    GENESIS(5),
    APPLY_FAULT(6);

    private final int code;

    GateReason(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static GateReason fromCode(final int code) {
        for (final GateReason v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        return null;
    }
}

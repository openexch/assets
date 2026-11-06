// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Why an order-entry gate is in its state (money-schema GateReason). Codes equal the wire values, so the publisher maps by
 * {@link #code()}. v7 (RC2 S1) adds ME_LEG_LIVE 7 and ME_BEHAVIOR 8: set by RC2 S8, bitmask semantics land in S7; nothing in
 * S1 sets them.
 */
public enum GateReason {
    ADMIN(0),
    RETENTION_BUDGET(1),
    BRIDGE_HALT(2),
    RECOVERY(3),
    ME_CAPACITY(4),
    GENESIS(5),
    APPLY_FAULT(6),
    /** v7: set by RC2 S8. */
    ME_LEG_LIVE(7),
    /** v7: set by RC2 S8. */
    ME_BEHAVIOR(8);

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

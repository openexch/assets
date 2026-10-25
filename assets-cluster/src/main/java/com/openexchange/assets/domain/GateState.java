// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** Order-entry gate state (money-schema v6 GateState). Codes equal the wire values, so the publisher maps by {@link #code()}. */
public enum GateState {
    OPEN(0),
    SUSPENDED(1);

    private final int code;

    GateState(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static GateState fromCode(final int code) {
        for (final GateState v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        return null;
    }
}

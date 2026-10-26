// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * The ME EngineConfig state machine's decision as journalled (money-schema v7 ConfigDecision, RC2 S1 4.1), carried by
 * MeEngineConfig (68); served by RC2 S7.
 */
public enum ConfigDecision {
    FRESH(0),
    ADOPT(1),
    SWITCH(2),
    BUMP(3);

    private final int code;

    ConfigDecision(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** The value for a wire code, or null when the code is outside this build's range (decode poison). */
    public static ConfigDecision fromCode(final int code) {
        for (final ConfigDecision v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        return null;
    }
}

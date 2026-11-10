// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.domain;

/**
 * The ME EngineConfig state machine's decision as journalled (money-schema v7 ConfigDecision), carried by
 * MeEngineConfig (68); served under behaviour v3.
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

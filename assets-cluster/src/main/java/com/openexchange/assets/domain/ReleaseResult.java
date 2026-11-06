// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * The ME's result for one released command id range (money-schema v7 ReleaseResult, RC2 S1 4.1); served by RC2 S8.
 */
public enum ReleaseResult {
    APPLIED(0),
    NOOP(1),
    LEG_LIVE(2);

    private final int code;

    ReleaseResult(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** The value for a wire code, or null when the code is outside this build's range (decode poison). */
    public static ReleaseResult fromCode(final int code) {
        for (final ReleaseResult v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        return null;
    }
}

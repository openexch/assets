// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Scope of an ME command-id release (money-schema v7 ReleaseScope, RC2 S1 4.1), numerically equal to ME journal
 * schema 4. Carried by MeCommandRelease (67) entries and MeDispatch RELEASE_ID; served by RC2 S8.
 */
public enum ReleaseScope {
    UP_TO(0),
    ORDER(1);

    private final int code;

    ReleaseScope(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** The value for a wire code, or null when the code is outside this build's range (decode poison). */
    public static ReleaseScope fromCode(final int code) {
        for (final ReleaseScope v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        return null;
    }
}

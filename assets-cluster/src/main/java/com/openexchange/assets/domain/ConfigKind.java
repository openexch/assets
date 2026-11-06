// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** Which config record a ConfigApplied names. Codes equal the wire values. */
public enum ConfigKind {
    CORE(0),
    RISK(1),
    BREAKER(2),
    GATE(3);

    private final int code;

    ConfigKind(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

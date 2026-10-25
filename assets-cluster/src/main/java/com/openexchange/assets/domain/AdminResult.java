// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** Outcome of an admin command or a RegisterSession (money-schema v6 AdminResult). Codes equal the wire values, so the publisher maps by {@link #code()}. */
public enum AdminResult {
    ACCEPTED(0),
    DUPLICATE(1),
    REJECTED(2),
    ADOPTED(3),
    STALE(4);

    private final int code;

    AdminResult(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

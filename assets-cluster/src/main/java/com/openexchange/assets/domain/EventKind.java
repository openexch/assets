// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** What an OrderEvent reports (money-schema v6 EventKind). Codes equal the wire values. */
public enum EventKind {
    ACCEPTED(0),
    DISPATCHED(1),
    ACK_NEW(2),
    FILL(3),
    CANCELLED(4),
    EXPIRED(5),
    REJECTED(6),
    REPLACED(7),
    TRIGGERED(8),
    HOLD_CHANGED(9),
    SLICE_SUBMITTED(10),
    QUARANTINED(11);

    private final int code;

    EventKind(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** The one reject vocabulary for every ORIGIN-replyable ingress command (money-schema v6, critic C3). Codes equal the wire values, so the publisher maps by {@link #code()}. */
public enum CoreRejectReason {
    NONE(0),
    RATE_LIMIT(1),
    CIRCUIT_BREAKER(2),
    SIZE_TOO_SMALL(3),
    SIZE_TOO_LARGE(4),
    NOTIONAL_TOO_SMALL(5),
    NOTIONAL_TOO_LARGE(6),
    PRICE_COLLAR(7),
    OPEN_ORDER_LIMIT(8),
    INSUFFICIENT_FUNDS(9),
    POSITION_LIMIT(10),
    DUPLICATE_CLIENT_ORDER_ID(11),
    REQUEST_ID_PAYLOAD_MISMATCH(12),
    UNKNOWN_ORDER(13),
    NOT_OWNER(14),
    INVALID(15),
    EXPIRED(16),
    NOT_OPEN(17),
    GATE_CLOSED(18),
    UNKNOWN_MARKET(19),
    UNAUTHORIZED_CLASS(20),
    MISSING_REQUEST_ID(21),
    ME_REJECTED(23),
    ME_UNKNOWN_LEG(24),
    ME_CAPACITY(25),
    ME_CONFLICT(26),
    HOLD_FAULT(27),
    UNSUPPORTED_FLAGS(29),
    NO_REFERENCE_PRICE(30),
    AMEND_PENDING(31),
    TOO_MANY_PENDING_REQUESTS(32),
    STOP_WOULD_TRIGGER_IMMEDIATELY(33),
    UNSUPPORTED_TIF(34),
    UNREGISTERED_SESSION(35),
    NOT_RESUMABLE(36);

    private final int code;

    CoreRejectReason(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

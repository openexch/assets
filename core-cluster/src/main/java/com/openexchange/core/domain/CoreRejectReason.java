// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.domain;

/** The one reject vocabulary for every ORIGIN-replyable ingress command (money-schema v6, v7 additions 40..42; critic C3). Codes equal the wire values, so the publisher maps by {@link #code()}. */
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
    NOT_RESUMABLE(36),
    /** Behaviour v2: price outside the recorded market band. */
    PRICE_OUT_OF_BAND(37),
    /** Behaviour v2: price not a multiple of the recorded tick size. */
    PRICE_OFF_TICK(38),
    /** Behaviour v2: quantity not a multiple of the recorded lot size. */
    QTY_OFF_LOT(39),
    /** v7: ADMIN OPEN refused while the feed is latched. */
    FEED_HALTED(40),
    /** v7: ADMIN OPEN refused while meBehaviorVersion &lt; 2 under core behaviour v3. */
    ME_LANE_INACTIVE(41),
    /** v7: terminal REJECTED after an INVALID 8 outcome on a pending command, or the REPLACED abort of an AMEND. */
    ME_INVALID(42),
    /** v7: terminal REJECTED or the REPLACED abort after RELEASED 7 on a pending command. */
    ME_RELEASED(43),
    /**
     * A BRIDGE RegisterSession whose identity a different registered, live session holds.
     * AdminAck only: the AdminAck reason is a {@code uint8} on the wire, so this code is not in the money-schema
     * {@code CoreRejectReason} enum (no schema change) and is never put on an OrderReject.
     */
    DUPLICATE_IDENTITY(44),
    /**
     * An OPEN of GLOBAL while a halt holder awaits the operator's acknowledgement
     * (HOLDER_NONGAP_AWAITING_OPEN) that does not name BRIDGE_HALT. AdminAck only, like {@link #DUPLICATE_IDENTITY}:
     * a uint8 on AdminAck.reason, not in the money-schema enum, no schema change.
     */
    HALT_HOLDER_PENDING(45);

    /** True for a code that only an AdminAck carries (not in the money-schema enum). */
    public boolean adminAckOnly() {
        return this == DUPLICATE_IDENTITY || this == HALT_HOLDER_PENDING;
    }

    private final int code;

    CoreRejectReason(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

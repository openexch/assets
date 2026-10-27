/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum CoreRejectReason
{
    NONE((short)0),

    RATE_LIMIT((short)1),

    CIRCUIT_BREAKER((short)2),

    SIZE_TOO_SMALL((short)3),

    SIZE_TOO_LARGE((short)4),

    NOTIONAL_TOO_SMALL((short)5),

    NOTIONAL_TOO_LARGE((short)6),

    PRICE_COLLAR((short)7),

    OPEN_ORDER_LIMIT((short)8),

    INSUFFICIENT_FUNDS((short)9),

    POSITION_LIMIT((short)10),

    DUPLICATE_CLIENT_ORDER_ID((short)11),

    REQUEST_ID_PAYLOAD_MISMATCH((short)12),

    UNKNOWN_ORDER((short)13),

    NOT_OWNER((short)14),

    INVALID((short)15),

    EXPIRED((short)16),

    NOT_OPEN((short)17),

    GATE_CLOSED((short)18),

    UNKNOWN_MARKET((short)19),

    UNAUTHORIZED_CLASS((short)20),

    MISSING_REQUEST_ID((short)21),

    ME_REJECTED((short)23),

    ME_UNKNOWN_LEG((short)24),

    ME_CAPACITY((short)25),

    ME_CONFLICT((short)26),

    HOLD_FAULT((short)27),

    UNSUPPORTED_FLAGS((short)29),

    NO_REFERENCE_PRICE((short)30),

    AMEND_PENDING((short)31),

    TOO_MANY_PENDING_REQUESTS((short)32),

    STOP_WOULD_TRIGGER_IMMEDIATELY((short)33),

    UNSUPPORTED_TIF((short)34),

    UNREGISTERED_SESSION((short)35),

    NOT_RESUMABLE((short)36),


    /**
     * behaviour v2: price outside the recorded market [minPrice, maxPrice] (RC1 S7)
     */
    PRICE_OUT_OF_BAND((short)37),


    /**
     * behaviour v2: price not a multiple of the recorded tickSize (RC1 S7)
     */
    PRICE_OFF_TICK((short)38),


    /**
     * behaviour v2: quantity not a multiple of the recorded lotSize (RC1 S7)
     */
    QTY_OFF_LOT((short)39),


    /**
     * v7: ADMIN OPEN refused while the feed is latched (RC2 S7)
     */
    FEED_HALTED((short)40),


    /**
     * v7: ADMIN OPEN refused while meBehaviorVersion &lt; 2 under core behaviour v3 (RC2 S7/S8)
     */
    ME_LANE_INACTIVE((short)41),


    /**
     * v7: terminal REJECTED after an INVALID 8 outcome on a pending command, or the REPLACED abort of an AMEND (RC2 S8)
     */
    ME_INVALID((short)42),


    /**
     * v7 (RC2 S8, appended D-S8-1): terminal REJECTED or the REPLACED abort after RELEASED 7 on a pending command
     */
    ME_RELEASED((short)43),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    CoreRejectReason(final short value)
    {
        this.value = value;
    }

    /**
     * The raw encoded value in the Java type representation.
     *
     * @return the raw value encoded.
     */
    public short value()
    {
        return value;
    }

    /**
     * Lookup the enum value representing the value.
     *
     * @param value encoded to be looked up.
     * @return the enum value representing the value.
     */
    public static CoreRejectReason get(final short value)
    {
        switch (value)
        {
            case 0: return NONE;
            case 1: return RATE_LIMIT;
            case 2: return CIRCUIT_BREAKER;
            case 3: return SIZE_TOO_SMALL;
            case 4: return SIZE_TOO_LARGE;
            case 5: return NOTIONAL_TOO_SMALL;
            case 6: return NOTIONAL_TOO_LARGE;
            case 7: return PRICE_COLLAR;
            case 8: return OPEN_ORDER_LIMIT;
            case 9: return INSUFFICIENT_FUNDS;
            case 10: return POSITION_LIMIT;
            case 11: return DUPLICATE_CLIENT_ORDER_ID;
            case 12: return REQUEST_ID_PAYLOAD_MISMATCH;
            case 13: return UNKNOWN_ORDER;
            case 14: return NOT_OWNER;
            case 15: return INVALID;
            case 16: return EXPIRED;
            case 17: return NOT_OPEN;
            case 18: return GATE_CLOSED;
            case 19: return UNKNOWN_MARKET;
            case 20: return UNAUTHORIZED_CLASS;
            case 21: return MISSING_REQUEST_ID;
            case 23: return ME_REJECTED;
            case 24: return ME_UNKNOWN_LEG;
            case 25: return ME_CAPACITY;
            case 26: return ME_CONFLICT;
            case 27: return HOLD_FAULT;
            case 29: return UNSUPPORTED_FLAGS;
            case 30: return NO_REFERENCE_PRICE;
            case 31: return AMEND_PENDING;
            case 32: return TOO_MANY_PENDING_REQUESTS;
            case 33: return STOP_WOULD_TRIGGER_IMMEDIATELY;
            case 34: return UNSUPPORTED_TIF;
            case 35: return UNREGISTERED_SESSION;
            case 36: return NOT_RESUMABLE;
            case 37: return PRICE_OUT_OF_BAND;
            case 38: return PRICE_OFF_TICK;
            case 39: return QTY_OFF_LOT;
            case 40: return FEED_HALTED;
            case 41: return ME_LANE_INACTIVE;
            case 42: return ME_INVALID;
            case 43: return ME_RELEASED;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

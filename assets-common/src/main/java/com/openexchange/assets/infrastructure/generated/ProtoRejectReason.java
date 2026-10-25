/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum ProtoRejectReason
{
    NONE((short)0),

    RATE_LIMIT((short)1),

    CIRCUIT_BREAKER((short)2),

    SIZE((short)3),

    NOTIONAL((short)4),

    PRICE_COLLAR((short)5),

    OPEN_ORDERS((short)6),

    INSUFFICIENT_FUNDS((short)7),

    POSITION((short)8),

    DUPLICATE_CLIENT_ORDER_ID((short)9),

    UNKNOWN_ORDER((short)10),

    NOT_OWNER((short)11),

    INVALID((short)12),

    EXPIRED((short)13),

    NOT_OPEN((short)14),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    ProtoRejectReason(final short value)
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
    public static ProtoRejectReason get(final short value)
    {
        switch (value)
        {
            case 0: return NONE;
            case 1: return RATE_LIMIT;
            case 2: return CIRCUIT_BREAKER;
            case 3: return SIZE;
            case 4: return NOTIONAL;
            case 5: return PRICE_COLLAR;
            case 6: return OPEN_ORDERS;
            case 7: return INSUFFICIENT_FUNDS;
            case 8: return POSITION;
            case 9: return DUPLICATE_CLIENT_ORDER_ID;
            case 10: return UNKNOWN_ORDER;
            case 11: return NOT_OWNER;
            case 12: return INVALID;
            case 13: return EXPIRED;
            case 14: return NOT_OPEN;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

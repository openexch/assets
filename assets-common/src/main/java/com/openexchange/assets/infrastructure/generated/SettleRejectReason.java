/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum SettleRejectReason
{
    UNKNOWN_MARKET((short)0),

    INVALID_PRICE((short)1),

    INVALID_QUANTITY((short)2),

    NOTIONAL_OVERFLOW((short)3),

    INVALID_PARTY((short)4),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    SettleRejectReason(final short value)
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
    public static SettleRejectReason get(final short value)
    {
        switch (value)
        {
            case 0: return UNKNOWN_MARKET;
            case 1: return INVALID_PRICE;
            case 2: return INVALID_QUANTITY;
            case 3: return NOTIONAL_OVERFLOW;
            case 4: return INVALID_PARTY;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum RejectReason
{
    NONE((short)0),

    INSUFFICIENT_FUNDS((short)1),

    INVALID_AMOUNT((short)2),

    UNKNOWN_HOLD((short)3),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    RejectReason(final short value)
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
    public static RejectReason get(final short value)
    {
        switch (value)
        {
            case 0: return NONE;
            case 1: return INSUFFICIENT_FUNDS;
            case 2: return INVALID_AMOUNT;
            case 3: return UNKNOWN_HOLD;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum FeedHaltReason
{
    NONE((short)0),

    GAP((short)1),

    EPOCH_MISMATCH((short)2),

    SOURCE_SCHEMA((short)3),

    SOURCE_UNBOUND((short)4),

    SETTLE_REFUSED((short)5),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    FeedHaltReason(final short value)
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
    public static FeedHaltReason get(final short value)
    {
        switch (value)
        {
            case 0: return NONE;
            case 1: return GAP;
            case 2: return EPOCH_MISMATCH;
            case 3: return SOURCE_SCHEMA;
            case 4: return SOURCE_UNBOUND;
            case 5: return SETTLE_REFUSED;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

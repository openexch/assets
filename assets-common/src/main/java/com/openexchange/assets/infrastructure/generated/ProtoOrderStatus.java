/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum ProtoOrderStatus
{
    NEW((short)0),

    PARTIALLY_FILLED((short)1),

    FILLED((short)2),

    CANCELLED((short)3),

    REJECTED((short)4),

    EXPIRED((short)5),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    ProtoOrderStatus(final short value)
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
    public static ProtoOrderStatus get(final short value)
    {
        switch (value)
        {
            case 0: return NEW;
            case 1: return PARTIALLY_FILLED;
            case 2: return FILLED;
            case 3: return CANCELLED;
            case 4: return REJECTED;
            case 5: return EXPIRED;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

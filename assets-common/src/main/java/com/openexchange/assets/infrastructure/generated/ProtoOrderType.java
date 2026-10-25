/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum ProtoOrderType
{
    LIMIT((short)0),

    MARKET((short)1),

    STOP_LOSS((short)2),

    STOP_LIMIT((short)3),

    TRAILING_STOP((short)4),

    ICEBERG((short)5),

    LIMIT_MAKER((short)6),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    ProtoOrderType(final short value)
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
    public static ProtoOrderType get(final short value)
    {
        switch (value)
        {
            case 0: return LIMIT;
            case 1: return MARKET;
            case 2: return STOP_LOSS;
            case 3: return STOP_LIMIT;
            case 4: return TRAILING_STOP;
            case 5: return ICEBERG;
            case 6: return LIMIT_MAKER;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

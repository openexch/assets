/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum ProtoTimeInForce
{
    GTC((short)0),

    IOC((short)1),

    FOK((short)2),

    GTD((short)3),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    ProtoTimeInForce(final short value)
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
    public static ProtoTimeInForce get(final short value)
    {
        switch (value)
        {
            case 0: return GTC;
            case 1: return IOC;
            case 2: return FOK;
            case 3: return GTD;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

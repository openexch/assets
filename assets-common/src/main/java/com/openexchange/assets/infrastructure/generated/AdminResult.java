/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum AdminResult
{
    ACCEPTED((short)0),

    DUPLICATE((short)1),

    REJECTED((short)2),

    ADOPTED((short)3),

    STALE((short)4),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    AdminResult(final short value)
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
    public static AdminResult get(final short value)
    {
        switch (value)
        {
            case 0: return ACCEPTED;
            case 1: return DUPLICATE;
            case 2: return REJECTED;
            case 3: return ADOPTED;
            case 4: return STALE;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

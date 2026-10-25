/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum DispatchKind
{
    CREATE((short)0),

    CANCEL((short)1),

    AMEND((short)2),

    RELEASE_ID((short)3),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    DispatchKind(final short value)
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
    public static DispatchKind get(final short value)
    {
        switch (value)
        {
            case 0: return CREATE;
            case 1: return CANCEL;
            case 2: return AMEND;
            case 3: return RELEASE_ID;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

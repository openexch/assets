/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum GateReason
{
    ADMIN((short)0),

    RETENTION_BUDGET((short)1),

    BRIDGE_HALT((short)2),

    RECOVERY((short)3),

    ME_CAPACITY((short)4),

    GENESIS((short)5),

    APPLY_FAULT((short)6),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    GateReason(final short value)
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
    public static GateReason get(final short value)
    {
        switch (value)
        {
            case 0: return ADMIN;
            case 1: return RETENTION_BUDGET;
            case 2: return BRIDGE_HALT;
            case 3: return RECOVERY;
            case 4: return ME_CAPACITY;
            case 5: return GENESIS;
            case 6: return APPLY_FAULT;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

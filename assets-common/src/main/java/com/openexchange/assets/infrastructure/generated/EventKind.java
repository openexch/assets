/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum EventKind
{
    ACCEPTED((short)0),

    DISPATCHED((short)1),

    ACK_NEW((short)2),

    FILL((short)3),

    CANCELLED((short)4),

    EXPIRED((short)5),

    REJECTED((short)6),

    REPLACED((short)7),

    TRIGGERED((short)8),

    HOLD_CHANGED((short)9),

    SLICE_SUBMITTED((short)10),

    QUARANTINED((short)11),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    EventKind(final short value)
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
    public static EventKind get(final short value)
    {
        switch (value)
        {
            case 0: return ACCEPTED;
            case 1: return DISPATCHED;
            case 2: return ACK_NEW;
            case 3: return FILL;
            case 4: return CANCELLED;
            case 5: return EXPIRED;
            case 6: return REJECTED;
            case 7: return REPLACED;
            case 8: return TRIGGERED;
            case 9: return HOLD_CHANGED;
            case 10: return SLICE_SUBMITTED;
            case 11: return QUARANTINED;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum SessionClass
{
    UNSET((short)0),

    GATEWAY((short)1),

    FORWARDER((short)2),

    BRIDGE((short)3),

    ADMIN((short)4),

    TOOL((short)5),

    TREASURY((short)6),


    /**
     * v7: RCS replica process; no ingress template carries the REPLICA class bit except RegisterSession (key provisioning is RCS S5)
     */
    REPLICA((short)7),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    SessionClass(final short value)
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
    public static SessionClass get(final short value)
    {
        switch (value)
        {
            case 0: return UNSET;
            case 1: return GATEWAY;
            case 2: return FORWARDER;
            case 3: return BRIDGE;
            case 4: return ADMIN;
            case 5: return TOOL;
            case 6: return TREASURY;
            case 7: return REPLICA;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum SettleRejectReason
{
    UNKNOWN_MARKET((short)0),

    INVALID_PRICE((short)1),

    INVALID_QUANTITY((short)2),

    NOTIONAL_OVERFLOW((short)3),

    INVALID_PARTY((short)4),


    /**
     * tradeId jumped past lastApplied+1: trades were skipped upstream; refused, cursor held (RC1 S5)
     */
    TRADE_GAP((short)5),


    /**
     * v7
     */
    FEED_GAP((short)6),


    /**
     * v7
     */
    ME_EPOCH_MISMATCH((short)7),


    /**
     * v7
     */
    FEED_UNBOUND((short)8),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    SettleRejectReason(final short value)
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
    public static SettleRejectReason get(final short value)
    {
        switch (value)
        {
            case 0: return UNKNOWN_MARKET;
            case 1: return INVALID_PRICE;
            case 2: return INVALID_QUANTITY;
            case 3: return NOTIONAL_OVERFLOW;
            case 4: return INVALID_PARTY;
            case 5: return TRADE_GAP;
            case 6: return FEED_GAP;
            case 7: return ME_EPOCH_MISMATCH;
            case 8: return FEED_UNBOUND;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

@SuppressWarnings("all")
public enum OrderFaultKind
{
    OVERFILL((short)0),

    SETTLE((short)1),

    POSITION_OVERFLOW((short)2),


    /**
     * v7: RELEASED 7 on a still-pending command (RC2 W8)
     */
    ME_RELEASE_EARLY((short)3),


    /**
     * v7
     */
    ME_LEG_LIVE((short)4),


    /**
     * v7: RC2 L 3.7
     */
    ME_CAPACITY_WHILE_OPEN((short)5),


    /**
     * v7: the control event FeedHaltApplied: orderId 0, tradeId = seenJournalSeq, amount = expectedJournalSeq; also the audit event of FeedLatchClear (tradeId = journalSeq, amount 0)
     */
    FEED_HALT((short)6),

    /**
     * To be used to represent not present or null.
     */
    NULL_VAL((short)255);

    private final short value;

    OrderFaultKind(final short value)
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
    public static OrderFaultKind get(final short value)
    {
        switch (value)
        {
            case 0: return OVERFILL;
            case 1: return SETTLE;
            case 2: return POSITION_OVERFLOW;
            case 3: return ME_RELEASE_EARLY;
            case 4: return ME_LEG_LIVE;
            case 5: return ME_CAPACITY_WHILE_OPEN;
            case 6: return FEED_HALT;
            case 255: return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

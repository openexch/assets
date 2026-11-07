// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** Wire {@code OrderFaultKind} (money-schema v6, v7 additions 3..8; template 113): an order-level anomaly that is settled, never silent. */
public enum OrderFaultKind {
    /** The ME filled more than the order's remaining quantity (budget-only fills, K11 #2); the excess is reported. */
    OVERFILL(0),
    /** A fill or terminal that the order accounting could not reconcile with the money settlement. */
    SETTLE(1),
    /** A fill would wrap the user's net position; the value was clamped. */
    POSITION_OVERFLOW(2),
    /** v7 (RC2 S1, served by S8 W8): RELEASED 7 on a still-pending command. */
    ME_RELEASE_EARLY(3),
    /** v7 (RC2 S1, served by S8). */
    ME_LEG_LIVE(4),
    /** v7 (RC2 S1, served by S8, L 3.7). */
    ME_CAPACITY_WHILE_OPEN(5),
    /**
     * v7 (RC2 S1, served by S7): the control event FeedHaltApplied (orderId 0, tradeId = seenJournalSeq,
     * amount = expectedJournalSeq), also the audit event of FeedLatchClear 76 (tradeId = journalSeq, amount 0).
     */
    FEED_HALT(6),
    /** v7 (RC2 S8, appended D-S8-1): INVALID 8 on a pending command; {@code amount} = the command's idLow. */
    ME_INVALID(7),
    /** v7 (RC2 S8, appended D-S8-1): AMEND REJECTED reason 16; {@code amount} = the core's expectedOldRemaining at mint. */
    ME_AMEND_MISMATCH(8);

    private final int code;

    OrderFaultKind(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

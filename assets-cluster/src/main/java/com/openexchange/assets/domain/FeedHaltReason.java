// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Why the ME feed is latched (money-schema v7 FeedHaltReason, RC2 S1 4.1; B 2.5). Codes equal the wire values.
 * {@link #NONE} is legal only on FeedPositionReport.haltReason while feedHalted = FALSE; a FeedHalt (66) or a
 * FeedLatchClear (76) carrying 0 is a decode reject. Defined in S1, served by RC2 S7.
 */
public enum FeedHaltReason {
    /** Not halted: the idle value of FeedPositionReport.haltReason only. */
    NONE(0),
    /** Latch exit: clears automatically when the cursor passes haltSeenSeq - 1, or by FeedLatchClear 76. */
    GAP(1),
    /** Latch exit: clears only by a new genesis. */
    EPOCH_MISMATCH(2),
    /** Latch exit: clears only by FeedLatchClear 76. */
    SOURCE_SCHEMA(3),
    /** Latch exit: clears only by a new genesis. */
    SOURCE_UNBOUND(4),
    /** Latch exit: clears only by FeedLatchClear 76 naming the refused journalSeq. */
    SETTLE_REFUSED(5);

    private final int code;

    FeedHaltReason(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** The value for a wire code, or null when the code is outside this build's range (decode poison). */
    public static FeedHaltReason fromCode(final int code) {
        for (final FeedHaltReason v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        return null;
    }
}

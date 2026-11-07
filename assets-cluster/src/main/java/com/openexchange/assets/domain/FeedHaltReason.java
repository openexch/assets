// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Why the ME feed is latched (money-schema v7 FeedHaltReason, RC2 S1 4.1; B 2.5). Codes equal the wire values.
 * {@link #NONE} is legal only on FeedPositionReport.haltReason while feedHalted = FALSE; a FeedHalt (66) or a
 * FeedLatchClear (76) carrying 0 is a decode reject. Defined in S1, served by RC2 S7 (contract section 5) under behaviour v3.
 */
public enum FeedHaltReason {
    /** Not halted: the idle value of FeedPositionReport.haltReason only. */
    NONE(0),
    /** Exit class 1: clears when the cursor reaches seen - 1 (the missing range applied), or by 76 with journalSeq 0. */
    GAP(1),
    /** Exit class 3: a new genesis only. */
    EPOCH_MISMATCH(2),
    /** Exit class 2: the reach-clear (the refused entry, or the expected one, applies), or 76 per RC2 S7 D3b. */
    SOURCE_SCHEMA(3),
    /** Exit class 3: a new genesis only (a well-formed journalSeq 1 that is not a valid FRESH). */
    SOURCE_UNBOUND(4),
    /** Exit class 2: the reach-clear, or 76 naming the refused journalSeq (which consumes exactly one tradeId). */
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

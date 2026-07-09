// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Outcome of a money mutation. {@link #NONE} means <em>accepted</em> (no rejection); every other
 * value is a reason the command could not be applied. Modeled as an enum (singletons) so the hot
 * path returns an outcome with zero allocation.
 */
public enum RejectReason {
    /** Accepted — no rejection. */
    NONE,
    /** Not enough available balance to hold/withdraw (no overdraft). */
    INSUFFICIENT_FUNDS,
    /** Amount was zero or negative. */
    INVALID_AMOUNT,
    /** Release/settle referenced a hold that does not exist. */
    UNKNOWN_HOLD;

    public boolean accepted() {
        return this == NONE;
    }
}

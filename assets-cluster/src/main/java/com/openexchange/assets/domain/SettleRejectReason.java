// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Why a feed {@code Settle} was refused BEFORE any mutation (RC1 S1, K5 "validate then mutate"). Each
 * of these used to be a throw out of the apply path ({@code Market.fromId}, {@code FixedPoint.multiply})
 * that would have latched the service and, on replay, every replica. Now it is a loud deterministic
 * outcome: nothing mutates, the journal cursor does not advance, and the feed session is told.
 */
public enum SettleRejectReason {
    UNKNOWN_MARKET,
    INVALID_PRICE,
    INVALID_QUANTITY,
    NOTIONAL_OVERFLOW,
    INVALID_PARTY,
    /** RC1 S5: tradeId &gt; lastApplied + 1 on the MeJournalTrade path; trades were skipped upstream. */
    TRADE_GAP,
    /** v7 (RC2 S1, served by S7): a refusal of 61-64 under the journalSeq cursor (B 2.3): journalSeq gap. */
    FEED_GAP,
    /** v7 (RC2 S1, served by S7): the entry's meEpoch is not the bound epoch. */
    ME_EPOCH_MISMATCH,
    /** v7 (RC2 S1, served by S7): no epoch is bound yet. */
    FEED_UNBOUND
}

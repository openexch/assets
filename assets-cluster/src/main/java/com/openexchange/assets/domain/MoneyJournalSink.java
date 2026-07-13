// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Outbound port for the money journal: every APPLIED money movement (deposit, withdrawal, settle)
 * plus the opening-balance epoch, in money terms (primitives only, zero allocation). The production
 * adapter ({@code MoneyJournal}) encodes these to SBE and hands them to the journal writer; tests
 * capture them directly. Invoked synchronously on the single deterministic engine thread, so the
 * journal stream is totally ordered and identical on every replica.
 *
 * <p>{@code journalSeq} is assigned by the engine: dense, starts at 1, one per emitted record (epoch
 * rows included). {@code balanceAfter} values are the post-event (available + locked) TOTAL for the
 * (user, asset): the journal carries no hold/release events, and holds do not change totals, so
 * totals are the only self-consistent balance representation. {@code clusterTimeMs} is the cluster's
 * deterministic log timestamp, never wall clock.</p>
 */
public interface MoneyJournalSink {

    /** Dark default: journaling disabled costs a dead branch per apply, never a null check. */
    MoneyJournalSink NO_OP = new MoneyJournalSink() {
        @Override
        public void onOpeningBalance(long journalSeq, long userId, int assetId, long amount) {
        }

        @Override
        public void onDeposit(long journalSeq, long userId, int assetId, long amount,
                              long balanceAfter, long clusterTimeMs) {
        }

        @Override
        public void onWithdraw(long journalSeq, long userId, int assetId, long amount,
                               long balanceAfter, long clusterTimeMs) {
        }

        @Override
        public void onSettle(long journalSeq, long tradeId, int marketId, long price, long quantity,
                             long buyerUserId, long sellerUserId, boolean takerIsBuy,
                             long buyerBaseAfter, long buyerQuoteAfter,
                             long sellerBaseAfter, long sellerQuoteAfter, long clusterTimeMs) {
        }
    };

    /**
     * One opening-balance epoch row: the (available + locked) total for a (user, asset) at the
     * instant journaling first engaged on a non-empty ledger. Emitted in ascending (userId, assetId)
     * order before the first journaled movement.
     */
    void onOpeningBalance(long journalSeq, long userId, int assetId, long amount);

    /** An applied deposit. */
    void onDeposit(long journalSeq, long userId, int assetId, long amount,
                   long balanceAfter, long clusterTimeMs);

    /** An applied withdrawal. */
    void onWithdraw(long journalSeq, long userId, int assetId, long amount,
                    long balanceAfter, long clusterTimeMs);

    /**
     * An applied settle, one record per trade (the projector explodes it to per-leg rows). The four
     * {@code *After} totals are authoritative: on a faulted leg the moved amount can be less than
     * price * quantity, and the totals reflect what actually moved.
     */
    void onSettle(long journalSeq, long tradeId, int marketId, long price, long quantity,
                  long buyerUserId, long sellerUserId, boolean takerIsBuy,
                  long buyerBaseAfter, long buyerQuoteAfter,
                  long sellerBaseAfter, long sellerQuoteAfter, long clusterTimeMs);
}

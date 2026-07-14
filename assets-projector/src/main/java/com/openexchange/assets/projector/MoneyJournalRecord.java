// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import java.util.List;

/**
 * A decoded, Postgres-ready money-journal record: the {@code money_journal} header row plus the
 * exploded {@code money_journal_leg} balance-after facts. Built by pure factories (no Aeron), so
 * the header mapping and the settle four-leg explosion are unit-testable without a driver.
 *
 * Nullable columns are boxed. A movement stamps only the columns its type carries; the rest stay
 * null (opening has no clusterTimeMs; settle has no single user/asset/amount, only per-leg totals).
 *
 * Leg explosion:
 *  - opening/deposit/withdraw -> ONE leg with a real assetId and a null assetRole.
 *  - settle                   -> FOUR legs, each with a null assetId and a set assetRole
 *    (0=BASE, 1=QUOTE); the base/quote assetIds are not on the settle record (it carries marketId
 *    only), so asset resolution is a downstream join on money_journal.market_id.
 */
public final class MoneyJournalRecord {

    /** movement_type values == journal message templateId. */
    public static final short TYPE_OPENING = 1;
    public static final short TYPE_DEPOSIT = 2;
    public static final short TYPE_WITHDRAW = 3;
    public static final short TYPE_SETTLE = 4;

    /** asset_role values for settle legs (resolved to a real asset via market_id downstream). */
    public static final short ROLE_BASE = 0;
    public static final short ROLE_QUOTE = 1;

    /**
     * One exploded balance-after fact. {@code assetId} is non-null for deposit/withdraw/opening and
     * null for settle legs (which set {@code assetRole} instead). {@code delta} is the signed
     * movement where known (deposit +amount, withdraw -amount) and null where it cannot be derived
     * from the record alone (opening epoch; settle, whose per-leg prior balance is unknown and whose
     * moved amount can be less than price*quantity on a faulted leg).
     */
    public record Leg(short legNo, long userId, Integer assetId, Short assetRole,
                      long balanceAfter, Long delta) {
    }

    private final long journalSeq;
    private final short movementType;
    private final Long clusterTimeMs;
    private final Long userId;
    private final Integer assetId;
    private final Long amount;
    private final Long balanceAfter;
    private final Long tradeId;
    private final Integer marketId;
    private final Long price;
    private final Long quantity;
    private final Long buyerUserId;
    private final Long sellerUserId;
    private final Boolean takerIsBuy;
    private final List<Leg> legs;

    private MoneyJournalRecord(final long journalSeq, final short movementType, final Long clusterTimeMs,
                              final Long userId, final Integer assetId, final Long amount,
                              final Long balanceAfter, final Long tradeId, final Integer marketId,
                              final Long price, final Long quantity, final Long buyerUserId,
                              final Long sellerUserId, final Boolean takerIsBuy, final List<Leg> legs) {
        this.journalSeq = journalSeq;
        this.movementType = movementType;
        this.clusterTimeMs = clusterTimeMs;
        this.userId = userId;
        this.assetId = assetId;
        this.amount = amount;
        this.balanceAfter = balanceAfter;
        this.tradeId = tradeId;
        this.marketId = marketId;
        this.price = price;
        this.quantity = quantity;
        this.buyerUserId = buyerUserId;
        this.sellerUserId = sellerUserId;
        this.takerIsBuy = takerIsBuy;
        this.legs = legs;
    }

    /** Opening-balance epoch row: amount = the (available + locked) total at journaling engage. */
    public static MoneyJournalRecord opening(final long journalSeq, final long userId,
                                             final int assetId, final long amount) {
        final Leg leg = new Leg((short) 0, userId, assetId, null, amount, null);
        return new MoneyJournalRecord(journalSeq, TYPE_OPENING, null, userId, assetId, amount, amount,
                null, null, null, null, null, null, null, List.of(leg));
    }

    public static MoneyJournalRecord deposit(final long journalSeq, final long userId, final int assetId,
                                             final long amount, final long balanceAfter,
                                             final long clusterTimeMs) {
        final Leg leg = new Leg((short) 0, userId, assetId, null, balanceAfter, amount);
        return new MoneyJournalRecord(journalSeq, TYPE_DEPOSIT, clusterTimeMs, userId, assetId, amount,
                balanceAfter, null, null, null, null, null, null, null, List.of(leg));
    }

    public static MoneyJournalRecord withdraw(final long journalSeq, final long userId, final int assetId,
                                              final long amount, final long balanceAfter,
                                              final long clusterTimeMs) {
        final Leg leg = new Leg((short) 0, userId, assetId, null, balanceAfter, -amount);
        return new MoneyJournalRecord(journalSeq, TYPE_WITHDRAW, clusterTimeMs, userId, assetId, amount,
                balanceAfter, null, null, null, null, null, null, null, List.of(leg));
    }

    /**
     * Settle: one header row (marketId, price, quantity, both users, taker side) exploded into four
     * balance-after legs. The *After totals are authoritative (they reflect what actually moved).
     */
    public static MoneyJournalRecord settle(final long journalSeq, final long tradeId, final int marketId,
                                            final long price, final long quantity, final long buyerUserId,
                                            final long sellerUserId, final boolean takerIsBuy,
                                            final long buyerBaseAfter, final long buyerQuoteAfter,
                                            final long sellerBaseAfter, final long sellerQuoteAfter,
                                            final long clusterTimeMs) {
        final List<Leg> legs = List.of(
                new Leg((short) 0, buyerUserId, null, ROLE_BASE, buyerBaseAfter, null),
                new Leg((short) 1, buyerUserId, null, ROLE_QUOTE, buyerQuoteAfter, null),
                new Leg((short) 2, sellerUserId, null, ROLE_BASE, sellerBaseAfter, null),
                new Leg((short) 3, sellerUserId, null, ROLE_QUOTE, sellerQuoteAfter, null));
        return new MoneyJournalRecord(journalSeq, TYPE_SETTLE, clusterTimeMs, null, null, null, null,
                tradeId, marketId, price, quantity, buyerUserId, sellerUserId, takerIsBuy, legs);
    }

    public long journalSeq() {
        return journalSeq;
    }

    public short movementType() {
        return movementType;
    }

    public Long clusterTimeMs() {
        return clusterTimeMs;
    }

    public Long userId() {
        return userId;
    }

    public Integer assetId() {
        return assetId;
    }

    public Long amount() {
        return amount;
    }

    public Long balanceAfter() {
        return balanceAfter;
    }

    public Long tradeId() {
        return tradeId;
    }

    public Integer marketId() {
        return marketId;
    }

    public Long price() {
        return price;
    }

    public Long quantity() {
        return quantity;
    }

    public Long buyerUserId() {
        return buyerUserId;
    }

    public Long sellerUserId() {
        return sellerUserId;
    }

    public Boolean takerIsBuy() {
        return takerIsBuy;
    }

    public List<Leg> legs() {
        return legs;
    }
}

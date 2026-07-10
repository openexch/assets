// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Domain service for the one money operation that spans <em>two</em> aggregates: settling a trade
 * between a buyer and a seller. Each account still owns its own side of the mutation (and its own
 * invariants); this service coordinates the pair and guarantees cross-account conservation by
 * construction: <b>each leg credits the counterparty exactly what the payer actually paid</b>.
 *
 * <ul>
 *   <li><b>quote leg:</b> buyer pays quoteAmt (hold first, available if the hold is short),
 *       seller receives the paid amount ⇒ Σ(available+locked) for the quote asset is unchanged.</li>
 *   <li><b>base leg:</b> seller delivers baseAmt the same way, buyer receives the delivered
 *       amount ⇒ Σ(available+locked) for the base asset is unchanged.</li>
 * </ul>
 *
 * <p>The NORMAL path draws each side from the specific order's hold (sized ≥ the execution by the
 * hold-gate), so no floor-guard is needed and a buy that fills better than its hold price leaves a
 * residual released on the order's terminal. The EXCEPTIONAL path (hold short/missing — e.g. an
 * early orphan-release raced an in-flight fill) NEVER throws: throwing would poison the replicated
 * log. It draws the shortfall from available, clamps the credit to what was actually debited, and
 * exposes the discrepancy via {@link #buyerLeg()}/{@link #sellerLeg()} for the engine to emit as a
 * loud {@code SettleFault}. Stateless apart from the two reusable result holders (single engine
 * thread).</p>
 */
public final class SettlementService {

    private final Account.SettleDebitResult buyerLeg = new Account.SettleDebitResult();
    private final Account.SettleDebitResult sellerLeg = new Account.SettleDebitResult();

    /**
     * Settle one fill between {@code buyer} and {@code seller}.
     *
     * @param baseAmt  base-asset quantity that changes hands (seller -> buyer)
     * @param quoteAmt quote-asset amount that changes hands (buyer -> seller) = price × quantity
     */
    public void settle(Account buyer, Account seller,
                       long buyerOrderId, long sellerOrderId,
                       int baseAsset, int quoteAsset,
                       long baseAmt, long quoteAmt) {
        final long paidQuote = buyer.settleDebit(buyerOrderId, quoteAsset, quoteAmt, buyerLeg);
        seller.settleCredit(quoteAsset, paidQuote);

        final long deliveredBase = seller.settleDebit(sellerOrderId, baseAsset, baseAmt, sellerLeg);
        buyer.settleCredit(baseAsset, deliveredBase);
    }

    /** The buyer-side (quote) leg result of the LAST settle — valid until the next call. */
    public Account.SettleDebitResult buyerLeg() {
        return buyerLeg;
    }

    /** The seller-side (base) leg result of the LAST settle — valid until the next call. */
    public Account.SettleDebitResult sellerLeg() {
        return sellerLeg;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Domain service for the one money operation that spans <em>two</em> aggregates: settling a trade
 * between a buyer and a seller. Each account still owns its own side of the mutation (and its own
 * invariants); this service only coordinates the pair and guarantees the cross-account conservation
 * invariant holds by construction:
 *
 * <ul>
 *   <li><b>quote leg:</b> buyer.locked[quote] -= quoteAmt, seller.available[quote] += quoteAmt
 *       ⇒ Σ(available+locked) for the quote asset is unchanged.</li>
 *   <li><b>base leg:</b> seller.locked[base] -= baseAmt, buyer.available[base] += baseAmt
 *       ⇒ Σ(available+locked) for the base asset is unchanged.</li>
 * </ul>
 *
 * <p>Because each side is drawn from the <em>specific order's hold</em> (sized ≥ the execution), no
 * floor-guard / oversettle clamp is needed: money is moved exactly, never created or destroyed. A buy
 * that fills better than its hold price simply leaves a residual, released on the order's terminal
 * status. Stateless and reusable.</p>
 */
public final class SettlementService {

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
        buyer.applyBuyFill(buyerOrderId, quoteAsset, quoteAmt, baseAsset, baseAmt);
        seller.applySellFill(sellerOrderId, baseAsset, baseAmt, quoteAsset, quoteAmt);
    }
}

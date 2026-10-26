// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.domain.FixedPoint;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * A deterministic price-time matching engine for the traffic generator: per market one bid and one ask book of
 * {@link LifecycleModelOrder} legs. It matches a leg the moment it is submitted (the generator acks every
 * dispatch at once), at the resting order's price. A MARKET BUY walks the book only as far as its budget pays
 * for (exact {@link FixedPoint} cost, quantity floored to the lot), so a budget-driven buy never spends more than
 * the hold the core locked for it. No self-trade prevention, no fees.
 */
final class LifecycleFakeMe {

    /** One execution: the incoming leg is the taker, the resting leg the maker, at the maker's price. */
    record Fill(LifecycleModelOrder taker, LifecycleModelOrder maker, long price, long quantity) { }

    private static final Comparator<LifecycleModelOrder> BIDS = (a, b) -> a.legPrice != b.legPrice
            ? Long.compare(b.legPrice, a.legPrice) : Long.compare(a.arrival, b.arrival);
    private static final Comparator<LifecycleModelOrder> ASKS = (a, b) -> a.legPrice != b.legPrice
            ? Long.compare(a.legPrice, b.legPrice) : Long.compare(a.arrival, b.arrival);

    private final List<TreeSet<LifecycleModelOrder>> bids = new ArrayList<>();
    private final List<TreeSet<LifecycleModelOrder>> asks = new ArrayList<>();
    private final long lot;
    private long arrivals;

    LifecycleFakeMe(final int markets, final long lot) {
        this.lot = lot;
        for (int m = 0; m <= markets; m++) {
            bids.add(new TreeSet<>(BIDS));
            asks.add(new TreeSet<>(ASKS));
        }
    }

    /** Would a post-only leg take liquidity? (the ME refuses it then) */
    boolean wouldCross(final LifecycleModelOrder o) {
        final TreeSet<LifecycleModelOrder> opp = o.buy() ? asks.get(o.market) : bids.get(o.market);
        if (opp.isEmpty()) {
            return false;
        }
        final long best = opp.first().legPrice;
        return o.buy() ? best <= o.legPrice : best >= o.legPrice;
    }

    /**
     * Match an incoming leg against the opposite book and rest what is left of a priced leg. A MARKET leg never
     * rests: whatever {@code legRemaining} is left after the call is for the caller to cancel.
     */
    List<Fill> submit(final LifecycleModelOrder o) {
        o.arrival = ++arrivals;
        final TreeSet<LifecycleModelOrder> opp = o.buy() ? asks.get(o.market) : bids.get(o.market);
        final List<Fill> fills = new ArrayList<>();
        while (o.legRemaining > 0 && !opp.isEmpty()) {
            final LifecycleModelOrder best = opp.first();
            if (o.meType != LifecycleModelOrder.MARKET) {
                final boolean crosses = o.buy() ? best.legPrice <= o.legPrice : best.legPrice >= o.legPrice;
                if (!crosses) {
                    break;
                }
            }
            long q = Math.min(o.legRemaining, best.legRemaining);
            if (o.meType == LifecycleModelOrder.MARKET && o.buy()) {
                long cost = FixedPoint.multiply(best.legPrice, q);
                if (cost > o.legBudget) {
                    q = affordable(o.legBudget, best.legPrice);
                    if (q <= 0) {
                        break;
                    }
                    cost = FixedPoint.multiply(best.legPrice, q);
                }
                o.legBudget -= cost;
            }
            fills.add(new Fill(o, best, best.legPrice, q));
            o.legRemaining -= q;
            best.legRemaining -= q;
            if (best.legRemaining == 0) {
                opp.pollFirst();
                best.resting = false;
            }
        }
        if (o.legRemaining > 0 && o.meType != LifecycleModelOrder.MARKET) {
            (o.buy() ? bids : asks).get(o.market).add(o);
            o.resting = true;
        }
        return fills;
    }

    /** Take a resting leg off the book (cancel, expiry, the old leg of an amend, a terminal). */
    void remove(final LifecycleModelOrder o) {
        if (o.resting) {
            (o.buy() ? bids : asks).get(o.market).remove(o);
            o.resting = false;
        }
    }

    long bestBid(final int market) {
        return bids.get(market).isEmpty() ? 0L : bids.get(market).first().legPrice;
    }

    long bestAsk(final int market) {
        return asks.get(market).isEmpty() ? 0L : asks.get(market).first().legPrice;
    }

    /** The largest lot multiple whose exact cost at {@code price} fits {@code budget}. */
    private long affordable(final long budget, final long price) {
        if (budget <= 0) {
            return 0L;
        }
        long q = FixedPoint.divide(budget, price);
        q -= q % lot;
        while (q > 0 && FixedPoint.multiply(price, q) > budget) {
            q -= lot;
        }
        return q;
    }
}

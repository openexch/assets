// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import org.agrona.collections.LongArrayList;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The armed synthetic orders per market (RC1 S6 contract section 2). Derived state: an order is here iff its record is
 * PENDING_TRIGGER and not quarantined, the book is rebuilt from the records at restore and never serialized. Stops are
 * keyed by stop price (orders at one price kept ascending by id), trailing orders are an ascending id list. The firing
 * order is decided by the caller (ascending order id across sides and types), so the internal order only has to be a
 * function of the records.
 */
final class TriggerBook {

    private static final int MARKETS = UserRisk.MARKET_SLOTS;

    private final TreeMap<Long, LongArrayList>[] sellStops;
    private final TreeMap<Long, LongArrayList>[] buyStops;
    private final LongArrayList[] trailing;
    private final int[] stopCount = new int[MARKETS];

    @SuppressWarnings("unchecked")
    TriggerBook() {
        sellStops = new TreeMap[MARKETS];
        buyStops = new TreeMap[MARKETS];
        trailing = new LongArrayList[MARKETS];
        for (int m = 0; m < MARKETS; m++) {
            sellStops[m] = new TreeMap<>();
            buyStops[m] = new TreeMap<>();
            trailing[m] = new LongArrayList();
        }
    }

    void add(final CoreOrder o) {
        if (o.orderType == OrderEngine.TYPE_TRAILING_STOP) {
            insertSorted(trailing[o.marketId], o.orderId);
            return;
        }
        final TreeMap<Long, LongArrayList> book = o.side == OrderEngine.SIDE_SELL ? sellStops[o.marketId] : buyStops[o.marketId];
        insertSorted(book.computeIfAbsent(o.stopPrice, k -> new LongArrayList()), o.orderId);
        stopCount[o.marketId]++;
    }

    void remove(final CoreOrder o) {
        if (o.orderType == OrderEngine.TYPE_TRAILING_STOP) {
            removeValue(trailing[o.marketId], o.orderId);
            return;
        }
        final TreeMap<Long, LongArrayList> book = o.side == OrderEngine.SIDE_SELL ? sellStops[o.marketId] : buyStops[o.marketId];
        final LongArrayList ids = book.get(o.stopPrice);
        if (ids != null && removeValue(ids, o.orderId)) {
            stopCount[o.marketId]--;
            if (ids.isEmpty()) {
                book.remove(o.stopPrice);
            }
        }
    }

    /**
     * Collect the stop orders this quote fires into {@code out} (unsorted): SELL stops with {@code stopPrice >= bestBid}
     * (bid at or below the stop), BUY stops with {@code stopPrice <= bestAsk}. A side at 0 has no quote and fires nothing.
     */
    void collectFiredStops(final int marketId, final long bestBid, final long bestAsk, final LongArrayList out) {
        if (bestBid > 0) {
            for (final Map.Entry<Long, LongArrayList> e : sellStops[marketId].tailMap(bestBid, true).entrySet()) {
                appendAll(e.getValue(), out);
            }
        }
        if (bestAsk > 0) {
            final NavigableMap<Long, LongArrayList> fired = buyStops[marketId].headMap(bestAsk, true);
            for (final Map.Entry<Long, LongArrayList> e : fired.entrySet()) {
                appendAll(e.getValue(), out);
            }
        }
    }

    /** The trailing orders of a market, ascending by id (the caller mutates arms in this order). */
    LongArrayList trailing(final int marketId) {
        return trailing[marketId];
    }

    int stopCount(final int marketId) {
        return stopCount[marketId];
    }

    int trailingCount(final int marketId) {
        return trailing[marketId].size();
    }

    void clear() {
        for (int m = 0; m < MARKETS; m++) {
            sellStops[m].clear();
            buyStops[m].clear();
            trailing[m].clear();
            stopCount[m] = 0;
        }
    }

    private static void appendAll(final LongArrayList from, final LongArrayList out) {
        for (int i = 0; i < from.size(); i++) {
            out.addLong(from.getLong(i));
        }
    }

    private static void insertSorted(final LongArrayList list, final long value) {
        int lo = 0;
        int hi = list.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (list.getLong(mid) < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < list.size() && list.getLong(lo) == value) {
            return;
        }
        list.addLong(lo, value);
    }

    private static boolean removeValue(final LongArrayList list, final long value) {
        for (int i = 0; i < list.size(); i++) {
            if (list.getLong(i) == value) {
                list.removeAt(i);
                return true;
            }
        }
        return false;
    }
}

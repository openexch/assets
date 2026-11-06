// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.Market;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * RC1 S5 (harness spec, invariants table I1-I13): the lifecycle invariants the current API can express, read
 * straight from a rig's engine state. {@link #check(OrderRig, Expectations)} throws an {@link AssertionError}
 * that names the invariant and the offending account, order or index entry. The model side of the table
 * (external totals, the exact hold residual after amends, per-user positions) is what the engine cannot know
 * about itself and is passed in through {@link Expectations}; every field of it is optional.
 *
 * <p>Record-derived I4 residual (used when no model residual is given): BUY {@code holdAmount - cumQuoteQty},
 * SELL {@code remainingQty}, plus the open window's top-up ({@code pendingHoldDelta > 0} while REPLACE_PENDING,
 * contract 3.4: "holdRemaining shows it now"). This is the harness table's formula; it is exact unless a
 * price-changing amend resolved AFTER fills, because contract 3.4 sizes the delta on the remaining leg only
 * (the price-improvement surplus of earlier fills stays locked until terminal). Callers that amend after fills
 * pass the bookkept residual through {@link Expectations#residualByOrder}.</p>
 *
 * <p>Not expressible on this API and left to the other groups: I9/I10 (no openNotional / per-market counters),
 * I12 and I14 (snapshot bytes), I15 beyond the light form the fuzz applies to its own events.</p>
 */
final class LifecycleInvariants {

    /** What the engine cannot know about itself; every field optional. */
    static final class Expectations {
        /** I1: per asset (index = assetId), deposits minus withdrawals; null skips I1. */
        long[] external;
        /** I4: expected hold residual per live order; an order missing here uses the record formula. */
        Map<Long, Long> residualByOrder;
        /** I7: per user, net position per market (index = marketId); null checks only that markets net to zero. */
        Map<Long, long[]> positionByUser;
        /** I7: once an OVERFILL clamped one side (contract 3.1.3) a market no longer nets to zero by design. */
        boolean positionsMayNotNet;
        /** I13 strong form: a sweep ran at the engine's current log time. */
        boolean justSwept;
        /** Prefix for every message (seed, op index, op text). */
        String context = "";
    }

    private LifecycleInvariants() {
    }

    static void check(final OrderRig r) {
        check(r, new Expectations());
    }

    static void check(final OrderRig r, final long[] external) {
        final Expectations x = new Expectations();
        x.external = external;
        check(r, x);
    }

    static void check(final OrderRig r, final Expectations x) {
        final List<CoreOrder> live = new ArrayList<>();
        r.orders.orders().forEachByOrderId(live::add);
        final List<Account> accounts = new ArrayList<>();
        r.engine.forEachAccountByUserId(accounts::add);
        checkLedger(r, x, accounts);   // I1 I2 I3 I5
        checkOrders(r, x, live);       // I4 I6 I8
        checkPositions(r, x);          // I7
        checkDeadlines(r, x, live);    // I11 I13
    }

    // ---------------------------------------------------------------- I1 I2 I3 I5

    private static void checkLedger(final OrderRig r, final Expectations x, final List<Account> accounts) {
        final int assets = Asset.count();
        final long[] total = new long[assets];
        for (final Account a : accounts) {
            final long[] sumHold = new long[assets];
            a.forEachHold((orderId, assetId, remaining, omsManagedRelease) -> {
                if (remaining < 0) {
                    fail(x, "I3", "hold " + orderId + " of user " + a.userId() + " has negative remaining " + remaining);
                }
                sumHold[assetId] += remaining;
                final CoreOrder o = r.orders.orders().get(orderId);
                if (o == null) {
                    fail(x, "I5", "user " + a.userId() + " still holds " + remaining + " of asset " + assetId + " for order "
                            + orderId + ", which is no longer in the store (hold must be released at terminal)");
                }
                if (o.userId != a.userId()) {
                    fail(x, "I5", "hold for order " + orderId + " sits on user " + a.userId() + " but the order belongs to " + o.userId);
                }
                if (o.holdAssetId != assetId) {
                    fail(x, "I4", "order " + orderId + " holds asset " + assetId + " but the record says holdAssetId " + o.holdAssetId);
                }
            });
            for (int asset = 0; asset < assets; asset++) {
                if (a.available(asset) < 0) {
                    fail(x, "I3", "user " + a.userId() + " asset " + asset + " available " + a.available(asset));
                }
                if (a.locked(asset) < 0) {
                    fail(x, "I3", "user " + a.userId() + " asset " + asset + " locked " + a.locked(asset));
                }
                if (a.locked(asset) != sumHold[asset]) {
                    fail(x, "I2", "user " + a.userId() + " asset " + asset + " locked " + a.locked(asset)
                            + " but the open holds sum to " + sumHold[asset]);
                }
                total[asset] += a.available(asset) + a.locked(asset);
            }
        }
        if (x.external != null) {
            for (int asset = 0; asset < assets; asset++) {
                final long expected = asset < x.external.length ? x.external[asset] : 0L;
                if (total[asset] != expected) {
                    fail(x, "I1", "asset " + asset + ": sum(available + locked) over accounts is " + total[asset]
                            + " but deposits - withdrawals is " + expected + " (money was created or destroyed)");
                }
            }
        }
    }

    // ---------------------------------------------------------------- I4 I6 I8

    private static void checkOrders(final OrderRig r, final Expectations x, final List<CoreOrder> live) {
        final Map<Long, Integer> openByUser = new TreeMap<>();
        for (final CoreOrder o : live) {
            openByUser.merge(o.userId, 1, Integer::sum);
            final String tag = "order " + o.orderId + " (" + (o.side == OrderEngine.SIDE_BUY ? "BUY" : "SELL") + " u=" + o.userId + ")";
            if (o.isTerminal()) {
                fail(x, "I5", tag + " is terminal (status " + o.status + ") but still in the store");
            }
            if (o.filledQty < 0 || o.filledQty > o.quantity) {
                fail(x, "I6", tag + " filled " + o.filledQty + " outside 0.." + o.quantity);
            }
            if (o.remainingQty != o.quantity - o.filledQty) {
                fail(x, "I6", tag + " remaining " + o.remainingQty + " != quantity " + o.quantity + " - filled " + o.filledQty);
            }
            if (o.cumQuoteQty < 0) {
                fail(x, "I6", tag + " cumQuoteQty " + o.cumQuoteQty + " is negative");
            }
            if (o.marketId < 1 || o.marketId > Market.ALL.length) {
                fail(x, "I4", tag + " has marketId " + o.marketId);
            }
            final Market m = Market.ALL[o.marketId - 1];
            final int expectedAsset = o.side == OrderEngine.SIDE_BUY ? m.quoteAsset().id() : m.baseAsset().id();
            if (o.holdAssetId != expectedAsset) {
                fail(x, "I4", tag + " holdAssetId " + o.holdAssetId + " but a " + (o.side == OrderEngine.SIDE_BUY ? "BUY locks the quote " : "SELL locks the base ")
                        + expectedAsset);
            }
            final Account a = r.engine.account(o.userId);
            if (a == null) {
                fail(x, "I4", tag + " is live but its user has no account");
            }
            if (!a.hasHold(o.orderId)) {
                fail(x, "I4", tag + " is live but has no hold keyed by its orderId");
            }
            if (a.holdAssetId(o.orderId) != o.holdAssetId) {
                fail(x, "I4", tag + " hold is in asset " + a.holdAssetId(o.orderId) + " not holdAssetId " + o.holdAssetId);
            }
            final long remaining = a.holdRemaining(o.orderId);
            if (remaining <= 0) {
                fail(x, "I4", tag + " is live but its hold remaining is " + remaining);
            }
            final long expected = expectedResidual(x, o);
            if (remaining != expected) {
                fail(x, "I4", tag + " hold remaining " + remaining + " but the expected residual is " + expected
                        + " (holdAmount " + o.holdAmount + ", cumQuoteQty " + o.cumQuoteQty + ", remainingQty " + o.remainingQty
                        + ", pendingHoldDelta " + o.pendingHoldDelta + ", flags " + o.stateFlags + ")");
            }
        }
        for (final Map.Entry<Long, Integer> e : openByUser.entrySet()) {
            final int slots = r.orders.orders().openCount(e.getKey());
            if (slots != e.getValue()) {
                fail(x, "I8", "user " + e.getKey() + " openCount " + slots + " but " + e.getValue() + " live orders");
            }
        }
        r.orders.forEachUserByUserId(u -> {
            if (!openByUser.containsKey(u.userId) && r.orders.orders().openCount(u.userId) != 0) {
                fail(x, "I8", "user " + u.userId + " openCount " + r.orders.orders().openCount(u.userId) + " with no live orders");
            }
        });
        if (r.orders.orders().size() != live.size()) {
            fail(x, "I8", "store size " + r.orders.orders().size() + " but " + live.size() + " live records visited");
        }
    }

    private static long expectedResidual(final Expectations x, final CoreOrder o) {
        if (x.residualByOrder != null) {
            final Long v = x.residualByOrder.get(o.orderId);
            if (v != null) {
                return v;
            }
        }
        final long topUp = o.has(CoreOrder.F_REPLACE_PENDING) && o.pendingHoldDelta > 0 ? o.pendingHoldDelta : 0L;
        return (o.side == OrderEngine.SIDE_BUY ? o.holdAmount - o.cumQuoteQty : o.remainingQty) + topUp;
    }

    // ---------------------------------------------------------------- I7

    private static void checkPositions(final OrderRig r, final Expectations x) {
        final long[] net = new long[UserRisk.MARKET_SLOTS];
        final List<Long> seen = new ArrayList<>();
        r.orders.forEachUserByUserId(u -> {
            seen.add(u.userId);
            final long[] expected = x.positionByUser == null ? null : x.positionByUser.get(u.userId);
            for (int m = 1; m < UserRisk.MARKET_SLOTS; m++) {
                net[m] += u.position[m];
                if (x.positionByUser != null) {
                    final long want = expected == null ? 0L : expected[m];
                    if (u.position[m] != want) {
                        fail(x, "I7", "user " + u.userId + " market " + m + " position " + u.position[m]
                                + " but buys - sells of its fills is " + want);
                    }
                }
            }
        });
        if (x.positionByUser != null) {
            for (final Map.Entry<Long, long[]> e : x.positionByUser.entrySet()) {
                if (seen.contains(e.getKey())) {
                    continue;
                }
                for (int m = 1; m < UserRisk.MARKET_SLOTS; m++) {
                    if (e.getValue()[m] != 0) {
                        fail(x, "I7", "user " + e.getKey() + " has no risk record but a filled position " + e.getValue()[m] + " on market " + m);
                    }
                }
            }
        }
        if (!x.positionsMayNotNet) {
            for (int m = 1; m < UserRisk.MARKET_SLOTS; m++) {
                if (net[m] != 0) {
                    fail(x, "I7", "market " + m + " positions net to " + net[m] + " across users, not 0");
                }
            }
        }
    }

    // ---------------------------------------------------------------- I11 I13

    private static void checkDeadlines(final OrderRig r, final Expectations x, final List<CoreOrder> live) {
        final long now = r.engine.getLastLogTime();
        final List<String> gtd = new ArrayList<>();
        final List<Long> replace = new ArrayList<>();
        final List<String> ttl = new ArrayList<>();
        final List<String> due = new ArrayList<>();
        r.engine.deadlines().forEach((deadlineMs, id, kind) -> {
            switch (kind) {
                case DeadlineIndex.KIND_GTD:
                    gtd.add(deadlineMs + ":" + id);
                    break;
                case DeadlineIndex.KIND_REPLACE:
                    replace.add(id);
                    break;
                case DeadlineIndex.KIND_REQUEST_TTL:
                    ttl.add(deadlineMs + ":" + id);
                    break;
                default:
                    fail(x, "I11", "deadline index holds an entry of unknown kind " + kind + " (" + deadlineMs + ", " + id + ")");
            }
            if (deadlineMs <= now) {
                due.add(deadlineMs + ":" + id + ":" + kind);
            }
        });
        final List<String> expectedGtd = new ArrayList<>();
        final List<Long> expectedReplace = new ArrayList<>();
        for (final CoreOrder o : live) {
            if (o.expiresAtMs > 0 && !o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
                expectedGtd.add(o.expiresAtMs + ":" + o.orderId);
            }
            if (o.has(CoreOrder.F_REPLACE_PENDING)) {
                expectedReplace.add(o.orderId);
            }
        }
        final List<String> expectedTtl = new ArrayList<>();
        for (final RequestTable.Entry e : r.orders.requests().all()) {
            expectedTtl.add(e.expiresAtMs + ":" + e.requestSeq);
        }
        Collections.sort(gtd);
        Collections.sort(expectedGtd);
        Collections.sort(replace);
        Collections.sort(expectedReplace);
        Collections.sort(ttl);
        Collections.sort(expectedTtl);
        if (!gtd.equals(expectedGtd)) {
            fail(x, "I11", "GTD entries (deadline:orderId) in the index " + gtd + " but the live GTD orders without EXPIRY_REQUESTED are " + expectedGtd);
        }
        if (!replace.equals(expectedReplace)) {
            fail(x, "I11", "REPLACE entries (orderId) in the index " + replace + " but the open replace windows are " + expectedReplace);
        }
        if (!ttl.equals(expectedTtl)) {
            fail(x, "I11", "REQUEST_TTL entries (deadline:requestSeq) in the index " + ttl + " but the request table holds " + expectedTtl);
        }
        for (final CoreOrder o : live) {
            if (x.justSwept && o.expiresAtMs > 0 && o.expiresAtMs <= now && !o.has(CoreOrder.F_EXPIRY_REQUESTED)) {
                fail(x, "I13", "after the sweep at " + now + " GTD order " + o.orderId + " expiring at " + o.expiresAtMs
                        + " is live without EXPIRY_REQUESTED");
            }
        }
        if (x.justSwept && !due.isEmpty()) {
            fail(x, "I13", "after the sweep at " + now + " entries due at or before it remain in the index: " + due);
        }
    }

    private static void fail(final Expectations x, final String invariant, final String detail) {
        throw new AssertionError((x.context == null || x.context.isEmpty() ? "" : x.context) + invariant + " violated: " + detail);
    }
}

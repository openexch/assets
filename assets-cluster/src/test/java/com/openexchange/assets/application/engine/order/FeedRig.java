// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;

/**
 * RC1 S5 feed driver for the order-engine tests: BRIDGE-class events (create outcome, trade, terminal) over an
 * {@link OrderRig}, with the standard two-party fixture (user 1 BUY on orderId 1, user 2 SELL on orderId 2,
 * market 1) and the reads the fill/terminal tests share. The egressSeq is carried, not checked (contract 3.1
 * step 4), so a per-rig counter is enough.
 */
final class FeedRig {

    static final int MKT = 1;
    static final long LEG1 = 101L;
    static final long LEG2 = 202L;
    static final long LEG3 = 303L;
    static final long LEG4 = 404L;
    static final int ME_FILLED = 2;
    static final int ME_CANCELLED = 3;
    static final int ME_REJECTED = 4;
    private static final int KIND_CREATE = 0;
    private static final int RESULT_APPLIED = 0;

    final OrderRig r;
    private long egress;

    FeedRig(final OrderRig r) {
        this.r = r;
    }

    static FeedRig open() {
        return new FeedRig(OrderRig.open());
    }

    /** user 1 BUY LIMIT (orderId 1) and user 2 SELL LIMIT (orderId 2) on market 1, both legs learned by CREATE APPLIED. */
    static FeedRig pair(final long usd, final long btc, final long buyPx, final long buyQty, final long sellPx, final long sellQty) {
        final FeedRig f = open();
        f.r.deposit(1, USD, usd);
        f.r.deposit(2, BTC, btc);
        f.r.create(f.r.limit(1, MKT, 0, buyPx, buyQty));
        f.r.create(f.r.limit(2, MKT, 1, sellPx, sellQty));
        f.ack(1, 1, LEG1);
        f.ack(2, 2, LEG2);
        return f;
    }

    // ---------------------------------------------------------------- feed events

    /** CREATE APPLIED outcome for attempt 0 of {@code orderId}: the core learns {@code legId} (contract 3.3). */
    void ack(final long orderId, final long userId, final long legId) {
        r.orders.applyOutcome(++egress, orderId, 0L, userId, orderId, 0L, 0L, 0L, 0L, MKT, KIND_CREATE, RESULT_APPLIED,
                legId, 0, r.now, 0L, r.now);
    }

    void trade(final long tradeId, final long takerOrder, final long takerLeg, final long takerUser, final long makerOrder,
               final long makerLeg, final long makerUser, final long px, final long qty, final boolean takerIsBuy) {
        r.orders.applyTrade(++egress, tradeId, MKT, takerLeg, takerUser, makerLeg, makerUser, px, qty, takerIsBuy, r.now,
                takerOrder, makerOrder, 0L, r.now);
    }

    /** Order 2 (user 2, SELL, LEG2) takes against resting order 1 (user 1, BUY, LEG1). */
    void sellHits(final long tradeId, final long px, final long qty) {
        trade(tradeId, 2, LEG2, 2, 1, LEG1, 1, px, qty, false);
    }

    /** Order 1 (user 1, BUY, LEG1) takes against resting order 2 (user 2, SELL, LEG2). */
    void buyHits(final long tradeId, final long px, final long qty) {
        trade(tradeId, 1, LEG1, 1, 2, LEG2, 2, px, qty, true);
    }

    void terminal(final long orderId, final long userId, final long legId, final int status) {
        r.orders.applyTerminal(++egress, legId, userId, MKT, status, r.now, orderId, 0L, r.now);
    }

    // ---------------------------------------------------------------- reads

    List<AssetsEngineEvent.OrderEvent> orderEvents(final long orderId) {
        final List<AssetsEngineEvent.OrderEvent> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.OrderEvent oe && oe.orderId() == orderId) {
                out.add(oe);
            }
        }
        return out;
    }

    List<String> renders() {
        final List<String> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            out.add(e.render());
        }
        return out;
    }

    /** Deadline-index entries of {@code kind} for {@code id} ({@code id < 0} = any id). */
    int deadlines(final int kind, final long id) {
        final int[] n = new int[1];
        r.engine.deadlines().forEach((d, i, k) -> {
            if (k == kind && (id < 0 || i == id)) {
                n[0]++;
            }
        });
        return n[0];
    }

    /** The v4 snapshot image: the byte-level definition of "no state change". */
    byte[] image() {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(r.engine, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    /** I: for every asset, {@code locked == sum of holdRemaining} over the user's LIVE orders, and no hold without a live order. */
    void assertLockedEqualsHolds(final long user) {
        final int[] live = new int[1];
        for (final int asset : new int[] {USD, BTC}) {
            final long[] sum = new long[1];
            r.orders.orders().forEachByOrderId(o -> {
                if (o.userId == user && o.holdAssetId == asset) {
                    sum[0] += r.engine.account(user).holdRemaining(o.orderId);
                    live[0]++;
                }
            });
            assertEquals("locked == sum of holdRemaining over live orders (u=" + user + " asset=" + asset + ")",
                    sum[0], r.engine.account(user).locked(asset));
        }
        assertEquals("every hold of u=" + user + " belongs to a live order", live[0], r.engine.account(user).holdCount());
    }
}

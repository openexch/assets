// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.proto;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.Arrays;

import static com.openexchange.assets.application.proto.ProtoHarness.BTC;
import static com.openexchange.assets.application.proto.ProtoHarness.USD;
import static com.openexchange.assets.application.proto.ProtoHarness.fp;
import static com.openexchange.assets.application.proto.ProtoOrderEngine.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** The prototype order lifecycle on the ledger: admission, holds, fills, closes, snapshot. */
public class ProtoOrderEngineTest {

    private static final long T0 = 1_700_000_000_000L;

    @Test
    public void createHoldsCancelReleasesAndConservationHolds() {
        final ProtoHarness h = new ProtoHarness();
        h.deposit(1, USD, 1_000_000);
        final long usdBefore = h.total(USD);

        final long[] ev = h.limit(11, 1, SIDE_BUY, 50_000, 0.1, T0);
        assertEquals(ST_NEW, ev[4]);
        final long orderId = ev[1];
        assertEquals(1L, orderId);
        assertEquals(fp(5_000), ev[10]);                       // hold = price * qty
        assertEquals(fp(5_000), h.locked(1, USD));
        assertEquals(fp(1_000_000) - fp(5_000), h.available(1, USD));
        final ProtoOrder o = h.engine.proto().order(orderId);
        assertNotNull(o);
        assertEquals(fp(0.1), o.remainingQty);
        assertEquals(1, h.engine.proto().user(1).openOrderCount);
        assertEquals(1, h.engine.proto().market(1).openOrders);

        final long[] cx = h.cancel(12, 1, orderId, T0 + 5);
        assertEquals(ST_CANCELLED, cx[4]);
        assertEquals(0L, h.locked(1, USD));
        assertEquals(fp(1_000_000), h.available(1, USD));
        assertNull(h.engine.proto().order(orderId));
        assertEquals(0, h.engine.proto().user(1).openOrderCount);
        assertEquals(usdBefore, h.total(USD));
        assertTrue(h.lockedMatchesHolds());

        // Cancel again: unknown order, no state change.
        final long[] again = h.cancel(13, 1, orderId, T0 + 6);
        assertEquals(ST_REJECTED, again[4]);
        assertEquals(RR_UNKNOWN_ORDER, again[5]);
        // Someone else's order: not owner.
        final long other = h.limit(14, 1, SIDE_BUY, 50_000, 0.1, T0 + 7)[1];
        assertEquals(RR_NOT_OWNER, h.cancel(15, 2, other, T0 + 8)[5]);
    }

    @Test
    public void aFillSettlesBothLegsAndReleasesTheBuyersResidualAtFilled() {
        final ProtoHarness h = new ProtoHarness();
        h.deposit(1, USD, 1_000_000);
        h.deposit(2, BTC, 10);
        final long usd = h.total(USD), btc = h.total(BTC);

        final long buy = h.limit(1, 1, SIDE_BUY, 50_000, 0.1, T0)[1];
        final long sell = h.limit(2, 2, SIDE_SELL, 49_000, 0.1, T0)[1];
        assertEquals(fp(0.1), h.locked(2, BTC));

        final int evBefore = h.events.size();
        h.fill(1001, buy, sell, 49_000, 0.1, T0 + 10);
        assertEquals(1, h.settlements);
        assertEquals(0, h.settleFaults);
        // Buyer paid 4,900 of the 5,000 held: 100 released back at FILLED (the OMS leaked this).
        assertEquals(fp(1_000_000) - fp(4_900), h.available(1, USD));
        assertEquals(0L, h.locked(1, USD));
        assertEquals(fp(0.1), h.available(1, BTC));
        assertEquals(fp(9.9), h.available(2, BTC));
        assertEquals(0L, h.locked(2, BTC));
        assertEquals(fp(4_900), h.available(2, USD));
        assertEquals(usd, h.total(USD));
        assertEquals(btc, h.total(BTC));
        assertTrue(h.lockedMatchesHolds());
        assertNull(h.engine.proto().order(buy));
        assertNull(h.engine.proto().order(sell));
        final long[] buyEv = h.events.get(evBefore), sellEv = h.events.get(evBefore + 1);
        assertEquals(ST_FILLED, buyEv[4]);
        assertEquals(fp(0.1), buyEv[6]);
        assertEquals(0L, buyEv[7]);
        assertEquals(fp(49_000), buyEv[8]);
        assertEquals(ST_FILLED, sellEv[4]);
        assertEquals(0L, buyEv[0]); // fill-driven events are uncorrelated (broadcast)
        assertEquals(fp(0.1), h.engine.proto().user(1).position[1]);
        assertEquals(-fp(0.1), h.engine.proto().user(2).position[1]);
        assertEquals(fp(49_000), h.engine.proto().market(1).lastTradePrice);

        // Same tradeId again: idempotent no-op.
        h.fill(1001, buy, sell, 49_000, 0.1, T0 + 11);
        assertEquals(1, h.settlements);
        assertEquals(1, h.engine.proto().fillCount());

        // Partial fill path.
        final long buy2 = h.limit(3, 1, SIDE_BUY, 50_000, 1.0, T0 + 20)[1];
        final long sell2 = h.limit(4, 2, SIDE_SELL, 50_000, 0.4, T0 + 20)[1];
        h.fill(1002, buy2, sell2, 50_000, 0.4, T0 + 21);
        final ProtoOrder b2 = h.engine.proto().order(buy2);
        assertNotNull(b2);
        assertEquals(ST_PARTIALLY_FILLED, b2.status);
        assertEquals(fp(0.6), b2.remainingQty);
        assertEquals(fp(50_000) * 6 / 10, h.locked(1, USD)); // 30,000 still held for the open 0.6
        assertNull(h.engine.proto().order(sell2));
        assertTrue(h.lockedMatchesHolds());
        assertEquals(usd, h.total(USD));
        assertEquals(btc, h.total(BTC));
    }

    @Test
    public void admissionChecksRejectInTheOmsOrderAndRejectsConsumeRateBudget() {
        final ProtoHarness h = new ProtoHarness();
        h.deposit(1, USD, 10_000_000);
        h.deposit(2, BTC, 100);

        // Insufficient funds: the hold IS the balance check.
        assertEquals(RR_INSUFFICIENT_FUNDS, h.limit(1, 3, SIDE_BUY, 50_000, 0.1, T0)[5]);
        // Invalid market / qty / side.
        assertEquals(RR_INVALID, h.create(2, 1, 7, 50_000, 0.1, 0, 0, 0, T0)[5]);
        // Price-less buy needs a budget price.
        assertEquals(RR_INVALID, h.limit(3, 1, SIDE_BUY, 0, 0.1, T0)[5]);
        // Duplicate clientOrderId.
        assertEquals(ST_NEW, h.create(4, 1, SIDE_BUY, 50_000, 0.1, 777L, 0, 0, T0)[4]);
        assertEquals(RR_DUPLICATE_CLIENT_ORDER_ID, h.create(5, 1, SIDE_BUY, 50_000, 0.1, 777L, 0, 0, T0)[5]);
        // Collar: once there is a last trade, 20% band.
        final long b = h.limit(6, 1, SIDE_BUY, 50_000, 0.1, T0)[1];
        final long s = h.limit(7, 2, SIDE_SELL, 50_000, 0.1, T0)[1];
        h.fill(1, b, s, 50_000, 0.1, T0 + 1);
        assertEquals(RR_PRICE_COLLAR, h.limit(8, 1, SIDE_BUY, 70_000, 0.1, T0 + 2)[5]);
        assertEquals(ST_NEW, h.limit(9, 1, SIDE_BUY, 59_000, 0.1, T0 + 2)[4]);
        // Rate limit: 100/s default; the creates above in this second count (incl. rejects).
        int rateRejects = 0;
        for (int i = 0; i < 100; i++) {
            if (h.limit(100 + i, 1, SIDE_BUY, 50_000, 0.01, T0 + 3)[5] == RR_RATE_LIMIT) {
                rateRejects++;
            }
        }
        assertTrue("rate limit must engage within the second (" + rateRejects + ")", rateRejects > 0);
        assertEquals(RR_RATE_LIMIT, h.limit(300, 1, SIDE_BUY, 50_000, 0.01, T0 + 999)[5]);
        assertEquals(ST_NEW, h.limit(301, 1, SIDE_BUY, 50_000, 0.01, T0 + 1_000)[4]); // next bucket
        // Open-order slot: default 500.
        final ProtoHarness g = new ProtoHarness();
        g.deposit(5, BTC, 1_000_000);
        for (int i = 0; i < 500; i++) {
            assertEquals(ST_NEW, g.limit(i, 5, SIDE_SELL, 50_000, 0.001, T0 + i * 20L)[4]);
        }
        assertEquals(RR_OPEN_ORDERS, g.limit(600, 5, SIDE_SELL, 50_000, 0.001, T0 + 20_000)[5]);
        assertTrue(h.lockedMatchesHolds());
        assertTrue(g.lockedMatchesHolds());
    }

    @Test
    public void gtdArmsATimerAndExpiryClosesTheOrderOnce() {
        final ProtoHarness h = new ProtoHarness();
        h.deposit(1, USD, 100_000);
        final long[] ev = h.create(1, 1, SIDE_BUY, 50_000, 0.1, 0, TIF_GTD, T0 + 60_000, T0);
        assertEquals(ST_NEW, ev[4]);
        assertEquals(1, h.timers.size());
        assertEquals(ev[1], h.timers.get(0)[0]);
        assertEquals(T0 + 60_000, h.timers.get(0)[1]);
        h.expire(ev[1], T0 + 60_000);
        final long[] last = h.events.get(h.events.size() - 1);
        assertEquals(ST_EXPIRED, last[4]);
        assertEquals(RR_EXPIRED, last[5]);
        assertEquals(0L, h.locked(1, USD));
        final int n = h.events.size();
        h.expire(ev[1], T0 + 60_001); // late duplicate: nothing
        assertEquals(n, h.events.size());
        assertEquals(1, h.engine.proto().expiredCount());
    }

    @Test
    public void snapshotV4RoundTripsByteIdenticalAndRestoresEveryIndex() {
        final ProtoHarness h = new ProtoHarness();
        for (long u = 1; u <= 5; u++) {
            h.deposit(u, USD, 1_000_000);
            h.deposit(u, BTC, 10);
        }
        long corr = 1;
        for (int i = 0; i < 60; i++) {
            final long user = 1 + (i % 5);
            h.create(corr++, user, i % 2, 50_000 + (i % 7) * 10, 0.01 + (i % 3) * 0.01, 1_000 + i, (i % 4 == 0) ? TIF_GTD : 0,
                    (i % 4 == 0) ? T0 + 100_000 : 0, T0 + i * 15L);
        }
        h.fill(1, 1, 2, 50_000, 0.01, T0 + 2_000);
        h.fill(2, 3, 4, 50_010, 0.02, T0 + 2_001);
        h.cancel(corr++, 1, 6, T0 + 2_002);
        h.cancel(corr++, 2, 7, T0 + 2_003);
        final int open = h.engine.proto().openOrderCount();
        assertTrue(open > 50);

        final ExpandableArrayBuffer a = new ExpandableArrayBuffer();
        final int lenA = BalanceSnapshotCodec.serialize(h.engine, a);

        final ProtoHarness r = new ProtoHarness();
        final BalanceSnapshotCodec.Decoded decoded = BalanceSnapshotCodec.deserialize(a, 0, lenA, r.engine);
        assertEquals(lenA, decoded.bytesConsumed);
        assertEquals(open, decoded.orderCount);
        assertEquals(open, r.engine.proto().openOrderCount());
        assertEquals(h.engine.proto().nextOrderId(), r.engine.proto().nextOrderId());
        assertEquals(h.engine.proto().eventSeq(), r.engine.proto().eventSeq());
        assertEquals(h.engine.proto().lastFillTradeId(), r.engine.proto().lastFillTradeId());
        for (long u = 1; u <= 5; u++) {
            assertEquals(h.engine.proto().user(u).openOrderCount, r.engine.proto().user(u).openOrderCount);
            assertEquals(h.engine.proto().user(u).openOrders.size(), r.engine.proto().user(u).openOrders.size());
            assertEquals(h.engine.proto().user(u).clientOrderIds.size(), r.engine.proto().user(u).clientOrderIds.size());
            assertEquals(h.engine.proto().user(u).openNotional, r.engine.proto().user(u).openNotional);
            assertEquals(h.engine.proto().user(u).position[1], r.engine.proto().user(u).position[1]);
        }

        final ExpandableArrayBuffer b = new ExpandableArrayBuffer();
        final int lenB = BalanceSnapshotCodec.serialize(r.engine, b);
        assertEquals(lenA, lenB);
        assertArrayEquals(Arrays.copyOf(a.byteArray(), lenA), Arrays.copyOf(b.byteArray(), lenB));

        // The restored engine behaves identically: same cancel, same event, same money.
        final long[] e1 = h.cancel(9_000, 3, 8, T0 + 3_000);
        final long[] e2 = r.cancel(9_000, 3, 8, T0 + 3_000);
        assertArrayEquals(e1, e2);
        assertEquals(h.locked(3, USD) + h.locked(3, BTC), r.locked(3, USD) + r.locked(3, BTC));
        // A duplicate clientOrderId is still detected after the restore (index rebuilt).
        assertEquals(RR_DUPLICATE_CLIENT_ORDER_ID, r.create(9_001, 1, SIDE_BUY, 50_000, 0.01, 1_010, 0, 0, T0 + 4_000)[5]);
    }

    @Test
    public void snapshotBytesMatchTheDeclaredRecordSizes() {
        final ProtoHarness h = new ProtoHarness();
        h.deposit(1, USD, 1_000_000);
        for (int i = 0; i < 10; i++) {
            h.limit(i, 1, SIDE_BUY, 50_000, 0.01, T0);
        }
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int total = BalanceSnapshotCodec.serialize(h.engine, buf);
        final int ledgerBytes = 37 + 112 + 10 * 21; // v3 header + one account (6 assets) + 10 holds
        assertEquals(ledgerBytes + h.engine.proto().snapshotBytes(), total);
        assertEquals(3 * 8 + 4 + 6 * ProtoMarketState.SNAPSHOT_BYTES + 4 + ProtoUserState.SNAPSHOT_BYTES + 4 + 10 * ProtoOrder.SNAPSHOT_BYTES,
                h.engine.proto().snapshotBytes());
        assertEquals(194, ProtoOrder.SNAPSHOT_BYTES);
    }
}

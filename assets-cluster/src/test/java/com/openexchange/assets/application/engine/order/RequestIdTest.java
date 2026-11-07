// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import org.junit.Test;

import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S4 (lifecycle B6/R-C1b/R-C1c/R-dep, critic D5): the replicated requestId table. A replay with the
 * same payload returns the stored decision (no second mutation); a different payload is a mismatch; the
 * table is bounded per user and swept by TTL on the single deadline index.
 */
public class RequestIdTest {

    @Test
    public void redC1b_sameRequestSamePayloadReplaysTheDecision() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        c.clientOrderIdLow = 1;
        r.markNow();
        r.create(c);
        final AssetsEngineEvent.OrderAck first = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals(4, r.events().size()); // balance (the hold), dispatch, ack, event
        r.markNow();
        r.create(c); // byte-equal retry (same requestId, same payload)
        final AssetsEngineEvent.OrderAck second = r.only(AssetsEngineEvent.OrderAck.class);
        assertTrue(second.duplicate());
        assertEquals(first.orderId(), second.orderId());
        assertEquals("one record, one hold, one dispatch", 1, r.orders.orders().size());
        assertEquals(100 * ONE, r.engine.account(1).locked(USD));
        assertEquals(1, r.engine.getDispatchSeq());
        assertEquals("a replay consumes no rate budget", 1, r.orders.user(1).secCount);
    }

    @Test
    public void redC1c_sameRequestDifferentPayloadIsAMismatch() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        r.create(c);
        c.quantity = 2 * ONE; // same requestId, different payload
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH, r.reject().reason());
        assertEquals(1, r.orders.orders().size());
        assertEquals(100 * ONE, r.engine.account(1).locked(USD));
    }

    @Test
    public void aRejectedDecisionIsReplayedToo() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 50 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, 0, 100 * ONE, ONE);
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.INSUFFICIENT_FUNDS, r.reject().reason());
        r.deposit(1, USD, 1_000 * ONE); // now it would pass...
        r.markNow();
        r.create(c);
        assertEquals("...but the stored decision stands for this requestId", CoreRejectReason.INSUFFICIENT_FUNDS, r.reject().reason());
        assertEquals(0, r.orders.orders().size());
    }

    @Test
    public void perUserCapAndTtlSweep() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000_000 * ONE);
        for (int i = 0; i < RequestTable.PER_USER_CAP; i++) {
            final CreateOrderCommand c = r.limit(1, 1, 0, ONE, ONE);
            c.requestIdLow = 10_000 + i;
            c.clientOrderIdLow = 10_000 + i;
            r.create(c);
            r.advance(1_000); // stay under the rate limit
        }
        assertEquals(RequestTable.PER_USER_CAP, r.orders.requests().countFor(1));
        r.markNow();
        final CreateOrderCommand over = r.limit(1, 1, 0, ONE, ONE);
        over.requestIdLow = 99_999;
        r.create(over);
        assertEquals(CoreRejectReason.TOO_MANY_PENDING_REQUESTS, r.reject().reason());
        assertEquals(RequestTable.PER_USER_CAP, r.orders.requests().countFor(1));
        // every entry sits on the deadline index; the sweep at TTL drops them in order
        assertTrue(r.engine.deadlines().size() >= RequestTable.PER_USER_CAP);
        r.advance(RequestTable.TTL_MS);
        r.sweep();
        assertEquals(0, r.orders.requests().countFor(1));
        assertNull(r.orders.requests().get(1, 0xABCDL, 10_000));
        r.markNow();
        r.create(over);
        assertEquals("after the TTL the request is new again", 1, r.count(AssetsEngineEvent.OrderAck.class));
    }

    @Test
    public void redDep_depositWithARequestIdIsCreditedOnce() {
        final OrderRig r = OrderRig.open();
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setCorrelationId(1);
        d.setUserId(3);
        d.setAssetId(USD);
        d.setAmount(10 * ONE);
        d.setRequestIdHigh(1);
        d.setRequestIdLow(1);
        r.markNow();
        r.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, r.now);
        assertFalse(r.first(AssetsEngineEvent.DepositAck.class).duplicate());
        r.markNow();
        r.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, r.now);
        assertTrue("replayed ack", r.only(AssetsEngineEvent.DepositAck.class).duplicate());
        assertEquals(10 * ONE, r.engine.account(3).available(USD));
        d.setAmount(20 * ONE);
        r.markNow();
        r.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, r.now);
        assertEquals(RejectReason.REQUEST_ID_MISMATCH, r.only(AssetsEngineEvent.DepositReject.class).reason());
        assertEquals(10 * ONE, r.engine.account(3).available(USD));
    }

    @Test
    public void aRejectedDepositEmitsATypedReject() {
        final OrderRig r = OrderRig.open();
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setCorrelationId(2);
        d.setUserId(4);
        d.setAssetId(USD);
        d.setAmount(0);
        r.markNow();
        r.engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, r.now);
        assertEquals(RejectReason.INVALID_AMOUNT, r.only(AssetsEngineEvent.DepositReject.class).reason());
        assertNull("still no account on a reject", r.engine.account(4));
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.AmendOrderCommand;

import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 G3 fixture around {@link OrderRig}: the base BUY LIMIT of user 1 with a learned leg, the SELL
 * counterparty of user 2, the bridge events (outcome, trade, terminal) with the contract's command-id
 * encoding ({@code idHigh = orderId, idLow = attempt << 2 | kind}), and the replace-deadline reads.
 * Package-private; shared by {@link AmendTest} and {@link AmendAdmissionTest}.
 */
final class AmendSupport {

    static final long LEG = 77;       // the base order's create leg
    static final long NEW_LEG = 78;   // the leg the ME assigns to the replace
    static final long SELL_LEG = 88;  // the counterparty's leg
    static final int KIND_CREATE = 0;
    static final int KIND_CANCEL = 1;
    static final int KIND_AMEND = 2;
    static final int APPLIED = 0;
    static final int ME_REJECTED = 1;
    static final int T_AMEND = 52;
    static final long REPLACE_WINDOW_MS = 10_000L;
    static final long PRICE = 100 * ONE;

    final OrderRig r;
    private long egress;
    private long nextTradeId = 1;
    private long nextAmendRequest = 1;

    AmendSupport(final OrderRig r) {
        this.r = r;
    }

    /** User 1 holds 1000 USD and BUY LIMIT 100 x 1 on market 1 (orderId 1) with leg 77 learned via CREATE APPLIED. */
    static AmendSupport base() {
        return new AmendSupport(OrderRig.open()).buyBase(true);
    }

    /** The same order with its create still dispatch-pending (no outcome yet). */
    static AmendSupport baseWithoutLeg() {
        return new AmendSupport(OrderRig.open()).buyBase(false);
    }

    AmendSupport buyBase(final boolean withLeg) {
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, 0, PRICE, ONE));
        if (withLeg) {
            createOutcome(1, 1, LEG);
            assertLeg(1, LEG);
        }
        return this;
    }

    /** User 2 holds 1 BTC and SELL LIMIT 100 x 1 on market 1 (orderId 2) with leg 88 learned. */
    void sellSide() {
        r.deposit(2, BTC, ONE);
        r.create(r.limit(2, 1, 1, PRICE, ONE));
        createOutcome(2, 2, SELL_LEG);
        assertLeg(2, SELL_LEG);
    }

    void assertLeg(final long orderId, final long leg) {
        final CoreOrder o = order(orderId);
        assertNotNull("precondition: order " + orderId + " is live", o);
        assertEquals("precondition (contract 3.3 CREATE APPLIED): order " + orderId + " must have learned leg " + leg
                + " before an amend can be exercised", leg, o.currentLegId);
        assertEquals("precondition: ACK_NEW moves the record to NEW", OrderStatus.NEW.code(), o.status);
        assertFalse("precondition: the create outcome clears DISPATCH_PENDING", o.has(CoreOrder.F_DISPATCH_PENDING));
    }

    void createOutcome(final long userId, final long orderId, final long legId) {
        outcome(userId, orderId, 0, KIND_CREATE, APPLIED, legId, 0L);
    }

    /** MeCommandOutcome for (orderId, attempt, kind) with the contract's id encoding; market 1, reason 0. */
    void outcome(final long userId, final long orderId, final int attempt, final int kind, final int result, final long legId,
                 final long oldOrderId) {
        r.orders.applyOutcome(++egress, orderId, ((long) attempt << 2) | kind, userId, orderId, oldOrderId, 0L, 0L, 0L, 1,
                kind, result, legId, 0, r.now, 0L, r.now);
    }

    /** User 2's sell leg (taker) takes {@code qty} out of the base BUY (maker, leg 77) at 100; dense tradeIds from 1. */
    void fillBase(final long qty) {
        r.orders.applyTrade(++egress, nextTradeId++, 1, SELL_LEG, 2, LEG, 1, PRICE, qty, false, r.now, 2, 1, 0L, r.now);
    }

    void terminal(final long legId, final long userId, final long orderId, final int statusCode) {
        r.orders.applyTerminal(++egress, legId, userId, 1, statusCode, r.now, orderId, 0L, r.now);
    }

    AmendOrderCommand amendCmd(final long userId, final long orderId, final long newPrice, final long newQuantity) {
        final AmendOrderCommand c = new AmendOrderCommand();
        c.reset();
        c.correlationId = 8_000 + nextAmendRequest;
        c.userId = userId;
        c.orderId = orderId;
        c.requestIdHigh = 0xA3E0L;
        c.requestIdLow = nextAmendRequest++;
        c.newPrice = newPrice;
        c.newQuantity = newQuantity;
        return c;
    }

    AmendOrderCommand amend(final long userId, final long orderId, final long newPrice, final long newQuantity) {
        final AmendOrderCommand c = amendCmd(userId, orderId, newPrice, newQuantity);
        amend(c);
        return c;
    }

    void amend(final AmendOrderCommand c) {
        r.orders.applyAmend(c, r.now);
    }

    CoreOrder order(final long orderId) {
        return r.orders.orders().get(orderId);
    }

    Account acct(final long userId) {
        return r.engine.account(userId);
    }

    int replaceDeadlines() {
        final int[] n = new int[1];
        r.engine.deadlines().forEach((d, id, k) -> {
            if (k == DeadlineIndex.KIND_REPLACE) {
                n[0]++;
            }
        });
        return n[0];
    }

    /** The replace deadline of {@code orderId} in the index, or -1 when there is none. */
    long replaceDeadlineOf(final long orderId) {
        final long[] at = {-1L};
        r.engine.deadlines().forEach((d, id, k) -> {
            if (k == DeadlineIndex.KIND_REPLACE && id == orderId) {
                at[0] = d;
            }
        });
        return at[0];
    }

    /** The one event since the mark must be an OrderReject naming the AmendOrder template; returns its reason. */
    CoreRejectReason rejectOnly(final String what) {
        final List<AssetsEngineEvent> e = r.events();
        assertEquals(what + ": exactly one OrderReject and nothing else, got " + e, 1, e.size());
        assertTrue(what + ": expected an OrderReject, got " + e.get(0), e.get(0) instanceof AssetsEngineEvent.OrderReject);
        final AssetsEngineEvent.OrderReject rej = (AssetsEngineEvent.OrderReject) e.get(0);
        assertEquals("the reject names the AmendOrder template", T_AMEND, rej.refTemplateId());
        return rej.reason();
    }
}

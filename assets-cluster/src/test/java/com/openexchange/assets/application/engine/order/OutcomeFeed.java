// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.EventKind;

/**
 * Feeds ME-side events (MeCommandOutcome, MeJournalTrade, MeJournalTerminal) into an {@link OrderRig} with the
 * contract S5 argument order spelled out once. Codes are the wire codes: outcome kind CREATE 0 / CANCEL 1 /
 * AMEND 2; outcome result APPLIED 0, REJECTED 1, CONFLICT 2, CAPACITY 3, UNKNOWN_LEG 4, NOT_OWNER 5,
 * UNKNOWN_MARKET 6; terminal status FILLED 2 / CANCELLED 3 / REJECTED 4 / EXPIRED 5.
 */
final class OutcomeFeed {

    static final int KIND_CREATE = 0;
    static final int KIND_CANCEL = 1;
    static final int KIND_AMEND = 2;
    static final int APPLIED = 0;
    static final int REJECTED = 1;
    static final int CONFLICT = 2;
    static final int CAPACITY = 3;
    static final int UNKNOWN_LEG = 4;
    static final int NOT_OWNER = 5;
    static final int UNKNOWN_MARKET = 6;
    static final int BUY = 0;
    static final int SELL = 1;
    static final int ME_FILLED = 2;
    static final int ME_CANCELLED = 3;
    static final long LEG_A = 77;
    static final long LEG_B = 78;

    /** The bridge egress sequence is carried, never decided on (contract 3.1 #4): a process-wide counter is enough. */
    private static long egressSeq;

    private OutcomeFeed() { }

    /** The live record, or an assertion failure naming the order. */
    static CoreOrder live(final OrderRig r, final long orderId) {
        final CoreOrder o = r.orders.orders().get(orderId);
        if (o == null) {
            throw new AssertionError("expected a live record for order " + orderId);
        }
        return o;
    }

    /** MeCommandOutcome (64) for (orderId, attempt, kind) with an explicit party: usable when the record is gone. */
    static void outcome(final OrderRig r, final long orderId, final long userId, final int marketId, final int attempt,
                        final int kind, final int result, final long legId, final long oldLegId, final int reason) {
        r.orders.applyOutcome(++egressSeq, orderId, ((long) attempt << 2) | kind, userId, orderId, oldLegId, 0L, 0L, 0L,
                marketId, kind, result, legId, reason, r.now, 0L, r.now);
    }

    /** MeCommandOutcome for a live order (party read from the record). */
    static void outcome(final OrderRig r, final long orderId, final int attempt, final int kind, final int result,
                        final long legId, final long oldLegId) {
        final CoreOrder o = live(r, orderId);
        outcome(r, orderId, o.userId, o.marketId, attempt, kind, result, legId, oldLegId, 0);
    }

    /** CREATE outcome APPLIED for attempt 0: the ME tells the core which leg rests for this order. */
    static void ack(final OrderRig r, final long orderId, final long legId) {
        outcome(r, orderId, 0, KIND_CREATE, APPLIED, legId, 0L);
    }

    /** MeJournalTerminal (63) for a live order's leg. */
    static void terminal(final OrderRig r, final long orderId, final long legId, final int statusCode) {
        final CoreOrder o = live(r, orderId);
        r.orders.applyTerminal(++egressSeq, legId, o.userId, o.marketId, statusCode, r.now, orderId, 0L, r.now);
    }

    /** MeJournalTrade (61) between two core orders, taker first. */
    static void trade(final OrderRig r, final long tradeId, final int marketId, final long takerOrderId, final long takerLegId,
                      final long takerUserId, final long makerOrderId, final long makerLegId, final long makerUserId,
                      final long price, final long qty, final boolean takerIsBuy) {
        r.orders.applyTrade(++egressSeq, tradeId, marketId, takerLegId, takerUserId, makerLegId, makerUserId, price, qty,
                takerIsBuy, r.now, takerOrderId, makerOrderId, 0L, r.now);
    }

    /** The first OrderEvent of this kind for this order since the mark, or an assertion failure listing what came. */
    static AssetsEngineEvent.OrderEvent event(final OrderRig r, final long orderId, final EventKind kind) {
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.OrderEvent ev && ev.orderId() == orderId && ev.kind() == kind) {
                return ev;
            }
        }
        throw new AssertionError("expected an OrderEvent " + kind + " for order " + orderId + ", got " + r.events());
    }

    /** How many OrderEvents of this kind the order produced since the mark. */
    static long kindCount(final OrderRig r, final long orderId, final EventKind kind) {
        long n = 0;
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.OrderEvent ev && ev.orderId() == orderId && ev.kind() == kind) {
                n++;
            }
        }
        return n;
    }

    /** The last OrderAck since the mark, or an assertion failure. */
    static AssetsEngineEvent.OrderAck lastAck(final OrderRig r) {
        AssetsEngineEvent.OrderAck last = null;
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.OrderAck a) {
                last = a;
            }
        }
        if (last == null) {
            throw new AssertionError("expected an OrderAck, got " + r.events());
        }
        return last;
    }

    /** GTD entries on the single deadline index (request TTLs ride the same index). */
    static int gtdDeadlines(final OrderRig r) {
        final int[] n = new int[1];
        r.engine.deadlines().forEach((d, id, k) -> { if (k == DeadlineIndex.KIND_GTD) n[0]++; });
        return n[0];
    }
}

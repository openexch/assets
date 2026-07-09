// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayDeque;

/**
 * Aggregate root for one user's money. It is the <em>only</em> place that mutates that user's
 * balances and holds, so every invariant lives here and cannot be bypassed:
 *
 * <ul>
 *   <li><b>No overdraft</b> — a HOLD or WITHDRAW is rejected when {@code available < amount}.</li>
 *   <li><b>No negative balances</b> — available/locked never go below zero (guarded in every method).</li>
 *   <li><b>Locked is fully accounted</b> — for each asset, {@code locked == Σ remaining} over that
 *       asset's open holds (preserved by construction on every mutation).</li>
 * </ul>
 *
 * <p><b>DDD ↔ zero-alloc:</b> this is a long-lived mutable aggregate — exactly one instance per user,
 * created once and mutated in place. Balances are a dense {@code long[]} indexed by assetId; holds are
 * pooled. Nothing is allocated per operation once the user and its holds exist. Not thread-safe: the
 * engine drives all accounts from a single thread.</p>
 *
 * <p>Cross-account operations (settlement) are coordinated by {@link SettlementService}, which calls
 * this aggregate's {@code applyBuyFill}/{@code applySellFill} — the account still owns its own side of
 * the mutation and its own invariants.</p>
 */
public final class Account {

    /** Visitor for iterating open holds (snapshotting). Not on the hot path. */
    public interface HoldVisitor {
        void visit(long orderId, int assetId, long remaining);
    }

    private static final int ASSETS = Asset.count();

    private final long userId;
    // [avail_0, locked_0, avail_1, locked_1, ...] — index 2*assetId = available, +1 = locked.
    private final long[] bal = new long[2 * ASSETS];
    private final Long2ObjectHashMap<Hold> holds = new Long2ObjectHashMap<>();
    private final ArrayDeque<Hold> holdPool = new ArrayDeque<>();

    public Account(long userId) {
        this.userId = userId;
    }

    public long userId() {
        return userId;
    }

    public long available(int assetId) {
        return bal[2 * assetId];
    }

    public long locked(int assetId) {
        return bal[2 * assetId + 1];
    }

    /** assetId of the hold for {@code orderId}, or -1 if there is no such hold. */
    public int holdAssetId(long orderId) {
        Hold h = holds.get(orderId);
        return h == null ? -1 : h.assetId();
    }

    public long holdRemaining(long orderId) {
        Hold h = holds.get(orderId);
        return h == null ? 0L : h.remaining();
    }

    public boolean hasHold(long orderId) {
        return holds.containsKey(orderId);
    }

    // ---- external boundary ----

    /** Credit available balance. Rejects a non-positive amount; never creates a hold. */
    public RejectReason deposit(int assetId, long amount) {
        if (amount <= 0) {
            return RejectReason.INVALID_AMOUNT;
        }
        bal[2 * assetId] += amount;
        return RejectReason.NONE;
    }

    /** Debit available balance. Rejects a non-positive amount or an overdraft. */
    public RejectReason withdraw(int assetId, long amount) {
        if (amount <= 0) {
            return RejectReason.INVALID_AMOUNT;
        }
        if (bal[2 * assetId] < amount) {
            return RejectReason.INSUFFICIENT_FUNDS;
        }
        bal[2 * assetId] -= amount;
        return RejectReason.NONE;
    }

    // ---- holds ----

    /**
     * Reserve {@code amount} of {@code assetId} for {@code orderId} (available -> locked). Rejects a
     * non-positive amount or an overdraft. Precondition: {@code orderId} is unique (the matching
     * engine allocates monotonic order ids); a duplicate would overwrite the prior hold reference.
     */
    public RejectReason hold(long orderId, int assetId, long amount) {
        if (amount <= 0) {
            return RejectReason.INVALID_AMOUNT;
        }
        if (bal[2 * assetId] < amount) {
            return RejectReason.INSUFFICIENT_FUNDS;
        }
        bal[2 * assetId] -= amount;
        bal[2 * assetId + 1] += amount;
        Hold h = acquireHold();
        h.set(assetId, amount);
        holds.put(orderId, h);
        return RejectReason.NONE;
    }

    /**
     * Release part or all of a hold's residual (locked -> available). {@code amount < 0} releases the
     * full remaining reservation. Removes (and recycles) the hold once nothing is left. Returns the
     * amount actually released (0 if there is no such hold).
     */
    public long release(long orderId, long amount) {
        Hold h = holds.get(orderId);
        if (h == null) {
            return 0L;
        }
        int assetId = h.assetId();
        long rel = (amount < 0) ? h.remaining() : Math.min(amount, h.remaining());
        bal[2 * assetId + 1] -= rel;
        bal[2 * assetId] += rel;
        h.drawDown(rel);
        if (h.remaining() == 0) {
            holds.remove(orderId);
            recycle(h);
        }
        return rel;
    }

    // ---- settlement (called by SettlementService; the account owns its own side) ----

    /**
     * Apply the buyer's side of a fill: pay {@code quoteAmt} out of the buyer's locked quote (drawing
     * the buyer's quote hold down) and receive {@code baseAmt} into available base.
     * @throws IllegalStateException if the hold or locked balance cannot cover the payment (a
     *         conservation breach that the matching engine's guarantees make impossible).
     */
    public void applyBuyFill(long buyerOrderId, int quoteAsset, long quoteAmt, int baseAsset, long baseAmt) {
        drawDownHold(buyerOrderId, quoteAsset, quoteAmt);
        bal[2 * quoteAsset + 1] -= quoteAmt;   // pay from locked quote
        bal[2 * baseAsset] += baseAmt;         // receive base into available
    }

    /**
     * Apply the seller's side of a fill: deliver {@code baseAmt} out of the seller's locked base
     * (drawing the seller's base hold down) and receive {@code quoteAmt} into available quote.
     */
    public void applySellFill(long sellerOrderId, int baseAsset, long baseAmt, int quoteAsset, long quoteAmt) {
        drawDownHold(sellerOrderId, baseAsset, baseAmt);
        bal[2 * baseAsset + 1] -= baseAmt;     // deliver from locked base
        bal[2 * quoteAsset] += quoteAmt;       // receive quote into available
    }

    private void drawDownHold(long orderId, int assetId, long amount) {
        Hold h = holds.get(orderId);
        if (h == null || h.assetId() != assetId || h.remaining() < amount || bal[2 * assetId + 1] < amount) {
            // Money must never be created: refuse to draw more than is reserved/locked.
            throw new IllegalStateException(
                    "settlement would breach conservation: user=" + userId + " order=" + orderId
                            + " asset=" + assetId + " draw=" + amount
                            + " holdRemaining=" + (h == null ? -1 : h.remaining())
                            + " locked=" + bal[2 * assetId + 1]);
        }
        h.drawDown(amount);
    }

    // ---- snapshot support (infrastructure/persistence adapter reads/writes through these) ----

    /** Restore a balance pair verbatim (deserialize). */
    public void restoreBalance(int assetId, long available, long locked) {
        bal[2 * assetId] = available;
        bal[2 * assetId + 1] = locked;
    }

    /** Restore a hold verbatim (deserialize). */
    public void restoreHold(long orderId, int assetId, long remaining) {
        Hold h = acquireHold();
        h.set(assetId, remaining);
        holds.put(orderId, h);
    }

    public int holdCount() {
        return holds.size();
    }

    /** Visit every open hold (serialize / invariant checks). Boxes the key — not hot-path (snapshot only). */
    public void forEachHold(HoldVisitor v) {
        holds.forEach((orderId, h) -> v.visit(orderId, h.assetId(), h.remaining()));
    }

    private Hold acquireHold() {
        Hold h = holdPool.poll();
        return h != null ? h : new Hold();
    }

    private void recycle(Hold h) {
        holdPool.push(h);
    }
}

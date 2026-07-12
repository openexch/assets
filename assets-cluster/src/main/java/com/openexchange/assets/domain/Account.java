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
 * this aggregate's {@code settleDebit}/{@code settleCredit} — the account still owns its own side of
 * the mutation and its own invariants.</p>
 */
public final class Account {

    /** Visitor for iterating open holds (snapshotting). Not on the hot path. */
    public interface HoldVisitor {
        void visit(long orderId, int assetId, long remaining, boolean omsManagedRelease);
    }

    /** Visitor for iterating non-zero balances (snapshot queries). Not on the hot path. */
    public interface BalanceVisitor {
        void visit(int assetId, long available, long locked);
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

    /** TRUE when this order's hold exists and the OMS owns its terminal release (feed must no-op). */
    public boolean isOmsManagedRelease(long orderId) {
        Hold h = holds.get(orderId);
        return h != null && h.omsManagedRelease();
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
     * Reserve {@code amount} of {@code assetId} for {@code orderId} (available -> locked).
     *
     * <p><b>Create vs. top-up.</b> If no hold exists for {@code orderId} this creates one (the common
     * case: the matching engine allocates monotonic order ids). If a hold for {@code orderId} <em>already
     * exists</em> this is an atomic <b>top-up</b> that adds {@code amount} to the existing reservation:
     * this is how an <em>amend</em> that raises an order's reserved value is expressed — the OMS places
     * the amend's hold <b>delta</b> under the same {@code orderId}. (Blind client retries of a hold are
     * banned client-side, so a duplicate {@code orderId} is always a genuine incremental reservation,
     * never an accidental double-debit.)</p>
     *
     * <p>This top-up contract exists to close a real corruption bug: the previous behaviour overwrote the
     * hold reference on a duplicate {@code orderId} while still debiting available, silently leaking the
     * first hold's residual and breaking the {@code locked == Σ remaining} invariant.</p>
     *
     * <p><b>Rejections mutate nothing:</b></p>
     * <ul>
     *   <li>non-positive {@code amount} -> {@link RejectReason#INVALID_AMOUNT};</li>
     *   <li>top-up whose {@code assetId} differs from the existing hold's asset ->
     *       {@link RejectReason#INVALID_AMOUNT} (a hold reserves exactly one asset);</li>
     *   <li>{@code available < amount} (create or top-up) -> {@link RejectReason#INSUFFICIENT_FUNDS}.</li>
     * </ul>
     *
     * <p>On accept (create or top-up): {@code available -= amount; locked += amount; remaining += amount}
     * — so {@code locked == Σ remaining} is preserved on every path.</p>
     */
    public RejectReason hold(long orderId, int assetId, long amount) {
        return hold(orderId, assetId, amount, false);
    }

    /**
     * @param omsManagedRelease TRUE = the OMS owns this hold's terminal release (iceberg/stop
     *        PARENT holds); the feed's TerminalRelease must no-op on it. Placement-time property:
     *        a top-up keeps the ORIGINAL flag (an amend delta cannot change release ownership).
     */
    public RejectReason hold(long orderId, int assetId, long amount, boolean omsManagedRelease) {
        if (amount <= 0) {
            return RejectReason.INVALID_AMOUNT;
        }
        Hold existing = holds.get(orderId);
        if (existing != null) {
            // Top-up an existing reservation (amend delta). A hold reserves exactly one asset.
            if (existing.assetId() != assetId) {
                return RejectReason.INVALID_AMOUNT; // cross-asset top-up is illegal — mutate nothing
            }
            if (bal[2 * assetId] < amount) {
                return RejectReason.INSUFFICIENT_FUNDS; // overdraft — mutate nothing
            }
            bal[2 * assetId] -= amount;
            bal[2 * assetId + 1] += amount;
            existing.topUp(amount);
            return RejectReason.NONE;
        }
        // Create a fresh hold.
        if (bal[2 * assetId] < amount) {
            return RejectReason.INSUFFICIENT_FUNDS;
        }
        bal[2 * assetId] -= amount;
        bal[2 * assetId + 1] += amount;
        Hold h = acquireHold();
        h.set(assetId, amount, omsManagedRelease);
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
     * Result of one settle debit — reusable holder (single engine thread), written by
     * {@link #settleDebit}. A nonzero field means the hold could not fully cover the leg.
     */
    public static final class SettleDebitResult {
        /** Recovered from the payer's available balance because the hold was short/missing. */
        public long drawnFromAvailable;
        /** Could not be moved at all (hold AND available exhausted) — a reportable breach. */
        public long uncovered;
        /**
         * TRUE when this debit left the order's hold with {@code remaining == 0} and it was therefore
         * removed and recycled (mirroring {@link Account#release}). Normal-path bookkeeping, NOT a
         * fault — {@link #faulted()} is unaffected.
         */
        public boolean reapedExhaustedHold;

        void reset() {
            drawnFromAvailable = 0;
            uncovered = 0;
            reapedExhaustedHold = false;
        }

        public boolean faulted() {
            return drawnFromAvailable != 0 || uncovered != 0;
        }
    }

    /**
     * Debit {@code amount} of {@code assetId} for a settle leg, preferring the order's hold.
     *
     * <p>NORMAL path (the hold-gate guarantee): the whole amount comes out of the order's hold
     * (locked). EXCEPTIONAL path — the hold is missing, short, or holds a different asset (e.g. an
     * early orphan-release raced an in-flight fill): the shortfall is recovered from AVAILABLE,
     * floored at zero, and anything still uncovered is reported in {@code out} — <b>never thrown</b>.
     * A throw here would poison the replicated log and re-crash the service on every replay; a
     * deterministic partial debit + a loud {@code SettleFault} event keeps the ledger live and the
     * breach observable. Conservation stays intact because the caller credits the counterparty
     * exactly what was debited: {@code amount - out.uncovered}.</p>
     *
     * <p>If the draw leaves the hold with {@code remaining == 0} the hold is removed and recycled,
     * exactly mirroring {@link #release} — an exhausted hold never lingers as a tombstone waiting for
     * a terminal release that may never arrive. Reported via {@code out.reapedExhaustedHold} (not a
     * fault).</p>
     *
     * @return the amount actually debited ({@code amount - out.uncovered})
     */
    public long settleDebit(long orderId, int assetId, long amount, SettleDebitResult out) {
        out.reset();
        long fromHold = 0;
        Hold h = holds.get(orderId);
        if (h != null && h.assetId() == assetId) {
            fromHold = Math.min(h.remaining(), amount);
            // Defensive clamp: locked == Σ remaining structurally; never let locked go negative.
            fromHold = Math.min(fromHold, bal[2 * assetId + 1]);
            h.drawDown(fromHold);
            bal[2 * assetId + 1] -= fromHold;
            if (h.remaining() == 0) {
                // Exactly mirrors release(): an exhausted hold is removed and recycled, never left
                // behind as a remaining=0 tombstone. Defense-in-depth — a fully-filled order whose
                // TerminalRelease never arrives (e.g. the ME's missing maker terminal) must not leak
                // its hold. Pure map bookkeeping: balances are untouched, and a later hold() with the
                // same orderId is a fresh reservation, exactly as after a full release().
                holds.remove(orderId);
                recycle(h);
                out.reapedExhaustedHold = true;
            }
        }
        long shortfall = amount - fromHold;
        if (shortfall > 0) {
            long fromAvailable = Math.min(bal[2 * assetId], shortfall);
            bal[2 * assetId] -= fromAvailable;
            out.drawnFromAvailable = fromAvailable;
            out.uncovered = shortfall - fromAvailable;
        }
        return amount - out.uncovered;
    }

    /** Credit a settle leg's proceeds into available. The amount is what the payer actually paid. */
    public void settleCredit(int assetId, long amount) {
        bal[2 * assetId] += amount;
    }

    // ---- snapshot support (infrastructure/persistence adapter reads/writes through these) ----

    /** Restore a balance pair verbatim (deserialize). */
    public void restoreBalance(int assetId, long available, long locked) {
        bal[2 * assetId] = available;
        bal[2 * assetId + 1] = locked;
    }

    /** Restore a hold verbatim (deserialize). */
    public void restoreHold(long orderId, int assetId, long remaining, boolean omsManagedRelease) {
        Hold h = acquireHold();
        h.set(assetId, remaining, omsManagedRelease);
        holds.put(orderId, h);
    }

    public int holdCount() {
        return holds.size();
    }

    /** Visit every open hold (serialize / invariant checks). Boxes the key — not hot-path (snapshot only). */
    public void forEachHold(HoldVisitor v) {
        holds.forEach((orderId, h) -> v.visit(orderId, h.assetId(), h.remaining(), h.omsManagedRelease()));
    }

    /**
     * Visit every asset with a non-zero available or locked balance, in ascending assetId order
     * (the dense array's natural, deterministic order). Snapshot-query use only — not hot-path.
     */
    public void forEachNonZeroBalance(BalanceVisitor v) {
        for (int assetId = 0; assetId < ASSETS; assetId++) {
            long available = bal[2 * assetId];
            long locked = bal[2 * assetId + 1];
            if (available != 0L || locked != 0L) {
                v.visit(assetId, available, locked);
            }
        }
    }

    private Hold acquireHold() {
        Hold h = holdPool.poll();
        return h != null ? h : new Hold();
    }

    private void recycle(Hold h) {
        holdPool.push(h);
    }
}

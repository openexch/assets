// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.domain.CoreOrderView;

/**
 * One order as the core keeps it (lifecycle spec B3; layout page section 2). Plain fields, pooled, slab
 * indexed by the dense {@code orderId}. Every field is replicated state except nothing: diagnostics live on
 * the engine. Codes are wire codes (money-schema v6 enums) so the record serializes without a mapping table.
 *
 * <p>Serialized size is fixed at {@link #SNAPSHOT_BYTES}: 35 longs, 4 ints, 1 short, 6 bytes (S5 added
 * {@code pendingDispatchSeq} and {@code replaceDeadlineMs}; RC2 S8 5.1 added {@code pendingLegQty} and
 * {@code pendingExpectedOldRemaining}, 288 -> 304 B, every behaviour).</p>
 */
public final class CoreOrder implements CoreOrderView {

    public static final int SNAPSHOT_BYTES = 35 * 8 + 4 * 4 + 2 + 6;

    // state flags (u16)
    public static final int F_CANCEL_PENDING = 1;
    public static final int F_REPLACE_PENDING = 1 << 1;
    public static final int F_DISPATCH_PENDING = 1 << 2;
    public static final int F_EXPIRY_REQUESTED = 1 << 3;
    public static final int F_IOC = 1 << 4;
    public static final int F_FOK = 1 << 5;
    public static final int F_QUARANTINED = 1 << 6;
    /** RC2 S8 1.1: the pending command got CAPACITY and was not resolved since; decides nothing, feeds a metric. */
    public static final int F_ME_CAPACITY_WAIT = 1 << 7;
    /**
     * RC2 S8 1.1: a pending command of this order came back RELEASED 7, so the ME floor or tombstone of the order is unknown
     * to the core: its backlog entry was abandoned and no release is minted for it while set (3.1 rule 2).
     */
    public static final int F_ME_RELEASED = 1 << 8;
    /** Every state flag this build defines (S8 5.1 a: a restored record carries no other bit). */
    public static final int KNOWN_STATE_FLAGS = (1 << 9) - 1;

    // pending command kinds (byte)
    public static final int PK_NONE = 0;
    public static final int PK_CREATE = 1;
    public static final int PK_CANCEL = 2;
    public static final int PK_AMEND = 3;

    public long orderId;
    public long userId;
    public long clientOrderIdHigh;
    public long clientOrderIdLow;
    public long requestIdHigh;
    public long requestIdLow;
    public long price;
    public long quantity;
    public long budget;
    public long stopPrice;
    public long trailingDelta;
    public long trailingArmPrice;
    public long displayQty;
    public long hiddenQty;
    public long sliceRemainingQty;
    public long expiresAtMs;
    public long filledQty;
    public long remainingQty;
    public long cumQuoteQty;
    public long lastPrice;
    public long lastQty;
    public long holdAmount;
    public long createdAtMs;
    public long updatedAtMs;
    public long eventSeq;
    public long currentLegId;
    public long pendingPrice;
    public long pendingQuantity;
    public long pendingHoldDelta;
    public long pendingHoldTarget;
    public long oldLegId;
    /** dispatchSeq of the ME command in flight (0 = none); orders the RequestPendingDispatch replay (S5). */
    public long pendingDispatchSeq;
    /** Log-time deadline of the open replace window (0 = none); its deadline-index entry is derived from it (S5). */
    public long replaceDeadlineMs;
    /** RC2 S8 3.7: the AMEND quantity minted ({@code newQty - filledQty} at mint); 0 outside a replace window. */
    public long pendingLegQty;
    /** RC2 S8 3.7: {@code remainingQty} at the amend mint (S1 4.4 row 104); 0 outside a replace window. */
    public long pendingExpectedOldRemaining;
    public int marketId;
    public int flags;
    public int holdAssetId;
    public int attempt;
    public int stateFlags;
    public byte side;
    public byte orderType;
    public byte timeInForce;
    public byte status;
    public byte rejectReason;
    public byte pendingKind;

    public void clear() {
        orderId = 0; userId = 0; clientOrderIdHigh = 0; clientOrderIdLow = 0; requestIdHigh = 0; requestIdLow = 0;
        price = 0; quantity = 0; budget = 0; stopPrice = 0; trailingDelta = 0; trailingArmPrice = 0; displayQty = 0;
        hiddenQty = 0; sliceRemainingQty = 0; expiresAtMs = 0; filledQty = 0; remainingQty = 0; cumQuoteQty = 0;
        lastPrice = 0; lastQty = 0; holdAmount = 0; createdAtMs = 0; updatedAtMs = 0; eventSeq = 0; currentLegId = 0;
        pendingPrice = 0; pendingQuantity = 0; pendingHoldDelta = 0; pendingHoldTarget = 0; oldLegId = 0; pendingDispatchSeq = 0; replaceDeadlineMs = 0;
        pendingLegQty = 0; pendingExpectedOldRemaining = 0;
        marketId = 0; flags = 0; holdAssetId = 0; attempt = 0; stateFlags = 0;
        side = 0; orderType = 0; timeInForce = 0; status = 0; rejectReason = 0; pendingKind = 0;
    }

    public boolean has(final int flag) {
        return (stateFlags & flag) != 0;
    }

    public void set(final int flag) {
        stateFlags |= flag;
    }

    public void unset(final int flag) {
        stateFlags &= ~flag;
    }

    /** FILLED, CANCELLED, REJECTED or EXPIRED. */
    public boolean isTerminal() {
        return status == 2 || status == 3 || status == 4 || status == 5;
    }

    public boolean hasClientOrderId() {
        return clientOrderIdHigh != 0 || clientOrderIdLow != 0;
    }

    // ---- CoreOrderView ----
    public long orderId() { return orderId; }
    public long userId() { return userId; }
    public int marketId() { return marketId; }
    public int statusCode() { return status; }
    public int sideCode() { return side; }
    public int orderTypeCode() { return orderType; }
    public int timeInForceCode() { return timeInForce; }
    public long price() { return price; }
    public long stopPrice() { return stopPrice; }
    public long quantity() { return quantity; }
    public long filledQty() { return filledQty; }
    public long remainingQty() { return remainingQty; }
    public long meOrderId() { return currentLegId; }
    public int holdAssetId() { return holdAssetId; }
    public long holdAmount() { return holdAmount; }
    public long clientOrderIdHigh() { return clientOrderIdHigh; }
    public long clientOrderIdLow() { return clientOrderIdLow; }
    public long createdAtMs() { return createdAtMs; }
    public long updatedAtMs() { return updatedAtMs; }
    public long budget() { return budget; }
    public long displayQty() { return displayQty; }
    public long hiddenQty() { return hiddenQty; }
    public long expiresAtMs() { return expiresAtMs; }
    public long trailingArmPrice() { return trailingArmPrice; }
    public int sliceSeq() { return attempt; }

    /** A live ME leg is known for this order (learned from the create outcome or an implicit-ack trade). */
    public boolean hasLeg() {
        return currentLegId != 0;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** Reserve funds for an order (available -> locked). Pooled; {@link #reset()} for reuse. */
public final class HoldCommand {
    private long correlationId;
    private long orderId;
    private long userId;
    private int assetId;
    private long amount;

    public void reset() {
        correlationId = 0L;
        orderId = 0L;
        userId = 0L;
        assetId = 0;
        amount = 0L;
    }

    /** Client-chosen id echoed on the HoldAck / HoldReject (0 = fire-and-forget). */
    public long getCorrelationId() { return correlationId; }
    public void setCorrelationId(long correlationId) { this.correlationId = correlationId; }

    public long getOrderId() { return orderId; }
    public void setOrderId(long orderId) { this.orderId = orderId; }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public int getAssetId() { return assetId; }
    public void setAssetId(int assetId) { this.assetId = assetId; }

    public long getAmount() { return amount; }
    public void setAmount(long amount) { this.amount = amount; }
}

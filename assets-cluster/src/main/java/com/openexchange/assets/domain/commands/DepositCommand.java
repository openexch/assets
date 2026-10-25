// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** Credit available balance (external boundary). Pooled; {@link #reset()} for reuse. */
public final class DepositCommand {
    private long correlationId;
    private long userId;
    private int assetId;
    private long amount;

    /** v6: 128-bit durable identity (0/0 = none). */

    private long requestIdHigh;

    private long requestIdLow;


    public void reset() {
        correlationId = 0L;
        userId = 0L;
        assetId = 0;
        amount = 0L;

        requestIdHigh = 0L;

        requestIdLow = 0L;
    }

    /** Client-chosen id echoed on the DepositAck (0 = fire-and-forget). */
    public long getCorrelationId() { return correlationId; }
    public void setCorrelationId(long correlationId) { this.correlationId = correlationId; }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public int getAssetId() { return assetId; }
    public void setAssetId(int assetId) { this.assetId = assetId; }

    public long getAmount() { return amount; }
    public void setAmount(long amount) { this.amount = amount; }

    public long getRequestIdHigh() { return requestIdHigh; }
    public void setRequestIdHigh(long v) { this.requestIdHigh = v; }
    public long getRequestIdLow() { return requestIdLow; }
    public void setRequestIdLow(long v) { this.requestIdLow = v; }
    public boolean hasRequestId() { return requestIdHigh != 0L || requestIdLow != 0L; }
}

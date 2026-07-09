// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/**
 * Release a hold's residual (locked -> available). {@code amount < 0} releases the full remaining
 * reservation (the terminal-status case). Pooled; {@link #reset()} for reuse.
 */
public final class ReleaseCommand {
    private long orderId;
    private long userId;
    private long amount;

    public void reset() {
        orderId = 0L;
        userId = 0L;
        amount = 0L;
    }

    public long getOrderId() { return orderId; }
    public void setOrderId(long orderId) { this.orderId = orderId; }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public long getAmount() { return amount; }
    public void setAmount(long amount) { this.amount = amount; }
}

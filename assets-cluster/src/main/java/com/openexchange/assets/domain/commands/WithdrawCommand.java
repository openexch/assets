// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** Debit available balance (external boundary). Pooled; {@link #reset()} for reuse. */
public final class WithdrawCommand {
    private long userId;
    private int assetId;
    private long amount;

    public void reset() {
        userId = 0L;
        assetId = 0;
        amount = 0L;
    }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public int getAssetId() { return assetId; }
    public void setAssetId(int assetId) { this.assetId = assetId; }

    public long getAmount() { return amount; }
    public void setAmount(long amount) { this.amount = amount; }
}

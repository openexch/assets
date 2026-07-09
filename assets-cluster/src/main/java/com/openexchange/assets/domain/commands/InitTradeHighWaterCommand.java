// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/**
 * Cutover primer: seed the settlement high-water mark and consume position on a <em>virgin</em> ledger
 * so a freshly-formed Assets Engine can start consuming the matching engine's trade stream from a known
 * cutover point instead of from zero. The engine refuses it (strict no-op) unless the ledger is
 * untouched (no accounts, {@code lastAppliedTradeId == 0}, {@code consumePosition == 0}); it never
 * mutates money and emits no egress. Pooled; {@link #reset()} for reuse.
 */
public final class InitTradeHighWaterCommand {
    private long tradeId;
    private long consumePosition;

    public void reset() {
        tradeId = 0L;
        consumePosition = 0L;
    }

    public long getTradeId() { return tradeId; }
    public void setTradeId(long tradeId) { this.tradeId = tradeId; }

    public long getConsumePosition() { return consumePosition; }
    public void setConsumePosition(long consumePosition) { this.consumePosition = consumePosition; }
}

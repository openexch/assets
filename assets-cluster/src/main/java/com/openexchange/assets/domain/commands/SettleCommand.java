// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/**
 * Apply a trade (feed-forward from the matching engine). Carries the matching context's identifiers;
 * the engine translates them into money terms (base/quote asset + amounts) before settling — the
 * anti-corruption boundary. Idempotent on {@code tradeId}. Pooled; {@link #reset()} for reuse.
 */
public final class SettleCommand {
    private long tradeId;
    private int marketId;
    private long takerOrderId;
    private long takerUserId;
    private long makerOrderId;
    private long makerUserId;
    private long price;
    private long quantity;
    private boolean takerIsBuy;

    public void reset() {
        tradeId = 0L;
        marketId = 0;
        takerOrderId = 0L;
        takerUserId = 0L;
        makerOrderId = 0L;
        makerUserId = 0L;
        price = 0L;
        quantity = 0L;
        takerIsBuy = false;
    }

    public long getTradeId() { return tradeId; }
    public void setTradeId(long tradeId) { this.tradeId = tradeId; }

    public int getMarketId() { return marketId; }
    public void setMarketId(int marketId) { this.marketId = marketId; }

    public long getTakerOrderId() { return takerOrderId; }
    public void setTakerOrderId(long takerOrderId) { this.takerOrderId = takerOrderId; }

    public long getTakerUserId() { return takerUserId; }
    public void setTakerUserId(long takerUserId) { this.takerUserId = takerUserId; }

    public long getMakerOrderId() { return makerOrderId; }
    public void setMakerOrderId(long makerOrderId) { this.makerOrderId = makerOrderId; }

    public long getMakerUserId() { return makerUserId; }
    public void setMakerUserId(long makerUserId) { this.makerUserId = makerUserId; }

    public long getPrice() { return price; }
    public void setPrice(long price) { this.price = price; }

    public long getQuantity() { return quantity; }
    public void setQuantity(long quantity) { this.quantity = quantity; }

    public boolean isTakerIsBuy() { return takerIsBuy; }
    public void setTakerIsBuy(boolean takerIsBuy) { this.takerIsBuy = takerIsBuy; }
}

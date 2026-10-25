// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** RC0 spike: a trade between two core orders (the ME journal feed's shape, minus the bridge). Pooled. */
public final class ProtoOrderFillCommand {
    public long correlationId;
    public long tradeId;
    public int marketId;
    public long buyOrderId;
    public long sellOrderId;
    public long price;
    public long quantity;
    public boolean takerIsBuy;

    public void reset() {
        correlationId = 0; tradeId = 0; marketId = 0; buyOrderId = 0; sellOrderId = 0; price = 0; quantity = 0; takerIsBuy = false;
    }
}

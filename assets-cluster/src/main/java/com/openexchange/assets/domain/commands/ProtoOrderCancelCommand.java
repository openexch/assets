// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** RC0 spike: cancel an open core order. Pooled. */
public final class ProtoOrderCancelCommand {
    public long correlationId;
    public long userId;
    public long orderId;

    public void reset() {
        correlationId = 0; userId = 0; orderId = 0;
    }
}

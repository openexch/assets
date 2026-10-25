// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/** RC0 spike: a GTD deadline reached (a cluster TimerEvent whose correlationId is the orderId). Pooled. */
public final class ProtoOrderExpireCommand {
    public long orderId;

    public void reset() {
        orderId = 0;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.gate;

import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;

/**
 * The replicated order-entry gate (K5 item 3, hardening B4): one global switch (marketId 0) and one per
 * market. While a gate is SUSPENDED, order admission (CreateOrder, AmendOrder, trigger children, iceberg
 * refills) is refused GATE_CLOSED; cancels, deposits, withdrawals, settlements, queries and timers keep
 * flowing. Genesis state is SUSPENDED/GENESIS for the global gate: a fresh cluster admits no orders until
 * an ADMIN opens it, so the cutover checklist has a hard step instead of a hope. Rules may close a gate
 * (retention budget, bridge halt, ME capacity); only an ADMIN opens one. Snapshotted (layout v4 section 5).
 */
public final class OrderEntryGate {

    public static final int GLOBAL = 0;
    public static final int MARKETS = 5;

    private final GateState[] state = new GateState[MARKETS + 1];
    private final GateReason[] reason = new GateReason[MARKETS + 1];

    public OrderEntryGate() {
        reset();
    }

    /** Genesis: global SUSPENDED/GENESIS, every market OPEN/ADMIN. */
    public void reset() {
        for (int m = 0; m <= MARKETS; m++) {
            state[m] = GateState.OPEN;
            reason[m] = GateReason.ADMIN;
        }
        state[GLOBAL] = GateState.SUSPENDED;
        reason[GLOBAL] = GateReason.GENESIS;
    }

    public static boolean validMarket(final int marketId) {
        return marketId >= 0 && marketId <= MARKETS;
    }

    /** Does order entry for {@code marketId} pass? Requires the global gate AND the market gate OPEN. */
    public boolean admits(final int marketId) {
        if (!validMarket(marketId) || marketId == GLOBAL) {
            return false;
        }
        return state[GLOBAL] == GateState.OPEN && state[marketId] == GateState.OPEN;
    }

    /** @return true when the state or reason changed */
    public boolean set(final int marketId, final GateState newState, final GateReason newReason) {
        if (!validMarket(marketId)) {
            throw new IllegalArgumentException("marketId " + marketId);
        }
        if (state[marketId] == newState && reason[marketId] == newReason) {
            return false;
        }
        state[marketId] = newState;
        reason[marketId] = newReason;
        return true;
    }

    public GateState state(final int marketId) {
        return state[marketId];
    }

    public GateReason reason(final int marketId) {
        return reason[marketId];
    }
}

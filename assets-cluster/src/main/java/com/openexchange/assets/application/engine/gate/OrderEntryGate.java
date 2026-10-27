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
 *
 * <p><b>RC2 S7 (contract section 6): the reason set.</b> Under behaviour v1/v2 a gate holds ONE state and ONE reason and
 * {@link #set} overwrites both (the RC1 rule, unchanged); {@code reasons} is derived from them ({@code OPEN -> 0},
 * {@code SUSPENDED -> 1 << reason}). Under v3 {@code reasons} is the authority: {@link #addReason} and
 * {@link #clearReason} touch one bit, and state and reason are derived ({@code reasons == 0 -> OPEN/ADMIN}, else
 * SUSPENDED with the lowest set bit), so one cause can never overwrite or clear another.</p>
 */
public final class OrderEntryGate {

    public static final int GLOBAL = 0;
    public static final int MARKETS = 5;
    /** The highest GateReason code (ME_BEHAVIOR 8); a bit above it is refused on restore. */
    public static final int MAX_REASON_BIT = 8;

    private final GateState[] state = new GateState[MARKETS + 1];
    private final GateReason[] reason = new GateReason[MARKETS + 1];
    private final int[] reasons = new int[MARKETS + 1];

    public OrderEntryGate() {
        reset();
    }

    /** Genesis: global SUSPENDED/GENESIS, every market OPEN/ADMIN. */
    public void reset() {
        for (int m = 0; m <= MARKETS; m++) {
            state[m] = GateState.OPEN;
            reason[m] = GateReason.ADMIN;
            reasons[m] = 0;
        }
        state[GLOBAL] = GateState.SUSPENDED;
        reason[GLOBAL] = GateReason.GENESIS;
        reasons[GLOBAL] = 1 << GateReason.GENESIS.code();
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

    /** The v1/v2 overwrite rule (RC1). @return true when the state or reason changed */
    public boolean set(final int marketId, final GateState newState, final GateReason newReason) {
        if (!validMarket(marketId)) {
            throw new IllegalArgumentException("marketId " + marketId);
        }
        if (state[marketId] == newState && reason[marketId] == newReason) {
            return false;
        }
        state[marketId] = newState;
        reason[marketId] = newReason;
        reasons[marketId] = derived(newState, newReason);
        return true;
    }

    /** v3: {@code reasons |= bit}. @return true when the bit was new */
    public boolean addReason(final int marketId, final GateReason r) {
        final int bit = 1 << r.code();
        if ((reasons[marketId] & bit) != 0) {
            return false;
        }
        setReasons(marketId, reasons[marketId] | bit);
        return true;
    }

    /** v3: {@code reasons &= ~bit} (only that bit). @return true when it was set */
    public boolean clearReason(final int marketId, final GateReason r) {
        final int bit = 1 << r.code();
        if ((reasons[marketId] & bit) == 0) {
            return false;
        }
        setReasons(marketId, reasons[marketId] & ~bit);
        return true;
    }

    /** v3: ADMIN OPEN, every bit cleared. @return true when anything was set */
    public boolean clearAll(final int marketId) {
        if (reasons[marketId] == 0) {
            return false;
        }
        setReasons(marketId, 0);
        return true;
    }

    /** v3: install a bitmask and derive state and reason from it (6.1). */
    public void setReasons(final int marketId, final int bits) {
        reasons[marketId] = bits;
        state[marketId] = bits == 0 ? GateState.OPEN : GateState.SUSPENDED;
        reason[marketId] = bits == 0 ? GateReason.ADMIN : GateReason.fromCode(Integer.numberOfTrailingZeros(bits));
    }

    /**
     * 6.1: the conversion at the SWITCH to behaviour 3 (same apply as the CoreConfig). OPEN markets get no bit; a SUSPENDED
     * market keeps an ADMIN-owned reason, and an engine-owned one becomes ADMIN (feed-virgin: no engine rule has run, so
     * ADMIN set it); GLOBAL keeps its reason and gains GENESIS, which only an ADMIN OPEN after the bind clears.
     */
    public void convertToReasonSet() {
        for (int m = 0; m <= MARKETS; m++) {
            if (m == GLOBAL) {
                setReasons(m, (state[m] == GateState.OPEN ? 0 : 1 << reason[m].code()) | 1 << GateReason.GENESIS.code());
            } else if (state[m] == GateState.OPEN) {
                setReasons(m, 0);
            } else {
                setReasons(m, adminOwned(reason[m]) ? 1 << reason[m].code() : 1 << GateReason.ADMIN.code());
            }
        }
    }

    /** 6.2 bit ownership: ADMIN 0, RETENTION_BUDGET 1, RECOVERY 3, APPLY_FAULT 6 are ADMIN's; the rest are engine-owned. */
    public static boolean adminOwned(final GateReason r) {
        return r == GateReason.ADMIN || r == GateReason.RETENTION_BUDGET || r == GateReason.RECOVERY || r == GateReason.APPLY_FAULT;
    }

    /** S1's derivation of the bitmask from one (state, reason). */
    public static int derived(final GateState s, final GateReason r) {
        return s == GateState.OPEN ? 0 : 1 << r.code();
    }

    /** Restore only (section 5 layout v2); {@link #restoreBreach} validates once the behaviour is known. */
    public void restore(final int marketId, final GateState s, final GateReason r, final int bits) {
        state[marketId] = s;
        reason[marketId] = r;
        reasons[marketId] = bits;
    }

    /** 6.3 restore validation for one market, or null. */
    public String restoreBreach(final int marketId, final boolean v3) {
        final int bits = reasons[marketId];
        if ((bits & ~((1 << (MAX_REASON_BIT + 1)) - 1)) != 0) {
            return "gate " + marketId + " reasons 0x" + Integer.toHexString(bits) + " has a bit above " + MAX_REASON_BIT;
        }
        if (!v3) {
            return bits == derived(state[marketId], reason[marketId]) ? null
                    : "gate " + marketId + " reasons 0x" + Integer.toHexString(bits) + " is not " + state[marketId] + "/" + reason[marketId];
        }
        final GateState s = bits == 0 ? GateState.OPEN : GateState.SUSPENDED;
        final GateReason r = bits == 0 ? GateReason.ADMIN : GateReason.fromCode(Integer.numberOfTrailingZeros(bits));
        return s == state[marketId] && r == reason[marketId] ? null
                : "gate " + marketId + " " + state[marketId] + "/" + reason[marketId] + " is not derived from reasons 0x"
                        + Integer.toHexString(bits);
    }

    public GateState state(final int marketId) {
        return state[marketId];
    }

    public GateReason reason(final int marketId) {
        return reason[marketId];
    }

    public int reasons(final int marketId) {
        return reasons[marketId];
    }
}

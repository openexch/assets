// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * Assets the ledger holds balances in, with dense integer ids for array indexing.
 *
 * <p>Ids and ordering mirror the OMS ({@code com.openexchange.oms.common.enums.Asset}) so a
 * {@code marketId} maps to the same base/quote pair on both sides of the trade feed. The AE keeps
 * its own copy (single-substrate ownership: no cross-context dependency).</p>
 */
public enum Asset {
    USD(0),
    BTC(1),
    ETH(2),
    SOL(3),
    XRP(4),
    DOGE(5);

    private final int id;

    Asset(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    private static final Asset[] BY_ID = new Asset[6];
    static {
        for (Asset a : values()) {
            BY_ID[a.id] = a;
        }
    }

    public static Asset fromId(int id) {
        if (id < 0 || id >= BY_ID.length) {
            throw new IllegalArgumentException("Unknown asset id: " + id);
        }
        return BY_ID[id];
    }

    /** Number of distinct assets — the width of an {@code Account}'s dense balance record. */
    public static int count() {
        return BY_ID.length;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/**
 * A reservation of funds against a single order, an entity <em>inside</em> the {@link Account}
 * aggregate (keyed there by {@code orderId}). Holds exactly the asset it reserves and how much of
 * the reservation is still outstanding.
 *
 * <p>Mutable and pooled by its owning {@code Account} (acquired on HOLD, recycled when
 * fully released) so repeated hold/release cycles allocate nothing on the steady-state hot path.
 * Not shared across accounts or threads.</p>
 */
public final class Hold {

    private int assetId;
    private long remaining;
    private boolean omsManagedRelease;

    /** Package-private: only the owning {@link Account} constructs/pools these. */
    Hold() {
    }

    void set(int assetId, long remaining, boolean omsManagedRelease) {
        this.assetId = assetId;
        this.remaining = remaining;
        this.omsManagedRelease = omsManagedRelease;
    }

    public int assetId() {
        return assetId;
    }

    public long remaining() {
        return remaining;
    }

    /** Reduce the outstanding reservation by {@code amount}; caller guarantees {@code amount <= remaining}. */
    void drawDown(long amount) {
        remaining -= amount;
    }

    /** TRUE = the OMS owns this hold's terminal release; the feed's TerminalRelease must no-op. */
    public boolean omsManagedRelease() {
        return omsManagedRelease;
    }

    /**
     * Increase the outstanding reservation by {@code amount} — an amend's hold <em>delta</em> placed
     * under the same order. Caller (the owning {@link Account}) guarantees the available balance was
     * already debited by {@code amount}, so {@code locked == Σ remaining} is preserved.
     */
    void topUp(long amount) {
        remaining += amount;
    }
}

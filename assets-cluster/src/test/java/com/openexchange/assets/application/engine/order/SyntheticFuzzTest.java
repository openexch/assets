// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import org.junit.Test;

/**
 * RC1 S6 G4 (contract section 6: "fuzz extension with stop/trailing/iceberg operations and invariants I1-I8,
 * I11, I13 after every op"). Three seeds, about 300 legal operations each over three users on market 1:
 * deposits, LIMIT creates, STOP_LOSS / STOP_LIMIT / TRAILING_STOP creates on both sides with the stop placed
 * away from the current quote, occasional ICEBERG creates, MarketTicks (increasing bookVersion, non-crossed
 * random walk, plus a few stale and crossed ones), CREATE outcomes for every kind of pending create (parents,
 * triggered children, slices), trades between resting legs, cancels resolved by outcomes or journal terminals,
 * quarantine of armed orders, pending-dispatch replays and sweeps. {@link SyntheticFuzzDriver} checks the
 * events each operation owes and, after EVERY operation, {@link LifecycleInvariants} plus the S6 record and
 * book invariants. Failure messages carry the seed and the operation index.
 */
public class SyntheticFuzzTest {

    private static final int OPS = 300;

    @Test
    public void seed1_syntheticInvariantsHoldAfterEveryOp() {
        new SyntheticFuzzDriver(1).run(OPS);
    }

    @Test
    public void seed2_syntheticInvariantsHoldAfterEveryOp() {
        new SyntheticFuzzDriver(2).run(OPS);
    }

    @Test
    public void seed3_syntheticInvariantsHoldAfterEveryOp() {
        new SyntheticFuzzDriver(3).run(OPS);
    }
}

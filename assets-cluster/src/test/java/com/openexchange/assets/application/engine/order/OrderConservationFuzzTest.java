// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import org.junit.Test;

/**
 * RC1 S5 (contract section 7, G4: "invariants I1-I15 as a LifecycleInvariants helper used by a seeded
 * ConservationFuzz with order verbs"; harness spec table I1-I13 plus the light form of I15). Five seeds, about
 * 300 legal operations each over three users and market 1: deposits, LIMIT creates on both sides (GTC, short
 * GTD, IOC), create outcomes (APPLIED with fresh legs, some REJECTED), trades between a resting BUY and a
 * resting SELL that both have legs (at most the smaller remaining, occasionally more to exercise OVERFILL),
 * cancels resolved by CANCEL outcomes or journal terminals (some refused), amends up and down resolved by
 * AMEND outcomes APPLIED or REJECTED, time advances with sweeps, plus replayed trades, re-delivered outcomes
 * and stale terminals. After EVERY operation {@link OrderFuzzDriver} runs {@link LifecycleInvariants}, compares
 * each live record with its model and checks that every emitted BALANCE matches the post-apply balance.
 * The failure message names the seed, the operation index and the operation.
 */
public class OrderConservationFuzzTest {

    private static final int OPS = 300;

    @Test
    public void seed1_lifecycleInvariantsHoldAfterEveryOp() {
        new OrderFuzzDriver(1).run(OPS);
    }

    @Test
    public void seed2_lifecycleInvariantsHoldAfterEveryOp() {
        new OrderFuzzDriver(2).run(OPS);
    }

    @Test
    public void seed3_lifecycleInvariantsHoldAfterEveryOp() {
        new OrderFuzzDriver(3).run(OPS);
    }

    @Test
    public void seed4_lifecycleInvariantsHoldAfterEveryOp() {
        new OrderFuzzDriver(4).run(OPS);
    }

    @Test
    public void seed5_lifecycleInvariantsHoldAfterEveryOp() {
        new OrderFuzzDriver(5).run(OPS);
    }
}

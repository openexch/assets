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

    // ---------------------------------------------------------------- RC2 S8 7 G10: behaviour-3 seeds

    /**
     * Behaviour 3 against the reference ME of {@link V3CoreFuzzDriver}, with releases, confirmations, LEG_LIVE and orphan
     * rounds, CAPACITY with resync re-offers, INVALID, RELEASED and leg-0 entries: the S8 invariant set (locked ==
     * sum(holds), the 5.1/5.2 rules, no live order with remainingQty 0, the R1 counter) after every apply, a snapshot round
     * trip at random steps, and once the backlog drains no ME ledger entry of a dead order that is not PARKED.
     */
    @Test
    public void v3SeedsHoldTheS8InvariantsAndDrainTheMeLedger() {
        long releases = 0, capacity = 0, invalid = 0, released = 0, legLive = 0, legZero = 0, roundTrips = 0, flagged = 0, leaks = 0;
        long parked = 0, orphans = 0, confirmed = 0;
        for (long seed = 101; seed <= 106; seed++) {
            final V3CoreFuzzDriver d = new V3CoreFuzzDriver(seed, true);
            d.run(400);
            d.drainAndCheckLedger();
            releases += d.releaseMints;
            capacity += d.capacityInjected;
            invalid += d.invalidInjected;
            released += d.releasedInjected;
            legLive += d.legLiveInjected;
            legZero += d.legZeroInjected;
            roundTrips += d.roundTrips;
            flagged += d.diedFlagged.size();
            leaks += d.flaggedLeaks();
            parked += d.r.engine.orders().parkedCount();
            orphans += d.r.engine.orders().orphanCancelCount();
            confirmed += d.r.engine.orders().releaseConfirmedCount();
        }
        System.out.println("[S8-FUZZ] seeds 101-106 x 400 steps: releases=" + releases + " confirmed=" + confirmed + " orphans="
                + orphans + " parked=" + parked + " capacity=" + capacity + " invalid=" + invalid + " released=" + released
                + " legLive=" + legLive + " legZero=" + legZero + " roundTrips=" + roundTrips + " diedFlagged=" + flagged
                + " flaggedLedgerLeaks=" + leaks);
        org.junit.Assert.assertEquals("review Q1 option (d): no flagged order dies owning an ME ledger entry", 0L, leaks);
        org.junit.Assert.assertTrue("the verbs were exercised: releases " + releases + " capacity " + capacity + " invalid "
                + invalid + " released " + released + " legLive " + legLive + " legZero " + legZero + " roundTrips "
                + roundTrips, releases > 100 && capacity > 0 && invalid > 0 && released > 0 && legLive > 0 && legZero > 0
                && roundTrips > 0);
    }
}

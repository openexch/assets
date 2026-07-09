// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Money-invariant unit tests for {@link Account}, focused on the HOLD top-up semantics that replaced the
 * old overwrite-on-duplicate behaviour (a real corruption bug: it leaked the first hold's residual and
 * broke {@code locked == Σ remaining}). The invariant checked throughout: on every accepted mutation
 * {@code available + locked} is conserved, and on every rejection <b>nothing</b> is mutated.
 */
public class AccountTest {

    private static final int USD = Asset.USD.id(); // 0
    private static final int BTC = Asset.BTC.id(); // 1

    @Test
    public void createThenTopUpAddsToTheSameHold() {
        Account a = new Account(1);
        assertEquals(RejectReason.NONE, a.deposit(USD, 1000));

        // Create.
        assertEquals(RejectReason.NONE, a.hold(1L, USD, 600));
        assertEquals(400, a.available(USD));
        assertEquals(600, a.locked(USD));
        assertEquals(600, a.holdRemaining(1L));

        // Top-up the SAME order: available -= 300, locked += 300, remaining += 300.
        assertEquals(RejectReason.NONE, a.hold(1L, USD, 300));
        assertEquals(100, a.available(USD));
        assertEquals(900, a.locked(USD));
        assertEquals(900, a.holdRemaining(1L));
        assertEquals(1, a.holdCount()); // still one hold, not two
    }

    @Test
    public void topUpInsufficientFundsRejectsWithNoPartialMutation() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        a.hold(1L, USD, 600); // avail=400, locked=600, remaining=600

        // Ask to top up by 500 but only 400 is available -> rejected, nothing changes.
        assertEquals(RejectReason.INSUFFICIENT_FUNDS, a.hold(1L, USD, 500));
        assertEquals(400, a.available(USD));
        assertEquals(600, a.locked(USD));
        assertEquals(600, a.holdRemaining(1L));
    }

    @Test
    public void topUpNonPositiveAmountRejectsWithNoMutation() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        a.hold(1L, USD, 600);

        assertEquals(RejectReason.INVALID_AMOUNT, a.hold(1L, USD, 0));
        assertEquals(RejectReason.INVALID_AMOUNT, a.hold(1L, USD, -50));
        assertEquals(400, a.available(USD));
        assertEquals(600, a.locked(USD));
        assertEquals(600, a.holdRemaining(1L));
    }

    @Test
    public void topUpWithMismatchedAssetRejectsInvalidAmountAndMutatesNothing() {
        Account a = new Account(2);
        a.deposit(USD, 1000);
        a.deposit(BTC, 100_000_000);
        assertEquals(RejectReason.NONE, a.hold(5L, USD, 500)); // order 5 reserves USD

        // A top-up of order 5 in a *different* asset is illegal: reject, mutate nothing anywhere.
        assertEquals(RejectReason.INVALID_AMOUNT, a.hold(5L, BTC, 100_000_000));
        assertEquals(500, a.available(USD));
        assertEquals(500, a.locked(USD));
        assertEquals(100_000_000, a.available(BTC));
        assertEquals(0, a.locked(BTC));
        assertEquals(USD, a.holdAssetId(5L));
        assertEquals(500, a.holdRemaining(5L));
    }

    @Test
    public void releaseAllAfterTopUpReturnsTheFullSum() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        a.hold(1L, USD, 600);
        a.hold(1L, USD, 300); // topped up to 900 reserved

        long released = a.release(1L, -1L); // release full residual
        assertEquals("full topped-up reservation is released", 900, released);
        assertEquals(1000, a.available(USD)); // fully restored
        assertEquals(0, a.locked(USD));
        assertFalse(a.hasHold(1L)); // hold recycled once emptied
    }

    @Test
    public void distinctOrdersRemainSeparateHolds() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        assertEquals(RejectReason.NONE, a.hold(1L, USD, 300));
        assertEquals(RejectReason.NONE, a.hold(2L, USD, 200)); // different order -> separate hold
        assertEquals(2, a.holdCount());
        assertEquals(300, a.holdRemaining(1L));
        assertEquals(200, a.holdRemaining(2L));
        assertEquals(500, a.available(USD));
        assertEquals(500, a.locked(USD)); // locked == Σ remaining
    }

    @Test
    public void topUpKeepsLockedEqualToSumOfRemainingAcrossOrders() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        a.hold(1L, USD, 300);
        a.hold(2L, USD, 200);
        a.hold(1L, USD, 100); // top up order 1 to 400
        assertEquals(400, a.holdRemaining(1L));
        assertEquals(200, a.holdRemaining(2L));
        assertEquals(600, a.locked(USD));               // 400 + 200
        assertEquals(1000 - 600, a.available(USD));      // conservation
        assertTrue(a.locked(USD) == a.holdRemaining(1L) + a.holdRemaining(2L));
    }
}

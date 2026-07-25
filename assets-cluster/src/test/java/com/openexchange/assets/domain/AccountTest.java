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
    public void settleDrawingHoldToExactlyZeroReapsTheHold() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        a.hold(1L, USD, 600); // avail=400, locked=600
        Account.SettleDebitResult out = new Account.SettleDebitResult();

        // Two settles exactly consume the hold (300 + 300).
        assertEquals(300, a.settleDebit(1L, USD, 300, out));
        assertFalse("partial draw must NOT reap", out.reapedExhaustedHold);
        assertTrue("residual hold must survive a partial draw", a.hasHold(1L));
        assertEquals(300, a.holdRemaining(1L));

        assertEquals(300, a.settleDebit(1L, USD, 300, out));
        assertTrue("exact exhaustion must reap", out.reapedExhaustedHold);
        assertFalse("reaping is bookkeeping, not a fault", out.faulted());
        assertFalse("exhausted hold must be gone, not a remaining=0 tombstone", a.hasHold(1L));
        assertEquals(0, a.holdCount());
        assertEquals(0, a.locked(USD));
        assertEquals(400, a.available(USD)); // settle debits locked only; available untouched
    }

    @Test
    public void releaseAfterExhaustionReapIsAnIdempotentNoOp() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        a.hold(1L, USD, 600);
        Account.SettleDebitResult out = new Account.SettleDebitResult();
        a.settleDebit(1L, USD, 600, out); // exhausts and reaps the hold

        // The terminal release that arrives later must be a no-op (nothing to release, no mutation).
        assertEquals("full-residual release after reap releases nothing", 0, a.release(1L, -1L));
        assertEquals(400, a.available(USD));
        assertEquals(0, a.locked(USD));
        assertFalse(a.hasHold(1L));
    }

    @Test
    public void holdAfterExhaustionReapCreatesAFreshHoldNotATopUp() {
        Account a = new Account(1);
        a.deposit(USD, 1000);
        a.deposit(BTC, 100_000_000);
        a.hold(1L, USD, 600);
        Account.SettleDebitResult out = new Account.SettleDebitResult();
        a.settleDebit(1L, USD, 600, out); // exhausts and reaps the hold

        // Re-holding the same orderId is a FRESH reservation: in a DIFFERENT asset it must be
        // ACCEPTED (against a lingering tombstone it would be rejected as a cross-asset top-up).
        assertEquals(RejectReason.NONE, a.hold(1L, BTC, 40_000_000));
        assertEquals(1, a.holdCount());
        assertEquals(BTC, a.holdAssetId(1L));
        assertEquals("fresh hold, not a top-up of stale state", 40_000_000, a.holdRemaining(1L));
        assertEquals(60_000_000, a.available(BTC));
        assertEquals(40_000_000, a.locked(BTC));
        assertEquals(400, a.available(USD)); // USD side untouched by the fresh BTC hold
        assertEquals(0, a.locked(USD));
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

    // ---- assets#18: a credit must never wrap a balance into negative ----

    @Test
    public void depositThatWouldOverflowIsRejectedAndMutatesNothing() {
        Account a = new Account(1);
        assertEquals(RejectReason.NONE, a.deposit(USD, Long.MAX_VALUE - 10));

        assertEquals("one unit past the ceiling must be refused",
                RejectReason.BALANCE_OVERFLOW, a.deposit(USD, 11));
        assertEquals("a rejected deposit mutates nothing", Long.MAX_VALUE - 10, a.available(USD));

        assertEquals("landing exactly on the ceiling is legal",
                RejectReason.NONE, a.deposit(USD, 10));
        assertEquals(Long.MAX_VALUE, a.available(USD));

        assertEquals(RejectReason.BALANCE_OVERFLOW, a.deposit(USD, 1));
        assertTrue("the balance must never go negative", a.available(USD) > 0);
    }

    @Test
    public void headroomCountsLockedNotJustAvailable() {
        Account a = new Account(1);
        a.deposit(USD, Long.MAX_VALUE);
        a.hold(1L, USD, 400); // moves available -> locked; the TOTAL is what is full

        assertEquals(Long.MAX_VALUE - 400, a.available(USD));
        assertEquals(400, a.locked(USD));
        assertEquals("locking funds does not create room to deposit more",
                RejectReason.BALANCE_OVERFLOW, a.deposit(USD, 1));
    }

    @Test
    public void internalMovesStayLegalAtTheCeiling() {
        Account a = new Account(1);
        a.deposit(USD, Long.MAX_VALUE);

        // hold / release / settleDebit conserve available+locked, so a full account can still trade.
        assertEquals(RejectReason.NONE, a.hold(1L, USD, 1000));
        assertEquals(1000, a.locked(USD));
        assertEquals(600, a.release(1L, 600));
        assertEquals(400, a.locked(USD));

        Account.SettleDebitResult out = new Account.SettleDebitResult();
        assertEquals(400, a.settleDebit(1L, USD, 400, out));
        assertFalse(out.faulted());
        assertEquals("conservation held at the ceiling", Long.MAX_VALUE - 400, a.available(USD));
    }

    @Test
    public void settleCreditReportsWhatItCouldNotCreditInsteadOfWrapping() {
        Account a = new Account(1);
        a.deposit(USD, Long.MAX_VALUE - 100);

        assertEquals("what fits is credited silently", 0, a.settleCredit(USD, 100));
        assertEquals(Long.MAX_VALUE, a.available(USD));

        // A settle cannot be rejected: the trade already happened. It credits what fits and reports
        // the rest, so the breach is visible instead of becoming a negative balance.
        assertEquals("the excess is reported, not absorbed", 250, a.settleCredit(USD, 250));
        assertEquals("balance is clamped, never wrapped", Long.MAX_VALUE, a.available(USD));
    }

    @Test
    public void settlementServiceSurfacesAnOverflowingCreditAsALegFault() {
        Account buyer = new Account(1);
        Account seller = new Account(2);
        buyer.deposit(USD, 1000);
        buyer.hold(10L, USD, 1000);
        seller.deposit(BTC, 5);
        seller.hold(20L, BTC, 5);
        seller.deposit(USD, Long.MAX_VALUE); // seller cannot receive another unit of USD

        SettlementService s = new SettlementService();
        s.settle(buyer, seller, 10L, 20L, BTC, USD, 5, 1000);

        assertEquals("the buyer paid in full", 0, buyer.locked(USD));
        assertEquals("the seller could not receive any of it", 1000, s.buyerLeg().uncredited);
        assertTrue("an uncreditable leg is a fault", s.buyerLeg().faulted());
        assertFalse("but not a DEBIT fault — the payer was fine", s.buyerLeg().debitFaulted());
        assertEquals("seller's USD is clamped, not wrapped", Long.MAX_VALUE, seller.available(USD));

        // The base leg was ordinary.
        assertEquals(0, s.sellerLeg().uncredited);
        assertEquals(5, buyer.available(BTC));
    }
}

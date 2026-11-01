// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

/**
 * RC2 S9h 5.2 (review FIX-1): a test-local copy of the five 3.8 entry digests of {@code AssetsEngine} that takes the raw
 * {@code int} of every enum byte instead of the production boolean / enum, so an out-of-range value (a takerIsBuy of 7,
 * an oldCancelled of 2, a decision of 200) is digested as itself instead of collapsing or throwing. On every in-range
 * value it equals the production functions ({@code rawDigestsEqualProductionInRange} pins that).
 */
public final class RawDigests {

    private static final long K_TRADE = 0x61L;
    private static final long K_TERMINAL = 0x63L;
    private static final long K_OUTCOME = 0x64L;
    private static final long K_RELEASE = 0x67L;
    private static final long K_CONFIG = 0x68L;

    private RawDigests() {
    }

    public static long trade(long egressSeq, long journalSeq, long tradeId, int marketId, long takerLegId, long takerUserId,
                             long makerLegId, long makerUserId, long price, long quantity, int takerIsBuyRaw,
                             long meTimestamp, long takerOmsOrderId, long makerOmsOrderId, long meEpoch) {
        long h = d(K_TRADE, egressSeq);
        h = d(h, journalSeq);
        h = d(h, tradeId);
        h = d(h, marketId);
        h = d(h, takerLegId);
        h = d(h, takerUserId);
        h = d(h, makerLegId);
        h = d(h, makerUserId);
        h = d(h, price);
        if (!FeedHarnessSeams.on(FeedHarnessSeams.RAWDIGEST_DROP_FIELD)) {
            h = d(h, quantity);
        }
        h = d(h, takerIsBuyRaw);
        h = d(h, meTimestamp);
        h = d(h, takerOmsOrderId);
        h = d(h, makerOmsOrderId);
        return d(h, meEpoch);
    }

    public static long terminal(long egressSeq, long journalSeq, long legId, long userId, int marketId, int statusRaw,
                                int rejectReasonRaw, long meTimestamp, long omsOrderId, long meEpoch) {
        long h = d(K_TERMINAL, egressSeq);
        h = d(h, journalSeq);
        h = d(h, legId);
        h = d(h, userId);
        h = d(h, marketId);
        h = d(h, statusRaw);
        h = d(h, rejectReasonRaw);
        h = d(h, meTimestamp);
        h = d(h, omsOrderId);
        h = d(h, meEpoch);
        return d(h, 0);
    }

    /** The widened 64 digest, in the production field order, with the raw {@code oldCancelled} byte. */
    public static long outcome(long egressSeq, long journalSeq, long commandIdHigh, long commandIdLow, long userId,
                               long omsOrderId, long oldOrderId, long price, long quantity, long budget, int marketId,
                               int commandKind, int result, long orderId, int reason, int oldCancelledRaw, long meTimestamp,
                               long meEpoch, int orderType, int orderSide, long appliedPosition, int status, int timeInForce,
                               long expectedOldRemaining, long ledgerSize, long ledgerCapacity, long behaviorVersion) {
        long h = d(K_OUTCOME, egressSeq);
        h = d(h, journalSeq);
        h = d(h, commandIdHigh);
        h = d(h, commandIdLow);
        h = d(h, userId);
        h = d(h, omsOrderId);
        h = d(h, oldOrderId);
        h = d(h, price);
        h = d(h, quantity);
        h = d(h, budget);
        h = d(h, marketId);
        h = d(h, commandKind);
        h = d(h, result);
        h = d(h, orderId);
        h = d(h, reason);
        h = d(h, oldCancelledRaw);
        h = d(h, meTimestamp);
        h = d(h, meEpoch);
        h = d(h, orderType);
        h = d(h, orderSide);
        h = d(h, appliedPosition);
        h = d(h, status);
        h = d(h, timeInForce);
        h = d(h, expectedOldRemaining);
        h = d(h, ledgerSize);
        h = d(h, ledgerCapacity);
        return d(h, behaviorVersion);
    }

    /** The widened 67 digest; entries as parallel arrays of length {@code count}. */
    public static long release(long egressSeq, long journalSeq, long meEpoch, long releaseSeq, long ledgerSize,
                               long ledgerCapacity, long tombstoneRanges, int count, long[] commandIdHigh, long[] idLowFloor,
                               int[] scopeRaw, int[] resultRaw, int[] removed) {
        long h = d(K_RELEASE, egressSeq);
        h = d(h, journalSeq);
        h = d(h, meEpoch);
        h = d(h, releaseSeq);
        h = d(h, ledgerSize);
        h = d(h, ledgerCapacity);
        h = d(h, tombstoneRanges);
        h = d(h, count);
        for (int i = 0; i < count; i++) {
            h = d(h, commandIdHigh[i]);
            h = d(h, idLowFloor[i]);
            h = d(h, scopeRaw[i]);
            h = d(h, resultRaw[i]);
            h = d(h, removed[i]);
        }
        return h;
    }

    public static long config(long egressSeq, long journalSeq, long meEpoch, long configVersion, long behaviorVersion,
                              long ledgerCapacity, int maxWireVersion, int decisionRaw) {
        long h = d(K_CONFIG, egressSeq);
        h = d(h, journalSeq);
        h = d(h, meEpoch);
        h = d(h, configVersion);
        h = d(h, behaviorVersion);
        h = d(h, ledgerCapacity);
        h = d(h, maxWireVersion);
        h = d(h, decisionRaw);
        return d(h, 0);
    }

    /** The 3.8 mixer, copied from {@code AssetsEngine.digest} (package-private there). */
    static long d(final long h, final long v) {
        long x = (h ^ 0x9E3779B97F4A7C15L) * 0xBF58476D1CE4E5B9L + v;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        x ^= x >>> 31;
        return x == 0 ? 1 : x;
    }
}

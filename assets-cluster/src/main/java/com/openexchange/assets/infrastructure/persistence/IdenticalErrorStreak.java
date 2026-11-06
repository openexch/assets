// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

/**
 * Hot-loop backstop for the Aeron error handler (RC1 S1, port of the ME's match#35 fence). The
 * 2026-07-02 and 2026-07-04 ME incidents each threw the IDENTICAL exception thousands of times
 * ("replayPosition ... does not point to a valid frame") while the node sat wedged forever. No healthy
 * scenario repeats the exact same error this many times in a row; the operator (and the process
 * manager's crash accounting) must see a broken node instead of a silent spinner.
 *
 * <p>Signature = {@code String.valueOf(throwable)} (class + message), exactly what stayed byte-identical
 * across the replay-loop incidents. Any different error resets the streak. Trips exactly once.</p>
 *
 * <p>Thread-safe through a single monitor: the error handler can be invoked from any Aeron agent thread,
 * and correctness here is "trip once at the threshold", not throughput.</p>
 */
final class IdenticalErrorStreak {

    private final int threshold;
    private String lastSignature = "";
    private long count;
    private boolean tripped;

    IdenticalErrorStreak(final int threshold) {
        this.threshold = threshold;
    }

    /** @return true exactly once, on the {@code threshold}-th consecutive identical signature. */
    synchronized boolean record(final String signature) {
        if (signature.equals(lastSignature)) {
            count++;
        } else {
            lastSignature = signature;
            count = 1;
        }
        if (!tripped && count >= threshold) {
            tripped = true;
            return true;
        }
        return false;
    }

    /** Consecutive identical errors seen so far (diagnostic). */
    synchronized long count() {
        return count;
    }
}

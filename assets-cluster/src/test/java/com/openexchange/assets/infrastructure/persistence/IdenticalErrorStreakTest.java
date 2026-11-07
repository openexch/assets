// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S1 (critic G11 / hardening M5): the ME's hot-loop backstop ported to the AE launcher. A node that
 * throws the IDENTICAL Aeron error 200 times in a row is not healthy, it is wedged; the error handler
 * must trip exactly once at the threshold, and any different error resets the streak.
 */
public class IdenticalErrorStreakTest {

    @Test
    public void tripsOnceAtTheThresholdAndResetsOnADifferentError() {
        final IdenticalErrorStreak streak = new IdenticalErrorStreak(200);
        for (int i = 1; i < 200; i++) {
            assertFalse("below threshold: " + i, streak.record("java.lang.IllegalStateException: replayPosition"));
        }
        assertTrue("the 200th identical error trips", streak.record("java.lang.IllegalStateException: replayPosition"));
        assertFalse("tripped once, never again", streak.record("java.lang.IllegalStateException: replayPosition"));
        assertEquals(201, streak.count());
        final IdenticalErrorStreak fresh = new IdenticalErrorStreak(3);
        assertFalse(fresh.record("a"));
        assertFalse(fresh.record("a"));
        assertFalse("a different error resets the streak", fresh.record("b"));
        assertEquals(1, fresh.count());
        assertFalse(fresh.record("b"));
        assertTrue(fresh.record("b"));
    }
}

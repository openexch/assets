// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The open-loop timeline: slot k is due at {@code start + k / rate} whatever happened before it. After an injected
 * 500 ms stall the late slots keep their scheduled times (the stall shows in their latency) and the backlog counts the
 * slots owed. Control: {@code -Dcore.loadgen.nc=SCHEDULE_RESET_ON_LAG} re-anchors the timeline at the late send and
 * must fail with {@code a stall was hidden: t0 moved by <n> ms}.
 */
public class OpenLoopScheduleTest {

    private static final long MS = 1_000_000L;

    @Test
    public void theScheduleNeverResetsAfterAStall() {
        final boolean control = "SCHEDULE_RESET_ON_LAG".equals(System.getProperty("core.loadgen.nc", ""));
        final long start = 1_000_000_000L;
        final OpenLoopSchedule s = new OpenLoopSchedule(start, 1_000, control);
        for (int k = 0; k < 10; k++) {
            final long now = start + k * MS;
            assertTrue("slot " + k + " due at its time", s.due(now));
            assertEquals(k, s.take(now));
            assertFalse("slot " + (k + 1) + " not due before its time", s.due(now));
        }
        // a 500 ms stall: the sender comes back late
        final long late = s.t0(10) + 500 * MS;
        assertEquals("slots owed after the stall", 500, s.backlog(late));
        assertEquals(10, s.take(late));
        for (long k = 10; k < 600; k++) {
            final long expected = start + k * MS;
            if (s.t0(k) != expected) {
                throw new AssertionError("a stall was hidden: t0 moved by " + (s.t0(k) - expected) / MS + " ms (slot "
                        + k + ")");
            }
        }
        // the owed slots are due at once and keep their times
        assertTrue(s.due(late));
        assertEquals(start + 11 * MS, s.t0(s.nextSlot()));
        assertEquals(500, s.maxBacklog());
    }

    @Test
    public void slotTimesHaveNoAccumulatedRounding() {
        // 3 slots/s: a per-slot step of 333 333 333 ns would drift 1 ns per 3 slots
        final OpenLoopSchedule s = new OpenLoopSchedule(0, 3);
        assertEquals(1_000_000_000L, s.t0(3));
        assertEquals(1_000_000_000_000L, s.t0(3_000));
        assertEquals(333_333_333L, s.t0(1));
        assertEquals("rate must be at least 1 slot/s: 0", assertThrows(IllegalArgumentException.class,
                () -> new OpenLoopSchedule(0, 0)).getMessage());
    }
}

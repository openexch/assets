// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Modifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S10' 4R.2 RC-5 (S2-6, C6): the eight required core negative controls, plus review FIX-1's CORE_GAP_SUPPRESSED and the
 * SETTLE_REFUSED case of AUTO_REOPEN_ANY_REASON (NOTE-9). Each switches ONE {@link FeedSeams} seam on,
 * runs the scenario that proves the rule, and requires it to FAIL on the rule's named text (an AssertionError whose
 * message carries it; a crash or another text is not a RED). {@link #feedSeamsAllOffByDefault} pins the default; surefire
 * runs every test class in a fresh JVM ({@code reuseForks=false}), so that test sees a fresh class load.
 */
public class FeedSeamsTest {

    @After
    public void allOff() {
        for (final FeedSeams s : FeedSeams.values()) {
            s.on = false;
        }
    }

    @Test
    public void feedSeamsAllOffByDefault() {
        for (final FeedSeams s : FeedSeams.values()) {
            assertFalse("seam " + s + " is off on a fresh class load", s.on);
        }
        assertEquals("the eight seams of 4R.2 RC-5 and review FIX-1's", 9, FeedSeams.values().length);
        assertFalse("the enum is package-private", Modifier.isPublic(FeedSeams.class.getModifiers()));
        assertFalse("the switch is package-private", Modifier.isPublic(fieldModifiers()));
    }

    private static int fieldModifiers() {
        try {
            return FeedSeams.class.getDeclaredField("on").getModifiers();
        } catch (final NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    /** Run {@code scenario} with {@code seam} on; it must fail on {@code text}. Prints the RED line for the record. */
    static void red(final FeedSeams seam, final String text, final Runnable scenario) {
        seam.on = true;
        try {
            scenario.run();
        } catch (final AssertionError e) {
            final String m = String.valueOf(e.getMessage());
            assertTrue(seam + ": failed, but not on \"" + text + "\": " + m, m.contains(text));
            System.out.println("[RED] " + seam + " -> " + m.replace('\n', ' '));
            return;
        } finally {
            seam.on = false;
        }
        fail(seam + ": the scenario PASSED with the seam on (INCONCLUSIVE: the rule is not what makes it pass)");
    }

    @Test
    public void autoReopenClearsAllIsRed() {
        red(FeedSeams.AUTO_REOPEN_CLEARS_ALL, "state OPEN while ADMIN holds", AutoReopenScenarios::operatorSuspendSurvivesTheReopen);
    }

    @Test
    public void autoReopenSingleSlotIsRed() {
        red(FeedSeams.AUTO_REOPEN_SINGLE_SLOT, "OPEN with lane 2's record open", AutoReopenScenarios::twoLanesBothMustClear);
    }

    @Test
    public void laneTableNotSnapshottedIsRed() {
        seamOnEitherText(FeedSeams.LANE_TABLE_NOT_SNAPSHOTTED, AutoReopenScenarios::snapshotMidHaltRestoreFillBoth,
                "after restore: reopened with a record open", "never reopened after both fills");
    }

    @Test
    public void overflowReadsClearedIsRed() {
        red(FeedSeams.OVERFLOW_READS_CLEARED, "OPEN with the overflow record set", AutoReopenScenarios::ninthLaneOverflowNeedsTheOperator);
    }

    @Test
    public void autoReopenAnyReasonIsRed() {
        red(FeedSeams.AUTO_REOPEN_ANY_REASON, "OPEN after a SOURCE_SCHEMA reach-clear",
                AutoReopenScenarios::sourceSchemaReachClearKeepsTheBit);
    }

    @Test
    public void autoReopenOverAwaitingOpenIsRed() {
        red(FeedSeams.AUTO_REOPEN_OVER_AWAITING_OPEN, "OPEN after a SOURCE_SCHEMA skip that awaited OPEN",
                AutoReopenScenarios::nonGapAwaitingOpenSurvivesAGapReach);
    }

    @Test
    public void coreGapSuppressedIsRed() {
        red(FeedSeams.CORE_GAP_SUPPRESSED, "OPEN below a hole the core refused", AutoReopenScenarios::aLaterCoreDetectedHoleKeepsTheGateClosed);
    }

    @Test
    public void autoReopenAnyReasonOnSettleRefusedIsRed() {
        red(FeedSeams.AUTO_REOPEN_ANY_REASON, "OPEN after a SETTLE_REFUSED reach-clear",
                AutoReopenScenarios::settleRefusedReachClearKeepsTheBit);
    }

    @Test
    public void replaceTakesLastSeenIsRed() {
        red(FeedSeams.REPLACE_TAKES_LAST_SEEN, "reopened with the first lane's hole open",
                AutoReopenScenarios::twoSessionsOneIdentityNeverWeakenTheRecord);
    }

    @Test
    public void unknownSectionVersionAcceptedIsRed() {
        red(FeedSeams.UNKNOWN_SECTION_VERSION_ACCEPTED, "restore accepted an unknown halt section version", () -> {
            try {
                LaneHaltTableTest.restoreWithVersionPlusOne();
            } catch (final IllegalStateException refused) {
                assertTrue(refused.getMessage(), refused.getMessage().contains("section 12"));
                return; // the rule held: refused, naming the section
            }
            fail("restore accepted an unknown halt section version");
        });
    }

    private static void seamOnEitherText(final FeedSeams seam, final Runnable scenario, final String a, final String b) {
        seam.on = true;
        try {
            scenario.run();
        } catch (final AssertionError e) {
            final String m = String.valueOf(e.getMessage());
            assertTrue(seam + ": failed, but not on \"" + a + "\" or \"" + b + "\": " + m, m.contains(a) || m.contains(b));
            System.out.println("[RED] " + seam + " -> " + m.replace('\n', ' '));
            return;
        } finally {
            seam.on = false;
        }
        fail(seam + ": the scenario PASSED with the seam on");
    }
}

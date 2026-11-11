// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.persistence.SnapshotModeSettings;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.cluster.shadow.ShadowFloor;
import com.openexchange.cluster.testkit.RecordedLog;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Core half (the kit half is {@code ShadowFloorPrunerTest.tailReplayNeedsRetainedLogKitHalf}):
 * on a real single member in shadow mode, with the LIVE housekeeping and pruner (no test-side purge). Shadows at
 * P_1 &lt; P_2 &lt; P_cm with the margin on purge nothing (no covering shadow); once P_3 = P_cm is VERIFIED from two
 * producers (the default anchor rule's two witnesses) the floor
 * moves to P_2 and the pruner purges the log below P_2 on its own; then a CONFLICT at P_3 takes EQUAL away and the
 * restart restores TAIL from P_2 with an image equal to the pre-crash one. Control: {@code SHADOW_FLOOR_MARGIN=none}
 * (the floor taken from the anchor instead of the margin) purges past P_2: {@code tail start <p2> below log start
 * <s>: TAIL impossible}.
 */
public class TailReplayNeedsRetainedLogTest {

    private static SnapshotModeSettings settings(final String margin) {
        return SnapshotModeSettings.with(Map.of(SnapshotModeSettings.Key.SNAPSHOT_MODE, "shadow",
                SnapshotModeSettings.Key.SHADOW_FLOOR_MARGIN, margin,
                SnapshotModeSettings.Key.SHADOW_SCAN_PERIOD_MS, "500",
                SnapshotModeSettings.Key.SHADOW_DEEP_READ_PACE_MS, "0"));
    }

    private static void assertTailBaseRetained(final StubStory s, final long p2) {
        final long start = s.rig.logStart(s.m);
        if (start > p2) {
            throw new AssertionError("tail start " + p2 + " below log start " + start + ": TAIL impossible");
        }
    }

    private static ShadowFloor.Evaluation awaitFloor(final StubStory s, final ShadowFloor.Evaluation.Kind kind,
                                                     final long floorAt, final int restorable, final String why) {
        final ShadowFloor floor = ServiceProbe.shadowFloor(s.m.service);
        assertNotNull("the floor is wired", floor);
        final ShadowFloor.Evaluation[] last = new ShadowFloor.Evaluation[1];
        s.rig.await(() -> {
            last[0] = floor.lastEvaluation();
            return last[0] != null && last[0].kind() == kind && last[0].floor() == floorAt
                    && last[0].restorable().size() >= restorable;
        }, 60_000, () -> why + ": last purge pass " + last[0]);
        return last[0];
    }

    /** P_1, P_2, then P_3 = P_cm; returns {p1, p2, pCm} after the pruner applied the floor P_2. */
    private static long[] story(final StubStory s, final String margin) {
        s.deposits(2400);
        final long pCm = s.snapshot();
        s.deposits(120);
        final RecordedLog log = s.log();
        final int iCm = StubStory.indexOf(log, pCm);
        // P_1 in the first segment, P_2 the last frame end below P_cm's segment: the floor P_2 purges a segment above
        // P_1 and keeps P_2; the floor P_cm (the control) purges past P_2
        final long cmSegment = pCm / StubStory.SEGMENT * StubStory.SEGMENT;
        assertTrue("P_cm " + pCm + " lies in the third segment or later", cmSegment >= 2L * StubStory.SEGMENT);
        long a1 = -1;
        long a2 = -1;
        for (final long q : log.positions()) {
            if (q < StubStory.SEGMENT) {
                a1 = q;
            }
            if (q < cmSegment) {
                a2 = q;
            }
        }
        final long p1 = a1;
        final long p2 = a2;
        assertTrue("P_1 " + p1 + " < P_2 " + p2 + " past the first segment", p1 > 0 && p2 >= StubStory.SEGMENT);

        s.produce(log, p1);
        s.produce(log, p2);
        final ShadowFloor.Evaluation before = awaitFloor(s, ShadowFloor.Evaluation.Kind.NO_COVERING, 0, 2,
                "P_1 and P_2 VERIFIED, no covering shadow");
        assertEquals(before.toString(), 2, before.restorable().size());
        if (s.rig.logStart(s.m) > p1) {
            throw new AssertionError("purged below P_1 before any covering shadow: log start " + s.rig.logStart(s.m));
        }

        // P_3 = P_cm from two producers of one digest: the anchor rule's two witnesses
        s.produce(log, pCm);
        s.produce(log, pCm);
        final long expected = "none".equals(margin) ? pCm : p2;
        final ShadowFloor.Evaluation moved = awaitFloor(s, ShadowFloor.Evaluation.Kind.FLOOR, expected, 3,
                "P_3 = P_cm VERIFIED: the floor moves to " + expected);
        assertEquals(moved.toString(), pCm, moved.anchorPosition());
        s.rig.await(() -> s.rig.logStart(s.m) > p1, 30_000,
                () -> "the pruner did not purge below the floor " + expected + ": log start " + s.rig.logStart(s.m));
        System.out.println("[11.8] margin=" + margin + " P_1=" + p1 + " P_2=" + p2 + " P_cm=" + pCm + " floor "
                + moved + " log start " + s.rig.logStart(s.m));

        // EQUAL lost: a second copy at P_3 with different content
        assertTrue(s.producer().produceDifferentContent(s.who(), log, pCm, log.positions()[iCm + 2]).ok());
        return new long[] {p1, p2, pCm};
    }

    @Test
    public void afterTheFloorMovesToP2ARestartRestoresTailFromP2() throws Exception {
        try (StubStory s = new StubStory(27600, "rcs-s4-11.8-tail", settings("previous-verified")).start(true)) {
            final long[] p = story(s, "previous-verified");
            final long p2 = p[1];
            assertTailBaseRetained(s, p2);
            final long start = s.rig.logStart(s.m);
            assertTrue("the log below P_2 went: start " + start, start > p[0] && start <= p2);
            final byte[] pre = s.quiesce();
            s.kill();
            s.restart();
            s.awaitReady();
            s.awaitImage(pre, "restored image after TAIL from P_2");
            final String line = s.pathLine();
            assertTrue("TAIL from P_2: " + line + "; stderr " + s.rig.errTee.lines,
                    line != null && line.contains("restored path=TAIL P_s=" + p2 + " "));
            assertEquals("core_restore_conflict_total{EQUAL}", 1, ServiceProbe.conflicts(s.m.service, "EQUAL"));
            assertTrue(s.m.halts.toString(), s.m.halts.isEmpty());
            System.out.println("GREEN 11.8 core: floor moved to P_2=" + p2 + ", log start " + start + ", " + line);
        }
    }

    @Test
    public void controlTheFloorFromTheAnchorPurgesPastP2() throws Exception {
        try (StubStory s = new StubStory(27700, "rcs-s4-11.8-anchor", settings("none")).start(true)) {
            final long[] p = story(s, "none");
            final long p2 = p[1];
            s.rig.await(() -> s.rig.logStart(s.m) > p2, 30_000,
                    () -> "margin none did not purge past P_2: log start " + s.rig.logStart(s.m));
            RestoreDrillDryRun.nc("11.8-floor-from-anchor", "tail start " + p2 + " below log start ",
                    () -> assertTailBaseRetained(s, p2));
        }
    }
}

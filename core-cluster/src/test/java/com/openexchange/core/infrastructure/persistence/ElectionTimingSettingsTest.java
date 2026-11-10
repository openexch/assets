// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Defaults, each bound at min-1 / min / max / max+1, a non-integer, and the timeout-to-interval rule. */
public class ElectionTimingSettingsTest {

    private static final ElectionTimingSettings.Key INTERVAL = ElectionTimingSettings.Key.CORE_LEADER_HEARTBEAT_INTERVAL_MS;
    private static final ElectionTimingSettings.Key TIMEOUT = ElectionTimingSettings.Key.CORE_LEADER_HEARTBEAT_TIMEOUT_MS;
    private static final ElectionTimingSettings.Key ELECTION = ElectionTimingSettings.Key.CORE_ELECTION_TIMEOUT_MS;

    @Test
    public void defaultsAreTheValuesTheClusterRanWithBefore() {
        final ElectionTimingSettings s = ElectionTimingSettings.with(Map.of());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(100), s.heartbeatIntervalNs());
        assertEquals(TimeUnit.SECONDS.toNanos(1), s.heartbeatTimeoutNs());
        assertEquals(TimeUnit.SECONDS.toNanos(1), s.electionTimeoutNs());
    }

    @Test
    public void overridesAreApplied() {
        final ElectionTimingSettings s = ElectionTimingSettings.with(Map.of(
                INTERVAL, "200", TIMEOUT, "3000", ELECTION, " 2500 "));
        assertEquals(TimeUnit.MILLISECONDS.toNanos(200), s.heartbeatIntervalNs());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(3000), s.heartbeatTimeoutNs());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(2500), s.electionTimeoutNs());
    }

    @Test
    public void everyBoundIsEnforcedWithANamedText() {
        refused(Map.of(INTERVAL, "9"), "CORE_LEADER_HEARTBEAT_INTERVAL_MS=9 out of range [10,600000]");
        ElectionTimingSettings.with(Map.of(INTERVAL, "10"));
        refused(Map.of(INTERVAL, "600001"), "CORE_LEADER_HEARTBEAT_INTERVAL_MS=600001 out of range [10,600000]");
        refused(Map.of(TIMEOUT, "99"), "CORE_LEADER_HEARTBEAT_TIMEOUT_MS=99 out of range [100,600000]");
        ElectionTimingSettings.with(Map.of(TIMEOUT, "600000"));
        refused(Map.of(TIMEOUT, "600001"), "CORE_LEADER_HEARTBEAT_TIMEOUT_MS=600001 out of range [100,600000]");
        refused(Map.of(ELECTION, "99"), "CORE_ELECTION_TIMEOUT_MS=99 out of range [100,600000]");
        ElectionTimingSettings.with(Map.of(ELECTION, "100"));
        ElectionTimingSettings.with(Map.of(ELECTION, "600000"));
        refused(Map.of(ELECTION, "600001"), "CORE_ELECTION_TIMEOUT_MS=600001 out of range [100,600000]");
        refused(Map.of(ELECTION, "1s"), "CORE_ELECTION_TIMEOUT_MS='1s' is not an integer");
    }

    @Test
    public void aTimeoutShorterThanThreeIntervalsIsRefused() {
        refused(Map.of(INTERVAL, "400", TIMEOUT, "1000"),
                "CORE_LEADER_HEARTBEAT_TIMEOUT_MS=1000 must be at least 3 x CORE_LEADER_HEARTBEAT_INTERVAL_MS (400)");
        ElectionTimingSettings.with(Map.of(INTERVAL, "400", TIMEOUT, "1200"));
    }

    @Test
    public void theMemberReadsTheSettingsInsteadOfConstants() throws Exception {
        final String src = Files.readString(Path.of(
                "src/main/java/com/openexchange/core/infrastructure/persistence/AeronCluster.java"));
        assertTrue(src.contains(".leaderHeartbeatIntervalNs(electionTiming.heartbeatIntervalNs())"));
        assertTrue(src.contains(".leaderHeartbeatTimeoutNs(electionTiming.heartbeatTimeoutNs())"));
        assertTrue(src.contains(".electionTimeoutNs(electionTiming.electionTimeoutNs())"));
    }

    private static void refused(final Map<ElectionTimingSettings.Key, String> values, final String text) {
        final IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> ElectionTimingSettings.with(values));
        assertEquals(text, e.getMessage());
    }
}

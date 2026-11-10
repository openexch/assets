// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.shadow.ShadowFloor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * One row per numeric setting at min-1, min, max, max+1 and a non-integer, each
 * refused with the 3.5 text in {@code off} AND {@code shadow} mode; the enumerated settings; the defaults; and the boot
 * guard's guard row 1 / guard row 2 / guard row OK rows (4.1).
 */
public class SnapshotModeSettingsTest {

    private record Range(SnapshotModeSettings.Key key, long min, long max) {
    }

    private static final Range[] RANGES = {
            new Range(SnapshotModeSettings.Key.SHADOW_RETAIN_N, 1, 1000),
            new Range(SnapshotModeSettings.Key.SHADOW_DEEP_READ_PACE_MS, 0, 3_600_000L),
            new Range(SnapshotModeSettings.Key.SHADOW_SCAN_PERIOD_MS, 100, 3_600_000L),
            new Range(SnapshotModeSettings.Key.SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS, 1000, 86_400_000L),
            new Range(SnapshotModeSettings.Key.SNAPSHOT_RESTORE_READY_BUDGET_MS, 1000, 86_400_000L),
            new Range(SnapshotModeSettings.Key.CONSENSUS_SNAPSHOT_MAX_BYTES, 4096, 1_073_741_824L),
            new Range(SnapshotModeSettings.Key.SHADOW_LOAD_TIMEOUT_MS, 1000, 86_400_000L),
    };

    private static SnapshotModeSettings parse(final String mode, final SnapshotModeSettings.Key key, final String value) {
        final Map<SnapshotModeSettings.Key, String> m = new HashMap<>();
        m.put(SnapshotModeSettings.Key.SNAPSHOT_MODE, mode);
        m.put(key, value);
        return SnapshotModeSettings.with(m);
    }

    private static long read(final SnapshotModeSettings s, final SnapshotModeSettings.Key key) {
        return switch (key) {
            case SHADOW_RETAIN_N -> s.retainN();
            case SHADOW_DEEP_READ_PACE_MS -> s.deepReadPaceMs();
            case SHADOW_SCAN_PERIOD_MS -> s.scanPeriodMs();
            case SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS -> s.tailReplayTimeoutMs();
            case SNAPSHOT_RESTORE_READY_BUDGET_MS -> s.readyBudgetMs();
            case CONSENSUS_SNAPSHOT_MAX_BYTES -> s.consensusSnapshotMaxBytes();
            case SHADOW_LOAD_TIMEOUT_MS -> s.loadTimeoutMs();
            default -> throw new IllegalArgumentException(key.name());
        };
    }

    @Test
    public void everyNumericSettingAtItsBoundariesInBothModes() {
        for (final String mode : new String[] {"off", "shadow"}) {
            for (final Range r : RANGES) {
                final String range = " out of range [" + r.min() + "," + r.max() + "]";
                assertEquals(mode + " " + r.key() + " min", r.min(), read(parse(mode, r.key(), Long.toString(r.min())),
                        r.key()));
                assertEquals(mode + " " + r.key() + " max", r.max(), read(parse(mode, r.key(), Long.toString(r.max())),
                        r.key()));
                assertEquals(r.key().name() + "=" + (r.min() - 1) + range, assertThrows(IllegalArgumentException.class,
                        () -> parse(mode, r.key(), Long.toString(r.min() - 1))).getMessage());
                assertEquals(r.key().name() + "=" + (r.max() + 1) + range, assertThrows(IllegalArgumentException.class,
                        () -> parse(mode, r.key(), Long.toString(r.max() + 1))).getMessage());
                assertEquals(r.key().name() + "='5s' is not an integer", assertThrows(IllegalArgumentException.class,
                        () -> parse(mode, r.key(), "5s")).getMessage());
            }
        }
    }

    @Test
    public void enumeratedSettingsAndDefaults() {
        final SnapshotModeSettings d = SnapshotModeSettings.defaults();
        assertEquals(SnapshotMode.OFF, d.mode());
        assertEquals(3, d.retainN());
        assertEquals(ShadowFloor.Margin.PREVIOUS_VERIFIED, d.floorMargin());
        assertEquals(2000, d.deepReadPaceMs());
        assertEquals(5000, d.scanPeriodMs());
        assertEquals(60_000, d.tailReplayTimeoutMs());
        assertEquals(30_000, d.readyBudgetMs());
        assertEquals(1_048_576, d.consensusSnapshotMaxBytes());
        assertEquals(60_000, d.loadTimeoutMs());
        assertTrue(d.floorEnabled());

        assertEquals(SnapshotMode.SHADOW, SnapshotModeSettings.with(Map.of(SnapshotModeSettings.Key.SNAPSHOT_MODE, "shadow")).mode());
        assertEquals("SNAPSHOT_MODE 'bogus' (expected off|shadow)", assertThrows(IllegalArgumentException.class,
                () -> SnapshotModeSettings.with(Map.of(SnapshotModeSettings.Key.SNAPSHOT_MODE, "bogus"))).getMessage());
        for (final String mode : new String[] {"off", "shadow"}) {
            assertEquals(ShadowFloor.Margin.NONE, parse(mode, SnapshotModeSettings.Key.SHADOW_FLOOR_MARGIN, "none").floorMargin());
            assertEquals("SHADOW_FLOOR_MARGIN 'last' (expected previous-verified|none)",
                    assertThrows(IllegalArgumentException.class,
                            () -> parse(mode, SnapshotModeSettings.Key.SHADOW_FLOOR_MARGIN, "last")).getMessage());
            assertEquals(false, parse(mode, SnapshotModeSettings.Key.SHADOW_FLOOR_ENABLED, "false").floorEnabled());
            assertEquals("SHADOW_FLOOR_ENABLED 'no' (expected true|false)", assertThrows(IllegalArgumentException.class,
                    () -> parse(mode, SnapshotModeSettings.Key.SHADOW_FLOOR_ENABLED, "no")).getMessage());
        }
    }

    @Test
    public void bootGuardRows() {
        // guard row 1: a bad setting is refused with the guard prefix and the 3.5 text, in every mode
        for (final String mode : new String[] {"off", "shadow"}) {
            final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                    () -> StubModeBootGuard.enforce(source(mode, "SHADOW_RETAIN_N", "0", null), true));
            assertEquals("CORE BOOT GUARD: SHADOW_RETAIN_N=0 out of range [1,1000]", r.getMessage());
        }
        assertEquals("CORE BOOT GUARD: SNAPSHOT_MODE 'bogus' (expected off|shadow)", assertThrows(
                StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforce(source("bogus", null, null,
                        null), true)).getMessage());
        // guard row 2: shadow + prune + floor disabled
        assertEquals("CORE BOOT GUARD: SNAPSHOT_MODE=shadow needs ShadowFloor on the pruner or SNAPSHOT_PRUNE_ENABLED=false",
                assertThrows(StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforce(
                        source("shadow", "SHADOW_FLOOR_ENABLED", "false", null), true)).getMessage());
        // guard row OK: prune off, or off mode, or the floor on
        assertEquals(SnapshotMode.SHADOW, StubModeBootGuard.enforce(source("shadow", "SHADOW_FLOOR_ENABLED", "false", null),
                false).mode());
        assertEquals(SnapshotMode.OFF, StubModeBootGuard.enforce(source("off", "SHADOW_FLOOR_ENABLED", "false", null),
                true).mode());
        assertEquals(SnapshotMode.SHADOW, StubModeBootGuard.enforce(source("shadow", null, null, null), true).mode());
        // the test-only override lets guard row 2 through, loudly
        System.setProperty(StubModeBootGuard.UNCHECKED_PROPERTY, "true");
        try {
            assertEquals(false, StubModeBootGuard.enforce(source("shadow", "SHADOW_FLOOR_ENABLED", "false", null), true)
                    .floorEnabled());
        } finally {
            System.clearProperty(StubModeBootGuard.UNCHECKED_PROPERTY);
        }
    }

    private static SnapshotModeSettings.Source source(final String mode, final String env, final String value,
                                             final String unused) {
        return (e, p) -> e.equals("SNAPSHOT_MODE") ? mode : e.equals(env) ? value : null;
    }
}

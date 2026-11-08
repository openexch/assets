// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.cluster.shadow.ShadowFloor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 3.5 and 11.15 (review F10): one row per numeric setting at min-1, min, max, max+1 and a non-integer, each
 * refused with the 3.5 text in {@code off} AND {@code shadow} mode; the enumerated settings; the defaults; and the boot
 * guard's G-S1 / G-S2 / G-OK rows (4.1).
 */
public class RcsSettingsTest {

    private record Range(RcsSettings.Key key, long min, long max) {
    }

    private static final Range[] RANGES = {
            new Range(RcsSettings.Key.SHADOW_RETAIN_N, 1, 1000),
            new Range(RcsSettings.Key.SHADOW_DEEP_READ_PACE_MS, 0, 3_600_000L),
            new Range(RcsSettings.Key.SHADOW_SCAN_PERIOD_MS, 100, 3_600_000L),
            new Range(RcsSettings.Key.RCS_TAIL_REPLAY_TIMEOUT_MS, 1000, 86_400_000L),
            new Range(RcsSettings.Key.RCS_READY_BUDGET_MS, 1000, 86_400_000L),
            new Range(RcsSettings.Key.RCS_A04_CM_SNAPSHOT_MAX_BYTES, 4096, 1_073_741_824L),
            new Range(RcsSettings.Key.SHADOW_LOAD_TIMEOUT_MS, 1000, 86_400_000L),
    };

    private static RcsSettings parse(final String mode, final RcsSettings.Key key, final String value) {
        final Map<RcsSettings.Key, String> m = new HashMap<>();
        m.put(RcsSettings.Key.RCS_MODE, mode);
        m.put(key, value);
        return RcsSettings.with(m);
    }

    private static long read(final RcsSettings s, final RcsSettings.Key key) {
        return switch (key) {
            case SHADOW_RETAIN_N -> s.retainN();
            case SHADOW_DEEP_READ_PACE_MS -> s.deepReadPaceMs();
            case SHADOW_SCAN_PERIOD_MS -> s.scanPeriodMs();
            case RCS_TAIL_REPLAY_TIMEOUT_MS -> s.tailReplayTimeoutMs();
            case RCS_READY_BUDGET_MS -> s.readyBudgetMs();
            case RCS_A04_CM_SNAPSHOT_MAX_BYTES -> s.a04CmSnapshotMaxBytes();
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
        final RcsSettings d = RcsSettings.defaults();
        assertEquals(RcsMode.OFF, d.mode());
        assertEquals(3, d.retainN());
        assertEquals(ShadowFloor.Margin.PREVIOUS_VERIFIED, d.floorMargin());
        assertEquals(2000, d.deepReadPaceMs());
        assertEquals(5000, d.scanPeriodMs());
        assertEquals(60_000, d.tailReplayTimeoutMs());
        assertEquals(30_000, d.readyBudgetMs());
        assertEquals(1_048_576, d.a04CmSnapshotMaxBytes());
        assertEquals(60_000, d.loadTimeoutMs());
        assertTrue(d.floorEnabled());

        assertEquals(RcsMode.SHADOW, RcsSettings.with(Map.of(RcsSettings.Key.RCS_MODE, "shadow")).mode());
        assertEquals("RCS_MODE 'bogus' (expected off|shadow)", assertThrows(IllegalArgumentException.class,
                () -> RcsSettings.with(Map.of(RcsSettings.Key.RCS_MODE, "bogus"))).getMessage());
        for (final String mode : new String[] {"off", "shadow"}) {
            assertEquals(ShadowFloor.Margin.NONE, parse(mode, RcsSettings.Key.SHADOW_FLOOR_MARGIN, "none").floorMargin());
            assertEquals("SHADOW_FLOOR_MARGIN 'last' (expected previous-verified|none)",
                    assertThrows(IllegalArgumentException.class,
                            () -> parse(mode, RcsSettings.Key.SHADOW_FLOOR_MARGIN, "last")).getMessage());
            assertEquals(false, parse(mode, RcsSettings.Key.SHADOW_FLOOR_ENABLED, "false").floorEnabled());
            assertEquals("SHADOW_FLOOR_ENABLED 'no' (expected true|false)", assertThrows(IllegalArgumentException.class,
                    () -> parse(mode, RcsSettings.Key.SHADOW_FLOOR_ENABLED, "no")).getMessage());
        }
    }

    @Test
    public void bootGuardRows() {
        // G-S1: a bad setting is refused with the guard prefix and the 3.5 text, in every mode
        for (final String mode : new String[] {"off", "shadow"}) {
            final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                    () -> StubModeBootGuard.enforce(source(mode, "SHADOW_RETAIN_N", "0", null), true));
            assertEquals("ASSETS BOOT GUARD: SHADOW_RETAIN_N=0 out of range [1,1000]", r.getMessage());
        }
        assertEquals("ASSETS BOOT GUARD: RCS_MODE 'bogus' (expected off|shadow)", assertThrows(
                StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforce(source("bogus", null, null,
                        null), true)).getMessage());
        // G-S2: shadow + prune + floor disabled
        assertEquals("ASSETS BOOT GUARD: RCS_MODE=shadow needs ShadowFloor on the pruner or SNAPSHOT_PRUNE_ENABLED=false"
                        + " (MF-4)", assertThrows(StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforce(
                        source("shadow", "SHADOW_FLOOR_ENABLED", "false", null), true)).getMessage());
        // G-OK: prune off, or off mode, or the floor on
        assertEquals(RcsMode.SHADOW, StubModeBootGuard.enforce(source("shadow", "SHADOW_FLOOR_ENABLED", "false", null),
                false).mode());
        assertEquals(RcsMode.OFF, StubModeBootGuard.enforce(source("off", "SHADOW_FLOOR_ENABLED", "false", null),
                true).mode());
        assertEquals(RcsMode.SHADOW, StubModeBootGuard.enforce(source("shadow", null, null, null), true).mode());
        // the test-only override lets G-S2 through, loudly
        System.setProperty(StubModeBootGuard.UNCHECKED_PROPERTY, "true");
        try {
            assertEquals(false, StubModeBootGuard.enforce(source("shadow", "SHADOW_FLOOR_ENABLED", "false", null), true)
                    .floorEnabled());
        } finally {
            System.clearProperty(StubModeBootGuard.UNCHECKED_PROPERTY);
        }
    }

    private static RcsSettings.Source source(final String mode, final String env, final String value,
                                             final String unused) {
        return (e, p) -> e.equals("RCS_MODE") ? mode : e.equals(env) ? value : null;
    }
}

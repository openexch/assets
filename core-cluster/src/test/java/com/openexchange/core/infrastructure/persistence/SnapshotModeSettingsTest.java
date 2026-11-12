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
            new Range(SnapshotModeSettings.Key.SHADOW_WITNESS_ALARM_MS, 1000, 604_800_000L),
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
            case SHADOW_WITNESS_ALARM_MS -> s.witnessAlarmMs();
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

    // ------------------------------------------------------------------------------- anchor rule and puller rows

    private record PullRange(SnapshotModeSettings.Key key, long min, long max) {
    }

    private static final PullRange[] PULL_RANGES = {
            new PullRange(SnapshotModeSettings.Key.SHADOW_PULL_PERIOD_MS, 500, 3_600_000L),
            new PullRange(SnapshotModeSettings.Key.SHADOW_PULL_STARTUP_WAIT_MS, 1000, 86_400_000L),
            new PullRange(SnapshotModeSettings.Key.SHADOW_PULL_TIMEOUT_MS, 1000, 86_400_000L),
            new PullRange(SnapshotModeSettings.Key.SHADOW_PULL_RETRY_BACKOFF_MS, 100, 3_600_000L),
    };

    private static long readPull(final SnapshotModeSettings s, final SnapshotModeSettings.Key key) {
        return switch (key) {
            case SHADOW_PULL_PERIOD_MS -> s.pullSettings().periodMs();
            case SHADOW_PULL_STARTUP_WAIT_MS -> s.pullSettings().startupWaitMs();
            case SHADOW_PULL_TIMEOUT_MS -> s.pullSettings().timeoutMs();
            case SHADOW_PULL_RETRY_BACKOFF_MS -> s.pullSettings().retryBackoffMs();
            default -> throw new IllegalArgumentException(key.name());
        };
    }

    @Test
    public void pullSettingsAtTheirBoundariesInBothModes() {
        int rows = 0;
        for (final String mode : new String[] {"off", "shadow"}) {
            for (final PullRange r : PULL_RANGES) {
                final String name = r.key().name();
                assertEquals(mode + " " + name + " min", r.min(), readPull(parse(mode, r.key(), Long.toString(r.min())),
                        r.key()));
                assertEquals(mode + " " + name + " max", r.max(), readPull(parse(mode, r.key(), Long.toString(r.max())),
                        r.key()));
                assertEquals(name + " must be " + r.min() + ".." + r.max() + " ms: " + (r.min() - 1),
                        assertThrows(IllegalArgumentException.class,
                                () -> parse(mode, r.key(), Long.toString(r.min() - 1))).getMessage());
                assertEquals(name + " must be " + r.min() + ".." + r.max() + " ms: " + (r.max() + 1),
                        assertThrows(IllegalArgumentException.class,
                                () -> parse(mode, r.key(), Long.toString(r.max() + 1))).getMessage());
                assertEquals(name + " is not an integer: 5s", assertThrows(IllegalArgumentException.class,
                        () -> parse(mode, r.key(), "5s")).getMessage());
                rows += 5;
            }
            final SnapshotModeSettings.Key inflight = SnapshotModeSettings.Key.SHADOW_PULL_MAX_INFLIGHT;
            assertEquals(1, parse(mode, inflight, "1").pullSettings().maxInflight());
            assertEquals(3, parse(mode, inflight, "3").pullSettings().maxInflight());
            assertEquals("SHADOW_PULL_MAX_INFLIGHT must be 1..3: 0", assertThrows(IllegalArgumentException.class,
                    () -> parse(mode, inflight, "0")).getMessage());
            assertEquals("SHADOW_PULL_MAX_INFLIGHT must be 1..3: 4", assertThrows(IllegalArgumentException.class,
                    () -> parse(mode, inflight, "4")).getMessage());
            assertEquals("SHADOW_PULL_MAX_INFLIGHT is not an integer: two", assertThrows(IllegalArgumentException.class,
                    () -> parse(mode, inflight, "two")).getMessage());
            rows += 5;
        }
        System.out.println("GREEN pull settings: " + rows + " boundary rows");
    }

    @Test
    public void pullDefaultsSourcesKindsAndEndpoints() {
        final SnapshotModeSettings d = SnapshotModeSettings.defaults();
        final com.openexchange.cluster.shadow.ShadowPullSettings p = d.pullSettings();
        assertEquals(ShadowFloor.AnchorRule.TWO_PRODUCERS, d.anchorRule());
        assertEquals(java.util.List.of(), p.sources());
        assertEquals(false, p.enabled());
        assertEquals(5000, p.periodMs());
        assertEquals(60_000, p.startupWaitMs());
        assertEquals(120_000, p.timeoutMs());
        assertEquals(5000, p.retryBackoffMs());
        assertEquals(1, p.maxInflight());
        assertEquals(java.util.Set.of(com.openexchange.cluster.shadow.ShadowKind.ALIGNED), p.kinds());
        assertEquals(null, p.replicationEndpoint());
        assertEquals(null, p.responseEndpoint());
        assertEquals("an empty source list is a CRITICAL line, never a refusal",
                java.util.List.of(com.openexchange.cluster.shadow.ShadowPullSettings.DISABLED_LINE), d.criticalLines());

        // sources: an ordered allowlist of host:port, never derived
        final SnapshotModeSettings two = parse("shadow", SnapshotModeSettings.Key.SHADOW_PULL_SOURCES,
                "10.0.0.2:9301, 10.0.0.3:9301");
        assertEquals("[10.0.0.2:9301, 10.0.0.3:9301]", two.pullSettings().sources().toString());
        assertTrue(two.pullSettings().enabled());
        assertEquals(java.util.List.of(), two.criticalLines());
        assertEquals("SHADOW_PULL_SOURCES entry '10.0.0.2' is not host:port", assertThrows(
                IllegalArgumentException.class, () -> parse("off", SnapshotModeSettings.Key.SHADOW_PULL_SOURCES,
                        "10.0.0.2")).getMessage());
        assertEquals("SHADOW_PULL_SOURCES entry 'host:99999' is not host:port", assertThrows(
                IllegalArgumentException.class, () -> parse("off", SnapshotModeSettings.Key.SHADOW_PULL_SOURCES,
                        "host:99999")).getMessage());
        // this node's own archive endpoint is a refusal, once the endpoint is known
        assertEquals("SHADOW_PULL_SOURCES lists this node's own archive endpoint 10.0.0.2:9301", assertThrows(
                IllegalArgumentException.class, () -> two.pullSettingsFor("10.0.0.2:9301")).getMessage());
        assertEquals(2, two.pullSettingsFor("10.0.0.1:9301").sources().size());

        // kinds: ALIGNED by default, a widening is allowed with a CRITICAL line, a typo is refused
        final SnapshotModeSettings wide = parse("shadow", SnapshotModeSettings.Key.SHADOW_PULL_KINDS, "ALIGNED,STANDBY");
        assertEquals(java.util.Set.of(com.openexchange.cluster.shadow.ShadowKind.ALIGNED,
                com.openexchange.cluster.shadow.ShadowKind.STANDBY), wide.pullSettings().kinds());
        assertTrue(wide.criticalLines().toString(), wide.criticalLines().contains(
                "pulling single-producer kinds ALIGNED,STANDBY: a held source's suspects can be pulled"));
        assertEquals("SHADOW_PULL_KINDS 'ALIGNED,FULL' (expected a comma list of ALIGNED|STANDBY|CADENCE|LOCAL)",
                assertThrows(IllegalArgumentException.class, () -> parse("off", SnapshotModeSettings.Key.SHADOW_PULL_KINDS,
                        "ALIGNED,FULL")).getMessage());

        // both fixed endpoints (an isolated host): parsed into the settings the puller hands ReplicationParams
        final Map<SnapshotModeSettings.Key, String> m = new HashMap<>();
        m.put(SnapshotModeSettings.Key.SHADOW_PULL_REPLICATION_ENDPOINT, "10.0.0.1:9450");
        m.put(SnapshotModeSettings.Key.SHADOW_PULL_RESPONSE_ENDPOINT, "10.0.0.1:9451");
        final com.openexchange.cluster.shadow.ShadowPullSettings fixed = SnapshotModeSettings.with(m).pullSettings();
        assertEquals("aeron:udp?endpoint=10.0.0.1:9450", fixed.replicationEndpoint().udpChannel());
        assertEquals("aeron:udp?endpoint=10.0.0.1:9451", fixed.responseEndpoint().udpChannel());
        assertEquals("SHADOW_PULL_REPLICATION_ENDPOINT 'nowhere' is not host:port", assertThrows(
                IllegalArgumentException.class, () -> parse("off",
                        SnapshotModeSettings.Key.SHADOW_PULL_REPLICATION_ENDPOINT, "nowhere")).getMessage());
        assertEquals("SHADOW_PULL_RESPONSE_ENDPOINT 'nowhere:' is not host:port", assertThrows(
                IllegalArgumentException.class, () -> parse("off",
                        SnapshotModeSettings.Key.SHADOW_PULL_RESPONSE_ENDPOINT, "nowhere:")).getMessage());
    }

    @Test
    public void anchorRuleRows() {
        for (final String mode : new String[] {"off", "shadow"}) {
            assertEquals(ShadowFloor.AnchorRule.TWO_PRODUCERS,
                    parse(mode, SnapshotModeSettings.Key.SHADOW_ANCHOR_RULE, "two-producers").anchorRule());
            final SnapshotModeSettings any = parse(mode, SnapshotModeSettings.Key.SHADOW_ANCHOR_RULE, "any-verified");
            assertEquals(ShadowFloor.AnchorRule.ANY_VERIFIED, any.anchorRule());
            assertTrue("any-verified is allowed with a CRITICAL line: " + any.criticalLines(),
                    any.criticalLines().contains(ShadowFloor.AnchorRule.ANY_VERIFIED.criticalLine()));
            assertEquals("SHADOW_ANCHOR_RULE 'one-producer' (expected two-producers|any-verified)",
                    assertThrows(IllegalArgumentException.class, () -> parse(mode,
                            SnapshotModeSettings.Key.SHADOW_ANCHOR_RULE, "one-producer")).getMessage());
        }
        // the margin keeps its default under the new rule
        assertEquals(ShadowFloor.Margin.PREVIOUS_VERIFIED, SnapshotModeSettings.defaults().floorMargin());
    }

    @Test
    public void bootGuardPullRows() {
        // guard row 1 covers the pull rows: refused with the prefix and the kit's text
        assertEquals("CORE BOOT GUARD: SHADOW_PULL_PERIOD_MS must be 500..3600000 ms: 499", assertThrows(
                StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforce(
                        source("shadow", "SHADOW_PULL_PERIOD_MS", "499", null), true)).getMessage());
        assertEquals("CORE BOOT GUARD: SHADOW_ANCHOR_RULE 'x' (expected two-producers|any-verified)", assertThrows(
                StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforce(
                        source("off", "SHADOW_ANCHOR_RULE", "x", null), true)).getMessage());
        // guard row 3: this node's own archive endpoint
        final SnapshotModeSettings s = StubModeBootGuard.enforce(source("shadow", "SHADOW_PULL_SOURCES",
                "localhost:9301,localhost:9401", null), true);
        assertEquals("CORE BOOT GUARD: SHADOW_PULL_SOURCES lists this node's own archive endpoint localhost:9401",
                assertThrows(StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforcePullSources(s,
                        StubModeBootGuard.endpointOf("aeron:udp?endpoint=localhost:9401"))).getMessage());
        StubModeBootGuard.enforcePullSources(s, StubModeBootGuard.endpointOf("aeron:udp?endpoint=localhost:9501"));
        assertEquals("localhost:9401", StubModeBootGuard.endpointOf("aeron:udp?endpoint=localhost:9401|mtu=8192"));
        // guard row 4: any-verified boots, with its CRITICAL line among the boot lines
        assertEquals(ShadowFloor.AnchorRule.ANY_VERIFIED, StubModeBootGuard.enforce(source("shadow",
                "SHADOW_ANCHOR_RULE", "any-verified", null), true).anchorRule());
    }

    private static SnapshotModeSettings.Source source(final String mode, final String env, final String value,
                                             final String unused) {
        return (e, p) -> e.equals("SNAPSHOT_MODE") ? mode : e.equals(env) ? value : null;
    }

    /** One producer row: the setting, its min and max, the unit text of its refusal, the read-back. */
    private record ProducerRange(SnapshotModeSettings.Key key, long min, long max, String unit,
                                 java.util.function.ToLongFunction<com.openexchange.cluster.shadow.ShadowProducerSettings> read) {
    }

    private static final ProducerRange[] PRODUCER_RANGES = {
            new ProducerRange(SnapshotModeSettings.Key.SHADOW_PRODUCER_BUFFER_MB, 1, 2047, "", p -> p.bufferMb()),
            new ProducerRange(SnapshotModeSettings.Key.SHADOW_WRITE_CONNECT_TIMEOUT_MS, 1, 86_400_000L, " ms",
                    p -> p.connectTimeoutMs()),
            new ProducerRange(SnapshotModeSettings.Key.SHADOW_WRITE_OFFER_TIMEOUT_MS, 1, 86_400_000L, " ms",
                    p -> p.offerTimeoutMs()),
            new ProducerRange(SnapshotModeSettings.Key.SHADOW_WRITE_RECORD_TIMEOUT_MS, 1, 86_400_000L, " ms",
                    p -> p.recordTimeoutMs()),
            new ProducerRange(SnapshotModeSettings.Key.SHADOW_PRODUCER_WINDOW_BUDGET_PCT, 1, 100, "",
                    p -> p.windowBudgetPct()),
            new ProducerRange(SnapshotModeSettings.Key.SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS, 1, 600_000L, " ms",
                    p -> p.serializeEstimateMs()),
            new ProducerRange(SnapshotModeSettings.Key.SHADOW_PRODUCER_RATE_WINDOW_MS, 1000, 3_600_000L, " ms",
                    p -> p.rateWindowMs()),
    };

    /**
     * The follower producer rows of the settings table: each numeric row at its boundaries in both modes with the kit
     * parser's text, the term length rule, {@code SHADOW_PRODUCER} values and its two CRITICAL lines (none in shadow
     * mode; an explicit designated-follower in off mode), and the defaults.
     */
    @Test
    public void followerProducerRowsAtTheirBoundaries() {
        for (final String mode : new String[] {"off", "shadow"}) {
            for (final ProducerRange r : PRODUCER_RANGES) {
                final String name = r.key().name();
                assertEquals(mode + " " + name + " min", r.min(), r.read().applyAsLong(
                        parse(mode, r.key(), Long.toString(r.min())).producerSettings()));
                assertEquals(mode + " " + name + " max", r.max(), r.read().applyAsLong(
                        parse(mode, r.key(), Long.toString(r.max())).producerSettings()));
                assertEquals(name + " must be " + r.min() + ".." + r.max() + r.unit() + ": " + (r.min() - 1),
                        assertThrows(IllegalArgumentException.class,
                                () -> parse(mode, r.key(), Long.toString(r.min() - 1))).getMessage());
                assertEquals(name + " must be " + r.min() + ".." + r.max() + r.unit() + ": " + (r.max() + 1),
                        assertThrows(IllegalArgumentException.class,
                                () -> parse(mode, r.key(), Long.toString(r.max() + 1))).getMessage());
                assertEquals(name + " is not an integer: 5s", assertThrows(IllegalArgumentException.class,
                        () -> parse(mode, r.key(), "5s")).getMessage());
            }
            assertEquals("SHADOW_TERM_LENGTH 32k outside [64k, 16m] or not a power of two (every replay maps 3 x the"
                    + " recording's term length)", assertThrows(IllegalArgumentException.class,
                    () -> parse(mode, SnapshotModeSettings.Key.SHADOW_TERM_LENGTH, "32k")).getMessage());
            assertEquals("64k", parse(mode, SnapshotModeSettings.Key.SHADOW_TERM_LENGTH, "64k").producerSettings()
                    .termLength());
            assertEquals("16m", parse(mode, SnapshotModeSettings.Key.SHADOW_TERM_LENGTH, "16m").producerSettings()
                    .termLength());
            assertTrue(assertThrows(IllegalArgumentException.class,
                    () -> parse(mode, SnapshotModeSettings.Key.SHADOW_PRODUCER, "both")).getMessage()
                    .startsWith("SHADOW_PRODUCER 'both'"));
        }
        final com.openexchange.cluster.shadow.ShadowProducerSettings d = SnapshotModeSettings.defaults().producerSettings();
        assertEquals(com.openexchange.cluster.shadow.ShadowProducerSettings.Mode.DESIGNATED_FOLLOWER, d.mode());
        assertEquals("4m", d.termLength());
        assertEquals(512, d.bufferMb());
        assertEquals(10_000, d.connectTimeoutMs());
        assertEquals(30_000, d.offerTimeoutMs());
        assertEquals(60_000, d.recordTimeoutMs());
        assertEquals(50, d.windowBudgetPct());
        assertEquals(1_000, d.serializeEstimateMs());
        assertEquals(10_000, d.rateWindowMs());
        assertEquals(7_200_000L, SnapshotModeSettings.defaults().witnessAlarmMs());

        // SHADOW_PRODUCER=none in shadow mode: the CRITICAL line; the default is silent in both modes
        final SnapshotModeSettings none = parse("shadow", SnapshotModeSettings.Key.SHADOW_PRODUCER, "none");
        assertTrue(none.criticalLines().toString(), none.criticalLines().contains(
                "SHADOW_PRODUCER=none: this member never writes an ALIGNED: the two-producer anchor depends on other"
                        + " producers"));
        assertTrue(SnapshotModeSettings.with(Map.of(SnapshotModeSettings.Key.SNAPSHOT_MODE, "shadow")).criticalLines()
                .stream().noneMatch(l -> l.startsWith("SHADOW_PRODUCER")));
        assertTrue(SnapshotModeSettings.defaults().criticalLines().stream().noneMatch(l -> l.startsWith("SHADOW_PRODUCER")));
        // an explicit designated-follower in off mode: ignored, one CRITICAL line
        final SnapshotModeSettings ignored = parse("off", SnapshotModeSettings.Key.SHADOW_PRODUCER, "designated-follower");
        assertTrue(ignored.criticalLines().toString(), ignored.criticalLines().contains(
                "SHADOW_PRODUCER=designated-follower ignored: SNAPSHOT_MODE=off"));
    }

    /**
     * Guard row 5: a member's archive under another spelling in {@code SHADOW_PULL_SOURCES} refuses the boot (the
     * leader-source skip compares text); the members table is read from the cluster members string.
     */
    @Test
    public void aMemberSourceSpelledDifferentlyFromTheMembersTableRefusesTheBoot() {
        final String members = "0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|"
                + "1,localhost:9402,localhost:9403,localhost:9404,localhost:9405,localhost:9401";
        final java.util.Map<Integer, String> table = StubModeBootGuard.memberArchiveEndpoints(members);
        assertEquals(java.util.Map.of(0, "localhost:9301", 1, "localhost:9401"), table);
        final SnapshotModeSettings s = parse("shadow", SnapshotModeSettings.Key.SHADOW_PULL_SOURCES, "127.0.0.1:9401");
        assertEquals("CORE BOOT GUARD: SHADOW_PULL_SOURCES entry 127.0.0.1:9401 names member 1's archive endpoint"
                + " localhost:9401 with a different spelling: spell it as the cluster members table does so the leader"
                + " skip can recognise it", assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StubModeBootGuard.enforcePullSources(s, "localhost:9301", table)).getMessage());
        // spelled as the table does: accepted
        StubModeBootGuard.enforcePullSources(parse("shadow", SnapshotModeSettings.Key.SHADOW_PULL_SOURCES,
                "localhost:9401"), "localhost:9301", table);
    }
}

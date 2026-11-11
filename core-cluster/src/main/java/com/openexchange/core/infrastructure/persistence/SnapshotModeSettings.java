// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.TransportConfig;
import com.openexchange.cluster.shadow.ShadowFloor;
import com.openexchange.cluster.shadow.ShadowPullSettings;

import java.util.Objects;

/**
 * Every setting with its default, its range and its refusal text, in ONE table. Parsed at boot
 * in EVERY mode (a typo in a setting the current mode does not use is still a typo) and refused before any Aeron
 * component ({@link StubModeBootGuard}, guard row 1). The ranges catch typos and unit mistakes (seconds for milliseconds),
 * not operational choices. Nothing here is a hard acceptance limit: the acceptance rows read these values.
 *
 * <p>Each setting is read as environment variable first, then system property (the transport settings' helper), else
 * its default.</p>
 */
public final class SnapshotModeSettings {

    /** A setting: env name, property name, default text. */
    public enum Key {
        SNAPSHOT_MODE("snapshot.mode", "off"),
        SHADOW_RETAIN_N("shadow.retain.n", "3"),
        SHADOW_FLOOR_MARGIN("shadow.floor.margin", "previous-verified"),
        SHADOW_DEEP_READ_PACE_MS("shadow.deep.read.pace.ms", "2000"),
        SHADOW_SCAN_PERIOD_MS("shadow.scan.period.ms", "5000"),
        SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS("snapshot.tail.replay.timeout.ms", "60000"),
        SNAPSHOT_RESTORE_READY_BUDGET_MS("snapshot.restore.ready.budget.ms", "30000"),
        CONSENSUS_SNAPSHOT_MAX_BYTES("consensus.snapshot.max.bytes", "1048576"),
        SHADOW_LOAD_TIMEOUT_MS("shadow.load.timeout.ms", "60000"),
        SHADOW_FLOOR_ENABLED("shadow.floor.enabled", "true"),
        SHADOW_ANCHOR_RULE("shadow.anchor.rule", "two-producers"),
        SHADOW_PULL_SOURCES("shadow.pull.sources", ""),
        SHADOW_PULL_PERIOD_MS("shadow.pull.period.ms", "5000"),
        SHADOW_PULL_STARTUP_WAIT_MS("shadow.pull.startup.wait.ms", "60000"),
        SHADOW_PULL_TIMEOUT_MS("shadow.pull.timeout.ms", "120000"),
        SHADOW_PULL_RETRY_BACKOFF_MS("shadow.pull.retry.backoff.ms", "5000"),
        SHADOW_PULL_MAX_INFLIGHT("shadow.pull.max.inflight", "1"),
        SHADOW_PULL_KINDS("shadow.pull.kinds", "ALIGNED"),
        SHADOW_PULL_REPLICATION_ENDPOINT("shadow.pull.replication.endpoint", ""),
        SHADOW_PULL_RESPONSE_ENDPOINT("shadow.pull.response.endpoint", "");

        private final String property;
        private final String defaultText;

        Key(final String property, final String defaultText) {
            this.property = property;
            this.defaultText = defaultText;
        }

        public String property() {
            return property;
        }

        public String defaultText() {
            return defaultText;
        }
    }

    /** Reads a raw value: (env name, property name) to the raw text or null when unset. */
    @FunctionalInterface
    public interface Source {
        String get(String envName, String propertyName);

        /** Env (non-empty), then system property. */
        Source ENVIRONMENT = (env, prop) -> {
            final String v = TransportConfig.envOrProp(env, prop, null);
            return v == null || v.isEmpty() ? null : v;
        };
    }

    private final SnapshotMode mode;
    private final int retainN;
    private final ShadowFloor.Margin floorMargin;
    private final long deepReadPaceMs;
    private final long scanPeriodMs;
    private final long tailReplayTimeoutMs;
    private final long readyBudgetMs;
    private final long consensusSnapshotMaxBytes;
    private final long loadTimeoutMs;
    private final boolean floorEnabled;
    private final ShadowFloor.AnchorRule anchorRule;
    private final java.util.function.Function<String, String> pullEnv;
    private final ShadowPullSettings pullSettings;

    private SnapshotModeSettings(final Source source) {
        this.mode = SnapshotMode.parse(raw(source, Key.SNAPSHOT_MODE));
        this.retainN = (int)range(source, Key.SHADOW_RETAIN_N, 1, 1000);
        this.floorMargin = ShadowFloor.Margin.parse(raw(source, Key.SHADOW_FLOOR_MARGIN));
        this.deepReadPaceMs = range(source, Key.SHADOW_DEEP_READ_PACE_MS, 0, 3_600_000L);
        this.scanPeriodMs = range(source, Key.SHADOW_SCAN_PERIOD_MS, 100, 3_600_000L);
        this.tailReplayTimeoutMs = range(source, Key.SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS, 1000, 86_400_000L);
        this.readyBudgetMs = range(source, Key.SNAPSHOT_RESTORE_READY_BUDGET_MS, 1000, 86_400_000L);
        this.consensusSnapshotMaxBytes = range(source, Key.CONSENSUS_SNAPSHOT_MAX_BYTES, 4096, 1_073_741_824L);
        this.loadTimeoutMs = range(source, Key.SHADOW_LOAD_TIMEOUT_MS, 1000, 86_400_000L);
        this.floorEnabled = bool(source, Key.SHADOW_FLOOR_ENABLED);
        this.anchorRule = ShadowFloor.AnchorRule.parse(raw(source, Key.SHADOW_ANCHOR_RULE));
        // the puller rows: the kit's one parser (its ranges and refusal texts), read through this table's source
        this.pullEnv = name -> raw(source, Key.valueOf(name));
        this.pullSettings = ShadowPullSettings.parse(pullEnv, null);
    }

    /** The settings of this process. @throws IllegalArgumentException with the 3.5 text of the first bad setting */
    public static SnapshotModeSettings fromEnv() {
        return parse(Source.ENVIRONMENT);
    }

    /** @throws IllegalArgumentException with the 3.5 text of the first bad setting (table order) */
    public static SnapshotModeSettings parse(final Source source) {
        return new SnapshotModeSettings(Objects.requireNonNull(source, "source"));
    }

    /** Defaults only (in-process unit tests that construct the service without an environment). */
    public static SnapshotModeSettings defaults() {
        return parse((env, prop) -> null);
    }

    /** Defaults with the given overrides ({@code Key.name() -> text}); test convenience. */
    public static SnapshotModeSettings with(final java.util.Map<Key, String> values) {
        final java.util.Map<Key, String> copy = java.util.Map.copyOf(values);
        return parse((env, prop) -> copy.get(Key.valueOf(env)));
    }

    private static String raw(final Source source, final Key key) {
        final String v = source.get(key.name(), key.property());
        return v == null ? key.defaultText() : v.trim();
    }

    private static long range(final Source source, final Key key, final long min, final long max) {
        final String v = raw(source, key);
        final long value;
        try {
            value = Long.parseLong(v);
        } catch (final NumberFormatException notANumber) {
            throw new IllegalArgumentException(key.name() + "='" + v + "' is not an integer");
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(key.name() + "=" + value + " out of range [" + min + "," + max + "]");
        }
        return value;
    }

    private static boolean bool(final Source source, final Key key) {
        final String v = raw(source, key);
        if ("true".equals(v)) {
            return true;
        }
        if ("false".equals(v)) {
            return false;
        }
        throw new IllegalArgumentException(key.name() + " '" + v + "' (expected true|false)");
    }

    public SnapshotMode mode() {
        return mode;
    }

    public int retainN() {
        return retainN;
    }

    public ShadowFloor.Margin floorMargin() {
        return floorMargin;
    }

    public long deepReadPaceMs() {
        return deepReadPaceMs;
    }

    public long scanPeriodMs() {
        return scanPeriodMs;
    }

    public long tailReplayTimeoutMs() {
        return tailReplayTimeoutMs;
    }

    public long readyBudgetMs() {
        return readyBudgetMs;
    }

    public long consensusSnapshotMaxBytes() {
        return consensusSnapshotMaxBytes;
    }

    public long loadTimeoutMs() {
        return loadTimeoutMs;
    }

    /** {@code SHADOW_FLOOR_ENABLED}: a test-only override (4.1); false is refused by the boot guard in shadow mode. */
    public boolean floorEnabled() {
        return floorEnabled;
    }

    /** {@code SHADOW_ANCHOR_RULE}: what may anchor a purge below P_cm on a member (default two-producers). */
    public ShadowFloor.AnchorRule anchorRule() {
        return anchorRule;
    }

    /** The {@code SHADOW_PULL_*} rows (disabled when {@code SHADOW_PULL_SOURCES} is empty). */
    public ShadowPullSettings pullSettings() {
        return pullSettings;
    }

    /**
     * The pull rows checked against this node's own archive control endpoint ({@code host:port}): a source list that
     * names it is refused with the kit's text.
     *
     * @throws IllegalArgumentException {@code SHADOW_PULL_SOURCES lists this node's own archive endpoint <v>}
     */
    public ShadowPullSettings pullSettingsFor(final String ownArchiveEndpoint) {
        return ShadowPullSettings.parse(pullEnv, ownArchiveEndpoint);
    }

    /** The CRITICAL lines a member prints at boot: the anchor rule's opt-out, the disabled puller, a kinds widening. */
    public java.util.List<String> criticalLines() {
        final java.util.List<String> out = new java.util.ArrayList<>();
        if (anchorRule.criticalLine() != null) {
            out.add(anchorRule.criticalLine());
        }
        out.addAll(pullSettings.criticalLines());
        return out;
    }

    @Override
    public String toString() {
        return "snapshotMode=" + mode.text() + " retainN=" + retainN + " floorMargin=" + floorMargin.text()
                + " deepReadPaceMs=" + deepReadPaceMs + " scanPeriodMs=" + scanPeriodMs + " tailReplayTimeoutMs="
                + tailReplayTimeoutMs + " readyBudgetMs=" + readyBudgetMs + " consensusSnapshotMaxBytes="
                + consensusSnapshotMaxBytes + " loadTimeoutMs=" + loadTimeoutMs + " floorEnabled=" + floorEnabled;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Leader liveness and election timing of a core member. These decide how long the cluster goes without a leader
 * after a leader dies (detection plus election) and how often load or a GC pause on a healthy leader is mistaken for
 * a dead one. No client is acked for a command before it is committed by a quorum and applied, so a failover never
 * loses an acked command; during the leaderless window ingress is refused and clients retry.
 *
 * <p>Every member of one cluster must run with the same values. They are read once at boot, before any Aeron
 * component exists; a bad value refuses boot.</p>
 */
public final class ElectionTimingSettings {

    /** A setting: env name, property name, default text. */
    public enum Key {
        CORE_LEADER_HEARTBEAT_INTERVAL_MS("core.leader.heartbeat.interval.ms", "100"),
        CORE_LEADER_HEARTBEAT_TIMEOUT_MS("core.leader.heartbeat.timeout.ms", "1000"),
        CORE_ELECTION_TIMEOUT_MS("core.election.timeout.ms", "1000");

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

    /** A heartbeat timeout must cover at least this many heartbeat intervals, so one lost heartbeat is not a death. */
    public static final int MIN_INTERVALS_PER_TIMEOUT = 3;

    private static final long MAX_MS = 600_000L;

    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private final long electionTimeoutMs;

    private ElectionTimingSettings(final SnapshotModeSettings.Source source) {
        this.heartbeatIntervalMs = range(source, Key.CORE_LEADER_HEARTBEAT_INTERVAL_MS, 10, MAX_MS);
        this.heartbeatTimeoutMs = range(source, Key.CORE_LEADER_HEARTBEAT_TIMEOUT_MS, 100, MAX_MS);
        this.electionTimeoutMs = range(source, Key.CORE_ELECTION_TIMEOUT_MS, 100, MAX_MS);
        if (heartbeatTimeoutMs < MIN_INTERVALS_PER_TIMEOUT * heartbeatIntervalMs) {
            throw new IllegalArgumentException(Key.CORE_LEADER_HEARTBEAT_TIMEOUT_MS.name() + "=" + heartbeatTimeoutMs
                    + " must be at least " + MIN_INTERVALS_PER_TIMEOUT + " x "
                    + Key.CORE_LEADER_HEARTBEAT_INTERVAL_MS.name() + " (" + heartbeatIntervalMs + ")");
        }
    }

    /** The settings of this process. @throws IllegalArgumentException naming the first bad setting */
    public static ElectionTimingSettings fromEnv() {
        return parse(SnapshotModeSettings.Source.ENVIRONMENT);
    }

    /** @throws IllegalArgumentException naming the first bad setting (table order) */
    public static ElectionTimingSettings parse(final SnapshotModeSettings.Source source) {
        return new ElectionTimingSettings(Objects.requireNonNull(source, "source"));
    }

    /** Defaults with the given overrides ({@code Key -> text}); test convenience. */
    public static ElectionTimingSettings with(final Map<Key, String> values) {
        final Map<Key, String> copy = Map.copyOf(values);
        return parse((env, prop) -> copy.get(Key.valueOf(env)));
    }

    public long heartbeatIntervalNs() {
        return TimeUnit.MILLISECONDS.toNanos(heartbeatIntervalMs);
    }

    public long heartbeatTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(heartbeatTimeoutMs);
    }

    public long electionTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(electionTimeoutMs);
    }

    @Override
    public String toString() {
        return "heartbeatIntervalMs=" + heartbeatIntervalMs + " heartbeatTimeoutMs=" + heartbeatTimeoutMs
                + " electionTimeoutMs=" + electionTimeoutMs;
    }

    private static long range(final SnapshotModeSettings.Source source, final Key key, final long min, final long max) {
        final String raw = source.get(key.name(), key.property());
        final String v = raw == null ? key.defaultText() : raw.trim();
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
}

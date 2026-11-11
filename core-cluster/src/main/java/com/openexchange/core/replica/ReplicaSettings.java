// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.replica;

import com.openexchange.core.auth.CoreAuth;
import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import com.openexchange.core.infrastructure.persistence.SnapshotModeSettings;
import com.openexchange.core.infrastructure.persistence.StatePaths;
import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.cluster.auth.ClusterCredentials;
import com.openexchange.cluster.auth.ClusterSessionClass;
import com.openexchange.cluster.replica.MismatchDump;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaContexts;
import io.aeron.CommonContext;
import io.aeron.cluster.ClusterBackup;
import org.agrona.SystemUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Every replica setting with its
 * default, its range and its refusal text, in ONE table, parsed first in {@code CoreShadowReplicaMain} and refused
 * with the {@value #PREFIX} prefix before any Aeron component exists. The shared shadow keys ({@code SHADOW_RETAIN_N},
 * {@code SHADOW_FLOOR_MARGIN}, {@code SHADOW_DEEP_READ_PACE_MS}, {@code SHADOW_SCAN_PERIOD_MS},
 * {@code SHADOW_LOAD_TIMEOUT_MS}) are {@link SnapshotModeSettings}' with the member shadow ranges and texts.
 *
 * <p>Ranges catch typos and unit mistakes, they are not operational limits. The members' backup window
 * ({@code REPLICA_MEMBER_BACKUP_WINDOW_MS}, default 11 000 = the members' session timeout + 1 s, {@code CFG:188-189})
 * is a setting: a cool-down or a query interval below {@code window + 1000} is a CRITICAL line, never a refusal.
 * Each setting is read as environment variable first, then system property, else its
 * default; {@code CORE_AUTH_CREDENTIAL} keeps the kit's order (system property first, {@code AuthPolicy}).</p>
 *
 * <p>Boot guards beyond the ranges: {@code REPLICA_BACKUP_SOURCE=FOLLOWER} with one member (the backup
 * agent would spin on a one-element publication group); {@code CLUSTER_REPLICA_ADDRESS} defaults to {@code localhost}
 * only when every member is local; the credential must claim class REPLICA; a replica port on a member's
 * host must not be one of that member's ports, and no replica port may exceed 65535; when
 * {@code REPLICA_MEMBER_CADENCE_MS} is set, {@code REPLICA_READY_SHADOW_AGE_MS} must cover it plus
 * {@code REPLICA_VERIFY_WAIT_MS} (a dropped ALIGNED suspends STANDBY until the next equal compare); the mismatch dump
 * dir ({@code REPLICA_MISMATCH_DIR}) must be writable at boot.</p>
 */
public final class ReplicaSettings {

    public static final String PREFIX = "CORE REPLICA BOOT GUARD: ";
    /** 2 x the query interval max (86 400 000) + the cool-down max (3 600 000). */
    public static final long MAX_VERIFY_WAIT_MS = 2 * 86_400_000L + 3_600_000L;

    /** A refused setting: exit 1 through the lifecycle's {@code startupFailed}, before any Aeron component. */
    public static final class Refused extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Refused(final String message, final Throwable cause) {
            super(PREFIX + message, cause);
        }
    }

    /** A setting: env name and property name. Defaults and ranges live in the parser, one row each. */
    public enum Key {
        TRANSPORT_DRIVER_MODE("transport.driver.mode"),
        AERON_DIR("aeron.driver.dir"),
        TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS(EngineLifecycle.DRIVER_WAIT_PROP),
        TRANSPORT_IDLE_MODE("transport.idle.mode"),
        CLUSTER_ADDRESSES("cluster.addresses"),
        CLUSTER_PORT_BASE("port.base"),
        CLUSTER_PORT_STRIDE("cluster.port.stride"),
        CLUSTER_REPLICA_SLOT("cluster.replica.slot"),
        CLUSTER_REPLICA_ADDRESS("cluster.replica.address"),
        BASE_DIR("base.dir"),
        /** The members' {@code aeron.cluster.id} (CFG never sets one: 0). */
        CLUSTER_ID("aeron.cluster.id"),
        CORE_AUTH_CREDENTIAL(CoreAuth.POLICY.credentialProperty()),
        CORE_NODE_PORT("core.node.port"),
        REPLICA_BACKUP_SOURCE("replica.backup.source"),
        REPLICA_MEMBER_BACKUP_WINDOW_MS("replica.member.backup.window.ms"),
        REPLICA_BACKUP_COOLDOWN_MS("replica.backup.cooldown.ms"),
        REPLICA_BACKUP_RESPONSE_TIMEOUT_MS("replica.backup.response.timeout.ms"),
        REPLICA_BACKUP_QUERY_INTERVAL_MS("replica.backup.query.interval.ms"),
        REPLICA_BACKUP_PROGRESS_TIMEOUT_MS("replica.backup.progress.timeout.ms"),
        REPLICA_IMAGE_LIVENESS_MS("replica.image.liveness.ms"),
        REPLICA_CATCHUP_RCVBUF("replica.catchup.rcvbuf"),
        REPLICA_CATCHUP_RCV_WND("replica.catchup.rcv.wnd"),
        /** The catch-up channel's so-sndbuf (Aeron's 512k default). */
        REPLICA_CATCHUP_SNDBUF("replica.catchup.sndbuf"),
        /** The embedded driver's MTU (8192, the members'). */
        REPLICA_DRIVER_MTU("replica.driver.mtu"),
        /** The embedded driver's IPC and publication term length (16m). */
        REPLICA_DRIVER_TERM_LENGTH("replica.driver.term.length"),
        REPLICA_SHADOW_BUFFER_MB("replica.shadow.buffer.mb"),
        REPLICA_ALIGNED_WAIT_MS("replica.aligned.wait.ms"),
        /** The members' standby schedule; read here only to derive the two defaults below. */
        SNAPSHOT_STANDBY_INTERVAL_MINUTES("snapshot.standby.interval.minutes"),
        SHADOW_STANDBY_FALLBACK_MINUTES("shadow.standby.fallback.minutes"),
        REPLICA_RETRIEVED_SETS_KEEP("replica.retrieved.sets.keep"),
        /** The members' log term length; read here only to derive {@code REPLICA_READY_MAX_LAG_BYTES}. */
        TRANSPORT_LOG_TERM_LENGTH("transport.log.term.length"),
        REPLICA_READY_MAX_LAG_BYTES("replica.ready.max.lag.bytes"),
        REPLICA_READY_SHADOW_AGE_MS("replica.ready.shadow.age.ms"),
        REPLICA_READY_STALE_MS("replica.ready.stale.ms"),
        REPLICA_LIVENESS_STALE_MS("replica.liveness.stale.ms"),
        REPLICA_READY_TIME_LAG_MS("replica.ready.time.lag.ms"),
        REPLICA_ATTACH_BACKOFF_MS("replica.attach.backoff.ms"),
        REPLICA_BOOTSTRAP_WAIT_MS("replica.bootstrap.wait.ms"),
        /** The bound of an ALIGNED compare's wait for the retrieved member snapshot. */
        REPLICA_VERIFY_WAIT_MS("replica.verify.wait.ms"),
        /** How long an oracle mismatch holds before exit 1; 0 holds until the operator stops the process. */
        REPLICA_MISMATCH_HOLD_MS("replica.mismatch.hold.ms"),
        /** Where an oracle mismatch dumps both images and its report (default {@code BASE_DIR/mismatch}). */
        REPLICA_MISMATCH_DIR("replica.mismatch.dir"),
        /** The cap per dumped image file in MiB; 0 writes the report only. */
        REPLICA_MISMATCH_DUMP_MAX_MB("replica.mismatch.dump.max.mb"),
        /** How long a history fence waits for the purge of its suspect shadows before exit 1. */
        REPLICA_FENCE_PURGE_WAIT_MS("replica.fence.purge.wait.ms"),
        /** The members' snapshot cadence, for the readiness shadow age cross-check; 0 = not checked. */
        REPLICA_MEMBER_CADENCE_MS("replica.member.cadence.ms"),
        /** The cluster log stream the backup records L on (Aeron's 100, the members'). */
        REPLICA_LOG_STREAM_ID("replica.log.stream.id"),
        /** The members' snapshot streams, comma separated (Aeron's 106,107). */
        REPLICA_SNAPSHOT_STREAM_IDS("replica.snapshot.stream.ids"),
        SHADOW_TERM_LENGTH("shadow.term.length"),
        SHADOW_WRITE_CONNECT_TIMEOUT_MS("shadow.write.connect.timeout.ms"),
        SHADOW_WRITE_OFFER_TIMEOUT_MS("shadow.write.offer.timeout.ms"),
        SHADOW_WRITE_RECORD_TIMEOUT_MS("shadow.write.record.timeout.ms"),
        /** Replay-below-start refusals in a row before a re-bootstrap; 0 counts and never acts. */
        SHADOW_REBOOTSTRAP_REPLAY_FAILURES("shadow.rebootstrap.replay.failures"),
        /** Re-bootstraps allowed inside {@code SHADOW_REBOOTSTRAP_WINDOW_MS} (the loop guard). */
        SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE("shadow.rebootstrap.max.consecutive"),
        /** The loop guard's window. */
        SHADOW_REBOOTSTRAP_WINDOW_MS("shadow.rebootstrap.window.ms"),
        /** Moved-aside {@code cluster.rebootstrap-*} dirs kept. */
        SHADOW_REBOOTSTRAP_KEEP("shadow.rebootstrap.keep"),
        /** At most one {@code re-bootstrap not taken} line per interval. */
        SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS("shadow.rebootstrap.not.taken.log.ms");

        private final String property;

        Key(final String property) {
            this.property = property;
        }

        public String property() {
            return property;
        }
    }

    static final String TMPFS_WARNING = "WARNING: CORE REPLICA STATE ON TMPFS: the replica's shadows and its"
            + " log copy will not survive a power loss; set BASE_DIR to a disk path for anything beyond dev";

    private static final int[] REPLICA_PORT_OFFSETS = {
            ClusterPorts.ARCHIVE_CONTROL_OFFSET, ClusterPorts.MEMBER_FACING_OFFSET, ClusterPorts.LOG_OFFSET,
            ClusterPorts.TRANSFER_OFFSET,
    };
    private static final int[] MEMBER_PORT_OFFSETS = {
            ClusterPorts.ARCHIVE_CONTROL_OFFSET, ClusterPorts.CLIENT_FACING_OFFSET, ClusterPorts.MEMBER_FACING_OFFSET,
            ClusterPorts.LOG_OFFSET, ClusterPorts.TRANSFER_OFFSET,
    };

    /** The production source: env (non-empty) then system property; the credential in the kit's order. */
    static final SnapshotModeSettings.Source ENVIRONMENT = (env, prop) -> {
        if (Key.CORE_AUTH_CREDENTIAL.name().equals(env)) {
            final String p = System.getProperty(prop);
            if (p != null && !p.isBlank()) {
                return p;
            }
            final String e = System.getenv(env);
            return e == null || e.isBlank() ? null : e;
        }
        return SnapshotModeSettings.Source.ENVIRONMENT.get(env, prop);
    };

    private final SnapshotModeSettings.Source source;
    private java.util.Set<Integer> snapshotStreamIds;
    private final Map<Key, Long> numbers = new EnumMap<>(Key.class);
    private ReplicaConfig.DriverMode driverMode;
    private boolean busySpin;
    private ClusterBackup.SourceType sourceType;
    private List<String> memberHosts;
    private String replicaAddress;
    private String aeronDir;
    private File baseDir;
    private File mismatchDir;
    private String catchupRcvbuf;
    private String catchupSndbuf;
    private String catchupRcvWnd;
    private String shadowTermLength;
    private ClusterCredentials credentials;
    private SnapshotModeSettings shared;
    private ReplicaConfig config;

    private ReplicaSettings(final SnapshotModeSettings.Source source) {
        this.source = source;
    }

    /** The settings of this process; prints a refusal to stderr and rethrows it. */
    public static ReplicaSettings enforce() {
        try {
            final ReplicaSettings s = parse(ENVIRONMENT);
            s.requireMismatchDirWritable();
            System.out.println("[REPLICA-BOOT-GUARD] ok " + s);
            return s;
        } catch (final Refused refused) {
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    /** @throws Refused with {@value #PREFIX} and the refusal text of the first bad setting */
    public static ReplicaSettings parse(final SnapshotModeSettings.Source source) {
        final ReplicaSettings s = new ReplicaSettings(Objects.requireNonNull(source, "source"));
        try {
            s.read();
        } catch (final IllegalArgumentException | IllegalStateException bad) {
            throw new Refused(bad.getMessage(), bad);
        }
        return s;
    }

    /**
     * The boot check of {@code REPLICA_MISMATCH_DIR}: created and a probe file written and removed, so a mismatch
     * dump never meets a missing or read-only dir at the worst moment. Separate from {@link #parse} (which has no side
     * effect on the file system); {@link #enforce} runs it before any Aeron component. The check is the kit's
     * ({@link MismatchDump#requireWritable}); this adds the {@value #PREFIX} prefix.
     *
     * @throws Refused {@code REPLICA_MISMATCH_DIR is not writable: <path>}
     */
    public void requireMismatchDirWritable() {
        try {
            MismatchDump.requireWritable(config.mismatchDir());
        } catch (final IllegalArgumentException notWritable) {
            throw new Refused(notWritable.getMessage(), notWritable.getCause());
        }
    }

    /** {@code REPLICA_MISMATCH_DIR} as the kit resolves it ({@code BASE_DIR/mismatch} when unset). */
    public File mismatchDir() {
        return config.mismatchDir();
    }

    // ------------------------------------------------------------------------------------------------ the table

    private void read() {
        driverMode = choice(Key.TRANSPORT_DRIVER_MODE, "embedded", "external|embedded",
                v -> ReplicaConfig.DriverMode.valueOf(v.toUpperCase(Locale.ROOT)));
        integer(Key.TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS, ReplicaConfig.DEFAULT_EXTERNAL_DRIVER_TIMEOUT_MS, 1,
                86_400_000L);
        busySpin = choice(Key.TRANSPORT_IDLE_MODE, "backoff", "busy_spin|backoff", v -> {
            switch (v.toLowerCase(Locale.ROOT)) {
                case "busy_spin":
                    return Boolean.TRUE;
                case "backoff":
                    return Boolean.FALSE;
                default:
                    throw new IllegalArgumentException(v);
            }
        });

        memberHosts = hosts();
        sourceType = choice(Key.REPLICA_BACKUP_SOURCE, "FOLLOWER", "FOLLOWER|LEADER|ANY",
                v -> ClusterBackup.SourceType.valueOf(v.toUpperCase(Locale.ROOT)));
        if (sourceType == ClusterBackup.SourceType.FOLLOWER && memberHosts.size() < 2) {
            // LogSourceValidator(FOLLOWER) excludes the sole member, PublicationGroup then spins
            throw new IllegalArgumentException("REPLICA_BACKUP_SOURCE=FOLLOWER needs >= 2 members; use ANY");
        }

        final long base = integer(Key.CLUSTER_PORT_BASE, 9300, 1, 65_530);
        final long stride = integer(Key.CLUSTER_PORT_STRIDE, ClusterPorts.DEFAULT_STRIDE, 0, 10_000);
        final long slot = integer(Key.CLUSTER_REPLICA_SLOT, 3, 0, 1000);
        final long top = ClusterPorts.port((int)slot, (int)base, ClusterPorts.TRANSFER_OFFSET, (int)stride);
        if (top > 65_535) {
            throw new IllegalArgumentException("replica port " + top + " (CLUSTER_PORT_BASE=" + base + " + slot " + slot
                    + " x CLUSTER_PORT_STRIDE=" + stride + " + " + ClusterPorts.TRANSFER_OFFSET + ") is above 65535");
        }
        replicaAddress = resolveReplicaAddress();
        collision((int)base, (int)stride, (int)slot);
        integer(Key.CLUSTER_ID, 0, 0, Integer.MAX_VALUE);
        integer(Key.CORE_NODE_PORT, 9700 + slot, 1, 65_535);

        final String dir = raw(Key.AERON_DIR);
        aeronDir = dir != null ? dir : CommonContext.getAeronDirectoryName() + "-core-replica" + slot + "-driver";
        final String bd = raw(Key.BASE_DIR);
        baseDir = bd != null ? new File(bd) : new File(System.getProperty("user.dir"), "core-replica" + slot);

        credentials = credential();

        final long window = integer(Key.REPLICA_MEMBER_BACKUP_WINDOW_MS, ReplicaConfig.DEFAULT_MEMBER_BACKUP_WINDOW_MS,
                1000, 3_600_000L);
        integer(Key.REPLICA_BACKUP_COOLDOWN_MS, Math.min(window + 1000, 3_600_000L), 100, 3_600_000L);
        integer(Key.REPLICA_BACKUP_RESPONSE_TIMEOUT_MS, ReplicaConfig.DEFAULT_BACKUP_RESPONSE_TIMEOUT_MS, 100, 600_000L);
        integer(Key.REPLICA_BACKUP_QUERY_INTERVAL_MS, ReplicaConfig.DEFAULT_BACKUP_QUERY_INTERVAL_MS, 100,
                86_400_000L);
        integer(Key.REPLICA_BACKUP_PROGRESS_TIMEOUT_MS, ReplicaConfig.DEFAULT_BACKUP_PROGRESS_TIMEOUT_MS, 1000,
                600_000L);
        integer(Key.REPLICA_IMAGE_LIVENESS_MS, ReplicaConfig.DEFAULT_IMAGE_LIVENESS_MS, 500, 60_000L);
        catchupRcvbuf = size(Key.REPLICA_CATCHUP_RCVBUF, ReplicaConfig.DEFAULT_CATCHUP_RCVBUF, 64 * 1024,
                64 * 1024 * 1024, false);
        catchupRcvWnd = size(Key.REPLICA_CATCHUP_RCV_WND, ReplicaConfig.DEFAULT_CATCHUP_RCV_WND, 64 * 1024,
                64 * 1024 * 1024, false);
        // Settings, not hard-coded values; the defaults are the old literals
        catchupSndbuf = size(Key.REPLICA_CATCHUP_SNDBUF, ReplicaConfig.DEFAULT_CATCHUP_SNDBUF, 64 * 1024,
                64 * 1024 * 1024, false);
        final long mtu = integer(Key.REPLICA_DRIVER_MTU, ReplicaConfig.DEFAULT_DRIVER_MTU, 1024, 65_504);
        if (mtu % 32 != 0) {
            throw new IllegalArgumentException(Key.REPLICA_DRIVER_MTU.name() + "=" + mtu + " is not a multiple of 32");
        }
        // the default is the kit's constant, never a second literal
        size(Key.REPLICA_DRIVER_TERM_LENGTH, Integer.toString(ReplicaConfig.DEFAULT_DRIVER_TERM_LENGTH), 64 * 1024,
                1024 * 1024 * 1024, true);
        integer(Key.REPLICA_SHADOW_BUFFER_MB, ReplicaConfig.DEFAULT_SHADOW_BUFFER_MB, 16,
                ReplicaConfig.MAX_SHADOW_BUFFER_MB);
        integer(Key.REPLICA_ALIGNED_WAIT_MS, ReplicaConfig.DEFAULT_ALIGNED_WAIT_MS, 1000, 3_600_000L);

        final long standby = integer(Key.SNAPSHOT_STANDBY_INTERVAL_MINUTES,
                com.openexchange.cluster.SnapshotCadence.DEFAULT_STANDBY_INTERVAL_MINUTES, 0,
                com.openexchange.cluster.SnapshotCadence.MAX_STANDBY_INTERVAL_MINUTES);
        integer(Key.SHADOW_STANDBY_FALLBACK_MINUTES, Math.min(2 * standby, 1440), 0, 1440);
        integer(Key.REPLICA_RETRIEVED_SETS_KEEP, ReplicaConfig.DEFAULT_RETRIEVED_SETS_KEEP, 1, 100);
        size(Key.TRANSPORT_LOG_TERM_LENGTH, "64m", 64 * 1024, 1024 * 1024 * 1024, true);
        integer(Key.REPLICA_READY_MAX_LAG_BYTES, numbers.get(Key.TRANSPORT_LOG_TERM_LENGTH), 4096, 1L << 40);
        integer(Key.REPLICA_READY_SHADOW_AGE_MS, Math.min(2 * standby * 60_000L, 86_400_000L), 0, 86_400_000L);
        final long ready = integer(Key.REPLICA_READY_STALE_MS, ReplicaConfig.DEFAULT_READY_STALE_MS, 100, 3_600_000L);
        final long liveness = integer(Key.REPLICA_LIVENESS_STALE_MS, ReplicaConfig.DEFAULT_LIVENESS_STALE_MS, 100,
                3_600_000L);
        if (liveness < ready) {
            throw new IllegalArgumentException("REPLICA_LIVENESS_STALE_MS=" + liveness
                    + " must be >= REPLICA_READY_STALE_MS=" + ready);
        }
        integer(Key.REPLICA_READY_TIME_LAG_MS, ReplicaConfig.DEFAULT_READY_TIME_LAG_MS, 0, 86_400_000L);
        integer(Key.REPLICA_ATTACH_BACKOFF_MS, ReplicaConfig.DEFAULT_ATTACH_BACKOFF_MS, 1,
                ReplicaConfig.MAX_ATTACH_BACKOFF_MS);
        integer(Key.REPLICA_BOOTSTRAP_WAIT_MS, ReplicaConfig.DEFAULT_BOOTSTRAP_WAIT_MS, 1000, 86_400_000L);
        // the verify wait and the cluster stream ids are settings, never literals; the verify wait is never shorter
        // than one backup cycle; the range max covers 2 x the largest query interval plus
        // the largest cool-down, so every accepted interval has an accepted bound
        final long verifyWait = integer(Key.REPLICA_VERIFY_WAIT_MS, ReplicaConfig.DEFAULT_VERIFY_WAIT_MS, 1000,
                MAX_VERIFY_WAIT_MS);
        final String cycle = ReplicaConfig.verifyWaitShorterThanOneBackupCycle(verifyWait,
                numbers.get(Key.REPLICA_BACKUP_QUERY_INTERVAL_MS), numbers.get(Key.REPLICA_BACKUP_COOLDOWN_MS));
        if (cycle != null) {
            throw new IllegalArgumentException(cycle);
        }
        // the live oracle's settings: the kit's refusal texts, one source for both layers
        checked(Key.REPLICA_MISMATCH_HOLD_MS, ReplicaConfig.DEFAULT_MISMATCH_HOLD_MS,
                ReplicaConfig::mismatchHoldRefusal);
        final String md = raw(Key.REPLICA_MISMATCH_DIR);
        mismatchDir = md != null ? new File(md) : null;
        checked(Key.REPLICA_MISMATCH_DUMP_MAX_MB, ReplicaConfig.DEFAULT_MISMATCH_DUMP_MAX_MB,
                ReplicaConfig::mismatchDumpMaxMbRefusal);
        checked(Key.REPLICA_FENCE_PURGE_WAIT_MS, ReplicaConfig.DEFAULT_FENCE_PURGE_WAIT_MS,
                ReplicaConfig::fencePurgeWaitRefusal);
        final long cadence = checked(Key.REPLICA_MEMBER_CADENCE_MS, ReplicaConfig.DEFAULT_MEMBER_CADENCE_MS,
                ReplicaConfig::memberCadenceRefusal);
        final String age = ReplicaConfig.readyShadowAgeBelowCadence(numbers.get(Key.REPLICA_READY_SHADOW_AGE_MS),
                cadence, verifyWait);
        if (age != null) {
            throw new IllegalArgumentException(age);
        }
        final long logStream = integer(Key.REPLICA_LOG_STREAM_ID, ReplicaConfig.DEFAULT_LOG_STREAM_ID, 1,
                Integer.MAX_VALUE);
        snapshotStreamIds = streamIds(Key.REPLICA_SNAPSHOT_STREAM_IDS, ReplicaConfig.DEFAULT_SNAPSHOT_STREAM_IDS,
                (int)logStream);
        shadowTermLength = size(Key.SHADOW_TERM_LENGTH, ReplicaConfig.DEFAULT_SHADOW_TERM_LENGTH, 64 * 1024,
                16 * 1024 * 1024, true);
        integer(Key.SHADOW_WRITE_CONNECT_TIMEOUT_MS, ReplicaConfig.DEFAULT_WRITE_CONNECT_TIMEOUT_MS, 1000, 3_600_000L);
        integer(Key.SHADOW_WRITE_OFFER_TIMEOUT_MS, ReplicaConfig.DEFAULT_WRITE_OFFER_TIMEOUT_MS, 1000, 3_600_000L);
        integer(Key.SHADOW_WRITE_RECORD_TIMEOUT_MS, ReplicaConfig.DEFAULT_WRITE_RECORD_TIMEOUT_MS, 1000, 3_600_000L);

        // re-bootstrap (members pruned past this replica's log): the kit's ranges and refusal texts
        checked(Key.SHADOW_REBOOTSTRAP_REPLAY_FAILURES, ReplicaConfig.DEFAULT_REBOOTSTRAP_REPLAY_FAILURES,
                ReplicaConfig::rebootstrapReplayFailuresRefusal);
        checked(Key.SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE, ReplicaConfig.DEFAULT_REBOOTSTRAP_MAX_CONSECUTIVE,
                ReplicaConfig::rebootstrapMaxConsecutiveRefusal);
        checked(Key.SHADOW_REBOOTSTRAP_WINDOW_MS, ReplicaConfig.DEFAULT_REBOOTSTRAP_WINDOW_MS,
                ReplicaConfig::rebootstrapWindowRefusal);
        checked(Key.SHADOW_REBOOTSTRAP_KEEP, ReplicaConfig.DEFAULT_REBOOTSTRAP_KEEP,
                ReplicaConfig::rebootstrapKeepRefusal);
        checked(Key.SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS, ReplicaConfig.DEFAULT_REBOOTSTRAP_NOT_TAKEN_LOG_MS,
                ReplicaConfig::rebootstrapNotTakenLogRefusal);

        // the shared shadow keys and the SHADOW_PULL_* rows (the member table, the kit's one pull parser); the kit
        // refuses a source list that names this replica's own archive endpoint
        shared = SnapshotModeSettings.parse(source);
        // the kit re-checks structure; a refusal there also carries this prefix
        config = configBuilder().build();
    }

    /** A comma separated list of stream ids, each in [1, 2^31-1], none equal to the log stream. */
    private static java.util.Set<Integer> parseStreamIds(final String name, final String text, final int logStream) {
        final java.util.Set<Integer> out = new java.util.TreeSet<>();
        for (final String part : text.split(",")) {
            final long id;
            try {
                id = Long.parseLong(part.trim());
            } catch (final NumberFormatException notANumber) {
                throw new IllegalArgumentException(name + "='" + text + "' is not a list of stream ids");
            }
            if (id < 1 || id > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(name + "=" + text + " out of range [1," + Integer.MAX_VALUE + "]");
            }
            out.add((int)id);
        }
        if (out.contains(logStream)) {
            throw new IllegalArgumentException(name + "=" + text + " names the log stream " + logStream);
        }
        return java.util.Set.copyOf(out);
    }

    private java.util.Set<Integer> streamIds(final Key key, final java.util.Set<Integer> defaults,
                                             final int logStream) {
        final String v = raw(key);
        if (v == null) {
            if (defaults.contains(logStream)) {
                throw new IllegalArgumentException(key.name() + " default " + defaults + " names the log stream "
                        + logStream + " (set " + key.name() + ")");
            }
            return defaults;
        }
        return parseStreamIds(key.name(), v.replace(" ", ""), logStream);
    }

    private String raw(final Key key) {
        final String v = source.get(key.name(), key.property());
        return v == null || v.isBlank() ? null : v.trim();
    }

    private long integer(final Key key, final long defaultValue, final long min, final long max) {
        final String v = raw(key);
        final long value;
        if (v == null) {
            value = defaultValue;
        } else {
            try {
                value = Long.parseLong(v);
            } catch (final NumberFormatException notANumber) {
                throw new IllegalArgumentException(key.name() + "='" + v + "' is not an integer");
            }
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(key.name() + "=" + value + " out of range [" + min + "," + max + "]");
        }
        numbers.put(key, value);
        return value;
    }

    /** An integer whose range and refusal text are the kit's ({@code refusal} returns null when valid). */
    private long checked(final Key key, final long defaultValue,
                         final java.util.function.LongFunction<String> refusal) {
        final String v = raw(key);
        final long value;
        if (v == null) {
            value = defaultValue;
        } else {
            try {
                value = Long.parseLong(v);
            } catch (final NumberFormatException notANumber) {
                throw new IllegalArgumentException(key.name() + "='" + v + "' is not an integer");
            }
        }
        final String refused = refusal.apply(value);
        if (refused != null) {
            throw new IllegalArgumentException(refused);
        }
        numbers.put(key, value);
        return value;
    }

    /** A byte size ({@code 4m}, {@code 65536}); returns the text the channel or the writer takes. */
    private String size(final Key key, final String defaultText, final long min, final long max,
                        final boolean powerOfTwo) {
        final String v = raw(key);
        final String text = v == null ? defaultText : v;
        final long value;
        try {
            value = SystemUtil.parseSize(key.name(), text);
        } catch (final NumberFormatException notASize) {
            throw new IllegalArgumentException(key.name() + "='" + text + "' is not a size (bytes, or a k/m/g suffix)");
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(key.name() + "=" + value + " out of range [" + min + "," + max + "]");
        }
        if (powerOfTwo && Long.bitCount(value) != 1) {
            throw new IllegalArgumentException(key.name() + "=" + text + " is not a power of two");
        }
        numbers.put(key, value);
        return text;
    }

    private <T> T choice(final Key key, final String defaultText, final String expected,
                         final java.util.function.Function<String, T> parse) {
        final String v = raw(key);
        final String text = v == null ? defaultText : v;
        try {
            return parse.apply(text);
        } catch (final IllegalArgumentException unknown) {
            throw new IllegalArgumentException(key.name() + " '" + text + "' (expected " + expected + ")");
        }
    }

    private List<String> hosts() {
        final String v = raw(Key.CLUSTER_ADDRESSES);
        if (v == null) {
            throw new IllegalArgumentException("CLUSTER_ADDRESSES is required (one host per member)");
        }
        final List<String> out = new ArrayList<>();
        for (final String h : v.split(",", -1)) {
            if (h.isBlank()) {
                throw new IllegalArgumentException("CLUSTER_ADDRESSES has an empty entry: '" + v + "'");
            }
            out.add(h.trim());
        }
        return List.copyOf(out);
    }

    private static boolean local(final String host) {
        return "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host);
    }

    /** A fallback with a stated precondition, never a silent default for a distributed cluster. */
    private String resolveReplicaAddress() {
        final String v = raw(Key.CLUSTER_REPLICA_ADDRESS);
        if (v != null) {
            return v;
        }
        for (final String h : memberHosts) {
            if (!local(h)) {
                throw new IllegalArgumentException("CLUSTER_REPLICA_ADDRESS is required when members are not local");
            }
        }
        return "localhost";
    }

    private static String hostKey(final String host) {
        return local(host) || host.startsWith("127.") ? "loopback" : host.toLowerCase(Locale.ROOT);
    }

    /** A replica port on a member's host must not be one of that member's ports (the bind would fail later, less clearly). */
    private void collision(final int base, final int stride, final int slot) {
        final String replicaHost = hostKey(replicaAddress);
        for (int m = 0; m < memberHosts.size(); m++) {
            if (!hostKey(memberHosts.get(m)).equals(replicaHost)) {
                continue;
            }
            for (final int ro : REPLICA_PORT_OFFSETS) {
                final int rp = ClusterPorts.port(slot, base, ro, stride);
                for (final int mo : MEMBER_PORT_OFFSETS) {
                    if (rp == ClusterPorts.port(m, base, mo, stride)) {
                        throw new IllegalArgumentException("CLUSTER_REPLICA_SLOT=" + slot + " collides with member " + m
                                + " on " + memberHosts.get(m) + " (port " + rp + "); " + (stride == 0
                                ? "co-located processes need CLUSTER_PORT_STRIDE > 0"
                                : "use a slot >= " + memberHosts.size()));
                    }
                }
            }
        }
    }

    /** The kit's client start-up rule ({@code ClusterCredentials.requireFromEnvironment}, the same texts). */
    private ClusterCredentials credential() {
        final String env = CoreAuth.POLICY.credentialEnv();
        final String prop = CoreAuth.POLICY.credentialProperty();
        final String spec = raw(Key.CORE_AUTH_CREDENTIAL);
        if (spec == null) {
            throw new IllegalStateException(env + " (-D" + prop + ") is not set; this client cannot authenticate to the "
                    + CoreAuth.POLICY.engine() + " cluster");
        }
        final ClusterCredentials c;
        try {
            c = ClusterCredentials.fromSpec(spec);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException(env + " (-D" + prop + ") is malformed: " + e.getMessage());
        }
        if (c.sessionClass() != ClusterSessionClass.REPLICA) {
            throw new IllegalStateException(env + " claims class " + c.sessionClass() + " (keyId " + c.keyId()
                    + "); this client must present [" + ClusterSessionClass.REPLICA + "]");
        }
        return c;
    }

    // ------------------------------------------------------------------------------------------------ results

    /** The value of a numeric or size setting (sizes in bytes). */
    public long numeric(final Key key) {
        final Long v = numbers.get(key);
        if (v == null) {
            throw new IllegalArgumentException(key + " is not a numeric setting");
        }
        return v;
    }

    private int intOf(final Key key) {
        return Math.toIntExact(numeric(key));
    }

    public ReplicaConfig.DriverMode driverMode() {
        return driverMode;
    }

    public boolean busySpin() {
        return busySpin;
    }

    public ClusterBackup.SourceType sourceType() {
        return sourceType;
    }

    public List<String> memberHosts() {
        return memberHosts;
    }

    public int slot() {
        return intOf(Key.CLUSTER_REPLICA_SLOT);
    }

    public int nodePort() {
        return intOf(Key.CORE_NODE_PORT);
    }

    public String replicaAddress() {
        return replicaAddress;
    }

    public String aeronDir() {
        return aeronDir;
    }

    public File baseDir() {
        return baseDir;
    }

    public String catchupRcvbuf() {
        return catchupRcvbuf;
    }

    public String catchupRcvWnd() {
        return catchupRcvWnd;
    }

    public String shadowTermLength() {
        return shadowTermLength;
    }

    /** The REPLICA credential. */
    public ClusterCredentials credentials() {
        return credentials;
    }

    /** The shared shadow keys, the member shadow table. */
    public SnapshotModeSettings shared() {
        return shared;
    }

    /**
     * A fresh kit builder with every setting applied and no test hooks; the in-process tests add
     * {@code testHooks(...)} before {@code build()}.
     */
    public ReplicaConfig.Builder configBuilder() {
        return ReplicaConfig.builder()
                .memberHosts(memberHosts)
                .portBase(intOf(Key.CLUSTER_PORT_BASE))
                .portStride(intOf(Key.CLUSTER_PORT_STRIDE))
                .slot(slot())
                .replicaAddress(replicaAddress)
                .baseDir(baseDir)
                .clusterId(intOf(Key.CLUSTER_ID))
                .engineId(CoreLogApplier.ENGINE_ID_CORE)
                .driverMode(driverMode)
                .aeronDir(aeronDir)
                .externalDriverTimeoutMs(numeric(Key.TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS))
                .busySpin(busySpin)
                .sourceType(sourceType)
                .credentials(credentials.supplier())
                .memberBackupWindowMs(numeric(Key.REPLICA_MEMBER_BACKUP_WINDOW_MS))
                .backupCooldownMs(numeric(Key.REPLICA_BACKUP_COOLDOWN_MS))
                .backupResponseTimeoutMs(numeric(Key.REPLICA_BACKUP_RESPONSE_TIMEOUT_MS))
                .backupQueryIntervalMs(numeric(Key.REPLICA_BACKUP_QUERY_INTERVAL_MS))
                .backupProgressTimeoutMs(numeric(Key.REPLICA_BACKUP_PROGRESS_TIMEOUT_MS))
                .imageLivenessMs(numeric(Key.REPLICA_IMAGE_LIVENESS_MS))
                .catchupRcvbuf(catchupRcvbuf)
                .catchupSndbuf(catchupSndbuf)
                .driverMtu(intOf(Key.REPLICA_DRIVER_MTU))
                .driverTermLength(intOf(Key.REPLICA_DRIVER_TERM_LENGTH))
                .catchupRcvWnd(catchupRcvWnd)
                .shadowBufferMb(intOf(Key.REPLICA_SHADOW_BUFFER_MB))
                .alignedWaitMs(numeric(Key.REPLICA_ALIGNED_WAIT_MS))
                .standbyFallbackMs(numeric(Key.SHADOW_STANDBY_FALLBACK_MINUTES) * 60_000L)
                .retrievedSetsKeep(intOf(Key.REPLICA_RETRIEVED_SETS_KEEP))
                .readyMaxLagBytes(numeric(Key.REPLICA_READY_MAX_LAG_BYTES))
                .readyShadowAgeMs(numeric(Key.REPLICA_READY_SHADOW_AGE_MS))
                .readyStaleMs(numeric(Key.REPLICA_READY_STALE_MS))
                .livenessStaleMs(numeric(Key.REPLICA_LIVENESS_STALE_MS))
                .readyTimeLagMs(numeric(Key.REPLICA_READY_TIME_LAG_MS))
                .attachBackoffMs(numeric(Key.REPLICA_ATTACH_BACKOFF_MS))
                .bootstrapWaitMs(numeric(Key.REPLICA_BOOTSTRAP_WAIT_MS))
                .verifyWaitMs(numeric(Key.REPLICA_VERIFY_WAIT_MS))
                .mismatchHoldMs(numeric(Key.REPLICA_MISMATCH_HOLD_MS))
                .mismatchDir(mismatchDir)
                .mismatchDumpMaxMb(intOf(Key.REPLICA_MISMATCH_DUMP_MAX_MB))
                .fencePurgeWaitMs(numeric(Key.REPLICA_FENCE_PURGE_WAIT_MS))
                .memberCadenceMs(numeric(Key.REPLICA_MEMBER_CADENCE_MS))
                .logStreamId(intOf(Key.REPLICA_LOG_STREAM_ID))
                .snapshotStreamIds(snapshotStreamIds)
                .shadowTermLength(shadowTermLength)
                .writeConnectTimeoutMs(numeric(Key.SHADOW_WRITE_CONNECT_TIMEOUT_MS))
                .writeOfferTimeoutMs(numeric(Key.SHADOW_WRITE_OFFER_TIMEOUT_MS))
                .writeRecordTimeoutMs(numeric(Key.SHADOW_WRITE_RECORD_TIMEOUT_MS))
                .retainN(shared.retainN())
                .floorMargin(shared.floorMargin())
                .deepReadPaceMs(shared.deepReadPaceMs())
                .scanPeriodMs(shared.scanPeriodMs())
                .loadTimeoutMs(shared.loadTimeoutMs())
                .pullSettings(shared.pullSettings())
                .rebootstrapReplayFailures(intOf(Key.SHADOW_REBOOTSTRAP_REPLAY_FAILURES))
                .rebootstrapMaxConsecutive(intOf(Key.SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE))
                .rebootstrapWindowMs(numeric(Key.SHADOW_REBOOTSTRAP_WINDOW_MS))
                .rebootstrapKeep(intOf(Key.SHADOW_REBOOTSTRAP_KEEP))
                .rebootstrapNotTakenLogMs(numeric(Key.SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS))
                .nodePort(nodePort());
    }

    /** The production kit config (built once, at parse time, so its refusals carry the prefix too). */
    public ReplicaConfig config() {
        return config;
    }

    /** Lines the process prints at start that are not refusals: the backup-window CRITICAL lines and the tmpfs one. */
    public List<String> warnings() {
        final List<String> lines = new ArrayList<>(ReplicaContexts.windowWarnings(config));
        lines.addAll(storageWarnings());
        return lines;
    }

    /**
     * The puller's CRITICAL boot lines ({@code shadow puller disabled: ...}, a kinds widening), printed by the main
     * next to {@link #warnings}.
     */
    List<String> pullWarnings() {
        final List<String> out = new ArrayList<>();
        for (final String line : shared.pullSettings().criticalLines()) {
            out.add("CRITICAL " + line);
        }
        return out;
    }

    /** The BASE_DIR tmpfs warning only (the kit prints the window lines itself at launch). */
    List<String> storageWarnings() {
        return StatePaths.looksEphemeral(baseDir.getAbsolutePath()) ? List.of(TMPFS_WARNING) : List.of();
    }

    /** Never prints the credential secret. */
    @Override
    public String toString() {
        return "ReplicaSettings{members=" + memberHosts + " replicaAddress=" + replicaAddress + " slot=" + slot()
                + " driverMode=" + driverMode + " aeronDir=" + aeronDir + " baseDir=" + baseDir + " source=" + sourceType
                + " credential=keyId " + credentials.keyId() + " class " + credentials.sessionClass() + " numbers="
                + numbers + " catchup=" + catchupRcvbuf + "/" + catchupRcvWnd + " shadowTerm=" + shadowTermLength
                + " shared={" + shared + "}}";
    }
}

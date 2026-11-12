// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.SnapshotCadence;

/**
 * The boot refusal of stub mode without a floor. Runs first in {@code AeronCluster.run}, before
 * the service is constructed from the settings it returns and before any Aeron component launches, so it reads
 * settings only: no archive is open here and a stub payload is inside an archive recording (review F1). The clusterId
 * of the newest stub is checked in {@code onStart}, where the stub bytes are readable (3.3).
 *
 * <table>
 *   <caption>Decision table</caption>
 *   <tr><td>guard row 1</td><td>any setting of {@link SnapshotModeSettings} fails its range</td><td>refuse with its 3.5 text (every
 *       mode)</td></tr>
 *   <tr><td>guard row 2</td><td>{@code SNAPSHOT_MODE=shadow}, {@code SNAPSHOT_PRUNE_ENABLED=true} and
 *       {@code SHADOW_FLOOR_ENABLED=false}</td><td>refuse, unless {@value #UNCHECKED_PROPERTY} is true (a
 *       test-only override, the {@code ShadowSelfCheck} pattern)</td></tr>
 *   <tr><td>guard row 3</td><td>{@code SHADOW_PULL_SOURCES} names this node's own archive control endpoint
 *       ({@link #enforcePullSources}, once the endpoint is known)</td><td>refuse with the kit's text</td></tr>
 *   <tr><td>guard row 4</td><td>{@code SHADOW_ANCHOR_RULE=any-verified}, an empty {@code SHADOW_PULL_SOURCES},
 *       {@code SHADOW_PULL_KINDS} wider than ALIGNED</td><td>allowed, one {@code [BOOT-GUARD] CRITICAL: } line
 *       each</td></tr>
 *   <tr><td>guard row 5</td><td>a {@code SHADOW_PULL_SOURCES} entry resolves to a member's archive endpoint under
 *       another spelling ({@link #enforcePullSources} with the members table)</td><td>refuse with the kit's text; an
 *       entry that does not resolve is one {@code [BOOT-GUARD] CRITICAL: } line and kept; the boot line classifies
 *       each source</td></tr>
 *   <tr><td>guard row 6</td><td>the log channel settings ({@code TRANSPORT_LOG_RCV_WINDOW},
 *       {@code TRANSPORT_LOG_SO_RCVBUF}, {@code TRANSPORT_LOG_SO_SNDBUF}) or {@code CORE_DRIVER_DIR_ACTIVE_MS} fail
 *       their range, or the kernel grants a log socket buffer smaller than configured ({@link #enforceTransport}, every
 *       mode and both driver modes)</td><td>refuse with the setting's text or the probe's text naming the sysctl</td></tr>
 *   <tr><td>guard row OK</td><td>otherwise</td><td>{@code [BOOT-GUARD] snapshotMode=<mode> prune=<bool> floor=wired}</td></tr>
 * </table>
 *
 * <p>The runtime second net is in {@code CoreClusteredService.startDurability}: in shadow mode the pruner's
 * watermark must be the {@code ShadowFloor} instance ({@code SnapshotLogPruner.watermarkIs}).</p>
 */
public final class StubModeBootGuard {

    /** Lets {@code SHADOW_FLOOR_ENABLED=false} through guard row 2; hosts never set it (11.6 control only). */
    public static final String UNCHECKED_PROPERTY = "oxs.shadow.floor.unchecked";

    private StubModeBootGuard() {
    }

    /** The guard over this process's environment. */
    public static SnapshotModeSettings enforce() {
        return enforce(SnapshotModeSettings.Source.ENVIRONMENT, SnapshotCadence.booleanFromEnv(
                "SNAPSHOT_PRUNE_ENABLED", "snapshot.prune.enabled", true));
    }

    /**
     * @return the parsed settings when the boot may proceed
     * @throws StandbySnapshotBootGuard.Refused with {@code CORE BOOT GUARD: } and the reason
     */
    public static SnapshotModeSettings enforce(final SnapshotModeSettings.Source source, final boolean pruneEnabled) {
        try {
            return check(source, pruneEnabled);
        } catch (final StandbySnapshotBootGuard.Refused refused) {
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    /**
     * Guard row 3: the pull sources against this node's own archive control endpoint, run once the cluster
     * configuration named it and before any Aeron component launches.
     *
     * @param ownArchiveEndpoint {@code host:port} of this node's archive control channel
     * @throws StandbySnapshotBootGuard.Refused with {@code CORE BOOT GUARD: } and the kit's text
     */
    public static void enforcePullSources(final SnapshotModeSettings settings, final String ownArchiveEndpoint) {
        enforcePullSources(settings, ownArchiveEndpoint, null);
    }

    /**
     * Guard rows 3 and 5: the own endpoint, then the spelling of every source against the members table.
     *
     * @param memberArchiveEndpoints member id to archive control endpoint as the members table spells it; null skips row 5
     */
    public static void enforcePullSources(final SnapshotModeSettings settings, final String ownArchiveEndpoint,
                                          final java.util.Map<Integer, String> memberArchiveEndpoints) {
        try {
            final com.openexchange.cluster.shadow.ShadowPullSettings ps = settings.pullSettingsFor(ownArchiveEndpoint);
            if (memberArchiveEndpoints != null && ps.enabled()) {
                final com.openexchange.cluster.shadow.ShadowPullSettings.SpellingCheck check =
                        ps.checkSpelling(memberArchiveEndpoints);
                for (final String line : check.criticalLines()) {
                    System.err.println("[BOOT-GUARD] CRITICAL: " + line);
                }
                System.out.println("[BOOT-GUARD] " + check.bootLine());
            }
        } catch (final IllegalArgumentException ownEndpoint) {
            final StandbySnapshotBootGuard.Refused refused =
                    new StandbySnapshotBootGuard.Refused(ownEndpoint.getMessage(), ownEndpoint);
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    /** The members table of a {@code clusterMembers} string: member id to its archive control endpoint. */
    public static java.util.Map<Integer, String> memberArchiveEndpoints(final String clusterMembers) {
        final java.util.Map<Integer, String> out = new java.util.TreeMap<>();
        if (clusterMembers == null || clusterMembers.isBlank()) {
            return out;
        }
        for (final io.aeron.cluster.ClusterMember m : io.aeron.cluster.ClusterMember.parse(clusterMembers)) {
            out.put(m.id(), m.archiveEndpoint());
        }
        return out;
    }

    /** What guard row 6 accepted: the log channel settings and the embedded driver directory bound. */
    public record Transport(com.openexchange.core.infrastructure.TransportConfig.LogTransport log, long driverDirActiveMs) {
    }

    /** Guard row 6 over this process's environment and the real kernel. */
    public static Transport enforceTransport() {
        return enforceTransport(com.openexchange.core.infrastructure.TransportConfig::settingFromEnv,
                com.openexchange.cluster.SocketBufferProbe.SYSTEM);
    }

    /**
     * Guard row 6: the log channel settings and the driver directory bound parse and fit their ranges, then the kernel
     * probe (a scratch UDP socket, {@code setOption} then {@code getOption}, for both buffers) grants each configured
     * buffer in full. Runs before any driver is launched or connected. The kernel limits are kernel-wide, so in
     * external mode the probe measures what the aeronmd's log socket on this host will get.
     *
     * @throws StandbySnapshotBootGuard.Refused with {@code CORE BOOT GUARD: } and the text
     */
    public static Transport enforceTransport(final java.util.function.Function<String, String> env,
                                             final com.openexchange.cluster.SocketBufferProbe.Kernel kernel) {
        return enforceTransport(env, kernel, false);
    }

    /** {@code probeOff}: the negative-control seam only (the probe answers nothing whatever the kernel granted). */
    static Transport enforceTransport(final java.util.function.Function<String, String> env,
                                      final com.openexchange.cluster.SocketBufferProbe.Kernel kernel,
                                      final boolean probeOff) {
        try {
            final com.openexchange.core.infrastructure.TransportConfig.LogTransport log =
                    com.openexchange.core.infrastructure.TransportConfig.LogTransport.parse(env);
            final long activeMs = com.openexchange.core.infrastructure.TransportConfig.driverDirActiveMs(env);
            if (!probeOff) {
                final com.openexchange.cluster.SocketBufferProbe.Request rcv =
                        new com.openexchange.cluster.SocketBufferProbe.Request(
                                com.openexchange.core.infrastructure.TransportConfig.LOG_SO_RCVBUF, log.soRcvbufText(),
                                log.soRcvbuf(), com.openexchange.cluster.SocketBufferProbe.RMEM_MAX,
                                com.openexchange.core.infrastructure.TransportConfig.LOG_RCV_WINDOW);
                final com.openexchange.cluster.SocketBufferProbe.Request snd =
                        new com.openexchange.cluster.SocketBufferProbe.Request(
                                com.openexchange.core.infrastructure.TransportConfig.LOG_SO_SNDBUF, log.soSndbufText(),
                                log.soSndbuf(), com.openexchange.cluster.SocketBufferProbe.WMEM_MAX, null);
                final com.openexchange.cluster.SocketBufferProbe.Result r =
                        com.openexchange.cluster.SocketBufferProbe.rcvbuf(kernel, rcv);
                if (r.refused()) {
                    throw new IllegalArgumentException(r.refusalText());
                }
                final com.openexchange.cluster.SocketBufferProbe.Result s =
                        com.openexchange.cluster.SocketBufferProbe.sndbuf(kernel, snd);
                if (s.refused()) {
                    throw new IllegalArgumentException(s.refusalText());
                }
                System.out.println("[BOOT-GUARD] log channel " + log.uri() + ": " + r.line() + "; " + s.line());
            } else {
                System.err.println("[BOOT-GUARD] CRITICAL: the log socket buffer probe is off (test seam only)");
            }
            return new Transport(log, activeMs);
        } catch (final IllegalArgumentException bad) {
            final StandbySnapshotBootGuard.Refused refused = new StandbySnapshotBootGuard.Refused(bad.getMessage(), bad);
            System.err.println(refused.getMessage());
            throw refused;
        } catch (final java.io.IOException unverifiable) {
            final StandbySnapshotBootGuard.Refused refused = new StandbySnapshotBootGuard.Refused(
                    "the log socket buffer probe could not open a scratch UDP socket: " + unverifiable, unverifiable);
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    /** {@code host:port} of an {@code aeron:udp?endpoint=host:port} channel, or null. */
    public static String endpointOf(final String channel) {
        if (channel == null) {
            return null;
        }
        final String key = "endpoint=";
        final int at = channel.indexOf(key);
        if (at < 0) {
            return null;
        }
        final int end = channel.indexOf('|', at);
        return channel.substring(at + key.length(), end < 0 ? channel.length() : end);
    }

    private static SnapshotModeSettings check(final SnapshotModeSettings.Source source, final boolean pruneEnabled) {
        final SnapshotModeSettings settings;
        try {
            settings = SnapshotModeSettings.parse(source);
        } catch (final IllegalArgumentException badSetting) {
            throw new StandbySnapshotBootGuard.Refused(badSetting.getMessage(), badSetting);
        }
        if (settings.mode() == SnapshotMode.SHADOW && pruneEnabled && !settings.floorEnabled()) {
            if (!Boolean.getBoolean(UNCHECKED_PROPERTY)) {
                throw new StandbySnapshotBootGuard.Refused("SNAPSHOT_MODE=shadow needs ShadowFloor on the pruner or"
                        + " SNAPSHOT_PRUNE_ENABLED=false");
            }
            System.err.println("[BOOT-GUARD] CRITICAL: SNAPSHOT_MODE=shadow with SHADOW_FLOOR_ENABLED=false let through by "
                    + UNCHECKED_PROPERTY + "=true (test only): this node may purge log below a stub it cannot restore");
        }
        for (final String line : settings.criticalLines()) {
            System.err.println("[BOOT-GUARD] CRITICAL: " + line);
        }
        System.out.println("[BOOT-GUARD] snapshotMode=" + settings.mode().text() + " prune=" + pruneEnabled
                + " floor=" + (settings.mode() == SnapshotMode.SHADOW && pruneEnabled
                ? (settings.floorEnabled() ? "wired" : "UNCHECKED-OFF") : "not-needed") + " " + settings);
        return settings;
    }
}

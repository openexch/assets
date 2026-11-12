// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.Logger;
import com.openexchange.core.infrastructure.TransportConfig;
import com.openexchange.core.infrastructure.TransportConfig.DriverMode;
import io.aeron.archive.Archive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import com.openexchange.cluster.EngineLifecycle;
import org.agrona.ErrorHandler;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Integer.parseInt;

/**
 * Core cluster node bootstrap. Wires a media driver (embedded by default) + archive +
 * consensus module + {@link CoreClusteredService} container on core-isolated ports/dirs. Config is
 * env-var-first: {@code CLUSTER_PORT_BASE} (default {@value InfrastructureConstants#CLUSTER_PORT_BASE}),
 * {@code CLUSTER_NODE}, {@code CLUSTER_ADDRESSES} (default localhost → single node), {@code BASE_DIR}.
 * Idle mode defaults to BACKOFF (see {@code TransportConfig}) so the core does not busy-spin on a shared
 * box.
 */
public final class AeronCluster {

    private static final Logger log = Logger.getLogger(AeronCluster.class);
    public static final AtomicLong AERON_ERROR_COUNT = new AtomicLong();
    /** Exit status of the hot-loop fail-fast (distinct from the lifecycle's fatal 1). */
    static final int EXIT_HOT_LOOP = 2;
    /** See {@link IdenticalErrorStreak}: the hot-loop fail-fast the ME has had since match#35. */
    private static final int IDENTICAL_ERROR_EXIT_THRESHOLD = 200;
    private static final IdenticalErrorStreak IDENTICAL_ERRORS = new IdenticalErrorStreak(IDENTICAL_ERROR_EXIT_THRESHOLD);

    /**
     * Loud startup warning when the resolved cluster/archive state dir (the money ledger's snapshot +
     * log, see {@link ClusterConfig#baseDir}) lands on a tmpfs mount: a power loss wipes it. This is
     * distinct from the media-driver dir (term buffers), which is fine on tmpfs by design: see the
     * comment at {@link ClusterConfig#create}.
     */
    static final String TMPFS_STATE_WARNING =
            "CORE STATE ON TMPFS: the money ledger will not survive power loss; "
                    + "set BASE_DIR to a disk path for anything beyond dev";

    /**
     * Runs the node until it stops, then exits through the kit's {@link EngineLifecycle} (the same code
     * the ME runs): a fatal fault or a failed start exits non-zero, SIGTERM closes within
     * {@code ENGINE_SHUTDOWN_TIMEOUT_MS}, a cluster termination exits 0. Never returns normally while the node is
     * running.
     */
    public AeronCluster() {
        final EngineLifecycle lifecycle = EngineLifecycle.install("Core node", msg -> log.error("%s", msg));
        try {
            run(lifecycle);
        } catch (final Throwable t) {
            lifecycle.startupFailed(t);
            return;
        }
        lifecycle.finish();
    }

    private static void run(final EngineLifecycle lifecycle) {
        final int portBase = getBasePort();
        final int nodeId = getClusterNode();
        final String hosts = getClusterAddresses();

        final List<String> hostAddresses = List.of(hosts.split(","));
        // guard row 1 (D1): the 3.5 settings are parsed under the boot guard FIRST, so a bad value is refused with
        // the "CORE BOOT GUARD: " prefix and exit 1 before the service, the journal runtime or any Aeron component
        // exists. The service takes the parsed settings; it never re-reads the environment.
        final SnapshotModeSettings snapshotModeSettings = StubModeBootGuard.enforce();
        // guard row 6: the log channel settings, the driver directory bound and the kernel's grant of the log socket
        // buffers, before any driver is launched or connected (every mode, both driver modes)
        final StubModeBootGuard.Transport transport = StubModeBootGuard.enforceTransport();
        final ElectionTimingSettings electionTiming = ElectionTimingSettings.fromEnv();
        final CoreClusteredService coreService = new CoreClusteredService(snapshotModeSettings);

        // Money journal: the ring is wired onto the engine NOW so the service thread can journal from
        // the first command, replayed or live. Whether it DOES is the cluster's replicated setting,
        // restored from the snapshot or set by a SetMoneyJournal command — never this node's config.
        // The writer thread starts inside the launch path once the media driver + archive are up.
        final com.openexchange.core.infrastructure.journal.MoneyJournalRuntime journalRuntime =
                com.openexchange.core.infrastructure.journal.MoneyJournalRuntime.create(nodeId);
        coreService.setMoneyJournalSink(journalRuntime.journal());

        // Balance feed side-channel (dark unless BALANCE_FEED_CHANNEL is set): the conflation store
        // is wired NOW so the service thread taps live balance updates from the first applied
        // command; the publisher thread starts inside the launch path once the media driver is up.
        final com.openexchange.core.infrastructure.feed.BalanceFeedRuntime feedRuntime =
                com.openexchange.core.infrastructure.feed.BalanceFeedRuntime.createIfEnabled(nodeId);
        if (feedRuntime != null) {
            coreService.setBalanceFeed(feedRuntime.store());
        }

        final ClusterConfig clusterConfig = ClusterConfig.create(nodeId, hostAddresses, hostAddresses, portBase,
                coreService);

        // guard rows 3 and 5: a pull source list must never name this node's own archive, and a member's archive must
        // be spelled as the members table spells it (the leader-source skip compares text); refused before any launch
        StubModeBootGuard.enforcePullSources(snapshotModeSettings,
                StubModeBootGuard.endpointOf(clusterConfig.archiveContext().controlChannel()),
                StubModeBootGuard.memberArchiveEndpoints(clusterConfig.consensusModuleContext().clusterMembers()));

        final File resolvedBaseDir = getBaseDir(nodeId);
        clusterConfig.baseDir(resolvedBaseDir);
        if (pathLooksEphemeral(resolvedBaseDir.getAbsolutePath())) {
            log.warn(TMPFS_STATE_WARNING);
        }

        final DriverMode driverMode = TransportConfig.driverMode();

        final ErrorHandler errorHandler = (Throwable t) -> {
            AERON_ERROR_COUNT.incrementAndGet();
            final StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            log.error("Aeron error #%d: %s", AERON_ERROR_COUNT.get(), sw.toString());
            // Hot-loop fail-fast: a deterministic apply fault or a broken replay throws the IDENTICAL
            // error on every poll. Exit loudly (the lifecycle closes, then halts with 2) instead of
            // spinning with /ready 503 forever; local state likely needs restore + replay.
            if (IDENTICAL_ERRORS.record(String.valueOf(t))) {
                lifecycle.fatal(EXIT_HOT_LOOP, "FAIL-FAST: " + IDENTICAL_ERROR_EXIT_THRESHOLD
                        + " consecutive identical Aeron errors (hot-loop): " + t);
            }
            // A dead external media driver (FATAL AeronException) or an agent that terminated unexpectedly: the
            // lifecycle closes the node and exits 1, never 0, so the process manager restarts a clean node.
            lifecycle.onError(t, driverMode == DriverMode.EXTERNAL);
        };
        clusterConfig.errorHandler(errorHandler);

        // Idle strategies for all agents: BACKOFF by default (never busy-spin on a shared box).
        clusterConfig.idleStrategySupplier(TransportConfig.idleStrategySupplier());

        clusterConfig.consensusModuleContext().ingressChannel(TransportConfig.udpChannel("aeron:udp"));
        clusterConfig.consensusModuleContext().egressChannel(TransportConfig.udpChannel("aeron:udp"));
        // rcv-wnd, so-rcvbuf and so-sndbuf on the URI: the log image gets them under either driver mode
        clusterConfig.consensusModuleContext().logChannel(transport.log().uri());

        clusterConfig.consensusModuleContext()
                .leaderHeartbeatIntervalNs(electionTiming.heartbeatIntervalNs())
                .leaderHeartbeatTimeoutNs(electionTiming.heartbeatTimeoutNs())
                .electionTimeoutNs(electionTiming.electionTimeoutNs())
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .sessionTimeoutNs(InfrastructureConstants.SESSION_TIMEOUT_NS);

        // Refuse to boot on standby-snapshot state before any driver/archive/CM thread starts.
        // A refusal propagates to the lifecycle's startupFailed: printed, exit 1.
        clusterConfig.enforceBootGuards();

        log.info("Core node %d starting: portBase=%d driverMode=%s idle=%s %s",
                nodeId, portBase, driverMode, TransportConfig.idleMode(), electionTiming);

        if (driverMode == DriverMode.EXTERNAL) {
            launchWithExternalDriver(clusterConfig, nodeId, lifecycle, journalRuntime, feedRuntime);
        } else {
            launchWithEmbeddedDriver(clusterConfig, lifecycle, journalRuntime, feedRuntime, transport.driverDirActiveMs());
        }
    }

    private static void launchWithExternalDriver(
            final ClusterConfig clusterConfig, final int nodeId, final EngineLifecycle lifecycle,
            final com.openexchange.core.infrastructure.journal.MoneyJournalRuntime journalRuntime,
            final com.openexchange.core.infrastructure.feed.BalanceFeedRuntime feedRuntime) {
        final String aeronDir = TransportConfig.aeronDir(nodeId);
        log.info("Driver mode EXTERNAL: connecting to media driver at %s", aeronDir);
        final long driverWaitMs = EngineLifecycle.driverWaitMs();
        lifecycle.awaitExternalDriver(aeronDir, driverWaitMs);
        log.info("External media driver detected at %s", aeronDir);
        clusterConfig.aeronDirectoryName(aeronDir);

        // The journal writer is an archive CLIENT recording on the node's own archive (no second
        // archive): declared right after the Archive so it outlives the container (the ring keeps
        // draining through service shutdown) but closes before the Archive stops recording. The
        // balance feed is a plain driver client with no durable half; same outlive-the-container
        // placement so the store's tail drains through service shutdown.
        try (
                Archive ignored = Archive.launch(clusterConfig.archiveContext());
                com.openexchange.core.infrastructure.journal.MoneyJournalRuntime ignoredJournal =
                        journalRuntime == null ? null
                                : journalRuntime.start(clusterConfig.aeronArchiveContext().clone(),
                                        clusterConfig.archiveContext().errorHandler());
                com.openexchange.core.infrastructure.feed.BalanceFeedRuntime ignoredFeed =
                        feedRuntime == null ? null
                                : feedRuntime.start(aeronDir,
                                        clusterConfig.archiveContext().errorHandler());
                ConsensusModule ignored1 = watchQuorum(lifecycle, ConsensusModule.launch(
                        clusterConfig.consensusModuleContext().terminationHook(lifecycle::onClusterTermination)));
                ClusteredServiceContainer ignored2 = ClusteredServiceContainer.launch(
                        clusterConfig.clusteredServiceContext().terminationHook(lifecycle::onClusterTermination))) {
            lifecycle.awaitStop();
        }
    }

    /** What else may own an embedded member driver's directory (the refusal text). */
    static final String DRIVER_DIR_OTHERS = "another core member's embedded driver or an aeronmd";

    /**
     * The embedded member driver over the kit's directory check: a directory whose driver heartbeat is at most
     * {@code CORE_DRIVER_DIR_ACTIVE_MS} old is a live driver and refuses the boot (exit 1, the directory untouched); a
     * stale one is deleted and recreated. Aeron's {@code dirDeleteOnStart(true)} skipped that liveness check entirely.
     */
    static ClusteredMediaDriver launchEmbeddedDriver(final ClusterConfig clusterConfig, final long driverDirActiveMs,
                                                     final java.util.function.Supplier<ClusteredMediaDriver> launch) {
        final String dir = clusterConfig.mediaDriverContext().aeronDirectoryName();
        try {
            return com.openexchange.cluster.EmbeddedDriverDir.launch(dir, driverDirActiveMs, launch, DRIVER_DIR_OTHERS,
                    "member");
        } catch (final com.openexchange.cluster.EmbeddedDriverDir.LiveDriverException live) {
            final StandbySnapshotBootGuard.Refused refused =
                    new StandbySnapshotBootGuard.Refused(live.getMessage(), live);
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    private static void launchWithEmbeddedDriver(
            final ClusterConfig clusterConfig, final EngineLifecycle lifecycle,
            final com.openexchange.core.infrastructure.journal.MoneyJournalRuntime journalRuntime,
            final com.openexchange.core.infrastructure.feed.BalanceFeedRuntime feedRuntime,
            final long driverDirActiveMs) {
        try (
                ClusteredMediaDriver ignored = watchQuorum(lifecycle, launchEmbeddedDriver(clusterConfig,
                        driverDirActiveMs, () -> ClusteredMediaDriver.launch(
                        com.openexchange.cluster.EmbeddedDriverDir.guard(clusterConfig.mediaDriverContext(),
                                        driverDirActiveMs)
                                .dirDeleteOnShutdown(true)
                                .timerIntervalNs(TimeUnit.MICROSECONDS.toNanos(500))
                                .clientLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                                .publicationUnblockTimeoutNs(TimeUnit.SECONDS.toNanos(10))
                                .socketSndbufLength(4 * 1024 * 1024)
                                .socketRcvbufLength(4 * 1024 * 1024)
                                .initialWindowLength(4 * 1024 * 1024)
                                .ipcTermBufferLength(16 * 1024 * 1024)
                                .publicationTermBufferLength(16 * 1024 * 1024)
                                .mtuLength(8192),
                        clusterConfig.archiveContext(),
                        clusterConfig.consensusModuleContext().terminationHook(lifecycle::onClusterTermination))));
                // Journal writer is a driver/archive client: start only after ClusteredMediaDriver
                // is up, and let it outlive the container so the ring drains through shutdown.
                com.openexchange.core.infrastructure.journal.MoneyJournalRuntime ignoredJournal =
                        journalRuntime == null ? null
                                : journalRuntime.start(clusterConfig.aeronArchiveContext().clone(),
                                        clusterConfig.archiveContext().errorHandler());
                // Same for the balance feed publisher (plain driver client, no archive).
                com.openexchange.core.infrastructure.feed.BalanceFeedRuntime ignoredFeed =
                        feedRuntime == null ? null
                                : feedRuntime.start(
                                        clusterConfig.mediaDriverContext().aeronDirectoryName(),
                                        clusterConfig.archiveContext().errorHandler());
                ClusteredServiceContainer ignored1 = ClusteredServiceContainer.launch(
                        clusterConfig.clusteredServiceContext().terminationHook(lifecycle::onClusterTermination))) {
            lifecycle.awaitStop();
        }
    }

    /**
     * (R-KIT-Q): the kit's quorum watchdog, started the moment the consensus module has launched (after the
     * external-driver wait, before the service container): exit 1 when no leader is ever known within
     * ENGINE_QUORUM_WAIT_MS. Returns the module so it stays in the try-with-resources.
     */
    private static ConsensusModule watchQuorum(final EngineLifecycle lifecycle, final ConsensusModule module) {
        lifecycle.watchQuorum(module.context());
        return module;
    }

    /** Embedded mode: the consensus module inside the clustered media driver. */
    private static ClusteredMediaDriver watchQuorum(final EngineLifecycle lifecycle, final ClusteredMediaDriver driver) {
        lifecycle.watchQuorum(driver.consensusModule().context());
        return driver;
    }

    private static File getBaseDir(final int nodeId) {
        final String baseDir = System.getenv("BASE_DIR");
        if (null == baseDir || baseDir.isEmpty()) {
            return new File(System.getProperty("user.dir"), "core-node" + nodeId);
        }
        return new File(baseDir);
    }

    /** {@link StatePaths#looksEphemeral}: the tmpfs startup warning's predicate (kept here for its callers). */
    static boolean pathLooksEphemeral(final String path) {
        return StatePaths.looksEphemeral(path);
    }

    private static String getClusterAddresses() {
        String clusterAddresses = System.getenv("CLUSTER_ADDRESSES");
        if (null == clusterAddresses || clusterAddresses.isEmpty()) {
            clusterAddresses = System.getProperty("cluster.addresses", "localhost");
        }
        return clusterAddresses;
    }

    private static int getClusterNode() {
        String clusterNode = System.getenv("CLUSTER_NODE");
        if (null == clusterNode || clusterNode.isEmpty()) {
            clusterNode = System.getProperty("node.id", "0");
        }
        return parseInt(clusterNode);
    }

    private static int getBasePort() {
        String portBaseString = System.getenv("CLUSTER_PORT_BASE");
        if (null == portBaseString || portBaseString.isEmpty()) {
            portBaseString = System.getProperty("port.base", String.valueOf(InfrastructureConstants.CLUSTER_PORT_BASE));
        }
        return parseInt(portBaseString);
    }
}

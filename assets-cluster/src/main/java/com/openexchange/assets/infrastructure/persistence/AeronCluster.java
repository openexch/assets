// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.InfrastructureConstants;
import com.openexchange.assets.infrastructure.Logger;
import com.openexchange.assets.infrastructure.TransportConfig;
import com.openexchange.assets.infrastructure.TransportConfig.DriverMode;
import io.aeron.archive.Archive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Integer.parseInt;

/**
 * Assets Engine cluster node bootstrap. Wires a media driver (embedded by default) + archive +
 * consensus module + {@link AssetsClusteredService} container on AE-isolated ports/dirs. Config is
 * env-var-first: {@code CLUSTER_PORT_BASE} (default {@value InfrastructureConstants#CLUSTER_PORT_BASE}),
 * {@code CLUSTER_NODE}, {@code CLUSTER_ADDRESSES} (default localhost → single node), {@code BASE_DIR}.
 * Idle mode defaults to BACKOFF (see {@code TransportConfig}) so the AE does not busy-spin on a shared
 * box.
 */
public final class AeronCluster {

    private static final Logger log = Logger.getLogger(AeronCluster.class);
    public static final AtomicLong AERON_ERROR_COUNT = new AtomicLong();
    private static final AtomicBoolean FATAL_EXIT_STARTED = new AtomicBoolean();

    /**
     * Loud startup warning when the resolved cluster/archive state dir (the money ledger's snapshot +
     * log, see {@link ClusterConfig#baseDir}) lands on a tmpfs mount: a power loss wipes it. This is
     * distinct from the media-driver dir (term buffers), which is fine on tmpfs by design: see the
     * comment at {@link ClusterConfig#create}.
     */
    static final String TMPFS_STATE_WARNING =
            "ASSETS ENGINE STATE ON TMPFS: the money ledger will not survive power loss; "
                    + "set BASE_DIR to a disk path for anything beyond dev";

    public AeronCluster() {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final int portBase = getBasePort();
        final int nodeId = getClusterNode();
        final String hosts = getClusterAddresses();

        final List<String> hostAddresses = List.of(hosts.split(","));
        final ClusterConfig clusterConfig = ClusterConfig.create(nodeId, hostAddresses, hostAddresses, portBase,
                new AssetsClusteredService());

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
            // In EXTERNAL mode a dead media driver surfaces as a FATAL AeronException; guarantee process
            // death so the process manager restarts a clean node.
            if (driverMode == DriverMode.EXTERNAL
                    && t instanceof AeronException ae
                    && ae.category() == AeronException.Category.FATAL
                    && FATAL_EXIT_STARTED.compareAndSet(false, true)) {
                log.error("FATAL Aeron error in EXTERNAL driver mode - node exiting in 5s for restart");
                barrier.signal();
                final Thread exitBackstop = new Thread(() -> {
                    quietSleep(5000);
                    Runtime.getRuntime().halt(1);
                }, "driver-death-exit");
                exitBackstop.setDaemon(true);
                exitBackstop.start();
            }
        };
        clusterConfig.errorHandler(errorHandler);

        // Idle strategies for all agents: BACKOFF by default (never busy-spin on a shared box).
        clusterConfig.idleStrategySupplier(TransportConfig.idleStrategySupplier());

        clusterConfig.consensusModuleContext().ingressChannel(TransportConfig.udpChannel("aeron:udp"));
        clusterConfig.consensusModuleContext().egressChannel(TransportConfig.udpChannel("aeron:udp"));
        clusterConfig.consensusModuleContext().logChannel(
                "aeron:udp?term-length=" + TransportConfig.logTermLength());

        clusterConfig.consensusModuleContext()
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .sessionTimeoutNs(InfrastructureConstants.SESSION_TIMEOUT_NS);

        log.info("Assets Engine node %d starting: portBase=%d driverMode=%s idle=%s",
                nodeId, portBase, driverMode, TransportConfig.idleMode());

        if (driverMode == DriverMode.EXTERNAL) {
            launchWithExternalDriver(clusterConfig, nodeId, barrier);
        } else {
            launchWithEmbeddedDriver(clusterConfig, barrier);
        }
    }

    private static void launchWithExternalDriver(
            final ClusterConfig clusterConfig, final int nodeId, final ShutdownSignalBarrier barrier) {
        final String aeronDir = TransportConfig.aeronDir(nodeId);
        log.info("Driver mode EXTERNAL: connecting to media driver at %s", aeronDir);
        TransportConfig.awaitExternalDriver(aeronDir, TransportConfig.EXTERNAL_DRIVER_TIMEOUT_MS);
        clusterConfig.aeronDirectoryName(aeronDir);

        try (
                Archive ignored = Archive.launch(clusterConfig.archiveContext());
                ConsensusModule ignored1 = ConsensusModule.launch(
                        clusterConfig.consensusModuleContext().terminationHook(barrier::signal));
                ClusteredServiceContainer ignored2 = ClusteredServiceContainer.launch(
                        clusterConfig.clusteredServiceContext().terminationHook(barrier::signal))) {
            barrier.await();
        } catch (Exception e) {
            System.err.println("FATAL: Assets Engine node failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void launchWithEmbeddedDriver(
            final ClusterConfig clusterConfig, final ShutdownSignalBarrier barrier) {
        try (
                ClusteredMediaDriver ignored = ClusteredMediaDriver.launch(
                        clusterConfig.mediaDriverContext()
                                .dirDeleteOnStart(true)
                                .dirDeleteOnShutdown(true)
                                .driverTimeoutMs(500)
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
                        clusterConfig.consensusModuleContext().terminationHook(barrier::signal));
                ClusteredServiceContainer ignored1 = ClusteredServiceContainer.launch(
                        clusterConfig.clusteredServiceContext().terminationHook(barrier::signal))) {
            barrier.await();
        } catch (Exception e) {
            System.err.println("FATAL: Assets Engine node failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static File getBaseDir(final int nodeId) {
        final String baseDir = System.getenv("BASE_DIR");
        if (null == baseDir || baseDir.isEmpty()) {
            return new File(System.getProperty("user.dir"), "ae-node" + nodeId);
        }
        return new File(baseDir);
    }

    /**
     * True if {@code path} is (or is nested under) a well-known ephemeral/tmpfs mount ({@code /tmp} or
     * {@code /dev/shm}). Matches on path-segment boundaries, so a sibling like {@code /tmpfoo} does
     * <b>not</b> false-positive against {@code /tmp}. Pure and static so it is unit-testable without
     * booting a node. Relative paths always return {@code false} (nothing to warn about until they are
     * resolved against a cwd: callers should pass an absolute path, e.g. {@code File#getAbsolutePath()}).
     */
    static boolean pathLooksEphemeral(final String path) {
        if (null == path || path.isEmpty()) {
            return false;
        }
        String normalized = path;
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return "/tmp".equals(normalized) || normalized.startsWith("/tmp/")
                || "/dev/shm".equals(normalized) || normalized.startsWith("/dev/shm/");
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

    private static void quietSleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}

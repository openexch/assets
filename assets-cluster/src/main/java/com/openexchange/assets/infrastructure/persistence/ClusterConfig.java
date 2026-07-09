// SPDX-License-Identifier: Apache-2.0
/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NoOpLock;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Cluster configuration builder (Real Logic Aeron sample, forked for the Assets Engine). Directory
 * names are isolated with an {@code -assets-} / {@code aeron-assets-} prefix so the AE's embedded
 * driver and state never collide with the co-resident matching engine. Threading modes stay DEDICATED
 * but the actual idle strategy is overridden at runtime via {@link #idleStrategySupplier} — the AE
 * defaults to BACKOFF (see {@code TransportConfig}) so it does not busy-spin on a shared box.
 */
public final class ClusterConfig {

    public static final int PORTS_PER_NODE = 100;
    public static final int ARCHIVE_CONTROL_PORT_OFFSET = 1;
    public static final int CLIENT_FACING_PORT_OFFSET = 2;
    public static final int MEMBER_FACING_PORT_OFFSET = 3;
    public static final int LOG_PORT_OFFSET = 4;
    public static final int TRANSFER_PORT_OFFSET = 5;
    public static final String ARCHIVE_SUB_DIR = "archive";
    public static final String CLUSTER_SUB_DIR = "cluster";

    private final int memberId;
    private final String ingressHostname;
    private final String clusterHostname;
    private final MediaDriver.Context mediaDriverContext;
    private final Archive.Context archiveContext;
    private final AeronArchive.Context aeronArchiveContext;
    private final ConsensusModule.Context consensusModuleContext;
    private final List<ClusteredServiceContainer.Context> clusteredServiceContexts;

    ClusterConfig(
            final int memberId,
            final String ingressHostname,
            final String clusterHostname,
            final MediaDriver.Context mediaDriverContext,
            final Archive.Context archiveContext,
            final AeronArchive.Context aeronArchiveContext,
            final ConsensusModule.Context consensusModuleContext,
            final List<ClusteredServiceContainer.Context> clusteredServiceContexts) {
        this.memberId = memberId;
        this.ingressHostname = ingressHostname;
        this.clusterHostname = clusterHostname;
        this.mediaDriverContext = mediaDriverContext;
        this.archiveContext = archiveContext;
        this.aeronArchiveContext = aeronArchiveContext;
        this.consensusModuleContext = consensusModuleContext;
        this.clusteredServiceContexts = clusteredServiceContexts;
    }

    public static ClusterConfig create(
            final int startingMemberId,
            final int memberId,
            final List<String> ingressHostnames,
            final List<String> clusterHostnames,
            final int portBase,
            final File parentDir,
            final ClusteredService clusteredService,
            final ClusteredService... additionalServices) {
        if (memberId < startingMemberId || (startingMemberId + ingressHostnames.size()) <= memberId) {
            throw new IllegalArgumentException(
                    "memberId=" + memberId + " is invalid, should be " + startingMemberId +
                            " <= memberId < " + startingMemberId + ingressHostnames.size());
        }

        final String clusterMembers = clusterMembers(startingMemberId, ingressHostnames, clusterHostnames, portBase);

        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-assets-" + memberId + "-driver";
        final File baseDir = new File(parentDir, "aeron-assets-" + memberId);

        final String ingressHostname = ingressHostnames.get(memberId - startingMemberId);
        final String hostname = clusterHostnames.get(memberId - startingMemberId);

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.DEDICATED)
                .termBufferSparseFile(false)
                .conductorIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
                .senderIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
                .receiverIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
                .sharedIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .publicationTermBufferLength(16 * 1024 * 1024)
                .ipcTermBufferLength(16 * 1024 * 1024)
                .socketSndbufLength(4 * 1024 * 1024)
                .socketRcvbufLength(4 * 1024 * 1024)
                .timerIntervalNs(1_000_000)
                .clientLivenessTimeoutNs(10_000_000_000L)
                .nakUnicastDelayNs(100_000)
                .nakMulticastMaxBackoffNs(1_000_000)
                .retransmitUnicastDelayNs(100_000);

        final AeronArchive.Context replicationArchiveContext = new AeronArchive.Context()
                .controlResponseChannel("aeron:udp?endpoint=" + hostname + ":0");

        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveDir(new File(baseDir, ARCHIVE_SUB_DIR))
                .controlChannel(udpChannel(memberId, hostname, portBase))
                .archiveClientContext(replicationArchiveContext)
                .localControlChannel("aeron:ipc?term-length=16m")
                .replicationChannel("aeron:udp?endpoint=" + hostname + ":0")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.DEDICATED)
                .catalogCapacity(1024 * 1024)
                .segmentFileLength(64 * 1024 * 1024)
                .maxConcurrentRecordings(10)
                .maxConcurrentReplays(10);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(archiveContext.localControlChannel())
                .controlRequestStreamId(archiveContext.localControlStreamId())
                .controlResponseChannel(archiveContext.localControlChannel())
                .aeronDirectoryName(aeronDirName);

        final String snapshotChannel = "aeron:ipc?term-length=67108864";

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
                .clusterMemberId(memberId)
                .clusterMembers(clusterMembers)
                .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
                .archiveContext(aeronArchiveContext.clone())
                .serviceCount(1 + additionalServices.length)
                .replicationChannel("aeron:udp?endpoint=" + hostname + ":0")
                .snapshotChannel(snapshotChannel)
                .idleStrategySupplier(org.agrona.concurrent.BusySpinIdleStrategy::new)
                .logFragmentLimit(64)
                .wheelTickResolutionNs(1_000_000)
                .ticksPerWheel(1024)
                .maxConcurrentSessions(50);

        final List<ClusteredServiceContainer.Context> serviceContexts = new ArrayList<>();

        final ClusteredServiceContainer.Context clusteredServiceContext = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(aeronArchiveContext.clone())
                .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
                .clusteredService(clusteredService)
                .serviceId(0)
                .idleStrategySupplier(org.agrona.concurrent.BusySpinIdleStrategy::new)
                .snapshotDurationThresholdNs(5_000_000_000L)
                .snapshotChannel(snapshotChannel);
        serviceContexts.add(clusteredServiceContext);

        for (int i = 0; i < additionalServices.length; i++) {
            final ClusteredServiceContainer.Context additionalServiceContext = new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(aeronDirName)
                    .archiveContext(aeronArchiveContext.clone())
                    .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
                    .clusteredService(additionalServices[i])
                    .serviceId(i + 1)
                    .idleStrategySupplier(org.agrona.concurrent.BusySpinIdleStrategy::new)
                    .snapshotDurationThresholdNs(5_000_000_000L)
                    .snapshotChannel(snapshotChannel);
            serviceContexts.add(additionalServiceContext);
        }

        return new ClusterConfig(
                memberId, ingressHostname, hostname, mediaDriverContext, archiveContext,
                aeronArchiveContext, consensusModuleContext, serviceContexts);
    }

    public static ClusterConfig create(
            final int nodeId,
            final List<String> ingressHostnames,
            final List<String> clusterHostnames,
            final int portBase,
            final ClusteredService clusteredService,
            final ClusteredService... additionalServices) {
        return create(0, nodeId, ingressHostnames, clusterHostnames, portBase,
                new File(System.getProperty("user.dir")), clusteredService, additionalServices);
    }

    public static ClusterConfig create(
            final int nodeId,
            final List<String> hostnames,
            final int portBase,
            final ClusteredService clusteredService) {
        return create(nodeId, hostnames, hostnames, portBase, clusteredService);
    }

    public void errorHandler(final ErrorHandler errorHandler) {
        this.mediaDriverContext.errorHandler(errorHandler);
        this.archiveContext.errorHandler(errorHandler);
        this.aeronArchiveContext.errorHandler(errorHandler);
        this.consensusModuleContext.errorHandler(errorHandler);
        this.clusteredServiceContexts.forEach((ctx) -> ctx.errorHandler(errorHandler));
    }

    public void idleStrategySupplier(final java.util.function.Supplier<org.agrona.concurrent.IdleStrategy> supplier) {
        this.mediaDriverContext
                .conductorIdleStrategy(supplier.get())
                .senderIdleStrategy(supplier.get())
                .receiverIdleStrategy(supplier.get())
                .sharedIdleStrategy(supplier.get());
        this.consensusModuleContext.idleStrategySupplier(supplier::get);
        this.clusteredServiceContexts.forEach((ctx) -> ctx.idleStrategySupplier(supplier::get));
    }

    public void aeronDirectoryName(final String aeronDir) {
        this.mediaDriverContext.aeronDirectoryName(aeronDir);
        this.archiveContext.aeronDirectoryName(aeronDir);
        this.aeronArchiveContext.aeronDirectoryName(aeronDir);
        this.consensusModuleContext.aeronDirectoryName(aeronDir);
        this.clusteredServiceContexts.forEach(ctx -> ctx.aeronDirectoryName(aeronDir));
    }

    public void baseDir(final File baseDir) {
        this.archiveContext.archiveDir(new File(baseDir, ARCHIVE_SUB_DIR));
        this.consensusModuleContext.clusterDir(new File(baseDir, CLUSTER_SUB_DIR));
        this.clusteredServiceContexts.forEach((ctx) -> ctx.clusterDir(new File(baseDir, CLUSTER_SUB_DIR)));
    }

    public MediaDriver.Context mediaDriverContext() {
        return mediaDriverContext;
    }

    public Archive.Context archiveContext() {
        return archiveContext;
    }

    public AeronArchive.Context aeronArchiveContext() {
        return aeronArchiveContext;
    }

    public ConsensusModule.Context consensusModuleContext() {
        return consensusModuleContext;
    }

    public ClusteredServiceContainer.Context clusteredServiceContext() {
        return clusteredServiceContexts.get(0);
    }

    public List<ClusteredServiceContainer.Context> clusteredServiceContexts() {
        return clusteredServiceContexts;
    }

    public int memberId() {
        return memberId;
    }

    public String ingressHostname() {
        return ingressHostname;
    }

    public String clusterHostname() {
        return clusterHostname;
    }

    public static String clusterMembers(
            final List<String> ingressHostnames, final List<String> clusterHostnames, final int portBase) {
        return clusterMembers(0, ingressHostnames, clusterHostnames, portBase);
    }

    public static String clusterMembers(
            final int startingMemberId,
            final List<String> ingressHostnames,
            final List<String> clusterHostnames,
            final int portBase) {
        if (ingressHostnames.size() != clusterHostnames.size()) {
            throw new IllegalArgumentException("ingressHostnames and clusterHostnames must be the same size");
        }

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ingressHostnames.size(); i++) {
            final int memberId = i + startingMemberId;
            sb.append(memberId);
            sb.append(',').append(endpoint(memberId, ingressHostnames.get(i), portBase, CLIENT_FACING_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, MEMBER_FACING_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, LOG_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, TRANSFER_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, ARCHIVE_CONTROL_PORT_OFFSET));
            sb.append('|');
        }

        return sb.toString();
    }

    public static String ingressEndpoints(
            final List<String> hostnames, final int portBase, final int clientFacingPortOffset) {
        return ingressEndpoints(0, hostnames, portBase, clientFacingPortOffset);
    }

    public static String ingressEndpoints(
            final int startingMemberId,
            final List<String> hostnames,
            final int portBase,
            final int clientFacingPortOffset) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++) {
            final int memberId = i + startingMemberId;
            sb.append(memberId).append('=');
            sb.append(hostnames.get(i)).append(':').append(calculatePort(memberId, portBase, clientFacingPortOffset));
            sb.append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static int calculatePort(final int nodeId, final int portBase, final int offset) {
        return portBase + (nodeId * PORTS_PER_NODE) + offset;
    }

    private static String udpChannel(final int nodeId, final String hostname, final int portBase) {
        final int port = calculatePort(nodeId, portBase, ClusterConfig.ARCHIVE_CONTROL_PORT_OFFSET);
        return "aeron:udp?endpoint=" + hostname + ":" + port;
    }

    private static String endpoint(final int nodeId, final String hostname, final int portBase, final int portOffset) {
        return hostname + ":" + calculatePort(nodeId, portBase, portOffset);
    }
}

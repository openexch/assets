// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The Assets Engine snapshots and reclaims its own disk with nothing else
 * running.
 *
 * <p>This is the engine the whole exercise is about. The AE shipped with no
 * snapshot trigger of any kind - the gateway's scheduler only ever knew about
 * the matching engine - so its cluster log grew from the first day until
 * /dev/shm was full, the archive started failing writes, and the money path
 * stopped for seventeen hours on 2026-07-25. Proving the rhythm on the matching
 * engine alone would prove it on the engine that never had the problem.</p>
 *
 * <p>Same shape as the matching engine's test, and deliberately so: one copy of
 * the mechanism in cluster-kit, wired the same way, asserted the same way.
 * Scaled down to 64 KiB segments and a 64 KiB threshold; every moving part -
 * control toggle, SNAPSHOT action, {@code onTakeSnapshot}, recording.log,
 * {@code purgeSegments} - is the production one.</p>
 */
public class SnapshotCadenceClusterTest {

    private static final int SEGMENT_LENGTH = 64 * 1024;
    private static final int PORT_BASE = 19400; // disjoint from the boot smoke test (19300)
    private static final long USER = 7373L;
    private static final long TIMEOUT_MS = 90_000;

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();

    @Test
    public void theClusterSnapshotsAndReclaimsItsOwnLogWithNoAdminProcess() throws Exception {
        // BEFORE the service is constructed: the cadence is built in a field
        // initialiser, so a property set later would be read by nobody.
        System.setProperty("snapshot.log.bytes", Integer.toString(SEGMENT_LENGTH));
        // Timer off: this is the byte trigger's test, and a timer firing
        // underneath it would let a broken byte path pass.
        System.setProperty("snapshot.interval.minutes", "0");

        final File tmp = new File(System.getProperty("java.io.tmpdir"),
                "ae-cadence-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");

        final List<String> hosts = List.of("localhost");
        final AssetsClusteredService service = new AssetsClusteredService();
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        cfg.baseDir(baseDir);
        cfg.aeronDirectoryName(aeronDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new); // never busy-spin
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=64k")
                // A recording's segments can never be smaller than its term buffer,
                // so the log channel has to shrink with segmentFileLength below or
                // nothing is ever whole-segment reclaimable.
                .logChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        cfg.archiveContext().segmentFileLength(SEGMENT_LENGTH);

        final File clusterDir = cfg.consensusModuleContext().clusterDir();
        final File archiveDir = cfg.archiveContext().archiveDir();

        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        AeronCluster client = null;
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                    cfg.archiveContext(),
                    cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

            client = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener((sessionId, timestamp, buffer, offset, length, header) -> { })
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .ingressChannel("aeron:udp?term-length=64k")
                    .aeronDirectoryName(aeronDir)
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(
                            hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));

            for (int i = 0; i < 4000; i++) {
                deposit(client);
            }

            final long snapshotPosition = awaitSnapshot(clusterDir, client);
            assertTrue("the AE should have snapshotted with nothing external asking for one",
                    snapshotPosition >= 0);
            System.out.println("[TEST] AE snapshot at log position " + snapshotPosition
                    + " - no admin gateway, no ClusterTool");

            final long logRecordingId;
            try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
                logRecordingId = recordingLog.findLastTermRecordingId();
            }
            final File firstSegment = new File(archiveDir, logRecordingId + "-0.rec");
            assertTrue("the log's first segment should exist before pruning: " + firstSegment,
                    firstSegment.exists());

            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (firstSegment.exists()) {
                if (System.currentTimeMillis() > deadline) {
                    fail("the first log segment was never reclaimed: " + firstSegment
                            + " (snapshot at " + snapshotPosition + ") - the AE snapshotted but "
                            + "nothing purged the log below it, which is the 2026-07-25 mechanism");
                }
                deposit(client);
                Thread.sleep(200);
            }
            System.out.println("[TEST] AE reclaimed " + firstSegment.getName()
                    + " - it snapshotted AND pruned on its own");
        } finally {
            CloseHelper.quietClose(client);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
            IoUtil.delete(tmp, true);
            System.clearProperty("snapshot.log.bytes");
            System.clearProperty("snapshot.interval.minutes");
        }
    }

    private long awaitSnapshot(final File clusterDir, final AeronCluster client) throws Exception {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
                for (final RecordingLog.Entry entry : recordingLog.entries()) {
                    if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid) {
                        return entry.logPosition;
                    }
                }
            }
            deposit(client);
            Thread.sleep(250);
        }
        return -1;
    }

    private void deposit(final AeronCluster client) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0L).userId(USER).assetId(Asset.USD.id())
                .amount(FixedPoint.fromDouble(1.0));
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();

        final long deadline = System.currentTimeMillis() + 10_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            if (client.offer(ingress, 0, length) > 0) {
                return;
            }
            client.pollEgress(); // keep the session serviced while backpressured
            idle.idle();
        }
        throw new IllegalStateException("failed to offer ingress within timeout");
    }
}

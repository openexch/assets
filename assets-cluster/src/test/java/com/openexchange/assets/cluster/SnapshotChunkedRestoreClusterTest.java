// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.RecordingLog;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A ledger whose snapshot is LARGER than the snapshot channel's {@code maxMessageLength} survives
 * snapshot, log pruning and a cold restart, in the real cluster.
 *
 * <p>The transport unit test proves the chunk encoder and the reassembly algorithm; this test proves
 * the two production call sites, {@code onTakeSnapshot} and {@code loadSnapshot}, through Aeron
 * Cluster itself. The snapshot term is shrunk to 64 KiB (property, read by {@code ClusterConfig})
 * so {@code maxMessageLength} is 8 KiB, and 1,500 funded accounts serialize to about 178 KB: the
 * snapshot can only reach the archive as 22 messages. Before 2026-10-25 the single
 * {@code offer()} threw inside the callback and the snapshot never became valid (the RED run of this
 * test). The log below the snapshot is then pruned by the engine's own cadence, so after the restart
 * the only source of the balances is the chunked snapshot plus the log tail; the restored ledger is
 * read back over the wire ({@code RequestBalanceSnapshot}, {@code RequestHoldSnapshot}, a
 * {@code DepositAck}'s {@code newAvailable}).</p>
 */
public class SnapshotChunkedRestoreClusterTest {

    private static final int SEGMENT_LENGTH = 64 * 1024;
    private static final int SNAPSHOT_TERM_LENGTH = 64 * 1024;          // maxMessageLength = term / 8
    private static final int MAX_MESSAGE_LENGTH = SNAPSHOT_TERM_LENGTH / 8;
    private static final int PORT_BASE = 19500; // disjoint from boot smoke (19300) and cadence (19400)
    private static final int USERS = 1500;
    private static final int HOLDS = 500;        // users 1..HOLDS carry one hold each
    private static final long FILLER_USER = 1L;  // in range, so the expected entry count stays USERS
    private static final long TIMEOUT_MS = 90_000;
    private static final long HOLD_AMOUNT = FixedPoint.fromDouble(0.5);
    private static final long UNIT = FixedPoint.fromDouble(1.0);

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final HoldEncoder holdEnc = new HoldEncoder();
    private final RequestBalanceSnapshotEncoder balanceReqEnc = new RequestBalanceSnapshotEncoder();
    private final RequestHoldSnapshotEncoder holdReqEnc = new RequestHoldSnapshotEncoder();

    /** Replies routed to this session: snapshot-end counts and deposit acks, keyed by correlationId. */
    private static final class Replies implements EgressListener {
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final BalanceSnapshotEndDecoder balanceEnd = new BalanceSnapshotEndDecoder();
        private final HoldSnapshotEndDecoder holdEnd = new HoldSnapshotEndDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();
        long balanceEndCorrelation = -1; int balanceEntries = -1;
        long holdEndCorrelation = -1; int holdEntries = -1;
        long depositAckCorrelation = -1; long depositAckNewAvailable = -1;

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp,
                              final org.agrona.DirectBuffer buffer, final int offset, final int length,
                              final io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
            switch (header.templateId()) {
                case BalanceSnapshotEndDecoder.TEMPLATE_ID:
                    balanceEnd.wrap(buffer, body, header.blockLength(), header.version());
                    balanceEndCorrelation = balanceEnd.correlationId();
                    balanceEntries = balanceEnd.entryCount();
                    break;
                case HoldSnapshotEndDecoder.TEMPLATE_ID:
                    holdEnd.wrap(buffer, body, header.blockLength(), header.version());
                    holdEndCorrelation = holdEnd.correlationId();
                    holdEntries = holdEnd.entryCount();
                    break;
                case DepositAckDecoder.TEMPLATE_ID:
                    depositAck.wrap(buffer, body, header.blockLength(), header.version());
                    if (depositAck.correlationId() != 0L) {
                        depositAckCorrelation = depositAck.correlationId();
                        depositAckNewAvailable = depositAck.newAvailable();
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /** One booted single-node AE: everything needed to stop it and to boot it again on the same dirs. */
    private static final class Node {
        final ClusterConfig cfg;
        final ClusteredMediaDriver mediaDriver;
        final ClusteredServiceContainer container;

        Node(final ClusterConfig cfg, final ClusteredMediaDriver mediaDriver,
             final ClusteredServiceContainer container) {
            this.cfg = cfg;
            this.mediaDriver = mediaDriver;
            this.container = container;
        }

        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
        }
    }

    @Test
    public void aMultiChunkSnapshotRestoresTheLedgerAfterAColdRestart() throws Exception {
        // All three BEFORE anything is constructed: the cadence reads its properties in a field
        // initialiser and ClusterConfig reads the snapshot term when the channel string is built.
        System.setProperty("snapshot.log.bytes", Integer.toString(SEGMENT_LENGTH));
        System.setProperty("snapshot.interval.minutes", "0");
        System.setProperty("transport.snapshot.term.length", SNAPSHOT_TERM_LENGTH / 1024 + "k");

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-chunked-restore-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");
        final List<String> hosts = List.of("localhost");

        Node node = null;
        AeronCluster client = null;
        Replies replies = new Replies();
        try {
            node = boot(hosts, baseDir, aeronDir);
            client = connect(hosts, aeronDir, replies);

            // Fund USERS accounts and hold on the first HOLDS of them. correlationId 0 = no ack frame
            // needed; the state is read back over the wire at the end, through the same log.
            for (long user = 1; user <= USERS; user++) {
                deposit(client, user, UNIT * user, 0L);
            }
            for (long user = 1; user <= HOLDS; user++) {
                hold(client, user, user, HOLD_AMOUNT, (user & 1) == 1);
            }

            int fillerDeposits = 0;
            final File clusterDir = node.cfg.consensusModuleContext().clusterDir();
            final File archiveDir = node.cfg.archiveContext().archiveDir();

            // 1. The engine snapshots on its own (byte trigger); the snapshot recording must hold far
            //    more bytes than one message can carry, on a 64 KiB term.
            long snapshotRecordingId = -1;
            final long snapshotDeadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (snapshotRecordingId < 0) {
                if (System.currentTimeMillis() > snapshotDeadline) {
                    fail("no valid service snapshot within " + TIMEOUT_MS + "ms: with " + USERS
                            + " accounts the snapshot exceeds maxMessageLength=" + MAX_MESSAGE_LENGTH
                            + " and a single offer() fails inside onTakeSnapshot (pre-2026-10-25 behaviour)");
                }
                try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
                    for (final RecordingLog.Entry entry : recordingLog.entries()) {
                        if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid
                                && entry.serviceId == 0) {
                            snapshotRecordingId = entry.recordingId;
                        }
                    }
                }
                if (snapshotRecordingId < 0) {
                    deposit(client, FILLER_USER, UNIT, 0L);
                    fillerDeposits++;
                    Thread.sleep(250);
                }
            }
            final long[] termAndStop = new long[2];
            try (AeronArchive archive = AeronArchive.connect(
                    node.cfg.consensusModuleContext().archiveContext().clone())) {
                final long found = archive.listRecording(snapshotRecordingId,
                        (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                         startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                         mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                            termAndStop[0] = termBufferLength;
                            termAndStop[1] = stopPosition - startPosition;
                        });
                assertEquals("snapshot recording descriptor", 1L, found);
            }
            assertEquals("the snapshot channel must run on the shrunken term, or this test exercises "
                    + "the single-message path", SNAPSHOT_TERM_LENGTH, termAndStop[0]);
            assertTrue("snapshot recording holds " + termAndStop[1] + " bytes; expected well beyond one "
                    + MAX_MESSAGE_LENGTH + "-byte message", termAndStop[1] > 4L * MAX_MESSAGE_LENGTH);
            System.out.println("[TEST] service snapshot recording " + snapshotRecordingId + ": "
                    + termAndStop[1] + " bytes on a " + termAndStop[0] + "-byte term (maxMessageLength "
                    + MAX_MESSAGE_LENGTH + ")");

            // 2. Let the engine prune the log below that snapshot, so the restart cannot replay from
            //    the beginning: the snapshot is the only source of the early deposits.
            final long logRecordingId;
            try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
                logRecordingId = recordingLog.findLastTermRecordingId();
            }
            final File firstSegment = new File(archiveDir, logRecordingId + "-0.rec");
            final long pruneDeadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (firstSegment.exists()) {
                if (System.currentTimeMillis() > pruneDeadline) {
                    fail("first log segment never reclaimed: " + firstSegment);
                }
                deposit(client, FILLER_USER, UNIT, 0L);
                fillerDeposits++;
                Thread.sleep(200);
            }
            System.out.println("[TEST] log pruned below the snapshot (" + firstSegment.getName()
                    + " gone), filler deposits=" + fillerDeposits);

            // 3. Cold restart on the same cluster + archive dirs.
            CloseHelper.quietClose(client);
            client = null;
            node.close();
            node = boot(hosts, baseDir, aeronDir);
            replies = new Replies();
            client = connect(hosts, aeronDir, replies);

            // 4. Read the ledger back over the wire.
            request(client, 42L, true);
            awaitReply(client, replies, 42L, true);
            assertEquals("non-zero (user, asset) balances after restart", USERS, replies.balanceEntries);

            request(client, 43L, false);
            awaitReply(client, replies, 43L, false);
            assertEquals("outstanding holds after restart", HOLDS, replies.holdEntries);

            // FILLER_USER (=1): deposited 1 + fillerDeposits units, holds 0.5, now deposits 1 more.
            deposit(client, FILLER_USER, UNIT, 44L);
            final long ackDeadline = System.currentTimeMillis() + 20_000;
            while (replies.depositAckCorrelation != 44L) {
                if (System.currentTimeMillis() > ackDeadline) {
                    fail("no DepositAck for correlationId 44 after restart");
                }
                if (client.pollEgress() == 0) {
                    Thread.sleep(1);
                }
            }
            final long expectedAvailable = UNIT * (1 + fillerDeposits + 1) - HOLD_AMOUNT;
            assertEquals("user 1 available after restart = snapshot balance + log tail + new deposit - hold",
                    expectedAvailable, replies.depositAckNewAvailable);
            System.out.println("[TEST] restored over a chunked snapshot: " + replies.balanceEntries
                    + " balances, " + replies.holdEntries + " holds, user 1 available "
                    + FixedPoint.toDouble(replies.depositAckNewAvailable));
        } finally {
            CloseHelper.quietClose(client);
            if (node != null) {
                node.close();
            }
            IoUtil.delete(tmp, true);
            System.clearProperty("snapshot.log.bytes");
            System.clearProperty("snapshot.interval.minutes");
            System.clearProperty("transport.snapshot.term.length");
        }
    }

    private static Node boot(final List<String> hosts, final File baseDir, final String aeronDir) {
        final AssetsClusteredService service = new AssetsClusteredService();
        TestAuth.install();
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        cfg.baseDir(baseDir);
        cfg.aeronDirectoryName(aeronDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        cfg.archiveContext().segmentFileLength(SEGMENT_LENGTH);

        final ClusteredMediaDriver mediaDriver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                cfg.archiveContext(),
                cfg.consensusModuleContext());
        final ClusteredServiceContainer container =
                ClusteredServiceContainer.launch(cfg.clusteredServiceContext());
        return new Node(cfg, mediaDriver, container);
    }

    private static AeronCluster connect(final List<String> hosts, final String aeronDir,
                                        final EgressListener listener) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                    .credentialsSupplier(TestAuth.credentials(SessionClass.TOOL))
                        .egressListener(listener)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp?term-length=64k")
                        .aeronDirectoryName(aeronDir)
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(
                                hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
            } catch (final RuntimeException e) {
                last = e; // the node is still electing / replaying; try again
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect to the restarted node", last);
    }

    private void deposit(final AeronCluster client, final long user, final long amount, final long correlationId) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(correlationId).userId(user).assetId(Asset.USD.id()).amount(amount).requestIdHigh(0).requestIdLow(0);
        offer(client, MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength());
    }

    private void hold(final AeronCluster client, final long orderId, final long user, final long amount,
                      final boolean omsManagedRelease) {
        holdEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0L).orderId(orderId).userId(user).assetId(Asset.USD.id()).amount(amount)
                .omsManagedRelease(omsManagedRelease ? BoolFlag.TRUE : BoolFlag.FALSE);
        offer(client, MessageHeaderEncoder.ENCODED_LENGTH + holdEnc.encodedLength());
    }

    private void request(final AeronCluster client, final long correlationId, final boolean balances) {
        final int length;
        if (balances) {
            balanceReqEnc.wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(correlationId);
            length = MessageHeaderEncoder.ENCODED_LENGTH + balanceReqEnc.encodedLength();
        } else {
            holdReqEnc.wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(correlationId);
            length = MessageHeaderEncoder.ENCODED_LENGTH + holdReqEnc.encodedLength();
        }
        offer(client, length);
    }

    private static void awaitReply(final AeronCluster client, final Replies replies, final long correlationId,
                                   final boolean balances) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 20_000;
        while ((balances ? replies.balanceEndCorrelation : replies.holdEndCorrelation) != correlationId) {
            if (System.currentTimeMillis() > deadline) {
                fail("no " + (balances ? "BalanceSnapshotEnd" : "HoldSnapshotEnd") + " for correlationId "
                        + correlationId + " after restart");
            }
            if (client.pollEgress() == 0) {
                Thread.sleep(1);
            }
        }
    }

    private void offer(final AeronCluster client, final int length) {
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

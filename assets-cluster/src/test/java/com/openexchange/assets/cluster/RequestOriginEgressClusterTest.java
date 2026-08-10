// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckBatchDecoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Proves request-origin egress routing end-to-end through a real embedded cluster: request-scoped
 * events (acks, query replies) are delivered ONLY to the session that sent the command, while
 * genuinely shared streams (live BalanceUpdates) still broadcast to every subscribed session. Before
 * this routing existed, every session's queue got a copy of every frame — session count was an egress
 * multiplier — and consumers client-filtered foreign acks.
 *
 * <p>Two clients, A and B, both default-subscribed (CH_ALL — neither ever sends Subscribe):</p>
 * <ol>
 *   <li>A's Deposit: the DepositAck reaches A alone; the live BalanceUpdate reaches both.</li>
 *   <li>B's balance-snapshot query: entries + terminator reach B alone (the entries are ordinary
 *       BalanceUpdate frames — the query-reply context is what separates them from the live stream);
 *       a subsequent deposit's live update still reaches both, proving live vs reply separation.</li>
 *   <li>A's Hold: the HoldAck reaches A alone; the hold's BalanceUpdate reaches both.</li>
 * </ol>
 *
 * <p>Assertions are decode-level (template + correlation) and batch-aware: since schema v5 the drain
 * coalesces BalanceUpdate/HoldAck runs into batch frames, so a lone update may arrive as a batch of
 * one. Negative assertions ride on per-session egress ORDER: an ack is enqueued before its balance
 * update, so once a session has seen the update, the ack can no longer be in flight for it.</p>
 */
public class RequestOriginEgressClusterTest {

    private static final int PORT_BASE = 19600; // disjoint: boot smoke 19300, cadence 19400, cap 19500
    private static final long USER_A = 7001L;

    /** Batch-aware counts of what one session observed. Values are FixedPoint raw longs. */
    private static final class RecordingEgress implements EgressListener {
        final AtomicInteger depositAcks = new AtomicInteger();
        final AtomicLong lastDepositAckCorrelation = new AtomicLong(-1);
        final AtomicInteger balanceUpdateEntries = new AtomicInteger(); // singles + batch entries
        final AtomicLong lastAvailable = new AtomicLong(-1);
        final AtomicLong lastLocked = new AtomicLong(-1);
        final AtomicInteger snapshotEnds = new AtomicInteger();
        final AtomicLong lastSnapshotEndCorrelation = new AtomicLong(-1);
        final AtomicInteger lastSnapshotEndEntryCount = new AtomicInteger(-1);
        final AtomicInteger holdAcks = new AtomicInteger(); // singles + batch entries
        final AtomicLong lastHoldAckCorrelation = new AtomicLong(-1);

        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();
        private final BalanceUpdateDecoder balance = new BalanceUpdateDecoder();
        private final BalanceUpdateBatchDecoder balanceBatch = new BalanceUpdateBatchDecoder();
        private final BalanceSnapshotEndDecoder snapshotEnd = new BalanceSnapshotEndDecoder();
        private final HoldAckDecoder holdAck = new HoldAckDecoder();
        private final HoldAckBatchDecoder holdAckBatch = new HoldAckBatchDecoder();

        @Override
        public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer,
                              int offset, int length, Header aeronHeader) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case DepositAckDecoder.TEMPLATE_ID:
                    depositAck.wrapAndApplyHeader(buffer, offset, header);
                    lastDepositAckCorrelation.set(depositAck.correlationId());
                    depositAcks.incrementAndGet();
                    break;
                case BalanceUpdateDecoder.TEMPLATE_ID:
                    balance.wrapAndApplyHeader(buffer, offset, header);
                    lastAvailable.set(balance.available());
                    lastLocked.set(balance.locked());
                    balanceUpdateEntries.incrementAndGet();
                    break;
                case BalanceUpdateBatchDecoder.TEMPLATE_ID:
                    balanceBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final BalanceUpdateBatchDecoder.UpdatesDecoder u : balanceBatch.updates()) {
                        lastAvailable.set(u.available());
                        lastLocked.set(u.locked());
                        balanceUpdateEntries.incrementAndGet();
                    }
                    break;
                case BalanceSnapshotEndDecoder.TEMPLATE_ID:
                    snapshotEnd.wrapAndApplyHeader(buffer, offset, header);
                    lastSnapshotEndCorrelation.set(snapshotEnd.correlationId());
                    lastSnapshotEndEntryCount.set(snapshotEnd.entryCount());
                    snapshotEnds.incrementAndGet();
                    break;
                case HoldAckDecoder.TEMPLATE_ID:
                    holdAck.wrapAndApplyHeader(buffer, offset, header);
                    lastHoldAckCorrelation.set(holdAck.correlationId());
                    holdAcks.incrementAndGet();
                    break;
                case HoldAckBatchDecoder.TEMPLATE_ID:
                    holdAckBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final HoldAckBatchDecoder.AcksDecoder a : holdAckBatch.acks()) {
                        lastHoldAckCorrelation.set(a.correlationId());
                        holdAcks.incrementAndGet();
                    }
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    public void requestScopedEgressReachesOnlyTheOriginSession() {
        final File tmp = new File(System.getProperty("java.io.tmpdir"),
                "ae-request-origin-" + PORT_BASE);
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
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));

        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        AeronCluster clientA = null;
        AeronCluster clientB = null;
        final RecordingEgress egressA = new RecordingEgress();
        final RecordingEgress egressB = new RecordingEgress();
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                            .mtuLength(8192),
                    cfg.archiveContext(),
                    cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

            clientA = connect(egressA, aeronDir, hosts);
            clientB = connect(egressB, aeronDir, hosts);
            final AeronCluster a = clientA;
            final AeronCluster b = clientB;

            // ---- (a) A's deposit: ack to A only, live BalanceUpdate to both ----
            offer(a, encodeDeposit(0xA1L, USER_A, Asset.USD.id(), FixedPoint.fromDouble(100.0)));
            pumpUntil(a, b, () -> egressA.depositAcks.get() >= 1
                            && egressA.balanceUpdateEntries.get() >= 1
                            && egressB.balanceUpdateEntries.get() >= 1,
                    "deposit ack to A + balance update to both");
            assertEquals(0xA1L, egressA.lastDepositAckCorrelation.get());
            assertEquals(1, egressA.balanceUpdateEntries.get());
            // B saw the deposit's BalanceUpdate. Its queue is ordered and the ack (enqueued first)
            // never arrived: the ack was never queued for B, not merely still in flight.
            assertEquals("foreign DepositAck must not reach B", 0, egressB.depositAcks.get());
            assertEquals(1, egressB.balanceUpdateEntries.get());
            assertEquals(FixedPoint.fromDouble(100.0), egressB.lastAvailable.get());

            // ---- (b) B's balance-snapshot query: entries + terminator to B only ----
            offer(b, encodeRequestBalanceSnapshot(0xB1L));
            pumpUntil(a, b, () -> egressB.snapshotEnds.get() >= 1, "snapshot reply to B");
            assertEquals(0xB1L, egressB.lastSnapshotEndCorrelation.get());
            assertEquals("one non-zero (user,asset) exists", 1, egressB.lastSnapshotEndEntryCount.get());
            assertEquals("B: live update + snapshot entry", 2, egressB.balanceUpdateEntries.get());
            assertEquals("snapshot terminator must not reach A", 0, egressA.snapshotEnds.get());
            assertEquals("snapshot entries must not reach A", 1, egressA.balanceUpdateEntries.get());

            // ...and live updates after the query still broadcast: A deposits again, BOTH see it.
            offer(a, encodeDeposit(0xA2L, USER_A, Asset.USD.id(), FixedPoint.fromDouble(50.0)));
            final long avail150 = FixedPoint.fromDouble(150.0);
            pumpUntil(a, b, () -> egressA.depositAcks.get() >= 2
                            && egressA.lastAvailable.get() == avail150
                            && egressB.lastAvailable.get() == avail150,
                    "post-query live update to both");
            assertEquals(0xA2L, egressA.lastDepositAckCorrelation.get());
            // A's ordered stream has advanced past the query window with no snapshot frames: the
            // reply was never queued for A. B got exactly the snapshot entry more than A did.
            assertEquals(2, egressA.balanceUpdateEntries.get());
            assertEquals(3, egressB.balanceUpdateEntries.get());
            assertEquals(0, egressA.snapshotEnds.get());
            assertEquals(0, egressB.depositAcks.get());

            // ---- (c) A's hold: HoldAck to A only, the hold's BalanceUpdate to both ----
            offer(a, encodeHold(7777L, 7777L, USER_A, Asset.USD.id(), FixedPoint.fromDouble(25.0)));
            pumpUntil(a, b, () -> egressA.holdAcks.get() >= 1
                            && egressA.balanceUpdateEntries.get() >= 3
                            && egressB.balanceUpdateEntries.get() >= 4,
                    "hold ack to A + balance update to both");
            assertEquals(7777L, egressA.lastHoldAckCorrelation.get());
            assertEquals("foreign HoldAck must not reach B", 0, egressB.holdAcks.get());
            assertEquals(1, egressA.holdAcks.get());
            final long avail125 = FixedPoint.fromDouble(125.0);
            final long locked25 = FixedPoint.fromDouble(25.0);
            assertEquals(avail125, egressA.lastAvailable.get());
            assertEquals(avail125, egressB.lastAvailable.get());
            assertEquals(locked25, egressA.lastLocked.get());
            assertEquals(locked25, egressB.lastLocked.get());
        } finally {
            CloseHelper.quietClose(clientA);
            CloseHelper.quietClose(clientB);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
            IoUtil.delete(tmp, true);
        }
    }

    private static AeronCluster connect(final EgressListener listener, final String aeronDir,
                                        final List<String> hosts) {
        return AeronCluster.connect(new AeronCluster.Context()
                .egressListener(listener)
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .aeronDirectoryName(aeronDir)
                .ingressEndpoints(ClusterConfig.ingressEndpoints(
                        hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
    }

    /** Poll BOTH sessions until the condition holds — so "did not receive" is absence, not lag. */
    private static void pumpUntil(final AeronCluster a, final AeronCluster b,
                                  final BooleanSupplier condition, final String what) {
        final long deadline = System.currentTimeMillis() + 15_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (!condition.getAsBoolean()) {
            assertTrue("timed out waiting for: " + what, System.currentTimeMillis() < deadline);
            a.pollEgress();
            b.pollEgress();
            idle.idle();
        }
    }

    // ---- ingress SBE encoding ----

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final HoldEncoder holdEnc = new HoldEncoder();
    private final RequestBalanceSnapshotEncoder balanceSnapshotEnc = new RequestBalanceSnapshotEncoder();

    private int encodeDeposit(long correlationId, long userId, int assetId, long amount) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount);
        return MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();
    }

    /** The loadgen's hold shape: correlationId == orderId (one id, several roles). */
    private int encodeHold(long correlationId, long orderId, long userId, int assetId, long amount) {
        holdEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(correlationId).orderId(orderId).userId(userId).assetId(assetId)
                .amount(amount).omsManagedRelease(BoolFlag.FALSE);
        return MessageHeaderEncoder.ENCODED_LENGTH + holdEnc.encodedLength();
    }

    private int encodeRequestBalanceSnapshot(long correlationId) {
        balanceSnapshotEnc.wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(correlationId);
        return MessageHeaderEncoder.ENCODED_LENGTH + balanceSnapshotEnc.encodedLength();
    }

    private void offer(AeronCluster client, int length) {
        final long deadline = System.currentTimeMillis() + 10_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            long result = client.offer(ingress, 0, length);
            if (result > 0) {
                return;
            }
            client.pollEgress(); // keep the session serviced while backpressured
            idle.idle();
        }
        throw new IllegalStateException("failed to offer ingress within timeout (result kept <= 0)");
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Proves the close-at-cap decision end-to-end through a real embedded cluster: a RESUMABLE consumer
 * (one that sent QueryFeedPosition) that stops draining its egress gets its session CLOSED when its
 * queue hits the growth ceiling — and the engine keeps serving everyone else. Before this decision
 * existed, the leader's single deterministic thread blocked in {@code appendAfterFreeingSpace} on the
 * wedged consumer's full queue for as long as the wedge lasted, taking every session's acks down
 * with it.
 *
 * <p>Two knobs make the ceiling reachable in test time instead of after 16MB of egress: the queue cap
 * is lowered to 64KB via its transport-tuning property, and the wedged client's egress term buffer to
 * 64KB (so transport back-pressure starts after ~32KB and the QUEUE, not the term buffer, absorbs the
 * rest). The property is read once when {@code SessionEgressQueue} loads, which is safe here because
 * surefire runs every test class in its own JVM ({@code reuseForks=false}): the static block below
 * runs before any cluster class is initialised, and no other test class ever sees the small cap.</p>
 */
public class EgressCloseAtCapClusterTest {

    static {
        // Before any cluster class loads (static-final read in SessionEgressQueue): 64KB ceiling.
        System.setProperty("assets.egress.max.capacity.bytes", String.valueOf(1 << 16));
    }

    private static final int PORT_BASE = 19500; // disjoint from boot smoke (19300) and cadence (19400)
    private static final long USER = 5151L;

    /** Captures the last observed available balance, batch or single (v5 coalescing). */
    private static final class BalanceEgress implements EgressListener {
        final AtomicLong lastAvail = new AtomicLong(-1);
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final BalanceUpdateDecoder balance = new BalanceUpdateDecoder();
        private final BalanceUpdateBatchDecoder balanceBatch = new BalanceUpdateBatchDecoder();

        @Override
        public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer,
                              int offset, int length, Header aeronHeader) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case BalanceUpdateDecoder.TEMPLATE_ID:
                    balance.wrapAndApplyHeader(buffer, offset, header);
                    lastAvail.set(balance.available());
                    break;
                case BalanceUpdateBatchDecoder.TEMPLATE_ID:
                    balanceBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final BalanceUpdateBatchDecoder.UpdatesDecoder u : balanceBatch.updates()) {
                        lastAvail.set(u.available());
                    }
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    public void closesTheResumableSessionAtItsCapAndTheClusterStaysLive() {
        final File tmp = new File(System.getProperty("java.io.tmpdir"),
                "ae-close-at-cap-" + PORT_BASE);
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
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                // The wedged session must die by the close-at-cap DECISION, never by session timeout:
                // a timeout close would pass the liveness assert while proving nothing.
                .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(60));

        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        AeronCluster wedged = null;  // bridge-shaped: marks itself resumable, then stops polling
        AeronCluster healthy = null; // proves the engine keeps flowing while the wedged one dies
        final BalanceEgress healthyEgress = new BalanceEgress();
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                            .mtuLength(8192),
                    cfg.archiveContext(),
                    cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

            wedged = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) -> { })
                    // 64K term: transport back-pressure after ~32KB un-polled, so the session's queue
                    // is what accumulates toward the (lowered) cap.
                    .egressChannel("aeron:udp?endpoint=localhost:0|term-length=64k")
                    .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                    .aeronDirectoryName(aeronDir)
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(
                            hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
            healthy = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(healthyEgress)
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                    .aeronDirectoryName(aeronDir)
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(
                            hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));

            // Step 1 of the resume protocol: the wedged client identifies itself as a feed consumer.
            // After this offer it never polls egress again — the metal-drill wedge, in miniature.
            offer(wedged, encodeQueryFeedPosition(0xFEEDL));

            // Pump: every deposit's egress fans out to BOTH sessions' queues. The healthy session
            // drains; the wedged one absorbs ~32KB in its response publication, then its queue grows
            // to the 64KB cap, where the engine must CLOSE it rather than block.
            final long floodDeadline = System.currentTimeMillis() + 60_000;
            int sent = 0;
            while (service.resumableSessionsClosed() == 0) {
                assertTrue("cap not reached before the deadline (deposits sent=" + sent + ")",
                        System.currentTimeMillis() < floodDeadline);
                offer(healthy, encodeDeposit(USER, Asset.USD.id(), FixedPoint.fromDouble(1.0)));
                sent++;
                healthy.pollEgress();
            }

            // (a) server side: the close-at-cap decision fired for the resumable session.
            assertTrue(service.resumableSessionsClosed() >= 1);

            // (a) client side: the wedged client observes the close terminally — the cluster closes
            // its response publication, pollEgress detects the dead egress image, and from then on
            // the client is closed / its offers fail. (SessionClosedException in the bridge.)
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            boolean terminal = false;
            final long terminalDeadline = System.currentTimeMillis() + 20_000;
            while (!terminal && System.currentTimeMillis() < terminalDeadline) {
                try {
                    wedged.pollEgress();
                    terminal = wedged.isClosed();
                    if (!terminal) {
                        wedged.offer(ingress, 0, encodeQueryFeedPosition(0xDEADL));
                    }
                } catch (final RuntimeException ex) {
                    terminal = true; // ClusterException: session/ingress closed
                }
                idle.idle();
            }
            assertTrue("the wedged client must observe its session close terminally", terminal);

            // (b) the engine never stopped: a fresh deposit on the healthy session round-trips.
            final long expectAvail = FixedPoint.fromDouble(777.0);
            offer(healthy, encodeDeposit(USER + 1, Asset.USD.id(), expectAvail));
            final long ackDeadline = System.currentTimeMillis() + 15_000;
            idle.reset();
            while (System.currentTimeMillis() < ackDeadline
                    && healthyEgress.lastAvail.get() != expectAvail) {
                healthy.pollEgress();
                idle.idle();
            }
            assertEquals("cluster must stay live for the healthy session after the close",
                    expectAvail, healthyEgress.lastAvail.get());
        } finally {
            CloseHelper.quietClose(healthy);
            CloseHelper.quietClose(wedged);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
            IoUtil.delete(tmp, true);
        }
    }

    // ---- ingress SBE encoding ----

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final QueryFeedPositionEncoder queryEnc = new QueryFeedPositionEncoder();

    private int encodeDeposit(long userId, int assetId, long amount) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0L).userId(userId).assetId(assetId).amount(amount);
        return MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();
    }

    private int encodeQueryFeedPosition(long correlationId) {
        queryEnc.wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(correlationId);
        return MessageHeaderEncoder.ENCODED_LENGTH + queryEnc.encodedLength();
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

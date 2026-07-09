// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Boots a real single-node, embedded-driver, <b>backoff-idle</b> Assets Engine cluster in-process on
 * isolated ports/dirs, connects an Aeron cluster client, submits DEPOSIT + HOLD as SBE ingress over the
 * wire, and verifies the SBE egress. This is the first proof that the whole node actually runs — the
 * {@code AssetsClusteredService} → demuxer → engine → publisher path works end-to-end through real
 * consensus + archive, not just in-process against a plain engine.
 *
 * <p>Backoff idle (not busy-spin) keeps the footprint low enough to co-reside with the live stack.
 * Everything is torn down and the temp dirs deleted in a finally block.</p>
 */
public class ClusterBootSmokeTest {

    private static final int PORT_BASE = 19300; // disjoint from ME (9000-9299) and reserved AE (9300)
    private static final long USER = 4242L;
    private static final long HOLD_CORR = 0xC0FFEEL; // client correlationId echoed on the HoldAck

    /** Captures decoded egress messages on the client's poll thread. */
    private static final class CapturingEgress implements EgressListener {
        final AtomicInteger holdAcks = new AtomicInteger();
        final AtomicLong lastHoldAckCorr = new AtomicLong(-1);
        final AtomicLong lastLocked = new AtomicLong(-1);
        final AtomicLong lastAvail = new AtomicLong(-1);
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final HoldAckDecoder holdAck = new HoldAckDecoder();
        private final BalanceUpdateDecoder balance = new BalanceUpdateDecoder();

        @Override
        public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer,
                              int offset, int length, Header aeronHeader) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case HoldAckDecoder.TEMPLATE_ID:
                    // v2 HoldAck: correlationId is the first field, ahead of orderId.
                    holdAck.wrapAndApplyHeader(buffer, offset, header);
                    lastHoldAckCorr.set(holdAck.correlationId());
                    holdAcks.incrementAndGet();
                    break;
                case BalanceUpdateDecoder.TEMPLATE_ID:
                    balance.wrapAndApplyHeader(buffer, offset, header);
                    lastAvail.set(balance.available());
                    lastLocked.set(balance.locked());
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    public void bootsAcceptsIngressAndEmitsEgress() {
        final File tmp = new File(System.getProperty("java.io.tmpdir"),
                "ae-boot-smoke-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");

        final List<String> hosts = List.of("localhost");
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, new AssetsClusteredService());
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
        AeronCluster client = null;
        final CapturingEgress egress = new CapturingEgress();
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                            .mtuLength(8192),
                    cfg.archiveContext(),
                    cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

            client = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(egress)
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                    .aeronDirectoryName(aeronDir)
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(
                            hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));

            // DEPOSIT 1000 USD, then HOLD 600 USD for order 1 (carrying a correlationId to echo).
            offer(client, encodeDeposit(USER, Asset.USD.id(), FixedPoint.fromDouble(1000.0)));
            offer(client, encodeHold(HOLD_CORR, 1L, USER, Asset.USD.id(), FixedPoint.fromDouble(600.0)));

            // Poll egress until we've seen the HoldAck + the post-hold BalanceUpdate (locked=600), or time out.
            final long expectLocked = FixedPoint.fromDouble(600.0);
            final long deadline = System.currentTimeMillis() + 15_000;
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (System.currentTimeMillis() < deadline) {
                int work = client.pollEgress();
                if (egress.holdAcks.get() >= 1 && egress.lastLocked.get() == expectLocked) {
                    break;
                }
                idle.idle(work);
            }

            assertTrue("expected a HoldAck egress", egress.holdAcks.get() >= 1);
            assertEquals("HoldAck echoes the request correlationId", HOLD_CORR, egress.lastHoldAckCorr.get());
            assertEquals("post-hold locked balance", expectLocked, egress.lastLocked.get());
            assertEquals("post-hold available balance", FixedPoint.fromDouble(400.0), egress.lastAvail.get());
        } finally {
            CloseHelper.quietClose(client);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
            IoUtil.delete(tmp, true);
        }
    }

    // ---- ingress SBE encoding ----

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final HoldEncoder holdEnc = new HoldEncoder();

    private int encodeDeposit(long userId, int assetId, long amount) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0L).userId(userId).assetId(assetId).amount(amount);
        return MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();
    }

    private int encodeHold(long correlationId, long orderId, long userId, int assetId, long amount) {
        holdEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(correlationId).orderId(orderId).userId(userId).assetId(assetId).amount(amount);
        return MessageHeaderEncoder.ENCODED_LENGTH + holdEnc.encodedLength();
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

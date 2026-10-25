// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The node's own {@code /metrics} answers the RC0 question "what does one ingress message cost the
 * service thread" from a real cluster: apply and drain histograms, per-template counts, role and
 * log position, all read over HTTP from the cluster-kit {@code NodeEndpoint} the service starts.
 */
public class NodeMetricsClusterTest {

    private static final int PORT_BASE = 19600; // disjoint from 19300/19400/19500
    private static final int NODE_PORT = 19760; // /health /ready /metrics
    private static final int DEPOSITS = 300;
    private static final long USER = 4242L;

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final MessageHeaderDecoder headerDec = new MessageHeaderDecoder();
    private int acks;

    @Test
    public void metricsReportServiceThreadCostPerMessage() throws Exception {
        System.setProperty("assets.node.port", Integer.toString(NODE_PORT));
        System.setProperty("snapshot.interval.minutes", "0");

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-node-metrics-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final List<String> hosts = List.of("localhost");
        final AssetsClusteredService service = new AssetsClusteredService();
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        cfg.baseDir(new File(tmp, "node0"));
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

        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        AeronCluster client = null;
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                    cfg.archiveContext(), cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());
            client = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener((sessionId, timestamp, buffer, offset, length, header) -> {
                        if (length >= MessageHeaderDecoder.ENCODED_LENGTH
                                && headerDec.wrap(buffer, offset).templateId() == DepositAckDecoder.TEMPLATE_ID) {
                            acks++;
                        }
                    })
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .ingressChannel("aeron:udp?term-length=64k")
                    .aeronDirectoryName(aeronDir)
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(
                            hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));

            for (int i = 1; i <= DEPOSITS; i++) {
                deposit(client, i);
            }
            final long deadline = System.currentTimeMillis() + 20_000;
            while (acks < DEPOSITS) {
                assertTrue("only " + acks + " deposit acks within 20s", System.currentTimeMillis() < deadline);
                if (client.pollEgress() == 0) {
                    Thread.sleep(1);
                }
            }

            final HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + NODE_PORT + "/metrics")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            final String body = response.body();
            System.out.println("[TEST] /metrics " + body.length() + " bytes");

            assertTrue(body, value(body, "assets_apply_dispatch_seconds_count") >= DEPOSITS);
            assertTrue(body, value(body, "assets_apply_drain_seconds_count") >= DEPOSITS);
            assertTrue(body, value(body, "assets_apply_template_dispatch_count{template=\"1\"}") >= DEPOSITS);
            assertTrue(body, doubleValue(body, "assets_apply_dispatch_seconds_max") > 0);
            assertTrue(body, doubleValue(body, "assets_apply_dispatch_seconds_sum") > 0);
            assertEquals(2, value(body, "assets_cluster_role"));
            assertEquals(0, value(body, "assets_member_id"));
            assertEquals(1, value(body, "assets_accounts"));
            assertEquals(1, value(body, "assets_sessions"));
            assertEquals(0, value(body, "assets_application_callback_failed"));
            assertTrue(body, value(body, "assets_log_position") > 0);
            assertEquals(-1, value(body, "assets_snapshot_last_bytes"));
            assertTrue(body, value(body, "assets_timer_drain_seconds_count") >= 1);
            System.out.println("[TEST] apply p-max " + doubleValue(body, "assets_apply_dispatch_seconds_max") * 1e6
                    + " us, mean " + doubleValue(body, "assets_apply_dispatch_seconds_sum")
                    / value(body, "assets_apply_dispatch_seconds_count") * 1e6 + " us over "
                    + value(body, "assets_apply_dispatch_seconds_count") + " messages (dev box, SHARED driver)");
        } finally {
            CloseHelper.quietClose(client);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
            IoUtil.delete(tmp, true);
            System.clearProperty("assets.node.port");
            System.clearProperty("snapshot.interval.minutes");
        }
    }

    private static long value(final String body, final String series) {
        return (long) doubleValue(body, series);
    }

    private static double doubleValue(final String body, final String series) {
        for (final String line : body.split("\n")) {
            if (line.startsWith(series + " ")) {
                return Double.parseDouble(line.substring(series.length() + 1).trim());
            }
        }
        throw new AssertionError("series not found: " + series + "\n" + body);
    }

    private void deposit(final AeronCluster client, final long correlationId) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(correlationId).userId(USER).assetId(Asset.USD.id())
                .amount(FixedPoint.fromDouble(1.0));
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();
        final long deadline = System.currentTimeMillis() + 10_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            if (client.offer(ingress, 0, length) > 0) {
                return;
            }
            client.pollEgress();
            idle.idle();
        }
        throw new IllegalStateException("failed to offer ingress within timeout");
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.application.proto.ProtoOrderEngine;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderCancelEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderCreateEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderEventDecoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderFillEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderType;
import com.openexchange.assets.infrastructure.generated.ProtoRiskConfigEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoSide;
import com.openexchange.assets.infrastructure.generated.ProtoTimeInForce;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;
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
import static org.junit.Assert.fail;

/**
 * RC0 spike, end to end on a real single-node cluster: the prototype order lifecycle over the wire,
 * GTD expiry through a real cluster timer, a chunked + enveloped v4 snapshot taken by the engine's
 * own cadence, the log pruned beneath it, a cold restart, and the restored order state answering
 * cancels correctly. Per-template apply costs are read from {@code /metrics} and printed.
 */
public class ProtoOrderClusterTest {

    private static final int PORT_BASE = 20100; // disjoint from 19300..20000 used by the other cluster tests
    private static final int NODE_PORT = 20160;
    private static final int SEGMENT_LENGTH = 64 * 1024;
    private static final int USERS = 200;
    private static final int ORDERS = 1_000;
    private static final int FILLS = 200;
    private static final int CANCELS = 100;
    private static final long TIMEOUT_MS = 90_000;

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final ProtoOrderCreateEncoder createEnc = new ProtoOrderCreateEncoder();
    private final ProtoOrderCancelEncoder cancelEnc = new ProtoOrderCancelEncoder();
    private final ProtoOrderFillEncoder fillEnc = new ProtoOrderFillEncoder();
    private final ProtoRiskConfigEncoder riskEnc = new ProtoRiskConfigEncoder();

    /** Order events seen by this session: correlated acks by correlationId, plus every broadcast. */
    static final class Events implements EgressListener {
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final ProtoOrderEventDecoder ev = new ProtoOrderEventDecoder();
        final Long2LongHashMap statusByCorrelation = new Long2LongHashMap(-1L);
        final Long2LongHashMap reasonByCorrelation = new Long2LongHashMap(-1L);
        final Long2LongHashMap orderIdByCorrelation = new Long2LongHashMap(-1L);
        final LongHashSet open = new LongHashSet();
        int filled, expired, cancelled, broadcasts, total;

        public void onMessage(final long sessionId, final long timestamp, final org.agrona.DirectBuffer buffer,
                              final int offset, final int length, final io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            if (header.templateId() != ProtoOrderEventDecoder.TEMPLATE_ID) {
                return;
            }
            ev.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
            total++;
            final int status = ev.status().value();
            final long corr = ev.correlationId();
            if (corr != 0) {
                statusByCorrelation.put(corr, status);
                reasonByCorrelation.put(corr, ev.reason().value());
                orderIdByCorrelation.put(corr, ev.orderId());
            } else {
                broadcasts++;
            }
            switch (status) {
                case ProtoOrderEngine.ST_NEW: open.add(ev.orderId()); break;
                case ProtoOrderEngine.ST_FILLED: filled++; open.remove(ev.orderId()); break;
                case ProtoOrderEngine.ST_CANCELLED: cancelled++; open.remove(ev.orderId()); break;
                case ProtoOrderEngine.ST_EXPIRED: expired++; open.remove(ev.orderId()); break;
                default: break;
            }
        }
    }

    private static final class Node {
        ClusteredMediaDriver driver;
        ClusteredServiceContainer container;
        ClusterConfig cfg;

        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(driver);
        }
    }

    @Test
    public void lifecycleSnapshotRestartOnTheRealCluster() throws Exception {
        System.setProperty("snapshot.log.bytes", Integer.toString(SEGMENT_LENGTH));
        System.setProperty("snapshot.interval.minutes", "0");
        System.setProperty("transport.snapshot.term.length", "64k");
        System.setProperty("assets.node.port", Integer.toString(NODE_PORT));

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-proto-order-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");
        final List<String> hosts = List.of("localhost");

        Node node = null;
        AeronCluster client = null;
        final Events events = new Events();
        final Events after = new Events();
        try {
            node = boot(hosts, baseDir, aeronDir);
            client = connect(hosts, aeronDir, events);

            riskConfig(client);
            for (long u = 1; u <= USERS; u++) {
                deposit(client, u, Asset.USD.id(), FixedPoint.fromDouble(1_000_000));
                deposit(client, u, Asset.BTC.id(), FixedPoint.fromDouble(100));
            }
            // 1. Create: alternating buys and sells, 5 per user, with clientOrderIds.
            long corr = 1;
            for (int i = 0; i < ORDERS; i++) {
                create(client, corr++, 1 + (i % USERS), (i & 1) == 0 ? ProtoSide.BUY : ProtoSide.SELL,
                        FixedPoint.fromDouble(50_000), FixedPoint.fromDouble(0.01), 100_000L + i, ProtoTimeInForce.GTC, 0L);
            }
            awaitAcks(client, events, 1, ORDERS, ProtoOrderEngine.ST_NEW);
            assertEquals(ORDERS, events.open.size());

            // 2. Fills: buy 2k+1 against sell 2k+2 (orderIds are dense from 1).
            for (int k = 0; k < FILLS; k++) {
                final long buyOrder = events.orderIdByCorrelation.get(2L * k + 1);
                final long sellOrder = events.orderIdByCorrelation.get(2L * k + 2);
                fill(client, 1 + k, buyOrder, sellOrder, FixedPoint.fromDouble(50_000), FixedPoint.fromDouble(0.01));
            }
            awaitCount(client, () -> events.filled >= 2 * FILLS, "filled events: " + events.filled);

            // 3. Cancels of the last CANCELS creates (their owners).
            final long cancelBase = corr;
            for (int k = 0; k < CANCELS; k++) {
                final long createCorr = ORDERS - k;
                final long orderId = events.orderIdByCorrelation.get(createCorr);
                cancel(client, corr++, 1 + ((int) (createCorr - 1) % USERS), orderId);
            }
            awaitAcks(client, events, cancelBase, CANCELS, ProtoOrderEngine.ST_CANCELLED);

            // 4. GTD: a real cluster timer expires the order.
            final long gtdCorr = corr++;
            final long expiresAt = System.currentTimeMillis() + 1_500;
            create(client, gtdCorr, 1, ProtoSide.BUY, FixedPoint.fromDouble(50_000), FixedPoint.fromDouble(0.01), 0L,
                    ProtoTimeInForce.GTD, expiresAt);
            awaitAcks(client, events, gtdCorr, 1, ProtoOrderEngine.ST_NEW);
            awaitCount(client, () -> events.expired >= 1, "GTD expiry via cluster timer");
            final int expectedOpen = ORDERS - 2 * FILLS - CANCELS;
            assertEquals(expectedOpen, events.open.size());

            // 5. The engine snapshots on its own (64 KiB term: chunked + enveloped) and prunes the log.
            final File clusterDir = node.cfg.consensusModuleContext().clusterDir();
            final File archiveDir = node.cfg.archiveContext().archiveDir();
            long filler = 0;
            final long snapDeadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (!hasServiceSnapshot(clusterDir)) {
                if (System.currentTimeMillis() > snapDeadline) {
                    fail("no service snapshot within " + TIMEOUT_MS + " ms");
                }
                deposit(client, 1, Asset.USD.id(), FixedPoint.fromDouble(1));
                filler++;
                Thread.sleep(200);
            }
            final long logRecordingId;
            try (RecordingLog log = new RecordingLog(clusterDir, false)) {
                logRecordingId = log.findLastTermRecordingId();
            }
            final File firstSegment = new File(archiveDir, logRecordingId + "-0.rec");
            final long pruneDeadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (firstSegment.exists()) {
                if (System.currentTimeMillis() > pruneDeadline) {
                    fail("first log segment never reclaimed");
                }
                deposit(client, 1, Asset.USD.id(), FixedPoint.fromDouble(1));
                filler++;
                Thread.sleep(200);
            }

            // 6. Per-template cost from /metrics (dev box: a shape, not a measurement).
            final String metrics = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + NODE_PORT + "/metrics")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            for (final int t : new int[] {1, 40, 41, 42}) {
                final double sum = value(metrics, "assets_apply_template_dispatch_seconds_sum{template=\"" + t + "\"}");
                final double count = value(metrics, "assets_apply_template_dispatch_count{template=\"" + t + "\"}");
                final double max = value(metrics, "assets_apply_template_dispatch_max_seconds{template=\"" + t + "\"}");
                System.out.printf("[TEST] template %d: n=%.0f mean %.1f us max %.1f us%n", t, count, sum / count * 1e6, max * 1e6);
            }
            assertTrue(value(metrics, "assets_apply_template_dispatch_count{template=\"40\"}") >= ORDERS + 1);
            assertTrue(value(metrics, "assets_apply_template_dispatch_count{template=\"42\"}") >= FILLS);
            assertTrue(value(metrics, "assets_snapshot_last_chunks") >= 2);
            System.out.println("[TEST] snapshot bytes=" + (long) value(metrics, "assets_snapshot_last_bytes")
                    + " chunks=" + (long) value(metrics, "assets_snapshot_last_chunks") + " open=" + expectedOpen
                    + " filler deposits=" + filler);

            // 7. Cold restart on the same dirs: the only source of the orders is the v4 snapshot + log tail.
            CloseHelper.quietClose(client);
            client = null;
            node.close();
            node = boot(hosts, baseDir, aeronDir);
            client = connect(hosts, aeronDir, after);

            // An open order cancels (restored); a filled one is unknown; a live clientOrderId is a duplicate.
            final long openOrderId = 2L * FILLS + 1;                       // create corr 401: never filled or cancelled
            final long openOwner = 1 + ((2 * FILLS) % USERS);
            cancel(client, 9_001, openOwner, openOrderId);
            awaitAcks(client, after, 9_001, 1, ProtoOrderEngine.ST_CANCELLED);
            cancel(client, 9_002, 1, 1L);                                   // orderId 1 was filled
            awaitAcks(client, after, 9_002, 1, ProtoOrderEngine.ST_REJECTED);
            assertEquals(ProtoOrderEngine.RR_UNKNOWN_ORDER, after.reasonByCorrelation.get(9_002));
            final long dupOwner = 1 + ((2 * FILLS + 1) % USERS);
            create(client, 9_003, dupOwner, ProtoSide.BUY, FixedPoint.fromDouble(50_000), FixedPoint.fromDouble(0.01),
                    100_000L + 2 * FILLS + 1, ProtoTimeInForce.GTC, 0L); // cid of create #402, still open
            awaitAcks(client, after, 9_003, 1, ProtoOrderEngine.ST_REJECTED);
            assertEquals(ProtoOrderEngine.RR_DUPLICATE_CLIENT_ORDER_ID, after.reasonByCorrelation.get(9_003));
            System.out.println("[TEST] restored: open order cancelled, filled order unknown, live clientOrderId duplicate");
        } finally {
            CloseHelper.quietClose(client);
            if (node != null) {
                node.close();
            }
            IoUtil.delete(tmp, true);
            System.clearProperty("snapshot.log.bytes");
            System.clearProperty("snapshot.interval.minutes");
            System.clearProperty("transport.snapshot.term.length");
            System.clearProperty("assets.node.port");
        }
    }

    private static boolean hasServiceSnapshot(final File clusterDir) {
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Node boot(final List<String> hosts, final File baseDir, final String aeronDir) {
        final Node n = new Node();
        final AssetsClusteredService service = new AssetsClusteredService();
        n.cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        n.cfg.baseDir(baseDir);
        n.cfg.aeronDirectoryName(aeronDir);
        n.cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        n.cfg.errorHandler(Throwable::printStackTrace);
        n.cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        n.cfg.archiveContext().segmentFileLength(SEGMENT_LENGTH);
        n.driver = ClusteredMediaDriver.launch(
                n.cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                n.cfg.archiveContext(), n.cfg.consensusModuleContext());
        n.container = ClusteredServiceContainer.launch(n.cfg.clusteredServiceContext());
        return n;
    }

    private static AeronCluster connect(final List<String> hosts, final String aeronDir, final EgressListener listener)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .egressListener(listener)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp?term-length=64k")
                        .aeronDirectoryName(aeronDir)
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
            } catch (final RuntimeException e) {
                last = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect", last);
    }

    private interface Condition {
        boolean met();
    }

    private static void awaitCount(final AeronCluster client, final Condition c, final String what) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (!c.met()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + what);
            }
            if (client.pollEgress() == 0) {
                Thread.sleep(1);
            }
        }
    }

    private static void awaitAcks(final AeronCluster client, final Events events, final long firstCorr, final int n,
                                  final int expectedStatus) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        for (long c = firstCorr; c < firstCorr + n; c++) {
            while (events.statusByCorrelation.get(c) == -1L) {
                if (System.currentTimeMillis() > deadline) {
                    fail("no ack for correlationId " + c + " (have " + events.total + " events)");
                }
                if (client.pollEgress() == 0) {
                    Thread.sleep(1);
                }
            }
            assertEquals("status for correlationId " + c + " reason=" + events.reasonByCorrelation.get(c),
                    expectedStatus, events.statusByCorrelation.get(c));
        }
    }

    private void riskConfig(final AeronCluster client) {
        riskEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0L).marketId(1).maxOrdersPerSec(1_000_000).maxOrdersPerMin(10_000_000)
                .minQty(1L).maxQty(Long.MAX_VALUE / 4).maxNotional(Long.MAX_VALUE / 4).collarPct(20)
                .maxOpenOrders(100_000).maxPositionPerMarket(Long.MAX_VALUE).circuitBreaker(BoolFlag.FALSE);
        offer(client, MessageHeaderEncoder.ENCODED_LENGTH + riskEnc.encodedLength());
    }

    private void deposit(final AeronCluster client, final long user, final int asset, final long amount) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(0L).userId(user).assetId(asset).amount(amount);
        offer(client, MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength());
    }

    private void create(final AeronCluster client, final long corr, final long user, final ProtoSide side, final long price,
                        final long qty, final long clientOrderId, final ProtoTimeInForce tif, final long expiresAtMs) {
        createEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).userId(user).clientOrderIdHigh(clientOrderId == 0 ? 0 : 0x5A5AL).clientOrderIdLow(clientOrderId)
                .requestIdHigh(0L).requestIdLow(corr).marketId(1).side(side).orderType(ProtoOrderType.LIMIT).timeInForce(tif)
                .price(price).quantity(qty).stopPrice(0L).displayQty(0L).expiresAtMs(expiresAtMs).flags(0L);
        offer(client, MessageHeaderEncoder.ENCODED_LENGTH + createEnc.encodedLength());
    }

    private void cancel(final AeronCluster client, final long corr, final long user, final long orderId) {
        cancelEnc.wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(corr).userId(user).orderId(orderId);
        offer(client, MessageHeaderEncoder.ENCODED_LENGTH + cancelEnc.encodedLength());
    }

    private void fill(final AeronCluster client, final long tradeId, final long buyOrderId, final long sellOrderId,
                      final long price, final long qty) {
        fillEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0L).tradeId(tradeId).marketId(1).buyOrderId(buyOrderId).sellOrderId(sellOrderId)
                .price(price).quantity(qty).takerIsBuy(BoolFlag.TRUE);
        offer(client, MessageHeaderEncoder.ENCODED_LENGTH + fillEnc.encodedLength());
    }

    private void offer(final AeronCluster client, final int length) {
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

    private static double value(final String body, final String series) {
        for (final String line : body.split("\n")) {
            if (line.startsWith(series + " ")) {
                return Double.parseDouble(line.substring(series.length() + 1).trim());
            }
        }
        throw new AssertionError("series not found: " + series);
    }
}

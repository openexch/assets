// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.application.engine.feed.FeedState;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.ConfigDecision;
import com.openexchange.assets.infrastructure.generated.CoreConfigEncoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.GateReason;
import com.openexchange.assets.infrastructure.generated.GateState;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.security.CredentialsSupplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S7 16.4 on a real single-member cluster: behaviour 3 (the supported set widened on EVERY service instance through the
 * engine seam, before its container starts; no env or property knob, review 2 NOTE-F), the ME epoch bound by the FRESH
 * record from a registered BRIDGE, a GAP latch, a service snapshot, a cold restart on the same dirs: the restored node
 * reports the same FeedPositionReport, the missing entries then apply and reach-clear the latch, and ADMIN OPEN passes.
 */
public class FeedStateRestoreClusterTest {

    private static final int PORT_BASE = 21000;
    private static final long TIMEOUT_MS = 20_000;
    private static final int SNAPSHOT_LOG_BYTES = 32 * 1024;
    private static final long GENESIS = 0x5EEDL;
    private static final long ONE = FixedPoint.fromDouble(1.0);

    private static final class Capture implements EgressListener {
        final List<String> frames = new CopyOnWriteArrayList<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final AdminAckDecoder ack = new AdminAckDecoder();
        private final FeedPositionReportDecoder report = new FeedPositionReportDecoder();

        public void onMessage(final long sessionId, final long timestamp, final DirectBuffer buffer, final int offset,
                              final int length, final io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case AdminAckDecoder.TEMPLATE_ID:
                    ack.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("ADMINACK ref=" + ack.refTemplateId() + " corr=" + ack.correlationId() + " result=" + ack.result()
                            + " reason=" + ack.reason());
                    break;
                case FeedPositionReportDecoder.TEMPLATE_ID:
                    report.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("FEEDPOS corr=" + report.correlationId() + " epoch=" + report.meEpoch() + " jseq="
                            + report.lastAppliedJournalSeq() + " halted=" + report.feedHalted() + " reason=" + report.haltReason()
                            + " consumePos=" + report.consumePosition());
                    break;
                default:
                    break;
            }
        }

        String await(final AeronCluster client, final Predicate<String> match, final String why) {
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            long keepAlive = System.currentTimeMillis();
            while (System.currentTimeMillis() < deadline) {
                for (final String f : frames) {
                    if (match.test(f)) {
                        return f;
                    }
                }
                client.pollEgress();
                if (System.currentTimeMillis() - keepAlive >= 1_000) {
                    client.sendKeepAlive();
                    keepAlive = System.currentTimeMillis();
                }
                idle.idle();
            }
            fail(why + "; frames seen: " + frames);
            return null;
        }
    }

    private static final class Node {
        final ClusterConfig cfg;
        final AssetsClusteredService service;
        final ClusteredMediaDriver mediaDriver;
        final ClusteredServiceContainer container;

        Node(final ClusterConfig cfg, final AssetsClusteredService service, final ClusteredMediaDriver mediaDriver,
             final ClusteredServiceContainer container) {
            this.cfg = cfg;
            this.service = service;
            this.mediaDriver = mediaDriver;
            this.container = container;
        }

        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
        }
    }

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();

    private int coreConfig(final long corr, final long version) {
        final CoreConfigEncoder e = new CoreConfigEncoder().wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(corr)
                .configVersion(version).behaviorVersion(version).genesisId(GENESIS);
        final CoreConfigEncoder.MarketsEncoder m = e.marketsCount(Market.ALL.length);
        for (final Market d : Market.ALL) {
            m.next().marketId(d.marketId()).symbol(d.symbol()).baseAssetId(d.baseAsset().id()).quoteAssetId(d.quoteAsset().id())
                    .tickSize(1).minPrice(1).maxPrice(1_000_000_000_000_000L).lotSize(0);
        }
        final CoreConfigEncoder.AssetsEncoder a = e.assetsCount(Asset.values().length);
        for (final Asset d : Asset.values()) {
            a.next().assetId(d.id()).code(d.name()).scale((short) 8);
        }
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int engineConfig(final long js, final long cfg, final ConfigDecision decision) {
        final MeEngineConfigEncoder e = new MeEngineConfigEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .egressSeq(100 + js).journalSeq(js).meEpoch(GENESIS).configVersion(cfg).behaviorVersion(2).ledgerCapacity(4096)
                .maxWireVersion(12).decision(decision);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int query(final long corr) {
        final QueryFeedPositionEncoder e = new QueryFeedPositionEncoder().wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(corr);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int gateOpen(final long corr) {
        final OrderEntryGateEncoder e = new OrderEntryGateEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).marketId(0).state(GateState.OPEN).reason(GateReason.ADMIN);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int deposit(final long userId) {
        final DepositEncoder e = new DepositEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0).userId(userId).assetId(Asset.USD.id()).amount(ONE).requestIdHigh(0).requestIdLow(0);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private void offer(final AeronCluster client, final int length) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            if (client.offer(ingress, 0, length) > 0) {
                return;
            }
            client.pollEgress();
            idle.idle();
        }
        throw new IllegalStateException("offer not accepted within timeout");
    }

    private static AeronCluster connect(final String aeronDir, final List<String> hosts, final CredentialsSupplier credentials,
                                        final Capture capture) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .credentialsSupplier(credentials)
                        .egressListener(capture)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                        .aeronDirectoryName(aeronDir)
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
            } catch (final RuntimeException e) {
                last = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect to the node", last);
    }

    private static Node boot(final List<String> hosts, final File baseDir, final String aeronDir) {
        TestAuth.install();
        final AssetsClusteredService service = new AssetsClusteredService();
        // the S7 jar ships {1, 2}: every instance (the cold-restarted one included) is widened through the seam
        service.engine().coreConfig().setSupportedBehaviorVersions(1, 2, 3);
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        cfg.baseDir(baseDir);
        cfg.aeronDirectoryName(aeronDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        final ClusteredMediaDriver mediaDriver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true).mtuLength(8192),
                cfg.archiveContext(), cfg.consensusModuleContext());
        final ClusteredServiceContainer container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());
        return new Node(cfg, service, mediaDriver, container);
    }

    private static boolean hasValidServiceSnapshot(final File clusterDir) {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid && entry.serviceId == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void register(final AeronCluster client, final SessionClass cls) {
        TestAuth.register(client, cls);
    }

    @Test
    public void bindGapSnapshotColdRestartReportEqualThenHealAndOpen() throws Exception {
        System.setProperty("snapshot.log.bytes", Integer.toString(SNAPSHOT_LOG_BYTES));
        System.setProperty("snapshot.interval.minutes", "0");
        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-feedstate-restore-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");
        final List<String> hosts = List.of("localhost");
        Node node = null;
        AeronCluster admin = null, bridge = null, tool = null, admin2 = null, bridge2 = null;
        try {
            node = boot(hosts, baseDir, aeronDir);
            final Capture adminCap = new Capture();
            admin = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.ADMIN), adminCap);
            for (long v = 1; v <= 3; v++) {
                offer(admin, coreConfig(0x70L + v, v));
                final long corr = 0x70L + v;
                final String ack = adminCap.await(admin, f -> f.startsWith("ADMINACK ref=70 corr=" + corr),
                        "CoreConfig " + v + " must be acked");
                assertTrue(ack, ack.contains(v == 1 ? "result=ADOPTED" : "result=ACCEPTED"));
            }
            final Capture bridgeCap = new Capture();
            bridge = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.BRIDGE), bridgeCap);
            register(bridge, SessionClass.BRIDGE);
            bridgeCap.await(bridge, f -> f.startsWith("ADMINACK ref=80"), "the BRIDGE RegisterSession must be acked");
            // bind on the FRESH record, then a gap: journalSeq 5 while the cursor is 1
            offer(bridge, engineConfig(1, 1, ConfigDecision.FRESH));
            offer(bridge, engineConfig(5, 5, ConfigDecision.BUMP));
            offer(bridge, query(0xFE1L));
            final String before = bridgeCap.await(bridge, f -> f.startsWith("FEEDPOS corr=" + 0xFE1L), "the feed report");
            assertTrue(before, before.contains("epoch=" + GENESIS) && before.contains("jseq=1") && before.contains("halted=TRUE")
                    && before.contains("reason=GAP"));

            // a service snapshot that holds the latch, then a cold restart on the same dirs
            final Capture toolCap = new Capture();
            tool = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.TOOL), toolCap);
            final File clusterDir = node.cfg.consensusModuleContext().clusterDir();
            final long snapshotDeadline = System.currentTimeMillis() + 30_000;
            while (!hasValidServiceSnapshot(clusterDir)) {
                if (System.currentTimeMillis() > snapshotDeadline) {
                    fail("no valid service snapshot within 30 s");
                }
                for (int i = 0; i < 128; i++) {
                    offer(tool, deposit(9));
                }
                Thread.sleep(200);
            }
            CloseHelper.quietCloseAll(tool, bridge, admin);
            tool = null; bridge = null; admin = null;
            node.close();
            node = boot(hosts, baseDir, aeronDir);

            final Capture bridgeCap2 = new Capture();
            bridge2 = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.BRIDGE), bridgeCap2);
            register(bridge2, SessionClass.BRIDGE);
            bridgeCap2.await(bridge2, f -> f.startsWith("ADMINACK ref=80"), "the restarted node acks the bridge");
            offer(bridge2, query(0xFE2L));
            final String after = bridgeCap2.await(bridge2, f -> f.startsWith("FEEDPOS corr=" + 0xFE2L), "the restored report");
            assertEquals("the restored node reports the same FeedPositionReport", before.replace("corr=" + 0xFE1L, ""),
                    after.replace("corr=" + 0xFE2L, ""));
            final FeedState restored = node.service.engine().feed();
            assertEquals(GENESIS, restored.boundMeEpoch());
            assertEquals(1L, restored.lastAppliedJournalSeq());
            assertEquals(com.openexchange.assets.domain.FeedHaltReason.GAP, restored.latchReason());

            // the missing entries are delivered: the GAP reach-clears, the held entry then applies
            offer(bridge2, engineConfig(2, 2, ConfigDecision.BUMP));
            offer(bridge2, engineConfig(3, 3, ConfigDecision.BUMP));
            offer(bridge2, engineConfig(4, 4, ConfigDecision.BUMP));
            offer(bridge2, engineConfig(5, 5, ConfigDecision.BUMP));
            offer(bridge2, query(0xFE3L));
            final String healed = bridgeCap2.await(bridge2, f -> f.startsWith("FEEDPOS corr=" + 0xFE3L), "the healed report");
            assertTrue(healed, healed.contains("jseq=5") && healed.contains("halted=FALSE") && healed.contains("reason=NONE"));

            final Capture adminCap2 = new Capture();
            admin2 = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.ADMIN), adminCap2);
            offer(admin2, gateOpen(0xAD9L));
            final String open = adminCap2.await(admin2, f -> f.startsWith("ADMINACK ref=73"), "ADMIN OPEN must be acked");
            assertTrue("ADMIN OPEN passes once the epoch is bound and the latch is gone: " + open, open.contains("result=ACCEPTED"));
        } finally {
            CloseHelper.quietCloseAll(admin2, bridge2, tool, bridge, admin);
            if (node != null) {
                node.close();
            }
            IoUtil.delete(tmp, true);
            System.clearProperty("snapshot.log.bytes");
            System.clearProperty("snapshot.interval.minutes");
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.application.engine.feed.FeedState;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchDecoder;
import com.openexchange.assets.infrastructure.generated.OrderType;
import com.openexchange.assets.infrastructure.generated.PendingDispatchEndDecoder;
import com.openexchange.assets.infrastructure.generated.ReleaseResult;
import com.openexchange.assets.infrastructure.generated.ReleaseScope;
import com.openexchange.assets.infrastructure.generated.RequestPendingDispatchEncoder;
import com.openexchange.assets.infrastructure.generated.Side;
import com.openexchange.assets.infrastructure.generated.TimeInForce;
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
 * RC2 S8 7 G9 on a real single-member cluster: behaviour 3 bound, a CREATE acked by the ME (its M6 UP_TO release enters the
 * backlog), a service snapshot, a cold restart on the same dirs: the restored node's RequestPendingDispatch returns the
 * backlog entry (section 11 restored, the index rebuilt), and the 67 that confirms it drops it.
 */
public class ReleaseBacklogRestoreClusterTest {

    private static final int PORT_BASE = 21100;
    private static final long TIMEOUT_MS = 20_000;
    private static final int SNAPSHOT_LOG_BYTES = 32 * 1024;
    private static final long GENESIS = 0x5EEDL;
    private static final long ONE = FixedPoint.fromDouble(1.0);

    private static final class Capture implements EgressListener {
        final List<String> frames = new CopyOnWriteArrayList<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final AdminAckDecoder ack = new AdminAckDecoder();

        public void onMessage(final long sessionId, final long timestamp, final DirectBuffer buffer, final int offset,
                              final int length, final io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case AdminAckDecoder.TEMPLATE_ID:
                    ack.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("ADMINACK ref=" + ack.refTemplateId() + " corr=" + ack.correlationId() + " result=" + ack.result());
                    break;
                case MeDispatchDecoder.TEMPLATE_ID: {
                    final MeDispatchDecoder d = new MeDispatchDecoder().wrapAndApplyHeader(buffer, offset, header);
                    frames.add("DISPATCH seq=" + d.dispatchSeq() + " id=" + d.commandIdHigh() + "/" + d.commandIdLow() + " kind="
                            + d.kind() + " scope=" + d.releaseScope());
                    break;
                }
                case MeDispatchBatchDecoder.TEMPLATE_ID:
                    for (final MeDispatchBatchDecoder.DispatchesDecoder d : new MeDispatchBatchDecoder()
                            .wrapAndApplyHeader(buffer, offset, header).dispatches()) {
                        frames.add("DISPATCH seq=" + d.dispatchSeq() + " id=" + d.commandIdHigh() + "/" + d.commandIdLow() + " kind="
                                + d.kind() + " scope=" + d.releaseScope());
                    }
                    break;
                case PendingDispatchEndDecoder.TEMPLATE_ID: {
                    final PendingDispatchEndDecoder e = new PendingDispatchEndDecoder().wrapAndApplyHeader(buffer, offset, header);
                    frames.add("END corr=" + e.correlationId() + " count=" + e.count() + " head=" + e.headDispatchSeq());
                    break;
                }
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
                .correlationId(0).userId(userId).assetId(Asset.USD.id()).amount(1_000 * ONE).requestIdHigh(0).requestIdLow(0);
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
        // the S8 jar ships {1, 2, 3}: no seam needed (RC2 S8 3.10 item 5)
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

    private int create(final long user) {
        final CreateOrderEncoder e = new CreateOrderEncoder().wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(0x5001).userId(user)
                .clientOrderIdHigh(0).clientOrderIdLow(0).requestIdHigh(0xAB).requestIdLow(1).marketId(1).side(Side.BUY)
                .orderType(OrderType.LIMIT).timeInForce(TimeInForce.GTC).price(100 * ONE).quantity(ONE).budget(0).stopPrice(0)
                .trailingDelta(0).displayQty(0).expiresAtMs(0);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int ack(final long js, final long h, final long user, final long leg) {
        final MeCommandOutcomeEncoder e = new MeCommandOutcomeEncoder().wrapAndApplyHeader(ingress, 0, headerEnc).egressSeq(100 + js)
                .commandIdHigh(h).commandIdLow(0).userId(user).omsOrderId(h).oldOrderId(0).price(100 * ONE).quantity(ONE).budget(0)
                .marketId(1).commandKind((short) 0).orderType((short) 0).orderSide((short) 0).appliedPosition(100 + js).meTimestamp(1)
                .orderId(leg).status(0).reason(0).oldCancelled((short) 0).result(0).meEpoch(GENESIS).journalSeq(js)
                .timeInForce((short) 0).expectedOldRemaining(0).ledgerSize(1).ledgerCapacity(4096).behaviorVersion(2);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int confirm(final long js, final long h, final long floor) {
        final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(ingress, 0, headerEnc).egressSeq(100 + js)
                .journalSeq(js).meEpoch(GENESIS).releaseSeq(js).ledgerSize(0).ledgerCapacity(4096).tombstoneRanges(0);
        e.releasesCount(1).next().commandIdHigh(h).idLowFloor(floor).scope(ReleaseScope.UP_TO).result(ReleaseResult.APPLIED).removed(1);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int pending(final long corr) {
        final RequestPendingDispatchEncoder e = new RequestPendingDispatchEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).fromDispatchSeq(0).maxEntries(1024);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void backlogSurvivesAColdRestartAndThe67DropsIt() throws Exception {
        System.setProperty("snapshot.log.bytes", Integer.toString(SNAPSHOT_LOG_BYTES));
        System.setProperty("snapshot.interval.minutes", "0");
        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-release-backlog-restore-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");
        final List<String> hosts = List.of("localhost");
        Node node = null;
        AeronCluster admin = null, bridge = null, tool = null, gateway = null, fwd = null, bridge2 = null;
        try {
            node = boot(hosts, baseDir, aeronDir);
            final Capture adminCap = new Capture();
            admin = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.ADMIN), adminCap);
            for (long v = 1; v <= 3; v++) {
                offer(admin, coreConfig(0x70L + v, v));
                final long corr = 0x70L + v;
                adminCap.await(admin, f -> f.startsWith("ADMINACK ref=70 corr=" + corr), "CoreConfig " + v + " must be acked");
            }
            final Capture bridgeCap = new Capture();
            bridge = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.BRIDGE), bridgeCap);
            register(bridge, SessionClass.BRIDGE);
            bridgeCap.await(bridge, f -> f.startsWith("ADMINACK ref=80"), "the BRIDGE RegisterSession must be acked");
            offer(bridge, engineConfig(1, 1, ConfigDecision.FRESH));
            // the FRESH bind and the ADMIN OPEN come from different sessions: retry the OPEN until the bind is in the log
            boolean opened = false;
            for (long corr = 0xAD1L; !opened && corr < 0xAD1L + 50; corr++) {
                offer(admin, gateOpen(corr));
                final long c = corr;
                opened = adminCap.await(admin, f -> f.startsWith("ADMINACK ref=73 corr=" + c), "ADMIN OPEN").contains("ACCEPTED");
                if (!opened) {
                    Thread.sleep(50);
                }
            }
            assertTrue("ADMIN OPEN accepted once the epoch is bound", opened);
            final Capture toolCap = new Capture();
            tool = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.TOOL), toolCap);
            offer(tool, deposit(9));
            final Capture gwCap = new Capture();
            gateway = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.GATEWAY), gwCap);
            register(gateway, SessionClass.GATEWAY);
            gwCap.await(gateway, f -> f.startsWith("ADMINACK ref=80"), "the GATEWAY RegisterSession must be acked");
            offer(gateway, create(9));
            gwCap.await(gateway, f -> f.startsWith("DISPATCH seq=1 id=1/0 kind=CREATE"), "the CREATE dispatch");
            offer(bridge, ack(2, 1, 9, 101));
            gwCap.await(gateway, f -> f.startsWith("DISPATCH seq=2 id=1/0 kind=RELEASE_ID scope=UP_TO"), "the M6 UP_TO release");

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
            CloseHelper.quietCloseAll(gateway, tool, bridge, admin);
            gateway = null; tool = null; bridge = null; admin = null;
            node.close();
            node = boot(hosts, baseDir, aeronDir);

            final Capture fwdCap = new Capture();
            fwd = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.FORWARDER), fwdCap);
            register(fwd, SessionClass.FORWARDER);
            fwdCap.await(fwd, f -> f.startsWith("ADMINACK ref=80"), "the FORWARDER RegisterSession must be acked");
            offer(fwd, pending(0x65A));
            fwdCap.await(fwd, f -> f.startsWith("END corr=" + 0x65A), "the reply end");
            assertTrue("the backlog release is replayed: " + fwdCap.frames,
                    fwdCap.frames.contains("DISPATCH seq=2 id=1/0 kind=RELEASE_ID scope=UP_TO"));
            assertTrue(fwdCap.frames.contains("END corr=" + 0x65A + " count=1 head=2"));
            assertEquals("the restored node holds the backlog entry", 1, node.service.engine().orders().backlog().size());

            final Capture bridgeCap2 = new Capture();
            bridge2 = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.BRIDGE), bridgeCap2);
            register(bridge2, SessionClass.BRIDGE);
            bridgeCap2.await(bridge2, f -> f.startsWith("ADMINACK ref=80"), "the restarted node acks the bridge");
            offer(bridge2, confirm(3, 1, 0));
            // the 67 and the next query come from different sessions: wait until the 67 is applied (a read of the node)
            final long applyDeadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (node.service.engine().feed().lastAppliedJournalSeq() < 3) {
                if (System.currentTimeMillis() > applyDeadline) {
                    fail("the 67 was not applied");
                }
                bridge2.pollEgress();
                Thread.sleep(20);
            }
            fwdCap.frames.clear();
            offer(fwd, pending(0x65B));
            fwdCap.await(fwd, f -> f.startsWith("END corr=" + 0x65B), "the second reply end");
            assertTrue("the 67 dropped the entry: " + fwdCap.frames, fwdCap.frames.contains("END corr=" + 0x65B + " count=0 head=2"));
            assertEquals(0, node.service.engine().orders().backlog().size());
        } finally {
            CloseHelper.quietCloseAll(bridge2, fwd, gateway, tool, bridge, admin);
            if (node != null) {
                node.close();
            }
            IoUtil.delete(tmp, true);
            System.clearProperty("snapshot.log.bytes");
            System.clearProperty("snapshot.interval.minutes");
        }
    }
}

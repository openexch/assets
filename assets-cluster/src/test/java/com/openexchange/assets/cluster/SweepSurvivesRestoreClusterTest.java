// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.GateReason;
import com.openexchange.assets.infrastructure.generated.GateState;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderAckDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEventBatchDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEventDecoder;
import com.openexchange.assets.infrastructure.generated.OrderRejectDecoder;
import com.openexchange.assets.infrastructure.generated.OrderType;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.Side;
import com.openexchange.assets.infrastructure.generated.TimeInForce;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC1 S5 G4 on a real single-member cluster (contract 3.3 CREATE APPLIED, 3.7 GTD, lifecycle C9 + C16): a GTD
 * order whose leg was learned from a BRIDGE {@code MeCommandOutcome} is snapshotted; the node is cold-restarted
 * on the same dirs; at the deadline the restored deadline index and the re-armed sweep mint exactly ONE
 * {@code MeDispatch{CANCEL}} to a forwarder that connected after the restart, none before the deadline and no
 * second one afterwards (R-C9a "GTD tek cancel", across a restore).
 */
public class SweepSurvivesRestoreClusterTest {

    private static final int PORT_BASE = 20300;
    private static final long TIMEOUT_MS = 20_000;
    private static final long GTD_WINDOW_MS = 20_000;
    private static final long QUIET_AFTER_MS = 3_000;
    private static final int SNAPSHOT_LOG_BYTES = 32 * 1024;
    private static final long ONE = FixedPoint.fromDouble(1.0);
    private static final long USER = 7;
    private static final long FILLER_USER = 8;
    private static final long LEG = 77;
    private static final long ORDER_ID = 1;

    /** Egress frames as text, with their arrival time, decoded on the polling thread. */
    private static final class Capture implements EgressListener {
        final List<String> frames = new CopyOnWriteArrayList<>();
        final List<Long> arrivedAt = new CopyOnWriteArrayList<>();
        volatile long lastRejectedAtMs = -1;
        volatile long lastAcceptedAtMs = -1;
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final OrderAckDecoder ack = new OrderAckDecoder();
        private final OrderRejectDecoder reject = new OrderRejectDecoder();
        private final OrderEventDecoder event = new OrderEventDecoder();
        private final OrderEventBatchDecoder eventBatch = new OrderEventBatchDecoder();
        private final MeDispatchDecoder dispatch = new MeDispatchDecoder();
        private final MeDispatchBatchDecoder dispatchBatch = new MeDispatchBatchDecoder();
        private final AdminAckDecoder adminAck = new AdminAckDecoder();

        private void add(final String frame) {
            frames.add(frame);
            arrivedAt.add(System.currentTimeMillis());
        }

        public void onMessage(final long sessionId, final long timestamp, final DirectBuffer buffer, final int offset, final int length,
                              final io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case OrderAckDecoder.TEMPLATE_ID:
                    ack.wrapAndApplyHeader(buffer, offset, header);
                    lastAcceptedAtMs = ack.acceptedAtMs();
                    add("ORDERACK corr=" + ack.correlationId() + " order=" + ack.orderId() + " status=" + ack.status()
                            + " hold=" + ack.holdAmount() + " at=" + ack.acceptedAtMs());
                    break;
                case OrderRejectDecoder.TEMPLATE_ID:
                    reject.wrapAndApplyHeader(buffer, offset, header);
                    lastRejectedAtMs = reject.rejectedAtMs();
                    add("REJECT ref=" + reject.refTemplateId() + " reason=" + reject.reason() + " corr=" + reject.correlationId()
                            + " at=" + reject.rejectedAtMs());
                    break;
                case OrderEventDecoder.TEMPLATE_ID:
                    event.wrapAndApplyHeader(buffer, offset, header);
                    add("EVENT seq=" + event.eventSeq() + " order=" + event.orderId() + " kind=" + event.eventKind() + " user=" + event.userId());
                    break;
                case OrderEventBatchDecoder.TEMPLATE_ID:
                    eventBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final OrderEventBatchDecoder.EventsDecoder e : eventBatch.events()) {
                        add("EVENT seq=" + e.eventSeq() + " order=" + e.orderId() + " kind=" + e.eventKind() + " user=" + e.userId());
                    }
                    break;
                case MeDispatchDecoder.TEMPLATE_ID:
                    dispatch.wrapAndApplyHeader(buffer, offset, header);
                    add("DISPATCH seq=" + dispatch.dispatchSeq() + " id=" + dispatch.commandIdHigh() + "/" + dispatch.commandIdLow()
                            + " kind=" + dispatch.kind() + " order=" + dispatch.omsOrderId() + " old=" + dispatch.oldOrderId());
                    break;
                case MeDispatchBatchDecoder.TEMPLATE_ID:
                    dispatchBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final MeDispatchBatchDecoder.DispatchesDecoder d : dispatchBatch.dispatches()) {
                        add("DISPATCH seq=" + d.dispatchSeq() + " id=" + d.commandIdHigh() + "/" + d.commandIdLow()
                                + " kind=" + d.kind() + " order=" + d.omsOrderId() + " old=" + d.oldOrderId());
                    }
                    break;
                case AdminAckDecoder.TEMPLATE_ID:
                    adminAck.wrapAndApplyHeader(buffer, offset, header);
                    add("ADMINACK ref=" + adminAck.refTemplateId() + " result=" + adminAck.result());
                    break;
                default:
                    break;
            }
        }

        int indexOf(final Predicate<String> match) {
            for (int i = 0; i < frames.size(); i++) {
                if (match.test(frames.get(i))) {
                    return i;
                }
            }
            return -1;
        }

        long count(final Predicate<String> match) {
            return frames.stream().filter(match).count();
        }

        /** Poll egress until a matching frame exists or the deadline passes; fail with {@code why}. */
        String await(final AeronCluster client, final Predicate<String> match, final long deadlineMs, final String why) {
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            long lastKeepAlive = System.currentTimeMillis();
            while (System.currentTimeMillis() < deadlineMs) {
                final int i = indexOf(match);
                if (i >= 0) {
                    return frames.get(i);
                }
                client.pollEgress();
                lastKeepAlive = keepAlive(client, lastKeepAlive);
                idle.idle();
            }
            fail(why + "; frames seen: " + frames);
            return null;
        }

        /** Keep polling until {@code untilMs}. */
        void pollUntil(final AeronCluster client, final long untilMs) {
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            long lastKeepAlive = System.currentTimeMillis();
            while (System.currentTimeMillis() < untilMs) {
                if (client.pollEgress() == 0) {
                    idle.idle();
                }
                lastKeepAlive = keepAlive(client, lastKeepAlive);
            }
        }

        /**
         * {@code AeronCluster.pollEgress} only polls the subscription; a client that stays silent for the consensus
         * module's session timeout (10 s default) is closed with TIMEOUT. A real forwarder keeps its session alive the
         * same way: one keep-alive per second while idle.
         */
        private static long keepAlive(final AeronCluster client, final long lastKeepAliveMs) {
            final long now = System.currentTimeMillis();
            if (now - lastKeepAliveMs >= 1_000) {
                client.sendKeepAlive();
                return now;
            }
            return lastKeepAliveMs;
        }
    }

    private static Predicate<String> cancelDispatchFor(final long orderId) {
        return f -> f.startsWith("DISPATCH") && f.contains("kind=CANCEL") && f.contains(" order=" + orderId + " ");
    }

    /** One booted single-node AE: enough to stop it and to boot it again on the same dirs. */
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

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();

    private int encodeDeposit(final long userId, final long amount) {
        final DepositEncoder e = new DepositEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(0).userId(userId).assetId(Asset.USD.id()).amount(amount).requestIdHigh(0).requestIdLow(0);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int encodeGateOpen(final long corr) {
        final OrderEntryGateEncoder e = new OrderEntryGateEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).marketId(0).state(GateState.OPEN).reason(GateReason.ADMIN);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int encodeCreate(final long corr, final long userId, final long requestLow, final TimeInForce tif, final long expiresAtMs) {
        final CreateOrderEncoder e = new CreateOrderEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).userId(userId).clientOrderIdHigh(0).clientOrderIdLow(0)
                .requestIdHigh(1).requestIdLow(requestLow).marketId(1).side(Side.BUY).orderType(OrderType.LIMIT)
                .timeInForce(tif).price(100 * ONE).quantity(ONE).budget(0).stopPrice(0).trailingDelta(0)
                .displayQty(0).expiresAtMs(expiresAtMs);
        e.flags().clear();
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    /** MeCommandOutcome APPLIED (result 0) for commandId (orderId, attempt 0 << 2 | CREATE) with the ME leg id in {@code orderId}. */
    private int encodeCreateApplied(final long omsOrderId, final long userId, final long legId) {
        final MeCommandOutcomeEncoder e = new MeCommandOutcomeEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .egressSeq(1).commandIdHigh(omsOrderId).commandIdLow(0L).userId(userId).omsOrderId(omsOrderId).oldOrderId(0)
                .price(100 * ONE).quantity(ONE).budget(0).marketId(1).commandKind((short) 0).orderType((short) 0).orderSide((short) 0)
                .appliedPosition(0).meTimestamp(0L).orderId(legId).status(0).reason(0).oldCancelled((short) 0).result(0).meEpoch(0L);
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
                last = e; // the node is still electing / replaying; try again
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect to the node", last);
    }

    private static void register(final AeronCluster client, final SessionClass cls, final boolean settlements, final boolean orders,
                                 final boolean dispatch) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);
        final int len = TestAuth.encodeRegisterSession(buf, 0x8E6L, cls, true, true, false, settlements, false, orders, dispatch, false);
        while (client.offer(buf, 0, len) <= 0) {
            client.pollEgress();
            Thread.onSpinWait();
        }
    }

    private static Node boot(final List<String> hosts, final File baseDir, final String aeronDir) {
        TestAuth.install();
        final AssetsClusteredService service = new AssetsClusteredService();
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

    @Test
    public void theGtdCancelIsMintedExactlyOnceAtTheDeadlineAfterASnapshotAndAColdRestart() throws Exception {
        // Byte-triggered snapshots only (the cadence reads both properties when the service is constructed).
        System.setProperty("snapshot.log.bytes", Integer.toString(SNAPSHOT_LOG_BYTES));
        System.setProperty("snapshot.interval.minutes", "0");

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-sweep-restore-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");
        final List<String> hosts = List.of("localhost");

        Node node = null;
        AeronCluster admin = null, tool = null, gateway = null, forwarder = null, bridge = null, forwarder2 = null;
        try {
            node = boot(hosts, baseDir, aeronDir);
            final Capture gw = new Capture();
            gateway = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.GATEWAY), gw);
            register(gateway, SessionClass.GATEWAY, false, true, false);
            final Capture fwd = new Capture();
            forwarder = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.FORWARDER), fwd);
            register(forwarder, SessionClass.FORWARDER, false, false, true);
            final Capture toolCap = new Capture();
            tool = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.TOOL), toolCap);
            offer(tool, encodeDeposit(USER, 1_000 * ONE));

            // The cluster time, read from the GATE_CLOSED reject of a probe create (no record, no dispatch).
            offer(gateway, encodeCreate(0xA1L, USER, 1, TimeInForce.GTC, 0L));
            gw.await(gateway, f -> f.startsWith("REJECT ref=50 reason=GATE_CLOSED corr=" + 0xA1L), System.currentTimeMillis() + TIMEOUT_MS,
                    "the probe create before the gate opens must be rejected GATE_CLOSED");
            final long clusterNow = gw.lastRejectedAtMs;
            assertTrue("cluster time is epoch milliseconds (K3), got " + clusterNow, Math.abs(clusterNow - System.currentTimeMillis()) < 60_000);
            final long expiresAtMs = clusterNow + GTD_WINDOW_MS;

            final Capture adminCap = new Capture();
            admin = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.ADMIN), adminCap);
            offer(admin, encodeGateOpen(0xAD1L));
            assertTrue(adminCap.await(admin, f -> f.startsWith("ADMINACK ref=73"), System.currentTimeMillis() + TIMEOUT_MS,
                    "the ADMIN gate open must be acked").contains("result=ACCEPTED"));

            // GTD BUY LIMIT expiring 20 s from the cluster time; the forwarder sees its create.
            offer(gateway, encodeCreate(0xA2L, USER, 2, TimeInForce.GTD, expiresAtMs));
            final String ack = gw.await(gateway, f -> f.startsWith("ORDERACK corr=" + 0xA2L), System.currentTimeMillis() + TIMEOUT_MS,
                    "the GTD create must be acked");
            assertTrue(ack, ack.contains(" order=" + ORDER_ID + " ") && ack.contains("status=PENDING_NEW"));
            final String create = fwd.await(forwarder, f -> f.startsWith("DISPATCH seq=1 "), System.currentTimeMillis() + TIMEOUT_MS,
                    "the forwarder must receive DISPATCH seq=1 for the create");
            assertTrue(create, create.contains("kind=CREATE") && create.contains(" order=" + ORDER_ID + " "));

            // BRIDGE: the ME applied the create as leg 77. First observable step of contract 3.3: ACK_NEW on the orders channel.
            final Capture bridgeCap = new Capture();
            bridge = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.BRIDGE), bridgeCap);
            register(bridge, SessionClass.BRIDGE, true, false, false);
            bridgeCap.await(bridge, f -> f.startsWith("ADMINACK ref=80"), System.currentTimeMillis() + TIMEOUT_MS,
                    "the BRIDGE RegisterSession must be acked");
            offer(bridge, encodeCreateApplied(ORDER_ID, USER, LEG));
            gw.await(gateway, f -> f.startsWith("EVENT") && f.contains(" order=" + ORDER_ID + " ") && f.contains("kind=ACK_NEW"),
                    System.currentTimeMillis() + TIMEOUT_MS,
                    "the BRIDGE MeCommandOutcome APPLIED must give order " + ORDER_ID + " its leg (contract 3.3): an ACK_NEW OrderEvent "
                            + "was expected on the orders channel");
            assertEquals("the record carries the learned leg", LEG, node.service.engine().orders().orders().get(ORDER_ID).currentLegId);

            // Force a service snapshot that holds the order with its leg: grow the log past the byte trigger.
            final File clusterDir = node.cfg.consensusModuleContext().clusterDir();
            final long snapshotDeadline = System.currentTimeMillis() + 30_000;
            while (!hasValidServiceSnapshot(clusterDir)) {
                if (System.currentTimeMillis() > snapshotDeadline) {
                    fail("no valid service snapshot within 30 s (snapshot.log.bytes=" + SNAPSHOT_LOG_BYTES + ")");
                }
                for (int i = 0; i < 128; i++) {
                    offer(tool, encodeDeposit(FILLER_USER, ONE));
                }
                Thread.sleep(200);
            }

            // Cold restart on the same cluster + archive dirs, then a FRESH forwarder session.
            CloseHelper.quietCloseAll(bridge, forwarder, gateway, tool, admin);
            bridge = null; forwarder = null; gateway = null; tool = null; admin = null;
            node.close();
            node = boot(hosts, baseDir, aeronDir);
            final Capture fwd2 = new Capture();
            forwarder2 = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.FORWARDER), fwd2);
            register(forwarder2, SessionClass.FORWARDER, false, false, true);
            fwd2.await(forwarder2, f -> f.startsWith("ADMINACK ref=80"), System.currentTimeMillis() + TIMEOUT_MS,
                    "the restarted node must ack the forwarder's RegisterSession");
            final long reconnectedAt = System.currentTimeMillis();
            assertTrue("precondition: the node must be back before the GTD deadline so the before-deadline check means something "
                    + "(deadline was " + (reconnectedAt - expiresAtMs) + " ms ago)", reconnectedAt < expiresAtMs - 500);
            final CoreOrder restored = node.service.engine().orders().orders().get(ORDER_ID);
            assertNotNull("the GTD order survives the restore", restored);
            assertEquals("the leg learned from the BRIDGE outcome survives the restore", LEG, restored.currentLegId);
            final boolean[] gtdRestored = {false};
            node.service.engine().deadlines().forEach((d, id, k) -> {
                if (k == DeadlineIndex.KIND_GTD && id == ORDER_ID && d == expiresAtMs) {
                    gtdRestored[0] = true;
                }
            });
            assertTrue("the GTD deadline (" + expiresAtMs + ", order " + ORDER_ID + ") is rebuilt from the record after the restore", gtdRestored[0]);

            // Nothing before the deadline.
            fwd2.pollUntil(forwarder2, expiresAtMs - 250);
            assertEquals("no CANCEL may be minted for order " + ORDER_ID + " before its GTD deadline; frames: " + fwd2.frames,
                    0, fwd2.count(cancelDispatchFor(ORDER_ID)));

            // Exactly one at the deadline.
            final String cancel = fwd2.await(forwarder2, cancelDispatchFor(ORDER_ID), expiresAtMs + 10_000,
                    "exactly one DISPATCH kind=CANCEL for order " + ORDER_ID + " is expected at the GTD deadline after the cold restart "
                            + "(contract 3.7 + C16: the restored deadline index and the re-armed sweep mint it once)");
            assertTrue("the cancel is attempt 1 of order 1 on leg " + LEG + ": " + cancel,
                    cancel.contains("id=" + ORDER_ID + "/" + ((1L << 2) | 1) + " ") && cancel.contains("old=" + LEG));
            final long arrived = fwd2.arrivedAt.get(fwd2.indexOf(cancelDispatchFor(ORDER_ID)));
            assertTrue("the cancel arrives at or after the deadline (arrived " + (arrived - expiresAtMs) + " ms relative)",
                    arrived >= expiresAtMs - 200);

            // And no second one.
            fwd2.pollUntil(forwarder2, arrived + QUIET_AFTER_MS);
            assertEquals("the GTD cancel is minted exactly once (R-C9a): no second DISPATCH kind=CANCEL within " + QUIET_AFTER_MS
                    + " ms; frames: " + fwd2.frames, 1, fwd2.count(cancelDispatchFor(ORDER_ID)));
            final CoreOrder after = node.service.engine().orders().orders().get(ORDER_ID);
            assertNotNull("the order stays live until the ME terminal", after);
            assertTrue("EXPIRY_REQUESTED + CANCEL_PENDING are set", after.has(CoreOrder.F_EXPIRY_REQUESTED) && after.has(CoreOrder.F_CANCEL_PENDING));
            assertEquals("one cancel attempt", 1, after.attempt);
        } finally {
            CloseHelper.quietCloseAll(forwarder2, bridge, forwarder, gateway, tool, admin);
            if (node != null) {
                node.close();
            }
            IoUtil.delete(tmp, true);
            System.clearProperty("snapshot.log.bytes");
            System.clearProperty("snapshot.interval.minutes");
        }
    }
}

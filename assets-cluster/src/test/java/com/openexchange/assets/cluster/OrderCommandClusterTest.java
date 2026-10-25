// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.GateReason;
import com.openexchange.assets.infrastructure.generated.GateState;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC1 S4 on a real single-member cluster: the gate opens by ADMIN, a TOOL funds the user, a GATEWAY session
 * creates an order and gets its OrderAck; a FORWARDER session registered on the dispatch channel receives
 * the MeDispatch; a second GATEWAY session receives the OrderEvent broadcast on the orders channel.
 */
public class OrderCommandClusterTest {

    private static final int PORT_BASE = 20200;
    private static final long TIMEOUT_MS = 20_000;
    private static final long ONE = FixedPoint.fromDouble(1.0);

    private static final class Capture implements EgressListener {
        final List<String> frames = new CopyOnWriteArrayList<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final OrderAckDecoder ack = new OrderAckDecoder();
        private final OrderRejectDecoder reject = new OrderRejectDecoder();
        private final OrderEventDecoder event = new OrderEventDecoder();
        private final OrderEventBatchDecoder eventBatch = new OrderEventBatchDecoder();
        private final MeDispatchDecoder dispatch = new MeDispatchDecoder();
        private final MeDispatchBatchDecoder dispatchBatch = new MeDispatchBatchDecoder();
        private final AdminAckDecoder adminAck = new AdminAckDecoder();

        public void onMessage(long sessionId, long timestamp, DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case OrderAckDecoder.TEMPLATE_ID:
                    ack.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("ORDERACK corr=" + ack.correlationId() + " order=" + ack.orderId() + " status=" + ack.status() + " hold=" + ack.holdAmount());
                    break;
                case OrderRejectDecoder.TEMPLATE_ID:
                    reject.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("REJECT ref=" + reject.refTemplateId() + " reason=" + reject.reason() + " corr=" + reject.correlationId());
                    break;
                case OrderEventDecoder.TEMPLATE_ID:
                    event.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("EVENT seq=" + event.eventSeq() + " order=" + event.orderId() + " kind=" + event.eventKind() + " user=" + event.userId());
                    break;
                case OrderEventBatchDecoder.TEMPLATE_ID:
                    eventBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final OrderEventBatchDecoder.EventsDecoder e : eventBatch.events()) {
                        frames.add("EVENT seq=" + e.eventSeq() + " order=" + e.orderId() + " kind=" + e.eventKind() + " user=" + e.userId());
                    }
                    break;
                case MeDispatchDecoder.TEMPLATE_ID:
                    dispatch.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("DISPATCH seq=" + dispatch.dispatchSeq() + " id=" + dispatch.commandIdHigh() + "/" + dispatch.commandIdLow()
                            + " kind=" + dispatch.kind() + " qty=" + dispatch.quantity() + " px=" + dispatch.price());
                    break;
                case MeDispatchBatchDecoder.TEMPLATE_ID:
                    dispatchBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final MeDispatchBatchDecoder.DispatchesDecoder d : dispatchBatch.dispatches()) {
                        frames.add("DISPATCH seq=" + d.dispatchSeq() + " id=" + d.commandIdHigh() + "/" + d.commandIdLow()
                                + " kind=" + d.kind() + " qty=" + d.quantity() + " px=" + d.price());
                    }
                    break;
                case AdminAckDecoder.TEMPLATE_ID:
                    adminAck.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("ADMINACK ref=" + adminAck.refTemplateId() + " result=" + adminAck.result());
                    break;
                default:
                    break;
            }
        }

        String await(final AeronCluster client, final String prefix) {
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (System.currentTimeMillis() < deadline) {
                for (final String f : frames) {
                    if (f.startsWith(prefix)) {
                        return f;
                    }
                }
                client.pollEgress();
                idle.idle();
            }
            fail("no frame starting with '" + prefix + "' within " + TIMEOUT_MS + " ms; saw " + frames);
            return null;
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

    private int encodeCreate(final long corr, final long userId, final long requestLow) {
        final CreateOrderEncoder e = new CreateOrderEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).userId(userId).clientOrderIdHigh(0).clientOrderIdLow(requestLow)
                .requestIdHigh(1).requestIdLow(requestLow).marketId(1).side(Side.BUY).orderType(OrderType.LIMIT)
                .timeInForce(TimeInForce.GTC).price(100 * ONE).quantity(ONE).budget(0).stopPrice(0).trailingDelta(0)
                .displayQty(0).expiresAtMs(0);
        e.flags().clear();
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
                                        final Capture capture) {
        return AeronCluster.connect(new AeronCluster.Context()
                .credentialsSupplier(credentials)
                .egressListener(capture)
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .aeronDirectoryName(aeronDir)
                .ingressEndpoints(ClusterConfig.ingressEndpoints(hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
    }

    private static void register(final AeronCluster client, final SessionClass cls, final boolean orders, final boolean dispatch) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);
        final int len = TestAuth.encodeRegisterSession(buf, 0x8E6L, cls, true, true, false, false, false, orders, dispatch, false);
        while (client.offer(buf, 0, len) <= 0) {
            client.pollEgress();
            Thread.onSpinWait();
        }
    }

    @Test
    public void aGatewayOrderIsAckedDispatchedToTheForwarderAndBroadcastToGateways() {
        TestAuth.install();
        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-orders-" + PORT_BASE);
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
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));

        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        AeronCluster admin = null, tool = null, gatewayA = null, gatewayB = null, forwarder = null;
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true).mtuLength(8192),
                    cfg.archiveContext(), cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

            final Capture gwA = new Capture();
            gatewayA = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.GATEWAY), gwA);
            register(gatewayA, SessionClass.GATEWAY, true, false);
            final Capture gwB = new Capture();
            gatewayB = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.GATEWAY), gwB);
            register(gatewayB, SessionClass.GATEWAY, true, false);
            final Capture fwd = new Capture();
            forwarder = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.FORWARDER), fwd);
            register(forwarder, SessionClass.FORWARDER, false, true);
            final Capture toolCap = new Capture();
            tool = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.TOOL), toolCap);
            offer(tool, encodeDeposit(7, 1_000 * ONE));

            // genesis gate: refused with GATE_CLOSED, nothing dispatched
            offer(gatewayA, encodeCreate(0xA1L, 7, 1));
            gwA.await(gatewayA, "REJECT ref=50 reason=GATE_CLOSED corr=" + 0xA1L);

            final Capture adminCap = new Capture();
            admin = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.ADMIN), adminCap);
            offer(admin, encodeGateOpen(0xAD1L));
            assertTrue(adminCap.await(admin, "ADMINACK ref=73").contains("result=ACCEPTED"));

            offer(gatewayA, encodeCreate(0xA2L, 7, 2));
            final String ack = gwA.await(gatewayA, "ORDERACK corr=" + 0xA2L);
            assertTrue(ack, ack.contains("status=PENDING_NEW"));
            assertTrue(ack, ack.contains("hold=" + (100 * ONE)));
            final String dispatch = fwd.await(forwarder, "DISPATCH seq=1");
            assertTrue(dispatch, dispatch.contains("kind=CREATE") && dispatch.contains("qty=" + ONE));
            final String eventOnB = gwB.await(gatewayB, "EVENT seq=1");
            assertTrue(eventOnB, eventOnB.contains("kind=ACCEPTED") && eventOnB.contains("user=7"));
            gwA.await(gatewayA, "EVENT seq=1");
            assertEquals("the forwarder is not on the orders channel", 0, fwd.frames.stream().filter(f -> f.startsWith("EVENT")).count());
            assertEquals("gateways are not on the dispatch channel", 0, gwA.frames.stream().filter(f -> f.startsWith("DISPATCH")).count());
            assertEquals(1, service.engine().orders().orders().size());
            assertEquals(100 * ONE, service.engine().account(7).locked(Asset.USD.id()));
        } finally {
            CloseHelper.quietCloseAll(forwarder, gatewayB, gatewayA, tool, admin, container, mediaDriver);
            IoUtil.delete(tmp, true);
        }
    }
}

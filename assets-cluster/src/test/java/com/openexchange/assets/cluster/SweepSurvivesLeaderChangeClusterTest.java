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
import com.openexchange.cluster.ClusterPorts;
import io.aeron.Counter;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.security.CredentialsSupplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC1 S5 G4, the K3 failover case of the periodic deadline sweep (contract 3.7 GTD, 3.8 #16 per-term timer ids,
 * 3.8 #17 keep-alives; lifecycle C9 + C16) on a real THREE-member AE cluster: a GTD order whose leg was learned from
 * a BRIDGE {@code MeCommandOutcome} rests on the leader; the leader is killed well before the deadline; a new leader is
 * elected among the survivors; a FRESH forwarder session registers with it. At the deadline the new leader's sweep
 * (armed under its own leadership term's timer id, 3.8 #16) must mint exactly ONE {@code MeDispatch{CANCEL}}: none
 * before the deadline, none afterwards, and both survivors' engines must carry the order as
 * EXPIRY_REQUESTED | CANCEL_PENDING | DISPATCH_PENDING with attempt 1.
 *
 * <p>Why this is the sharp case: the ex-follower consumed every tick of the old leader's sweep from the log while
 * holding no pending timer with that id, so Aeron 1.53's consensus module counted them and would swallow that many
 * {@code scheduleTimer} calls for a FIXED id on the new leader; the belt would never re-arm and the GTD would never
 * fire (3.8 #16). A per-term id starts at count 0.</p>
 *
 * <p>Scaffolding: member boot / leader detection / kill as {@code TimerSurvivalClusterTest}; client, session, encoder
 * and {@code Capture} as {@code SweepSurvivesRestoreClusterTest}. {@code AssetsClusteredService} exposes no role
 * accessor, so the leader is read from the consensus module's own role counter (never on a killed member: its
 * counters file is unmapped). After the kill the fresh forwarder connects with the survivors' ingress endpoints.</p>
 */
public class SweepSurvivesLeaderChangeClusterTest {

    private static final int PORT_BASE = 20400; // stride 100: members on 20400 / 20500 / 20600 (+offsets)
    private static final int MEMBERS = 3;
    private static final long TIMEOUT_MS = 20_000;
    private static final long GTD_WINDOW_MS = 25_000;
    private static final long MIN_KILL_LEAD_MS = 10_000;
    private static final long QUIET_AFTER_MS = 3_000;
    private static final long ONE = FixedPoint.fromDouble(1.0);
    private static final long USER = 7;
    private static final long LEG = 77;
    private static final long ORDER_ID = 1;
    /** The restore test's ingress sizing (MTU 8192 for the AE's batched egress), a smaller term for three co-located members. */
    private static final String INGRESS_CHANNEL = "aeron:udp?term-length=1m|mtu=8192";

    /** Egress frames as text, with their arrival time, decoded on the polling thread. */
    private static final class Capture implements EgressListener {
        final List<String> frames = new CopyOnWriteArrayList<>();
        final List<Long> arrivedAt = new CopyOnWriteArrayList<>();
        volatile long lastRejectedAtMs = -1;
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

        /** Contract 3.8 #17: {@code pollEgress} only polls; a silent client is closed TIMEOUT after 10 s. One keep-alive per second. */
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

    private static Predicate<String> registerAck() {
        return f -> f.startsWith("ADMINACK ref=80");
    }

    /** One booted AE member: a real {@link AssetsClusteredService} on its own media driver, archive and consensus module. */
    private static final class Member {
        final int id;
        ClusterConfig cfg;
        AssetsClusteredService service;
        String driverDir;
        ClusteredMediaDriver driver;
        ClusteredServiceContainer container;
        volatile boolean killed;

        Member(final int id) {
            this.id = id;
        }

        /** The consensus module's role counter; the service has no role accessor. Never read on a killed member. */
        Cluster.Role role() {
            if (killed) {
                throw new IllegalStateException("member " + id + " is killed: its counters file is unmapped");
            }
            final Counter counter = cfg.consensusModuleContext().clusterNodeRoleCounter();
            return counter == null ? Cluster.Role.FOLLOWER : Cluster.Role.get(counter);
        }

        CoreOrder order(final long orderId) {
            return service.engine().orders().orders().get(orderId);
        }

        void close() {
            killed = true;
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(driver);
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

    /** {@code memberId=host:port} for every live member (or all of them): the arithmetic ClusterConfig exposes. */
    private static String ingressEndpointsOf(final List<Member> members, final boolean liveOnly) {
        final StringBuilder sb = new StringBuilder();
        for (final Member m : members) {
            if (liveOnly && m.killed) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(m.id).append("=localhost:")
                    .append(ClusterConfig.calculatePort(m.id, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET));
        }
        return sb.toString();
    }

    private static AeronCluster connect(final String aeronDir, final String ingressEndpoints, final CredentialsSupplier credentials,
                                        final Capture capture) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .credentialsSupplier(credentials)
                        .egressListener(capture)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel(INGRESS_CHANNEL)
                        .aeronDirectoryName(aeronDir)
                        .ingressEndpoints(ingressEndpoints)
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
            } catch (final RuntimeException e) {
                last = e; // still electing / replaying; try again
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect to the cluster via " + ingressEndpoints, last);
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

    private static Member boot(final int id, final List<String> hosts, final File tmp) {
        final Member m = new Member(id);
        TestAuth.install();
        m.service = new AssetsClusteredService();
        m.cfg = ClusterConfig.create(id, hosts, hosts, PORT_BASE, m.service);
        m.cfg.baseDir(new File(tmp, "node" + id));
        m.driverDir = new File(tmp, "driver" + id).getAbsolutePath();
        m.cfg.aeronDirectoryName(m.driverDir);
        m.cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        m.cfg.errorHandler(Throwable::printStackTrace);
        m.cfg.consensusModuleContext()
                .ingressChannel(INGRESS_CHANNEL)
                .logChannel("aeron:udp?term-length=1m")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        m.driver = ClusteredMediaDriver.launch(
                m.cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true).mtuLength(8192),
                m.cfg.archiveContext(), m.cfg.consensusModuleContext());
        m.container = ClusteredServiceContainer.launch(m.cfg.clusteredServiceContext());
        return m;
    }

    private static Member awaitLeader(final List<Member> members, final Member except) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (final Member m : members) {
                if (!m.killed && (except == null || m.id != except.id) && m.role() == Cluster.Role.LEADER) {
                    return m;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no leader" + (except == null ? "" : " other than member " + except.id) + " within 30 s");
    }

    private static List<Member> liveMembersExcept(final List<Member> members, final Member except) {
        final List<Member> out = new ArrayList<>();
        for (final Member m : members) {
            if (!m.killed && m.id != except.id) {
                out.add(m);
            }
        }
        return out;
    }

    /** Poll a member's engine until the order carries EXPIRY_REQUESTED (a follower applies the TimerEvent a little later). */
    private static CoreOrder awaitExpiryRequested(final Member m, final long orderId, final long deadlineMs) throws InterruptedException {
        CoreOrder o = m.order(orderId);
        while (System.currentTimeMillis() < deadlineMs && (o == null || !o.has(CoreOrder.F_EXPIRY_REQUESTED))) {
            Thread.sleep(20);
            o = m.order(orderId);
        }
        return o;
    }

    private static void assertGtdDeadlineIndexed(final Member m, final long orderId, final long expiresAtMs) {
        final boolean[] indexed = {false};
        m.service.engine().deadlines().forEach((d, id, k) -> {
            if (k == DeadlineIndex.KIND_GTD && id == orderId && d == expiresAtMs) {
                indexed[0] = true;
            }
        });
        assertTrue("member " + m.id + " holds the GTD deadline (" + expiresAtMs + ", order " + orderId + ") in its index: the index is "
                + "replicated state, not the dead leader's timer", indexed[0]);
    }

    @Test
    public void theGtdCancelIsMintedExactlyOnceAtTheDeadlineByTheNewLeaderAfterTheOldLeaderIsKilled() throws Exception {
        // Three members on one host need the shared-host port layout; the stride is read once.
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        // Three AE services in one JVM would all bind the probe endpoint's default port (9700 + node.id); take ephemeral ports.
        System.setProperty("assets.node.port", "0");

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-sweep-leader-change-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final List<String> hosts = List.of("localhost", "localhost", "localhost");
        final List<Member> members = new ArrayList<>();
        AeronCluster admin = null, tool = null, gateway = null, forwarder = null, bridge = null, forwarder2 = null;
        try {
            for (int i = 0; i < MEMBERS; i++) {
                members.add(boot(i, hosts, tmp));
            }
            final Member oldLeader = awaitLeader(members, null);
            System.out.println("[TEST] leader elected: member " + oldLeader.id);
            // Every client rides a FOLLOWER's media driver so the kill never takes a client's driver with it.
            final String clientDriver = liveMembersExcept(members, oldLeader).get(0).driverDir;
            final String allEndpoints = ingressEndpointsOf(members, false);

            final Capture gw = new Capture();
            gateway = connect(clientDriver, allEndpoints, TestAuth.credentials(SessionClass.GATEWAY), gw);
            register(gateway, SessionClass.GATEWAY, false, true, false);
            gw.await(gateway, registerAck(), System.currentTimeMillis() + TIMEOUT_MS, "the GATEWAY RegisterSession must be acked");
            final Capture fwd = new Capture();
            forwarder = connect(clientDriver, allEndpoints, TestAuth.credentials(SessionClass.FORWARDER), fwd);
            register(forwarder, SessionClass.FORWARDER, false, false, true);
            fwd.await(forwarder, registerAck(), System.currentTimeMillis() + TIMEOUT_MS, "the FORWARDER RegisterSession must be acked");
            final Capture toolCap = new Capture();
            tool = connect(clientDriver, allEndpoints, TestAuth.credentials(SessionClass.TOOL), toolCap);
            offer(tool, encodeDeposit(USER, 1_000 * ONE));

            // The cluster time, read from the GATE_CLOSED reject of a probe create (no record, no dispatch).
            offer(gateway, encodeCreate(0xA1L, USER, 1, TimeInForce.GTC, 0L));
            gw.await(gateway, f -> f.startsWith("REJECT ref=50 reason=GATE_CLOSED corr=" + 0xA1L), System.currentTimeMillis() + TIMEOUT_MS,
                    "the probe create before the gate opens must be rejected GATE_CLOSED");
            final long clusterNow = gw.lastRejectedAtMs;
            assertTrue("cluster time is epoch milliseconds (K3), got " + clusterNow, Math.abs(clusterNow - System.currentTimeMillis()) < 60_000);
            final long expiresAtMs = clusterNow + GTD_WINDOW_MS;

            final Capture adminCap = new Capture();
            admin = connect(clientDriver, allEndpoints, TestAuth.credentials(SessionClass.ADMIN), adminCap);
            offer(admin, encodeGateOpen(0xAD1L));
            assertTrue(adminCap.await(admin, f -> f.startsWith("ADMINACK ref=73"), System.currentTimeMillis() + TIMEOUT_MS,
                    "the ADMIN gate open must be acked").contains("result=ACCEPTED"));

            // GTD BUY LIMIT expiring 25 s from the cluster time; the forwarder sees its create.
            offer(gateway, encodeCreate(0xA2L, USER, 2, TimeInForce.GTD, expiresAtMs));
            final String ack = gw.await(gateway, f -> f.startsWith("ORDERACK corr=" + 0xA2L), System.currentTimeMillis() + TIMEOUT_MS,
                    "the GTD create must be acked");
            assertTrue(ack, ack.contains(" order=" + ORDER_ID + " ") && ack.contains("status=PENDING_NEW"));
            final String create = fwd.await(forwarder, f -> f.startsWith("DISPATCH seq=1 "), System.currentTimeMillis() + TIMEOUT_MS,
                    "the forwarder must receive DISPATCH seq=1 for the create");
            assertTrue(create, create.contains("kind=CREATE") && create.contains(" order=" + ORDER_ID + " "));

            // BRIDGE: the ME applied the create as leg 77 (contract 3.3 CREATE APPLIED): ACK_NEW on the orders channel.
            final Capture bridgeCap = new Capture();
            bridge = connect(clientDriver, allEndpoints, TestAuth.credentials(SessionClass.BRIDGE), bridgeCap);
            register(bridge, SessionClass.BRIDGE, true, false, false);
            bridgeCap.await(bridge, registerAck(), System.currentTimeMillis() + TIMEOUT_MS, "the BRIDGE RegisterSession must be acked");
            offer(bridge, encodeCreateApplied(ORDER_ID, USER, LEG));
            gw.await(gateway, f -> f.startsWith("EVENT") && f.contains(" order=" + ORDER_ID + " ") && f.contains("kind=ACK_NEW"),
                    System.currentTimeMillis() + TIMEOUT_MS,
                    "the BRIDGE MeCommandOutcome APPLIED must give order " + ORDER_ID + " its leg (contract 3.3): an ACK_NEW OrderEvent "
                            + "was expected on the orders channel");
            assertEquals("the leader's record carries the learned leg", LEG, oldLeader.order(ORDER_ID).currentLegId);
            assertEquals("no CANCEL is minted while the order rests before the kill; frames: " + fwd.frames,
                    0, fwd.count(cancelDispatchFor(ORDER_ID)));

            // The old sessions close cleanly first: after the kill the ONLY dispatch consumer is the fresh forwarder.
            CloseHelper.quietCloseAll(bridge, forwarder, gateway, tool, admin);
            bridge = null; forwarder = null; gateway = null; tool = null; admin = null;

            // Kill the leader (container, then its media driver) at least 10 s before the deadline.
            final long killMs = System.currentTimeMillis();
            assertTrue("precondition: the leader must die at least " + MIN_KILL_LEAD_MS + " ms before the GTD deadline (it would die "
                    + (expiresAtMs - killMs) + " ms before it); the setup took too long for the kill to be a mid-life failover",
                    expiresAtMs - killMs >= MIN_KILL_LEAD_MS);
            oldLeader.close();
            System.out.println("[TEST] killed leader member " + oldLeader.id + " at deadline-" + (expiresAtMs - killMs) + " ms");

            // A new leader among the survivors; its sweep fires so far were replicated ticks of the old leader's belt.
            final Member newLeader = awaitLeader(members, oldLeader);
            assertNotEquals(oldLeader.id, newLeader.id);
            final long firesAtTakeover = newLeader.service.sweepTimerFires();
            final Member follower = liveMembersExcept(members, newLeader).get(0);
            final List<Member> survivors = liveMembersExcept(members, oldLeader);
            System.out.println("[TEST] new leader: member " + newLeader.id + " after " + (System.currentTimeMillis() - killMs)
                    + " ms; sweep fires so far " + firesAtTakeover + "; follower member " + follower.id);

            // A FRESH forwarder session through the survivors' ingress endpoints, on the surviving follower's driver.
            final Capture fwd2 = new Capture();
            forwarder2 = connect(follower.driverDir, ingressEndpointsOf(members, true), TestAuth.credentials(SessionClass.FORWARDER), fwd2);
            register(forwarder2, SessionClass.FORWARDER, false, false, true);
            fwd2.await(forwarder2, registerAck(), System.currentTimeMillis() + TIMEOUT_MS,
                    "the new leader must ack the fresh forwarder's RegisterSession");
            final long reconnectedAt = System.currentTimeMillis();
            assertTrue("precondition: the fresh forwarder must be registered before the GTD deadline so the before-deadline check means "
                    + "something (deadline was " + (reconnectedAt - expiresAtMs) + " ms ago)", reconnectedAt < expiresAtMs - 500);
            for (final Member m : survivors) {
                final CoreOrder o = m.order(ORDER_ID);
                assertNotNull("member " + m.id + " holds the GTD order (replicated state)", o);
                assertEquals("member " + m.id + " knows the leg learned from the BRIDGE outcome", LEG, o.currentLegId);
                assertFalse("member " + m.id + ": no expiry yet, the deadline is still ahead", o.has(CoreOrder.F_EXPIRY_REQUESTED));
                assertGtdDeadlineIndexed(m, ORDER_ID, expiresAtMs);
            }

            // Nothing before the deadline.
            fwd2.pollUntil(forwarder2, expiresAtMs - 250);
            assertEquals("no CANCEL may be minted for order " + ORDER_ID + " before its GTD deadline; frames: " + fwd2.frames,
                    0, fwd2.count(cancelDispatchFor(ORDER_ID)));

            // Exactly one at the deadline, minted by the NEW leader's sweep.
            final String cancel = fwd2.await(forwarder2, cancelDispatchFor(ORDER_ID), expiresAtMs + 10_000,
                    "exactly one DISPATCH kind=CANCEL for order " + ORDER_ID + " is expected at the GTD deadline after the leader change "
                            + "(contract 3.7 + 3.8 #16: the new leader arms the sweep under its own term's timer id; a fixed id would be "
                            + "swallowed by the ex-follower's replay counter and the belt would never re-arm)");
            assertTrue("the cancel is attempt 1 of order " + ORDER_ID + " on leg " + LEG + ": " + cancel,
                    cancel.contains("id=" + ORDER_ID + "/" + ((1L << 2) | 1) + " ") && cancel.contains("old=" + LEG));
            final long arrived = fwd2.arrivedAt.get(fwd2.indexOf(cancelDispatchFor(ORDER_ID)));
            assertTrue("the cancel arrives at or after the deadline (arrived " + (arrived - expiresAtMs) + " ms relative)",
                    arrived >= expiresAtMs - 200);
            assertTrue("the new leader's sweep ticked after the takeover (fires " + newLeader.service.sweepTimerFires() + " vs "
                    + firesAtTakeover + " at takeover): the belt re-armed under the new term's timer id (3.8 #16)",
                    newLeader.service.sweepTimerFires() > firesAtTakeover);

            // And no second one.
            fwd2.pollUntil(forwarder2, arrived + QUIET_AFTER_MS);
            assertEquals("the GTD cancel is minted exactly once (R-C9a) across the leader change: no second DISPATCH kind=CANCEL within "
                    + QUIET_AFTER_MS + " ms; frames: " + fwd2.frames, 1, fwd2.count(cancelDispatchFor(ORDER_ID)));

            // Both survivors applied the same TimerEvent: identical record state on the leader and its follower.
            for (final Member m : survivors) {
                final CoreOrder o = awaitExpiryRequested(m, ORDER_ID, System.currentTimeMillis() + 5_000);
                assertNotNull("member " + m.id + ": the order stays live until the ME terminal", o);
                assertTrue("member " + m.id + ": EXPIRY_REQUESTED is set by the GTD pop", o.has(CoreOrder.F_EXPIRY_REQUESTED));
                assertTrue("member " + m.id + ": CANCEL_PENDING is set by the GTD pop", o.has(CoreOrder.F_CANCEL_PENDING));
                assertTrue("member " + m.id + ": DISPATCH_PENDING is set by the minted cancel", o.has(CoreOrder.F_DISPATCH_PENDING));
                assertEquals("member " + m.id + ": one cancel attempt", 1, o.attempt);
                assertEquals("member " + m.id + ": the pending command is the cancel", CoreOrder.PK_CANCEL, o.pendingKind);
                assertEquals("member " + m.id + ": the cancel targets the learned leg", LEG, o.currentLegId);
                assertEquals("member " + m.id + ": status stays NEW (0), EXPIRY_REQUESTED and CANCEL_PENDING are flags, never statuses "
                        + "(contract section 1)", 0, o.status);
            }
            // The dead leader died before the deadline: its frozen record never saw the pop.
            final CoreOrder frozen = oldLeader.order(ORDER_ID);
            assertNotNull("the killed leader's engine still holds the order it accepted", frozen);
            assertFalse("the killed leader never popped the GTD deadline (it died " + (expiresAtMs - killMs) + " ms before it)",
                    frozen.has(CoreOrder.F_EXPIRY_REQUESTED));
            assertEquals("the killed leader never minted the cancel", 0, frozen.attempt);
            System.out.println("[TEST] GTD cancel for order " + ORDER_ID + " minted once by member " + newLeader.id + " at deadline+"
                    + (arrived - expiresAtMs) + " ms; new leader sweep fires " + newLeader.service.sweepTimerFires()
                    + " (was " + firesAtTakeover + " at takeover)");
        } finally {
            CloseHelper.quietCloseAll(forwarder2, bridge, forwarder, gateway, tool, admin);
            for (final Member m : members) {
                m.close();
            }
            IoUtil.delete(tmp, true);
            System.clearProperty("cluster.port.stride");
            System.clearProperty("assets.node.port");
            ClusterPorts.resetStrideForTest();
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
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
import com.openexchange.assets.infrastructure.generated.OrderRejectDecoder;
import com.openexchange.assets.infrastructure.generated.OrderSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.OrderType;
import com.openexchange.assets.infrastructure.generated.RequestOrderSnapshotEncoder;
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
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared machinery of {@link CoreChunkedRestore100kClusterTest}: one single-node AE booted on fixed dirs (cold
 * restart = boot again on the same dirs), TestAuth sessions with a keep-alive once per second in every polling loop
 * (S5 decision 17), the ingress encoders, a lean egress listener that counts acks instead of storing 100k strings,
 * and a stdout/stderr tee that timestamps the service's {@code [SNAPSHOT]} lines. Package-private.
 */
final class CoreRestoreRig {

    static final int PORT_BASE = 20500;
    static final long ONE = FixedPoint.fromDouble(1.0);
    static final long OP_TIMEOUT_MS = 30_000;
    private static final long[] BASE_PX = {0, 50_000 * ONE, 3_000 * ONE, 150 * ONE, 2 * ONE, ONE};

    private CoreRestoreRig() {
    }

    /** The market a user trades on: every user funds USD and that market's base asset. */
    static int marketOf(final long user) {
        return 1 + (int) (user % 5);
    }

    /** A resting price that never crosses: BUY below the base, SELL above it. */
    static long priceOf(final int market, final boolean buy, final int k) {
        final long step = BASE_PX[market] / 1000;
        return buy ? BASE_PX[market] - (1 + k) * step : BASE_PX[market] + (1 + k) * step;
    }

    // ---------------------------------------------------------------- node

    static final class Node {
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

    /**
     * Boot one member. {@code failFast} replaces the service's halt seam (package-private field, set by reflection
     * before launch) so a refused restore is recorded instead of killing the surefire fork.
     */
    static Node boot(final File baseDir, final String aeronDir, final Runnable failFast) {
        TestAuth.install();
        final AssetsClusteredService service = new AssetsClusteredService();
        try {
            final Field f = AssetsClusteredService.class.getDeclaredField("failFast");
            f.setAccessible(true);
            f.set(service, failFast);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("cannot swap the failFast seam", e);
        }
        final List<String> hosts = List.of("localhost");
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

    /** The newest valid service snapshot's recording id, or -1. */
    static long latestServiceSnapshot(final File clusterDir) {
        long id = -1;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == 0) {
                    id = e.recordingId;
                }
            }
        }
        return id;
    }

    static AeronCluster connect(final String aeronDir, final SessionClass cls, final Listener listener, final long deadlineMs,
                                final long retrySleepMs) throws InterruptedException {
        final CredentialsSupplier credentials = TestAuth.credentials(cls);
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadlineMs) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .credentialsSupplier(credentials)
                        .egressListener(listener)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                        .aeronDirectoryName(aeronDir)
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(List.of("localhost"), PORT_BASE,
                                ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
            } catch (final RuntimeException e) {
                last = e; // still electing / restoring
                Thread.sleep(retrySleepMs);
            }
        }
        throw new IllegalStateException("could not connect a " + cls + " session", last);
    }

    // ---------------------------------------------------------------- sessions

    /** A client plus its listener and its keep-alive clock. */
    static final class Session {
        final AeronCluster client;
        final Listener in;
        private long lastKeepAliveMs = System.currentTimeMillis();

        Session(final AeronCluster client, final Listener in) {
            this.client = client;
            this.in = in;
        }

        int poll() {
            final int n = client.pollEgress();
            final long now = System.currentTimeMillis();
            if (now - lastKeepAliveMs >= 1_000) {
                client.sendKeepAlive();
                lastKeepAliveMs = now;
            }
            return n;
        }
    }

    /** Poll every session (egress + keep-alive) until {@code done} holds; fail with {@code why} at the deadline. */
    static void await(final List<Session> sessions, final java.util.function.BooleanSupplier done, final long deadlineMs,
                      final String why) {
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (!done.getAsBoolean()) {
            if (System.currentTimeMillis() > deadlineMs) {
                throw new AssertionError(why);
            }
            int work = 0;
            for (final Session s : sessions) {
                work += s.poll();
            }
            idle.idle(work);
        }
    }

    /** Poll every session (egress + keep-alive) until the wall clock reaches {@code untilMs}. */
    static void pollUntil(final List<Session> sessions, final long untilMs) {
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < untilMs) {
            int work = 0;
            for (final Session s : sessions) {
                work += s.poll();
            }
            idle.idle(work);
        }
    }

    /** Offer until accepted, polling every session meanwhile (back pressure must not starve egress or keep-alives). */
    static void offer(final Session to, final UnsafeBuffer buf, final int length, final List<Session> all) {
        final long deadline = System.currentTimeMillis() + OP_TIMEOUT_MS;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (to.client.offer(buf, 0, length) <= 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("offer not accepted within " + OP_TIMEOUT_MS + " ms");
            }
            for (final Session s : all) {
                s.poll();
            }
            idle.idle();
        }
    }

    static void register(final Session s, final SessionClass cls, final boolean settlements, final boolean dispatch,
                         final List<Session> all) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);
        final int len = TestAuth.encodeRegisterSession(buf, 0x8E6L, cls, true, true, false, settlements, false, false, dispatch, false);
        offer(s, buf, len, all);
        await(all, () -> s.in.registered.get() > 0, System.currentTimeMillis() + OP_TIMEOUT_MS,
                cls + " RegisterSession must be acked (AdminAck ref=80)");
    }

    // ---------------------------------------------------------------- encoders

    static final class Encoders {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[512]);
        private final MessageHeaderEncoder header = new MessageHeaderEncoder();
        private final DepositEncoder deposit = new DepositEncoder();
        private final CreateOrderEncoder create = new CreateOrderEncoder();
        private final OrderEntryGateEncoder gate = new OrderEntryGateEncoder();
        private final MeCommandOutcomeEncoder outcome = new MeCommandOutcomeEncoder();
        private final RequestOrderSnapshotEncoder query = new RequestOrderSnapshotEncoder();

        int deposit(final long corr, final long user, final int asset, final long amount) {
            deposit.wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).assetId(asset).amount(amount)
                    .requestIdHigh(0).requestIdLow(0);
            return MessageHeaderEncoder.ENCODED_LENGTH + deposit.encodedLength();
        }

        int gateOpen(final long corr) {
            gate.wrapAndApplyHeader(buf, 0, header).correlationId(corr).marketId(0).state(GateState.OPEN).reason(GateReason.ADMIN);
            return MessageHeaderEncoder.ENCODED_LENGTH + gate.encodedLength();
        }

        int limit(final long corr, final long user, final long requestLow, final int market, final boolean buy, final long price,
                  final long qty, final TimeInForce tif, final long expiresAtMs) {
            create.wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).clientOrderIdHigh(0).clientOrderIdLow(0)
                    .requestIdHigh(0x100L).requestIdLow(requestLow).marketId(market).side(buy ? Side.BUY : Side.SELL)
                    .orderType(OrderType.LIMIT).timeInForce(tif).price(price).quantity(qty).budget(0).stopPrice(0)
                    .trailingDelta(0).displayQty(0).expiresAtMs(expiresAtMs);
            create.flags().clear();
            return MessageHeaderEncoder.ENCODED_LENGTH + create.encodedLength();
        }

        /** MeCommandOutcome APPLIED for (orderId, attempt 0, CREATE) carrying the ME leg id. */
        int createApplied(final long orderId, final long user, final int market, final long legId) {
            outcome.wrapAndApplyHeader(buf, 0, header).egressSeq(1).commandIdHigh(orderId).commandIdLow(0L).userId(user)
                    .omsOrderId(orderId).oldOrderId(0).price(0).quantity(0).budget(0).marketId(market).commandKind((short) 0)
                    .orderType((short) 0).orderSide((short) 0).appliedPosition(0).meTimestamp(0L).orderId(legId).status(0).reason(0)
                    .oldCancelled((short) 0).result(0).meEpoch(0L);
            return MessageHeaderEncoder.ENCODED_LENGTH + outcome.encodedLength();
        }

        int orderSnapshot(final long corr, final long user) {
            query.wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).fromOrderId(0);
            return MessageHeaderEncoder.ENCODED_LENGTH + query.encodedLength();
        }
    }

    static int baseAssetOf(final int market) {
        return Market.fromId(market).baseAsset().id();
    }

    static int usd() {
        return Asset.USD.id();
    }

    // ---------------------------------------------------------------- egress

    /** Counts acks and rejects; keeps only the rare frames as text. Decoded on the polling thread. */
    static final class Listener implements EgressListener {
        final AtomicInteger registered = new AtomicInteger();
        int acks;
        int rejects;
        long lastAckOrderId;
        long lastAckAtMs;
        String lastAckStatus;
        String firstReject;
        long lastDepositAckCorr = -1;
        final List<String> frames = new CopyOnWriteArrayList<>();
        final List<long[]> snapshotEnds = new CopyOnWriteArrayList<>(); // {corr, entryCount, nextOrderId, arrivedNs}
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final OrderAckDecoder ack = new OrderAckDecoder();
        private final OrderRejectDecoder reject = new OrderRejectDecoder();
        private final MeDispatchDecoder dispatch = new MeDispatchDecoder();
        private final MeDispatchBatchDecoder dispatchBatch = new MeDispatchBatchDecoder();
        private final AdminAckDecoder adminAck = new AdminAckDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();
        private final OrderSnapshotEndDecoder snapshotEnd = new OrderSnapshotEndDecoder();

        public void onMessage(final long sessionId, final long timestamp, final DirectBuffer buffer, final int offset, final int length,
                              final io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case OrderAckDecoder.TEMPLATE_ID:
                    ack.wrapAndApplyHeader(buffer, offset, header);
                    acks++;
                    lastAckOrderId = ack.orderId();
                    lastAckAtMs = ack.acceptedAtMs();
                    lastAckStatus = String.valueOf(ack.status());
                    break;
                case OrderRejectDecoder.TEMPLATE_ID:
                    reject.wrapAndApplyHeader(buffer, offset, header);
                    rejects++;
                    if (firstReject == null) {
                        firstReject = "REJECT ref=" + reject.refTemplateId() + " reason=" + reject.reason() + " corr=" + reject.correlationId();
                    }
                    break;
                case MeDispatchDecoder.TEMPLATE_ID:
                    dispatch.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("DISPATCH seq=" + dispatch.dispatchSeq() + " id=" + dispatch.commandIdHigh() + "/" + dispatch.commandIdLow()
                            + " kind=" + dispatch.kind() + " order=" + dispatch.omsOrderId() + " old=" + dispatch.oldOrderId()
                            + " at=" + System.currentTimeMillis());
                    break;
                case MeDispatchBatchDecoder.TEMPLATE_ID:
                    dispatchBatch.wrapAndApplyHeader(buffer, offset, header);
                    for (final MeDispatchBatchDecoder.DispatchesDecoder d : dispatchBatch.dispatches()) {
                        frames.add("DISPATCH seq=" + d.dispatchSeq() + " id=" + d.commandIdHigh() + "/" + d.commandIdLow()
                                + " kind=" + d.kind() + " order=" + d.omsOrderId() + " old=" + d.oldOrderId()
                                + " at=" + System.currentTimeMillis());
                    }
                    break;
                case AdminAckDecoder.TEMPLATE_ID:
                    adminAck.wrapAndApplyHeader(buffer, offset, header);
                    if (adminAck.refTemplateId() == 80) {
                        registered.incrementAndGet();
                    }
                    frames.add("ADMINACK ref=" + adminAck.refTemplateId() + " result=" + adminAck.result());
                    break;
                case DepositAckDecoder.TEMPLATE_ID:
                    depositAck.wrapAndApplyHeader(buffer, offset, header);
                    if (depositAck.correlationId() != 0) {
                        lastDepositAckCorr = depositAck.correlationId();
                    }
                    break;
                case OrderSnapshotEndDecoder.TEMPLATE_ID:
                    snapshotEnd.wrapAndApplyHeader(buffer, offset, header);
                    snapshotEnds.add(new long[] {snapshotEnd.correlationId(), snapshotEnd.entryCount(), snapshotEnd.nextOrderId(),
                            System.nanoTime()});
                    break;
                default:
                    break;
            }
        }

        long[] snapshotEnd(final long corr) {
            for (final long[] e : snapshotEnds) {
                if (e[0] == corr) {
                    return e;
                }
            }
            return null;
        }

        long count(final java.util.function.Predicate<String> match) {
            return frames.stream().filter(match).count();
        }
    }

    // ---------------------------------------------------------------- stdout / stderr tee

    /** Records every printed line containing a marker, with the nanoTime it was printed at. */
    static final class Tee extends PrintStream {
        final List<String> lines = new CopyOnWriteArrayList<>();
        final List<Long> at = new CopyOnWriteArrayList<>();
        private final String marker;

        Tee(final PrintStream target, final String marker) {
            super(target, true);
            this.marker = marker;
        }

        @Override
        public void println(final String x) {
            if (x != null && x.contains(marker)) {
                lines.add(x);
                at.add(System.nanoTime());
            }
            super.println(x);
        }

        @Override
        public void println(final Object x) {
            final String s = String.valueOf(x);
            if (s.contains(marker)) {
                lines.add(s);
                at.add(System.nanoTime());
            }
            super.println(x);
        }

        void clear() {
            lines.clear();
            at.clear();
        }

        /** Index of the first recorded line containing {@code needle}, or -1. */
        int first(final String needle) {
            for (int i = 0; i < lines.size(); i++) {
                if (lines.get(i).contains(needle)) {
                    return i;
                }
            }
            return -1;
        }

        List<String> all(final String needle) {
            final List<String> out = new ArrayList<>();
            for (final String l : lines) {
                if (l.contains(needle)) {
                    out.add(l);
                }
            }
            return out;
        }
    }
}

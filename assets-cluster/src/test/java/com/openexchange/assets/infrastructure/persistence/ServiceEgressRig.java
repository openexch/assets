// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.infrastructure.generated.MeDispatchBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.PendingDispatchEndDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import com.openexchange.cluster.auth.ClusterPrincipal;
import io.aeron.Aeron;
import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * RC2 S8 G11 (S12 13.1 A-1): an {@link AssetsClusteredService} as the leader, with real {@code SessionEgressQueue}s behind
 * recording client sessions and the production {@link AssetsEventPublisher} wired as {@code onStart} does (never called
 * here: it starts the endpoint and the pruner). Frames go through {@code onSessionMessage}, which applies and then drains.
 */
final class ServiceEgressRig {

    /** A client session that records every frame offered to it; can back-pressure. */
    static final class RecSession implements ClientSession {
        final long id;
        final byte[] principal;
        final List<byte[]> delivered = new ArrayList<>();
        int accepts = Integer.MAX_VALUE;
        /** A non-zero Publication result every offer returns (NOT_CONNECTED, MAX_POSITION_EXCEEDED, CLOSED). */
        long failWith;
        boolean closed;

        RecSession(final long id, final byte[] principal) {
            this.id = id;
            this.principal = principal;
        }

        public long id() { return id; }
        public int responseStreamId() { return 0; }
        public String responseChannel() { return "rec:" + id; }
        public byte[] encodedPrincipal() { return principal; }
        public void close() { closed = true; }
        public boolean isClosing() { return closed; }
        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            if (failWith != 0) {
                return failWith;
            }
            if (accepts <= 0) {
                return Publication.BACK_PRESSURED;
            }
            if (accepts != Integer.MAX_VALUE) {
                accepts--;
            }
            final byte[] c = new byte[length];
            buffer.getBytes(offset, c);
            delivered.add(c);
            return 1;
        }
        public long offer(final DirectBufferVector[] vectors) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int length, final BufferClaim claim) { throw new UnsupportedOperationException(); }

        /** Delivered dispatch-channel entries flattened in arrival order: {@code D<seq>} per 104 entry, {@code E<count>/<head>}. */
        List<String> dispatchStream() {
            final List<String> out = new ArrayList<>();
            final MessageHeaderDecoder h = new MessageHeaderDecoder();
            for (final byte[] f : delivered) {
                final UnsafeBuffer b = new UnsafeBuffer(f);
                h.wrap(b, 0);
                switch (h.templateId()) {
                    case MeDispatchDecoder.TEMPLATE_ID:
                        out.add("D" + new MeDispatchDecoder().wrapAndApplyHeader(b, 0, h).dispatchSeq());
                        break;
                    case MeDispatchBatchDecoder.TEMPLATE_ID:
                        for (final MeDispatchBatchDecoder.DispatchesDecoder d : new MeDispatchBatchDecoder().wrapAndApplyHeader(b, 0, h).dispatches()) {
                            out.add("D" + d.dispatchSeq());
                        }
                        break;
                    case PendingDispatchEndDecoder.TEMPLATE_ID: {
                        final PendingDispatchEndDecoder e = new PendingDispatchEndDecoder().wrapAndApplyHeader(b, 0, h);
                        out.add("E" + e.count() + "/" + e.headDispatchSeq());
                        break;
                    }
                    default:
                        break;
                }
            }
            return out;
        }

        boolean sawBatch() {
            final MessageHeaderDecoder h = new MessageHeaderDecoder();
            for (final byte[] f : delivered) {
                if (h.wrap(new UnsafeBuffer(f), 0).templateId() == MeDispatchBatchDecoder.TEMPLATE_ID) {
                    return true;
                }
            }
            return false;
        }

        List<Integer> templates() {
            final List<Integer> out = new ArrayList<>();
            final MessageHeaderDecoder h = new MessageHeaderDecoder();
            for (final byte[] f : delivered) {
                out.add(h.wrap(new UnsafeBuffer(f), 0).templateId());
            }
            return out;
        }
    }

    final class StubCluster implements Cluster {
        Role role = Role.LEADER;
        public Role role() { return role; }
        public long logPosition() { return position; }
        public int memberId() { return 0; }
        public Aeron aeron() { throw new UnsupportedOperationException(); }
        public ClusteredServiceContainer.Context context() { throw new UnsupportedOperationException(); }
        public ClientSession getClientSession(final long id) { return sessions.get(id); }
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Collection<ClientSession> clientSessions() { return (Collection) Collections.unmodifiableCollection(sessions.values()); }
        public void forEachClientSession(final Consumer<? super ClientSession> action) { sessions.values().forEach(action); }
        public boolean closeClientSession(final long id) { sessions.get(id).close(); return true; }
        public long time() { return now; }
        public TimeUnit timeUnit() { return TimeUnit.MILLISECONDS; }
        public boolean scheduleTimer(final long correlationId, final long deadline) { return true; }
        public boolean cancelTimer(final long correlationId) { return true; }
        public IdleStrategy idleStrategy() { return NoOpIdleStrategy.INSTANCE; }
        public long offer(final DirectBuffer b, final int o, final int l) { throw new UnsupportedOperationException(); }
        public long offer(final DirectBufferVector[] v) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int l, final BufferClaim c) { throw new UnsupportedOperationException(); }
    }

    final AssetsClusteredService service = new AssetsClusteredService();
    final Map<Long, RecSession> sessions = new LinkedHashMap<>();
    final StubCluster cluster = new StubCluster();
    final CoreFrameEncoders enc = new CoreFrameEncoders();
    long now = CoreFrameStory.T0;
    long position = 64;
    long jseq = 1;
    long egress = 1;
    private long corr = 1;

    ServiceEgressRig() {
        service.failFast = () -> { throw new AssertionError("fail-fast"); };
        ApplierParityRig.inject(service, "cluster", cluster);
        ApplierParityRig.inject(service, "isLeader", true);
        ApplierParityRig.inject(service, "idleStrategy", NoOpIdleStrategy.INSTANCE);
        final AssetsLogApplier applier = (AssetsLogApplier) ApplierParityRig.read(service, AssetsClusteredService.class, "applier");
        try {
            final Method enqueue = AssetsClusteredService.class.getDeclaredMethod("enqueueEgress", MutableDirectBuffer.class, int.class,
                    int.class, int.class, AssetsEventPublisher.Routing.class);
            enqueue.setAccessible(true);
            final Field query = AssetsClusteredService.class.getDeclaredField("originIsQuery");
            query.setAccessible(true);
            applier.setEventSink(new AssetsEventPublisher((buffer, offset, length, channel, routing) -> {
                try {
                    enqueue.invoke(service, buffer, offset, length, channel, routing);
                } catch (final ReflectiveOperationException e) {
                    throw new AssertionError(e);
                }
            }, () -> {
                try {
                    return query.getBoolean(service);
                } catch (final IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }));
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        engine().coreConfig().setSupportedBehaviorVersions(1, 2, 3);
        enc.meEpoch = V3Genesis.GENESIS_ID;
        enc.genesisId = V3Genesis.GENESIS_ID;
    }

    AssetsEngine engine() {
        return service.engine();
    }

    /** Setup through the engine API: applied as a follower would (no client egress), then leader again. */
    void quiet(final Runnable setup) {
        ApplierParityRig.inject(service, "isLeader", false);
        try {
            setup.run();
        } finally {
            ApplierParityRig.inject(service, "isLeader", true);
        }
    }

    /** Behaviour 3 bound (FRESH at journalSeq 1) and GLOBAL opened, through the engine (no session yet). */
    ServiceEgressRig v3() {
        quiet(() -> {
            V3Genesis.boot(engine(), now);
            engine().applyOrderEntryGate(1, com.openexchange.assets.application.engine.gate.OrderEntryGate.GLOBAL,
                    com.openexchange.assets.domain.GateState.OPEN, com.openexchange.assets.domain.GateReason.ADMIN, now);
        });
        return this;
    }

    /** A USD deposit for {@code user}, quiet. */
    void deposit(final long user) {
        quiet(() -> engine().applyCommand(AssetsEngine.CMD_DEPOSIT, PendingReplyContiguityTest.deposit(user), now));
    }

    RecSession open(final long id, final SessionClass cls) {
        final RecSession s = new RecSession(id, ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(cls), (int) id).encode());
        sessions.put(id, s);
        service.onSessionOpen(s, now);
        return s;
    }

    /** Open and register (acks, orders, dispatch, control). */
    RecSession registered(final long id, final SessionClass cls) {
        final RecSession s = open(id, cls);
        send(s, enc.register(corr++, cls));
        return s;
    }

    void send(final RecSession s, final byte[] frame) {
        position += 64;
        service.onSessionMessage(s, now, new UnsafeBuffer(frame), 0, frame.length, LogHeaders.at(position));
    }

    /** The container's close of {@code s}: removed from the cluster's sessions, then onSessionClose. */
    void containerClose(final RecSession s) {
        sessions.remove(s.id);
        service.onSessionClose(s, now, io.aeron.cluster.codecs.CloseReason.CLIENT_ACTION);
    }

    /** The node's {@code /metrics} text, as the endpoint renders it. */
    String metrics() {
        try {
            final Method m = AssetsClusteredService.class.getDeclaredMethod("metricsBody");
            m.setAccessible(true);
            return ((com.openexchange.assets.infrastructure.metrics.AssetsMetricsBody) m.invoke(service)).render();
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    long series(final String name) {
        for (final String line : metrics().split("\n")) {
            if (line.startsWith(name + " ")) {
                return Long.parseLong(line.substring(name.length() + 1).trim());
            }
        }
        throw new AssertionError("no series " + name);
    }

    long nextCorr() {
        return corr++;
    }

    /** A LIMIT BUY of {@code user} at 100 for 1, from a gateway session. */
    void create(final RecSession gateway, final long user) {
        send(gateway, enc.create(corr++, user, 0, 0xAB, corr, 1, 0, 0, 0, 100_00000000L, 100_000000L, 0, 0, 0, 0, 0));
    }

    /** The ME outcome of attempt 0 CREATE APPLIED for order {@code h} of {@code user} at {@code leg}, from the bridge. */
    void ack(final RecSession bridge, final long h, final long user, final long leg) {
        send(bridge, enc.outcome(++egress, h, 0, user, h, 0, 100_00000000L, 100_000000L, 0, 1, 0, 0, 0, now, leg, 0, 0, ++jseq));
    }
}

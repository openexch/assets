// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.cluster.SnapshotEnvelope;
import io.aeron.Aeron;
import io.aeron.DirectBufferVector;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Test rig: the two ways one {@link SessionedLog} can be applied, behind one {@link Arm} interface, plus a
 * per-step {@link Probe} that turns "what this entry did" into a comparable string.
 *
 * <ul>
 *   <li>{@link ServiceArm} is the live oracle: an {@link CoreClusteredService} with a stub {@link Cluster} injected
 *       (never {@code onStart}, which starts the endpoint and the pruner) and a container emulation of
 *       {@code ClusteredServiceAgent}: an open adds the session then calls back (a clashing id throws, as the
 *       container does), a close removes it and calls back only if it was present, a message passes
 *       {@code map.get(id)} (null for an id the container does not hold).</li>
 *   <li>{@link ApplierArm} drives one {@link CoreLogApplier} by frame ids.</li>
 * </ul>
 * The stub cluster and the injection are copies of {@code SweepTimerServiceTest}'s (private there; that file is not
 * edited), extended so the leader-local transport hooks reach a session object.
 */
final class ApplierParityRig {

    private ApplierParityRig() {
    }

    /** A container-held client session: id and principal; close is recorded, every transport call throws. */
    static final class StubSession implements ClientSession {
        final long id;
        final byte[] principal;
        boolean closed;

        StubSession(final long id, final byte[] principal) {
            this.id = id;
            this.principal = principal;
        }

        public long id() { return id; }
        public int responseStreamId() { return 0; }
        public String responseChannel() { return "stub:" + id; }
        public byte[] encodedPrincipal() { return principal; }
        public void close() { closed = true; }
        public boolean isClosing() { return closed; }
        public long offer(final DirectBuffer buffer, final int offset, final int length) { throw new UnsupportedOperationException(); }
        public long offer(final DirectBufferVector[] vectors) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int length, final BufferClaim bufferClaim) { throw new UnsupportedOperationException(); }
    }

    /** Records scheduleTimer calls; reports no client sessions (so egress has no queue); aeron/context throw. */
    static final class StubCluster implements Cluster {
        final Role role;
        final Map<Long, StubSession> container;
        long time = CoreFrameStory.T0;
        /**
         * The position the cluster serves to the callback in flight, set from the
         * step before every callback ({@link ServiceArm#apply}). In this was a constant 42 on purpose, because open,
         * close and timer bodies had to IGNORE the position; with they stamp it, so a constant would make the
         * service arm diverge from the frame-id arm on every such step. 42 stays the default for the rigs that call the
         * service directly and never set it ({@code CoreSessionAuthorityTest} asserts it in its halt line).
         */
        long logPosition = 42;
        int scheduled;
        /** Instrumentation: how often the service asked for the position, and the last value it was served. */
        long positionReads;
        long lastPositionRead = -1;

        StubCluster(final Role role, final Map<Long, StubSession> container) {
            this.role = role;
            this.container = container;
        }

        public Role role() { return role; }
        public long logPosition() {
            positionReads++;
            lastPositionRead = logPosition;
            return logPosition;
        }
        public int memberId() { return 1; }
        public Aeron aeron() { throw new UnsupportedOperationException(); }
        public ClusteredServiceContainer.Context context() { throw new UnsupportedOperationException(); }
        public ClientSession getClientSession(final long id) { return container.get(id); }
        public Collection<ClientSession> clientSessions() { return List.of(); }
        public void forEachClientSession(final Consumer<? super ClientSession> action) { }
        public boolean closeClientSession(final long id) { throw new UnsupportedOperationException(); }
        public long time() { return time; }
        public TimeUnit timeUnit() { return TimeUnit.MILLISECONDS; }
        public boolean scheduleTimer(final long correlationId, final long deadline) {
            scheduled++;
            return true;
        }
        public boolean cancelTimer(final long correlationId) { throw new UnsupportedOperationException(); }
        public IdleStrategy idleStrategy() { return NoOpIdleStrategy.INSTANCE; }
        public long offer(final DirectBuffer b, final int o, final int l) { throw new UnsupportedOperationException(); }
        public long offer(final DirectBufferVector[] v) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int l, final BufferClaim c) { throw new UnsupportedOperationException(); }
    }

    static void inject(final CoreClusteredService service, final String field, final Object value) {
        try {
            final Field f = CoreClusteredService.class.getDeclaredField(field);
            f.setAccessible(true);
            f.set(service, value);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError("cannot inject " + field, e);
        }
    }

    static Object read(final Object target, final Class<?> type, final String field) {
        try {
            final Field f = type.getDeclaredField(field);
            f.setAccessible(true);
            return f.get(target);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError("cannot read " + field, e);
        }
    }

    interface Arm {
        void apply(SessionedLog.LogStep step);

        CoreEngine engine();

        CoreSbeDemuxer demuxer();

        StampSink sink();

        byte[] image();
    }

    static byte[] copy(final ExpandableArrayBuffer buffer, final int length) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    /** The live oracle: the service behind a container emulation. */
    static final class ServiceArm implements Arm {
        final CoreClusteredService service = new CoreClusteredService();
        final Map<Long, StubSession> container;
        final StubCluster cluster;
        final StampSink sink = new StampSink();
        final int[] failFastCalls = new int[1];
        private final UnsafeBuffer frame = new UnsafeBuffer(new byte[0]);

        ServiceArm(final Cluster.Role role) {
            this(role, new LinkedHashMap<>());
        }

        /** {@code container} is this arm's own map (a restored arm starts from a copy of the container's sessions). */
        ServiceArm(final Cluster.Role role, final Map<Long, StubSession> container) {
            this.container = container;
            this.cluster = new StubCluster(role, container);
            service.failFast = () -> failFastCalls[0]++;
            inject(service, "cluster", cluster);
            inject(service, "isLeader", role == Cluster.Role.LEADER);
            inject(service, "idleStrategy", cluster.idleStrategy());
            service.engine().setEventSink(sink);
        }

        public void apply(final SessionedLog.LogStep s) {
            cluster.time = s.timestamp();
            // every callback kind sees its own frame end: Aeron sets logPosition before invoking the service
            cluster.logPosition = s.logPosition();
            switch (s.kind()) {
                case OPEN -> {
                    if (container.containsKey(s.sessionId())) {
                        throw new IllegalStateException("container clash: open of a held id " + s.sessionId());
                    }
                    final StubSession session = new StubSession(s.sessionId(), s.bytes());
                    container.put(s.sessionId(), session);
                    service.onSessionOpen(session, s.timestamp());
                }
                case CLOSE -> {
                    final StubSession session = container.remove(s.sessionId());
                    if (session != null) {
                        service.onSessionClose(session, s.timestamp(), CloseReason.CLIENT_ACTION);
                    }
                }
                case MESSAGE -> {
                    frame.wrap(s.bytes());
                    service.onSessionMessage(container.get(s.sessionId()), s.timestamp(), frame, 0, s.bytes().length,
                            LogHeaders.at(s.logPosition()));
                }
                case TIMER -> service.onTimerEvent(s.correlationId(), s.timestamp());
                default -> throw new IllegalStateException("kind " + s.kind());
            }
        }

        public CoreEngine engine() { return service.engine(); }
        public CoreSbeDemuxer demuxer() { return service.demuxer(); }
        public StampSink sink() { return sink; }

        public byte[] image() {
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64 * 1024);
            return copy(buffer, CoreClusteredService.encodeSnapshot(service.engine(), buffer, new SnapshotEnvelope()));
        }

        Map<Long, StubSession> containerCopy() {
            return new LinkedHashMap<>(container);
        }
    }

    /** One applier driven by frame ids; {@code dropOpens} feeds it the log with every OPEN removed. */
    static final class ApplierArm implements Arm {
        final int[] failFastCalls = new int[1];
        final CoreLogApplier applier = new CoreLogApplier(() -> failFastCalls[0]++);
        final StampSink sink = new StampSink();
        final boolean dropOpens;
        private final UnsafeBuffer frame = new UnsafeBuffer(new byte[0]);

        ApplierArm() {
            this(false);
        }

        ApplierArm(final boolean dropOpens) {
            this.dropOpens = dropOpens;
            applier.setEventSink(sink);
        }

        public void apply(final SessionedLog.LogStep s) {
            switch (s.kind()) {
                case OPEN -> {
                    if (!dropOpens) {
                        applier.onSessionOpen(s.sessionId(), s.logPosition(), s.timestamp(), s.bytes());
                    }
                }
                case CLOSE -> applier.onSessionClose(s.sessionId(), s.logPosition(), s.timestamp(), CloseReason.CLIENT_ACTION);
                case MESSAGE -> {
                    frame.wrap(s.bytes());
                    applier.onSessionMessage(s.sessionId(), s.logPosition(), s.timestamp(), frame, 0, s.bytes().length);
                }
                case TIMER -> applier.onTimerEvent(s.correlationId(), s.logPosition(), s.timestamp());
                default -> throw new IllegalStateException("kind " + s.kind());
            }
        }

        public CoreEngine engine() { return applier.engine(); }
        public CoreSbeDemuxer demuxer() { return applier.demuxer(); }
        public StampSink sink() { return sink; }

        public byte[] image() {
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64 * 1024);
            return copy(buffer, applier.serialize(buffer));
        }
    }

    static long[] counters(final Arm a) {
        final CoreSbeDemuxer d = a.demuxer();
        return new long[] {d.schemaRejectCount(), d.decodeRejectCount(), d.unknownTemplateCount(),
                d.unhandledTemplateCount(), d.applyErrorCount(), d.unauthorisedCount()};
    }

    /**
     * What one entry did to an arm, as a string: the rendered events it emitted, the position-carrying calls (index
     * within the entry, position), the engine's log position after it, and the counter increments. Every part is
     * relative to the previous mark, so arms that started at different points (a restored arm) stay comparable.
     */
    static final class Probe {
        private final Arm arm;
        private int events;
        private long calls;
        private int stamps;
        private long[] counters;

        Probe(final Arm arm) {
            this.arm = arm;
            events = arm.sink().events().size();
            calls = arm.sink().calls;
            stamps = arm.sink().stamps.size();
            counters = counters(arm);
        }

        String step() {
            final StampSink sink = arm.sink();
            final StringBuilder sb = new StringBuilder();
            for (int i = events; i < sink.events().size(); i++) {
                sb.append(sink.events().get(i).render()).append('\n');
            }
            for (int i = stamps; i < sink.stamps.size(); i++) {
                final long[] s = sink.stamps.get(i);
                sb.append("@").append(s[0] - calls).append('=').append(s[1]).append('\n');
            }
            sb.append("P=").append(arm.engine().getLogPosition());
            final long[] now = counters(arm);
            sb.append(" C=");
            for (int i = 0; i < now.length; i++) {
                sb.append(now[i] - counters[i]).append(',');
            }
            events = sink.events().size();
            calls = sink.calls;
            stamps = sink.stamps.size();
            counters = now;
            return sb.toString();
        }
    }
}

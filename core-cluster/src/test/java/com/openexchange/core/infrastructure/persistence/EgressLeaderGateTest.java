// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.publisher.CoreEventPublisher;
import io.aeron.Aeron;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * F3, decision D8: the egress leader gate that makes the 11.2 TAIL heap/queue bound structural. The tail
 * runs inside {@code onStart}, where {@code isLeader} is {@code cluster.role() == LEADER} and the role before any
 * election is FOLLOWER; a frame enqueued then must return before the session registry or any queue is touched. The
 * leader row is the paired control: the same call on a leader reaches the registry, so the follower row can see the
 * gate.
 */
public class EgressLeaderGateTest {

    /** Counts {@code clientSessions()} lookups (the first thing the enqueue path does past the gate). */
    private static final class GateCluster implements Cluster {
        final Role role;
        int sessionLookups;

        GateCluster(final Role role) {
            this.role = role;
        }

        public Role role() { return role; }
        public long logPosition() { return 42; }
        public int memberId() { return 0; }
        public Aeron aeron() { throw new UnsupportedOperationException(); }
        public ClusteredServiceContainer.Context context() { throw new UnsupportedOperationException(); }
        public ClientSession getClientSession(final long id) { throw new UnsupportedOperationException(); }
        public Collection<ClientSession> clientSessions() {
            sessionLookups++;
            return List.of();
        }
        public void forEachClientSession(final Consumer<? super ClientSession> action) { }
        public boolean closeClientSession(final long id) { throw new UnsupportedOperationException(); }
        public long time() { return 1_000_000L; }
        public TimeUnit timeUnit() { return TimeUnit.MILLISECONDS; }
        public boolean scheduleTimer(final long correlationId, final long deadline) { return true; }
        public boolean cancelTimer(final long correlationId) { throw new UnsupportedOperationException(); }
        public IdleStrategy idleStrategy() { return NoOpIdleStrategy.INSTANCE; }
        public long offer(final org.agrona.DirectBuffer b, final int o, final int l) { throw new UnsupportedOperationException(); }
        public long offer(final io.aeron.DirectBufferVector[] v) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int l, final io.aeron.logbuffer.BufferClaim c) { throw new UnsupportedOperationException(); }
    }

    /** The service as {@code applyOnStart} leaves it: the cluster bound, {@code isLeader} from the role. */
    private static CoreClusteredService onStartState(final GateCluster cluster) throws Exception {
        final CoreClusteredService service = new CoreClusteredService(SnapshotModeSettings.defaults());
        set(service, "cluster", cluster);
        set(service, "isLeader", cluster.role() == Cluster.Role.LEADER);
        set(service, "idleStrategy", cluster.idleStrategy());
        return service;
    }

    private static void set(final CoreClusteredService service, final String name, final Object value)
            throws Exception {
        final Field f = CoreClusteredService.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(service, value);
    }

    private static void enqueue(final CoreClusteredService service, final CoreEventPublisher.Routing routing)
            throws Exception {
        final Method m = CoreClusteredService.class.getDeclaredMethod("enqueueEgress", MutableDirectBuffer.class,
                int.class, int.class, int.class, CoreEventPublisher.Routing.class);
        m.setAccessible(true);
        try {
            m.invoke(service, new UnsafeBuffer(new byte[64]), 0, 64, 1, routing);
        } catch (final InvocationTargetException e) {
            if (e.getCause() instanceof Exception ex) {
                throw ex;
            }
            throw (Error)e.getCause();
        }
    }

    @Test
    public void aFollowerDuringOnStartEnqueuesNothing() throws Exception {
        final GateCluster cluster = new GateCluster(Cluster.Role.FOLLOWER);
        final CoreClusteredService service = onStartState(cluster);
        for (int i = 0; i < 10_000; i++) {
            enqueue(service, CoreEventPublisher.Routing.BROADCAST);
        }
        enqueue(service, CoreEventPublisher.Routing.ORIGIN); // past the gate this would trip the origin assert
        assertEquals("the gate returns before the session registry", 0, cluster.sessionLookups);
        assertEquals("no egress queue exists", 0, ServiceProbe.egressQueues(service));
        System.out.println("GREEN D8 F3 gate: a FOLLOWER (onStart, before any election) enqueued 10001 frames, "
                + "touched no session and created no queue");
    }

    @Test
    public void controlTheSameCallOnALeaderReachesTheRegistry() throws Exception {
        final GateCluster cluster = new GateCluster(Cluster.Role.LEADER);
        final CoreClusteredService service = onStartState(cluster);
        enqueue(service, CoreEventPublisher.Routing.BROADCAST);
        assertTrue("a leader's enqueue consults the sessions: the follower row can see the gate",
                cluster.sessionLookups > 0);
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.time.DeadlineIndex;
import io.aeron.Aeron;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RCS S1 review (added by the independent verifier): the K3 per-term timer ids survive the applier extraction. The
 * service still owns {@code leadershipTermId}; after each {@code onNewLeadershipTermEvent} the drain and sweep timers
 * it arms carry {@code BASE + term * TIMER_ID_TERM_STRIDE}, and a timer that fires under a later term re-arms with the
 * later term's id. The applier classifies every such id as the right kind (the stride never crosses a base).
 * Neither the base nor the slice had an in-process test for this (only the cluster-level leader-change test).
 */
public class TermTimerIdAfterApplierTest {

    private static final class StubCluster implements Cluster {
        long time = 1_000_000L;
        final List<Long> scheduledIds = new ArrayList<>();

        public Role role() { return Role.LEADER; }
        public long logPosition() { return 42; }
        public int memberId() { return 1; }
        public Aeron aeron() { throw new UnsupportedOperationException(); }
        public ClusteredServiceContainer.Context context() { throw new UnsupportedOperationException(); }
        public ClientSession getClientSession(final long id) { throw new UnsupportedOperationException(); }
        public Collection<ClientSession> clientSessions() { return List.of(); }
        public void forEachClientSession(final Consumer<? super ClientSession> action) { }
        public boolean closeClientSession(final long id) { throw new UnsupportedOperationException(); }
        public long time() { return time; }
        public TimeUnit timeUnit() { return TimeUnit.MILLISECONDS; }
        public boolean scheduleTimer(final long correlationId, final long deadline) {
            scheduledIds.add(correlationId);
            return true;
        }
        public boolean cancelTimer(final long correlationId) { throw new UnsupportedOperationException(); }
        public IdleStrategy idleStrategy() { return NoOpIdleStrategy.INSTANCE; }
        public long offer(final org.agrona.DirectBuffer b, final int o, final int l) { throw new UnsupportedOperationException(); }
        public long offer(final io.aeron.DirectBufferVector[] v) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int l, final io.aeron.logbuffer.BufferClaim c) { throw new UnsupportedOperationException(); }

        List<Long> idsSince(final int from) {
            return scheduledIds.subList(from, scheduledIds.size());
        }
    }

    private static void inject(final AssetsClusteredService service, final String field, final Object value)
            throws Exception {
        final Field f = AssetsClusteredService.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(service, value);
    }

    private static AssetsClusteredService leader(final StubCluster cluster) throws Exception {
        final AssetsClusteredService service = new AssetsClusteredService();
        inject(service, "cluster", cluster);
        inject(service, "isLeader", true);
        inject(service, "idleStrategy", cluster.idleStrategy());
        service.engine().setEventSink(new SnapshotEnvelopeAdoptionTest.NoopSink());
        return service;
    }

    private static void term(final AssetsClusteredService service, final long termId, final long time) {
        service.onNewLeadershipTermEvent(termId, 4_096, time, 0, 1, 7, TimeUnit.MILLISECONDS, 0);
    }

    private static long drainId(final long term) {
        return AssetsLogApplier.EGRESS_DRAIN_TIMER_BASE + term * AssetsLogApplier.TIMER_ID_TERM_STRIDE;
    }

    private static long sweepId(final long term) {
        return AssetsLogApplier.SWEEP_TIMER_BASE + term * AssetsLogApplier.TIMER_ID_TERM_STRIDE;
    }

    @Test
    public void timersArmedAfterATermEventCarryThatTermAndReArmUnderTheNextTerm() throws Exception {
        final StubCluster cluster = new StubCluster();
        final AssetsClusteredService service = leader(cluster);
        service.engine().deadlines().add(cluster.time + 5_000, 1, DeadlineIndex.KIND_GTD);

        term(service, 5, cluster.time);
        service.onSessionOpen(null, cluster.time); // arms the drain belt and, with a non-empty index, the sweep
        assertEquals("term 5 ids: drain then sweep", List.of(drainId(5), sweepId(5)), cluster.idsSince(0));
        assertEquals(AssetsLogApplier.TIMER_DRAIN, AssetsLogApplier.timerKind(drainId(5)));
        assertEquals(AssetsLogApplier.TIMER_SWEEP, AssetsLogApplier.timerKind(sweepId(5)));

        // A new term before the old timers fire: each firing re-arms under the NEW term's id (K3), through the
        // applier-delegating timer path. The sweep is not yet due, so the index stays non-empty and it re-arms.
        term(service, 6, cluster.time);
        final int mark = cluster.scheduledIds.size();
        service.onTimerEvent(drainId(5), cluster.time);
        assertEquals(List.of(drainId(6)), cluster.idsSince(mark));
        service.onTimerEvent(sweepId(5), cluster.time);
        assertEquals(List.of(drainId(6), sweepId(6)), cluster.idsSince(mark));
        assertEquals("the sweep fired once through the applier", 1, service.sweepTimerFires());
        assertEquals("the sweep popped nothing: not due", 1, service.engine().deadlines().size());

        // The stride keeps every realistic term inside its kind's band.
        final long farTerm = 1_000_000L;
        assertEquals(AssetsLogApplier.TIMER_DRAIN, AssetsLogApplier.timerKind(drainId(farTerm)));
        assertEquals(AssetsLogApplier.TIMER_SWEEP, AssetsLogApplier.timerKind(sweepId(farTerm)));
        assertTrue(drainId(farTerm) < AssetsLogApplier.SWEEP_TIMER_BASE);
    }

    @Test
    public void beforeAnyTermEventTheIdsAreTermZero() throws Exception {
        final StubCluster cluster = new StubCluster();
        final AssetsClusteredService service = leader(cluster);
        service.onSessionOpen(null, cluster.time);
        assertEquals(List.of(AssetsClusteredService.EGRESS_DRAIN_TIMER_ID), cluster.idsSince(0));
        assertEquals(drainId(0), AssetsClusteredService.EGRESS_DRAIN_TIMER_ID);
    }
}

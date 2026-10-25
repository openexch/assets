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
 * RC1 S2 (K3-D at the service boundary): ONE sweep timer with a constant id, armed on the leader only
 * while the deadline index is non-empty, re-armed from its own firing, idempotent when it fires twice
 * at the same time, and re-armed after a role change (the drain-timer rule: a timer does not survive
 * every recover-into-leader, so the flag is cleared and the next legal callback arms a fresh one).
 */
public class SweepTimerServiceTest {

    /** Records scheduleTimer calls; everything the sweep path does not touch throws. */
    private static final class StubCluster implements Cluster {
        Role role;
        long time = 1_000_000L;
        final List<long[]> scheduled = new ArrayList<>();

        StubCluster(final Role role) {
            this.role = role;
        }

        public Role role() { return role; }
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
            scheduled.add(new long[] {correlationId, deadline});
            return true;
        }
        public boolean cancelTimer(final long correlationId) { throw new UnsupportedOperationException(); }
        public IdleStrategy idleStrategy() { return NoOpIdleStrategy.INSTANCE; }
        public long offer(final org.agrona.DirectBuffer b, final int o, final int l) { throw new UnsupportedOperationException(); }
        public long offer(final io.aeron.DirectBufferVector[] v) { throw new UnsupportedOperationException(); }
        public long tryClaim(final int l, final io.aeron.logbuffer.BufferClaim c) { throw new UnsupportedOperationException(); }

        long sweepArms() {
            return scheduled.stream().filter(s -> s[0] == AssetsClusteredService.SWEEP_TIMER_ID).count();
        }

        long lastSweepDeadline() {
            long d = -1;
            for (final long[] s : scheduled) {
                if (s[0] == AssetsClusteredService.SWEEP_TIMER_ID) {
                    d = s[1];
                }
            }
            return d;
        }
    }

    private static void inject(final AssetsClusteredService service, final String field, final Object value)
            throws Exception {
        final Field f = AssetsClusteredService.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(service, value);
    }

    private static AssetsClusteredService leaderService(final StubCluster cluster) throws Exception {
        final AssetsClusteredService service = new AssetsClusteredService();
        inject(service, "cluster", cluster);
        inject(service, "isLeader", cluster.role == Cluster.Role.LEADER);
        inject(service, "idleStrategy", cluster.idleStrategy());
        service.engine().setEventSink(new SnapshotEnvelopeAdoptionTest.NoopSink());
        return service;
    }

    @Test
    public void anEmptyIndexArmsNothingAndANonEmptyIndexArmsExactlyOneSweep() throws Exception {
        final StubCluster cluster = new StubCluster(Cluster.Role.LEADER);
        final AssetsClusteredService service = leaderService(cluster);
        service.onSessionOpen(null, cluster.time);
        assertEquals("nothing to sweep, nothing armed", 0, cluster.sweepArms());

        service.engine().deadlines().add(cluster.time + 5_000, 1, DeadlineIndex.KIND_GTD);
        service.onSessionOpen(null, cluster.time);
        assertEquals(1, cluster.sweepArms());
        assertEquals(cluster.time + AssetsClusteredService.SWEEP_PERIOD_MS, cluster.lastSweepDeadline());
        service.onSessionOpen(null, cluster.time);
        assertEquals("re-arming an armed sweep is a no-op", 1, cluster.sweepArms());
    }

    @Test
    public void theSweepFiringPopsDueEntriesReArmsWhileNonEmptyAndIsIdempotent() throws Exception {
        final StubCluster cluster = new StubCluster(Cluster.Role.LEADER);
        final AssetsClusteredService service = leaderService(cluster);
        final DeadlineIndex idx = service.engine().deadlines();
        idx.add(cluster.time + 100, 1, DeadlineIndex.KIND_GTD);
        idx.add(cluster.time + 250, 2, DeadlineIndex.KIND_GTD);
        service.onSessionOpen(null, cluster.time);
        assertEquals(1, cluster.sweepArms());

        cluster.time += 100;
        service.onTimerEvent(AssetsClusteredService.SWEEP_TIMER_ID, cluster.time);
        assertEquals("the due entry is gone", 1, idx.size());
        assertEquals("still non-empty: re-armed from the firing", 2, cluster.sweepArms());
        assertEquals(1, service.sweepTimerFires());
        service.onTimerEvent(AssetsClusteredService.SWEEP_TIMER_ID, cluster.time);
        assertEquals("firing twice at the same time pops nothing more", 1, idx.size());

        cluster.time += 200;
        service.onTimerEvent(AssetsClusteredService.SWEEP_TIMER_ID, cluster.time);
        assertTrue(idx.isEmpty());
        final long armsWhenEmpty = cluster.sweepArms();
        service.onSessionOpen(null, cluster.time);
        assertEquals("an empty index arms nothing more", armsWhenEmpty, cluster.sweepArms());
        assertEquals(3, service.sweepTimerFires());
    }

    @Test
    public void aFollowerSweepsTheLogEntryButNeverArms() throws Exception {
        final StubCluster cluster = new StubCluster(Cluster.Role.FOLLOWER);
        final AssetsClusteredService service = leaderService(cluster);
        final DeadlineIndex idx = service.engine().deadlines();
        idx.add(cluster.time + 100, 1, DeadlineIndex.KIND_GTD);
        service.onSessionOpen(null, cluster.time);
        assertEquals(0, cluster.sweepArms());
        cluster.time += 100;
        service.onTimerEvent(AssetsClusteredService.SWEEP_TIMER_ID, cluster.time); // replicated TimerEvent
        assertTrue("the follower applies the sweep from the log", idx.isEmpty());
        assertEquals(0, cluster.sweepArms());
    }

    @Test
    public void aRoleChangeClearsTheFlagSoTheNextCallbackArmsAFreshSweep() throws Exception {
        final StubCluster cluster = new StubCluster(Cluster.Role.LEADER);
        final AssetsClusteredService service = leaderService(cluster);
        service.engine().deadlines().add(cluster.time + 5_000, 1, DeadlineIndex.KIND_GTD);
        service.onSessionOpen(null, cluster.time);
        assertEquals(1, cluster.sweepArms());
        service.onRoleChange(Cluster.Role.LEADER);
        service.onSessionOpen(null, cluster.time);
        assertEquals("recover-into-leader re-arms exactly one fresh sweep", 2, cluster.sweepArms());
    }
}

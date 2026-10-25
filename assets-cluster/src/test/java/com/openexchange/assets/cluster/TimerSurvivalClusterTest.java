// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.cluster.ClusterPorts;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * K3 fact-finding for the OMS + ledger core: do cluster timers survive a leader change, and what
 * unit and epoch is the cluster time the service sees?
 *
 * <p>The core will carry order lifecycle timers (GTD expiry, replace timeouts, requestId sweeps) as
 * {@code cluster.scheduleTimer} calls. If a timer armed on one leader died with it, every such
 * deadline would need re-arming from replicated state in {@code onRoleChange}; if it survives, the
 * timer service is already the replicated deadline index. The two readings of Aeron's contract were
 * in circulation (plan K3), so this is settled by a real three-member cluster, in process: schedule
 * a 500 ms control timer and a 4 s timer through the leader, kill the leader after the control
 * fires, and require the 4 s timer to fire exactly once on BOTH survivors (the new leader fires it
 * and appends the TimerEvent to the log; the follower sees it by replay).</p>
 *
 * <p>Also asserted: {@code cluster.timeUnit()} is milliseconds under the engine's ClusterConfig and
 * the {@code timestamp} handed to the service callbacks is epoch milliseconds, so a deadline such as
 * a GTD {@code expiresAt} can be compared to it directly.</p>
 */
public class TimerSurvivalClusterTest {

    private static final int PORT_BASE = 19800; // stride 100: members on 19800 / 19900 / 20000 (+offsets)
    private static final int MEMBERS = 3;
    private static final long CONTROL_TIMER = 1L;
    private static final long SURVIVOR_TIMER = 2L;
    private static final long CONTROL_DELAY_MS = 500;
    private static final long SURVIVOR_DELAY_MS = 4_000;

    // Tiny private wire: [int kind][long a][long b][int member].
    static final int KIND_SCHEDULE = 1;
    static final int KIND_FIRED = 3;
    static final int FRAME_LENGTH = 4 + 8 + 8 + 4;

    /** A service whose only state is the timers it was asked to arm; one instance per member. */
    static final class TimerProbeService implements ClusteredService {
        static final ConcurrentHashMap<Integer, TimerProbeService> INSTANCES = new ConcurrentHashMap<>();

        final int memberId;
        private Cluster cluster;
        volatile Cluster.Role role = Cluster.Role.FOLLOWER;
        /** cluster.timeUnit() as seen in onStart: null there (the unit arrives with the log join). */
        volatile TimeUnit timeUnitAtStart;
        /** cluster.timeUnit() as seen inside a session-message callback. */
        volatile TimeUnit timeUnit;
        /** {correlationId, timestamp, roleCodeAtFire}. */
        final List<long[]> fired = new CopyOnWriteArrayList<>();
        final List<long[]> scheduled = new CopyOnWriteArrayList<>();
        private final UnsafeBuffer out = new UnsafeBuffer(new byte[FRAME_LENGTH]);

        TimerProbeService(final int memberId) {
            this.memberId = memberId;
            INSTANCES.put(memberId, this);
        }

        public void onStart(final Cluster cluster, final Image snapshotImage) {
            this.cluster = cluster;
            this.role = cluster.role();
            this.timeUnitAtStart = cluster.timeUnit();
        }

        public void onSessionOpen(final ClientSession session, final long timestamp) { }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) { }

        public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                     final int offset, final int length, final Header header) {
            if (buffer.getInt(offset) != KIND_SCHEDULE) {
                return;
            }
            final long correlationId = buffer.getLong(offset + 4);
            final long delayMs = buffer.getLong(offset + 12);
            this.timeUnit = cluster.timeUnit();
            final long deadline = timestamp + cluster.timeUnit().convert(delayMs, TimeUnit.MILLISECONDS);
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (!cluster.scheduleTimer(correlationId, deadline)) {
                idle.idle();
            }
            scheduled.add(new long[] {correlationId, deadline, timestamp});
        }

        public void onTimerEvent(final long correlationId, final long timestamp) {
            fired.add(new long[] {correlationId, timestamp, cluster.role().code()});
            out.putInt(0, KIND_FIRED);
            out.putLong(4, correlationId);
            out.putLong(12, timestamp);
            out.putInt(20, memberId);
            for (final ClientSession session : cluster.clientSessions()) {
                for (int attempt = 0; attempt < 1_000 && session.offer(out, 0, FRAME_LENGTH) < 0; attempt++) {
                    Thread.onSpinWait();
                }
            }
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication) { }

        public void onRoleChange(final Cluster.Role newRole) {
            this.role = newRole;
        }

        public void onTerminate(final Cluster cluster) { }
    }

    private static final class Member {
        final int id;
        ClusteredMediaDriver driver;
        ClusteredServiceContainer container;
        TimerProbeService service;

        Member(final int id) {
            this.id = id;
        }

        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(driver);
        }
    }

    @Test
    public void aTimerScheduledOnTheOldLeaderFiresOnceOnTheNewLeaderAndItsFollower() throws Exception {
        // Three members on one host need the shared-host port layout; the stride is read once.
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        TimerProbeService.INSTANCES.clear();

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-timer-survival-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final List<String> hosts = List.of("localhost", "localhost", "localhost");
        final List<Member> members = new ArrayList<>();
        AeronCluster client = null;
        final List<long[]> firedSeenByClient = new CopyOnWriteArrayList<>();
        try {
            for (int i = 0; i < MEMBERS; i++) {
                members.add(boot(i, hosts, tmp));
            }
            final Member oldLeader = awaitLeader(members, null);
            System.out.println("[TEST] leader elected: member " + oldLeader.id);

            final String clientAeronDir = new File(tmp, "driver0").getAbsolutePath(); // any live driver
            client = connect(hosts, members.get(0).id == oldLeader.id && MEMBERS > 1
                    ? new File(tmp, "driver1").getAbsolutePath() : clientAeronDir, firedSeenByClient);

            final long sentMs = System.currentTimeMillis();
            schedule(client, CONTROL_TIMER, CONTROL_DELAY_MS);
            schedule(client, SURVIVOR_TIMER, SURVIVOR_DELAY_MS);

            // 1. Control timer fires on the current leader: proves the mechanism and the time unit.
            final long[] control = awaitFired(oldLeader.service, CONTROL_TIMER, sentMs + 10_000, client);
            final long nowMs = System.currentTimeMillis();
            assertEquals("cluster.timeUnit() inside a callback, under the engine's ClusterConfig",
                    TimeUnit.MILLISECONDS, oldLeader.service.timeUnit);
            System.out.println("[TEST] cluster.timeUnit() in onStart = " + oldLeader.service.timeUnitAtStart
                    + ", in onSessionMessage = " + oldLeader.service.timeUnit);
            assertTrue("timer timestamp " + control[1] + " should be epoch millis (now " + nowMs + ")",
                    Math.abs(control[1] - nowMs) < 10_000);
            assertTrue("control fired " + (control[1] - sentMs) + " ms after send, expected >= " + CONTROL_DELAY_MS,
                    control[1] - sentMs >= CONTROL_DELAY_MS - 5);
            assertEquals("control fired with the leader role", Cluster.Role.LEADER.code(), control[2]);
            final long survivorDeadline = deadlineOf(oldLeader.service, SURVIVOR_TIMER);

            // 2. Kill the leader while the 4 s timer is pending.
            final long killMs = System.currentTimeMillis();
            assertTrue("the 4 s timer must still be pending when the leader dies (" + (killMs - sentMs) + " ms)",
                    killMs - sentMs < SURVIVOR_DELAY_MS - 1_000);
            oldLeader.close();
            System.out.println("[TEST] killed leader member " + oldLeader.id + " at +" + (killMs - sentMs) + " ms");

            // 3. A new leader, and the timer fires there, once, and once on the surviving follower.
            final Member newLeader = awaitLeader(members, oldLeader);
            assertNotEquals(oldLeader.id, newLeader.id);
            System.out.println("[TEST] new leader: member " + newLeader.id + " after "
                    + (System.currentTimeMillis() - killMs) + " ms");
            final long[] survivor = awaitFired(newLeader.service, SURVIVOR_TIMER, sentMs + SURVIVOR_DELAY_MS + 30_000, client);
            assertEquals("survivor timer fired on the NEW leader with the leader role",
                    Cluster.Role.LEADER.code(), survivor[2]);
            assertTrue("fired at " + survivor[1] + " before its deadline " + survivorDeadline,
                    survivor[1] >= survivorDeadline);
            for (final Member m : members) {
                if (m.id == oldLeader.id) {
                    continue;
                }
                final long[] onMember = awaitFired(m.service, SURVIVOR_TIMER, System.currentTimeMillis() + 10_000, client);
                assertEquals("member " + m.id + " must see the survivor timer exactly once (TimerEvent is a log entry)",
                        1, countFired(m.service, SURVIVOR_TIMER));
                assertEquals("same TimerEvent timestamp on every survivor", survivor[1], onMember[1]);
            }
            assertEquals("the dead leader never fired it", 0, countFired(oldLeader.service, SURVIVOR_TIMER));
            System.out.println("[TEST] timer " + SURVIVOR_TIMER + " scheduled on member " + oldLeader.id
                    + " fired on member " + newLeader.id + " at +" + (survivor[1] - sentMs)
                    + " ms (deadline +" + (survivorDeadline - sentMs) + " ms); seen once by every survivor; "
                    + "timeUnit=" + oldLeader.service.timeUnit + ", timestamps are epoch ms");
        } finally {
            CloseHelper.quietClose(client);
            for (final Member m : members) {
                m.close();
            }
            IoUtil.delete(tmp, true);
            System.clearProperty("cluster.port.stride");
            ClusterPorts.resetStrideForTest();
        }
    }

    private static Member boot(final int id, final List<String> hosts, final File tmp) {
        final Member m = new Member(id);
        m.service = new TimerProbeService(id);
        final ClusterConfig cfg = ClusterConfig.create(id, hosts, hosts, PORT_BASE, m.service);
        cfg.baseDir(new File(tmp, "node" + id));
        cfg.aeronDirectoryName(new File(tmp, "driver" + id).getAbsolutePath());
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        m.driver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                cfg.archiveContext(), cfg.consensusModuleContext());
        m.container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());
        return m;
    }

    private static Member awaitLeader(final List<Member> members, final Member except) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (final Member m : members) {
                if ((except == null || m.id != except.id) && m.service.role == Cluster.Role.LEADER) {
                    return m;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no leader" + (except == null ? "" : " other than member " + except.id) + " within 30s");
    }

    private static AeronCluster connect(final List<String> hosts, final String aeronDir, final List<long[]> firedSeen)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .egressListener((sessionId, timestamp, buffer, offset, length, header) -> {
                            if (length >= FRAME_LENGTH && buffer.getInt(offset) == KIND_FIRED) {
                                firedSeen.add(new long[] {buffer.getLong(offset + 4), buffer.getLong(offset + 12),
                                        buffer.getInt(offset + 20)});
                            }
                        })
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp?term-length=64k")
                        .aeronDirectoryName(aeronDir)
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
            } catch (final RuntimeException e) {
                last = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect a cluster client", last);
    }

    private static void schedule(final AeronCluster client, final long correlationId, final long delayMs) {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[FRAME_LENGTH]);
        frame.putInt(0, KIND_SCHEDULE);
        frame.putLong(4, correlationId);
        frame.putLong(12, delayMs);
        frame.putInt(20, -1);
        final long deadline = System.currentTimeMillis() + 10_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            if (client.offer(frame, 0, FRAME_LENGTH) > 0) {
                return;
            }
            client.pollEgress();
            idle.idle();
        }
        throw new IllegalStateException("failed to offer schedule within timeout");
    }

    private static long[] awaitFired(final TimerProbeService service, final long correlationId, final long deadlineMs,
                                     final AeronCluster client) throws InterruptedException {
        while (System.currentTimeMillis() < deadlineMs) {
            for (final long[] f : service.fired) {
                if (f[0] == correlationId) {
                    return f;
                }
            }
            try {
                client.pollEgress(); // keeps the session serviced; the client may be mid-reconnect
            } catch (final RuntimeException ignored) {
                // leader gone; the client re-resolves on its own
            }
            Thread.sleep(5);
        }
        fail("timer " + correlationId + " never fired on member " + service.memberId + " (fired so far: "
                + describe(service.fired) + ")");
        return null;
    }

    private static long deadlineOf(final TimerProbeService service, final long correlationId) {
        for (final long[] s : service.scheduled) {
            if (s[0] == correlationId) {
                return s[1];
            }
        }
        throw new AssertionError("timer " + correlationId + " was never scheduled on member " + service.memberId);
    }

    private static int countFired(final TimerProbeService service, final long correlationId) {
        int n = 0;
        for (final long[] f : service.fired) {
            if (f[0] == correlationId) {
                n++;
            }
        }
        return n;
    }

    private static String describe(final List<long[]> fired) {
        final StringBuilder sb = new StringBuilder("[");
        for (final long[] f : fired) {
            sb.append('(').append(f[0]).append('@').append(f[1]).append(",role=").append(f[2]).append(") ");
        }
        return sb.append(']').toString();
    }
}

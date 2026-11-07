// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.cluster.ClusterMemberPositions;
import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.ReaderRegistry;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.RecordingLog;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * R03-A01/A03/A04: the retention contract runs on a real cluster, and every input to the purge
 * boundary reaches the service through the replicated log.
 *
 * <p>Three in-process members run a probe service around {@link ReaderRegistry}. A reader (the
 * bridge of the contract) registers and checkpoints through its own cluster-client session. The
 * leader's "housekeeping thread" (the test thread here, deliberately NOT a cluster callback) asks the
 * consensus module for member positions with {@link ClusterMemberPositions#query} and commits what it
 * saw through a client session of its own. The service decides only at committed DECIDE messages,
 * with the log timestamp as its clock and the log position as the head. Asserted: identical decision
 * sequences on all three members; cold start holds at 0; the boundary advances only after the
 * observation is committed; an unregistered reader changes nothing; a quiet registered reader HOLDs;
 * an operator STRAND releases it; a member restarted from a snapshot reproduces the same decisions.</p>
 */
public class RetentionAuthorityClusterTest {

    private static final int PORT_BASE = 20600; // stride 100: 20600 / 20700 / 20800
    private static final int MEMBERS = 3;
    private static final long EPOCH = 4242L;
    private static final long LEASE_MS = 1_500L;

    // ---- private probe wire ----
    static final int K_REGISTER = 1, K_CHECKPOINT = 2, K_OBSERVE = 3, K_STRAND = 4, K_DECIDE = 5, K_UNREGISTER = 6;
    static final int E_DECISION = 10, E_REJECT = 11, E_ACK = 12;

    /** One member's view: every decision it took, in log order. {logPosition, boundary, holds, stranded, gate}. Reject counts are node-local diagnostics, deliberately not part of a decision. */
    static final class RetentionProbeService implements ClusteredService {
        final int memberId;
        final ReaderRegistry registry = new ReaderRegistry(EPOCH, new int[] {0, 1, 2});
        final List<long[]> decisions = new CopyOnWriteArrayList<>();
        volatile long rejects;
        volatile Cluster.Role role = Cluster.Role.FOLLOWER;
        private Cluster cluster;
        private final UnsafeBuffer out = new UnsafeBuffer(new byte[64]);

        RetentionProbeService(final int memberId) {
            this.memberId = memberId;
            registry.configure(1L << 30, 60_000L, 1L << 30, false);
        }

        public void onStart(final Cluster cluster, final Image snapshotImage) {
            this.cluster = cluster;
            this.role = cluster.role();
            if (snapshotImage != null) {
                final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
                final int[] len = {0};
                final io.aeron.FragmentAssembler assembler = new io.aeron.FragmentAssembler((b, o, l, h) -> {
                    buf.putBytes(len[0], b, o, l);
                    len[0] += l;
                });
                final BackoffIdleStrategy idle = new BackoffIdleStrategy();
                while (!snapshotImage.isEndOfStream()) {
                    idle.idle(snapshotImage.poll(assembler, 16));
                }
                while (snapshotImage.poll(assembler, 16) > 0) { }
                if (len[0] > 0) {
                    registry.deserialize(buf, 0);
                }
            }
        }

        public void onSessionOpen(final ClientSession session, final long timestamp) { }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) { }

        public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer b,
                                     final int o, final int length, final Header header) {
            final int kind = b.getInt(o);
            final long head = cluster.logPosition();
            ReaderRegistry.Reject reject = ReaderRegistry.Reject.NONE;
            switch (kind) {
                case K_REGISTER -> reject = registry.register("r" + b.getLong(o + 4),
                        ReaderRegistry.Kind.values()[b.getInt(o + 12)], b.getLong(o + 16), b.getLong(o + 24), timestamp);
                case K_CHECKPOINT -> reject = registry.checkpoint("r" + b.getLong(o + 4), b.getLong(o + 12),
                        b.getLong(o + 20), b.getLong(o + 28), b.getLong(o + 36), head, timestamp);
                case K_OBSERVE -> {
                    final int n = b.getInt(o + 4);
                    final int[] ids = new int[n];
                    final long[] pos = new long[n];
                    final boolean[] fresh = new boolean[n];
                    int p = o + 8;
                    for (int i = 0; i < n; i++) {
                        ids[i] = b.getInt(p); pos[i] = b.getLong(p + 4); fresh[i] = b.getByte(p + 12) != 0; p += 13;
                    }
                    registry.observeMembers(timestamp, ids, pos, fresh);
                }
                case K_STRAND -> reject = registry.strandReader("r" + b.getLong(o + 4), "operator", timestamp);
                case K_UNREGISTER -> registry.unregister("r" + b.getLong(o + 4));
                case K_DECIDE -> {
                    final ReaderRegistry.Decision d = registry.decide(head, head, timestamp);
                    decisions.add(new long[] {head, d.boundary, d.holds.size(), d.stranded.size(),
                            d.gate == ReaderRegistry.Gate.OPEN ? 0 : 1});
                    out.putInt(0, E_DECISION); out.putLong(4, head); out.putLong(12, d.boundary);
                    out.putInt(20, d.holds.size()); out.putInt(24, d.stranded.size());
                    for (final ClientSession s : cluster.clientSessions()) {
                        for (int attempt = 0; attempt < 1_000 && s.offer(out, 0, 28) < 0; attempt++) Thread.onSpinWait();
                    }
                }
                default -> { }
            }
            if (reject != ReaderRegistry.Reject.NONE) {
                rejects++;
                out.putInt(0, E_REJECT); out.putInt(4, reject.ordinal());
                for (int attempt = 0; attempt < 1_000 && session.offer(out, 0, 8) < 0; attempt++) Thread.onSpinWait();
            } else if (kind != K_DECIDE) {
                // Applied (committed and processed): lets a test order its own sends across sessions.
                out.putInt(0, E_ACK); out.putInt(4, kind);
                for (int attempt = 0; attempt < 1_000 && session.offer(out, 0, 8) < 0; attempt++) Thread.onSpinWait();
            }
        }

        public void onTimerEvent(final long correlationId, final long timestamp) { }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
            final int len = registry.serialize(buf, 0);
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (snapshotPublication.offer(buf, 0, len) < 0) idle.idle();
        }

        public void onRoleChange(final Cluster.Role newRole) { this.role = newRole; }

        public void onTerminate(final Cluster cluster) { }
    }

    private static final class Member {
        final int id;
        RetentionProbeService service;
        ClusterConfig cfg;
        ClusteredMediaDriver driver;
        ClusteredServiceContainer container;

        Member(final int id) { this.id = id; }

        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(driver);
        }
    }

    /** Egress seen by one client: decisions {logPosition, boundary, holds, stranded} and reject codes. */
    static final class Seen implements io.aeron.cluster.client.EgressListener {
        final List<long[]> decisions = new CopyOnWriteArrayList<>();
        final List<Integer> rejects = new CopyOnWriteArrayList<>();
        final List<Integer> acks = new CopyOnWriteArrayList<>();

        public void onMessage(final long sessionId, final long timestamp, final DirectBuffer b, final int o, final int length, final Header h) {
            if (length < 8) return;
            if (b.getInt(o) == E_DECISION) {
                decisions.add(new long[] {b.getLong(o + 4), b.getLong(o + 12), b.getInt(o + 20), b.getInt(o + 24)});
            } else if (b.getInt(o) == E_REJECT) {
                rejects.add(b.getInt(o + 4));
            } else if (b.getInt(o) == E_ACK) {
                acks.add(b.getInt(o + 4));
            }
        }
    }

    private final UnsafeBuffer frame = new UnsafeBuffer(new byte[256]);

    @Test
    public void everyRetentionInputReachesTheBoundaryThroughTheCommittedLog() throws Exception {
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-retention-authority-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final List<String> hosts = List.of("localhost", "localhost", "localhost");
        final List<Member> members = new ArrayList<>();
        AeronCluster reader = null, observer = null;
        ClusterMemberPositions positions = null;
        try {
            for (int i = 0; i < MEMBERS; i++) members.add(boot(i, hosts, tmp));
            final Member leader = awaitLeader(members);
            assertEquals(0, leader.id);
            // Clients attach to member 2's driver: member 1 is the one restarted below, and an Aeron client
            // whose driver disappears exits the JVM ("Media Driver timeout ... exiting client").
            final String clientDir = new File(tmp, "driver2").getAbsolutePath();
            final Seen readerSeen = new Seen(), observerSeen = new Seen();
            reader = connect(hosts, clientDir, readerSeen);
            observer = connect(hosts, clientDir, observerSeen);

            // 1. Cold: nothing committed about anyone -> boundary 0 on every member.
            decide(observer); long[] d = awaitDecision(observer, observerSeen, 1);
            assertEquals("cold leader holds at 0", 0L, d[1]);

            // 2. The leader's housekeeping thread (this thread, outside any callback) asks the consensus
            //    module and COMMITS what it saw through its own session. Only then does the boundary move.
            positions = ClusterMemberPositions.from(
                    leader.cfg.clusteredServiceContext().aeron(), leader.cfg.clusteredServiceContext());
            ClusterMemberPositions.Snapshot snap = null;
            for (int i = 0; i < 20 && snap == null; i++) snap = positions.query(1_000);
            assertNotNull("consensus module answered the member query", snap);
            assertEquals(3, snap.members.size());
            long minObserved = Long.MAX_VALUE;
            for (final ClusterMemberPositions.Member m : snap.members) {
                minObserved = Math.min(minObserved, m.logPosition);
                System.out.printf("[TEST] observed member %d logPosition=%d lastAppendAgeMs=%d%n", m.memberId, m.logPosition,
                        m.timeOfLastAppendNs <= 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(snap.currentTimeNs - m.timeOfLastAppendNs));
            }
            assertTrue("member positions are real log positions", minObserved > 0);
            observe(observer, snap);
            decide(observer); d = awaitDecision(observer, observerSeen, 2);
            assertTrue("boundary advanced to a committed member floor: " + d[1] + " vs min " + minObserved,
                    d[1] > 0 && d[1] <= minObserved);
            final long membersFloor = d[1];

            // 3. An unregistered reader: rejected, boundary unchanged. A registered one holds at its checkpoint
            //    once the members have moved past it (the boundary is monotone, so the reader has to be in
            //    place BEFORE the member floor passes its position; a late reader reseeds, it cannot pull back).
            checkpoint(reader, 99L, 1_000L, 1_000L);
            awaitRejects(reader, readerSeen, 1);
            assertEquals(ReaderRegistry.Reject.UNKNOWN_READER.ordinal(), (int) readerSeen.rejects.get(0));
            decide(observer); d = awaitDecision(observer, observerSeen, 3);
            assertEquals(membersFloor, d[1]);
            // Above the current boundary (monotone: a reader below it is late and must reseed), below where
            // the members will be observed next, so the reader becomes the limiter.
            final long readerPos = membersFloor + 88;
            register(reader, 7L, ReaderRegistry.Kind.BRIDGE, EPOCH, LEASE_MS);
            checkpoint(reader, 7L, 0L, readerPos);
            awaitAcks(reader, readerSeen, 2); // the two sessions are independent: wait until both are applied
            observe(observer, positions.query(1_000));
            decide(observer); d = awaitDecision(observer, observerSeen, 4);
            assertEquals("a registered reader's checkpoint is the floor", readerPos, d[1]);
            // Wrong epoch and a future position are refused.
            checkpointEpoch(reader, 7L, EPOCH + 1, 0L, readerPos + 10);
            checkpoint(reader, 7L, 0L, Long.MAX_VALUE / 4);
            awaitRejects(reader, readerSeen, 3);
            assertEquals(ReaderRegistry.Reject.WRONG_EPOCH.ordinal(), (int) readerSeen.rejects.get(1));
            assertEquals(ReaderRegistry.Reject.FUTURE_POSITION.ordinal(), (int) readerSeen.rejects.get(2));

            // 4. The reader goes quiet past its lease: HOLD, still holding, reported.
            Thread.sleep(LEASE_MS + 500);
            observe(observer, positions.query(1_000));
            decide(observer); d = awaitDecision(observer, observerSeen, 5);
            assertEquals(readerPos, d[1]);
            assertEquals("one reader in HOLD", 1L, d[2]);

            // 5. Operator strands it: boundary moves to the member floor, the reader is reported stranded.
            observe(observer, positions.query(1_000)); // refresh members so the floor is current
            strand(observer, 7L);
            decide(observer); d = awaitDecision(observer, observerSeen, 6);
            assertTrue("boundary released past the stranded reader: " + d[1], d[1] > readerPos);
            assertEquals(0L, d[2]);
            assertEquals(1L, d[3]);

            // 6. Every member took the same decisions in the same order.
            final List<long[]> ref = members.get(0).service.decisions;
            assertEquals(6, ref.size());
            for (final Member m : members) {
                awaitDecisions(m, 6);
                for (int i = 0; i < 6; i++) {
                    assertArrayEquals("member " + m.id + " decision " + i, ref.get(i), m.service.decisions.get(i));
                }
            }

            // 7. Snapshot, restart a follower from it, decide again: same answer as the others.
            final AtomicCounter toggle = ClusterControl.findControlToggle(
                    leader.cfg.consensusModuleContext().aeron().countersReader(), leader.cfg.consensusModuleContext().clusterId());
            assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
            final long snapDeadline = System.currentTimeMillis() + 20_000;
            while (!hasSnapshot(members.get(1)) && System.currentTimeMillis() < snapDeadline) { observer.pollEgress(); Thread.sleep(50); }
            assertTrue("follower 1 has a snapshot", hasSnapshot(members.get(1)));
            members.get(1).close();
            final List<long[]> beforeRestart = new ArrayList<>(members.get(1).service.decisions);
            members.set(1, boot(1, hosts, tmp));
            decide(observer); d = awaitDecision(observer, observerSeen, 7);
            awaitDecisions(members.get(1), 1);
            final long[] restarted = members.get(1).service.decisions.get(members.get(1).service.decisions.size() - 1);
            final long[] leaderLast = ref.get(ref.size() - 1);
            assertArrayEquals("restarted follower's decision equals the leader's", leaderLast, restarted);
            System.out.printf("[TEST] decisions=%d boundary cold=0 -> members=%d -> reader=members+88 (HOLD) -> stranded -> %d; rejects=%s; restarted follower had %d decisions before, agrees after%n",
                    ref.size(), membersFloor, d[1], readerSeen.rejects, beforeRestart.size());
        } catch (final Throwable t) {
            // Print BEFORE teardown: an Aeron client whose driver disappears during cleanup can take the
            // JVM down ("Media Driver timeout ... exiting client"), and the assertion would be lost.
            System.out.println("[TEST] FAILED: " + t);
            t.printStackTrace(System.out);
            try (java.io.PrintStream ps = new java.io.PrintStream(new java.io.FileOutputStream(
                    new File(System.getProperty("java.io.tmpdir"), "ae-retention-authority-FAIL.txt")))) {
                t.printStackTrace(ps); // survives a JVM exit during teardown
            }
            throw t;
        } finally {
            CloseHelper.quietClose(positions);
            CloseHelper.quietClose(reader);
            CloseHelper.quietClose(observer);
            // Containers first on every member, then the drivers: a container's own Aeron client must
            // be gone before its driver is, or its conductor exits the JVM on the driver timeout.
            for (final Member m : members) CloseHelper.quietClose(m.container);
            Thread.sleep(300);
            for (final Member m : members) CloseHelper.quietClose(m.driver);
            IoUtil.delete(tmp, true);
            System.clearProperty("cluster.port.stride");
            ClusterPorts.resetStrideForTest();
        }
    }

    // ---- wire helpers (all go through a cluster-client session = the replicated log) ----

    private void send(final AeronCluster client, final int length) {
        final long deadline = System.currentTimeMillis() + 10_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (client.offer(frame, 0, length) <= 0) {
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException("offer timeout");
            client.pollEgress(); idle.idle();
        }
    }

    private void register(final AeronCluster c, final long key, final ReaderRegistry.Kind kind, final long epoch, final long leaseMs) {
        frame.putInt(0, K_REGISTER); frame.putLong(4, key); frame.putInt(12, kind.ordinal()); frame.putLong(16, epoch); frame.putLong(24, leaseMs);
        send(c, 32);
    }

    private void checkpoint(final AeronCluster c, final long key, final long recordingId, final long position) {
        checkpointEpoch(c, key, EPOCH, recordingId, position);
    }

    private void checkpointEpoch(final AeronCluster c, final long key, final long epoch, final long recordingId, final long position) {
        frame.putInt(0, K_CHECKPOINT); frame.putLong(4, key); frame.putLong(12, epoch); frame.putLong(20, recordingId); frame.putLong(28, position); frame.putLong(36, 0L);
        send(c, 44);
    }

    private void observe(final AeronCluster c, final ClusterMemberPositions.Snapshot snap) {
        frame.putInt(0, K_OBSERVE); frame.putInt(4, snap.members.size());
        int p = 8;
        for (final ClusterMemberPositions.Member m : snap.members) {
            final boolean fresh = m.timeOfLastAppendNs > 0 && (snap.currentTimeNs - m.timeOfLastAppendNs) < TimeUnit.SECONDS.toNanos(5);
            frame.putInt(p, m.memberId); frame.putLong(p + 4, m.logPosition); frame.putByte(p + 12, (byte) (fresh ? 1 : 0)); p += 13;
        }
        send(c, p);
    }

    private void strand(final AeronCluster c, final long key) { frame.putInt(0, K_STRAND); frame.putLong(4, key); send(c, 12); }

    private void decide(final AeronCluster c) { frame.putInt(0, K_DECIDE); send(c, 4); }

    private static long[] awaitDecision(final AeronCluster c, final Seen seen, final int count) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 15_000;
        while (seen.decisions.size() < count) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("no decision #" + count);
            if (c.pollEgress() == 0) Thread.sleep(1);
        }
        return seen.decisions.get(count - 1);
    }

    private static void awaitAcks(final AeronCluster c, final Seen seen, final int count) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 15_000;
        while (seen.acks.size() < count) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("no ack #" + count + " (have " + seen.acks + ")");
            if (c.pollEgress() == 0) Thread.sleep(1);
        }
    }

    private static void awaitRejects(final AeronCluster c, final Seen seen, final int count) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 15_000;
        while (seen.rejects.size() < count) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("no reject #" + count + " (have " + seen.rejects + ")");
            if (c.pollEgress() == 0) Thread.sleep(1);
        }
    }

    private static void awaitDecisions(final Member m, final int count) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (m.service.decisions.size() < count) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("member " + m.id + " has " + m.service.decisions.size() + " decisions");
            Thread.sleep(10);
        }
    }

    private static boolean hasSnapshot(final Member m) {
        try (RecordingLog log = new RecordingLog(m.cfg.consensusModuleContext().clusterDir(), false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == 0) return true;
            }
        }
        return false;
    }

    private static Member boot(final int id, final List<String> hosts, final File tmp) {
        final Member m = new Member(id);
        m.service = new RetentionProbeService(id);
        TestAuth.install();
        m.cfg = ClusterConfig.create(id, hosts, hosts, PORT_BASE, m.service);
        m.cfg.baseDir(new File(tmp, "node" + id));
        m.cfg.aeronDirectoryName(new File(tmp, "driver" + id).getAbsolutePath());
        m.cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        m.cfg.errorHandler(Throwable::printStackTrace);
        m.cfg.consensusModuleContext()
                .appointedLeaderId(0)
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        m.driver = ClusteredMediaDriver.launch(
                m.cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                m.cfg.archiveContext(), m.cfg.consensusModuleContext());
        m.container = ClusteredServiceContainer.launch(m.cfg.clusteredServiceContext());
        return m;
    }

    private static Member awaitLeader(final List<Member> members) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (final Member m : members) if (m.service.role == Cluster.Role.LEADER) return m;
            Thread.sleep(50);
        }
        throw new AssertionError("no leader within 30s");
    }

    private static AeronCluster connect(final List<String> hosts, final String aeronDir, final Seen seen) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                    .credentialsSupplier(TestAuth.credentials(SessionClass.TOOL))
                        .egressListener(seen)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp?term-length=64k")
                        .aeronDirectoryName(aeronDir)
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
            } catch (final RuntimeException e) {
                last = e; Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect", last);
    }
}

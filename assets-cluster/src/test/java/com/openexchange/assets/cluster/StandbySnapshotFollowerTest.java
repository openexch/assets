// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.cluster.ClusterPorts;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * K7 fact-finding: how much of the (premium) standby-snapshot flow does OPEN-SOURCE Aeron 1.53 run
 * when the "standby" is simply a follower member whose service container has
 * {@code standbySnapshotEnabled(true)}?
 *
 * <p>Bytecode facts this test is built on: the leader's {@code STANDBY_SNAPSHOT} control toggle
 * appends a SNAPSHOT action with flags=1 and does NOT move the consensus module into the SNAPSHOT
 * state; {@code ConsensusModuleAgent.onReplayClusterAction} only enters SNAPSHOT for flags=0;
 * {@code ClusteredServiceAgent.shouldSnapshot(flags)} is true for flags=0 or when
 * {@code standbySnapshotEnabled}. So a flagged action should make ONLY the enabled containers
 * snapshot, with no leader pause. Measured here: leader ack RTT across the toggle, which members'
 * services snapshotted, consensus-module errors, recording-log entries and archive recordings.</p>
 *
 * <p><b>Result (2026-10-25, Aeron 1.53.0): the flag reaches nobody.</b> {@code standbySnapshotFlags} in
 * the service agent is set from {@code ActiveLogEvent.isStandby}, which comes from the consensus
 * module's {@code JoinLog} message; an open-source consensus module always joins its services with
 * {@code isStandby=false}, so {@code standbySnapshotEnabled(true)} on a member's container changes
 * nothing. The leader does not pause (ack RTT unchanged), no member snapshots, no errors, no
 * recording-log entry, and a normal snapshot afterwards works. The producing side of standby snapshots
 * is the premium {@code ClusterStandby}; this test pins the open-source behaviour so the K7 design
 * does not assume otherwise.</p>
 */
public class StandbySnapshotFollowerTest {

    private static final int PORT_BASE = 20300; // stride 100: 20300 / 20400 / 20500
    private static final int MEMBERS = 3;
    private static final long USER = 9191L;

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final MessageHeaderDecoder headerDec = new MessageHeaderDecoder();
    private final DepositAckDecoder ackDec = new DepositAckDecoder();
    private final Long2LongHashMap sentNs = new Long2LongHashMap(-1L);
    private long acks;
    private long maxRttNs;
    private long worstCorr;

    private static final class Member {
        final int id;
        final AssetsClusteredService service = new AssetsClusteredService();
        ClusterConfig cfg;
        ClusteredMediaDriver driver;
        ClusteredServiceContainer container;
        final List<String> errors = new CopyOnWriteArrayList<>();

        Member(final int id) { this.id = id; }

        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(driver);
        }
    }

    @Test
    public void followersSnapshotOnTheStandbyFlagWhileTheLeaderKeepsServingIngress() throws Exception {
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        System.setProperty("transport.snapshot.term.length", "64k"); // chunked path on the followers too
        System.setProperty("snapshot.interval.minutes", "0");

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-standby-follower-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final List<String> hosts = List.of("localhost", "localhost", "localhost");
        final List<Member> members = new ArrayList<>();
        AeronCluster client = null;
        try {
            for (int i = 0; i < MEMBERS; i++) {
                members.add(boot(i, hosts, tmp));
            }
            final Member leader = awaitLeader(members);
            assertEquals("appointed leader", 0, leader.id);
            System.out.println("[TEST] leader member " + leader.id + "; followers have standbySnapshotEnabled=true");
            client = connect(hosts, new File(tmp, "driver1").getAbsolutePath());

            // Phase 1: steady deposits, baseline RTT.
            final long t0 = System.nanoTime();
            long corr = 1;
            corr = depositsFor(client, corr, 3_000);
            final long baselineMax = maxRttNs;
            System.out.printf("[TEST] baseline: %d acks, max RTT %.1f ms%n", acks, baselineMax / 1e6);

            // Phase 2: STANDBY_SNAPSHOT on the LEADER's cluster control toggle, deposits keep flowing.
            final AtomicCounter toggle = ClusterControl.findControlToggle(
                    leader.cfg.consensusModuleContext().aeron().countersReader(),
                    leader.cfg.consensusModuleContext().clusterId());
            assertNotNull("leader control toggle counter", toggle);
            maxRttNs = 0;
            final boolean toggled = ClusterControl.ToggleState.STANDBY_SNAPSHOT.toggle(toggle);
            assertTrue("toggle accepted", toggled);
            final long toggleMs = System.currentTimeMillis();
            corr = depositsFor(client, corr, 4_000);
            final long duringMax = maxRttNs;
            System.out.printf("[TEST] during/after standby toggle: max RTT %.1f ms (worst corr %d)%n", duringMax / 1e6, worstCorr);

            // Give the followers' service snapshots a moment to land, then look at every member.
            final long deadline = System.currentTimeMillis() + 3_000; // a snapshot would land within a second
            while (System.currentTimeMillis() < deadline
                    && (members.get(1).service.nodeMetrics().lastSnapshotBytes() < 0
                    || members.get(2).service.nodeMetrics().lastSnapshotBytes() < 0)) {
                client.pollEgress();
                Thread.sleep(20);
            }
            for (final Member m : members) {
                final long bytes = m.service.nodeMetrics().lastSnapshotBytes();
                final long snapMs = m.service.nodeMetrics().lastSnapshotMs();
                int recordingLogSnapshots = 0, standbyEntries = 0;
                try (RecordingLog log = new RecordingLog(m.cfg.consensusModuleContext().clusterDir(), false)) {
                    for (final RecordingLog.Entry e : log.entries()) {
                        if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT) recordingLogSnapshots++;
                        if (e.type == RecordingLog.ENTRY_TYPE_STANDBY_SNAPSHOT) standbyEntries++;
                    }
                }
                int recordings = 0;
                try (AeronArchive archive = AeronArchive.connect(m.cfg.consensusModuleContext().archiveContext().clone())) {
                    recordings = archive.listRecordings(0, 100, (controlSessionId, correlationId, recordingId, startTimestamp,
                            stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                            mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> { });
                }
                System.out.printf("[TEST] member %d role=%s serviceSnapshotBytes=%d (at %s) recordingLog: snapshots=%d standby=%d archiveRecordings=%d errors=%d%n",
                        m.id, m.service.engine() == null ? "?" : roleOf(m), bytes,
                        snapMs == 0 ? "never" : ("+" + (snapMs - toggleMs) + " ms after toggle"), recordingLogSnapshots, standbyEntries,
                        recordings, m.errors.size());
                for (final String err : m.errors) {
                    System.out.println("[TEST]   error: " + err);
                }
            }
            // Open-source behaviour, pinned: the standby-flagged action is a no-op for every member's service.
            for (final Member m : members) {
                assertEquals("member " + m.id + " service must not snapshot on a standby-flagged action (open-source CM joins with isStandby=false)",
                        -1L, m.service.nodeMetrics().lastSnapshotBytes());
                assertEquals("member " + m.id + " consensus-module errors", 0, m.errors.size());
            }
            assertTrue("leader kept serving ingress across the toggle (max RTT " + duringMax / 1e6 + " ms)",
                    duringMax < TimeUnit.MILLISECONDS.toNanos(100));

            // Phase 3: a NORMAL snapshot still works on all three (the stray standby acks did not wedge anything),
            // and the cluster keeps serving.
            maxRttNs = 0;
            assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
            corr = depositsFor(client, corr, 3_000);
            final long normalMax = maxRttNs;
            final long normalDeadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < normalDeadline && !hasRecordingLogSnapshot(members.get(0))) {
                client.pollEgress();
                Thread.sleep(50);
            }
            assertTrue("normal snapshot landed in the leader's recording log", hasRecordingLogSnapshot(members.get(0)));
            System.out.printf("[TEST] normal snapshot after standby: leader snapshotted %d bytes; max RTT around it %.1f ms; total acks %d, errors %d/%d/%d%n",
                    members.get(0).service.nodeMetrics().lastSnapshotBytes(), normalMax / 1e6, acks,
                    members.get(0).errors.size(), members.get(1).errors.size(), members.get(2).errors.size());
            System.out.printf("[TEST] SUMMARY baselineMaxRtt=%.1fms standbyToggleMaxRtt=%.1fms normalSnapshotMaxRtt=%.1fms%n",
                    baselineMax / 1e6, duringMax / 1e6, normalMax / 1e6);
        } finally {
            CloseHelper.quietClose(client);
            for (final Member m : members) m.close();
            IoUtil.delete(tmp, true);
            System.clearProperty("cluster.port.stride");
            System.clearProperty("transport.snapshot.term.length");
            System.clearProperty("snapshot.interval.minutes");
            ClusterPorts.resetStrideForTest();
        }
    }

    private static String roleOf(final Member m) {
        return m.service.nodeMetrics().role() == Cluster.Role.LEADER.code() ? "LEADER" : "FOLLOWER";
    }

    private static boolean hasRecordingLogSnapshot(final Member m) {
        try (RecordingLog log = new RecordingLog(m.cfg.consensusModuleContext().clusterDir(), false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == 0) return true;
            }
        }
        return false;
    }

    private Member boot(final int id, final List<String> hosts, final File tmp) {
        final Member m = new Member(id);
        m.cfg = ClusterConfig.create(id, hosts, hosts, PORT_BASE, m.service);
        m.cfg.baseDir(new File(tmp, "node" + id));
        m.cfg.aeronDirectoryName(new File(tmp, "driver" + id).getAbsolutePath());
        m.cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        m.cfg.errorHandler(t -> { m.errors.add(String.valueOf(t)); });
        m.cfg.consensusModuleContext()
                .appointedLeaderId(0)
                .acceptStandbySnapshots(true)
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        // The experiment: followers' service containers answer standby-flagged snapshot actions.
        m.cfg.clusteredServiceContext().standbySnapshotEnabled(id != 0);
        m.driver = ClusteredMediaDriver.launch(
                m.cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                m.cfg.archiveContext(), m.cfg.consensusModuleContext());
        m.container = ClusteredServiceContainer.launch(m.cfg.clusteredServiceContext());
        return m;
    }

    private static Member awaitLeader(final List<Member> members) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (final Member m : members) {
                if (m.service.nodeMetrics().role() == Cluster.Role.LEADER.code()) return m;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no leader within 30s");
    }

    private AeronCluster connect(final List<String> hosts, final String aeronDir) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .egressListener((sessionId, timestamp, buffer, offset, length, header) -> {
                            if (length < MessageHeaderDecoder.ENCODED_LENGTH) return;
                            headerDec.wrap(buffer, offset);
                            if (headerDec.templateId() != DepositAckDecoder.TEMPLATE_ID) return;
                            ackDec.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDec.blockLength(), headerDec.version());
                            final long sent = sentNs.remove(ackDec.correlationId());
                            if (sent != -1L) {
                                acks++;
                                final long rtt = System.nanoTime() - sent;
                                if (rtt > maxRttNs) { maxRttNs = rtt; worstCorr = ackDec.correlationId(); }
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
        throw new IllegalStateException("could not connect", last);
    }

    /** Send {@code n} deposits at ~2k/s with correlation ids, polling egress; returns the next correlationId. */
    private long depositsFor(final AeronCluster client, long corr, final int n) throws InterruptedException {
        final long start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            final long due = start + i * 500_000L;
            while (System.nanoTime() < due) {
                client.pollEgress();
                Thread.onSpinWait();
            }
            depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                    .correlationId(corr).userId(USER).assetId(Asset.USD.id()).amount(FixedPoint.fromDouble(1.0));
            final int length = MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();
            final long deadline = System.currentTimeMillis() + 10_000;
            while (client.offer(ingress, 0, length) <= 0) {
                if (System.currentTimeMillis() > deadline) throw new IllegalStateException("offer timeout");
                client.pollEgress();
                Thread.onSpinWait();
            }
            sentNs.put(corr, System.nanoTime());
            corr++;
        }
        final long drainDeadline = System.currentTimeMillis() + 10_000;
        while (!sentNs.isEmpty() && System.currentTimeMillis() < drainDeadline) {
            if (client.pollEgress() == 0) Thread.sleep(1);
        }
        return corr;
    }
}

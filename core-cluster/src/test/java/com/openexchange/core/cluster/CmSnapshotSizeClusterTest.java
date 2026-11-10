// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.CoreRestoreRig.Listener;
import com.openexchange.core.cluster.CoreRestoreRig.Session;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import com.openexchange.core.infrastructure.persistence.SnapshotModeSettings;
import com.openexchange.cluster.ClusterPorts;
import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.TimerDecoder;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * In stub mode the consensus module's snapshot does not grow with the order count. N GTD
 * orders rest on a single member (default 100k; {@code -Dsnapshot.nightly=true} runs 1M), a normal snapshot is taken, the CM
 * entry of the newest group is located through the {@code RecordingLog} read side ({@code serviceId ==
 * ConsensusModule.Configuration.SERVICE_ID}), its recorded length must stay below {@code CONSENSUS_SNAPSHOT_MAX_BYTES}
 * (3.5, read from the member's settings) and, decoded with the public cluster codecs, it holds at most two
 * {@code Timer} records per term (the two {@code scheduleTimer} sites, K5).
 *
 * <p>Control ({@link TimerPerOrderControlService}): a test-only service that schedules one cluster timer per order; the
 * same gate on it must FAIL {@code CM snapshot <bytes> B exceeds <max> B; timer records=<n>}, which proves the gate
 * measures what it claims.</p>
 */
public class CmSnapshotSizeClusterTest {

    private static final boolean NIGHTLY = Boolean.getBoolean("snapshot.nightly");
    private static final int ORDERS = NIGHTLY ? 1_000_000 : 100_000;
    private static final int ORDERS_PER_USER = 10;
    private static final int WINDOW = 4_000;
    private static final int CONTROL_ORDERS = 100_000;

    /** What the gate measured in one CM snapshot. */
    record CmSnapshot(long logPosition, long recordingId, long bytes, int timers, int terms) {
    }

    /** The A04 gate: the CM snapshot is smaller than the setting and holds at most two timers per term. */
    static void gate(final CmSnapshot s, final long maxBytes) {
        if (s.bytes() >= maxBytes) {
            throw new AssertionError("CM snapshot " + s.bytes() + " B exceeds " + maxBytes + " B; timer records=" + s.timers());
        }
        if (s.timers() > 2L * s.terms()) {
            throw new AssertionError("CM snapshot holds " + s.timers() + " timer records over " + s.terms()
                    + " term(s); at most 2 per term (K5)");
        }
    }

    @Test(timeout = 1_800_000)
    public void inStubModeTheCmSnapshotStaysSmallWithManyRestingGtdOrders() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(23100, "consensus-snapshot-size", 1, "16m", 0)) {
            final Member m = rig.members.get(0);
            m.settings = StubStory.shadow();
            rig.boot(m);
            rig.awaitLeader(null);
            final long maxBytes = m.settings.consensusSnapshotMaxBytes();
            final Client admin = rig.connect(SessionClass.ADMIN, m);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, m);
            final Client gw = rig.connect(SessionClass.GATEWAY, m);
            final List<Session> all = rig.sessions();
            CoreRestoreRig.register(gw.session, SessionClass.GATEWAY, false, false, all);
            final Encoders enc = new Encoders();
            CoreRestoreRig.offer(admin.session, enc.buf, enc.gateOpen(0xAD1L), all);
            rig.await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 60_000,
                    "the ADMIN gate open must be ACCEPTED");
            final int users = ORDERS / ORDERS_PER_USER;
            for (long u = 1; u <= users; u++) {
                CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(0, u, CoreRestoreRig.usd(), 1_000_000 * ShadowClusterRig.ONE), all);
                CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(0, u,
                        CoreRestoreRig.baseAssetOf(CoreRestoreRig.marketOf(u)), 1_000 * ShadowClusterRig.ONE), all);
            }
            CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(99, 1, CoreRestoreRig.usd(), ShadowClusterRig.ONE), all);
            rig.await(() -> tool.in.lastDepositAckCorr == 99, 300_000, "the funding deposits must be applied");

            final Listener g = gw.in;
            final long expiresAt = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
            final long b0 = System.nanoTime();
            for (int i = 0; i < ORDERS; i++) {
                final long user = 1 + i / ORDERS_PER_USER;
                final int k = i % ORDERS_PER_USER;
                final boolean buy = k % 2 == 0;
                final int market = CoreRestoreRig.marketOf(user);
                while (i - (g.acks + g.rejects) >= WINDOW) {
                    for (final Session s : all) {
                        s.poll();
                    }
                }
                CoreRestoreRig.offer(gw.session, enc.buf, enc.limit(i + 1, user, i + 1, market, buy,
                        CoreRestoreRig.priceOf(market, buy, k / 2), ShadowClusterRig.ONE / 10, TimeInForce.GTD, expiresAt), all);
            }
            rig.await(() -> g.acks + g.rejects >= ORDERS, 600_000, "every GTD create must be answered");
            assertNull("no create refused: " + g.firstReject, g.firstReject);
            assertEquals("every GTD order rests", ORDERS, m.service.engine().orders().orders().size());
            System.out.println("[11.9] " + ORDERS + " GTD orders resting, build " + (System.nanoTime() - b0) / 1_000_000 + " ms");

            final long pCm = rig.snapshotAll();
            assertTrue("the service snapshot is a stub: " + rig.tee.all("kind="),
                    !rig.tee.all("logPosition=" + pCm + " kind=stub").isEmpty());
            final CmSnapshot s = cmSnapshot(m.clusterDir(), rig.aeron(m), rig.archive(m));
            assertEquals("the newest group is the one just taken", pCm, s.logPosition());
            System.out.println("[11.9] CM snapshot at " + s.logPosition() + ": " + s.bytes() + " B, timer records=" + s.timers()
                    + ", terms=" + s.terms() + ", max=" + maxBytes + " (CONSENSUS_SNAPSHOT_MAX_BYTES)");
            gate(s, maxBytes);
            System.out.println("GREEN 11.9 orders=" + ORDERS + " cmSnapshotBytes=" + s.bytes() + " timers=" + s.timers());
        }
    }

    @Test(timeout = 900_000)
    public void controlOneClusterTimerPerOrderFailsTheGate() throws Exception {
        final int portBase = 23200;
        final long maxBytes = SnapshotModeSettings.defaults().consensusSnapshotMaxBytes();
        final File tmp = new File(System.getProperty("java.io.tmpdir"), "rcs-s4-11.9-control-" + portBase);
        IoUtil.delete(tmp, true);
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        TestAuth.install();
        final TimerPerOrderControlService service = new TimerPerOrderControlService();
        final List<String> hosts = List.of("localhost");
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, hosts, portBase, service);
        final String driverDir = new File(tmp, "driver").getAbsolutePath();
        cfg.baseDir(new File(tmp, "node0"));
        cfg.aeronDirectoryName(driverDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext().ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        ClusteredMediaDriver driver = null;
        ClusteredServiceContainer container = null;
        AeronCluster client = null;
        Aeron aeron = null;
        AeronArchive archive = null;
        try {
            driver = ClusteredMediaDriver.launch(cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                    .mtuLength(8192), cfg.archiveContext(), cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());
            client = connect(driverDir, portBase);
            final UnsafeBuffer msg = new UnsafeBuffer(new byte[16]);
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            for (int i = 0; i < CONTROL_ORDERS; i++) {
                msg.putLong(0, i);
                while (client.offer(msg, 0, 16) < 0) {
                    client.pollEgress();
                    idle.idle();
                }
                if ((i & 1023) == 0) {
                    client.pollEgress();
                    client.sendKeepAlive();
                }
            }
            final long deadline = System.currentTimeMillis() + 300_000;
            while (service.scheduled.get() < CONTROL_ORDERS) {
                assertTrue("the control service must schedule " + CONTROL_ORDERS + " timers, at " + service.scheduled.get(),
                        System.currentTimeMillis() < deadline);
                client.pollEgress();
                idle.idle();
            }
            final File clusterDir = cfg.consensusModuleContext().clusterDir();
            final int before = cmSnapshots(clusterDir);
            final AtomicCounter toggle = ClusterControl.findControlToggle(
                    cfg.consensusModuleContext().aeron().countersReader(), cfg.consensusModuleContext().clusterId());
            assertTrue("the SNAPSHOT toggle", ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
            while (cmSnapshots(clusterDir) <= before) {
                assertTrue("a CM snapshot after the toggle", System.currentTimeMillis() < deadline);
                client.pollEgress();
                client.sendKeepAlive();
                Thread.sleep(50);
            }
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverDir));
            archive = AeronArchive.connect(cfg.aeronArchiveContext().clone().aeron(aeron).ownsAeronClient(false));
            final CmSnapshot s = cmSnapshot(clusterDir, aeron, archive);
            System.out.println("[11.9-control] " + CONTROL_ORDERS + " cluster timers: CM snapshot " + s.bytes() + " B, timer records="
                    + s.timers() + ", max=" + maxBytes);
            assertTrue("the control holds one timer record per order: " + s.timers(), s.timers() >= CONTROL_ORDERS);
            RestoreDrillDryRun.nc("11.9-timer-per-order", "CM snapshot " + s.bytes() + " B exceeds " + maxBytes + " B; timer records="
                    + s.timers(), () -> gate(s, maxBytes));
        } finally {
            CloseHelper.quietCloseAll(archive, aeron, client, container, driver);
            IoUtil.delete(tmp, true);
            System.clearProperty("cluster.port.stride");
            ClusterPorts.resetStrideForTest();
        }
    }

    private static AeronCluster connect(final String driverDir, final int portBase) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .credentialsSupplier(TestAuth.credentials(SessionClass.TOOL))
                        .egressListener((sid, ts, buffer, offset, length, header) -> { })
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                        .aeronDirectoryName(driverDir)
                        .ingressEndpoints("0=localhost:" + ClusterConfig.calculatePort(0, portBase,
                                ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
            } catch (final RuntimeException e) {
                last = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect to the control cluster", last);
    }

    private static int cmSnapshots(final File clusterDir) {
        int n = 0;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid
                        && e.serviceId == ConsensusModule.Configuration.SERVICE_ID) {
                    n++;
                }
            }
        }
        return n;
    }

    /**
     * The newest valid CM snapshot entry (RecordingLog read side, entry order not relied on), its recorded length and
     * its Timer records, decoded with the public cluster codecs over a replay of the recording.
     */
    static CmSnapshot cmSnapshot(final File clusterDir, final Aeron aeron, final AeronArchive archive) {
        RecordingLog.Entry newest = null;
        int terms = 0;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_TERM && e.isValid) {
                    terms++;
                }
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid
                        && e.serviceId == ConsensusModule.Configuration.SERVICE_ID
                        && (newest == null || e.logPosition > newest.logPosition)) {
                    newest = e;
                }
            }
        }
        assertTrue("a valid CM snapshot entry in " + clusterDir, newest != null);
        final long recordingId = newest.recordingId;
        final long start = archive.getStartPosition(recordingId);
        final long stop = archive.getStopPosition(recordingId);
        assertTrue("the CM snapshot recording is stopped: " + stop, stop > start);
        final int streamId = 9_300 + (int)(System.nanoTime() & 0xFF);
        final int sessionId = (int)archive.startReplay(recordingId, start, stop - start, "aeron:ipc", streamId);
        final int[] timers = {0};
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, h) -> {
            if (length >= MessageHeaderDecoder.ENCODED_LENGTH) {
                header.wrap(buffer, offset);
                if (header.templateId() == TimerDecoder.TEMPLATE_ID && header.schemaId() == TimerDecoder.SCHEMA_ID) {
                    timers[0]++;
                }
            }
        });
        try (Subscription sub = aeron.addSubscription(ChannelUri.addSessionId("aeron:ipc", sessionId), streamId)) {
            final long deadline = System.currentTimeMillis() + 120_000;
            Image image;
            while ((image = sub.imageBySessionId(sessionId)) == null) {
                assertTrue("a replay image of the CM snapshot", System.currentTimeMillis() < deadline);
                Thread.onSpinWait();
            }
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (image.position() < stop) {
                assertTrue("the CM snapshot replay reaches " + stop + " (at " + image.position() + ")",
                        System.currentTimeMillis() < deadline);
                final int n = image.poll(assembler, 256);
                if (n == 0 && (image.isEndOfStream() || image.isClosed()) && image.position() < stop) {
                    throw new AssertionError("CM snapshot replay ended at " + image.position() + " < " + stop);
                }
                idle.idle(n);
            }
        }
        return new CmSnapshot(newest.logPosition, recordingId, stop - start, timers[0], Math.max(1, terms));
    }
}

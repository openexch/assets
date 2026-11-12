// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Listener;
import com.openexchange.core.cluster.CoreRestoreRig.Session;
import com.openexchange.core.cluster.CoreRestoreRig.Tee;
import com.openexchange.core.domain.Asset;
import com.openexchange.core.domain.FixedPoint;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.CoreClusteredService;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The three-member in-process rig of the cluster-level restore tests (contract 11.2, 11.4, 11.11, W4 reuses
 * it). Real {@link CoreClusteredService}s, each on its own embedded media driver, archive and consensus module, node
 * directories that survive a restart ({@link #restart}), the cluster control toggle for snapshots, the producer of
 * contract 11.0 pointed at a member's own archive, and the oracles the drills need (image equality across members,
 * readiness, the {@code [SNAPSHOT]} lines).
 *
 * <p>Only the fork-wide singletons are touched through system properties and restored in {@link #close}: the shared-host
 * port stride, the ephemeral probe port, the snapshot cadence timer (off: every snapshot here is the one a test asked
 * for).</p>
 *
 * <p>One "kill" is {@code close(container); close(driver)}: no JVM shutdown hook and no clean cluster shutdown, but NOT
 * a crash of the service: closing the container runs {@code ClusteredServiceAgent.onClose}, which calls
 * {@code service.onTerminate} (Aeron 1.53.1 {@code ClusteredServiceAgent:209-225}), so the pruner stop, the
 * housekeeping close and the endpoint stop all run. The archive and directories are left as they are at that moment
 * (the pattern of {@code TimerSurvivalClusterTest} and {@code CoreRestoreRig}). Real {@code kill -9} coverage
 * ({@code destroyForcibly} of a child JVM) is {@code ShadowRestoreExternalDriverProcessTest}.</p>
 */
final class ShadowClusterRig implements AutoCloseable {

    static final int MEMBERS = 3;
    static final long ONE = FixedPoint.fromDouble(1.0);
    static final long OP_TIMEOUT_MS = 60_000;
    private static final String INGRESS_CHANNEL = "aeron:udp?term-length=1m|mtu=8192";
    static final long GENESIS = 0x5EEDL;

    /** One member: its service, driver and container, and the directories that outlive a kill. */
    static final class Member {
        final int id;
        final File baseDir;
        final String driverDir;
        CoreClusteredService service;
        ClusterConfig cfg;
        ClusteredMediaDriver driver;
        ClusteredServiceContainer container;
        volatile boolean killed = true;
        int starts;
        /** The settings this member boots with (null: the process environment, as before). */
        com.openexchange.core.infrastructure.persistence.SnapshotModeSettings settings;
        /** Negative-control seams switched on at the next boot ({@code ShadowRestore.Faults} field names). */
        String[] faults = new String[0];
        /** (11.6 runtime net): a pruner watermark other than the ShadowFloor at the next boot. */
        java.util.function.LongSupplier watermarkOverride;
        /** Runs on the new service before its ClusterConfig is built (journal sink, fault hooks). */
        java.util.function.Consumer<CoreClusteredService> beforeLaunch;
        /** Runs after the driver, archive and consensus module launched, before the container (AC:208-212). */
        java.util.function.Consumer<Member> beforeContainer;
        /** Runs after ClusterConfig.create and before the launch (wraps the authenticator supplier). */
        java.util.function.Consumer<Member> afterConfig;
        final List<String> halts = new CopyOnWriteArrayList<>();
        private Aeron producerAeron;
        private AeronArchive producerArchive;

        Member(final int id, final File tmp) {
            this.id = id;
            this.baseDir = new File(tmp, "node" + id);
            this.driverDir = new File(tmp, "driver" + id).getAbsolutePath();
        }

        File clusterDir() {
            return cfg.consensusModuleContext().clusterDir();
        }

        File archiveDir() {
            return cfg.archiveContext().archiveDir();
        }

        Cluster.Role role() {
            if (killed) {
                throw new IllegalStateException("member " + id + " is killed");
            }
            final io.aeron.Counter counter = cfg.consensusModuleContext().clusterNodeRoleCounter();
            return counter == null ? Cluster.Role.FOLLOWER : Cluster.Role.get(counter);
        }

        void closeProducerClients() {
            CloseHelper.quietCloseAll(producerArchive, producerAeron);
            producerArchive = null;
            producerAeron = null;
        }
    }

    /** A client session with its listener; every rig wait pumps all of them so none times out. */
    final class Client {
        final Session session;
        final Listener in;
        long corr = 1;

        Client(final Session session, final Listener in) {
            this.session = session;
            this.in = in;
        }

        void offer(final UnsafeBuffer buf, final int length) {
            CoreRestoreRig.offer(session, buf, length, sessions());
        }

        /** {@code n} deposits (no requestId, so none dedupes), then waits for the last ack. */
        void deposits(final int n) {
            final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
            long last = 0;
            for (int i = 0; i < n; i++) {
                last = corr++;
                offer(enc.buf, enc.deposit(last, 1 + (i % 5), Asset.USD.id(), ONE));
            }
            final long target = last;
            await(() -> in.lastDepositAckCorr >= target, OP_TIMEOUT_MS, () -> "deposit ack for corr " + target + " (last "
                    + in.lastDepositAckCorr + ")");
        }
    }

    final int portBase;
    final File tmp;
    final List<String> hosts;
    /** The log channel's term length (a recording's segment can never be smaller than its term). */
    final String logTermLength;
    /** The archive segment length; 0 keeps ClusterConfig's (64 MiB). */
    final int segmentLength;
    /** Every stderr line that names (the halt texts), recorded like the stdout tee. */
    final Tee errTee;
    private final PrintStream originalErr;
    final List<Member> members = new ArrayList<>();
    /** D4: the first member's {@code aeron.archive.id}; distinct per rig in one JVM. */
    final long archiveIdBase = NEXT_ARCHIVE_ID_BASE.getAndAdd(16);
    private static final java.util.concurrent.atomic.AtomicLong NEXT_ARCHIVE_ID_BASE =
            new java.util.concurrent.atomic.AtomicLong(10_000);
    final Tee tee;
    private final PrintStream originalOut;
    private final List<Client> clients = new ArrayList<>();

    ShadowClusterRig(final int portBase, final String name) {
        this(portBase, name, MEMBERS, "1m", 0);
    }

    /**
     * {@code members} members (1 for the single-member rows of 11.3, 11.5, 11.6, 11.10), the log term length
     * and the archive segment length (small segments make a log purge reachable with a few hundred frames).
     */
    ShadowClusterRig(final int portBase, final String name, final int members, final String logTermLength,
                     final int segmentLength) {
        this.portBase = portBase;
        final List<String> h = new ArrayList<>();
        for (int i = 0; i < members; i++) {
            h.add("localhost");
        }
        this.hosts = List.copyOf(h);
        this.logTermLength = logTermLength;
        this.segmentLength = segmentLength;
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        System.setProperty("core.node.port", "0");
        // the cadence is built in a field initialiser of the service: BEFORE the first service is constructed
        System.setProperty("snapshot.interval.minutes", "0");
        this.originalOut = System.out;
        this.tee = new Tee(originalOut, "[SNAPSHOT]", "[shadow restore]");
        System.setOut(tee);
        this.originalErr = System.err;
        this.errTee = new Tee(originalErr, "shadow restore", "SNAPSHOT_MODE");
        System.setErr(errTee);
        this.tmp = new File(System.getProperty("java.io.tmpdir"), name + "-" + portBase);
        IoUtil.delete(tmp, true);
        for (int i = 0; i < members; i++) {
            this.members.add(new Member(i, tmp));
        }
    }

    // ---------------------------------------------------------------------------------------------- life cycle

    void bootAll() {
        for (final Member m : members) {
            boot(m);
        }
    }

    void boot(final Member m) {
        TestAuth.install();
        m.service = m.settings == null ? new CoreClusteredService() : new CoreClusteredService(m.settings);
        ServiceProbe.swapFailFast(m.service, () -> m.halts.add("failFast at " + System.currentTimeMillis()));
        if (m.faults.length > 0) {
            ServiceProbe.faults(m.service, m.faults);
        }
        if (m.watermarkOverride != null) {
            ServiceProbe.prunerWatermarkOverride(m.service, m.watermarkOverride);
        }
        if (m.beforeLaunch != null) {
            m.beforeLaunch.accept(m.service);
        }
        m.cfg = ClusterConfig.create(m.id, hosts, hosts, portBase, m.service);
        m.cfg.baseDir(m.baseDir);
        m.cfg.aeronDirectoryName(m.driverDir);
        m.cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        m.cfg.errorHandler(Throwable::printStackTrace);
        m.cfg.consensusModuleContext()
                .ingressChannel(INGRESS_CHANNEL)
                .logChannel("aeron:udp?term-length=" + logTermLength)
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        if (segmentLength > 0) {
            m.cfg.archiveContext().segmentFileLength(segmentLength);
        }
        // D4: one JVM hosts every member, and ShadowRetention keeps one purge owner per archive id per process (C9),
        // so each member's archive gets its own id (production runs one member per process). Stable across restarts
        // of the same member, so a restart re-registers the same id.
        m.cfg.archiveContext().archiveId(archiveIdBase + m.id);
        if (m.afterConfig != null) {
            m.afterConfig.accept(m);
        }
        m.driver = ClusteredMediaDriver.launch(
                m.cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true).mtuLength(8192),
                m.cfg.archiveContext(), m.cfg.consensusModuleContext());
        if (m.beforeContainer != null) {
            m.beforeContainer.accept(m);
        }
        m.container = ClusteredServiceContainer.launch(m.cfg.clusteredServiceContext());
        m.killed = false;
        m.starts++;
    }

    /**
     * Abrupt in-process stop: no shutdown hook, no clean cluster shutdown, but closing the container still runs
     * {@code onTerminate} (class Javadoc); the node and archive directories stay as they are.
     */
    void kill(final Member m) {
        m.killed = true;
        m.closeProducerClients();
        CloseHelper.quietClose(m.container);
        CloseHelper.quietClose(m.driver);
    }

    void restart(final Member m) {
        if (!m.killed) {
            throw new IllegalStateException("member " + m.id + " is running");
        }
        boot(m);
    }

    @Override
    public void close() {
        for (final Client c : clients) {
            CloseHelper.quietClose(c.session.client);
        }
        for (final Member m : members) {
            m.killed = true;
            m.closeProducerClients();
            CloseHelper.quietClose(m.container);
            CloseHelper.quietClose(m.driver);
        }
        System.setOut(originalOut);
        System.setErr(originalErr);
        IoUtil.delete(tmp, true);
        System.clearProperty("cluster.port.stride");
        System.clearProperty("core.node.port");
        System.clearProperty("snapshot.interval.minutes");
        ClusterPorts.resetStrideForTest();
    }

    // ---------------------------------------------------------------------------------------------- waiting

    List<Session> sessions() {
        final List<Session> out = new ArrayList<>(clients.size());
        for (final Client c : clients) {
            out.add(c.session);
        }
        return out;
    }

    /** Polls every client (egress and keep-alives) until {@code done}; fails with {@code why} at the deadline. */
    void await(final BooleanSupplier done, final long timeoutMs, final String why) {
        await(done, timeoutMs, () -> why);
    }

    /** As above with the failure text computed at the deadline (the state it reports is the state at failure). */
    void await(final BooleanSupplier done, final long timeoutMs, final java.util.function.Supplier<String> why) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (!done.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(why.get());
            }
            int work = 0;
            for (final Client c : clients) {
                work += c.session.poll();
            }
            idle.idle(work);
        }
    }

    void pumpMs(final long ms) {
        final long until = System.currentTimeMillis() + ms;
        await(() -> System.currentTimeMillis() >= until, ms + 1_000, "pump");
    }

    Member awaitLeader(final Member except) {
        final Member[] found = new Member[1];
        await(() -> {
            for (final Member m : members) {
                if (!m.killed && (except == null || m.id != except.id) && m.role() == Cluster.Role.LEADER) {
                    found[0] = m;
                    return true;
                }
            }
            return false;
        }, 60_000, "no leader" + (except == null ? "" : " other than member " + except.id));
        return found[0];
    }

    List<Member> live() {
        final List<Member> out = new ArrayList<>();
        for (final Member m : members) {
            if (!m.killed) {
                out.add(m);
            }
        }
        return out;
    }

    /** Live members other than {@code exclude}, leader last. */
    Member followerOtherThan(final Member leader, final Member exclude) {
        for (final Member m : live()) {
            if (m.id != leader.id && (exclude == null || m.id != exclude.id)) {
                return m;
            }
        }
        throw new IllegalStateException("no such follower");
    }

    /** Milliseconds from now until {@code m} reports {@code /ready}. */
    long awaitReady(final Member m, final long timeoutMs) {
        final long t0 = System.nanoTime();
        await(() -> ServiceProbe.ready(m.service), timeoutMs, () -> "member " + m.id + " never became ready: "
                + ServiceProbe.readiness(m.service) + "; halts " + m.halts);
        return (System.nanoTime() - t0) / 1_000_000;
    }

    // ---------------------------------------------------------------------------------------------- clients

    /**
     * The clients' new-leader timeout, 10 s by default. Aeron's own default is 2 x the members' leader heartbeat
     * timeout, 2 s with this rig's 1 s heartbeat timeout: shorter than an election after a leader kill here (heartbeat
     * timeout plus election, measured 2 038..2 320 ms), and on a loaded CI runner a spurious election of a healthy
     * leader ended a client session the same way, so a deposit was never acked. Real clients retry with a request id;
     * the rig's clients keep their sessions across an election instead. {@code Aeron.NULL_VALUE} restores Aeron's
     * default for a test that wants it.
     */
    long clientNewLeaderTimeoutNs = java.util.concurrent.TimeUnit.SECONDS.toNanos(10);

    String ingressEndpoints() {
        final StringBuilder sb = new StringBuilder();
        for (final Member m : members) {
            if (m.killed) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(m.id).append("=localhost:").append(ClusterConfig.calculatePort(m.id, portBase,
                    ClusterConfig.CLIENT_FACING_PORT_OFFSET));
        }
        return sb.toString();
    }

    /** A session of {@code cls} through the media driver of {@code host} and the live members' ingress endpoints. */
    Client connect(final SessionClass cls, final Member host) throws InterruptedException {
        return connect(cls, host, null);
    }

    /** As above with {@code tap} also receiving every egress frame (decoders the rig listener lacks). */
    Client connect(final SessionClass cls, final Member host, final io.aeron.cluster.client.EgressListener tap)
            throws InterruptedException {
        final Listener in = new Listener();
        final long deadline = System.currentTimeMillis() + 90_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                final AeronCluster client = AeronCluster.connect(new AeronCluster.Context()
                        .credentialsSupplier(TestAuth.credentials(cls))
                        .egressListener(tap == null ? in : (sid, ts, buffer, offset, length, header) -> {
                            in.onMessage(sid, ts, buffer, offset, length, header);
                            tap.onMessage(sid, ts, buffer, offset, length, header);
                        })
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel(INGRESS_CHANNEL)
                        .aeronDirectoryName(host.driverDir)
                        .ingressEndpoints(ingressEndpoints())
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                        .newLeaderTimeoutNs(clientNewLeaderTimeoutNs));
                final Client c = new Client(new Session(client, in), in);
                clients.add(c);
                return c;
            } catch (final RuntimeException e) {
                last = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect a " + cls + " session via member " + host.id, last);
    }

    void forget(final Client c) {
        clients.remove(c);
        CloseHelper.quietClose(c.session.client);
    }

    /**
     * Closes every client session. A single-member rig must do this before a kill: the clients ride the
     * member's own media driver, and an Aeron client whose driver dies exits the JVM.
     */
    void forgetAll() {
        for (final Client c : new ArrayList<>(clients)) {
            forget(c);
        }
    }

    /** An archive client of {@code m}'s archive (lazily connected, closed by {@link #kill}). */
    AeronArchive archive(final Member m) {
        return memberArchive(m).archive();
    }

    /** The rig's Aeron client on {@code m}'s driver (lazily connected, closed by {@link #kill}). */
    Aeron aeron(final Member m) {
        return memberArchive(m).aeron();
    }

    /** The start position of {@code m}'s log recording (0 until segments are purged). */
    long logStart(final Member m) {
        return archive(m).getStartPosition(logRecordingId(m));
    }

    /**
     * {@code purgeSegments} of {@code m}'s log recording to {@code newStart} (a segment boundary); returns the start
     * after it. The archive refuses a detach of the segment it is still writing, so this first waits (bounded) until
     * the recording has reached {@code newStart}. The member's own pruner purges to the segment below its snapshot
     * after each snapshot, asynchronously: a start already at or past {@code newStart}, before or during this call,
     * means there is nothing left to detach, and the start is returned as it is.
     */
    long purgeLogTo(final Member m, final long newStart) {
        final long rec = logRecordingId(m);
        final AeronArchive archive = archive(m);
        await(() -> archive.getStartPosition(rec) >= newStart || recordedEnd(archive, rec) >= newStart, OP_TIMEOUT_MS,
                () -> "member " + m.id + " log rec=" + rec + " did not record past the purge boundary " + newStart
                        + " within " + OP_TIMEOUT_MS + " ms (start " + archive.getStartPosition(rec) + ", recorded "
                        + recordedEnd(archive, rec) + ")");
        if (archive.getStartPosition(rec) < newStart) {
            try {
                archive.purgeSegments(rec, newStart);
            } catch (final io.aeron.archive.client.ArchiveException e) {
                if (archive.getStartPosition(rec) < newStart) {
                    throw e;
                }
                System.out.println("[rig] member " + m.id + " log rec=" + rec + ": its own pruner reached " + newStart
                        + " first (" + e.getMessage() + ")");
            }
        }
        return archive.getStartPosition(rec);
    }

    /** The recorded end of {@code rec}: the live recording position, else its stop position. */
    private static long recordedEnd(final AeronArchive archive, final long rec) {
        final long live = archive.getRecordingPosition(rec);
        return live != AeronArchive.NULL_POSITION ? live : archive.getStopPosition(rec);
    }

    /** The nonce {@link #adopt} ADOPTs: {@link #GENESIS} unless a re-genesis test sets another. */
    long adoptGenesis = GENESIS;

    /** ADMIN CoreConfig version 1: the ADOPT of the genesis nonce (A7); the producer of 11.0 needs it. */
    void adopt(final Client admin) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[4096]);
        admin.offer(buf, encodeAdopt(buf, adoptGenesis));
        await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=70 result=ADOPTED")) > 0, OP_TIMEOUT_MS,
                () -> "the CoreConfig ADOPT must be acked ADOPTED; frames " + admin.in.frames);
    }

    /** The ADOPT frame of {@link #adopt} (also offered by the external-driver test's own client). */
    static int encodeAdopt(final UnsafeBuffer buf) {
        return encodeAdopt(buf, GENESIS);
    }

    /** The ADOPT of another genesis nonce, for a re-genesised rig. */
    static int encodeAdopt(final UnsafeBuffer buf, final long genesis) {
        return CoreRestoreRig.Encoders.coreConfig(buf, 0x71L, 1, genesis);
    }

    /**
     * ADMIN SetMoneyJournal(enabled); journaling is replicated state, armed by the log. The command has no
     * ack, so the wait is on every live member's engine.
     */
    void armMoneyJournal(final Client admin) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
        admin.offer(buf, encodeArmJournal(buf));
        await(() -> live().stream().allMatch(m -> ServiceProbe.journalEnabled(m.service)), OP_TIMEOUT_MS,
                "SetMoneyJournal must reach every live member");
    }

    static int encodeArmJournal(final UnsafeBuffer buf) {
        final com.openexchange.core.infrastructure.generated.SetMoneyJournalEncoder e =
                new com.openexchange.core.infrastructure.generated.SetMoneyJournalEncoder()
                        .wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder()).correlationId(0x7A1L)
                        .enabled(com.openexchange.core.infrastructure.generated.BoolFlag.TRUE);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    // ---------------------------------------------------------------------------------------------- snapshots

    /** Valid service snapshot entries of {@code m} as {@code {logPosition, recordingId, leadershipTermId}}, oldest first. */
    List<long[]> snapshots(final Member m) {
        final List<long[]> out = new ArrayList<>();
        try (RecordingLog log = new RecordingLog(m.clusterDir(), false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == 0) {
                    out.add(new long[] {e.logPosition, e.recordingId, e.leadershipTermId});
                }
            }
        }
        return out;
    }

    /** {@code {leadershipTermId, termBaseLogPosition, recordingId}} of every term entry of {@code m}. */
    List<long[]> terms(final Member m) {
        final List<long[]> out = new ArrayList<>();
        try (RecordingLog log = new RecordingLog(m.clusterDir(), false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_TERM) {
                    out.add(new long[] {e.leadershipTermId, e.termBaseLogPosition, e.recordingId});
                }
            }
        }
        return out;
    }

    long logRecordingId(final Member m) {
        try (RecordingLog log = new RecordingLog(m.clusterDir(), false)) {
            return log.findLastTermRecordingId();
        }
    }

    /**
     * The cluster control toggle on the leader asks for one snapshot; waits until every live member holds one more valid
     * service snapshot, and returns the new snapshot's log position (the action frame end; equal on all members).
     */
    long snapshotAll() {
        final Member leader = awaitLeader(null);
        final int[] before = new int[members.size()];
        for (final Member m : live()) {
            before[m.id] = snapshots(m).size();
        }
        final AtomicCounter toggle = ClusterControl.findControlToggle(
                leader.cfg.consensusModuleContext().aeron().countersReader(), leader.cfg.consensusModuleContext().clusterId());
        if (!ClusterControl.ToggleState.SNAPSHOT.toggle(toggle)) {
            throw new AssertionError("the SNAPSHOT toggle was not accepted");
        }
        await(() -> {
            for (final Member m : live()) {
                if (snapshots(m).size() <= before[m.id]) {
                    return false;
                }
            }
            return true;
        }, 120_000, "every live member must take the requested snapshot");
        final List<long[]> mine = snapshots(leader);
        return mine.get(mine.size() - 1)[0];
    }

    // ---------------------------------------------------------------------------------------------- shadows

    /** The committed frames of {@code m}'s own log recording from position 0 (11.0), bounded by its commit position. */
    RecordedLog captureCommitted(final Member m) {
        final TestShadowProducer.MemberArchive a = memberArchive(m);
        final long recording = logRecordingId(m);
        final long commit = m.cfg.consensusModuleContext().commitPositionCounter().get();
        final long recorded = a.archive().getRecordingPosition(recording);
        final long to = recorded < 0 ? commit : Math.min(commit, recorded);
        return TestShadowProducer.captureLog(a.aeron(), a.archive(), recording, 0, to);
    }

    TestShadowProducer producer(final Member m) {
        final TestShadowProducer.MemberArchive a = memberArchive(m);
        return new TestShadowProducer(a.aeron(), a.archive(), m.cfg.consensusModuleContext().clusterId(), m.archiveDir());
    }

    private TestShadowProducer.MemberArchive memberArchive(final Member m) {
        if (m.producerAeron == null) {
            m.producerAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(m.driverDir));
            m.producerArchive = AeronArchive.connect(m.cfg.aeronArchiveContext().clone().aeron(m.producerAeron)
                    .ownsAeronClient(false));
        }
        return new TestShadowProducer.MemberArchive(m.producerAeron, m.producerArchive);
    }

    /**
     * A live replication of {@code src}'s log recording into {@code dst}'s archive (from 0, following the log); the
     * members' own log purges (the pruner after a snapshot, {@link #purgeLogTo}) never touch it, so
     * {@link #captureCopy} reads the log from 0 at any time. Holds one replay of {@code src}'s archive. Returns the
     * copy's recording id (the active recording on {@code dst} that is neither its own log nor a shadow).
     */
    long liveLogCopy(final Member src, final Member dst) {
        final AeronArchive a = archive(dst);
        final long own = logRecordingId(dst);
        a.replicate(logRecordingId(src), Aeron.NULL_VALUE, AeronArchive.Configuration.controlStreamId(),
                "aeron:udp?endpoint=" + archiveEndpoint(src), null);
        final long[] found = {-1};
        await(() -> {
            a.listRecordings(0, 1_000, (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                        startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                        mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                        sourceIdentity) -> {
                if (recordingId != own && startPosition == 0 && stopPosition == AeronArchive.NULL_POSITION
                        && !originalChannel.contains("alias=oxs.")) {
                    found[0] = recordingId;
                }
            });
            return found[0] >= 0;
        }, 30_000, "the live log copy never appeared on member " + dst.id);
        System.out.println("[rig] live copy of member " + src.id + "'s log on member " + dst.id + ": rec=" + found[0]);
        return found[0];
    }

    /** The log committed on {@code src} read from 0 out of the live copy {@code copy} on {@code dst}. */
    RecordedLog captureCopy(final Member src, final Member dst, final long copy) {
        final long commit = src.cfg.consensusModuleContext().commitPositionCounter().get();
        await(() -> archive(dst).getRecordingPosition(copy) >= commit, 30_000,
                "the live log copy never reached the commit position " + commit);
        return TestShadowProducer.captureLog(aeron(dst), archive(dst), copy, 0, commit);
    }

    // ---------------------------------------------------------------------------------------------- pulls

    /** {@code m}'s archive control endpoint {@code localhost:<p1>} (a pull source entry). */
    String archiveEndpoint(final Member m) {
        return "localhost:" + ClusterConfig.calculatePort(m.id, portBase, ClusterConfig.ARCHIVE_CONTROL_PORT_OFFSET);
    }

    /** The {@code SHADOW_PULL_SOURCES} text naming {@code sources}' archives, in order. */
    String sources(final Member... sources) {
        final StringBuilder sb = new StringBuilder();
        for (final Member m : sources) {
            sb.append(sb.length() == 0 ? "" : ",").append(archiveEndpoint(m));
        }
        return sb.toString();
    }

    /** Shadow-mode settings with {@code extra} on top ({@code Key} name, value pairs). */
    static com.openexchange.core.infrastructure.persistence.SnapshotModeSettings shadowSettings(final String... extra) {
        final java.util.Map<com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.Key, String> m =
                new java.util.HashMap<>();
        m.put(com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.Key.SNAPSHOT_MODE, "shadow");
        // the restore and puller rows stand the test producer in for the follower: the real one is opted in by name
        m.put(com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.Key.SHADOW_PRODUCER, "none");
        for (int i = 0; i < extra.length; i += 2) {
            m.put(com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.Key.valueOf(extra[i]),
                    extra[i + 1]);
        }
        return com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.with(m);
    }

    /** One local catalog scan of {@code m}'s archive (genesis {@link #adoptGenesis}), through the rig's client. */
    com.openexchange.cluster.shadow.ShadowCatalog.Scan scan(final Member m) {
        return new com.openexchange.cluster.shadow.ShadowCatalog(aeron(m), archive(m),
                com.openexchange.core.infrastructure.persistence.CoreLogApplier.ENGINE_ID_CORE,
                m.cfg.consensusModuleContext().clusterId(), adoptGenesis,
                new com.openexchange.cluster.shadow.ShadowVerdicts(),
                com.openexchange.cluster.shadow.ShadowCatalog.DEFAULT_LIST_BATCH,
                org.agrona.concurrent.SystemNanoClock.INSTANCE, TimeUnit.SECONDS.toNanos(30),
                new com.openexchange.cluster.shadow.ShadowCounters()).scan();
    }

    /**
     * Test seam: purges every shadow recording of {@code m}'s archive (a wiped shadow store), stopped ones only. The
     * member's own puller must be closed first or it pulls them back.
     */
    int purgeShadows(final Member m) {
        final List<Long> ids = new ArrayList<>();
        archive(m).listRecordingsForUri(0, 10_000,
                com.openexchange.cluster.shadow.ShadowAlias.prefix(
                        com.openexchange.core.infrastructure.persistence.CoreLogApplier.ENGINE_ID_CORE),
                com.openexchange.cluster.shadow.ShadowAlias.STREAM_ID,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                 stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                 strippedChannel, originalChannel, sourceIdentity) -> {
                    if (stopPosition != AeronArchive.NULL_POSITION) {
                        ids.add(recordingId);
                    }
                });
        for (final long id : ids) {
            // a peer's puller or this member's own deep read may be replaying the recording: retry, bounded
            final long deadline = System.currentTimeMillis() + OP_TIMEOUT_MS;
            while (true) {
                try {
                    archive(m).purgeRecording(id);
                    break;
                } catch (final io.aeron.archive.client.ArchiveException replaying) {
                    if (System.currentTimeMillis() > deadline || replaying.getMessage() == null
                            || !replaying.getMessage().contains("active replay")) {
                        throw replaying;
                    }
                    pumpMs(200);
                }
            }
        }
        return ids.size();
    }

    // ---------------------------------------------------------------------------------------------- oracles

    /**
     * Polls until the two members' snapshot images are byte equal on two consecutive reads (the images are read from the
     * test thread while the service threads may still apply; equality at rest is what is asserted).
     */
    void awaitImagesEqual(final Member a, final Member b, final long timeoutMs) {
        awaitImagesEqual(a, b, timeoutMs, bytes -> bytes);
    }

    /** As {@link #awaitImagesEqual(Member, Member, long)} with {@code tamperA} applied to a's bytes (a negative control). */
    void awaitImagesEqual(final Member a, final Member b, final long timeoutMs,
                          final java.util.function.UnaryOperator<byte[]> tamperA) {
        final byte[][] last = new byte[2][];
        final RuntimeException[] race = new RuntimeException[1];
        final int[] stable = {0};
        await(() -> {
            try {
                final byte[] ia = tamperA.apply(ServiceProbe.image(a.service));
                final byte[] ib = ServiceProbe.image(b.service);
                last[0] = ia;
                last[1] = ib;
                if (Arrays.equals(ia, ib)) {
                    return ++stable[0] >= 2;
                }
            } catch (final RuntimeException racing) {
                // a read torn by a concurrent apply: not equal yet
                race[0] = racing;
            }
            stable[0] = 0;
            return false;
        }, timeoutMs, () -> "member " + a.id + " and member " + b.id + " images never became equal (sizes "
                + (last[0] == null ? -1 : last[0].length) + " vs " + (last[1] == null ? -1 : last[1].length)
                + (race[0] == null ? "" : ", last read error " + race[0]) + ")");
    }
}

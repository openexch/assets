// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.CoreRestoreRig.Session;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.replica.ReplicaTestBindings;
import com.openexchange.cluster.NodeReadiness;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaContexts;
import com.openexchange.cluster.replica.ReplicaEngine;
import com.openexchange.cluster.replica.ReplicaFaults;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaProcess;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowHousekeeping;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowImageLoader;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.codecs.ClusterAction;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The three-member {@link ShadowClusterRig} plus an in-JVM replica built by {@link ReplicaProcess} in
 * EMBEDDED mode on its own driver, with the production core binding, a recording host (the
 * lifecycle's fatal hook is recorded, never exits the JVM), the members' snapshot images captured on their service
 * threads at each {@code onTakeSnapshot} (the image at P_cm), and the oracles the replica cluster tests share.
 */
final class ReplicaRig implements AutoCloseable {

    static final int SLOT = 3;
    static final int STRIDE = 100;
    static final long WAIT_MS = 90_000;

    /** The test fault seams ({@code throwAtFrame}, {@code mutateImage}, the slow apply). */
    static final class Faults implements ReplicaFaults {
        /** Throw on the n-th frame seen after arming (one shot); 0 = off. */
        volatile long throwAtFrame;
        volatile long framesSinceArm;
        volatile long thrownAt = -1;
        /** Flip the first payload byte before the hand-off, once. */
        volatile boolean mutateNextImage;
        volatile long mutatedAt = -1;
        /** Sleep this many ms before every frame (the slow applier of the time-lag row); 0 = off. */
        volatile long sleepPerFrameMs;

        /** The named-copy rig: the running replica's backup context (the seam hands it over before launch). */
        volatile io.aeron.cluster.ClusterBackup.Context backupContext;
        private long savedTimeoutNs = -1;
        private long savedIntervalNs = -1;

        @Override
        public void backupContext(final io.aeron.cluster.ClusterBackup.Context context) {
            backupContext = context;
            savedTimeoutNs = -1;
            savedIntervalNs = -1;
        }

        /**
         * {@code true} resets the backup at every SNAPSHOT_RETRIEVE (a 1 ns replication progress timeout,
         * read by the agent when it starts each snapshot retrieval, so the first poll throws "log replication has not
         * progressed" and the agent goes to RESET_BACKUP); {@code false} restores the values the backup ran with.
         */
        synchronized void wedgeSnapshotRetrieval(final boolean wedge) {
            final io.aeron.cluster.ClusterBackup.Context c = backupContext;
            if (c == null) {
                throw new IllegalStateException("no backup context (start a replica first)");
            }
            if (wedge) {
                if (savedTimeoutNs < 0) {
                    savedTimeoutNs = c.replicationProgressTimeoutNs();
                    savedIntervalNs = c.replicationProgressIntervalNs();
                }
                c.replicationProgressTimeoutNs(1).replicationProgressIntervalNs(1);
                System.out.println("[rig] backup snapshot retrieval WEDGED (replication progress timeout 1 ns)");
            } else if (savedTimeoutNs >= 0) {
                c.replicationProgressTimeoutNs(savedTimeoutNs).replicationProgressIntervalNs(savedIntervalNs);
                System.out.println("[rig] backup snapshot retrieval restored (timeout "
                        + TimeUnit.NANOSECONDS.toMillis(savedTimeoutNs) + " ms)");
                savedTimeoutNs = -1;
            }
        }

        void armThrowAtFrame(final long n) {
            framesSinceArm = 0;
            thrownAt = -1;
            throwAtFrame = n;
        }

        @Override
        public void beforeFrame(final FrameKind kind, final long logPosition) {
            final long sleep = sleepPerFrameMs;
            if (sleep > 0) {
                try {
                    Thread.sleep(sleep);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            final long n = throwAtFrame;
            if (n > 0 && ++framesSinceArm >= n) {
                throwAtFrame = 0;
                thrownAt = logPosition;
                throw new IllegalStateException("Faults.throwAtFrame: injected apply failure at " + logPosition);
            }
        }

        @Override
        public void beforeOffer(final ShadowImageBuffer image, final LogCursor at) {
            if (mutateNextImage) {
                mutateNextImage = false;
                mutatedAt = at.logPosition();
                final org.agrona.MutableDirectBuffer payload = image.payloadView();
                // the snapshot envelope's first bytes: the scratch restore refuses the image (decode, not a quiet value)
                for (int i = 0; i < 8; i++) {
                    payload.putByte(i, (byte)~payload.getByte(i));
                }
            }
        }
    }

    final ShadowClusterRig rig;
    final Map<Integer, byte[]> imageAtSnapshot = new ConcurrentHashMap<>();
    final Map<Integer, Long> snapshotsTaken = new ConcurrentHashMap<>();
    final List<String> fatals = new CopyOnWriteArrayList<>();
    final List<String> aeronErrors = new CopyOnWriteArrayList<>();
    final Faults faults = new Faults();
    ReplicaProcess replica;
    ReplicaEngine binding;
    private AeronArchive testArchive;

    Client admin;
    Client tool;
    Client gw;
    Client bridge;
    Member clientHost;
    private final Encoders enc = new Encoders();
    private long nextOrder = 1;
    private long expiresAt;

    ReplicaRig(final int portBase, final String name) {
        this(new ShadowClusterRig(portBase, name));
    }

    ReplicaRig(final ShadowClusterRig rig) {
        System.setProperty(ReplicaFaults.PROPERTY, "true");
        this.rig = rig;
    }

    // ---------------------------------------------------------------------------------------------- members

    void bootAll() {
        rig.bootAll();
        for (final Member m : rig.members) {
            hook(m);
        }
    }

    /** The member's image at each {@code onTakeSnapshot}, captured on its service thread (state at P_cm). */
    void hook(final Member m) {
        ServiceProbe.afterTakeSnapshot(m.service, () -> {
            imageAtSnapshot.put(m.id, ServiceProbe.image(m.service));
            snapshotsTaken.merge(m.id, 1L, Long::sum);
        });
    }

    /**
     * ADOPT, then GATEWAY, BRIDGE, TOOL and ADMIN sessions through {@code host} (a member that is not killed by the
     * test), the order-entry gate open and funded users.
     */
    void story(final Member host, final int users) throws InterruptedException {
        clientHost = host;
        admin = rig.connect(SessionClass.ADMIN, host);
        rig.adopt(admin);
        tool = rig.connect(SessionClass.TOOL, host);
        gw = rig.connect(SessionClass.GATEWAY, host);
        bridge = rig.connect(SessionClass.BRIDGE, host);
        final List<Session> all = rig.sessions();
        CoreRestoreRig.register(gw.session, SessionClass.GATEWAY, false, false, all);
        CoreRestoreRig.register(bridge.session, SessionClass.BRIDGE, true, false, all);
        CoreRestoreRig.offer(admin.session, enc.buf, enc.gateOpen(0xAD1L), all);
        rig.await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 60_000,
                "the ADMIN gate open must be ACCEPTED");
        for (long u = 1; u <= users; u++) {
            CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(0, u, CoreRestoreRig.usd(),
                    1_000_000 * ShadowClusterRig.ONE), all);
            CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(0, u,
                    CoreRestoreRig.baseAssetOf(CoreRestoreRig.marketOf(u)), 1_000 * ShadowClusterRig.ONE), all);
        }
        CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(99, 1, CoreRestoreRig.usd(), ShadowClusterRig.ONE), all);
        rig.await(() -> tool.in.lastDepositAckCorr == 99, 120_000, "the funding deposits must be applied");
        tool.corr = 100;
        expiresAt = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
        orders(users, 2 * users);
    }

    /** {@code n} GATEWAY GTD limit orders over users {@code 1..users}, every one answered. */
    void orders(final int users, final int n) {
        final List<Session> all = rig.sessions();
        final int target = gw.in.acks + gw.in.rejects + n;
        for (int i = 0; i < n; i++) {
            final long user = 1 + (nextOrder % users);
            final boolean buy = nextOrder % 2 == 0;
            final int market = CoreRestoreRig.marketOf(user);
            CoreRestoreRig.offer(gw.session, enc.buf, enc.limit(nextOrder, user, nextOrder, market, buy,
                    CoreRestoreRig.priceOf(market, buy, (int)(nextOrder % 7)), ShadowClusterRig.ONE / 10,
                    TimeInForce.GTD, expiresAt), all);
            nextOrder++;
        }
        rig.await(() -> gw.in.acks + gw.in.rejects >= target, 120_000, "every GTD create must be answered");
    }

    /**
     * After a leader kill: a fresh TOOL session through the same host (the shadow leader-change tests reconnect too; an
     * old session may not fail over within the offer timeout).
     */
    void reconnectTool() throws InterruptedException {
        rig.forget(tool);
        tool = rig.connect(SessionClass.TOOL, clientHost);
    }

    /** Light traffic: deposits plus orders. */
    void traffic(final int deposits, final int orders) {
        tool.deposits(deposits);
        if (orders > 0) {
            orders(4, orders);
        }
    }

    // ---------------------------------------------------------------------------------------------- replica

    static int freeTcp() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    File replicaBase(final String name) {
        return new File(rig.tmp, name);
    }

    /** The test defaults: slot {@code slot}, its own dirs under the rig, the REPLICA credential, small buffers. */
    ReplicaConfig.Builder builder(final int slot, final String name) {
        return ReplicaConfig.builder()
                .memberHosts(rig.hosts)
                .portBase(rig.portBase)
                .portStride(STRIDE)
                .slot(slot)
                .replicaAddress("localhost")
                .baseDir(replicaBase(name))
                .aeronDir(new File(rig.tmp, name + "-driver").getAbsolutePath())
                .engineId(CoreLogApplier.ENGINE_ID_CORE)
                .nodePort(freeTcp())
                .credentials(TestAuth.credentials(SessionClass.REPLICA))
                .backupQueryIntervalMs(12_000)
                .shadowBufferMb(16)
                .shadowTermLength("1m")
                .scanPeriodMs(500)
                .deepReadPaceMs(0)
                .socketBufferLength(1024 * 1024)
                .archiveSegmentLength(Math.max(rig.segmentLength, 4 * 1024 * 1024))
                .testHooks(faults, EnumSet.noneOf(ReplicaNegativeControl.class));
    }

    ReplicaProcess startReplica(final ReplicaConfig cfg) {
        return startReplica(cfg, ReplicaTestBindings.production(() -> fatals.add("CoreConfig cross-check failed")));
    }

    ReplicaProcess startReplica(final ReplicaConfig cfg, final ReplicaEngine engine) {
        if (replica != null) {
            throw new IllegalStateException("a replica is running (one replica per core cluster)");
        }
        fatals.clear();
        binding = engine;
        replica = ReplicaProcess.launch(cfg, engine, new ReplicaProcess.Host() {
            @Override
            public void fatal(final String reason) {
                fatals.add(reason);
                System.err.println("[REPLICA-TEST] host.fatal: " + reason);
            }

            @Override
            public void onError(final Throwable t, final boolean externalDriver) {
                aeronErrors.add(String.valueOf(t));
            }

            @Override
            public void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
                throw new IllegalStateException("the in-JVM rig runs EMBEDDED");
            }
        }, null);
        return replica;
    }

    /** The in-JVM stop: endpoint, follower, writer, housekeeping, clients, backup, archive, driver. */
    void stopReplica() {
        CloseHelper.quietClose(testArchive);
        testArchive = null;
        CloseHelper.quietClose(replica);
        replica = null;
    }

    /** Waits a full member backup window so the next backup query is not inside a window of the previous one. */
    void windowGap() {
        rig.pumpMs(ReplicaConfig.DEFAULT_MEMBER_BACKUP_WINDOW_MS + 1_500);
    }

    void awaitBootstrapped(final long timeoutMs) {
        rig.await(() -> replica.follower().bootstrapDone() || !fatals.isEmpty(), timeoutMs,
                () -> "the replica never bootstrapped: " + replica.readiness().probe().detail());
        if (!replica.follower().bootstrapDone()) {
            throw new AssertionError("the replica refused its bootstrap: " + fatals);
        }
    }

    /** Applied up to every live member's commit position (the replica caught up). */
    void awaitCaughtUp(final long timeoutMs) {
        rig.await(() -> {
            final long commit = commit();
            return replica.follower().appliedPosition() >= commit && commit > 0;
        }, timeoutMs, () -> "the replica never caught up: applied=" + replica.follower().appliedPosition()
                + " commit=" + commit() + " " + replica.readiness().probe().detail());
    }

    long commit() {
        long min = Long.MAX_VALUE;
        for (final Member m : rig.live()) {
            min = Math.min(min, m.cfg.consensusModuleContext().commitPositionCounter().get());
        }
        return min == Long.MAX_VALUE ? -1 : min;
    }

    NodeReadiness.Probe probe() {
        return replica.readiness().probe();
    }

    /** {@code GET /ready} over the replica's node endpoint: {status, body}. */
    String[] httpReady() {
        try {
            final HttpURLConnection c = (HttpURLConnection)URI.create("http://localhost:" + replica.endpoint().port()
                    + "/ready").toURL().openConnection();
            c.setConnectTimeout(2_000);
            c.setReadTimeout(2_000);
            final int code = c.getResponseCode();
            final java.io.InputStream in = code >= 400 ? c.getErrorStream() : c.getInputStream();
            final String body = in == null ? "" : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            c.disconnect();
            return new String[] {Integer.toString(code), body};
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One metric value from the replica's {@code /metrics} body (exact name, labels included), -1 when absent. */
    long metric(final String name) {
        for (final String line : replica.metrics().body().split("\n")) {
            if (line.startsWith(name + " ")) {
                return Long.parseLong(line.substring(name.length() + 1).trim());
            }
        }
        return -1;
    }

    // ---------------------------------------------------------------------------------------------- shadows

    ShadowCatalog.Scan scan() {
        final ShadowHousekeeping hk = replica.housekeeping().housekeeping();
        return hk == null ? null : hk.scan();
    }

    static ShadowKind kindOf(final LogicalShadow s) {
        return s.representative().header().kindEnum();
    }

    /** VERIFIED positions of {@code kind} (null: any) in the newest housekeeping scan. */
    List<Long> verified(final ShadowKind kind) {
        final List<Long> out = new ArrayList<>();
        final ShadowCatalog.Scan s = scan();
        if (s != null) {
            for (final LogicalShadow v : s.verified()) {
                if (kind == null || kindOf(v) == kind) {
                    out.add(v.logPosition());
                }
            }
        }
        out.sort(Long::compare);
        return out;
    }

    /** Every recording's (position, state) in the newest scan, for failure texts. */
    String describeScan() {
        final ShadowCatalog.Scan s = scan();
        if (s == null) {
            return "no scan";
        }
        final StringBuilder sb = new StringBuilder();
        for (final ShadowEntry e : s.all()) {
            sb.append(" rec=").append(e.recordingId()).append(':').append(e.header() == null ? -1
                    : e.header().logPosition()).append(':').append(e.state());
            if (e.header() != null) {
                sb.append(':').append(e.header().kindEnum()).append(':').append(e.producer());
            }
        }
        return sb.toString();
    }

    LogicalShadow awaitVerified(final long position, final ShadowKind kind, final long timeoutMs) {
        rig.await(() -> verified(kind).contains(position) || !fatals.isEmpty(), timeoutMs,
                () -> "no VERIFIED " + kind + " shadow at " + position + " within " + timeoutMs + " ms: "
                        + describeScan() + " " + probe().detail());
        if (!verified(kind).contains(position)) {
            throw new AssertionError("the replica halted before a VERIFIED " + kind + " shadow at " + position + ": "
                    + fatals);
        }
        return scan().atPosition(position);
    }

    /** A scan of the replica's archive by a catalog of the test's own (no verdict cache, not the housekeeping one). */
    ShadowCatalog.Scan freshScan() {
        return scanOf(replica.aeron(), replicaArchive(), replica.config().clusterId(),
                Math.max(replica.follower().knownGenesis(), 0));
    }

    /** A scan of a replica archive through any client of it (the external-driver test reads a child replica's over it). */
    static ShadowCatalog.Scan scanOf(final io.aeron.Aeron aeron, final AeronArchive archive, final int clusterId,
                                     final long genesis) {
        return new ShadowCatalog(aeron, archive, CoreLogApplier.ENGINE_ID_CORE, clusterId, genesis,
                new ShadowVerdicts(), ShadowCatalog.DEFAULT_LIST_BATCH, SystemNanoClock.INSTANCE,
                TimeUnit.SECONDS.toNanos(30), new ShadowCounters()).scan();
    }

    /** Header positions of every shadow recording (any state) in a fresh scan. */
    List<Long> allShadowPositions() {
        final List<Long> out = new ArrayList<>();
        for (final ShadowEntry e : freshScan().all()) {
            if (e.header() != null) {
                out.add(e.header().logPosition());
            }
        }
        out.sort(Long::compare);
        return out;
    }

    /** The shadow test producer writing into the replica's archive (forgeries, crashed writes). */
    com.openexchange.core.infrastructure.persistence.TestShadowProducer producerOnReplica() {
        return new com.openexchange.core.infrastructure.persistence.TestShadowProducer(replica.aeron(),
                replicaArchive(), replica.config().clusterId(), replica.config().archiveDir());
    }

    /** A client of the replica's archive on the replica's own Aeron client (closed by {@link #stopReplica}). */
    AeronArchive replicaArchive() {
        if (testArchive == null) {
            testArchive = AeronArchive.connect(ReplicaContexts.localArchiveClient(replica.config())
                    .aeron(replica.aeron()).ownsAeronClient(false));
        }
        return testArchive;
    }

    /** Whether {@code recordingId} is still listed by the replica's archive. */
    boolean inReplicaArchive(final long recordingId) {
        return replicaArchive().listRecording(recordingId, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, q) -> { }) > 0;
    }

    /**
     * Every recording id the backup RecordingLog of the replica named {@code name} references (any entry type), read
     * from its {@code cluster/} dir; empty when there is none.
     */
    java.util.Set<Long> recordingLogIds(final String name) {
        final java.util.Set<Long> ids = new java.util.TreeSet<>();
        final File clusterDir = new File(replicaBase(name), "cluster");
        if (!new File(clusterDir, io.aeron.cluster.RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            return ids;
        }
        try (io.aeron.cluster.RecordingLog log = new io.aeron.cluster.RecordingLog(clusterDir, false)) {
            for (final io.aeron.cluster.RecordingLog.Entry e : log.entries()) {
                ids.add(e.recordingId);
            }
        }
        return ids;
    }

    /** The payload bytes of {@code shadow}'s representative copy, loaded and digest-checked by the kit loader. */
    byte[] payload(final LogicalShadow shadow) {
        return payloadOf(replica.aeron(), replicaArchive(), shadow);
    }

    /** {@link #payload} through any client of the replica archive. */
    static byte[] payloadOf(final io.aeron.Aeron aeron, final AeronArchive archive, final LogicalShadow shadow) {
        final ShadowEntry e = shadow.representative();
        final UnsafeBuffer into = new UnsafeBuffer(ByteBuffer.allocateDirect((int)e.header().payloadLength()));
        final ShadowImageLoader loader = new ShadowImageLoader(aeron, archive, new ShadowVerdicts(),
                SystemNanoClock.INSTANCE, TimeUnit.SECONDS.toNanos(30), new ShadowCounters());
        final ShadowImageLoader.LoadResult r = loader.load(e, into);
        if (!r.ok()) {
            throw new AssertionError("the shadow at " + shadow.logPosition() + " does not load: " + r.kind() + " "
                    + r.reason());
        }
        final byte[] out = new byte[r.loaded().length()];
        r.loaded().payload().getBytes(0, out);
        return out;
    }

    /** The follow oracle: the replica's image at {@code p} equals the members' {@code encodeSnapshot} at p. */
    static void assertPayloadEqualsMember(final long p, final byte[] replica, final byte[] member) {
        if (!Arrays.equals(replica, member)) {
            int at = 0;
            while (at < Math.min(replica.length, member.length) && replica[at] == member[at]) {
                at++;
            }
            throw new AssertionError("payload differs from member snapshot at " + p + " (replica " + replica.length
                    + " B, member " + member.length + " B, first difference at byte " + at + ")");
        }
    }

    /** Flips one payload byte of a shadow recording in its segment file (TestShadowProducer.flipPayloadByte). */
    static void flipPayloadByte(final File archiveDir, final long recordingId) {
        final File segment = new File(archiveDir, recordingId + "-0.rec");
        if (!segment.isFile()) {
            throw new IllegalStateException("segment file " + segment + " not found");
        }
        final long offset = ShadowHeader.FRAME_LENGTH + 32 + 64;
        try (RandomAccessFile f = new RandomAccessFile(segment, "rw")) {
            f.seek(offset);
            final int b = f.read();
            f.seek(offset);
            f.write(b ^ 0x01);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ---------------------------------------------------------------------------------------------- cluster

    /** A control toggle on the leader (SNAPSHOT or STANDBY_SNAPSHOT), retried while another action holds it. */
    void toggle(final ClusterControl.ToggleState state) {
        final Member leader = rig.awaitLeader(null);
        final AtomicCounter toggle = ClusterControl.findControlToggle(
                leader.cfg.consensusModuleContext().aeron().countersReader(),
                leader.cfg.consensusModuleContext().clusterId());
        rig.await(() -> state.toggle(toggle), 30_000, "the " + state + " toggle was not accepted");
    }

    /** The CLUSTER_ACTION SNAPSHOT frames with {@code flags} in {@code m}'s committed log, as frame ends. */
    List<Long> actionFrames(final Member m, final int flags) {
        final RecordedLog log = rig.captureCommitted(m);
        final List<Long> out = new ArrayList<>();
        for (final LogFrame f : log.frames()) {
            if (f.kind() == FrameKind.CLUSTER_ACTION && f.action() == ClusterAction.SNAPSHOT && f.flags() == flags) {
                out.add(f.position());
            }
        }
        return out;
    }

    static void each(final Iterable<Member> members, final Consumer<Member> body) {
        for (final Member m : members) {
            body.accept(m);
        }
    }

    static Set<ReplicaNegativeControl> off(final ReplicaNegativeControl... controls) {
        final EnumSet<ReplicaNegativeControl> s = EnumSet.noneOf(ReplicaNegativeControl.class);
        s.addAll(Arrays.asList(controls));
        return s;
    }

    /**
     * The named-copy rig: what a backup reset during SNAPSHOT_RETRIEVE leaves in the replica archive, made exactly: a
     * stopped recording on the member snapshot's stream holding a message-exact PREFIX of member {@code m}'s service
     * snapshot at {@code position}. {@code throughEnd} false: the BEGIN marker only (no END); true: every message
     * through Aeron's END marker plus the first {@code payloadBytes} of the service payload (its envelope then fails
     * length and CRC). Returns the new recording id.
     */
    long partialCopy(final Member m, final long position, final boolean throughEnd, final int payloadBytes) {
        long src = -1;
        try (io.aeron.cluster.RecordingLog log = new io.aeron.cluster.RecordingLog(
                m.cfg.consensusModuleContext().clusterDir(), false)) {
            for (final io.aeron.cluster.RecordingLog.Entry e : log.entries()) {
                if (e.type == io.aeron.cluster.RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == 0
                        && e.logPosition == position) {
                    src = e.recordingId;
                }
            }
        }
        if (src < 0) {
            throw new AssertionError("partial-copy rig: member " + m.id + " has no service snapshot at " + position);
        }
        final long[] d = {-1, -1, -1};
        rig.archive(m).listRecording(src, (a, b, c, ts0, ts1, start, stop, h, i, j, k, l, streamId, n, o, q) -> {
            d[0] = start;
            d[1] = stop;
            d[2] = streamId;
        });
        final List<byte[]> messages = new ArrayList<>();
        new com.openexchange.cluster.shadow.ShadowReplay(rig.aeron(m), rig.archive(m), SystemNanoClock.INSTANCE)
                .readAll(src, d[0], d[1], (buffer, offset, length, header) -> {
                    final byte[] b = new byte[length];
                    buffer.getBytes(offset, b);
                    messages.add(b);
                }, System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
        final io.aeron.cluster.codecs.MessageHeaderDecoder hd = new io.aeron.cluster.codecs.MessageHeaderDecoder();
        final io.aeron.cluster.codecs.SnapshotMarkerDecoder md = new io.aeron.cluster.codecs.SnapshotMarkerDecoder();
        final List<byte[]> prefix = new ArrayList<>();
        boolean ended = false;
        for (final byte[] msg : messages) {
            if (ended) {
                if (payloadBytes > 0) {
                    prefix.add(Arrays.copyOf(msg, Math.min(payloadBytes, msg.length)));
                }
                break;
            }
            prefix.add(msg);
            final UnsafeBuffer ub = new UnsafeBuffer(msg);
            if (msg.length >= io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH) {
                hd.wrap(ub, 0);
                if (hd.schemaId() == io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID
                        && hd.templateId() == io.aeron.cluster.codecs.SnapshotMarkerDecoder.TEMPLATE_ID) {
                    md.wrap(ub, io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH, hd.blockLength(),
                            hd.version());
                    if (md.mark() == io.aeron.cluster.codecs.SnapshotMark.BEGIN && !throughEnd) {
                        break;
                    }
                    if (md.mark() == io.aeron.cluster.codecs.SnapshotMark.END) {
                        ended = true;
                    }
                }
            }
        }
        final AeronArchive archive = replicaArchive();
        final io.aeron.Aeron aeron = replica.aeron();
        final String channel = "aeron:ipc?term-length=1m";
        final int streamId = (int)d[2];
        try (io.aeron.ExclusivePublication pub = aeron.addExclusivePublication(channel, streamId)) {
            final String recCh = io.aeron.ChannelUri.addSessionId(channel, pub.sessionId());
            final long subscriptionId = archive.startRecording(recCh, streamId,
                    io.aeron.archive.codecs.SourceLocation.LOCAL);
            final org.agrona.concurrent.status.CountersReader counters = aeron.countersReader();
            int counterId;
            final long deadline = System.currentTimeMillis() + 30_000;
            while ((counterId = io.aeron.archive.status.RecordingPos.findCounterIdBySession(counters,
                    pub.sessionId(), archive.archiveId())) == org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("partial-copy rig: the partial copy recording never started");
                }
                Thread.onSpinWait();
            }
            final long recordingId = io.aeron.archive.status.RecordingPos.getRecordingId(counters, counterId);
            for (final byte[] msg : prefix) {
                final UnsafeBuffer ub = new UnsafeBuffer(msg);
                while (pub.offer(ub, 0, msg.length) < 0) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new AssertionError("partial-copy rig: the partial copy offer never went through");
                    }
                    Thread.onSpinWait();
                }
            }
            while (counters.getCounterValue(counterId) < pub.position()) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("partial-copy rig: the partial copy was never fully recorded");
                }
                Thread.onSpinWait();
            }
            archive.stopRecording(subscriptionId);
            while (archive.getStopPosition(recordingId) < 0) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("partial-copy rig: the partial copy recording never stopped");
                }
                Thread.onSpinWait();
            }
            System.out.println("[rig] partial copy rec=" + recordingId + " of member " + m.id + " snapshot rec="
                    + src + " at " + position + ": " + prefix.size() + " of " + messages.size() + " message(s), "
                    + (throughEnd ? "through END + " + payloadBytes + " B of payload" : "BEGIN only, no END")
                    + ", stopped at " + archive.getStopPosition(recordingId) + " on stream " + streamId);
            return recordingId;
        }
    }

    @Override
    public void close() {
        stopReplica();
        rig.close();
    }
}

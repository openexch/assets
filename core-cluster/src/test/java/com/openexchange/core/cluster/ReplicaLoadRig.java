// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.loadgen.CoreOrderLoadGenerator;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The replica under load: a {@link ReplicaRig} (three members in {@code shadow} mode with the real designated-follower
 * producer, pull sources = the replica and the peers, as {@link DualProducerLiveTest}) driven by the order load
 * generator's in-JVM {@link CoreOrderLoadGenerator.Session} (requestId on every command, one bounded re-offer after an
 * election), plus the lag sampler of the replica rows.
 *
 * <p>The generator and the ADMIN client ride a media driver of their own, so any member can be killed under load. The
 * generator ticks and the sampler samples on every {@link ShadowClusterRig#await} iteration, the thread that polls the
 * clients' egress, so a row's waits keep the load open loop and the samples at their rate.</p>
 *
 * <p>The lag: {@code lag = position(leader) - applied(replica)}, sampled at {@code core.lag.hz} (10). In-JVM the
 * position is the exact commit position (the leader's {@code commitPositionCounter}); the gauge proxy
 * {@code core_log_position - applied} is recorded alongside, so its error is a number. Every row writes
 * {@code lag.tsv} and {@code summary.txt} under {@code core.lag.out} ({@code target/lag}).</p>
 *
 * <p>Settings (system properties, each logged at boot; every bound a setting, never a hard-coded limit):
 * {@code core.lag.rate} (500 slots/s), {@code core.lag.open} (2000 resting orders), {@code core.lag.hz} (10),
 * {@code core.lag.seconds} (60), {@code core.lag.catalog.bound.s} (5), {@code core.lag.peerloss.bound.s} (30: the
 * supplier's liveness timeout of about 3 s, the backup's 12 s cool-down, one backup query and the catch-up; 16 to 28 s
 * measured),
 * {@code core.lag.pause.s} (3), {@code core.lag.pause.recovery.s} (10), {@code core.lag.promotion.attempts} (3),
 * {@code core.lag.restart.ready.bound.s} (60), {@code core.lag.members} (3; 2..3, the lag row only).</p>
 */
final class ReplicaLoadRig implements AutoCloseable {

    static final long RATE = Long.getLong("core.lag.rate", 500);
    static final int OPEN = Integer.getInteger("core.lag.open", 2_000);
    static final int HZ = Integer.getInteger("core.lag.hz", 10);
    static final int SECONDS = Integer.getInteger("core.lag.seconds", 60);
    static final long CATALOG_BOUND_S = Long.getLong("core.lag.catalog.bound.s", 5);
    static final long PEERLOSS_BOUND_S = Long.getLong("core.lag.peerloss.bound.s", 30);
    static final long PAUSE_S = Long.getLong("core.lag.pause.s", 3);
    static final long PAUSE_RECOVERY_S = Long.getLong("core.lag.pause.recovery.s", 10);
    static final int PROMOTION_ATTEMPTS = Integer.getInteger("core.lag.promotion.attempts", 3);
    static final long RESTART_READY_BOUND_S = Long.getLong("core.lag.restart.ready.bound.s", 60);
    static final int MEMBERS = Integer.getInteger("core.lag.members", 3);
    /**
     * The shadow image buffers of the members' producer and the replica (MB): the core's requestId table keeps every
     * request for 15 minutes, so under the load the image grows by the load's requests (measured: about 15 MB after
     * three minutes at 500 slots/s), past the unloaded rows' 16.
     */
    static final int BUFFER_MB = Integer.getInteger("core.lag.buffer.mb", 64);

    static {
        refuse(RATE >= 1 && RATE <= 100_000, "core.lag.rate must be 1..100000: " + RATE);
        refuse(HZ >= 1 && HZ <= 100, "core.lag.hz must be 1..100: " + HZ);
        refuse(SECONDS >= 10 && SECONDS <= 3_600, "core.lag.seconds must be 10..3600: " + SECONDS);
        refuse(CATALOG_BOUND_S >= 1 && CATALOG_BOUND_S <= 600, "core.lag.catalog.bound.s must be 1..600");
        refuse(PEERLOSS_BOUND_S >= 1 && PEERLOSS_BOUND_S <= 600, "core.lag.peerloss.bound.s must be 1..600");
        refuse(PAUSE_S >= 1 && PAUSE_S <= 60, "core.lag.pause.s must be 1..60");
        refuse(PAUSE_RECOVERY_S >= 1 && PAUSE_RECOVERY_S <= 600, "core.lag.pause.recovery.s must be 1..600");
        refuse(PROMOTION_ATTEMPTS >= 1 && PROMOTION_ATTEMPTS <= 10, "core.lag.promotion.attempts must be 1..10");
        refuse(RESTART_READY_BOUND_S >= 1 && RESTART_READY_BOUND_S <= 600,
                "core.lag.restart.ready.bound.s must be 1..600");
        refuse(MEMBERS >= 2 && MEMBERS <= 3, "core.lag.members must be 2..3: " + MEMBERS);
        refuse(BUFFER_MB >= 1 && BUFFER_MB <= 2047, "core.lag.buffer.mb must be 1..2047: " + BUFFER_MB);
    }

    private static void refuse(final boolean ok, final String text) {
        if (!ok) {
            throw new IllegalArgumentException("the test refuses: " + text);
        }
    }

    /** The settings of a run, written into its summary. */
    static Map<String, Object> settings() {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("rate_slots_per_s", RATE);
        m.put("open_orders", OPEN);
        m.put("users", CoreOrderLoadGenerator.minUsers(RATE, OPEN, true));
        m.put("sample_hz", HZ);
        m.put("seconds", SECONDS);
        m.put("catalog_bound_s", CATALOG_BOUND_S);
        m.put("peerloss_bound_s", PEERLOSS_BOUND_S);
        m.put("pause_s", PAUSE_S);
        m.put("pause_recovery_s", PAUSE_RECOVERY_S);
        m.put("promotion_attempts", PROMOTION_ATTEMPTS);
        m.put("restart_ready_bound_s", RESTART_READY_BOUND_S);
        m.put("heartbeat_timeout_ms", ShadowClusterRig.DEFAULT_LEADER_HEARTBEAT_TIMEOUT_MS);
        m.put("shadow_buffer_mb", BUFFER_MB);
        return m;
    }

    final String row;
    final ReplicaRig r;
    final ShadowClusterRig rig;
    /** The log term in bytes ({@code TRANSPORT_LOG_TERM_LENGTH} of the run): the lag bound. */
    final long termBytes;
    /** The replica's archive control endpoint, first in every member's pull sources. */
    final String replicaEndpoint;
    private final File clientDir;
    private MediaDriver clientDriver;
    /** The client host: a member-shaped handle on the clients' own driver (never killed). */
    Member clientHost;
    Client admin;
    CoreOrderLoadGenerator.Session gen;
    CoreOrderLoadGenerator.Settings load;
    final Sampler sampler;

    /**
     * @param term the log term ({@code 1m} for the lag rows; {@code 64k} with {@code segment} 64k where the members'
     *             logs must purge within a row)
     */
    ReplicaLoadRig(final int portBase, final String row, final int members, final String term, final int segment) {
        this.row = row;
        this.rig = new ShadowClusterRig(portBase, row, members, term, segment);
        this.r = new ReplicaRig(rig);
        this.termBytes = parseSize(term);
        this.replicaEndpoint = r.builder(ReplicaRig.SLOT, "replica").build().archiveControlEndpoint();
        for (final Member m : rig.members) {
            final List<Member> peers = new ArrayList<>(rig.members);
            peers.remove(m);
            m.settings = ShadowProducerRoleTest.producerSettings("SHADOW_PULL_SOURCES", replicaEndpoint + ","
                    + rig.sources(peers.toArray(new Member[0])), "SHADOW_PULL_PERIOD_MS", "1000",
                    "SHADOW_PRODUCER_BUFFER_MB", Integer.toString(BUFFER_MB));
        }
        this.clientDir = new File(System.getProperty("java.io.tmpdir"), row + "-clients-" + portBase);
        this.sampler = new Sampler(row, termBytes);
        System.out.println("[lag] " + row + " settings " + settings() + " members " + members + " term " + term
                + " segment " + segment);
    }

    static long parseSize(final String s) {
        final String t = s.trim().toLowerCase(Locale.ROOT);
        final char u = t.charAt(t.length() - 1);
        final long n = Character.isDigit(u) ? Long.parseLong(t) : Long.parseLong(t.substring(0, t.length() - 1));
        return switch (u) {
            case 'k' -> n * 1024;
            case 'm' -> n * 1024 * 1024;
            case 'g' -> n * 1024 * 1024 * 1024;
            default -> n;
        };
    }

    /** The rig's replica settings with the load rows' shadow buffer. */
    ReplicaConfig.Builder builder(final int slot, final String name) {
        return r.builder(slot, name).shadowBufferMb(BUFFER_MB);
    }

    /** Boots the members, the clients' driver, ADOPT and the order-entry gate. */
    void bootMembers() throws InterruptedException {
        r.bootAll();
        rig.awaitLeader(null);
        IoUtil.delete(clientDir, true);
        clientDriver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(new File(clientDir, "driver9").getAbsolutePath())
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        clientHost = new Member(9, clientDir);
        admin = rig.connect(SessionClass.ADMIN, clientHost);
        rig.adopt(admin);
        final Encoders enc = new Encoders();
        admin.offer(enc.buf, enc.gateOpen(0xAD1L));
        rig.await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 60_000,
                "the ADMIN gate open must be ACCEPTED");
    }

    /** The in-JVM replica with the row's settings: bootstrapped, caught up, every member's writer armed. */
    void startReplica(final ReplicaConfig cfg) {
        r.startReplica(cfg);
        r.awaitBootstrapped(ReplicaRig.WAIT_MS);
        r.awaitCaughtUp(120_000);
        ShadowProducerRoleTest.awaitArmed(rig);
        sampler.attach(() -> r.replica == null ? -1 : r.replica.follower().appliedPosition(),
                () -> r.replica == null ? -1 : r.replica.follower().recordedPosition());
    }

    /**
     * The generator's session (prefunded users, a ramp of resting orders, then the steady slots until
     * {@link #endLoad}); returns once the steady phase runs. The tick and the sampler join every rig wait.
     */
    void startLoad() throws InterruptedException {
        load = new CoreOrderLoadGenerator.Settings();
        load.rate = RATE;
        load.durationSec = 3_600; // ended by endLoad: the faults decide a row's length
        load.warmupSec = 2;
        load.openOrders = OPEN;
        load.prefund = true;
        load.users = (int)load.minUsers();
        load.reofferAfterMs = ShadowClusterRig.DEFAULT_LEADER_HEARTBEAT_TIMEOUT_MS
                + ShadowClusterRig.ELECTION_TIMEOUT_MS;
        final CoreOrderLoadGenerator.Session[] holder = new CoreOrderLoadGenerator.Session[1];
        final io.aeron.cluster.client.EgressListener relay = new io.aeron.cluster.client.EgressListener() {
            @Override
            public void onMessage(final long sid, final long ts, final org.agrona.DirectBuffer b, final int o,
                                  final int l, final io.aeron.logbuffer.Header h) {
                holder[0].onMessage(sid, ts, b, o, l, h);
            }

            @Override
            public void onNewLeader(final long sid, final long term, final int leader, final String ingress) {
                holder[0].onNewLeader(sid, term, leader, ingress);
            }
        };
        final Client gw = rig.connect(SessionClass.GATEWAY, clientHost, relay);
        final Client tool = rig.connect(SessionClass.TOOL, clientHost, relay);
        holder[0] = new CoreOrderLoadGenerator.Session(load, gw.session.client::offer, tool.session.client::offer,
                System.out);
        gen = holder[0];
        rig.duringAwait.add(() -> gen.tick(System.nanoTime()));
        rig.duringAwait.add(() -> sampler.sample(rig));
        rig.await(() -> "STEADY".equals(gen.phase()), 900_000, () -> "the generator never reached its steady phase: "
                + gen.phase() + " " + gen.result());
        sampler.start();
        System.out.println("[lag] " + row + " load steady at " + RATE + " slots/s over " + load.users + " users, "
                + OPEN + " open orders");
    }

    /**
     * Ends the steady phase and drains: every command answered once (a replayed answer counted as {@code replayed},
     * never as new work), no reject. Returns the generator's result.
     */
    CoreOrderLoadGenerator.Result endLoad() {
        gen.endSteady(System.nanoTime());
        rig.await(gen::done, 120_000, () -> "the generator did not drain: " + gen.phase() + " " + gen.result());
        final CoreOrderLoadGenerator.Result res = gen.result();
        System.out.println("[lag] " + row + " generator " + res);
        if (res.unanswered() > 0) {
            throw new AssertionError("unanswered commands after the drain: " + res.unanswered() + " (" + res + ")");
        }
        if (res.rejects() > 0) {
            throw new AssertionError("rejected commands by reason " + res.rejectsByReason() + " (" + res + ")");
        }
        if (res.acks() != res.issued()) {
            throw new AssertionError("acks " + res.acks() + " != issued " + res.issued() + " (" + res + ")");
        }
        return res;
    }

    /** The exact commit position: the largest live member's commit counter (the leader's). */
    long commit() {
        long max = -1;
        for (final Member m : rig.live()) {
            max = Math.max(max, m.cfg.consensusModuleContext().commitPositionCounter().get());
        }
        return max;
    }

    /** The live leader, or null during an election. */
    Member leaderOrNull() {
        for (final Member m : rig.live()) {
            if (m.role() == Cluster.Role.LEADER) {
                return m;
            }
        }
        return null;
    }

    /** Kills {@code m} (the rig's in-process stop). */
    void kill(final Member m) {
        rig.kill(m);
        System.out.println("[lag] " + row + " killed member " + m.id + " (roles " + rig.roles() + ")");
    }

    /** Restarts {@code m} on its directories and re-hooks its snapshot images. */
    void restart(final Member m) {
        rig.restart(m);
        r.hook(m);
        System.out.println("[lag] " + row + " restarted member " + m.id);
    }

    /** A snapshot on every live member; returns P_cm. */
    long snapshot() {
        final long p = rig.snapshotAll();
        System.out.println("[lag] " + row + " P_cm=" + p);
        return p;
    }

    /**
     * The catalog bound: from P_cm on every live member to the replica's housekeeping scan listing a usable ALIGNED
     * at {@code p}; returns the milliseconds (the caller compares them with the bound it names).
     */
    long msToCatalog(final long p, final long timeoutMs) {
        final long t0 = System.nanoTime();
        rig.await(() -> catalogued(p), timeoutMs, () -> "the replica never listed an ALIGNED at " + p + " within "
                + timeoutMs + " ms: " + r.describeScan() + " " + r.probe().detail());
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    }

    private boolean catalogued(final long p) {
        final ShadowCatalog.Scan s = r.scan();
        if (s == null) {
            return false;
        }
        for (final ShadowEntry e : s.all()) {
            if (e.header() != null && e.header().logPosition() == p && e.header().kindEnum() == ShadowKind.ALIGNED
                    && e.state().usable()) {
                return true;
            }
        }
        return false;
    }

    /** The replica's ALIGNED at {@code p} VERIFIED and its payload equal to every live member's image at p. */
    void assertAlignedEqual(final long p) {
        final LogicalShadow aligned = r.awaitVerified(p, ShadowKind.ALIGNED, 120_000);
        final byte[] payload = r.payload(aligned);
        int compared = 0;
        for (final Member m : rig.live()) {
            if (r.imageAt.containsKey(new ReplicaRig.MemberPosition(m.id, p))) {
                ReplicaRig.assertPayloadEqualsMember(p, payload, r.memberImage(m.id, p));
                compared++;
            }
        }
        if (compared == 0) {
            throw new AssertionError("no member image captured at " + p + " to compare the ALIGNED with");
        }
        System.out.println("[lag] " + row + " ALIGNED at " + p + " VERIFIED, payload (" + payload.length + " B) equals "
                + compared + " member image(s)");
    }

    private String[] lastReady = {"", ""};
    private long nextReadyAtMs;

    /**
     * {@code GET /ready} of the in-JVM replica: {code, body}. At most one request per 200 ms (the answer before is
     * returned in between): a wait loop that called it every iteration would run the host out of ephemeral ports.
     */
    String[] ready() {
        final long now = System.currentTimeMillis();
        if (now >= nextReadyAtMs) {
            nextReadyAtMs = now + 200;
            lastReady = r.httpReady();
        }
        return lastReady;
    }

    // ---------------------------------------------------------------------------------------------- child replica

    /** The REPLICA credential the rigs' {@link TestAuth} accepts (the external-driver test's). */
    static final String REPLICA_CREDENTIAL = "8:REPLICA:" + "88".repeat(32);

    /**
     * The production replica's environment for a child ({@code java ... Main replica}) following this rig's members:
     * slot {@link ReplicaRig#SLOT} (the endpoint in the members' pull sources), {@code mode} embedded or external on
     * {@code aeronDir}.
     */
    Map<String, String> childEnv(final Path state, final String aeronDir, final int nodePort, final String mode) {
        final Map<String, String> env = new java.util.HashMap<>();
        env.put("TRANSPORT_DRIVER_MODE", mode);
        env.put("AERON_DIR", aeronDir);
        env.put("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "30000");
        env.put("TRANSPORT_IDLE_MODE", "backoff");
        env.put("CLUSTER_ADDRESSES", String.join(",", rig.hosts));
        env.put("CLUSTER_PORT_BASE", Integer.toString(rig.portBase));
        env.put("CLUSTER_PORT_STRIDE", Integer.toString(ReplicaRig.STRIDE));
        env.put("CLUSTER_REPLICA_SLOT", Integer.toString(ReplicaRig.SLOT));
        env.put("CLUSTER_REPLICA_ADDRESS", "localhost");
        env.put("BASE_DIR", state.toString());
        env.put("CORE_AUTH_CREDENTIAL", REPLICA_CREDENTIAL);
        env.put("CORE_NODE_PORT", Integer.toString(nodePort));
        env.put("REPLICA_BACKUP_QUERY_INTERVAL_MS", "12000");
        env.put("REPLICA_SHADOW_BUFFER_MB", Integer.toString(BUFFER_MB));
        env.put("SHADOW_TERM_LENGTH", "1m");
        env.put("SHADOW_SCAN_PERIOD_MS", "500");
        env.put("SHADOW_DEEP_READ_PACE_MS", "0");
        return env;
    }

    /** The newest {@code /metrics} body of the child replica, fetched off the polling thread. */
    private volatile String childMetricsBody = "";
    private volatile boolean childPolling;
    private Thread childPoller;

    /**
     * The child's applied and recorded positions feed the sampler: a daemon thread fetches the child's {@code /metrics}
     * at the sample rate and the sampler reads the newest body, so an HTTP read never stalls the thread that polls the
     * clients' egress (a stalled poll delays the generator's answers and shows as re-offers).
     */
    void sampleChild(final int nodePort) {
        if (childPoller == null) {
            childPolling = true;
            childPoller = new Thread(() -> {
                while (childPolling) {
                    childMetricsBody = fetchMetrics(nodePort);
                    try {
                        Thread.sleep(1_000L / HZ);
                    } catch (final InterruptedException e) {
                        return;
                    }
                }
            }, "child-metrics-" + nodePort);
            childPoller.setDaemon(true);
            childPoller.start();
        }
        sampler.attach(() -> childMetric("shadow_replica_applied_position"),
                () -> childMetric("shadow_replica_recorded_position"));
    }

    /** One value of the child's newest {@code /metrics} body (exact name with labels), -1 when absent. */
    long childMetric(final String name) {
        return metricOf(childMetricsBody, name);
    }

    static long metricOf(final String body, final String name) {
        for (final String line : body.split("\n")) {
            if (line.startsWith(name + " ")) {
                try {
                    return Long.parseLong(line.substring(name.length() + 1).trim());
                } catch (final NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    /** One value of the child's {@code /metrics} read now (exact name with labels), -1 when absent or unreachable. */
    static long childMetric(final int port, final String name) {
        return metricOf(fetchMetrics(port), name);
    }

    private static String fetchMetrics(final int port) {
        try {
            final java.net.HttpURLConnection c = (java.net.HttpURLConnection)java.net.URI.create("http://127.0.0.1:"
                    + port + "/metrics").toURL().openConnection();
            c.setConnectTimeout(500);
            c.setReadTimeout(1_000);
            try (java.io.InputStream in = c.getInputStream()) {
                return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            }
        } catch (final IOException e) {
            return "";
        }
    }

    /** {@code GET /ready} of a child: {code, body}; {"-1", error} when unreachable. */
    static String[] childReady(final int port) {
        try {
            final java.net.HttpURLConnection c = (java.net.HttpURLConnection)java.net.URI.create("http://127.0.0.1:"
                    + port + "/ready").toURL().openConnection();
            c.setConnectTimeout(500);
            c.setReadTimeout(1_000);
            final int code = c.getResponseCode();
            final java.io.InputStream in = code >= 400 ? c.getErrorStream() : c.getInputStream();
            final String body = in == null ? ""
                    : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            c.disconnect();
            return new String[] {Integer.toString(code), body.trim()};
        } catch (final IOException e) {
            return new String[] {"-1", String.valueOf(e)};
        }
    }

    /** Pumps the rig (load and sampler) until the child reads {@code /ready} 200; fails if it exits. */
    void awaitChildReady(final ReplicaChild child, final int port, final long timeoutMs) {
        final String[][] last = {{"", ""}};
        final long[] nextAt = {0};
        rig.await(() -> {
            if (!child.process.isAlive()) {
                throw new AssertionError(child.name + " exited (" + child.process.exitValue() + ") before /ready"
                        + tails(child));
            }
            if (System.currentTimeMillis() < nextAt[0]) {
                return false; // one HTTP read per 200 ms: the egress poll of this thread must not stall
            }
            nextAt[0] = System.currentTimeMillis() + 200;
            last[0] = childReady(port);
            return "200".equals(last[0][0]);
        }, timeoutMs, () -> "no /ready 200 from " + child.name + " within " + timeoutMs + " ms (last " + last[0][0]
                + " " + last[0][1] + ")" + tails(child));
    }

    /** Pumps the rig until the child's output holds {@code needle}; returns the line. */
    String awaitChildLine(final ReplicaChild child, final String needle, final long timeoutMs) throws Exception {
        child.awaitLine(needle, timeoutMs, () -> rig.pumpMs(100));
        return ReplicaChild.line(child.out() + child.err(), needle);
    }

    static String tails(final ReplicaChild child) {
        try {
            return child.tails();
        } catch (final IOException e) {
            return " (no output: " + e + ")";
        }
    }

    @Override
    public void close() {
        rig.duringAwait.clear();
        childPolling = false;
        if (childPoller != null) {
            childPoller.interrupt();
        }
        sampler.close();
        try {
            r.close();
        } finally {
            CloseHelper.quietClose(clientDriver);
            IoUtil.delete(clientDir, true);
        }
    }

    // ---------------------------------------------------------------------------------------------- the sampler

    /**
     * The lag sampler: at {@code core.lag.hz}, {@code commit - applied} (exact) and {@code core_log_position - applied}
     * (the gauge proxy, the leader's), plus the replica's recorded position; one TSV row per sample. The windows
     * ({@link #mark}) give the maximum since a mark (the pause control).
     */
    final class Sampler implements AutoCloseable {
        private final long periodNs = TimeUnit.SECONDS.toNanos(1) / HZ;
        private final File dir;
        private final long term;
        private PrintStream tsv;
        private LongSupplier applied = () -> -1;
        private LongSupplier recorded = () -> -1;
        private boolean on;
        private long nextNs;
        private long startNs;
        long samples;
        long maxCommitLag = Long.MIN_VALUE;
        long maxGaugeLag = Long.MIN_VALUE;
        long maxInternalLag = Long.MIN_VALUE;
        long maxProxyError = Long.MIN_VALUE;
        long lastCommitLag = Long.MIN_VALUE;
        long windowMax = Long.MIN_VALUE;
        long nonZero;

        Sampler(final String row, final long term) {
            this.dir = new File(System.getProperty("core.lag.out", "target/lag"), row);
            this.term = term;
        }

        void attach(final LongSupplier applied, final LongSupplier recorded) {
            this.applied = applied;
            this.recorded = recorded;
        }

        void start() {
            try {
                Files.createDirectories(dir.toPath());
                tsv = new PrintStream(Files.newOutputStream(dir.toPath().resolve("lag.tsv")), false, "UTF-8");
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            tsv.println("t_ms\tcommit\tcore_log_position\tapplied\trecorded\tcommit_minus_applied"
                    + "\tgauge_minus_applied\tinternal_lag");
            startNs = System.nanoTime();
            nextNs = startNs;
            on = true;
        }

        /** Starts a window: {@link #windowMax} from here. */
        void mark() {
            windowMax = Long.MIN_VALUE;
        }

        void sample(final ShadowClusterRig rig) {
            if (!on) {
                return;
            }
            final long now = System.nanoTime();
            if (now < nextNs) {
                return;
            }
            nextNs += periodNs;
            if (nextNs < now) {
                nextNs = now + periodNs; // a stalled wait: no burst of catch-up samples
            }
            final long a = applied.getAsLong();
            if (a < 0) {
                return;
            }
            final long commit = commit();
            final Member leader = leaderOrNull();
            final long gauge = leader == null ? -1 : ServiceProbe.coreLogPosition(leader.service);
            final long rec = recorded.getAsLong();
            final long lag = commit - a;
            final long gaugeLag = gauge < 0 ? Long.MIN_VALUE : gauge - a;
            final long internal = rec < 0 ? Long.MIN_VALUE : rec - a;
            samples++;
            lastCommitLag = lag;
            maxCommitLag = Math.max(maxCommitLag, lag);
            windowMax = Math.max(windowMax, lag);
            if (lag > 0) {
                nonZero++;
            }
            if (gauge >= 0) {
                maxGaugeLag = Math.max(maxGaugeLag, gaugeLag);
                maxProxyError = Math.max(maxProxyError, commit - gauge);
            }
            if (rec >= 0) {
                maxInternalLag = Math.max(maxInternalLag, internal);
            }
            tsv.printf(Locale.ROOT, "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s%n", TimeUnit.NANOSECONDS.toMillis(now - startNs),
                    commit, gauge, a, rec, lag, gaugeLag == Long.MIN_VALUE ? "" : Long.toString(gaugeLag),
                    internal == Long.MIN_VALUE ? "" : Long.toString(internal));
        }

        double achievedHz() {
            final double s = (System.nanoTime() - startNs) / 1e9;
            return s <= 0 ? 0 : samples / s;
        }

        String summary() {
            return String.format(Locale.ROOT, "samples %d (%.1f Hz achieved of %d), max commit-applied %d B, max"
                            + " core_log_position-applied %d B, max commit-core_log_position %d B, max recorded-applied"
                            + " %d B, non-zero samples %d, term %d B", samples, achievedHz(), HZ, maxCommitLag,
                    maxGaugeLag, maxProxyError, maxInternalLag, nonZero, term);
        }

        /** The row's summary file: the settings, the sampler's maxima and {@code extra}. */
        void writeSummary(final Map<String, Object> extra) {
            final Map<String, Object> m = new LinkedHashMap<>(settings());
            m.put("term_bytes", term);
            m.put("samples", samples);
            m.put("achieved_hz", String.format(Locale.ROOT, "%.2f", achievedHz()));
            m.put("max_commit_minus_applied", maxCommitLag);
            m.put("max_core_log_position_minus_applied", maxGaugeLag);
            m.put("max_commit_minus_core_log_position", maxProxyError);
            m.put("max_recorded_minus_applied", maxInternalLag);
            m.put("nonzero_samples", nonZero);
            m.putAll(extra);
            try {
                Files.createDirectories(dir.toPath());
                final StringBuilder sb = new StringBuilder();
                m.forEach((k, v) -> sb.append(k).append('\t').append(v).append('\n'));
                Files.writeString(dir.toPath().resolve("summary.txt"), sb.toString());
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            System.out.println("[lag] " + row + " summary " + m + " in " + dir);
        }

        Path dir() {
            return dir.toPath();
        }

        /** The vacuity check: lag observed at all. */
        void assertObserved() {
            if (samples == 0 || nonZero == 0) {
                throw new AssertionError("no lag observed: the load did not reach the replica (" + summary() + ")");
            }
        }

        /** The pass condition of the lag clause: max commit-applied inside the term, and lag observed at all. */
        void assertInsideTheTerm() {
            assertObserved();
            if (maxCommitLag > term) {
                throw new AssertionError("max commit-applied " + maxCommitLag + " B above the term " + term + " B ("
                        + summary() + ")");
            }
        }

        @Override
        public void close() {
            on = false;
            if (tsv != null) {
                tsv.close();
            }
        }
    }
}

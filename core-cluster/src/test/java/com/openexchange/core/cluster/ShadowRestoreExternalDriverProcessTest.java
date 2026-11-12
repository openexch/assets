// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.CoreRestoreRig.Listener;
import com.openexchange.core.cluster.CoreRestoreRig.Session;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import com.openexchange.core.infrastructure.persistence.SnapshotModeSettings;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The production entry point ({@code com.openexchange.core.Main}) in a child
 * JVM on an EXTERNAL media driver ({@code io.aeron.driver.MediaDriver}, another child JVM), the
 * {@code EngineDriverLifecycleProcessTest} shape. A single member in stub mode ({@code SNAPSHOT_MODE=shadow}); shadows come
 * from the contract 11.0 producer, run here in the test JVM as a client of the same external driver and of the child's
 * archive (its local IPC control channel). Rows:
 * <ul>
 *   <li>kill -9 of the engine, restart on the external driver: {@code restored path=EQUAL}, {@code /ready} within
 *       {@code SNAPSHOT_RESTORE_READY_BUDGET_MS}; the node logs {@code Driver mode EXTERNAL} (the contract's embedded-restart control
 *       has no refusal to show: embedded mode derives its own driver directory, {@code ClusterConfig:108});</li>
 *   <li>a TAIL restore paused by {@code Faults.afterTailFrames(n)} (review F8), kill -9 of the DRIVER during the pause:
 *       the engine exits 1 within the client driver timeout plus the halt ({@code EDLPT:59-60} numbers), stderr names
 *       the driver loss (or a {@code ReaderStop});</li>
 *   <li>journal-writer order (review F3, rule 12.14): a TAIL that moves more money than the 16 MiB ring holds, with the
 *       journal writer agent stopped by a {@code Faults.afterTailFrames} hook, ends in the
 *       {@code SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS} halt {@code tail replay timed out at <p> (journal ring full?)}, never a
 *       silent hang. The embedded leg of this row runs in {@link ShadowRestoreClusterTest}'s rig.</li>
 *   <li>the follower producer on external drivers (two members, nightly with {@code -Dcore.producer.external=true}):
 *       the designated follower writes a VERIFIED ALIGNED, its log image carries the URI's 16m receiver window and
 *       socket buffer under a driver whose own defaults are 128k and 0, and a driver kill inside its write exits 1
 *       with no write completed after the kill.</li>
 * </ul>
 * The faults are installed inside the child through JDWP at the service's {@code onStart} breakpoint
 * ({@code ChildFaults}, the {@code EngineDriverLifecycleProcessTest.OnStartHold} technique): the production entry point
 * offers no other handle on its service instance.
 */
public class ShadowRestoreExternalDriverProcessTest {

    private static final String ENGINE_MAIN = "com.openexchange.core.Main";
    private static final String SERVICE_CLASS = "com.openexchange.core.infrastructure.persistence.CoreClusteredService";
    private static final String CHILD_FAULTS = "com.openexchange.core.infrastructure.persistence.ChildFaults";
    private static final long GENESIS = ShadowClusterRig.GENESIS;
    private static final Pattern PATH = Pattern.compile("restored path=(\\w+) P_s=(\\d+) P_cm=(\\d+)");

    private final List<Process> processes = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private final List<AutoCloseable> clients = new ArrayList<>();

    @Before
    public void defaultStride() {
        System.clearProperty("cluster.port.stride");
        ClusterPorts.resetStrideForTest();
    }

    @After
    public void cleanUp() throws Exception {
        for (int i = clients.size() - 1; i >= 0; i--) {
            CloseHelper.quietClose(clients.get(i));
        }
        for (final Process p : processes) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        for (final Path d : dirs) {
            deleteTree(d);
        }
    }

    // ---------------------------------------------------------------------------------------------------- rows

    @Test(timeout = 600_000)
    public void aStubMemberOnAnExternalDriverRestoresEqualWithinTheBudget() throws Exception {
        final Node node = new Node("rcs-s4-11.14-equal-", 26100);
        node.startDriver();
        node.startEngine(Map.of(), List.of());
        node.awaitReady(90_000);
        final Clients c = node.clients();
        c.adopt();
        c.deposits(200);
        final long pCm = node.snapshot();
        c.deposits(30);
        node.quiet();
        final RecordedLog log = node.captureLog();
        final Written w = node.producer().produce(new Who(ProducerKind.FOLLOWER, 800), log, pCm);
        assertTrue(w.toString(), w.ok());
        c.close();
        node.killEngine();

        final long t0 = System.nanoTime();
        node.startEngine(Map.of(), List.of());
        node.awaitReady(90_000);
        final long readyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        final String out = node.log();
        final Matcher m = PATH.matcher(out);
        assertTrue("a restore path line: " + tail(out), m.find());
        assertEquals("EQUAL: " + m.group(), "EQUAL", m.group(1));
        assertEquals(pCm, Long.parseLong(m.group(2)));
        assertTrue("external driver mode: " + tail(out), out.contains("Driver mode EXTERNAL"));
        final long budget = SnapshotModeSettings.defaults().readyBudgetMs();
        System.out.println("GREEN 11.14 external EQUAL: " + m.group() + " readyMs=" + readyMs + " (from spawn, budget "
                + budget + ")");
        assertTrue("ready within SNAPSHOT_RESTORE_READY_BUDGET_MS " + budget + ": " + readyMs, readyMs <= budget);

        // the leg ran external: the node logged it, and the driver-kill row below exits 1 on the driver's death
    }

    @Test(timeout = 600_000)
    public void aDriverKillDuringAPausedTailExitsOne() throws Exception {
        final Node node = new Node("rcs-s4-11.14-drvkill-", 26200);
        node.startDriver();
        node.startEngine(Map.of(), List.of());
        node.awaitReady(90_000);
        final Clients c = node.clients();
        c.adopt();
        c.deposits(100);
        node.quiet();
        final long pS = node.lastFrameEnd();
        c.deposits(200);
        final long pCm = node.snapshot();
        node.quiet();
        final RecordedLog log = node.captureLog();
        assertTrue("P_s is a frame end", Arrays.binarySearch(log.positions(), pS) >= 0);
        final Written w = node.producer().produce(new Who(ProducerKind.FOLLOWER, 801), log, pS);
        assertTrue(w.toString(), w.ok());
        c.close();
        node.killEngine();

        final int jdwp = freePort();
        final Process engine = node.startEngine(Map.of(),
                List.of("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:" + jdwp));
        try (ChildFaultInstaller ignored = new ChildFaultInstaller(jdwp, "pauseTailAfter", 5)) {
            node.awaitLine("[restore-child] tail paused after 5 frame(s)", 90_000);
            node.closeAeron();
            final long t0 = System.nanoTime();
            node.driver.destroyForcibly(); // kill -9 of the media driver during the paused TAIL
            final boolean exited = engine.waitFor(45, TimeUnit.SECONDS);
            final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            final String out = node.log();
            assertTrue("the engine must exit after the driver kill (" + ms + " ms): " + tail(out), exited);
            System.out.println("GREEN 11.14 driver kill -9 during a paused TAIL (P_s=" + pS + " P_cm=" + pCm + "): exit="
                    + engine.exitValue() + " after " + ms + " ms; " + line(out, "[restore-child] tail paused") + " | "
                    + line(out, "media driver lost in EXTERNAL driver mode") + " | " + line(out, "tail reader stopped"));
            assertEquals("driver death during the tail exits 1: " + tail(out), 1, engine.exitValue());
            assertTrue("within the 10 s client driver timeout plus the halt: " + ms, ms < 18_000);
            assertTrue("stderr names the driver loss or the reader stop: " + tail(out),
                    out.contains("media driver lost in EXTERNAL driver mode") || out.contains("tail reader stopped"));
            assertFalse("no restore path line: the tail never finished", out.contains("restored path="));
            assertFalse("no JVM crash: " + tail(out), out.contains("A fatal error has been detected by the Java Runtime"));
        }
    }

    /**
     * The startup pull (3b) and the steady-state puller on an external driver, both fixed pull endpoints set (the
     * isolated-host shape): the source is a second archive in the test JVM (its own embedded driver, UDP control on a
     * free port) holding a genuine ALIGNED at P_cm; the child has no shadow of its own, so rules 1-3 find nothing and
     * 3b pulls it: {@code restored path=EQUAL}, {@code /ready}, SIGTERM exits 143. Then a 3b against a source that never
     * answers (the pulled copy purged by a boot without sources first): kill -9 of the driver during the pull exits 1 at
     * once. Then the steady-state puller running (the restore pulled again, the puller listing its source every
     * period): kill -9 of the driver exits 1 at once. About two minutes: nightly only
     * ({@code -Dcore.pull.external=true}).
     */
    @Test(timeout = 900_000)
    public void aStartupPullAndARunningPullerOnAnExternalDriverWithFixedEndpoints() throws Exception {
        org.junit.Assume.assumeTrue("the external-driver pull rows run only with -Dcore.pull.external=true",
                Boolean.getBoolean("core.pull.external"));
        final Node node = new Node("pull-external-", 50800);
        final int srcPort = freePort();
        final Path srcDir = dir("pull-external-source-");
        final Path srcAeron = shmDir("pull-external-source-aeron-");
        final io.aeron.archive.ArchivingMediaDriver source = io.aeron.archive.ArchivingMediaDriver.launch(
                new io.aeron.driver.MediaDriver.Context().aeronDirectoryName(srcAeron.toString())
                        .dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                        .threadingMode(io.aeron.driver.ThreadingMode.SHARED),
                new io.aeron.archive.Archive.Context().aeronDirectoryName(srcAeron.toString())
                        .archiveDir(srcDir.resolve("archive").toFile()).deleteArchiveOnStart(true)
                        .controlChannel("aeron:udp?endpoint=localhost:" + srcPort)
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .localControlChannel("aeron:ipc").recordingEventsEnabled(false).archiveId(0x5A5AL)
                        .threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED));
        clients.add(source);
        final Aeron srcAeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(srcAeron.toString()));
        clients.add(srcAeronClient);
        final AeronArchive srcArchive = AeronArchive.connect(new AeronArchive.Context().aeron(srcAeronClient)
                .ownsAeronClient(false).controlRequestChannel("aeron:ipc").controlResponseChannel("aeron:ipc"));
        clients.add(srcArchive);

        node.startDriver();
        node.startEngine(Map.of(), List.of());
        node.awaitReady(90_000);
        final Clients c = node.clients();
        c.adopt();
        c.deposits(200);
        final long pCm = node.snapshot();
        c.deposits(30);
        node.quiet();
        final RecordedLog log = node.captureLog();
        final Written w = new TestShadowProducer(srcAeronClient, srcArchive, layoutClusterId(node),
                srcDir.resolve("archive").toFile()).produce(new Who(ProducerKind.FOLLOWER, 850), log, pCm);
        assertTrue(w.toString(), w.ok());
        c.close();
        node.killEngine();

        final Map<String, String> pulling = Map.of("SHADOW_PULL_SOURCES", "localhost:" + srcPort,
                "SHADOW_PULL_REPLICATION_ENDPOINT", "localhost:" + freePort(),
                "SHADOW_PULL_RESPONSE_ENDPOINT", "localhost:" + freePort(),
                "SHADOW_PULL_PERIOD_MS", "1000", "SHADOW_PULL_STARTUP_WAIT_MS", "30000");
        final Process first = node.startEngine(pulling, List.of());
        node.awaitReady(120_000);
        String out = node.log();
        final Matcher m = PATH.matcher(out);
        assertTrue("a restore path line: " + tail(out), m.find());
        assertEquals("EQUAL from the pulled shadow: " + m.group(), "EQUAL", m.group(1));
        assertEquals(pCm, Long.parseLong(m.group(2)));
        assertTrue("3b pulled: " + tail(out), out.contains("[shadow restore] 3b pulled shadow "));
        assertTrue("external driver mode: " + tail(out), out.contains("Driver mode EXTERNAL"));
        System.out.println("GREEN external 3b: " + m.group() + " | " + line(out, "3b pulled shadow"));
        first.destroy(); // SIGTERM
        assertTrue("the node stops on SIGTERM", first.waitFor(60, TimeUnit.SECONDS));
        assertEquals("SIGTERM exit: " + tail(node.log()), 143, first.exitValue());
        Thread.sleep(12_000);

        // no local shadow at all for the next rows: a boot without sources (EQUAL from the pulled copy, no puller)
        // purges the pulled copy through the test JVM's client, then stops
        final Process plain = node.startEngine(Map.of(), List.of());
        node.awaitReady(120_000);
        purgeShadows(node);
        node.closeArchive();
        plain.destroy();
        assertTrue("the plain node stops on SIGTERM", plain.waitFor(60, TimeUnit.SECONDS));
        Thread.sleep(12_000);

        // a 3b against a source that never answers: kill -9 of the driver during the pull exits 1 at once
        final Map<String, String> silent = Map.of("SHADOW_PULL_SOURCES", "localhost:" + freePort(),
                "SHADOW_PULL_REPLICATION_ENDPOINT", "localhost:" + freePort(),
                "SHADOW_PULL_RESPONSE_ENDPOINT", "localhost:" + freePort(), "SHADOW_PULL_STARTUP_WAIT_MS", "120000");
        final Process waiting = node.startEngine(silent, List.of());
        node.awaitLine("[shadow restore] 3b restore: pulling a shadow", 90_000);
        node.closeAeron();
        long t0 = System.nanoTime();
        node.driver.destroyForcibly();
        assertTrue("the node exits after the driver kill during 3b", waiting.waitFor(45, TimeUnit.SECONDS));
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        out = node.log();
        assertEquals("driver loss during 3b exits 1: " + tail(out), 1, waiting.exitValue());
        assertTrue("within the client driver timeout plus the halt: " + ms, ms < 18_000);
        assertFalse("no restore path line", out.contains("restored path="));
        System.out.println("GREEN external 3b driver kill: exit 1 after " + ms + " ms | "
                + line(out, "media driver lost in EXTERNAL driver mode"));
        Thread.sleep(12_000); // the dead node's mark files time out before a new node may claim them

        // the steady-state puller running: kill -9 of the driver exits 1 at once
        node.startDriver();
        final Process running = node.startEngine(pulling, List.of());
        node.awaitReady(120_000);
        node.awaitLine("[SHADOW-PULL] puller started", 30_000);
        node.closeAeron();
        t0 = System.nanoTime();
        node.driver.destroyForcibly();
        assertTrue("the node exits after the driver kill", running.waitFor(45, TimeUnit.SECONDS));
        ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        out = node.log();
        if (running.exitValue() != 1) {
            throw new AssertionError("external driver loss did not halt a pulling node: exit " + running.exitValue()
                    + tail(out));
        }
        assertTrue("within the client driver timeout plus the halt: " + ms, ms < 18_000);
        System.out.println("GREEN external steady-state puller driver kill: exit 1 after " + ms + " ms | "
                + line(out, "media driver lost in EXTERNAL driver mode"));
    }

    /**
     * How long the slow writer holds the first payload chunk of every follower ALIGNED write in the producer row: longer
     * than the 10 s client driver timeout plus the halt, so the write is still held when the member halts. (Measured
     * with a 5 s hold: the writer, the archive and the IPC publication share the dead driver's mapped buffers, so a
     * write can still complete between the driver's death and the client's keepalive timeout.)
     */
    private static final long PRODUCER_HOLD_MS = 30_000;
    private static final long LOG_RCV_WINDOW = 16L * 1024 * 1024;

    /**
     * The follower producer on external drivers: two members, each the production entry point in a child JVM on its own
     * aeronmd-shaped driver started with {@code aeron.rcv.initial.window.length=128k} and {@code aeron.socket.so_rcvbuf=0}
     * (so a 16m window on the follower's log image can only come from the log channel URI), the log channel at its
     * defaults ({@code term-length=64m}, {@code rcv-wnd=16m}, {@code so-rcvbuf=16m}, {@code so-sndbuf=16m}), the writer
     * slowed by a 30 s hold on its first payload chunk (installed through JDWP at {@code onStart}). Rows: both members
     * boot through the socket buffer probe in external mode; at P_cm1 the follower (the designated member of a two-member
     * cluster) writes an {@code F<id>} ALIGNED that its own archive holds VERIFIED; the follower's log image metadata
     * reads {@code receiverWindowLength} and {@code socketRcvbufLength} 16m; at P_cm2, inside the held write, kill -9 of
     * the follower's driver exits the follower 1 within the client driver timeout plus the halt, and no write completes
     * after the kill. Nightly only ({@code -Dcore.producer.external=true}); control
     * {@code -Dcore.producer.nc=PRODUCER_NONE} ({@code SHADOW_PRODUCER=none}) fails the first assertion.
     */
    @Test(timeout = 1_200_000)
    public void theDesignatedFollowerOnAnExternalDriverWritesAnAlignedAndADriverKillMidWriteExitsOne() throws Exception {
        org.junit.Assume.assumeTrue("the external-driver producer rows run only with -Dcore.producer.external=true",
                Boolean.getBoolean("core.producer.external"));
        final boolean producerNone = "PRODUCER_NONE".equals(System.getProperty("core.producer.nc", ""));
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        final int portBase = 58400;
        final List<Node> nodes = List.of(new Node("producer-external-0-", portBase, 0, 2),
                new Node("producer-external-1-", portBase, 1, 2));
        final Map<String, String> env = Map.of("SHADOW_PRODUCER", producerNone ? "none" : "designated-follower",
                "SHADOW_PRODUCER_BUFFER_MB", "16", "SHADOW_SCAN_PERIOD_MS", "500", "SHADOW_DEEP_READ_PACE_MS", "0",
                "TRANSPORT_LOG_TERM_LENGTH", "64m", "TRANSPORT_LOG_RCV_WINDOW", "16m",
                "TRANSPORT_LOG_SO_RCVBUF", "16m", "TRANSPORT_LOG_SO_SNDBUF", "16m");
        final List<String> driverProps = List.of("-Daeron.rcv.initial.window.length=131072",
                "-Daeron.socket.so_rcvbuf=0");
        final List<ChildFaultInstaller> installers = new ArrayList<>();
        for (final Node n : nodes) {
            n.startDriver(driverProps);
            final int jdwp = freePort();
            n.startEngine(env, List.of("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:"
                    + jdwp));
            installers.add(new ChildFaultInstaller(jdwp, "slowShadowWriter", PRODUCER_HOLD_MS));
        }
        try {
            for (final Node n : nodes) {
                n.awaitReady(180_000);
                n.awaitLine("[restore-child] installed a slow shadow writer", 10_000);
            }
        } finally {
            for (final ChildFaultInstaller i : installers) {
                i.close();
            }
        }
        for (final Node n : nodes) {
            final String out = n.log();
            assertTrue("external driver mode on member " + n.nodeId + ": " + tail(out),
                    out.contains("Driver mode EXTERNAL"));
            final String probe = line(out, "[BOOT-GUARD] log channel ");
            assertTrue("the socket buffer probe ran in external mode on member " + n.nodeId + ": " + tail(out),
                    probe != null && probe.contains("rcv-wnd=16m"));
            System.out.println("[producer-external] member " + n.nodeId + ": " + probe);
        }
        final Node leader = awaitLeader(nodes);
        final Node follower = nodes.get(1 - leader.nodeId);
        System.out.println("[producer-external] leader member " + leader.nodeId + ", designated follower member "
                + follower.nodeId);

        // P_cm1: the designated follower writes its ALIGNED on the external driver; its own archive holds it VERIFIED
        final Clients c = new Clients(nodes);
        c.adopt();
        c.deposits(200);
        for (final Node n : nodes) { // the writer arms on the first housekeeping wake after the ADOPT (none: never)
            if (!producerNone) {
                n.awaitLine("[SHADOW-PRODUCER] writer armed: producer=F" + n.nodeId, 60_000);
            }
        }
        final long p1 = leader.snapshot();
        c.close(); // the waits below do not poll the sessions (their keepalive would lapse)
        awaitSnapshotAt(follower, p1);
        try {
            follower.awaitLine("[SHADOW-PRODUCER] wrote the ALIGNED at P_cm=" + p1 + ":", PRODUCER_HOLD_MS + 60_000);
        } catch (final AssertionError none) {
            throw new AssertionError("shadow_write_total{kind=ALIGNED} == 1 fails on member " + follower.nodeId
                    + ": no ALIGNED at P_cm=" + p1 + " from the designated follower on an external driver: "
                    + line(follower.log(), "logPosition=" + p1 + " "));
        }
        final com.openexchange.cluster.shadow.ShadowEntry written = verifiedCopy(follower, p1);
        System.out.println("GREEN external producer: member " + follower.nodeId + " wrote and holds VERIFIED "
                + written.alias() + " at P_cm=" + p1 + " | " + line(follower.log(), "wrote the ALIGNED at P_cm=" + p1));

        // the follower's log image: the window and the socket buffer come from the URI, not the driver defaults
        final long[] window = logImageWindow(follower, 64L * 1024 * 1024);
        assertEquals("receiverWindowLength of the follower's log image under the external driver (driver default 128k)",
                LOG_RCV_WINDOW, window[0]);
        assertEquals("socketRcvbufLength of the follower's log image under the external driver (driver default 0)",
                LOG_RCV_WINDOW, window[1]);
        System.out.println("GREEN external log image: receiverWindowLength=" + window[0] + " socketRcvbufLength="
                + window[1] + " (aeronmd started with rcv initial window 131072, so_rcvbuf 0)");

        // P_cm2: kill -9 of the follower's driver inside the held write
        final Clients c2 = new Clients(nodes);
        c2.deposits(50);
        c2.close();
        final int held = count(follower.log(), "holding the first payload chunk");
        final long p2 = leader.snapshot();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (count(follower.log(), "holding the first payload chunk") <= held) {
            assertTrue("the follower never started its write at P_cm2=" + p2 + ": " + tail(follower.log()),
                    System.nanoTime() < deadline);
            Thread.sleep(20);
        }
        for (final Node n : nodes) {
            n.closeAeron();
        }
        final long t0 = System.nanoTime();
        follower.driver.destroyForcibly(); // kill -9 of the follower's media driver during its ALIGNED write
        assertTrue("the follower exits after its driver kill: " + tail(follower.log()),
                follower.engine.waitFor(45, TimeUnit.SECONDS));
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        final String out = follower.log();
        if (follower.engine.exitValue() != 1) {
            throw new AssertionError("external driver loss did not halt a producing node: exit "
                    + follower.engine.exitValue() + tail(out));
        }
        assertTrue("within the client driver timeout plus the halt: " + ms, ms < 18_000);
        assertFalse("no ALIGNED completed after the kill: " + tail(out),
                out.contains("[SHADOW-PRODUCER] wrote the ALIGNED at P_cm=" + p2 + ":"));
        assertEquals("shadow_write_total{ALIGNED} did not move after the kill", 1,
                count(out, "[SHADOW-PRODUCER] wrote the ALIGNED at P_cm="));
        System.out.println("GREEN external producer driver kill mid-write at P_cm2=" + p2 + ": exit 1 after " + ms
                + " ms | " + line(out, "media driver lost in EXTERNAL driver mode"));
    }

    /** The member whose role counter reads LEADER. */
    private static Node awaitLeader(final List<Node> nodes) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            for (final Node n : nodes) {
                if (n.role() == io.aeron.cluster.service.Cluster.Role.LEADER) {
                    return n;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no leader among the external-driver members");
    }

    private static void awaitSnapshotAt(final Node node, final long p) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (node.latestSnapshot() < p) {
            assertTrue("member " + node.nodeId + " never recorded the snapshot at " + p, System.nanoTime() < deadline);
            Thread.sleep(50);
        }
    }

    /** The follower's own {@code F<id>} ALIGNED at exactly {@code p}, deep-read VERIFIED through its archive. */
    private static com.openexchange.cluster.shadow.ShadowEntry verifiedCopy(final Node node, final long p)
            throws InterruptedException {
        final com.openexchange.cluster.shadow.ShadowCatalog catalog = new com.openexchange.cluster.shadow.ShadowCatalog(
                node.aeron(), node.archive(), com.openexchange.core.infrastructure.persistence.CoreLogApplier.ENGINE_ID_CORE,
                layoutClusterId(node), GENESIS, new com.openexchange.cluster.shadow.ShadowVerdicts(),
                com.openexchange.cluster.shadow.ShadowCatalog.DEFAULT_LIST_BATCH,
                org.agrona.concurrent.SystemNanoClock.INSTANCE, TimeUnit.SECONDS.toNanos(30),
                new com.openexchange.cluster.shadow.ShadowCounters());
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        String last = "no scan";
        while (System.nanoTime() < deadline) {
            final com.openexchange.cluster.shadow.ShadowCatalog.Scan scan = catalog.scan();
            for (final com.openexchange.cluster.shadow.ShadowEntry e : scan.all()) {
                if (e.alias() != null && e.alias().logPosition() == p && e.header() != null
                        && e.header().producerKind() == ProducerKind.FOLLOWER.code()
                        && e.header().producerId() == node.nodeId
                        && e.header().kindEnum() == com.openexchange.cluster.shadow.ShadowKind.ALIGNED) {
                    final com.openexchange.cluster.shadow.ShadowCatalog.DeepResult r = catalog.verifyDeep(e);
                    last = e.state() + " deep " + r.kind();
                    if (r.kind() == com.openexchange.cluster.shadow.ShadowCatalog.DeepResult.Kind.VERIFIED) {
                        return e;
                    }
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError("no VERIFIED F" + node.nodeId + " ALIGNED at " + p + " in member " + node.nodeId
                + "'s archive: " + last);
    }

    /**
     * {@code {receiverWindowLength, socketRcvbufLength}} from the metadata of the node's image log buffer whose term
     * length is {@code termLength} (the cluster log; no other image of a member uses that term length).
     */
    private static long[] logImageWindow(final Node node, final long termLength) throws IOException {
        final List<long[]> found = new ArrayList<>();
        final List<String> seen = new ArrayList<>();
        try (Stream<Path> files = Files.list(node.aeronDir.resolve("images"))) {
            for (final Path f : files.filter(x -> x.toString().endsWith(".logbuffer")).toList()) {
                try (io.aeron.LogBuffers lb = new io.aeron.LogBuffers(f.toString())) {
                    final org.agrona.concurrent.UnsafeBuffer md = lb.metaDataBuffer();
                    seen.add(f.getFileName() + " term=" + lb.termLength() + " rcvWnd="
                            + io.aeron.logbuffer.LogBufferDescriptor.receiverWindowLength(md));
                    if (lb.termLength() == termLength) {
                        found.add(new long[] {io.aeron.logbuffer.LogBufferDescriptor.receiverWindowLength(md),
                                io.aeron.logbuffer.LogBufferDescriptor.socketRcvbufLength(md)});
                    }
                }
            }
        }
        assertEquals("one log image on member " + node.nodeId + ": " + seen, 1, found.size());
        return found.get(0);
    }

    private static int count(final String out, final String needle) {
        int n = 0;
        for (int i = out.indexOf(needle); i >= 0; i = out.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }

    private static int layoutClusterId(final Node node) {
        return node.layout.consensusModuleContext().clusterId();
    }

    /** Purges every stopped shadow recording of the child's archive (through the test JVM's IPC client). */
    private static void purgeShadows(final Node node) {
        final List<Long> ids = new ArrayList<>();
        node.archive().listRecordingsForUri(0, 10_000, com.openexchange.cluster.shadow.ShadowAlias.prefix(
                        com.openexchange.core.infrastructure.persistence.CoreLogApplier.ENGINE_ID_CORE),
                com.openexchange.cluster.shadow.ShadowAlias.STREAM_ID,
                (cs, corr, id, st, sp, startPosition, stopPosition, it, sfl, tbl, mtu, sid, str, ch, oc, src) -> {
                    if (stopPosition != AeronArchive.NULL_POSITION) {
                        ids.add(id);
                    }
                });
        for (final long id : ids) {
            node.archive().purgeRecording(id);
        }
        assertTrue("the pulled copy was purged", !ids.isEmpty());
    }

    /**
     * Q9 drill, child leg (D6): the engine halts ({@code Runtime.halt}) right after {@code onTakeSnapshot}
     * returns, before Aeron's {@code awaitRecordingComplete} and the service ack, at least 5 times. Proof that a kill
     * landed in the window: right after the halt, the snapshot's group is NOT in {@code RecordingLog}. Every restart
     * recovers from the newest group {@code RecordingLog} holds at that boot: EQUAL from the baseline stub group (its
     * shadow), or from a stub group the NEXT boot wrote when its recovery replay re-ran the halted
     * {@code ClusterAction(SNAPSHOT)} (the halt hook lets a snapshot through until the node leads; that group has no
     * shadow of its own, so the restore after it is TAIL from the baseline shadow); {@code /ready} and a live deposit
     * ack after each restart; never {@code unknown recording id}.
     */
    @Test(timeout = 1_500_000)
    public void q9HaltRightAfterOnTakeSnapshotInAChildJvmLeavesNoGroupAndEveryRestartRecovers() throws Exception {
        final int halts = Math.max(5, Integer.getInteger("snapshot.restore.halts", 5));
        final int exitCode = 86;
        final Node node = new Node("rcs-s4-11.13-q9-child-", 26500);
        node.startDriver();
        node.startEngine(Map.of(), List.of());
        node.awaitReady(90_000);
        Clients c = node.clients();
        c.adopt();
        c.deposits(100);
        final long baseline = node.snapshot();
        c.deposits(20);
        node.quiet();
        final Written w = node.producer().produce(new Who(ProducerKind.FOLLOWER, 810), node.captureLog(), baseline);
        assertTrue(w.toString(), w.ok());
        c.close();
        node.killEngine();

        int inWindow = 0;
        for (int i = 1; i <= halts; i++) {
            final long newestAtBoot = node.latestSnapshot();
            final int jdwp = freePort();
            final Process engine = node.startEngine(Map.of(),
                    List.of("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:" + jdwp));
            try (ChildFaultInstaller ignored = new ChildFaultInstaller(jdwp, "haltAfterTakeSnapshot", exitCode)) {
                node.awaitReady(120_000);
                final String out = node.log();
                assertRecovered(out, newestAtBoot, baseline, "halt " + i);
                c = node.clients();
                c.deposits(10); // the restored node serves live commands
                final long groupBefore = node.latestSnapshot();
                final AtomicCounter toggle = ClusterControl.findControlToggle(node.aeron().countersReader(),
                        node.layout.consensusModuleContext().clusterId());
                assertTrue("the SNAPSHOT toggle", toggle != null && ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
                final boolean exited = engine.waitFor(60, TimeUnit.SECONDS);
                final String after = node.log();
                assertTrue("halt " + i + ": the engine must halt right after onTakeSnapshot: " + tail(after), exited);
                assertEquals("halt " + i + ": the seam's exit code: " + tail(after), exitCode, engine.exitValue());
                assertTrue(tail(after), after.contains("[restore-child] halting right after onTakeSnapshot returned"));
                c.close();
                node.closeArchive();
                Thread.sleep(12_000); // the dead clients' driver resources time out before a new node claims them
                final long groupAfter = node.latestSnapshot();
                final boolean written = groupAfter != groupBefore;
                if (!written) {
                    inWindow++;
                }
                System.out.println("[11.13-Q9-child] halt " + i + "/" + halts + ": group "
                        + (written ? "WRITTEN at " + groupAfter + " (the halt missed the window)"
                        : "NOT in RecordingLog (newest still " + groupBefore + "): the halt landed in the window"));
            }
        }
        final long newestAtLastBoot = node.latestSnapshot();
        node.startEngine(Map.of(), List.of());
        node.awaitReady(120_000);
        assertRecovered(node.log(), newestAtLastBoot, baseline, "final restart");
        c = node.clients();
        c.deposits(10);
        c.close();
        System.out.println("GREEN 11.13 Q9 child leg: " + halts + " halts right after onTakeSnapshot, " + inWindow
                + " landed in the window (no group in RecordingLog); every restart recovered");
        assertTrue("at least one halt landed in the window (its group is not in RecordingLog): " + inWindow,
                inWindow >= 1);
    }

    /**
     * The restore after a Q9 halt, from the newest group at this boot: EQUAL at the baseline (its shadow), or TAIL from
     * the baseline shadow when the newest group is a later stub (one a recovery replay wrote, no shadow at it).
     */
    private static void assertRecovered(final String out, final long newestGroup, final long baseline,
                                        final String when) {
        assertFalse(when + ": never unknown recording id: " + tail(out), out.contains("unknown recording id"));
        final Matcher m = PATH.matcher(out);
        assertTrue(when + ": a restore path line: " + tail(out), m.find());
        assertEquals(when + ": restored from the newest group at boot " + newestGroup + ": " + m.group(), newestGroup,
                Long.parseLong(m.group(3)));
        assertEquals(when + ": from the baseline shadow " + baseline + ": " + m.group(), baseline,
                Long.parseLong(m.group(2)));
        assertEquals(when + ": " + m.group(), newestGroup == baseline ? "EQUAL" : "TAIL", m.group(1));
        System.out.println("[11.13-Q9-child] " + when + " recovered: " + m.group());
    }

    @Test(timeout = 1_200_000)
    public void aTailWithTheJournalWriterStoppedTimesOutLoudlyOnTheExternalDriver() throws Exception {
        journalRow(26300, true);
    }

    /** Paired control: the same TAIL with the writer left running completes ({@code restored path=TAIL}). */
    @Test(timeout = 1_200_000)
    public void controlTheSameTailWithTheWriterRunningCompletesOnTheExternalDriver() throws Exception {
        journalRow(26400, false);
    }

    private void journalRow(final int portBase, final boolean stopWriter) throws Exception {
        final Node node = new Node("rcs-s4-11.14-journal-" + stopWriter + "-", portBase);
        node.startDriver();
        node.startEngine(Map.of(), List.of());
        node.awaitReady(90_000);
        final Clients c = node.clients();
        c.adopt();
        c.armJournal();
        node.awaitLine("SetMoneyJournal APPLIED: money journal ENABLED", 60_000);
        c.deposits(50);
        node.quiet();
        final long pS = node.lastFrameEnd();
        final RecordedLog early = node.captureLog();
        final Written w = node.producer().produce(new Who(ProducerKind.FOLLOWER, 802), early, pS);
        assertTrue(w.toString(), w.ok());
        // more journal entries in (P_s, P_cm] than the production 16 MiB ring holds (a deposit entry is 64 ring bytes)
        final int deposits = (16 * 1024 * 1024) / 64 + 20_000;
        final long d0 = System.nanoTime();
        try {
            c.deposits(deposits);
        } catch (final AssertionError e) {
            throw new AssertionError(e.getMessage() + "; engine log tail:" + tail(node.log()), e);
        }
        System.out.println("[11.14-ext] " + deposits + " deposits in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - d0)
                + " ms");
        final long pCm = node.snapshot();
        node.quiet();
        c.close();
        node.killEngine();

        final long timeoutMs = 5_000;
        if (!stopWriter) {
            node.startEngine(Map.of("SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS", Long.toString(timeoutMs)), List.of());
            node.awaitReady(120_000);
            final String out = node.log();
            final Matcher m = PATH.matcher(out);
            assertTrue("a restore path line: " + tail(out), m.find());
            assertEquals("TAIL from P_s=" + pS + ": " + m.group(), "TAIL", m.group(1));
            assertFalse("no timeout with the writer running", out.contains("tail replay timed out"));
            System.out.println("CONTROL 11.14 external, writer running: " + m.group() + "; " + line(out, "[shadow restore] tail ("));
            return;
        }
        final int jdwp = freePort();
        final Process engine = node.startEngine(Map.of("SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS", Long.toString(timeoutMs)),
                List.of("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:" + jdwp));
        try (ChildFaultInstaller ignored = new ChildFaultInstaller(jdwp, "stopJournalWriterAfter", 1)) {
            node.awaitLine("[restore-child] journal writer agent stopped", 90_000);
            final boolean exited = engine.waitFor(120, TimeUnit.SECONDS);
            final String out = node.log();
            assertTrue("the engine must not hang on a full ring: " + tail(out), exited);
            final Matcher at = Pattern.compile("tail replay timed out at (\\d+) \\(journal ring full\\?\\)").matcher(out);
            assertTrue("the SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS halt text: " + tail(out), at.find());
            final long p = Long.parseLong(at.group(1));
            System.out.println("GREEN 11.14 external journal-writer row: exit=" + engine.exitValue() + " halt at " + p
                    + " (P_s=" + pS + " P_cm=" + pCm + "): " + line(out, "tail replay timed out at"));
            assertTrue("the tail stopped inside (P_s, P_cm): " + pS + " < " + p + " < " + pCm, pS < p && p < pCm);
            assertTrue("the ring was full: " + tail(out), out.contains("MONEY JOURNAL STALLED"));
            assertTrue("a halt exits non-zero: " + engine.exitValue(), engine.exitValue() != 0);
            assertFalse("no restore path line", out.contains("restored path="));
        }
    }

    // ---------------------------------------------------------------------------------------------------- node

    /** One single-member node: the driver child, the engine child, the test JVM's clients and its directory layout. */
    private final class Node {
        final Path base;
        final Path aeronDir;
        final int portBase;
        final int probePort;
        final int nodeId;
        final int members;
        final ClusterConfig layout;
        Process driver;
        Process engine;
        Aeron aeron;
        AeronArchive archive;
        private int starts;

        Node(final String prefix, final int portBase) throws IOException {
            this(prefix, portBase, 0, 1);
        }

        /** Member {@code nodeId} of a {@code members}-member cluster on this host (port stride 100 when more than one). */
        Node(final String prefix, final int portBase, final int nodeId, final int members) throws IOException {
            this.base = dir(prefix);
            this.aeronDir = shmDir(prefix + "aeron-");
            this.portBase = portBase;
            this.probePort = freePort();
            this.nodeId = nodeId;
            this.members = members;
            // the same layout the child derives (CLUSTER_NODE, the hosts, BASE_DIR); built for its paths and contexts
            this.layout = ClusterConfig.create(nodeId, hosts(members), hosts(members), portBase,
                    new TimerPerOrderControlService());
            layout.baseDir(base.resolve("state").toFile());
            layout.aeronDirectoryName(aeronDir.toString());
        }

        File clusterDir() {
            return layout.consensusModuleContext().clusterDir();
        }

        void startDriver() throws Exception {
            startDriver(List.of());
        }

        /** An aeronmd-shaped driver process; {@code props} are extra {@code -D} driver settings. */
        void startDriver(final List<String> props) throws Exception {
            final List<String> cmd = java();
            cmd.addAll(props);
            cmd.addAll(List.of("-Daeron.dir=" + aeronDir, "-Daeron.dir.delete.on.start=true",
                    "-Daeron.dir.delete.on.shutdown=true", "-Daeron.term.buffer.sparse.file=true",
                    "-cp", System.getProperty("java.class.path"), "io.aeron.driver.MediaDriver"));
            driver = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(base.resolve("driver.log").toFile())
                    .start();
            processes.add(driver);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!Files.exists(aeronDir.resolve("cnc.dat"))) {
                assertTrue("driver did not start", System.nanoTime() < deadline && driver.isAlive());
                Thread.sleep(50);
            }
        }

        Path logFile() {
            return base.resolve("engine-" + starts + ".log");
        }

        String log() throws IOException {
            return Files.readString(logFile());
        }

        Process startEngine(final Map<String, String> extraEnv, final List<String> jvmArgs) throws IOException {
            starts++;
            final List<String> cmd = java();
            cmd.addAll(jvmArgs);
            cmd.addAll(List.of("-Dcore.auth.keys=" + TestAuth.KEYS, "-Dsnapshot.interval.minutes=0",
                    "-XX:ErrorFile=" + base.resolve("hs_err_pid%p.log"), "-cp", System.getProperty("java.class.path"),
                    ENGINE_MAIN));
            final ProcessBuilder pb = new ProcessBuilder(cmd);
            final Map<String, String> env = pb.environment();
            env.put("BASE_DIR", base.resolve("state").toString());
            env.put("AERON_DIR", aeronDir.toString());
            env.put("TRANSPORT_DRIVER_MODE", "external");
            env.put("TRANSPORT_IDLE_MODE", "backoff");
            env.put("TRANSPORT_LOG_TERM_LENGTH", "4m");
        env.put("TRANSPORT_LOG_RCV_WINDOW", "2m"); // at most half the log term (the boot refuses more)
        env.put("TRANSPORT_LOG_SO_RCVBUF", "2m");
        env.put("TRANSPORT_LOG_SO_SNDBUF", "2m");
            env.put("TRANSPORT_SNAPSHOT_TERM_LENGTH", "4m");
            env.put("CLUSTER_NODE", Integer.toString(nodeId));
            env.put("CLUSTER_ADDRESSES", String.join(",", hosts(members)));
            if (members > 1) {
                env.put("CLUSTER_PORT_STRIDE", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
            }
            env.put("CLUSTER_PORT_BASE", String.valueOf(portBase));
            env.put("CORE_NODE_PORT", String.valueOf(probePort));
            env.put("SNAPSHOT_MODE", "shadow");
            env.put("SHADOW_PRODUCER", "none"); // the test producer stands in for the follower here
            env.putAll(extraEnv);
            engine = pb.redirectErrorStream(true).redirectOutput(logFile().toFile()).start();
            processes.add(engine);
            return engine;
        }

        void killEngine() throws Exception {
            closeArchive();
            engine.destroyForcibly(); // kill -9
            assertTrue("the engine died", engine.waitFor(20, TimeUnit.SECONDS));
            // the killed client's mark files and driver resources time out before a new node may claim them
            Thread.sleep(12_000);
        }

        void awaitReady(final long timeoutMs) throws Exception {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (System.nanoTime() < deadline) {
                assertTrue("engine exited before /ready: " + tail(log()), engine.isAlive());
                try {
                    final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + probePort + "/ready")
                            .toURL().openConnection();
                    c.setConnectTimeout(500);
                    c.setReadTimeout(1_000);
                    if (c.getResponseCode() == 200) {
                        return;
                    }
                } catch (final IOException notYet) {
                    // probe not up yet
                }
                Thread.sleep(100);
            }
            throw new AssertionError("no /ready 200 within " + timeoutMs + " ms: " + tail(log()));
        }

        void awaitLine(final String needle, final long timeoutMs) throws Exception {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (!log().contains(needle)) {
                assertTrue("no <" + needle + "> within " + timeoutMs + " ms: " + tail(log()), System.nanoTime() < deadline);
                Thread.sleep(100);
            }
        }

        Aeron aeron() {
            if (aeron == null) {
                // never the default handler: it exits this JVM on a driver timeout, and these rows kill the driver
                aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir.toString())
                        .errorHandler(t -> System.out.println("[11.14] test-JVM Aeron client: " + t)));
                clients.add(aeron);
            }
            return aeron;
        }

        AeronArchive archive() {
            if (archive == null) {
                archive = AeronArchive.connect(layout.aeronArchiveContext().clone().aeron(aeron()).ownsAeronClient(false));
            }
            return archive;
        }

        void closeArchive() {
            CloseHelper.quietClose(archive);
            archive = null;
        }

        /** Before a driver kill: the test JVM's own clients of that driver go first. */
        void closeAeron() {
            closeArchive();
            CloseHelper.quietClose(aeron);
            aeron = null;
        }

        Clients clients() throws InterruptedException {
            return new Clients(List.of(this));
        }

        /** The consensus module's role counter on this node's driver ({@code Cluster.Role}), null before it exists. */
        io.aeron.cluster.service.Cluster.Role role() {
            final org.agrona.concurrent.status.CountersReader counters = aeron().countersReader();
            final io.aeron.cluster.service.Cluster.Role[] role = {null};
            counters.forEach((counterId, typeId, keyBuffer, label) -> {
                if (typeId == io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID
                        && counters.getCounterState(counterId) == org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED) {
                    role[0] = io.aeron.cluster.service.Cluster.Role.get(counters.getCounterValue(counterId));
                }
            });
            return role[0];
        }

        long logRecordingId() {
            try (RecordingLog l = new RecordingLog(clusterDir(), false)) {
                return l.findLastTermRecordingId();
            }
        }

        /** The log's recorded position once nothing is in flight: the end of the last frame. */
        long lastFrameEnd() {
            return archive().getRecordingPosition(logRecordingId());
        }

        /**
         * Lets the last acked command reach the recording. The log never stops moving (the leader's timers append
         * TimerEvent frames), so positions are taken from the recording, which is frame-aligned, never from a rest.
         */
        void quiet() throws InterruptedException {
            Thread.sleep(300);
        }

        RecordedLog captureLog() {
            return TestShadowProducer.captureLog(aeron(), archive(), logRecordingId(), 0, lastFrameEnd());
        }

        TestShadowProducer producer() {
            return new TestShadowProducer(aeron(), archive(), layout.consensusModuleContext().clusterId(),
                    layout.archiveContext().archiveDir());
        }

        long latestSnapshot() {
            long p = -1;
            try (RecordingLog l = new RecordingLog(clusterDir(), false)) {
                for (final RecordingLog.Entry e : l.entries()) {
                    if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == 0) {
                        p = Math.max(p, e.logPosition);
                    }
                }
            }
            return p;
        }

        /** The cluster control toggle through the external driver's counters; returns the new snapshot's position. */
        long snapshot() throws Exception {
            final long before = latestSnapshot();
            final AtomicCounter toggle = ClusterControl.findControlToggle(aeron().countersReader(),
                    layout.consensusModuleContext().clusterId());
            assertTrue("a control toggle on the external driver", toggle != null);
            assertTrue("the SNAPSHOT toggle", ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
            final long deadline = System.currentTimeMillis() + 60_000;
            while (latestSnapshot() == before) {
                assertTrue("a new snapshot group: " + tail(log()), System.currentTimeMillis() < deadline);
                Thread.sleep(100);
            }
            final long p = latestSnapshot();
            awaitLine("logPosition=" + p + " kind=stub", 10_000);
            return p;
        }
    }

    /** ADMIN and TOOL sessions of the test JVM on the external driver. */
    /** Deposits a test client keeps unacked; far below what fills the engine's 16 MiB per-session egress queue. */
    private static final long DEPOSITS_IN_FLIGHT = 8192;

    private final class Clients implements AutoCloseable {
        final Session admin;
        final Session tool;
        final List<Session> all = new ArrayList<>();
        long corr = 1;

        Clients(final List<Node> nodes) throws InterruptedException {
            clients.add(this);
            admin = session(nodes, SessionClass.ADMIN);
            tool = session(nodes, SessionClass.TOOL);
        }

        private Session session(final List<Node> nodes, final SessionClass cls) throws InterruptedException {
            final Node node = nodes.get(0);
            final StringBuilder endpoints = new StringBuilder();
            for (final Node n : nodes) {
                endpoints.append(endpoints.length() == 0 ? "" : ",").append(n.nodeId).append("=localhost:")
                        .append(ClusterConfig.calculatePort(n.nodeId, n.portBase,
                                ClusterConfig.CLIENT_FACING_PORT_OFFSET));
            }
            final Listener in = new Listener();
            final long deadline = System.currentTimeMillis() + 60_000;
            RuntimeException last = null;
            while (System.currentTimeMillis() < deadline) {
                try {
                    final AeronCluster client = AeronCluster.connect(new AeronCluster.Context()
                            .credentialsSupplier(TestAuth.credentials(cls))
                            .errorHandler(t -> System.out.println("[11.14] test-JVM cluster client: " + t))
                            .egressListener(in)
                            .egressChannel("aeron:udp?endpoint=localhost:0")
                            .ingressChannel("aeron:udp")
                            .aeronDirectoryName(node.aeronDir.toString())
                            .ingressEndpoints(endpoints.toString())
                            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
                    final Session s = new Session(client, in);
                    all.add(s);
                    return s;
                } catch (final RuntimeException e) {
                    last = e;
                    Thread.sleep(250);
                }
            }
            throw new IllegalStateException("could not connect a " + cls + " session", last);
        }

        void adopt() {
            final UnsafeBuffer buf = new UnsafeBuffer(new byte[4096]);
            CoreRestoreRig.offer(admin, buf, ShadowClusterRig.encodeAdopt(buf), all);
            CoreRestoreRig.await(all, () -> admin.in.count(f -> f.startsWith("ADMINACK ref=70 result=ADOPTED")) > 0,
                    System.currentTimeMillis() + 60_000, "the CoreConfig ADOPT (genesis " + GENESIS + ") must be acked");
        }

        void armJournal() {
            final UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
            CoreRestoreRig.offer(admin, buf, ShadowClusterRig.encodeArmJournal(buf), all);
        }

        void deposits(final int n) {
            final Encoders enc = new Encoders();
            long lastCorr = 0;
            for (int i = 0; i < n; i++) {
                lastCorr = corr++;
                CoreRestoreRig.offer(tool, enc.buf, enc.deposit(lastCorr, 1 + (i % 5), CoreRestoreRig.usd(),
                        ShadowClusterRig.ONE), all);
                // a bounded in-flight window: the engine closes a non-resumable session whose 16 MiB egress queue
                // fills (by design), so an unpaced flood loses every later ack when egress drains slowly (small
                // socket buffers on CI runners). Polling inside await refreshes lastDepositAckCorr.
                if (lastCorr - tool.in.lastDepositAckCorr > DEPOSITS_IN_FLIGHT) {
                    awaitDepositAck(lastCorr - DEPOSITS_IN_FLIGHT, 60_000);
                }
            }
            awaitDepositAck(lastCorr, 300_000);
        }

        private void awaitDepositAck(final long target, final long timeoutMs) {
            try {
                CoreRestoreRig.await(all, () -> tool.in.lastDepositAckCorr >= target,
                        System.currentTimeMillis() + timeoutMs, "deposit ack " + target);
            } catch (final AssertionError e) {
                throw new AssertionError(e.getMessage() + " (last acked " + tool.in.lastDepositAckCorr + ")", e);
            }
        }

        @Override
        public void close() {
            for (final Session s : all) {
                CloseHelper.quietClose(s.client);
            }
            all.clear();
        }
    }

    /**
     * JDWP: the child starts with {@code suspend=y}; at the service's {@code onStart} breakpoint this calls
     * {@code ChildFaults.<method>(service, n)} on the service thread (the child's own instance), then detaches.
     */
    static final class ChildFaultInstaller implements AutoCloseable {
        private final com.sun.jdi.VirtualMachine vm;
        private final Thread events;
        private volatile Throwable failure;

        ChildFaultInstaller(final int port, final String method, final long n) throws Exception {
            final com.sun.jdi.connect.AttachingConnector connector = com.sun.jdi.Bootstrap.virtualMachineManager()
                    .attachingConnectors().stream().filter(c -> c.name().equals("com.sun.jdi.SocketAttach"))
                    .findFirst().orElseThrow();
            final Map<String, com.sun.jdi.connect.Connector.Argument> args = connector.defaultArguments();
            args.get("hostname").setValue("127.0.0.1");
            args.get("port").setValue(Integer.toString(port));
            com.sun.jdi.VirtualMachine attached = null;
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (attached == null) {
                try {
                    attached = connector.attach(args);
                } catch (final IOException notYet) {
                    if (System.nanoTime() > deadline) {
                        throw notYet;
                    }
                    Thread.sleep(100);
                }
            }
            vm = attached;
            final com.sun.jdi.request.ClassPrepareRequest prepare = vm.eventRequestManager().createClassPrepareRequest();
            prepare.addClassFilter(SERVICE_CLASS);
            prepare.enable();
            events = new Thread(() -> {
                try {
                    while (true) {
                        final com.sun.jdi.event.EventSet set = vm.eventQueue().remove();
                        boolean done = false;
                        for (final com.sun.jdi.event.Event e : set) {
                            if (e instanceof com.sun.jdi.event.ClassPrepareEvent cp) {
                                final com.sun.jdi.Method onStart = cp.referenceType().methodsByName("onStart").get(0);
                                final com.sun.jdi.request.BreakpointRequest bp =
                                        vm.eventRequestManager().createBreakpointRequest(onStart.location());
                                bp.setSuspendPolicy(com.sun.jdi.request.EventRequest.SUSPEND_EVENT_THREAD);
                                bp.enable();
                            } else if (e instanceof com.sun.jdi.event.BreakpointEvent be) {
                                install(be.thread(), method, n);
                                vm.eventRequestManager().deleteAllBreakpoints();
                                done = true;
                            } else if (e instanceof com.sun.jdi.event.VMDisconnectEvent
                                    || e instanceof com.sun.jdi.event.VMDeathEvent) {
                                return;
                            }
                        }
                        set.resume();
                        if (done) {
                            vm.dispose();
                            return;
                        }
                    }
                } catch (final InterruptedException | com.sun.jdi.VMDisconnectedException finished) {
                    // the child exited
                } catch (final Throwable t) {
                    failure = t;
                    t.printStackTrace();
                }
            }, "rcs-11.14-jdwp");
            events.setDaemon(true);
            events.start();
            vm.resume(); // suspend=y: the child waits for the debugger before main
        }

        private void install(final com.sun.jdi.ThreadReference thread, final String method, final long n)
                throws Exception {
            final com.sun.jdi.ObjectReference service = thread.frame(0).thisObject();
            final com.sun.jdi.ClassLoaderReference loader = service.referenceType().classLoader();
            final com.sun.jdi.ClassType classType = (com.sun.jdi.ClassType)vm.classesByName("java.lang.Class").get(0);
            final com.sun.jdi.Method forName = classType.concreteMethodByName("forName",
                    "(Ljava/lang/String;ZLjava/lang/ClassLoader;)Ljava/lang/Class;");
            final com.sun.jdi.ClassObjectReference faults = (com.sun.jdi.ClassObjectReference)classType.invokeMethod(thread,
                    forName, List.of(vm.mirrorOf(CHILD_FAULTS), vm.mirrorOf(true), loader),
                    com.sun.jdi.ClassType.INVOKE_SINGLE_THREADED);
            final com.sun.jdi.ClassType faultsType = (com.sun.jdi.ClassType)faults.reflectedType();
            final com.sun.jdi.Method m = faultsType.concreteMethodByName(method, "(Ljava/lang/Object;J)V");
            faultsType.invokeMethod(thread, m, List.of(service, vm.mirrorOf(n)), com.sun.jdi.ClassType.INVOKE_SINGLE_THREADED);
        }

        @Override
        public void close() {
            events.interrupt();
            try {
                vm.dispose();
            } catch (final RuntimeException ignored) {
                // already disposed or disconnected
            }
            if (failure != null) {
                throw new AssertionError("the JDWP fault installer failed", failure);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------- helpers

    private static List<String> hosts(final int members) {
        final List<String> hosts = new ArrayList<>();
        for (int i = 0; i < members; i++) {
            hosts.add("localhost");
        }
        return hosts;
    }

    private static List<String> java() {
        return new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx1g", "-Daeron.threading.mode=SHARED", "-Daeron.archive.threading.mode=SHARED",
                "-Daeron.archive.segment.file.length=67108864"));
    }

    private static String line(final String out, final String needle) {
        for (final String l : out.split("\n")) {
            if (l.contains(needle)) {
                return l.trim();
            }
        }
        return null;
    }

    private static String tail(final String out) {
        final String[] lines = out.split("\n");
        final StringBuilder sb = new StringBuilder();
        for (int i = Math.max(0, lines.length - 40); i < lines.length; i++) {
            sb.append('\n').append(lines[i]);
        }
        return sb.toString();
    }

    private Path dir(final String prefix) throws IOException {
        final Path d = Files.createTempDirectory(prefix);
        dirs.add(d);
        return d;
    }

    private Path shmDir(final String prefix) throws IOException {
        final Path shm = Path.of("/dev/shm");
        final Path d = Files.isDirectory(shm) && Files.isWritable(shm)
                ? Files.createTempDirectory(shm, prefix) : Files.createTempDirectory(prefix);
        dirs.add(d);
        return d;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}

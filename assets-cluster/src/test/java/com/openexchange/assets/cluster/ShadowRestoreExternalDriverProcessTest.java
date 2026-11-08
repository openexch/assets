// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.CoreRestoreRig.Encoders;
import com.openexchange.assets.cluster.CoreRestoreRig.Listener;
import com.openexchange.assets.cluster.CoreRestoreRig.Session;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.assets.infrastructure.persistence.RcsSettings;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Written;
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
 * RCS S4 contract 11.14, the C13 leg (G6): the production entry point ({@code com.openexchange.assets.Main}) in a child
 * JVM on an EXTERNAL media driver ({@code io.aeron.driver.MediaDriver}, another child JVM), the
 * {@code EngineDriverLifecycleProcessTest} shape. A single member in stub mode ({@code RCS_MODE=shadow}); shadows come
 * from the contract 11.0 producer, run here in the test JVM as a client of the same external driver and of the child's
 * archive (its local IPC control channel). Rows:
 * <ul>
 *   <li>kill -9 of the engine, restart on the external driver: {@code restored path=EQUAL}, {@code /ready} within
 *       {@code RCS_READY_BUDGET_MS}; the node logs {@code Driver mode EXTERNAL} (the contract's embedded-restart control
 *       has no refusal to show: embedded mode derives its own driver directory, {@code ClusterConfig:108});</li>
 *   <li>a TAIL restore paused by {@code Faults.afterTailFrames(n)} (review F8), kill -9 of the DRIVER during the pause:
 *       the engine exits 1 within the client driver timeout plus the halt ({@code EDLPT:59-60} numbers), stderr names
 *       the driver loss (or a {@code ReaderStop});</li>
 *   <li>journal-writer order (review F3, rule 12.14): a TAIL that moves more money than the 16 MiB ring holds, with the
 *       journal writer agent stopped by a {@code Faults.afterTailFrames} hook, ends in the
 *       {@code RCS_TAIL_REPLAY_TIMEOUT_MS} halt {@code tail replay timed out at <p> (journal ring full?)}, never a
 *       silent hang. The embedded leg of this row runs in {@link ShadowRestoreClusterTest}'s rig.</li>
 * </ul>
 * The faults are installed inside the child through JDWP at the service's {@code onStart} breakpoint
 * ({@code ChildFaults}, the {@code EngineDriverLifecycleProcessTest.OnStartHold} technique): the production entry point
 * offers no other handle on its service instance.
 */
public class ShadowRestoreExternalDriverProcessTest {

    private static final String ENGINE_MAIN = "com.openexchange.assets.Main";
    private static final String SERVICE_CLASS = "com.openexchange.assets.infrastructure.persistence.AssetsClusteredService";
    private static final String CHILD_FAULTS = "com.openexchange.assets.infrastructure.persistence.ChildFaults";
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
        final long budget = RcsSettings.defaults().readyBudgetMs();
        System.out.println("GREEN 11.14 external EQUAL: " + m.group() + " readyMs=" + readyMs + " (from spawn, budget "
                + budget + ")");
        assertTrue("ready within RCS_READY_BUDGET_MS " + budget + ": " + readyMs, readyMs <= budget);

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
            node.awaitLine("[RCS-CHILD] tail paused after 5 frame(s)", 90_000);
            node.closeAeron();
            final long t0 = System.nanoTime();
            node.driver.destroyForcibly(); // kill -9 of the media driver during the paused TAIL
            final boolean exited = engine.waitFor(45, TimeUnit.SECONDS);
            final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            final String out = node.log();
            assertTrue("the engine must exit after the driver kill (" + ms + " ms): " + tail(out), exited);
            System.out.println("GREEN 11.14 driver kill -9 during a paused TAIL (P_s=" + pS + " P_cm=" + pCm + "): exit="
                    + engine.exitValue() + " after " + ms + " ms; " + line(out, "[RCS-CHILD] tail paused") + " | "
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
     * RCS S4 11.13 Q9 drill, child leg (D6): the engine halts ({@code Runtime.halt}) right after {@code onTakeSnapshot}
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
        final int halts = Math.max(5, Integer.getInteger("rcs.q9.halts", 5));
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
                assertTrue(tail(after), after.contains("[RCS-CHILD] halting right after onTakeSnapshot returned"));
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
            node.startEngine(Map.of("RCS_TAIL_REPLAY_TIMEOUT_MS", Long.toString(timeoutMs)), List.of());
            node.awaitReady(120_000);
            final String out = node.log();
            final Matcher m = PATH.matcher(out);
            assertTrue("a restore path line: " + tail(out), m.find());
            assertEquals("TAIL from P_s=" + pS + ": " + m.group(), "TAIL", m.group(1));
            assertFalse("no timeout with the writer running", out.contains("tail replay timed out"));
            System.out.println("CONTROL 11.14 external, writer running: " + m.group() + "; " + line(out, "[RCS] tail ("));
            return;
        }
        final int jdwp = freePort();
        final Process engine = node.startEngine(Map.of("RCS_TAIL_REPLAY_TIMEOUT_MS", Long.toString(timeoutMs)),
                List.of("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:" + jdwp));
        try (ChildFaultInstaller ignored = new ChildFaultInstaller(jdwp, "stopJournalWriterAfter", 1)) {
            node.awaitLine("[RCS-CHILD] journal writer agent stopped", 90_000);
            final boolean exited = engine.waitFor(120, TimeUnit.SECONDS);
            final String out = node.log();
            assertTrue("the engine must not hang on a full ring: " + tail(out), exited);
            final Matcher at = Pattern.compile("tail replay timed out at (\\d+) \\(journal ring full\\?\\)").matcher(out);
            assertTrue("the RCS_TAIL_REPLAY_TIMEOUT_MS halt text: " + tail(out), at.find());
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
        final ClusterConfig layout;
        Process driver;
        Process engine;
        Aeron aeron;
        AeronArchive archive;
        private int starts;

        Node(final String prefix, final int portBase) throws IOException {
            this.base = dir(prefix);
            this.aeronDir = shmDir(prefix + "aeron-");
            this.portBase = portBase;
            this.probePort = freePort();
            // the same layout the child derives (CLUSTER_NODE 0, one host, BASE_DIR); built for its paths and contexts
            this.layout = ClusterConfig.create(0, List.of("localhost"), List.of("localhost"), portBase,
                    new TimerPerOrderControlService());
            layout.baseDir(base.resolve("state").toFile());
            layout.aeronDirectoryName(aeronDir.toString());
        }

        File clusterDir() {
            return layout.consensusModuleContext().clusterDir();
        }

        void startDriver() throws Exception {
            final List<String> cmd = java();
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
            cmd.addAll(List.of("-Dassets.auth.keys=" + TestAuth.KEYS, "-Dsnapshot.interval.minutes=0",
                    "-XX:ErrorFile=" + base.resolve("hs_err_pid%p.log"), "-cp", System.getProperty("java.class.path"),
                    ENGINE_MAIN));
            final ProcessBuilder pb = new ProcessBuilder(cmd);
            final Map<String, String> env = pb.environment();
            env.put("BASE_DIR", base.resolve("state").toString());
            env.put("AERON_DIR", aeronDir.toString());
            env.put("TRANSPORT_DRIVER_MODE", "external");
            env.put("TRANSPORT_IDLE_MODE", "backoff");
            env.put("TRANSPORT_LOG_TERM_LENGTH", "4m");
            env.put("TRANSPORT_SNAPSHOT_TERM_LENGTH", "4m");
            env.put("CLUSTER_NODE", "0");
            env.put("CLUSTER_ADDRESSES", "localhost");
            env.put("CLUSTER_PORT_BASE", String.valueOf(portBase));
            env.put("ASSETS_NODE_PORT", String.valueOf(probePort));
            env.put("RCS_MODE", "shadow");
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
            return new Clients(this);
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

        Clients(final Node node) throws InterruptedException {
            clients.add(this);
            admin = session(node, SessionClass.ADMIN);
            tool = session(node, SessionClass.TOOL);
        }

        private Session session(final Node node, final SessionClass cls) throws InterruptedException {
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
                            .ingressEndpoints("0=localhost:" + ClusterConfig.calculatePort(0, node.portBase,
                                    ClusterConfig.CLIENT_FACING_PORT_OFFSET))
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
    private static final class ChildFaultInstaller implements AutoCloseable {
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

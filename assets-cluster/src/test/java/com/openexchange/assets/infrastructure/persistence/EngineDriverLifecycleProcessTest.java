// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S16 W9 (C13 for the Java engines): the production entry point on an EXTERNAL media driver, both in child JVMs,
 * so the exit status asserted is the one a process manager sees. The ME runs the same test on the same kit code
 * ({@code com.openexchange.cluster.EngineLifecycle}).
 *
 * <p>RED evidence: {@code -Dw9.engine.classpath=<pre-fix assets-cluster.jar>} runs the child on that jar instead of this
 * build (the pre-fix engine exits 0 on driver death, hangs on the absent driver and on SIGTERM).
 */
public class EngineDriverLifecycleProcessTest {

    private static final String ENGINE_MAIN = "com.openexchange.assets.Main";
    private static final String PROBE_PORT_ENV = "ASSETS_NODE_PORT";

    private final List<Process> processes = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();

    @After
    public void cleanUp() throws Exception {
        for (final Process p : processes) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        for (final Path d : dirs) {
            deleteTree(d);
        }
    }

    @Test
    public void mediaDriverDeathAfterStartExitsOneNeverZero() throws Exception {
        final Node node = startNodeOnDriver(Map.of());
        node.driver.destroyForcibly(); // kill -9 of the media driver
        final long t0 = System.nanoTime();
        final Exit exit = awaitExit(node.engine, 45_000, t0);
        System.out.println("W9 driver kill -9: exit=" + exit.code + " after " + exit.ms + " ms");
        final String out = node.log();
        assertEquals("driver death must exit 1 (took " + exit.ms + " ms): " + out, 1, exit.code);
        assertTrue("within the 10 s client driver timeout plus the halt: " + exit.ms, exit.ms < 18_000);
        // W9b: driver loss halts on the reporting thread before its Aeron client unmaps the CnC (no component close)
        assertTrue("the lifecycle halted on the driver loss: " + out,
                out.contains("media driver lost in EXTERNAL driver mode")
                        && out.contains("halting now, no component close"));
        assertFalse("no JVM crash: " + out, out.contains("A fatal error has been detected by the Java Runtime"));
        assertFalse("no Aeron default error handler exit: " + out, out.contains("exiting client..."));
        try (Stream<Path> files = Files.list(node.base)) {
            assertEquals("no hs_err file", List.of(), files.filter(f -> f.getFileName().toString().startsWith("hs_err"))
                    .toList());
        }
    }

    @Test
    public void absentDriverAtStartExitsOneWithinTheDriverWait() throws Exception {
        final Path base = dir("assets-w9-nodriver-");
        final Path aeronDir = Files.createDirectories(base.resolve("aeron"));
        final Path log = base.resolve("engine.log");
        final long t0 = System.nanoTime();
        final Process engine = engine(base, aeronDir, freePort(), log,
                Map.of("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "2000"));
        final Exit exit = awaitExit(engine, 20_000, t0);
        System.out.println("W9 no driver, wait 2000 ms: exit=" + exit.code + " after " + exit.ms + " ms from spawn");
        final String out = Files.readString(log);
        assertEquals("absent driver must exit 1 (took " + exit.ms + " ms): " + out, 1, exit.code);
        assertTrue("within the 2000 ms wait plus the JVM start: " + exit.ms, exit.ms < 12_000);
        assertTrue(out, out.contains("No external media driver found"));
    }

    @Test
    public void sigtermShutsDownCleanlyWithinTheShutdownTimeout() throws Exception {
        final Node node = startNodeOnDriver(Map.of());
        final long t0 = System.nanoTime();
        node.engine.destroy(); // SIGTERM
        final Exit exit = awaitExit(node.engine, 20_000, t0);
        System.out.println("W9 SIGTERM: exit=" + exit.code + " after " + exit.ms + " ms");
        final String out = node.log();
        assertTrue("SIGTERM must exit 0 or 143, got " + exit.code + " after " + exit.ms + " ms: " + out,
                exit.code == 0 || exit.code == 143);
        assertTrue("within ENGINE_SHUTDOWN_TIMEOUT_MS (default 4000) plus slack: " + exit.ms, exit.ms < 6_000);
        assertTrue("the close finished, not the bound: " + out, out.contains("stopped cleanly on JVM shutdown"));
    }

    /**
     * RC2 S17 W12 (R-KIT-Q on the AE): a lone member of a three-member cluster (its peers halted on a refused state or
     * never started) exits 1 within ENGINE_QUORUM_WAIT_MS plus the fatal grace, never stays up unready.
     */
    @Test
    public void aLoneMemberOfThreeExitsOneWithinTheQuorumWait() throws Exception {
        final Path base = dir("assets-w12-lone-");
        final Path aeronDir = shmDir("assets-w12-lone-aeron-");
        final Process driver = driver(aeronDir, base.resolve("driver.log"));
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!Files.exists(aeronDir.resolve("cnc.dat"))) {
            assertTrue("driver did not start", System.nanoTime() < deadline && driver.isAlive());
            Thread.sleep(50);
        }
        final Path log = base.resolve("engine.log");
        final long t0 = System.nanoTime();
        final Process engine = engine(base, aeronDir, freePort(), log,
                Map.of("CLUSTER_ADDRESSES", "localhost,localhost,localhost", "ENGINE_QUORUM_WAIT_MS", "3000"));
        final Exit exit = awaitExit(engine, 30_000, t0);
        System.out.println("W12 lone member, quorum wait 3000 ms: exit=" + exit.code + " after " + exit.ms + " ms");
        final String out = Files.readString(log);
        assertEquals("a lone member exits 1: " + out, 1, exit.code);
        assertTrue(out, out.contains("no cluster quorum observed within 3000 ms since the consensus module launched"));
    }

    /** Review V11: the same on the embedded driver (the watchdog on the ClusteredMediaDriver's consensus module). */
    @Test
    public void aLoneMemberOfThreeExitsOneWithinTheQuorumWaitEmbeddedDriver() throws Exception {
        final Path base = dir("assets-w12-lone-emb-");
        final Path shm = shmDir("assets-w12-lone-emb-aeron-");
        final Path log = base.resolve("engine.log");
        final long t0 = System.nanoTime();
        final Map<String, String> env = new java.util.HashMap<>(Map.of("CLUSTER_ADDRESSES", "localhost,localhost,localhost",
                "ENGINE_QUORUM_WAIT_MS", "3000", "TRANSPORT_DRIVER_MODE", "embedded"));
        final Process engine = engine(base, shm, freePort(), log, env, List.of("-Daeron.dir=" + shm.resolve("aeron")));
        final Exit exit = awaitExit(engine, 30_000, t0);
        System.out.println("W12 lone member (embedded), quorum wait 3000 ms: exit=" + exit.code + " after " + exit.ms
                + " ms");
        final String out = Files.readString(log);
        assertEquals("a lone member exits 1: " + out, 1, exit.code);
        assertTrue("embedded, not external: " + out, out.contains("driverMode=EMBEDDED")
                && !out.contains("Driver mode EXTERNAL"));
        assertTrue(out, out.contains("no cluster quorum observed within 3000 ms since the consensus module launched"));
    }

    /**
     * Review V1/V9 on the AE: the consensus module sits in INIT while the service's onStart runs (its snapshot load in
     * production). onStart is held for three quorum waits by a JDWP breakpoint; the node must never trip, reach
     * /ready once released, and stay up. Embedded driver, single member.
     */
    @Test
    public void aLongServiceOnStartInInitNeverTripsTheWatchdog() throws Exception {
        final Path base = dir("assets-w12-v9-");
        final Path shm = shmDir("assets-w12-v9-aeron-");
        final int jdwp = freePort();
        final int probePort = freePort();
        final long waitMs = 2_000;
        final Path log = base.resolve("engine.log");
        final Process engine = engine(base, shm, probePort, log,
                new java.util.HashMap<>(Map.of("ENGINE_QUORUM_WAIT_MS", Long.toString(waitMs), "TRANSPORT_DRIVER_MODE",
                        "embedded")),
                List.of("-Daeron.dir=" + shm.resolve("aeron"),
                        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:" + jdwp));
        try (OnStartHold hold = new OnStartHold(jdwp,
                "com.openexchange.assets.infrastructure.persistence.AssetsClusteredService", 3 * waitMs)) {
            awaitReady(engine, probePort, log, 90_000);
            assertTrue("the breakpoint held onStart", hold.held.get());
        }
        Thread.sleep(waitMs + EngineLifecycleDefaults.FATAL_GRACE_MS);
        final String out = Files.readString(log);
        assertTrue("still running after an onStart three waits long: " + out, engine.isAlive());
        assertFalse(out, out.contains("no cluster quorum observed"));
        assertTrue(out, out.contains("quorum watchdog armed: exit 1 if no leader is known within " + waitMs + " ms"));
    }

    private static final class EngineLifecycleDefaults {
        static final long FATAL_GRACE_MS = com.openexchange.cluster.EngineLifecycle.DEFAULT_FATAL_EXIT_GRACE_MS;
    }

    /**
     * RC2 S17 W12 review V9: hold the service container's onStart for {@code holdMs} with a JDWP breakpoint (the
     * child starts with suspend=y), so the consensus module dwells in INIT waiting for the service's ack, exactly as
     * during a long snapshot load. Only the service thread is suspended; the watchdog thread keeps polling. Returns
     * true once the breakpoint was hit, held and released.
     */
    static final class OnStartHold implements AutoCloseable {
        private final com.sun.jdi.VirtualMachine vm;
        final java.util.concurrent.atomic.AtomicBoolean held = new java.util.concurrent.atomic.AtomicBoolean();
        private final Thread events;

        OnStartHold(final int port, final String serviceClass, final long holdMs) throws Exception {
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
                } catch (final java.io.IOException notYet) {
                    if (System.nanoTime() > deadline) {
                        throw notYet;
                    }
                    Thread.sleep(100);
                }
            }
            vm = attached;
            final com.sun.jdi.request.ClassPrepareRequest prepare = vm.eventRequestManager().createClassPrepareRequest();
            prepare.addClassFilter(serviceClass);
            prepare.enable();
            events = new Thread(() -> {
                try {
                    while (true) {
                        final com.sun.jdi.event.EventSet set = vm.eventQueue().remove();
                        boolean release = false;
                        for (final com.sun.jdi.event.Event e : set) {
                            if (e instanceof com.sun.jdi.event.ClassPrepareEvent cp) {
                                final com.sun.jdi.Method onStart = cp.referenceType().methodsByName("onStart").get(0);
                                final com.sun.jdi.request.BreakpointRequest bp =
                                        vm.eventRequestManager().createBreakpointRequest(onStart.location());
                                bp.setSuspendPolicy(com.sun.jdi.request.EventRequest.SUSPEND_EVENT_THREAD);
                                bp.enable();
                            } else if (e instanceof com.sun.jdi.event.BreakpointEvent) {
                                System.out.println("[W12-V9] onStart held for " + holdMs + " ms");
                                Thread.sleep(holdMs);
                                vm.eventRequestManager().deleteAllBreakpoints();
                                held.set(true);
                                release = true;
                            } else if (e instanceof com.sun.jdi.event.VMDisconnectEvent
                                    || e instanceof com.sun.jdi.event.VMDeathEvent) {
                                return;
                            }
                        }
                        set.resume();
                        if (release) {
                            vm.dispose();
                            return;
                        }
                    }
                } catch (final InterruptedException | com.sun.jdi.VMDisconnectedException done) {
                    // the child exited or the hold ended
                }
            }, "w12-onstart-hold");
            events.setDaemon(true);
            events.start();
            vm.resume(); // suspend=y: the child waits for the debugger before main
        }

        @Override
        public void close() {
            events.interrupt();
            try {
                vm.dispose();
            } catch (final RuntimeException ignored) {
                // already disposed or disconnected
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private record Exit(int code, long ms) { }

    private record Node(Process driver, Process engine, Path logFile, Path base) {
        String log() throws IOException {
            return Files.readString(logFile);
        }
    }

    private Node startNodeOnDriver(final Map<String, String> extraEnv) throws Exception {
        final Path base = dir("assets-w9-");
        final Path aeronDir = shmDir("assets-w9-aeron-");
        final Process driver = driver(aeronDir, base.resolve("driver.log"));
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!Files.exists(aeronDir.resolve("cnc.dat"))) {
            assertTrue("driver did not start: " + Files.readString(base.resolve("driver.log")),
                    System.nanoTime() < deadline && driver.isAlive());
            Thread.sleep(50);
        }
        final int probePort = freePort();
        final Path log = base.resolve("engine.log");
        final Process engine = engine(base, aeronDir, probePort, log, extraEnv);
        awaitReady(engine, probePort, log, 90_000);
        return new Node(driver, engine, log, base);
    }

    private static String childClasspath() {
        final String override = System.getProperty("w9.engine.classpath");
        return override == null || override.isEmpty() ? System.getProperty("java.class.path") : override;
    }

    private static List<String> java() {
        return new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx512m", "-Daeron.threading.mode=SHARED", "-Daeron.archive.threading.mode=SHARED",
                "-Daeron.archive.segment.file.length=1048576"));
    }

    private Process driver(final Path aeronDir, final Path log) throws IOException {
        final List<String> cmd = java();
        cmd.addAll(List.of("-Daeron.dir=" + aeronDir, "-Daeron.dir.delete.on.start=true",
                "-Daeron.dir.delete.on.shutdown=true", "-Daeron.term.buffer.sparse.file=true",
                "-cp", System.getProperty("java.class.path"), "io.aeron.driver.MediaDriver"));
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.add(p);
        return p;
    }

    private Process engine(final Path base, final Path aeronDir, final int probePort, final Path log,
                           final Map<String, String> extraEnv) throws IOException {
        return engine(base, aeronDir, probePort, log, extraEnv, List.of());
    }

    private Process engine(final Path base, final Path aeronDir, final int probePort, final Path log,
                           final Map<String, String> extraEnv, final List<String> jvmArgs) throws IOException {
        final List<String> cmd = java();
        cmd.addAll(jvmArgs);
        cmd.addAll(List.of("-XX:ErrorFile=" + base.resolve("hs_err_pid%p.log"), "-cp", childClasspath(), ENGINE_MAIN));
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
        env.put("CLUSTER_PORT_BASE", String.valueOf(freePort()));
        env.put(PROBE_PORT_ENV, String.valueOf(probePort));
        env.putAll(extraEnv);
        final Process p = pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.add(p);
        return p;
    }

    private static void awaitReady(final Process engine, final int port, final Path log, final long timeoutMs)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            assertTrue("engine exited before /ready: " + Files.readString(log), engine.isAlive());
            try {
                final HttpURLConnection c = (HttpURLConnection) URI.create("http://127.0.0.1:" + port + "/ready")
                        .toURL().openConnection();
                c.setConnectTimeout(500);
                c.setReadTimeout(1_000);
                if (c.getResponseCode() == 200) {
                    return;
                }
            } catch (final IOException notYet) {
                // probe not up yet
            }
            Thread.sleep(200);
        }
        throw new AssertionError("no /ready 200 within " + timeoutMs + " ms: " + Files.readString(log));
    }

    private static Exit awaitExit(final Process p, final long boundMs, final long t0) throws Exception {
        final boolean exited = p.waitFor(boundMs, TimeUnit.MILLISECONDS);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        if (!exited) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
            throw new AssertionError("still running " + ms + " ms after the fault (bound " + boundMs + " ms)");
        }
        return new Exit(p.exitValue(), ms);
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

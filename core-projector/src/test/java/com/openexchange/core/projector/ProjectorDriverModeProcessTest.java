// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * ((a), row 44): the production {@link ProjectorMain} in a child JVM in both driver modes, projecting
 * a real archive recording ({@link JournalSourceMain}, its own archiving driver, as the core is) into a throwaway
 * Postgres. The exit statuses asserted are the ones a process manager sees.
 *
 * <p>External mode runs on a separate driver process: the real C aeronmd when {@code -Dprojector.aeronmd=<path>} (or
 * {@code PROJECTOR_AERONMD}) names one (its directory is its {@code LD_LIBRARY_PATH}), else a Java
 * {@code io.aeron.driver.MediaDriver} child (the engines' {@code EngineDriverLifecycleProcessTest} shape).
 * Needs the PostgreSQL server binaries ({@link ProjectorProcesses}); the projecting cases are skipped without them.
 *
 * <p>RED evidence: {@code -Dprojector.driver.classpath=<pre-W10 projector.jar>} runs the child on that jar: it ignores
 * the mode, starts its own driver in every case, never exits on an absent driver.
 */
public class ProjectorDriverModeProcessTest {

    private static final int INITIAL = 5;

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void embeddedModeProjectsTheJournalOnItsOwnDriver() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final Source source = startSource();
        final Path base = procs.dir("projector-w10-emb-");
        final Projector p = projector(base, source, Map.of()); // no driver keys at all: the default
        ProjectorProcesses.awaitLine(p.process, p.log, "[PROJECTOR] driver mode EMBEDDED", 30_000);
        awaitRows(p, INITIAL, 60_000);
        assertTrue("the embedded driver dir exists while running", Files.exists(p.embeddedDir()));

        final int exit = sigterm(p);
        final String out = p.out();
        assertEquals("SIGTERM exit status: " + out, 143, exit);
        assertFalse("the embedded driver removed its dir", Files.exists(p.embeddedDir()));
        assertNoCrash(base, out);
    }

    @Test
    public void externalModeProjectsTheJournalThroughTheExternalDriverAndOwnsNone() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final Source source = startSource();
        final Path base = procs.dir("projector-w10-ext-");
        final Driver driver = startDriver(base);
        final Projector p = projector(base, source, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, driver.dir.toString()));
        ProjectorProcesses.awaitLine(p.process, p.log, "[PROJECTOR] external media driver active at " + driver.dir,
                30_000);
        awaitRows(p, INITIAL, 60_000);
        assertFalse("external mode launches no embedded driver", Files.exists(p.embeddedDir()));
        source.publish(3);
        awaitRows(p, INITIAL + 3, 60_000); // live-follow through the external driver

        final int exit = sigterm(p);
        final String out = p.out();
        assertEquals("SIGTERM exit status: " + out, 143, exit);
        assertTrue("the agent stopped cleanly: " + out, out.contains("[PROJECTOR] stopped."));
        assertTrue("the projector does not own (or stop) the external driver: " + driver.out(), driver.process.isAlive());
        assertTrue("and leaves its CnC in place", Files.exists(driver.dir.resolve("cnc.dat")));
        assertNoCrash(base, out);
    }

    @Test
    public void externalDriverLossExitsOneAndTheRestartResumesFromPostgres() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final Source source = startSource();
        final Path base = procs.dir("projector-w10-loss-");
        final Driver first = startDriver(base);
        final Map<String, String> env = Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, first.dir.toString());
        final Projector p = projector(base, source, env);
        awaitRows(p, INITIAL, 60_000);

        first.process.destroyForcibly(); // kill -9 of the external driver
        first.process.waitFor(10, TimeUnit.SECONDS);
        final long t0 = System.nanoTime();
        final int exit = awaitExit(p.process, 30_000);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        final String out = p.out();
        System.out.println("W10 projector, external driver kill -9: exit=" + exit + " after " + ms + " ms");
        // The lifecycle halts at once on the driver loss (C13: 1 for Java), on the archive client's
        // conductor thread before that client unmaps the CnC; never Aeron's default handler (prints "exiting
        // client", Runtime.exit(-1) = 255 through the shutdown path).
        assertEquals("driver loss must exit 1: " + out, 1, exit);
        assertTrue("within the 10 s client driver timeout plus the halt: " + ms, ms < 18_000);
        assertTrue("the lifecycle halted on the driver loss: " + out,
                out.contains("media driver lost in EXTERNAL driver mode")
                        && out.contains("halting now, no component close"));
        assertFalse("no Aeron default error handler exit: " + out, out.contains("exiting client"));
        assertNoCrash(base, out);

        source.publish(4); // the core side keeps recording while the projector is down
        final Driver second = startDriver(base, first.dir); // the process manager restarts aeronmd on the same dir
        final Path base2 = procs.dir("projector-w10-loss2-");
        final Projector restarted = projector(base2, source, env);
        awaitRows(restarted, INITIAL + 4, 60_000);
        assertEquals("dense, nothing lost, nothing twice", INITIAL + 4 + "|1|" + (INITIAL + 4),
                procs.query("select count(distinct journal_seq) || '|' || min(journal_seq) || '|' || max(journal_seq) "
                        + "from money_journal"));
        assertEquals(143, sigterm(restarted));
        assertTrue(second.process.isAlive());
        assertNoCrash(base2, restarted.out());
    }

    @Test
    public void externalModeWithNoDriverExitsOneWithinTheDriverWait() throws Exception {
        final Path base = procs.dir("projector-w10-nodriver-");
        final Path aeronDir = Files.createDirectories(base.resolve("aeron"));
        final long t0 = System.nanoTime();
        final Projector p = projectorNoSource(base, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, aeronDir.toString(), "TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "2000"));
        final int exit = awaitExit(p.process, 20_000);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        System.out.println("W10 projector, no driver, wait 2000 ms: exit=" + exit + " after " + ms + " ms");
        final String out = p.out();
        assertEquals("absent driver must exit 1: " + out, 1, exit);
        assertTrue("within the 2000 ms wait plus the JVM start: " + ms, ms >= 2_000 && ms < 12_000);
        assertTrue(out, out.contains("No external media driver found at " + aeronDir));
        assertFalse("no embedded fallback", Files.exists(p.embeddedDir()));
    }

    @Test
    public void sigtermDuringTheExternalDriverWaitExitsPromptlyWith143() throws Exception {
        final Path base = procs.dir("projector-w10-waitterm-");
        final Path aeronDir = Files.createDirectories(base.resolve("aeron"));
        final Projector p = projectorNoSource(base, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, aeronDir.toString()));
        ProjectorProcesses.awaitLine(p.process, p.log, "[PROJECTOR] driver mode EXTERNAL", 30_000);
        Thread.sleep(300);
        final long t0 = System.nanoTime();
        final int exit = sigterm(p);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertEquals("SIGTERM in the 30 s driver wait: " + p.out(), 143, exit);
        assertTrue("the wait ends on the stop, not at its bound: " + ms, ms < 3_000);
    }

    @Test
    public void externalModeOnTheStaleCncOfAKilledDriverExitsOne() throws Exception {
        final Path base = procs.dir("projector-w10-stale-");
        final Driver dead = startDriver(base);
        dead.process.destroyForcibly(); // kill -9: no shutdown, so its cnc.dat stays behind
        dead.process.waitFor(10, TimeUnit.SECONDS);
        assertTrue("the killed driver left its cnc.dat", Files.exists(dead.dir.resolve("cnc.dat")));
        Thread.sleep(1_500); // the CnC heartbeat is now older than the projector's driver timeout below
        final Projector p = projectorNoSource(base, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, dead.dir.toString(), "TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "2000",
                // a heartbeat older than this is a dead driver (Aeron's own check); short so the case is quick
                "JAVA_TOOL_OPTIONS", "-Daeron.driver.timeout=1000"));
        final int exit = awaitExit(p.process, 20_000);
        final String out = p.out();
        assertEquals("a dead driver's CnC must exit 1: " + out, 1, exit);
        assertTrue(out, out.contains("a cnc.dat exists but its driver is not active"));
    }

    @Test
    public void anUnknownModeOrAStrayAeronDirRefusesTheStart() throws Exception {
        final Path base = procs.dir("projector-w10-strict-");
        for (final Map<String, String> env : List.of(
                Map.of(ProjectorDriver.MODE_ENV, "extern"),
                Map.of(ProjectorDriver.MODE_ENV, "external"),
                Map.of(ProjectorDriver.AERON_DIR_ENV, "/dev/shm/some-aeronmd"),
                Map.of("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "0"))) {
            final Projector p = projectorNoSource(Files.createTempDirectory(base, "case-"), env);
            final int exit = awaitExit(p.process, 20_000);
            final String out = p.out();
            assertEquals(env + " must refuse the start: " + out, 1, exit);
            assertTrue(env + ": " + out, out.contains("FATAL (exit 1)"));
            assertFalse(env + ": no driver started", Files.exists(p.embeddedDir()));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private record Projector(Process process, Path log) {
        String out() throws IOException {
            return Files.readString(log);
        }

        Path embeddedDir() {
            return Path.of("/dev/shm/aeron-projector-" + process.pid());
        }
    }

    private record Driver(Process process, Path dir, Path log) {
        String out() throws IOException {
            return Files.readString(log);
        }
    }

    private record Source(Process process, Path log, int controlPort, int[] last) {
        void publish(final int n) throws Exception {
            final OutputStream in = process.getOutputStream();
            in.write(("publish " + n + "\n").getBytes(StandardCharsets.UTF_8));
            in.flush();
            last[0] += n;
            ProjectorProcesses.awaitLine(process, log, "[SOURCE] recorded " + last[0], 30_000);
        }
    }

    private Source startSource() throws Exception {
        final Path base = procs.dir("projector-w10-src-");
        final Path aeronDir = procs.shmName("projector-w10-src-");
        final int port = ProjectorProcesses.freePort();
        final List<String> cmd = ProjectorProcesses.java(ProjectorProcesses.testClasspath(), base,
                "-Daeron.term.buffer.sparse.file=true", "-Daeron.term.buffer.length=1m");
        cmd.addAll(List.of(JournalSourceMain.class.getName(), aeronDir.toString(), base.resolve("archive").toString(),
                String.valueOf(port), String.valueOf(INITIAL)));
        final Path log = base.resolve("source.log");
        final Process p = procs.start(new ProcessBuilder(cmd), log);
        ProjectorProcesses.awaitLine(p, log, "[SOURCE] recorded " + INITIAL, 60_000);
        return new Source(p, log, port, new int[] {INITIAL});
    }

    private Driver startDriver(final Path base) throws Exception {
        return startDriver(base, procs.shmName("projector-w10-aeronmd-"));
    }

    private Driver startDriver(final Path base, final Path dir) throws Exception {
        final Path log = Files.createTempFile(base, "driver-", ".log");
        final String aeronmd = aeronmd();
        final ProcessBuilder pb;
        if (aeronmd != null) {
            pb = new ProcessBuilder(aeronmd);
            final Map<String, String> env = pb.environment();
            env.put("LD_LIBRARY_PATH", Path.of(aeronmd).getParent().toString());
            env.put("AERON_DIR", dir.toString());
            env.put("AERON_DIR_DELETE_ON_START", "1");
            env.put("AERON_DIR_DELETE_ON_SHUTDOWN", "1");
            env.put("AERON_TERM_BUFFER_SPARSE_FILE", "1");
            env.put("AERON_TERM_BUFFER_LENGTH", String.valueOf(1024 * 1024));
            env.put("AERON_THREADING_MODE", "SHARED");
        } else {
            final List<String> cmd = ProjectorProcesses.java(ProjectorProcesses.testClasspath(), base,
                    "-Daeron.dir=" + dir, "-Daeron.dir.delete.on.start=true", "-Daeron.dir.delete.on.shutdown=true",
                    "-Daeron.term.buffer.sparse.file=true", "-Daeron.term.buffer.length=1m",
                    "-Daeron.threading.mode=SHARED");
            cmd.add("io.aeron.driver.MediaDriver");
            pb = new ProcessBuilder(cmd);
        }
        final Process p = procs.start(pb, log);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!Files.exists(dir.resolve("cnc.dat"))) {
            assertTrue("driver did not start: " + Files.readString(log), System.nanoTime() < deadline && p.isAlive());
            Thread.sleep(50);
        }
        System.out.println("W10 external driver: " + (aeronmd != null ? aeronmd : "Java MediaDriver") + " at " + dir);
        return new Driver(p, dir, log);
    }

    private static String aeronmd() {
        String v = System.getProperty("projector.aeronmd");
        if (v == null || v.isEmpty()) {
            v = System.getenv("PROJECTOR_AERONMD");
        }
        if (v == null || v.isEmpty()) {
            return null;
        }
        assertTrue("not an executable aeronmd: " + v, Files.isExecutable(Path.of(v)));
        return v;
    }

    private Projector projector(final Path base, final Source source, final Map<String, String> extraEnv)
            throws Exception {
        final ProcessBuilder pb = projectorBuilder(base, extraEnv);
        final Map<String, String> env = pb.environment();
        env.put("PROJECTOR_POSTGRES_URL", "jdbc:postgresql://127.0.0.1:" + procs.pgPort() + "/core");
        env.put("PROJECTOR_CORE_JOURNAL_ARCHIVES", "localhost:" + source.controlPort);
        final Path log = base.resolve("projector.log");
        final Process p = procs.start(pb, log);
        procs.track(Path.of("/dev/shm/aeron-projector-" + p.pid()));
        return new Projector(p, log);
    }

    /** A projector with no Postgres and no source: for the cases that must refuse before either is touched. */
    private Projector projectorNoSource(final Path base, final Map<String, String> extraEnv) throws Exception {
        final ProcessBuilder pb = projectorBuilder(base, extraEnv);
        pb.environment().put("PROJECTOR_POSTGRES_URL", "jdbc:postgresql://127.0.0.1:" + ProjectorProcesses.freePort()
                + "/core");
        pb.environment().put("PROJECTOR_CORE_JOURNAL_ARCHIVES", "localhost:" + ProjectorProcesses.freePort());
        final Path log = base.resolve("projector.log");
        final Process p = procs.start(pb, log);
        procs.track(Path.of("/dev/shm/aeron-projector-" + p.pid()));
        return new Projector(p, log);
    }

    private static ProcessBuilder projectorBuilder(final Path base, final Map<String, String> extraEnv)
            throws IOException {
        final List<String> cmd = ProjectorProcesses.java(childClasspath(), base);
        cmd.add(ProjectorMain.class.getName());
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Map<String, String> env = pb.environment();
        env.remove(ProjectorDriver.MODE_ENV);
        env.remove(ProjectorDriver.AERON_DIR_ENV);
        env.remove("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS");
        env.put("PROJECTOR_POSTGRES_USER", "core");
        env.put("PROJECTOR_METRICS_PORT", String.valueOf(ProjectorProcesses.freePort()));
        env.put("PROJECTOR_LOCAL_HOST", "localhost");
        env.putAll(extraEnv);
        return pb;
    }

    private static String childClasspath() {
        final String override = System.getProperty("projector.driver.classpath");
        return override == null || override.isEmpty() ? ProjectorProcesses.testClasspath() : override;
    }

    private String rows() throws Exception {
        return procs.query("select count(*) from money_journal");
    }

    private void awaitRows(final Projector p, final int expected, final long timeoutMs) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        String n;
        while (!(n = rows()).equals(String.valueOf(expected))) {
            assertTrue("projected " + n + " of " + expected + " within " + timeoutMs + " ms: " + p.out(),
                    System.nanoTime() < deadline && p.process.isAlive());
            Thread.sleep(200);
        }
    }

    private static int sigterm(final Projector p) throws Exception {
        p.process.destroy();
        return awaitExit(p.process, 8_000);
    }

    private static int awaitExit(final Process p, final long timeoutMs) throws Exception {
        assertTrue("did not exit within " + timeoutMs + " ms", p.waitFor(timeoutMs, TimeUnit.MILLISECONDS));
        return p.exitValue();
    }

    private static void assertNoCrash(final Path base, final String out) throws IOException {
        assertFalse("no JVM crash: " + out, out.contains("A fatal error has been detected by the Java Runtime"));
        try (Stream<Path> files = Files.list(base)) {
            assertEquals("no hs_err file", List.of(), files.filter(f -> f.getFileName().toString().startsWith("hs_err"))
                    .toList());
        }
    }
}

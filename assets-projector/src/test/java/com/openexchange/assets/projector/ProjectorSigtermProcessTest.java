// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
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
import static org.junit.Assume.assumeTrue;

/**
 * RC2 S16 W9b: the production {@link ProjectorMain} in a child JVM, on a throwaway local Postgres, receives SIGTERM.
 * The exit status asserted is the one docker sees. Before the fix the projector's own hook finished in about 1 ms but
 * agrona's ShutdownSignalBarrier hook waited for a close() nobody called, so the JVM never exited (docker: 137 at
 * 10 s; here: still running at the bound, plus agrona's "hasn't terminated in 10 seconds" warning).
 *
 * <p>Needs the PostgreSQL server binaries ({@code initdb}, {@code postgres}, {@code psql}): {@code PROJECTOR_PG_BIN}, or
 * the newest {@code /usr/lib/postgresql/<n>/bin}. Skipped (assumption) when there are none.
 * RED evidence: {@code -Dw9b.projector.classpath=<pre-fix projector.jar>}.
 */
public class ProjectorSigtermProcessTest {

    private final List<Process> processes = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private Path pgData;
    private Path pgBin;
    private long projectorPid = -1;

    @After
    public void cleanUp() throws Exception {
        for (final Process p : processes) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        if (pgData != null && Files.exists(pgData.resolve("postmaster.pid"))) {
            new ProcessBuilder(pgBin.resolve("pg_ctl").toString(), "-D", pgData.toString(), "-m", "immediate", "stop")
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start()
                    .waitFor(20, TimeUnit.SECONDS);
        }
        if (projectorPid > 0) {
            deleteTree(Path.of("/dev/shm/aeron-projector-" + projectorPid)); // left behind by a killed child
        }
        for (final Path d : dirs) {
            deleteTree(d);
        }
    }

    @Test
    public void sigtermExitsPromptlyWith143AfterTheCleanStop() throws Exception {
        pgBin = pgBin();
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", pgBin != null);
        final int pgPort = startPostgres();

        final Path base = dir("projector-w9b-");
        final Path log = base.resolve("projector.log");
        final int metricsPort = freePort();
        final List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx256m", "-XX:ErrorFile=" + base.resolve("hs_err_pid%p.log"),
                "-cp", childClasspath(), ProjectorMain.class.getName()));
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Map<String, String> env = pb.environment();
        env.put("PROJECTOR_POSTGRES_URL", "jdbc:postgresql://127.0.0.1:" + pgPort + "/assets");
        env.put("PROJECTOR_POSTGRES_USER", "assets");
        env.put("PROJECTOR_METRICS_PORT", String.valueOf(metricsPort));
        // No AE: the agent keeps retrying its archive connect, as it does while the core is down.
        env.put("PROJECTOR_AE_JOURNAL_ARCHIVES", "localhost:" + freePort());
        final Process projector = pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.add(projector);
        projectorPid = projector.pid();

        awaitLine(projector, log, "[PROJECTOR] metrics /metrics + /health on port " + metricsPort, 30_000);
        awaitLine(projector, log, "Postgres high-water journalSeq=0", 30_000); // the agent is running its epoch
        Thread.sleep(500);

        final long t0 = System.nanoTime();
        projector.destroy(); // SIGTERM, as docker stop sends it
        final boolean exited = projector.waitFor(8_000, TimeUnit.MILLISECONDS);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        System.out.println("W9b projector SIGTERM: exited=" + exited + " after " + ms + " ms"
                + (exited ? " exit=" + projector.exitValue() : ""));
        final String out = Files.readString(log);
        assertTrue("the projector JVM must exit well inside docker's 10 s stop timeout (" + ms + " ms): " + out, exited);
        assertEquals("SIGTERM exit status (128 + 15) after the shutdown hook: " + out, 143, projector.exitValue());
        assertTrue("prompt, not the hook's 5 s join bound (" + ms + " ms): " + out, ms < 3_000);
        assertTrue("the agent stopped cleanly: " + out, out.contains("[PROJECTOR] stopped."));
        assertFalse(out, out.contains("ShutdownSignalBarrier hasn't terminated"));
        assertFalse("the embedded driver removed its dir", Files.exists(Path.of("/dev/shm/aeron-projector-" + projectorPid)));
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static String childClasspath() {
        final String override = System.getProperty("w9b.projector.classpath");
        return override == null || override.isEmpty() ? System.getProperty("java.class.path") : override;
    }

    private static Path pgBin() {
        final String env = System.getenv("PROJECTOR_PG_BIN");
        if (env != null && !env.isBlank()) {
            return Files.isExecutable(Path.of(env, "initdb")) ? Path.of(env) : null;
        }
        final File[] versions = new File("/usr/lib/postgresql").listFiles();
        if (versions == null) {
            return null;
        }
        Path best = null;
        int bestVersion = -1;
        for (final File v : versions) {
            final Path bin = v.toPath().resolve("bin");
            if (!Files.isExecutable(bin.resolve("initdb")) || !Files.isExecutable(bin.resolve("postgres"))
                    || !Files.isExecutable(bin.resolve("psql"))) {
                continue;
            }
            try {
                final int n = Integer.parseInt(v.getName());
                if (n > bestVersion) {
                    bestVersion = n;
                    best = bin;
                }
            } catch (final NumberFormatException ignored) {
                // not a version directory
            }
        }
        return best;
    }

    /** A throwaway cluster: trust auth, TCP on 127.0.0.1 only, socket in its own dir, V001 applied to "assets". */
    private int startPostgres() throws Exception {
        final Path root = dir("projector-w9b-pg-");
        pgData = root.resolve("data");
        final Path socketDir = Files.createDirectories(root.resolve("sock"));
        run(root.resolve("initdb.log"), pgBin.resolve("initdb").toString(), "-D", pgData.toString(), "-U", "assets",
                "--auth=trust", "--no-sync", "-E", "UTF8");
        final int port = freePort();
        final Process pg = new ProcessBuilder(pgBin.resolve("postgres").toString(), "-D", pgData.toString(),
                "-p", String.valueOf(port), "-k", socketDir.toString(), "-c", "listen_addresses=127.0.0.1",
                "-c", "fsync=off")
                .redirectErrorStream(true).redirectOutput(root.resolve("postgres.log").toFile()).start();
        processes.add(pg);
        final String psql = pgBin.resolve("psql").toString();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            assertTrue("postgres exited: " + Files.readString(root.resolve("postgres.log")), pg.isAlive());
            final Process probe = new ProcessBuilder(psql, "-h", "127.0.0.1", "-p", String.valueOf(port), "-U",
                    "assets", "-d", "postgres", "-Atc", "select 1")
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
            if (probe.waitFor(10, TimeUnit.SECONDS) && probe.exitValue() == 0) {
                break;
            }
            assertTrue("postgres not ready: " + Files.readString(root.resolve("postgres.log")),
                    System.nanoTime() < deadline);
            Thread.sleep(200);
        }
        run(root.resolve("createdb.log"), psql, "-h", "127.0.0.1", "-p", String.valueOf(port), "-U", "assets",
                "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE assets");
        final Path migration = root.resolve("V001__money_journal.sql");
        try (var in = ProjectorSigtermProcessTest.class.getResourceAsStream("/db/migration/V001__money_journal.sql")) {
            assertTrue("V001 on the classpath", in != null);
            Files.copy(in, migration);
        }
        run(root.resolve("migrate.log"), psql, "-h", "127.0.0.1", "-p", String.valueOf(port), "-U", "assets",
                "-d", "assets", "-v", "ON_ERROR_STOP=1", "-f", migration.toString());
        return port;
    }

    private static void run(final Path log, final String... cmd) throws Exception {
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(String.join(" ", cmd) + " timed out", p.waitFor(60, TimeUnit.SECONDS));
        assertEquals(String.join(" ", cmd) + ": " + Files.readString(log), 0, p.exitValue());
    }

    private static void awaitLine(final Process p, final Path log, final String line, final long timeoutMs)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!(Files.exists(log) && Files.readString(log).contains(line))) {
            assertTrue("no '" + line + "' within " + timeoutMs + " ms: " + Files.readString(log),
                    System.nanoTime() < deadline && p.isAlive());
            Thread.sleep(100);
        }
    }

    private Path dir(final String prefix) throws IOException {
        final Path d = Files.createTempDirectory(prefix);
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

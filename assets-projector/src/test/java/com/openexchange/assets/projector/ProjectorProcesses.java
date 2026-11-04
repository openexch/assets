// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Child processes for the projector's process-level tests (S16 W9b SIGTERM, S17 W10 driver modes): a throwaway local
 * Postgres with V001 applied, child JVMs on this build's classpath, and the bookkeeping that removes them. One per test;
 * {@link #close()} in {@code @After}.
 *
 * <p>Postgres needs the server binaries ({@code initdb}, {@code postgres}, {@code psql}): {@code PROJECTOR_PG_BIN}, or
 * the newest {@code /usr/lib/postgresql/<n>/bin}; {@link #pgBin()} is null when there are none (tests assume it).
 */
final class ProjectorProcesses implements AutoCloseable {

    final List<Process> processes = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private Path pgData;
    private Path pgBin;
    private int pgPort = -1;

    @Override
    public void close() throws Exception {
        for (final Process p : processes) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        if (pgData != null && Files.exists(pgData.resolve("postmaster.pid"))) {
            new ProcessBuilder(pgBin.resolve("pg_ctl").toString(), "-D", pgData.toString(), "-m", "immediate", "stop")
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start()
                    .waitFor(20, TimeUnit.SECONDS);
        }
        for (final Path d : dirs) {
            deleteTree(d);
        }
    }

    /** Delete {@code d} in {@link #close()} (temp dirs, /dev/shm driver dirs a killed child left behind). */
    Path track(final Path d) {
        dirs.add(d);
        return d;
    }

    Path dir(final String prefix) throws IOException {
        return track(Files.createTempDirectory(prefix));
    }

    /** A fresh name under /dev/shm (not created), removed in {@link #close()}. */
    Path shmName(final String prefix) {
        return track(Path.of("/dev/shm", prefix + ProcessHandle.current().pid() + "-" + System.nanoTime()));
    }

    Process start(final ProcessBuilder pb, final Path log) throws IOException {
        final Process p = pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.add(p);
        return p;
    }

    static List<String> java(final String classpath, final Path errorDir, final String... jvmArgs) {
        final List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx256m", "-XX:ErrorFile=" + errorDir.resolve("hs_err_pid%p.log")));
        cmd.addAll(List.of(jvmArgs));
        cmd.addAll(List.of("-cp", classpath));
        return cmd;
    }

    static String testClasspath() {
        return System.getProperty("java.class.path");
    }

    // ---- Postgres --------------------------------------------------------------------------------------------------

    Path pgBin() {
        if (pgBin == null) {
            pgBin = findPgBin();
        }
        return pgBin;
    }

    int pgPort() {
        return pgPort;
    }

    /** A throwaway cluster: trust auth, TCP on 127.0.0.1 only, socket in its own dir, V001 applied to "assets". */
    int startPostgres() throws Exception {
        final Path bin = pgBin();
        assertTrue("no PostgreSQL server binaries", bin != null);
        final Path root = dir("projector-pg-");
        pgData = root.resolve("data");
        final Path socketDir = Files.createDirectories(root.resolve("sock"));
        run(root.resolve("initdb.log"), bin.resolve("initdb").toString(), "-D", pgData.toString(), "-U", "assets",
                "--auth=trust", "--no-sync", "-E", "UTF8");
        final int port = freePort();
        final Process pg = start(new ProcessBuilder(bin.resolve("postgres").toString(), "-D", pgData.toString(),
                "-p", String.valueOf(port), "-k", socketDir.toString(), "-c", "listen_addresses=127.0.0.1",
                "-c", "fsync=off"), root.resolve("postgres.log"));
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            assertTrue("postgres exited: " + Files.readString(root.resolve("postgres.log")), pg.isAlive());
            final Process probe = new ProcessBuilder(psql(port, "postgres", "-Atc", "select 1"))
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
            if (probe.waitFor(10, TimeUnit.SECONDS) && probe.exitValue() == 0) {
                break;
            }
            assertTrue("postgres not ready: " + Files.readString(root.resolve("postgres.log")),
                    System.nanoTime() < deadline);
            Thread.sleep(200);
        }
        run(root.resolve("createdb.log"), psql(port, "postgres", "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE assets"));
        final Path migration = root.resolve("V001__money_journal.sql");
        try (var in = ProjectorProcesses.class.getResourceAsStream("/db/migration/V001__money_journal.sql")) {
            assertTrue("V001 on the classpath", in != null);
            Files.copy(in, migration);
        }
        run(root.resolve("migrate.log"), psql(port, "assets", "-v", "ON_ERROR_STOP=1", "-f", migration.toString()));
        pgPort = port;
        return port;
    }

    /** One scalar from the "assets" database. */
    String query(final String sql) throws Exception {
        final Path out = Files.createTempFile("projector-psql-", ".out");
        try {
            run(out, psql(pgPort, "assets", "-v", "ON_ERROR_STOP=1", "-Atc", sql));
            return Files.readString(out).trim();
        } finally {
            Files.deleteIfExists(out);
        }
    }

    private String[] psql(final int port, final String db, final String... args) {
        final List<String> cmd = new ArrayList<>(List.of(pgBin.resolve("psql").toString(), "-h", "127.0.0.1",
                "-p", String.valueOf(port), "-U", "assets", "-d", db));
        cmd.addAll(List.of(args));
        return cmd.toArray(String[]::new);
    }

    private static Path findPgBin() {
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

    // ---- small helpers ---------------------------------------------------------------------------------------------

    static void run(final Path log, final String... cmd) throws Exception {
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(String.join(" ", cmd) + " timed out", p.waitFor(60, TimeUnit.SECONDS));
        assertEquals(String.join(" ", cmd) + ": " + Files.readString(log), 0, p.exitValue());
    }

    static void awaitLine(final Process p, final Path log, final String line, final long timeoutMs)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!(Files.exists(log) && Files.readString(log).contains(line))) {
            assertTrue("no '" + line + "' within " + timeoutMs + " ms: " + Files.readString(log),
                    System.nanoTime() < deadline && p.isAlive());
            Thread.sleep(100);
        }
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}

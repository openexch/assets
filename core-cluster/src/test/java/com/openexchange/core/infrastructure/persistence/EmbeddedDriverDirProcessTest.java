// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import io.aeron.CommonContext;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The member's embedded driver directory check (embedded mode; external mode never touches a driver directory).
 *
 * <ul>
 *   <li>(a) aeronmd-shaped: a plain {@code MediaDriver} with Aeron's default 1 s timer owns the directory; the member's
 *       launch path ({@code AeronCluster.launchEmbeddedDriver} with {@code CORE_DRIVER_DIR_ACTIVE_MS=10000}) is refused
 *       20 times out of 20, spread over the heartbeat cycle, with the host text; the live driver still answers. Then
 *       one child JVM member on that directory exits 1 with the same text. Control
 *       {@code -Dcore.driverdir.nc=DIR_DELETE_ON_START} (today's {@code dirDeleteOnStart(true)}, 500 ms) must fail with
 *       {@code the member took over a live driver's directory at attempt <n>}.</li>
 *   <li>(b) member against member, child JVMs: a second member on the first one's directory exits 1 and the first still
 *       answers {@code /ready}; after the first is killed and the bound has passed, the second starts and replaces the
 *       stale directory.</li>
 *   <li>(c) the trade-off: a member killed and restarted inside {@code CORE_DRIVER_DIR_ACTIVE_MS} is refused with the
 *       text until the bound passes.</li>
 * </ul>
 */
public class EmbeddedDriverDirProcessTest {

    private static final String ENGINE_MAIN = "com.openexchange.core.Main";
    private static final long CHILD_BOUND_MS = 5_000;

    private final List<Process> processes = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();

    @After
    public void cleanUp() throws Exception {
        for (final Process p : processes) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        for (final Path d : dirs) {
            IoUtil.delete(d.toFile(), true);
        }
    }

    private Path tmp(final String label) throws IOException {
        final Path base = Files.isWritable(Path.of("/dev/shm"))
                ? Files.createTempDirectory(Path.of("/dev/shm"), "driverdir-" + label + "-")
                : Files.createTempDirectory("driverdir-" + label + "-");
        dirs.add(base);
        return base;
    }

    @Test
    public void aLiveAeronmdShapedDriverRefusesTheMemberTwentyTimesOutOfTwenty() throws Exception {
        final Path base = tmp("a");
        final String aeronDir = base.resolve("aeron").toString();
        final String driverDir = aeronDir + "-core-0-driver";
        final boolean control = "DIR_DELETE_ON_START".equals(System.getProperty("core.driverdir.nc", ""));
        final MediaDriver live = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(driverDir)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED));
        try {
            final long cncInode = inode(driverDir);
            for (int attempt = 1; attempt <= 20; attempt++) {
                final ClusterConfig cfg = ClusterConfig.create(0, List.of("localhost"), List.of("localhost"),
                        freePort(), new CoreClusteredService(SnapshotModeSettings.defaults()));
                cfg.aeronDirectoryName(driverDir);
                cfg.baseDir(base.resolve("state-" + attempt).toFile());
                AutoCloseable took = null;
                try {
                    if (control) {
                        // today's member driver context: no liveness check before the directory is deleted
                        took = MediaDriver.launch(cfg.mediaDriverContext().dirDeleteOnStart(true)
                                .dirDeleteOnShutdown(true).driverTimeoutMs(500));
                    } else {
                        took = AeronCluster.launchEmbeddedDriver(cfg, 10_000, () -> ClusteredMediaDriver.launch(
                                com.openexchange.cluster.EmbeddedDriverDir.guard(cfg.mediaDriverContext(), 10_000)
                                        .dirDeleteOnShutdown(true), cfg.archiveContext(),
                                cfg.consensusModuleContext()));
                    }
                } catch (final StandbySnapshotBootGuard.Refused refused) {
                    final String text = refused.getMessage();
                    assertTrue(text, text.startsWith("CORE BOOT GUARD: AERON_DIR " + new File(driverDir).getAbsolutePath()
                            + " has a live media driver (another core member's embedded driver or an aeronmd; heartbeat "));
                    assertTrue(text, text.endsWith(" ms old, bound 10000 ms): stop it or point this member at its own"
                            + " directory"));
                }
                if (took != null) {
                    CloseHelper.quietClose(took);
                    throw new AssertionError("the member took over a live driver's directory at attempt " + attempt
                            + " (CnC inode " + cncInode + " -> " + inode(driverDir) + ")");
                }
                assertEquals("CnC untouched at attempt " + attempt, cncInode, inode(driverDir));
                Thread.sleep(53); // spread over the 1 s heartbeat cycle
            }
            assertTrue("the live driver still answers",
                    CommonContext.isDriverActive(new File(driverDir), 10_000, s -> { }));
            System.out.println("GREEN (a): 20/20 refused over a 1 s-timer driver, CnC untouched");

            // one child JVM member on that directory: exit 1 with the text
            final Child refusedChild = child("a-child", base, aeronDir, Map.of());
            assertTrue("the child must exit", refusedChild.process.waitFor(60, TimeUnit.SECONDS));
            final String out = Files.readString(refusedChild.log);
            assertEquals("exit code; output=" + out, 1, refusedChild.process.exitValue());
            assertTrue(out, out.contains("CORE BOOT GUARD: AERON_DIR " + driverDir + " has a live media driver"));
            assertTrue("the live driver still answers after the child",
                    CommonContext.isDriverActive(new File(driverDir), 10_000, s -> { }));
            System.out.println("GREEN (a) child: exit 1, " + line(out, "CORE BOOT GUARD"));
        } finally {
            CloseHelper.quietClose(live);
        }
    }

    @Test
    public void aSecondMemberOnTheSameDirectoryExitsAndAKilledOneIsReplacedAfterTheBound() throws Exception {
        final Path base = tmp("b");
        final String aeronDir = base.resolve("aeron").toString();
        final String driverDir = aeronDir + "-core-0-driver";
        final Map<String, String> bound = Map.of("CORE_DRIVER_DIR_ACTIVE_MS", Long.toString(CHILD_BOUND_MS));
        final Child first = child("b-first", base, aeronDir, bound);
        awaitReady(first, 90_000);
        final Child second = child("b-second", base, aeronDir, bound);
        assertTrue("the second member must exit", second.process.waitFor(60, TimeUnit.SECONDS));
        final String out = Files.readString(second.log);
        assertEquals("exit code; output=" + out, 1, second.process.exitValue());
        assertTrue(out, out.contains("CORE BOOT GUARD: AERON_DIR " + driverDir + " has a live media driver (another core"
                + " member's embedded driver or an aeronmd; heartbeat "));
        assertTrue("first driver still live after the second start", first.process.isAlive() && ready(first.probePort));
        System.out.println("GREEN (b): the second member exited 1; the first still ready");

        // the first killed: its directory is stale only after the bound
        first.process.destroyForcibly();
        assertTrue(first.process.waitFor(20, TimeUnit.SECONDS));
        final long killedNs = System.nanoTime();
        // (c) the trade-off: a restart inside the bound is refused
        final Child early = child("c-early", base, aeronDir, bound);
        assertTrue("the early restart must exit", early.process.waitFor(60, TimeUnit.SECONDS));
        final String earlyOut = Files.readString(early.log);
        final long earlyMs = (System.nanoTime() - killedNs) / 1_000_000;
        assertEquals("exit code of a restart " + earlyMs + " ms after the kill; output=" + earlyOut, 1,
                early.process.exitValue());
        assertTrue(earlyOut, earlyOut.contains("has a live media driver") && earlyOut.contains("bound "
                + CHILD_BOUND_MS + " ms"));
        System.out.println("GREEN (c): a restart inside the bound refused: " + line(earlyOut, "CORE BOOT GUARD"));
        final long waitMs = CHILD_BOUND_MS + 1_000 - (System.nanoTime() - killedNs) / 1_000_000;
        if (waitMs > 0) {
            Thread.sleep(waitMs);
        }
        final Child replaced = child("b-replaced", base, aeronDir, bound);
        awaitReady(replaced, 90_000);
        System.out.println("GREEN (b): after the bound the member replaced the stale directory and is ready");
    }

    // ------------------------------------------------------------------------------------------------ children

    private record Child(Process process, Path log, int probePort) {
    }

    private Child child(final String label, final Path base, final String aeronDir, final Map<String, String> env)
            throws IOException {
        final Path state = base.resolve("state-" + label);
        final Path log = base.resolve(label + ".log");
        final int probePort = freePort();
        final List<String> cmd = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-Xmx512m", "-Daeron.threading.mode=SHARED",
                "-Daeron.archive.threading.mode=SHARED", "-Daeron.archive.segment.file.length=1048576",
                "-Daeron.dir=" + aeronDir, "-XX:ErrorFile=" + base.resolve("hs_err_pid%p.log"), "-cp",
                System.getProperty("java.class.path"), ENGINE_MAIN));
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Map<String, String> e = pb.environment();
        e.put("BASE_DIR", state.toString());
        e.put("TRANSPORT_DRIVER_MODE", "embedded");
        e.put("TRANSPORT_IDLE_MODE", "backoff");
        e.put("TRANSPORT_LOG_TERM_LENGTH", "4m");
        e.put("TRANSPORT_LOG_RCV_WINDOW", "2m");
        e.put("TRANSPORT_LOG_SO_RCVBUF", "2m");
        e.put("TRANSPORT_LOG_SO_SNDBUF", "2m");
        e.put("TRANSPORT_SNAPSHOT_TERM_LENGTH", "4m");
        e.put("CLUSTER_NODE", "0");
        e.put("CLUSTER_ADDRESSES", "localhost");
        e.put("CLUSTER_PORT_BASE", String.valueOf(freePort()));
        e.put("CORE_NODE_PORT", String.valueOf(probePort));
        e.putAll(env);
        final Process p = pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.add(p);
        return new Child(p, log, probePort);
    }

    private static void awaitReady(final Child c, final long timeoutMs) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (!c.process.isAlive()) {
                throw new AssertionError("the member exited " + c.process.exitValue() + ": " + Files.readString(c.log));
            }
            if (ready(c.probePort)) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("the member never became ready: " + Files.readString(c.log));
    }

    private static boolean ready(final int port) {
        try {
            final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + port + "/ready").toURL()
                    .openConnection();
            c.setConnectTimeout(500);
            c.setReadTimeout(1_000);
            return c.getResponseCode() == 200;
        } catch (final IOException notYet) {
            return false;
        }
    }

    private static String line(final String out, final String needle) {
        for (final String l : out.split("\n")) {
            if (l.contains(needle)) {
                return l.trim();
            }
        }
        return "(none)";
    }

    private static long inode(final String driverDir) throws IOException {
        final Path cnc = Path.of(driverDir, "cnc.dat");
        return Files.exists(cnc) ? (Long)Files.getAttribute(cnc, "unix:ino") : -1;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}

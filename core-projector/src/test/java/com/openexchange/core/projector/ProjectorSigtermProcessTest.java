// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The production {@link ProjectorMain} in a child JVM, on a throwaway local Postgres, receives SIGTERM.
 * The exit status asserted is the one docker sees. Before the fix the projector's own hook finished in about 1 ms but
 * agrona's ShutdownSignalBarrier hook waited for a close() nobody called, so the JVM never exited (docker: 137 at
 * 10 s; here: still running at the bound, plus agrona's "hasn't terminated in 10 seconds" warning).
 *
 * <p>Needs the PostgreSQL server binaries ({@link ProjectorProcesses}); skipped (assumption) when there are none. The
 * child runs the default (embedded) driver mode: the driver keys are removed from its environment.
 * RED evidence: {@code -Dprojector.sigterm.classpath=<pre-fix projector.jar>}.
 */
public class ProjectorSigtermProcessTest {

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void sigtermExitsPromptlyWith143AfterTheCleanStop() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        final int pgPort = procs.startPostgres();

        final Path base = procs.dir("projector-w9b-");
        final Path log = base.resolve("projector.log");
        final int metricsPort = ProjectorProcesses.freePort();
        final List<String> cmd = ProjectorProcesses.java(childClasspath(), base);
        cmd.add(ProjectorMain.class.getName());
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Map<String, String> env = pb.environment();
        env.remove(ProjectorDriver.MODE_ENV);
        env.remove(ProjectorDriver.AERON_DIR_ENV);
        env.put("PROJECTOR_POSTGRES_URL", "jdbc:postgresql://127.0.0.1:" + pgPort + "/core");
        env.put("PROJECTOR_POSTGRES_USER", "core");
        env.put("PROJECTOR_METRICS_PORT", String.valueOf(metricsPort));
        // No core: the agent keeps retrying its archive connect, as it does while the core is down.
        env.put("PROJECTOR_CORE_JOURNAL_ARCHIVES", "localhost:" + ProjectorProcesses.freePort());
        final Process projector = procs.start(pb, log);
        final long projectorPid = projector.pid();
        procs.track(Path.of("/dev/shm/aeron-projector-" + projectorPid)); // left behind by a killed child

        ProjectorProcesses.awaitLine(projector, log, "[PROJECTOR] metrics /metrics + /health on port " + metricsPort,
                30_000);
        ProjectorProcesses.awaitLine(projector, log, "Postgres high-water journalSeq=0", 30_000); // the agent's epoch
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
        final String override = System.getProperty("projector.sigterm.classpath");
        return override == null || override.isEmpty() ? System.getProperty("java.class.path") : override;
    }
}

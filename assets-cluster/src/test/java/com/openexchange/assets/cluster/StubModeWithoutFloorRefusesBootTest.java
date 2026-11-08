// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.infrastructure.persistence.RcsSettings;
import com.openexchange.assets.infrastructure.persistence.ServiceProbe;
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
 * RCS S4 contract 11.6 (MF-4): stub mode never runs a pruner without the ShadowFloor.
 *
 * <p>Process level (child JVMs on the production entry point {@code com.openexchange.assets.Main}, embedded driver,
 * single member): {@code RCS_MODE=shadow} + {@code SNAPSHOT_PRUNE_ENABLED=true} + {@code SHADOW_FLOOR_ENABLED=false}
 * exits 1 before any Aeron component (no {@code aeron.dir} created) with the MF-4 text; {@code SNAPSHOT_PRUNE_ENABLED=
 * false} boots; {@code RCS_MODE=bogus} is refused with the 3.5 text; the test-only override
 * {@code -Doxs.shadow.floor.unchecked=true} lets the misconfiguration through (the control's door).</p>
 *
 * <p>In process (the rig): the runtime second net ({@code watermarkIs(floor)} false trips {@code failFast} with
 * {@code pruner watermark is not the ShadowFloor}); and the MF-4 oracle itself: with the floor wired, a stub snapshot and
 * a pruner pass purge nothing below the stub; control: the floor off (the guard's door open) purges below the stub,
 * {@code log purged below stub P_cm=<p> with no shadow (MF-4)}.</p>
 */
public class StubModeWithoutFloorRefusesBootTest {

    private static final String ENGINE_MAIN = "com.openexchange.assets.Main";

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

    // ------------------------------------------------------------------------------------------ process level

    /**
     * {@code aeronComponents}: the log shows the launch line printed right after the boot guards and before the media
     * driver, archive and consensus module start ({@code AeronCluster}: "Assets Engine node N starting"), or the state
     * directory holds an archive. The aeron dir itself is deleted on shutdown, so its absence alone proves nothing.
     */
    private record Run(int exit, String out, boolean aeronComponents, boolean ready) { }

    /** Starts the engine with {@code env}; waits for exit (refusal rows) or /ready (boot rows), then stops it. */
    private Run engine(final String label, final Map<String, String> env, final List<String> jvmArgs,
                       final boolean expectBoot) throws Exception {
        final Path base = Files.createTempDirectory("rcs-s4-11.6-" + label + "-");
        dirs.add(base);
        final Path shm = Files.isWritable(Path.of("/dev/shm"))
                ? Files.createTempDirectory(Path.of("/dev/shm"), "rcs-s4-11.6-" + label + "-") : base;
        dirs.add(shm);
        final Path aeronDir = shm.resolve("aeron");
        final Path log = base.resolve("engine.log");
        final int probePort = freePort();
        final List<String> cmd = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-Xmx512m", "-Daeron.threading.mode=SHARED",
                "-Daeron.archive.threading.mode=SHARED", "-Daeron.archive.segment.file.length=1048576",
                "-Daeron.dir=" + aeronDir));
        cmd.addAll(jvmArgs);
        cmd.addAll(List.of("-XX:ErrorFile=" + base.resolve("hs_err_pid%p.log"), "-cp", System.getProperty("java.class.path"),
                ENGINE_MAIN));
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Map<String, String> e = pb.environment();
        e.put("BASE_DIR", base.resolve("state").toString());
        e.put("AERON_DIR", aeronDir.toString());
        e.put("TRANSPORT_DRIVER_MODE", "embedded");
        e.put("TRANSPORT_IDLE_MODE", "backoff");
        e.put("TRANSPORT_LOG_TERM_LENGTH", "4m");
        e.put("TRANSPORT_SNAPSHOT_TERM_LENGTH", "4m");
        e.put("CLUSTER_NODE", "0");
        e.put("CLUSTER_ADDRESSES", "localhost");
        e.put("CLUSTER_PORT_BASE", String.valueOf(freePort()));
        e.put("ASSETS_NODE_PORT", String.valueOf(probePort));
        e.putAll(env);
        final Process p = pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.add(p);
        boolean ready = false;
        if (expectBoot) {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < deadline && p.isAlive() && !ready) {
                ready = ready(probePort);
                if (!ready) {
                    Thread.sleep(200);
                }
            }
            p.destroy(); // SIGTERM: the boot rows only need /ready
        }
        final boolean exited = p.waitFor(60, TimeUnit.SECONDS);
        if (!exited) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        final String out = Files.readString(log);
        final boolean launched = out.contains("Assets Engine node 0 starting")
                || Files.exists(base.resolve("state").resolve("archive")) || Files.exists(aeronDir.resolve("cnc.dat"));
        final Run r = new Run(exited ? p.exitValue() : -999, out, launched, ready);
        System.out.println("[11.6] " + label + ": exit=" + r.exit + " ready=" + r.ready + " aeronComponents="
                + r.aeronComponents + " guard lines: " + grep(out, "BOOT GUARD") + grep(out, "BOOT-GUARD"));
        return r;
    }

    private static String grep(final String out, final String needle) {
        final StringBuilder sb = new StringBuilder();
        for (final String l : out.split("\n")) {
            if (l.contains(needle)) {
                sb.append('<').append(l.trim()).append('>');
            }
        }
        return sb.toString();
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

    @Test
    public void stubModeWithPruneAndNoFloorExitsOneBeforeAnyAeronComponent() throws Exception {
        final Run r = engine("nofloor", Map.of("RCS_MODE", "shadow", "SNAPSHOT_PRUNE_ENABLED", "true",
                "SHADOW_FLOOR_ENABLED", "false"), List.of(), false);
        assertEquals("exit 1: " + r.out, 1, r.exit);
        assertFalse("the guard runs before any Aeron component: " + r.out, r.aeronComponents);
        assertTrue(r.out, r.out.contains("ASSETS BOOT GUARD: RCS_MODE=shadow needs ShadowFloor on the pruner or"
                + " SNAPSHOT_PRUNE_ENABLED=false (MF-4)"));
    }

    @Test
    public void stubModeWithPruneDisabledBoots() throws Exception {
        final Run r = engine("noprune", Map.of("RCS_MODE", "shadow", "SNAPSHOT_PRUNE_ENABLED", "false",
                "SHADOW_FLOOR_ENABLED", "false"), List.of(), true);
        assertTrue("boots to /ready: " + r.out, r.ready);
        assertTrue("the boot oracle sees a launch (it is not blind)", r.aeronComponents);
        assertTrue(r.out, r.out.contains("[BOOT-GUARD] rcsMode=shadow prune=false"));
    }

    @Test
    public void anUnknownRcsModeIsRefusedBeforeAnyAeronComponent() throws Exception {
        final Run r = engine("bogus", Map.of("RCS_MODE", "bogus"), List.of(), false);
        assertEquals("exit 1: " + r.out, 1, r.exit);
        assertFalse("refused before any Aeron component: " + r.out, r.aeronComponents);
        assertTrue(r.out, r.out.contains("RCS_MODE 'bogus' (expected off|shadow)"));
    }

    /**
     * The 3.5 refusal text carries the boot guard's prefix ({@code ASSETS BOOT GUARD: RCS_MODE 'x' (expected
     * off|shadow)}, G-S1). RED at W3 (the service parsed the settings in its no-arg constructor before the guard, so
     * the refusal had no prefix); GREEN once {@code AeronCluster.run} parses them under {@code StubModeBootGuard} first
     * and hands them to the service (D1).
     */
    @Test
    public void anUnknownRcsModeCarriesTheBootGuardPrefix() throws Exception {
        final Run r = engine("bogus-prefix", Map.of("RCS_MODE", "bogus"), List.of(), false);
        assertEquals("exit 1: " + r.out, 1, r.exit);
        assertFalse("refused before any Aeron component: " + r.out, r.aeronComponents);
        assertTrue("the G-S1 text with the boot guard prefix: " + grep(r.out, "bogus"),
                r.out.contains("ASSETS BOOT GUARD: RCS_MODE 'bogus' (expected off|shadow)"));
    }

    @Test
    public void theTestOnlyOverrideOpensTheGuard() throws Exception {
        final Run r = engine("unchecked", Map.of("RCS_MODE", "shadow", "SNAPSHOT_PRUNE_ENABLED", "true",
                "SHADOW_FLOOR_ENABLED", "false"), List.of("-Doxs.shadow.floor.unchecked=true"), true);
        assertTrue("the override lets the misconfiguration boot (the control's door): " + r.out, r.ready);
        assertTrue(r.aeronComponents);
        assertTrue(r.out, r.out.contains("[BOOT-GUARD] CRITICAL: RCS_MODE=shadow with SHADOW_FLOOR_ENABLED=false"));
    }

    // ------------------------------------------------------------------------------------------ in process

    @Test
    public void theRuntimeNetRefusesAPrunerWhoseWatermarkIsNotTheFloor() throws Exception {
        try (StubStory s = new StubStory(25500, "rcs-s4-11.6-net", StubStory.shadow())) {
            s.m.watermarkOverride = () -> Long.MAX_VALUE;
            s.rig.boot(s.m);
            s.rig.await(() -> !s.m.halts.isEmpty(), 60_000, "the runtime net must fire failFast");
            final String line = StubStory.line(s.rig.errTee.lines, "pruner watermark is not the ShadowFloor");
            System.out.println("GREEN 11.6 runtime net -> " + line);
            assertTrue("the halt names the net: " + s.rig.errTee.lines, line != null);
            assertEquals("no pruner was started", -1, ServiceProbe.prunesRun(s.m.service));
        }
    }

    /** The MF-4 oracle: after a stub snapshot and one pruner pass, the log was not purged below the stub. */
    private static void assertNoPurgeBelowTheStub(final StubStory s, final long pCm) {
        final long start = s.rig.logStart(s.m);
        if (start > 0 && start <= pCm) {
            throw new AssertionError("log purged below stub P_cm=" + pCm + " with no shadow (MF-4): log start " + start);
        }
    }

    private static long stubAndOnePrunerPass(final StubStory s) {
        s.deposits(900);
        final long before = ServiceProbe.prunesRun(s.m.service);
        final long pCm = s.snapshot();
        assertTrue("P_cm past the first segment: " + pCm, pCm > StubStory.SEGMENT);
        s.rig.await(() -> ServiceProbe.prunesRun(s.m.service) > before, 30_000,
                () -> "no pruner pass after the snapshot (prunesRun " + ServiceProbe.prunesRun(s.m.service) + ")");
        return pCm;
    }

    @Test
    public void withTheFloorWiredAStubSnapshotPurgesNothingBelowTheStub() throws Exception {
        try (StubStory s = new StubStory(25600, "rcs-s4-11.6-floor", StubStory.shadow()).start(true)) {
            final long pCm = stubAndOnePrunerPass(s);
            assertNoPurgeBelowTheStub(s, pCm);
            assertEquals("journal-tail reads with a stub and no shadow above P_cm (FIX-2)", 0,
                    ServiceProbe.rcs(s.m.service, "rcs_journal_tail_reads_total"));
            System.out.println("GREEN 11.6 floor wired: stub at " + pCm + ", pruner pass ran, log start "
                    + s.rig.logStart(s.m));
        }
    }

    /**
     * D2, the FULL case of the floor's precondition, on the real wiring: {@code RCS_MODE=off}, the floor is wired (the
     * runtime net held, no halt), the newest group is a full image read through {@code StubPeek}, so the pruner purges
     * to P_cm exactly as before S4. The STUB case on the same wiring is
     * {@link #withTheFloorWiredAStubSnapshotPurgesNothingBelowTheStub}: a peek that misread the stub as FULL would
     * purge to P_cm there and fail with the MF-4 text. The off-mode stub row and its control are the kit's
     * {@code ShadowFloorPrunerTest} precondition rows.
     */
    @Test
    public void offModeWithAFullNewestGroupPrunesToTheSnapshotAsBefore() throws Exception {
        try (StubStory s = new StubStory(27100, "rcs-s4-d2-off-full", StubStory.off()).start(true)) {
            final long pCm = stubAndOnePrunerPass(s);
            s.rig.await(() -> s.rig.logStart(s.m) > 0, 30_000,
                    () -> "off mode with a full newest group did not prune (log start " + s.rig.logStart(s.m)
                            + ", P_cm " + pCm + "): the floor precondition must answer P_cm");
            final long start = s.rig.logStart(s.m);
            assertTrue("purged no further than P_cm=" + pCm + ": log start " + start, start <= pCm);
            assertTrue("no halt: " + s.m.halts, s.m.halts.isEmpty());
            assertTrue("the floor read the newest group as FULL: " + s.rig.tee.lines,
                    StubStory.line(s.rig.tee.lines, "is FULL: the floor is P_cm") != null);
            // FIX-2: a FULL newest group never asks skipSafe, so no wake replays the money-journal tail
            s.rig.await(() -> ServiceProbe.rcs(s.m.service, "rcs_journal_tail_reads_skipped_total") > 0, 30_000,
                    "the housekeeping refresh step ran at least once");
            assertEquals("journal-tail reads with a FULL newest group", 0,
                    ServiceProbe.rcs(s.m.service, "rcs_journal_tail_reads_total"));
            System.out.println("GREEN D2 off mode, full newest group at " + pCm + ": log start " + start);
        }
    }

    @Test
    public void controlTheGuardOffPurgesBelowTheStub() throws Exception {
        final RcsSettings floorOff = RcsSettings.with(Map.of(RcsSettings.Key.RCS_MODE, "shadow",
                RcsSettings.Key.SHADOW_FLOOR_ENABLED, "false"));
        try (StubStory s = new StubStory(25700, "rcs-s4-11.6-nofloor", floorOff).start(true)) {
            final long pCm = stubAndOnePrunerPass(s);
            RcsDryRun.nc("11.6-guard-off", "log purged below stub P_cm=" + pCm + " with no shadow (MF-4)",
                    () -> assertNoPurgeBelowTheStub(s, pCm));
        }
    }

    // ------------------------------------------------------------------------------------------ helpers

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
            for (final Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }
}

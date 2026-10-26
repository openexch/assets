// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.cluster.RecordingLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RCS S1 boot guards: zero STANDBY_SNAPSHOT recording-log entries at start, and
 * {@code acceptStandbySnapshots == false}. Seeds use the public RecordingLog API only (a complete
 * standby group plus {@code invalidateLatestSnapshot()}), the shape Aeron itself would act on.
 * Mutates a JVM-global system property: must not run under in-JVM parallelism.
 */
public class RecordingLogStandbyGuardTest {

    private static final String PROP = "aeron.cluster.accept.standby.snapshots";
    private static final String ENDPOINT = "localhost:20599";

    private Path tmp;
    private String savedProp;

    @Before
    public void setUp() throws Exception {
        tmp = Files.createTempDirectory("assets-standby-guard-");
        savedProp = System.getProperty(PROP);
        System.clearProperty(PROP);
    }

    @After
    public void tearDown() throws Exception {
        if (savedProp == null) {
            System.clearProperty(PROP);
        } else {
            System.setProperty(PROP, savedProp);
        }
        try (Stream<Path> s = Files.walk(tmp)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private ClusterConfig config(final Path base) throws Exception {
        final ClusterConfig cc = ClusterConfig.create(0, List.of("localhost"), freePort(),
                new AssetsClusteredService());
        cc.baseDir(base.toFile());
        return cc;
    }

    private static File clusterDir(final Path base) {
        final File dir = new File(base.toFile(), ClusterConfig.CLUSTER_SUB_DIR);
        dir.mkdirs();
        return dir;
    }

    private interface Seed {
        void apply(RecordingLog log);
    }

    private static void seed(final Path base, final Seed seed) {
        try (RecordingLog log = new RecordingLog(clusterDir(base), true)) {
            seed.apply(log);
        }
    }

    /** Complete standby group (service 0 then -1) at one position, optionally invalidated. */
    private static void standbyGroup(final RecordingLog log, final boolean invalidate) {
        log.appendTerm(1, 0, 0, 1_000);
        log.appendStandbySnapshot(7, 0, 0, 4096, 2_000, 0, ENDPOINT);
        log.appendStandbySnapshot(8, 0, 0, 4096, 2_000, -1, ENDPOINT);
        if (invalidate) {
            assertTrue(log.invalidateLatestSnapshot());
        }
    }

    private static String sha(final Path base) throws Exception {
        final Path f = base.resolve(ClusterConfig.CLUSTER_SUB_DIR).resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        if (!Files.exists(f)) {
            return "absent";
        }
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(f)));
    }

    @Test
    public void absentRecordingLogIsGenesisAndPasses() throws Exception {
        final ClusterConfig cc = config(tmp);
        cc.enforceBootGuards();
        assertEquals("absent", sha(tmp));
    }

    @Test
    public void termAndSnapshotEntriesOnlyPass() throws Exception {
        seed(tmp, log -> {
            log.appendTerm(1, 0, 0, 1_000);
            log.appendSnapshot(2, 0, 0, 4096, 2_000, 0);
            log.appendSnapshot(3, 0, 0, 4096, 2_000, -1);
        });
        final String before = sha(tmp);
        config(tmp).enforceBootGuards();
        assertEquals(before, sha(tmp));
    }

    @Test
    public void validStandbyGroupIsRefusedAndListed() throws Exception {
        seed(tmp, log -> standbyGroup(log, false));
        final String before = sha(tmp);
        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> config(tmp).enforceBootGuards());
        final String m = r.getMessage();
        assertTrue(m, m.startsWith("ASSETS BOOT GUARD: "));
        assertTrue(m, m.contains("recordingId=7") && m.contains("recordingId=8"));
        assertTrue(m, m.contains("logPosition=4096") && m.contains("archiveEndpoint=" + ENDPOINT));
        assertTrue(m, m.contains("isValid=true"));
        assertEquals(before, sha(tmp));
    }

    @Test
    public void invalidatedStandbyGroupIsStillRefused() throws Exception {
        seed(tmp, log -> standbyGroup(log, true));
        final String before = sha(tmp);
        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> config(tmp).enforceBootGuards());
        assertTrue(r.getMessage(), r.getMessage().contains("isValid=false"));
        assertFalse(r.getMessage(), r.getMessage().contains("isValid=true"));
        assertEquals(before, sha(tmp));
    }

    @Test
    public void contextAcceptingStandbySnapshotsIsRefused() throws Exception {
        final ClusterConfig cc = config(tmp);
        cc.consensusModuleContext().acceptStandbySnapshots(true);
        assertThrows(StandbySnapshotBootGuard.Refused.class, cc::enforceBootGuards);
        assertEquals("absent", sha(tmp));
    }

    @Test
    public void trueSystemPropertyIsRefusedEvenThoughContextReadsFalse() throws Exception {
        System.setProperty(PROP, "true");
        final ClusterConfig cc = config(tmp);
        assertFalse(cc.consensusModuleContext().acceptStandbySnapshots());
        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                cc::enforceBootGuards);
        assertTrue(r.getMessage(), r.getMessage().contains(PROP));
        assertEquals("absent", sha(tmp));
    }

    @Test
    public void clusterConfigPinsAcceptStandbySnapshotsFalseDespiteProperty() throws Exception {
        System.setProperty(PROP, "true");
        assertFalse(config(tmp).consensusModuleContext().acceptStandbySnapshots());
    }

    // ---- forked production entry (shape of AssetsApplyFailureFenceTest) ----

    private Process fork(final Path base, final Path log) throws Exception {
        final ProcessBuilder pb = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                "com.openexchange.assets.Main");
        pb.environment().put("BASE_DIR", base.toString());
        pb.environment().put("AERON_DIR", base.resolve("aeron-driver").toString());
        pb.environment().put("CLUSTER_NODE", "0");
        pb.environment().put("CLUSTER_ADDRESSES", "localhost");
        pb.environment().put("CLUSTER_PORT_BASE", String.valueOf(freePort()));
        pb.environment().remove("TRANSPORT_DRIVER_MODE");
        return pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    @Test
    public void forkedNodeWithStandbyEntryExitsOneAndTouchesNothing() throws Exception {
        final Path base = Files.createDirectory(tmp.resolve("refused"));
        seed(base, log -> standbyGroup(log, false));
        final String before = sha(base);
        final Path out = tmp.resolve("refused.log");
        final Process child = fork(base, out);
        try {
            assertTrue("child must exit promptly: " + Files.readString(out), child.waitFor(15, TimeUnit.SECONDS));
            final String output = Files.readString(out);
            assertEquals(output, 1, child.exitValue());
            assertTrue(output, output.contains("ASSETS BOOT GUARD"));
            assertEquals(before, sha(base));
            assertFalse("no archive may be created", Files.exists(base.resolve(ClusterConfig.ARCHIVE_SUB_DIR)));
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void forkedNodeWithoutStandbyEntryKeepsRunning() throws Exception {
        final Path base = Files.createDirectory(tmp.resolve("clean"));
        final Path out = tmp.resolve("clean.log");
        final Process child = fork(base, out);
        try {
            assertFalse("control: an unseeded node must still be alive after 5 s: " + Files.readString(out),
                    child.waitFor(5, TimeUnit.SECONDS));
            assertFalse(Files.readString(out), Files.readString(out).contains("ASSETS BOOT GUARD"));
            // Alive is not enough: a child stuck before the guard would pass the two lines above. The guard ran and passed.
            final String output = Files.readString(out);
            assertTrue(output, output.contains("[BOOT-GUARD] standby=0 acceptStandbySnapshots=false"));
        } finally {
            child.destroyForcibly();
            child.waitFor(10, TimeUnit.SECONDS);
        }
    }
}

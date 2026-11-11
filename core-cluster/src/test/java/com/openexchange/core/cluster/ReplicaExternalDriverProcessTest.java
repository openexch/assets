// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaContexts;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowKind;
import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.archive.ArchiveTool;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The external-driver leg: the production entry point ({@code java ... com.openexchange.core.Main replica}) in a
 * child JVM on an EXTERNAL media driver, following three in-process core members (the {@link ReplicaRig} members, each
 * on its own embedded driver; the replica reaches them over UDP only). The external driver is the real C aeronmd when
 * {@code -Dreplica.aeronmd=<path>} or {@code REPLICA_AERONMD} names one (its directory is its {@code LD_LIBRARY_PATH}),
 * else an {@code io.aeron.driver.MediaDriver} child JVM; the row prints which. Rows, in one rig:
 * <ol>
 *   <li>the replica follows and writes an ALIGNED shadow at the members' P_cm (a complete copy whose payload the kit
 *       loader reads back digest-checked) whose payload equals every
 *       member's image there; its log says {@code Driver mode EXTERNAL}; {@code /ready} is 200;</li>
 *   <li>SIGTERM: exit 143, and nothing is left open: within the driver's publication linger (the driver's client
 *       liveness is set to 30 s, so a killed client's resources would outlive the check) the driver holds no
 *       publication, sender, subscriber or recording counter, and the replica archive's catalog has no recording
 *       without a stop position;</li>
 *   <li>control: {@code TRANSPORT_DRIVER_MODE=embedded} with the external driver's directory is refused by the
 *       embedded launch's own error ({@code Active media driver detected}, exit 1), and the external driver is alive
 *       with its CnC in place afterwards: the leg above ran on the external driver, and an operator's mode mistake
 *       never wipes a shared aeronmd directory;</li>
 *   <li>restart on the same dirs and the same driver ({@code restore_path=LOCAL_SHADOW}), then kill -9 of the driver:
 *       exit 1 within the client driver timeout (10 s) plus the fatal grace, stderr
 *       {@code media driver lost in EXTERNAL driver mode}.</li>
 * </ol>
 *
 * <p>The oracle mismatch hold on the external driver ({@link #aHeldReplicaStopsOnSigtermAndHaltsOnDriverLoss}, its own
 * rig): every follower's service snapshot forged in place (one payload byte, the envelope re-sealed), the child holds
 * ({@code /ready} 503 {@code recovery=fenced:oracle mismatch at <P>: ...}, {@code shadow_oracle_mismatch_total 1},
 * alive); SIGTERM exits 143; a restart on the same dirs re-latches at the same P; kill -9 of the driver while held
 * exits 1 within the client driver timeout plus the fatal grace, stderr naming the driver loss.</p>
 *
 * <p>The pull leg on the external driver ({@link #anExternalDriverReplicaPullsAtBootstrapAndExitsThreeToReBootstrap},
 * its own rig, both fixed pull endpoints set as on an isolated host): the members' logs purged past 0 with a genuine
 * ALIGNED at P_cm on member 1, the child bootstraps by a pull ({@code restore_path=PULLED_SHADOW}), verifies, and a
 * SIGTERM exits 143; the members then prune past its log with a newer ALIGNED on member 1, the restarted child pulls it
 * and exits 3 (a decided restart: {@code cluster/} moved to {@code cluster.rebootstrap-*} by its close); the next start
 * restores LOCAL_SHADOW from the pulled shadow and a SIGTERM exits 143.</p>
 */
public class ReplicaExternalDriverProcessTest {

    private static final int PORT_BASE = 33000; // members 33000/33100/33200, the replica at slot 3 (33300)
    private static final int PORT_BASE_HOLD = 47400;
    private static final int PORT_BASE_PULL = 55400; // members 55400/55500/55600, the replica at slot 3 (55700)
    private static final int PULL_SEGMENT = 64 * 1024;
    private static final long REBOOTSTRAP_WAIT_MS = 300_000;
    /** The smallest verify wait the rig's backup cycle allows (2 x 12 000 + the 12 000 cool-down). */
    private static final long VERIFY_WAIT_MS = 36_000;
    private static final String CREDENTIAL = "8:REPLICA:" + "88".repeat(32);
    private static final long CLIENT_DRIVER_TIMEOUT_MS = 10_000; // Aeron's client default, Aeron.Context
    private static final long FATAL_GRACE_MS = 5_000; // EngineLifecycle.DEFAULT_FATAL_EXIT_GRACE_MS
    private static final long DRIVER_CLIENT_LIVENESS_NS = TimeUnit.SECONDS.toNanos(30);
    /** Both drivers refuse an unblock timeout at or below the client liveness. */
    private static final long DRIVER_UNBLOCK_NS = TimeUnit.SECONDS.toNanos(60);
    private static final Set<Integer> HELD_BY_A_CLIENT = Set.of(AeronCounters.DRIVER_PUBLISHER_LIMIT_TYPE_ID,
            AeronCounters.DRIVER_SENDER_POSITION_TYPE_ID, AeronCounters.DRIVER_SUBSCRIBER_POSITION_TYPE_ID,
            AeronCounters.DRIVER_RECEIVER_POS_TYPE_ID, AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID,
            AeronCounters.DRIVER_SENDER_LIMIT_TYPE_ID, AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID);
    private static final Pattern STOP = Pattern.compile("recordingId=(\\d+).*?stopPosition=(-?\\d+)");

    /** The external driver child: the C aeronmd or a Java MediaDriver. */
    static final class Driver {
        final Process process;
        final Path dir;
        final Path log;
        final String kind;

        Driver(final Process process, final Path dir, final Path log, final String kind) {
            this.process = process;
            this.dir = dir;
            this.log = log;
            this.kind = kind;
        }
    }

    @Test(timeout = 1_500_000)
    public void anExternalDriverReplicaFollowsWritesAlignedAndExitsByTheRules() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-external-driver")) {
            final Path base = r.rig.tmp.toPath();
            final List<ReplicaChild> children = new ArrayList<>();
            Driver driver = null;
            try {
                r.bootAll();
                final Member leader = r.rig.awaitLeader(null);
                final Member host = r.rig.followerOtherThan(leader, null);
                r.story(host, 4);
                driver = startDriver(base);
                final Path state = base.resolve("replica-state");
                final Runnable pump = () -> r.rig.pumpMs(100); // keeps the test's cluster sessions alive
                final int nodePort = ReplicaRig.freeTcp();

                // 1. follow and ALIGNED on the external driver
                final ReplicaChild first = ReplicaChild.start(base, "external-1",
                        replicaEnv(driver, state, nodePort, "external"), List.of("replica"));
                children.add(first);
                first.awaitLine("[REPLICA] running", 120_000, pump);
                assertTrue("external driver mode" + first.tails(), first.out().contains("Driver mode EXTERNAL"));
                first.awaitLine("[REPLICA] bootstrap done: restore_path=GENESIS", 120_000, pump);
                awaitReady(first, nodePort, 120_000, pump);
                final long pCm = r.rig.snapshotAll();
                final byte[] image;
                try (Clients c = new Clients(driver)) {
                    final LogicalShadow aligned = c.awaitShadow(pCm, ShadowKind.ALIGNED, 90_000, first, nodePort, pump);
                    image = ReplicaRig.payloadOf(c.aeron, c.archive, aligned);
                }
                for (final Member m : r.rig.live()) {
                    ReplicaRig.assertPayloadEqualsMember(pCm, image, r.memberImage(m.id, pCm));
                }
                System.out.println("GREEN external follow: ALIGNED (digest-checked load) at P_cm=" + pCm + " (" + image.length
                        + " B) equals " + r.rig.live().size() + " members' images; driver " + driver.kind + "; "
                        + ReplicaChild.line(first.out(), "Driver mode EXTERNAL"));

                // 2. SIGTERM -> 143, nothing left open
                r.traffic(10, 2);
                first.process.destroy(); // SIGTERM
                final int sigterm = first.awaitExit(30_000);
                r.rig.pumpMs(100);
                assertEquals("SIGTERM exits 143" + first.tails(), 143, sigterm);
                final List<String> held = awaitNoClientResources(driver, 7_000);
                assertTrue("the driver still holds the replica's resources after a SIGTERM stop: " + held
                        + first.tails(), held.isEmpty());
                final List<String> open = openRecordings(state.resolve("archive").toFile());
                assertTrue("recordings left open (no stop position) after a SIGTERM stop: " + open + first.tails(),
                        open.isEmpty());
                System.out.println("GREEN SIGTERM: exit 143; no publication, subscription or recording counter"
                        + " on the driver; every recording in the catalog has a stop position");

                // 3. control: embedded mode on the external driver's directory is refused by the embedded launch
                final ReplicaChild control = ReplicaChild.start(base, "embedded-on-external-dir",
                        replicaEnv(driver, base.resolve("replica-state-control"), ReplicaRig.freeTcp(), "embedded"),
                        List.of("replica"));
                children.add(control);
                final int controlExit = control.awaitExit(60_000);
                final String controlText = control.out() + control.err();
                assertTrue("the embedded launch with the external dir must be refused by the embedded launch's own"
                        + " error" + control.tails(), controlText.contains("Active media driver detected"));
                assertEquals("the refused embedded launch exits 1" + control.tails(), 1, controlExit);
                assertTrue("the external driver survived the control: " + Files.readString(driver.log),
                        driver.process.isAlive());
                assertTrue("the external driver's CnC survived the control",
                        Files.exists(driver.dir.resolve("cnc.dat")));
                System.out.println("NC EMBEDDED_WITH_EXTERNAL_DIR -> "
                        + ReplicaChild.line(controlText, "Active media driver detected"));

                // 4. restart on the same dirs, then kill -9 of the driver -> exit 1
                r.windowGap();
                final ReplicaChild second = ReplicaChild.start(base, "external-2",
                        replicaEnv(driver, state, nodePort, "external"), List.of("replica"));
                children.add(second);
                second.awaitLine("[REPLICA] bootstrap done: restore_path=LOCAL_SHADOW", 120_000, pump);
                // a restart is UNVERIFIED (503 unverified continuation) until its first ALIGNED
                // compare with a member snapshot passes; the members snapshot here instead of waiting for the cadence
                final long verifyAt = r.rig.snapshotAll();
                second.awaitLine("[REPLICA] alignment: the state at " + verifyAt + " equals the member snapshot", 120_000,
                        pump);
                awaitReady(second, nodePort, 120_000, pump);
                final long t0 = System.nanoTime();
                driver.process.destroyForcibly(); // kill -9 of the media driver
                final int killed = second.awaitExit(CLIENT_DRIVER_TIMEOUT_MS + FATAL_GRACE_MS + 30_000);
                final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                System.out.println("GREEN driver kill -9: exit=" + killed + " after " + ms + " ms; "
                        + ReplicaChild.line(second.err(), "media driver lost in EXTERNAL driver mode"));
                assertEquals("a driver loss exits 1" + second.tails(), 1, killed);
                assertTrue("within the client driver timeout plus the fatal grace: " + ms + " ms",
                        ms < CLIENT_DRIVER_TIMEOUT_MS + FATAL_GRACE_MS + 3_000);
                assertTrue("stderr names the driver loss" + second.tails(),
                        second.err().contains("media driver lost in EXTERNAL driver mode"));
                assertFalse("no JVM crash" + second.tails(),
                        (second.out() + second.err()).contains("A fatal error has been detected by the Java Runtime"));
            } finally {
                for (final ReplicaChild c : children) {
                    c.destroyForcibly();
                }
                if (driver != null) {
                    driver.process.destroyForcibly();
                    driver.process.waitFor(20, TimeUnit.SECONDS);
                    deleteTree(driver.dir);
                }
            }
        }
    }

    @Test(timeout = 1_500_000)
    public void aHeldReplicaStopsOnSigtermAndHaltsOnDriverLoss() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_HOLD, "replica-external-driver-hold")) {
            final Path base = r.rig.tmp.toPath();
            final List<ReplicaChild> children = new ArrayList<>();
            Driver driver = null;
            try {
                r.bootAll();
                final Member leader = r.rig.awaitLeader(null);
                final Member host = r.rig.followerOtherThan(leader, null);
                r.story(host, 4);
                driver = startDriver(base);
                final Path state = base.resolve("replica-state");
                final Runnable pump = () -> r.rig.pumpMs(100);
                final int nodePort = ReplicaRig.freeTcp();
                final Map<String, String> env = replicaEnv(driver, state, nodePort, "external", PORT_BASE_HOLD);
                env.put("REPLICA_VERIFY_WAIT_MS", Long.toString(VERIFY_WAIT_MS));

                // 1. forge -> hold -> SIGTERM -> 143
                final ReplicaChild first = ReplicaChild.start(base, "hold-1", env, List.of("replica"));
                children.add(first);
                first.awaitLine("[REPLICA] bootstrap done: restore_path=GENESIS", 120_000, pump);
                awaitReady(first, nodePort, 120_000, pump);
                final long p0 = r.rig.snapshotAll();
                try (Clients c = new Clients(driver)) {
                    c.awaitShadow(p0, ShadowKind.ALIGNED, 90_000, first, nodePort, pump);
                }
                long p = -1;
                for (int attempt = 1; attempt <= 3 && p < 0; attempt++) {
                    r.windowGap();
                    final long at = r.rig.snapshotAll();
                    for (final Member m : r.rig.live()) {
                        if (m.id != leader.id) {
                            r.forgeMemberSnapshot(m, at, true);
                        }
                    }
                    final long deadline = System.currentTimeMillis() + 120_000;
                    while (System.currentTimeMillis() < deadline) {
                        final String text = first.out() + first.err();
                        if (text.contains("oracle mismatch at " + at + ": ")) {
                            p = at;
                            break;
                        }
                        if (metrics(nodePort).contains("shadow_align_verified_position " + at + "\n")) {
                            System.out.println("[external-driver] forge attempt " + attempt + " at " + at
                                    + " lost the race with the backup query");
                            break;
                        }
                        assertTrue("the child exited at the forge" + first.tails(), first.process.isAlive());
                        pump.run();
                    }
                }
                assertTrue("the forgery never reached the child's backup before its query" + first.tails(), p > 0);
                r.rig.pumpMs(5_000);
                assertTrue("the process exited before the metrics were read" + first.tails(), first.process.isAlive());
                final String[] ready = readyOf(nodePort);
                assertEquals("/ready while held: " + ready[1], "503", ready[0]);
                assertTrue("/ready names the mismatch: " + ready[1],
                        ready[1].contains("recovery=fenced:oracle mismatch at " + p + ": "));
                assertTrue("/metrics while held", metrics(nodePort).contains("shadow_oracle_mismatch_total 1\n"));
                assertTrue("the hold is logged through the core logger" + first.tails(),
                        (first.out() + first.err()).contains("CRITICAL replica HOLD: oracle mismatch at " + p));
                first.process.destroy(); // SIGTERM
                final int sigterm = first.awaitExit(30_000);
                assertEquals("SIGTERM while held exits 143" + first.tails(), 143, sigterm);
                System.out.println("GREEN external hold: held at " + p + " (" + ready[1].trim() + "), SIGTERM exit 143");

                // 2. restart: re-latched at P; kill -9 of the driver while held -> exit 1, driver loss named
                r.windowGap();
                final ReplicaChild second = ReplicaChild.start(base, "hold-2", env, List.of("replica"));
                children.add(second);
                second.awaitLine("CRITICAL replica HOLD: oracle mismatch at " + p, 240_000, pump);
                r.rig.pumpMs(2_000);
                assertTrue("the restarted child exited instead of holding" + second.tails(), second.process.isAlive());
                final long t0 = System.nanoTime();
                driver.process.destroyForcibly(); // kill -9 of the media driver
                final int killed = second.awaitExit(CLIENT_DRIVER_TIMEOUT_MS + FATAL_GRACE_MS + 30_000);
                final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertEquals("a driver loss while held exits 1" + second.tails(), 1, killed);
                assertTrue("within the client driver timeout plus the fatal grace: " + ms + " ms",
                        ms < CLIENT_DRIVER_TIMEOUT_MS + FATAL_GRACE_MS + 3_000);
                assertTrue("stderr names the driver loss" + second.tails(),
                        second.err().contains("media driver lost in EXTERNAL driver mode"));
                assertFalse("the exit is not blamed on the hold bound" + second.tails(),
                        (second.out() + second.err()).contains("the oracle mismatch hold ended"));
                System.out.println("GREEN external hold: re-latched at " + p + " after a restart; driver kill -9 while"
                        + " held: exit 1 after " + ms + " ms; "
                        + ReplicaChild.line(second.err(), "media driver lost in EXTERNAL driver mode"));
            } finally {
                for (final ReplicaChild c : children) {
                    c.destroyForcibly();
                }
                if (driver != null) {
                    driver.process.destroyForcibly();
                    driver.process.waitFor(20, TimeUnit.SECONDS);
                    deleteTree(driver.dir);
                }
            }
        }
    }

    /**
     * The replica's bootstrap pull and re-bootstrap on an external driver: three in-process members, a child replica
     * and an external driver child in one runner, about 80 s with several cluster snapshots. Nightly only
     * ({@code -Dcore.pull.external=true}): on a loaded PR runner the in-process members lose their leader heartbeat.
     */
    @Test(timeout = 1_500_000)
    public void anExternalDriverReplicaPullsAtBootstrapAndExitsThreeToReBootstrap() throws Exception {
        org.junit.Assume.assumeTrue("the external-driver pull rows run only with -Dcore.pull.external=true",
                Boolean.getBoolean("core.pull.external"));
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE_PULL, "replica-external-driver-pull", 3, "64k",
                PULL_SEGMENT))) {
            final Path base = r.rig.tmp.toPath();
            final List<ReplicaChild> children = new ArrayList<>();
            Driver driver = null;
            try {
                r.bootAll();
                final Member leader = r.rig.awaitLeader(null);
                final Member host = r.rig.followerOtherThan(leader, null);
                r.story(host, 4);
                final Member m1 = r.rig.members.get(1);
                final Member keeper = r.rig.members.get(2);
                // a live copy of member 1's log in member 2's archive (from 0, following): the test producer builds
                // its shadows from it after the members' logs were purged
                final long copy = r.rig.liveLogCopy(m1, keeper);
                driver = startDriver(base);
                final Path state = base.resolve("replica-state");
                final Runnable pump = () -> r.rig.pumpMs(100);
                final int nodePort = ReplicaRig.freeTcp();
                final Map<String, String> env = replicaEnv(driver, state, nodePort, "external", PORT_BASE_PULL);
                env.put("SHADOW_PULL_SOURCES", r.rig.sources(r.rig.members.get(0), m1, keeper));
                env.put("SHADOW_PULL_PERIOD_MS", "1000");
                env.put("SHADOW_PULL_REPLICATION_ENDPOINT", "localhost:" + freeUdp());
                env.put("SHADOW_PULL_RESPONSE_ENDPOINT", "localhost:" + freeUdp());

                // 1. B4: every member's log purged past 0, an ALIGNED at P_cm on member 1; the child pulls it
                r.tool.deposits(600);
                final long pA = r.rig.snapshotAll();
                r.tool.deposits(60);
                pruneAndProduce(r, m1, keeper, copy, pA, -1);
                final ReplicaChild first = ReplicaChild.start(base, "pull-1", env, List.of("replica"));
                children.add(first);
                first.awaitLine("[REPLICA] bootstrap done: restore_path=PULLED_SHADOW", 180_000, pump);
                assertTrue("the pull is logged" + first.tails(), first.out().contains("[SHADOW-PULL] pulled "));
                final long verifyAt = r.rig.snapshotAll();
                first.awaitLine("[REPLICA] alignment: the state at " + verifyAt + " equals the member snapshot", 120_000,
                        pump);
                awaitReady(first, nodePort, 120_000, pump);
                first.process.destroy(); // SIGTERM
                final int sigterm = first.awaitExit(30_000);
                assertEquals("SIGTERM after a pulled bootstrap exits 143" + first.tails(), 143, sigterm);
                final long lStopBound = leader.cfg.consensusModuleContext().commitPositionCounter().get();
                System.out.println("GREEN external B4 pull: restore_path=PULLED_SHADOW from P_cm=" + pA + " over both"
                        + " fixed endpoints; verified at " + verifyAt + "; SIGTERM exit 143; "
                        + ReplicaChild.line(first.out(), "[SHADOW-PULL] pulled "));

                // 2. the members prune past the child's log with a newer ALIGNED on member 1: the restart exits 3
                r.tool.deposits(900);
                r.rig.snapshotAll();
                r.tool.deposits(300);
                final long pB = r.rig.snapshotAll();
                r.tool.deposits(60);
                pruneAndProduce(r, m1, keeper, copy, pB, lStopBound);
                r.windowGap();
                final ReplicaChild second = ReplicaChild.start(base, "pull-2", env, List.of("replica"));
                children.add(second);
                final long t0 = System.nanoTime();
                final int exit = awaitExitPumping(second, REBOOTSTRAP_WAIT_MS, pump);
                final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertEquals("a decided re-bootstrap exits 3" + second.tails(), 3, exit);
                assertTrue("the restart is logged" + second.tails(), second.err().contains(
                        "replica re-bootstrap: members pruned past L.stop=") && second.err().contains(
                        "; pulled ALIGNED at " + pB + " from "));
                final File[] moved = state.toFile().listFiles((d, n) -> n.startsWith("cluster.rebootstrap-"));
                assertTrue("cluster/ moved aside", moved != null && moved.length == 1);
                assertTrue("the backup RecordingLog moved", new File(moved[0], "recording.log").isFile());
                assertFalse("BASE_DIR/cluster is gone", state.resolve("cluster").toFile().exists());
                System.out.println("GREEN external re-bootstrap: exit 3 after " + ms + " ms, moved to " + moved[0].getName()
                        + "; " + ReplicaChild.line(second.err(), "replica re-bootstrap: members pruned past"));

                // 3. the supervised restart: LOCAL_SHADOW from the pulled shadow, then SIGTERM -> 143
                r.windowGap();
                final ReplicaChild third = ReplicaChild.start(base, "pull-3", env, List.of("replica"));
                children.add(third);
                third.awaitLine("[REPLICA] bootstrap done: restore_path=LOCAL_SHADOW", 180_000, pump);
                r.rig.pumpMs(2_000);
                third.process.destroy(); // SIGTERM
                assertEquals("SIGTERM after the re-bootstrap exits 143" + third.tails(), 143, third.awaitExit(30_000));
                System.out.println("GREEN external restart after the re-bootstrap: "
                        + ReplicaChild.line(third.out(), "bootstrap done: restore_path=LOCAL_SHADOW") + "; SIGTERM 143");
            } finally {
                for (final ReplicaChild c : children) {
                    c.destroyForcibly();
                }
                if (driver != null) {
                    driver.process.destroyForcibly();
                    driver.process.waitFor(20, TimeUnit.SECONDS);
                    deleteTree(driver.dir);
                }
            }
        }
    }

    /**
     * Every member's log purged to the segment boundary below {@code p} (which must lie past {@code mustPass}), then
     * a genuine ALIGNED at {@code p} on {@code m1}, built from the live log copy (it still starts at 0).
     */
    private static void pruneAndProduce(final ReplicaRig r, final Member m1, final Member keeper, final long copy,
                                        final long p, final long mustPass) {
        final long boundary = p / PULL_SEGMENT * PULL_SEGMENT;
        assertTrue("the purge boundary " + boundary + " lies past " + mustPass, boundary > Math.max(0, mustPass));
        final com.openexchange.cluster.testkit.RecordedLog log = r.rig.captureCopy(m1, keeper, copy);
        for (final Member m : r.rig.live()) {
            assertEquals("member " + m.id + " log start after the purge", boundary, r.rig.purgeLogTo(m, boundary));
        }
        final com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written w = r.rig.producer(m1)
                .produce(new com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who(
                        com.openexchange.cluster.shadow.ProducerKind.FOLLOWER, 800 + (int)(p % 100)), log, p);
        assertTrue(w.toString(), w.ok());
        System.out.println("[external-driver] members purged to " + boundary + "; ALIGNED at " + p + " on member "
                + m1.id);
    }

    private static int freeUdp() throws IOException {
        try (java.net.DatagramSocket s = new java.net.DatagramSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** The child's exit status, pumping the test's cluster sessions while it runs. */
    private static int awaitExitPumping(final ReplicaChild child, final long timeoutMs, final Runnable pump)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (child.process.isAlive()) {
            assertTrue(child.name + " did not exit within " + timeoutMs + " ms" + child.tails(),
                    System.nanoTime() < deadline);
            pump.run();
        }
        return child.process.exitValue();
    }

    // ------------------------------------------------------------------------------------------------ helpers

    /** {@code GET /ready}: {status, body}. */
    private static String[] readyOf(final int port) throws IOException {
        final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + port + "/ready").toURL()
                .openConnection();
        c.setConnectTimeout(1_000);
        c.setReadTimeout(2_000);
        final int code = c.getResponseCode();
        final java.io.InputStream in = code >= 400 ? c.getErrorStream() : c.getInputStream();
        final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        c.disconnect();
        return new String[] {Integer.toString(code), body};
    }

    private static Map<String, String> replicaEnv(final Driver driver, final Path state, final int nodePort,
                                                  final String mode) {
        return replicaEnv(driver, state, nodePort, mode, PORT_BASE);
    }

    private static Map<String, String> replicaEnv(final Driver driver, final Path state, final int nodePort,
                                                  final String mode, final int portBase) {
        final Map<String, String> env = new HashMap<>();
        env.put("TRANSPORT_DRIVER_MODE", mode);
        env.put("AERON_DIR", driver.dir.toString());
        env.put("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "30000");
        env.put("TRANSPORT_IDLE_MODE", "backoff");
        env.put("CLUSTER_ADDRESSES", "localhost,localhost,localhost");
        env.put("CLUSTER_PORT_BASE", Integer.toString(portBase));
        env.put("CLUSTER_PORT_STRIDE", Integer.toString(ReplicaRig.STRIDE));
        env.put("CLUSTER_REPLICA_SLOT", Integer.toString(ReplicaRig.SLOT));
        env.put("CLUSTER_REPLICA_ADDRESS", "localhost");
        env.put("BASE_DIR", state.toString());
        env.put("CORE_AUTH_CREDENTIAL", CREDENTIAL);
        env.put("CORE_NODE_PORT", Integer.toString(nodePort));
        env.put("REPLICA_BACKUP_QUERY_INTERVAL_MS", "12000");
        env.put("REPLICA_SHADOW_BUFFER_MB", "16");
        env.put("SHADOW_TERM_LENGTH", "1m");
        env.put("SHADOW_SCAN_PERIOD_MS", "500");
        env.put("SHADOW_DEEP_READ_PACE_MS", "0");
        return env;
    }

    private static String aeronmd() {
        String v = System.getProperty("replica.aeronmd");
        if (v == null || v.isEmpty()) {
            v = System.getenv("REPLICA_AERONMD");
        }
        if (v == null || v.isEmpty()) {
            return null;
        }
        assertTrue("not an executable aeronmd: " + v, Files.isExecutable(Path.of(v)));
        return v;
    }

    private static Driver startDriver(final Path base) throws Exception {
        final Path shm = Path.of("/dev/shm");
        final Path dir = Files.isDirectory(shm) && Files.isWritable(shm)
                ? Files.createTempDirectory(shm, "replica-external-driver-aeronmd-")
                : Files.createTempDirectory("replica-external-driver-aeronmd-");
        final Path log = base.resolve("driver.log");
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
            env.put("AERON_THREADING_MODE", "SHARED");
            env.put("AERON_CLIENT_LIVENESS_TIMEOUT", Long.toString(DRIVER_CLIENT_LIVENESS_NS));
            env.put("AERON_PUBLICATION_UNBLOCK_TIMEOUT", Long.toString(DRIVER_UNBLOCK_NS));
        } else {
            final List<String> cmd = ReplicaChild.java();
            cmd.addAll(List.of("-Daeron.dir=" + dir, "-Daeron.dir.delete.on.start=true",
                    "-Daeron.dir.delete.on.shutdown=true", "-Daeron.term.buffer.sparse.file=true",
                    "-Daeron.client.liveness.timeout=" + DRIVER_CLIENT_LIVENESS_NS,
                    "-Daeron.publication.unblock.timeout=" + DRIVER_UNBLOCK_NS,
                    "-cp", System.getProperty("java.class.path"), "io.aeron.driver.MediaDriver"));
            pb = new ProcessBuilder(cmd);
        }
        final Process p = pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!Files.exists(dir.resolve("cnc.dat"))) {
            assertTrue("the external driver did not start: " + Files.readString(log),
                    System.nanoTime() < deadline && p.isAlive());
            Thread.sleep(50);
        }
        Thread.sleep(500); // a driver that fails its own validation creates the CnC and then exits
        assertTrue("the external driver exited after start: " + Files.readString(log), p.isAlive());
        final String kind = aeronmd != null ? "C aeronmd " + aeronmd : "Java MediaDriver child";
        System.out.println("[external-driver] external driver: " + kind + " at " + dir + " (pid " + p.pid() + ")");
        return new Driver(p, dir, log, kind);
    }

    /** {@code GET /ready} == 200 within the timeout, the child alive throughout. */
    private static void awaitReady(final ReplicaChild child, final int port, final long timeoutMs,
                                   final Runnable pause) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        String last = "";
        while (System.nanoTime() < deadline) {
            assertTrue(child.name + " exited before /ready" + child.tails(), child.process.isAlive());
            try {
                final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + port + "/ready")
                        .toURL().openConnection();
                c.setConnectTimeout(500);
                c.setReadTimeout(1_000);
                final int code = c.getResponseCode();
                final java.io.InputStream in = code >= 400 ? c.getErrorStream() : c.getInputStream();
                last = code + " " + (in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8).trim());
                c.disconnect();
                if (code == 200) {
                    return;
                }
            } catch (final IOException notYet) {
                last = String.valueOf(notYet);
            }
            pause.run();
        }
        throw new AssertionError("no /ready 200 from " + child.name + " within " + timeoutMs + " ms (last " + last + ")"
                + child.tails());
    }

    /**
     * The driver's counters a client holds (publications, senders, subscriptions, images, recordings), polled until
     * none is left or the deadline passes; returns what is still held. Read through a fresh client of the driver that
     * itself holds none.
     */
    private static List<String> awaitNoClientResources(final Driver driver, final long timeoutMs) throws Exception {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.dir.toString())
                .errorHandler(t -> System.out.println("[external-driver] test-JVM Aeron client: " + t)))) {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            List<String> held;
            do {
                held = new ArrayList<>();
                final CountersReader counters = aeron.countersReader();
                final List<String> found = held;
                counters.forEach((id, typeId, key, label) -> {
                    if (HELD_BY_A_CLIENT.contains(typeId)) {
                        found.add(id + ":" + typeId + ":" + label);
                    }
                });
                if (held.isEmpty()) {
                    return held;
                }
                Thread.sleep(100);
            } while (System.nanoTime() < deadline);
            return held;
        }
    }

    /** Recordings in the catalog without a stop position ({@code ArchiveTool.describe}, the archive is stopped). */
    private static List<String> openRecordings(final File archiveDir) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            ArchiveTool.describe(out, archiveDir);
        }
        final String text = bytes.toString(StandardCharsets.UTF_8);
        final List<String> open = new ArrayList<>();
        int recordings = 0;
        final Matcher m = STOP.matcher(text);
        while (m.find()) {
            recordings++;
            if (Long.parseLong(m.group(2)) < 0) {
                open.add("recordingId=" + m.group(1));
            }
        }
        assertTrue("the catalog lists the replica's recordings:\n" + text, recordings >= 2);
        System.out.println("[external-driver] catalog after the stop: " + recordings + " recordings, open " + open);
        return open;
    }

    /** The test JVM's clients of the external driver and the child's archive (its local IPC control channel). */
    private static final class Clients implements AutoCloseable {
        final Aeron aeron;
        final AeronArchive archive;

        Clients(final Driver driver) {
            // never the default handler: it exits this JVM on a driver timeout
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.dir.toString())
                    .errorHandler(t -> System.out.println("[external-driver] test-JVM Aeron client: " + t)));
            archive = AeronArchive.connect(new AeronArchive.Context().aeron(aeron).ownsAeronClient(false)
                    .controlRequestChannel(ReplicaContexts.LOCAL_CONTROL_CHANNEL)
                    .controlResponseChannel(ReplicaContexts.LOCAL_CONTROL_CHANNEL));
        }

        /**
         * A COMPLETE (trailer read) or VERIFIED copy of {@code kind} at {@code position}; a fresh catalog does no deep
         * read, so the caller loads the payload through the kit loader, which checks the digest.
         */
        LogicalShadow awaitShadow(final long position, final ShadowKind kind, final long timeoutMs,
                                    final ReplicaChild child, final int nodePort, final Runnable pause)
                throws Exception {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (true) {
                final ShadowCatalog.Scan scan = ReplicaRig.scanOf(aeron, archive, 0, ShadowClusterRig.GENESIS);
                for (final LogicalShadow v : scan.usable()) {
                    if (v.logPosition() == position && ReplicaRig.kindOf(v) == kind) {
                        return v;
                    }
                }
                assertTrue(child.name + " exited" + child.tails(), child.process.isAlive());
                if (System.nanoTime() > deadline) {
                    final StringBuilder sb = new StringBuilder();
                    for (final LogicalShadow v : scan.usable()) {
                        sb.append(' ').append(v.logPosition()).append(':').append(ReplicaRig.kindOf(v));
                    }
                    throw new AssertionError("no COMPLETE or VERIFIED " + kind + " shadow at " + position + " in the child's"
                            + " archive within " + timeoutMs + " ms; usable:" + sb + "; all:" + describe(scan)
                            + "\n--- /metrics:\n" + metrics(nodePort) + child.tails());
                }
                pause.run();
            }
        }

        @Override
        public void close() {
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
        }
    }

    private static String describe(final ShadowCatalog.Scan scan) {
        final StringBuilder sb = new StringBuilder();
        for (final com.openexchange.cluster.shadow.ShadowEntry e : scan.all()) {
            sb.append(" rec=").append(e.recordingId()).append(':').append(e.header() == null ? -1
                    : e.header().logPosition()).append(':').append(e.state());
        }
        return sb.toString();
    }

    /** The child's {@code /metrics} body (diagnostics in failure texts). */
    static String metrics(final int port) {
        try {
            final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + port + "/metrics").toURL()
                    .openConnection();
            c.setConnectTimeout(1_000);
            c.setReadTimeout(2_000);
            try (java.io.InputStream in = c.getInputStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (final IOException e) {
            return "(no /metrics: " + e + ")";
        }
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}

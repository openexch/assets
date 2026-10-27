// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.auth.AssetsAuth;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.cluster.ClusterPorts;
import io.aeron.Counter;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S9h 11.4 (cluster half): the T1 tools against a real AE cluster with the 7.1 key layout. In-JVM members (one and
 * three, {@code ClusterConfig} + the production {@code AssetsClusteredService}); {@code t1-core genesis --story},
 * {@code run --pump} (the {@link ReferencePump} feeding the story round by round behind the barriers), {@code await-bound},
 * {@code open}, {@code report}, {@code oracle} PASS; the oracle catches one wrong balance; without the BRIDGE key the pump
 * cannot register and the oracle fails. The process topology smoke (N-7) is {@link #processTopologySmoke}.
 */
public class FeedHarnessClusterTest {

    static final int ROUNDS = 8;

    /** One in-JVM AE member. */
    static final class Member {
        final int id;
        ClusterConfig cfg;
        ClusteredMediaDriver driver;
        ClusteredServiceContainer container;

        Member(final int id) {
            this.id = id;
        }

        Cluster.Role role() {
            final Counter c = cfg.consensusModuleContext().clusterNodeRoleCounter();
            return c == null ? Cluster.Role.FOLLOWER : Cluster.Role.get(c);
        }

        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(driver);
        }
    }

    private final List<Member> members = new ArrayList<>();
    private MediaDriver clientDriver;
    private File tmp;

    @After
    public void tearDown() {
        members.forEach(Member::close);
        CloseHelper.quietClose(clientDriver);
        if (tmp != null) {
            IoUtil.delete(tmp, true);
        }
        System.clearProperty(AssetsAuth.POLICY.keysProperty());
        FeedHarnessSeams.clear();
        STORY_DIRS.forEach(d -> IoUtil.delete(d.toFile(), true));
        STORY_DIRS.clear();
    }

    /** Boots {@code n} members on {@code portBase} with {@code keys}; returns the T1 tool environment. */
    Map<String, String> cluster(final int n, final int portBase, final String keys) throws Exception {
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        System.setProperty("assets.node.port", "0");
        System.setProperty(AssetsAuth.POLICY.keysProperty(), keys);
        tmp = new File(System.getProperty("java.io.tmpdir"), "s9h-t1-" + portBase);
        IoUtil.delete(tmp, true);
        final List<String> hosts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            hosts.add("localhost");
        }
        for (int i = 0; i < n; i++) {
            final Member m = new Member(i);
            m.cfg = ClusterConfig.create(i, hosts, hosts, portBase, new AssetsClusteredService());
            m.cfg.baseDir(new File(tmp, "node" + i));
            m.cfg.aeronDirectoryName(new File(tmp, "driver" + i).getAbsolutePath());
            m.cfg.idleStrategySupplier(BackoffIdleStrategy::new);
            m.cfg.errorHandler(Throwable::printStackTrace);
            m.cfg.consensusModuleContext().ingressChannel(T1Session.INGRESS_CHANNEL).logChannel("aeron:udp?term-length=1m")
                    .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                    .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1)).electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                    .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2)).terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
            m.driver = ClusteredMediaDriver.launch(
                    m.cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true).mtuLength(8192),
                    m.cfg.archiveContext(), m.cfg.consensusModuleContext());
            m.container = ClusteredServiceContainer.launch(m.cfg.clusteredServiceContext());
            members.add(m);
        }
        final long deadline = System.currentTimeMillis() + 30_000;
        boolean leader = false;
        while (!leader && System.currentTimeMillis() < deadline) {
            for (final Member m : members) {
                leader |= m.role() == Cluster.Role.LEADER;
            }
            Thread.sleep(50);
        }
        assertTrue("a leader within 30 s", leader);
        clientDriver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(new File(tmp, "client").getAbsolutePath())
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new BackoffIdleStrategy()));
        final StringBuilder ingress = new StringBuilder();
        for (int i = 0; i < n; i++) {
            ingress.append(i == 0 ? "" : ",").append(i).append("=localhost:")
                    .append(ClusterConfig.calculatePort(i, portBase, ClusterConfig.CLIENT_FACING_PORT_OFFSET));
        }
        return T1Keys.env(ingress.toString(), clientDriver.aeronDirectoryName());
    }

    /** {exit, output} of one in-process t1-core invocation. */
    record Out(int code, String text) { }

    static Out t1(final Map<String, String> env, final String... args) {
        final ByteArrayOutputStream b = new ByteArrayOutputStream();
        final PrintStream p = new PrintStream(b, true, StandardCharsets.UTF_8);
        int code;
        try {
            code = T1Core.run(args, env, p, p);
        } catch (FeedHarnessMain.Exit x) {
            p.println(x.getMessage());
            code = x.code;
        } catch (Exception e) {
            p.println("internal: " + e);
            code = FeedHarnessMain.INTERNAL;
        }
        final Out out = new Out(code, b.toString(StandardCharsets.UTF_8));
        System.out.println("[T1] " + String.join(" ", args) + " -> exit " + code + "\n" + out.text().trim());
        return out;
    }

    private static final List<Path> STORY_DIRS = new ArrayList<>();

    static Path story(final long seed) throws Exception {
        final Path d = Files.createTempDirectory("s9h-story-" + seed);
        STORY_DIRS.add(d);
        FeedStory.write(FeedStory.build(seed, ROUNDS), d);
        return d;
    }

    /** genesis, run --pump, await-bound, open, report, oracle: the T1 loop on the given cluster. */
    static void storyLoop(final Map<String, String> env, final Path dir) throws Exception {
        final FeedStory.Story s = FeedStory.read(dir);
        Out o = t1(env, "t1-core", "genesis", "--story", dir.toString());
        assertEquals(o.text(), 0, o.code());
        o = t1(env, "t1-core", "run", dir.toString(), "--pump", "--timeout", "30000");
        assertEquals("run: " + o.text(), 0, o.code());
        o = t1(env, "t1-core", "await-bound", "5000", "--genesis-id", Long.toString(s.genesis().genesisId()));
        assertEquals(o.text(), 0, o.code());
        o = t1(env, "t1-core", "open");
        assertEquals(o.text(), 0, o.code());
        assertTrue("the gate was already open (story round 1): " + o.text(), o.text().contains("result=DUPLICATE"));
        o = t1(env, "t1-core", "report");
        assertTrue(o.text(), o.text().contains("\"lastAppliedJournalSeq\":" + s.length()));
        o = t1(env, "t1-core", "oracle", dir.toString());
        assertEquals("oracle: " + o.text(), 0, o.code());
        assertTrue(o.text(), o.text().startsWith("PASS "));
    }

    @Test
    public void bootsOneMemberWithTheKeys() throws Exception {
        final Map<String, String> env = cluster(1, 21_400, T1Keys.table(true));
        storyLoop(env, story(3));
    }

    @Test
    public void bootsThreeMembersWithTheKeys() throws Exception {
        final Map<String, String> env = cluster(3, 21_600, T1Keys.table(true));
        storyLoop(env, story(4));
    }

    @Test
    public void oracleDetectsOneWrongBalance() throws Exception {
        final Map<String, String> env = cluster(1, 21_900, T1Keys.table(true));
        final Path dir = story(5);
        storyLoop(env, dir);
        Out o = t1(env, "t1-core", "deposit", "2", "0", "777");
        assertEquals(o.text(), 0, o.code());
        o = t1(env, "t1-core", "oracle", dir.toString());
        assertEquals("an extra deposit not in --extra: FAIL", 1, o.code());
        assertTrue("the FAIL names the user: " + o.text(), o.text().contains("FAIL") && o.text().contains("B 2 0 "));
        // the same deposit named in --extra: PASS again
        final Path extra = dir.resolve("extra.flog");
        Files.write(extra, FeedFiles.encodeFlog(List.of(new FeedFiles.LogEntry(FeedFiles.LogEntry.FRAME,
                com.openexchange.assets.infrastructure.persistence.CoreApplyRunner.TREASURY_SESSION, FeedStory.T0 + 999_999,
                new CoreFrames().deposit(0xE1L, 2, 0, 777, 0xE1L, 1)))));
        o = t1(env, "t1-core", "oracle", dir.toString(), "--extra", extra.toString());
        assertEquals(o.text(), 0, o.code());
        // a prefix the live core is past: FAIL; the operator verbs answer
        final long n = FeedStory.read(dir).length();
        assertEquals("--upto N-1 against a core at N", 1, t1(env, "t1-core", "oracle", dir.toString(), "--upto",
                Long.toString(n - 1)).code());
        o = t1(env, "t1-core", "latch-clear", "1", "0");
        assertTrue("no latch: DUPLICATE " + o.text(), o.code() == 0 && o.text().contains("ref=76 result=DUPLICATE"));
        o = t1(env, "t1-core", "withdraw", "2", "0", "777");
        assertTrue(o.text(), o.code() == 0 && o.text().contains("WITHDRAW template 16"));
        o = t1(env, "t1-core", "gate");
        assertTrue(o.text(), o.code() == 0 && o.text().contains("\"market\":0"));
        assertEquals("deposit then withdraw: PASS without --extra", 0, t1(env, "t1-core", "oracle", dir.toString()).code());
    }

    @Test
    public void withoutTheBridgeKeyThePumpCannotRegister() throws Exception {
        final Map<String, String> env = cluster(1, 22_200, T1Keys.table(false));
        final Path dir = story(6);
        assertEquals(0, t1(env, "t1-core", "genesis", "--story", dir.toString()).code());
        final Out run = t1(env, "t1-core", "run", dir.toString(), "--pump", "--timeout", "3000");
        assertTrue("the pump cannot connect or register without its key: " + run.text(), run.code() != 0);
        final Out oracle = t1(env, "t1-core", "oracle", dir.toString());
        assertEquals("oracle FAIL: " + oracle.text(), 1, oracle.code());
    }

    @Test
    public void aMissingKeyVariableExits78() throws Exception {
        final Map<String, String> env = new java.util.HashMap<>(T1Keys.env("0=localhost:1", "/nonexistent"));
        env.remove("T1_ADMIN_KEY");
        final Out o = t1(env, "t1-core", "report");
        assertEquals("H5: " + o.text(), 78, o.code());
        assertTrue(o.text(), o.text().contains("T1_ADMIN_KEY"));
    }

    // ------------------------------------------------------------------ N-7: the production process topology

    /**
     * 7.1 / N-7: three AE processes of the production main ({@code com.openexchange.assets.Main}; the shaded
     * {@code target/assets-cluster.jar} when {@code -Dt1.ae.jar} names it, else the same classes from this test's
     * classpath), env as {@code run-forwarder-it.sh}; {@code t1-journal} and {@code t1-core} as processes of the harness
     * main; the pump through {@code t1-core run --pump}; {@code oracle} PASS. Part of the default regression (it costs
     * about 3 s; surefire runs one test class at a time, so its JVMs never overlap another cluster test).
     */
    @Test
    public void processTopologySmoke() throws Exception {
        // in the default regression (review NOTE-12 / item 6: about 3 s here); -Dt1.process.smoke=false skips it
        org.junit.Assume.assumeTrue("skipped by -Dt1.process.smoke=false",
                !"false".equals(System.getProperty("t1.process.smoke")));
        final int base = 22_600;
        tmp = Files.createTempDirectory("s9h-t1-proc").toFile();
        final String cp = System.getProperty("java.class.path");
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final String aeJar = System.getProperty("t1.ae.jar");
        final List<Process> procs = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                final List<String> cmd = new ArrayList<>(List.of(java, "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
                        "--add-opens", "java.base/java.nio=ALL-UNNAMED", "-Xmx384m", "-Daeron.dir=" + new File(tmp, "ae" + i + "-driver")));
                if (aeJar != null) {
                    cmd.addAll(List.of("-jar", aeJar));
                } else {
                    cmd.addAll(List.of("-cp", cp, "com.openexchange.assets.Main"));
                }
                final ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true)
                        .redirectOutput(new File(tmp, "ae" + i + ".log"));
                final Map<String, String> e = pb.environment();
                e.put("CLUSTER_NODE", Integer.toString(i));
                e.put("CLUSTER_ADDRESSES", "localhost,localhost,localhost");
                e.put("CLUSTER_PORT_BASE", Integer.toString(base));
                e.put("CLUSTER_PORT_STRIDE", "100");
                e.put("BASE_DIR", new File(tmp, "ae" + i).getAbsolutePath());
                e.put("ASSETS_AUTH_KEYS", T1Keys.table(true));
                e.put("ASSETS_NODE_PORT", Integer.toString(base + 90 + i));
                e.put("TRANSPORT_IDLE_MODE", "backoff");
                e.put("TRANSPORT_DRIVER_THREADING", "shared");
                e.put("TRANSPORT_TERM_LENGTH", "4m");
                e.put("TRANSPORT_LOG_TERM_LENGTH", "16m");
                e.put("TRANSPORT_SNAPSHOT_TERM_LENGTH", "8m");
                procs.add(pb.start());
            }
            clientDriver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(new File(tmp, "client").getAbsolutePath())
                    .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED)
                    .sharedIdleStrategy(new BackoffIdleStrategy()));
            final String ingress = "0=localhost:" + (base + 2) + ",1=localhost:" + (base + 102) + ",2=localhost:" + (base + 202);
            final Map<String, String> env = T1Keys.env(ingress, clientDriver.aeronDirectoryName());
            final Path dir = story(7);
            // t1-journal as a process: two sources up, one write, QUIT
            final Process journal = harness(java, cp, env, List.of("t1-journal"));
            journal.getOutputStream().write(("SOURCES 2 " + (base + 500) + "\nLOAD " + dir.resolve("journal.fjr")
                    + "\nWRITE all 1 3\nLIST 0\nQUIT\n").getBytes(StandardCharsets.US_ASCII));
            journal.getOutputStream().close();
            assertTrue("t1-journal exits", journal.waitFor(60, TimeUnit.SECONDS));
            final String jout = new String(journal.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("[T1-PROC] t1-journal -> exit " + journal.exitValue() + "\n" + jout.trim());
            assertEquals(jout, 0, journal.exitValue());
            assertTrue(jout, jout.contains("OK wrote 6") && jout.contains("OK bye"));
            // t1-core as processes
            for (final List<String> step : List.of(List.of("t1-core", "genesis", "--story", dir.toString()),
                    List.of("t1-core", "run", dir.toString(), "--pump", "--timeout", "60000"),
                    List.of("t1-core", "oracle", dir.toString()))) {
                final Process p = harness(java, cp, env, step);
                assertTrue(step + " exits", p.waitFor(180, TimeUnit.SECONDS));
                final String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                System.out.println("[T1-PROC] " + String.join(" ", step) + " -> exit " + p.exitValue() + "\n" + out.trim());
                assertEquals(step + ": " + out, 0, p.exitValue());
                if (step.get(1).equals("oracle")) {
                    assertTrue(out, out.contains("PASS "));
                }
            }
            // H5: an env key variable removed
            final Map<String, String> missing = new java.util.HashMap<>(env);
            missing.remove("T1_ADMIN_KEY");
            final Process p = harness(java, cp, missing, List.of("t1-core", "report"));
            assertTrue(p.waitFor(60, TimeUnit.SECONDS));
            assertEquals("a missing key variable: exit 78", 78, p.exitValue());
        } finally {
            for (final Process p : procs) {
                p.destroyForcibly();
            }
            for (final Process p : procs) {
                p.waitFor(30, TimeUnit.SECONDS);
            }
        }
    }

    static Process harness(final String java, final String cp, final Map<String, String> env, final List<String> args)
            throws Exception {
        final List<String> cmd = new ArrayList<>(List.of(java, "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-Xmx384m", "-cp", cp, FeedHarnessMain.class.getName()));
        cmd.addAll(args);
        final ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        pb.environment().clear();
        pb.environment().put("PATH", System.getenv("PATH"));
        pb.environment().putAll(env);
        return pb.start();
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.loadgen.CoreOrderLoadGenerator;
import com.openexchange.core.loadgen.LatencyTracks;
import org.junit.Test;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The order load generator's in-JVM {@link CoreOrderLoadGenerator.Session} against real members (nightly,
 * {@code -Dcore.loadgen.smoke=true}). A smoke, not evidence of a rate: time values here are functional guards.
 *
 * <ul>
 *   <li>One member: 1 000 slots/s for 20 s after a ramp to 10 000 open orders over 10 000 prefunded users. Every
 *       command answered once ({@code ackFrames == creates + cancels + replayed}), no reject (no
 *       {@code TOO_MANY_PENDING_REQUESTS}), one per-second row per measured second, the commit position grew.</li>
 *   <li>Three members, the leader killed mid-run (right after a tick that offered frames; up to
 *       {@code core.loadgen.smoke.killAttempts} (3) kills, 12 s apart, the killed member restarted in between, until an
 *       election strands a command): the stranded commands are re-offered once with their requestIds and every
 *       command is answered. No strand in every attempt is INCONCLUSIVE, never a pass. Control
 *       {@code -Dcore.loadgen.nc=REOFFER_OFF}: the unanswered tail shows, {@code unacked tail after the election: <n>}.
 *   </li>
 * </ul>
 * Settings: {@code core.loadgen.smoke.rate} (1000), {@code core.loadgen.smoke.seconds} (20),
 * {@code core.loadgen.smoke.open} (10000), {@code core.loadgen.smoke.users} (10000).
 */
public class CoreOrderLoadGeneratorSmokeTest {

    private static final int PORT_BASE_ONE = 41_400;
    private static final int PORT_BASE_THREE = 41_800;

    private static void requireSmoke() {
        assumeTrue("nightly smoke: -Dcore.loadgen.smoke=true", Boolean.getBoolean("core.loadgen.smoke"));
    }

    /** ADOPT and the gate OPEN through an ADMIN session (what a harness does before the generator runs). */
    private static void adoptAndOpen(final ShadowClusterRig rig, final Member via) throws InterruptedException {
        final Client admin = rig.connect(SessionClass.ADMIN, via);
        rig.adopt(admin);
        final Encoders enc = new Encoders();
        admin.offer(enc.buf, enc.gateOpen(0xAD1L));
        rig.await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 60_000,
                "the ADMIN gate open must be ACCEPTED");
    }

    private static CoreOrderLoadGenerator.Session session(final ShadowClusterRig rig, final Member via,
                                                          final CoreOrderLoadGenerator.Settings s)
            throws InterruptedException {
        final CoreOrderLoadGenerator.Session[] holder = new CoreOrderLoadGenerator.Session[1];
        final io.aeron.cluster.client.EgressListener relay = new io.aeron.cluster.client.EgressListener() {
            @Override
            public void onMessage(final long sid, final long ts, final org.agrona.DirectBuffer b, final int o,
                                  final int l, final io.aeron.logbuffer.Header h) {
                holder[0].onMessage(sid, ts, b, o, l, h);
            }

            @Override
            public void onNewLeader(final long sid, final long term, final int leader, final String ingress) {
                holder[0].onNewLeader(sid, term, leader, ingress);
            }
        };
        final Client gw = rig.connect(SessionClass.GATEWAY, via, relay);
        final Client tool = rig.connect(SessionClass.TOOL, via, relay);
        holder[0] = new CoreOrderLoadGenerator.Session(s, gw.session.client::offer, tool.session.client::offer,
                System.out);
        return holder[0];
    }

    @Test
    public void aThousandSlotsPerSecondOnOneMember() throws Exception {
        requireSmoke();
        final CoreOrderLoadGenerator.Settings s = new CoreOrderLoadGenerator.Settings();
        s.rate = Long.getLong("core.loadgen.smoke.rate", 1_000);
        s.durationSec = Integer.getInteger("core.loadgen.smoke.seconds", 20);
        s.warmupSec = 2;
        s.openOrders = Integer.getInteger("core.loadgen.smoke.open", 10_000);
        s.users = Integer.getInteger("core.loadgen.smoke.users", 10_000);
        s.prefund = true;
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE_ONE, "loadgen-smoke", 1, "1m", 0)) {
            rig.bootAll();
            final Member m = rig.awaitLeader(null);
            adoptAndOpen(rig, m);
            final CoreOrderLoadGenerator.Session g = session(rig, m, s);
            final long commitBefore = m.cfg.consensusModuleContext().commitPositionCounter().get();
            final long t0 = System.nanoTime();
            rig.await(() -> {
                g.tick(System.nanoTime());
                return g.done();
            }, TimeUnit.MINUTES.toMillis(10), () -> "the generator did not finish: phase " + g.phase() + ", "
                    + g.result());
            final long commitAfter = m.cfg.consensusModuleContext().commitPositionCounter().get();
            final CoreOrderLoadGenerator.Result r = g.result();
            System.out.println("[loadgen-smoke] " + r + " wall " + (System.nanoTime() - t0) / 1_000_000 + " ms, commit "
                    + commitBefore + " -> " + commitAfter);
            final Path out = Path.of(System.getProperty("java.io.tmpdir"), "loadgen-smoke-seconds.tsv");
            try (PrintStream ps = new PrintStream(Files.newOutputStream(out))) {
                g.tracks().writeSecondsTsv(ps);
            }
            System.out.println("[loadgen-smoke] per-second rows in " + out);

            assertEquals("no reject (by reason " + r.rejectsByReason() + ")", 0, r.rejects());
            assertEquals("unanswered", 0, r.unanswered());
            assertEquals("every command answered once: ack frames == creates + cancels + replayed",
                    r.creates() + r.cancels() + r.replayed(), r.ackFrames());
            assertEquals("the steady slots: one create and one cancel each", s.openOrders + r.cancels(), r.creates());
            assertTrue("the steady phase ran: cancels " + r.cancels(), r.cancels() >= s.rate * (s.durationSec - 1));
            final Set<Long> seconds = new TreeSet<>();
            for (final LatencyTracks.SecondRow row : g.tracks().secondRows()) {
                if ("create-ack".equals(row.track())) {
                    seconds.add(row.second());
                }
            }
            assertTrue("one per-second row per measured second: " + seconds, seconds.size() >= s.durationSec
                    - s.warmupSec - 1);
            assertTrue("the commit position grew: " + commitBefore + " -> " + commitAfter, commitAfter > commitBefore);
        }
    }

    @Test
    public void theUnansweredTailAfterALeaderKillIsReofferedOnce() throws Exception {
        requireSmoke();
        final boolean control = "REOFFER_OFF".equals(System.getProperty("core.loadgen.nc", ""));
        final int attempts = Integer.getInteger("core.loadgen.smoke.killAttempts", 3);
        final CoreOrderLoadGenerator.Settings s = new CoreOrderLoadGenerator.Settings();
        s.rate = 1_000; // a few commands in flight at any moment
        s.durationSec = 12 + 12 * attempts;
        s.warmupSec = 2;
        s.openOrders = 2_000;
        s.prefund = true;
        s.users = (int)s.minUsers();
        s.reoffer = !control;
        // the clients ride their own media driver, so any leader can be killed
        final java.io.File clientDir = new java.io.File(System.getProperty("java.io.tmpdir"), "loadgen-election-clients");
        try (io.aeron.driver.MediaDriver clientDriver = io.aeron.driver.MediaDriver.launch(
                new io.aeron.driver.MediaDriver.Context().aeronDirectoryName(new java.io.File(clientDir, "driver9")
                        .getAbsolutePath()).threadingMode(io.aeron.driver.ThreadingMode.SHARED).dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true));
             ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE_THREE, "loadgen-election", 3, "1m", 0)) {
            rig.bootAll();
            rig.awaitLeader(null);
            final Member host = new Member(9, clientDir);
            adoptAndOpen(rig, host);
            final CoreOrderLoadGenerator.Session g = session(rig, host, s);
            final long[] steadyAt = {0};
            final long[] lastKillAt = {0};
            final int[] kills = {0};
            final Member[] down = {null};
            rig.await(() -> {
                final long now = System.nanoTime();
                final int offered = g.tick(now);
                if ("STEADY".equals(g.phase()) && steadyAt[0] == 0) {
                    steadyAt[0] = now;
                }
                if (steadyAt[0] == 0 || g.steadyOver(now)) {
                    return g.done();
                }
                // a killed member comes back 6 s after its kill, so the next attempt has three members again
                if (down[0] != null && now - lastKillAt[0] > TimeUnit.SECONDS.toNanos(6)) {
                    rig.restart(down[0]);
                    down[0] = null;
                }
                // a kill attempt every 12 s until an election strands a command: the kill follows a tick that
                // offered frames, before any egress is polled, so those commands are in flight on the old leader
                if (down[0] == null && kills[0] < attempts && g.result().unansweredAtElections() == 0
                        && offered > 0 && now - steadyAt[0] > TimeUnit.SECONDS.toNanos(8 + 12L * kills[0])) {
                    for (final Member m : rig.live()) {
                        if (m.role() == io.aeron.cluster.service.Cluster.Role.LEADER) {
                            kills[0]++;
                            lastKillAt[0] = now;
                            System.out.println("[loadgen-election] attempt " + kills[0] + ": killing leader " + m.id
                                    + " with " + g.inFlight() + " commands in flight");
                            rig.kill(m);
                            down[0] = m;
                            break;
                        }
                    }
                }
                return g.done();
            }, TimeUnit.MINUTES.toMillis(10), () -> "the generator did not finish: phase " + g.phase() + ", "
                    + g.result());
            final CoreOrderLoadGenerator.Result r = g.result();
            System.out.println("[loadgen-election] " + r + "; kills " + kills[0] + "; roles " + rig.roles());
            assertTrue("no election observed by the generator", r.elections() > 0);
            assumeTrue("INCONCLUSIVE: no leader kill stranded a command in " + kills[0] + " attempts",
                    r.unansweredAtElections() > 0);
            if (r.unanswered() > 0) {
                throw new AssertionError("unacked tail after the election: " + r.unanswered());
            }
            assertTrue("re-offered " + r.reoffered(), r.reoffered() > 0);
            assertEquals("every command answered (rejects by reason " + r.rejectsByReason() + ")", r.issued(),
                    r.acks() + r.rejects());
        }
    }
}

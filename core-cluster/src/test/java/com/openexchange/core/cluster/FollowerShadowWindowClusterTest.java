// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The Min flow control coupling, as a functional guard (three members, a 4m log term, the window and the socket
 * buffer on the log channel URI, nightly). While the designated follower serializes (a 2 s slow-serialize seam on its
 * service thread) its log subscribers stop and the leader's publication is paced by the slowest receiver. Steady
 * ingress runs throughout; the leader's commit position is sampled and the longest gap between advances recorded.
 *
 * <ul>
 *   <li>GREEN: {@code rcv-wnd=2m}: the leader keeps committing during the serialize (longest gap below 500 ms),
 *       nothing deferred, the F copy written.</li>
 *   <li>Coupling (must stall): {@code rcv-wnd=256k} with the window leg off: the longest gap is at least 1 s, else
 *       {@code the leader did not stall behind a serializing follower at rcv-wnd=256k}.</li>
 *   <li>Guard: {@code rcv-wnd=256k}, the window leg on, {@code SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS=2000}:
 *       {@code shadow_producer_deferred_window_total 1}, no stall, no F copy. Control
 *       {@code -Dcore.producer.nc=WINDOW_GUARD_OFF} must fail with {@code the guard produced over budget}.</li>
 * </ul>
 *
 * <p>Precondition: {@code net.core.rmem_max >= 2 MiB} on this host (the URI's {@code so-rcvbuf}); else skipped loudly.</p>
 */
public class FollowerShadowWindowClusterTest {

    private static final int PORT_BASE = 63400;
    private static final long SERIALIZE_MS = 2_000;
    /** Deposits per 10 ms tick: the steady ingress (the measured rate is printed). */
    private static final int PER_TICK = Integer.getInteger("core.window.perTick", 28);

    private record Outcome(long longestGapMs, double rateBytesPerSecond, long deferred, boolean written, long p) {
    }

    private static long rmemMax() throws Exception {
        final Path p = Path.of("/proc/sys/net/core/rmem_max");
        if (!Files.isReadable(p)) {
            return -1;
        }
        try (java.io.InputStream in = Files.newInputStream(p)) {
            return Long.parseLong(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.US_ASCII).trim());
        }
    }

    @Test
    public void theWindowDecidesWhetherASerializingFollowerStallsTheLeader() throws Exception {
        final long rmem = rmemMax();
        System.out.println("[window] net.core.rmem_max=" + rmem);
        assumeTrue("needs net.core.rmem_max >= 2 MiB (this host: " + rmem + ")", rmem >= 2L * 1024 * 1024);
        final boolean guardOffControl = "WINDOW_GUARD_OFF".equals(System.getProperty("core.producer.nc", ""));

        final Outcome green = run(0, "2m", false, "1000");
        System.out.println("[window] rcv-wnd=2m: " + green);
        assertTrue("the leader's commit advances at least every 500 ms at rcv-wnd=2m: longest gap "
                + green.longestGapMs() + " ms", green.longestGapMs() < 500);
        assertEquals("deferred_window_total 0", 0, green.deferred());
        assertTrue("the F copy at rcv-wnd=2m", green.written());

        final Outcome coupled = run(100, "256k", true, "1000");
        System.out.println("[window] rcv-wnd=256k, window leg off: " + coupled);
        if (coupled.longestGapMs() < 1_000) {
            throw new AssertionError("the leader did not stall behind a serializing follower at rcv-wnd=256k: longest"
                    + " gap " + coupled.longestGapMs() + " ms at " + Math.round(coupled.rateBytesPerSecond()) + " B/s");
        }

        final Outcome guarded = run(200, "256k", guardOffControl, "2000");
        System.out.println("[window] rcv-wnd=256k, window leg on, estimate 2000 ms: " + guarded);
        if (guarded.written()) {
            throw new AssertionError("the guard produced over budget: rcv-wnd=256k rate="
                    + Math.round(guarded.rateBytesPerSecond()) + " B/s estimate 2000 ms");
        }
        assertEquals("deferred_window_total 1", 1, guarded.deferred());
        assertTrue("no stall when deferred: longest gap " + guarded.longestGapMs() + " ms", guarded.longestGapMs() < 500);
        System.out.println("GREEN window: 2m gap " + green.longestGapMs() + " ms; 256k coupled gap "
                + coupled.longestGapMs() + " ms; 256k guarded gap " + guarded.longestGapMs() + " ms, deferred 1");
    }

    private Outcome run(final int portOffset, final String window, final boolean windowLegOff, final String estimateMs)
            throws Exception {
        final String channelTail = "4m|rcv-wnd=" + window + "|so-rcvbuf=" + window;
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + portOffset, "producer-window-" + window
                + (windowLegOff ? "-off" : ""), 3, channelTail, 0)) {
            for (final Member m : rig.members) {
                m.settings = ShadowProducerRoleTest.producerSettings("SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS",
                        estimateMs);
                m.beforeLaunch = svc -> {
                    ServiceProbe.producerFault(svc, "producerSerializeDelayMs", SERIALIZE_MS);
                    if (windowLegOff) {
                        ServiceProbe.producerFault(svc, "windowGuardOff", true);
                    }
                };
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member d = rig.members.get(ShadowProducerRoleTest.designated(rig, leader.id));
            final Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, leader);
            tool.deposits(100);
            ShadowProducerRoleTest.awaitArmed(rig);
            assertEquals("the producer's W is the URI's window", org.agrona.SystemUtil.parseSize("w", window),
                    ServiceProbe.producerWindow(d.service));
            final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
            final io.aeron.Counter commitCounter = leader.cfg.consensusModuleContext().commitPositionCounter();
            // steady ingress for 12 s (the guard's rate horizon is 10 s), then the snapshot under the same ingress
            final long warmEnd = System.currentTimeMillis() + 12_000;
            long c0 = commitCounter.get();
            long t0 = System.nanoTime();
            while (System.currentTimeMillis() < warmEnd) {
                tick(rig, tool, enc);
            }
            final double rate = (commitCounter.get() - c0) * 1e9 / (System.nanoTime() - t0);
            final int before = rig.snapshots(d).size();
            final AtomicCounter toggle = ClusterControl.findControlToggle(
                    leader.cfg.consensusModuleContext().aeron().countersReader(),
                    leader.cfg.consensusModuleContext().clusterId());
            assertTrue("the SNAPSHOT toggle", ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
            long last = commitCounter.get();
            long lastChangeNs = System.nanoTime();
            long longestNs = 0;
            long doneAt = -1;
            final long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                tick(rig, tool, enc);
                final long now = System.nanoTime();
                final long c = commitCounter.get();
                if (c != last) {
                    longestNs = Math.max(longestNs, now - lastChangeNs);
                    last = c;
                    lastChangeNs = now;
                }
                if (doneAt < 0 && rig.snapshots(d).size() > before) {
                    doneAt = System.currentTimeMillis();
                }
                if (doneAt > 0 && System.currentTimeMillis() - doneAt > 3_000) {
                    break;
                }
            }
            assertTrue("the snapshot completed on the designated", doneAt > 0);
            longestNs = Math.max(longestNs, System.nanoTime() - lastChangeNs);
            final java.util.List<long[]> snaps = rig.snapshots(d);
            final long p = snaps.get(snaps.size() - 1)[0];
            rig.pumpMs(2_000);
            final boolean written = ShadowProducerRoleTest.written(rig, d, p) != null;
            return new Outcome(longestNs / 1_000_000, rate, ServiceProbe.producerDeferredWindow(d.service), written, p);
        }
    }

    /** One 10 ms tick of steady ingress: {@link #PER_TICK} deposits without waiting for acks, then the clients polled. */
    private static void tick(final ShadowClusterRig rig, final Client tool, final CoreRestoreRig.Encoders enc)
            throws InterruptedException {
        final long tickEnd = System.nanoTime() + 10_000_000L;
        for (int i = 0; i < PER_TICK; i++) {
            tool.offer(enc.buf, enc.deposit(tool.corr++, 1 + (i % 5), com.openexchange.core.domain.Asset.USD.id(),
                    ShadowClusterRig.ONE));
        }
        while (System.nanoTime() < tickEnd) {
            for (final CoreRestoreRig.Session s : rig.sessions()) {
                s.poll();
            }
            Thread.onSpinWait();
        }
    }
}

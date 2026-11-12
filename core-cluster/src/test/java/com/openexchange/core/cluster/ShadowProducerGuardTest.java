// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The guard legs of the follower producer on a real two-member cluster in {@code shadow} mode: every false leg is a
 * counted skip with its reason, never a fault, and production resumes at the next P_cm.
 *
 * <ul>
 *   <li>(a) nightly, three members: the designated follower restarted into a SKIP restore (its newest snapshot group
 *       invalidated, a shadow at P_s above it) replays a same-term ClusterAction at P_x in (P_cm, P_s): the snapshot
 *       there is a stub inside the skip window, {@code skip_window} is the counted skip and no F copy exists at P_x.
 *       Control {@code ALIGNED_GUARD_OFF} (the skip-window leg off) must fail with {@code the skip-window leg did not
 *       decide the action inside the skip window}: the replayed action runs inside the election, so the next legs
 *       ({@code not_ready}, {@code leader_unknown}) still refuse it; the skip-window leg is the first and the one
 *       that names it.</li>
 *   <li>(c) PR: {@code SHADOW_PRODUCER_BUFFER_MB=1} with an image above 1 MiB: {@code capacity}, the stub snapshot
 *       intact, the callback did not fail, the member alive and ready. Control {@code CAPACITY_GUARD_OFF} (the catch
 *       removed) must fail with {@code the producer fenced or threw on an oversize image}.</li>
 *   <li>(e) PR: {@code SHADOW_PRODUCER=none}: no writer, no copy, {@code producer_none} counted.</li>
 *   <li>(b) nightly: readiness withdrawn right before the action: {@code not_ready}, no copy; control
 *       {@code READY_GUARD_OFF}.</li>
 *   <li>(d) nightly: a writer held busy across two snapshots: the second is {@code busy}, the first completes.</li>
 *   <li>(f) nightly: a serialize estimate above half the client liveness timeout the client context reports:
 *       {@code liveness_bound}; control {@code LIVENESS_BOUND_OFF}.</li>
 * </ul>
 *
 * <p>{@code -Dcore.producer.guard.nightly=true} adds (a), (b), (d), (f); {@code -Dcore.producer.nc=<seam>} runs a control.</p>
 */
public class ShadowProducerGuardTest {

    private static final int PORT_BASE = 62000;

    private static boolean nightly() {
        return Boolean.getBoolean("core.producer.guard.nightly");
    }

    private static String control() {
        return System.getProperty("core.producer.nc", "");
    }

    /** A two-member rig in shadow mode with the producer, extra settings on every member, adopted, with deposits. */
    private record Story(ShadowClusterRig rig, Member leader, Member designated, Client tool) {
    }

    private static Story start(final ShadowClusterRig rig, final java.util.function.Consumer<Member> beforeBoot,
                               final String... extra) throws Exception {
        for (final Member m : rig.members) {
            m.settings = ShadowProducerRoleTest.producerSettings(extra);
            if (beforeBoot != null) {
                beforeBoot.accept(m);
            }
        }
        rig.bootAll();
        final Member leader = rig.awaitLeader(null);
        final Client admin = rig.connect(SessionClass.ADMIN, leader);
        rig.adopt(admin);
        final Client tool = rig.connect(SessionClass.TOOL, leader);
        tool.deposits(100);
        return new Story(rig, leader, rig.members.get(ShadowProducerRoleTest.designated(rig, leader.id)), tool);
    }

    /** One snapshot; a member whose callback threw never records it, which the oracle below names. */
    private static long snapshot(final Story s) {
        try {
            return s.rig().snapshotAll();
        } catch (final AssertionError notTaken) {
            if (ServiceProbe.applicationCallbackFailed(s.designated().service) || !s.designated().halts.isEmpty()) {
                throw new AssertionError("the producer fenced or threw on an oversize image: applicationCallbackFailed="
                        + ServiceProbe.applicationCallbackFailed(s.designated().service) + " halts "
                        + s.designated().halts);
            }
            throw notTaken;
        }
    }

    @Test
    public void anImageAboveTheBufferIsACountedSkipAndTheMemberLives() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "producer-guard-capacity", 2, "1m", 0)) {
            final Story s = start(rig, m -> {
                if ("CAPACITY_GUARD_OFF".equals(control())) {
                    m.beforeLaunch = svc -> ServiceProbe.producerFault(svc, "capacityGuardOff", true);
                }
            // a 1 ms estimate: the account burst just before the snapshot must not defer it on the window budget
            }, "SHADOW_PRODUCER_BUFFER_MB", "1", "SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS", "1");
            ShadowProducerRoleTest.accounts(rig, s.tool(), 20_000);
            ShadowProducerRoleTest.awaitArmed(rig);
            final int before = rig.snapshots(s.designated()).size();
            final long p = snapshot(s);
            final Member d = s.designated();
            rig.await(() -> ServiceProbe.producerSkipped(d.service, "capacity") == 1, 30_000,
                    () -> "skipped_total{capacity} == 1 on member " + d.id + ": "
                            + ServiceProbe.producerSkipped(d.service, "capacity"));
            assertEquals("the stub snapshot is intact on member " + d.id, before + 1, rig.snapshots(d).size());
            assertTrue(rig.tee.all("kind=stub shadow=skipped:capacity").size() >= 1);
            assertFalse("applicationCallbackFailed", ServiceProbe.applicationCallbackFailed(d.service));
            assertTrue("the member is alive: " + d.halts, d.halts.isEmpty());
            rig.awaitReady(d, 30_000);
            assertEquals("no copy", 0, ServiceProbe.shadowWriteTotal(d.service, ShadowKind.ALIGNED));
            System.out.println("GREEN capacity: member " + d.id + " skipped capacity at " + p
                    + ", stub intact, ready, alive");
        }
    }

    @Test
    public void producerNoneWritesNothing() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + 100, "producer-guard-none", 2, "1m", 0)) {
            final Story s = start(rig, null, "SHADOW_PRODUCER", "none");
            final long p = s.rig().snapshotAll();
            final Member d = s.designated();
            rig.await(() -> ServiceProbe.producerSkipped(d.service, "producer_none") >= 1, 30_000,
                    "skipped_total{producer_none} 1");
            rig.pumpMs(6_000); // a housekeeping wake: still nothing armed
            assertFalse("no writer armed", ServiceProbe.producerArmed(d.service));
            assertNull("no copy at " + p, ShadowProducerRoleTest.written(rig, d, p));
            assertTrue(d.settings.criticalLines().contains(
                    com.openexchange.cluster.shadow.ShadowProducerSettings.NONE_LINE));
            System.out.println("GREEN none: member " + d.id + " skipped producer_none at " + p);
        }
    }

    @Test
    public void anActionReplayedInsideASkipWindowIsACountedSkip() throws Exception {
        assumeTrue("nightly rows (-Dcore.producer.guard.nightly=true)", nightly());
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + 700, "producer-guard-skip-window", 3, "1m", 0)) {
            // nobody produces before the restart: the restored node must find no copy of its own at P_cm (that is EQUAL)
            for (final Member m : rig.members) {
                m.settings = ShadowProducerRoleTest.producerSettings("SHADOW_PRODUCER", "none");
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.members.get(ShadowProducerRoleTest.designated(rig, leader.id));
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(200);
            final long pCm = rig.snapshotAll();
            tool.deposits(40);
            final long pX = rig.snapshotAll();
            tool.deposits(40);
            rig.awaitImagesEqual(f, leader, 60_000);
            final com.openexchange.cluster.testkit.RecordedLog log = rig.captureCommitted(f);
            final long[] pos = log.positions();
            final int ix = Arrays.binarySearch(pos, pX);
            assertTrue("P_x " + pX + " is a frame end of member " + f.id + "'s log", ix >= 0 && ix + 8 < pos.length);
            final long pS = pos[ix + 8];
            final com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written w = rig.producer(f)
                    .produce(new com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who(
                            com.openexchange.cluster.shadow.ProducerKind.FOLLOWER, 300), log, pS);
            assertTrue(w.toString(), w.ok());
            rig.kill(f);
            try (io.aeron.cluster.RecordingLog recordingLog = new io.aeron.cluster.RecordingLog(f.clusterDir(), true)) {
                assertTrue("invalidate the P_x group", recordingLog.invalidateLatestSnapshot());
            }
            tool.deposits(20);
            // the designated follower comes back as a producer
            f.settings = ShadowProducerRoleTest.producerSettings();
            if ("ALIGNED_GUARD_OFF".equals(control())) {
                f.beforeLaunch = svc -> ServiceProbe.producerFault(svc, "alignedGuardOff", true);
            }
            rig.tee.clear();
            rig.restart(f);
            rig.awaitReady(f, 120_000);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertTrue(f.halts.toString(), f.halts.isEmpty());
            RestoreDrillDryRun.requirePathLine(rig.tee, "SKIP P_s=" + pS, "producer skip window");
            String atPx = null;
            for (final String l : rig.tee.all("[SNAPSHOT] accounts=")) {
                if (l.startsWith("[SNAPSHOT] accounts=") && l.contains("logPosition=" + pX + " ")) {
                    atPx = l;
                }
            }
            assertTrue("a [SNAPSHOT] line at P_x=" + pX + ": " + rig.tee.lines, atPx != null);
            assertTrue("the snapshot at P_x is a stub inside the skip window: " + atPx,
                    atPx.contains("kind=stub inSkipWindow=true"));
            if (ShadowProducerRoleTest.written(rig, f, pX) != null) {
                throw new AssertionError("an ALIGNED was produced inside a skip window: P_x=" + pX + " in (" + pCm
                        + ", " + pS + ")");
            }
            if (!atPx.contains("shadow=skipped:skip_window")
                    || ServiceProbe.producerSkipped(f.service, "skip_window") != 1) {
                throw new AssertionError("the skip-window leg did not decide the action inside the skip window: P_x="
                        + pX + " in (" + pCm + ", " + pS + "), skipped_total{skip_window}="
                        + ServiceProbe.producerSkipped(f.service, "skip_window") + ": " + atPx);
            }
            System.out.println("GREEN skip_window: member " + f.id + " P_x=" + pX + " in (" + pCm + ", " + pS
                    + "): " + atPx);
        }
    }

    @Test
    public void aWithdrawnReadinessIsACountedSkipAndTheNextActionProduces() throws Exception {
        assumeTrue("nightly rows (-Dcore.producer.guard.nightly=true)", nightly());
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + 200, "producer-guard-nightly", 2, "1m", 0)) {
            final Story s = start(rig, m -> {
                if ("READY_GUARD_OFF".equals(control())) {
                    m.beforeLaunch = svc -> ServiceProbe.producerFault(svc, "readyGuardOff", true);
                }
            });
            final Member d = s.designated();
            ShadowProducerRoleTest.awaitArmed(rig);

            // (b) readiness withdrawn right before the action
            ServiceProbe.producerFault(d.service, "notReadyInjected", true);
            final long pb = s.rig().snapshotAll();
            rig.await(() -> ServiceProbe.producerSkipped(d.service, "not_ready") >= 1
                    || ServiceProbe.producerWritten(d.service) >= 1, 30_000, "skipped_total{not_ready} 1");
            rig.pumpMs(1_000);
            if (ShadowProducerRoleTest.written(rig, d, pb) != null) {
                throw new AssertionError("an ALIGNED was produced while not ready");
            }
            ServiceProbe.producerFault(d.service, "notReadyInjected", false);
            System.out.println("GREEN not_ready at " + pb);

            // recovery: the next P_cm is produced
            s.tool().deposits(20);
            final long pr = s.rig().snapshotAll();
            rig.await(() -> ShadowProducerRoleTest.written(rig, d, pr) != null, 60_000, "no ALIGNED after not_ready");

        }
    }

    @Test
    public void aSerializeEstimateAboveHalfTheClientLivenessIsACountedSkip() throws Exception {
        assumeTrue("nightly rows (-Dcore.producer.guard.nightly=true)", nightly());
        // the rig's driver context: ClusterConfig's client liveness timeout (10 s); the estimate is 60 % of it
        final long estimateMs = 6_000;
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + 400, "producer-guard-liveness", 2, "1m", 0)) {
            final Story s = start(rig, m -> {
                if ("LIVENESS_BOUND_OFF".equals(control())) {
                    m.beforeLaunch = svc -> ServiceProbe.producerFault(svc, "livenessBoundOff", true);
                }
            }, "SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS", Long.toString(estimateMs));
            final Member d = s.designated();
            final long livenessNs = d.cfg.mediaDriverContext().clientLivenessTimeoutNs();
            assertTrue("the estimate is above half the liveness timeout " + livenessNs,
                    TimeUnit.MILLISECONDS.toNanos(estimateMs) > livenessNs / 2);
            ShadowProducerRoleTest.awaitArmed(rig);
            final long p = s.rig().snapshotAll();
            rig.await(() -> ServiceProbe.producerSkipped(d.service, "liveness_bound") >= 1
                    || ServiceProbe.producerWritten(d.service) >= 1, 30_000, "skipped_total{liveness_bound} 1");
            rig.pumpMs(1_000);
            if (ShadowProducerRoleTest.written(rig, d, p) != null) {
                throw new AssertionError("the guard produced above the liveness bound: T_last=" + estimateMs
                        + " ms, liveness " + TimeUnit.NANOSECONDS.toMillis(livenessNs) + " ms");
            }
            assertEquals("skipped_total{liveness_bound} 1", 1, ServiceProbe.producerSkipped(d.service,
                    "liveness_bound"));
            System.out.println("GREEN liveness_bound at " + p + " (client liveness "
                    + TimeUnit.NANOSECONDS.toMillis(livenessNs) + " ms read from the driver context)");
        }
    }

    @Test
    public void aBusyWriterSkipsTheNextActionAndTheFirstWriteCompletes() throws Exception {
        assumeTrue("nightly rows (-Dcore.producer.guard.nightly=true)", nightly());
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + 300, "producer-guard-busy", 2, "1m", 0)) {
            final Story s = start(rig, m -> m.beforeLaunch = svc -> ServiceProbe.slowWriter(svc, 6_000));
            final Member d = s.designated();
            ShadowProducerRoleTest.awaitArmed(rig);
            final long p1 = s.rig().snapshotAll();
            s.tool().deposits(10);
            final long p2 = s.rig().snapshotAll();
            rig.await(() -> ServiceProbe.producerSkipped(d.service, "busy") >= 1, 30_000, "skipped_total{busy} 1");
            rig.await(() -> ShadowProducerRoleTest.written(rig, d, p1) != null, 60_000, "the first write never completed");
            assertNull("no copy at the busy position " + p2, ShadowProducerRoleTest.written(rig, d, p2));
            System.out.println("GREEN busy: " + p1 + " written, " + p2 + " skipped busy");
        }
    }
}

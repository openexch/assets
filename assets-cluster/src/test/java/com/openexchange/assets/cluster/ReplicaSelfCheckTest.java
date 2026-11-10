// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.replica.ReplicaTestBindings;
import com.openexchange.cluster.NodeReadiness;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaEngine;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The writer's self-check on the replica plus the time-lag readiness row.
 *
 * <ul>
 *   <li>A payload mutated between serialize and offer ({@code Faults.mutateImage}): {@code SELF_CHECK_FAILED}, nothing
 *       recorded at that position, the replica fences.</li>
 *   <li>A scratch self-check that writes into the frozen buffer: {@code SELF_CHECK_MUTATED_BUFFER}, the replica
 *       fences.</li>
 *   <li>Time lag: an applier that falls behind (a slow apply seam) while the cluster moves reads
 *       {@code time-lag} (not ready) within {@code REPLICA_READY_TIME_LAG_MS} plus one burst, and ready again once
 *       caught up.</li>
 * </ul>
 *
 * <p>The frozen-supplier row and its {@code TIME_LAG_IDLE_ALLOWED} control.</p>
 *
 * <p>Control: {@code UNCHECKED_TEST_ONLY} (with {@code -Doxs.shadow.unchecked=true}) -> the mutated image is written:
 * {@code poisoned shadow written at <p>}.</p>
 */
public class ReplicaSelfCheckTest {

    private static final int PORT_BASE = 17200;

    @Test(timeout = 900_000)
    public void aNonRoundTrippableImageIsNeverOffered() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-self-check")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);

            // mutated between serialize and offer
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica-mutate").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            r.faults.mutateNextImage = true;
            final long p = r.rig.snapshotAll();
            r.rig.await(() -> !r.fatals.isEmpty(), 30_000, "a mutated image did not fence the replica");
            assertEquals("the mutation hit the ALIGNED at P_cm", p, r.faults.mutatedAt);
            assertTrue("SELF_CHECK_FAILED fences: " + r.fatals, r.fatals.get(0).contains("SELF_CHECK_FAILED at " + p));
            assertNotWritten(r, p);
            assertEquals("shadow_replica_fenced", 1, r.metric("shadow_replica_fenced"));
            System.out.println("[self-check] mutated image: " + r.fatals.get(0) + "; shadows " + r.allShadowPositions());

            // a scratch check that writes into the frozen buffer
            r.stopReplica();
            r.windowGap();
            final ReplicaEngine scribbler = ReplicaTestBindings.withSelfCheck(
                    ReplicaTestBindings.assets(() -> r.fatals.add("CoreConfig")), real -> (payload, length) -> {
                        final boolean verdict = real.verify(payload, length);
                        final MutableDirectBuffer writable = (MutableDirectBuffer)payload;
                        writable.putByte(length / 2, (byte)~writable.getByte(length / 2));
                        return verdict;
                    });
            // a fresh replica re-applies the log from 0: its first ALIGNED trigger is the members' snapshot at P_cm
            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-scribble").build(), scribbler);
            r.rig.await(() -> !r.fatals.isEmpty(), 60_000, "a scribbling self-check did not fence the replica");
            assertTrue("SELF_CHECK_MUTATED_BUFFER fences at the first ALIGNED " + p + ": " + r.fatals,
                    r.fatals.get(0).contains("SELF_CHECK_MUTATED_BUFFER at " + p + ":"));
            assertNotWritten(r, p);
            final long p2 = p;
            System.out.println("[self-check] scribbling self-check: " + r.fatals.get(0));
            System.out.println("GREEN selfcheck p=" + p + " p2=" + p2);

            // control: no self-check at all, the mutated image is recorded
            r.stopReplica();
            r.windowGap();
            System.setProperty(ShadowSelfCheck.UNCHECKED_PROPERTY, "true");
            try {
                final ReplicaEngine unchecked = ReplicaTestBindings.withSelfCheck(
                        ReplicaTestBindings.assets(() -> r.fatals.add("CoreConfig")),
                        real -> ShadowSelfCheck.UNCHECKED_TEST_ONLY);
                r.startReplica(r.builder(ReplicaRig.SLOT + 2, "replica-unchecked").build(), unchecked);
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                r.awaitCaughtUp(60_000);
                r.faults.mutateNextImage = true;
                final long p3 = r.rig.snapshotAll();
                r.rig.await(() -> r.faults.mutatedAt == p3, 30_000, "the control's mutation never ran");
                r.rig.pumpMs(3_000);
                RcsDryRun.nc("UNCHECKED_TEST_ONLY", "poisoned shadow written at", () -> assertNotWritten(r, p3));
            } finally {
                System.clearProperty(ShadowSelfCheck.UNCHECKED_PROPERTY);
            }
        }
    }

    @Test(timeout = 600_000)
    public void aLaggingApplierFlipsReadyOnTheTimeLag() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE + 1000, "replica-self-check-timelag")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            final long timeLagMs = 2_000;
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").readyTimeLagMs(timeLagMs).readyShadowAgeMs(0)
                    .build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            r.rig.await(() -> r.probe().ready(), 30_000, () -> "never ready: " + r.probe().detail());

            // the applier falls behind a moving cluster: frames older than the setting, recorded ahead of applied
            r.faults.sleepPerFrameMs = 25;
            final long t0 = System.currentTimeMillis();
            r.tool.deposits(400);
            final String[] seen = {null};
            final long[] flippedAfter = {-1};
            r.rig.await(() -> {
                final NodeReadiness.Probe pr = r.probe();
                if (!pr.ready() && pr.detail().contains("recovery=time-lag ")) {
                    seen[0] = pr.detail();
                    flippedAfter[0] = System.currentTimeMillis() - t0;
                    return true;
                }
                return false;
            }, 60_000, () -> "a lagging applier never read time-lag: " + r.probe().detail());
            System.out.println("[self-check] time lag: not ready after " + flippedAfter[0] + " ms: " + seen[0]);
            r.faults.sleepPerFrameMs = 0;
            r.awaitCaughtUp(120_000);
            r.rig.await(() -> r.probe().ready(), 30_000, () -> "never ready again after catching up: "
                    + r.probe().detail());
            System.out.println("[self-check] time lag: ready again once caught up: " + r.probe().detail());

            System.out.println("GREEN timelag flippedAfterMs=" + flippedAfter[0]);
        }
    }

    /**
     * The frozen-supplier row (un-ignored): every member but the supplier is killed (quorum
     * lost, no leader). The supplier keeps its replay and its commit counter, so L's counter stays alive and
     * {@code recorded == applied}; with the idle branch gone and the leader's belt ticking a healthy log every ~10 ms,
     * {@code /ready} drops within {@code REPLICA_READY_TIME_LAG_MS} (default 3 s) plus one status period. Control
     * first: a quiet but healthy cluster (no traffic, a leader) stays ready for 30 s.
     */
    @Test(timeout = 600_000)
    public void aFrozenSupplierCommitFlipsReady() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE + 2000, "replica-self-check-frozen")) {
            frozenSupplierFlipsReady(r, ReplicaRig.off(), true);
            System.out.println("GREEN frozen supplier");
        }
    }

    /** {@code TIME_LAG_IDLE_ALLOWED} brings the idle branch back, so the frozen-supplier row fails. */
    @Test(timeout = 600_000)
    public void negativeControlTimeLagIdleAllowed() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE_NC_IDLE, "replica-self-check-frozen-nc")) {
            RcsDryRun.nc("TIME_LAG_IDLE_ALLOWED", "a frozen supplier commit must flip /ready",
                    () -> frozenSupplierFlipsReady(r, ReplicaRig.off(ReplicaNegativeControl.TIME_LAG_IDLE_ALLOWED),
                            false));
        }
    }

    private static final int PORT_BASE_NC_IDLE = 35200;
    /** The drop is bounded by the setting plus the follower's status period and one sample. */
    private static final long FLIP_MARGIN_MS = 2_000;

    private static void frozenSupplierFlipsReady(final ReplicaRig r,
                                                 final java.util.Set<ReplicaNegativeControl> off,
                                                 final boolean quietControl) {
        try {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").readyShadowAgeMs(0).testHooks(r.faults, off)
                    .build());
            final long timeLagMs = r.replica.config().readyTimeLagMs();
            if (timeLagMs != ReplicaConfig.DEFAULT_READY_TIME_LAG_MS) {
                throw new IllegalStateException("the row runs at the default REPLICA_READY_TIME_LAG_MS, got "
                        + timeLagMs);
            }
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            r.rig.await(() -> r.probe().ready(), 30_000, () -> "never ready: " + r.probe().detail());
            if (quietControl) {
                // control: a quiet but healthy cluster stays ready for 30 s (the belt ticks the log, E7a)
                final long until = System.currentTimeMillis() + 30_000;
                int samples = 0;
                while (System.currentTimeMillis() < until) {
                    final NodeReadiness.Probe pr = r.probe();
                    samples++;
                    if (!pr.ready()) {
                        throw new AssertionError("a quiet but healthy cluster flipped /ready after " + samples
                                + " samples: " + pr.detail());
                    }
                    r.rig.pumpMs(50);
                }
                System.out.println("[self-check] quiet healthy cluster: ready for 30 s (" + samples + " samples): "
                        + r.probe().detail());
            }
            final int supplier = r.replica.events().supplierMemberId();
            for (final Member m : r.rig.live()) {
                if (m.id != supplier && m.id != host.id) {
                    r.rig.kill(m);
                }
            }
            if (supplier != host.id) {
                r.rig.forgetAll();
                r.rig.kill(host);
            }
            final long killedAt = System.currentTimeMillis();
            final long bound = timeLagMs + FLIP_MARGIN_MS;
            final String[] flipped = {null};
            r.rig.await(() -> {
                final NodeReadiness.Probe pr = r.probe();
                if (!pr.ready()) {
                    flipped[0] = "not ready after " + (System.currentTimeMillis() - killedAt) + " ms: "
                            + pr.detail();
                    return true;
                }
                return System.currentTimeMillis() - killedAt > bound;
            }, bound + 30_000, "pump");
            final long tookMs = System.currentTimeMillis() - killedAt;
            System.out.println("[self-check] frozen supplier (quorum lost, supplier " + supplier + " alive, off=" + off
                    + "): " + (flipped[0] != null ? flipped[0] : "STILL READY after " + tookMs + " ms: "
                    + r.probe().detail()));
            if (flipped[0] == null || tookMs > bound) {
                throw new AssertionError("a frozen supplier commit must flip /ready within "
                        + bound + " ms (" + (flipped[0] != null ? flipped[0] : r.probe().detail()) + ")");
            }
            if (!flipped[0].contains("recovery=time-lag ")) {
                throw new AssertionError("the frozen supplier flipped /ready for another reason: " + flipped[0]);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void assertNotWritten(final ReplicaRig r, final long p) {
        final List<Long> all = r.allShadowPositions();
        if (all.contains(p)) {
            throw new AssertionError("poisoned shadow written at " + p + " (shadows " + all + ")");
        }
    }
}

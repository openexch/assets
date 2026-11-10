// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.shadow.ShadowKind;
import io.aeron.cluster.ClusterControl;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Apply fence: {@code Faults.throwAtFrame(n)} in the replica's apply path. The replica fences: the
 * host's fatal hook is called with {@code FATAL replica: apply fenced at}, {@code shadow_replica_fenced == 1},
 * {@code GET /ready} answers 503 {@code fenced:...}, and after 30 s of further SNAPSHOT and STANDBY toggles with traffic
 * no shadow with {@code P_s > position(n)} exists in the replica archive; the writer's abort counter is reported (no
 * write is in flight at the injected frame in this rig, so ABORTED is not forced).
 *
 * <p>Control: {@code FENCE_OFF} (the exception swallowed, the reader resumed) -> {@code shadow written after a failed
 * apply at <p>}.</p>
 */
public class ReplicaApplyFenceTest {

    private static final int PORT_BASE = 29200;

    @Test(timeout = 600_000)
    public void anApplyFailureFencesAndNothingIsWrittenAfterIt() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-apply-fence")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 60_000);

            r.faults.armThrowAtFrame(3);
            r.traffic(10, 2);
            r.rig.await(() -> !r.fatals.isEmpty(), 30_000, "the replica did not fence on the injected apply failure");
            final long failedAt = r.faults.thrownAt;
            assertTrue("the fault fired", failedAt > p1);
            assertTrue("the fatal hook names the fence: " + r.fatals,
                    r.fatals.get(0).startsWith("FATAL replica: apply fenced at"));
            assertEquals("shadow_replica_fenced", 1, r.metric("shadow_replica_fenced"));
            final String[] ready = r.httpReady();
            assertEquals("/ready of a fenced replica: " + ready[1], "503", ready[0]);
            assertTrue("/ready names the fence: " + ready[1], ready[1].contains("fenced:"));
            System.out.println("[apply-fence] fenced at " + failedAt + ": " + r.fatals.get(0) + "; /ready " + ready[0]);

            churn(r);
            assertNoShadowAfter(r, failedAt);
            assertEquals("still 503 after 30 s", "503", r.httpReady()[0]);
            assertFalse("the reader thread ended", r.replica.follower().thread().isAlive());
            System.out.println("[apply-fence] after 30 s of toggles: shadows " + r.allShadowPositions() + ", none above "
                    + failedAt + "; shadow_write_aborted_total=" + r.metric("shadow_write_aborted_total"));
            System.out.println("GREEN failedAt=" + failedAt);

            // control: the fence switched off
            r.stopReplica();
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-nc")
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.FENCE_OFF)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            r.faults.armThrowAtFrame(3);
            r.traffic(10, 2);
            r.rig.await(() -> r.faults.thrownAt >= 0, 30_000, "the fault never fired under FENCE_OFF");
            final long ncAt = r.faults.thrownAt;
            churn(r);
            RestoreDrillDryRun.nc("FENCE_OFF", "shadow written after a failed apply at",
                    () -> assertNoShadowAfter(r, ncAt));
        }
    }

    /** 30 s of SNAPSHOT and STANDBY toggles with traffic. */
    private static void churn(final ReplicaRig r) {
        final long until = System.currentTimeMillis() + 30_000;
        int i = 0;
        while (System.currentTimeMillis() < until) {
            r.traffic(5, 1);
            r.toggle(i++ % 2 == 0 ? ClusterControl.ToggleState.SNAPSHOT : ClusterControl.ToggleState.STANDBY_SNAPSHOT);
            r.rig.pumpMs(3_000);
        }
        r.rig.pumpMs(2_000);
    }

    private static void assertNoShadowAfter(final ReplicaRig r, final long failedAt) {
        final List<Long> all = r.allShadowPositions();
        for (final long p : all) {
            if (p > failedAt) {
                throw new AssertionError("shadow written after a failed apply at " + failedAt + ": P_s=" + p
                        + " (all " + all + ")");
            }
        }
    }
}

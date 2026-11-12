// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * A drill of inherent behaviour (three members, nightly): while the designated follower serializes inside its
 * {@code onTakeSnapshot} its consensus module sits in SNAPSHOT state, where the leader heartbeat check does not run.
 * The leader is killed 200 ms into a 2 s serialize (above the 1 s heartbeat timeout). The other follower, ACTIVE, detects
 * the dead leader and wins; the designated finishes its serialize, rejoins as FOLLOWER, and its ALIGNED completes (it
 * is not leader); the survivors' images are equal. The measured detection delay is the evidence; there is no control
 * (the row records behaviour). Vacuity: the kill must land inside the serialize window, else
 * {@code kill outside the serialize window}.
 */
public class DesignatedFollowerLateLeaderDetectionTest {

    private static final int PORT_BASE = 64200;
    private static final long SERIALIZE_MS = 2_000;

    @Test
    public void theOtherFollowerDetectsTheDeadLeaderWhileTheDesignatedSerializes() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "producer-late-detection", 3, "1m", 0)) {
            for (final Member m : rig.members) {
                m.settings = ShadowProducerRoleTest.producerSettings("SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS", "1");
                m.beforeLaunch = svc -> ServiceProbe.producerFault(svc, "producerSerializeDelayMs", SERIALIZE_MS);
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member d = rig.members.get(ShadowProducerRoleTest.designated(rig, leader.id));
            final Member other = rig.followerOtherThan(leader, d);
            final Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, leader);
            tool.deposits(100);
            ShadowProducerRoleTest.awaitArmed(rig);

            final int leaderSnapshots = rig.snapshots(leader).size();
            final int dSnapshots = rig.snapshots(d).size();
            final AtomicCounter toggle = ClusterControl.findControlToggle(
                    leader.cfg.consensusModuleContext().aeron().countersReader(),
                    leader.cfg.consensusModuleContext().clusterId());
            assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
            // the leader's own snapshot entry lands first; the designated is then inside its serialize
            rig.await(() -> rig.snapshots(leader).size() > leaderSnapshots, 30_000, "the leader's snapshot");
            final long serializeStartNs = System.nanoTime();
            Thread.sleep(200);
            final long p = rig.snapshots(leader).get(rig.snapshots(leader).size() - 1)[0];
            rig.forgetAll();
            final long killNs = System.nanoTime();
            rig.kill(leader);
            if (rig.snapshots(d).size() > dSnapshots) {
                throw new AssertionError("kill outside the serialize window: the designated's snapshot at " + p
                        + " was already recorded");
            }
            final Member next = rig.awaitLeader(leader);
            final long detectedMs = (System.nanoTime() - killNs) / 1_000_000;
            rig.await(() -> ShadowProducerRoleTest.written(rig, d, p) != null, 60_000,
                    () -> "shadow_write_total{ALIGNED} == 1 on the designated at " + p);
            if (ServiceProbe.isLeader(d.service)) {
                // the designated won: its write was aborted on leadership instead (the other branch of the drill)
                System.out.println("[drill] the designated member " + d.id + " won the election");
            }
            rig.awaitImagesEqual(d, other, 60_000);
            System.out.println("GREEN late detection: leader " + leader.id + " killed "
                    + (killNs - serializeStartNs) / 1_000_000 + " ms into member " + d.id + "'s " + SERIALIZE_MS
                    + " ms serialize; member " + next.id + " leads " + detectedMs
                    + " ms after the kill; the designated's ALIGNED at " + p + " completed; images equal");
        }
    }
}

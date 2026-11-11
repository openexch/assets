// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The leader never pulls, on a real cluster: every member lists every other member's archive, a genuine ALIGNED sits
 * on a follower's archive, and the leader's puller (running, its role read from the service) lists nothing and pulls
 * nothing while it leads, its skipped passes counted; the control hands the leader's puller a role that never reads
 * LEADER and the leader pulls. With three members ({@code -Dcore.pull.members=3}, the default and the nightly job's;
 * 2 runs the gate without the kill) the leader is killed: the new leader pulls nothing while it leads, and the old
 * leader, restarted as a follower, pulls within one period.
 */
public class LeaderDoesNotPullTest {

    private static final int PORT_BASE = 49000;
    private static final long PERIOD_MS = 1000;
    private static int nextProducerId = 400;

    @Test
    public void theLeaderListsAndPullsNothingWhileItLeads() throws Exception {
        final int n = ShadowPullerClusterTest.members();
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + n * 100, "leader-no-pull-" + n, n, "64k",
                StubStory.SEGMENT)) {
            for (final Member m : rig.members) {
                final List<Member> others = new ArrayList<>(rig.members);
                others.remove(m);
                m.settings = ShadowClusterRig.shadowSettings("SHADOW_PULL_SOURCES",
                        rig.sources(others.toArray(new Member[0])), "SHADOW_PULL_PERIOD_MS", Long.toString(PERIOD_MS));
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, leader);
            tool.deposits(300);
            final long pCm = rig.snapshotAll();
            tool.deposits(40);
            rig.awaitImagesEqual(f, leader, 60_000);
            for (final Member m : rig.live()) {
                rig.await(() -> ServiceProbe.puller(m.service) != null, 30_000, "member " + m.id + "'s puller");
            }

            // a pullable ALIGNED on a follower's archive
            final RecordedLog log = rig.captureCommitted(f);
            final Written w = rig.producer(f).produce(new Who(ProducerKind.FOLLOWER, nextProducerId++), log, pCm);
            assertTrue(w.toString(), w.ok());
            for (final Member m : ShadowPullerClusterTest.followers(rig, leader)) {
                if (m.id != f.id) {
                    rig.await(() -> ServiceProbe.pull(m.service, "shadow_pull_total") >= 1, 30_000,
                            "the other follower " + m.id + " pulls the shadow (it is pullable)");
                }
            }
            final long skipped0 = ServiceProbe.pull(leader.service, "shadow_pull_skipped_leader_total");
            rig.pumpMs(3 * PERIOD_MS + 500);
            assertLeaderDidNotPull(rig, leader, skipped0);

            // control: the leader's puller is handed a role that never reads LEADER: it pulls
            RestoreDrillDryRun.nc("leader-gate-off", "the leader pulled: shadow_pull_total=", () -> {
                ServiceProbe.pullLeaderGateOff(leader.service);
                final long s0 = ServiceProbe.pull(leader.service, "shadow_pull_skipped_leader_total");
                rig.await(() -> ServiceProbe.pull(leader.service, "shadow_pull_total") >= 1, 3 * PERIOD_MS + 10_000,
                        () -> "the control never pulled");
                assertLeaderDidNotPull(rig, leader, s0);
            });

            if (n < 3) {
                return;
            }
            // three members: kill the leader; the new leader pulls nothing while it leads; the old one pulls on return
            rig.forgetAll();
            rig.kill(leader);
            final Member next = rig.awaitLeader(leader);
            rig.await(() -> ServiceProbe.puller(next.service) != null, 30_000, "the new leader's puller");
            final long pulledBefore = ServiceProbe.pull(next.service, "shadow_pull_total");
            final long nextSkipped0 = ServiceProbe.pull(next.service, "shadow_pull_skipped_leader_total");
            final Member other = rig.followerOtherThan(next, null);
            final RecordedLog log2 = rig.captureCommitted(other);
            final long p2 = log2.positions()[StubStory.indexOf(log2, pCm) + 10];
            final Written w2 = rig.producer(other).produce(new Who(ProducerKind.FOLLOWER, nextProducerId++), log2, p2);
            assertTrue(w2.toString(), w2.ok());
            rig.pumpMs(3 * PERIOD_MS + 500);
            if (ServiceProbe.pull(next.service, "shadow_pull_total") != pulledBefore) {
                throw new AssertionError("the leader pulled: shadow_pull_total="
                        + ServiceProbe.pull(next.service, "shadow_pull_total") + " while LEADER (member " + next.id + ")");
            }
            assertTrue("no leader pass observed on the new leader " + next.id,
                    ServiceProbe.pull(next.service, "shadow_pull_skipped_leader_total") > nextSkipped0);
            rig.restart(leader);
            rig.await(() -> !leader.killed && leader.role() == Cluster.Role.FOLLOWER, 60_000,
                    "the old leader rejoins as a follower");
            final long backNs = System.nanoTime();
            rig.await(() -> ServiceProbe.puller(leader.service) != null
                            && ServiceProbe.pull(leader.service, "shadow_pull_total") >= 1, 30_000,
                    "the restarted old leader pulls as a follower");
            System.out.println("GREEN leader gate 3 members: the new leader " + next.id + " skipped "
                    + (ServiceProbe.pull(next.service, "shadow_pull_skipped_leader_total") - nextSkipped0)
                    + " passes, pulled nothing; the old leader pulled "
                    + ServiceProbe.pull(leader.service, "shadow_pull_total") + " within "
                    + (System.nanoTime() - backNs) / 1_000_000 + " ms of its puller start");
        }
    }

    private static void assertLeaderDidNotPull(final ShadowClusterRig rig, final Member leader, final long skipped0) {
        assertEquals("member " + leader.id + " still leads", Cluster.Role.LEADER, leader.role());
        final long pulled = ServiceProbe.pull(leader.service, "shadow_pull_total");
        final long started = ServiceProbe.pull(leader.service, "shadow_pull_started_total");
        if (pulled != 0 || started != 0) {
            throw new AssertionError("the leader pulled: shadow_pull_total=" + pulled + " while LEADER (started "
                    + started + ")");
        }
        final long skipped = ServiceProbe.pull(leader.service, "shadow_pull_skipped_leader_total") - skipped0;
        if (skipped <= 0) {
            throw new AssertionError("no leader pass observed");
        }
        System.out.println("GREEN leader gate: member " + leader.id + " skipped " + skipped + " passes as LEADER,"
                + " shadow_pull_total=0");
    }
}

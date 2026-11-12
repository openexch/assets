// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * The leader never produces and never finishes a write (three members, embedded drivers, nightly). The designated
 * follower's writer holds its first payload chunk; the other follower is down, the leader is killed and the other
 * follower restarted with a log behind the designated's, so the designated must win the election while its write is in
 * flight: the write is aborted on the role change, its partial recording purged, {@code shadow_write_total} does not
 * move while LEADER and the abort latency is recorded. After the designated is killed in turn, the next leader's
 * designated produces at the next P_cm.
 *
 * <p>Control {@code -Dcore.producer.nc=LEADER_FINISHES_WRITE} (no abort on becoming LEADER) must fail with
 * {@code shadow_write_total moved while LEADER}.</p>
 */
public class ShadowProducerNeverOnLeaderTest {

    private static final int PORT_BASE = 62600;
    private static final long HOLD_MS = 15_000;

    @Test
    public void aWriteInFlightIsAbortedWhenTheDesignatedBecomesLeader() throws Exception {
        final boolean finishes = "LEADER_FINISHES_WRITE".equals(System.getProperty("core.producer.nc", ""));
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "producer-never-on-leader", 3, "1m", 0)) {
            for (final Member m : rig.members) {
                m.settings = ShadowProducerRoleTest.producerSettings();
                m.beforeLaunch = svc -> {
                    ServiceProbe.slowWriter(svc, HOLD_MS);
                    if (finishes) {
                        ServiceProbe.producerFault(svc, "leaderFinishesWrite", true);
                    }
                };
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

            // the other follower down; the snapshot starts the designated's held write
            rig.kill(other);
            tool.deposits(50);
            final long writesBefore = ServiceProbe.shadowWriteTotal(d.service, ShadowKind.ALIGNED);
            final long p = rig.snapshotAll();
            rig.await(() -> rig.tee.all("shadow=aligned").size() >= 1, 10_000, "the designated never handed off");
            // the leader killed, the other follower back with an older log: the designated must win
            rig.forgetAll();
            rig.kill(leader);
            rig.restart(other);
            final Member next = rig.awaitLeader(leader);
            if (next.id != d.id) {
                throw new AssertionError("no designated-to-leader transition observed: member " + next.id + " leads");
            }
            final long leaderAtNs = System.nanoTime();
            rig.await(() -> ServiceProbe.producerAbortedLeader(d.service) >= 1
                            || ServiceProbe.shadowWriteTotal(d.service, ShadowKind.ALIGNED) > writesBefore,
                    HOLD_MS + 30_000, "neither an abort nor a write on the new leader");
            rig.pumpMs(1_000);
            final long moved = ServiceProbe.shadowWriteTotal(d.service, ShadowKind.ALIGNED) - writesBefore;
            if (moved != 0) {
                throw new AssertionError("shadow_write_total moved while LEADER: " + moved);
            }
            assertEquals("shadow_producer_aborted_leader_total", 1, ServiceProbe.producerAbortedLeader(d.service));
            final long latencyMs = ServiceProbe.producerAbortLatencyNs(d.service) / 1_000_000;
            assertNull("no F copy at " + p + " on the new leader", ShadowProducerRoleTest.written(rig, d, p));
            // the partial recording is purged: two scans see no incomplete copy
            rig.pumpMs(2 * 1_000);
            rig.await(() -> ServiceProbe.shadowCounter(d.service, ShadowCounters.Counter.CATALOG_INCOMPLETE) == 0,
                    30_000, () -> "catalog_incomplete "
                            + ServiceProbe.shadowCounter(d.service, ShadowCounters.Counter.CATALOG_INCOMPLETE));
            System.out.println("GREEN abort on leadership: member " + d.id + " aborted the write at P_cm=" + p + " in "
                    + latencyMs + " ms after the role change (" + (System.nanoTime() - leaderAtNs) / 1_000_000
                    + " ms after the leader was seen), no copy, catalog_incomplete 0");

            // after it steps down: restart the old leader, kill the designated; the new leader's designated produces
            rig.restart(leader);
            rig.awaitReady(leader, 120_000);
            rig.forgetAll();
            rig.kill(d);
            final Member third = rig.awaitLeader(d);
            final Client tool2 = rig.connect(SessionClass.TOOL, third);
            rig.restart(d);
            rig.awaitReady(d, 120_000);
            ShadowProducerRoleTest.awaitArmed(rig);
            tool2.deposits(20);
            final long[] before = ShadowProducerRoleTest.writes(rig);
            final long p2 = rig.snapshotAll();
            ShadowProducerRoleTest.assertProducedBy(rig, third, p2, before);
            System.out.println("GREEN after the step-down: leader " + third.id + ", designated member "
                    + ShadowProducerRoleTest.designated(rig, third.id) + " produced at " + p2);
        }
    }
}

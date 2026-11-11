// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.testkit.RecordedLog;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The real replica as a pull source, and the two-producer anchor on real members (every member in {@code shadow}
 * mode, {@code SHADOW_ANCHOR_RULE=two-producers}, sources = the replica's archive, then every peer's). The replica
 * writes a VERIFIED ALIGNED at the members' P_cm; the followers pull it; one producer is no witness: the floor
 * evaluates NO_WITNESS ({@code shadow_anchor_witnesses 1}) and the log start stays. At the next P_cm a second
 * producer's shadow of the same bytes (the test producer, as a follower slot) appears in the OTHER follower's archive;
 * the first follower, which already holds the replica's copy at that position VERIFIED, pulls it from that peer (the
 * second witness of a held position): two witnesses, the floor anchors there with the previous VERIFIED shadow as its
 * margin and the log start moves (to at most the first P_cm). Control {@code any-verified} on the other follower
 * (restarted with it before the first snapshot): it purges below P_cm behind the replica alone.
 */
public class MemberPullInteropTest {

    private static final int PORT_BASE = 52000;
    private static final int SEGMENT = 64 * 1024;
    private static final long PERIOD_MS = 1000;

    @Test(timeout = 1_800_000)
    public void theReplicaIsOneWitnessAndASecondProducerMakesTheAnchor() throws Exception {
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE, "member-pull-interop", 3, "64k", SEGMENT))) {
            final ReplicaConfig cfg = r.builder(ReplicaRig.SLOT, "replica").build();
            final String replicaEndpoint = cfg.archiveControlEndpoint();
            for (final Member m : r.rig.members) {
                m.settings = settings(r, m, replicaEndpoint, "two-producers");
            }
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member f1 = r.rig.followerOtherThan(leader, null);
            final Member f2 = r.rig.followerOtherThan(leader, f1);
            r.story(f1, 4);
            // the control member: f2 restarted with any-verified before any snapshot (a clean full restart)
            r.rig.kill(f2);
            f2.settings = settings(r, f2, replicaEndpoint, "any-verified");
            r.rig.restart(f2);
            r.hook(f2);
            r.rig.awaitImagesEqual(f2, leader, 60_000);
            r.startReplica(cfg);
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);

            // P_cm1: the replica's ALIGNED, pulled by both followers: one producer, no witness
            r.tool.deposits(600);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            for (final Member m : List.of(f1, f2)) {
                r.rig.await(() -> ServiceProbe.pull(m.service, "shadow_pull_total") >= 1, 2 * PERIOD_MS + 30_000,
                        () -> "member " + m.id + " never pulled the replica's ALIGNED at " + p1);
            }
            r.rig.await(() -> ServiceProbe.housekeeping(f1.service, "shadow_floor_no_witness_total") >= 1, 60_000,
                    () -> "member " + f1.id + "'s floor never evaluated NO_WITNESS: "
                            + "witnesses " + ServiceProbe.housekeeping(f1.service, "shadow_anchor_witnesses"));
            assertEquals("one producer is one witness", 1,
                    ServiceProbe.housekeeping(f1.service, "shadow_anchor_witnesses"));
            assertEquals("the log start stays behind one producer", 0, r.rig.logStart(f1));
            System.out.println("[interop] P_cm1=" + p1 + ": pulled from the replica, NO_WITNESS, witnesses 1, log start 0");

            // P_cm2: the replica's ALIGNED again, then a second producer's shadow of the same bytes on f1
            r.tool.deposits(600);
            final long p2 = r.rig.snapshotAll();
            r.awaitVerified(p2, ShadowKind.ALIGNED, 120_000);
            for (final Member m : List.of(f1, f2)) {
                r.rig.await(() -> ServiceProbe.pull(m.service, "shadow_pull_total") >= 2, 2 * PERIOD_MS + 30_000,
                        () -> "member " + m.id + " never pulled the replica's ALIGNED at " + p2);
            }
            // the control: behind the replica alone, the any-verified follower purges below P_cm2
            RestoreDrillDryRun.nc("any-verified anchor", "log purged below P_cm behind one producer", () -> {
                r.rig.await(() -> r.rig.logStart(f2) > 0, 60_000, "the control never purged");
                assertNoPurgeBehindOneProducer(r, f2, p2);
            });
            r.rig.pumpMs(3 * PERIOD_MS);
            assertNoPurgeBehindOneProducer(r, f1, p2);

            // the second producer's copy appears on the peer f2 (built from f1's unpurged log: one committed log);
            // f1 holds the replica's copy at P_cm2 VERIFIED and pulls this one from its peer as the second witness
            final RecordedLog log = r.rig.captureCommitted(f1);
            final Written second = r.rig.producer(f2).produce(new Who(ProducerKind.FOLLOWER, 700), log, p2);
            assertTrue(second.toString(), second.ok());
            final com.openexchange.cluster.shadow.ShadowEntry source = r.rig.scan(f2).entry(second.recordingId());
            r.rig.await(() -> ShadowPullerClusterTest.copyOf(r.rig.scan(f1), source) != null, 2 * PERIOD_MS + 30_000,
                    () -> "member " + f1.id + " never pulled the second producer's copy at P_cm2=" + p2 + " from its"
                            + " peer: a peer's second witness at a held position was never pulled (witnesses "
                            + ServiceProbe.housekeeping(f1.service, "shadow_anchor_witnesses") + ")");
            // the copy is listed (COMPLETE) before its deep read; pull_total moves after VERIFIED
            r.rig.await(() -> ServiceProbe.pull(f1.service, "shadow_pull_total") >= 3, 30_000,
                    () -> "member " + f1.id + " never verified the peer's second witness: shadow_pull_total "
                            + ServiceProbe.pull(f1.service, "shadow_pull_total"));
            r.rig.await(() -> ServiceProbe.housekeeping(f1.service, "shadow_anchor_witnesses") >= 2, 60_000,
                    () -> "member " + f1.id + ": two producers of one digest never made two witnesses: witnesses "
                            + ServiceProbe.housekeeping(f1.service, "shadow_anchor_witnesses"));
            r.rig.await(() -> r.rig.logStart(f1) > 0, 60_000, () -> "member " + f1.id + "'s log start never moved"
                    + " behind two witnesses: " + "witnesses " + ServiceProbe.housekeeping(f1.service, "shadow_anchor_witnesses"));
            final long start = r.rig.logStart(f1);
            assertTrue("the purge stays at or below the margin P_cm1=" + p1 + ": " + start, start <= p1);
            System.out.println("GREEN interop: P_cm2=" + p2 + " witnessed by the replica and a follower slot; member "
                    + f1.id + " log start " + start + " (margin P_cm1=" + p1 + ")");
        }
    }

    private static void assertNoPurgeBehindOneProducer(final ReplicaRig r, final Member m, final long pCm) {
        final long start = r.rig.logStart(m);
        if (start > 0) {
            throw new AssertionError("log purged below P_cm behind one producer: member " + m.id + " log start "
                    + start + " (P_cm " + pCm + ", witnesses "
                    + ServiceProbe.housekeeping(m.service, "shadow_anchor_witnesses") + ")");
        }
    }

    private static com.openexchange.core.infrastructure.persistence.SnapshotModeSettings settings(
            final ReplicaRig r, final Member m, final String replicaEndpoint, final String rule) {
        final List<Member> peers = new ArrayList<>(r.rig.members);
        peers.remove(m);
        return ShadowClusterRig.shadowSettings("SHADOW_PULL_SOURCES", replicaEndpoint + ","
                        + r.rig.sources(peers.toArray(new Member[0])), "SHADOW_PULL_PERIOD_MS", Long.toString(PERIOD_MS),
                "SHADOW_ANCHOR_RULE", rule, "SHADOW_DEEP_READ_PACE_MS", "0", "SHADOW_SCAN_PERIOD_MS", "500");
    }
}

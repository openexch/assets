// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowPullSettings;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Two real producers on real members (three members in {@code shadow} mode, {@code two-producers}, sources = the
 * replica and every peer, the real replica, no test producer; nightly). At P_cm the replica writes R and the designated
 * follower writes F with one payload digest; the designated pulls R (the second witness), the other follower pulls
 * the F and the R; both followers count two witnesses and their floor moves the log start; the followers' pullers skip
 * the leader's archive. Then the replica is stopped: two more snapshots carry F copies only, and every follower raises
 * the floor-side alarm naming the missing replica ({@code shadow_floor_witness_missing{kind=R} 1} and the CRITICAL
 * line) after {@code SHADOW_WITNESS_ALARM_MS} (a leader neither pulls nor produces, so its floor carries no alarm and
 * its gauge reads 0), and no follower purges below P_cm behind the one producer. Then the re-bootstrap from a real
 * second producer: the members' logs are purged past the replica's L.stop below the newest P_cm (the operator stand-in),
 * the replica restarts with the members as its pull sources, pulls the designated follower's F copy, exits 3 to
 * re-bootstrap, restores from that pulled F copy at the second start, follows, writes the next ALIGNED with the members'
 * digest, and the floor-side alarm clears on every follower.
 *
 * <p>Controls ({@code -Dcore.producer.nc=<seam>}): {@code WITNESS_ALARM_OFF} must fail with {@code no floor-side line
 * while the anchor had one witness}; {@code ANCHOR_RULE_IGNORED} ({@code SHADOW_ANCHOR_RULE=any-verified} on every
 * member) must fail with {@code log purged below P_cm behind one producer}.</p>
 */
public class DualProducerLiveTest {

    private static final int PORT_BASE = 63800;
    private static final int SEGMENT = 64 * 1024;
    private static final long PERIOD_MS = 1000;
    private static final long ALARM_MS = 3_000;
    private static final long CYCLE_MS = 300_000;

    @Test(timeout = 1_800_000)
    public void theReplicaAndTheDesignatedFollowerAreTheTwoWitnesses() throws Exception {
        final String nc = System.getProperty("core.producer.nc", "");
        final boolean alarmOff = "WITNESS_ALARM_OFF".equals(nc);
        final boolean anchorRuleIgnored = "ANCHOR_RULE_IGNORED".equals(nc);
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE, "dual-producer", 3, "64k", SEGMENT))) {
            final ReplicaConfig cfg = r.builder(ReplicaRig.SLOT, "replica").build();
            final String replicaEndpoint = cfg.archiveControlEndpoint();
            for (final Member m : r.rig.members) {
                final List<Member> peers = new ArrayList<>(r.rig.members);
                peers.remove(m);
                m.settings = ShadowProducerRoleTest.producerSettings("SHADOW_PULL_SOURCES", replicaEndpoint + ","
                                + r.rig.sources(peers.toArray(new Member[0])), "SHADOW_PULL_PERIOD_MS",
                        Long.toString(PERIOD_MS), "SHADOW_WITNESS_ALARM_MS", Long.toString(ALARM_MS),
                        "SHADOW_ANCHOR_RULE", anchorRuleIgnored ? "any-verified" : "two-producers");
                if (alarmOff) {
                    m.beforeLaunch = svc -> ServiceProbe.producerFault(svc, "witnessAlarmOff", true);
                }
            }
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member d = r.rig.members.get(ShadowProducerRoleTest.designated(r.rig, leader.id));
            final Member o = r.rig.followerOtherThan(leader, d);
            r.story(d, 4);
            r.startReplica(cfg);
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            ShadowProducerRoleTest.awaitArmed(r.rig);

            // P_cm1: R from the replica, F from the designated, one digest
            r.tool.deposits(400);
            final long p1 = r.rig.snapshotAll();
            final LogicalShadow rCopy = r.awaitVerified(p1, ShadowKind.ALIGNED, 120_000);
            r.rig.await(() -> ShadowProducerRoleTest.written(r.rig, d, p1) != null, 60_000,
                    "shadow_write_total{kind=ALIGNED} == 1 on member " + d.id + " at " + p1);
            final ShadowEntry fCopy = ShadowProducerRoleTest.written(r.rig, d, p1);
            assertEquals("R and F agree on the payload digest at " + p1,
                    rCopy.representative().header().payloadDigest(), fCopy.header().payloadDigest());

            // P_cm2: the floor of both followers anchors on two producers and moves the log start
            r.tool.deposits(400);
            final long p2 = r.rig.snapshotAll();
            r.awaitVerified(p2, ShadowKind.ALIGNED, 120_000);
            for (final Member m : List.of(d, o)) {
                if (!anchorRuleIgnored) { // the gauge of the two-producer rule; the control runs another rule
                    r.rig.await(() -> ServiceProbe.housekeeping(m.service, "shadow_anchor_witnesses") >= 2, 120_000,
                            () -> "shadow_anchor_witnesses == 2 on member " + m.id + ": "
                                    + ServiceProbe.housekeeping(m.service, "shadow_anchor_witnesses"));
                }
                r.rig.await(() -> r.rig.logStart(m) > 0, 60_000, () -> "member " + m.id + "'s log start never moved");
                assertTrue("member " + m.id + " skipped the leader's archive",
                        ServiceProbe.pull(m.service, "shadow_pull_source_skipped_leader_total") >= 1);
            }
            assertEquals("the leader pulls nothing", 0, ServiceProbe.pull(leader.service, "shadow_pull_total"));
            System.out.println("GREEN dual: P_cm1=" + p1 + " R and F one digest; P_cm2=" + p2 + " two witnesses on"
                    + " members " + d.id + " and " + o.id + ", log starts " + r.rig.logStart(d) + "/"
                    + r.rig.logStart(o));

            // the replica down: F only; the floor-side alarm on every member after the window
            final CoreRestoreRig.Tee err = new CoreRestoreRig.Tee(System.err, "[SHADOW-FLOOR] CRITICAL");
            final java.io.PrintStream prior = System.err;
            System.setErr(err);
            try {
                final long lStop = r.replica.follower().recordedPosition();
                r.stopReplica();
                // each F-only interval moves the log past a 64k segment, so a purge behind one producer is visible
                for (int i = 0; i < 2; i++) {
                    r.tool.deposits(1500);
                    final long p = r.rig.snapshotAll();
                    r.rig.await(() -> ShadowProducerRoleTest.written(r.rig, d, p) != null, 60_000, "F at " + p);
                }
                // two pruner wakes (every 5 s) after the F copies verified: no follower purges below P_cm on one producer
                r.rig.pumpMs(2 * 5_000 + 2_000);
                for (final Member m : List.of(d, o)) {
                    final long start = r.rig.logStart(m);
                    if (start > p2) {
                        throw new AssertionError("log purged below P_cm behind one producer: member " + m.id
                                + " log start " + start + " above the last two-producer anchor P_cm2=" + p2
                                + " (witnesses " + ServiceProbe.housekeeping(m.service, "shadow_anchor_witnesses")
                                + ")");
                    }
                }
                // the housekeeping evaluates on the pruner's wake (every 5 s): the window plus two wakes
                for (final Member m : List.of(d, o)) {
                    try {
                        r.rig.await(() -> ServiceProbe.witnessMissing(m.service, ProducerKind.REPLICA) == 1,
                                ALARM_MS + 2 * 5_000 + 5_000, "alarm");
                    } catch (final AssertionError none) {
                        throw new AssertionError("no floor-side line while the anchor had one witness for "
                                + (ALARM_MS / 1000) + " s: member " + m.id + " witness_missing{R}="
                                + ServiceProbe.witnessMissing(m.service, ProducerKind.REPLICA));
                    }
                }
                final String line = StubStory.line(err.lines, "missing kind R (replica)");
                assertNotNull("the floor-side CRITICAL line: " + err.lines, line);
                assertEquals("the leader's floor carries no alarm (it neither pulls nor produces)", 0,
                        ServiceProbe.witnessMissing(leader.service, ProducerKind.REPLICA));
                System.out.println("GREEN alarm: " + line.trim());

                reBootstrapFromTheFollowerCopy(r, d, o, lStop);
            } finally {
                System.setErr(prior);
            }
        }
    }

    /**
     * The replica pruned past while it was down re-bootstraps from the REAL designated follower's ALIGNED: the members
     * purge their logs past L.stop below the newest P_cm (the operator stand-in), the replica pulls the F copy, exits 3,
     * restores from it at the second start, follows, and its next ALIGNED is the second witness that clears the alarm.
     */
    private static void reBootstrapFromTheFollowerCopy(final ReplicaRig r, final Member d, final Member o,
                                                       final long lStop) {
        r.tool.deposits(900);
        r.rig.snapshotAll();
        r.tool.deposits(300);
        final long pB = r.rig.snapshotAll();
        r.rig.await(() -> ShadowProducerRoleTest.written(r.rig, d, pB) != null, 60_000, "F at " + pB);
        r.tool.deposits(60);
        final long boundary = pB / SEGMENT * SEGMENT;
        assertTrue("the purge boundary " + boundary + " lies past L.stop=" + lStop, boundary > lStop);
        for (final Member m : r.rig.live()) {
            assertEquals("member " + m.id + " log start after the purge", boundary, r.rig.purgeLogTo(m, boundary));
        }
        final List<Member> ms = r.rig.members;
        final ShadowPullSettings pull = ShadowPullSettings.parse(Map.of(
                ShadowPullSettings.SOURCES, r.rig.sources(ms.toArray(new Member[0])),
                ShadowPullSettings.PERIOD_MS, "1000",
                ShadowPullSettings.STARTUP_WAIT_MS, "20000")::get, null);
        System.out.println("[dual] members purged to " + boundary + " past L.stop=" + lStop + "; F" + d.id
                + " ALIGNED at " + pB);

        r.windowGap();
        r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
        r.awaitBootstrapped(ReplicaRig.WAIT_MS);
        r.rig.await(() -> !r.restarts.isEmpty() || !r.fatals.isEmpty(), CYCLE_MS,
                () -> "re-bootstrap not taken within " + CYCLE_MS + " ms: rebootstrap_total "
                        + r.metric("shadow_replica_rebootstrap_total") + ", " + r.probe().detail());
        assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
        final String restart = r.restarts.get(0);
        assertTrue(restart, restart.startsWith("3 replica re-bootstrap: members pruned past L.stop="));
        ReplicaReBootstrapTest.assertPulledBeforeTheRestart(r, restart, pB);
        assertEquals("rebootstrap_total", 1, r.metric("shadow_replica_rebootstrap_total"));
        r.stopReplica();
        System.out.println("GREEN re-bootstrap decided on the pulled F copy: " + restart);

        r.windowGap();
        r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
        r.awaitBootstrapped(ReplicaRig.WAIT_MS);
        assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
        assertEquals("restored from the pulled shadow", pB, r.replica.follower().restoredPosition());
        r.rig.await(() -> r.scan() != null && r.scan().atPosition(pB) != null, 60_000,
                () -> "the pulled copy at " + pB + " in the replica's catalog: " + r.describeScan());
        final LogicalShadow pulled = r.scan().atPosition(pB);
        boolean fromTheFollower = false;
        for (final ShadowEntry e : pulled.copies()) {
            fromTheFollower |= e.header() != null && e.header().producerKind() == ProducerKind.FOLLOWER.code()
                    && e.header().producerId() == d.id;
        }
        assertTrue("the restored shadow is the designated follower's F" + d.id + " copy: " + r.describeScan(),
                fromTheFollower);
        r.awaitCaughtUp(120_000);
        final long next = r.rig.snapshotAll();
        final LogicalShadow rNext = r.awaitVerified(next, ShadowKind.ALIGNED, 120_000);
        r.rig.await(() -> ShadowProducerRoleTest.written(r.rig, d, next) != null, 60_000, "F at " + next);
        assertEquals("R and F agree on the payload digest at " + next,
                rNext.representative().header().payloadDigest(),
                ShadowProducerRoleTest.written(r.rig, d, next).header().payloadDigest());
        for (final Member m : List.of(d, o)) {
            r.rig.await(() -> ServiceProbe.witnessMissing(m.service, ProducerKind.REPLICA) == 0
                    && ServiceProbe.housekeeping(m.service, "shadow_anchor_witnesses") >= 2, 120_000,
                    () -> "the floor-side alarm never cleared on member " + m.id + ": witness_missing{R}="
                            + ServiceProbe.witnessMissing(m.service, ProducerKind.REPLICA) + " witnesses "
                            + ServiceProbe.housekeeping(m.service, "shadow_anchor_witnesses"));
        }
        System.out.println("GREEN second start: LOCAL_SHADOW from the pulled F" + d.id + " copy at " + pB
                + ", the next ALIGNED at " + next + " has R and F with one digest, the alarm cleared on members "
                + d.id + " and " + o.id);
    }
}

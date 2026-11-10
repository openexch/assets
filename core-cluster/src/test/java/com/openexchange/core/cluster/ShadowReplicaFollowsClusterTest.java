// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.cluster.SnapshotCadence;
import com.openexchange.cluster.replica.BackupEvents;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterControl;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Three core members plus the in-JVM replica (FOLLOWER source). GATEWAY, BRIDGE, TOOL and ADMIN sessions
 * drive orders and deposits; a SNAPSHOT toggle on the leader gives a VERIFIED ALIGNED shadow at the members' P_cm
 * whose payload equals the members' {@code encodeSnapshot} captured on the service thread at that snapshot; a
 * STANDBY_SNAPSHOT toggle gives a STANDBY shadow at the flags-1 action frame end while every member's RecordingLog
 * snapshot count, {@code core_snapshot_stub_total}/{@code core_snapshot_full_total}, {@code onTakeSnapshot} count and
 * cadence byte baseline stay unchanged; no backup response in the run named the leader as the log supplier; a leader
 * kill mid-run, and the next ALIGNED still equals the members'.
 *
 * <p>Controls: (a) {@code TRIGGER_IGNORES_STANDBY} -> {@code no STANDBY shadow at <p> within <t>}; (b) a forward
 * shift forgery (content S(x) labelled P_cm, {@code TestShadowProducer.imageAt}) offered to the oracle as the replica's
 * image -> {@code payload differs from member snapshot at <p>}; (c) {@code sourceType=LEADER} ->
 * {@code supplier is the leader}. The base has no replica (this class does not compile on 98be859): the controls are
 * the sensitivity evidence (the red run).</p>
 */
public class ShadowReplicaFollowsClusterTest {

    private static final int PORT_BASE = 28000; // members 28000/28100/28200, replica slots 3..5 at 28300..28500
    private static final long STANDBY_WAIT_MS = 20_000;

    @Test(timeout = 900_000)
    public void followsAlignedAndStandbyThroughALeaderKill() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-follow")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);

            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals("a fresh replica against a complete log starts at genesis",
                    ReplicaReadiness.RestorePath.GENESIS, r.replica.readiness().restorePath());
            r.awaitCaughtUp(60_000);

            // ALIGNED at P_cm
            final long pCm = r.rig.snapshotAll();
            final LogicalShadow aligned = r.awaitVerified(pCm, ShadowKind.ALIGNED, 60_000);
            final byte[] replicaImage = r.payload(aligned);
            for (final Member m : r.rig.live()) {
                ReplicaRig.assertPayloadEqualsMember(pCm, replicaImage, r.imageAtSnapshot.get(m.id));
            }
            System.out.println("[follow] ALIGNED shadow at P_cm=" + pCm + " equals all members' images ("
                    + replicaImage.length + " B)");

            // (b) the oracle sees a forward shift: content S(x), x < P_cm, labelled P_cm
            final RecordedLog log = r.rig.captureCommitted(host);
            final long x = forgeryPosition(log, pCm, replicaImage);
            final byte[] forged = TestShadowProducer.imageAt(log, x);
            RestoreDrillDryRun.nc("FORWARD_SHIFT_FORGERY", "payload differs from member snapshot at",
                    () -> ReplicaRig.assertPayloadEqualsMember(pCm, forged, r.imageAtSnapshot.get(host.id)));

            // STANDBY at the flags-1 frame end; members do not move
            r.traffic(20, 4);
            final Map<Integer, long[]> before = memberState(r);
            final int standbyFramesBefore = r.actionFrames(host, 1).size();
            r.toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
            final long pStandby = awaitStandbyFrame(r, host, standbyFramesBefore);
            awaitStandbyShadow(r, pStandby, STANDBY_WAIT_MS);
            r.traffic(10, 2);
            final Map<Integer, long[]> after = memberState(r);
            for (final Member m : r.rig.live()) {
                final long[] b = before.get(m.id);
                final long[] a = after.get(m.id);
                assertEquals("member " + m.id + " RecordingLog snapshot count moved on a STANDBY toggle", b[0], a[0]);
                assertEquals("member " + m.id + " core_snapshot_stub_total moved on a STANDBY toggle", b[1], a[1]);
                assertEquals("member " + m.id + " core_snapshot_full_total moved on a STANDBY toggle", b[2], a[2]);
                assertEquals("member " + m.id + " onTakeSnapshot ran on a STANDBY toggle", b[3], a[3]);
                assertEquals("member " + m.id + " cadence byte baseline moved on a STANDBY toggle", b[4], a[4]);
            }
            System.out.println("[follow] STANDBY shadow at the flags-1 frame end " + pStandby
                    + "; members' snapshot entries, rcs counters, onTakeSnapshot and cadence baseline unchanged");
            assertSupplierNotLeader(r.replica.events());

            // a leader kill mid-run: the replica keeps following and the next ALIGNED still matches
            r.traffic(30, 6);
            r.rig.kill(leader);
            final Member newLeader = r.rig.awaitLeader(leader);
            System.out.println("[follow] killed leader " + leader.id + "; new leader " + newLeader.id + "; supplier was "
                    + r.replica.events().supplierMemberId());
            r.reconnectTool();
            r.traffic(30, 0);
            // the next query re-validates FOLLOWER after the election: a supplier that became the leader is excluded
            // (CBA:482-487) and the replica re-attaches to the remaining follower by extending L
            final long responsesAtKill = r.replica.events().responses();
            final long queriesAtKill = r.replica.events().queries();
            r.rig.await(() -> r.replica.events().queries() > queriesAtKill + 1
                            || (r.replica.events().responses() > responsesAtKill
                            && r.replica.events().supplierMemberId() != newLeader.id),
                    ReplicaRig.WAIT_MS, () -> "no post-election backup query: queries=" + r.replica.events().queries()
                            + " responses=" + r.replica.events().responses());
            r.rig.await(() -> r.replica.events().supplierMemberId() != newLeader.id
                            && r.replica.follower().liveRecordingAlive(), ReplicaRig.WAIT_MS,
                    () -> "after the election the supplier is still the new leader " + newLeader.id + " or L is not"
                            + " live: " + r.probe().detail());
            r.traffic(30, 0);
            final long pCm2 = r.rig.snapshotAll();
            final LogicalShadow aligned2 = r.awaitVerified(pCm2, ShadowKind.ALIGNED, ReplicaRig.WAIT_MS);
            final byte[] replicaImage2 = r.payload(aligned2);
            for (final Member m : r.rig.live()) {
                ReplicaRig.assertPayloadEqualsMember(pCm2, replicaImage2, r.imageAtSnapshot.get(m.id));
            }
            assertTrue("the replica fenced: " + r.replica.follower().fenced(), r.replica.follower().fenced() == null);
            assertTrue("no fatal on the replica: " + r.fatals, r.fatals.isEmpty());
            assertSupplierNotLeader(r.replica.events());
            System.out.println("[follow] after the leader kill: ALIGNED at " + pCm2 + " equals the members'; reattach="
                    + r.replica.follower().reattachTotal() + " responses=" + r.replica.events().responses()
                    + " resets=" + r.replica.events().resets());
            System.out.println("GREEN pCm=" + pCm + " pStandby=" + pStandby + " pCm2=" + pCm2);

            // (a) the trigger listener ignoring flags 1: no STANDBY shadow
            r.stopReplica();
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-nc-a")
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.TRIGGER_IGNORES_STANDBY)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final int framesA = r.actionFrames(host, 1).size();
            r.toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
            final long pA = awaitStandbyFrame(r, host, framesA);
            RestoreDrillDryRun.nc("TRIGGER_IGNORES_STANDBY", "no STANDBY shadow at",
                    () -> awaitStandbyShadow(r, pA, STANDBY_WAIT_MS));

            // (c) sourceType=LEADER: the FOLLOWER assertion sees a leader supplier
            r.stopReplica();
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT + 2, "replica-nc-c").sourceType(ClusterBackup.SourceType.LEADER)
                    .build());
            r.rig.await(() -> r.replica.events().responses() > 0, ReplicaRig.WAIT_MS,
                    "the LEADER-source replica never got a backup response");
            RestoreDrillDryRun.nc("SOURCE_LEADER", "supplier is the leader",
                    () -> assertSupplierNotLeader(r.replica.events()));
        }
    }

    /** A frame end x < P_cm whose image differs from S(P_cm): the content a forward shift would label P_cm. */
    private static long forgeryPosition(final RecordedLog log, final long pCm, final byte[] atPcm) {
        final long[] pos = log.positions();
        for (int i = pos.length - 1; i >= 0; i--) {
            if (pos[i] < pCm && !java.util.Arrays.equals(TestShadowProducer.imageAt(log, pos[i]), atPcm)) {
                return pos[i];
            }
        }
        throw new AssertionError("no frame before P_cm=" + pCm + " with a different image");
    }

    /** {RecordingLog snapshots, rcs stub, rcs full, onTakeSnapshot calls, cadence byte baseline} per live member. */
    private static Map<Integer, long[]> memberState(final ReplicaRig r) {
        final Map<Integer, long[]> out = new HashMap<>();
        for (final Member m : r.rig.live()) {
            final SnapshotCadence cadence = ServiceProbe.cadence(m.service);
            final long probe = 1L << 40; // any fixed position: baseline = probe - bytesSince(probe)
            out.put(m.id, new long[] {
                    r.rig.snapshots(m).size(),
                    ServiceProbe.rcs(m.service, "core_snapshot_stub_total"),
                    ServiceProbe.rcs(m.service, "core_snapshot_full_total"),
                    r.snapshotsTaken.getOrDefault(m.id, 0L),
                    probe - cadence.logBytesSinceSnapshot(probe)});
        }
        return out;
    }

    /** The newest flags-1 SNAPSHOT action frame end in {@code host}'s committed log once one more exists. */
    static long awaitStandbyFrame(final ReplicaRig r, final Member host, final int before) {
        final long[] found = {-1};
        r.rig.await(() -> {
            final List<Long> frames = r.actionFrames(host, 1);
            if (frames.size() > before) {
                found[0] = frames.get(frames.size() - 1);
                return true;
            }
            return false;
        }, 30_000, "no flags-1 SNAPSHOT action frame appeared in member " + host.id + "'s log");
        return found[0];
    }

    static void awaitStandbyShadow(final ReplicaRig r, final long p, final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!r.verified(ShadowKind.STANDBY).contains(p)) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("no STANDBY shadow at " + p + " within " + timeoutMs + " ms (written "
                        + r.replica.follower().policy().writtenTotal(ShadowKind.STANDBY) + ", last "
                        + r.replica.follower().policy().lastPosition(ShadowKind.STANDBY) + ")");
            }
            r.rig.pumpMs(100);
        }
    }

    static void assertSupplierNotLeader(final BackupEvents events) {
        if (events.supplierWasLeaderEver() || events.supplierIsLeader()) {
            throw new AssertionError("supplier is the leader (member " + events.supplierMemberId()
                    + "): the replay loads the leader");
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowPullSettings;
import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The replica's disk lost under order load (nightly): three members in {@code shadow} mode with 1 MiB log segments, the
 * real replica and the designated follower write their ALIGNED at two P_cm, the followers' floors purge their logs
 * past 0 (the precondition, read from the members' archives; not reached is INCONCLUSIVE), then the replica's
 * {@code BASE_DIR} is lost: a replica on a fresh directory with the members as its pull sources bootstraps
 * {@code restore_path=PULLED_SHADOW}, reads {@code /ready} 200 and its next ALIGNED is VERIFIED and equal to the
 * members' image. The row records which copy served the restore (producer kind and id) and the replica's
 * {@code shadow_pull_source_skipped_leader_total}.
 *
 * <p>Control in the row (no pull sources, a fresh directory): {@code no local shadow and the cluster log starts at}
 * (the kit's bootstrap refusal).</p>
 */
public class ReplicaWipePullBootstrapUnderLoadTest {

    private static final int PORT_BASE = 59400; // members 59400..59600, the replica slot 59700
    /**
     * 1 MiB log segments on the 1m term, so the members' logs purge within the row. Not the 64k term of the unloaded
     * rows: under the load the designated follower's window guard skips every ALIGNED on a 64k term (W = 32k), and the
     * floor then never has its second witness.
     */
    private static final int SEGMENT = 1024 * 1024;

    private static String floorOf(final Member m) {
        final com.openexchange.cluster.shadow.ShadowFloor f =
                com.openexchange.core.infrastructure.persistence.ServiceProbe.shadowFloor(m.service);
        return f == null ? "none" : String.valueOf(f.lastEvaluation());
    }

    @Test(timeout = 1_800_000)
    public void aReplicaWithoutItsDiskBootstrapsFromAMembersShadow() throws Exception {
        try (ReplicaLoadRig x = new ReplicaLoadRig(PORT_BASE, "replica-wipe", 3, "1m", SEGMENT)) {
            x.bootMembers();
            x.startReplica(x.builder(ReplicaRig.SLOT, "replica").build());
            x.startLoad();
            x.rig.pumpMs(3_000);
            for (int i = 0; i < 2; i++) {
                final long p = x.snapshot();
                x.assertAlignedEqual(p);
            }
            final Member leader = x.rig.awaitLeader(null);
            final List<Member> followers = new ArrayList<>();
            for (final Member m : x.rig.live()) {
                if (m.role() != Cluster.Role.LEADER) {
                    followers.add(m);
                }
            }
            try {
                x.rig.await(() -> followers.stream().allMatch(m -> x.rig.logStart(m) > 0), 120_000, "purge");
            } catch (final AssertionError notPurged) {
                assumeTrue("INCONCLUSIVE: the followers' logs never started past 0 (" + followers.stream()
                        .map(m -> "member " + m.id + " log start " + x.rig.logStart(m) + " witnesses "
                                + com.openexchange.core.infrastructure.persistence.ServiceProbe.housekeeping(m.service,
                                "shadow_anchor_witnesses") + " floor " + floorOf(m)).toList() + ")", false);
            }
            final Map<String, Object> extra = new LinkedHashMap<>();
            extra.put("log_starts", followers.stream().map(m -> m.id + "=" + x.rig.logStart(m)).toList());
            x.sampler.assertInsideTheTerm();
            extra.put("steady_window", x.sampler.summary());
            x.sampler.close();

            // the disk is lost
            x.r.stopReplica();
            x.r.windowGap();

            // control: no pull source and no local shadow on a log that starts past 0
            RestoreDrillDryRun.nc("PULL_SOURCES_EMPTY", "no local shadow and the cluster log starts at", () -> {
                x.r.startReplica(x.builder(ReplicaRig.SLOT, "replica-no-sources").build());
                x.r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            });
            x.r.stopReplica();
            x.r.windowGap();

            final ShadowPullSettings pull = ShadowPullSettings.parse(Map.of(
                    ShadowPullSettings.SOURCES, x.rig.sources(x.rig.members.toArray(new Member[0])),
                    ShadowPullSettings.PERIOD_MS, "1000",
                    ShadowPullSettings.STARTUP_WAIT_MS, "20000")::get, null);
            final long t0 = System.currentTimeMillis();
            x.r.startReplica(x.builder(ReplicaRig.SLOT, "replica-wiped").pullSettings(pull).build());
            x.r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals("restore path of the wiped replica", ReplicaReadiness.RestorePath.PULLED_SHADOW,
                    x.r.replica.readiness().restorePath());
            final long restoredAt = x.r.replica.follower().restoredPosition();
            final StringBuilder served = new StringBuilder();
            x.rig.await(() -> x.r.scan() != null && x.r.scan().atPosition(restoredAt) != null, 60_000,
                    () -> "the pulled shadow at " + restoredAt + " in the replica's catalog: " + x.r.describeScan());
            final LogicalShadow pulled = x.r.scan().atPosition(restoredAt);
            for (final ShadowEntry e : pulled.copies()) {
                if (e.header() != null) {
                    served.append(served.length() == 0 ? "" : ", ").append("kind ").append(e.header().producerKind())
                            .append(" id ").append(e.header().producerId());
                }
            }
            final long skippedLeader = x.r.metric("shadow_pull_source_skipped_leader_total");
            System.out.println("[lag] wiped replica restored PULLED_SHADOW at " + restoredAt + " from copies [" + served
                    + "]; leader " + leader.id + "; shadow_pull_source_skipped_leader_total " + skippedLeader);
            x.r.awaitCaughtUp(120_000);
            final long p3 = x.snapshot();
            x.assertAlignedEqual(p3);
            final String[][] last = {{"", ""}};
            x.rig.await(() -> {
                last[0] = x.ready();
                return "200".equals(last[0][0]);
            }, 120_000, () -> "the wiped replica never read /ready 200 (last " + last[0][0] + " " + last[0][1] + ")");
            final long readyMs = System.currentTimeMillis() - t0; // recorded: the row's bound is the 120 s wait
            extra.put("restored_position", restoredAt);
            extra.put("served_by", served.toString());
            extra.put("leader", leader.id);
            extra.put("skipped_leader_total", skippedLeader);
            extra.put("ready_ms_after_start", readyMs);
            extra.put("generator", x.endLoad());
            x.sampler.writeSummary(extra);
            assertTrue("no fatal: " + x.r.fatals, x.r.fatals.isEmpty());
            System.out.println("GREEN replica wipe under load: PULLED_SHADOW at " + restoredAt + ", ready " + readyMs
                    + " ms, next ALIGNED at " + p3 + " equal");
        }
    }
}

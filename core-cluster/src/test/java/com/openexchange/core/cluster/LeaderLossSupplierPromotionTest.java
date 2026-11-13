// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.BackupEvents;
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
 * Leader loss under order load (nightly), the two named states: the leader is stopped and either (i) the replica's
 * supplier won the election: the re-validation at the next backup query moves the backup out of BACKING_UP ({@code
 * shadow_replica_backup_state} RESET_BACKUP or BACKUP_QUERY) within one query interval plus {@code
 * core.lag.promotion.slack.ms}, and the supplier is a follower again ({@code supplier_is_leader} 0, a new supplier id)
 * within the query interval plus the cool-down plus the slack (recorded, not asserted: whether {@code
 * supplier_is_leader} ever read 1, since with the FOLLOWER source the next response already names a follower, and
 * {@code shadow_replica_backup_resets_total}, which counts possible-failure events only); or (ii) the other follower
 * won: {@code supplier_is_leader} stays 0, the resets do not move, the supplier is unchanged and the applied position
 * passes the commit position at the stop (the lag continues). The row asserts whichever occurred; it seeks (i) by
 * restarting the stopped leader and stopping the new one, at most {@code core.lag.promotion.attempts} times. No (i) in
 * every attempt is INCONCLUSIVE (a skip with {@code promotion not observed in <n> attempts}), never a pass. The lag of
 * each attempt stays inside the term unless the backup went through RESET_BACKUP in it (a reset waits out the cool-down
 * while the load grows the log: recorded with the attempt), and the P_cm after the attempts has its ALIGNED, VERIFIED
 * and equal to the members' image.
 *
 * <p>The replica runs a 13 000 ms backup query interval (the shortest above window + 1000 that logs no CRITICAL line).
 * Control {@code -Dcore.lag.nc=QUERY_INTERVAL_DEFAULT}: the default 60 000 ms interval, the window still the row's;
 * when (i) occurs it fails with {@code promotion re-validation outside <n> s}.</p>
 */
public class LeaderLossSupplierPromotionTest {

    private static final int PORT_BASE = 58200; // members 58200..58400, the replica slot 58500
    private static final long QUERY_INTERVAL_MS = 13_000;
    private static final long SLACK_MS = Long.getLong("core.lag.promotion.slack.ms", 5_000);
    private static final boolean CONTROL = "QUERY_INTERVAL_DEFAULT".equals(System.getProperty("core.lag.nc", ""));

    @Test(timeout = 1_800_000)
    public void aPromotedSupplierIsRevalidatedAndAnUnchangedOneKeepsFollowing() throws Exception {
        try (ReplicaLoadRig x = new ReplicaLoadRig(PORT_BASE, "leader-loss", 3, "1m", 0)) {
            x.bootMembers();
            final long queryMs = CONTROL
                    ? com.openexchange.cluster.replica.ReplicaConfig.DEFAULT_BACKUP_QUERY_INTERVAL_MS : QUERY_INTERVAL_MS;
            x.startReplica(x.builder(ReplicaRig.SLOT, "replica").backupQueryIntervalMs(queryMs).build());
            final long cooldownMs = x.r.replica.config().backupCooldownMs();
            final long revalidateMs = QUERY_INTERVAL_MS + SLACK_MS;
            final long backToFollowerMs = QUERY_INTERVAL_MS + cooldownMs + SLACK_MS;
            System.out.println("[lag] leader loss: query interval " + queryMs + " ms (row window from "
                    + QUERY_INTERVAL_MS + " ms), cool-down " + cooldownMs + " ms, re-validation window " + revalidateMs
                    + " ms, back to a follower within " + backToFollowerMs + " ms" + (CONTROL ? " (control)" : ""));
            x.startLoad();
            x.rig.pumpMs(5_000);

            final BackupEvents events = x.r.replica.events();
            final boolean[] sawSupplierLeader = {false};
            final java.util.Set<Long> states = new java.util.TreeSet<>();
            x.rig.duringAwait.add(() -> {
                if (x.r.replica != null) {
                    sawSupplierLeader[0] |= x.r.replica.events().supplierIsLeader();
                    states.add(x.r.metric("shadow_replica_backup_state"));
                }
            });
            final List<String> outcomes = new ArrayList<>();
            boolean promoted = false;
            Member down = null;
            for (int attempt = 1; attempt <= ReplicaLoadRig.PROMOTION_ATTEMPTS && !promoted; attempt++) {
                if (down != null) {
                    rejoin(x, down);
                    down = null;
                }
                final Member leader = x.rig.awaitLeader(null);
                final int supplier = events.supplierMemberId();
                assertTrue("the supplier " + supplier + " is a follower before attempt " + attempt + " (leader "
                        + leader.id + ")", supplier >= 0 && supplier != leader.id);
                final long resets0 = events.resets();
                final long commitAtKill = x.commit();
                sawSupplierLeader[0] = false;
                states.clear();
                x.sampler.mark();
                final long t0 = System.currentTimeMillis();
                x.kill(leader);
                down = leader;
                final Member next = x.rig.awaitLeader(leader);
                if (next.id == supplier) {
                    // (i) the supplier won: re-validated at the next query (the backup leaves BACKING_UP:
                    // RESET_BACKUP 6 or BACKUP_QUERY 0)
                    try {
                        x.rig.await(() -> states.contains(6L) || states.contains(0L), revalidateMs, "re-validation");
                    } catch (final AssertionError late) {
                        throw new AssertionError("promotion re-validation outside " + revalidateMs / 1000.0 + " s:"
                                + " backup states " + states + ", backup_resets_total " + resets0 + " -> "
                                + events.resets() + ", supplier " + events.supplierMemberId() + " is_leader "
                                + events.supplierIsLeader());
                    }
                    final long resetMs = System.currentTimeMillis() - t0;
                    x.rig.await(() -> !events.supplierIsLeader() && events.supplierMemberId() >= 0
                                    && events.supplierMemberId() != next.id
                                    && x.r.replica.follower().appliedPosition() > commitAtKill,
                            Math.max(0, t0 + backToFollowerMs - System.currentTimeMillis()),
                            () -> "the promoted supplier " + next.id + " was not replaced by a follower within "
                                    + backToFollowerMs + " ms: supplier " + events.supplierMemberId() + " is_leader "
                                    + events.supplierIsLeader() + " " + x.r.probe().detail());
                    // recorded, not asserted (measured): with REPLICA_BACKUP_SOURCE=FOLLOWER the backup's next response
                    // already names a follower, so the gauge of the last response may never read 1
                    final boolean sawOne = sawSupplierLeader[0] || events.supplierWasLeaderEver();
                    promoted = true;
                    outcomes.add("attempt " + attempt + ": (i) supplier " + supplier + " promoted, re-validated after "
                            + resetMs + " ms, new supplier " + events.supplierMemberId() + " after "
                            + (System.currentTimeMillis() - t0) + " ms, backup states " + states
                            + ", backup_resets_total " + resets0 + " -> " + events.resets()
                            + ", supplier_is_leader read 1: " + sawOne);
                } else {
                    // (ii) the other follower won: the supplier stays a follower, nothing resets, the lag continues
                    x.rig.await(() -> x.r.replica.follower().appliedPosition() > commitAtKill, backToFollowerMs,
                            () -> "the replica stopped following after member " + next.id + " won (supplier "
                                    + supplier + "): applied " + x.r.replica.follower().appliedPosition()
                                    + " commit at the stop " + commitAtKill);
                    x.rig.pumpMs(Math.max(0, t0 + revalidateMs - System.currentTimeMillis()));
                    assertEquals("supplier_is_leader stays 0 when the other follower won", false,
                            sawSupplierLeader[0]);
                    assertEquals("backup_resets_total unchanged when the other follower won", resets0,
                            events.resets());
                    assertEquals("the supplier is unchanged", supplier, events.supplierMemberId());
                    outcomes.add("attempt " + attempt + ": (ii) member " + next.id + " won, supplier " + supplier
                            + " kept, resets unchanged " + resets0 + ", backup states " + states);
                }
                // the lag of the attempt: inside the term unless the backup reset (a reset costs the cool-down before
                // the next query, and the lag grows with the load meanwhile: recorded, bounded by the recovery)
                final long attemptMax = x.sampler.windowMax;
                final boolean reset = states.contains(6L);
                outcomes.set(outcomes.size() - 1, outcomes.get(outcomes.size() - 1) + ", max commit-applied "
                        + attemptMax + " B" + (reset ? " through a backup reset" : ""));
                if (!reset && attemptMax > x.termBytes) {
                    throw new AssertionError("max commit-applied " + attemptMax + " B above the term " + x.termBytes
                            + " B in attempt " + attempt + " without a backup reset");
                }
                System.out.println("[lag] " + outcomes.get(outcomes.size() - 1));
            }
            if (down != null) {
                rejoin(x, down);
            }
            x.sampler.assertObserved();
            final long p = x.snapshot();
            x.assertAlignedEqual(p);
            final Map<String, Object> extra = new LinkedHashMap<>();
            extra.put("query_interval_ms", queryMs);
            extra.put("outcomes", outcomes);
            extra.put("generator", x.endLoad());
            x.sampler.writeSummary(extra);
            assertTrue("no fatal: " + x.r.fatals, x.r.fatals.isEmpty());
            System.out.println("[lag] leader loss outcomes " + outcomes);
            assumeTrue("INCONCLUSIVE: promotion not observed in " + ReplicaLoadRig.PROMOTION_ATTEMPTS + " attempts "
                    + outcomes, promoted);
            System.out.println("GREEN leader loss under load: " + outcomes);
        }
    }

    /** Restarts the stopped member and waits until it follows again. */
    private static void rejoin(final ReplicaLoadRig x, final Member m) {
        final long commitAtRestart = x.commit();
        x.restart(m);
        x.rig.await(() -> m.cfg.consensusModuleContext().commitPositionCounter().get() >= commitAtRestart
                && m.role() == Cluster.Role.FOLLOWER, 120_000, "member " + m.id + " never rejoined");
    }
}

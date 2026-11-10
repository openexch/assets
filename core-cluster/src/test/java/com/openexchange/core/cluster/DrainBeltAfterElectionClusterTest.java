// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * The log never goes idle while there is a leader. The core leader arms its 10 ms
 * egress drain belt when it gains leadership, not at the next ingress message, so after an election with connected
 * but quiet sessions the new leader's commit position advances within 1 s (cluster time keeps moving, which is what
 * the replica's time-lag readiness input reads). Before the fix the belt was re-armed only by the next
 * {@code onSessionMessage}, so a quiet cluster's log stood still after every election.
 *
 * <p>E7c guard: any later change to the belt policy must keep a liveness tick of at most 1 s; this test is that
 * guard.</p>
 */
public class DrainBeltAfterElectionClusterTest {

    private static final int PORT_BASE = 34000;
    /** E7c: the liveness tick bound. */
    static final long TICK_BOUND_MS = 1_000;

    @Test(timeout = 300_000)
    public void theCommitPositionAdvancesWithinOneSecondAfterALeaderChangeWithQuietSessions() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "replica-drain-belt")) {
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member host = rig.followerOtherThan(leader, null);
            final Client admin = rig.connect(SessionClass.ADMIN, host);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, host);
            tool.deposits(3);

            // precondition: an ingress message armed the belt on the first leader, so the quiet log ticks
            final String before = advance(rig, leader, "before the leader change");
            System.out.println("[E7a] first leader " + leader.id + ": " + before);

            rig.kill(leader);
            final Member next = rig.awaitLeader(leader);
            // the election completes (NEW_LEADERSHIP_TERM appended and committed); no session sends anything
            rig.pumpMs(500);
            final String after = advance(rig, next, "after the leader change with quiet sessions");
            System.out.println("[E7a] new leader " + next.id + ": " + after);
            assertFalse("the sessions stayed connected (quiet, not closed)", tool.session.client.isClosed());
            System.out.println("GREEN E7a belt after election: " + after);
        }
    }

    /** Polls the leader's commit position for {@link #TICK_BOUND_MS}; fails with the named text if it never moves. */
    private static String advance(final ShadowClusterRig rig, final Member leader, final String when) {
        final long p0 = commit(leader);
        final long t0 = System.currentTimeMillis();
        final long[] p = {p0};
        final long[] at = {-1};
        try {
            rig.await(() -> {
                p[0] = commit(leader);
                if (p[0] > p0) {
                    at[0] = System.currentTimeMillis() - t0;
                    return true;
                }
                return System.currentTimeMillis() - t0 > TICK_BOUND_MS;
            }, TICK_BOUND_MS + 5_000, "commit poll");
        } catch (final AssertionError timeout) {
            // the poll above ends itself at the bound; a rig timeout is the same verdict
        }
        if (at[0] < 0) {
            throw new AssertionError("E7a: the commit position did not advance within " + TICK_BOUND_MS + " ms "
                    + when + " (member " + leader.id + " commit " + p0 + " -> " + p[0] + "): the log is idle while"
                    + " there is a leader");
        }
        return "commit " + p0 + " -> " + p[0] + " within " + at[0] + " ms";
    }

    private static long commit(final Member m) {
        return m.cfg.consensusModuleContext().commitPositionCounter().get();
    }
}

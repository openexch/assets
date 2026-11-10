// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Money path, same shape as the drain belt: the deadline sweep is re-armed after an election. With
 * GTD deadlines pending and connected but quiet sessions, the new leader's sweep fires within its interval
 * ({@code SWEEP_PERIOD_MS}, 100 ms) after the leader change, instead of waiting for the next ingress message. The
 * NewLeadershipTermEvent is the first legal callback after the gain (scheduling is refused inside onRoleChange);
 * {@code armSweepIfNeeded()} is a no-op when the index is empty. No new timer site ({@code scheduleTimer(} stays at 2).
 */
public class SweepAfterElectionClusterTest {

    private static final int PORT_BASE = 36000;
    /** {@code AssetsClusteredService.SWEEP_PERIOD_MS}. */
    static final long SWEEP_PERIOD_MS = 100;
    /** The interval plus scheduling slack: the sweep must fire within this after the new term is applied. */
    static final long BOUND_MS = 10 * SWEEP_PERIOD_MS;

    /**
     * The gateway client's new-leader timeout. Aeron's default is 2 x the members' leader heartbeat
     * timeout (the rig's 1 s, so 2 s), and an election after the kill takes 2 038..2 320 ms here
     * (measured before the fix): a client whose egress image closed at the kill timed out before the
     * NewLeaderEvent and closed. A test setting, not a product defect: the earlier precondition retry is removed.
     */
    static final long CLIENT_NEW_LEADER_TIMEOUT_MS = 10_000;

    @Test(timeout = 900_000)
    public void theDeadlineSweepFiresWithinItsIntervalAfterALeaderChangeWithQuietSessions() throws Exception {
        final String dropped = attempt(1);
        if (dropped != null) {
            throw new AssertionError("sweep precondition: the gateway session did not stay connected through the"
                    + " election: " + dropped);
        }
    }

    /** One scenario; null when the property was measured with the precondition held, else why it did not hold. */
    private String attempt(final int attempt) throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-sweep-" + attempt)) {
            r.rig.clientNewLeaderTimeoutNs = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    CLIENT_NEW_LEADER_TIMEOUT_MS);
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 2); // GTD orders, expiring in a day: the deadline index is non-empty
            final long before = leader.service.sweepTimerFires();
            r.rig.pumpMs(500);
            assertTrue("precondition: the first leader's sweep ticks (" + before + " -> "
                    + leader.service.sweepTimerFires() + ")", leader.service.sweepTimerFires() > before);

            final long killAt = System.nanoTime();
            final long[] closedAtMs = {-1};
            final long[] electedAtMs = {-1};
            final Member[] found = new Member[1];
            r.rig.kill(leader);
            // evidence: when the gateway client closes relative to the election
            r.rig.await(() -> {
                final long ms = (System.nanoTime() - killAt) / 1_000_000;
                if (closedAtMs[0] < 0 && r.gw.session.client.isClosed()) {
                    closedAtMs[0] = ms;
                }
                if (found[0] == null) {
                    for (final Member m : r.rig.members) {
                        if (!m.killed && m.id != leader.id
                                && m.role() == io.aeron.cluster.service.Cluster.Role.LEADER) {
                            found[0] = m;
                            electedAtMs[0] = ms;
                        }
                    }
                }
                return found[0] != null;
            }, 60_000, "no leader other than member " + leader.id);
            final Member next = found[0];
            // the election completes; no session sends anything from here on
            r.rig.pumpMs(500);
            final long f0 = next.service.sweepTimerFires();
            final long t0 = System.currentTimeMillis();
            final long[] at = {-1};
            try {
                r.rig.await(() -> {
                    if (next.service.sweepTimerFires() > f0) {
                        at[0] = System.currentTimeMillis() - t0;
                        return true;
                    }
                    return System.currentTimeMillis() - t0 > BOUND_MS;
                }, BOUND_MS + 5_000, "sweep poll");
            } catch (final AssertionError timeout) {
                // the poll ends itself at the bound
            }
            if (r.gw.session.client.isClosed()) {
                return "the gateway session closed during the election (sweep fired after " + at[0] + " ms; client"
                        + " closed " + (closedAtMs[0] >= 0 ? closedAtMs[0] + " ms" : "after the leader was found,")
                        + " after the kill, new leader " + next.id + " seen " + electedAtMs[0] + " ms after it; client"
                        + " newLeaderTimeoutNs=" + r.gw.session.client.context().newLeaderTimeoutNs() / 1_000_000
                        + " ms, messageTimeoutNs=" + r.gw.session.client.context().messageTimeoutNs() / 1_000_000
                        + " ms)";
            }
            if (at[0] < 0) {
                throw new AssertionError("the deadline sweep did not fire within " + BOUND_MS + " ms after a"
                        + " leader change with quiet sessions (new leader " + next.id + " fires " + f0 + " -> "
                        + next.service.sweepTimerFires() + "): GTD expiry waits for the next ingress");
            }
            assertFalse("the sessions stayed connected (quiet, not closed)", r.gw.session.client.isClosed());
            System.out.println("GREEN sweep after election: new leader " + next.id + " fired within " + at[0]
                    + " ms (fires " + f0 + " -> " + next.service.sweepTimerFires() + "), attempt " + attempt
                    + "; new leader seen " + electedAtMs[0] + " ms after the kill, client newLeaderTimeoutNs="
                    + r.gw.session.client.context().newLeaderTimeoutNs() / 1_000_000 + " ms");
            return null;
        }
    }
}

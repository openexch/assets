// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.domain.Asset;
import com.openexchange.core.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The rig's deposits under an election (three members, the clients on a follower's driver). A first batch is acked;
 * a second batch is offered and the leader is killed before any of its answers is awaited, so the tail the old leader
 * had not answered is dropped or committed without an ack. The rig re-offers that tail with the same requestIds once
 * the session follows the new leader and no ack came for the election window (again after each further window, within
 * {@code core.rig.reoffer.total.ms}); the core answers what it already applied with its stored decision and credits
 * nothing.
 * Then ten applied
 * deposits are offered again on purpose (an ack the client never saw): ten replay answers, no credit. Every user's
 * balance is exactly the deposits it was sent.
 *
 * <p>Control {@code -Dcore.rig.nc=REOFFER_WITHOUT_REQUEST_ID}: the same re-offers without a requestId credit again and
 * the balance check fails with {@code a re-offered deposit credited twice}. The rig's former helper shape (no
 * requestId, no re-offer) is {@code -Dcore.rig.nc=DEPOSITS_WITHOUT_IDENTITY}: the second batch strands and the ack wait
 * times out with {@code an election drops uncommitted ingress}.</p>
 */
public class ShadowClusterRigDepositIdentityTest {

    private static final int PORT_BASE = 41_000;

    @Test
    public void aLeaderKillBetweenTwoDepositBatchesCreditsEachDepositOnce() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "rig-deposit-identity")) {
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member host = rig.followerOtherThan(leader, null);
            final Client admin = rig.connect(SessionClass.ADMIN, host);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, host).withIdentity();

            tool.deposits(50);
            final long t0 = System.nanoTime();
            tool.deposits(300, () -> rig.kill(leader));
            final long recoveredMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.println("[deposit-identity] second batch answered " + recoveredMs + " ms after its offers; tail"
                    + " re-offered " + tool.reoffered + "; roles " + rig.roles());
            final int electionReoffers = tool.reoffered;

            final int replays = tool.reofferLast(10);
            tool.deposits(5); // users 1..5: the last ack per user carries its balance

            final Map<Long, Long> expected = new HashMap<>();
            for (final long[] d : tool.depositLog) {
                expected.merge(d[1], ShadowClusterRig.ONE, Long::sum);
            }
            for (long user = 1; user <= 5; user++) {
                final Long available = tool.in.availableByUserAsset.get(user * 64 + Asset.USD.id());
                final long want = expected.get(user);
                if (available == null || available != want) {
                    throw new AssertionError("a re-offered deposit credited twice: user " + user + " available "
                            + available + ", expected " + want + " (" + want / ShadowClusterRig.ONE + " deposits)");
                }
            }
            assertEquals("every re-offered applied deposit answered with the stored decision", 10, replays);
            assertTrue("the election left a tail to re-offer (re-offered " + electionReoffers + ")",
                    electionReoffers > 0);
            System.out.println("GREEN deposit identity: " + tool.depositLog.size() + " deposits, " + electionReoffers
                    + " re-offered after the election, 10 replays, every balance exact");
        }
    }
}

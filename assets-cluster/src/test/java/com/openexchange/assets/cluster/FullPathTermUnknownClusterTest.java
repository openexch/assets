// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Client;
import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 6.5 / review N2, decision D3: a follower that restored a FULL image keeps {@code leadershipTermId = -1}
 * until the next term event (the pre-existing full-path hole). In {@code RCS_MODE=shadow} such a node cannot write a
 * stub (rule S6 refuses an unknown term), so it writes a FULL image, logs CRITICAL and counts
 * {@code rcs_snapshot_full_term_unknown_total}; a full image is always restorable, so its next restart restores it.
 *
 * <p>Story (3 members, one JVM): full snapshots in {@code off} mode; a follower restarted in {@code shadow} mode
 * restores the full group and joins the running term (no term event after P_cm), so its term id is -1; the next
 * cluster snapshot is FULL on that follower with the CRITICAL line and the counter at 1; a second restart restores that
 * group and the image equals the leader's.</p>
 */
public class FullPathTermUnknownClusterTest {

    @Test
    public void shadowModeWithAnUnknownTermWritesACountedFullImage() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(27300, "rcs-s4-d3-term-unknown")) {
            for (final Member m : rig.members) {
                m.settings = StubStory.off();
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(100);
            final long full = rig.snapshotAll();

            rig.kill(f);
            tool.deposits(20);
            f.settings = StubStory.shadow();
            rig.restart(f);
            rig.awaitReady(f, 120_000);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertEquals("precondition (the hole N2): the FULL-path restore at " + full + " left the term unknown",
                    -1L, ServiceProbe.leadershipTermId(f.service));

            rig.errTee.clear();
            tool.deposits(20);
            final long pCm = rig.snapshotAll();
            final String critical = StubStory.line(rig.errTee.lines,
                    "RCS_MODE=shadow but this node's leadershipTermId is unknown");
            assertTrue("the CRITICAL line for the full image at " + pCm + ": " + rig.errTee.lines, critical != null);
            assertEquals("counted once", 1, ServiceProbe.rcs(f.service, "rcs_snapshot_full_term_unknown_total"));
            assertEquals("no stub was written", 0, ServiceProbe.rcs(f.service, "rcs_snapshot_stub_total"));
            assertEquals("the off-mode leader never counts it", 0,
                    ServiceProbe.rcs(leader.service, "rcs_snapshot_full_term_unknown_total"));

            rig.kill(f);
            tool.deposits(10);
            rig.restart(f);
            rig.awaitReady(f, 120_000);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertTrue(f.halts.toString(), f.halts.isEmpty());
            System.out.println("GREEN D3 follower " + f.id + ": term -1 after the FULL restore at " + full
                    + ", shadow-mode snapshot at " + pCm + " written FULL and counted (" + critical
                    + "), restored from it with an equal image");
        }
    }
}

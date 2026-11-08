// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Client;
import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 9.2 / C9, decision D4: the one purge owner per archive ({@code ShadowRetention}) is released when the member
 * shuts down ({@code onTerminate} closes the housekeeping), so a member restarted in the SAME JVM registers its archive
 * again. Three members in one JVM, each archive with its own {@code aeron.archive.id} (the rig, D4): all three hold an
 * owner at once; a follower is stopped in process and restarted on the same directories and archive id and holds an
 * owner again. The in-process stop closes the container, which runs {@code onTerminate}: this row proves the CLOSE path
 * of the owner release, not a crash (review NOTE-1). After a real crash the JVM is gone and the owner set with it (the
 * owner registry is per process); real kill -9 and restart is covered by the child-JVM rows of
 * {@code ShadowRestoreExternalDriverProcessTest}.
 * The two-owners-at-once refusal over ONE archive stays in the kit ({@code ShadowRetentionTest}).
 */
public class PurgeOwnerRestartClusterTest {

    private static final long OWNER_WAIT_MS = 60_000;

    private static void awaitOwner(final ShadowClusterRig rig, final Member m, final String when) {
        rig.await(() -> ServiceProbe.ownsPurge(m.service), OWNER_WAIT_MS,
                () -> "member " + m.id + " has no purge owner " + when + " (archive id " + (rig.archiveIdBase + m.id)
                        + "): a released owner was not re-registered");
    }

    @Test
    public void aMemberRestartedInTheSameJvmGetsAPurgeOwnerAgain() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(27200, "rcs-s4-d4-owner")) {
            for (final Member m : rig.members) {
                m.settings = StubStory.shadow();
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(50);
            for (final Member m : rig.members) {
                awaitOwner(rig, m, "after ADOPT (three archives, three ids)");
            }
            final Object before = f.service;
            rig.kill(f);
            final Member killed = f;
            rig.await(() -> !ServiceProbe.ownsPurge(killed.service), OWNER_WAIT_MS,
                    "the stopped member never released its purge owner (onTerminate closes the housekeeping)");
            rig.restart(f);
            assertNotSame(before, f.service);
            rig.awaitReady(f, 120_000);
            awaitOwner(rig, f, "after a same-JVM restart");
            assertTrue(ServiceProbe.ownsPurge(leader.service));
            assertTrue(ServiceProbe.ownsPurge(g.service));
            System.out.println("GREEN D4 member " + f.id + " (archive id " + (rig.archiveIdBase + f.id)
                    + ") re-registered its purge owner after a same-JVM restart; members " + leader.id + "," + g.id
                    + " kept theirs");
        }
    }
}

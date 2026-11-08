// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 contract 11.5 (MF-2, D16): stub mode from genesis, no shadow ever written, a snapshot at P_cm, a cold restart:
 * the local log is complete from 0, so virtual genesis is taken, loud (the CRITICAL line, {@code
 * rcs_virtual_genesis_total == 1}), and the image equals the pre-restart image. Single member, embedded driver.
 *
 * <p>Control: the 7.1 precondition check off and the log pruned below P_cm by a forced {@code purgeSegments}. With the
 * check ON the 7.3 halt appears ({@code FATAL RCS: stub at P_cm}); with it OFF the node runs a virtual genesis on a
 * pruned log, which the oracle names: {@code virtual genesis taken on a pruned log}.</p>
 */
public class StubNoShadowCompleteLogTest {

    @Test
    public void aStubWithNoShadowOnACompleteLogTakesVirtualGenesisLoudly() throws Exception {
        try (StubStory s = new StubStory(25800, "rcs-s4-11.5-vg", StubStory.shadow()).start(true)) {
            s.deposits(400);
            final long pCm = s.snapshot();
            assertTrue("a stub: " + s.rig.tee.all("kind="), !s.rig.tee.all("kind=stub").isEmpty());
            s.deposits(60);
            assertEquals("the log is complete from 0", 0, s.rig.logStart(s.m));
            final byte[] pre = s.quiesce();
            s.kill();
            s.restart();
            final long readyMs = s.awaitReady();
            s.awaitImage(pre, "after virtual genesis");
            final String critical = StubStory.line(s.rig.errTee.lines, "RCS VIRTUAL GENESIS: no shadow for stub at P_cm="
                    + pCm + ", replaying the complete local log from 0");
            assertNotNull("the CRITICAL line: " + s.rig.errTee.lines, critical);
            assertTrue(critical, critical.contains("/ready will be late"));
            assertEquals(1, s.counter("rcs_virtual_genesis_total"));
            assertEquals("rcs_restore_path VIRTUAL_GENESIS", 5, s.counter("rcs_restore_path"));
            assertTrue(s.pathLine(), s.pathLine().contains("restored path=VIRTUAL_GENESIS P_s=0 P_cm=" + pCm));
            System.out.println("GREEN 11.5 virtual genesis: " + critical + "; " + s.pathLine() + " readyMs=" + readyMs);
        }
    }

    @Test
    public void controlThePreconditionOffOnAPrunedLog() throws Exception {
        try (StubStory s = new StubStory(25900, "rcs-s4-11.5-pruned", StubStory.shadow()).start(true)) {
            s.deposits(900);
            final long pCm = s.snapshot();
            final long boundary = pCm / StubStory.SEGMENT * StubStory.SEGMENT;
            assertTrue("P_cm past the first segment", boundary > 0);
            final long start = s.rig.purgeLogTo(s.m, boundary);
            assertEquals(boundary, start);
            s.quiesce();
            s.kill();
            s.restart();
            final String halt = s.awaitHalt("FATAL RCS: stub at P_cm=" + pCm);
            System.out.println("GREEN 11.5 pruned log, precondition on -> " + halt);
            assertEquals("never genesis", 0, s.counter("rcs_virtual_genesis_total"));

            s.kill();
            s.restart("virtualGenesisPreconditionFaked");
            s.rig.await(() -> !s.m.halts.isEmpty(), 60_000, "the faked virtual genesis must stop");
            RcsDryRun.nc("11.5-precondition-off", "virtual genesis taken on a pruned log", () -> {
                if (s.counter("rcs_virtual_genesis_total") > 0) {
                    throw new AssertionError("virtual genesis taken on a pruned log: start=" + start + " P_cm=" + pCm
                            + "; then " + StubStory.line(s.rig.errTee.lines, "FATAL RCS"));
                }
            });
        }
    }
}

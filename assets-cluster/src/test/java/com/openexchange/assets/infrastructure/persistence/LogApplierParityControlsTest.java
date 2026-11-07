// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ApplierArm;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.Probe;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.Kind;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.LogStep;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * RCS S1 review (added by the independent verifier): two more negative controls on the parity comparator. The
 * slice's skip-open control proves the comparator sees an EVENT divergence. These prove it also sees (a) a divergence
 * that is only in the stamped log position (same events, same image), which is the whole point of the S1 position
 * rule and of the stub cluster's constant 42, and (b) a missing SessionClose, which is the RF-1 defect class. Each
 * control must first diverge exactly where the mutation first matters and nowhere earlier.
 */
public class LogApplierParityControlsTest {

    private static final long SEED = 0x5EED_0001L;
    private static final long TAIL_SESSION = SessionedLog.sessionOf(SessionClass.ADMIN);

    /** Index of the first step where the two arms' probe strings differ, or -1. */
    private static int firstDivergence(final List<LogStep> log, final ApplierArm reference, final ApplierArm mutant,
                                       final java.util.function.BiFunction<Integer, LogStep, LogStep> mutate) {
        final Probe pr = new Probe(reference);
        final Probe pm = new Probe(mutant);
        int first = -1;
        for (int i = 0; i < log.size(); i++) {
            final LogStep s = log.get(i);
            reference.apply(s);
            final LogStep m = mutate.apply(i, s);
            if (m != null) {
                mutant.apply(m);
            }
            final String expected = pr.step();
            final String got = pm.step();
            if (first < 0 && !expected.equals(got)) {
                first = i;
            }
        }
        return first;
    }

    @Test
    public void aStampOnlyDivergenceIsSeenAtTheFirstMessage() {
        final List<LogStep> log = LogApplierParityTest.parityLog(SEED);
        int firstMessage = -1;
        for (int i = 0; i < log.size() && firstMessage < 0; i++) {
            if (log.get(i).kind() == Kind.MESSAGE) {
                firstMessage = i;
            }
        }
        assertTrue(firstMessage > 0);
        final ApplierArm reference = new ApplierArm();
        final ApplierArm shifted = new ApplierArm();
        // Every message is fed a position 32 bytes past its frame end: events and image stay identical, only the
        // stamps and the engine's log position differ.
        final int first = firstDivergence(log, reference, shifted,
                (i, s) -> s.kind() == Kind.MESSAGE ? s.at(s.logPosition() + 32) : s);
        assertEquals("comparator blind to a stamp-only divergence", firstMessage, first);
        assertEquals("same events: the divergence was position only",
                reference.sink.events().size(), shifted.sink.events().size());
        assertTrue(Arrays.equals(reference.image(), shifted.image()));
        assertNotEquals(reference.engine().getLogPosition(), shifted.engine().getLogPosition());
    }

    @Test
    public void aDroppedCloseIsSeenAtTheFirstFrameAfterIt() {
        final List<LogStep> log = LogApplierParityTest.parityLog(SEED);
        final int tailClose = log.size() - 2;
        assertEquals(Kind.CLOSE, log.get(tailClose).kind());
        assertEquals(TAIL_SESSION, log.get(tailClose).sessionId());
        assertEquals(TAIL_SESSION, log.get(tailClose + 1).sessionId());
        final ApplierArm reference = new ApplierArm();
        final ApplierArm noClose = new ApplierArm();
        final int first = firstDivergence(log, reference, noClose, (i, s) -> i == tailClose ? null : s);
        assertEquals("comparator blind to a missing SessionClose", tailClose + 1, first);
        assertTrue("section 9 keeps the session the close should have dropped",
                noClose.engine().sessions().sessionIds().contains(TAIL_SESSION));
        assertFalse(reference.engine().sessions().sessionIds().contains(TAIL_SESSION));
        assertEquals("the frame after the close was applied instead of refused",
                reference.engine().getUnauthorisedCount() - 1, noClose.engine().getUnauthorisedCount());
        assertFalse(Arrays.equals(reference.image(), noClose.image()));
    }
}

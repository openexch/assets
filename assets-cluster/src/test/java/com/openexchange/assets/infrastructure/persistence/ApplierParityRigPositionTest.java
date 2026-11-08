// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ServiceArm;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.LogStep;
import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * RCS S4 W0, the {@code ApplierParityRig} position-per-step fix of contract 5.4 (review N6). With RF-3 the service's
 * open, close and timer callbacks stamp the frame end they read from {@code cluster.logPosition()}; a rig that serves
 * a constant (it was 42, {@code APR:78}) would make the live arm an oracle for nothing. The rig must serve the step's
 * own frame end on every callback kind, so that once W2 lands the service arm and the frame-id arm are comparable.
 *
 * <p>Expected RED on the base rig: {@code ServiceArm served logPosition 42 for CLOSE, expected <p>}. GREEN after the
 * rig sets {@code cluster.logPosition} from the step before each callback.</p>
 */
public class ApplierParityRigPositionTest {

        static void assertServedPosition(final ServiceArm arm, final LogStep s) {
        final long reads = arm.cluster.positionReads;
        arm.apply(s);
        final boolean reachedService = switch (s.kind()) {
            case OPEN, CLOSE, TIMER -> arm.cluster.positionReads > reads;
            default -> false;
        };
        if (reachedService && arm.cluster.lastPositionRead != s.logPosition()) {
            throw new AssertionError("ServiceArm served logPosition " + arm.cluster.lastPositionRead + " for " + s.kind()
                    + ", expected " + s.logPosition());
        }
    }

    @Test
    public void theServiceArmServesTheStepsFrameEndOnOpenCloseAndTimer() {
        final List<LogStep> steps = SessionedLog.renumber(AssetsReplayParityTest.aeSteps(AssetsReplayParityTest.SEED, 400));
        final ServiceArm arm = new ServiceArm(Cluster.Role.LEADER);
        int open = 0, close = 0, timer = 0;
        for (final LogStep s : steps) {
            final long reads = arm.cluster.positionReads;
            assertServedPosition(arm, s);
            if (arm.cluster.positionReads > reads) {
                switch (s.kind()) {
                    case OPEN -> open++;
                    case CLOSE -> close++;
                    case TIMER -> timer++;
                    default -> { }
                }
            }
        }
        assertTrue("the story must exercise open (" + open + "), close (" + close + ") and timer (" + timer + ") callbacks",
                open > 0 && close > 0 && timer > 0);
        System.out.println("GREEN ApplierParityRig serves the step position: open=" + open + " close=" + close + " timer=" + timer);
    }
}

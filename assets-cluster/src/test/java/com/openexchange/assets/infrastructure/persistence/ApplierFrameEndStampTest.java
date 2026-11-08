// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import io.aeron.cluster.codecs.CloseReason;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * RCS S4 RF-3 (contract 5.1), W2 unit driver: every applier entry that reaches the engine stamps its frame end before
 * it touches the engine; a SessionClose of an unknown id stays a full no-op, stamp included (S2 R5); the drain belt
 * touches nothing and does not stamp. RED on the base (84b088b): open, close and sweep ignored their position.
 */
public class ApplierFrameEndStampTest {

    private static final long ADMIN = SessionedLog.sessionOf(SessionClass.ADMIN);

    @Test
    public void openCloseAndSweepStampTheirFrameEnd() {
        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        applier.onSessionOpen(ADMIN, 640, 1, SessionedLog.principalOf(SessionClass.ADMIN));
        assertEquals("event logPosition 0 != frame end 640 (SessionOpen)", 640, applier.engine().getLogPosition());

        applier.onTimerEvent(AssetsLogApplier.SWEEP_TIMER_BASE, 1280, 2);
        assertEquals("event logPosition != frame end 1280 (sweep TimerEvent)", 1280, applier.engine().getLogPosition());

        applier.onTimerEvent(AssetsLogApplier.EGRESS_DRAIN_TIMER_BASE, 1312, 3);
        assertEquals("the drain belt touches nothing and does not stamp", 1280, applier.engine().getLogPosition());

        applier.onSessionClose(ADMIN + 99, 1344, 4, CloseReason.CLIENT_ACTION);
        assertEquals("an unknown close is a full no-op, stamp included (S2 R5)", 1280,
                applier.engine().getLogPosition());

        applier.onSessionClose(ADMIN, 1376, 5, CloseReason.CLIENT_ACTION);
        assertEquals("event logPosition != frame end 1376 (SessionClose)", 1376, applier.engine().getLogPosition());
    }

    @Test
    public void stampLogPositionSetsTheImagePositionWithoutEndingPristine() {
        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        applier.stampLogPosition(4096);
        assertEquals(4096, applier.engine().getLogPosition());
        applier.genesis(); // still pristine: genesis is legal after a stamp
        applier.stampLogPosition(0);
        assertEquals(0, applier.engine().getLogPosition());
    }
}

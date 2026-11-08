// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowFloor;
import com.openexchange.cluster.shadow.ShadowHeader;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 review FIX-2: the housekeeping reads the money-journal tail only when the floor's table will ask
 * {@code skipSafe}: a STUB newest group and a VERIFIED journal-enabled shadow above P_cm. Every other shape answers
 * before {@code skipSafe} (FULL -> P_cm, UNKNOWN -> 0, nothing above P_cm), so no replay is opened.
 */
public class JournalTailGateTest {

    private static final long P_CM = 800_000;

    private static LogicalShadow verified(final long p, final boolean journal) {
        final ShadowHeader h = new ShadowHeader(ShadowHeader.ENGINE_ASSETS, 0, 1, 1, 1, 1, 7, p, 3, 0, 1_000, journal ? 1 : 0,
                41, 1_500, 0, 4096, 7);
        final ShadowEntry e = new ShadowEntry(1, 10, 0, 2_000, "aeron:ipc", null, h, new byte[0], 0, null,
                ShadowEntry.State.VERIFIED, "", 0);
        return new LogicalShadow(h.engineId(), h.clusterId(), h.genesisId(), p, List.of(e), false);
    }

    /** The gate under test: {@code MemberShadowHousekeeping.needsJournalTail}, or a control's replacement. */
    interface Gate {
        boolean needs(ShadowFloor.NewestImage kind, long pCm, List<LogicalShadow> verified);
    }

    static void check(final Gate gate) {
        final List<LogicalShadow> above = List.of(verified(P_CM + 100_000, true), verified(P_CM, true));
        assertTrue("STUB with a journal shadow above P_cm: the tail is read", gate.needs(ShadowFloor.NewestImage.STUB,
                P_CM, above));
        assertFalse("journal tail read for a FULL newest group: the floor is P_cm before skipSafe",
                gate.needs(ShadowFloor.NewestImage.FULL, P_CM, above));
        assertFalse("journal tail read for an unreadable newest group: the floor is 0 before skipSafe",
                gate.needs(ShadowFloor.NewestImage.UNKNOWN, P_CM, above));
        assertFalse("journal tail read with no snapshot", gate.needs(ShadowFloor.NewestImage.STUB, -1, above));
        assertFalse("journal tail read with nothing above P_cm (EQUAL and TAIL bases need no journal)",
                gate.needs(ShadowFloor.NewestImage.STUB, P_CM, List.of(verified(P_CM, true), verified(P_CM - 1, true))));
        assertFalse("journal tail read for a journal-off shadow above P_cm (skip-safe without the tail, 8.1)",
                gate.needs(ShadowFloor.NewestImage.STUB, P_CM, List.of(verified(P_CM + 1, false))));
        assertFalse("journal tail read with no shadow", gate.needs(ShadowFloor.NewestImage.STUB, P_CM, List.of()));
    }

    @Test
    public void readsOnlyForAStubWithAJournalShadowAboveThePcm() {
        check(MemberShadowHousekeeping::needsJournalTail);
        System.out.println("GREEN FIX-2 journal-tail gate: STUB + VERIFIED journal shadow above P_cm only");
    }

    /** Control: the pre-FIX-2 refresh read the tail on every wake, whatever the floor would ask. */
    @Test
    public void controlAnUngatedRefreshIsFlagged() {
        RcsClosureNeverSchedulesTest.nc("S4-FIX-2 ungated journal-tail refresh",
                "journal tail read for a FULL newest group", () -> check((kind, pCm, verified) -> true));
    }
}

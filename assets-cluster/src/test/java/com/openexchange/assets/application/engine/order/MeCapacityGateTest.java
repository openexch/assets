// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderFaultKind;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G4: the capacity gate (3.5): Schmitt trigger at 80 % / 60 % of the bound capacity, the CAPACITY outcome rule. */
public class MeCapacityGateTest {

    private static final long PX = 100 * ONE;

    private static V3Rig rig(final long capacity) {
        final V3Rig r = V3Rig.openWithCapacity(capacity);
        r.deposit(1, V3Rig.USD, 100_000_000 * ONE);
        return r;
    }

    private static long create(final V3Rig r) {
        return r.create(r.limit(1, 0, PX, ONE));
    }

    private static List<AssetsEngineEvent.AdmissionState> admissions(final V3Rig r) {
        return r.all(AssetsEngineEvent.AdmissionState.class);
    }

    /** A 67 with no entry reporting {@code size} (the clamp and the rule run; nothing else changes). */
    private static void report(final V3Rig r, final long size) {
        final com.openexchange.assets.application.engine.order.MeReleaseView v = r.releaseView();
        v.ledgerSize = size;
        assertTrue(r.apply(v));
    }

    @Test
    public void gateClosesAtEightyPercentBeforeAnyCapacity() {
        final V3Rig r = rig(10);
        for (int i = 1; i <= 7; i++) {
            create(r);
            assertFalse("e = " + i + " < 8", globalHas(r, GateReason.ME_CAPACITY));
        }
        r.mark();
        create(r);
        assertTrue("5 x 8 >= 4 x 10", globalHas(r, GateReason.ME_CAPACITY));
        assertTrue(r.engine.orders().capacityHigh());
        assertEquals("AdmissionState once", 1, admissions(r).size());
        assertEquals("no CAPACITY outcome was needed", 0L, r.engine.orders().capacityOutcomeCount());
        assertEquals(GateState.SUSPENDED, r.engine.gate().state(OrderEntryGate.GLOBAL));
    }

    @Test
    public void capacityAutoReopenKeepsOtherReasons() {
        final V3Rig r = rig(10);
        final long[] h = new long[8];
        for (int i = 0; i < 8; i++) {
            h[i] = create(r);
        }
        assertTrue(globalHas(r, GateReason.ME_CAPACITY));
        // BRIDGE_HALT joins: an entry two ahead latches GAP (the reach-clear later clears the latch, not the bit)
        final long next = r.jseq + 1;
        assertFalse(r.outcomeAt(next + 1, V3Rig.EPOCH, ++r.egress, h[1], 0, CREATE, APPLIED, 1_002, 0, false));
        assertTrue(globalHas(r, GateReason.BRIDGE_HALT));
        r.jseq = next - 1;
        for (int i = 0; i < 8; i++) {
            final MeOutcomeView v = r.outcomeView(h[i], 0, CREATE, APPLIED, 1_001 + i, 0);
            v.ledgerSize = 0; // the ME recorded nothing it keeps (the confirmations are not modelled here)
            assertTrue(r.apply(v));
        }
        r.mark();
        report(r, 0);
        cancel(r, 1, h[0]); // P moves below 60 % at the latest here
        assertFalse("ME_CAPACITY auto-cleared", globalHas(r, GateReason.ME_CAPACITY));
        assertFalse(r.engine.orders().capacityHigh());
        assertTrue("BRIDGE_HALT survives the auto-reopen (S7 D4)", globalHas(r, GateReason.BRIDGE_HALT));
        assertEquals(GateState.SUSPENDED, r.engine.gate().state(OrderEntryGate.GLOBAL));
    }

    @Test
    public void initialCapacityFromFresh() {
        // W6: C comes from the 68 FRESH; unbound (C = 0) never gates
        final V3Rig unbound = V3Rig.unbound();
        assertEquals(0L, unbound.feed().meLedgerCapacity());
        assertFalse(unbound.engine.orders().capacityHigh());
        final V3Rig big = rig(4096);
        for (int i = 0; i < 8; i++) {
            create(big);
        }
        assertFalse("8 of 4096", globalHas(big, GateReason.ME_CAPACITY));
        final V3Rig small = rig(10);
        for (int i = 0; i < 8; i++) {
            create(small);
        }
        assertTrue("8 of 10", globalHas(small, GateReason.ME_CAPACITY));
        assertEquals(10L, small.feed().meLedgerCapacity());
    }

    @Test
    public void thresholdArithmetic() {
        // {C, highest e that does not close, lowest e that closes, highest e that clears, lowest e that keeps it high}
        final long max = (1L << 31) - 1;
        final long[][] rows = {{1, 0, 1, 0, 1}, {5, 3, 4, 3, 4}, {100_000, 79_999, 80_000, 60_000, 60_001},
            {max, 1_717_986_917L, 1_717_986_918L, 1_288_490_188L, 1_288_490_189L}};
        for (final long[] row : rows) {
            final V3Rig r = rig(row[0]);
            final String w = "C = " + row[0];
            report(r, row[1]);
            assertFalse(w + ": e = " + row[1] + " stays open", globalHas(r, GateReason.ME_CAPACITY));
            report(r, row[2]);
            assertTrue(w + ": e = " + row[2] + " closes", globalHas(r, GateReason.ME_CAPACITY));
            report(r, row[4]);
            assertTrue(w + ": e = " + row[4] + " keeps it closed", globalHas(r, GateReason.ME_CAPACITY));
            report(r, row[3]);
            assertFalse(w + ": e = " + row[3] + " reopens", globalHas(r, GateReason.ME_CAPACITY));
            assertEquals(w + ": no link mismatch on legal sizes", 0L, r.engine.orders().meLinkMismatchCount());
        }
    }

    @Test
    public void capacityWhileBitClearFaultsAndNotWhenSet() {
        final V3Rig r = rig(4096);
        final long a = create(r);
        final long b = create(r);
        r.mark();
        assertTrue(r.outcome(a, 0, CREATE, CAPACITY, 0, 0, false));
        final AssetsEngineEvent.OrderFault f = r.first(AssetsEngineEvent.OrderFault.class);
        assertEquals("L 3.7: never silent", OrderFaultKind.ME_CAPACITY_WHILE_OPEN, f.kind());
        assertEquals("amount = e (L := C, P = 2)", 4096L + 2, f.amount());
        assertTrue(globalHas(r, GateReason.ME_CAPACITY));
        r.mark();
        assertTrue(r.outcome(b, 0, CREATE, CAPACITY, 0, 0, false));
        assertEquals("a CAPACITY with the bit set is legal: no fault", 0, r.count(AssetsEngineEvent.OrderFault.class));
        assertEquals(2L, r.engine.orders().capacityOutcomeCount());
        assertEquals(1L, r.engine.orders().capacityWhileOpenCount());
        assertEquals(2L, r.engine.orders().capacityParked());
    }

    @Test
    public void adminOverrideLastsUntilNextCrossing() {
        final V3Rig r = rig(10);
        for (int i = 0; i < 8; i++) {
            create(r);
        }
        assertTrue(globalHas(r, GateReason.ME_CAPACITY));
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        assertFalse("ADMIN OPEN clears the bit", globalHas(r, GateReason.ME_CAPACITY));
        assertTrue("and leaves capacityHigh (D-S8-4)", r.engine.orders().capacityHigh());
        create(r);
        assertFalse("no upward crossing while high", globalHas(r, GateReason.ME_CAPACITY));
        report(r, 0);
        assertTrue("still 9 pending: no downward crossing yet", r.engine.orders().capacityHigh());
        // the downward crossing (P <= 6) resets the trigger; the next upward one closes again
        for (final long h : new long[] {1, 2, 3}) {
            assertTrue(r.outcome(h, 0, CREATE, REJECTED, 900 + h, 0, false));
        }
        assertFalse(r.engine.orders().capacityHigh());
        create(r);
        create(r);
        assertTrue("the next crossing closes again", globalHas(r, GateReason.ME_CAPACITY));
    }

    @Test
    public void capacityOutcomeUnderReportingSizeDoesNotReopen() {
        // review 2 FIX-3: a CAPACITY 64 with ledgerSize < C: L := C, ME_BEHAVIOR set, no clear in the same endApply
        final V3Rig r = rig(4096);
        final long h = create(r);
        r.mark();
        final MeOutcomeView v = r.outcomeView(h, 0, CREATE, CAPACITY, 0, 0);
        v.ledgerSize = 0;
        assertTrue(r.apply(v));
        assertEquals("L := C", 4096L, r.engine.orders().meLedgerSize());
        assertTrue(globalHas(r, GateReason.ME_BEHAVIOR));
        assertTrue(globalHas(r, GateReason.ME_CAPACITY));
        for (final AssetsEngineEvent.AdmissionState a : admissions(r)) {
            assertTrue("no AdmissionState without ME_CAPACITY after it was set", a.gateReasons() == 0
                    || (a.gateReasons() & (1 << GateReason.ME_BEHAVIOR.code())) != 0);
        }
        assertEquals("one AdmissionState per new bit, none clearing", 2, admissions(r).size());
        assertTrue(r.engine.orders().capacityHigh());
    }

    @Test
    public void parkedDispatchReofferedAfterReopen() {
        final V3Rig r = rig(4096);
        final long h = create(r);
        final AssetsEngineEvent.MeDispatch minted = r.all(AssetsEngineEvent.MeDispatch.class).get(0);
        assertTrue(r.outcome(h, 0, CREATE, CAPACITY, 0, 0, false));
        assertEquals(1L, r.engine.orders().capacityParked());
        r.mark();
        report(r, 0);
        assertFalse("auto-reopen", globalHas(r, GateReason.ME_CAPACITY));
        assertEquals("the AdmissionState the forwarder resyncs on (W16)", 1, admissions(r).size());
        r.mark();
        r.engine.orders().applyRequestPendingDispatch(4, 0L, 1024);
        final AssetsEngineEvent.MeDispatchReply d = r.first(AssetsEngineEvent.MeDispatchReply.class);
        assertEquals("the parked bytes unchanged", minted.render().replace("MEDISPATCH", "DISPATCHREPLY"), d.render());
        assertTrue(r.order(h).has(CoreOrder.F_ME_CAPACITY_WAIT));
    }
}

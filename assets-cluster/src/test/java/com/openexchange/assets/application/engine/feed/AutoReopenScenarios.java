// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S10' 4R.2 RC-5: the automatic-reopen scenarios, each an executable rule. {@code GateAutoReopenTest} runs them with
 * every seam off (they pass); {@code FeedSeamsTest} runs each with ONE {@link FeedSeams} seam on and requires the
 * scenario to fail on the rule's named text, which is why each scenario asserts its named line FIRST, before any detail
 * a wrong core would also break.
 *
 * <p>Only engine behaviour is read here (acks, the gate, AdmissionState broadcasts), no 4R accessor, so the file also
 * compiles against the pre-4R core: that is how the RED-first run was taken (README Lane R).</p>
 *
 * <p>Timing (C1): every second-lane record arrives one apply after the first lane's; the restore scenario fills the holes
 * seconds of log time after the restore (C2).</p>
 */
final class AutoReopenScenarios {

    private AutoReopenScenarios() {
    }

    static final int G = OrderEntryGate.GLOBAL;
    private static long corr = 9_000;

    /** V3Rig.open() plus the two-party book: the cursor stands at 3, GLOBAL OPEN. */
    static V3Rig book() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        assertEquals("fixture cursor", 3L, r.cursor());
        assertEquals("fixture gate", GateState.OPEN, state(r));
        return r;
    }

    static GateState state(final V3Rig r) {
        return r.engine.gate().state(G);
    }

    static int bit(final GateReason g) {
        return 1 << g.code();
    }

    /** A GAP FeedHalt from {@code session} naming {@code identity}; returns the AdminAck result. */
    static AdminResult gap(final V3Rig r, final long session, final long identity, final long expected, final long seen) {
        return halt(r, session, identity, FeedHaltReason.GAP, expected, seen);
    }

    static AdminResult halt(final V3Rig r, final long session, final long identity, final FeedHaltReason reason,
                            final long expected, final long seen) {
        final int before = r.sink.events().size();
        r.engine.applyFeedHalt(session, ++corr, reason, 0L, expected, seen, identity, r.now);
        for (int i = before; i < r.sink.events().size(); i++) {
            if (r.sink.events().get(i) instanceof AssetsEngineEvent.AdminAck a && a.refTemplateId() == 66) {
                return a.result();
            }
        }
        throw new AssertionError("no AdminAck for the FeedHalt");
    }

    /** The next {@code n} feed entries (dense journalSeq and tradeId): a seller hit of 1/16 BTC each. */
    static void fill(final V3Rig r, final int n) {
        for (int i = 0; i < n; i++) {
            assertTrue("entry " + (r.jseq + 1) + " applies", r.sellHits(r.engine.getLastAppliedTradeId() + 1, ONE / 16));
        }
    }

    static AssetsEngineEvent.AdminAck admin(final V3Rig r, final GateState s, final GateReason reason) {
        final int before = r.sink.events().size();
        r.adminGate(G, s, reason);
        for (int i = before; i < r.sink.events().size(); i++) {
            if (r.sink.events().get(i) instanceof AssetsEngineEvent.AdminAck a && a.refTemplateId() == 73) {
                return a;
            }
        }
        throw new AssertionError("no AdminAck for the OrderEntryGate");
    }

    /** The GLOBAL AdmissionState broadcasts since the mark. */
    static List<AssetsEngineEvent.AdmissionState> broadcasts(final V3Rig r) {
        final List<AssetsEngineEvent.AdmissionState> out = new ArrayList<>();
        for (final AssetsEngineEvent.AdmissionState a : r.all(AssetsEngineEvent.AdmissionState.class)) {
            if (a.marketId() == G && !a.toOrigin()) {
                out.add(a);
            }
        }
        return out;
    }

    static long opens(final List<AssetsEngineEvent.AdmissionState> list) {
        return list.stream().filter(a -> a.gateState() == GateState.OPEN).count();
    }

    // ------------------------------------------------------------------------------------------------- the scenarios

    /** 4R.1 rule 1: a GAP reach-clear reopens and broadcasts once; the next apply is a no-op (rule 5, idempotent). */
    static void gapReachClearReopensAndBroadcastsOnce() {
        final V3Rig r = book();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 7));
        assertEquals("the record holds the gate", GateState.SUSPENDED, state(r));
        assertTrue("BRIDGE_HALT set", (r.globalReasons() & bit(GateReason.BRIDGE_HALT)) != 0);
        fill(r, 2); // c = 5 < seen - 1
        assertEquals("not reopened before the reach (c = 5, seen 7)", GateState.SUSPENDED, state(r));
        r.mark();
        fill(r, 1); // c = 6 = seen - 1: the reach-clear
        assertEquals("never reopened after the fill", GateState.OPEN, state(r));
        final List<AssetsEngineEvent.AdmissionState> b = broadcasts(r);
        assertEquals("exactly one AdmissionState at the reopen", 1, b.size());
        assertEquals("the broadcast is OPEN", GateState.OPEN, b.get(0).gateState());
        assertEquals("the broadcast carries no reason", 0, b.get(0).gateReasons());
        r.mark();
        fill(r, 1);
        assertEquals("the next apply broadcasts nothing (idempotent)", 0, broadcasts(r).size());
        assertEquals("an ADMIN OPEN after the reopen is DUPLICATE", AdminResult.DUPLICATE, admin(r, GateState.OPEN, GateReason.ADMIN).result());
        r.mark();
        r.engine.orders().applyCreate(r.limit(1, 0, 90 * ONE, ONE / 16), r.now);
        assertTrue("order entry admits again", r.all(AssetsEngineEvent.OrderReject.class).isEmpty());
    }

    /** 4R.1 rules 1 and 4: an operator SUSPEND during the halt survives the reopen (one bit cleared, ADMIN kept). */
    static void operatorSuspendSurvivesTheReopen() {
        final V3Rig r = book();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 7));
        assertEquals(AdminResult.ACCEPTED, admin(r, GateState.SUSPENDED, GateReason.ADMIN).result());
        assertEquals(bit(GateReason.ADMIN) | bit(GateReason.BRIDGE_HALT), r.globalReasons());
        r.mark();
        fill(r, 3); // the reach-clear
        assertEquals("state OPEN while ADMIN holds", GateState.SUSPENDED, state(r));
        assertEquals("only BRIDGE_HALT cleared: {ADMIN, BRIDGE_HALT} -> {ADMIN}", bit(GateReason.ADMIN), r.globalReasons());
        final List<AssetsEngineEvent.AdmissionState> b = broadcasts(r);
        assertEquals("one transition", 1, b.size());
        assertEquals(GateState.SUSPENDED, b.get(0).gateState());
        assertEquals(bit(GateReason.ADMIN), b.get(0).gateReasons());
        r.mark();
        r.engine.orders().applyCreate(r.limit(1, 0, 90 * ONE, ONE / 16), r.now);
        assertEquals("orders still refused", CoreRejectReason.GATE_CLOSED, r.first(AssetsEngineEvent.OrderReject.class).reason());
        assertEquals("the operator's OPEN", AdminResult.ACCEPTED, admin(r, GateState.OPEN, GateReason.ADMIN).result());
        assertEquals(GateState.OPEN, state(r));
    }

    /** 4R.1 rule 2: one of two lane records cleared keeps SUSPENDED/BRIDGE_HALT; both cleared reopens, once. */
    static void twoLanesBothMustClear() {
        final V3Rig r = book();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 6)); // lane 1: hole 4..5
        final AdminResult second = gap(r, 78, 2, 7, 9);          // lane 2, one apply later: hole 7..8
        r.mark();
        fill(r, 2); // c = 5: lane 1's record reaches, lane 2's does not
        assertEquals("OPEN with lane 2's record open", GateState.SUSPENDED, state(r));
        assertEquals("BRIDGE_HALT kept", bit(GateReason.BRIDGE_HALT), r.globalReasons());
        assertEquals("no transition while lane 2 is open", 0, broadcasts(r).size());
        fill(r, 3); // c = 8: lane 2's record reaches
        assertEquals("never reopened after both fills", GateState.OPEN, state(r));
        assertEquals("one reopen", 1L, opens(broadcasts(r)));
        assertEquals("lane 2's FeedHalt was a new record", AdminResult.ACCEPTED, second);
    }

    /** 4R.1 rule 4: a SOURCE_SCHEMA reach-clear clears the slot and keeps the bit (the operator's OPEN). */
    static void sourceSchemaReachClearKeepsTheBit() {
        final V3Rig r = book();
        assertEquals(AdminResult.ACCEPTED, halt(r, 77, 1, FeedHaltReason.SOURCE_SCHEMA, 4, 4));
        r.mark();
        fill(r, 1); // c = 4 >= expected: the slot reach-clears
        assertEquals("OPEN after a SOURCE_SCHEMA reach-clear", GateState.SUSPENDED, state(r));
        assertEquals("BRIDGE_HALT kept", bit(GateReason.BRIDGE_HALT), r.globalReasons());
        assertEquals("no broadcast", 0, broadcasts(r).size());
        assertEquals("the operator's OPEN passes (nothing latched)", AdminResult.ACCEPTED,
                admin(r, GateState.OPEN, GateReason.ADMIN).result());
        assertEquals(GateState.OPEN, state(r));
    }

    /** 4R.1 rule 4: EPOCH_MISMATCH never reaches; a GAP record clearing under it reopens nothing. */
    static void epochMismatchNeverReopens() {
        final V3Rig r = book();
        assertFalse(r.sellHitsAt(4, EPOCH + 1, r.egress + 1, 1, ONE / 16)); // a foreign epoch: EPOCH_MISMATCH
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 7));
        r.mark();
        fill(r, 3); // c = 6: the GAP record reaches
        assertEquals("OPEN under EPOCH_MISMATCH", GateState.SUSPENDED, state(r));
        assertEquals("no broadcast", 0, broadcasts(r).size());
        final AssetsEngineEvent.AdminAck refused = admin(r, GateState.OPEN, GateReason.ADMIN);
        assertEquals("ADMIN OPEN refused while latched", AdminResult.REJECTED, refused.result());
        assertEquals(CoreRejectReason.FEED_HALTED.code(), refused.reason());
    }

    /**
     * 4R.1 rule 4 (B2-1): a SOURCE_SCHEMA latch, the operator's 76 skip (slot cleared, bit kept, OPEN not yet issued), a
     * GAP record from another lane and its fill: still SUSPENDED, no reopen; the operator's OPEN then opens.
     */
    static void nonGapAwaitingOpenSurvivesAGapReach() {
        final V3Rig r = book();
        r.poison(61, 4, EPOCH, 0); // a poisoned record at c + 1: SOURCE_SCHEMA, skip 4
        r.mark();
        r.clear(FeedHaltReason.SOURCE_SCHEMA, 4);
        assertEquals("the 76 skip", AdminResult.ACCEPTED, r.onlyAck().result());
        r.jseq = 4;
        assertEquals("BRIDGE_HALT kept after the skip", bit(GateReason.BRIDGE_HALT), r.globalReasons());
        assertEquals(AdminResult.ACCEPTED, gap(r, 78, 2, 5, 7));
        r.mark();
        fill(r, 2); // c = 6: the GAP record reaches
        assertEquals("OPEN after a SOURCE_SCHEMA skip that awaited OPEN", GateState.SUSPENDED, state(r));
        assertEquals("no broadcast", 0, broadcasts(r).size());
        assertEquals("the operator's OPEN", AdminResult.ACCEPTED, admin(r, GateState.OPEN, GateReason.ADMIN).result());
        assertEquals(GateState.OPEN, state(r));
    }

    /** 4R.1 rule 2 (S2-3): two live sessions with one identity; the second's smaller {@code seen} never weakens the record. */
    static void twoSessionsOneIdentityNeverWeakenTheRecord() {
        final V3Rig r = book();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 5, 4, 9)); // session 77: hole 4..8
        final AdminResult second = gap(r, 78, 5, 4, 6);         // session 78, same identity, a smaller seen
        fill(r, 2); // c = 5: would reach the weakened record {4, 6}
        assertEquals("reopened with the first lane's hole open", GateState.SUSPENDED, state(r));
        fill(r, 3); // c = 8: the first lane's reach
        assertEquals("never reopened after the first lane's fill", GateState.OPEN, state(r));
        assertEquals("the merge marked the record shared (seen kept at 9): ACCEPTED", AdminResult.ACCEPTED, second);
    }

    /** 4R.1 rule 2: a ninth identity is ACCEPTED into overflow; every record reached keeps SUSPENDED; 76 then OPEN. */
    static void ninthLaneOverflowNeedsTheOperator() {
        final V3Rig r = book();
        for (int i = 1; i <= 9; i++) {
            assertEquals("lane " + i + " accepted (the ninth into overflow)", AdminResult.ACCEPTED, gap(r, 100 + i, i, 4, 6));
        }
        r.mark();
        fill(r, 2); // c = 5: every one of the eight records reaches
        assertEquals("OPEN with the overflow record set", GateState.SUSPENDED, state(r));
        assertEquals("no broadcast", 0, broadcasts(r).size());
        final AssetsEngineEvent.AdminAck refused = admin(r, GateState.OPEN, GateReason.ADMIN);
        assertEquals("ADMIN OPEN refused while the overflow is set", AdminResult.REJECTED, refused.result());
        assertEquals(CoreRejectReason.FEED_HALTED.code(), refused.reason());
        r.mark();
        r.clear(FeedHaltReason.GAP, 0);
        assertEquals("76 {GAP, 0} clears the table and the overflow", AdminResult.ACCEPTED, r.onlyAck().result());
        assertEquals("the bit waits for the operator's OPEN", GateState.SUSPENDED, state(r));
        assertEquals(AdminResult.ACCEPTED, admin(r, GateState.OPEN, GateReason.ADMIN).result());
        assertEquals(GateState.OPEN, state(r));
    }

    /**
     * 4R.1 rule 5: a snapshot mid-halt with two records, restore, fill both seconds later: one reopen and the same
     * broadcast sequence as the run without the restore.
     */
    static void snapshotMidHaltRestoreFillBoth() {
        final List<String> straight = twoRecordRun(false);
        final List<String> restored = twoRecordRun(true);
        assertEquals("the restored run broadcasts what the straight run does", straight, restored);
    }

    private static List<String> twoRecordRun(final boolean restore) {
        V3Rig r = book();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 6));
        assertEquals(AdminResult.ACCEPTED, gap(r, 78, 2, 7, 9));
        if (restore) {
            r = r.fork();
            r.now += 3_000; // the fills land seconds of log time after the restore (C2)
        }
        r.mark();
        fill(r, 2); // c = 5: lane 1 reaches
        assertEquals("after restore: reopened with a record open", GateState.SUSPENDED, state(r));
        fill(r, 3); // c = 8: lane 2 reaches
        final List<AssetsEngineEvent.AdmissionState> b = broadcasts(r);
        assertEquals("never reopened after both fills", 1L, opens(b));
        assertEquals(GateState.OPEN, state(r));
        final List<String> out = new ArrayList<>();
        for (final AssetsEngineEvent.AdmissionState a : b) {
            out.add(a.gateState() + "/" + Integer.toHexString(a.gateReasons()));
        }
        return out;
    }

    /** C4: a GAP holder added and removed twice in one run; two reopens. */
    static void twoHaltsInOneRunReopenTwice() {
        final V3Rig r = book();
        r.mark();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 6));
        fill(r, 2); // c = 5: first reopen
        assertEquals("first reopen", GateState.OPEN, state(r));
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 7, 9)); // the same lane, a later hole
        assertEquals("the second record closes the gate again", GateState.SUSPENDED, state(r));
        fill(r, 3); // c = 8: second reopen
        assertEquals("second reopen", GateState.OPEN, state(r));
        assertEquals("two OPEN broadcasts", 2L, opens(broadcasts(r)));
    }
}

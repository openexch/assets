// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.gate;

import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.determinism.V3Genesis;
import com.openexchange.core.domain.AdminResult;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.FeedHaltReason;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import org.junit.Test;

import static com.openexchange.core.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The gate as a reason bitmask under behaviour v3; the overwrite rule under v2. */
public class GateReasonSetTest {

    private static final int G = OrderEntryGate.GLOBAL;
    private static final int T73 = 73;

    private static int bit(final GateReason r) {
        return 1 << r.code();
    }

    private static CoreEngineEvent.AdminAck admin(final V3Rig r, final int market, final GateState s, final GateReason reason) {
        r.mark();
        r.adminGate(market, s, reason);
        final CoreEngineEvent.AdminAck ack = r.onlyAck();
        assertEquals("AdminAck names OrderEntryGate (73)", T73, ack.refTemplateId());
        return ack;
    }

    @Test
    public void gateReasonSetSurvivesCapacityOverBridgeHalt() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        final long pending = r.create(r.limit(1, 0, 100 * ONE, ONE));
        r.halt(FeedHaltReason.GAP, 0, 4, 8, 0);
        assertEquals("the latch adds BRIDGE_HALT", bit(GateReason.BRIDGE_HALT), r.globalReasons());
        // exactly-next entries keep applying while latched: a CAPACITY outcome adds its own bit, overwriting nothing
        assertTrue(r.outcome(pending, 0, V3Rig.KIND_CREATE, 3, 0, 0, false));
        assertEquals("BRIDGE_HALT | ME_CAPACITY", bit(GateReason.BRIDGE_HALT) | bit(GateReason.ME_CAPACITY), r.globalReasons());
        assertEquals("gateReason = the lowest set bit", GateReason.BRIDGE_HALT, r.engine.gate().reason(G));
        // the internal clear of ME_CAPACITY (D4 auto-clear) cannot open a gate BRIDGE_HALT holds
        r.mark();
        r.engine.clearGateInternally(G, GateReason.ME_CAPACITY);
        assertEquals(bit(GateReason.BRIDGE_HALT), r.globalReasons());
        assertEquals(GateState.SUSPENDED, r.engine.gate().state(G));
        final CoreEngineEvent.AdmissionState as = r.first(CoreEngineEvent.AdmissionState.class);
        assertEquals("AdmissionState carries the set", bit(GateReason.BRIDGE_HALT), as.gateReasons());
        // ADMIN OPEN is refused while the feed is latched
        final CoreEngineEvent.AdminAck refused = admin(r, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.REJECTED, refused.result());
        assertEquals(CoreRejectReason.FEED_HALTED.code(), refused.reason());
        assertEquals("nothing cleared", bit(GateReason.BRIDGE_HALT), r.globalReasons());
        // the gap is filled (4..7 applied): the GAP record reach-clears and clears the BRIDGE_HALT
        // bit by itself; no other reason is left, so the gate is OPEN and an ADMIN OPEN answers DUPLICATE
        for (long t = 1; t <= 3; t++) {
            assertTrue(r.sellHits(t, ONE / 8));
        }
        r.assertNotLatched("gap filled");
        assertEquals(0, r.globalReasons());
        assertEquals(GateState.OPEN, r.engine.gate().state(G));
        final CoreEngineEvent.AdminAck open = admin(r, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.DUPLICATE, open.result());
        assertEquals(0, r.globalReasons());
    }

    @Test
    public void everyRowOfD4() {
        final V3Rig r = V3Rig.open();
        // invalid market
        assertEquals(CoreRejectReason.UNKNOWN_MARKET.code(), admin(r, 9, GateState.SUSPENDED, GateReason.ADMIN).reason());
        // SUSPENDED with an engine-owned reason: REJECTED INVALID, nothing set
        for (final GateReason engineOwned : new GateReason[] {GateReason.BRIDGE_HALT, GateReason.ME_CAPACITY, GateReason.GENESIS,
            GateReason.ME_LEG_LIVE, GateReason.ME_BEHAVIOR}) {
            final CoreEngineEvent.AdminAck ack = admin(r, 2, GateState.SUSPENDED, engineOwned);
            assertEquals(engineOwned + ": REJECTED", AdminResult.REJECTED, ack.result());
            assertEquals(engineOwned + ": INVALID", CoreRejectReason.INVALID.code(), ack.reason());
            assertEquals(engineOwned + ": nothing set", 0, r.engine.gate().reasons(2));
        }
        // SUSPENDED with an ADMIN-owned reason adds its bit; again is DUPLICATE; a second ADMIN reason adds a second bit
        assertEquals(AdminResult.ACCEPTED, admin(r, 2, GateState.SUSPENDED, GateReason.RETENTION_BUDGET).result());
        assertEquals(new CoreEngineEvent.AdmissionState(2, GateState.SUSPENDED, GateReason.RETENTION_BUDGET, false, false,
                bit(GateReason.RETENTION_BUDGET)), r.first(CoreEngineEvent.AdmissionState.class));
        assertEquals(AdminResult.DUPLICATE, admin(r, 2, GateState.SUSPENDED, GateReason.RETENTION_BUDGET).result());
        assertEquals(0, r.count(CoreEngineEvent.AdmissionState.class));
        assertEquals(AdminResult.ACCEPTED, admin(r, 2, GateState.SUSPENDED, GateReason.RECOVERY).result());
        assertEquals(bit(GateReason.RETENTION_BUDGET) | bit(GateReason.RECOVERY), r.engine.gate().reasons(2));
        assertEquals("lowest set bit", GateReason.RETENTION_BUDGET, r.engine.gate().reason(2));
        assertEquals(AdminResult.ACCEPTED, admin(r, 2, GateState.SUSPENDED, GateReason.ADMIN).result());
        assertEquals(GateReason.ADMIN, r.engine.gate().reason(2));
        assertEquals(AdminResult.ACCEPTED, admin(r, 3, GateState.SUSPENDED, GateReason.APPLY_FAULT).result());
        // OPEN clears every bit (both OPEN refusals pass)
        assertEquals(AdminResult.ACCEPTED, admin(r, 2, GateState.OPEN, GateReason.ADMIN).result());
        assertEquals(0, r.engine.gate().reasons(2));
        assertEquals(new CoreEngineEvent.AdmissionState(2, GateState.OPEN, GateReason.ADMIN, false, false, 0),
                r.first(CoreEngineEvent.AdmissionState.class));
        // OPEN with nothing set: DUPLICATE
        assertEquals(AdminResult.DUPLICATE, admin(r, 2, GateState.OPEN, GateReason.ADMIN).result());
        // OPEN with BRIDGE_HALT while latched: FEED_HALTED
        r.halt(FeedHaltReason.GAP, 0, 2, 5, 0);
        final CoreEngineEvent.AdminAck halted = admin(r, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.REJECTED, halted.result());
        assertEquals(CoreRejectReason.FEED_HALTED.code(), halted.reason());
        // internal suspend: AdmissionState iff the bit is new; internal clear: iff changed
        r.mark();
        r.engine.suspendGateInternally(4, GateReason.ME_CAPACITY);
        assertEquals(1, r.count(CoreEngineEvent.AdmissionState.class));
        r.mark();
        r.engine.suspendGateInternally(4, GateReason.ME_CAPACITY);
        assertEquals(0, r.count(CoreEngineEvent.AdmissionState.class));
        r.mark();
        r.engine.clearGateInternally(4, GateReason.ME_CAPACITY);
        assertEquals(1, r.count(CoreEngineEvent.AdmissionState.class));
        assertEquals(0, r.engine.gate().reasons(4));
        r.mark();
        r.engine.clearGateInternally(4, GateReason.ME_CAPACITY);
        assertEquals(0, r.count(CoreEngineEvent.AdmissionState.class));

        // OPEN of GLOBAL while unbound: ME_LANE_INACTIVE
        final V3Rig u = V3Rig.unbound();
        final CoreEngineEvent.AdminAck lane = admin(u, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.REJECTED, lane.result());
        assertEquals(CoreRejectReason.ME_LANE_INACTIVE.code(), lane.reason());
    }

    /** HALT_HOLDER_PENDING (45), the AdminAck-only refusal of an OPEN that names nothing it clears. */
    private static final int HALT_HOLDER_PENDING = 45;

    /**
     * The measured sequence. A SOURCE_SCHEMA FeedHalt on an unbound core latches; the bind
     * record reach-clears the slot and leaves the holder; OPEN(GLOBAL, ADMIN) is then REJECTED 45 (it used to be
     * ACCEPTED, the 4.3 s window); a second FeedHalt latches again; OPEN(GLOBAL, BRIDGE_HALT) while latched keeps the
     * existing order of refusals (40 FEED_HALTED first).
     */
    @Test
    public void anOpenWhileAHaltHolderIsHeldMustNameBridgeHalt() {
        final V3Rig r = V3Rig.unbound();
        r.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 1, 1, 0);
        assertTrue("SOURCE_SCHEMA e1 latched", r.feed().latched());
        assertTrue("BRIDGE_HALT set", r.globalHas(GateReason.BRIDGE_HALT));
        r.mark();
        assertTrue(r.bindFresh());
        assertTrue("the bind reach-cleared the slot", !r.feed().latched());
        assertEquals("the holder is kept", com.openexchange.core.application.engine.feed.FeedState.HOLDER_NONGAP_AWAITING_OPEN,
                r.feed().holders());
        assertTrue("the bit is kept", r.globalHas(GateReason.BRIDGE_HALT));
        assertEquals("no broadcast", 0, r.all(CoreEngineEvent.AdmissionState.class).stream()
                .filter(a -> a.marketId() == G && !a.toOrigin()).count());

        final CoreEngineEvent.AdminAck refused = admin(r, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.REJECTED, refused.result());
        assertEquals("HALT_HOLDER_PENDING", HALT_HOLDER_PENDING, refused.reason());
        assertEquals(GateState.SUSPENDED, r.engine.gate().state(G));
        assertEquals("no broadcast for a refused OPEN", 0, r.count(CoreEngineEvent.AdmissionState.class));

        r.halt(FeedHaltReason.SOURCE_SCHEMA, 0, 2, 2, 0);
        assertTrue("e2 latched again", r.feed().latched());
        final CoreEngineEvent.AdminAck latched = admin(r, G, GateState.OPEN, GateReason.BRIDGE_HALT);
        assertEquals(AdminResult.REJECTED, latched.result());
        assertEquals("the latch refusal comes first", CoreRejectReason.FEED_HALTED.code(), latched.reason());
        assertEquals(GateState.SUSPENDED, r.engine.gate().state(G));
    }

    /**
     * Replay safety over a 76. {GAP, 0} clears the table and sets the holder; the holder rides the
     * snapshot, so a restored engine refuses OPEN(ADMIN) with 45 exactly as the straight one; OPEN(BRIDGE_HALT) opens.
     */
    @Test
    public void theHolderSurvivesASnapshotAndStillNeedsTheAcknowledgement() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        r.halt(FeedHaltReason.GAP, 0, 4, 8, 0);
        r.mark();
        r.clear(FeedHaltReason.GAP, 0);
        assertEquals("76 {GAP, 0}", AdminResult.ACCEPTED, r.onlyAck().result());
        assertEquals(com.openexchange.core.application.engine.feed.FeedState.HOLDER_NONGAP_AWAITING_OPEN, r.feed().holders());
        for (final V3Rig rig : new V3Rig[] {r.fork(), r}) {
            final CoreEngineEvent.AdminAck refused = admin(rig, G, GateState.OPEN, GateReason.ADMIN);
            assertEquals(AdminResult.REJECTED, refused.result());
            assertEquals(HALT_HOLDER_PENDING, refused.reason());
            final CoreEngineEvent.AdminAck ack = admin(rig, G, GateState.OPEN, GateReason.BRIDGE_HALT);
            assertEquals(AdminResult.ACCEPTED, ack.result());
            assertEquals(GateState.OPEN, rig.engine.gate().state(G));
            assertEquals("the acknowledgement empties the holder set", 0, rig.feed().holders());
        }
    }

    /** The controls. Only an OPEN of GLOBAL with a holder held changes. */
    @Test
    public void openControlsWithoutAHolderAreUnchanged() {
        // clean genesis: no holder, OPEN(ADMIN) ACCEPTED
        final V3Rig fresh = V3Rig.bound();
        assertEquals(0, fresh.feed().holders());
        assertEquals(AdminResult.ACCEPTED, admin(fresh, G, GateState.OPEN, GateReason.ADMIN).result());
        // a non-GLOBAL market is unaffected by a GLOBAL holder
        final V3Rig r = V3Rig.open();
        r.twoParty();
        r.halt(FeedHaltReason.GAP, 0, 4, 8, 0);
        r.clear(FeedHaltReason.GAP, 0);
        assertEquals(com.openexchange.core.application.engine.feed.FeedState.HOLDER_NONGAP_AWAITING_OPEN, r.feed().holders());
        admin(r, 1, GateState.SUSPENDED, GateReason.ADMIN);
        assertEquals("market 1 opens with ADMIN", AdminResult.ACCEPTED, admin(r, 1, GateState.OPEN, GateReason.ADMIN).result());
        // 76 {SOURCE_SCHEMA, skip} then OPEN(ADMIN): 45
        final V3Rig s = V3Rig.open();
        s.twoParty();
        s.poison(61, 4, V3Rig.EPOCH, 0);
        s.mark();
        s.clear(FeedHaltReason.SOURCE_SCHEMA, 4);
        assertEquals(AdminResult.ACCEPTED, s.onlyAck().result());
        final CoreEngineEvent.AdminAck refused = admin(s, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.REJECTED, refused.result());
        assertEquals(HALT_HOLDER_PENDING, refused.reason());
    }

    @Test
    public void globalStaysClosedUntilBound() {
        final V3Rig r = V3Rig.unbound();
        assertEquals("the switch converts GLOBAL to GENESIS", bit(GateReason.GENESIS), r.globalReasons());
        final CoreEngineEvent.AdminAck refused = admin(r, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.REJECTED, refused.result());
        assertEquals(CoreRejectReason.ME_LANE_INACTIVE.code(), refused.reason());
        assertEquals(GateState.SUSPENDED, r.engine.gate().state(G));
        assertEquals(bit(GateReason.GENESIS), r.globalReasons());
        assertTrue(r.bindFresh());
        final CoreEngineEvent.AdminAck open = admin(r, G, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.ACCEPTED, open.result());
        assertEquals(GateState.OPEN, r.engine.gate().state(G));
    }

    @Test
    public void switchConvertsGatesAndV2StaysOverwrite() {
        final V3Rig r = V3Rig.v2();
        // v2: one state, one reason, overwritten; the set is derived
        r.adminGate(2, GateState.SUSPENDED, GateReason.RETENTION_BUDGET);
        r.adminGate(3, GateState.SUSPENDED, GateReason.ME_CAPACITY);
        r.adminGate(4, GateState.SUSPENDED, GateReason.ADMIN);
        r.adminGate(4, GateState.SUSPENDED, GateReason.RECOVERY);
        assertEquals("v2 overwrites", GateReason.RECOVERY, r.engine.gate().reason(4));
        assertEquals("v2 derived set", bit(GateReason.RECOVERY), r.engine.gate().reasons(4));
        assertEquals(bit(GateReason.ME_CAPACITY), r.engine.gate().reasons(3));
        // the switch to 3 converts in the same apply, with no egress
        r.mark();
        r.engine.applyCoreConfig(V3Genesis.config(3, 3), r.now);
        assertEquals(3L, r.engine.coreConfig().behaviorVersion());
        assertEquals("the conversion emits no AdmissionState", 0, r.count(CoreEngineEvent.AdmissionState.class));
        assertEquals("OPEN market: no bit", 0, r.engine.gate().reasons(1));
        assertEquals("ADMIN-owned reason kept", bit(GateReason.RETENTION_BUDGET), r.engine.gate().reasons(2));
        assertEquals("engine-owned reason left by ADMIN becomes ADMIN (NOTE-A)", bit(GateReason.ADMIN), r.engine.gate().reasons(3));
        assertEquals(GateReason.ADMIN, r.engine.gate().reason(3));
        assertEquals(bit(GateReason.RECOVERY), r.engine.gate().reasons(4));
        assertEquals("GLOBAL keeps its reason and gains GENESIS (NOTE-B)", bit(GateReason.GENESIS), r.globalReasons());

        // GLOBAL suspended by ADMIN under v2 converts to ADMIN | GENESIS
        final V3Rig a = V3Rig.v2();
        a.adminGate(G, GateState.SUSPENDED, GateReason.ADMIN);
        a.engine.applyCoreConfig(V3Genesis.config(3, 3), a.now);
        assertEquals(bit(GateReason.ADMIN) | bit(GateReason.GENESIS), a.globalReasons());
        assertEquals(GateReason.ADMIN, a.engine.gate().reason(G));
    }
}

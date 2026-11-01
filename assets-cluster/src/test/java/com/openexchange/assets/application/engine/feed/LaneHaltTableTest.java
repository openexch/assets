// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.order.S8Support;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.AutoReopenScenarios.book;
import static com.openexchange.assets.application.engine.feed.AutoReopenScenarios.fill;
import static com.openexchange.assets.application.engine.feed.AutoReopenScenarios.gap;
import static com.openexchange.assets.application.engine.feed.AutoReopenScenarios.halt;
import static com.openexchange.assets.application.engine.feed.AutoReopenScenarios.state;
import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S10' 4R.1 rules 2-7 at the FeedState / engine level: the replicated lane-halt table (8 records + the overflow record,
 * keyed by identity, kept sorted), the holder set, DUPLICATE_IDENTITY, the counters, the report's one haltReason and
 * section 12 (its version byte, the fail-closed restore and the breach checks).
 */
public class LaneHaltTableTest {

    private static final int G = OrderEntryGate.GLOBAL;

    // ---------------------------------------------------------------- the table (RC-1, RC-2)

    @Test
    public void recordsAreKeyedByIdentitySortedAndReplacedPerSession() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        assertEquals(AdminResult.ACCEPTED, gap(r, 75, 5, 4, 9));
        assertEquals(AdminResult.ACCEPTED, gap(r, 72, 2, 4, 7));
        assertEquals(AdminResult.ACCEPTED, gap(r, 79, 9, 6, 12));
        assertEquals(3, f.laneHaltCount());
        assertArrayEquals("ascending identity whatever the arrival order", new long[] {2, 5, 9},
                new long[] {f.laneHaltIdentity(0), f.laneHaltIdentity(1), f.laneHaltIdentity(2)});
        assertEquals("the record's session", 75L, f.laneHaltSession(1));
        assertEquals("the record's log position is recorded", r.engine.getLogPosition(), f.laneHaltLogPosition(1));
        assertEquals(3L, f.haltAppliedCount());
        // identical from the same session: DUPLICATE, nothing written
        assertEquals(AdminResult.DUPLICATE, gap(r, 75, 5, 4, 9));
        assertEquals(1L, f.haltDuplicateCount());
        // the same session's changed view replaces (a lane has one halt at a time), a smaller seen included
        assertEquals(AdminResult.ACCEPTED, gap(r, 75, 5, 5, 8));
        assertEquals(5L, f.laneHaltExpected(1));
        assertEquals(8L, f.laneHaltSeen(1));
        // stale per record against c (c = 3): seen - 1 <= 3
        assertEquals(AdminResult.DUPLICATE, gap(r, 76, 6, 2, 4));
        assertEquals(1L, f.haltStaleCount());
        assertEquals("a stale FeedHalt writes nothing", 3, f.laneHaltCount());
        assertEquals("the holder is GAP", FeedState.HOLDER_GAP, f.holders());
    }

    @Test
    public void aReplaceFromAnotherSessionKeepsMaxSeen() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 5, 4, 9));
        assertEquals("the second session's merge marks the record shared: ACCEPTED", AdminResult.ACCEPTED, gap(r, 78, 5, 4, 6));
        assertEquals("seen = max(9, 6)", 9L, f.laneHaltSeen(0));
        assertEquals(FeedState.SESSION_SHARED, f.laneHaltSession(0));
        assertEquals("a repeat changes nothing: DUPLICATE", AdminResult.DUPLICATE, gap(r, 78, 5, 4, 6));
        assertEquals(AdminResult.ACCEPTED, gap(r, 78, 5, 5, 11));
        assertEquals("seen = max(9, 11)", 11L, f.laneHaltSeen(0));
        assertEquals("expected = min(4, 5)", 4L, f.laneHaltExpected(0));
        // neither holder can lower a shared record: the first session's smaller view merges too
        assertEquals("the first session lowering: still max", AdminResult.DUPLICATE, gap(r, 77, 5, 4, 7));
        assertEquals(11L, f.laneHaltSeen(0));
        fill(r, 7); // c = 10 = seen - 1: the shared record clears
        assertEquals(0, f.laneHaltCount());
        assertEquals("a fresh record is single-session again", AdminResult.ACCEPTED, gap(r, 77, 5, 12, 20));
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 5, 12, 15));
        assertEquals("its own session lowers it", 15L, f.laneHaltSeen(0));
    }

    @Test
    public void ninthIdentityGoesToTheOverflowRecord() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        for (int i = 1; i <= 8; i++) {
            assertEquals(AdminResult.ACCEPTED, gap(r, 100 + i, i, 4, 6 + i));
        }
        assertFalse(f.haltOverflow());
        assertEquals(AdminResult.ACCEPTED, gap(r, 109, 9, 5, 20));
        assertTrue(f.haltOverflow());
        assertEquals(8, f.laneHaltCount());
        assertEquals(5L, f.overflowExpected());
        assertEquals(20L, f.overflowSeen());
        assertEquals(FeedState.HOLDER_GAP | FeedState.HOLDER_OVERFLOW, f.holders());
        assertEquals("a repeat that changes nothing is DUPLICATE", AdminResult.DUPLICATE, gap(r, 109, 9, 5, 20));
        assertEquals("a tenth merges: min expected, max seen (informational)", AdminResult.ACCEPTED, gap(r, 110, 10, 4, 30));
        assertEquals(4L, f.overflowExpected());
        assertEquals(30L, f.overflowSeen());
        assertEquals("an open identity still replaces its own record", AdminResult.ACCEPTED, gap(r, 101, 1, 4, 40));
        assertEquals(40L, f.laneHaltSeen(0));
        fill(r, 40); // c = 43: every record reached
        assertEquals(0, f.laneHaltCount());
        assertEquals("the overflow never clears by reach", FeedState.HOLDER_OVERFLOW, f.holders());
        assertTrue(f.latched());
        assertEquals(FeedHaltReason.GAP, f.haltReason());
        assertEquals(GateState.SUSPENDED, state(r));
        r.mark();
        r.clear(FeedHaltReason.GAP, 7);
        assertEquals("76 {GAP, s != 0} skips nothing: REJECTED", AdminResult.REJECTED, r.onlyAck().result());
        r.mark();
        r.clear(FeedHaltReason.GAP, 0);
        assertEquals(AdminResult.ACCEPTED, r.onlyAck().result());
        assertFalse(f.haltOverflow());
        assertEquals(0L, f.overflowSeen());
        assertEquals("76 adds NONGAP_AWAITING_OPEN", FeedState.HOLDER_NONGAP_AWAITING_OPEN, f.holders());
        r.mark();
        r.clear(FeedHaltReason.GAP, 0);
        assertEquals("nothing left to clear", AdminResult.DUPLICATE, r.onlyAck().result());
        assertEquals(AdminResult.ACCEPTED, AutoReopenScenarios.admin(r, GateState.OPEN, GateReason.ADMIN).result());
        assertEquals("ADMIN OPEN empties the holder set", 0, f.holders());
        assertEquals("C5: an operator-cleared overflow, no automatic reopen counted", 0L, f.gateAutoReopenCount());
    }

    @Test
    public void aCoreDetectedGapIsTheRecordOfIdentityZero() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        assertFalse(r.sellHitsAt(7, EPOCH, r.egress + 1, 1, ONE / 16)); // D1 row 5: a frame above c + 1
        assertEquals(1, f.laneHaltCount());
        assertEquals(FeedState.CORE_DETECTED_IDENTITY, f.laneHaltIdentity(0));
        assertEquals(4L, f.laneHaltExpected(0));
        assertEquals(7L, f.laneHaltSeen(0));
        assertEquals("gap rejects counted", 1L, f.gapCount());
        assertFalse(r.sellHitsAt(9, EPOCH, r.egress + 1, 1, ONE / 16));
        assertEquals("a second core-detected GAP merges: seen = max (review FIX-1)", 9L, f.laneHaltSeen(0));
        assertEquals("expected = min", 4L, f.laneHaltExpected(0));
        assertEquals(0L, f.latchSuppressedCount());
        assertEquals("a lane's FeedHalt is its own record", AdminResult.ACCEPTED, gap(r, 77, 1, 4, 12));
        assertEquals(2, f.laneHaltCount());
        fill(r, 3); // c = 6: below both records' reach
        assertEquals(2, f.laneHaltCount());
        fill(r, 2); // c = 8: the core's merged record reaches, lane 1's does not
        assertEquals(1, f.laneHaltCount());
        assertEquals(1L, f.laneHaltIdentity(0));
        assertEquals(GateState.SUSPENDED, state(r));
    }

    @Test
    public void aSkipThatReachesARecordClearsItAndOpensNothing() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        r.poison(61, 4, EPOCH, 0); // SOURCE_SCHEMA skip 4
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 5));
        assertEquals(FeedState.HOLDER_GAP | FeedState.HOLDER_NONGAP_AWAITING_OPEN, f.holders());
        r.mark();
        r.clear(FeedHaltReason.SOURCE_SCHEMA, 4);
        assertEquals(AdminResult.ACCEPTED, r.onlyAck().result());
        assertEquals("the skip moved c to 4 = seen - 1: the record went", 0, f.laneHaltCount());
        assertEquals("NONGAP_AWAITING_OPEN keeps the bit", FeedState.HOLDER_NONGAP_AWAITING_OPEN, f.holders());
        assertEquals(GateState.SUSPENDED, state(r));
        assertTrue(r.all(AssetsEngineEvent.GateAutoReopen.class).isEmpty());
        r.engine.verifyRestoredInvariants();
    }

    // ---------------------------------------------------------------- the reopen, the counters and the control event (RC-3, RC-4)

    @Test
    public void twoRecordsClearedByOneEntryCountTwoClearsAndOneReopen() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 6));
        assertEquals(AdminResult.ACCEPTED, gap(r, 78, 2, 4, 6));
        assertEquals(2L, f.haltAppliedCount());
        r.mark();
        fill(r, 2);
        assertEquals("one auto clear per record", 2L, f.latchAutoClearCount());
        assertEquals("one reopen", 1L, f.gateAutoReopenCount());
        final List<AssetsEngineEvent.GateAutoReopen> ev = r.all(AssetsEngineEvent.GateAutoReopen.class);
        assertEquals(List.of(new AssetsEngineEvent.GateAutoReopen(5L, true, 0, 0)), ev);
        assertEquals("one FeedLatchCleared per record", 2, r.all(AssetsEngineEvent.FeedLatchCleared.class).size());
    }

    @Test
    public void aBitClearUnderAdminIsNotCountedAsAnOpen() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 6));
        assertEquals(AdminResult.ACCEPTED, AutoReopenScenarios.admin(r, GateState.SUSPENDED, GateReason.ADMIN).result());
        r.mark();
        fill(r, 2);
        assertEquals("the counter counts opens, not bit clears", 0L, f.gateAutoReopenCount());
        assertEquals(List.of(new AssetsEngineEvent.GateAutoReopen(5L, false, 1 << GateReason.ADMIN.code(), 0)),
                r.all(AssetsEngineEvent.GateAutoReopen.class));
        assertEquals("ADMIN OPEN empties the set", AdminResult.ACCEPTED,
                AutoReopenScenarios.admin(r, GateState.OPEN, GateReason.ADMIN).result());
    }

    @Test
    public void reportCarriesTheHigherExitClass() {
        final V3Rig r = book();
        assertEquals(AdminResult.ACCEPTED, gap(r, 77, 1, 4, 9));
        assertEquals(FeedHaltReason.GAP, report(r).haltReason());
        assertTrue(report(r).feedHalted());
        assertEquals(AdminResult.ACCEPTED, halt(r, 78, 2, FeedHaltReason.SOURCE_SCHEMA, 4, 4));
        assertEquals("GAP record + SOURCE_SCHEMA slot: the higher class", FeedHaltReason.SOURCE_SCHEMA, report(r).haltReason());
        fill(r, 1); // c = 4: the slot reach-clears, the record stays
        assertEquals(FeedHaltReason.GAP, report(r).haltReason());
        fill(r, 4); // c = 8: the record reaches
        assertFalse(report(r).feedHalted());
        assertEquals(FeedHaltReason.NONE, report(r).haltReason());
        assertEquals("NONGAP_AWAITING_OPEN from the SOURCE_SCHEMA keeps the gate shut", GateState.SUSPENDED, state(r));
    }

    private static AssetsEngineEvent.FeedPositionReport report(final V3Rig r) {
        r.mark();
        r.engine.reportFeedPosition(1L);
        return r.first(AssetsEngineEvent.FeedPositionReport.class);
    }

    @Test
    public void holderSetFollowsEveryAddAndRemove() {
        final V3Rig r = book();
        final FeedState f = r.feed();
        assertEquals(0, f.holders());
        assertEquals(AdminResult.ACCEPTED, halt(r, 77, 1, FeedHaltReason.SOURCE_SCHEMA, 4, 4));
        assertEquals("a non-GAP slot write adds NONGAP", FeedState.HOLDER_NONGAP_AWAITING_OPEN, f.holders());
        fill(r, 1);
        assertEquals("its reach-clear keeps it", FeedState.HOLDER_NONGAP_AWAITING_OPEN, f.holders());
        assertEquals(AdminResult.ACCEPTED, gap(r, 78, 2, 5, 7));
        assertEquals(FeedState.HOLDER_GAP | FeedState.HOLDER_NONGAP_AWAITING_OPEN, f.holders());
        fill(r, 2);
        assertEquals("the table emptied: GAP goes, NONGAP stays", FeedState.HOLDER_NONGAP_AWAITING_OPEN, f.holders());
        assertTrue((r.globalReasons() & (1 << GateReason.BRIDGE_HALT.code())) != 0);
        assertEquals(AdminResult.ACCEPTED, AutoReopenScenarios.admin(r, GateState.OPEN, GateReason.ADMIN).result());
        assertEquals(0, f.holders());
        r.engine.verifyRestoredInvariants();
    }

    // ---------------------------------------------------------------- DUPLICATE_IDENTITY (RC-2, S2-3)

    private static int register(final V3Rig r, final SessionClass cls, final long identity) {
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .correlationId(8_800).sessionClass(cls).readerKind((short) 0).resumable(BoolFlag.TRUE).identity(identity)
                .sourceEpoch(0);
        final EgressChannelsEncoder ch = e.channels().clear();
        ch.acks(true).control(true);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static AssetsEngineEvent.AdminAck registerAck(final V3Rig r, final long session, final SessionClass cls,
                                                          final long identity) {
        r.mark();
        r.dispatch(session, register(r, cls, identity));
        for (final AssetsEngineEvent.AdminAck a : r.all(AssetsEngineEvent.AdminAck.class)) {
            if (a.refTemplateId() == 80) {
                return a;
            }
        }
        throw new AssertionError("no RegisterSession AdminAck: " + r.events());
    }

    @Test
    public void duplicateIdentityIsRejectedByName() {
        final V3Rig r = V3Rig.open();
        r.openSession(51, SessionClass.BRIDGE);
        r.openSession(52, SessionClass.BRIDGE);
        assertEquals(AdminResult.ACCEPTED, registerAck(r, 51, SessionClass.BRIDGE, 7).result());
        assertEquals("the same session re-registering is DUPLICATE as before", AdminResult.DUPLICATE,
                registerAck(r, 51, SessionClass.BRIDGE, 7).result());
        final AssetsEngineEvent.AdminAck second = registerAck(r, 52, SessionClass.BRIDGE, 7);
        assertEquals(AdminResult.REJECTED, second.result());
        assertEquals("rejected by name", CoreRejectReason.DUPLICATE_IDENTITY.code(), second.reason());
        assertEquals("DUPLICATE_IDENTITY", CoreRejectReason.DUPLICATE_IDENTITY.name());
        assertTrue("the transport closes the refused session", r.rejectedSessions.contains(52L));
        assertFalse(r.engine.sessions().get(52).isRegistered());
        assertEquals("another identity is fine", AdminResult.ACCEPTED, registerAck(r, 52, SessionClass.BRIDGE, 8).result());
        assertEquals(2, r.engine.sessions().registeredCount(SessionClass.BRIDGE));
        // the first lane's session closes (a restart after its session timeout): its identity is free again
        r.engine.sessions().close(51);
        assertEquals(1, r.engine.sessions().registeredCount(SessionClass.BRIDGE));
        r.openSession(53, SessionClass.BRIDGE);
        assertEquals("a restarted lane re-registers its identity", AdminResult.ACCEPTED,
                registerAck(r, 53, SessionClass.BRIDGE, 7).result());
        // only BRIDGE identities are exclusive
        r.openSession(61, SessionClass.GATEWAY);
        r.openSession(62, SessionClass.GATEWAY);
        assertEquals(AdminResult.ACCEPTED, registerAck(r, 61, SessionClass.GATEWAY, 7).result());
        assertEquals(AdminResult.ACCEPTED, registerAck(r, 62, SessionClass.GATEWAY, 7).result());
        assertEquals(2, r.engine.sessions().registeredCount(SessionClass.GATEWAY));
        // the class check keeps its meaning (C8): a GATEWAY principal asserting BRIDGE is UNAUTHORIZED_CLASS, not an identity clash
        r.openSession(63, SessionClass.GATEWAY);
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS.code(), registerAck(r, 63, SessionClass.BRIDGE, 7).reason());
    }

    /** Review FIX-2: below behaviour 3 a logged RegisterSession replays with the result it was recorded with. */
    @Test
    public void duplicateIdentityIsNotExclusiveBelowBehaviourThree() {
        final V3Rig r = V3Rig.v2();
        r.openSession(51, SessionClass.BRIDGE);
        r.openSession(52, SessionClass.BRIDGE);
        assertEquals(AdminResult.ACCEPTED, registerAck(r, 51, SessionClass.BRIDGE, 3).result());
        assertEquals("v1/v2: the second holder is ACCEPTED as before S10'", AdminResult.ACCEPTED,
                registerAck(r, 52, SessionClass.BRIDGE, 3).result());
        assertEquals(2, r.engine.sessions().registeredCount(SessionClass.BRIDGE));
        // the same engine after the switch to behaviour 3: the rule applies from that apply on
        com.openexchange.assets.determinism.V3Genesis.widen(r.engine);
        r.engine.applyCoreConfig(com.openexchange.assets.determinism.V3Genesis.config(3, 3), r.now);
        assertTrue(r.engine.behaviorV3());
        r.openSession(53, SessionClass.BRIDGE);
        assertEquals("v3: one live session per BRIDGE identity", CoreRejectReason.DUPLICATE_IDENTITY.code(),
                registerAck(r, 53, SessionClass.BRIDGE, 3).reason());
    }

    @Test
    public void registeredCountsFollowRevokeAndRestore() {
        final V3Rig r = V3Rig.open();
        r.openSession(51, SessionClass.BRIDGE);
        r.openSession(52, SessionClass.TOOL);
        registerAck(r, 51, SessionClass.BRIDGE, 1);
        registerAck(r, 52, SessionClass.TOOL, 0);
        final AssetsEngine restored = S8Support.restore(S8Support.image(r.engine));
        assertEquals(1, restored.sessions().registeredCount(SessionClass.BRIDGE));
        assertEquals(1, restored.sessions().registeredCount(SessionClass.TOOL));
        assertEquals(0, restored.sessions().registeredCount(SessionClass.ADMIN));
    }

    // ---------------------------------------------------------------- section 12 (RC-1, rule 5)

    static byte[] image(final V3Rig r) {
        return S8Support.image(r.engine);
    }

    /** {payload offset, payload length} of section {@code id}, or null. */
    static int[] section(final byte[] img, final int id) {
        final UnsafeBuffer b = new UnsafeBuffer(img);
        final int header = 8 + 7 * 8 + 1 + 4;
        final int count = b.getInt(header - 4);
        int pos = header;
        for (int i = 0; i < count; i++) {
            final int sid = b.getShort(pos);
            final int len = b.getInt(pos + 2);
            if (sid == id) {
                return new int[] {pos + 6, len};
            }
            pos += 6 + len;
        }
        return null;
    }

    @Test
    public void sectionTwelveRoundTripsAndIsAFunctionOfState() {
        final V3Rig a = book();
        assertEquals(AdminResult.ACCEPTED, gap(a, 72, 2, 4, 7));
        assertEquals(AdminResult.ACCEPTED, gap(a, 75, 5, 4, 9));
        final V3Rig b = book();
        assertEquals(AdminResult.ACCEPTED, gap(b, 75, 5, 4, 9));
        assertEquals(AdminResult.ACCEPTED, gap(b, 72, 2, 4, 7));
        final byte[] ia = image(a);
        final int[] sa = section(ia, BalanceSnapshotCodec.SECTION_FEED_HALTS);
        final int[] sb = section(image(b), BalanceSnapshotCodec.SECTION_FEED_HALTS);
        assertEquals("section 12 written under v3", FeedState.HALT_SECTION_HEAD_BYTES + 2 * FeedState.HALT_SECTION_ENTRY_BYTES, sa[1]);
        assertEquals("version byte 1", 1, ia[sa[0]]);
        // the records' log positions differ by arrival order; the rest of the section is a function of the state
        final UnsafeBuffer ba = new UnsafeBuffer(ia);
        final UnsafeBuffer bb = new UnsafeBuffer(image(b));
        for (int i = 0; i < 2; i++) {
            for (int field = 0; field < 4; field++) {
                final int off = FeedState.HALT_SECTION_HEAD_BYTES + i * FeedState.HALT_SECTION_ENTRY_BYTES + field * 8;
                assertEquals("record " + i + " field " + field, ba.getLong(sa[0] + off), bb.getLong(sb[0] + off));
            }
        }
        // round trip: the restored engine writes the same image and carries the same table and holder set
        final AssetsEngine restored = S8Support.roundTrip(a.engine, "section 12");
        assertEquals(2, restored.feed().laneHaltCount());
        assertEquals(2L, restored.feed().laneHaltIdentity(0));
        assertEquals(9L, restored.feed().laneHaltSeen(1));
        assertEquals(FeedState.HOLDER_GAP, restored.feed().holders());
        assertEquals("layout v5 tag", BalanceSnapshotCodec.LAYOUT_V5_TAG, new UnsafeBuffer(ia).getLong(0));
    }

    @Test
    public void overflowAndHoldersRoundTrip() {
        final V3Rig r = book();
        for (int i = 1; i <= 9; i++) {
            gap(r, 100 + i, i, 4, 6 + i);
        }
        halt(r, 120, 20, FeedHaltReason.SOURCE_SCHEMA, 4, 4);
        final AssetsEngine restored = S8Support.roundTrip(r.engine, "overflow");
        assertTrue(restored.feed().haltOverflow());
        assertEquals(15L, restored.feed().overflowSeen());
        assertEquals(FeedState.HOLDER_GAP | FeedState.HOLDER_OVERFLOW | FeedState.HOLDER_NONGAP_AWAITING_OPEN,
                restored.feed().holders());
    }

    @Test
    public void unknownHaltSectionVersionIsRefused() {
        final IllegalStateException e = assertThrows(IllegalStateException.class, LaneHaltTableTest::restoreWithVersionPlusOne);
        assertTrue(e.getMessage(), e.getMessage().contains("section 12"));
        assertTrue(e.getMessage(), e.getMessage().contains("version 2"));
    }

    /** RC-5's version + 1 image: a record, then section 12's version byte set to 2. */
    static AssetsEngine restoreWithVersionPlusOne() {
        final V3Rig r = book();
        gap(r, 77, 1, 4, 9);
        final byte[] img = image(r);
        final int[] s = section(img, BalanceSnapshotCodec.SECTION_FEED_HALTS);
        img[s[0]] = (byte) (FeedState.HALT_SECTION_VERSION + 1);
        return S8Support.restore(img);
    }

    @Test
    public void restoreRefusesAnInconsistentHolderSet() {
        final V3Rig r = book();
        gap(r, 77, 1, 4, 9);
        final byte[] good = image(r);
        final int[] s = section(good, BalanceSnapshotCodec.SECTION_FEED_HALTS);
        // a record without GAP in the set, while the bit is set: "bit set iff holders" breaks first
        final byte[] noHolder = good.clone();
        noHolder[s[0] + 1] = 0;
        assertRefused(noHolder, "BRIDGE_HALT bit set with holder set 0x0");
        // a record with OVERFLOW only in the set
        final byte[] wrongHolder = good.clone();
        wrongHolder[s[0] + 1] = (byte) FeedState.HOLDER_OVERFLOW;
        assertRefused(wrongHolder, "lane-halt records without GAP in the holder set");
        // an unknown holder bit, an overflow byte 2, a reserved byte: refused at the read
        final byte[] unknownBit = good.clone();
        unknownBit[s[0] + 1] = (byte) 0x9;
        assertRefused(unknownBit, "holders 0x9");
        final byte[] overflowTwo = good.clone();
        overflowTwo[s[0] + 2] = 2;
        assertRefused(overflowTwo, "overflow 2");
        // overflow flag without OVERFLOW in the set
        final byte[] overflowNoHolder = good.clone();
        overflowNoHolder[s[0] + 2] = 1;
        assertRefused(overflowNoHolder, "overflow record without OVERFLOW in the holder set");
        // a record at its reach
        final byte[] reached = good.clone();
        new UnsafeBuffer(reached).putLong(s[0] + FeedState.HALT_SECTION_HEAD_BYTES + 8, 2L);  // expected 2
        new UnsafeBuffer(reached).putLong(s[0] + FeedState.HALT_SECTION_HEAD_BYTES + 16, 4L); // seen 4: c = 3 = seen - 1
        assertRefused(reached, "at its reach");
        // a GAP record but the GLOBAL bit cleared in section 5
        final V3Rig open = book();
        final byte[] openImage = image(open);
        final int[] so = section(openImage, BalanceSnapshotCodec.SECTION_FEED_HALTS);
        openImage[so[0] + 1] = (byte) FeedState.HOLDER_NONGAP_AWAITING_OPEN;
        assertRefused(openImage, "BRIDGE_HALT bit clear with holder set 0x2");
        // a v4 tag
        final byte[] v4 = good.clone();
        new UnsafeBuffer(v4).putLong(0, BalanceSnapshotCodec.LAYOUT_V4_TAG);
        assertRefused(v4, "reads only v5");
    }

    @Test
    public void sectionTwelveIsMandatoryUnderVThreeAndRefusedBelow() {
        final byte[] img = image(book());
        final int[] s = section(img, BalanceSnapshotCodec.SECTION_FEED_HALTS);
        final byte[] cut = java.util.Arrays.copyOf(img, s[0] - 6); // section 12 is the last one
        final UnsafeBuffer c = new UnsafeBuffer(cut);
        c.putInt(8 + 7 * 8 + 1, c.getInt(8 + 7 * 8 + 1) - 1);
        assertRefused(cut, "no FEED_HALTS section (12)");
        final byte[] v2 = image(V3Rig.v2());
        final byte[] longer = java.util.Arrays.copyOf(v2, v2.length + 6 + FeedState.HALT_SECTION_HEAD_BYTES);
        final UnsafeBuffer b = new UnsafeBuffer(longer);
        b.putShort(v2.length, BalanceSnapshotCodec.SECTION_FEED_HALTS);
        b.putInt(v2.length + 2, FeedState.HALT_SECTION_HEAD_BYTES);
        b.putByte(v2.length + 6, (byte) 1);
        b.putInt(8 + 7 * 8 + 1, b.getInt(8 + 7 * 8 + 1) + 1);
        assertRefused(longer, "below behaviour 3 carries a FEED_HALTS section (12)");
    }

    private static void assertRefused(final byte[] img, final String text) {
        final IllegalStateException e = assertThrows(text, IllegalStateException.class, () -> S8Support.restore(img));
        assertTrue("expected \"" + text + "\" in: " + e.getMessage(), e.getMessage().contains(text));
    }

    @Test
    public void switchToThreeWithAKeptBridgeHaltGivesItAHolder() {
        // a v2 core whose GLOBAL was SUSPENDED/BRIDGE_HALT by ADMIN (the v2 overwrite rule) switches to v3: the kept bit
        // has a holder (NONGAP_AWAITING_OPEN), so the restore invariant "bit iff holders" holds and only ADMIN OPEN clears it
        final V3Rig r = V3Rig.v2();
        r.engine.applyOrderEntryGate(1L, G, GateState.SUSPENDED, GateReason.BRIDGE_HALT, r.now);
        com.openexchange.assets.determinism.V3Genesis.widen(r.engine);
        r.engine.applyCoreConfig(com.openexchange.assets.determinism.V3Genesis.config(3, 3), r.now);
        assertTrue(r.engine.behaviorV3());
        assertTrue((r.globalReasons() & (1 << GateReason.BRIDGE_HALT.code())) != 0);
        assertEquals(FeedState.HOLDER_NONGAP_AWAITING_OPEN, r.feed().holders());
        r.engine.verifyRestoredInvariants();
        assertNotEquals(GateState.OPEN, state(r));
    }
}

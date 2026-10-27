// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderStatus;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 section 3 and 4 (D1, D2, 3.3-3.5, 4.2, 5.4): the replicated journalSeq cursor of behaviour v3. Every row is
 * driven through the 2.4 entry points (or, for 62 and the decode-poison rows, through the demuxer with real v7 frames),
 * and each asserts what applied, what was refused, what latched and where the cursor and the watermark stand.
 */
public class FeedCursorTest {

    private static final int T61 = 61;
    private static final int T63 = 63;
    private static final int T64 = 64;
    private static final int T67 = 67;
    private static final int T68 = 68;
    private static final int[] TEMPLATES = {T61, 62, T63, T64, T67, T68};

    /** Deliver one entry of {@code template} at (js, epoch, g) on the two-party book; returns the entry point's verdict. */
    private static boolean deliver(final V3Rig r, final int template, final long js, final long epoch, final long g) {
        switch (template) {
            case T61:
            case 62:
                return r.sellHitsAt(js, epoch, g, r.engine.getLastAppliedTradeId() + 1, ONE / 2);
            case T63:
                return r.terminalAt(js, epoch, g, 1, 101, 1, V3Rig.ME_CANCELLED, 0);
            case T64:
                return r.outcomeAt(js, epoch, g, 1, 0, V3Rig.KIND_CREATE, 0, 101, 0, false);
            case T67:
                return r.releaseAt(js, epoch, g);
            case T68:
                return r.configAt(js, epoch, g, 2, V3Genesis.ME_BEHAVIOR, V3Genesis.ME_CAPACITY, V3Genesis.ME_MAX_WIRE,
                        ConfigDecision.BUMP);
            default:
                throw new IllegalArgumentException("template " + template);
        }
    }

    private static V3Rig book() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        // (the cursor stands at 3 here under v3; asserted by the rows themselves, so a base run fails at the rule)
        return r;
    }

    @Test
    public void epochMismatchRefusedLatchedGated() {
        for (final int t : TEMPLATES) {
            final V3Rig r = book();
            final long c = r.cursor();
            final long p = r.engine.getConsumePosition();
            final long l = r.engine.getLastAppliedTradeId();
            final long foreign = EPOCH + 7;
            r.mark();
            final boolean verdict = deliver(r, t, c + 1, foreign, r.egress + 1);
            final String what = "template " + t + " with meEpoch " + foreign + " (bound " + EPOCH + ")";
            assertFalse(what + ": D1 row 3 REFUSE (the entry point returns false)", verdict);
            final AssetsEngineEvent.SettleRejected sr = r.first(AssetsEngineEvent.SettleRejected.class);
            assertEquals(what + ": SettleRejected reason", SettleRejectReason.ME_EPOCH_MISMATCH, sr.reason());
            assertEquals(what + ": detail = the bound epoch", EPOCH, sr.detail());
            assertEquals(what + ": journalPosition = journalSeq (S1 4.1)", c + 1, sr.journalPosition());
            assertEquals(what + ": nothing settled", 0, r.count(AssetsEngineEvent.Settled.class));
            assertEquals(what + ": no order event", 0, r.count(AssetsEngineEvent.OrderEvent.class));
            assertEquals(what + ": cursor unchanged", c, r.cursor());
            assertEquals(what + ": watermark unchanged", p, r.engine.getConsumePosition());
            assertEquals(what + ": high-water unchanged", l, r.engine.getLastAppliedTradeId());
            r.assertLatch(what, FeedHaltReason.EPOCH_MISMATCH, c + 1, c + 1, 0, 0);
            assertEquals(what + ": latchMeEpochSeen", foreign, r.feed().latchMeEpochSeen());
            r.assertGloballyHalted(what);
            assertNotNull(what + ": the order is untouched", r.order(1));
            assertEquals(what + ": the order keeps its leg", 101L, r.order(1).currentLegId);
        }
    }

    @Test
    public void nonPositiveSeqOrEpochIsSourceSchema() {
        final long[][] rows = {{0L, EPOCH}, {-3L, EPOCH}, {4L, 0L}, {4L, -1L}};
        for (final long[] row : rows) {
            for (final int t : TEMPLATES) {
                final V3Rig r = book();
                r.mark();
                final String what = "template " + t + " jseq " + row[0] + " meEpoch " + row[1];
                assertFalse(what + ": D1 row 1 REFUSE", deliver(r, t, row[0], row[1], r.egress + 1));
                assertEquals(what + ": decode-grade, no SettleRejected", 0, r.count(AssetsEngineEvent.SettleRejected.class));
                assertEquals(what + ": cursor unchanged", 3L, r.cursor());
                r.assertLatch(what, FeedHaltReason.SOURCE_SCHEMA, 4, row[0], 0, 0);
                r.assertGloballyHalted(what);
                assertEquals(what + ": the bound epoch never changes", EPOCH, r.feed().boundMeEpoch());
            }
        }
    }

    @Test
    public void bindsOnlyOnFreshConfigAtSeqOne() {
        // D2 row 1: the FRESH record at journalSeq 1 binds, applies 68 and commits (3.3)
        final V3Rig r = V3Rig.unbound();
        assertEquals("precondition: unbound", 0L, r.feed().boundMeEpoch());
        r.mark();
        assertTrue("D2 row 1: BIND", r.configAt(1, EPOCH, 40, 1, 2, 4096, 12, ConfigDecision.FRESH));
        assertEquals("bound epoch", EPOCH, r.feed().boundMeEpoch());
        assertEquals("the bind is a commit: cursor 1", 1L, r.cursor());
        assertEquals("the bind is a commit: watermark = egressSeq", 40L, r.engine.getConsumePosition());
        assertEquals("4.2: meConfigVersion", 1L, r.feed().meConfigVersion());
        assertEquals("4.2: meBehaviorVersion", 2L, r.feed().meBehaviorVersion());
        assertEquals("4.2: meLedgerCapacity", 4096L, r.feed().meLedgerCapacity());
        assertEquals("4.2: meMaxWireVersion", 12, r.feed().meMaxWireVersion());
        r.assertNotLatched("bind");
        assertEquals("68 emits nothing", 0, r.events().size());

        // D2 row 2: journalSeq 1 that is not a valid FRESH, and any other template at journalSeq 1
        final Object[][] notFresh = {
                {"68 BUMP at 1", ConfigDecision.BUMP, 2L, 4096L, 12},
                {"68 ADOPT at 1", ConfigDecision.ADOPT, 2L, 4096L, 12},
                {"68 FRESH behaviour 1", ConfigDecision.FRESH, 1L, 4096L, 12},
                {"68 FRESH capacity 0", ConfigDecision.FRESH, 2L, 0L, 12},
                {"68 FRESH maxWire 11", ConfigDecision.FRESH, 2L, 4096L, 11},
        };
        for (final Object[] row : notFresh) {
            final V3Rig u = V3Rig.unbound();
            u.mark();
            final String what = (String) row[0];
            assertFalse(what + ": D2 row 2 REFUSE", u.configAt(1, EPOCH, 5, 1, (Long) row[2], (Long) row[3], (Integer) row[4],
                    (ConfigDecision) row[1]));
            assertUnboundRefusal(u, what, 1);
            u.assertLatch(what, FeedHaltReason.SOURCE_UNBOUND, 1, 1, 0, 0);
        }
        // 3.8: the FRESH record must carry the recorded genesisId as its epoch (S2: meEpoch := genesisId)
        final V3Rig g = V3Rig.unbound();
        g.mark();
        assertFalse("FRESH under an epoch other than the genesisId", g.configAt(1, EPOCH + 9, 5, 1, 2, 4096, 12,
                ConfigDecision.FRESH));
        assertUnboundRefusal(g, "FRESH under a foreign epoch", 1);
        g.assertLatch("FRESH under a foreign epoch", FeedHaltReason.SOURCE_UNBOUND, 1, 1, 0, 0);
        for (final int t : new int[] {T61, T63, T64, T67}) {
            final V3Rig u = V3Rig.unbound();
            u.mark();
            final String what = "template " + t + " at journalSeq 1 while unbound";
            assertFalse(what + ": D2 row 2 REFUSE", deliver(u, t, 1, EPOCH, 5));
            assertUnboundRefusal(u, what, 1);
            u.assertLatch(what, FeedHaltReason.SOURCE_UNBOUND, 1, 1, 0, 0);
        }

        // D2 row 3: an unbound core seeing journalSeq > 1 latches GAP (expected 1), it never applies
        for (final int t : TEMPLATES) {
            final V3Rig u = V3Rig.unbound();
            u.deposit(1, V3Rig.USD, 1_000 * ONE);
            u.mark();
            final String what = "template " + t + " at journalSeq 7 while unbound";
            assertFalse(what + ": D2 row 3 REFUSE", deliver(u, t, 7, EPOCH, 5));
            assertUnboundRefusal(u, what, 7);
            u.assertLatch(what, FeedHaltReason.GAP, 1, 7, 0, 0);
            assertEquals(what + ": nothing settled", 0, u.count(AssetsEngineEvent.Settled.class));
        }
    }

    private static void assertUnboundRefusal(final V3Rig u, final String what, final long js) {
        final AssetsEngineEvent.SettleRejected sr = u.first(AssetsEngineEvent.SettleRejected.class);
        assertEquals(what + ": FEED_UNBOUND", SettleRejectReason.FEED_UNBOUND, sr.reason());
        assertEquals(what + ": detail 1", 1L, sr.detail());
        assertEquals(what + ": journalPosition", js, sr.journalPosition());
        assertEquals(what + ": still unbound", 0L, u.feed().boundMeEpoch());
        assertEquals(what + ": cursor 0", 0L, u.cursor());
        u.assertGloballyHalted(what);
    }

    @Test
    public void genesisRaceSeqTwoFirstHealsAfterRewind() {
        final V3Rig r = V3Rig.unbound();
        // seq 1 was streamed while the core was still at v2 (unserved); the bridge's next entry is seq 2
        r.mark();
        assertFalse("D2 row 3", r.configAt(2, EPOCH, 9, 2, 2, 4096, 12, ConfigDecision.BUMP));
        r.assertLatch("seq 2 first", FeedHaltReason.GAP, 1, 2, 0, 0);
        // the fence rewinds to the reported cursor 0: seq 1 binds, the GAP reach point (c >= seen - 1) is passed at the bind
        r.mark();
        assertTrue("seq 1 binds", r.configAt(1, EPOCH, 8, 1, 2, 4096, 12, ConfigDecision.FRESH));
        r.assertNotLatched("the bind reach-clears GAP{1, 2}");
        assertEquals("auto clear counted", 1L, r.feed().latchAutoClearCount());
        final List<AssetsEngineEvent.OrderFault> f = r.feedHaltFaults();
        assertEquals("the reach-clear BROADCASTs one audit OrderFault", 1, f.size());
        assertEquals("audit tradeId = cursor", 1L, f.get(0).tradeId());
        assertEquals("audit amount 0 (a clear)", 0L, f.get(0).amount());
        assertEquals(new AssetsEngineEvent.FeedLatchCleared(FeedHaltReason.GAP, 0L, true),
                r.first(AssetsEngineEvent.FeedLatchCleared.class));
        assertTrue("seq 2 resent applies", r.configAt(2, EPOCH, 9, 2, 2, 4096, 12, ConfigDecision.BUMP));
        assertEquals(2L, r.cursor());
        assertEquals(2L, r.feed().meConfigVersion());
        // the BRIDGE_HALT bit stays until ADMIN OPEN, which passes now that the epoch is bound and the latch is gone
        assertTrue(r.globalHas(GateReason.BRIDGE_HALT));
        r.mark();
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        assertEquals(com.openexchange.assets.domain.AdminResult.ACCEPTED, r.onlyAck().result());
        assertEquals(GateState.OPEN, r.engine.gate().state(OrderEntryGate.GLOBAL));
    }

    @Test
    public void engineConfigBumpRulesMirrorS2() {
        final V3Rig ok = V3Rig.bound();
        assertTrue("BUMP configVersion 2", ok.configAt(2, EPOCH, 3, 2, 2, 4096, 12, ConfigDecision.BUMP));
        assertTrue("BUMP raising maxWireVersion", ok.configAt(3, EPOCH, 4, 3, 2, 4096, 14, ConfigDecision.BUMP));
        assertEquals(3L, ok.cursor());
        assertEquals(3L, ok.feed().meConfigVersion());
        assertEquals(14, ok.feed().meMaxWireVersion());
        ok.assertNotLatched("valid bumps");

        final Object[][] bad = {
                {"configVersion not raised", 1L, 2L, 4096L, 12, ConfigDecision.BUMP},
                {"behaviour changed", 2L, 3L, 4096L, 12, ConfigDecision.BUMP},
                {"capacity changed", 2L, 2L, 8192L, 12, ConfigDecision.BUMP},
                {"maxWireVersion lowered", 2L, 2L, 4096L, 12, ConfigDecision.BUMP}, // applied below after a raise
                {"FRESH after seq 1", 2L, 2L, 4096L, 12, ConfigDecision.FRESH},
                {"ADOPT is never journaled", 2L, 2L, 4096L, 12, ConfigDecision.ADOPT},
                {"SWITCH is never journaled", 2L, 2L, 4096L, 12, ConfigDecision.SWITCH},
                {"configVersion 0", 0L, 2L, 4096L, 12, ConfigDecision.BUMP},
                {"capacity above 2^31 - 1", 2L, 2L, 1L << 31, 12, ConfigDecision.BUMP},
                {"maxWireVersion 65535", 2L, 2L, 4096L, 65535, ConfigDecision.BUMP},
        };
        for (final Object[] row : bad) {
            final V3Rig r = V3Rig.bound();
            final String what = (String) row[0];
            long js = 2;
            if (what.equals("maxWireVersion lowered")) {
                assertTrue(r.configAt(2, EPOCH, 3, 2, 2, 4096, 13, ConfigDecision.BUMP));
                js = 3;
                row[1] = 3L;
            }
            final long c = r.cursor();
            r.mark();
            assertFalse(what + ": 4.2 value rule REFUSE", r.configAt(js, EPOCH, 10, (Long) row[1], (Long) row[2], (Long) row[3],
                    (Integer) row[4], (ConfigDecision) row[5]));
            assertEquals(what + ": cursor unchanged", c, r.cursor());
            r.assertLatch(what, FeedHaltReason.SOURCE_SCHEMA, js, js, js, 0);
            assertEquals(what + ": no SettleRejected (decode-grade)", 0, r.count(AssetsEngineEvent.SettleRejected.class));
        }
    }

    @Test
    public void gapOnTerminalOrOutcomeLatchesAndGates() {
        for (final int t : new int[] {T63, T64}) {
            final V3Rig r = book();
            r.mark();
            final String what = "template " + t + " at journalSeq 6 with cursor 3";
            assertFalse(what + ": D1 row 5 REFUSE", deliver(r, t, 6, EPOCH, r.egress + 1));
            final AssetsEngineEvent.SettleRejected sr = r.first(AssetsEngineEvent.SettleRejected.class);
            assertEquals(what, SettleRejectReason.FEED_GAP, sr.reason());
            assertEquals(what + ": detail = c + 1", 4L, sr.detail());
            assertEquals(what + ": journalPosition", 6L, sr.journalPosition());
            assertEquals(what + ": tradeId 0 on 63/64", 0L, sr.tradeId());
            assertEquals(what + ": marketId", V3Rig.MKT, sr.marketId());
            r.assertLatch(what, FeedHaltReason.GAP, 4, 6, 0, 0);
            r.assertGloballyHalted(what);
            assertEquals(what + ": no order event", 0, r.count(AssetsEngineEvent.OrderEvent.class));
            assertEquals(what + ": gap counted", 1L, r.feed().gapCount());
            final AssetsEngineEvent.OrderFault f = r.feedHaltFaults().get(0);
            assertEquals(what + ": OrderFault tradeId = seen", 6L, f.tradeId());
            assertEquals(what + ": OrderFault amount = expected", 4L, f.amount());
        }
    }

    @Test
    public void tradeIdDensityBreachLatchesSettleRefused() {
        // a gap in tradeId at the exactly-next journalSeq
        final V3Rig r = book();
        assertTrue(r.sellHits(1, ONE / 4));
        final long c = r.cursor();
        r.mark();
        assertFalse("density breach REFUSE", r.sellHits(3, ONE / 4));
        final AssetsEngineEvent.SettleRejected sr = r.first(AssetsEngineEvent.SettleRejected.class);
        assertEquals(SettleRejectReason.TRADE_GAP, sr.reason());
        assertEquals("detail = last + 1", 2L, sr.detail());
        assertEquals("journalPosition = journalSeq under v3", c + 1, sr.journalPosition());
        assertEquals("SettleRejected names the frame's tradeId", 3L, sr.tradeId());
        assertEquals("cursor unchanged", c, r.cursor());
        assertEquals("high-water unchanged", 1L, r.engine.getLastAppliedTradeId());
        r.assertLatch("density", FeedHaltReason.SETTLE_REFUSED, c + 1, c + 1, c + 1, 2L);
        r.assertGloballyHalted("density");
        assertEquals(1L, r.feed().settleRefusedCount());

        // tradeId at or below the high-water at the exactly-next journalSeq: refused loudly, never swallowed
        final V3Rig d = book();
        assertTrue(d.sellHits(1, ONE / 4));
        d.mark();
        assertFalse("tradeId <= last at s == c + 1 REFUSE", d.sellHits(1, ONE / 4));
        assertEquals(SettleRejectReason.TRADE_GAP, d.first(AssetsEngineEvent.SettleRejected.class).reason());
        d.assertLatch("re-sent tradeId", FeedHaltReason.SETTLE_REFUSED, d.cursor() + 1, d.cursor() + 1, d.cursor() + 1, 2L);
        assertEquals("one settle only", 0, d.count(AssetsEngineEvent.Settled.class));

        // a ledger.settle refusal (unknown market) latches SETTLE_REFUSED too
        final V3Rig m = book();
        m.mark();
        assertFalse(m.engine.onMeTrade(m.egress + 1, 4, 1, 99, 202, 2, 101, 1, 100 * ONE, ONE, false, m.now, 2, 1, EPOCH, m.now));
        assertEquals(SettleRejectReason.UNKNOWN_MARKET, m.first(AssetsEngineEvent.SettleRejected.class).reason());
        assertEquals("the settle refusal echoes journalSeq", 4L, m.first(AssetsEngineEvent.SettleRejected.class).journalPosition());
        m.assertLatch("ledger refusal", FeedHaltReason.SETTLE_REFUSED, 4, 4, 4, 1L);
    }

    @Test
    public void appliedEntriesAdvanceConsumePositionDuplicatesDoNot() {
        final V3Rig r = book();
        final long p0 = r.engine.getConsumePosition();
        assertEquals("the bind and the two acks committed their egressSeq", 3L, p0);
        assertTrue(r.sellHitsAt(4, EPOCH, 500, 1, ONE / 4));
        assertEquals("commit: P := max(P, g)", 500L, r.engine.getConsumePosition());
        assertEquals(4L, r.cursor());
        r.mark();
        assertTrue("DUPLICATE returns true", r.sellHitsAt(4, EPOCH, 500, 1, ONE / 4));
        assertEquals("a DUPLICATE commits nothing: P", 500L, r.engine.getConsumePosition());
        assertEquals("a DUPLICATE commits nothing: c", 4L, r.cursor());
        assertEquals("a DUPLICATE emits nothing", 0, r.events().size());
        assertEquals(1L, r.feed().duplicateCount(T61));
        assertTrue(r.outcomeAt(3, EPOCH, 3, 2, 0, V3Rig.KIND_CREATE, 0, 202, 0, false));
        assertEquals("a duplicate outcome below the cursor is a no-op", 1L, r.feed().duplicateCount(T64));
        assertNotNull(r.order(1));
        assertTrue(r.terminalAt(5, EPOCH, 700, 1, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertEquals(700L, r.engine.getConsumePosition());
        assertEquals(5L, r.cursor());
        assertNull("the exactly-next terminal applied", r.order(1));
    }

    @Test
    public void egressRegressionAtNextSeqLatchesUnskippable() {
        final V3Rig r = book();
        assertTrue(r.sellHitsAt(4, EPOCH, 1_000, 1, ONE / 4));
        r.mark();
        assertFalse("D1 row 6 REFUSE", r.sellHitsAt(5, EPOCH, 900, 2, ONE / 4));
        assertEquals(1L, r.feed().egressRegressionCount());
        assertEquals("decode-grade: no SettleRejected", 0, r.count(AssetsEngineEvent.SettleRejected.class));
        r.assertLatch("egress regression", FeedHaltReason.SOURCE_SCHEMA, 5, 5, 0, 0);
        assertEquals(1L, r.engine.getLastAppliedTradeId());
        // no 76 can consume the entry: a skip-0 latch named with journalSeq != 0 is D3b row 6
        r.mark();
        r.clear(FeedHaltReason.SOURCE_SCHEMA, 5);
        assertEquals(com.openexchange.assets.domain.AdminResult.REJECTED, r.onlyAck().result());
        assertEquals(4L, r.cursor());
        // a correct copy of c + 1 from another lane applies and reach-clears (skip-0 row: c >= expected)
        r.mark();
        assertTrue(r.sellHitsAt(5, EPOCH, 1_100, 2, ONE / 4));
        assertEquals(5L, r.cursor());
        assertEquals(2L, r.engine.getLastAppliedTradeId());
        r.assertNotLatched("correct copy");
        assertEquals(new AssetsEngineEvent.FeedLatchCleared(FeedHaltReason.SOURCE_SCHEMA, 0L, true),
                r.first(AssetsEngineEvent.FeedLatchCleared.class));
    }

    @Test
    public void batch62AppliesEntryByEntryStopsAtFirstRefusal() {
        // [duplicate, applies, tradeId gap (refused), never looked at]
        final V3Rig r = book();
        assertTrue(r.sellHitsAt(4, EPOCH, 10, 1, ONE / 8));
        r.mark();
        r.dispatchTrusted(r.tradeBatch(List.of(
                new V3Rig.BatchEntry(4, EPOCH, 10, 1, 0, ONE / 8),
                new V3Rig.BatchEntry(5, EPOCH, 11, 2, 0, ONE / 8),
                new V3Rig.BatchEntry(6, EPOCH, 12, 9, 0, ONE / 8),
                new V3Rig.BatchEntry(7, EPOCH, 13, 3, 0, ONE / 8))));
        assertEquals("entry 2 applied, entry 3 refused, entry 4 not looked at", 5L, r.cursor());
        assertEquals(2L, r.engine.getLastAppliedTradeId());
        assertEquals("one settle from the batch", 1, r.count(AssetsEngineEvent.Settled.class));
        assertEquals(1L, r.feed().duplicateCount(T61) + r.feed().duplicateCount(62));
        r.assertLatch("batch refusal", FeedHaltReason.SETTLE_REFUSED, 6, 6, 6, 3L);
        assertEquals("one refusal only (the batch stopped)", 1, r.count(AssetsEngineEvent.SettleRejected.class));

        // a latching poison stops the batch: [applies, poisoned next entry, valid]
        final V3Rig p = book();
        p.mark();
        p.dispatchTrusted(p.tradeBatch(List.of(
                new V3Rig.BatchEntry(4, EPOCH, 10, 1, 0, ONE / 8),
                new V3Rig.BatchEntry(5, EPOCH, 11, 2, 2, ONE / 8),
                new V3Rig.BatchEntry(6, EPOCH, 12, 3, 0, ONE / 8))));
        assertEquals("entry 1 applied, entry 2 poisoned at c + 1, the batch stops", 4L, p.cursor());
        assertEquals(1L, p.engine.getLastAppliedTradeId());
        p.assertLatch("batch poison", FeedHaltReason.SOURCE_SCHEMA, 5, 5, 5, 2L);
        assertEquals("the poison is counted as a decode reject too", 1L, p.demuxer.decodeRejectCount());

        // a counted-only poison (a corrupt duplicate) is passed over: [poisoned duplicate, applies]
        final V3Rig q = book();
        assertTrue(q.sellHitsAt(4, EPOCH, 10, 1, ONE / 8));
        q.dispatchTrusted(q.tradeBatch(List.of(
                new V3Rig.BatchEntry(4, EPOCH, 10, 1, 7, ONE / 8),
                new V3Rig.BatchEntry(5, EPOCH, 11, 2, 0, ONE / 8))));
        assertEquals("the duplicate poison is only counted, entry 2 applies", 5L, q.cursor());
        q.assertNotLatched("counted-only poison");
        assertEquals(1L, q.feed().sourceSchemaCount());
    }

    @Test
    public void release67ConsumesTheCursor() {
        final V3Rig r = book();
        assertNotNull("precondition: the ack of order 1 minted its M6 UP_TO", r.engine.orders().backlog().get(1));
        r.mark();
        // RC2 S8 3.2: the content is applied, the cursor consumed as in S7
        assertTrue("67 applies", r.releaseEntry(1, 0L, 0, 0));
        assertEquals(4L, r.cursor());
        assertEquals("S8 serves the content", 0L, r.feed().unservedReleaseCount());
        assertNull("C2: the dominating UP_TO confirmation drops the entry", r.engine.orders().backlog().get(1));
        assertEquals(1L, r.engine.orders().releaseConfirmedCount());
        assertEquals("one node-local release effect, no wire event", 1, r.events().size());
        assertTrue("the next entry is exactly next, not a gap", r.terminal(1, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertEquals(5L, r.cursor());
        r.assertNotLatched("67 then 63");
    }

    // ---------------------------------------------------------------- 5.4 decode poison (demuxer-level frames)

    private static int frame61(final V3Rig r, final long js, final long epoch, final long tradeId, final int takerIsBuyRaw) {
        final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(r.egress + 1).tradeId(tradeId).marketId(V3Rig.MKT).takerOrderId(202).takerUserId(2)
                .makerOrderId(101).makerUserId(1).price(100 * ONE).quantity(ONE / 4).takerIsBuy(BoolFlag.FALSE)
                .meTimestamp(r.now).takerOmsOrderId(2).makerOmsOrderId(1).meEpoch(epoch).journalSeq(js);
        r.buffer().putByte(MessageHeaderEncoder.ENCODED_LENGTH + MeJournalTradeEncoder.takerIsBuyEncodingOffset(),
                (byte) takerIsBuyRaw);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int frame63(final V3Rig r, final long js, final int status, final int rejectReason) {
        final MeJournalTerminalEncoder e = new MeJournalTerminalEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(r.egress + 1).orderId(101).userId(1).marketId(V3Rig.MKT).status(OrderStatus.CANCELLED)
                .meTimestamp(r.now).omsOrderId(1).meEpoch(EPOCH).journalSeq(js).rejectReason((short) rejectReason);
        r.buffer().putByte(MessageHeaderEncoder.ENCODED_LENGTH + MeJournalTerminalEncoder.statusEncodingOffset(), (byte) status);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int frame64(final V3Rig r, final long js, final int result, final int oldCancelled) {
        final MeCommandOutcomeEncoder e = new MeCommandOutcomeEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(r.egress + 1).commandIdHigh(1).commandIdLow(0).userId(1).omsOrderId(1).oldOrderId(0).price(0)
                .quantity(0).budget(0).marketId(V3Rig.MKT).commandKind((short) 0).orderType((short) 0).orderSide((short) 0)
                .appliedPosition(0).meTimestamp(r.now).orderId(101).status(0).reason(0).oldCancelled((short) oldCancelled)
                .result(result).meEpoch(EPOCH).journalSeq(js).timeInForce((short) 0).expectedOldRemaining(0).ledgerSize(0)
                .ledgerCapacity(0).behaviorVersion(2);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void poisonedNextEntryLatchesSourceSchemaWithTradeId() {
        // 61: takerIsBuy 2 at c + 1, raw tradeId 999 (diagnostic only); latchTradeId = L + 1
        final V3Rig a = book();
        a.mark();
        a.dispatchTrusted(frame61(a, 4, EPOCH, 999, 2));
        assertEquals(1L, a.demuxer.decodeRejectCount());
        a.assertLatch("61 poison", FeedHaltReason.SOURCE_SCHEMA, 4, 4, 4, 1L);
        assertEquals(new AssetsEngineEvent.FeedLatch(FeedHaltReason.SOURCE_SCHEMA, 4, 4, 4, 999),
                a.first(AssetsEngineEvent.FeedLatch.class));
        a.assertGloballyHalted("61 poison");
        assertEquals(3L, a.cursor());

        // 62 entry
        final V3Rig b = book();
        b.dispatchTrusted(b.tradeBatch(List.of(new V3Rig.BatchEntry(4, EPOCH, 9, 1, 3, ONE / 4))));
        b.assertLatch("62 poison", FeedHaltReason.SOURCE_SCHEMA, 4, 4, 4, 1L);

        // 63 status 5 (EXPIRED never comes from the ME under v3), and rejectReason 255
        for (final int[] row : new int[][] {{5, 0}, {3, 255}}) {
            final V3Rig c = book();
            c.dispatchTrusted(frame63(c, 4, row[0], row[1]));
            c.assertLatch("63 status " + row[0] + " rejectReason " + row[1], FeedHaltReason.SOURCE_SCHEMA, 4, 4, 4, 0L);
            assertNotNull("the order stays", c.order(1));
        }

        // 64 result 2 with oldCancelled 1
        final V3Rig d = book();
        d.dispatchTrusted(frame64(d, 4, 2, 1));
        d.assertLatch("64 result 2 + oldCancelled 1", FeedHaltReason.SOURCE_SCHEMA, 4, 4, 4, 0L);

        // 5.4 row 3: a poisoned entry that is not exactly next (a duplicate, a gap, a foreign epoch) is only counted
        final V3Rig e = book();
        e.dispatchTrusted(frame61(e, 3, EPOCH, 1, 2));
        e.dispatchTrusted(frame61(e, 9, EPOCH, 1, 2));
        e.dispatchTrusted(frame61(e, 4, EPOCH + 1, 1, 2));
        e.assertNotLatched("counted-only poison");
        assertEquals(3L, e.feed().sourceSchemaCount());
        assertEquals(3L, e.cursor());
    }

    @Test
    public void poisonedSeqOneWhileUnboundLatchesSchemaAndHealsOnValidHead() {
        final V3Rig r = V3Rig.unbound();
        r.mark();
        r.poison(T68, 1, EPOCH, 0);
        r.assertLatch("poisoned seq 1 while unbound", FeedHaltReason.SOURCE_SCHEMA, 1, 1, 0, 0);
        r.assertGloballyHalted("poisoned seq 1");
        assertEquals("still unbound", 0L, r.feed().boundMeEpoch());
        r.mark();
        assertTrue("a fixed bridge's valid seq 1 binds", r.bindFresh());
        assertEquals(EPOCH, r.feed().boundMeEpoch());
        r.assertNotLatched("the reach-clear runs at the bind (c >= expected 1)");
        assertEquals(1L, r.feed().latchAutoClearCount());
        assertEquals(1, r.feedHaltFaults().size());
        assertEquals(0L, r.feedHaltFaults().get(0).amount());
    }

    @Test
    public void feedSettleRefusalCarriesFaultKindFeedHalt() {
        final V3Rig r = book();
        r.mark();
        assertFalse(r.sellHitsAt(9, EPOCH, r.egress + 1, 1, ONE));
        final AssetsEngineEvent.OrderFault f = r.feedHaltFaults().get(0);
        assertEquals(OrderFaultKind.FEED_HALT, f.kind());
        assertEquals("orderId 0", 0L, f.orderId());
        assertEquals("userId 0", 0L, f.userId());
        assertEquals("marketId 0", 0, f.marketId());
    }
}

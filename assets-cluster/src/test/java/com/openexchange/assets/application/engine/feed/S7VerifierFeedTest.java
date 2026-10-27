// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsSbeDemuxer;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.openexchange.assets.application.engine.feed.V3Rig.BTC;
import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.MKT;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static com.openexchange.assets.application.engine.feed.V3Rig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Independent verifier tests for RC2 S7 (review.md): money safety of the behaviour-v3 feed under hostile delivery.
 *
 * <p>{@link #randomisedTwoLaneDeliveryNeverSkipsNeverDoublesAndConverges} drives a model ME journal (trades as real 62 and
 * 61 frames through the demuxer, 67, 68 BUMP and 63 entries) through two interleaved bridge lanes that lose chunks, rewind
 * to old positions, send poisoned copies, raise 66, clear GAP and skip-0 latches with 76, and take leader changes (a real
 * snapshot round trip into a fresh engine). After every step the money equals the single-lane oracle at exactly the
 * cursor, the cursor moves by applied entries only, {@code locked == sum of holds}, and a latch implies BRIDGE_HALT. At the
 * end a clean drain converges byte-identically to the oracle image, and the run with leader changes equals the same run
 * without them (replica equivalence).</p>
 */
public class S7VerifierFeedTest {

    private static final long Q = ONE / 100;
    private static final long PX = 100 * ONE;
    private static final int K_TRADE = 0;
    private static final int K_RELEASE = 1;
    private static final int K_BUMP = 2;
    private static final int K_TERM1 = 3;
    private static final int K_TERM2 = 4;
    /** The first journalSeq after FRESH (1) and the two acks (2, 3). */
    private static final long FIRST = 4;

    record Entry(int kind, long jseq, long egress, long tradeId, long cv) { }

    /** A core replica: the engine and demuxer may be replaced by a restore (leader change); the sink persists. */
    static final class Core {
        final V3Rig rig;
        AssetsEngine engine;
        AssetsSbeDemuxer demuxer;

        Core(final V3Rig rig) {
            this.rig = rig;
            this.engine = rig.engine;
            this.demuxer = rig.demuxer;
        }

        long cursor() {
            return engine.feed().lastAppliedJournalSeq();
        }

        void restore() {
            final byte[] img = image(engine);
            final AssetsEngine fresh = new AssetsEngine();
            V3Genesis.widen(fresh);
            fresh.setEventSink(rig.sink);
            final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(img), 0, img.length, fresh);
            assertEquals(img.length, d.bytesConsumed);
            fresh.verifyRestoredInvariants();
            engine = fresh;
            demuxer = new AssetsSbeDemuxer(fresh, new SettlementProjector(fresh));
            demuxer.setTransportHooks(AssetsSbeDemuxer.TransportHooks.NONE);
            assertArrayEquals("restore is invisible", img, image(engine));
        }

        long total(final long user, final int asset) {
            final var a = engine.account(user);
            return a == null ? 0L : a.available(asset) + a.locked(asset);
        }

        long locked(final long user, final int asset) {
            final var a = engine.account(user);
            return a == null ? 0L : a.locked(asset);
        }

        /** Deliver one entry through the real path: 62 (batch of one) or 61 frames for trades, the engine API otherwise. */
        boolean deliver(final Entry e, final boolean asSingle61, final int takerIsBuyRaw) {
            switch (e.kind) {
                case K_TRADE:
                    if (asSingle61) {
                        return dispatch(single61(rig, e, takerIsBuyRaw));
                    }
                    return dispatch(rig.tradeBatch(List.of(new V3Rig.BatchEntry(e.jseq, EPOCH, e.egress, e.tradeId, takerIsBuyRaw, Q))));
                case K_RELEASE:
                    return engine.onMeRelease(e.egress, e.jseq, EPOCH, rig.now);
                case K_BUMP:
                    return engine.onMeEngineConfig(e.egress, e.jseq, EPOCH, e.cv, V3Genesis.ME_BEHAVIOR, V3Genesis.ME_CAPACITY,
                            V3Genesis.ME_MAX_WIRE, ConfigDecision.BUMP, rig.now, id -> { });
                case K_TERM1:
                    return engine.onMeTerminal(e.egress, e.jseq, 101, 1, MKT, V3Rig.ME_CANCELLED, 0, rig.now, 1, EPOCH, rig.now);
                case K_TERM2:
                    return engine.onMeTerminal(e.egress, e.jseq, 202, 2, MKT, V3Rig.ME_CANCELLED, 0, rig.now, 2, EPOCH, rig.now);
                default:
                    throw new AssertionError();
            }
        }

        /** A 62 batch of consecutive trade entries; {@code poisonAt} (or -1) carries takerIsBuy 7. */
        void deliverBatch(final List<Entry> trades, final int poisonAt) {
            final List<V3Rig.BatchEntry> b = new ArrayList<>();
            for (int i = 0; i < trades.size(); i++) {
                final Entry e = trades.get(i);
                b.add(new V3Rig.BatchEntry(e.jseq, EPOCH, e.egress, e.tradeId, i == poisonAt ? 7 : 0, Q));
            }
            dispatch(rig.tradeBatch(b));
        }

        boolean dispatch(final int length) {
            final long before = cursor();
            demuxer.dispatchTrusted(rig.buffer(), 0, length, rig.now);
            return cursor() >= before;
        }
    }

    static byte[] image(final AssetsEngine engine) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(engine, buf, 0);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    /** A 61 frame (seller order 2 leg 202 hits buyer order 1 leg 101), raw takerIsBuy byte written after the fact. */
    static int single61(final V3Rig r, final Entry e, final int takerIsBuyRaw) {
        final MeJournalTradeEncoder enc = new MeJournalTradeEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header());
        enc.egressSeq(e.egress).tradeId(e.tradeId).marketId(MKT).takerOrderId(202).takerUserId(2).makerOrderId(101).makerUserId(1)
                .price(PX).quantity(Q).takerIsBuy(BoolFlag.FALSE).meTimestamp(r.now).takerOmsOrderId(2).makerOmsOrderId(1)
                .meEpoch(EPOCH).journalSeq(e.jseq);
        r.buffer().putByte(MessageHeaderEncoder.ENCODED_LENGTH + MeJournalTradeEncoder.takerIsBuyEncodingOffset(), (byte) takerIsBuyRaw);
        return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /** Buyer 1 BUY 2 BTC @ 100 (order 1, leg 101), seller 2 SELL 2 BTC (order 2, leg 202), acked at jseq 2 and 3. */
    static Core boot() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        assertEquals(3L, r.cursor());
        return new Core(r);
    }

    static List<Entry> journal(final Random rnd, final int n) {
        final List<Entry> j = new ArrayList<>();
        long trade = 0;
        long cv = V3Genesis.ME_CONFIG_VERSION;
        for (int i = 0; i < n; i++) {
            final long js = FIRST + i;
            final long g = 100 + 10 * js;
            final int p = rnd.nextInt(100);
            if (p < 70) {
                j.add(new Entry(K_TRADE, js, g, ++trade, 0));
            } else if (p < 85) {
                j.add(new Entry(K_RELEASE, js, g, 0, 0));
            } else {
                j.add(new Entry(K_BUMP, js, g, 0, ++cv));
            }
        }
        j.add(new Entry(K_TERM1, FIRST + n, 100 + 10 * (FIRST + n), 0, 0));
        j.add(new Entry(K_TERM2, FIRST + n + 1, 100 + 10 * (FIRST + n + 1), 0, 0));
        return j;
    }

    /** money[k] = {total/locked of (1,USD),(1,BTC),(2,USD),(2,BTC), lastAppliedTradeId, consumePosition} after k entries. */
    static long[][] oracle(final List<Entry> j) {
        final Core o = boot();
        final long[][] m = new long[j.size() + 1][];
        m[0] = money(o);
        for (int k = 0; k < j.size(); k++) {
            assertTrue(o.deliver(j.get(k), false, 0));
            assertEquals(j.get(k).jseq, o.cursor());
            m[k + 1] = money(o);
        }
        o.engine.applyOrderEntryGate(9L, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, o.rig.now);
        return m;
    }

    static long[] money(final Core c) {
        return new long[] {c.total(1, USD), c.locked(1, USD), c.total(1, BTC), c.locked(1, BTC), c.total(2, USD), c.locked(2, USD),
                c.total(2, BTC), c.locked(2, BTC), c.engine.getLastAppliedTradeId(), c.engine.getConsumePosition()};
    }

    /** One run; returns {renders, final image}. {@code leaderChanges} false turns the restore action into a no-op. */
    static Object[] run(final long seed, final boolean leaderChanges, final List<Entry> j, final long[][] oracle) {
        final Random rnd = new Random(seed);
        final Core c = boot();
        final int[] lane = new int[2]; // next journal index per lane
        long lastCursor = c.cursor();
        boolean epochPoisoned = false;
        for (int step = 0; step < 600; step++) {
            final int l = rnd.nextInt(2);
            final int a = rnd.nextInt(100);
            final String what;
            if (a < 45) {
                // send the lane's next chunk: a batch of consecutive trades, or one other entry; sometimes lost or poisoned
                if (lane[l] >= j.size()) {
                    continue;
                }
                final Entry e = j.get(lane[l]);
                final boolean lost = rnd.nextInt(100) < 10;
                if (e.kind == K_TRADE && rnd.nextInt(4) != 0) {
                    final List<Entry> run = new ArrayList<>();
                    for (int i = lane[l]; i < j.size() && run.size() < 1 + rnd.nextInt(5) && j.get(i).kind == K_TRADE; i++) {
                        run.add(j.get(i));
                    }
                    final int poison = rnd.nextInt(100) < 8 ? rnd.nextInt(run.size()) : -1;
                    if (!lost) {
                        c.deliverBatch(run, poison);
                    }
                    lane[l] += run.size();
                    what = "lane " + l + (lost ? " LOST" : "") + " batch " + run.get(0).jseq + ".." + run.get(run.size() - 1).jseq
                            + (poison >= 0 ? " poison@" + run.get(poison).jseq : "");
                } else {
                    final boolean poison = e.kind == K_TRADE && rnd.nextInt(100) < 10;
                    if (!lost) {
                        c.deliver(e, true, poison ? 9 : 0);
                    }
                    lane[l]++;
                    what = "lane " + l + (lost ? " LOST" : "") + " single " + e.jseq + " kind " + e.kind + (poison ? " poison" : "");
                }
            } else if (a < 62) {
                // the bridge fence: rewind to the reported cursor, sometimes older (a resend from an old position)
                final int idx = (int) (c.cursor() - FIRST + 1) - rnd.nextInt(6);
                lane[l] = Math.max(0, Math.min(j.size(), idx));
                what = "lane " + l + " rewind to index " + lane[l];
            } else if (a < 68) {
                // a 66 GAP from a lane that sees a hole between the cursor and its own position
                final long expected = c.cursor() + 1;
                final long seen = FIRST + lane[l];
                if (expected >= 1 && expected < seen) {
                    c.engine.applyFeedHalt(77L, 6_600L + step, FeedHaltReason.GAP, 0L, expected, seen, 0x77, c.rig.now);
                }
                what = "66 GAP " + expected + " < " + seen;
            } else if (a < 73) {
                // the runbook-safe 76 rows only: a GAP or a skip-0 latch cleared with journalSeq 0 (never a skip)
                final FeedState f = c.engine.feed();
                if (f.latched() && f.latchSkipSeq() == 0 && (f.latchReason() == FeedHaltReason.GAP
                        || f.latchReason() == FeedHaltReason.SOURCE_SCHEMA)) {
                    c.engine.applyFeedLatchClear(7_600L + step, f.latchReason(), 0L, c.rig.now);
                }
                what = "76 clear";
            } else if (a < 80) {
                if (leaderChanges) {
                    c.restore();
                }
                what = "leader change";
            } else if (a < 81 && !epochPoisoned && seed % 3 == 0) {
                // a stale or foreign ME epoch frame at the exactly-next seq: EPOCH_MISMATCH (genesis-only), money unmoved
                final long js = c.cursor() + 1;
                c.engine.onMeTrade(100 + 10 * js, js, c.engine.getLastAppliedTradeId() + 1, MKT, 202, 2, 101, 1, PX, Q, false,
                        c.rig.now, 2, 1, EPOCH + 1, c.rig.now);
                epochPoisoned = true;
                what = "foreign epoch at " + js;
            } else {
                continue;
            }
            check(c, oracle, lastCursor, "seed " + seed + " step " + step + " (" + what + ")");
            lastCursor = c.cursor();
        }
        // drain: one lane rewinds to the cursor and delivers the rest cleanly, in journal order
        final long end = j.get(j.size() - 1).jseq;
        for (int pass = 0; pass < 3 && c.cursor() < end; pass++) {
            for (int i = (int) (c.cursor() - FIRST + 1); i < j.size(); i++) {
                c.deliver(j.get(i), false, 0);
                check(c, oracle, lastCursor, "seed " + seed + " drain " + i);
                lastCursor = c.cursor();
            }
        }
        assertEquals("seed " + seed + ": the drain converges", end, c.cursor());
        assertNull(c.engine.orders().orders().get(1L));
        assertNull(c.engine.orders().orders().get(2L));
        assertEquals(0L, c.locked(1, USD));
        assertEquals(0L, c.locked(2, BTC));
        if (!c.engine.feed().latched()) {
            c.engine.applyOrderEntryGate(9L, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, c.rig.now);
        }
        final List<String> renders = new ArrayList<>();
        final java.util.Map<String, Integer> stats = new java.util.TreeMap<>();
        for (final AssetsEngineEvent e : c.rig.sink.events()) {
            renders.add(e.render());
            if (e instanceof AssetsEngineEvent.FeedLatch fl) {
                stats.merge("latch " + fl.reason() + (fl.skip() != 0 ? " skip" : ""), 1, Integer::sum);
            } else if (e instanceof AssetsEngineEvent.FeedLatchCleared fc) {
                stats.merge("clear " + fc.reason() + (fc.auto() ? " auto" : " 76"), 1, Integer::sum);
            } else if (e instanceof AssetsEngineEvent.SettleRejected sr) {
                stats.merge("rejected " + sr.reason(), 1, Integer::sum);
            }
        }
        if (leaderChanges) {
            System.out.println("[S7-VERIFY] seed " + seed + " " + stats + " finalLatched=" + c.engine.feed().latched());
        }
        return new Object[] {renders, image(c.engine), c.engine.feed().latched()};
    }

    static void check(final Core c, final long[][] oracle, final long lastCursor, final String where) {
        final long cur = c.cursor();
        assertTrue(where + ": the cursor never moves back (" + lastCursor + " -> " + cur + ")", cur >= lastCursor);
        final int k = (int) (cur - FIRST + 1);
        assertTrue(where + ": cursor " + cur + " inside the journal", k >= 0 && k < oracle.length);
        assertArrayEquals(where + ": money == oracle after exactly the entries up to the cursor " + cur, oracle[k], money(c));
        c.engine.verifyRestoredInvariants(); // locked == sum of holds, gate and feed invariants
        // belt review F-2 (implementer edit): every lane resends verbatim, so the 3.8 belt must never trip
        assertEquals(where + ": no duplicate-belt mismatch on verbatim resends", 0L, c.engine.feed().duplicateMismatchCount());
        if (c.engine.feed().latched()) {
            assertTrue(where + ": a latch keeps BRIDGE_HALT on GLOBAL",
                    (c.engine.gate().reasons(OrderEntryGate.GLOBAL) & (1 << GateReason.BRIDGE_HALT.code())) != 0);
            assertEquals(GateState.SUSPENDED, c.engine.gate().state(OrderEntryGate.GLOBAL));
        }
        final CoreOrder buy = c.engine.orders().orders().get(1L);
        if (cur < oracleTermJseq(oracle)) {
            assertNotNull(where + ": the live buy order is never terminated early", buy);
            assertEquals(where + ": its leg is never changed", 101L, buy.currentLegId);
        }
    }

    /** The terminal of order 1 is the second-to-last entry: journalSeq FIRST + (oracle.length - 3). */
    static long oracleTermJseq(final long[][] oracle) {
        return FIRST + oracle.length - 3;
    }

    @Test
    public void randomisedTwoLaneDeliveryNeverSkipsNeverDoublesAndConverges() {
        final byte[] oracleImage;
        for (long seed = 1; seed <= 12; seed++) {
            final List<Entry> j = journal(new Random(seed * 7919L), 60);
            final long[][] oracle = oracle(j);
            final Core ref = boot();
            for (final Entry e : j) {
                ref.deliver(e, false, 0);
            }
            ref.engine.applyOrderEntryGate(9L, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, ref.rig.now);
            final byte[] clean = image(ref.engine);
            final Object[] withLeaderChanges = run(seed, true, j, oracle);
            final Object[] without = run(seed, false, j, oracle);
            assertEquals("seed " + seed + ": leader changes are invisible (same egress, event by event)", without[0], withLeaderChanges[0]);
            assertArrayEquals("seed " + seed + ": leader changes are invisible (same final image)", (byte[]) without[1],
                    (byte[]) withLeaderChanges[1]);
            if (!(Boolean) without[2]) {
                assertArrayEquals("seed " + seed + ": after the drain and ADMIN OPEN the hostile run equals the clean run byte for byte",
                        clean, (byte[]) without[1]);
            }
        }
    }

    // ---------------------------------------------------------------- targeted rows

    private static AssetsEngineEvent.AdminAck lastAck(final V3Rig r) {
        final List<AssetsEngineEvent.AdminAck> acks = r.all(AssetsEngineEvent.AdminAck.class);
        return acks.get(acks.size() - 1);
    }

    /** A poisoned 61 (or 62 entry) at c + 1, then a valid trade carrying tradeId L + 1, before and after a 76 skip. */
    @Test
    public void poisonedTradeThenValidTradeIdLPlusOne() {
        for (final boolean batch : new boolean[] {false, true}) {
            final Core c = boot();
            final V3Rig r = c.rig;
            assertTrue(r.sellHits(1, Q)); // jseq 4, L = 1
            final long[] before = money(c);
            final Entry poisoned = new Entry(K_TRADE, 5, 150, 999, 0); // raw tradeId 999 is never trusted
            if (batch) {
                c.deliverBatch(List.of(poisoned, new Entry(K_TRADE, 6, 160, 3, 0)), 0);
            } else {
                c.deliver(poisoned, true, 7);
            }
            r.assertLatch("poison at c + 1", FeedHaltReason.SOURCE_SCHEMA, 5, 5, 5, 2);
            assertEquals("the batch stopped at the poison", 4L, c.cursor());
            // a valid trade with tradeId L + 1 at the NEXT seq (s + 1): a gap, refused, latch kept, nothing moves
            r.mark();
            assertFalse(r.sellHitsAt(6, EPOCH, 160, 2, Q));
            assertEquals(SettleRejectReason.FEED_GAP, r.first(AssetsEngineEvent.SettleRejected.class).reason());
            r.assertLatch("GAP suppressed under the skippable class-2 latch", FeedHaltReason.SOURCE_SCHEMA, 5, 5, 5, 2);
            assertArrayEquals(before, money(c));
            // (a) the valid copy of s arrives from the other lane: it applies and reach-clears
            final Core c2 = boot();
            assertTrue(c2.rig.sellHits(1, Q));
            c2.deliver(poisoned, true, 7);
            assertTrue(c2.rig.sellHitsAt(5, EPOCH, 150, 2, Q));
            c2.rig.assertNotLatched("valid copy reach-clears");
            assertEquals(2L, c2.engine.getLastAppliedTradeId());
            c2.engine.verifyRestoredInvariants();
            // (b) the operator skips s instead: exactly one tradeId (L + 1 = 2) is consumed, whatever the frame said
            r.mark();
            r.clear(FeedHaltReason.SOURCE_SCHEMA, 5);
            assertEquals(AdminResult.ACCEPTED, lastAck(r).result());
            assertEquals(5L, c.cursor());
            assertEquals(2L, c.engine.getLastAppliedTradeId());
            assertArrayEquals("a skip moves no money", Arrays.copyOf(before, 8), Arrays.copyOf(money(c), 8));
            // a valid frame carrying the consumed tradeId 2 at s + 1 is refused loudly (density), never swallowed
            r.mark();
            assertFalse(r.sellHitsAt(6, EPOCH, 160, 2, Q));
            assertEquals(SettleRejectReason.TRADE_GAP, r.first(AssetsEngineEvent.SettleRejected.class).reason());
            r.assertLatch("density after the skip", FeedHaltReason.SETTLE_REFUSED, 6, 6, 6, 3);
            assertEquals(5L, c.cursor());
            // the correct next trade (tradeId 3) at s + 1 applies and reach-clears
            assertTrue(r.sellHitsAt(6, EPOCH, 160, 3, Q));
            r.assertNotLatched("next valid trade");
            assertEquals(3L, c.engine.getLastAppliedTradeId());
            assertNotNull("the live orders keep their holds", r.order(1));
            c.engine.verifyRestoredInvariants();
        }
    }

    /** ME re-genesis with a NEW epoch: every new-epoch entry refused, class-3 latch, no 76, no ADMIN OPEN. */
    @Test
    public void meRegenesisWithANewEpochIsRefusedAndGenesisOnly() {
        final Core c = boot();
        final V3Rig r = c.rig;
        assertTrue(r.sellHits(1, Q)); // jseq 4
        final long[] before = money(c);
        final long e2 = EPOCH + 0x100;
        r.mark();
        assertFalse(r.configAt(1, e2, 1, 1, 2, 4096, 12, ConfigDecision.FRESH));
        assertEquals(SettleRejectReason.ME_EPOCH_MISMATCH, r.first(AssetsEngineEvent.SettleRejected.class).reason());
        assertEquals(FeedHaltReason.EPOCH_MISMATCH, c.engine.feed().latchReason());
        assertEquals(e2, c.engine.feed().latchMeEpochSeen());
        for (long js = 2; js <= 6; js++) {
            assertFalse(r.tradeAt(js, e2, js, js - 1, 2, 202, 2, 1, 101, 1, PX, Q, false));
        }
        assertArrayEquals("no new-epoch entry moved money", before, money(c));
        assertEquals(4L, c.cursor());
        r.mark();
        r.clear(FeedHaltReason.EPOCH_MISMATCH, 0);
        assertEquals(AdminResult.REJECTED, lastAck(r).result());
        r.mark();
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        assertEquals(CoreRejectReason.FEED_HALTED.code(), lastAck(r).reason());
        c.restore();
        assertEquals("the latch survives a leader change", FeedHaltReason.EPOCH_MISMATCH, c.engine.feed().latchReason());
        c.engine.verifyRestoredInvariants();
    }

    /**
     * ME re-genesis that REUSES the epoch (genesisId reused, journal restarted at 1): contract 4.4 says unsupported, caught
     * by the operator precheck. Originally a characterisation of the silent swallow (review N-1); with the contract 3.8
     * duplicate belt (coordinator follow-up, implementer edit) the new ME's records at seqs {@code <= c} must byte-match what
     * was applied: a FRESH identical to the bound one is indistinguishable and stays a DUPLICATE, the first record with other
     * content is refused and latched, nothing is swallowed after it.
     */
    @Test
    public void meRegenesisReusingTheEpochCharacterisation() {
        final Core c = boot();
        final V3Rig r = c.rig;
        assertTrue(r.sellHitsAt(4, EPOCH, 40, 1, Q));
        assertTrue(r.sellHitsAt(5, EPOCH, 50, 2, Q));
        final long[] before = money(c);
        final long dups = c.engine.feed().duplicateCount(68) + c.engine.feed().duplicateCount(61) + c.engine.feed().duplicateCount(63);
        r.mark();
        // the re-genesised ME: FRESH at 1 (byte-identical to the bound one: a DUPLICATE), a terminal at 2 (a new leg) and a
        // trade at 3 carry other content than was applied there: refused, latched, counted
        assertTrue(r.configAt(1, EPOCH, 1, 1, 2, 4096, 12, ConfigDecision.FRESH));
        assertEquals(dups + 1, c.engine.feed().duplicateCount(68) + c.engine.feed().duplicateCount(61)
                + c.engine.feed().duplicateCount(63));
        assertFalse(r.terminalAt(2, EPOCH, 2, 1, 1, 1, V3Rig.ME_CANCELLED, 0));
        r.assertLatch("3.8 belt", FeedHaltReason.SOURCE_SCHEMA, 6, 2, 0, 0);
        assertFalse(r.sellHitsAt(3, EPOCH, 3, 1, Q));
        assertEquals("never swallowed: both mismatches counted", 2L, c.engine.feed().duplicateMismatchCount());
        assertEquals("no order event, no settle", 0, r.count(AssetsEngineEvent.OrderEvent.class) + r.count(AssetsEngineEvent.Settled.class));
        assertArrayEquals(before, money(c));
        // the first record past c from the young ME log: egressSeq 6 < P = 50, so D1 row 6 latches skip 0 (suppressed: same class)
        assertFalse(r.terminalAt(6, EPOCH, 6, 1, 1, 1, V3Rig.ME_CANCELLED, 0));
        r.assertLatch("still the belt's latch", FeedHaltReason.SOURCE_SCHEMA, 6, 2, 0, 0);
        assertNotNull("the live order is not terminated", r.order(1));
        assertArrayEquals(before, money(c));
    }

    /** CAPACITY suspends and its internal clear runs while BRIDGE_HALT is set: GLOBAL stays shut until the hole heals. */
    @Test
    public void capacityReopenWhileBridgeHaltIsSet() {
        final Core c = boot();
        final V3Rig r = c.rig;
        r.deposit(3, USD, 10_000 * ONE);
        final long o3 = r.create(r.limit(3, 0, PX, ONE)); // pending CREATE attempt 0
        assertFalse(r.sellHitsAt(6, EPOCH, 60, 2, Q)); // hole at 4..5: GAP
        assertEquals(FeedHaltReason.GAP, c.engine.feed().latchReason());
        // the exactly-next entry still applies while latched: CAPACITY for order 3
        assertTrue(r.outcomeAt(4, EPOCH, 40, o3, 0, V3Rig.KIND_CREATE, 3, 0, 0, false));
        final int both = (1 << GateReason.BRIDGE_HALT.code()) | (1 << GateReason.ME_CAPACITY.code());
        assertEquals(both, r.globalReasons());
        r.mark();
        c.engine.clearGateInternally(OrderEntryGate.GLOBAL, GateReason.ME_CAPACITY);
        assertEquals(1 << GateReason.BRIDGE_HALT.code(), r.globalReasons());
        assertEquals("the capacity clear cannot open a gate BRIDGE_HALT holds", GateState.SUSPENDED,
                c.engine.gate().state(OrderEntryGate.GLOBAL));
        final AssetsEngineEvent.AdmissionState as = r.first(AssetsEngineEvent.AdmissionState.class);
        assertEquals(GateState.SUSPENDED, as.gateState());
        r.mark();
        c.engine.orders().applyCreate(r.limit(3, 0, PX, ONE), r.now);
        assertEquals(CoreRejectReason.GATE_CLOSED, r.first(AssetsEngineEvent.OrderReject.class).reason());
        r.mark();
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        assertEquals(CoreRejectReason.FEED_HALTED.code(), lastAck(r).reason());
        assertNotNull("CAPACITY keeps the dispatch pending", r.order(o3));
        // the hole heals: reach-clear, gate still shut, ADMIN OPEN now accepted
        assertTrue(r.sellHitsAt(5, EPOCH, 50, 1, Q));
        r.assertNotLatched("healed");
        assertEquals(GateState.SUSPENDED, c.engine.gate().state(OrderEntryGate.GLOBAL));
        assertTrue(r.sellHitsAt(6, EPOCH, 60, 2, Q));
        r.mark();
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.ACCEPTED, lastAck(r).result());
        assertEquals(0, r.globalReasons());
        c.engine.verifyRestoredInvariants();
    }

    /** Latch, reach-clear, then the bridge resends the whole range: every resent entry is a DUPLICATE, nothing doubles. */
    @Test
    public void latchThenReachClearThenResend() {
        final Core c = boot();
        final V3Rig r = c.rig;
        assertTrue(r.sellHitsAt(4, EPOCH, 40, 1, Q));
        assertFalse(r.sellHitsAt(6, EPOCH, 60, 3, Q));
        assertEquals(FeedHaltReason.GAP, c.engine.feed().latchReason());
        r.mark();
        assertTrue(r.sellHitsAt(5, EPOCH, 50, 2, Q));
        assertEquals(1, r.feedHaltFaults().size());
        assertEquals("the clear audit names the cursor", 5L, r.feedHaltFaults().get(0).tradeId());
        final long[] healed = money(c);
        final long dup = c.engine.feed().duplicateCount(62);
        r.mark();
        // (implementer edit for the 3.8 belt: the resend no longer names journalSeq 2 as a trade; 2 was an ack, and a copy
        // with other content is refused and latched, FeedDuplicateBeltTest)
        c.deliverBatch(List.of(new Entry(K_TRADE, 4, 40, 1, 0), new Entry(K_TRADE, 5, 50, 2, 0)), -1);
        assertEquals("two DUPLICATEs, no event", 0, r.events().size());
        assertEquals(dup + 2, c.engine.feed().duplicateCount(62));
        assertArrayEquals(healed, money(c));
        assertTrue("BRIDGE_HALT stays until ADMIN OPEN", r.globalHas(GateReason.BRIDGE_HALT));
        c.deliverBatch(List.of(new Entry(K_TRADE, 5, 50, 2, 0), new Entry(K_TRADE, 6, 60, 3, 0)), -1);
        assertEquals(6L, c.cursor());
        assertEquals(3L, c.engine.getLastAppliedTradeId());
        c.deliverBatch(List.of(new Entry(K_TRADE, 4, 40, 1, 0), new Entry(K_TRADE, 5, 50, 2, 0), new Entry(K_TRADE, 6, 60, 3, 0)), -1);
        assertEquals(3L, c.engine.getLastAppliedTradeId());
        assertEquals("filled exactly three trades", 3 * Q, r.order(1).filledQty);
        c.engine.verifyRestoredInvariants();
    }

    /** 76 on each latch reason: only row 5 moves the cursor, no row moves money or releases a live hold. */
    @Test
    public void latchClearOnEachReasonNeverMovesMoneyOrAHold() {
        for (final FeedHaltReason reason : new FeedHaltReason[] {FeedHaltReason.GAP, FeedHaltReason.SOURCE_SCHEMA,
                FeedHaltReason.SETTLE_REFUSED, FeedHaltReason.EPOCH_MISMATCH, FeedHaltReason.SOURCE_UNBOUND}) {
            for (final long js : new long[] {0, 5, 6}) {
                final Core c = boot();
                final V3Rig r = c.rig;
                assertTrue(r.sellHits(1, Q)); // jseq 4
                switch (reason) {
                    case GAP -> r.sellHitsAt(7, EPOCH, 70, 2, Q);
                    case SOURCE_SCHEMA -> c.deliver(new Entry(K_TRADE, 5, 50, 2, 0), true, 9);
                    case SETTLE_REFUSED -> r.sellHitsAt(5, EPOCH, 50, 7, Q);
                    case EPOCH_MISMATCH -> r.sellHitsAt(5, EPOCH + 1, 50, 2, Q);
                    default -> { }
                }
                if (reason == FeedHaltReason.SOURCE_UNBOUND) {
                    final V3Rig u = V3Rig.unbound();
                    u.sellHitsAt(1, EPOCH, 1, 1, Q);
                    assertEquals(FeedHaltReason.SOURCE_UNBOUND, u.feed().latchReason());
                    u.mark();
                    u.clear(reason, js);
                    assertEquals(AdminResult.REJECTED, lastAck(u).result());
                    assertEquals(FeedHaltReason.SOURCE_UNBOUND, u.feed().latchReason());
                    continue;
                }
                assertEquals(reason, c.engine.feed().latchReason());
                final long holdBuy = r.locked(1, USD);
                final long holdSell = r.locked(2, BTC);
                final long[] before = money(c);
                r.mark();
                r.clear(reason, js);
                final AdminResult res = lastAck(r).result();
                final boolean skip = js == 5 && (reason == FeedHaltReason.SOURCE_SCHEMA || reason == FeedHaltReason.SETTLE_REFUSED);
                // both class-2 latches here are skippable (skip 5), so journalSeq 0 is D3b row 6, REJECTED
                final boolean clear0 = js == 0 && reason == FeedHaltReason.GAP;
                assertEquals(reason + " jseq " + js, skip || clear0 ? AdminResult.ACCEPTED : AdminResult.REJECTED, res);
                assertEquals(reason + " jseq " + js + ": cursor", skip ? 5L : 4L, c.cursor());
                assertEquals(reason + " jseq " + js + ": the buyer's live hold", holdBuy, r.locked(1, USD));
                assertEquals(reason + " jseq " + js + ": the seller's live hold", holdSell, r.locked(2, BTC));
                assertArrayEquals(Arrays.copyOf(before, 8), Arrays.copyOf(money(c), 8));
                assertEquals(skip ? 2L : 1L, c.engine.getLastAppliedTradeId());
                c.engine.verifyRestoredInvariants();
            }
        }
    }

    /** N = 2 on an iceberg: lane B resends the exhausted slice's trades and FILLED terminal after the next slice is live. */
    @Test
    public void icebergSliceSurvivesASecondLaneResendingTheOldSlice() {
        final V3Rig r = V3Rig.open();
        r.deposit(1, USD, 1_000_000 * ONE);
        r.deposit(2, BTC, 1_000 * ONE);
        final long ice = r.create(r.iceberg(1, 0, PX, 3 * ONE, ONE));
        final long sell = r.create(r.limit(2, 1, PX, 3 * ONE));
        assertTrue(r.ack(ice, 0, 101));     // jseq 2
        assertTrue(r.ack(sell, 0, 202));    // jseq 3
        assertTrue(r.sellHitsAt(4, EPOCH, 40, 1, ONE / 2));
        assertTrue(r.sellHitsAt(5, EPOCH, 50, 2, ONE / 2)); // slice 1 exhausted: slice 2 minted (pending CREATE)
        final long slices = r.engine.orders().sliceCount();
        assertTrue(r.terminalAt(6, EPOCH, 60, ice, 101, 1, V3Rig.ME_FILLED, 0)); // old slice terminal: stale
        assertTrue(r.outcomeAt(7, EPOCH, 70, ice, r.order(ice).attempt, V3Rig.KIND_CREATE, 0, 303, 0, false));
        final CoreOrder o = r.order(ice);
        assertEquals(303L, o.currentLegId);
        final long hold = r.locked(1, USD);
        final long[] snapshot = {o.filledQty, o.hiddenQty, o.sliceRemainingQty, hold};
        // lane B: the old range again, as single frames: js 2..7 as the old slice's FILLED (only js 6 was that entry: a
        // DUPLICATE; the other five carry other content than was applied and are refused by the 3.8 belt, implementer edit),
        // then the two trades verbatim (DUPLICATEs)
        r.mark();
        for (long js = 2; js <= 7; js++) {
            r.terminalAt(js, EPOCH, js * 10, ice, 101, 1, V3Rig.ME_FILLED, 0);
        }
        r.sellHitsAt(4, EPOCH, 40, 1, ONE / 2);
        r.sellHitsAt(5, EPOCH, 50, 2, ONE / 2);
        assertEquals("five mismatching copies refused", 5L, r.engine.feed().duplicateMismatchCount());
        r.assertLatch("the belt's latch", FeedHaltReason.SOURCE_SCHEMA, 8, 2, 0, 0);
        assertEquals("no order event, no settle", 0, r.count(AssetsEngineEvent.OrderEvent.class) + r.count(AssetsEngineEvent.Settled.class));
        assertEquals(slices, r.engine.orders().sliceCount());
        assertEquals(303L, r.order(ice).currentLegId);
        assertArrayEquals(snapshot, new long[] {o.filledQty, o.hiddenQty, o.sliceRemainingQty, r.locked(1, USD)});
        // and the exactly-next copy of the old slice's FILLED (a lane that re-numbered it) cannot end or re-slice the parent
        assertTrue(r.terminalAt(8, EPOCH, 80, ice, 101, 1, V3Rig.ME_FILLED, 0));
        assertNotNull(r.order(ice));
        assertEquals(slices, r.engine.orders().sliceCount());
        assertEquals(hold, r.locked(1, USD));
        r.engine.verifyRestoredInvariants();
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderStatus;
import com.openexchange.assets.infrastructure.generated.ReleaseResult;
import com.openexchange.assets.infrastructure.generated.ReleaseScope;
import org.junit.Test;

import java.util.function.Consumer;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;

/**
 * RC2 S8 7 G8 (3.9): under behaviour 3 the demuxer refuses a superset of what the projector refuses (PJ:61-63, :74-76,
 * :92-101, :112, :118-125) plus the core-only clauses (a) and (c); each poisoned frame at the exactly-next journalSeq
 * latches SOURCE_SCHEMA (S7 5.4) without moving the cursor. One row per clause; a well-formed baseline per template.
 */
public class FeedDecodeV3RangesTest {

    private static final int CAP = (int) V3Genesis.ME_CAPACITY;

    private static void assertPoison(final V3Rig r, final int length, final String what) {
        final long c = r.cursor();
        final long decode = r.demuxer.decodeRejectCount();
        r.dispatchTrusted(length);
        assertEquals(what + ": latched SOURCE_SCHEMA", FeedHaltReason.SOURCE_SCHEMA, r.feed().latchReason());
        assertEquals(what + ": expected = c + 1", c + 1, r.feed().latchExpectedSeq());
        assertEquals(what + ": the cursor did not move", c, r.cursor());
        assertEquals(what + ": counted as a decode reject", decode + 1, r.demuxer.decodeRejectCount());
    }

    private static void assertApplies(final V3Rig r, final int length, final String what) {
        final long c = r.cursor();
        r.dispatchTrusted(length);
        assertEquals(what + ": not latched", FeedHaltReason.NONE, r.feed().latchReason());
        assertEquals(what + ": applied", c + 1, r.cursor());
    }

    // ---------------------------------------------------------------- 64

    private static int outcome(final V3Rig r, final Consumer<MeCommandOutcomeEncoder> mutate) {
        final long g = r.egress + 1;
        final MeCommandOutcomeEncoder e = new MeCommandOutcomeEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(g).commandIdHigh(99).commandIdLow(0).userId(1).omsOrderId(99).oldOrderId(0).price(0).quantity(ONE)
                .budget(0).marketId(1).commandKind((short) 0).orderType((short) 0).orderSide((short) 0).appliedPosition(g)
                .meTimestamp(r.now).orderId(0).status(-1).reason(0).oldCancelled((short) 0).result(2).meEpoch(EPOCH)
                .journalSeq(r.cursor() + 1).timeInForce((short) 0).expectedOldRemaining(0).ledgerSize(0).ledgerCapacity(CAP)
                .behaviorVersion(V3Genesis.ME_BEHAVIOR);
        mutate.accept(e);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void outcome64EveryClause() {
        final V3Rig base = V3Rig.open();
        assertApplies(base, outcome(base, e -> { }), "64 baseline (an unmatched CONFLICT)");
        @SuppressWarnings("unchecked")
        final Consumer<MeCommandOutcomeEncoder>[] rows = new Consumer[] {
            (Consumer<MeCommandOutcomeEncoder>) e -> e.commandIdHigh(0).commandIdLow(0),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.userId(0),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.omsOrderId(0),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.marketId(0),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.appliedPosition(-1),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.appliedPosition(Long.MAX_VALUE),          // above egressSeq
            (Consumer<MeCommandOutcomeEncoder>) e -> e.orderId(-1),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.commandKind((short) 3),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.orderType((short) 3),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.orderSide((short) 2),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.status(-2),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.result(0).orderId(5).status(5),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.reason(-1),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.oldCancelled((short) 2),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.result(9),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.oldOrderId(5),                                   // kind 0 needs old 0
            (Consumer<MeCommandOutcomeEncoder>) e -> e.commandKind((short) 1).commandIdLow(1),           // kind 1 needs old > 0
            (Consumer<MeCommandOutcomeEncoder>) e -> e.result(0).status(0),                              // result <= 1, leg 0
            (Consumer<MeCommandOutcomeEncoder>) e -> e.result(1).orderId(5).status(-1),                  // result <= 1, status -1
            (Consumer<MeCommandOutcomeEncoder>) e -> e.orderId(5),                                       // result >= 2, leg != 0
            (Consumer<MeCommandOutcomeEncoder>) e -> e.status(0),                                        // result >= 2, status != -1
            (Consumer<MeCommandOutcomeEncoder>) e -> e.result(0).orderId(5).status(0).oldCancelled((short) 1), // oldCancelled, kind 0
            (Consumer<MeCommandOutcomeEncoder>) e -> e.timeInForce((short) 3),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.commandKind((short) 2).commandIdLow(2).oldOrderId(7).expectedOldRemaining(-1),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.expectedOldRemaining(5),                          // non-zero unless kind 2
            (Consumer<MeCommandOutcomeEncoder>) e -> e.ledgerSize(-1),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.behaviorVersion(0),
            (Consumer<MeCommandOutcomeEncoder>) e -> e.ledgerCapacity(0),                                // core-only (a)
        };
        for (int i = 0; i < rows.length; i++) {
            final V3Rig r = V3Rig.open();
            assertPoison(r, outcome(r, rows[i]), "64 row " + i);
        }
    }

    // ---------------------------------------------------------------- 61 and 62

    private static int trade(final V3Rig r, final Consumer<MeJournalTradeEncoder> mutate) {
        final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(r.egress + 1).tradeId(r.engine.getLastAppliedTradeId() + 1).marketId(V3Rig.MKT).takerOrderId(202)
                .takerUserId(2).makerOrderId(101).makerUserId(1).price(100 * ONE).quantity(ONE / 4).takerIsBuy(BoolFlag.FALSE)
                .meTimestamp(r.now).takerOmsOrderId(2).makerOmsOrderId(1).meEpoch(EPOCH).journalSeq(r.cursor() + 1);
        mutate.accept(e);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static V3Rig book() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        return r;
    }

    @Test
    public void trade61EveryClause() {
        final V3Rig base = book();
        assertApplies(base, trade(base, e -> { }), "61 baseline");
        @SuppressWarnings("unchecked")
        final Consumer<MeJournalTradeEncoder>[] rows = new Consumer[] {
            (Consumer<MeJournalTradeEncoder>) e -> e.tradeId(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.price(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.quantity(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.marketId(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.takerOrderId(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.makerOrderId(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.takerUserId(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.makerUserId(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.takerOmsOrderId(0),
            (Consumer<MeJournalTradeEncoder>) e -> e.makerOmsOrderId(0),
        };
        for (int i = 0; i < rows.length; i++) {
            final V3Rig r = book();
            assertPoison(r, trade(r, rows[i]), "61 row " + i);
        }
    }

    @Test
    public void tradeBatch62EntryClauses() {
        for (int row = 0; row < 4; row++) {
            final V3Rig r = book();
            final long c = r.cursor();
            final MeJournalTradeBatchEncoder e = new MeJournalTradeBatchEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header());
            final MeJournalTradeBatchEncoder.TradesEncoder g = e.tradesCount(1);
            g.next().egressSeq(r.egress + 1).tradeId(row == 0 ? 0 : 1).marketId(V3Rig.MKT).takerOrderId(row == 1 ? 0 : 202)
                    .takerUserId(2).makerOrderId(101).makerUserId(row == 2 ? 0 : 1).price(row == 3 ? 0 : 100 * ONE)
                    .quantity(ONE / 4).takerIsBuy(BoolFlag.FALSE).meTimestamp(r.now).takerOmsOrderId(2).makerOmsOrderId(1)
                    .meEpoch(EPOCH).journalSeq(c + 1);
            assertPoison(r, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength(), "62 entry row " + row);
        }
    }

    // ---------------------------------------------------------------- 63

    private static int terminal(final V3Rig r, final Consumer<MeJournalTerminalEncoder> mutate) {
        final MeJournalTerminalEncoder e = new MeJournalTerminalEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(r.egress + 1).orderId(101).userId(1).marketId(V3Rig.MKT).status(OrderStatus.CANCELLED)
                .rejectReason((short) 0).meTimestamp(r.now).omsOrderId(1).meEpoch(EPOCH).journalSeq(r.cursor() + 1);
        mutate.accept(e);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void terminal63EveryClause() {
        final V3Rig base = book();
        assertApplies(base, terminal(base, e -> { }), "63 baseline");
        @SuppressWarnings("unchecked")
        final Consumer<MeJournalTerminalEncoder>[] rows = new Consumer[] {
            (Consumer<MeJournalTerminalEncoder>) e -> e.orderId(0),
            (Consumer<MeJournalTerminalEncoder>) e -> e.userId(0),
            (Consumer<MeJournalTerminalEncoder>) e -> e.marketId(0),
            (Consumer<MeJournalTerminalEncoder>) e -> e.omsOrderId(0),
            (Consumer<MeJournalTerminalEncoder>) e -> e.status(OrderStatus.EXPIRED),
            (Consumer<MeJournalTerminalEncoder>) e -> e.rejectReason((short) 255),
        };
        for (int i = 0; i < rows.length; i++) {
            final V3Rig r = book();
            assertPoison(r, terminal(r, rows[i]), "63 row " + i);
        }
    }

    // ---------------------------------------------------------------- 67

    /** {@code row}: -1 baseline; 0..3 root clauses; 4.. entry clauses. */
    private static int release(final V3Rig r, final int row) {
        final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(r.egress + 1).journalSeq(r.cursor() + 1).meEpoch(EPOCH).releaseSeq(row == 0 ? -1 : 1)
                .ledgerSize(row == 1 ? -1 : 0).ledgerCapacity(row == 2 ? 0 : CAP).tombstoneRanges(row == 3 ? -1 : 0);
        e.releasesCount(1).next().commandIdHigh(row == 7 ? 0 : 1).idLowFloor(row == 8 ? -1 : 0)
                .scope(row == 9 ? ReleaseScope.UP_TO : ReleaseScope.UP_TO).result(row == 9 ? ReleaseResult.LEG_LIVE : ReleaseResult.APPLIED)
                .removed(row == 6 ? -1 : 1);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
        final int entry = MessageHeaderEncoder.ENCODED_LENGTH + MeCommandReleaseEncoder.BLOCK_LENGTH + 4;
        if (row == 4) {
            r.buffer().putByte(entry + MeCommandReleaseEncoder.ReleasesEncoder.scopeEncodingOffset(), (byte) 2);
        }
        if (row == 5) {
            r.buffer().putByte(entry + MeCommandReleaseEncoder.ReleasesEncoder.resultEncodingOffset(), (byte) 3);
        }
        return length;
    }

    @Test
    public void release67EveryClause() {
        final V3Rig base = book();
        assertApplies(base, release(base, -1), "67 baseline");
        final String[] names = {"releaseSeq < 0", "ledgerSize < 0", "ledgerCapacity 0 (core-only c)", "tombstoneRanges < 0",
            "scope 2", "result 3", "removed < 0", "commandIdHigh 0 (core-only c)", "idLowFloor < 0 (core-only c)",
            "LEG_LIVE on UP_TO (core-only c)"};
        for (int row = 0; row < names.length; row++) {
            final V3Rig r = book();
            assertPoison(r, release(r, row), "67 " + names[row]);
        }
    }
}

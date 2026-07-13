// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.journal;

import com.openexchange.assets.infrastructure.journal.generated.BoolFlag;
import com.openexchange.assets.infrastructure.journal.generated.JournalDepositDecoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalOpeningBalanceDecoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalSettleDecoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalWithdrawDecoder;
import com.openexchange.assets.infrastructure.journal.generated.MessageHeaderDecoder;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The money journal's ring half is a money-loss surface: these tests pin (1) field fidelity through
 * the SBE encode, (2) strict append order, (3) the block-never-shed contract on a full ring, and
 * (4) that journal bytes are a pure function of the append sequence (per-replica byte-identity
 * reduces to deterministic engine call order, which the cluster guarantees).
 */
public class MoneyJournalTest {

    private static final class Drained {
        final List<Integer> msgTypes = new ArrayList<>();
        final List<byte[]> payloads = new ArrayList<>();
    }

    private static Drained drainAll(final MoneyJournal journal) {
        final Drained out = new Drained();
        journal.ringBuffer().read((msgTypeId, buffer, index, length) -> {
            final byte[] copy = new byte[length];
            buffer.getBytes(index, copy);
            out.msgTypes.add(msgTypeId);
            out.payloads.add(copy);
        });
        return out;
    }

    @Test
    public void allFourRecordTypesRoundTripInAppendOrder() {
        final MoneyJournal journal = new MoneyJournal(1 << 16);

        journal.onOpeningBalance(1L, 100L, 1, 100_000_000L);
        journal.onDeposit(2L, 300L, 0, 10_000_000_000L, 10_000_000_000L, 1_752_100_000_123L);
        journal.onWithdraw(3L, 300L, 0, 4_000_000_000L, 6_000_000_000L, 1_752_100_000_124L);
        journal.onSettle(4L, 42L, 1, 6_000_000_000_000L, 100_000_000L,
                200L, 100L, true,
                100_000_000L, 0L, 0L, 6_000_500_000_000L, 1_752_100_000_125L);

        final Drained drained = drainAll(journal);
        assertEquals(4, drained.msgTypes.size());
        assertEquals(MoneyJournal.MSG_TYPE_OPENING_BALANCE, (int) drained.msgTypes.get(0));
        assertEquals(MoneyJournal.MSG_TYPE_DEPOSIT, (int) drained.msgTypes.get(1));
        assertEquals(MoneyJournal.MSG_TYPE_WITHDRAW, (int) drained.msgTypes.get(2));
        assertEquals(MoneyJournal.MSG_TYPE_SETTLE, (int) drained.msgTypes.get(3));

        final MessageHeaderDecoder header = new MessageHeaderDecoder();

        final UnsafeBuffer buf0 = new UnsafeBuffer(drained.payloads.get(0));
        header.wrap(buf0, 0);
        assertEquals(JournalOpeningBalanceDecoder.TEMPLATE_ID, header.templateId());
        final JournalOpeningBalanceDecoder opening = new JournalOpeningBalanceDecoder()
                .wrap(buf0, header.encodedLength(), header.blockLength(), header.version());
        assertEquals(1L, opening.journalSeq());
        assertEquals(100L, opening.userId());
        assertEquals(1, opening.assetId());
        assertEquals(100_000_000L, opening.amount());

        final UnsafeBuffer buf1 = new UnsafeBuffer(drained.payloads.get(1));
        header.wrap(buf1, 0);
        assertEquals(JournalDepositDecoder.TEMPLATE_ID, header.templateId());
        final JournalDepositDecoder deposit = new JournalDepositDecoder()
                .wrap(buf1, header.encodedLength(), header.blockLength(), header.version());
        assertEquals(2L, deposit.journalSeq());
        assertEquals(300L, deposit.userId());
        assertEquals(0, deposit.assetId());
        assertEquals(10_000_000_000L, deposit.amount());
        assertEquals(10_000_000_000L, deposit.balanceAfter());
        assertEquals(1_752_100_000_123L, deposit.clusterTimeMs());

        final UnsafeBuffer buf2 = new UnsafeBuffer(drained.payloads.get(2));
        header.wrap(buf2, 0);
        assertEquals(JournalWithdrawDecoder.TEMPLATE_ID, header.templateId());
        final JournalWithdrawDecoder withdraw = new JournalWithdrawDecoder()
                .wrap(buf2, header.encodedLength(), header.blockLength(), header.version());
        assertEquals(3L, withdraw.journalSeq());
        assertEquals(300L, withdraw.userId());
        assertEquals(0, withdraw.assetId());
        assertEquals(4_000_000_000L, withdraw.amount());
        assertEquals(6_000_000_000L, withdraw.balanceAfter());
        assertEquals(1_752_100_000_124L, withdraw.clusterTimeMs());

        final UnsafeBuffer buf3 = new UnsafeBuffer(drained.payloads.get(3));
        header.wrap(buf3, 0);
        assertEquals(JournalSettleDecoder.TEMPLATE_ID, header.templateId());
        final JournalSettleDecoder settle = new JournalSettleDecoder()
                .wrap(buf3, header.encodedLength(), header.blockLength(), header.version());
        assertEquals(4L, settle.journalSeq());
        assertEquals(42L, settle.tradeId());
        assertEquals(1, settle.marketId());
        assertEquals(6_000_000_000_000L, settle.price());
        assertEquals(100_000_000L, settle.quantity());
        assertEquals(200L, settle.buyerUserId());
        assertEquals(100L, settle.sellerUserId());
        assertEquals(BoolFlag.TRUE, settle.takerIsBuy());
        assertEquals(100_000_000L, settle.buyerBaseAfter());
        assertEquals(0L, settle.buyerQuoteAfter());
        assertEquals(0L, settle.sellerBaseAfter());
        assertEquals(6_000_500_000_000L, settle.sellerQuoteAfter());
        assertEquals(1_752_100_000_125L, settle.clusterTimeMs());

        assertEquals(4L, journal.appendedEvents());
        assertEquals(0L, journal.backpressureEvents());
    }

    @Test
    public void journalBytesAreAPureFunctionOfTheAppendSequence() {
        final byte[] first = appendFixedSequenceAndDrain();
        final byte[] second = appendFixedSequenceAndDrain();
        assertArrayEquals("identical append sequences must journal identical bytes", first, second);
    }

    private static byte[] appendFixedSequenceAndDrain() {
        final MoneyJournal journal = new MoneyJournal(1 << 16);
        long seq = 0;
        for (int i = 0; i < 50; i++) {
            journal.onDeposit(++seq, 100 + i, i % 3, 1_000_000L * (1 + i), 5_000_000L * (1 + i), 1_000_000L + i);
            if (i % 5 == 0) {
                journal.onSettle(++seq, 1 + i, 1 + (i % 5), 2_000_000L * (1 + i), 50_000_000L,
                        100 + i, 200 + i, (i & 1) == 0,
                        10L * i, 20L * i, 30L * i, 40L * i, 1_000_001L + i);
            }
            if (i % 7 == 0) {
                journal.onWithdraw(++seq, 100 + i, i % 3, 500_000L, 4_500_000L * (1 + i), 1_000_002L + i);
            }
        }
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        journal.ringBuffer().read((msgTypeId, buffer, index, length) -> {
            bytes.write(msgTypeId);
            final byte[] copy = new byte[length];
            buffer.getBytes(index, copy);
            bytes.writeBytes(copy);
        }, Integer.MAX_VALUE);
        return bytes.toByteArray();
    }

    @Test
    public void fullRingBlocksUntilDrainedAndCountsOneBackpressureEvent() throws Exception {
        // Smallest practical ring: fill it, prove the next append BLOCKS (never sheds), then
        // prove it completes once a consumer drains, with exactly one backpressure event.
        final MoneyJournal journal = new MoneyJournal(4096);

        // Fill by raw ring writes (same record size a deposit encodes to) until a write is
        // REFUSED: the ring is then deterministically too full for one more deposit record.
        final UnsafeBuffer filler = new UnsafeBuffer(new byte[52]);
        while (journal.ringBuffer().write(MoneyJournal.MSG_TYPE_DEPOSIT, filler, 0, filler.capacity())) {
            // keep filling
        }
        assertEquals(0L, journal.backpressureEvents());

        final CountDownLatch blockedAppendDone = new CountDownLatch(1);
        final Thread appender = new Thread(() -> {
            journal.onDeposit(9999L, 1L, 0, 100L, 100L, 9999L);
            blockedAppendDone.countDown();
        }, "test-blocked-appender");
        appender.start();

        // The append must be blocked (ring full), not dropped and not completed.
        assertFalse("append on a full ring must block, not return",
                blockedAppendDone.await(300, TimeUnit.MILLISECONDS));
        assertEquals(1L, journal.backpressureEvents());

        // Drain a few entries -> the blocked append must complete.
        final int[] drained = {0};
        while (drained[0] < 4) {
            journal.ringBuffer().read((int msgTypeId, MutableDirectBuffer b, int i, int l) -> drained[0]++, 4);
        }
        assertTrue("append must complete once the ring drains",
                blockedAppendDone.await(5, TimeUnit.SECONDS));
        appender.join(5_000);
        assertEquals(1L, journal.backpressureEvents());
    }
}

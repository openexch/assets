// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.journal;

import com.openexchange.assets.domain.MoneyJournalSink;
import com.openexchange.assets.infrastructure.Logger;
import com.openexchange.assets.infrastructure.journal.generated.BoolFlag;
import com.openexchange.assets.infrastructure.journal.generated.JournalDepositEncoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalOpeningBalanceEncoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalSettleEncoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalWithdrawEncoder;
import com.openexchange.assets.infrastructure.journal.generated.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.nio.ByteBuffer;

/**
 * Service-thread half of the money journal: encodes every applied money movement (and the
 * opening-balance epoch) into an in-memory SPSC ring, drained by {@link MoneyJournalWriterAgent}
 * into a recorded publication on the node's own archive.
 *
 * The journal is the external projector's money record: a missing entry is an unrecoverable lost
 * movement for that recording (recovered only by a snapshot-replay regenerating it). It is
 * therefore LOSSLESS BY CONSTRUCTION, mirroring the ME's SettlementJournal: the sink methods run
 * on the deterministic cluster service thread on EVERY replica and BLOCK (spin, loudly) if the
 * ring is full rather than shedding. A full ring means the local archive has stalled; blocking
 * degrades this node to a lagging replica while the other replicas' journals continue.
 *
 * Single-writer: all sink methods are called only from the cluster service thread. Counters are
 * volatile so the writer/metrics side can read them without tearing.
 */
public final class MoneyJournal implements MoneyJournalSink {

    private static final Logger log = Logger.getLogger(MoneyJournal.class);

    /** Ring message type ids (journal-internal; the SBE header inside the payload is authoritative). */
    public static final int MSG_TYPE_OPENING_BALANCE = 1;
    public static final int MSG_TYPE_DEPOSIT = 2;
    public static final int MSG_TYPE_WITHDRAW = 3;
    public static final int MSG_TYPE_SETTLE = 4;

    /** Spin count between escalating "journal stalled" logs while blocked on a full ring. */
    private static final long SPINS_PER_STALL_LOG = 50_000_000L;

    private final OneToOneRingBuffer ringBuffer;

    // Scratch encode buffer: max message = header(8) + JournalSettle block (93) < 128.
    private final UnsafeBuffer scratch = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final JournalOpeningBalanceEncoder openingBalanceEncoder = new JournalOpeningBalanceEncoder();
    private final JournalDepositEncoder depositEncoder = new JournalDepositEncoder();
    private final JournalWithdrawEncoder withdrawEncoder = new JournalWithdrawEncoder();
    private final JournalSettleEncoder settleEncoder = new JournalSettleEncoder();

    private volatile long appendedEvents;
    /** Entries that hit a full ring at least once (backpressure EVENTS, not spin iterations). */
    private volatile long backpressureEvents;

    public MoneyJournal(final int ringCapacityBytes) {
        final ByteBuffer byteBuffer =
                ByteBuffer.allocateDirect(ringCapacityBytes + RingBufferDescriptor.TRAILER_LENGTH);
        this.ringBuffer = new OneToOneRingBuffer(new UnsafeBuffer(byteBuffer));
    }

    /** The ring the {@link MoneyJournalWriterAgent} drains. */
    public OneToOneRingBuffer ringBuffer() {
        return ringBuffer;
    }

    @Override
    public void onOpeningBalance(final long journalSeq, final long userId, final int assetId, final long amount) {
        openingBalanceEncoder.wrapAndApplyHeader(scratch, 0, headerEncoder)
                .journalSeq(journalSeq)
                .userId(userId)
                .assetId(assetId)
                .amount(amount);
        blockingWrite(MSG_TYPE_OPENING_BALANCE,
                MessageHeaderEncoder.ENCODED_LENGTH + openingBalanceEncoder.encodedLength(), journalSeq);
    }

    @Override
    public void onDeposit(final long journalSeq, final long userId, final int assetId, final long amount,
                          final long balanceAfter, final long clusterTimeMs) {
        depositEncoder.wrapAndApplyHeader(scratch, 0, headerEncoder)
                .journalSeq(journalSeq)
                .userId(userId)
                .assetId(assetId)
                .amount(amount)
                .balanceAfter(balanceAfter)
                .clusterTimeMs(clusterTimeMs);
        blockingWrite(MSG_TYPE_DEPOSIT,
                MessageHeaderEncoder.ENCODED_LENGTH + depositEncoder.encodedLength(), journalSeq);
    }

    @Override
    public void onWithdraw(final long journalSeq, final long userId, final int assetId, final long amount,
                           final long balanceAfter, final long clusterTimeMs) {
        withdrawEncoder.wrapAndApplyHeader(scratch, 0, headerEncoder)
                .journalSeq(journalSeq)
                .userId(userId)
                .assetId(assetId)
                .amount(amount)
                .balanceAfter(balanceAfter)
                .clusterTimeMs(clusterTimeMs);
        blockingWrite(MSG_TYPE_WITHDRAW,
                MessageHeaderEncoder.ENCODED_LENGTH + withdrawEncoder.encodedLength(), journalSeq);
    }

    @Override
    public void onSettle(final long journalSeq, final long tradeId, final int marketId,
                         final long price, final long quantity,
                         final long buyerUserId, final long sellerUserId, final boolean takerIsBuy,
                         final long buyerBaseAfter, final long buyerQuoteAfter,
                         final long sellerBaseAfter, final long sellerQuoteAfter, final long clusterTimeMs) {
        settleEncoder.wrapAndApplyHeader(scratch, 0, headerEncoder)
                .journalSeq(journalSeq)
                .tradeId(tradeId)
                .marketId(marketId)
                .price(price)
                .quantity(quantity)
                .buyerUserId(buyerUserId)
                .sellerUserId(sellerUserId)
                .takerIsBuy(takerIsBuy ? BoolFlag.TRUE : BoolFlag.FALSE)
                .buyerBaseAfter(buyerBaseAfter)
                .buyerQuoteAfter(buyerQuoteAfter)
                .sellerBaseAfter(sellerBaseAfter)
                .sellerQuoteAfter(sellerQuoteAfter)
                .clusterTimeMs(clusterTimeMs);
        blockingWrite(MSG_TYPE_SETTLE,
                MessageHeaderEncoder.ENCODED_LENGTH + settleEncoder.encodedLength(), journalSeq);
    }

    /**
     * Write to the ring; on a full ring, spin until space frees (block, never shed). Escalates with
     * an ERROR log periodically so a stalled journal is impossible to miss.
     */
    private void blockingWrite(final int msgTypeId, final int length, final long journalSeq) {
        if (ringBuffer.write(msgTypeId, scratch, 0, length)) {
            appendedEvents = appendedEvents + 1;
            return;
        }
        backpressureEvents = backpressureEvents + 1;
        long spins = 0;
        while (!ringBuffer.write(msgTypeId, scratch, 0, length)) {
            spins++;
            if (spins % SPINS_PER_STALL_LOG == 0) {
                log.error("MONEY JOURNAL STALLED: ring full for %d spins (msgType=%d, journalSeq=%d)"
                        + " - journal archive not draining; this replica is now lagging",
                        spins, msgTypeId, journalSeq);
            }
            Thread.onSpinWait();
        }
        appendedEvents = appendedEvents + 1;
    }

    public long appendedEvents() {
        return appendedEvents;
    }

    public long backpressureEvents() {
        return backpressureEvents;
    }
}

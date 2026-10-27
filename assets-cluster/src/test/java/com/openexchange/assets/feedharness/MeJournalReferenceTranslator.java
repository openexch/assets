// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.match.infrastructure.mejournal.generated.CommandOutcomeDecoder;
import com.match.infrastructure.mejournal.generated.CommandReleaseDecoder;
import com.match.infrastructure.mejournal.generated.EngineConfigDecoder;
import com.match.infrastructure.mejournal.generated.TerminalDecoder;
import com.match.infrastructure.mejournal.generated.TradeDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.ConfigDecision;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeEncoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchEncoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderStatus;
import com.openexchange.assets.infrastructure.generated.ReleaseResult;
import com.openexchange.assets.infrastructure.generated.ReleaseScope;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * RC2 S9h section 3: the Java reference translator, schema 4 journal record -> money v7 feed frame, byte exact, and the
 * S9a 6.3 staging rule as a pure function of {@code (record | FLUSH)*}. Decoding uses match-common's generated schema 4
 * decoders, encoding the assets generated money v7 encoders (two independent sbe 1.40.1 codegen runs). Every value is
 * copied verbatim; enum bytes are read raw and written with {@code putByte} at the field's encoding offset, never through
 * a generated enum setter (which throws or clamps on 2..255). This class is the oracle the C++ bridge is graded against.
 */
public final class MeJournalReferenceTranslator {

    public static final int TRADE_BATCH_CAP = 64;
    static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    /** The gate's refusal of a record (review FIX-1: the only exception the CLI turns into exit 65). */
    public static final class GateRefusal extends IllegalArgumentException {
        public GateRefusal(final String message) {
            super(message);
        }
    }

    public interface Out {
        void frame(DirectBuffer buffer, int offset, int length);
    }

    private final Out out;
    private final List<byte[]> staged = new ArrayList<>(TRADE_BATCH_CAP);
    private final com.match.infrastructure.mejournal.generated.MessageHeaderDecoder inHeader =
            new com.match.infrastructure.mejournal.generated.MessageHeaderDecoder();
    private final MessageHeaderEncoder outHeader = new MessageHeaderEncoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final TerminalDecoder terminal = new TerminalDecoder();
    private final CommandOutcomeDecoder outcome = new CommandOutcomeDecoder();
    private final CommandReleaseDecoder release = new CommandReleaseDecoder();
    private final EngineConfigDecoder config = new EngineConfigDecoder();
    private final UnsafeBuffer in = new UnsafeBuffer(new byte[0]);
    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[8 * 1024]);

    public MeJournalReferenceTranslator(final Out out) {
        this.out = out;
    }

    /**
     * One schema 4 record (8-byte header + block [+ group]). Refuses (IllegalArgumentException, nothing emitted, staging
     * unchanged) a record that {@link ReferenceFrameGate} refuses: the translator never sees a frame the bridge would not
     * admit.
     */
    public void record(final DirectBuffer buffer, final int offset, final int length) {
        final ReferenceFrameGate.Verdict v = ReferenceFrameGate.check(buffer, offset, length);
        if (!v.admitted() && !FeedHarnessSeams.on(FeedHarnessSeams.GATE_OFF)) {
            throw new GateRefusal("record refused by the reference gate: " + v.reason() + " (length " + length + ")");
        }
        final byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        final int templateId = v.templateId() >= 0 ? v.templateId()
                : (copy.length >= 4 ? (copy[2] & 0xFF) | (copy[3] & 0xFF) << 8 : -1);
        if (templateId == 1) {
            staged.add(copy);
            final int cap = FeedHarnessSeams.on(FeedHarnessSeams.CAP_63) ? 63 : TRADE_BATCH_CAP;
            if (staged.size() == cap) {
                emitStaged();
            }
            return;
        }
        emitStaged();
        in.wrap(copy);
        switch (templateId) {
            case 2 -> terminal();
            case 3 -> outcome();
            case 4 -> release(copy.length);
            case 5 -> config();
            default -> throw new IllegalArgumentException("templateId " + templateId); // only reachable with GATE_OFF
        }
    }

    public void record(final byte[] bytes) {
        record(new UnsafeBuffer(bytes), 0, bytes.length);
    }

    /** An explicit FLUSH marker. */
    public void flush() {
        emitStaged();
    }

    /** End of input: an implicit FLUSH. */
    public void end() {
        emitStaged();
    }

    public int staged() {
        return staged.size();
    }

    /** The pure function of S9a 9.4: {@code (record | FLUSH)*} -> frames, as bytes. */
    public static List<byte[]> translate(final List<FeedFiles.Entry> input) {
        final List<byte[]> frames = new ArrayList<>();
        final MeJournalReferenceTranslator t = new MeJournalReferenceTranslator((b, o, l) -> {
            final byte[] f = new byte[l];
            b.getBytes(o, f);
            frames.add(f);
        });
        for (final FeedFiles.Entry e : input) {
            if (e.isFlush()) {
                t.flush();
            } else {
                t.record(e.bytes());
            }
        }
        t.end();
        return frames;
    }

    // ------------------------------------------------------------------ emission

    private void emitStaged() {
        if (staged.isEmpty()) {
            return;
        }
        if (staged.size() == 1) {
            in.wrap(staged.get(0));
            single();
        } else {
            batch();
        }
        staged.clear();
    }

    private void zero() {
        buf.setMemory(0, buf.capacity(), (byte) 0);
    }

    private void emit(final int length) {
        if (FeedHarnessSeams.on(FeedHarnessSeams.HEADER_VERSION_6)) {
            buf.putShort(6, (short) 6, ByteOrder.LITTLE_ENDIAN);
        }
        out.frame(buf, 0, length);
    }

    private long egress(final long egressSeq, final long journalSeq, final long meEpoch, final int slot) {
        // slot 0 = egressSeq, 1 = journalSeq, 2 = meEpoch; HEAD_NOT_PERMUTED copies the record's head slot by position
        if (FeedHarnessSeams.on(FeedHarnessSeams.HEAD_NOT_PERMUTED)) {
            return slot == 0 ? journalSeq : slot == 1 ? meEpoch : egressSeq;
        }
        return slot == 0 ? egressSeq : slot == 1 ? journalSeq : meEpoch;
    }

    /** 61 from one record 1. */
    private void single() {
        trade.wrapAndApplyHeader(in, 0, inHeader);
        zero();
        final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, outHeader);
        final boolean swap = FeedHarnessSeams.on(FeedHarnessSeams.SWAP_TAKER_MAKER_ORDER);
        final long eg = trade.egressSeq();
        final long js = trade.journalSeq();
        final long ep = trade.meEpoch();
        e.egressSeq(egress(eg, js, ep, 0)).tradeId(trade.tradeId()).marketId(trade.marketId())
                .takerOrderId(swap ? trade.makerOrderId() : trade.takerOrderId()).takerUserId(trade.takerUserId())
                .makerOrderId(swap ? trade.takerOrderId() : trade.makerOrderId()).makerUserId(trade.makerUserId())
                .price(trade.price()).quantity(trade.quantity())
                .meTimestamp(trade.timestamp()).takerOmsOrderId(trade.takerOmsOrderId())
                .makerOmsOrderId(trade.makerOmsOrderId()).meEpoch(egress(eg, js, ep, 2)).journalSeq(egress(eg, js, ep, 1));
        writeBool(HEADER + MeJournalTradeEncoder.takerIsBuyEncodingOffset(), trade.takerIsBuyRaw(), () -> e.takerIsBuy(
                BoolFlag.get(trade.takerIsBuyRaw())));
        int length = HEADER + MeJournalTradeEncoder.BLOCK_LENGTH;
        if (FeedHarnessSeams.on(FeedHarnessSeams.HEADER_BLOCK_PLUS_ONE)) {
            buf.putShort(0, (short) (MeJournalTradeEncoder.BLOCK_LENGTH + 1), ByteOrder.LITTLE_ENDIAN);
            length += 1; // the trailing byte stays 0 from zero()
        }
        emit(length);
    }

    /** 62 from 2..64 records 1. */
    private void batch() {
        zero();
        final MeJournalTradeBatchEncoder e = new MeJournalTradeBatchEncoder().wrapAndApplyHeader(buf, 0, outHeader);
        final MeJournalTradeBatchEncoder.TradesEncoder g = e.tradesCount(staged.size());
        final boolean swap = FeedHarnessSeams.on(FeedHarnessSeams.SWAP_TAKER_MAKER_ORDER);
        for (final byte[] r : staged) {
            in.wrap(r);
            trade.wrapAndApplyHeader(in, 0, inHeader);
            final int base = e.limit(); // the entry's base offset: next() places the entry at the parent's limit
            g.next();
            final long eg = trade.egressSeq();
            final long js = trade.journalSeq();
            final long ep = trade.meEpoch();
            g.egressSeq(egress(eg, js, ep, 0)).tradeId(trade.tradeId()).marketId(trade.marketId())
                    .takerOrderId(swap ? trade.makerOrderId() : trade.takerOrderId()).takerUserId(trade.takerUserId())
                    .makerOrderId(swap ? trade.takerOrderId() : trade.makerOrderId()).makerUserId(trade.makerUserId())
                    .price(trade.price()).quantity(trade.quantity())
                    .meTimestamp(trade.timestamp()).takerOmsOrderId(trade.takerOmsOrderId())
                    .makerOmsOrderId(trade.makerOmsOrderId()).meEpoch(egress(eg, js, ep, 2)).journalSeq(egress(eg, js, ep, 1));
            writeBool(base + MeJournalTradeBatchEncoder.TradesEncoder.takerIsBuyEncodingOffset(), trade.takerIsBuyRaw(),
                    () -> g.takerIsBuy(BoolFlag.get(trade.takerIsBuyRaw())));
        }
        emit(HEADER + e.encodedLength());
    }

    /** A BoolFlag byte: raw, or (seams) the setter path / a collapse to 0/1. */
    private void writeBool(final int at, final short raw, final Runnable setterPath) {
        if (FeedHarnessSeams.on(FeedHarnessSeams.CLAMP_ENUMS)) {
            setterPath.run(); // BoolFlag.get throws on 2..255 (the clamp the contract forbids)
            return;
        }
        final int v = FeedHarnessSeams.on(FeedHarnessSeams.COLLAPSE_BOOL) ? (raw != 0 ? 1 : 0) : raw;
        buf.putByte(at, (byte) v);
    }

    private void writeEnum(final int at, final short raw, final Runnable setterPath) {
        if (FeedHarnessSeams.on(FeedHarnessSeams.CLAMP_ENUMS)) {
            setterPath.run();
            return;
        }
        buf.putByte(at, (byte) raw);
    }

    /** 63 from record 2. */
    private void terminal() {
        terminal.wrapAndApplyHeader(in, 0, inHeader);
        zero();
        final MeJournalTerminalEncoder e = new MeJournalTerminalEncoder().wrapAndApplyHeader(buf, 0, outHeader);
        final long eg = terminal.egressSeq();
        final long js = terminal.journalSeq();
        final long ep = terminal.meEpoch();
        e.egressSeq(egress(eg, js, ep, 0)).orderId(terminal.orderId()).userId(terminal.userId()).marketId(terminal.marketId())
                .meTimestamp(terminal.timestamp()).omsOrderId(terminal.omsOrderId()).meEpoch(egress(eg, js, ep, 2))
                .journalSeq(egress(eg, js, ep, 1));
        writeEnum(HEADER + MeJournalTerminalEncoder.statusEncodingOffset(), terminal.statusRaw(),
                () -> e.status(OrderStatus.get(terminal.statusRaw())));
        // rejectReason is a plain uint8 on both sides (optional, null 255 on schema 4): copied raw, 255 included
        buf.putByte(HEADER + MeJournalTerminalEncoder.rejectReasonEncodingOffset(),
                in.getByte(HEADER + TerminalDecoder.rejectReasonEncodingOffset()));
        emit(HEADER + MeJournalTerminalEncoder.BLOCK_LENGTH);
    }

    /** 64 from record 3. */
    private void outcome() {
        outcome.wrapAndApplyHeader(in, 0, inHeader);
        zero();
        final MeCommandOutcomeEncoder e = new MeCommandOutcomeEncoder().wrapAndApplyHeader(buf, 0, outHeader);
        final long eg = outcome.egressSeq();
        final long js = outcome.journalSeq();
        final long ep = outcome.meEpoch();
        e.egressSeq(egress(eg, js, ep, 0)).commandIdHigh(outcome.commandIdHigh()).commandIdLow(outcome.commandIdLow())
                .userId(outcome.userId()).omsOrderId(outcome.omsOrderId()).oldOrderId(outcome.oldOrderId())
                .price(outcome.price()).quantity(outcome.quantity()).budget(outcome.budget()).marketId(outcome.marketId())
                .commandKind(outcome.commandKind()).orderType(outcome.orderType()).orderSide(outcome.orderSide())
                .appliedPosition(outcome.appliedPosition()).meTimestamp(outcome.timestamp()).orderId(outcome.orderId())
                .status(outcome.status()).reason(outcome.reason()).oldCancelled(outcome.oldCancelled())
                .result(outcome.result()).meEpoch(egress(eg, js, ep, 2)).journalSeq(egress(eg, js, ep, 1));
        if (!FeedHarnessSeams.on(FeedHarnessSeams.DROP_64_TAIL)) {
            e.timeInForce(outcome.timeInForce()).expectedOldRemaining(outcome.expectedOldRemaining())
                    .ledgerSize(outcome.ledgerSize()).ledgerCapacity(outcome.ledgerCapacity())
                    .behaviorVersion(outcome.behaviorVersion());
        }
        emit(HEADER + MeCommandOutcomeEncoder.BLOCK_LENGTH);
    }

    /** 67 from record 4. */
    private void release(final int inLength) {
        release.wrapAndApplyHeader(in, 0, inHeader);
        zero();
        final MeCommandReleaseEncoder e = new MeCommandReleaseEncoder().wrapAndApplyHeader(buf, 0, outHeader);
        final long eg = release.egressSeq();
        final long js = release.journalSeq();
        final long ep = release.meEpoch();
        e.egressSeq(egress(eg, js, ep, 0)).journalSeq(egress(eg, js, ep, 1)).meEpoch(egress(eg, js, ep, 2))
                .releaseSeq(release.releaseSeq()).ledgerSize(release.ledgerSize()).ledgerCapacity(release.ledgerCapacity())
                .tombstoneRanges(release.tombstoneRanges());
        final CommandReleaseDecoder.ReleasesDecoder entries = release.releases();
        final int n = entries.count();
        if (FeedHarnessSeams.on(FeedHarnessSeams.GROUP_HEADER_U8)) {
            // two u8 instead of two u16: the group shifts by two bytes and the frame shrinks
            final int at = HEADER + MeCommandReleaseEncoder.BLOCK_LENGTH;
            buf.putByte(at, (byte) MeCommandReleaseEncoder.ReleasesEncoder.sbeBlockLength());
            buf.putByte(at + 1, (byte) n);
            int p = at + 2;
            for (final CommandReleaseDecoder.ReleasesDecoder r : entries) {
                buf.putLong(p, r.commandIdHigh(), ByteOrder.LITTLE_ENDIAN);
                buf.putLong(p + 8, r.idLowFloor(), ByteOrder.LITTLE_ENDIAN);
                buf.putByte(p + 16, (byte) r.scopeRaw());
                buf.putByte(p + 17, (byte) r.resultRaw());
                buf.putInt(p + 18, r.removed(), ByteOrder.LITTLE_ENDIAN);
                p += 22;
            }
            emit(p);
            return;
        }
        final MeCommandReleaseEncoder.ReleasesEncoder g = e.releasesCount(n);
        for (final CommandReleaseDecoder.ReleasesDecoder r : entries) {
            final int base = e.limit();
            g.next();
            g.commandIdHigh(r.commandIdHigh()).idLowFloor(r.idLowFloor()).removed(r.removed());
            final short scope = r.scopeRaw();
            final short result = r.resultRaw();
            writeEnum(base + MeCommandReleaseEncoder.ReleasesEncoder.scopeEncodingOffset(), scope,
                    () -> g.scope(ReleaseScope.get(scope)));
            writeEnum(base + MeCommandReleaseEncoder.ReleasesEncoder.resultEncodingOffset(), result,
                    () -> g.result(ReleaseResult.get(result)));
        }
        emit(HEADER + e.encodedLength());
    }

    /** 68 from record 5. */
    private void config() {
        config.wrapAndApplyHeader(in, 0, inHeader);
        zero();
        final MeEngineConfigEncoder e = new MeEngineConfigEncoder().wrapAndApplyHeader(buf, 0, outHeader);
        final long eg = config.egressSeq();
        final long js = config.journalSeq();
        final long ep = config.meEpoch();
        e.egressSeq(egress(eg, js, ep, 0)).journalSeq(egress(eg, js, ep, 1)).meEpoch(egress(eg, js, ep, 2))
                .configVersion(config.configVersion()).behaviorVersion(config.behaviorVersion())
                .ledgerCapacity(config.ledgerCapacity()).maxWireVersion(config.maxWireVersion());
        final short decision = config.decisionRaw();
        writeEnum(HEADER + MeEngineConfigEncoder.decisionEncodingOffset(), decision,
                () -> e.decision(ConfigDecision.get(decision)));
        emit(HEADER + MeEngineConfigEncoder.BLOCK_LENGTH);
    }
}

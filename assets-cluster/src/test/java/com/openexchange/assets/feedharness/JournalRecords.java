// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.match.infrastructure.mejournal.generated.CommandOutcomeEncoder;
import com.match.infrastructure.mejournal.generated.CommandReleaseEncoder;
import com.match.infrastructure.mejournal.generated.EngineConfigEncoder;
import com.match.infrastructure.mejournal.generated.MessageHeaderEncoder;
import com.match.infrastructure.mejournal.generated.TerminalEncoder;
import com.match.infrastructure.mejournal.generated.TradeEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/**
 * RC2 S9h: schema 4 journal records (schemaId 4, version 1) built with match-common's generated encoders from a zeroed
 * buffer, every enum byte written raw at its encoding offset (so any 0..255 is representable, as an ME bug could write
 * it). The input side of every scripted, random and story journal.
 */
public final class JournalRecords {

    public static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    private void zero() {
        buf.setMemory(0, buf.capacity(), (byte) 0);
    }

    private byte[] out(final int bodyLength) {
        final byte[] b = new byte[HEADER + bodyLength];
        buf.getBytes(0, b);
        return b;
    }

    public byte[] trade(long js, long epoch, long egress, long tradeId, int market, long takerLeg, long takerUser, long makerLeg,
                        long makerUser, long price, long qty, int takerIsBuyRaw, long ts, long takerOms, long makerOms) {
        zero();
        final TradeEncoder e = new TradeEncoder().wrapAndApplyHeader(buf, 0, header).journalSeq(js).meEpoch(epoch)
                .egressSeq(egress).tradeId(tradeId).marketId(market).takerOrderId(takerLeg).takerUserId(takerUser)
                .makerOrderId(makerLeg).makerUserId(makerUser).price(price).quantity(qty).timestamp(ts)
                .takerOmsOrderId(takerOms).makerOmsOrderId(makerOms);
        buf.putByte(HEADER + TradeEncoder.takerIsBuyEncodingOffset(), (byte) takerIsBuyRaw);
        return out(e.encodedLength());
    }

    public byte[] terminal(long js, long epoch, long egress, long leg, long user, int market, int statusRaw, long ts, long oms,
                           int rejectReasonRaw) {
        zero();
        final TerminalEncoder e = new TerminalEncoder().wrapAndApplyHeader(buf, 0, header).journalSeq(js).meEpoch(epoch)
                .egressSeq(egress).orderId(leg).userId(user).marketId(market).timestamp(ts).omsOrderId(oms);
        buf.putByte(HEADER + TerminalEncoder.statusEncodingOffset(), (byte) statusRaw);
        buf.putByte(HEADER + TerminalEncoder.rejectReasonEncodingOffset(), (byte) rejectReasonRaw);
        return out(e.encodedLength());
    }

    public byte[] outcome(long js, long epoch, long egress, long cmdHigh, long cmdLow, long user, long oms, long old, long price,
                          long qty, long budget, int market, int kind, int type, int side, long applied, long ts, long leg,
                          int status, int reason, int oldCancelled, int result, int tif, long expectedOldRemaining,
                          int ledgerSize, int ledgerCapacity, long behavior) {
        zero();
        final CommandOutcomeEncoder e = new CommandOutcomeEncoder().wrapAndApplyHeader(buf, 0, header).journalSeq(js)
                .meEpoch(epoch).egressSeq(egress).commandIdHigh(cmdHigh).commandIdLow(cmdLow).userId(user).omsOrderId(oms)
                .oldOrderId(old).price(price).quantity(qty).budget(budget).marketId(market).commandKind((short) kind)
                .orderType((short) type).orderSide((short) side).appliedPosition(applied).timestamp(ts).orderId(leg)
                .status(status).reason(reason).oldCancelled((short) oldCancelled).result(result).timeInForce((short) tif)
                .expectedOldRemaining(expectedOldRemaining).ledgerSize(ledgerSize).ledgerCapacity(ledgerCapacity)
                .behaviorVersion(behavior);
        return out(e.encodedLength());
    }

    /** {@code entries[i] = {commandIdHigh, idLowFloor, scopeRaw, resultRaw, removed}}. */
    public byte[] release(long js, long epoch, long egress, long releaseSeq, int ledgerSize, int ledgerCapacity,
                          int tombstoneRanges, long[]... entries) {
        zero();
        final CommandReleaseEncoder e = new CommandReleaseEncoder().wrapAndApplyHeader(buf, 0, header).journalSeq(js)
                .meEpoch(epoch).egressSeq(egress).releaseSeq(releaseSeq).ledgerSize(ledgerSize).ledgerCapacity(ledgerCapacity)
                .tombstoneRanges(tombstoneRanges);
        final CommandReleaseEncoder.ReleasesEncoder g = e.releasesCount(entries.length);
        for (final long[] x : entries) {
            final int base = e.limit();
            g.next().commandIdHigh(x[0]).idLowFloor(x[1]).removed((int) x[4]);
            buf.putByte(base + CommandReleaseEncoder.ReleasesEncoder.scopeEncodingOffset(), (byte) x[2]);
            buf.putByte(base + CommandReleaseEncoder.ReleasesEncoder.resultEncodingOffset(), (byte) x[3]);
        }
        return out(e.encodedLength());
    }

    public byte[] config(long js, long epoch, long egress, long configVersion, long behavior, long capacity, int maxWire,
                         int decisionRaw) {
        zero();
        final EngineConfigEncoder e = new EngineConfigEncoder().wrapAndApplyHeader(buf, 0, header).journalSeq(js).meEpoch(epoch)
                .egressSeq(egress).configVersion(configVersion).behaviorVersion(behavior).ledgerCapacity(capacity)
                .maxWireVersion(maxWire);
        buf.putByte(HEADER + EngineConfigEncoder.decisionEncodingOffset(), (byte) decisionRaw);
        return out(e.encodedLength());
    }

    // ------------------------------------------------------------------ head patching (copies, never in place)

    /** A copy of {@code record} with its journalSeq @0, meEpoch @8 and egressSeq @16 replaced. */
    public static byte[] withHead(final byte[] record, final long journalSeq, final long meEpoch, final long egressSeq) {
        final byte[] c = record.clone();
        final UnsafeBuffer b = new UnsafeBuffer(c);
        b.putLong(HEADER, journalSeq, ByteOrder.LITTLE_ENDIAN);
        b.putLong(HEADER + 8, meEpoch, ByteOrder.LITTLE_ENDIAN);
        b.putLong(HEADER + 16, egressSeq, ByteOrder.LITTLE_ENDIAN);
        return c;
    }

    public static byte[] withJournalSeq(final byte[] record, final long journalSeq) {
        final byte[] c = record.clone();
        new UnsafeBuffer(c).putLong(HEADER, journalSeq, ByteOrder.LITTLE_ENDIAN);
        return c;
    }

    public static long journalSeq(final byte[] record) {
        return new UnsafeBuffer(record).getLong(HEADER, ByteOrder.LITTLE_ENDIAN);
    }

    public static long meEpoch(final byte[] record) {
        return new UnsafeBuffer(record).getLong(HEADER + 8, ByteOrder.LITTLE_ENDIAN);
    }

    public static long egressSeq(final byte[] record) {
        return new UnsafeBuffer(record).getLong(HEADER + 16, ByteOrder.LITTLE_ENDIAN);
    }

    public static int templateId(final byte[] record) {
        return (record[2] & 0xFF) | (record[3] & 0xFF) << 8;
    }
}

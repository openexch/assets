// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.match.infrastructure.journal.generated.BooleanType;
import com.match.infrastructure.journal.generated.JournalTerminalDecoder;
import com.match.infrastructure.journal.generated.JournalTradeDecoder;
import com.match.infrastructure.journal.generated.MessageHeaderDecoder;

import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseEncoder;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Pure journal -> money wire translator: decodes one ME settlement-journal message (JournalTrade or
 * JournalTerminal, {@code com.match.infrastructure.journal.generated}, schema id=3) and encodes the
 * equivalent Assets Engine money-schema command (Settle or TerminalRelease, {@code
 * com.openexchange.assets.infrastructure.generated}, schema id=2). Field-for-field, nothing more: no
 * I/O, no retry, no dedup, no ordering decision.
 *
 * <p>The bridge forwards journal messages in journal order — that ordering discipline lives in the
 * agent loop (a separate, lead-owned change), not here. Idempotency on the money side lives entirely in
 * the Assets Engine: a tradeId high-water on Settle, and a hold-scoped no-op release on TerminalRelease
 * for holds that are unknown or already gone. This class does not, and structurally cannot (it only
 * ever sees one message at a time), participate in either — it must be bit-faithful and nothing more,
 * so that dedup/replay correctness stays a single, auditable property of the AE rather than being
 * smeared across the pipeline.</p>
 *
 * <p>{@code journalPosition} on both money messages is the journal's {@code egressSeq} carried through
 * verbatim: per the journal schema's own field doc this is an order key only (sparse, non-unique), not
 * a gap-detect sequence.</p>
 *
 * <p>The journal's JournalTerminal {@code status} (FILLED/CANCELLED/REJECTED) and {@code marketId} are
 * deliberately NOT carried onto TerminalRelease: releasing an order's residual hold means the same thing
 * regardless of which terminal status produced it, so every status maps to the identical TerminalRelease
 * shape.</p>
 *
 * <p><b>Not thread-safe.</b> The decoder/encoder flyweights are constructed once and re-wrapped on every
 * call (zero allocation after construction), so a single instance must be confined to one thread.</p>
 */
public final class JournalToMoneyTranslator {

    // Journal-side (match-common) decoders: reused flyweights, re-wrapped on every call.
    private final MessageHeaderDecoder journalHeader = new MessageHeaderDecoder();
    private final JournalTradeDecoder journalTrade = new JournalTradeDecoder();
    private final JournalTerminalDecoder journalTerminal = new JournalTerminalDecoder();

    // Money-side (assets-common) encoders: reused flyweights, re-wrapped on every call.
    private final MessageHeaderEncoder moneyHeader = new MessageHeaderEncoder();
    private final SettleEncoder settle = new SettleEncoder();
    private final TerminalReleaseEncoder terminalRelease = new TerminalReleaseEncoder();

    /**
     * Decodes a JournalTrade at {@code [offset, ...)} in {@code journalMsg} and encodes the equivalent
     * Settle command (money MessageHeader + body) at {@code outOffset} in {@code out}. Every field maps
     * directly (tradeId, marketId, taker/maker order+user ids, price, quantity, takerIsBuy);
     * {@code journalPosition} takes the journal's {@code egressSeq}. Returns the encoded length.
     */
    public int translateTrade(DirectBuffer journalMsg, int offset, MutableDirectBuffer out, int outOffset) {
        journalHeader.wrap(journalMsg, offset);
        final int blockLength = journalHeader.blockLength();
        final int version = journalHeader.version();
        journalTrade.wrap(journalMsg, offset + journalHeader.encodedLength(), blockLength, version);

        settle.wrapAndApplyHeader(out, outOffset, moneyHeader)
                .tradeId(journalTrade.tradeId())
                .marketId(journalTrade.marketId())
                // THE MONEY KEY: the AE holds are keyed by OMS order ids (placed before a
                // cluster id exists), so settles must reference those, not the cluster ids.
                .takerOrderId(journalTrade.takerOmsOrderId())
                .takerUserId(journalTrade.takerUserId())
                .makerOrderId(journalTrade.makerOmsOrderId())
                .makerUserId(journalTrade.makerUserId())
                .price(journalTrade.price())
                .quantity(journalTrade.quantity())
                .takerIsBuy(journalTrade.takerIsBuy() == BooleanType.TRUE ? BoolFlag.TRUE : BoolFlag.FALSE)
                .journalPosition(journalTrade.egressSeq());

        return moneyHeader.encodedLength() + settle.encodedLength();
    }

    /**
     * Decodes a JournalTerminal at {@code [offset, ...)} in {@code journalMsg} and encodes the
     * equivalent TerminalRelease command (money MessageHeader + body) at {@code outOffset} in
     * {@code out}. {@code orderId}, {@code userId} and {@code timestamp} map directly;
     * {@code journalPosition} takes the journal's {@code egressSeq}. The journal's {@code status} and
     * {@code marketId} are deliberately not carried (see class doc). Returns the encoded length.
     */
    public int translateTerminal(DirectBuffer journalMsg, int offset, MutableDirectBuffer out, int outOffset) {
        journalHeader.wrap(journalMsg, offset);
        final int blockLength = journalHeader.blockLength();
        final int version = journalHeader.version();
        journalTerminal.wrap(journalMsg, offset + journalHeader.encodedLength(), blockLength, version);

        terminalRelease.wrapAndApplyHeader(out, outOffset, moneyHeader)
                .journalPosition(journalTerminal.egressSeq())
                // Money key (see translateTrade). Slice terminals carry the parent's omsOrderId;
                // the AE suppresses the release on omsManagedRelease holds.
                .orderId(journalTerminal.omsOrderId())
                .userId(journalTerminal.userId())
                .timestamp(journalTerminal.timestamp());

        return moneyHeader.encodedLength() + terminalRelease.encodedLength();
    }

    /**
     * Header sniffer: true if the message at {@code [offset, ...)} in {@code journalMsg} is a
     * JournalTrade on the settlement-journal schema (schemaId==3, not just templateId==1 — a
     * templateId-only check could alias a message from a different schema entirely).
     */
    public boolean isTrade(DirectBuffer journalMsg, int offset) {
        journalHeader.wrap(journalMsg, offset);
        return journalHeader.schemaId() == JournalTradeDecoder.SCHEMA_ID
                && journalHeader.templateId() == JournalTradeDecoder.TEMPLATE_ID;
    }

    /**
     * Header sniffer: true if the message at {@code [offset, ...)} in {@code journalMsg} is a
     * JournalTerminal on the settlement-journal schema (schemaId==3, not just templateId==2).
     */
    public boolean isTerminal(DirectBuffer journalMsg, int offset) {
        journalHeader.wrap(journalMsg, offset);
        return journalHeader.schemaId() == JournalTerminalDecoder.SCHEMA_ID
                && journalHeader.templateId() == JournalTerminalDecoder.TEMPLATE_ID;
    }
}

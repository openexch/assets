// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * RC2 S9h 2.1: core feed fields -> schema 4 record, the inverse of the reference translator. Used only by the story
 * builder (its fake ME produces the core-side frames it applies to its model, and this turns them into the journal the
 * bridge reads). {@code translate(reverse(frame)) == frame} is a test ({@code reverseThenTranslateIsIdentity}). Enum
 * bytes are read raw and written raw.
 */
public final class MeJournalReverseEncoder {

    private final JournalRecords j = new JournalRecords();
    private final MessageHeaderDecoder h = new MessageHeaderDecoder();

    /** The schema 4 records of one money v7 feed frame (n records for a 62 of n entries). */
    public List<byte[]> reverse(final byte[] frame) {
        final UnsafeBuffer b = new UnsafeBuffer(frame);
        final boolean swap = FeedHarnessSeams.on(FeedHarnessSeams.REVERSE_SWAP_TAKER_MAKER_ORDER);
        final List<byte[]> out = new ArrayList<>();
        switch (FeedDigests.templateOf(frame)) {
            case 61 -> {
                final MeJournalTradeDecoder t = new MeJournalTradeDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(j.trade(t.journalSeq(), t.meEpoch(), t.egressSeq(), t.tradeId(), t.marketId(),
                        swap ? t.makerOrderId() : t.takerOrderId(), t.takerUserId(), swap ? t.takerOrderId() : t.makerOrderId(),
                        t.makerUserId(), t.price(), t.quantity(), t.takerIsBuyRaw(), t.meTimestamp(), t.takerOmsOrderId(),
                        t.makerOmsOrderId()));
            }
            case 62 -> {
                final MeJournalTradeBatchDecoder d = new MeJournalTradeBatchDecoder().wrapAndApplyHeader(b, 0, h);
                for (final MeJournalTradeBatchDecoder.TradesDecoder t : d.trades()) {
                    out.add(j.trade(t.journalSeq(), t.meEpoch(), t.egressSeq(), t.tradeId(), t.marketId(),
                            swap ? t.makerOrderId() : t.takerOrderId(), t.takerUserId(),
                            swap ? t.takerOrderId() : t.makerOrderId(), t.makerUserId(), t.price(), t.quantity(),
                            t.takerIsBuyRaw(), t.meTimestamp(), t.takerOmsOrderId(), t.makerOmsOrderId()));
                }
            }
            case 63 -> {
                final MeJournalTerminalDecoder t = new MeJournalTerminalDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(j.terminal(t.journalSeq(), t.meEpoch(), t.egressSeq(), t.orderId(), t.userId(), t.marketId(),
                        t.statusRaw(), t.meTimestamp(), t.omsOrderId(), t.rejectReason()));
            }
            case 64 -> {
                final MeCommandOutcomeDecoder o = new MeCommandOutcomeDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(j.outcome(o.journalSeq(), o.meEpoch(), o.egressSeq(), o.commandIdHigh(), o.commandIdLow(), o.userId(),
                        o.omsOrderId(), o.oldOrderId(), o.price(), o.quantity(), o.budget(), o.marketId(), o.commandKind(),
                        o.orderType(), o.orderSide(), o.appliedPosition(), o.meTimestamp(), o.orderId(), o.status(), o.reason(),
                        o.oldCancelled(), o.result(), o.timeInForce(), o.expectedOldRemaining(), o.ledgerSize(),
                        o.ledgerCapacity(), o.behaviorVersion()));
            }
            case 67 -> {
                final MeCommandReleaseDecoder d = new MeCommandReleaseDecoder().wrapAndApplyHeader(b, 0, h);
                final long js = d.journalSeq();
                final long ep = d.meEpoch();
                final long eg = d.egressSeq();
                final long rs = d.releaseSeq();
                final int ls = d.ledgerSize();
                final int lc = d.ledgerCapacity();
                final int tr = d.tombstoneRanges();
                final List<long[]> e = new ArrayList<>();
                for (final MeCommandReleaseDecoder.ReleasesDecoder r : d.releases()) {
                    e.add(new long[] {r.commandIdHigh(), r.idLowFloor(), r.scopeRaw(), r.resultRaw(), r.removed()});
                }
                out.add(j.release(js, ep, eg, rs, ls, lc, tr, e.toArray(new long[0][])));
            }
            case 68 -> {
                final MeEngineConfigDecoder c = new MeEngineConfigDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(j.config(c.journalSeq(), c.meEpoch(), c.egressSeq(), c.configVersion(), c.behaviorVersion(),
                        c.ledgerCapacity(), c.maxWireVersion(), c.decisionRaw()));
            }
            default -> throw new IllegalArgumentException("not a feed frame: template " + FeedDigests.templateOf(frame));
        }
        return out;
    }
}

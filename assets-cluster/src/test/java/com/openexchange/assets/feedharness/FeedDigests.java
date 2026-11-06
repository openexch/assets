// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.match.infrastructure.mejournal.generated.CommandOutcomeDecoder;
import com.match.infrastructure.mejournal.generated.CommandReleaseDecoder;
import com.match.infrastructure.mejournal.generated.EngineConfigDecoder;
import com.match.infrastructure.mejournal.generated.TerminalDecoder;
import com.match.infrastructure.mejournal.generated.TradeDecoder;
import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.order.MeOutcomeView;
import com.openexchange.assets.application.engine.order.MeReleaseView;
import com.openexchange.assets.domain.ConfigDecision;
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
 * RC2 S9h 5.2: the digests of a schema 4 record (read with match-common's decoders) and of a money v7 frame (read with
 * the assets generated decoders), in two forms: {@link RawDigests} (every enum byte raw; complete for any value) and the
 * production {@code AssetsEngine} 3.8 functions (in-range values only, the form the core stores per applied seq).
 * A 62 yields one digest per entry; every other frame one.
 */
public final class FeedDigests {

    private FeedDigests() {
    }

    // ------------------------------------------------------------------ source (schema 4)

    /** RawDigests of one schema 4 record (template 4 included). */
    public static long rawOfRecord(final byte[] r) {
        final UnsafeBuffer b = new UnsafeBuffer(r);
        final com.match.infrastructure.mejournal.generated.MessageHeaderDecoder h =
                new com.match.infrastructure.mejournal.generated.MessageHeaderDecoder();
        switch (templateOf(r)) {
            case 1: {
                final TradeDecoder t = new TradeDecoder().wrapAndApplyHeader(b, 0, h);
                return RawDigests.trade(t.egressSeq(), t.journalSeq(), t.tradeId(), t.marketId(), t.takerOrderId(),
                        t.takerUserId(), t.makerOrderId(), t.makerUserId(), t.price(), t.quantity(), t.takerIsBuyRaw(),
                        t.timestamp(), t.takerOmsOrderId(), t.makerOmsOrderId(), t.meEpoch());
            }
            case 2: {
                final TerminalDecoder t = new TerminalDecoder().wrapAndApplyHeader(b, 0, h);
                return RawDigests.terminal(t.egressSeq(), t.journalSeq(), t.orderId(), t.userId(), t.marketId(), t.statusRaw(),
                        b.getByte(8 + TerminalDecoder.rejectReasonEncodingOffset()) & 0xFF, t.timestamp(), t.omsOrderId(),
                        t.meEpoch());
            }
            case 3: {
                final CommandOutcomeDecoder o = new CommandOutcomeDecoder().wrapAndApplyHeader(b, 0, h);
                return RawDigests.outcome(o.egressSeq(), o.journalSeq(), o.commandIdHigh(), o.commandIdLow(), o.userId(),
                        o.omsOrderId(), o.oldOrderId(), o.price(), o.quantity(), o.budget(), o.marketId(), o.commandKind(),
                        o.result(), o.orderId(), o.reason(), o.oldCancelled(), o.timestamp(), o.meEpoch(), o.orderType(),
                        o.orderSide(), o.appliedPosition(), o.status(), o.timeInForce(), o.expectedOldRemaining(),
                        o.ledgerSize(), o.ledgerCapacity(), o.behaviorVersion());
            }
            case 4: {
                final CommandReleaseDecoder d = new CommandReleaseDecoder().wrapAndApplyHeader(b, 0, h);
                final long eg = d.egressSeq();
                final long js = d.journalSeq();
                final long ep = d.meEpoch();
                final long rs = d.releaseSeq();
                final long ls = d.ledgerSize();
                final long lc = d.ledgerCapacity();
                final long tr = d.tombstoneRanges();
                final CommandReleaseDecoder.ReleasesDecoder g = d.releases();
                final int n = g.count();
                final long[] hi = new long[n];
                final long[] fl = new long[n];
                final int[] sc = new int[n];
                final int[] rs2 = new int[n];
                final int[] rm = new int[n];
                int i = 0;
                for (final CommandReleaseDecoder.ReleasesDecoder e : g) {
                    hi[i] = e.commandIdHigh();
                    fl[i] = e.idLowFloor();
                    sc[i] = e.scopeRaw();
                    rs2[i] = e.resultRaw();
                    rm[i] = e.removed();
                    i++;
                }
                return RawDigests.release(eg, js, ep, rs, ls, lc, tr, n, hi, fl, sc, rs2, rm);
            }
            case 5: {
                final EngineConfigDecoder c = new EngineConfigDecoder().wrapAndApplyHeader(b, 0, h);
                return RawDigests.config(c.egressSeq(), c.journalSeq(), c.meEpoch(), c.configVersion(), c.behaviorVersion(),
                        c.ledgerCapacity(), c.maxWireVersion(), c.decisionRaw());
            }
            default:
                throw new IllegalArgumentException("record template " + templateOf(r));
        }
    }

    /** The production 3.8 digest of one schema 4 record's fields (in-range enum values only). */
    public static long productionOfRecord(final byte[] r) {
        final UnsafeBuffer b = new UnsafeBuffer(r);
        final com.match.infrastructure.mejournal.generated.MessageHeaderDecoder h =
                new com.match.infrastructure.mejournal.generated.MessageHeaderDecoder();
        switch (templateOf(r)) {
            case 1: {
                final TradeDecoder t = new TradeDecoder().wrapAndApplyHeader(b, 0, h);
                return AssetsEngine.tradeDigest(t.egressSeq(), t.journalSeq(), t.tradeId(), t.marketId(), t.takerOrderId(),
                        t.takerUserId(), t.makerOrderId(), t.makerUserId(), t.price(), t.quantity(), t.takerIsBuyRaw() == 1,
                        t.timestamp(), t.takerOmsOrderId(), t.makerOmsOrderId(), t.meEpoch());
            }
            case 2: {
                final TerminalDecoder t = new TerminalDecoder().wrapAndApplyHeader(b, 0, h);
                return AssetsEngine.terminalDigest(t.egressSeq(), t.journalSeq(), t.orderId(), t.userId(), t.marketId(),
                        t.statusRaw(), b.getByte(8 + TerminalDecoder.rejectReasonEncodingOffset()) & 0xFF, t.timestamp(),
                        t.omsOrderId(), t.meEpoch());
            }
            case 3: {
                final CommandOutcomeDecoder o = new CommandOutcomeDecoder().wrapAndApplyHeader(b, 0, h);
                final MeOutcomeView v = new MeOutcomeView().reset();
                v.egressSeq = o.egressSeq();
                v.journalSeq = o.journalSeq();
                v.meEpoch = o.meEpoch();
                v.commandIdHigh = o.commandIdHigh();
                v.commandIdLow = o.commandIdLow();
                v.userId = o.userId();
                v.omsOrderId = o.omsOrderId();
                v.oldOrderId = o.oldOrderId();
                v.price = o.price();
                v.quantity = o.quantity();
                v.budget = o.budget();
                v.marketId = o.marketId();
                v.commandKind = o.commandKind();
                v.orderType = o.orderType();
                v.orderSide = o.orderSide();
                v.appliedPosition = o.appliedPosition();
                v.meTimestamp = o.timestamp();
                v.orderId = o.orderId();
                v.status = o.status();
                v.reason = o.reason();
                v.oldCancelled = o.oldCancelled() == 1;
                v.result = o.result();
                v.timeInForce = o.timeInForce();
                v.expectedOldRemaining = o.expectedOldRemaining();
                v.ledgerSize = o.ledgerSize();
                v.ledgerCapacity = o.ledgerCapacity();
                v.behaviorVersion = o.behaviorVersion();
                return AssetsEngine.outcomeDigest(v);
            }
            case 4: {
                final CommandReleaseDecoder d = new CommandReleaseDecoder().wrapAndApplyHeader(b, 0, h);
                final MeReleaseView v = new MeReleaseView().reset();
                v.egressSeq = d.egressSeq();
                v.journalSeq = d.journalSeq();
                v.meEpoch = d.meEpoch();
                v.releaseSeq = d.releaseSeq();
                v.ledgerSize = d.ledgerSize();
                v.ledgerCapacity = d.ledgerCapacity();
                v.tombstoneRanges = d.tombstoneRanges();
                for (final CommandReleaseDecoder.ReleasesDecoder e : d.releases()) {
                    v.add(e.commandIdHigh(), e.idLowFloor(), e.scopeRaw(), e.resultRaw(), e.removed());
                }
                return AssetsEngine.releaseDigest(v);
            }
            case 5: {
                final EngineConfigDecoder c = new EngineConfigDecoder().wrapAndApplyHeader(b, 0, h);
                return AssetsEngine.configDigest(c.egressSeq(), c.journalSeq(), c.meEpoch(), c.configVersion(),
                        c.behaviorVersion(), c.ledgerCapacity(), c.maxWireVersion(), ConfigDecision.fromCode(c.decisionRaw()));
            }
            default:
                throw new IllegalArgumentException("record template " + templateOf(r));
        }
    }

    // ------------------------------------------------------------------ output (money v7)

    /** RawDigests of one money v7 frame, read with the assets generated decoders: one per 62 entry, else one. */
    public static List<Long> rawOfFrame(final byte[] f) {
        final UnsafeBuffer b = new UnsafeBuffer(f);
        final MessageHeaderDecoder h = new MessageHeaderDecoder();
        final List<Long> out = new ArrayList<>();
        switch (templateOf(f)) {
            case 61: {
                final MeJournalTradeDecoder t = new MeJournalTradeDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(RawDigests.trade(t.egressSeq(), t.journalSeq(), t.tradeId(), t.marketId(), t.takerOrderId(),
                        t.takerUserId(), t.makerOrderId(), t.makerUserId(), t.price(), t.quantity(), t.takerIsBuyRaw(),
                        t.meTimestamp(), t.takerOmsOrderId(), t.makerOmsOrderId(), t.meEpoch()));
                break;
            }
            case 62: {
                final MeJournalTradeBatchDecoder d = new MeJournalTradeBatchDecoder().wrapAndApplyHeader(b, 0, h);
                for (final MeJournalTradeBatchDecoder.TradesDecoder t : d.trades()) {
                    out.add(RawDigests.trade(t.egressSeq(), t.journalSeq(), t.tradeId(), t.marketId(), t.takerOrderId(),
                            t.takerUserId(), t.makerOrderId(), t.makerUserId(), t.price(), t.quantity(), t.takerIsBuyRaw(),
                            t.meTimestamp(), t.takerOmsOrderId(), t.makerOmsOrderId(), t.meEpoch()));
                }
                break;
            }
            case 63: {
                final MeJournalTerminalDecoder t = new MeJournalTerminalDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(RawDigests.terminal(t.egressSeq(), t.journalSeq(), t.orderId(), t.userId(), t.marketId(), t.statusRaw(),
                        t.rejectReason(), t.meTimestamp(), t.omsOrderId(), t.meEpoch()));
                break;
            }
            case 64: {
                final MeCommandOutcomeDecoder o = new MeCommandOutcomeDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(RawDigests.outcome(o.egressSeq(), o.journalSeq(), o.commandIdHigh(), o.commandIdLow(), o.userId(),
                        o.omsOrderId(), o.oldOrderId(), o.price(), o.quantity(), o.budget(), o.marketId(), o.commandKind(),
                        o.result(), o.orderId(), o.reason(), o.oldCancelled(), o.meTimestamp(), o.meEpoch(), o.orderType(),
                        o.orderSide(), o.appliedPosition(), o.status(), o.timeInForce(), o.expectedOldRemaining(),
                        o.ledgerSize(), o.ledgerCapacity(), o.behaviorVersion()));
                break;
            }
            case 67: {
                final MeCommandReleaseDecoder d = new MeCommandReleaseDecoder().wrapAndApplyHeader(b, 0, h);
                final long eg = d.egressSeq();
                final long js = d.journalSeq();
                final long ep = d.meEpoch();
                final long rs = d.releaseSeq();
                final long ls = d.ledgerSize();
                final long lc = d.ledgerCapacity();
                final long tr = d.tombstoneRanges();
                final MeCommandReleaseDecoder.ReleasesDecoder g = d.releases();
                final int n = g.count();
                final long[] hi = new long[n];
                final long[] fl = new long[n];
                final int[] sc = new int[n];
                final int[] rr = new int[n];
                final int[] rm = new int[n];
                int i = 0;
                for (final MeCommandReleaseDecoder.ReleasesDecoder e : g) {
                    hi[i] = e.commandIdHigh();
                    fl[i] = e.idLowFloor();
                    sc[i] = e.scopeRaw();
                    rr[i] = e.resultRaw();
                    rm[i] = e.removed();
                    i++;
                }
                out.add(RawDigests.release(eg, js, ep, rs, ls, lc, tr, n, hi, fl, sc, rr, rm));
                break;
            }
            case 68: {
                final MeEngineConfigDecoder c = new MeEngineConfigDecoder().wrapAndApplyHeader(b, 0, h);
                out.add(RawDigests.config(c.egressSeq(), c.journalSeq(), c.meEpoch(), c.configVersion(), c.behaviorVersion(),
                        c.ledgerCapacity(), c.maxWireVersion(), c.decisionRaw()));
                break;
            }
            default:
                throw new IllegalArgumentException("frame template " + templateOf(f));
        }
        return out;
    }

    public static int templateOf(final byte[] f) {
        return (f[2] & 0xFF) | (f[3] & 0xFF) << 8;
    }

    public static long journalSeqOfRecord(final byte[] r) {
        return new UnsafeBuffer(r).getLong(8, java.nio.ByteOrder.LITTLE_ENDIAN);
    }
}

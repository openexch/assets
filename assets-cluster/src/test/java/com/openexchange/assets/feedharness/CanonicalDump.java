// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotBatchDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.OrderSnapshotBatchDecoder;
import com.openexchange.assets.infrastructure.generated.OrderSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * RC2 S9h 7.3 {@code oracle}: the canonical money dump of a core, read through the core's own queries as ADMIN
 * (RequestBalanceSnapshot, RequestHoldSnapshot, RequestOrderSnapshot paged, QueryFeedPosition), one line per fact, sorted:
 * {@code B user asset available locked}, {@code H order user asset remaining}, {@code O order user market status price
 * qty filled remaining holdAsset hold leg sliceSeq}, one {@code F consume lastTradeId epoch jseq} and one {@code L halted
 * reason} (the latch, reported, not part of the money comparison). No time
 * and no session fact is in it, so a live cluster (real log time, real session ids) and the in-process model of the same
 * story give the same dump. Its sha256 is what the oracle compares.
 */
public final class CanonicalDump {

    public static final int PAGE = 256;

    private CanonicalDump() {
    }

    /**
     * The sha256 of the MONEY lines (B, H, O, F): the latch line {@code L halted reason} is reported but not hashed, so a
     * core halted on a hole at e compares equal to the clean prefix up to e - 1 (S9b 13.17's hole class, review FIX-4).
     */
    public static String sha256(final List<String> lines) {
        return FeedFiles.sha256Hex(String.join("\n", money(lines)).getBytes(StandardCharsets.US_ASCII));
    }

    public static List<String> money(final List<String> lines) {
        final List<String> out = new ArrayList<>();
        for (final String l : lines) {
            if (!l.startsWith("L ")) {
                out.add(l);
            }
        }
        return out;
    }

    private static String o(final long order, final long user, final int market, final int status, final long price,
                            final long qty, final long filled, final long remaining, final int holdAsset, final long hold,
                            final long leg, final int slice) {
        return "O " + order + " " + user + " " + market + " " + status + " " + price + " " + qty + " " + filled + " " + remaining
                + " " + holdAsset + " " + hold + " " + leg + " " + slice;
    }

    // ------------------------------------------------------------------ the in-process model

    public static List<String> of(final CoreApplyRunner r, final long t) {
        final CoreFrames enc = new CoreFrames();
        final TreeSet<String> lines = new TreeSet<>();
        int from = r.sink().events().size();
        r.apply(CoreApplyRunner.ADMIN_SESSION, enc.balanceSnapshot(0x0BA1L), t);
        r.apply(CoreApplyRunner.ADMIN_SESSION, enc.holdSnapshot(0x0BA2L), t);
        long next = 0;
        while (true) {
            r.apply(CoreApplyRunner.ADMIN_SESSION, enc.orderSnapshot(0x0BA3L, 0, next), t);
            long n = 0;
            for (int i = from; i < r.sink().events().size(); i++) {
                final AssetsEngineEvent e = r.sink().events().get(i);
                if (e instanceof AssetsEngineEvent.Balance b) {
                    lines.add("B " + b.userId() + " " + b.assetId() + " " + b.available() + " " + b.locked());
                } else if (e instanceof AssetsEngineEvent.HoldSnapshotEntry h) {
                    lines.add("H " + h.orderId() + " " + h.userId() + " " + h.assetId() + " " + h.remaining());
                } else if (e instanceof AssetsEngineEvent.OrderSnapshotEntry s) {
                    lines.add(o(s.orderId(), s.userId(), s.marketId(), s.status() == null ? -1 : s.status().code(), s.price(),
                            s.quantity(), s.filledQty(), s.remainingQty(), s.holdAssetId(), s.holdAmount(), s.meOrderId(),
                            s.sliceSeq()));
                } else if (e instanceof AssetsEngineEvent.OrderSnapshotEnd end) {
                    n = end.nextOrderId();
                }
            }
            from = r.sink().events().size();
            if (n == 0 || n <= next) {
                break;
            }
            next = n;
        }
        final CoreApplyRunner.Report f = r.query(CoreApplyRunner.ADMIN_SESSION, 0x0BA4L, t);
        final List<String> out = new ArrayList<>(lines);
        out.add("F " + f.consumePosition() + " " + f.lastAppliedTradeId() + " " + f.meEpoch() + " " + f.lastAppliedJournalSeq());
        out.add("L " + f.feedHalted() + " " + f.haltReason());
        return out;
    }

    // ------------------------------------------------------------------ a live cluster

    public static List<String> of(final T1Session admin) {
        final CoreFrames enc = new CoreFrames();
        final MessageHeaderDecoder h = new MessageHeaderDecoder();
        final TreeSet<String> lines = new TreeSet<>();
        admin.skipToEnd();
        admin.offer(enc.balanceSnapshot(0x0BA1L));
        collect(admin, h, lines, 18, 0x0BA1L);
        admin.offer(enc.holdSnapshot(0x0BA2L));
        collect(admin, h, lines, 20, 0x0BA2L);
        long next = 0;
        while (true) {
            admin.offer(enc.orderSnapshot(0x0BA3L, 0, next));
            final byte[] end = collect(admin, h, lines, 107, 0x0BA3L);
            final long n = new OrderSnapshotEndDecoder().wrapAndApplyHeader(new UnsafeBuffer(end), 0, h).nextOrderId();
            if (n == 0 || n <= next) {
                break;
            }
            next = n;
        }
        admin.offer(enc.query(0x0BA4L));
        final byte[] rep = admin.next(x -> T1Session.templateOf(x) == 17 && T1Session.correlationOf(x) == 0x0BA4L,
                T1Session.TIMEOUT_MS);
        if (rep == null) {
            throw new IllegalStateException("no FeedPositionReport");
        }
        final FeedPositionReportDecoder f = new FeedPositionReportDecoder().wrapAndApplyHeader(new UnsafeBuffer(rep), 0, h);
        final List<String> out = new ArrayList<>(lines);
        out.add("F " + f.consumePosition() + " " + f.lastAppliedTradeId() + " " + f.meEpoch() + " " + f.lastAppliedJournalSeq());
        out.add("L " + (f.feedHaltedRaw() == 1) + " " + f.haltReasonRaw());
        return out;
    }

    /** Collects the dump frames until the end template {@code endTemplate} with {@code corr}; returns the end frame. */
    private static byte[] collect(final T1Session s, final MessageHeaderDecoder h, final TreeSet<String> lines,
                                  final int endTemplate, final long corr) {
        while (true) {
            final byte[] f = s.next(x -> true, T1Session.TIMEOUT_MS);
            if (f == null) {
                throw new IllegalStateException("the dump stopped before template " + endTemplate);
            }
            final UnsafeBuffer b = new UnsafeBuffer(f);
            switch (T1Session.templateOf(f)) {
                case 12 -> {
                    final BalanceUpdateDecoder d = new BalanceUpdateDecoder().wrapAndApplyHeader(b, 0, h);
                    lines.add("B " + d.userId() + " " + d.assetId() + " " + d.available() + " " + d.locked());
                }
                case 22 -> {
                    for (final BalanceUpdateBatchDecoder.UpdatesDecoder d
                            : new BalanceUpdateBatchDecoder().wrapAndApplyHeader(b, 0, h).updates()) {
                        lines.add("B " + d.userId() + " " + d.assetId() + " " + d.available() + " " + d.locked());
                    }
                }
                case 19 -> {
                    final HoldSnapshotEntryDecoder d = new HoldSnapshotEntryDecoder().wrapAndApplyHeader(b, 0, h);
                    lines.add("H " + d.orderId() + " " + d.userId() + " " + d.assetId() + " " + d.remaining());
                }
                case 25 -> {
                    for (final HoldSnapshotBatchDecoder.HoldsDecoder d
                            : new HoldSnapshotBatchDecoder().wrapAndApplyHeader(b, 0, h).holds()) {
                        lines.add("H " + d.orderId() + " " + d.userId() + " " + d.assetId() + " " + d.remaining());
                    }
                }
                case 106 -> {
                    for (final OrderSnapshotBatchDecoder.OrdersDecoder d
                            : new OrderSnapshotBatchDecoder().wrapAndApplyHeader(b, 0, h).orders()) {
                        lines.add(o(d.orderId(), d.userId(), d.marketId(), d.statusRaw(), d.price(), d.quantity(), d.filledQty(),
                                d.remainingQty(), d.holdAssetId(), d.holdAmount(), d.meOrderId(), d.sliceSeq()));
                    }
                }
                default -> { }
            }
            if (T1Session.templateOf(f) == endTemplate && T1Session.correlationOf(f) == corr) {
                return f;
            }
        }
    }
}

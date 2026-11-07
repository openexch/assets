// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreOrderView;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.ReleaseScope;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G7: the pending-dispatch index and the RequestPendingDispatch 65 reply rules (3.8, every behaviour). */
public class PendingDispatchIndexTest {

    /** Counts replies, allocates nothing. */
    static final class CountingSink implements AssetsEventSink {
        long replies;
        long ends;
        long lastCount;
        long lastHead;
        public void onDepositAck(long a, long b, int c, long d, long e, boolean f) { }
        public void onWithdrawAck(long a, long b, int c, long d, long e, boolean f) { }
        public void onHoldAck(long a, long b, long c, int d, long e) { }
        public void onHoldReject(long a, long b, long c, int d, long e, RejectReason f) { }
        public void onBalanceUpdate(long a, int b, long c, long d) { }
        public void onSettlementApplied(long a, long b, long c) { }
        public void onWithdrawReject(long a, long b, int c, long d, RejectReason e) { }
        public void onBalanceSnapshotEnd(long a, int b) { }
        public void onHoldSnapshotEntry(long a, long b, int c, long d) { }
        public void onHoldSnapshotEnd(long a, int b) { }
        public void onFeedPositionReport(long a, long b, long c, boolean d) { }
        public void onSettleFault(long a, long b, long c, int d, long e, long f) { }
        public void onSettleRejected(long a, int b, long c, SettleRejectReason d, long e) { }
        public void onOrderReject(long a, long b, long c, long d, long e, int f, int g, CoreRejectReason h, int i, long j, long k) { }
        public void onAdminAck(long a, int b, AdminResult c, int d, long e) { }
        public void onAdmissionState(int a, GateState b, GateReason c, boolean d, long e, long f, boolean g) { }
        public void onOrderAck(long a, long b, long c, long d, long e, int f, OrderStatus g, boolean h, int i, long j, long k, long l) { }
        public void onOrderEvent(long a, long b, CoreOrderView c, EventKind d, CoreRejectReason e, long f, long g, long h, long i, long j) { }
        public void onMeDispatch(long a, long b, long c, DispatchKind d, long e, long f, long g, int h, int i, int j, long k, long l,
                                 long m, int n) { }
        public void onDepositReject(long a, long b, long c, long d, int e, long f, RejectReason g) { }
        public void onConfigApplied(ConfigKind a, int b, long c, long d) { }
        @Override
        public void onMeDispatchReply(long a, long b, long c, DispatchKind d, long e, long f, long g, int h, int i, int j, long k,
                                      long l, long m, int n, ReleaseScope o, long p) {
            replies++;
        }
        @Override
        public void onPendingDispatchEnd(long correlationId, int count, long headDispatchSeq) {
            ends++;
            lastCount = count;
            lastHead = headDispatchSeq;
        }
    }

    /** A v1 core with {@code live} orders, all acked but the last {@code pending} creates. */
    private static OrderRig book(final int live, final int pending) {
        final OrderRig r = OrderRig.open();
        final RiskConfigSetCommand cfg = r.riskConfig(1, 1);
        cfg.maxOrdersPerSec = Integer.MAX_VALUE;
        cfg.maxOrdersPerMin = Integer.MAX_VALUE;
        cfg.maxOpenOrders = Integer.MAX_VALUE;
        r.setRisk(cfg);
        final int users = Math.max(1, live / 200);
        for (long u = 1; u <= users; u++) {
            r.deposit(u, USD, 10_000_000 * ONE);
        }
        r.engine.setEventSink(new CountingSink());
        for (int i = 0; i < live; i++) {
            final long u = 1 + (i % users);
            r.orders.applyCreate(r.limit(u, 1, 0, 100 * ONE, ONE / 100), r.now);
            final long h = i + 1;
            if (i < live - pending) {
                r.orders.applyOutcome(i + 1, h, 0, u, h, 0, 0, 0, 0, 1, 0, 0, 10_000 + h, 0, r.now, 0, r.now);
            }
        }
        assertEquals(live, r.orders.orders().size());
        return r;
    }

    private static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    private static long allocated() {
        return THREADS.getCurrentThreadAllocatedBytes();
    }

    @Test
    public void resyncPageCostIsIndependentOfLiveOrderCount() {
        final long[] visited = new long[2];
        int k = 0;
        for (final int live : new int[] {1_000, 100_000}) {
            final OrderRig r = book(live, 10);
            final CountingSink s = (CountingSink) r.engine.eventSink();
            for (int warm = 0; warm < 3; warm++) {
                r.orders.applyRequestPendingDispatch(1, 0L, 1024);
            }
            s.replies = 0;
            long calibration = Long.MAX_VALUE;
            for (int i = 0; i < 5; i++) {
                final long a = allocated();
                calibration = Math.min(calibration, allocated() - a); // what the probe itself costs
            }
            final long before = allocated();
            r.orders.applyRequestPendingDispatch(2, 0L, 1024);
            final long bytes = allocated() - before - calibration;
            assertEquals("the ten pending creates", 10, s.replies);
            assertEquals(10, s.lastCount);
            assertTrue("visited <= 10 + 64 at " + live + " live: " + r.orders.pendingReplyVisited(),
                    r.orders.pendingReplyVisited() <= 10 + 64);
            assertEquals("zero bytes allocated by the reply at " + live + " live", 0L, bytes);
            visited[k++] = r.orders.pendingReplyVisited();
        }
        // dead slots in range depend on where the last compaction fell; both stay under the bound, 100x the orders or not
        System.out.println("[S8-RESYNC-COST] visited at 1k live=" + visited[0] + " at 100k live=" + visited[1] + " (bound 74)");
        assertTrue("the cost does not grow with the live order count", visited[1] <= 10 + 64 && visited[0] <= 10 + 64);
    }

    @Test
    public void headQueryDoesNotScan() {
        final OrderRig r = book(50, 5);
        final CountingSink s = (CountingSink) r.engine.eventSink();
        r.orders.applyRequestPendingDispatch(3, 0L, 0);
        assertEquals("no entry", 0, s.replies);
        assertEquals("count 0", 0, s.lastCount);
        assertEquals("head = dispatchSeq", r.engine.getDispatchSeq(), s.lastHead);
        assertEquals("no index visit", 0, r.orders.pendingReplyVisited());
    }

    @Test
    public void negativeMaxIsRefused() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.markNow();
        r.orders.applyRequestPendingDispatch(0x65, 0L, -3);
        assertEquals("one OrderReject, nothing else", 1, r.events().size());
        final AssetsEngineEvent.OrderReject rej = r.first(AssetsEngineEvent.OrderReject.class);
        assertEquals(CoreRejectReason.INVALID, rej.reason());
        assertEquals(65, rej.refTemplateId());
        assertEquals("detail = maxEntries", -3L, rej.detail());
        assertEquals(0x65, rej.correlationId());
    }

    /** The brute-force answer: every live pending record and every RELEASE/ORPHAN backlog entry, by seq. */
    private static List<Long> scan(final OrderEngine oe, final long from) {
        final TreeSet<Long> seqs = new TreeSet<>();
        oe.orders().forEachByOrderId(o -> {
            if (o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingDispatchSeq > from) {
                seqs.add(o.pendingDispatchSeq);
            }
        });
        oe.backlog().forEachByOrderId(b -> {
            if (b.stage != ReleaseBacklog.STAGE_PARKED && b.dispatchSeq > from) {
                seqs.add(b.dispatchSeq);
            }
        });
        return new ArrayList<>(seqs);
    }

    private static List<Long> reply(final V3Rig r, final com.openexchange.assets.application.engine.AssetsEngine e, final long from,
                                    final int max) {
        final com.openexchange.assets.determinism.RecordingAssetsSink sink = new com.openexchange.assets.determinism.RecordingAssetsSink();
        final AssetsEventSink saved = e.eventSink();
        e.setEventSink(sink);
        e.orders().applyRequestPendingDispatch(9, from, max);
        e.setEventSink(saved);
        final List<Long> out = new ArrayList<>();
        for (final AssetsEngineEvent ev : sink.events()) {
            if (ev instanceof AssetsEngineEvent.MeDispatchReply d) {
                out.add(d.dispatchSeq());
            }
        }
        return out;
    }

    @Test
    public void indexEqualsScanAfterRandomOps() {
        for (long seed = 21; seed <= 24; seed++) {
            final V3CoreFuzzDriver d = new V3CoreFuzzDriver(seed, true);
            final Random rnd = new Random(seed);
            for (int round = 0; round < 12; round++) {
                d.run(30);
                final OrderEngine oe = d.r.engine.orders();
                final long from = rnd.nextInt((int) Math.max(1, d.r.engine.getDispatchSeq()));
                assertEquals("seed " + seed + " round " + round + ": index == scan", scan(oe, from),
                        reply(d.r, d.r.engine, from, 4096));
                // paging walks the same list
                final List<Long> paged = new ArrayList<>();
                long cursor = 0;
                while (true) {
                    final List<Long> page = reply(d.r, d.r.engine, cursor, 3);
                    paged.addAll(page);
                    if (page.size() < 3) {
                        break;
                    }
                    cursor = page.get(page.size() - 1);
                }
                assertEquals("pages of 3 cover the whole list", scan(oe, 0), paged);
                // the restore rebuild (sorted) equals the live index
                final com.openexchange.assets.application.engine.AssetsEngine restored = S8Support.restore(S8Support.image(d.r.engine));
                assertEquals("restore rebuild == live", reply(d.r, d.r.engine, 0, 4096), reply(d.r, restored, 0, 4096));
                assertEquals(oe.pendingOldestSeq(), restored.orders().pendingOldestSeq());
                assertEquals(oe.oldestReleaseSeq(), restored.orders().oldestReleaseSeq());
            }
        }
    }

    /** {p50, p99.9, max} of {@code samples[0, n)}. */
    private static long[] pct(final long[] samples, final int n) {
        final long[] c = Arrays.copyOf(samples, n);
        Arrays.sort(c);
        return new long[] {c[n / 2], c[Math.min(n - 1, (int) Math.ceil(n * 0.999) - 1)], c[n - 1]};
    }

    @Test
    public void compactionCostPerApplyAtTheDesignPoint() {
        // review NOTE-1 (coordinator decision 6): at C = 100,000 live entries the index work of one apply (one add or one
        // remove, with its bounded share of an incremental compaction) stays under 200 us at p99.9; the review measured a
        // whole compaction inside one apply at 1.8-8.5 ms
        final int c = 100_000;
        final PendingDispatchIndex index = new PendingDispatchIndex();
        final Random rnd = new Random(7);
        final long[] fifo = new long[1 << 20];
        int head = 0, tail = 0;
        long seq = 0;
        for (int i = 0; i < c; i++) {
            index.add(++seq, seq, rnd.nextInt(3));
            fifo[tail++ & (fifo.length - 1)] = seq;
        }
        final int warm = 300_000, ops = 600_000;
        final long[] samples = new long[ops];
        int n = 0;
        for (int i = 0; i < warm + ops; i++) {
            final long t0;
            final long dt;
            if ((i & 1) == 0) {
                // a resolution: mostly old entries, in a little disorder
                final int size = tail - head;
                final int k = rnd.nextInt(Math.min(256, size));
                final int a = (head + k) & (fifo.length - 1), h0 = head & (fifo.length - 1);
                final long victim = fifo[a];
                fifo[a] = fifo[h0];
                head++;
                t0 = System.nanoTime();
                final boolean removed = index.remove(victim);
                dt = System.nanoTime() - t0;
                assertTrue(removed);
            } else {
                final long s = ++seq;
                t0 = System.nanoTime();
                index.add(s, s, rnd.nextInt(3));
                dt = System.nanoTime() - t0;
                fifo[tail++ & (fifo.length - 1)] = s;
            }
            if (i >= warm) {
                samples[n++] = dt;
            }
        }
        final long[] p = pct(samples, n);
        System.out.println("[S8-INDEX-COST] C=" + c + " live=" + index.live() + " ops=" + n + " compactions=" + index.compactions()
                + " p50_ns=" + p[0] + " p999_ns=" + p[1] + " max_ns=" + p[2]);
        assertTrue("compaction rounds ran during the measurement: " + index.compactions(), index.compactions() >= 2);
        assertEquals(c, index.live());
        assertTrue("p99.9 per apply under 200 us: " + p[1] + " ns", p[1] < 200_000);

        // the review probe's shape: 200,000 slots, removals from the oldest until a compaction has run, one remove per apply
        final PendingDispatchIndex big = new PendingDispatchIndex();
        for (int i = 1; i <= 200_000; i++) {
            big.add(i, i, PendingDispatchIndex.TAG_TRADING);
        }
        final long[] one = new long[200_000];
        int m = 0;
        for (int i = 1; i <= 200_000 && big.compactions() == 0; i++) {
            final long t0 = System.nanoTime();
            big.remove(i);
            one[m++] = System.nanoTime() - t0;
        }
        final long[] q = pct(one, m);
        System.out.println("[S8-INDEX-COST] review shape: removals=" + m + " live=" + big.live() + " compactions=" + big.compactions()
                + " p50_ns=" + q[0] + " p999_ns=" + q[1] + " max_ns=" + q[2]);
        assertTrue("one compaction completed", big.compactions() >= 1);
        assertTrue("p99.9 per remove under 200 us: " + q[1] + " ns", q[1] < 200_000);
    }

    /** The incremental compaction keeps lookups, the reply walk and the heads exact while it runs (model check). */
    @Test
    public void incrementalCompactionStaysExact() {
        final PendingDispatchIndex index = new PendingDispatchIndex();
        final java.util.TreeMap<Long, Integer> model = new java.util.TreeMap<>();
        final Random rnd = new Random(11);
        long seq = 0;
        boolean sawCompacting = false;
        for (int i = 0; i < 60_000; i++) {
            if (model.isEmpty() || rnd.nextInt(10) < 5) {
                final int t = rnd.nextInt(3);
                index.add(++seq, seq * 10, t);
                model.put(seq, t);
            } else {
                final Long probe = model.ceilingKey(1 + (long) rnd.nextInt((int) seq));
                final long victim = rnd.nextInt(4) == 0 || probe == null ? model.lastKey() : probe;
                assertTrue(index.remove(victim));
                model.remove(victim);
                assertTrue("a second remove is false", !index.remove(victim));
            }
            sawCompacting |= index.compacting();
            if (i % 997 == 0 || index.compacting() && i % 31 == 0) {
                final long from = rnd.nextInt((int) seq + 1);
                final java.util.List<Long> walk = new java.util.ArrayList<>();
                for (int s = index.firstSlotAfter(from); s < index.end(); s = index.nextSlot(s)) {
                    if (!index.isDead(s)) {
                        walk.add(index.seqAt(s));
                        assertEquals(index.seqAt(s) * 10, index.refAt(s));
                        assertEquals((int) model.get(index.seqAt(s)), index.tagAt(s));
                    }
                }
                assertEquals("walk from " + from, new java.util.ArrayList<>(model.tailMap(from, false).keySet()), walk);
                for (int t = 0; t < 3; t++) {
                    long expected = 0;
                    for (final java.util.Map.Entry<Long, Integer> e : model.entrySet()) {
                        if (e.getValue() == t) {
                            expected = e.getKey();
                            break;
                        }
                    }
                    assertEquals("head of tag " + t, expected, index.headSeq(t));
                }
                assertEquals(model.size(), index.live());
            }
        }
        assertTrue("the checks ran while a compaction was in progress", sawCompacting);
        assertTrue(index.compactions() > 0);
    }

    @Test
    public void aConflictOutcomeAfterTheAppliedOneIsAStaleNoOp() {
        // F0 / forwarder 2.7 point 5 (guard): the applied outcome resolved the id; a later CONFLICT copy is stale
        final V3Rig r = S8Support.funded();
        final long h = r.create(r.limit(1, 0, 100 * ONE, ONE));
        assertTrue(r.ack(h, 0, 101));
        final long stale = r.engine.orders().staleOutcomeCount();
        assertTrue(r.outcome(h, 0, S8Support.CREATE, S8Support.CONFLICT, 0, 0, false));
        assertEquals(stale + 1, r.engine.orders().staleOutcomeCount());
        assertTrue("not quarantined", !r.order(h).has(CoreOrder.F_QUARANTINED));
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projection;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.projection.SettlementProjector;
import com.openexchange.core.domain.Asset;
import com.openexchange.core.domain.CoreEventSink;
import com.openexchange.core.domain.FixedPoint;
import com.openexchange.core.domain.Market;
import com.openexchange.core.domain.RejectReason;
import com.openexchange.core.domain.commands.DepositCommand;
import com.openexchange.core.domain.commands.HoldCommand;
import com.openexchange.core.domain.commands.ReleaseCommand;
import com.openexchange.core.domain.commands.WithdrawCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Total-core-loss recovery: even if the core loses everything, its exact state is reconstructable from two
 * <b>independent records owned by other systems</b> — the OMS client-command log (deposits, withdrawals,
 * holds, cancels) and the ME trade archive (trades, terminals). Replaying the two records <b>merged in
 * causal order</b> onto a fresh engine reproduces the live state exactly.
 *
 * <p>This is the correctness basis for the sealed recovery design: the core holds no state that isn't a
 * pure function of those two input streams — no hidden randomness, no wall-clock, no un-derivable
 * counter. A regression that leaked such state (or mis-tracked the settlement high-water /
 * consume-position) would diverge here.</p>
 */
public class TotalLossRebuildTest {

    private static final int N = Asset.count();
    private static final long[] SEEDS = {7L, 99L, 123456L};
    private static final int OPS = 600;
    private static final int USERS = 6;

    /** One applied operation, tagged with its global causal sequence and which record it belongs to. */
    private record Op(long seq, boolean feed, BiConsumer<CoreEngine, SettlementProjector> action) { }

    @Test
    public void stateReconstructsFromClientLogPlusTradeArchive() {
        for (long seed : SEEDS) {
            runOne(seed);
        }
    }

    private void runOne(long seed) {
        // 1. Generate an interleaved live stream, recording each op with a global sequence.
        List<Op> all = generate(seed);

        // 2. Apply it to the LIVE engine in causal order.
        CoreEngine live = newEngine();
        SettlementProjector liveProj = new SettlementProjector(live);
        for (Op op : all) {
            op.action().accept(live, liveProj);
        }

        // 3. Split into the two INDEPENDENT records (as other systems would durably hold them).
        List<Op> clientLog = new ArrayList<>();   // OMS ingress: deposits/withdrawals/holds/cancels
        List<Op> tradeArchive = new ArrayList<>(); // ME egress: trades/terminals
        for (Op op : all) {
            (op.feed() ? tradeArchive : clientLog).add(op);
        }

        // 4. Rebuild from scratch: merge the two records by causal sequence, replay onto a fresh engine.
        CoreEngine rebuilt = newEngine();
        SettlementProjector rebuiltProj = new SettlementProjector(rebuilt);
        int ci = 0, ti = 0;
        while (ci < clientLog.size() || ti < tradeArchive.size()) {
            boolean takeClient;
            if (ci == clientLog.size()) {
                takeClient = false;
            } else if (ti == tradeArchive.size()) {
                takeClient = true;
            } else {
                takeClient = clientLog.get(ci).seq() < tradeArchive.get(ti).seq();
            }
            Op op = takeClient ? clientLog.get(ci++) : tradeArchive.get(ti++);
            op.action().accept(rebuilt, rebuiltProj);
        }

        // 5. The reconstruction must equal the live state exactly.
        assertSameState(live, rebuilt, seed);
    }

    private List<Op> generate(long seed) {
        Random rng = new Random(seed);
        List<Op> ops = new ArrayList<>();
        long seq = 1, nextOrderId = 1, nextTradeId = 1, nextPos = 1;
        List<long[]> cancelable = new ArrayList<>(); // standalone holds: {orderId, userId}

        for (int i = 0; i < OPS; i++) {
            int roll = rng.nextInt(100);
            if (roll < 30) {
                long user = user(rng); int asset = rng.nextInt(N); long amt = amount(rng);
                ops.add(client(seq++, (e, p) -> deposit(e, user, asset, amt)));
            } else if (roll < 45) {
                long user = user(rng); int asset = rng.nextInt(N); long amt = amount(rng);
                ops.add(client(seq++, (e, p) -> withdraw(e, user, asset, amt)));
            } else if (roll < 60) {
                long user = user(rng); int asset = rng.nextInt(N); long amt = amount(rng);
                long orderId = nextOrderId++;
                ops.add(client(seq++, (e, p) -> hold(e, orderId, user, asset, amt)));
                cancelable.add(new long[]{orderId, user});
            } else if (roll < 72) {
                if (!cancelable.isEmpty()) {
                    long[] h = cancelable.remove(rng.nextInt(cancelable.size()));
                    ops.add(client(seq++, (e, p) -> release(e, h[0], h[1], -1L)));
                }
            } else {
                // A trade cycle: client holds (client log) + trade/terminals (trade archive), interleaved.
                Market m = Market.ALL[rng.nextInt(Market.ALL.length)];
                int base = m.baseAsset().id(), quote = m.quoteAsset().id();
                long buyer = user(rng), seller = differentUser(rng, buyer);
                long price = 1 + rng.nextInt(100_000);
                long qtyFull = 1 + (long) rng.nextInt(1_000_000);
                long quoteFull = FixedPoint.multiply(price, qtyFull);
                if (quoteFull <= 0) { continue; }
                long buyOrder = nextOrderId++, sellOrder = nextOrderId++;
                long tradeId = nextTradeId++, pos = nextPos; nextPos += 3;
                long qtyFill = 1 + Math.floorMod(rng.nextLong(), qtyFull);

                ops.add(client(seq++, (e, p) -> deposit(e, buyer, quote, quoteFull)));
                ops.add(client(seq++, (e, p) -> hold(e, buyOrder, buyer, quote, quoteFull)));
                ops.add(client(seq++, (e, p) -> deposit(e, seller, base, qtyFull)));
                ops.add(client(seq++, (e, p) -> hold(e, sellOrder, seller, base, qtyFull)));
                ops.add(feed(seq++, (e, p) -> p.onTrade(pos, tradeId, m.marketId(), buyOrder, buyer,
                        sellOrder, seller, price, qtyFill, true, 1L)));
                ops.add(feed(seq++, (e, p) -> p.onTerminal(pos + 1, buyOrder, buyer, 1L)));
                ops.add(feed(seq++, (e, p) -> p.onTerminal(pos + 2, sellOrder, seller, 1L)));
            }
        }
        return ops;
    }

    private static Op client(long seq, BiConsumer<CoreEngine, SettlementProjector> a) {
        return new Op(seq, false, a);
    }

    private static Op feed(long seq, BiConsumer<CoreEngine, SettlementProjector> a) {
        return new Op(seq, true, a);
    }

    private void assertSameState(CoreEngine live, CoreEngine rebuilt, long seed) {
        assertEquals("seed " + seed + ": high-water", live.getLastAppliedTradeId(), rebuilt.getLastAppliedTradeId());
        assertEquals("seed " + seed + ": consumePosition", live.getConsumePosition(), rebuilt.getConsumePosition());
        assertEquals("seed " + seed + ": accountCount", live.accountCount(), rebuilt.accountCount());
        live.forEachAccount(acc -> {
            long u = acc.userId();
            assertTrue("seed " + seed + ": missing account " + u, rebuilt.account(u) != null);
            for (int a = 0; a < N; a++) {
                assertEquals("seed " + seed + ": avail u" + u + " a" + a,
                        acc.available(a), rebuilt.account(u).available(a));
                assertEquals("seed " + seed + ": locked u" + u + " a" + a,
                        acc.locked(a), rebuilt.account(u).locked(a));
            }
            assertEquals("seed " + seed + ": holdCount u" + u, acc.holdCount(), rebuilt.account(u).holdCount());
        });
    }

    // ---- helpers ----

    private static CoreEngine newEngine() {
        CoreEngine e = new CoreEngine();
        e.setEventSink(NOOP);
        return e;
    }

    private static void deposit(CoreEngine e, long user, int asset, long amt) {
        DepositCommand c = new DepositCommand();
        c.setUserId(user); c.setAssetId(asset); c.setAmount(amt);
        e.applyCommand(CoreEngine.CMD_DEPOSIT, c, 1L);
    }

    private static void withdraw(CoreEngine e, long user, int asset, long amt) {
        WithdrawCommand c = new WithdrawCommand();
        c.setUserId(user); c.setAssetId(asset); c.setAmount(amt);
        e.applyCommand(CoreEngine.CMD_WITHDRAW, c, 1L);
    }

    private static void hold(CoreEngine e, long orderId, long user, int asset, long amt) {
        HoldCommand c = new HoldCommand();
        c.setOrderId(orderId); c.setUserId(user); c.setAssetId(asset); c.setAmount(amt);
        e.applyCommand(CoreEngine.CMD_HOLD, c, 1L);
    }

    private static void release(CoreEngine e, long orderId, long user, long amt) {
        ReleaseCommand c = new ReleaseCommand();
        c.setOrderId(orderId); c.setUserId(user); c.setAmount(amt);
        e.applyCommand(CoreEngine.CMD_RELEASE, c, 1L);
    }

    private static long user(Random rng) {
        return 1 + rng.nextInt(USERS);
    }

    private static long differentUser(Random rng, long other) {
        long u;
        do { u = user(rng); } while (u == other);
        return u;
    }

    private static long amount(Random rng) {
        return 1 + (long) rng.nextInt(10_000_000);
    }

    private static final CoreEventSink NOOP = new CoreEventSink() {
        @Override public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) { }
        @Override public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) { }
        @Override public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) { }
        @Override public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason) { }
        @Override public void onBalanceUpdate(long userId, int assetId, long available, long locked) { }
        @Override public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) { }
        @Override public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) { }
        @Override public void onBalanceSnapshotEnd(long correlationId, int entryCount) { }
        @Override public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) { }
        @Override public void onHoldSnapshotEnd(long correlationId, int entryCount) { }
        @Override public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId, boolean journalEnabled) { }
        @Override public void onSettleFault(long tradeId, long orderId, long userId, int assetId, long drawnFromAvailable, long uncovered) { }
        @Override public void onSettleRejected(long tradeId, int marketId, long journalPosition, com.openexchange.core.domain.SettleRejectReason reason, long detail) { }
        public void onOrderReject(long c, long rh, long rl, long u, long o, int m, int ref, com.openexchange.core.domain.CoreRejectReason r, int me, long d, long t) { }
        public void onAdminAck(long c, int ref, com.openexchange.core.domain.AdminResult r, int reason, long cfg) { }
        public void onAdmissionState(int m, com.openexchange.core.domain.GateState gs, com.openexchange.core.domain.GateReason gr, boolean b, long at, long lp, boolean o) { }
        public void onOrderAck(long c, long rh, long rl, long o, long u, int m, com.openexchange.core.domain.OrderStatus st, boolean dup, int ha, long hm, long at, long seq) { }
        public void onOrderEvent(long seq, long lp, com.openexchange.core.domain.CoreOrderView o, com.openexchange.core.domain.EventKind k, com.openexchange.core.domain.CoreRejectReason r, long lpx, long lq, long t, long hr, long up) { }
        public void onMeDispatch(long ds, long ih, long il, com.openexchange.core.domain.DispatchKind k, long u, long o, long old, int m, int ot, int sd, long px, long q, long b, int tif) { }
        public void onDepositReject(long c, long rh, long rl, long u, int a, long amt, com.openexchange.core.domain.RejectReason r) { }
        public void onConfigApplied(com.openexchange.core.domain.ConfigKind k, int m, long v, long lp) { }
    };
}

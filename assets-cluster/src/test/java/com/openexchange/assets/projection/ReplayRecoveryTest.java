// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projection;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Phase 1 recovery gate: the settlement projector's feed-forward is correct, and recovery from a
 * snapshot + replay of the ME trade tape converges to the same state — driven by the snapshotted
 * {@code consumePosition} and safe under ReplayMerge overlap.
 *
 * <p>Three properties, all in-process against a replayed trade tape (the live Aeron archive
 * subscription is Phase 2):</p>
 * <ol>
 *   <li><b>Feed-forward</b> — a tape of trades + terminals lands the money exactly where it belongs.</li>
 *   <li><b>Replay-gap recovery</b> — after a crash, restore the snapshot, read its
 *       {@code consumePosition}, replay only the tape suffix at {@code position > consumePosition}, and
 *       converge to the no-crash baseline. The position bookkeeping drives the replay start.</li>
 *   <li><b>Idempotent overlap</b> — replaying from <em>before</em> {@code consumePosition} (a
 *       ReplayMerge that starts too early) still converges: settles are high-water no-ops, terminal
 *       releases are gone-hold no-ops. Money is never double-applied.</li>
 * </ol>
 */
public class ReplayRecoveryTest {

    // ---- a minimal trade tape ----

    private interface FeedEvent {
        long position();
        void applyTo(SettlementProjector p, long ts);
    }

    private record Trade(long position, long tradeId, int marketId, long takerOrder, long takerUser,
                         long makerOrder, long makerUser, long price, long qty, boolean takerBuy)
            implements FeedEvent {
        @Override
        public void applyTo(SettlementProjector p, long ts) {
            p.onTrade(position, tradeId, marketId, takerOrder, takerUser, makerOrder, makerUser,
                    price, qty, takerBuy, ts);
        }
    }

    private record Terminal(long position, long orderId, long userId) implements FeedEvent {
        @Override
        public void applyTo(SettlementProjector p, long ts) {
            p.onTerminal(position, orderId, userId, ts);
        }
    }

    private static final long SELLER = 100, BUYER = 200;

    /** A 1 BTC order that fills in two halves (tradeIds 1,2) then both sides terminate FILLED. */
    private static List<FeedEvent> tape() {
        List<FeedEvent> t = new ArrayList<>();
        t.add(new Trade(10, 1, 1, 2, BUYER, 1, SELLER, px(60000), qty(0.5), true));
        t.add(new Trade(20, 2, 1, 2, BUYER, 1, SELLER, px(60000), qty(0.5), true));
        t.add(new Terminal(30, 1, SELLER));
        t.add(new Terminal(40, 2, BUYER));
        return t;
    }

    // ---- tests ----

    @Test
    public void feedForwardLandsMoneyExactly() {
        AssetsEngine e = fresh();
        SettlementProjector p = new SettlementProjector(e);
        for (FeedEvent ev : tape()) {
            ev.applyTo(p, 1000L);
        }
        // Buyer bought 1 BTC for 60000 USD; seller the mirror. No residual locked, no holds.
        assertEquals(qty(1.0), e.account(BUYER).available(Asset.BTC.id()));
        assertEquals(0L, e.account(BUYER).available(Asset.USD.id()));
        assertEquals(0L, e.account(BUYER).locked(Asset.USD.id()));
        assertEquals(px(60000), e.account(SELLER).available(Asset.USD.id()));
        assertEquals(0L, e.account(SELLER).available(Asset.BTC.id()));
        assertEquals(0L, e.account(SELLER).locked(Asset.BTC.id()));
        assertEquals(2L, e.getLastAppliedTradeId());
    }

    @Test
    public void replayGapRecoveryConvergesDrivenByConsumePosition() {
        AssetsEngine baseline = fresh();
        SettlementProjector bp = new SettlementProjector(baseline);
        for (FeedEvent ev : tape()) {
            ev.applyTo(bp, 1000L);
        }

        // Crash after the first fill (position 10): live has applied only that trade.
        AssetsEngine live = fresh();
        SettlementProjector lp = new SettlementProjector(live);
        tape().get(0).applyTo(lp, 1000L);
        assertEquals(10L, live.getConsumePosition());

        // Recover: snapshot -> fresh engine. consumePosition + high-water ride through.
        AssetsEngine recovered = restore(live);
        SettlementProjector rp = new SettlementProjector(recovered);

        // Replay ONLY the suffix the snapshot says we haven't consumed (position > consumePosition).
        long from = recovered.getConsumePosition();
        for (FeedEvent ev : tape()) {
            if (ev.position() > from) {
                ev.applyTo(rp, 1000L);
            }
        }
        assertConverged(baseline, recovered);
    }

    @Test
    public void replayMergeOverlapIsIdempotent() {
        AssetsEngine baseline = fresh();
        SettlementProjector bp = new SettlementProjector(baseline);
        for (FeedEvent ev : tape()) {
            ev.applyTo(bp, 1000L);
        }

        // Crash after the SECOND fill (position 20).
        AssetsEngine live = fresh();
        SettlementProjector lp = new SettlementProjector(live);
        tape().get(0).applyTo(lp, 1000L);
        tape().get(1).applyTo(lp, 1000L);
        assertEquals(20L, live.getConsumePosition());

        AssetsEngine recovered = restore(live);
        SettlementProjector rp = new SettlementProjector(recovered);

        // Deliberately over-replay: replay the ENTIRE tape from the start (positions 10..40),
        // including the two trades already reflected in the snapshot. Both re-settles must be
        // high-water no-ops; only the terminals do new work.
        for (FeedEvent ev : tape()) {
            ev.applyTo(rp, 1000L);
        }
        assertConverged(baseline, recovered);
    }

    // ---- helpers ----

    private static AssetsEngine fresh() {
        AssetsEngine e = new AssetsEngine();
        e.setEventSink(NOOP);
        // seller holds 1 BTC; buyer holds 60000 USD (the AE's own log, present before any trade).
        deposit(e, SELLER, Asset.BTC.id(), qty(1.0));
        deposit(e, BUYER, Asset.USD.id(), px(60000));
        hold(e, 1, SELLER, Asset.BTC.id(), qty(1.0));
        hold(e, 2, BUYER, Asset.USD.id(), px(60000));
        return e;
    }

    private static AssetsEngine restore(AssetsEngine source) {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int len = BalanceSnapshotCodec.serialize(source, buf);
        AssetsEngine e = new AssetsEngine();
        e.setEventSink(NOOP);
        BalanceSnapshotCodec.deserialize(buf, 0, len, e);
        return e;
    }

    private static void deposit(AssetsEngine e, long userId, int assetId, long amount) {
        DepositCommand c = new DepositCommand();
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        e.applyCommand(AssetsEngine.CMD_DEPOSIT, c, 1L);
    }

    private static void hold(AssetsEngine e, long orderId, long userId, int assetId, long amount) {
        HoldCommand c = new HoldCommand();
        c.setOrderId(orderId);
        c.setUserId(userId);
        c.setAssetId(assetId);
        c.setAmount(amount);
        e.applyCommand(AssetsEngine.CMD_HOLD, c, 1L);
    }

    /** Two engines have converged: same balances for every user/asset, same settlement high-water. */
    private static void assertConverged(AssetsEngine expected, AssetsEngine actual) {
        assertEquals("high-water", expected.getLastAppliedTradeId(), actual.getLastAppliedTradeId());
        assertEquals("consumePosition", expected.getConsumePosition(), actual.getConsumePosition());
        for (long userId : new long[]{SELLER, BUYER}) {
            for (int a = 0; a < Asset.count(); a++) {
                assertEquals("avail user " + userId + " asset " + a,
                        expected.account(userId).available(a), actual.account(userId).available(a));
                assertEquals("locked user " + userId + " asset " + a,
                        expected.account(userId).locked(a), actual.account(userId).locked(a));
            }
        }
    }

    private static long px(double v) {
        return FixedPoint.fromDouble(v);
    }

    private static long qty(double v) {
        return FixedPoint.fromDouble(v);
    }

    private static final AssetsEventSink NOOP = new AssetsEventSink() {
        @Override public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable) { }
        @Override public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable) { }
        @Override public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) { }
        @Override public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason) { }
        @Override public void onBalanceUpdate(long userId, int assetId, long available, long locked) { }
        @Override public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) { }
        @Override public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) { }
        @Override public void onBalanceSnapshotEnd(long correlationId, int entryCount) { }
        @Override public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) { }
        @Override public void onHoldSnapshotEnd(long correlationId, int entryCount) { }
        @Override public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId) { }
        @Override public void onSettleFault(long tradeId, long orderId, long userId, int assetId, long drawnFromAvailable, long uncovered) { }
    };
}

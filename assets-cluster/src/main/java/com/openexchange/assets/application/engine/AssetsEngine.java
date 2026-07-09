// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine;

import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettlementService;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.function.Consumer;

/**
 * Application-layer orchestrator for the Assets Engine — the deterministic state machine that turns a
 * money command into aggregate mutations and emits the resulting events through the
 * {@link AssetsEventSink} port. Deliberately <b>thin</b>: it routes, but every money rule lives in the
 * {@code domain} layer ({@link Account}, {@link SettlementService}). Single-threaded; no allocation on
 * the steady-state hot path (accounts and holds are created once and reused).
 *
 * <p>It owns the two engine-global scalars that are not per-account state:</p>
 * <ul>
 *   <li>{@code lastAppliedTradeId} — a monotonic high-water mark for settlement idempotency. The
 *       matching engine's tradeId is globally monotonic and gap-free on the recorded stream, so a
 *       {@code Settle} whose {@code tradeId <= lastAppliedTradeId} has already been applied and is a
 *       no-op (cheaper and exactly correct versus an unbounded processed-set).</li>
 *   <li>{@code consumePosition} — how far into the matching engine's recorded trade stream this state
 *       reflects. Snapshotted atomically with balances so recovery has no skew. Inert until Phase 1.</li>
 * </ul>
 */
public final class AssetsEngine {

    public static final int CMD_DEPOSIT = 0;
    public static final int CMD_WITHDRAW = 1;
    public static final int CMD_HOLD = 2;
    public static final int CMD_RELEASE = 3;
    public static final int CMD_SETTLE = 4;

    private final Long2ObjectHashMap<Account> accounts = new Long2ObjectHashMap<>();
    private final SettlementService settlement = new SettlementService();
    private AssetsEventSink sink;

    private long lastAppliedTradeId = 0L;
    private long consumePosition = 0L;

    public void setEventSink(AssetsEventSink sink) {
        this.sink = sink;
    }

    /**
     * Apply one command deterministically. {@code timestamp} is accepted for parity with the engine
     * log (audit / future TTL on holds); Phase 0 balance events are logical and do not carry it, so
     * output is trivially wall-clock independent.
     */
    public void applyCommand(int type, Object command, long timestamp) {
        switch (type) {
            case CMD_DEPOSIT:  applyDeposit((DepositCommand) command); break;
            case CMD_WITHDRAW: applyWithdraw((WithdrawCommand) command); break;
            case CMD_HOLD:     applyHold((HoldCommand) command); break;
            case CMD_RELEASE:  applyRelease((ReleaseCommand) command); break;
            case CMD_SETTLE:   applySettle((SettleCommand) command); break;
            default: throw new IllegalArgumentException("unknown command type " + type);
        }
    }

    private void applyDeposit(DepositCommand c) {
        Account a = getOrCreateAccount(c.getUserId());
        RejectReason r = a.deposit(c.getAssetId(), c.getAmount());
        if (r.accepted()) {
            sink.onBalanceUpdate(c.getUserId(), c.getAssetId(), a.available(c.getAssetId()), a.locked(c.getAssetId()));
        }
    }

    private void applyWithdraw(WithdrawCommand c) {
        Account a = getOrCreateAccount(c.getUserId());
        RejectReason r = a.withdraw(c.getAssetId(), c.getAmount());
        if (r.accepted()) {
            sink.onBalanceUpdate(c.getUserId(), c.getAssetId(), a.available(c.getAssetId()), a.locked(c.getAssetId()));
        } else {
            sink.onWithdrawReject(c.getUserId(), c.getAssetId(), c.getAmount(), r);
        }
    }

    private void applyHold(HoldCommand c) {
        Account a = getOrCreateAccount(c.getUserId());
        RejectReason r = a.hold(c.getOrderId(), c.getAssetId(), c.getAmount());
        if (r.accepted()) {
            sink.onHoldAck(c.getOrderId(), c.getUserId(), c.getAssetId(), c.getAmount());
            sink.onBalanceUpdate(c.getUserId(), c.getAssetId(), a.available(c.getAssetId()), a.locked(c.getAssetId()));
        } else {
            sink.onHoldReject(c.getOrderId(), c.getUserId(), c.getAssetId(), c.getAmount(), r);
        }
    }

    private void applyRelease(ReleaseCommand c) {
        Account a = accounts.get(c.getUserId());
        if (a == null) {
            return; // unknown user: nothing held
        }
        int assetId = a.holdAssetId(c.getOrderId());
        if (assetId < 0) {
            return; // no such hold: idempotent no-op (already released / never held)
        }
        a.release(c.getOrderId(), c.getAmount());
        sink.onBalanceUpdate(c.getUserId(), assetId, a.available(assetId), a.locked(assetId));
    }

    private void applySettle(SettleCommand c) {
        if (c.getTradeId() <= lastAppliedTradeId) {
            return; // already applied (monotonic, gap-free) — idempotent no-op
        }
        Market m = Market.fromId(c.getMarketId());
        int base = m.baseAsset().id();
        int quote = m.quoteAsset().id();
        long baseAmt = c.getQuantity();
        long quoteAmt = FixedPoint.multiply(c.getPrice(), c.getQuantity());

        final long buyerUser, buyerOrder, sellerUser, sellerOrder;
        if (c.isTakerIsBuy()) {
            buyerUser = c.getTakerUserId();  buyerOrder = c.getTakerOrderId();
            sellerUser = c.getMakerUserId(); sellerOrder = c.getMakerOrderId();
        } else {
            buyerUser = c.getMakerUserId();  buyerOrder = c.getMakerOrderId();
            sellerUser = c.getTakerUserId(); sellerOrder = c.getTakerOrderId();
        }

        Account buyer = getOrCreateAccount(buyerUser);
        Account seller = getOrCreateAccount(sellerUser);
        settlement.settle(buyer, seller, buyerOrder, sellerOrder, base, quote, baseAmt, quoteAmt);
        lastAppliedTradeId = c.getTradeId();

        // Emit the four (user, asset) lines that changed, in a deterministic order.
        sink.onBalanceUpdate(buyerUser, quote, buyer.available(quote), buyer.locked(quote));
        sink.onBalanceUpdate(buyerUser, base, buyer.available(base), buyer.locked(base));
        sink.onBalanceUpdate(sellerUser, base, seller.available(base), seller.locked(base));
        sink.onBalanceUpdate(sellerUser, quote, seller.available(quote), seller.locked(quote));
        sink.onSettlementApplied(c.getTradeId(), buyerUser, sellerUser);
    }

    // ---- account access ----

    public Account getOrCreateAccount(long userId) {
        Account a = accounts.get(userId);
        if (a == null) {
            a = new Account(userId);
            accounts.put(userId, a);
        }
        return a;
    }

    /** The account for {@code userId}, or {@code null} if none exists yet. */
    public Account account(long userId) {
        return accounts.get(userId);
    }

    // ---- snapshot support ----

    public int accountCount() {
        return accounts.size();
    }

    /** Visit every account (serialize / invariant checks). */
    public void forEachAccount(Consumer<Account> visitor) {
        accounts.values().forEach(visitor);
    }

    public long getLastAppliedTradeId() {
        return lastAppliedTradeId;
    }

    public void setLastAppliedTradeId(long value) {
        this.lastAppliedTradeId = value;
    }

    public long getConsumePosition() {
        return consumePosition;
    }

    public void setConsumePosition(long value) {
        this.consumePosition = value;
    }
}

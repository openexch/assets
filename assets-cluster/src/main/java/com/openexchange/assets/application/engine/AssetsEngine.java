// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine;

import com.openexchange.assets.application.engine.feed.FeedState;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.order.CoreMetricsView;
import com.openexchange.assets.application.engine.order.MeOutcomeView;
import com.openexchange.assets.application.engine.order.MeReleaseView;
import com.openexchange.assets.application.engine.order.OrderEngine;
import com.openexchange.assets.application.engine.session.SessionAuthority;
import com.openexchange.assets.application.engine.session.SessionRegistry;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.MoneyJournalSink;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.domain.SettlementService;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.InitTradeHighWaterCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SetMoneyJournalCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import com.openexchange.assets.infrastructure.Logger;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

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
 *   <li>{@code journalSeq}: the money journal's dense sequence (last assigned; 0 = nothing journaled).
 *       Advances only while journaling is armed (CMD_SET_MONEY_JOURNAL), one per emitted record,
 *       and is snapshotted with the balances so a restart resumes the sequence without gaps or reuse.
 *       Because it is replicated state, its gate travels through the log rather than a node's
 *       environment: replicas cannot disagree about whether it advances, and a replayed log advances
 *       it exactly where the original did.</li>
 * </ul>
 */
public final class AssetsEngine {

    public static final int CMD_DEPOSIT = 0;
    public static final int CMD_WITHDRAW = 1;
    public static final int CMD_HOLD = 2;
    public static final int CMD_RELEASE = 3;
    public static final int CMD_SETTLE = 4;
    public static final int CMD_INIT_HIGH_WATER = 5;
    public static final int CMD_SET_MONEY_JOURNAL = 6;

    private static final Logger LOG = Logger.getLogger(AssetsEngine.class);

    private final Long2ObjectHashMap<Account> accounts = new Long2ObjectHashMap<>();
    private final SettlementService settlement = new SettlementService();
    private AssetsEventSink sink;

    private long lastAppliedTradeId = 0L;
    private long consumePosition = 0L;
    /** Settle legs that could not draw fully from their hold (SettleFault emitted). Alarm target. */
    private long settleFaultCount = 0L;
    /** Feed terminal releases suppressed on omsManagedRelease holds (iceberg/stop parents). */
    private long suppressedFeedReleaseCount = 0L;
    /** Holds exhausted by a settle (remaining hit 0) and reaped at settle time — never tombstoned. */
    private long exhaustedHoldsReaped = 0L;
    /** Feed settles refused before any mutation (SettleRejected emitted). Alarm target. */
    private long settleRejectCount = 0L;

    // ---- RC1 S2: replicated v4 scalars and the derived deadline index ----
    /**
     * Monotone log time: {@code max(timestamp, previous)} over every applied entry (risk spec rule). The
     * cluster timestamp is written by the leader and is not assumed never to step backwards across a
     * leadership change; every window, GTD sweep and TTL compares against THIS value, so a backwards
     * step can only delay a deadline, never grant one twice. Snapshotted.
     */
    private long lastLogTime = 0L;
    /** Replicated dense order id counter (RC1 S4 mints from it; 0 until then). Snapshotted. */
    private long nextOrderId = 0L;
    /** OrderEvent dense sequence (RC1 S4). Snapshotted. */
    private long eventSeq = 0L;
    /** MeDispatch dense sequence (RC1 S4). Snapshotted. */
    private long dispatchSeq = 0L;
    /**
     * The ONE deadline index (K3-D). DERIVED: rebuilt on restore from the records that own the deadlines
     * (GTD orders, replace windows, requestId TTLs land in S4/S5), never serialized. The service arms the
     * single sweep timer while it is non-empty; {@link #sweep} pops what is due.
     */
    private final DeadlineIndex deadlines = new DeadlineIndex();
    /** RC1 S3 (K6): every live session's authority; replicated-derived, snapshotted (section 9). */
    private final SessionRegistry sessions = new SessionRegistry();
    /** RC1 S3 (K5 item 3): the replicated order-entry gate; snapshotted (section 5). */
    private final OrderEntryGate gate = new OrderEntryGate();
    /** RC1 S4: order lifecycle, risk and the requestId table (sections 2, 3, 4, 6). */
    private final OrderEngine orders = new OrderEngine(this);
    /** Commands refused for session authority (class, scope, registration). Alarm target, not snapshotted. */
    private long unauthorisedCount = 0L;
    /** The log position of the entry being applied; set by the service, carried on control egress. Transient. */
    private long logPosition = 0L;

    // Money journal (dark by default): a dead, perfectly-predicted branch per applied command when
    // off; when armed, every APPLIED movement is emitted through the port with a dense journalSeq.
    //
    // The two halves are deliberately separate. `journalEnabled` is REPLICATED state, flipped only by
    // CMD_SET_MONEY_JOURNAL arriving through the log and carried in the snapshot, because it gates
    // journalSeq, which is itself replicated. `journal` is the NODE-LOCAL sink: where this particular
    // node writes its records. A node whose sink is a no-op still advances journalSeq when the log says
    // journaling is on — it just writes nothing — so a node that cannot journal falls behind on records
    // without ever diverging on state.
    private MoneyJournalSink journal = MoneyJournalSink.NO_OP;
    private boolean journalEnabled = false;
    private long journalSeq = 0L;
    // Opening-balance epoch rows (userId, assetId, available+locked total), captured from PRE-event
    // state on the first journaled apply while journalSeq == 0 and emitted only if that apply
    // succeeds. Used at most once per engine life; allocation here is off the steady-state path.
    private final ArrayList<long[]> epochScratch = new ArrayList<>();

    // Reusable scratch behind forEachAccountByUserId (rare paths: snapshot + read-only queries, never
    // the money path). Sorting by userId makes those traversals a pure function of *state* rather than
    // of insertion/replay history, so they reproduce identically across replicas and across a restore.
    private final ArrayList<Account> snapshotScratch = new ArrayList<>();
    private boolean orderedTraversalActive;

    public void setEventSink(AssetsEventSink sink) {
        this.sink = sink;
    }

    /**
     * Wire this node's journal sink — where its records are written. Node-local: it does NOT decide
     * whether journaling happens, which is replicated state driven by {@link #CMD_SET_MONEY_JOURNAL}.
     * Safe to call at any time and on every node regardless of the cluster's journal setting.
     */
    public void setMoneyJournalSink(MoneyJournalSink journalSink) {
        this.journal = journalSink == null ? MoneyJournalSink.NO_OP : journalSink;
    }

    /** Whether the cluster's money journal is armed (replicated; snapshotted). */
    public boolean isMoneyJournalEnabled() {
        return journalEnabled;
    }

    /** Restore the journal setting from a snapshot (recovery only). */
    public void setMoneyJournalEnabled(boolean enabled) {
        this.journalEnabled = enabled;
    }

    /**
     * Apply one command deterministically. {@code timestamp} is the cluster's deterministic log
     * timestamp (ms): journaled movements carry it as {@code clusterTimeMs}; Phase 0 balance egress
     * stays logical (does not carry it), so egress output is trivially wall-clock independent.
     */
    public void applyCommand(int type, Object command, long timestamp) {
        observeLogTime(timestamp);
        switch (type) {
            case CMD_DEPOSIT:         applyDeposit((DepositCommand) command, timestamp); break;
            case CMD_WITHDRAW:        applyWithdraw((WithdrawCommand) command, timestamp); break;
            case CMD_HOLD:            applyHold((HoldCommand) command); break;
            case CMD_RELEASE:         applyRelease((ReleaseCommand) command); break;
            case CMD_SETTLE:          applySettle((SettleCommand) command, timestamp); break;
            case CMD_INIT_HIGH_WATER: applyInitHighWater((InitTradeHighWaterCommand) command); break;
            case CMD_SET_MONEY_JOURNAL: applySetMoneyJournal((SetMoneyJournalCommand) command); break;
            default: throw new IllegalArgumentException("unknown command type " + type);
        }
    }

    private void applyDeposit(DepositCommand c, long timestamp) {
        // Validate, THEN mutate (RC1 S1, K5): a rejected command creates no account. Everything that can
        // refuse a deposit on a NEW account is decidable before the account exists (a fresh account's
        // headroom is Long.MAX_VALUE, so only the amount and the asset can reject it).
        if (!validAsset(c.getAssetId())) {
            return; // the demuxer already counted and refused this; defensive only
        }
        final long fingerprint = OrderEngine.moneyFingerprint(c.getUserId(), c.getAssetId(), c.getAmount());
        if (c.hasRequestId()) {
            final var seen = orders.requests().get(c.getUserId(), c.getRequestIdHigh(), c.getRequestIdLow());
            if (seen != null) {
                if (seen.fingerprint != fingerprint) {
                    sink.onDepositReject(c.getCorrelationId(), c.getRequestIdHigh(), c.getRequestIdLow(), c.getUserId(),
                            c.getAssetId(), c.getAmount(), RejectReason.REQUEST_ID_MISMATCH);
                    return;
                }
                final Account existing = accounts.get(c.getUserId());
                sink.onDepositAck(c.getCorrelationId(), c.getUserId(), c.getAssetId(), c.getAmount(),
                        existing == null ? 0L : existing.available(c.getAssetId()), true);
                return;
            }
        }
        if (c.getAmount() <= 0) {
            sink.onDepositReject(c.getCorrelationId(), c.getRequestIdHigh(), c.getRequestIdLow(), c.getUserId(),
                    c.getAssetId(), c.getAmount(), RejectReason.INVALID_AMOUNT);
            return; // no money effect, no account
        }
        final Account known = accounts.get(c.getUserId());
        if (known != null && c.getAmount() > known.headroom(c.getAssetId())) {
            sink.onDepositReject(c.getCorrelationId(), c.getRequestIdHigh(), c.getRequestIdLow(), c.getUserId(),
                    c.getAssetId(), c.getAmount(), RejectReason.BALANCE_OVERFLOW);
            return;
        }
        captureOpeningEpochIfArmed();
        Account a = getOrCreateAccount(c.getUserId());
        RejectReason r = a.deposit(c.getAssetId(), c.getAmount());
        if (r.accepted()) {
            // Ack (echoing the correlationId) precedes the balance update, matching hold's order.
            sink.onDepositAck(c.getCorrelationId(), c.getUserId(), c.getAssetId(), c.getAmount(),
                    a.available(c.getAssetId()), false);
            sink.onBalanceUpdate(c.getUserId(), c.getAssetId(), a.available(c.getAssetId()), a.locked(c.getAssetId()));
            if (journalEnabled) {
                emitOpeningEpochIfPending();
                journal.onDeposit(++journalSeq, c.getUserId(), c.getAssetId(), c.getAmount(),
                        a.available(c.getAssetId()) + a.locked(c.getAssetId()), timestamp);
            }
            if (c.hasRequestId()) {
                orders.rememberMoney(c.getUserId(), c.getRequestIdHigh(), c.getRequestIdLow(), fingerprint,
                        com.openexchange.assets.application.engine.order.RequestTable.KIND_DEPOSIT, c.getAmount(), lastLogTime);
            }
        } else {
            sink.onDepositReject(c.getCorrelationId(), c.getRequestIdHigh(), c.getRequestIdLow(), c.getUserId(),
                    c.getAssetId(), c.getAmount(), r);
        }
    }

    private void applyWithdraw(WithdrawCommand c, long timestamp) {
        if (!validAsset(c.getAssetId())) {
            return; // never a throw, never an account
        }
        final long fingerprint = OrderEngine.moneyFingerprint(c.getUserId(), c.getAssetId(), c.getAmount());
        if (c.hasRequestId()) {
            final var seen = orders.requests().get(c.getUserId(), c.getRequestIdHigh(), c.getRequestIdLow());
            if (seen != null) {
                // money commands are remembered only when ACCEPTED (a refused one may be retried as-is)
                if (seen.fingerprint != fingerprint) {
                    sink.onWithdrawReject(c.getCorrelationId(), c.getUserId(), c.getAssetId(), c.getAmount(), RejectReason.REQUEST_ID_MISMATCH);
                } else {
                    final Account existing = accounts.get(c.getUserId());
                    sink.onWithdrawAck(c.getCorrelationId(), c.getUserId(), c.getAssetId(), c.getAmount(),
                            existing == null ? 0L : existing.available(c.getAssetId()), true);
                }
                return;
            }
        }
        Account a = accounts.get(c.getUserId());
        if (a == null) {
            // An unknown user holds nothing: the same reject the account itself would give, without
            // creating the account to find that out.
            sink.onWithdrawReject(c.getCorrelationId(), c.getUserId(), c.getAssetId(), c.getAmount(),
                    c.getAmount() <= 0 ? RejectReason.INVALID_AMOUNT : RejectReason.INSUFFICIENT_FUNDS);
            return;
        }
        captureOpeningEpochIfArmed();
        RejectReason r = a.withdraw(c.getAssetId(), c.getAmount());
        if (r.accepted()) {
            sink.onWithdrawAck(c.getCorrelationId(), c.getUserId(), c.getAssetId(), c.getAmount(),
                    a.available(c.getAssetId()), false);
            sink.onBalanceUpdate(c.getUserId(), c.getAssetId(), a.available(c.getAssetId()), a.locked(c.getAssetId()));
            if (journalEnabled) {
                emitOpeningEpochIfPending();
                journal.onWithdraw(++journalSeq, c.getUserId(), c.getAssetId(), c.getAmount(),
                        a.available(c.getAssetId()) + a.locked(c.getAssetId()), timestamp);
            }
            if (c.hasRequestId()) {
                orders.rememberMoney(c.getUserId(), c.getRequestIdHigh(), c.getRequestIdLow(), fingerprint,
                        com.openexchange.assets.application.engine.order.RequestTable.KIND_WITHDRAW, c.getAmount(), lastLogTime);
            }
        } else {
            sink.onWithdrawReject(c.getCorrelationId(), c.getUserId(), c.getAssetId(), c.getAmount(), r);
        }
    }

    private void applyHold(HoldCommand c) {
        if (!validAsset(c.getAssetId())) {
            return; // never a throw, never an account
        }
        if (behaviorV3() && c.getOrderId() >= 1) {
            // RC2 S8 3.12 (D-S8-10): under v3 the core owns every orderId >= 1; the engine guard for every caller (no reply)
            coreOrderHoldRefusedCount++;
            return;
        }
        Account a = accounts.get(c.getUserId());
        if (a == null) {
            sink.onHoldReject(c.getCorrelationId(), c.getOrderId(), c.getUserId(), c.getAssetId(), c.getAmount(),
                    c.getAmount() <= 0 ? RejectReason.INVALID_AMOUNT : RejectReason.INSUFFICIENT_FUNDS);
            return;
        }
        RejectReason r = a.hold(c.getOrderId(), c.getAssetId(), c.getAmount(), c.isOmsManagedRelease());
        if (r.accepted()) {
            sink.onHoldAck(c.getCorrelationId(), c.getOrderId(), c.getUserId(), c.getAssetId(), c.getAmount());
            sink.onBalanceUpdate(c.getUserId(), c.getAssetId(), a.available(c.getAssetId()), a.locked(c.getAssetId()));
        } else {
            sink.onHoldReject(c.getCorrelationId(), c.getOrderId(), c.getUserId(), c.getAssetId(), c.getAmount(), r);
        }
    }

    private void applyRelease(ReleaseCommand c) {
        Account a = accounts.get(c.getUserId());
        if (a == null) {
            return; // unknown user: nothing held
        }
        if (behaviorV3() && c.getOrderId() >= 1) {
            // RC2 S8 3.12 (supersedes S7 7.2 row 4): a core order's hold has ONE release owner, terminalize (S5 3.2), live or
            // dead; the engine guard for every caller (no reply, nothing moves)
            coreOrderReleaseRefusedCount++;
            return;
        }
        int assetId = a.holdAssetId(c.getOrderId());
        if (assetId < 0) {
            return; // no such hold: idempotent no-op (already released / never held)
        }
        if (c.isFromFeed() && a.isOmsManagedRelease(c.getOrderId())) {
            // Iceberg/stop PARENT hold: slices share the parent's omsOrderId, so a slice FILLED on
            // the feed must not release the parent's residual. The OMS owns this hold's terminal
            // (state listener releases at the PARENT's terminal; the reconciler is the backstop).
            suppressedFeedReleaseCount++;
            return;
        }
        a.release(c.getOrderId(), c.getAmount());
        sink.onBalanceUpdate(c.getUserId(), assetId, a.available(assetId), a.locked(assetId));
    }

    private void applySettle(SettleCommand c, long timestamp) {
        settle(c, timestamp);
    }

    /**
     * Apply a trade. Returns {@code false} when the trade was REFUSED before any mutation (unknown market,
     * non-positive price or quantity, notional overflow, bad party): a {@code SettleRejected} is emitted,
     * nothing moves, and the caller must NOT advance the journal cursor past it. Every one of these
     * used to be a throw out of the apply path, which would have latched this replica and, on replay,
     * every replica. A duplicate (tradeId at or below the high-water) is an idempotent no-op and returns
     * {@code true}: it was consumed, just not applied again.
     */
    public boolean settle(SettleCommand c, long timestamp) {
        observeLogTime(timestamp);
        if (c.getTradeId() <= lastAppliedTradeId) {
            return true; // already applied (monotonic, gap-free): idempotent no-op, journals nothing
        }
        final Market m = marketOrNull(c.getMarketId());
        if (m == null) {
            return refuseSettle(c, SettleRejectReason.UNKNOWN_MARKET, c.getMarketId());
        }
        if (c.getPrice() <= 0) {
            return refuseSettle(c, SettleRejectReason.INVALID_PRICE, c.getPrice());
        }
        if (c.getQuantity() <= 0) {
            return refuseSettle(c, SettleRejectReason.INVALID_QUANTITY, c.getQuantity());
        }
        if (c.getTakerUserId() <= 0 || c.getMakerUserId() <= 0) {
            return refuseSettle(c, SettleRejectReason.INVALID_PARTY,
                    c.getTakerUserId() <= 0 ? c.getTakerUserId() : c.getMakerUserId());
        }
        if (!FixedPoint.multiplyFits(c.getPrice(), c.getQuantity())) {
            return refuseSettle(c, SettleRejectReason.NOTIONAL_OVERFLOW, c.getPrice());
        }
        captureOpeningEpochIfArmed();
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

        // Defense-in-depth accounting: a leg whose settle exhausted its hold (remaining hit 0) had
        // that hold removed at settle time — it never lingers as a tombstone awaiting a terminal
        // release that may never arrive. Pure hold-map bookkeeping; balances are untouched.
        if (settlement.buyerLeg().reapedExhaustedHold) {
            exhaustedHoldsReaped++;
        }
        if (settlement.sellerLeg().reapedExhaustedHold) {
            exhaustedHoldsReaped++;
        }

        // Exceptional path: a leg that could not draw fully from its order hold. Emitted BEFORE the
        // balance lines (the fault explains them). Deterministic, loud, never a throw — a throw here
        // would re-crash the service on every log replay.
        if (settlement.buyerLeg().debitFaulted()) {
            settleFaultCount++;
            sink.onSettleFault(c.getTradeId(), buyerOrder, buyerUser, quote,
                    settlement.buyerLeg().drawnFromAvailable, settlement.buyerLeg().uncovered);
        }
        if (settlement.sellerLeg().debitFaulted()) {
            settleFaultCount++;
            sink.onSettleFault(c.getTradeId(), sellerOrder, sellerUser, base,
                    settlement.sellerLeg().drawnFromAvailable, settlement.sellerLeg().uncovered);
        }
        // The other half of a conservation breach: the PAYEE could not receive, because that asset's
        // total for their account has reached Long.MAX_VALUE (assets#18). Attributed to the party who
        // could not receive and reported in `uncovered`, which the message already defines as "could
        // not be moved". A dedicated wire field belongs with the next schema change, not here.
        if (settlement.buyerLeg().uncredited != 0) {
            settleFaultCount++;
            sink.onSettleFault(c.getTradeId(), sellerOrder, sellerUser, quote,
                    0L, settlement.buyerLeg().uncredited);
        }
        if (settlement.sellerLeg().uncredited != 0) {
            settleFaultCount++;
            sink.onSettleFault(c.getTradeId(), buyerOrder, buyerUser, base,
                    0L, settlement.sellerLeg().uncredited);
        }

        // Emit the four (user, asset) lines that changed, in a deterministic order.
        sink.onBalanceUpdate(buyerUser, quote, buyer.available(quote), buyer.locked(quote));
        sink.onBalanceUpdate(buyerUser, base, buyer.available(base), buyer.locked(base));
        sink.onBalanceUpdate(sellerUser, base, seller.available(base), seller.locked(base));
        sink.onBalanceUpdate(sellerUser, quote, seller.available(quote), seller.locked(quote));
        sink.onSettlementApplied(c.getTradeId(), buyerUser, sellerUser);

        if (journalEnabled) {
            emitOpeningEpochIfPending();
            journal.onSettle(++journalSeq, c.getTradeId(), c.getMarketId(), c.getPrice(), c.getQuantity(),
                    buyerUser, sellerUser, c.isTakerIsBuy(),
                    buyer.available(base) + buyer.locked(base), buyer.available(quote) + buyer.locked(quote),
                    seller.available(base) + seller.locked(base), seller.available(quote) + seller.locked(quote),
                    timestamp);
        }
        return true;
    }

    private boolean refuseSettle(SettleCommand c, SettleRejectReason reason, long detail) {
        settleRejectCount++;
        sink.onSettleRejected(c.getTradeId(), c.getMarketId(), c.getJournalPosition(), reason, detail);
        return false;
    }

    private static boolean validAsset(int assetId) {
        return assetId >= 0 && assetId < Asset.count();
    }

    /** Non-throwing market lookup: the apply path must never throw on input. */
    private static Market marketOrNull(int marketId) {
        if (marketId < 1 || marketId >= Market.ALL.length + 1) {
            return null;
        }
        return Market.ALL[marketId - 1].marketId() == marketId ? Market.ALL[marketId - 1] : null;
    }

    // ---- money journal (movements only; holds/releases never journal: they do not change totals) ----

    /**
     * Capture the opening-balance epoch from PRE-event state: one (userId, assetId, available+locked)
     * row per nonzero total, in ascending (userId, assetId) order. Runs only while journaling is armed
     * and nothing has been journaled yet ({@code journalSeq == 0}); rows are emitted by
     * {@link #emitOpeningEpochIfPending} only if the triggering apply succeeds, so a reject or dedupe
     * still journals nothing.
     */
    private void captureOpeningEpochIfArmed() {
        if (!journalEnabled || journalSeq != 0L) {
            return;
        }
        epochScratch.clear();
        forEachAccountByUserId(a -> {
            final long userId = a.userId();
            // forEachNonZeroBalance visits assets in ascending id order (dense array): deterministic.
            a.forEachNonZeroBalance((assetId, available, locked) ->
                    epochScratch.add(new long[] {userId, assetId, available + locked}));
        });
    }

    /**
     * First journaled event only ({@code journalSeq == 0}): emit the captured epoch rows, each
     * consuming a journalSeq, before the triggering event takes the next one. An empty capture
     * (empty or all-zero ledger) emits nothing: the epoch is implicit.
     */
    private void emitOpeningEpochIfPending() {
        if (journalSeq != 0L) {
            return;
        }
        for (int i = 0; i < epochScratch.size(); i++) {
            final long[] row = epochScratch.get(i);
            journal.onOpeningBalance(++journalSeq, row[0], (int) row[1], row[2]);
        }
        epochScratch.clear();
    }

    /**
     * Cutover primer: seed the settlement high-water and consume position, but <b>only on a virgin
     * ledger</b> (no accounts, {@code lastAppliedTradeId == 0}, {@code consumePosition == 0}). On any
     * non-virgin ledger this is a strict no-op — it must never rewrite the high-water of a running
     * ledger and silently drop or replay settlements. Emits no egress (it is a primer, not a money
     * event); both the accept and the refusal are logged at WARN with the values, since a cutover is a
     * rare, operationally-significant action worth an audit line.
     */
    private void applyInitHighWater(InitTradeHighWaterCommand c) {
        boolean virgin = lastAppliedTradeId == 0L && consumePosition == 0L && accounts.isEmpty();
        if (behaviorV3()) {
            // RC2 S7 7.2 row 5: the primer would break the dense tradeId the v3 cursor relies on (3.4)
            LOG.warn("InitTradeHighWater REFUSED (behaviour v3): requested tradeId=%d consumePosition=%d",
                    c.getTradeId(), c.getConsumePosition());
            return;
        }
        if (!virgin) {
            LOG.warn("InitTradeHighWater REFUSED (ledger not virgin): requested tradeId=%d consumePosition=%d; "
                            + "current lastAppliedTradeId=%d consumePosition=%d accountCount=%d",
                    c.getTradeId(), c.getConsumePosition(), lastAppliedTradeId, consumePosition, accounts.size());
            return; // strict no-op — mutate nothing
        }
        lastAppliedTradeId = c.getTradeId();
        consumePosition = c.getConsumePosition();
        LOG.warn("InitTradeHighWater ACCEPTED (virgin ledger primed): lastAppliedTradeId=%d consumePosition=%d",
                c.getTradeId(), c.getConsumePosition());
    }

    /**
     * Arm or disarm the money journal cluster-wide. Applied from the log, so every replica flips at the
     * same position and a replay flips there too — the property that lets a bundle reproduce the ledger
     * it captured rather than a ledger that merely resembles it.
     *
     * <p>Idempotent, emits no egress, and never touches {@code journalSeq}: re-arming a journal that has
     * already written resumes at its high-water, so sequences are neither reused nor gapped across an
     * off/on cycle. Logged at WARN with the value, like the other rare operator-driven primer
     * ({@link #applyInitHighWater}), since the absence of this line is how an operator learns their
     * command never reached the log.</p>
     */
    private void applySetMoneyJournal(SetMoneyJournalCommand c) {
        if (journalEnabled == c.isEnabled()) {
            LOG.warn("SetMoneyJournal: already %s (journalSeq=%d) — no change",
                    journalEnabled ? "ENABLED" : "DISABLED", journalSeq);
            return;
        }
        journalEnabled = c.isEnabled();
        LOG.warn("SetMoneyJournal APPLIED: money journal %s (journalSeq=%d, correlationId=%d)",
                journalEnabled ? "ENABLED" : "DISABLED", journalSeq, c.getCorrelationId());
    }

    // ---- read-only snapshot queries (leader answers; deterministic; no state mutation) ----

    /**
     * Answer a {@code QueryFeedPosition}: emit the journal consume position + settlement high-water
     * through the sink. Read-only; deterministic on every replica (egress is leader-gated downstream).
     */
    public void reportFeedPosition(long correlationId) {
        // RC2 S7 3.6: the v7 tail is the FeedState (all zero under v1/v2: S1's 0, 0, FALSE, NONE)
        sink.onFeedPositionReport(correlationId, consumePosition, lastAppliedTradeId, journalEnabled, feed.boundMeEpoch(),
                feed.lastAppliedJournalSeq(), feed.latched(), feed.latchReason());
    }

    /** Settle legs that faulted (drew from available / left an uncovered residue) since boot. */
    public long getSettleFaultCount() {
        return settleFaultCount;
    }

    /** Feed terminal releases suppressed on omsManagedRelease holds since boot. */
    public long getSuppressedFeedReleaseCount() {
        return suppressedFeedReleaseCount;
    }

    /** Holds exhausted by a settle (drawn to remaining=0) and reaped at settle time since boot. */
    public long getExhaustedHoldsReaped() {
        return exhaustedHoldsReaped;
    }

    /** Feed settles refused before any mutation (SettleRejected emitted) since boot. Alarm target. */
    public long getSettleRejectCount() {
        return settleRejectCount;
    }

    /**
     * Stream one {@code onBalanceUpdate} per (user, asset) with a non-zero available or locked balance,
     * then an {@code onBalanceSnapshotEnd} carrying the entry count. Deterministic: accounts are visited
     * in ascending userId order and each account's assets in ascending assetId order, so the answer is a
     * pure function of state (independent of insertion/replay history). Read-only — no mutation, no
     * high-water change.
     */
    public void requestBalanceSnapshot(long correlationId) {
        final int[] entryCount = {0}; // mutable holder so the visitor lambda can tally (rare query, off hot path)
        forEachAccountByUserId(a -> {
            final long userId = a.userId();
            // forEachNonZeroBalance visits assets in ascending id order (dense array) — deterministic.
            a.forEachNonZeroBalance((assetId, available, locked) -> {
                sink.onBalanceUpdate(userId, assetId, available, locked);
                entryCount[0]++;
            });
        });
        sink.onBalanceSnapshotEnd(correlationId, entryCount[0]);
    }

    /**
     * Stream one {@code onHoldSnapshotEntry} per outstanding hold, then an {@code onHoldSnapshotEnd}
     * carrying the entry count. Deterministic: holds are visited in ascending (userId, orderId) order,
     * so the answer is a pure function of state. Read-only.
     */
    public void requestHoldSnapshot(long correlationId) {
        final int[] entryCount = {0};
        forEachAccountByUserId(a -> {
            final long userId = a.userId();
            a.forEachHoldByOrderId((orderId, assetId, remaining, omsManagedRelease) -> {
                sink.onHoldSnapshotEntry(orderId, userId, assetId, remaining);
                entryCount[0]++;
            });
        });
        sink.onHoldSnapshotEnd(correlationId, entryCount[0]);
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

    /**
     * Visit every account in hash-table order. Use only where the result is order-insensitive (sums,
     * invariant checks): the order depends on the order users were first touched, not on state, so two
     * ledgers holding identical balances can traverse differently. Anything durable or client-visible
     * must use {@link #forEachAccountByUserId}.
     */
    public void forEachAccount(Consumer<Account> visitor) {
        accounts.values().forEach(visitor);
    }

    /**
     * Visit every account in ascending {@code userId} order, so the traversal is a pure function of
     * state rather than of replay history. The single ordering primitive: the snapshot codec and every
     * client-facing query go through this, because each place that sorted for itself was one more place
     * to forget to — and the place that forgot was the durable one.
     *
     * <p>One shared scratch, so it is not reentrant. Enforced rather than documented: a visitor that
     * started a second ordered traversal would clear the list being walked and silently drop accounts
     * from a snapshot, which is precisely the kind of quiet wrong answer this method exists to prevent.</p>
     */
    public void forEachAccountByUserId(Consumer<Account> visitor) {
        if (orderedTraversalActive) {
            throw new IllegalStateException(
                    "nested forEachAccountByUserId: the ordered traversal shares one scratch list");
        }
        orderedTraversalActive = true;
        try {
            snapshotScratch.clear();
            accounts.values().forEach(snapshotScratch::add);
            snapshotScratch.sort(Comparator.comparingLong(Account::userId));
            for (int i = 0; i < snapshotScratch.size(); i++) {
                visitor.accept(snapshotScratch.get(i));
            }
        } finally {
            snapshotScratch.clear();
            orderedTraversalActive = false;
        }
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

    /** Last assigned money-journal sequence (dense, starts at 1; 0 = nothing journaled yet). */
    public long getJournalSeq() {
        return journalSeq;
    }

    /** Restore the journal sequence from a snapshot (recovery only). */
    public void setJournalSeq(long value) {
        this.journalSeq = value;
    }

    // ---- RC1 S2: log time, the deadline sweep, restore verification ----

    public void observeLogTime(long timestamp) {
        if (timestamp > lastLogTime) {
            lastLogTime = timestamp;
        }
    }

    /** The derived deadline index; owners add and remove entries, the sweep pops them. */
    public DeadlineIndex deadlines() {
        return deadlines;
    }

    /**
     * The deadline sweep (one cluster TimerEvent, applied on every replica): pop every entry due at or
     * before the monotone log time and apply its expiry. One-shot per entry, so a sweep that fires twice
     * at the same time applies nothing the second time (what makes re-arming after a restore safe).
     * Expiry semantics per kind land with their slices (GTD/replace in S5, requestId TTL in S4); this
     * build pops and counts.
     *
     * @return entries popped
     */
    public int sweep(long timestamp) {
        observeLogTime(timestamp);
        return deadlines.popDue(lastLogTime, this::onDeadline);
    }

    private void onDeadline(long deadlineMs, long id, int kind) {
        orders.onDeadline(deadlineMs, id, kind, lastLogTime);
    }

    public long getLastLogTime() {
        return lastLogTime;
    }

    /** Restore only. */
    public void setLastLogTime(long value) {
        this.lastLogTime = value;
    }

    public long getNextOrderId() {
        return nextOrderId;
    }

    /** Restore only. */
    public void setNextOrderId(long value) {
        this.nextOrderId = value;
    }

    public long getEventSeq() {
        return eventSeq;
    }

    /** Restore only. */
    public void setEventSeq(long value) {
        this.eventSeq = value;
    }

    public long getDispatchSeq() {
        return dispatchSeq;
    }

    /** Restore only. */
    public void setDispatchSeq(long value) {
        this.dispatchSeq = value;
    }

    /**
     * The structural invariants a restored ledger must satisfy before this node serves (layout page §3):
     * no negative balance, and for every (account, asset) {@code locked == Σ remaining} over that asset's
     * holds. Bytes that break them are a corrupt or foreign snapshot; the caller stops the node.
     *
     * @throws IllegalStateException naming the first breach
     */
    public void verifyRestoredInvariants() {
        final int assets = Asset.count();
        final long[] sumHold = new long[assets];
        final Account[] offender = new Account[1];
        final String[] breach = new String[1];
        forEachAccountByUserId(acc -> {
            if (breach[0] != null) {
                return;
            }
            java.util.Arrays.fill(sumHold, 0L);
            acc.forEachHold((orderId, assetId, remaining, oms) -> {
                if (assetId >= 0 && assetId < assets) {
                    sumHold[assetId] += remaining;
                }
            });
            for (int a = 0; a < assets && breach[0] == null; a++) {
                if (acc.available(a) < 0 || acc.locked(a) < 0) {
                    breach[0] = "negative balance user=" + acc.userId() + " asset=" + a
                            + " available=" + acc.available(a) + " locked=" + acc.locked(a);
                } else if (acc.locked(a) != sumHold[a]) {
                    breach[0] = "locked != sum(holds) user=" + acc.userId() + " asset=" + a
                            + " locked=" + acc.locked(a) + " holds=" + sumHold[a];
                }
                offender[0] = acc;
            }
        });
        if (breach[0] != null) {
            throw new IllegalStateException("restored ledger fails its invariants: " + breach[0]);
        }
        // RC2 S7 6.3 and 2.1: the gate reason sets and the feed state
        final boolean v3 = behaviorV3();
        for (int m = 0; m <= OrderEntryGate.MARKETS; m++) {
            final String g = gate.restoreBreach(m, v3);
            if (g != null) {
                throw new IllegalStateException("restored gates fail their invariants: " + g);
            }
        }
        final String f = feed.invariantBreach(lastAppliedTradeId);
        if (f != null) {
            throw new IllegalStateException("restored feed state fails its invariants: " + f);
        }
        if (!v3 && (feed.boundMeEpoch() != 0 || feed.lastAppliedJournalSeq() != 0 || feed.latched())) {
            throw new IllegalStateException("restored feed state is not empty below behaviour 3");
        }
        if (v3 && feed.boundMeEpoch() == 0 && gate.state(OrderEntryGate.GLOBAL) != GateState.SUSPENDED) {
            throw new IllegalStateException("restored v3 core is unbound with GLOBAL " + gate.state(OrderEntryGate.GLOBAL));
        }
        // review F-3: every latch adds BRIDGE_HALT on GLOBAL and only ADMIN OPEN clears it, refused while latched
        if (v3 && feed.latched() && (gate.reasons(OrderEntryGate.GLOBAL) & (1 << GateReason.BRIDGE_HALT.code())) == 0) {
            throw new IllegalStateException("restored v3 core is latched " + feed.latchReason() + " without BRIDGE_HALT on GLOBAL");
        }
        // review F-3: EPOCH_MISMATCH is raised only against a bound epoch (D1 row 3, a non-stale 66)
        if (feed.latchReason() == FeedHaltReason.EPOCH_MISMATCH && feed.boundMeEpoch() == 0) {
            throw new IllegalStateException("restored EPOCH_MISMATCH latch on an unbound feed");
        }
        // RC2 S8 5.1 a-d (every behaviour; d under v3) and 5.2 rules 1-7 (v3), each with its live rule (5.0)
        final String o = orders.orderRestoreBreach(v3);
        if (o != null) {
            throw new IllegalStateException("restored orders fail their invariants: " + o);
        }
        if (v3) {
            final String l = orders.meLinkRestoreBreach();
            if (l != null) {
                throw new IllegalStateException("restored ME link (section 11) fails its invariants: " + l);
            }
            orders.countRestoreDiagnostics();
        }
        orders.rebuildDerived();
    }

    // ---- RC1 S3: session authority and the order-entry gate ----

    public SessionRegistry sessions() {
        return sessions;
    }

    public OrderEntryGate gate() {
        return gate;
    }

    public OrderEngine orders() {
        return orders;
    }

    // ---- RC1 S7: CoreConfig and the behaviour version ----

    private final com.openexchange.assets.application.engine.config.CoreConfigStateMachine coreConfig =
            new com.openexchange.assets.application.engine.config.CoreConfigStateMachine();
    /** Node-local fail-fast hook for the CoreConfig cross-check (A-9 rule); the service installs its halt. */
    private Runnable configFailFast = () -> { };

    public com.openexchange.assets.application.engine.config.CoreConfigStateMachine coreConfig() {
        return coreConfig;
    }

    public void setConfigFailFast(Runnable failFast) {
        this.configFailFast = failFast;
    }

    /** CoreConfig (70), ADMIN class: contract S7 section 3, then the node-local cross-check (section 4). */
    public void applyCoreConfig(com.openexchange.assets.domain.commands.CoreConfigCommand c, long timestamp) {
        observeLogTime(timestamp);
        // RC2 S8 3.12: one O(holds) walk inside the switching apply only (review 2 NOTE-E kept: one boolean into decide)
        final boolean foreignCoreHold = c.behaviorVersion == 3 && coreConfig.behaviorVersion() == 2 && foreignHoldInCoreRange();
        final var decision = coreConfig.decide(c, gate.state(OrderEntryGate.GLOBAL) == GateState.OPEN, feedVirgin(),
                foreignCoreHold);
        final long version = coreConfig.apply(decision, c);
        if (decision == com.openexchange.assets.application.engine.config.CoreConfigStateMachine.Decision.SWITCH
                && c.behaviorVersion == 3) {
            gate.convertToReasonSet(); // RC2 S7 6.1: same apply as the switch, no egress
        }
        final AdminResult result;
        switch (decision) {
            case ADOPT: result = AdminResult.ADOPTED; break;
            case ACCEPT:
            case SWITCH: result = AdminResult.ACCEPTED; break;
            case DUPLICATE: result = AdminResult.DUPLICATE; break;
            case STALE: result = AdminResult.STALE; break;
            default: result = AdminResult.REJECTED; break;
        }
        sink.onAdminAck(c.correlationId, 70, result, result == AdminResult.REJECTED ? CoreRejectReason.INVALID.code() : 0, version);
        if (decision == com.openexchange.assets.application.engine.config.CoreConfigStateMachine.Decision.ADOPT
                || decision == com.openexchange.assets.application.engine.config.CoreConfigStateMachine.Decision.ACCEPT
                || decision == com.openexchange.assets.application.engine.config.CoreConfigStateMachine.Decision.SWITCH) {
            sink.onConfigApplied(com.openexchange.assets.domain.ConfigKind.CORE, 0, version, logPosition);
            crossCheckConfigOrFail(decision.name());
        }
    }

    /**
     * RC2 S8 3.12 switch precondition: some account holds a hold with {@code orderId >= 1} that is not the hold of a live
     * core order of that same account (a TOOL/ADMIN hold written under v1/v2 in what becomes the core's id space).
     */
    private boolean foreignHoldInCoreRange() {
        final boolean[] found = {false};
        forEachAccount(acc -> {
            if (found[0]) {
                return;
            }
            acc.forEachHold((orderId, assetId, remaining, oms) -> {
                if (orderId >= 1) {
                    final var o = orders.orders().get(orderId);
                    if (o == null || o.userId != acc.userId()) {
                        found[0] = true;
                    }
                }
            });
        });
        return found[0];
    }

    /** Section 4: a node whose compiled truth differs from the declared config must not serve (never a reject). */
    public void crossCheckConfigOrFail(String context) {
        final String mismatch = coreConfig.crossCheck();
        if (mismatch != null) {
            LOG.error("CORE-CONFIG CRITICAL (%s): this node differs from the cluster's declared config: %s; exiting (fail-fast)",
                    context, mismatch);
            configFailFast.run();
        }
    }

    // ---- RC1 S5: hold and gate effects owned by the order lifecycle (single release owner) ----

    /**
     * Release {@code amount} of the order's hold back to available ({@code amount < 0} = everything left) and
     * publish the balance line. Returns the amount actually released (0 when there is no such hold).
     */
    public long releaseOrderHold(long userId, long orderId, long amount) {
        final Account a = accounts.get(userId);
        if (a == null) {
            return 0L;
        }
        final int assetId = a.holdAssetId(orderId);
        if (assetId < 0) {
            return 0L;
        }
        final long released = a.release(orderId, amount);
        sink.onBalanceUpdate(userId, assetId, a.available(assetId), a.locked(assetId));
        return released;
    }

    /** An internal gate transition (no AdminAck): MeCommandOutcome CAPACITY, bridge halt, apply fault. */
    public void suspendGateInternally(int marketId, GateReason reason) {
        // v3 (D4): add the bit, AdmissionState only when it is new; v1/v2: the RC1 overwrite
        if (behaviorV3() ? gate.addReason(marketId, reason) : gate.set(marketId, GateState.SUSPENDED, reason)) {
            emitAdmission(marketId, false);
        }
    }

    public AssetsEventSink eventSink() {
        return sink;
    }

    /** The service sets this before dispatching a log entry; control egress carries it as an ordering key. */
    public void setLogPosition(long position) {
        this.logPosition = position;
    }

    public long getLogPosition() {
        return logPosition;
    }

    public long getUnauthorisedCount() {
        return unauthorisedCount;
    }

    /**
     * A command refused before any mutation for a session-authority reason (class mismatch, foreign user,
     * unregistered session): a deterministic ORIGIN reject naming the template, never a fence event.
     */
    public void rejectUnauthorised(int refTemplateId, long correlationId, long userId, CoreRejectReason reason, long detail) {
        unauthorisedCount++;
        sink.onOrderReject(correlationId, 0L, 0L, userId, 0L, 0, refTemplateId, reason, 255, detail, lastLogTime);
    }

    /**
     * RegisterSession (replicated): assert the class, require resumable, record the declaration; reply
     * AdminAck plus the global gate state so a gateway learns admission at connect.
     *
     * @return the registration outcome (the service closes the session on REJECTED)
     */
    public SessionRegistry.Registration applyRegisterSession(long sessionId, long correlationId, SessionClass assertedClass,
                                                              int channelMask, boolean resumable, int readerKind,
                                                              long identity, long sourceEpoch, long timestamp) {
        observeLogTime(timestamp);
        if (behaviorV3() && feed.boundMeEpoch() != 0 && sourceEpoch != 0 && sourceEpoch != feed.boundMeEpoch()) {
            // RC2 S7 D8a: a FORWARDER/BRIDGE declaring another ME epoch after the bind; the registry is not called
            final SessionAuthority a = sessions.get(sessionId);
            if (a != null && (a.sessionClass() == SessionClass.FORWARDER || a.sessionClass() == SessionClass.BRIDGE)) {
                final SessionRegistry.Registration refused =
                        new SessionRegistry.Registration(AdminResult.REJECTED, CoreRejectReason.INVALID, false);
                sink.onAdminAck(correlationId, 80, refused.result(), refused.reason().code(), 0L);
                return refused;
            }
        }
        final SessionRegistry.Registration r = sessions.register(sessionId, assertedClass, channelMask, resumable,
                readerKind, identity, sourceEpoch);
        sink.onAdminAck(correlationId, 80, r.result(), r.reason().code(), 0L);
        if (r.result() != AdminResult.REJECTED) {
            emitAdmission(OrderEntryGate.GLOBAL, true);
        }
        return r;
    }

    // ---- RC2 S7: behaviour v3, the ME feed (contract RC2-me-link/S7/contract.md sections 2-5, 10) ----

    private final FeedState feed = new FeedState();
    /** v5 money templates refused under v3 (7.2 row 3). Node-local. */
    private long v5MoneyRefusedCount;
    /** RC2 S8 3.12: Hold 3 / Release 4 naming {@code orderId >= 1} under v3, refused (demuxer step or engine guard). */
    private long coreOrderHoldRefusedCount;
    private long coreOrderReleaseRefusedCount;

    private static final int V_APPLY = 0;
    private static final int V_DUPLICATE = 1;
    private static final int V_REFUSED = 2;
    private static final int L_WRITE = 0;
    private static final int L_MERGE = 1;
    private static final int L_SUPPRESS = 2;
    private static final long MAX_LEDGER_CAPACITY = (1L << 31) - 1;
    private static final int MIN_WIRE = 12;
    private static final int MAX_WIRE = 65534;

    /** The replicated feed view of behaviour v3 (all zero under v1/v2). */
    public FeedState feed() {
        return feed;
    }

    private final CoreMetricsView metricsView = new CoreMetricsView(this);

    /** RC2 S8 3.10 item 3: read-only plain longs (feed + order) for the {@code OrderEngineMetrics} registrar. */
    public CoreMetricsView metricsView() {
        return metricsView;
    }

    /** {@code coreConfig().behaviorVersion() >= 3}: every S7 rule is active only then (section 0). */
    public boolean behaviorV3() {
        return coreConfig.behaviorVersion() >= 3;
    }

    public long v5MoneyRefusedCount() {
        return v5MoneyRefusedCount;
    }

    public long coreOrderHoldRefusedCount() {
        return coreOrderHoldRefusedCount;
    }

    public long coreOrderReleaseRefusedCount() {
        return coreOrderReleaseRefusedCount;
    }

    /** The demuxer's 7.2 row 3 step: a v5 money template refused under v3, for every authority. */
    public void refuseV5Money(final int templateId, final long correlationId, final long userId, final int classCode) {
        v5MoneyRefusedCount++;
        rejectUnauthorised(templateId, correlationId, userId, CoreRejectReason.UNAUTHORIZED_CLASS, classCode);
    }

    /**
     * RC2 S8 3.12: the demuxer's step for Hold 3 / Release 4 from TOOL or ADMIN (the trusted path included) naming
     * {@code orderId >= 1} under v3: counted and refused with the ORIGIN {@code OrderReject{UNAUTHORIZED_CLASS}}; nothing
     * applies.
     */
    public void refuseCoreOrderMoney(final int templateId, final long correlationId, final long userId, final int classCode) {
        if (templateId == 3) {
            coreOrderHoldRefusedCount++;
        } else {
            coreOrderReleaseRefusedCount++;
        }
        rejectUnauthorised(templateId, correlationId, userId, CoreRejectReason.UNAUTHORIZED_CLASS, classCode);
    }

    /** MeJournalTrade 61. Returns true on APPLY and DUPLICATE, false on REFUSE (a 62 batch stops at the first false). */
    public boolean onMeTrade(long egressSeq, long journalSeq, long tradeId, int marketId, long takerLegId, long takerUserId,
                             long makerLegId, long makerUserId, long price, long quantity, boolean takerIsBuy, long meTimestamp,
                             long takerOmsOrderId, long makerOmsOrderId, long meEpoch, long timestamp) {
        return onMeTradeEntry(61, egressSeq, journalSeq, tradeId, marketId, takerLegId, takerUserId, makerLegId, makerUserId,
                price, quantity, takerIsBuy, meTimestamp, takerOmsOrderId, makerOmsOrderId, meEpoch, timestamp);
    }

    /** One 61, or one 62 entry ({@code templateId} names which, for the duplicate counter). */
    public boolean onMeTradeEntry(int templateId, long egressSeq, long journalSeq, long tradeId, int marketId, long takerLegId,
                                  long takerUserId, long makerLegId, long makerUserId, long price, long quantity, boolean takerIsBuy,
                                  long meTimestamp, long takerOmsOrderId, long makerOmsOrderId, long meEpoch, long timestamp) {
        if (!behaviorV3()) {
            orders.applyTrade(egressSeq, tradeId, marketId, takerLegId, takerUserId, makerLegId, makerUserId, price, quantity,
                    takerIsBuy, meTimestamp, takerOmsOrderId, makerOmsOrderId, meEpoch, timestamp);
            return true;
        }
        observeLogTime(timestamp);
        final long digest = tradeDigest(egressSeq, journalSeq, tradeId, marketId, takerLegId, takerUserId, makerLegId, makerUserId,
                price, quantity, takerIsBuy, meTimestamp, takerOmsOrderId, makerOmsOrderId, meEpoch);
        final int verdict = admit(templateId, journalSeq, meEpoch, egressSeq, tradeId, marketId, digest, false);
        if (verdict != V_APPLY) {
            return verdict == V_DUPLICATE;
        }
        // 3.4: the second invariant (dense tradeId) and the S5 apply; a refusal latches SETTLE_REFUSED, skip s,
        // latchTradeId = L + 1 (never the frame's tradeId, review 2 BLOCKER-A)
        final long nextTradeId = lastAppliedTradeId + 1;
        if (!orders.applyTradeV3(journalSeq, egressSeq, tradeId, marketId, takerLegId, takerUserId, makerLegId, makerUserId,
                price, quantity, takerIsBuy, meTimestamp, takerOmsOrderId, makerOmsOrderId, meEpoch, timestamp)) {
            feed.countSettleRefused();
            latch(FeedHaltReason.SETTLE_REFUSED, journalSeq, journalSeq, journalSeq, nextTradeId, 0L, 0L, tradeId);
            return false;
        }
        commit(journalSeq, egressSeq, digest);
        return true;
    }

    /** MeJournalTerminal 63. Under v1/v2 the RC1 apply (meReason unused). */
    public boolean onMeTerminal(long egressSeq, long journalSeq, long legId, long userId, int marketId, int statusCode,
                                int meRejectReason, long meTimestamp, long omsOrderId, long meEpoch, long timestamp) {
        if (!behaviorV3()) {
            orders.applyTerminal(egressSeq, legId, userId, marketId, statusCode, meTimestamp, omsOrderId, meEpoch, timestamp);
            return true;
        }
        observeLogTime(timestamp);
        final long digest = terminalDigest(egressSeq, journalSeq, legId, userId, marketId, statusCode, meRejectReason, meTimestamp,
                omsOrderId, meEpoch);
        final int verdict = admit(63, journalSeq, meEpoch, egressSeq, 0L, marketId, digest, false);
        if (verdict != V_APPLY) {
            return verdict == V_DUPLICATE;
        }
        orders.applyTerminal(egressSeq, legId, userId, marketId, statusCode, meTimestamp, omsOrderId, meEpoch, timestamp,
                meRejectReason);
        commit(journalSeq, egressSeq, digest);
        return true;
    }

    private final MeOutcomeView s7Outcome = new MeOutcomeView();
    private final MeReleaseView s7Release = new MeReleaseView();

    /**
     * MeCommandOutcome 64, the S7 signature (kept, FIX-7): under v1/v2 the RC1 apply ({@code oldCancelled} unused); under v3
     * the view form with the section 2 defaults for the 64 tail.
     */
    public boolean onMeOutcome(long egressSeq, long journalSeq, long commandIdHigh, long commandIdLow, long userId,
                               long omsOrderId, long oldOrderId, long price, long quantity, long budget, int marketId,
                               int commandKind, int result, long legId, int reason, boolean oldCancelled, long meTimestamp,
                               long meEpoch, long timestamp) {
        if (!behaviorV3()) {
            orders.applyOutcome(egressSeq, commandIdHigh, commandIdLow, userId, omsOrderId, oldOrderId, price, quantity, budget,
                    marketId, commandKind, result, legId, reason, meTimestamp, meEpoch, timestamp);
            return true;
        }
        return onMeOutcome(orders.outcomeWithDefaults(s7Outcome, egressSeq, journalSeq, commandIdHigh, commandIdLow, userId,
                omsOrderId, oldOrderId, price, quantity, budget, marketId, commandKind, result, legId, reason, oldCancelled,
                meTimestamp, meEpoch), timestamp);
    }

    /**
     * MeCommandOutcome 64 with every field (RC2 S8 section 2): under v3 D1 admission with the widened digest (3.10 item 2),
     * then the S8 tables; under v1/v2 the RC1 apply of the fields it knows.
     */
    public boolean onMeOutcome(final MeOutcomeView v, final long timestamp) {
        if (!behaviorV3()) {
            orders.applyOutcome(v.egressSeq, v.commandIdHigh, v.commandIdLow, v.userId, v.omsOrderId, v.oldOrderId, v.price,
                    v.quantity, v.budget, v.marketId, v.commandKind, v.result, v.orderId, v.reason, v.meTimestamp, v.meEpoch,
                    timestamp);
            return true;
        }
        observeLogTime(timestamp);
        final long digest = outcomeDigest(v);
        final int verdict = admit(64, v.journalSeq, v.meEpoch, v.egressSeq, 0L, v.marketId, digest, false);
        if (verdict != V_APPLY) {
            return verdict == V_DUPLICATE;
        }
        orders.applyOutcomeV3(v, timestamp);
        commit(v.journalSeq, v.egressSeq, digest);
        return true;
    }

    /**
     * MeCommandRelease 67, the S7 signature (kept, FIX-7): {@code releaseSeq 0}, {@code ledgerSize = meLedgerSize},
     * {@code ledgerCapacity = meLedgerCapacity}, {@code tombstoneRanges 0}, no entries.
     */
    public boolean onMeRelease(long egressSeq, long journalSeq, long meEpoch, long timestamp) {
        final MeReleaseView v = s7Release.reset();
        v.egressSeq = egressSeq;
        v.journalSeq = journalSeq;
        v.meEpoch = meEpoch;
        v.ledgerSize = orders.meLedgerSize();
        v.ledgerCapacity = feed.meLedgerCapacity();
        return onMeRelease(v, timestamp);
    }

    /** MeCommandRelease 67, v3 only (RC2 S8 3.2): D1 with the widened digest, then the content against the backlog. */
    public boolean onMeRelease(final MeReleaseView v, final long timestamp) {
        if (!behaviorV3()) {
            return false; // not served below v3 (the demuxer counts it unhandled)
        }
        observeLogTime(timestamp);
        final long digest = releaseDigest(v);
        final int verdict = admit(67, v.journalSeq, v.meEpoch, v.egressSeq, 0L, 0, digest, false);
        if (verdict != V_APPLY) {
            return verdict == V_DUPLICATE;
        }
        orders.applyReleaseV3(v, timestamp);
        commit(v.journalSeq, v.egressSeq, digest);
        return true;
    }

    /**
     * MeEngineConfig 68, v3 only (D2 row 1 and 4.2): the FRESH record at journalSeq 1 binds the epoch (trust on first use),
     * is applied and committed, then every FORWARDER/BRIDGE registration with a foreign sourceEpoch is revoked (10.2);
     * later records must be valid BUMPs.
     */
    public boolean onMeEngineConfig(long egressSeq, long journalSeq, long meEpoch, long configVersion, long behaviorVersion,
                                    long ledgerCapacity, int maxWireVersion, ConfigDecision decision, long timestamp,
                                    LongConsumer revokedSession) {
        if (!behaviorV3()) {
            return false;
        }
        observeLogTime(timestamp);
        final boolean values = configVersion >= 1 && behaviorVersion >= 2 && ledgerCapacity >= 1
                && ledgerCapacity <= MAX_LEDGER_CAPACITY && maxWireVersion >= MIN_WIRE && maxWireVersion <= MAX_WIRE;
        final long digest = configDigest(egressSeq, journalSeq, meEpoch, configVersion, behaviorVersion, ledgerCapacity,
                maxWireVersion, decision);
        // D2 row 1 with the 3.8 belt: the epoch must be the recorded genesisId (S2: meEpoch := genesisId), so a core
        // re-genesised without its ME never binds the old ME's retained FRESH (4.4, review 2 NOTE-I)
        if (feed.boundMeEpoch() == 0 && journalSeq == 1 && meEpoch > 0 && decision == ConfigDecision.FRESH && values
                && meEpoch == coreConfig.recorded().genesisId) {
            feed.bind(meEpoch);
            feed.recordMeConfig(configVersion, behaviorVersion, ledgerCapacity, maxWireVersion);
            commit(journalSeq, egressSeq, digest);
            feed.countEpochRevoked(sessions.revokeForeignEpoch(meEpoch, revokedSession));
            return true;
        }
        final int verdict = admit(68, journalSeq, meEpoch, egressSeq, 0L, 0, digest, decision == ConfigDecision.FRESH);
        if (verdict != V_APPLY) {
            return verdict == V_DUPLICATE;
        }
        // 4.2 (bound, s >= 2): only a BUMP of the record the ME journaled before, mirroring S2 rows 9, 11, 13, 14
        if (!values || decision != ConfigDecision.BUMP || configVersion <= feed.meConfigVersion()
                || behaviorVersion != feed.meBehaviorVersion() || ledgerCapacity != feed.meLedgerCapacity()
                || maxWireVersion < feed.meMaxWireVersion()) {
            latch(FeedHaltReason.SOURCE_SCHEMA, journalSeq, journalSeq, journalSeq, 0L, 0L, 0L, 0L);
            return false;
        }
        feed.recordMeConfig(configVersion, behaviorVersion, ledgerCapacity, maxWireVersion);
        commit(journalSeq, egressSeq, digest);
        return true;
    }

    /**
     * 5.4: a feed frame that passed the header, bounds and class gates but failed a value range, v3 only. Returns true
     * when the frame latched (rows 1 and 2): a 62 batch stops there. {@code rawTradeId} is diagnostic only.
     */
    public boolean onFeedPoison(int templateId, long journalSeq, long meEpoch, long rawTradeId, long timestamp) {
        if (!behaviorV3()) {
            return false;
        }
        // no observeLogTime: a frame that failed its value ranges is decode poison, it leaves no trace but the latch (5.4)
        final long bound = feed.boundMeEpoch();
        if (bound == 0 && journalSeq == 1) {
            latch(FeedHaltReason.SOURCE_SCHEMA, 1L, 1L, 0L, 0L, 0L, 0L, rawTradeId);
            return true;
        }
        if (bound != 0 && meEpoch == bound && journalSeq == feed.lastAppliedJournalSeq() + 1) {
            final long tradeId = templateId == 61 || templateId == 62 ? lastAppliedTradeId + 1 : 0L;
            latch(FeedHaltReason.SOURCE_SCHEMA, journalSeq, journalSeq, journalSeq, tradeId, 0L, 0L, rawTradeId);
            return true;
        }
        feed.countSourceSchema(); // a corrupt duplicate or gap entry does not stall the cursor
        return false;
    }

    /** FeedHalt 66 (D3a), BRIDGE, v3 only. */
    public void applyFeedHalt(long sessionId, long correlationId, FeedHaltReason reason, long meEpochSeen,
                              long expectedJournalSeq, long seenJournalSeq, long bridgeIdentity, long timestamp) {
        if (!behaviorV3()) {
            return;
        }
        observeLogTime(timestamp);
        final long c = feed.lastAppliedJournalSeq();
        final long bound = feed.boundMeEpoch();
        final boolean invalid;
        switch (reason) {
            case GAP: invalid = !(expectedJournalSeq >= 1 && expectedJournalSeq < seenJournalSeq); break;
            case EPOCH_MISMATCH: invalid = meEpochSeen == 0; break;
            case SOURCE_SCHEMA: invalid = expectedJournalSeq < 1; break;
            case SOURCE_UNBOUND:
            case SETTLE_REFUSED: invalid = false; break;
            default: invalid = true; break;
        }
        if (invalid) {
            sink.onAdminAck(correlationId, 66, AdminResult.REJECTED, CoreRejectReason.INVALID.code(), 0L);
            return;
        }
        if (reason == FeedHaltReason.SOURCE_UNBOUND || reason == FeedHaltReason.SETTLE_REFUSED) {
            feed.countHaltDuplicate(); // binding and refusal are the core's judgment only
            sink.onAdminAck(correlationId, 66, AdminResult.DUPLICATE, 0, 0L);
            return;
        }
        final boolean stale = reason == FeedHaltReason.GAP ? c >= seenJournalSeq - 1
                : reason == FeedHaltReason.EPOCH_MISMATCH ? bound == 0 || meEpochSeen == bound
                : c >= expectedJournalSeq;
        if (stale) {
            feed.countHaltStale();
            sink.onAdminAck(correlationId, 66, AdminResult.DUPLICATE, 0, 0L);
            return;
        }
        if (feed.latchReason() == reason || latchDecision(reason, 0L) != L_WRITE) {
            if (feed.latchReason() != reason && latchDecision(reason, 0L) == L_SUPPRESS) {
                feed.countLatchSuppressed();
            }
            feed.countHaltDuplicate();
            sink.onAdminAck(correlationId, 66, AdminResult.DUPLICATE, 0, 0L);
            return;
        }
        feed.countHaltApplied();
        sink.onAdminAck(correlationId, 66, AdminResult.ACCEPTED, 0, 0L);
        final long expected = reason == FeedHaltReason.EPOCH_MISMATCH && expectedJournalSeq < 1 ? Math.max(1L, c + 1)
                : expectedJournalSeq;
        writeLatch(reason, expected, seenJournalSeq, 0L, 0L,
                reason == FeedHaltReason.EPOCH_MISMATCH ? meEpochSeen : 0L, bridgeIdentity, 0L);
    }

    /** FeedLatchClear 76 (D3b, total), ADMIN, v3 only. */
    public void applyFeedLatchClear(long correlationId, FeedHaltReason reason, long journalSeq, long timestamp) {
        if (!behaviorV3()) {
            return;
        }
        observeLogTime(timestamp);
        if (reason == FeedHaltReason.EPOCH_MISMATCH || reason == FeedHaltReason.SOURCE_UNBOUND || reason == FeedHaltReason.NONE) {
            sink.onAdminAck(correlationId, 76, AdminResult.REJECTED, CoreRejectReason.INVALID.code(), 0L); // genesis only
            return;
        }
        if (!feed.latched() || feed.latchReason() != reason) {
            sink.onAdminAck(correlationId, 76, AdminResult.DUPLICATE, 0, 0L);
            return;
        }
        final long skip = feed.latchSkipSeq();
        final boolean clearOnly = journalSeq == 0 && (reason == FeedHaltReason.GAP || skip == 0);
        final boolean skipIt = reason != FeedHaltReason.GAP && skip != 0 && journalSeq == skip;
        if (!clearOnly && !skipIt) {
            sink.onAdminAck(correlationId, 76, AdminResult.REJECTED, CoreRejectReason.INVALID.code(), 0L);
            return;
        }
        if (skipIt) {
            // row 5: exactly the one entry the core refused or found poisoned at c + 1, and exactly one tradeId
            final long tradeId = feed.latchTradeId();
            feed.advanceCursorTo(journalSeq);
            feed.recordDigest(journalSeq, 0L); // content unknown: a later copy of it cannot be byte-checked
            if (tradeId != 0) {
                lastAppliedTradeId = tradeId;
            }
            feed.setLastSkippedJournalSeq(journalSeq);
        }
        feed.zeroLatch();
        feed.countLatchClear();
        sink.onAdminAck(correlationId, 76, AdminResult.ACCEPTED, 0, 0L);
        sink.onOrderFault(OrderFaultKind.FEED_HALT, 0L, 0L, 0, journalSeq, 0L, logPosition);
        sink.onFeedLatchCleared(reason, journalSeq, false);
    }

    /** D4 internal clear of one engine-owned bit (S8's D4 auto-clear; no S7 caller). Only under v3. */
    public void clearGateInternally(int marketId, GateReason reason) {
        if (behaviorV3() && gate.clearReason(marketId, reason)) {
            emitAdmission(marketId, false);
        }
    }

    // ---- D1, the commit, the latch slot ----

    /** D1 (and D2 rows 2-3 for a non-binding entry while unbound). Every refusal latches through {@link #latch}. */
    private int admit(final int templateId, final long s, final long m, final long g, final long tradeId, final int marketId,
                      final long digest, final boolean freshRecord) {
        final long bound = feed.boundMeEpoch();
        final long c = feed.lastAppliedJournalSeq();
        if (s < 1 || m <= 0) {
            latch(FeedHaltReason.SOURCE_SCHEMA, Math.max(1L, c + 1), s, 0L, 0L, 0L, 0L, tradeId); // D1 row 1, decode-grade
            return V_REFUSED;
        }
        if (bound == 0) {
            feed.countUnbound();
            refuseFeed(tradeId, marketId, s, SettleRejectReason.FEED_UNBOUND, 1L);
            if (s == 1) {
                latch(FeedHaltReason.SOURCE_UNBOUND, 1L, 1L, 0L, 0L, 0L, 0L, tradeId); // D2 row 2: ME content, genesis only
            } else {
                latch(FeedHaltReason.GAP, 1L, s, 0L, 0L, 0L, 0L, tradeId); // D2 row 3: a hole at 1
            }
            return V_REFUSED;
        }
        if (m != bound) {
            feed.countEpochMismatch();
            refuseFeed(tradeId, marketId, s, SettleRejectReason.ME_EPOCH_MISMATCH, bound);
            latch(FeedHaltReason.EPOCH_MISMATCH, Math.max(1L, c + 1), s, 0L, 0L, m, 0L, tradeId);
            return V_REFUSED;
        }
        if (s <= c) {
            // 3.8 (the duplicate belt): an entry at or below the cursor must byte-match what was applied there. The FRESH
            // record is kept for the epoch's life, the last DIGEST_WINDOW entries in a ring; a 76-skipped entry is unknown.
            final boolean held = s == 1 || feed.digestHeld(s);
            final long applied = s == 1 ? feed.freshDigest() : held ? feed.digestAt(s) : 0L;
            if (held && applied != 0 && applied != digest) {
                feed.countDuplicateMismatch();
                if (s == 1 && freshRecord) {
                    // another FRESH under the bound epoch: the ME was re-genesised (or restored) reusing the epoch, genesis only
                    // (belt review D-1: only a 68 FRESH; any other template labelled seq 1 is the SOURCE_SCHEMA row below)
                    latch(FeedHaltReason.EPOCH_MISMATCH, Math.max(1L, c + 1), s, 0L, 0L, m, 0L, tradeId);
                } else {
                    // the source served other content at an applied seq (an in-epoch ME rewind, or a corrupt copy): loud,
                    // skip 0 (nothing to skip: the cursor is past it), reach-cleared by the next exactly-next entry
                    latch(FeedHaltReason.SOURCE_SCHEMA, c + 1, s, 0L, 0L, 0L, 0L, tradeId);
                }
                return V_REFUSED;
            }
            if (!held) {
                feed.countDuplicateUnverified(); // older than the window: cannot be compared (counted, contract 3.8)
            }
            feed.countDuplicate(templateId);
            if (s == feed.lastSkippedJournalSeq()) {
                feed.countSkippedEntryRedelivered(); // FIX-G: a valid copy of an entry a 76 skipped
            }
            return V_DUPLICATE;
        }
        if (s > c + 1) {
            feed.countGap();
            refuseFeed(tradeId, marketId, s, SettleRejectReason.FEED_GAP, c + 1);
            latch(FeedHaltReason.GAP, c + 1, s, 0L, 0L, 0L, 0L, tradeId);
            return V_REFUSED;
        }
        if (g < consumePosition) {
            feed.countEgressRegression(); // D1 row 6: the ME rewound inside its epoch (4.4); skip 0, never consumable
            latch(FeedHaltReason.SOURCE_SCHEMA, c + 1, s, 0L, 0L, 0L, 0L, tradeId);
            return V_REFUSED;
        }
        return V_APPLY;
    }

    /** ORIGIN SettleRejected for a v3 feed refusal: journalPosition = journalSeq (S1 4.1). */
    private void refuseFeed(final long tradeId, final int marketId, final long s, final SettleRejectReason reason, final long detail) {
        settleRejectCount++;
        sink.onSettleRejected(tradeId, marketId, s, reason, detail);
    }

    /** 3.3: {@code c := s}, {@code P := max(P, g)}, the entry's digest (3.8), then the reach-clear. */
    private void commit(final long s, final long g, final long digest) {
        feed.advanceCursorTo(s);
        feed.recordDigest(s, digest);
        if (g > consumePosition) {
            consumePosition = g;
        }
        reachClear();
    }

    /** 3.3 reach-clear: the only automatic exit; it never moves the cursor and never reopens the gate. */
    private void reachClear() {
        if (!feed.latched()) {
            return;
        }
        final long c = feed.lastAppliedJournalSeq();
        final FeedHaltReason reason = feed.latchReason();
        final boolean reached;
        switch (reason) {
            case GAP: reached = c >= feed.latchSeenSeq() - 1; break;
            case SOURCE_SCHEMA:
            case SETTLE_REFUSED: reached = feed.latchSkipSeq() != 0 ? c >= feed.latchSkipSeq() : c >= feed.latchExpectedSeq(); break;
            default: reached = false; break;
        }
        if (!reached) {
            return;
        }
        feed.zeroLatch();
        feed.countLatchAutoClear();
        sink.onOrderFault(OrderFaultKind.FEED_HALT, 0L, 0L, 0, c, 0L, logPosition);
        sink.onFeedLatchCleared(reason, 0L, true);
    }

    /** Exit class of a latch reason (5.1): 1 auto, 2 admin, 3 genesis. */
    private static int exitClass(final FeedHaltReason r) {
        switch (r) {
            case GAP: return 1;
            case SOURCE_SCHEMA:
            case SETTLE_REFUSED: return 2;
            case EPOCH_MISMATCH:
            case SOURCE_UNBOUND: return 3;
            default: return 0;
        }
    }

    /** 5.1 precedence of a new latch against the one slot. */
    private int latchDecision(final FeedHaltReason reason, final long skip) {
        if (!feed.latched()) {
            return L_WRITE;
        }
        final int next = exitClass(reason);
        final int current = exitClass(feed.latchReason());
        if (next > current) {
            return L_WRITE;
        }
        if (next == current && skip != 0 && feed.latchSkipSeq() == 0) {
            return L_WRITE; // review NOTE-1: a skippable latch replaces a skip-0 one of its class
        }
        if (skip != 0 && feed.latchSkipSeq() == skip) {
            return L_MERGE; // latchTradeId is L + 1 on both sides by construction: nothing changes
        }
        return L_SUPPRESS;
    }

    /** 5.1 / 5.2: raise, replace, merge or suppress a latch. */
    private void latch(final FeedHaltReason reason, final long expected, final long seen, final long skip, final long tradeId,
                       final long meEpochSeen, final long bridgeIdentity, final long rawTradeId) {
        switch (latchDecision(reason, skip)) {
            case L_WRITE:
                writeLatch(reason, expected, seen, skip, tradeId, meEpochSeen, bridgeIdentity, rawTradeId);
                break;
            case L_MERGE:
                break;
            default:
                feed.countLatchSuppressed();
                break;
        }
    }

    /** 5.2 latching effects: the fields, the BRIDGE_HALT bit, the FeedHaltApplied OrderFault, the node-local sink call. */
    private void writeLatch(final FeedHaltReason reason, final long expected, final long seen, final long skip, final long tradeId,
                            final long meEpochSeen, final long bridgeIdentity, final long rawTradeId) {
        feed.writeLatch(reason, expected, seen, skip, tradeId, meEpochSeen, bridgeIdentity, logPosition);
        suspendGateInternally(OrderEntryGate.GLOBAL, GateReason.BRIDGE_HALT);
        sink.onOrderFault(OrderFaultKind.FEED_HALT, 0L, 0L, 0, seen, expected, logPosition);
        sink.onFeedLatch(reason, expected, seen, skip, rawTradeId);
    }

    /** The AdmissionState of one gate with its reason set (all four emission sites go through here or pass it). */
    private void emitAdmission(final int marketId, final boolean toOrigin) {
        sink.onAdmissionState(marketId, gate.state(marketId), gate.reason(marketId), false, lastLogTime, logPosition, toOrigin,
                gate.reasons(marketId));
    }

    // 3.8: the entry digest, over the decoded fields the core applies (a 62 entry and the same 61 digest alike)
    private static final long K_TRADE = 0x61L;
    private static final long K_TERMINAL = 0x63L;
    private static final long K_OUTCOME = 0x64L;
    private static final long K_RELEASE = 0x67L;
    private static final long K_CONFIG = 0x68L;

    // The digest definition is part of behaviour 3 (contract 3.8): pinned by DigestVectorTest. Behaviour 3 is in the
    // production set from RC2 S8; the digest and every v3 rule freeze at the first v3 log outside a test rig (S8 D-S8-3,
    // 3.10 item 5) and may change only under a new behaviour version from then on.

    /** 3.8 digest of a 61 or of one 62 entry. */
    public static long tradeDigest(long egressSeq, long journalSeq, long tradeId, int marketId, long takerLegId, long takerUserId,
                                   long makerLegId, long makerUserId, long price, long quantity, boolean takerIsBuy,
                                   long meTimestamp, long takerOmsOrderId, long makerOmsOrderId, long meEpoch) {
        return digest(digest(digest(digest(digest(digest(digest(digest(digest(digest(digest(digest(digest(digest(
                digest(K_TRADE, egressSeq), journalSeq), tradeId), marketId), takerLegId), takerUserId), makerLegId), makerUserId),
                price), quantity), takerIsBuy ? 1 : 0), meTimestamp), takerOmsOrderId), makerOmsOrderId), meEpoch);
    }

    /** 3.8 digest of a 63. */
    public static long terminalDigest(long egressSeq, long journalSeq, long legId, long userId, int marketId, int statusCode,
                                      int meRejectReason, long meTimestamp, long omsOrderId, long meEpoch) {
        return digest(digest(digest(digest(digest(digest(digest(digest(digest(digest(digest(K_TERMINAL, egressSeq),
                journalSeq), legId), userId), marketId), statusCode), meRejectReason), meTimestamp), omsOrderId), meEpoch), 0);
    }

    /**
     * 3.8 digest of a 64, widened by RC2 S8 3.10 item 2: the S7 fields, then orderType, orderSide, appliedPosition, status,
     * timeInForce, expectedOldRemaining, ledgerSize, ledgerCapacity, behaviorVersion.
     */
    public static long outcomeDigest(final MeOutcomeView v) {
        long h = digest(K_OUTCOME, v.egressSeq);
        h = digest(h, v.journalSeq);
        h = digest(h, v.commandIdHigh);
        h = digest(h, v.commandIdLow);
        h = digest(h, v.userId);
        h = digest(h, v.omsOrderId);
        h = digest(h, v.oldOrderId);
        h = digest(h, v.price);
        h = digest(h, v.quantity);
        h = digest(h, v.budget);
        h = digest(h, v.marketId);
        h = digest(h, v.commandKind);
        h = digest(h, v.result);
        h = digest(h, v.orderId);
        h = digest(h, v.reason);
        h = digest(h, v.oldCancelled ? 1 : 0);
        h = digest(h, v.meTimestamp);
        h = digest(h, v.meEpoch);
        h = digest(h, v.orderType);
        h = digest(h, v.orderSide);
        h = digest(h, v.appliedPosition);
        h = digest(h, v.status);
        h = digest(h, v.timeInForce);
        h = digest(h, v.expectedOldRemaining);
        h = digest(h, v.ledgerSize);
        h = digest(h, v.ledgerCapacity);
        return digest(h, v.behaviorVersion);
    }

    /**
     * 3.8 digest of a 67, widened by RC2 S8 3.10 item 2: egressSeq, journalSeq, meEpoch, releaseSeq, ledgerSize,
     * ledgerCapacity, tombstoneRanges, the entry count, then every entry (commandIdHigh, idLowFloor, scope, result, removed)
     * in group order.
     */
    public static long releaseDigest(final MeReleaseView v) {
        long h = digest(K_RELEASE, v.egressSeq);
        h = digest(h, v.journalSeq);
        h = digest(h, v.meEpoch);
        h = digest(h, v.releaseSeq);
        h = digest(h, v.ledgerSize);
        h = digest(h, v.ledgerCapacity);
        h = digest(h, v.tombstoneRanges);
        h = digest(h, v.count);
        for (int i = 0; i < v.count; i++) {
            h = digest(h, v.commandIdHigh[i]);
            h = digest(h, v.idLowFloor[i]);
            h = digest(h, v.scope[i]);
            h = digest(h, v.result[i]);
            h = digest(h, v.removed[i]);
        }
        return h;
    }

    /** 3.8 digest of a 68. */
    public static long configDigest(long egressSeq, long journalSeq, long meEpoch, long configVersion, long behaviorVersion,
                                    long ledgerCapacity, int maxWireVersion, ConfigDecision decision) {
        return digest(digest(digest(digest(digest(digest(digest(digest(digest(K_CONFIG, egressSeq), journalSeq),
                meEpoch), configVersion), behaviorVersion), ledgerCapacity), maxWireVersion), decision.code()), 0);
    }

    /** One step of the deterministic 64-bit entry digest; never 0 (0 means "content unknown"). */
    static long digest(final long h, final long v) {
        long x = (h ^ 0x9E3779B97F4A7C15L) * 0xBF58476D1CE4E5B9L + v;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        x ^= x >>> 31;
        return x == 0 ? 1 : x;
    }

    /** Feed-virgin (7.1): nothing the RC1 feed rules did can have moved money or minted an ME command. */
    private boolean feedVirgin() {
        return lastAppliedTradeId == 0L && consumePosition == 0L && dispatchSeq == 0L;
    }


    /** OrderEntryGate (ADMIN, replicated): set one gate; DUPLICATE when unchanged; broadcast the new state. */
    public void applyOrderEntryGate(long correlationId, int marketId, GateState state, GateReason reason, long timestamp) {
        observeLogTime(timestamp);
        if (!OrderEntryGate.validMarket(marketId) || state == null || reason == null) {
            sink.onAdminAck(correlationId, 73, AdminResult.REJECTED, CoreRejectReason.UNKNOWN_MARKET.code(), 0L);
            return;
        }
        if (behaviorV3()) {
            applyOrderEntryGateV3(correlationId, marketId, state, reason);
            return;
        }
        final boolean changed = gate.set(marketId, state, reason);
        sink.onAdminAck(correlationId, 73, changed ? AdminResult.ACCEPTED : AdminResult.DUPLICATE, 0, 0L);
        if (changed) {
            emitAdmission(marketId, false);
        }
    }

    /** RC2 S7 D4 under v3: ADMIN sets only ADMIN-owned bits; OPEN clears every bit once the two refusals pass. */
    private void applyOrderEntryGateV3(final long correlationId, final int marketId, final GateState state, final GateReason reason) {
        final boolean changed;
        if (state == GateState.SUSPENDED) {
            if (!OrderEntryGate.adminOwned(reason)) {
                sink.onAdminAck(correlationId, 73, AdminResult.REJECTED, CoreRejectReason.INVALID.code(), 0L);
                return;
            }
            changed = gate.addReason(marketId, reason);
        } else {
            final int bits = gate.reasons(marketId);
            if ((bits & (1 << GateReason.BRIDGE_HALT.code())) != 0 && feed.latched()) {
                sink.onAdminAck(correlationId, 73, AdminResult.REJECTED, CoreRejectReason.FEED_HALTED.code(), 0L);
                return;
            }
            if (marketId == OrderEntryGate.GLOBAL && (feed.boundMeEpoch() == 0 || feed.meBehaviorVersion() < 2)) {
                sink.onAdminAck(correlationId, 73, AdminResult.REJECTED, CoreRejectReason.ME_LANE_INACTIVE.code(), 0L);
                return; // X13: GLOBAL stays closed until the ME epoch is bound
            }
            changed = gate.clearAll(marketId);
        }
        sink.onAdminAck(correlationId, 73, changed ? AdminResult.ACCEPTED : AdminResult.DUPLICATE, 0, 0L);
        if (changed) {
            emitAdmission(marketId, false);
        }
    }
}

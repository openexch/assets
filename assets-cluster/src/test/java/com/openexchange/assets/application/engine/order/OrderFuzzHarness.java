// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Predicate;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;

/**
 * The model half of the order fuzz (contract S5 sections 1 and 3): the rig, the seeded random source, the
 * model of every live order and of the external totals, the event expectations shared by the operations, and
 * {@link #verify()}, which after every operation runs {@link LifecycleInvariants} with the model's external
 * totals, residuals and positions, compares each live record with its model field by field, checks the engine
 * counters and applies the light form of I15 (the last BALANCE per (user, asset) emitted by the operation
 * equals the post-apply balance). {@link OrderFuzzDriver} supplies the operations.
 */
class OrderFuzzHarness {

    static final int MARKET = 1;
    static final int USERS = 3;
    static final long CENT = ONE / 100;
    static final int NO_STATUS = -1;

    /** The model of one live order. */
    static final class MOrder {
        long id;
        long user;
        int side;
        int tif;
        long price;
        long qty;
        long filled;
        long cum;
        int holdAsset;
        long holdAmount;
        long residual;
        long leg;
        int attempt;
        int pendingKind = CoreOrder.PK_NONE;
        boolean dispatchPending;
        long pendingDispatchSeq;
        boolean cancelPending;
        boolean expiryRequested;
        boolean replacePending;
        long expiresAtMs;
        long pPrice;
        long pQty;
        long pDelta;
        long pTarget;
        long oldLeg;
        int status;

        long remaining() {
            return qty - filled;
        }

        /** Largest fill that keeps the order inside the contract (strictly below the pending quantity in a window). */
        long tradeable() {
            return replacePending ? Math.min(remaining(), pQty - filled - CENT) : remaining();
        }

        /** Highest price a BUY may fill at: inside a window the lower of the old and the pending limit (see driver). */
        long buyLimit() {
            return replacePending ? Math.min(price, pPrice) : price;
        }
    }

    record Gone(long id, long user, long leg) { }

    final OrderRig r = OrderRig.open();
    final long seed;
    final Random rng;
    final long[] external = new long[Asset.count()];
    final TreeMap<Long, MOrder> live = new TreeMap<>();
    final Map<Long, long[]> position = new TreeMap<>();
    final List<Gone> gone = new ArrayList<>();
    long nextOrderId = 1;
    long nextLeg = 1_000;
    long nextAmendRequest = 1;
    long egressSeq;
    long tradeId;
    long dispatchSeq;
    long[] lastTrade;
    long expectedStaleTerminals;
    long expectedStaleOutcomes;
    long expectedRedundantOutcomes;
    long expectedCancelRefused;
    boolean overfilled;
    boolean justSwept;
    int op;
    String opText = "setup";

    OrderFuzzHarness(final long seed) {
        this.seed = seed;
        this.rng = new Random(seed);
        final RiskConfigSetCommand cfg = r.riskConfig(MARKET, 1);
        cfg.priceCollarBps = 0;
        cfg.maxOrdersPerSec = 1_000;
        cfg.maxOrdersPerMin = 100_000;
        r.setRisk(cfg);
        for (long u = 1; u <= USERS; u++) {
            deposit(u, USD, 1_000_000 * ONE);
            deposit(u, BTC, 1_000 * ONE);
            position.put(u, new long[UserRisk.MARKET_SLOTS]);
        }
    }

    // ---------------------------------------------------------------- model helpers

    void deposit(final long user, final int asset, final long amount) {
        r.deposit(user, asset, amount);
        external[asset] += amount;
    }

    void mintCancel(final MOrder o) {
        o.attempt++;
        o.pendingKind = CoreOrder.PK_CANCEL;
        o.dispatchPending = true;
        o.pendingDispatchSeq = ++dispatchSeq;
    }

    /** The order left the store: its hold is released in full, its deadlines and pending dispatch are gone. */
    void terminal(final MOrder o) {
        if (o.residual > 0) {
            balanceEvent(o.user, o.holdAsset, "the residual release of order " + o.id);
        }
        if (r.orders.orders().get(o.id) != null) {
            throw fail("terminal order " + o.id + " must be removed from the store in the same apply");
        }
        live.remove(o.id);
        gone.add(new Gone(o.id, o.user, o.leg));
    }

    void expectTerminalEvent(final MOrder o, final EventKind kind) {
        final AssetsEngineEvent.OrderEvent ev = orderEvent(o.id, kind);
        final OrderStatus status = kind == EventKind.EXPIRED ? OrderStatus.EXPIRED : OrderStatus.CANCELLED;
        if (ev.status() != status) {
            throw fail(kind + " carries status " + status + ": " + ev.render());
        }
    }

    List<MOrder> select(final Predicate<MOrder> p) {
        final List<MOrder> out = new ArrayList<>();
        for (final MOrder o : live.values()) {
            if (p.test(o)) {
                out.add(o);
            }
        }
        return out;
    }

    MOrder pick(final List<MOrder> cands) {
        return cands.get(rng.nextInt(cands.size()));
    }

    // ---------------------------------------------------------------- event expectations

    <T extends AssetsEngineEvent> T find(final Class<T> type, final Predicate<T> p, final String what) {
        for (final AssetsEngineEvent e : r.events()) {
            if (type.isInstance(e) && p.test(type.cast(e))) {
                return type.cast(e);
            }
        }
        throw fail("expected " + what + ", got " + events());
    }

    AssetsEngineEvent.OrderEvent orderEvent(final long orderId, final EventKind kind) {
        return find(AssetsEngineEvent.OrderEvent.class, e -> e.orderId() == orderId && e.kind() == kind,
                "ORDEREVENT kind=" + kind + " for order " + orderId);
    }

    AssetsEngineEvent.MeDispatch expectDispatch(final DispatchKind kind, final MOrder o) {
        final long idLow = (long) o.attempt << 2 | kind.code();
        final AssetsEngineEvent.MeDispatch d = find(AssetsEngineEvent.MeDispatch.class,
                e -> e.kind() == kind && e.commandIdHigh() == o.id && e.commandIdLow() == idLow,
                "MEDISPATCH kind=" + kind + " id=" + o.id + "/" + idLow);
        if (d.dispatchSeq() != o.pendingDispatchSeq) {
            throw fail("dispatchSeq of that mint must be " + o.pendingDispatchSeq + ": " + d.render());
        }
        return d;
    }

    void balanceEvent(final long user, final int asset, final String what) {
        find(AssetsEngineEvent.Balance.class, b -> b.userId() == user && b.assetId() == asset,
                "BALANCE u=" + user + " asset=" + asset + " for " + what);
    }

    void expectReject(final CoreRejectReason reason, final int refTemplateId) {
        final AssetsEngineEvent.OrderReject rej = find(AssetsEngineEvent.OrderReject.class, e -> true, "ORDERREJECT " + reason);
        if (rej.reason() != reason || rej.refTemplateId() != refTemplateId) {
            throw fail("expected ORDERREJECT reason=" + reason + " ref=" + refTemplateId + ", got " + rej.render());
        }
        if (r.events().size() != 1) {
            throw fail("a reject is the only event of its apply, got " + events());
        }
    }

    // ---------------------------------------------------------------- verification after every op

    void verify() {
        final LifecycleInvariants.Expectations x = new LifecycleInvariants.Expectations();
        x.context = ctx();
        x.external = external;
        x.residualByOrder = residuals();
        x.positionByUser = position;
        x.positionsMayNotNet = overfilled;
        x.justSwept = justSwept;
        LifecycleInvariants.check(r, x);
        final OrderStore store = r.orders.orders();
        if (store.size() != live.size()) {
            final List<Long> ids = new ArrayList<>();
            store.forEachByOrderId(o -> ids.add(o.orderId));
            throw fail("live records: model " + live.keySet() + " but the store holds " + ids);
        }
        for (final MOrder m : live.values()) {
            compare(m, store.get(m.id));
        }
        if (r.engine.getLastAppliedTradeId() != tradeId) {
            throw fail("lastAppliedTradeId expected " + tradeId + " got " + r.engine.getLastAppliedTradeId());
        }
        if (r.engine.getDispatchSeq() != dispatchSeq) {
            throw fail("dispatchSeq expected " + dispatchSeq + " got " + r.engine.getDispatchSeq());
        }
        if (r.orders.staleTerminalCount() != expectedStaleTerminals) {
            throw fail("staleTerminalCount expected " + expectedStaleTerminals + " got " + r.orders.staleTerminalCount());
        }
        if (r.orders.redundantOutcomeCount() != expectedRedundantOutcomes) {
            throw fail("redundantOutcomeCount expected " + expectedRedundantOutcomes + " got " + r.orders.redundantOutcomeCount());
        }
        if (r.orders.staleOutcomeCount() != expectedStaleOutcomes) {
            throw fail("staleOutcomeCount expected " + expectedStaleOutcomes + " got " + r.orders.staleOutcomeCount());
        }
        if (r.orders.cancelRefusedCount() != expectedCancelRefused) {
            throw fail("cancelRefusedCount expected " + expectedCancelRefused + " got " + r.orders.cancelRefusedCount());
        }
        balancesMatchPostApply();
    }

    private void compare(final MOrder m, final CoreOrder o) {
        if (o == null) {
            throw fail("order " + m.id + " is live in the model but missing from the store");
        }
        final List<String> bad = new ArrayList<>();
        eq(bad, "userId", m.user, o.userId);
        eq(bad, "marketId", MARKET, o.marketId);
        eq(bad, "side", m.side, o.side);
        eq(bad, "timeInForce", m.tif, o.timeInForce);
        eq(bad, "price", m.price, o.price);
        eq(bad, "quantity", m.qty, o.quantity);
        eq(bad, "filledQty", m.filled, o.filledQty);
        eq(bad, "remainingQty", m.remaining(), o.remainingQty);
        eq(bad, "cumQuoteQty", m.cum, o.cumQuoteQty);
        eq(bad, "holdAssetId", m.holdAsset, o.holdAssetId);
        eq(bad, "holdAmount", m.holdAmount, o.holdAmount);
        eq(bad, "currentLegId", m.leg, o.currentLegId);
        eq(bad, "attempt", m.attempt, o.attempt);
        eq(bad, "pendingKind", m.pendingKind, o.pendingKind);
        eq(bad, "pendingDispatchSeq", m.pendingDispatchSeq, o.pendingDispatchSeq);
        eq(bad, "expiresAtMs", m.expiresAtMs, o.expiresAtMs);
        flag(bad, "DISPATCH_PENDING", m.dispatchPending, o.has(CoreOrder.F_DISPATCH_PENDING));
        flag(bad, "CANCEL_PENDING", m.cancelPending, o.has(CoreOrder.F_CANCEL_PENDING));
        flag(bad, "REPLACE_PENDING", m.replacePending, o.has(CoreOrder.F_REPLACE_PENDING));
        flag(bad, "EXPIRY_REQUESTED", m.expiryRequested, o.has(CoreOrder.F_EXPIRY_REQUESTED));
        flag(bad, "IOC", m.tif == OrderEngine.TIF_IOC, o.has(CoreOrder.F_IOC));
        flag(bad, "QUARANTINED", false, o.has(CoreOrder.F_QUARANTINED));
        if (m.status != NO_STATUS) {
            eq(bad, "status", m.status, o.status);
        }
        if (m.replacePending) {
            eq(bad, "pendingPrice", m.pPrice, o.pendingPrice);
            eq(bad, "pendingQuantity", m.pQty, o.pendingQuantity);
            eq(bad, "pendingHoldDelta", m.pDelta, o.pendingHoldDelta);
            eq(bad, "pendingHoldTarget", m.pTarget, o.pendingHoldTarget);
            eq(bad, "oldLegId", m.oldLeg, o.oldLegId);
        } else {
            eq(bad, "oldLegId", 0L, o.oldLegId);
        }
        if (!bad.isEmpty()) {
            throw fail("order " + m.id + " differs from the model: " + bad);
        }
    }

    private static void eq(final List<String> bad, final String field, final long expected, final long actual) {
        if (expected != actual) {
            bad.add(field + " expected " + expected + " got " + actual);
        }
    }

    private static void flag(final List<String> bad, final String name, final boolean expected, final boolean actual) {
        if (expected != actual) {
            bad.add(name + " expected " + expected + " got " + actual);
        }
    }

    /** I15 light form: the last BALANCE per (user, asset) emitted by this op equals the post-apply balance. */
    private void balancesMatchPostApply() {
        final Map<String, AssetsEngineEvent.Balance> last = new LinkedHashMap<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.Balance b) {
                last.put(b.userId() + ":" + b.assetId(), b);
            }
        }
        for (final AssetsEngineEvent.Balance b : last.values()) {
            final var acct = r.engine.account(b.userId());
            if (acct == null || acct.available(b.assetId()) != b.available() || acct.locked(b.assetId()) != b.locked()) {
                throw fail("I15 violated: " + b.render() + " but the post-apply balance is avail="
                        + (acct == null ? "none" : acct.available(b.assetId())) + " locked=" + (acct == null ? "none" : acct.locked(b.assetId())));
            }
        }
    }

    private Map<Long, Long> residuals() {
        final Map<Long, Long> out = new TreeMap<>();
        for (final MOrder o : live.values()) {
            out.put(o.id, o.residual);
        }
        return out;
    }

    String ctx() {
        return "seed " + seed + " op " + op + " [" + opText + "]: ";
    }

    String events() {
        final StringBuilder sb = new StringBuilder("[");
        for (final AssetsEngineEvent e : r.events()) {
            if (sb.length() > 1) {
                sb.append(" | ");
            }
            sb.append(e.render());
        }
        return sb.append(']').toString();
    }

    AssertionError fail(final String message) {
        return new AssertionError(ctx() + message);
    }
}

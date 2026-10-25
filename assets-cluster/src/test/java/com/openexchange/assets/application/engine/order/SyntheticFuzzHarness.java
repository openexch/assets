// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;

/**
 * The state half of the S6 fuzz ({@link SyntheticFuzzDriver} supplies the operations): the rig with market 1
 * opened to collar 0 and high rate limits, three funded users, the quote the generator walks, every MeDispatch by
 * dispatchSeq (for the replay check), the admission prediction and check shared by all creates, the child payload
 * check of contract 3.2, and {@link #verify()}: {@link LifecycleInvariants} (I1-I8, I11, I13) plus the S6 record
 * and book invariants after every operation. Messages carry the seed and the operation index.
 */
abstract class SyntheticFuzzHarness {

    static final int MKT = 1;
    static final int USERS = 3;
    static final long CENT = ONE / 100;
    static final long HALF = ONE / 2;
    static final long SLIP = ONE + 500L * 10_000L;
    static final int ST_PENDING_TRIGGER = 7;
    static final int ST_PENDING_NEW = 6;

    final OrderRig r = OrderRig.open();
    final long seed;
    final Random rng;
    final long[] external = new long[Asset.count()];
    final Map<Long, AssetsEngineEvent.MeDispatch> dispatched = new HashMap<>();
    long bid = 100 * ONE;
    long ask = 101 * ONE;
    long ver = 1;
    long nextLeg = 10_000;
    long corr = 1;
    boolean justSwept;
    int op;
    String opText = "setup";

    SyntheticFuzzHarness(final long seed) {
        this.seed = seed;
        this.rng = new Random(seed);
        final RiskConfigSetCommand cfg = r.riskConfig(MKT, 1);
        cfg.priceCollarBps = 0;
        cfg.maxOrdersPerSec = 1_000;
        cfg.maxOrdersPerMin = 100_000;
        r.setRisk(cfg);
        for (long u = 1; u <= USERS; u++) {
            deposit(u, USD, 1_000_000 * ONE);
            deposit(u, BTC, 1_000 * ONE);
        }
        r.tick(MKT, bid, ask, ver);
    }

    // ---------------------------------------------------------------- creates

    void deposit(final long u, final int asset, final long amount) {
        r.deposit(u, asset, amount);
        external[asset] += amount;
    }

    /** What admission must refuse for a structurally valid create, in chain order (S4: requestId cap, slot, hold), or null. */
    CoreRejectReason predictReject(final long u, final int holdAsset, final long hold) {
        if (r.orders.requests().countFor(u) >= RequestTable.PER_USER_CAP) {
            return CoreRejectReason.TOO_MANY_PENDING_REQUESTS;
        }
        if (r.orders.orders().openCount(u) >= r.orders.market(MKT).maxOpenOrders) {
            return CoreRejectReason.OPEN_ORDER_LIMIT;
        }
        return r.engine.account(u).available(holdAsset) < hold ? CoreRejectReason.INSUFFICIENT_FUNDS : null;
    }

    long nextOrderId() {
        return Math.max(1L, r.engine.getNextOrderId());
    }

    long price() {
        return (95 + rng.nextInt(11)) * ONE + (rng.nextBoolean() ? HALF : 0L);
    }

    /** Apply a create and check the S4/S6 admission outcome; returns the live record when it was admitted. */
    CoreOrder admit(final CreateOrderCommand c, final int holdAsset, final long hold, final OrderStatus status) {
        final long id = nextOrderId();
        final CoreRejectReason expected = predictReject(c.userId, holdAsset, hold);
        r.create(c);
        if (expected != null) {
            final AssetsEngineEvent.OrderReject rej = find(AssetsEngineEvent.OrderReject.class, x -> true, "ORDERREJECT " + expected);
            if (rej.reason() != expected) {
                throw fail("expected reject " + expected + ", got " + rej.render());
            }
            return null;
        }
        final AssetsEngineEvent.OrderAck ack = find(AssetsEngineEvent.OrderAck.class, a -> a.orderId() == id,
                "an ORDERACK " + status + " for order " + id + " (contract 1: a legal create is admitted)");
        if (ack.status() != status || ack.duplicate() || ack.holdAmount() != hold || ack.holdAssetId() != holdAsset) {
            throw fail("ack must be " + status + " holding " + hold + " of asset " + holdAsset + " (contract 1), got " + ack.render());
        }
        final AssetsEngineEvent.OrderEvent ev = orderEvent(id, EventKind.ACCEPTED);
        if (ev.status() != status) {
            throw fail("ACCEPTED event carries status " + status + ", got " + ev.render());
        }
        final CoreOrder o = record(id);
        if (o.holdAmount != hold || r.engine.account(o.userId).holdRemaining(id) != hold) {
            throw fail("hold of order " + id + " must be " + hold + ", got holdAmount " + o.holdAmount);
        }
        return o;
    }

    /** Contract 3.2: parent PENDING_NEW, attempt 1, one CREATE whose payload is a function of the record. */
    void checkChild(final CoreOrder o, final AssetsEngineEvent.MeDispatch d, final long holdRemaining) {
        if (o.status != ST_PENDING_NEW || o.attempt != 1 || !o.has(CoreOrder.F_DISPATCH_PENDING) || o.pendingKind != CoreOrder.PK_CREATE) {
            throw fail("a triggered parent is PENDING_NEW, attempt 1, with its CREATE pending (3.2), got status " + o.status
                    + " attempt " + o.attempt + " flags " + o.stateFlags + " pendingKind " + o.pendingKind);
        }
        final boolean limit = o.orderType == OrderEngine.TYPE_STOP_LIMIT;
        final boolean buy = o.side == OrderEngine.SIDE_BUY;
        final long wantBudget = !limit && buy ? holdRemaining : 0L;
        if (d.kind() != DispatchKind.CREATE || d.commandIdLow() != (1L << 2) || d.quantity() != o.remainingQty
                || d.orderType() != (limit ? OrderEngine.TYPE_LIMIT : OrderEngine.TYPE_MARKET)
                || d.price() != (limit ? o.price : 0L) || d.budget() != wantBudget || (buy && !limit && wantBudget <= 0)) {
            throw fail("child of order " + o.orderId + " must be " + (limit ? "LIMIT at " + o.price : "MARKET") + " qty "
                    + o.remainingQty + " budget " + wantBudget + " attempt 1 (3.2 table), got " + d.render());
        }
    }

    static boolean marketChild(final CoreOrder o) {
        return o.orderType == OrderEngine.TYPE_STOP_LOSS || o.orderType == OrderEngine.TYPE_TRAILING_STOP;
    }

    static long tradeable(final CoreOrder o) {
        return o.orderType == OrderEngine.TYPE_ICEBERG ? Math.min(o.sliceRemainingQty, o.remainingQty) : o.remainingQty;
    }

    // ---------------------------------------------------------------- invariants

    void verify() {
        final LifecycleInvariants.Expectations x = new LifecycleInvariants.Expectations();
        x.external = external.clone();
        x.justSwept = justSwept;
        x.context = ctx();
        LifecycleInvariants.check(r, x);
        final int[] stops = new int[1];
        final int[] trailing = new int[1];
        r.orders.orders().forEachByOrderId(o -> {
            if (o.filledQty + o.remainingQty != o.quantity) {
                throw fail("order " + o.orderId + ": filled " + o.filledQty + " + remaining " + o.remainingQty + " != quantity " + o.quantity);
            }
            if (o.orderType == OrderEngine.TYPE_ICEBERG && o.remainingQty != o.sliceRemainingQty + o.hiddenQty) {
                throw fail("iceberg " + o.orderId + ": remaining " + o.remainingQty + " != slice " + o.sliceRemainingQty
                        + " + hidden " + o.hiddenQty + " (3.4)");
            }
            if (o.status == ST_PENDING_TRIGGER) {
                if (o.has(CoreOrder.F_DISPATCH_PENDING) || o.currentLegId != 0 || o.filledQty != 0) {
                    throw fail("armed order " + o.orderId + " has no dispatch, no leg and no fill (contract 2)");
                }
                if (!o.has(CoreOrder.F_QUARANTINED)) {
                    if (o.orderType == OrderEngine.TYPE_TRAILING_STOP) {
                        trailing[0]++;
                    } else {
                        stops[0]++;
                    }
                }
            } else if (!o.has(CoreOrder.F_DISPATCH_PENDING) && o.currentLegId == 0) {
                throw fail("live non-armed order " + o.orderId + " (status " + o.status + ") has neither DISPATCH_PENDING nor a leg");
            }
        });
        if (r.orders.armedStopCount(MKT) != stops[0] || r.orders.armedTrailingCount(MKT) != trailing[0]) {
            throw fail("books must hold exactly the live non-quarantined PENDING_TRIGGER records (contract 2): stops "
                    + stops[0] + " trailing " + trailing[0] + ", got " + r.orders.armedStopCount(MKT) + " / "
                    + r.orders.armedTrailingCount(MKT));
        }
        for (int m = 2; m <= OrderEngine.MARKETS; m++) {
            if (r.orders.armedStopCount(m) + r.orders.armedTrailingCount(m) != 0) {
                throw fail("nothing is armed on market " + m);
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    String ctx() {
        return "seed=" + seed + " op=" + op + " (" + opText + "): ";
    }

    AssertionError fail(final String detail) {
        return new AssertionError(ctx() + detail);
    }

    CoreOrder record(final long id) {
        final CoreOrder o = r.orders.orders().get(id);
        if (o == null) {
            throw fail("order " + id + " must be live, the store has no record; events " + r.events());
        }
        return o;
    }

    void gone(final long id, final EventKind kind) {
        if (r.orders.orders().get(id) != null) {
            throw fail("order " + id + " must be terminal " + kind + " and removed, still live; events " + r.events());
        }
        orderEvent(id, kind);
    }

    List<CoreOrder> live(final Predicate<CoreOrder> p) {
        final List<CoreOrder> out = new ArrayList<>();
        r.orders.orders().forEachByOrderId(o -> {
            if (p.test(o)) {
                out.add(o);
            }
        });
        return out;
    }

    <T extends AssetsEngineEvent> List<T> all(final Class<T> type) {
        final List<T> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (type.isInstance(e)) {
                out.add(type.cast(e));
            }
        }
        return out;
    }

    long count(final Class<? extends AssetsEngineEvent> type) {
        return all(type).size();
    }

    <T extends AssetsEngineEvent> T find(final Class<T> type, final Predicate<T> p, final String what) {
        for (final T e : all(type)) {
            if (p.test(e)) {
                return e;
            }
        }
        throw fail("expected " + what + ", got " + r.events());
    }

    AssetsEngineEvent.OrderEvent orderEvent(final long id, final EventKind kind) {
        return find(AssetsEngineEvent.OrderEvent.class, e -> e.orderId() == id && e.kind() == kind, "OrderEvent " + kind + " for order " + id);
    }

    AssetsEngineEvent.MeDispatch dispatchFor(final long id) {
        return find(AssetsEngineEvent.MeDispatch.class, d -> d.omsOrderId() == id, "a MeDispatch for order " + id);
    }
}

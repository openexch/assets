// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.application.engine.gate.OrderEntryGate;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.determinism.RecordingCoreSink;
import com.openexchange.core.domain.Account;
import com.openexchange.core.domain.Asset;
import com.openexchange.core.domain.DispatchKind;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.ReleaseScope;
import com.openexchange.core.domain.commands.CancelOrderCommand;
import com.openexchange.core.domain.commands.CreateOrderCommand;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Test support shared by the G1-G10 classes: release reads, the invariant set, a snapshot round trip. */
public final class ReleaseTestSupport {

    public static final long ONE = V3Rig.ONE;
    public static final int CREATE = V3Rig.KIND_CREATE;
    public static final int CANCEL = V3Rig.KIND_CANCEL;
    public static final int AMEND = V3Rig.KIND_AMEND;
    public static final int APPLIED = 0, REJECTED = 1, CONFLICT = 2, CAPACITY = 3, UNKNOWN_LEG = 4, NOT_OWNER = 5,
            UNKNOWN_MARKET = 6, RELEASED = 7, INVALID = 8;
    public static final int R_APPLIED = 0, R_NOOP = 1, R_LEG_LIVE = 2;
    public static final int UP_TO = 0, ORDER = 1;
    private static long cancelRequest = 90_000;

    private ReleaseTestSupport() {
    }

    /** MeDispatch events of {@code kind} since the rig's mark. */
    public static List<CoreEngineEvent.MeDispatch> dispatches(final V3Rig r, final DispatchKind kind) {
        final List<CoreEngineEvent.MeDispatch> out = new ArrayList<>();
        for (final CoreEngineEvent.MeDispatch d : r.all(CoreEngineEvent.MeDispatch.class)) {
            if (d.kind() == kind) {
                out.add(d);
            }
        }
        return out;
    }

    public static List<CoreEngineEvent.MeDispatch> releases(final V3Rig r) {
        return dispatches(r, DispatchKind.RELEASE_ID);
    }

    /** The single RELEASE_ID minted since the mark, asserted to name {@code h}, {@code floor} and {@code scope}. */
    public static CoreEngineEvent.MeDispatch oneRelease(final V3Rig r, final String what, final long h, final long floor,
                                                          final ReleaseScope scope) {
        final List<CoreEngineEvent.MeDispatch> rel = releases(r);
        assertEquals(what + ": exactly one release, got " + rel, 1, rel.size());
        final CoreEngineEvent.MeDispatch d = rel.get(0);
        assertEquals(what + ": idHigh = the order", h, d.commandIdHigh());
        assertEquals(what + ": idLowFloor", floor, d.commandIdLow());
        assertEquals(what + ": scope", scope, d.releaseScope());
        assertEquals(what + ": every other payload field 0", 0L, d.userId() + d.omsOrderId() + d.oldOrderId() + d.marketId()
                + d.orderType() + d.side() + d.price() + d.quantity() + d.budget() + d.timeInForce() + d.expectedOldRemaining());
        final ReleaseBacklog.Entry b = r.engine.orders().backlog().get(h);
        assertNotNull(what + ": backlog entry", b);
        assertEquals(what + ": backlog floor", floor, b.floor);
        assertEquals(what + ": backlog scope", scope == ReleaseScope.ORDER ? ORDER : UP_TO, b.scope);
        assertEquals(what + ": backlog dispatchSeq", d.dispatchSeq(), b.dispatchSeq);
        return d;
    }

    public static void noRelease(final V3Rig r, final String what) {
        assertEquals(what + ": no release minted, got " + releases(r), 0, releases(r).size());
    }

    public static ReleaseBacklog.Entry entry(final V3Rig r, final long h) {
        return r.engine.orders().backlog().get(h);
    }

    public static void cancel(final V3Rig r, final long user, final long order) {
        final CancelOrderCommand c = new CancelOrderCommand();
        c.reset();
        c.correlationId = 5_100;
        c.userId = user;
        c.orderId = order;
        c.requestIdHigh = 0xCA;
        c.requestIdLow = cancelRequest++;
        r.engine.orders().applyCancel(c, r.now);
    }

    public static CreateOrderCommand tif(final CreateOrderCommand c, final int tif) {
        c.timeInForce = tif;
        return c;
    }

    /** A rig with buyer 1 (USD) and seller 2 (BTC) funded. */
    public static V3Rig funded() {
        final V3Rig r = V3Rig.open();
        r.deposit(1, V3Rig.USD, 1_000_000 * ONE);
        r.deposit(2, V3Rig.BTC, 1_000 * ONE);
        r.deposit(3, V3Rig.USD, 1_000_000 * ONE);
        r.deposit(3, V3Rig.BTC, 1_000 * ONE);
        return r;
    }

    /** Buy 1 BTC at 100 for user 1, acked at {@code leg}; returns the orderId. */
    public static long restingBuy(final V3Rig r, final long leg) {
        final long h = r.create(r.limit(1, 0, 100 * ONE, ONE));
        r.ack(h, 0, leg);
        return h;
    }

    public static boolean globalHas(final V3Rig r, final GateReason g) {
        return r.globalHas(g);
    }

    public static boolean marketHas(final V3Rig r, final int market, final GateReason g) {
        return (r.engine.gate().reasons(market) & (1 << g.code())) != 0;
    }

    /**
     * The invariant set after any apply (7 G10): locked == sum(holds) and no negative balance on every account, every
     * hold with orderId >= 1 the hold of a live core order of its account, 5.1 a-d, 5.2 rules 1-7, no live order with
     * remainingQty 0, terminalWithPendingCreateCount 0.
     */
    public static void checkInvariants(final V3Rig r, final String what) {
        final CoreEngine e = r.engine;
        final OrderEngine oe = e.orders();
        final List<Account> accounts = new ArrayList<>();
        e.forEachAccountByUserId(accounts::add);
        for (final Account a : accounts) {
            final long[] sum = new long[Asset.count()];
            a.forEachHold((orderId, assetId, remaining, oms) -> {
                sum[assetId] += remaining;
                if (orderId >= 1) {
                    final CoreOrder o = oe.orders().get(orderId);
                    if (o == null || o.userId != a.userId()) {
                        fail(what + ": user " + a.userId() + " holds " + remaining + " under core id " + orderId
                                + " that is not a live order of that user");
                    }
                }
            });
            for (int asset = 0; asset < Asset.count(); asset++) {
                if (a.available(asset) < 0 || a.locked(asset) < 0) {
                    fail(what + ": negative balance user " + a.userId() + " asset " + asset);
                }
                assertEquals(what + ": locked == sum(holds) user " + a.userId() + " asset " + asset, sum[asset], a.locked(asset));
            }
        }
        assertNull(what + ": 5.1", oe.orderRestoreBreach(e.behaviorV3()));
        if (e.behaviorV3()) {
            assertNull(what + ": 5.2", oe.meLinkRestoreBreach());
        }
        oe.orders().forEachByOrderId(o -> {
            if (o.remainingQty == 0) {
                fail(what + ": live order " + o.orderId + " with remainingQty 0");
            }
        });
        assertEquals(what + ": R1 assertion", 0L, oe.terminalWithPendingCreateCount());
        assertEquals(what + ": index live == trading pending + RELEASE/ORPHAN backlog", expectedIndexLive(oe),
                oe.dispatchIndex().live());
    }

    private static long expectedIndexLive(final OrderEngine oe) {
        final long[] n = {0};
        oe.orders().forEachByOrderId(o -> {
            if (o.has(CoreOrder.F_DISPATCH_PENDING)) {
                n[0]++;
            }
        });
        oe.backlog().forEachByOrderId(b -> {
            if (b.stage != ReleaseBacklog.STAGE_PARKED) {
                n[0]++;
            }
        });
        return n[0];
    }

    public static byte[] image(final CoreEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf, 0);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    /** Restore {@code image} into a fresh widened engine and run the restore invariants. */
    public static CoreEngine restore(final byte[] image) {
        final CoreEngine e = new CoreEngine();
        e.coreConfig().setSupportedBehaviorVersions(1, 2, 3);
        e.setEventSink(new RecordingCoreSink());
        BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, e);
        e.verifyRestoredInvariants();
        return e;
    }

    /** A snapshot round trip: the restored engine re-serializes to the same bytes. */
    public static CoreEngine roundTrip(final CoreEngine e, final String what) {
        final byte[] img = image(e);
        final CoreEngine restored = restore(img);
        assertArrayEquals(what + ": restore is invisible", img, image(restored));
        return restored;
    }

    public static int global() {
        return OrderEntryGate.GLOBAL;
    }
}

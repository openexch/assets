// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.openexchange.assets.application.engine.order.AmendSupport.APPLIED;
import static com.openexchange.assets.application.engine.order.AmendSupport.KIND_AMEND;
import static com.openexchange.assets.application.engine.order.AmendSupport.LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.ME_REJECTED;
import static com.openexchange.assets.application.engine.order.AmendSupport.NEW_LEG;
import static com.openexchange.assets.application.engine.order.AmendSupport.PRICE;
import static com.openexchange.assets.application.engine.order.AmendSupport.REPLACE_WINDOW_MS;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 gap tests around AmendOrder (contract 3.4, 3.6, 3.8 #6, section 1 / C16) that the G3 and G2 suites
 * do not pin: the LIMIT_MAKER type, the size/notional vocabulary on the NEW leg, the breaker, the per-user
 * request cap, a stored amend reject replayed, the exact BALANCE lines of a top-up and an abort, the
 * RequestPendingDispatch replay of an AMEND, the 1024/4096 paging, and an open replace window across the codec.
 */
public class AmendEdgeCasesTest {

    private static void assertUntouched(final AmendSupport s, final String what) {
        final CoreOrder o = s.order(1);
        assertNotNull(what + ": order 1 stays live", o);
        assertFalse(what + ": no window", o.has(CoreOrder.F_REPLACE_PENDING));
        assertFalse(what + ": nothing in flight", o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(what + ": attempt unchanged", 0, o.attempt);
        assertEquals(what + ": no pending delta", 0L, o.pendingHoldDelta);
        assertEquals(what + ": no pending quantity", 0L, o.pendingQuantity);
        assertEquals(what + ": price unchanged", PRICE, o.price);
        assertEquals(what + ": quantity unchanged", ONE, o.quantity);
        assertEquals(what + ": status unchanged", OrderStatus.NEW.code(), o.status);
        assertEquals(what + ": hold unchanged", 100 * ONE, s.acct(1).holdRemaining(1));
        assertEquals(what + ": locked unchanged", 100 * ONE, s.acct(1).locked(USD));
        assertEquals(what + ": available unchanged", 900 * ONE, s.acct(1).available(USD));
        assertEquals(what + ": no dispatch", 1, s.r.engine.getDispatchSeq());
        assertEquals(what + ": no replace deadline", 0, s.replaceDeadlines());
    }

    private static byte[] bytes(final AssetsEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    private static AssetsEngine restore(final byte[] image) {
        final AssetsEngine e = new AssetsEngine();
        e.setEventSink(new RecordingAssetsSink());
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, e);
        assertEquals(image.length, d.bytesConsumed);
        e.verifyRestoredInvariants();
        return e;
    }

    /** The KIND_REPLACE entries of the deadline index as "deadline:id". */
    private static List<String> replaceEntries(final AssetsEngine e) {
        final List<String> out = new ArrayList<>();
        e.deadlines().forEach((d, id, k) -> {
            if (k == DeadlineIndex.KIND_REPLACE) {
                out.add(d + ":" + id);
            }
        });
        return out;
    }

    private static List<String> renders(final List<AssetsEngineEvent> events) {
        final List<String> out = new ArrayList<>(events.size());
        for (final AssetsEngineEvent e : events) {
            out.add(e.render());
        }
        return out;
    }

    private static List<AssetsEngineEvent.MeDispatchReply> replies(final OrderRig r) {
        final List<AssetsEngineEvent.MeDispatchReply> out = new ArrayList<>();
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatchReply d) {
                out.add(d);
            }
        }
        return out;
    }

    /** Every MEDISPATCH frame minted on the rig, rendered as the DISPATCHREPLY it must come back as. */
    private static Map<Long, String> mintedFrames(final OrderRig r) {
        final Map<Long, String> out = new HashMap<>();
        for (final AssetsEngineEvent e : r.sink.events()) {
            if (e instanceof AssetsEngineEvent.MeDispatch d) {
                out.put(d.dispatchSeq(), d.render().replace("MEDISPATCH", "DISPATCHREPLY"));
            }
        }
        return out;
    }

    /** The replies of one RequestPendingDispatch must be seqs {@code from..to} in order, frame for frame. */
    private static void assertPage(final OrderRig r, final Map<Long, String> minted, final long from, final long to, final long corr) {
        final List<AssetsEngineEvent.MeDispatchReply> p = replies(r);
        final int n = (int) (to - from + 1);
        assertEquals("page size for seqs " + from + ".." + to, n, p.size());
        for (int i = 0; i < n; i++) {
            assertEquals("ascending pendingDispatchSeq", from + i, p.get(i).dispatchSeq());
            assertEquals("the reply is the original frame", minted.get(from + i), p.get(i).render());
        }
        final AssetsEngineEvent.PendingDispatchEnd end = r.first(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals(corr, end.correlationId());
        assertEquals("PENDINGEND n = the page size", n, end.count());
        assertEquals("head = the engine's dispatchSeq", r.engine.getDispatchSeq(), end.headDispatchSeq());
        assertEquals("replies + one end frame, nothing else", n + 1, r.events().size());
    }

    // contract 3.4 NOT_OPEN "type not LIMIT/LIMIT_MAKER": a LIMIT_MAKER (orderType 2) order can be amended
    @Test
    public void limitMakerOrderCanBeAmended() {
        final OrderRig r = OrderRig.open();
        final AmendSupport s = new AmendSupport(r);
        r.deposit(1, USD, 1_000 * ONE);
        final CreateOrderCommand c = r.limit(1, 1, 0, PRICE, ONE);
        c.orderType = OrderEngine.TYPE_LIMIT_MAKER;
        r.markNow();
        r.create(c);
        assertEquals("precondition: a LIMIT_MAKER create is accepted", EventKind.ACCEPTED, r.first(AssetsEngineEvent.OrderEvent.class).kind());
        s.createOutcome(1, 1, LEG);
        s.assertLeg(1, LEG);
        assertEquals(OrderEngine.TYPE_LIMIT_MAKER, s.order(1).orderType);
        r.markNow();
        s.amend(1, 1, 110 * ONE, 2 * ONE); // delta = 110 x 2 - 100 x 1 = +120
        assertEquals("a LIMIT_MAKER order is amendable: no reject", 0, r.count(AssetsEngineEvent.OrderReject.class));
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertEquals(OrderStatus.PENDING_REPLACE, ack.status());
        assertFalse(ack.duplicate());
        final CoreOrder o = s.order(1);
        assertTrue(o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(OrderStatus.PENDING_REPLACE.code(), o.status);
        assertEquals(110 * ONE, o.pendingPrice);
        assertEquals(2 * ONE, o.pendingQuantity);
        assertEquals(120 * ONE, o.pendingHoldDelta);
        assertEquals(220 * ONE, s.acct(1).holdRemaining(1));
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.AMEND, d.kind());
        assertEquals("the AMEND frame carries the record's type", OrderEngine.TYPE_LIMIT_MAKER, d.orderType());
        assertEquals(LEG, d.oldOrderId());
        assertEquals(110 * ONE, d.price());
        assertEquals(2 * ONE, d.quantity());
        assertEquals(EventKind.DISPATCHED, r.first(AssetsEngineEvent.OrderEvent.class).kind());
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        assertEquals("resolves like a LIMIT", 110 * ONE, s.order(1).price);
        assertEquals(2 * ONE, s.order(1).quantity);
        assertEquals("the type survives the replace", OrderEngine.TYPE_LIMIT_MAKER, s.order(1).orderType);
    }

    // contract 3.4 "size/notional on (newQty, newPrice x legQty)": the four rejects, nothing mutated, decided after the rate check
    @Test
    public void sizeAndNotionalChecksApplyToTheNewLegValuesAndMutateNothing() {
        final AmendSupport s = AmendSupport.base(); // BUY 100 x 1, hold 100
        final OrderRig r = s.r;
        final RiskConfigSetCommand cfg = r.riskConfig(1, 1);
        cfg.minQuantity = 50_000_000L; // 0.5
        cfg.maxQuantity = 3 * ONE;
        cfg.minNotional = 60 * ONE;
        cfg.maxNotional = 250 * ONE;
        r.setRisk(cfg);
        assertEquals("precondition: config applied", 50_000_000L, r.orders.market(1).minQuantity);
        final int secBefore = r.orders.user(1).secCount;
        final int stored = r.orders.requests().countFor(1);
        r.markNow();
        s.amend(1, 1, 0, 40_000_000L); // newQty 0.4 < 0.5
        assertEquals(CoreRejectReason.SIZE_TOO_SMALL, s.rejectOnly("newQty 0.4 under minQuantity 0.5"));
        assertEquals("detail = minQuantity", 50_000_000L, r.first(AssetsEngineEvent.OrderReject.class).detail());
        r.markNow();
        s.amend(1, 1, 0, 4 * ONE); // newQty 4 > 3
        assertEquals(CoreRejectReason.SIZE_TOO_LARGE, s.rejectOnly("newQty 4 over maxQuantity 3"));
        assertEquals("detail = maxQuantity", 3 * ONE, r.first(AssetsEngineEvent.OrderReject.class).detail());
        r.markNow();
        s.amend(1, 1, 0, 50_000_000L); // leg notional 100 x 0.5 = 50 < 60
        assertEquals(CoreRejectReason.NOTIONAL_TOO_SMALL, s.rejectOnly("leg notional 50 under minNotional 60"));
        assertEquals("detail = the leg notional", 50 * ONE, r.first(AssetsEngineEvent.OrderReject.class).detail());
        r.markNow();
        s.amend(1, 1, 0, 3 * ONE); // leg notional 100 x 3 = 300 > 250
        assertEquals(CoreRejectReason.NOTIONAL_TOO_LARGE, s.rejectOnly("leg notional 300 over maxNotional 250"));
        assertEquals("detail = the leg notional", 300 * ONE, r.first(AssetsEngineEvent.OrderReject.class).detail());
        assertUntouched(s, "size/notional rejects");
        assertEquals("decided after the rate check: four units consumed", secBefore + 4, r.orders.user(1).secCount);
        assertEquals("stored from the rate check on: four decisions remembered", stored + 4, r.orders.requests().countFor(1));
        // the notional is the NEW LEG's (newPrice x (newQty - filled)), not the total's: fill 0.4, then raise the floor
        s.sellSide();
        s.fillBase(40_000_000L);
        assertEquals("precondition (3.1): filled 0.4", 40_000_000L, s.order(1).filledQty);
        final RiskConfigSetCommand cfg2 = r.riskConfig(1, 2);
        cfg2.minQuantity = 50_000_000L;
        cfg2.maxQuantity = 3 * ONE;
        cfg2.minNotional = 70 * ONE; // total 100 x 1 = 100 passes, leg 100 x 0.6 = 60 does not
        cfg2.maxNotional = 250 * ONE;
        r.setRisk(cfg2);
        r.markNow();
        s.amend(1, 1, PRICE, 0); // keep the total quantity 1: legQty 0.6
        assertEquals(CoreRejectReason.NOTIONAL_TOO_SMALL, s.rejectOnly("leg notional 60 under minNotional 70 (total would pass)"));
        assertEquals("detail = 100 x 0.6, the leg", 60 * ONE, r.first(AssetsEngineEvent.OrderReject.class).detail());
        final RiskConfigSetCommand cfg3 = r.riskConfig(1, 3);
        cfg3.minQuantity = 50_000_000L;
        cfg3.maxQuantity = 3 * ONE;
        cfg3.minNotional = 60 * ONE; // inclusive
        cfg3.maxNotional = 250 * ONE;
        r.setRisk(cfg3);
        r.markNow();
        s.amend(1, 1, PRICE, 0);
        assertEquals("leg notional 60 at minNotional 60 is admitted (3.8 #15: same values are accepted)", 1,
                r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals(0, r.count(AssetsEngineEvent.OrderReject.class));
        assertEquals("same values: no hold delta", 0L, s.order(1).pendingHoldDelta);
    }

    // contract 3.4 CIRCUIT_BREAKER: the amend is refused (after the rate check, stored), the cancel still goes through
    @Test
    public void circuitBreakerRejectsAmendWhileCancelStillPasses() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.markNow();
        r.orders.applyCircuitBreaker(0x77, 1, true, r.now);
        assertEquals(AdminResult.ACCEPTED, r.first(AssetsEngineEvent.AdminAck.class).result());
        assertTrue("precondition: market 1 tripped", r.orders.market(1).breakerTripped);
        final int secBefore = r.orders.user(1).secCount;
        r.markNow();
        final AmendOrderCommand c = s.amend(1, 1, PRICE, 2 * ONE);
        assertEquals(CoreRejectReason.CIRCUIT_BREAKER, s.rejectOnly("amend on a tripped market"));
        assertEquals("detail = marketId", 1, r.first(AssetsEngineEvent.OrderReject.class).detail());
        assertUntouched(s, "breaker");
        assertEquals("the breaker is decided after the rate check: one unit consumed", secBefore + 1, r.orders.user(1).secCount);
        final RequestTable.Entry e = r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow);
        assertNotNull("the decision is stored (3.4: stored from the rate check on)", e);
        assertEquals(CoreRejectReason.CIRCUIT_BREAKER.code(), e.reason);
        assertEquals(RequestTable.KIND_AMEND, e.kind);
        r.markNow();
        r.cancel(1, 1);
        assertEquals("the cancel is not gated by the breaker", 0, r.count(AssetsEngineEvent.OrderReject.class));
        assertEquals(OrderStatus.PENDING_CANCEL, r.first(AssetsEngineEvent.OrderAck.class).status());
        final AssetsEngineEvent.MeDispatch d = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals(DispatchKind.CANCEL, d.kind());
        assertEquals(LEG, d.oldOrderId());
        assertEquals(EventKind.DISPATCHED, r.first(AssetsEngineEvent.OrderEvent.class).kind());
        assertTrue(s.order(1).has(CoreOrder.F_CANCEL_PENDING));
        assertEquals(2, r.engine.getDispatchSeq());
        assertEquals("a cancel is free of rate budget", secBefore + 1, r.orders.user(1).secCount);
    }

    // contract 3.4 "per-user cap": 256 stored decisions, then a fresh requestId is TOO_MANY_PENDING_REQUESTS (before UNKNOWN_ORDER)
    @Test
    public void perUserRequestCapRejectsTheNextAmendWithAFreshRequestId() {
        final OrderRig r = OrderRig.open();
        final RiskConfigSetCommand cfg = r.riskConfig(1, 1);
        cfg.maxOrdersPerSec = 1_000; // the rate check must never be what fills the table
        cfg.maxOrdersPerMin = 1_000;
        cfg.minNotional = 50 * ONE;
        r.setRisk(cfg);
        final AmendSupport s = new AmendSupport(r).buyBase(true); // decision 1 of 256: the accepted create
        assertEquals(1, r.orders.requests().countFor(1));
        for (int i = 1; i < RequestTable.PER_USER_CAP; i++) {
            r.markNow();
            r.create(r.limit(1, 1, 0, ONE, ONE)); // 1 x 1 = notional 1 < 50: refused after the rate check, so stored
            assertEquals("create " + i + " is a stored NOTIONAL_TOO_SMALL", CoreRejectReason.NOTIONAL_TOO_SMALL, r.reject().reason());
        }
        assertEquals("the table is full", RequestTable.PER_USER_CAP, r.orders.requests().countFor(1));
        assertEquals("the rejects created no order", 1, r.orders.orders().size());
        final int secBefore = r.orders.user(1).secCount;
        r.markNow();
        final AmendOrderCommand c = s.amend(1, 1, PRICE, 2 * ONE); // fresh requestId
        assertEquals(CoreRejectReason.TOO_MANY_PENDING_REQUESTS, s.rejectOnly("amend over a full request table"));
        assertEquals("detail = the user's count", RequestTable.PER_USER_CAP, r.first(AssetsEngineEvent.OrderReject.class).detail());
        assertUntouched(s, "per-user cap");
        assertNull("not stored: the cap reject is decided before the rate check", r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow));
        assertEquals(RequestTable.PER_USER_CAP, r.orders.requests().countFor(1));
        assertEquals("no rate unit consumed", secBefore, r.orders.user(1).secCount);
        r.markNow();
        s.amend(1, 42, PRICE, 2 * ONE);
        assertEquals("the cap is decided before UNKNOWN_ORDER", CoreRejectReason.TOO_MANY_PENDING_REQUESTS,
                s.rejectOnly("amend of an unknown order over a full table"));
    }

    // contract 3.4 requestId replay of a stored reject: the stored reason comes back, nothing is re-evaluated
    @Test
    public void storedAmendRejectReplayedWithTheSameRequestIdReturnsTheStoredReasonWithoutReEvaluating() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.orders.market(1).lastTradePrice = PRICE; // default collar 2000 bps: [80, 120]
        r.markNow();
        final AmendOrderCommand c = s.amend(1, 1, 121 * ONE, 0);
        assertEquals(CoreRejectReason.PRICE_COLLAR, s.rejectOnly("121 against last trade 100"));
        final RequestTable.Entry e = r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow);
        assertNotNull("the collar reject is stored", e);
        assertEquals(CoreRejectReason.PRICE_COLLAR.code(), e.reason);
        assertEquals(RequestTable.KIND_AMEND, e.kind);
        assertEquals(1, e.orderId);
        r.orders.market(1).lastTradePrice = 121 * ONE; // a fresh evaluation of the same payload would now pass
        final int secBefore = r.orders.user(1).secCount;
        final long eventSeq = r.engine.getEventSeq();
        r.markNow();
        s.amend(c); // byte-equal retry
        assertEquals("the stored decision stands", CoreRejectReason.PRICE_COLLAR, s.rejectOnly("replay of the stored reject"));
        assertUntouched(s, "replayed reject");
        assertEquals("a replay consumes no rate unit", secBefore, r.orders.user(1).secCount);
        assertEquals("a replay emits no OrderEvent", eventSeq, r.engine.getEventSeq());
        assertEquals("still one stored decision for that requestId", e, r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow));
        r.markNow();
        s.amend(1, 1, 121 * ONE, 0); // the same payload under a NEW requestId is evaluated afresh and passes
        assertEquals("proof the replay did not re-evaluate: a fresh request with the same payload is accepted", 1,
                r.count(AssetsEngineEvent.OrderAck.class));
        assertEquals(0, r.count(AssetsEngineEvent.OrderReject.class));
        assertEquals(121 * ONE, s.order(1).pendingPrice);
    }

    // contract 3.8 #6: one BALANCE line for the top-up inside the amend apply, one for the abort release; none for a negative delta
    @Test
    public void amendTopUpEmitsOneBalanceLineAndTheAbortReleaseEmitsOne() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        r.markNow();
        s.amend(1, 1, PRICE, 2 * ONE); // delta +100
        assertEquals("exactly one BALANCE in the amend apply", 1, r.count(AssetsEngineEvent.Balance.class));
        AssetsEngineEvent.Balance b = r.first(AssetsEngineEvent.Balance.class);
        assertEquals(1, b.userId());
        assertEquals(USD, b.assetId());
        assertEquals(800 * ONE, b.available());
        assertEquals(200 * ONE, b.locked());
        assertTrue("like the create hold, the balance line leads the apply, got " + r.events(), r.events().get(0) instanceof AssetsEngineEvent.Balance);
        assertEquals("BALANCE, MEDISPATCH, ORDERACK, ORDEREVENT and nothing else, got " + r.events(), 4, r.events().size());
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, ME_REJECTED, 0L, LEG);
        assertEquals("exactly one BALANCE for the abort release", 1, r.count(AssetsEngineEvent.Balance.class));
        b = r.first(AssetsEngineEvent.Balance.class);
        assertEquals(900 * ONE, b.available());
        assertEquals(100 * ONE, b.locked());
        assertEquals("the release and REPLACED(ME_REJECTED), nothing else, got " + r.events(), 2, r.events().size());
        assertEquals(CoreRejectReason.ME_REJECTED, r.first(AssetsEngineEvent.OrderEvent.class).reason());
        // a negative delta moves no money in the apply; the release is the one BALANCE at resolution (3.3)
        r.markNow();
        s.amend(1, 1, 0, 50_000_000L); // delta = 100 x 0.5 - 100 x 1 = -50
        assertEquals("no BALANCE for a negative delta in the apply", 0, r.count(AssetsEngineEvent.Balance.class));
        assertEquals(-50 * ONE, s.order(1).pendingHoldDelta);
        r.markNow();
        s.outcome(1, 1, 2, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        assertEquals("one BALANCE at resolution", 1, r.count(AssetsEngineEvent.Balance.class));
        b = r.first(AssetsEngineEvent.Balance.class);
        assertEquals(950 * ONE, b.available());
        assertEquals(50 * ONE, b.locked());
    }

    // contract 3.6 RequestPendingDispatch, amend row: original dispatchSeq, kind AMEND, oldOrderId = old leg, pendingPrice, legQty, budget 0
    @Test
    public void pendingDispatchReplaysAPendingAmendWithItsOriginalFrame() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.sellSide();
        s.fillBase(40_000_000L); // filled 0.4 before the amend
        assertEquals("precondition (3.1): filled 0.4", 40_000_000L, s.order(1).filledQty);
        r.markNow();
        s.amend(1, 1, 110 * ONE, 2 * ONE); // legQty = 2 - 0.4 = 1.6
        final AssetsEngineEvent.MeDispatch minted = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals("precondition: the AMEND is dispatch 3", 3, minted.dispatchSeq());
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xD7, 0L, 0);
        assertEquals("one DISPATCHREPLY + PENDINGEND, got " + r.events(), 2, r.events().size());
        final AssetsEngineEvent.MeDispatchReply d = r.first(AssetsEngineEvent.MeDispatchReply.class);
        assertEquals("original dispatchSeq", 3, d.dispatchSeq());
        assertEquals(DispatchKind.AMEND, d.kind());
        assertEquals(1, d.commandIdHigh());
        assertEquals("attempt 1, kind AMEND", (1L << 2) | 2, d.commandIdLow());
        assertEquals("oldOrderId = the old leg", LEG, d.oldOrderId());
        assertEquals("price = pendingPrice", 110 * ONE, d.price());
        assertEquals("quantity = pendingQuantity - filledQty", 160_000_000L, d.quantity());
        assertEquals(0L, d.budget());
        assertEquals(1, d.userId());
        assertEquals(1, d.omsOrderId());
        assertEquals(1, d.marketId());
        assertEquals(OrderEngine.TYPE_LIMIT, d.orderType());
        assertEquals(OrderEngine.SIDE_BUY, d.side());
        assertEquals(OrderEngine.TIF_GTC, d.timeInForce());
        assertEquals("the reply is the minted frame, field for field", minted.render().replace("MEDISPATCH", "DISPATCHREPLY"), d.render());
        final AssetsEngineEvent.PendingDispatchEnd end = r.first(AssetsEngineEvent.PendingDispatchEnd.class);
        assertEquals(0xD7, end.correlationId());
        assertEquals(1, end.count());
        assertEquals(3, end.headDispatchSeq());
    }

    // contract 3.6 "amend: pendingPrice/legQty": a fill INSIDE the window moves filledQty, and the replayed leg follows it
    @Test
    public void pendingDispatchReplayOfAnAmendRecomputesTheLegFromFillsInsideTheWindow() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        s.sellSide();
        r.markNow();
        s.amend(1, 1, 110 * ONE, 2 * ONE); // legQty 2.0 at mint, hold 220
        final AssetsEngineEvent.MeDispatch minted = r.first(AssetsEngineEvent.MeDispatch.class);
        assertEquals("precondition: minted with the leg quantity 2.0", 2 * ONE, minted.quantity());
        s.fillBase(50_000_000L); // 0.5 on the OLD leg while the window is open
        assertEquals("precondition (3.8 #7): filled moves inside the window", 50_000_000L, s.order(1).filledQty);
        assertTrue(s.order(1).has(CoreOrder.F_REPLACE_PENDING));
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xD8, 0L, 0);
        final AssetsEngineEvent.MeDispatchReply d = r.first(AssetsEngineEvent.MeDispatchReply.class);
        assertEquals(3, d.dispatchSeq());
        assertEquals((1L << 2) | 2, d.commandIdLow());
        assertEquals(110 * ONE, d.price());
        assertEquals("legQty = pendingQuantity - filledQty at replay time (2 - 0.5), not the 2.0 minted", 150_000_000L, d.quantity());
        assertEquals(1, r.first(AssetsEngineEvent.PendingDispatchEnd.class).count());
    }

    // contract 3.6 "maxEntries 0 -> 1024, cap 4096": pages, the cap, and continuation from the 1024th seq
    @Test
    public void pendingDispatchPagesAt1024ByDefaultCapsAt4096AndContinuesFromTheCursor() {
        final OrderRig r = OrderRig.open();
        final RiskConfigSetCommand cfg = r.riskConfig(1, 1);
        cfg.maxOrdersPerSec = 1_000;
        cfg.maxOrdersPerMin = 1_000;
        cfg.maxOpenOrders = 1_000;
        r.setRisk(cfg);
        final int users = 17, perUser = 250; // 4250 pending creates, under the 256 per-user request cap
        for (long u = 1; u <= users; u++) {
            r.deposit(u, USD, 100_000 * ONE);
            for (int i = 0; i < perUser; i++) {
                r.create(r.limit(u, 1, 0, 100 * ONE, ONE));
            }
        }
        final long total = (long) users * perUser;
        assertEquals("precondition: every create was accepted and minted", 0, r.orders.rejectedCount());
        assertEquals(total, r.engine.getDispatchSeq());
        assertEquals(total, r.orders.orders().size());
        final Map<Long, String> minted = mintedFrames(r);
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xE0, 0L, 0);
        assertPage(r, minted, 1, 1_024, 0xE0); // maxEntries 0 = the default page
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xE1, 0L, 5_000);
        assertPage(r, minted, 1, 4_096, 0xE1); // min(4096, pending) = the cap
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xE2, 1_024L, 0);
        assertPage(r, minted, 1_025, 2_048, 0xE2); // continuation, default page
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xE3, 1_024L, 5_000);
        assertPage(r, minted, 1_025, total, 0xE3); // the rest: min(4096, 3226) = 3226
        r.markNow();
        r.orders.applyRequestPendingDispatch(0xE4, total, 5_000);
        assertEquals("a cursor at the head: an empty page, still an end frame", 0, replies(r).size());
        assertEquals(0, r.first(AssetsEngineEvent.PendingDispatchEnd.class).count());
        assertEquals(total, r.first(AssetsEngineEvent.PendingDispatchEnd.class).headDispatchSeq());
        assertEquals("a query mutates nothing", total, r.engine.getDispatchSeq());
        assertEquals(total, r.orders.orders().size());
    }

    // contract section 1 (replaceDeadlineMs derives the REPLACE entry) / lifecycle C16: an open window survives the codec
    @Test
    public void openReplaceWindowSurvivesACodecRoundTrip() {
        final AmendSupport s = AmendSupport.base();
        final OrderRig r = s.r;
        final long t0 = r.now;
        s.amend(1, 1, PRICE, 2 * ONE);
        final CoreOrder a = s.order(1);
        assertEquals("precondition: window open with its deadline", t0 + REPLACE_WINDOW_MS, a.replaceDeadlineMs);
        final byte[] image = bytes(r.engine);

        final AssetsEngine b = restore(image);
        final OrderEngine ob = b.orders();
        final CoreOrder o = ob.orders().get(1);
        assertNotNull("order 1 restored", o);
        assertEquals("replaceDeadlineMs", a.replaceDeadlineMs, o.replaceDeadlineMs);
        assertEquals("pendingHoldDelta", a.pendingHoldDelta, o.pendingHoldDelta);
        assertEquals("pendingHoldTarget", a.pendingHoldTarget, o.pendingHoldTarget);
        assertEquals("oldLegId", a.oldLegId, o.oldLegId);
        assertEquals("pendingPrice", a.pendingPrice, o.pendingPrice);
        assertEquals("pendingQuantity", a.pendingQuantity, o.pendingQuantity);
        assertEquals("pendingDispatchSeq", a.pendingDispatchSeq, o.pendingDispatchSeq);
        assertEquals(a.attempt, o.attempt);
        assertEquals(CoreOrder.PK_AMEND, o.pendingKind);
        assertTrue(o.has(CoreOrder.F_REPLACE_PENDING));
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(OrderStatus.PENDING_REPLACE.code(), o.status);
        assertEquals(LEG, o.currentLegId);
        assertEquals("one KIND_REPLACE entry rebuilt at the deadline", List.of((t0 + REPLACE_WINDOW_MS) + ":1"), replaceEntries(b));
        assertEquals(replaceEntries(r.engine), replaceEntries(b));
        final Account ab = b.account(1);
        assertEquals("the top-up rides the hold", 200 * ONE, ab.holdRemaining(1));
        assertEquals(200 * ONE, ab.locked(USD));
        assertEquals(800 * ONE, ab.available(USD));
        assertArrayEquals("bytes are a function of state", image, bytes(b));

        // the restored engine fires replaceSlowCount once at the deadline and re-arms (3.8 #14), like the original
        r.advance(REPLACE_WINDOW_MS);
        r.sweep();
        b.sweep(r.now);
        assertEquals("restored: fired once", 1, ob.replaceSlowCount());
        assertEquals("original: fired once", 1, r.orders.replaceSlowCount());
        assertEquals("re-armed at deadline + 10 s", t0 + 2 * REPLACE_WINDOW_MS, ob.orders().get(1).replaceDeadlineMs);
        assertEquals(List.of((t0 + 2 * REPLACE_WINDOW_MS) + ":1"), replaceEntries(b));
        assertEquals(replaceEntries(r.engine), replaceEntries(b));
        assertArrayEquals("both engines agree byte for byte after the fire", bytes(r.engine), bytes(b));

        // the AMEND APPLIED outcome resolves identically on both
        final RecordingAssetsSink sb = (RecordingAssetsSink) b.eventSink();
        final int markB = sb.events().size();
        r.markNow();
        s.outcome(1, 1, 1, KIND_AMEND, APPLIED, NEW_LEG, LEG);
        ob.applyOutcome(9_999L, 1, (1L << 2) | KIND_AMEND, 1, 1, LEG, 0L, 0L, 0L, 1, KIND_AMEND, APPLIED, NEW_LEG, 0, r.now, 0L, r.now);
        final List<String> onOriginal = renders(r.events());
        final List<String> onRestored = renders(sb.events().subList(markB, sb.events().size()));
        assertEquals("identical rendered events", onOriginal, onRestored);
        assertTrue("the resolution is a REPLACED event, got " + onOriginal, onOriginal.stream().anyMatch(x -> x.contains("kind=REPLACED")));
        final CoreOrder resolved = ob.orders().get(1);
        assertEquals(NEW_LEG, resolved.currentLegId);
        assertEquals(PRICE, resolved.price);
        assertEquals(2 * ONE, resolved.quantity);
        assertEquals(200 * ONE, resolved.holdAmount);
        assertEquals(OrderStatus.NEW.code(), resolved.status);
        assertEquals(0L, resolved.replaceDeadlineMs);
        assertEquals("deadline removed on both", 0, replaceEntries(b).size());
        assertEquals(0, replaceEntries(r.engine).size());
        assertArrayEquals("both engines agree byte for byte after the resolution", bytes(r.engine), bytes(b));
    }
}

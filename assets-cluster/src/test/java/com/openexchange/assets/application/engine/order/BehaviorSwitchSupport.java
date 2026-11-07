// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.config.CoreConfigStateMachine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;

import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S7 fixture for {@link BehaviorSwitchTest} (contract section 5): the CoreConfig built from the compiled tables
 * with tick 0.5, band [10, 1000] and lot 0.1 on every market, the ADOPT (v1) and SWITCH (v2) steps with their
 * AdminAck / ConfigApplied preconditions, and the structural-refusal check (no rate budget, no stored request, no
 * hold, no record). Package-private.
 */
final class BehaviorSwitchSupport {

    static final int T_CORE_CONFIG = 70;
    static final int T_CREATE = 50;
    static final long TICK = ONE / 2;              // 0.5
    static final long MIN = 10 * ONE;              // 10
    static final long MAX = 1_000 * ONE;           // 1000
    static final long LOT = ONE / 10;              // 0.1
    static final long PX_OFF_TICK = 100 * ONE + 30_000_000L;  // 100.3
    static final long PX_ON_TICK = 100 * ONE + ONE / 2;       // 100.5
    static final long PX_BELOW_MIN = 5 * ONE;                 // 5
    static final long PX_ABOVE_MAX = 1_000 * ONE + ONE / 2;   // 1000.5 (on tick, above max)
    static final long QTY_OFF_LOT = 15_000_000L;              // 0.15
    static final long QTY_ON_LOT = 30_000_000L;               // 0.3

    private BehaviorSwitchSupport() {
    }

    static CoreConfigCommand config(final long version, final long behavior, final long lot, final long corr) {
        return AssetsScenarioRunner.coreConfigFor(version, behavior, TICK, MIN, MAX, lot, corr);
    }

    /** Gate open, user 1 holds 100 000 USD, user 2 holds 100 BTC, no CoreConfig recorded. */
    static OrderRig funded() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 100_000 * ONE);
        r.deposit(2, BTC, 100 * ONE);
        return r;
    }

    /** {@link #funded()} then ADOPT v1 (behaviour 1) and SWITCH v2 (behaviour 2), both with lot 0.1. */
    static OrderRig fundedV2() {
        final OrderRig r = funded();
        adoptV1(r, LOT);
        switchToV2(r, LOT);
        return r;
    }

    static CoreConfigStateMachine sm(final OrderRig r) {
        return r.engine.coreConfig();
    }

    /** Contract 3 row 2: the first CoreConfig (behaviour 1) is ADOPTED, recorded, broadcast; behaviour stays 1. */
    static void adoptV1(final OrderRig r, final long lot) {
        r.markNow();
        r.engine.applyCoreConfig(config(1, 1, lot, 701), r.now);
        final AssetsEngineEvent.AdminAck ack = r.first(AssetsEngineEvent.AdminAck.class);
        assertEquals("precondition (contract 3 row 2): the AdminAck names CoreConfig (70)", T_CORE_CONFIG, ack.refTemplateId());
        assertEquals("precondition (contract 3 row 2): the first CoreConfig is ADOPTED", AdminResult.ADOPTED, ack.result());
        assertEquals("precondition (contract 3 row 6): the AdminAck carries the recorded configVersion", 1L, ack.configVersion());
        assertEquals("precondition (contract 3 row 6): ConfigApplied{CORE, market 0, v1}",
                new AssetsEngineEvent.ConfigApplied(ConfigKind.CORE, 0, 1L), r.first(AssetsEngineEvent.ConfigApplied.class));
        assertNotNull("precondition (contract 2): the adopted config is recorded", sm(r).recorded());
        assertEquals("precondition (contract 2): behaviour 1 after the ADOPT", 1L, sm(r).behaviorVersion());
    }

    /** Contract 3 row 5: configVersion 2 with behaviour current + 1 is accepted and switches at this log position. */
    static void switchToV2(final OrderRig r, final long lot) {
        final long switchesBefore = sm(r).switchCount();
        r.markNow();
        r.engine.applyCoreConfig(config(2, 2, lot, 702), r.now);
        final List<AssetsEngineEvent> e = r.events();
        for (final AssetsEngineEvent ev : e) {
            assertFalse("contract 5: the switch takes no action on any order (no retroactive effect); got " + e,
                    ev instanceof AssetsEngineEvent.OrderEvent || ev instanceof AssetsEngineEvent.OrderReject
                            || ev instanceof AssetsEngineEvent.MeDispatch);
        }
        final AssetsEngineEvent.AdminAck ack = r.first(AssetsEngineEvent.AdminAck.class);
        assertEquals("contract 3 row 5: the AdminAck names CoreConfig (70)", T_CORE_CONFIG, ack.refTemplateId());
        assertEquals("contract 3 row 5: behaviour current + 1 is ACCEPTED (the switch)", AdminResult.ACCEPTED, ack.result());
        assertEquals("contract 3 row 6: the AdminAck carries configVersion 2", 2L, ack.configVersion());
        assertEquals("contract 3 row 6: ConfigApplied{CORE, market 0, v2}",
                new AssetsEngineEvent.ConfigApplied(ConfigKind.CORE, 0, 2L), r.first(AssetsEngineEvent.ConfigApplied.class));
        assertEquals("contract 5: behaviour 2 from this log position on", 2L, sm(r).behaviorVersion());
        assertNotNull("contract 2: the v2 config is recorded", sm(r).recorded());
        assertEquals("contract 2: recorded configVersion 2", 2L, sm(r).recorded().configVersion);
        assertEquals("contract 2: recorded behaviorVersion 2", 2L, sm(r).recorded().behaviorVersion);
        assertEquals("contract 5: exactly one switch counted", switchesBefore + 1, sm(r).switchCount());
    }

    /** Create and require admission: an OrderAck (not a replay), no OrderReject. Returns the orderId. */
    static long admit(final OrderRig r, final CreateOrderCommand c, final String what) {
        r.markNow();
        r.create(c);
        final List<AssetsEngineEvent> e = r.events();
        for (final AssetsEngineEvent ev : e) {
            assertFalse(what + ": admitted, never refused; got " + e, ev instanceof AssetsEngineEvent.OrderReject);
        }
        final AssetsEngineEvent.OrderAck ack = r.first(AssetsEngineEvent.OrderAck.class);
        assertFalse(what + ": a fresh decision, not a requestId replay", ack.duplicate());
        assertNotNull(what + ": the admitted order has a live record", r.orders.orders().get(ack.orderId()));
        return ack.orderId();
    }

    /**
     * Contract 5: the v2 rules are checked right after the gate, so a refusal is structural: exactly one OrderReject
     * naming CreateOrder with {@code reason}; no rate budget consumed; no request stored; no hold; no record; the
     * orderId, request and dispatch counters do not move.
     */
    static void refuseStructurally(final OrderRig r, final CreateOrderCommand c, final CoreRejectReason reason,
                                   final String what) {
        final UserRisk before = r.orders.user(c.userId);
        final int sec = before == null ? 0 : before.secCount;
        final int min = before == null ? 0 : before.minCount;
        final int requests = r.orders.requests().countFor(c.userId);
        final long requestSeq = r.orders.requests().nextRequestSeq();
        final int orders = r.orders.orders().size();
        final long nextOrderId = r.engine.getNextOrderId();
        final long dispatchSeq = r.engine.getDispatchSeq();
        final Account a = r.engine.account(c.userId);
        final long usdLocked = a.locked(USD);
        final long btcLocked = a.locked(BTC);
        final int holds = a.holdCount();

        r.markNow();
        r.create(c);

        final List<AssetsEngineEvent> e = r.events();
        assertEquals(what + ": exactly one OrderReject and nothing else, got " + e, 1, e.size());
        assertTrue(what + ": expected an OrderReject, got " + e.get(0), e.get(0) instanceof AssetsEngineEvent.OrderReject);
        final AssetsEngineEvent.OrderReject rej = (AssetsEngineEvent.OrderReject) e.get(0);
        assertEquals(what + ": reason", reason, rej.reason());
        assertEquals(what + ": the reject names CreateOrder (50)", T_CREATE, rej.refTemplateId());
        assertEquals(what + ": user", c.userId, rej.userId());
        assertEquals(what + ": market", c.marketId, rej.marketId());
        final UserRisk after = r.orders.user(c.userId);
        assertEquals(what + ": structural, no per-second rate budget consumed", sec, after == null ? 0 : after.secCount);
        assertEquals(what + ": structural, no per-minute rate budget consumed", min, after == null ? 0 : after.minCount);
        assertNull(what + ": structural, the request is not stored", r.orders.requests().get(c.userId, c.requestIdHigh, c.requestIdLow));
        assertEquals(what + ": structural, the user's request count is unchanged", requests, r.orders.requests().countFor(c.userId));
        assertEquals(what + ": structural, no request sequence consumed", requestSeq, r.orders.requests().nextRequestSeq());
        assertEquals(what + ": no record", orders, r.orders.orders().size());
        assertEquals(what + ": no orderId consumed", nextOrderId, r.engine.getNextOrderId());
        assertEquals(what + ": no dispatch", dispatchSeq, r.engine.getDispatchSeq());
        assertEquals(what + ": no hold (USD locked unchanged)", usdLocked, a.locked(USD));
        assertEquals(what + ": no hold (BTC locked unchanged)", btcLocked, a.locked(BTC));
        assertEquals(what + ": no hold (hold count unchanged)", holds, a.holdCount());
        assertEquals(what + ": no hold under the would-be orderId", 0L, a.holdRemaining(nextOrderId));
    }

    /** {@code limit} with a chosen requestId low word (same high word as the rig's). */
    static CreateOrderCommand withRequestId(final CreateOrderCommand c, final long requestIdLow) {
        c.requestIdLow = requestIdLow;
        return c;
    }

    static long countContaining(final List<String> lines, final String needle) {
        return lines.stream().filter(l -> l.contains(needle)).count();
    }

    static long countContaining(final String text, final String needle) {
        long n = 0;
        int at = text.indexOf(needle);
        while (at >= 0) {
            n++;
            at = text.indexOf(needle, at + needle.length());
        }
        return n;
    }
}

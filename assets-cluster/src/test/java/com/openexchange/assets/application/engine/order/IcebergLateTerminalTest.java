// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.ReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseEncoder;
import org.junit.Test;

import static com.openexchange.assets.application.engine.feed.V3Rig.BTC;
import static com.openexchange.assets.application.engine.feed.V3Rig.ONE;
import static com.openexchange.assets.application.engine.feed.V3Rig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 section 12 (H1, R-B2-3): a late duplicate terminal can never release a live iceberg slice. Layer 1 is the cursor
 * (a re-delivered entry is a DUPLICATE), layer 2 the leg-monotonicity belt at the six install sites (an exactly-next entry
 * naming an older leg installs nothing), layer 3 the v5 release paths.
 */
public class IcebergLateTerminalTest {

    private static final long L1 = 101;
    private static final long L2 = 303;
    private static final long BUY_LEG = 202;

    /**
     * Iceberg SELL 3 BTC, display 1 (order 1, user 2) and a BUY 3 BTC (order 2, user 1), both at 100. Slice 1 (L1) fills,
     * its FILLED terminal is consumed (stale), slice 2 (L2) is acked and fills: slice 3 is pending with oldLegId = L2.
     * Returns the journalSeq the L1 FILLED terminal was delivered at.
     */
    private static long[] h1(final V3Rig r) {
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        final long ice = r.create(r.iceberg(2, 1, 100 * ONE, 3 * ONE, ONE));
        final long buy = r.create(r.limit(1, 0, 100 * ONE, 3 * ONE));
        assertEquals(1L, ice);
        assertEquals(2L, buy);
        r.ack(ice, 0, L1);
        r.ack(buy, 0, BUY_LEG);
        assertTrue(r.trade(1, buy, BUY_LEG, 1, ice, L1, 2, 100 * ONE, ONE, true));
        assertTrue(r.terminal(ice, L1, 2, V3Rig.ME_FILLED, 0));
        final long[] l1Terminal = {r.jseq, r.egress};
        assertTrue(r.ack(ice, 1, L2));
        assertTrue(r.trade(2, buy, BUY_LEG, 1, ice, L2, 2, 100 * ONE, ONE, true));
        final CoreOrder o = r.order(ice);
        assertNotNull(o);
        assertEquals("fixture: slice 3 pending", 0L, o.currentLegId);
        assertEquals("fixture: oldLegId = L2", L2, o.oldLegId);
        assertTrue(o.has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(CoreOrder.PK_CREATE, o.pendingKind);
        assertEquals(2, o.attempt);
        return l1Terminal;
    }

    private static void assertSlice3StillPending(final V3Rig r, final String what, final long slices) {
        final CoreOrder o = r.order(1);
        assertNotNull(what + ": the iceberg is live", o);
        assertEquals(what + ": no leg learned", 0L, o.currentLegId);
        assertEquals(what + ": oldLegId stays L2", L2, o.oldLegId);
        assertEquals(what + ": slice 3 still pending", CoreOrder.PK_CREATE, o.pendingKind);
        assertEquals(what + ": no slice 4 minted", slices, r.engine.orders().sliceCount());
        assertEquals(what + ": attempt unchanged", 2, o.attempt);
        assertEquals(what + ": hidden + slice remain 1 BTC", ONE, o.remainingQty);
    }

    @Test
    public void duplicateBelowCursorIsANoOp() {
        final V3Rig r = V3Rig.open();
        final long[] l1Terminal = h1(r);
        final long slices = r.engine.orders().sliceCount();
        final long stale = r.engine.orders().staleTerminalCount();
        final long locked = r.locked(2, BTC);
        r.mark();
        assertTrue("a re-delivered entry is a DUPLICATE (true)",
                r.terminalAt(l1Terminal[0], V3Rig.EPOCH, l1Terminal[1], 1, L1, 2, V3Rig.ME_FILLED, 0));
        assertEquals("nothing emitted", 0, r.events().size());
        assertSlice3StillPending(r, "re-delivered L1 FILLED below the cursor", slices);
        assertEquals("not even counted stale: the cursor stopped it", stale, r.engine.orders().staleTerminalCount());
        assertEquals("the hold is untouched", locked, r.locked(2, BTC));
        assertEquals(1L, r.feed().duplicateCount(63));
    }

    @Test
    public void exactlyNextOlderLegCannotBeLearned() {
        // site G9 (:1124): exactly-next L1 FILLED while slice 3 is pending
        final V3Rig g9 = V3Rig.open();
        h1(g9);
        long slices = g9.engine.orders().sliceCount();
        g9.mark();
        assertTrue(g9.terminal(1, L1, 2, V3Rig.ME_FILLED, 0));
        assertSlice3StillPending(g9, "G9 with L1 < L2", slices);
        assertEquals(1L, g9.engine.orders().legRegressionCount());
        assertEquals("no event", 0, g9.count(AssetsEngineEvent.OrderEvent.class));

        // site implicit ack (:977): an exactly-next late trade on the just-exhausted slice L2
        final V3Rig ia = V3Rig.open();
        h1(ia);
        slices = ia.engine.orders().sliceCount();
        ia.mark();
        assertTrue("money stands: the trade settles", ia.trade(3, 2, BUY_LEG, 1, 1, L2, 2, 100 * ONE, ONE / 2, true));
        assertEquals(1, ia.count(AssetsEngineEvent.Settled.class));
        assertSlice3StillPending(ia, "late trade on L2 (L2 > max(0, L2) is false)", slices);
        assertEquals(1L, ia.engine.orders().legRegressionCount());
        assertTrue("the SETTLE fault path", ia.all(AssetsEngineEvent.OrderFault.class).stream()
                .anyMatch(f -> f.kind() == OrderFaultKind.SETTLE && f.orderId() == 1));
        assertTrue("no ACK_NEW", ia.all(AssetsEngineEvent.OrderEvent.class).stream()
                .noneMatch(e -> e.orderId() == 1 && e.kind() == EventKind.ACK_NEW));

        // site outcome CREATE APPLIED (:1210): slice 3's create outcome naming an older leg
        final V3Rig oc = V3Rig.open();
        h1(oc);
        slices = oc.engine.orders().sliceCount();
        final long staleOutcomes = oc.engine.orders().staleOutcomeCount();
        assertTrue(oc.outcome(1, 2, V3Rig.KIND_CREATE, 0, L1, 0, false));
        assertSlice3StillPending(oc, "CREATE APPLIED with L1", slices);
        assertTrue("the pending command is left as it is", oc.order(1).has(CoreOrder.F_DISPATCH_PENDING));
        assertEquals(1L, oc.engine.orders().legRegressionCount());
        assertEquals(staleOutcomes + 1, oc.engine.orders().staleOutcomeCount());

        // the three replace-window sites on a LIMIT order (leg 505), amend pending (oldLegId = currentLegId = 505)
        for (final String site : new String[] {"trade in window", "terminal in window", "outcome AMEND APPLIED"}) {
            final V3Rig w = window();
            w.mark();
            switch (site) {
                case "trade in window":
                    assertTrue(w.trade(1, 2, 606, 2, 1, 404, 1, 100 * ONE, ONE / 4, false));
                    assertTrue("the SETTLE fault path", w.all(AssetsEngineEvent.OrderFault.class).stream()
                            .anyMatch(f -> f.kind() == OrderFaultKind.SETTLE && f.orderId() == 1));
                    break;
                case "terminal in window":
                    assertTrue(w.terminal(1, 404, 1, V3Rig.ME_CANCELLED, 0));
                    break;
                default:
                    assertTrue(w.outcome(1, 1, V3Rig.KIND_AMEND, 0, 404, 0, false));
                    break;
            }
            final CoreOrder o = w.order(1);
            assertNotNull(site + ": live", o);
            assertTrue(site + ": the window stays open", o.has(CoreOrder.F_REPLACE_PENDING));
            assertEquals(site + ": the leg is not replaced", 505L, o.currentLegId);
            assertEquals(site + ": amend still pending", CoreOrder.PK_AMEND, o.pendingKind);
            assertEquals(site, 1L, w.engine.orders().legRegressionCount());
            assertTrue(site + ": no REPLACED", w.all(AssetsEngineEvent.OrderEvent.class).stream()
                    .noneMatch(e -> e.orderId() == 1 && e.kind() == EventKind.REPLACED));
        }
    }

    /** LIMIT BUY 1 BTC @ 100 (order 1, user 1) acked at leg 505, a resting SELL (order 2, user 2, leg 606), amend to 101. */
    private static V3Rig window() {
        final V3Rig w = V3Rig.open();
        w.deposit(1, USD, 1_000 * ONE);
        w.deposit(2, BTC, 10 * ONE);
        w.create(w.limit(1, 0, 100 * ONE, ONE));
        w.create(w.limit(2, 1, 105 * ONE, ONE));
        w.ack(1, 0, 505);
        w.ack(2, 0, 606);
        w.amend(1, 1, 101 * ONE, 0);
        final CoreOrder o = w.order(1);
        assertTrue("fixture: window open", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(505L, o.oldLegId);
        return w;
    }

    @Test
    public void replacePendingOlderLegTerminalIsStale() {
        final V3Rig w = window();
        final long stale = w.engine.orders().staleTerminalCount();
        w.mark();
        assertTrue(w.terminal(1, 404, 1, V3Rig.ME_FILLED, 0));
        final CoreOrder o = w.order(1);
        assertNotNull("the order stays live", o);
        assertTrue("the replace is not resolved by an older leg", o.has(CoreOrder.F_REPLACE_PENDING));
        assertEquals(505L, o.currentLegId);
        assertEquals(stale + 1, w.engine.orders().staleTerminalCount());
        assertEquals("no event", 0, w.count(AssetsEngineEvent.OrderEvent.class));
    }

    // ---------------------------------------------------------------- layer 3: the v5 release paths

    private static int register(final V3Rig r) {
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .correlationId(8_001).sessionClass(SessionClass.BRIDGE).readerKind((short) 0).resumable(BoolFlag.TRUE).identity(0)
                .sourceEpoch(0);
        final EgressChannelsEncoder ch = e.channels().clear();
        ch.acks(true);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int terminalRelease(final V3Rig r, final long order, final long user) {
        final TerminalReleaseEncoder e = new TerminalReleaseEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .journalPosition(9_999).orderId(order).userId(user).timestamp(r.now);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int release(final V3Rig r, final long order, final long user) {
        final ReleaseEncoder e = new ReleaseEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .orderId(order).userId(user).amount(-1L);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void v5ReleaseCannotStripALiveIceberg() {
        final V3Rig r = V3Rig.open();
        h1(r);
        final long locked = r.locked(2, BTC);
        assertEquals("fixture: the iceberg holds its 1 BTC remainder", ONE, locked);
        final long bridge = r.openSession(31, SessionClass.BRIDGE);
        r.dispatch(bridge, register(r));
        final long tool = r.openSession(32, SessionClass.TOOL);
        final long admin = r.openSession(33, SessionClass.ADMIN);
        // TerminalRelease 6 from BRIDGE and TOOL: refused (7.2 row 3)
        for (final long session : new long[] {bridge, tool}) {
            r.mark();
            r.dispatch(session, terminalRelease(r, 1, 2));
            assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.first(AssetsEngineEvent.OrderReject.class).reason());
            assertEquals(locked, r.locked(2, BTC));
        }
        // Release 4 from TOOL, ADMIN and the trusted path naming the live core order: a counted no-op (7.2 row 4)
        long refused = 0;
        for (final long session : new long[] {tool, admin, -1L}) {
            r.mark();
            if (session < 0) {
                r.dispatchTrusted(release(r, 1, 2));
            } else {
                r.dispatch(session, release(r, 1, 2));
            }
            assertEquals("no balance moved", 0, r.count(AssetsEngineEvent.Balance.class));
            assertEquals(locked, r.locked(2, BTC));
            assertEquals(++refused, r.engine.coreOwnedReleaseRefusedCount());
        }
        assertNotNull(r.order(1));
    }
}

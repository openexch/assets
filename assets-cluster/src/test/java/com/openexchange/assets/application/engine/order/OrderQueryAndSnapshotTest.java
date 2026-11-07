// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsReplica;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.cluster.testkit.SnapshotSpliceRunner;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.assets.application.engine.order.OrderRig.BTC;
import static com.openexchange.assets.application.engine.order.OrderRig.ONE;
import static com.openexchange.assets.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S5 group G4a (contract 3.6 queries, 1 record and scalars, 6 DSL verbs): RequestOrderSnapshot scope and
 * paging, the section 2 record with {@code pendingDispatchSeq} through the codec and an identical
 * RequestPendingDispatch answer after restore, and the splice runner holding with the S5 verbs.
 *
 * <p>RED against the S5 stubs: every query, feed and amend method is a no-op today.</p>
 */
public class OrderQueryAndSnapshotTest {

    private static final int T_REQUEST_ORDER_SNAPSHOT = 53;
    private static final int PAGE = 512;
    /** Contract section 1 (amended 2026-10-26): 33 longs (pendingDispatchSeq, replaceDeadlineMs), 4 ints, 1 short, 6 bytes. */
    private static final int SECTION2_RECORD_BYTES = 35 * 8 + 4 * 4 + 2 + 6; // RC2 S8 5.1: 288 -> 304

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

    private static List<String> renders(final List<AssetsEngineEvent> events) {
        final List<String> out = new ArrayList<>(events.size());
        for (final AssetsEngineEvent e : events) {
            out.add(e.render());
        }
        return out;
    }

    /** The ORDERSNAP line the recording sink renders for a live record. */
    private static String snapOf(final long corr, final CoreOrder o) {
        return new AssetsEngineEvent.OrderSnapshotEntry(corr, o.orderId, o.userId, o.marketId, OrderStatus.fromCode(o.status),
                o.price, o.quantity, o.filledQty, o.remainingQty, o.holdAssetId, o.holdAmount, o.currentLegId, o.attempt).render();
    }

    private static String endOf(final long corr, final int n, final long next) {
        return new AssetsEngineEvent.OrderSnapshotEnd(corr, n, next).render();
    }

    /** MeCommandOutcome APPLIED for the create of {@code orderId} (attempt 0, kind CREATE): contract 3.3, the leg is learned. */
    private static void ackCreate(final OrderRig r, final long egressSeq, final long orderId, final long userId, final long legId) {
        r.orders.applyOutcome(egressSeq, orderId, 0L, userId, orderId, 0L, 0L, 0L, 0L, 1, 0, 0, legId, 0, r.now, 0L, r.now);
    }

    /** Five live orders over three users: 1 (u1 BUY, acked as leg 101), 2 (u2 BUY), 3 (u1 BUY), 4 (u3 SELL), 5 (u1 BUY). */
    private static OrderRig threeUsersFiveOrders() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, USD, 1_000 * ONE);
        r.deposit(3, BTC, 10 * ONE);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));
        r.create(r.limit(2, 1, 0, 100 * ONE, 2 * ONE));
        r.create(r.limit(1, 1, 0, 101 * ONE, ONE));
        r.create(r.limit(3, 1, 1, 100 * ONE, 3 * ONE));
        r.create(r.limit(1, 1, 0, 99 * ONE, ONE));
        ackCreate(r, 1, 1, 1, 101);
        return r;
    }

    @Test
    public void orderSnapshot_returnsOnlyTheRequestingUsersLiveOrdersAscendingThenEnd() {
        // contract 3.6 RequestOrderSnapshot: page = the user's live orders with orderId > fromOrderId, ascending, then ORDERSNAPEND next=0
        final OrderRig r = threeUsersFiveOrders();
        final OrderStore s = r.orders.orders();
        r.markNow();
        r.orders.applyRequestOrderSnapshot(501L, 1L, 0L, false);
        final List<String> expected = List.of(snapOf(501L, s.get(1)), snapOf(501L, s.get(3)), snapOf(501L, s.get(5)), endOf(501L, 3, 0L));
        assertEquals("user 1 must see orders 1, 3, 5 ascending as ORDERSNAP entries then ORDERSNAPEND n=3 next=0",
                expected, renders(r.events()));
        final AssetsEngineEvent.OrderSnapshotEntry first = r.first(AssetsEngineEvent.OrderSnapshotEntry.class);
        assertEquals("order 1 was acked by the ME: its entry carries status NEW", OrderStatus.NEW, first.status());
        assertEquals("order 1 was acked by the ME: its entry carries leg 101", 101L, first.meOrderId());

        r.markNow();
        r.orders.applyRequestOrderSnapshot(502L, 2L, 0L, false);
        assertEquals("user 2 sees only order 2", List.of(snapOf(502L, s.get(2)), endOf(502L, 1, 0L)), renders(r.events()));

        r.markNow();
        r.orders.applyRequestOrderSnapshot(503L, 9L, 0L, false);
        assertEquals("a user without live orders gets just the end frame", List.of(endOf(503L, 0, 0L)), renders(r.events()));
    }

    @Test
    public void orderSnapshot_userZeroWithoutAdminAuthorityIsRejectedInvalid() {
        // contract 3.6: userId == 0 requires ADMIN (allUsersAllowed); otherwise OrderReject INVALID and no entries
        final OrderRig r = threeUsersFiveOrders();
        r.markNow();
        r.orders.applyRequestOrderSnapshot(504L, 0L, 0L, false);
        assertEquals("a non-admin userId 0 request is answered with exactly one OrderReject INVALID, got " + r.events(),
                1, r.events().size());
        final AssetsEngineEvent.OrderReject rej = r.reject();
        assertEquals(CoreRejectReason.INVALID, rej.reason());
        assertEquals(T_REQUEST_ORDER_SNAPSHOT, rej.refTemplateId());
        assertEquals(504L, rej.correlationId());
        assertEquals(0, r.count(AssetsEngineEvent.OrderSnapshotEntry.class));
    }

    @Test
    public void orderSnapshot_adminUserZeroReturnsEveryLiveOrderAscending() {
        // contract 3.6: allUsersAllowed with userId 0 = every live order, ascending, one page
        final OrderRig r = threeUsersFiveOrders();
        final OrderStore s = r.orders.orders();
        r.markNow();
        r.orders.applyRequestOrderSnapshot(505L, 0L, 0L, true);
        final List<String> expected = new ArrayList<>();
        for (long id = 1; id <= 5; id++) {
            expected.add(snapOf(505L, s.get(id)));
        }
        expected.add(endOf(505L, 5, 0L));
        assertEquals("ADMIN with userId 0 must see all five live orders ascending then ORDERSNAPEND n=5 next=0",
                expected, renders(r.events()));
    }

    @Test
    public void orderSnapshot_pagesAt512WithTheLastOrderIdAsTheExclusiveCursor() {
        // contract 3.6: at most 512 per request; nextOrderId = last orderId of a full page, else 0; fromOrderId is exclusive
        final OrderRig r = OrderRig.open();
        final RiskConfigSetCommand cfg = r.riskConfig(1, 1);
        cfg.maxOpenOrders = 1_000;
        cfg.maxOrdersPerSec = 1_000;
        cfg.maxOrdersPerMin = 10_000;
        r.setRisk(cfg);
        r.deposit(1, USD, 10_000 * ONE);
        // 600 one-dollar orders; the per-user requestId cap (256) is respected by letting the TTLs expire between batches
        for (final int batch : new int[] {250, 250, 100}) {
            for (int i = 0; i < batch; i++) {
                r.create(r.limit(1, 1, 0, ONE, ONE));
            }
            r.advance(RequestTable.TTL_MS + 1_000);
            r.sweep();
        }
        assertEquals("rig precondition: 600 live orders for user 1", 600, r.orders.orders().openCount(1));
        assertEquals("rig precondition: 600 USD locked", 600L, r.engine.account(1).locked(USD) / ONE);

        r.markNow();
        r.orders.applyRequestOrderSnapshot(506L, 1L, 0L, false);
        final List<AssetsEngineEvent> page1 = r.events();
        assertEquals("page 1 must hold exactly 512 ORDERSNAP entries plus the end frame, got " + page1.size() + " events",
                PAGE + 1, page1.size());
        for (int i = 0; i < PAGE; i++) {
            final AssetsEngineEvent e = page1.get(i);
            assertTrue("entry " + i + " is an ORDERSNAP: " + e, e instanceof AssetsEngineEvent.OrderSnapshotEntry);
            assertEquals("entries ascend by orderId", i + 1, ((AssetsEngineEvent.OrderSnapshotEntry) e).orderId());
        }
        final AssetsEngineEvent.OrderSnapshotEnd end1 = r.first(AssetsEngineEvent.OrderSnapshotEnd.class);
        assertEquals(PAGE, end1.entryCount());
        assertEquals("a full page names its last orderId as the cursor", 512L, end1.nextOrderId());

        r.markNow();
        r.orders.applyRequestOrderSnapshot(507L, 1L, end1.nextOrderId(), false);
        assertEquals("page 2 = orders 513..600 then the end frame", 88 + 1, r.events().size());
        assertEquals("the cursor is exclusive: page 2 starts at 513", 513L, r.first(AssetsEngineEvent.OrderSnapshotEntry.class).orderId());
        final AssetsEngineEvent.OrderSnapshotEnd end2 = r.first(AssetsEngineEvent.OrderSnapshotEnd.class);
        assertEquals(88, end2.entryCount());
        assertEquals("the last page carries next 0", 0L, end2.nextOrderId());
    }

    @Test
    public void section2RecordIs304BytesPerLiveOrder() {
        // contract 1 (2026-10-26): CoreOrder gains pendingDispatchSeq and replaceDeadlineMs (288 B); RC2 S8 5.1 adds
        // pendingLegQty and pendingExpectedOldRemaining after replaceDeadlineMs: 304 B, every behaviour
        final OrderRig r = threeUsersFiveOrders();
        assertEquals("CoreOrder.SNAPSHOT_BYTES must be 35 longs + 4 ints + 1 short + 6 bytes (S5 contract 1 and RC2 S8 5.1)",
                SECTION2_RECORD_BYTES, CoreOrder.SNAPSHOT_BYTES);
        final byte[] image = bytes(r.engine);
        final int[] section2 = ordersSection(image);
        assertEquals("section 2 declares the five live records", 5, section2[0]);
        assertEquals("section 2 body = 4-byte count + 288 B per record", 4 + 5 * SECTION2_RECORD_BYTES, section2[1]);
        final AssetsEngine b = restore(image);
        assertEquals(5, b.orders().orders().size());
        assertArrayEquals("bytes are a function of state", image, bytes(b));
    }

    /** Walk the v4 payload (tag, 7 scalars, flags byte, section count, sections) to section 2: {recordCount, sectionLength}. */
    private static int[] ordersSection(final byte[] image) {
        final UnsafeBuffer b = new UnsafeBuffer(image);
        int pos = 8 * 8 + 1;
        final int sections = b.getInt(pos);
        pos += 4;
        for (int s = 0; s < sections; s++) {
            final short id = b.getShort(pos);
            final int len = b.getInt(pos + 2);
            pos += 6;
            if (id == BalanceSnapshotCodec.SECTION_ORDERS) {
                return new int[] {b.getInt(pos), len};
            }
            pos += len;
        }
        throw new AssertionError("no ORDERS section in the image");
    }

    @Test
    public void pendingDispatchSeqIsMintedSnapshottedAndTheReplayIsIdenticalAfterRestore() {
        // contract 1 + 3.6 RequestPendingDispatch: every mint sets pendingDispatchSeq, the outcome clears it, the cursor survives restore
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000 * ONE);
        r.deposit(2, BTC, 10 * ONE);
        r.create(r.limit(1, 1, 0, 100 * ONE, ONE));        // order 1: dispatch 1 (create), stays pending
        r.create(r.limit(2, 1, 1, 100 * ONE, 2 * ONE));    // order 2: dispatch 2 (create)
        r.create(r.limit(1, 1, 0, 101 * ONE, ONE));        // order 3: dispatch 3 (create), stays pending
        ackCreate(r, 1, 2, 2, 102);                         // order 2 rests as leg 102: dispatch 2 resolved
        r.cancel(2, 2);                                     // order 2: cancel minted as dispatch 4 (attempt 1)
        final OrderStore s = r.orders.orders();
        assertEquals("the create mint must record its dispatchSeq on the order", 1L, s.get(1).pendingDispatchSeq);
        assertEquals(3L, s.get(3).pendingDispatchSeq);
        assertEquals("the create outcome cleared dispatch 2 and the cancel mint recorded dispatch 4", 4L, s.get(2).pendingDispatchSeq);
        assertEquals(4L, r.engine.getDispatchSeq());

        final byte[] image = bytes(r.engine);
        final AssetsEngine b = restore(image);
        final OrderStore sb = b.orders().orders();
        for (long id = 1; id <= 3; id++) {
            assertNotNull(sb.get(id));
            assertEquals("pendingDispatchSeq of order " + id + " survives the codec", s.get(id).pendingDispatchSeq, sb.get(id).pendingDispatchSeq);
        }
        assertArrayEquals("bytes are a function of state", image, bytes(b));

        r.markNow();
        r.orders.applyRequestPendingDispatch(601L, 0L, 1024);
        final List<String> original = renders(r.events());
        final String cancelReply = new AssetsEngineEvent.MeDispatchReply(4, 2, (1L << 2) | 1, DispatchKind.CANCEL, 2, 2, 102, 1, 0, 1,
                0, 0, 0, 0).render();
        final List<String> expected = List.of(
                new AssetsEngineEvent.MeDispatchReply(1, 1, 0, DispatchKind.CREATE, 1, 1, 0, 1, 0, 0, 100 * ONE, ONE, 0, 0).render(),
                new AssetsEngineEvent.MeDispatchReply(3, 3, 0, DispatchKind.CREATE, 1, 3, 0, 1, 0, 0, 101 * ONE, ONE, 0, 0).render(),
                cancelReply,
                new AssetsEngineEvent.PendingDispatchEnd(601L, 3, 4L).render());
        assertEquals("pending commands replay in dispatchSeq order with their original payloads, then PENDINGEND n=3 head=4",
                expected, original);

        final RecordingAssetsSink sinkB = (RecordingAssetsSink) b.eventSink();
        final int before = sinkB.events().size();
        b.orders().applyRequestPendingDispatch(601L, 0L, 1024);
        assertEquals("the restored engine answers RequestPendingDispatch identically", original,
                renders(sinkB.events().subList(before, sinkB.events().size())));

        final int beforeCursor = sinkB.events().size();
        b.orders().applyRequestPendingDispatch(602L, 3L, 1024);
        assertEquals("the forwarder cursor (fromDispatchSeq 3) survives restore: only dispatch 4 is re-offered",
                List.of(cancelReply, new AssetsEngineEvent.PendingDispatchEnd(602L, 1, 4L).render()),
                renders(sinkB.events().subList(beforeCursor, sinkB.events().size())));
    }

    @Test
    public void theSpliceRunnerHoldsWithTheS5Verbs() {
        // contract 6 DSL verbs; A+B == A+SNAPSHOT+B at every cut once the verbs do something (today they produce nothing)
        final List<String> lines = List.of(
                "CLOCK 1700000000000",
                "GATE market=0 open=1",
                "DEPOSIT u=1 asset=0 amt=1000.0",
                "DEPOSIT u=2 asset=1 amt=10.0",
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=2.0 rid=1",
                "CREATE u=2 market=1 side=1 type=0 px=100.0 qty=3.0 rid=2",
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 tif=3 exp=1700000030000 rid=3",
                "MEOUTCOME order=1 kind=0 attempt=0 result=0 leg=101",
                "MEOUTCOME order=2 kind=0 attempt=0 result=0 leg=102",
                "PENDINGDISPATCH corr=1",
                "METRADE id=1 market=1 taker=2 maker=1 px=100.0 qty=0.5 takerBuy=0",
                "METRADE id=1 market=1 taker=2 maker=1 px=100.0 qty=0.5 takerBuy=0",
                "AMEND u=1 order=1 px=110.0 rid=4 corr=2",
                "METRADE id=2 market=1 taker=2 maker=1 px=100.0 qty=0.5 takerBuy=0",
                "MEOUTCOME order=1 kind=2 attempt=1 result=0 leg=103 old=101",
                "ORDERSNAP u=1 corr=3",
                "QUARANTINE order=2 corr=4",
                "CANCEL u=2 order=2 rid=5",
                "METERMINAL order=2 leg=102 status=3",
                "TICK 30000",
                "MEOUTCOME order=3 kind=0 attempt=0 result=0 leg=104",
                "PENDINGDISPATCH corr=5",
                "MEOUTCOME order=3 kind=1 attempt=1 result=0",
                "METERMINAL order=3 leg=104 status=3",
                "CANCEL u=1 order=1 rid=6",
                "METERMINAL order=1 leg=103 status=3",
                "ORDERSNAP u=0 corr=6",
                "PENDINGDISPATCH corr=7");
        final String out = AssetsScenarioRunner.runLines(lines);
        for (final String must : new String[] {"SETTLED tradeId=1 buyer=1 seller=2", "SETTLED tradeId=2 buyer=1 seller=2",
                "kind=ACK_NEW", "kind=FILL", "kind=REPLACED", "kind=QUARANTINED", "kind=EXPIRED", "kind=CANCELLED",
                "ORDERSNAPEND corr=3 n=2 next=0", "ORDERSNAPEND corr=6 n=0 next=0", "PENDINGEND corr=1 n=1 head=3",
                "PENDINGEND corr=5 n=1 head=5", "PENDINGEND corr=7 n=0 head=6"}) {
            assertTrue("the S5 verbs must produce '" + must + "' (contract 3.1-3.6); the rendered output was:\n" + out, out.contains(must));
        }
        SnapshotSpliceRunner.run(AssetsReplica::new, lines);
    }
}

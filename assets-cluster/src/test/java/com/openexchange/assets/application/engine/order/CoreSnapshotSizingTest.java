// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.time.DeadlineIndex;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC1 S8, harness B7 test 1 (RC1-A05 sizing evidence). Builds 10k users x 100k open orders through the engine API
 * in-process (deposits, a gate open, a CoreConfig ADOPT v1, LIMIT creates over 5 markets and both sides with legs
 * learned through {@code applyOutcome APPLIED}, plus 5% armed stops, 2% GTD and 1% icebergs), then measures
 * {@code serialize -> deserialize} and asserts:
 * <ol>
 *   <li>the image size equals the declared layout arithmetic (snapshot-v4-layout page, S4/S5/S7 rows), derived here
 *       from the counts this test built, not read back from the codec;</li>
 *   <li>the restored engine holds the same orders, armed stops, GTD deadlines, users and request rows;</li>
 *   <li>the restored engine re-serializes byte-identical (full compare, not sampled);</li>
 *   <li>on the restored engine, per (account, asset) {@code locked == sum of hold remaining} (I2), per-asset totals
 *       equal the source engine's, and every live order owns exactly one hold (I4 shape).</li>
 * </ol>
 * Prints one {@code [SIZING]} line in the RC0 spike's format. 1M orders (100k users) runs only with
 * {@code -Dcore.sizing.million=true}.
 */
public class CoreSnapshotSizingTest {

    private static final long ONE = 100_000_000L;
    private static final int ORDERS_PER_USER = 10;
    private static final long START_MS = 1_700_000_000_000L;
    private static final long GTD_AHEAD_MS = 3_600_000L;
    private static final long LEG_BASE = 1_000_000_000L;
    private static final long[] BASE_PX = {0, 50_000 * ONE, 3_000 * ONE, 150 * ONE, 2 * ONE, ONE};

    // ---- the declared record sizes (layout page), written as literals so a code change cannot move both sides
    private static final int HEADER_BYTES = 8 + 7 * 8 + 1 + 4;          // tag, 7 scalars, flags, sectionCount
    private static final int SECTION_HEADER_BYTES = 2 + 4;
    private static final int ACCOUNT_ROW_BYTES = 8 + 4 + 6 * 16 + 4;    // userId, numAssets, 6 x (avail, locked), numHolds
    private static final int HOLD_ROW_BYTES = 8 + 4 + 8 + 1;
    private static final int ORDER_BYTES = 288;
    private static final int USER_RISK_BYTES = 80;
    private static final int MARKET_STATE_BYTES = 117;
    private static final int MARKET_SLOTS = 6;
    private static final int GATE_ROW_BYTES = 4 + 1 + 1 + 4;            // RC2 S7 6.3: + reasons u32 (layout v2)
    private static final int REQUEST_ROW_BYTES = 58;
    private static final int CONFIG_MARKET_BYTES = 4 + 16 + 4 + 4 + 4 * 8;
    private static final int CONFIG_ASSET_BYTES = 4 + 8 + 1;

    /** Counts what the build must produce; also the input of the expected-size arithmetic. */
    private static final class Built {
        final AssetsEngine engine;
        int stops;
        int gtd;
        int icebergs;
        int limits;
        int legs;

        Built(final AssetsEngine engine) {
            this.engine = engine;
        }
    }

    @Test
    public void tenThousandUsersAndOneHundredThousandOrdersMatchTheDeclaredLayoutAndRestoreByteIdentical() {
        measure(10_000, 100_000, false);
    }

    @Test
    public void oneMillionOrders() {
        Assume.assumeTrue("1M sizing runs only with -Dcore.sizing.million=true", Boolean.getBoolean("core.sizing.million"));
        measure(100_000, 1_000_000, true);
    }

    /** The pinned literals must equal the code's constants, or the arithmetic below checks a layout nobody writes. */
    /** RC2 S7 16.5: under behaviour 3 the image gains section 10 FEED_STATE (92 bytes): 9 sections. */
    @Test
    public void aV3ImageCarriesTheFeedStateSection() {
        final com.openexchange.assets.application.engine.feed.V3Rig r = com.openexchange.assets.application.engine.feed.V3Rig.bound();
        final ExpandableArrayBuffer out = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(r.engine, out, 0);
        final long v2Shape = expectedImageBytes(0, 0, 0, 0, Market.ALL.length, Asset.count());
        // section 10 layout v2 (3.8): 92-byte head + freshDigest 8 + digestCount 4 + one digest per entry (cursor 1)
        assertEquals("v3 = the v1/v2 layout + one section header + 92 + 12 + 8 bytes", v2Shape + SECTION_HEADER_BYTES + 92 + 12 + 8,
                length);
        final AssetsEngine restored = new AssetsEngine();
        restored.coreConfig().setSupportedBehaviorVersions(1, 2, 3);
        restored.setEventSink(new QuietSink());
        final BalanceSnapshotCodec.Decoded decoded = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(out.byteArray(), 0, length),
                0, length, restored);
        assertEquals("sections (1..6, 8 CORE_CONFIG, 9 SESSIONS, 10 FEED_STATE)", 9, decoded.sectionCount);
    }

    @Test
    public void declaredRecordSizesMatchTheCodeConstants() {
        assertEquals("order record (S5 contract 1: 33 longs, 4 ints, 1 short, 6 bytes)", ORDER_BYTES, CoreOrder.SNAPSHOT_BYTES);
        assertEquals("user risk record", USER_RISK_BYTES, UserRisk.SNAPSHOT_BYTES);
        assertEquals("market state record", MARKET_STATE_BYTES, MarketRisk.SNAPSHOT_BYTES);
        assertEquals("market slots (0 = defaults, 1..5)", MARKET_SLOTS, UserRisk.MARKET_SLOTS);
        assertEquals("request row", REQUEST_ROW_BYTES, RequestTable.ENTRY_BYTES);
        assertEquals("dense per-account asset width", 6, Asset.count());
        assertEquals("gate rows = global + 5 markets", MARKET_SLOTS, OrderEntryGate.MARKETS + 1);
    }

    /**
     * @param lowMemory the 1M run: surefire pins {@code -Xmx2g} after {@code ${argLine}}, so the source engine is
     *                  dropped before the restore and the byte-identity check compares SHA-256 digests over every byte
     *                  of both images instead of holding two images at once
     */
    private static void measure(final int users, final int orders, final boolean lowMemory) {
        assertEquals("fixture shape: " + ORDERS_PER_USER + " orders per user", (long) users * ORDERS_PER_USER, orders);
        Built b = build(users, orders);
        AssetsEngine src = b.engine;
        final int stops = b.stops;
        final int gtd = b.gtd;

        // ---- the build produced exactly the intended state
        assertEquals("live orders", orders, src.orders().orders().size());
        assertEquals("accounts", users, src.accountCount());
        assertEquals("user risk rows", users, src.orders().userCount());
        assertEquals("request rows (one per accepted create, all within the 15 min TTL)", orders, src.orders().requests().size());
        assertEquals("armed stops (5%)", orders / 20, b.stops);
        assertEquals("GTD orders (2%)", orders / 50, b.gtd);
        assertEquals("icebergs (1%)", orders / 100, b.icebergs);
        assertEquals("every non-armed order learned its leg", orders - b.stops, b.legs);
        assertEquals("armed stops in the trigger books", b.stops, armedStops(src));
        assertEquals("GTD deadlines on the index", b.gtd, deadlines(src, DeadlineIndex.KIND_GTD));
        final long[] srcLocked = lockedTotals(src);
        final int holds = holdsAndCheckLocked(src, "source");
        assertEquals("one hold per live order", orders, holds);

        // ---- expected image size from the counts, per section
        final long expected = expectedImageBytes(users, orders, holds, orders, Market.ALL.length, Asset.count());

        ExpandableArrayBuffer out = new ExpandableArrayBuffer((int) Math.min(Integer.MAX_VALUE - 64, expected + 64));
        final long s0 = System.nanoTime();
        final int length = BalanceSnapshotCodec.serialize(src, out);
        final long s1 = System.nanoTime();
        assertEquals("image size = header + section headers + sum of declared rows (users=" + users + " orders=" + orders
                + " holds=" + holds + " requests=" + orders + ")", expected, length);
        final long srcNextOrderId = src.getNextOrderId();
        final long srcDispatchSeq = src.getDispatchSeq();
        if (lowMemory) {
            b = null;
            src = null;
        }

        final AssetsEngine restored = new AssetsEngine();
        restored.setEventSink(new QuietSink());
        final long d0 = System.nanoTime();
        final BalanceSnapshotCodec.Decoded decoded = BalanceSnapshotCodec.deserialize(new UnsafeBuffer(out.byteArray(), 0, length),
                0, length, restored);
        final long d1 = System.nanoTime();
        assertEquals("deserialize consumes exactly the payload", length, decoded.bytesConsumed);
        assertEquals("sections (1..6, 8 CORE_CONFIG, 9 SESSIONS)", 8, decoded.sectionCount);
        assertEquals("restored accounts", users, decoded.accountCount);
        assertEquals("restored holds", holds, decoded.holdCount);

        // ---- restored state, then the byte-identical re-serialize
        assertEquals("restored live orders", orders, restored.orders().orders().size());
        assertEquals("restored user risk rows", users, restored.orders().userCount());
        assertEquals("restored request rows", orders, restored.orders().requests().size());
        assertEquals("restored armed stops (books derived from the records)", stops, armedStops(restored));
        assertEquals("restored GTD deadlines (derived from the records)", gtd, deadlines(restored, DeadlineIndex.KIND_GTD));
        assertNotNull("restored CoreConfig", restored.coreConfig().recorded());
        assertEquals("restored behaviour version", 1L, restored.coreConfig().behaviorVersion());
        assertEquals("restored next orderId", srcNextOrderId, restored.getNextOrderId());
        assertEquals("restored dispatchSeq", srcDispatchSeq, restored.getDispatchSeq());
        restored.verifyRestoredInvariants();
        assertEquals("restored holds walk", holds, holdsAndCheckLocked(restored, "restored"));
        assertTrue("per-asset locked totals survive the restore: src=" + Arrays.toString(srcLocked) + " restored="
                + Arrays.toString(lockedTotals(restored)), Arrays.equals(srcLocked, lockedTotals(restored)));
        everyLiveOrderOwnsOneHold(restored);

        final byte[] firstDigest = lowMemory ? sha256(out.byteArray(), length) : null;
        if (lowMemory) {
            out = null;
        }
        final ExpandableArrayBuffer again = new ExpandableArrayBuffer(length + 64);
        final int length2 = BalanceSnapshotCodec.serialize(restored, again);
        assertEquals("re-serialized length", length, length2);
        if (lowMemory) {
            assertTrue("the restored engine re-serializes byte-identical (SHA-256 over every byte of both images)",
                    Arrays.equals(firstDigest, sha256(again.byteArray(), length2)));
        } else {
            final int diff = Arrays.mismatch(out.byteArray(), 0, length, again.byteArray(), 0, length2);
            assertEquals("the restored engine re-serializes byte-identical (first differing offset)", -1, diff);
        }

        final double serializeMs = (s1 - s0) / 1e6;
        final double deserializeMs = (d1 - d0) / 1e6;
        System.out.println(String.format(Locale.ROOT, "[SIZING] users=%d orders=%d bytes=%d serializeMs=%.1f deserializeMs=%.1f perOrderBytes=%.1f",
                users, orders, length, serializeMs, deserializeMs, (double) length / orders));
    }

    private static byte[] sha256(final byte[] bytes, final int length) {
        try {
            final java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            md.update(bytes, 0, length);
            return md.digest();
        } catch (final java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The declared layout: every term is (count x row) from the layout page, plus fixed section bodies. */
    static long expectedImageBytes(final int users, final int orders, final int holds, final int requestRows,
                                   final int configMarkets, final int configAssets) {
        final int sections = 8; // 1 ACCOUNTS, 2 ORDERS, 3 USER_RISK, 4 MARKET_STATE, 5 GATES, 6 REQUEST_IDS, 8 CORE_CONFIG, 9 SESSIONS
        final long accounts = 4 + (long) users * ACCOUNT_ROW_BYTES + (long) holds * HOLD_ROW_BYTES;
        final long orderSection = 4 + (long) orders * ORDER_BYTES;
        final long userRisk = 4 + (long) users * USER_RISK_BYTES;
        final long marketState = 4 + (long) MARKET_SLOTS * MARKET_STATE_BYTES;
        final long gates = 4 + (long) MARKET_SLOTS * GATE_ROW_BYTES;
        final long requests = 8 + 4 + (long) requestRows * REQUEST_ROW_BYTES;
        // RC2 S7 7.3: section 8 layout v2, genesisId after behaviorVersion
        final long config = 8 + 8 + 8 + 4 + (long) configMarkets * CONFIG_MARKET_BYTES + 4 + (long) configAssets * CONFIG_ASSET_BYTES;
        final long sessions = 4; // in-process: no session authorities
        return HEADER_BYTES + (long) sections * SECTION_HEADER_BYTES
                + accounts + orderSection + userRisk + marketState + gates + requests + config + sessions;
    }

    // ---------------------------------------------------------------- the build

    private static Built build(final int users, final int orders) {
        final AssetsEngine e = new AssetsEngine();
        final QuietSink sink = new QuietSink();
        e.setEventSink(sink);
        final Built b = new Built(e);
        final long now = START_MS;

        for (long u = 1; u <= users; u++) {
            deposit(e, u, Asset.USD.id(), 1_000_000 * ONE, now);
            for (final Market m : Market.ALL) {
                deposit(e, u, m.baseAsset().id(), 1_000 * ONE, now);
            }
        }
        e.applyOrderEntryGate(1L, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, now);
        assertTrue("the global gate opens", e.gate().admits(1));
        sink.lastAdmin = null;
        e.applyCoreConfig(AssetsScenarioRunner.coreConfigFor(1, 1, 1L, 1L, 1_000_000_000_000_000L, 0L, 2L), now);
        assertEquals("CoreConfig v1 is ADOPTED", AdminResult.ADOPTED, sink.lastAdmin);

        final CreateOrderCommand c = new CreateOrderCommand();
        long egressSeq = 0;
        for (int i = 0; i < orders; i++) {
            final long user = 1 + i / ORDERS_PER_USER;
            final int market = 1 + i % 5;
            final int side = (i / 5) % 2;
            final int sel = (int) ((i * 37L) % 100); // a bijection on each block of 100: the mix is exact
            final long px = BASE_PX[market] + (i % 7) * (ONE / 100);
            final long qty = (ONE / 10) * (1 + i % 3);
            c.reset();
            c.correlationId = i + 1;
            c.userId = user;
            c.marketId = market;
            c.side = side;
            c.quantity = qty;
            c.requestIdHigh = 0x5153L;
            c.requestIdLow = i + 1;
            boolean armed = false;
            if (sel < 5) {
                armed = true;
                final boolean buy = side == OrderEngine.SIDE_BUY;
                c.stopPrice = buy ? px + px / 10 : px - px / 10;
                if ((i / 100) % 2 == 0) {
                    c.orderType = OrderEngine.TYPE_STOP_LOSS; // BUY: budget = stop x (1 + slip) x qty
                } else {
                    c.orderType = OrderEngine.TYPE_STOP_LIMIT;
                    c.price = buy ? c.stopPrice + c.stopPrice / 100 : c.stopPrice - c.stopPrice / 100;
                }
                b.stops++;
            } else if (sel < 7) {
                c.orderType = OrderEngine.TYPE_LIMIT;
                c.timeInForce = OrderEngine.TIF_GTD;
                c.price = px;
                c.expiresAtMs = now + GTD_AHEAD_MS + i;
                b.gtd++;
            } else if (sel == 7) {
                c.orderType = OrderEngine.TYPE_ICEBERG;
                c.price = px;
                c.displayQty = qty / 2;
                b.icebergs++;
            } else {
                c.orderType = OrderEngine.TYPE_LIMIT;
                c.price = px;
                b.limits++;
            }
            final long orderId = e.getNextOrderId() <= 0 ? 1L : e.getNextOrderId();
            sink.lastReject = null;
            e.orders().applyCreate(c, now);
            if (sink.lastReject != null || e.orders().orders().get(orderId) == null) {
                fail("create " + i + " (user " + user + " market " + market + " side " + side + " type " + c.orderType
                        + ") must be admitted; rejected " + sink.lastReject);
            }
            if (!armed) {
                final long leg = LEG_BASE + orderId;
                e.orders().applyOutcome(++egressSeq, orderId, 0L, user, orderId, 0L, 0L, 0L, 0L, market, 0, 0, leg, 0, now, 0L, now);
                assertEquals("order " + orderId + " learned its leg from APPLIED", leg, e.orders().orders().get(orderId).currentLegId);
                b.legs++;
            }
        }
        return b;
    }

    private static void deposit(final AssetsEngine e, final long user, final int asset, final long amount, final long now) {
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setUserId(user);
        d.setAssetId(asset);
        d.setAmount(amount);
        e.applyCommand(AssetsEngine.CMD_DEPOSIT, d, now);
    }

    // ---------------------------------------------------------------- checks

    private static int armedStops(final AssetsEngine e) {
        int n = 0;
        for (int m = 1; m <= OrderEngine.MARKETS; m++) {
            n += e.orders().armedStopCount(m);
        }
        return n;
    }

    private static int deadlines(final AssetsEngine e, final int kind) {
        final int[] n = new int[1];
        e.deadlines().forEach((d, id, k) -> {
            if (k == kind) {
                n[0]++;
            }
        });
        return n[0];
    }

    private static long[] lockedTotals(final AssetsEngine e) {
        final long[] t = new long[Asset.count()];
        e.forEachAccount(a -> {
            for (int i = 0; i < t.length; i++) {
                t[i] += a.locked(i);
            }
        });
        return t;
    }

    /** I2 per (account, asset): locked == sum of the hold remainders; returns the hold count. */
    private static int holdsAndCheckLocked(final AssetsEngine e, final String which) {
        final int[] holds = new int[1];
        final long[] sum = new long[Asset.count()];
        e.forEachAccount(a -> {
            Arrays.fill(sum, 0L);
            a.forEachHold((orderId, assetId, remaining, oms) -> {
                sum[assetId] += remaining;
                holds[0]++;
            });
            for (int i = 0; i < sum.length; i++) {
                if (a.locked(i) != sum[i]) {
                    fail(which + ": locked != sum(holds) for user " + a.userId() + " asset " + i + ": " + a.locked(i) + " vs " + sum[i]);
                }
            }
        });
        return holds[0];
    }

    /** I4 shape: each live order has one hold keyed by its orderId, in its hold asset, with remaining == holdAmount. */
    private static void everyLiveOrderOwnsOneHold(final AssetsEngine e) {
        e.orders().orders().forEachByOrderId(o -> {
            final long remaining = e.account(o.userId).holdRemaining(o.orderId);
            if (remaining != o.holdAmount) {
                fail("order " + o.orderId + ": hold remaining " + remaining + " != holdAmount " + o.holdAmount);
            }
        });
    }

    /** A sink that records only the last order reject and admin result (the build checks every create). */
    private static final class QuietSink implements AssetsEventSink {
        CoreRejectReason lastReject;
        AdminResult lastAdmin;

        public void onDepositAck(long c, long u, int a, long amt, long n, boolean dup) { }
        public void onWithdrawAck(long c, long u, int a, long amt, long n, boolean dup) { }
        public void onHoldAck(long c, long o, long u, int a, long amt) { }
        public void onHoldReject(long c, long o, long u, int a, long amt, RejectReason r) { }
        public void onBalanceUpdate(long u, int a, long av, long lk) { }
        public void onSettlementApplied(long t, long b, long s) { }
        public void onWithdrawReject(long c, long u, int a, long amt, RejectReason r) { }
        public void onBalanceSnapshotEnd(long c, int n) { }
        public void onHoldSnapshotEntry(long o, long u, int a, long r) { }
        public void onHoldSnapshotEnd(long c, int n) { }
        public void onFeedPositionReport(long c, long p, long t, boolean j) { }
        public void onSettleFault(long t, long o, long u, int a, long d, long un) { }
        public void onSettleRejected(long t, int m, long p, com.openexchange.assets.domain.SettleRejectReason r, long d) { }
        public void onOrderReject(long c, long rh, long rl, long u, long o, int m, int ref, CoreRejectReason r, int me, long d, long t) {
            lastReject = r;
        }
        public void onAdminAck(long c, int ref, AdminResult r, int reason, long cfg) {
            lastAdmin = r;
        }
        public void onAdmissionState(int m, GateState gs, GateReason gr, boolean b, long at, long lp, boolean o) { }
        public void onOrderAck(long c, long rh, long rl, long o, long u, int m, com.openexchange.assets.domain.OrderStatus st, boolean dup,
                               int ha, long hm, long at, long seq) { }
        public void onOrderEvent(long seq, long lp, com.openexchange.assets.domain.CoreOrderView o, com.openexchange.assets.domain.EventKind k,
                                 CoreRejectReason r, long lpx, long lq, long t, long hr, long up) { }
        public void onMeDispatch(long ds, long ih, long il, com.openexchange.assets.domain.DispatchKind k, long u, long o, long old, int m,
                                 int ot, int sd, long px, long q, long b, int tif) { }
        public void onDepositReject(long c, long rh, long rl, long u, int a, long amt, RejectReason r) { }
        public void onConfigApplied(com.openexchange.assets.domain.ConfigKind k, int m, long v, long lp) { }
    }

    @Test
    public void quietSinkSeesRejects() {
        // guard on the guard: the build's admission check must be able to see a refusal
        final AssetsEngine e = new AssetsEngine();
        final QuietSink sink = new QuietSink();
        e.setEventSink(sink);
        final CreateOrderCommand c = new CreateOrderCommand();
        c.reset();
        c.userId = 1;
        c.marketId = 1;
        c.price = ONE;
        c.quantity = ONE;
        c.requestIdLow = 1;
        assertNull(sink.lastReject);
        e.orders().applyCreate(c, START_MS);
        assertEquals("genesis gate is closed", CoreRejectReason.GATE_CLOSED, sink.lastReject);
    }
}

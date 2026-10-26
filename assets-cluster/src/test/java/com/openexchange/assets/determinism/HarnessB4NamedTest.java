// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.order.MarketRisk;
import com.openexchange.assets.application.engine.order.RequestTable;
import com.openexchange.assets.application.engine.order.UserRisk;
import com.openexchange.cluster.testkit.ScenarioText;
import com.openexchange.cluster.testkit.SnapshotSpliceRunner;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The six named RED-list rows of harness B4 (spec/harness.md B4 table), on the real DSL. Each row is run as A+B and
 * as A+SNAPSHOT+B (events and final enveloped image byte-identical), and the whole A+B also goes through the kit's
 * every-cut {@link SnapshotSpliceRunner}; each row then asserts its own decision on the uninterrupted run.
 *
 * <p>Adaptations to the real core, recorded where they differ from the spec text: orderIds are minted in sequence,
 * so "scrambled orderId order" is scrambled USER order with interleaved cancels; a MarketTick is a full top of book
 * (0 = that side is empty), so the one-sided row checks the retained other side and the stop evaluation, not a delta
 * merge; the rejected-create row separates pre-rate rejects (byte-identical) from a balance reject, which by the risk
 * spec creates a UserRisk record and a stored decision but never an Account.</p>
 */
public class HarnessB4NamedTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final long ONE = 100_000_000L;

    // ---------------------------------------------------------------- 1. expiry order

    /** 40 GTD orders on one deadline (resting legs and armed stops), users scrambled, every third cancelled. */
    @Test
    public void expiryOrderIsAFunctionOfStateNotHistory() {
        final List<String> a = start();
        final int n = 40;
        final long[] users = new long[n];
        for (int i = 0; i < n; i++) {
            users[i] = 1_000L + ((i * 17L) % n) * 3L; // a permutation of 40 user ids, never ascending
        }
        for (int i = n - 1; i >= 0; i--) {
            a.add("DEPOSIT u=" + users[i] + " asset=0 amt=1000.0");
            a.add("DEPOSIT u=" + users[i] + " asset=1 amt=5.0");
        }
        final long exp = T0 + 5_000;
        for (int i = 0; i < n; i++) {
            final int orderId = i + 1;
            a.add(orderId % 5 == 0
                    ? "CREATE u=" + users[i] + " market=1 side=1 type=3 qty=1.0 stop=50.0 tif=3 exp=" + exp + " rid=" + orderId
                    : "CREATE u=" + users[i] + " market=1 side=0 type=0 px=" + (100 + i % 7) + ".0 qty=1.0 tif=3 exp=" + exp
                    + " rid=" + orderId);
        }
        for (int k = 0; k < n; k++) {
            final int orderId = 1 + (k * 13) % n; // outcomes in a scrambled order
            if (orderId % 5 != 0) {
                a.add("MEOUTCOME order=" + orderId + " kind=0 attempt=0 result=0 leg=" + (5_000 + orderId));
            }
        }
        for (int orderId = 3; orderId <= n; orderId += 3) {
            a.add("CANCEL u=" + users[orderId - 1] + " order=" + orderId + " rid=" + (100 + orderId));
            if (orderId % 5 != 0) {
                a.add("MEOUTCOME order=" + orderId + " kind=1 attempt=1 result=0");
            }
        }
        final List<String> b = new ArrayList<>(List.of("TICK 6000"));
        for (int k = 0; k < n; k++) {
            final int orderId = 1 + (k * 7) % n;
            if (orderId % 3 != 0 && orderId % 5 != 0) {
                b.add("MEOUTCOME order=" + orderId + " kind=1 attempt=1 result=0");
            }
        }
        final List<String> events = assertSnapshotTransparent(a, b);
        // the sweep's own output (the TICK apply): a cancel dispatch per resting survivor and the EXPIRED of each armed
        // one, in (deadline, orderId) order whatever the history
        final List<String> sweep = eventsOf(concat(a, List.of("TICK 6000")));
        final List<Long> swept = new ArrayList<>();
        for (final String e : sweep.subList(eventsOf(a).size(), sweep.size())) {
            if ((e.startsWith("MEDISPATCH") && e.contains(" kind=CANCEL ")) || (e.startsWith("ORDEREVENT") && e.contains(" kind=EXPIRED "))) {
                swept.add(field(e, "order"));
            }
        }
        assertEquals("one expiry per surviving order", survivors(n), swept.size());
        for (int i = 1; i < swept.size(); i++) {
            assertTrue("expiry order is the index order " + swept, swept.get(i - 1) < swept.get(i));
        }
        assertEquals(survivors(n), count(events, " kind=EXPIRED "));
    }

    private static int survivors(final int n) {
        return n - n / 3;
    }

    // ---------------------------------------------------------------- 2. stop trigger order

    /** 8 BUY stops at one stop price from 8 scrambled users plus 3 trailing BUYs; two stops cancelled; one tick fires all. */
    @Test
    public void stopTriggerOrderAtOnePriceIsAFunctionOfState() {
        final List<String> a = start();
        final long[] users = {470, 130, 610, 220, 390, 550, 160, 300, 710, 440, 280};
        for (final long u : users) {
            a.add("DEPOSIT u=" + u + " asset=0 amt=100000.0");
            a.add("DEPOSIT u=" + u + " asset=1 amt=10.0");
        }
        a.add("MARKETTICK market=1 bid=99.0 ask=100.0 ver=1");
        int rid = 0;
        for (int i = 0; i < users.length; i++) {
            final long u = users[i];
            a.add(i == 2 || i == 6 || i == 9
                    ? "CREATE u=" + u + " market=1 side=0 type=5 qty=1.0 delta=2.0 rid=" + (++rid)  // trailing BUY, armed at 100
                    : "CREATE u=" + u + " market=1 side=0 type=3 qty=1.0 stop=104.0 rid=" + (++rid));
        }
        a.add("CREATE u=130 market=1 side=1 type=4 px=94.0 qty=1.0 stop=95.0 rid=" + (++rid)); // SELL stop that must not fire
        a.add("CANCEL u=220 order=4 rid=" + (++rid));
        a.add("CANCEL u=300 order=8 rid=" + (++rid));
        final List<String> b = List.of("MARKETTICK market=1 bid=104.0 ask=105.0 ver=2");
        final List<String> events = assertSnapshotTransparent(a, b);
        final List<Long> dispatched = new ArrayList<>();
        for (final String e : events.subList(eventsOf(a).size(), events.size())) {
            if (e.startsWith("MEDISPATCH") && e.contains(" kind=CREATE ")) {
                dispatched.add(field(e, "order"));
            }
        }
        assertEquals("6 stops and 3 trailing orders fire, in ascending orderId", List.of(1L, 2L, 3L, 5L, 6L, 7L, 9L, 10L, 11L),
                dispatched);
    }

    // ---------------------------------------------------------------- 3. rate limit

    /** 99 creates in one second, restart, the 100th passes and the 101st is RATE_LIMIT; the next second admits again. */
    @Test
    public void rateLimitDecisionSurvivesRestore() {
        final List<String> a = start();
        a.add("DEPOSIT u=1 asset=0 amt=1000000.0");
        for (int i = 1; i <= 99; i++) {
            a.add(create(1, i));
        }
        final List<String> b = List.of(create(1, 100), create(1, 101), "TICK 1000", create(1, 102));
        final List<String> events = assertSnapshotTransparent(a, b);
        assertEquals(1, count(events, " reason=RATE_LIMIT "));
        assertEquals("99 + the 100th + the next second's", 101, count(events, " kind=ACCEPTED "));
        final List<String> tail = events.subList(eventsOf(a).size(), events.size());
        assertTrue(tail.stream().anyMatch(e -> e.startsWith("ORDERREJECT") && e.contains(" reason=RATE_LIMIT ")));
    }

    private static String create(final long user, final int rid) {
        return "CREATE u=" + user + " market=1 side=0 type=0 px=100.0 qty=0.01 rid=" + rid;
    }

    // ---------------------------------------------------------------- 4. requestId replay

    /** Create, stored reject, amend and cancel decisions replay identically after a restart; a changed payload is refused. */
    @Test
    public void requestReplaySurvivesRestore() {
        final List<String> a = start();
        a.add("DEPOSIT u=1 asset=0 amt=1000.0");
        a.add("CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 rid=7");     // order 1
        a.add("CREATE u=1 market=1 side=0 type=0 px=100.0 qty=100.0 rid=8");   // INSUFFICIENT_FUNDS, stored
        a.add("MEOUTCOME order=1 kind=0 attempt=0 result=0 leg=11");
        a.add("AMEND u=1 order=1 px=101.0 rid=9");
        a.add("MEOUTCOME order=1 kind=2 attempt=1 result=0 leg=12");
        final List<String> b = List.of(
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 rid=7",     // duplicate ack for order 1
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=2.0 rid=7",     // REQUEST_ID_PAYLOAD_MISMATCH
                "DEPOSIT u=1 asset=0 amt=100000.0",
                "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=100.0 rid=8",   // the stored INSUFFICIENT_FUNDS stands
                "AMEND u=1 order=1 px=101.0 rid=9",                              // duplicate ack, no second top-up (G12)
                "AMEND u=1 order=1 px=102.0 rid=9",                              // mismatch
                "CANCEL u=1 order=1 rid=10",
                "CANCEL u=1 order=1 rid=10");                                    // duplicate ack, one cancel only
        final List<String> events = assertSnapshotTransparent(a, b);
        final List<String> tail = events.subList(eventsOf(a).size(), events.size());
        assertEquals(3, count(tail, " dup=1 "));
        assertEquals(2, count(tail, " reason=REQUEST_ID_PAYLOAD_MISMATCH "));
        assertEquals(1, count(tail, " reason=INSUFFICIENT_FUNDS "));
        assertEquals("one cancel dispatch", 1, count(tail, " kind=CANCEL "));
        assertEquals("no hold change from the replays", 1, count(tail, "BALANCE ")); // the deposit, nothing else
    }

    // ---------------------------------------------------------------- 5. rejected create

    /** Pre-rate rejects of a new user change no byte; a balance reject never creates the Account (K7). */
    @Test
    public void rejectedCreateLeavesBytesUnchanged() {
        final List<String> base = start();
        base.add("DEPOSIT u=1 asset=0 amt=1000.0");
        final byte[] s1 = bytesOf(base);
        final List<String> structural = new ArrayList<>(base);
        structural.addAll(List.of(
                "CREATE u=2 market=9 side=0 type=0 px=100.0 qty=1.0 rid=1",                       // UNKNOWN_MARKET
                "CREATE u=2 market=1 side=0 type=0 qty=1.0 rid=2",                                // INVALID: LIMIT without price
                "CREATE u=2 market=1 side=0 type=0 px=100.0 qty=1.0",                             // MISSING_REQUEST_ID
                "CREATE u=2 market=1 side=0 type=0 px=100.0 qty=1.0 tif=2 rid=3",                 // UNSUPPORTED_TIF
                "CREATE u=2 market=1 side=0 type=0 px=100.0 qty=1.0 tif=3 exp=" + T0 + " rid=4",  // EXPIRED at admission
                "CANCEL u=2 order=77 rid=5",                                                      // UNKNOWN_ORDER
                "AMEND u=2 order=77 px=100.0 rid=6",                                              // UNKNOWN_ORDER
                "WITHDRAW u=2 asset=0 amt=1.0"));                                                 // no account to debit
        assertArrayEquals("a pre-rate reject or a refused money command changes no byte", s1, bytesOf(structural));
        assertEquals(7, count(eventsOf(structural), "ORDERREJECT "));

        final List<String> funds = new ArrayList<>(base);
        funds.add("CREATE u=2 market=1 side=0 type=0 px=100.0 qty=1.0 rid=7");                  // INSUFFICIENT_FUNDS
        final AssetsScenarioRunner r = runner(funds);
        assertNull("a rejected create never creates the ledger account", r.engine().account(2));
        assertNotNull("the risk spec (risk.md 183) creates the UserRisk record at the rate step", r.engine().orders().user(2));
        final byte[] s2 = r.snapshotBytes();
        assertEquals("the image grows by one UserRisk record and one stored decision, not by an Account",
                UserRisk.SNAPSHOT_BYTES + RequestTable.ENTRY_BYTES, s2.length - s1.length);
        assertSnapshotTransparent(base, funds.subList(base.size(), funds.size()));
    }

    // ---------------------------------------------------------------- 6. one-sided tick

    /** A tick with one empty side keeps the side it carries; an empty side never fires a stop watching it. */
    @Test
    public void oneSidedTickDoesNotClearTheOtherSide() {
        final List<String> a = start();
        a.add("DEPOSIT u=1 asset=0 amt=100000.0");
        a.add("DEPOSIT u=2 asset=1 amt=10.0");
        a.add("MARKETTICK market=1 bid=100.0 ask=101.0 ver=1");
        a.add("CREATE u=2 market=1 side=1 type=3 qty=1.0 stop=99.5 rid=1");   // order 1: SELL stop below the bid
        a.add("CREATE u=1 market=1 side=0 type=3 qty=1.0 stop=103.0 rid=2");  // order 2: BUY stop above the ask
        final List<String> b1 = List.of("MARKETTICK market=1 bid=0 ask=102.0 ver=2"); // the bid side is empty
        final AssetsScenarioRunner afterOneSided = runner(concat(a, b1));
        final MarketRisk m = afterOneSided.engine().orders().market(1);
        assertEquals("the ask the tick carries is applied", 102 * ONE, m.bestAsk);
        assertEquals("the empty side is recorded as empty", 0L, m.bestBid);
        assertEquals("bid 0 is no bid: the SELL stop at 99.5 does not fire", 0, count(afterOneSided.renderedEvents(), " kind=TRIGGERED "));
        assertEquals(2, afterOneSided.engine().orders().armedStopCount(1));

        final List<String> b = new ArrayList<>(b1);
        b.add("CREATE u=1 market=1 side=0 type=1 qty=1.0 rid=3");             // MARKET BUY prices off the retained ask
        b.add("MARKETTICK market=1 bid=0 ask=103.0 ver=3");                    // fires the BUY stop only
        b.add("MARKETTICK market=1 bid=99.0 ask=0 ver=4");                     // ask side empty: fires the SELL stop only
        final List<String> events = assertSnapshotTransparent(a, b);
        assertTrue("no NO_REFERENCE_PRICE after a one-sided tick", count(events, " reason=NO_REFERENCE_PRICE ") == 0);
        assertTrue("the market buy holds 102 x 1.05: " + events,
                events.stream().anyMatch(e -> e.startsWith("ORDERACK") && e.contains(" order=3 ") && e.contains(" hold=10710000000 ")));
        final List<Long> fired = new ArrayList<>();
        for (final String e : events) {
            if (e.startsWith("ORDEREVENT") && e.contains(" kind=TRIGGERED ")) {
                fired.add(field(e, "order"));
            }
        }
        assertEquals("BUY stop on ask 103, then SELL stop on bid 99", List.of(2L, 1L), fired);
    }

    // ---------------------------------------------------------------- shared

    private static List<String> start() {
        return new ArrayList<>(List.of("CLOCK " + T0, "GATE market=0 open=1"));
    }

    /** A+B and A+SNAPSHOT+B end with the same events and image; the kit splices A+B at every cut too. */
    private static List<String> assertSnapshotTransparent(final List<String> a, final List<String> b) {
        final List<String> plain = concat(a, b);
        final List<String> spliced = new ArrayList<>(a);
        spliced.add("SNAPSHOT");
        spliced.addAll(b);
        final AssetsScenarioRunner p = runner(plain);
        final AssetsScenarioRunner s = runner(spliced);
        assertEquals("events of A+B vs A+SNAPSHOT+B", p.renderedEvents(), s.renderedEvents());
        assertTrue("image of A+B vs A+SNAPSHOT+B", Arrays.equals(p.snapshotBytes(), s.snapshotBytes()));
        final SnapshotSpliceRunner.Report report = SnapshotSpliceRunner.run(AssetsReplica::new, plain);
        assertEquals(report.lines() + 1, report.cuts());
        return p.renderedEvents();
    }

    private static AssetsScenarioRunner runner(final List<String> lines) {
        final AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
        r.execAll(ScenarioText.executableLines(lines));
        return r;
    }

    private static List<String> eventsOf(final List<String> lines) {
        return runner(lines).renderedEvents();
    }

    private static byte[] bytesOf(final List<String> lines) {
        return runner(lines).snapshotBytes();
    }

    private static List<String> concat(final List<String> a, final List<String> b) {
        final List<String> all = new ArrayList<>(a);
        all.addAll(b);
        return all;
    }

    private static int count(final List<String> events, final String needle) {
        int c = 0;
        for (final String e : events) {
            if (e.contains(needle)) {
                c++;
            }
        }
        return c;
    }

    private static long field(final String event, final String key) {
        for (final String tok : event.split(" ")) {
            if (tok.startsWith(key + "=")) {
                return Long.parseLong(tok.substring(key.length() + 1));
            }
        }
        throw new AssertionError("no " + key + "= in " + event);
    }
}

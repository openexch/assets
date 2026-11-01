// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S10' 4.5 items 1, 2, 4, 6, 7, 8: the t1-core verbs of the halt drills against a real one-member AE cluster with the
 * T1 key layout ({@link FeedHarnessClusterTest}'s rig). The halt is a real FeedHalt from a BRIDGE session; the "ME" that
 * answers a dispatch is a BRIDGE session that sends the journal entry a correct ME would have journaled.
 */
public class S10pVerbsClusterTest {

    /** Identity of this test's BRIDGE session (the pump's is 0xB1: two live lanes, two identities). */
    static final long LANE = 0xB7L;

    private final FeedHarnessClusterTest rig = new FeedHarnessClusterTest();
    private T1Session bridge;

    @After
    public void tearDown() {
        if (bridge != null) {
            bridge.close();
        }
        rig.tearDown();
    }

    static FeedHarnessClusterTest.Out t1(final Map<String, String> env, final String... args) {
        return FeedHarnessClusterTest.t1(env, args);
    }

    /** genesis, the whole story, await-bound, open, oracle PASS. */
    static FeedStory.Story fullStory(final Map<String, String> env, final Path dir) throws Exception {
        FeedHarnessClusterTest.storyLoop(env, dir);
        return FeedStory.read(dir);
    }

    T1Session bridge(final Map<String, String> env) throws Exception {
        bridge = T1Session.connect(env, "T1_BRIDGE_KEY");
        bridge.offer(new CoreFrames().register(0x77_01L, SessionClass.BRIDGE, LANE, 0));
        assertEquals("the test lane registers", 0, bridge.reply(0x77_01L, 110)[8 + 10] & 0xFF);
        return bridge;
    }

    private static final java.util.concurrent.atomic.AtomicLong QUERY = new java.util.concurrent.atomic.AtomicLong(0x17_000L);

    /** {meEpoch, cursor, consumePosition} from a FeedPositionReport on the bridge session. */
    static long[] position(final T1Session s) {
        final long corr = QUERY.incrementAndGet();
        s.offer(new CoreFrames().query(corr));
        final FeedPositionReportDecoder d = new FeedPositionReportDecoder().wrapAndApplyHeader(new UnsafeBuffer(s.reply(corr, 17)),
                0, new MessageHeaderDecoder());
        return new long[] {d.meEpoch(), d.lastAppliedJournalSeq(), d.consumePosition()};
    }

    static int ackResult(final byte[] ack) {
        return ack[8 + 10] & 0xFF;
    }

    static String line(final String text, final String prefix) {
        for (final String l : text.split("\n")) {
            if (l.startsWith(prefix)) {
                return l;
            }
        }
        throw new AssertionError("no line starting '" + prefix + "' in: " + text);
    }

    /**
     * A live, amendable and cancellable core order of the story's final state, read from the MODEL's order store (the dump
     * has no type or flags): a resting LIMIT / LIMIT_MAKER with a leg and a hold, nothing pending. {order, user, market,
     * leg, holdAsset, price, side}, or null.
     */
    static long[] liveOrder(final Path dir) throws Exception {
        final FeedStory.Story s = FeedStory.read(dir);
        final var model = T1Core.modelRunner(s, s.length(), List.of(), 0L, new long[1]);
        final int blocked = com.openexchange.assets.application.engine.order.CoreOrder.F_CANCEL_PENDING
                | com.openexchange.assets.application.engine.order.CoreOrder.F_REPLACE_PENDING
                | com.openexchange.assets.application.engine.order.CoreOrder.F_DISPATCH_PENDING
                | com.openexchange.assets.application.engine.order.CoreOrder.F_IOC
                | com.openexchange.assets.application.engine.order.CoreOrder.F_FOK
                | com.openexchange.assets.application.engine.order.CoreOrder.F_QUARANTINED;
        final long[][] found = {null};
        model.engine().orders().orders().forEachByOrderId(o -> {
            if (found[0] == null && (o.orderTypeCode() == 0 || o.orderTypeCode() == 2) && !o.isTerminal() && (o.flags & blocked) == 0
                    && o.remainingQty > 0 && o.currentLegId > 0 && o.holdAmount > 0) {
                found[0] = new long[] {o.orderId, o.userId, o.marketId, o.currentLegId, o.holdAssetId, o.price, o.sideCode()};
            }
        });
        return found[0];
    }

    // ------------------------------------------------------------------ items 1 and 2 under a real halt

    @Test
    public void probesInvariantsAndMoneyDuringAHalt() throws Exception {
        final Map<String, String> env = rig.cluster(1, 24_000, T1Keys.table(true));
        final Path dir = FeedHarnessClusterTest.story(21);
        final FeedStory.Story s = fullStory(env, dir);
        // invariantsPassOnTheStoryModel / invariantsFailOnABrokenLocked (the 4.3 RED)
        FeedHarnessClusterTest.Out o = t1(env, "t1-core", "invariants");
        assertEquals(o.text(), 0, o.code());
        assertTrue(o.text(), o.text().contains("INVARIANTS PASS rows="));
        o = t1(env, "t1-core", "invariants", "--seam", "BREAK_LOCKED");
        assertEquals("the broken locked FAILs: " + o.text(), 1, o.code());
        assertTrue("names the user and asset: " + o.text(), o.text().matches("(?s).*INVARIANTS FAIL user=\\d+ asset=\\d+ .*"));
        final long[] live = liveOrder(dir);
        assertTrue("the story leaves an amendable order", live != null);
        // a real halt: this lane's FeedHalt GAP for a hole above the cursor
        final T1Session b = bridge(env);
        final long n = s.length();
        b.offer(new CoreFrames().feedHalt(0x66_01L, 1, 0, n + 1, n + 3, LANE));
        assertEquals("FeedHalt GAP ACCEPTED", 0, ackResult(b.reply(0x66_01L, 110)));
        o = t1(env, "t1-core", "gate");
        assertTrue("SUSPENDED BRIDGE_HALT: " + o.text(), o.text().contains("SUSPENDED") && o.text().contains("BRIDGE_HALT"));
        // probeCreateIsRefusedGateClosedWhileLatched, probe-amend GATE_CLOSED, both state-free (the oracle still PASSes)
        o = t1(env, "t1-core", "probe-create", "1", "1");
        assertEquals(o.text(), 0, o.code());
        assertTrue("by name: " + o.text(), o.text().contains("OrderReject GATE_CLOSED"));
        o = t1(env, "t1-core", "probe-amend", Long.toString(live[0]));
        assertEquals(o.text(), 0, o.code());
        assertTrue("by name: " + o.text(), o.text().contains("OrderReject GATE_CLOSED"));
        o = t1(env, "t1-core", "oracle", dir.toString());
        assertEquals("the refused probes changed nothing: " + o.text(), 0, o.code());
        FeedHarnessClusterTest.assertLatch(o, "L true 1");
        // withdrawPrintsResultAndReasonByName: deposits and withdrawals keep flowing during the halt
        o = t1(env, "t1-core", "deposit", "1", "0", "5");
        assertTrue(o.text(), o.code() == 0 && o.text().contains("DEPOSIT template 15 result=ACCEPTED"));
        o = t1(env, "t1-core", "withdraw", "1", "0", "5");
        assertTrue(o.text(), o.code() == 0 && o.text().contains("WITHDRAW template 16 result=ACCEPTED"));
        o = t1(env, "t1-core", "invariants");
        final long available = Long.parseLong(line(o.text(), "I 1 0 ").split(" ")[3].substring("available=".length()));
        o = t1(env, "t1-core", "withdraw", "1", "0", Long.toString(available + 1));
        assertTrue("REJECTED by name: " + o.text(), o.code() == 0
                && o.text().contains("WITHDRAW template 14 result=REJECTED reason=INSUFFICIENT_FUNDS"));
        o = t1(env, "t1-core", "invariants");
        assertEquals("invariants during the halt: " + o.text(), 0, o.code());
        // C7: an unknown user, market or order is an ERR before anything is sent
        o = t1(env, "t1-core", "probe-create", "999", "1");
        assertTrue(o.text(), o.code() == FeedHarnessMain.USAGE && o.text().contains("ERR unknown user 999"));
        o = t1(env, "t1-core", "probe-create", "1", "9");
        assertTrue(o.text(), o.code() == FeedHarnessMain.USAGE && o.text().contains("ERR unknown market 9"));
        o = t1(env, "t1-core", "probe-cancel", "987654");
        assertTrue(o.text(), o.code() == FeedHarnessMain.USAGE && o.text().contains("ERR unknown order 987654"));
        // probeCreateAcceptedWhenOpen (the non-vacuity of the GATE_CLOSED line): 76 {GAP, 0}, OPEN, the same probe
        o = t1(env, "t1-core", "latch-clear", "1", "0");
        assertTrue(o.text(), o.text().contains("ref=76 result=ACCEPTED"));
        o = t1(env, "t1-core", "open");
        assertTrue(o.text(), o.text().contains("ref=73 result=ACCEPTED"));
        // at a resting order's own price and side (inside the collar), for that order's user and market
        o = t1(env, "t1-core", "probe-create", Long.toString(live[1]), Long.toString(live[2]), "--side", live[6] == 0 ? "buy" : "sell",
                "--price", Long.toString(live[5]));
        assertTrue("the same verb ACCEPTED when open: " + o.text(), o.text().contains("OrderAck order=") && o.text().contains("status="));
    }

    // ------------------------------------------------------------------ item 1: probe-cancel, the ME answers

    @Test
    public void probeCancelReleasesTheHoldWhenTheMeAnswers() throws Exception {
        final Map<String, String> env = rig.cluster(1, 24_300, T1Keys.table(true));
        final Path dir = FeedHarnessClusterTest.story(22);
        fullStory(env, dir);
        final long[] live = liveOrder(dir);
        assertTrue("the story leaves a live order with a hold and a leg", live != null);
        final Path ids = Files.createTempDirectory("s10p-dump").resolve("orders");
        Files.writeString(ids, live[0] + "\n");
        FeedHarnessClusterTest.Out o = t1(env, "t1-core", "dump", "--orders", ids.toString());
        assertTrue("the hold before: " + o.text(), o.text().contains("\nH " + live[0] + " ") || o.text().startsWith("H " + live[0] + " "));
        o = t1(env, "t1-core", "probe-cancel", Long.toString(live[0]));
        assertTrue("ACCEPTED: " + o.text(), o.text().contains("OrderAck order=" + live[0]));
        // the ME's answer: the leg's CANCELLED terminal at the next journalSeq
        final T1Session b = bridge(env);
        final long[] p = position(b);
        b.offer(new CoreFrames().terminal(p[2] + 1, p[1] + 1, p[0], live[3], live[1], (int) live[2], 3, FeedStory.T0, live[0], 0));
        final long deadline = System.currentTimeMillis() + 10_000;
        while (position(b)[1] < p[1] + 1) {
            assertTrue("the terminal applies", System.currentTimeMillis() < deadline);
            Thread.sleep(20);
        }
        // the cancelled order leaves the order store and its hold goes with it
        o = t1(env, "t1-core", "dump", "--orders", ids.toString());
        assertTrue("the order is gone (an ERR now): " + o.text(), o.code() == FeedHarnessMain.USAGE
                && o.text().contains("ERR unknown order " + live[0]));
        try (T1Session admin = T1Session.connect(env, "T1_ADMIN_KEY")) {
            for (final String l : CanonicalDump.of(admin)) {
                assertTrue("no O or H line left: " + l, !l.startsWith("O " + live[0] + " ") && !l.startsWith("H " + live[0] + " "));
            }
        }
        o = t1(env, "t1-core", "invariants");
        assertEquals("locked == sum(H) after the release: " + o.text(), 0, o.code());
    }

    // ------------------------------------------------------------------ item 6

    @Test
    public void dumpOrdersSubsetEqualsTheFullDumpLines() throws Exception {
        final Map<String, String> env = rig.cluster(1, 24_600, T1Keys.table(true));
        fullStory(env, FeedHarnessClusterTest.story(23));
        final List<String> full;
        try (T1Session admin = T1Session.connect(env, "T1_ADMIN_KEY")) {
            full = CanonicalDump.of(admin);
        }
        final List<Long> picked = new ArrayList<>();
        for (final String l : full) {
            if (l.startsWith("O ") && picked.size() < 3) {
                picked.add(Long.parseLong(l.split(" ")[1]));
            }
        }
        assertEquals("three orders in the story", 3, picked.size());
        final Path ids = Files.createTempDirectory("s10p-dump").resolve("orders");
        Files.writeString(ids, picked.get(0) + "\n" + picked.get(1) + "\n\n" + picked.get(2) + "\n");
        final FeedHarnessClusterTest.Out o = t1(env, "t1-core", "dump", "--orders", ids.toString());
        assertEquals(o.text(), 0, o.code());
        final java.util.Set<Long> users = new java.util.HashSet<>();
        for (final String l : full) {
            if (l.startsWith("O ") && picked.contains(Long.parseLong(l.split(" ")[1]))) {
                users.add(Long.parseLong(l.split(" ")[2]));
            }
        }
        final List<String> expected = new ArrayList<>();
        for (final String l : full) {
            final String[] f = l.split(" ");
            if (((f[0].equals("O") || f[0].equals("H")) && picked.contains(Long.parseLong(f[1])))
                    || (f[0].equals("B") && users.contains(Long.parseLong(f[1])))) {
                expected.add(l);
            }
        }
        final List<String> got = new ArrayList<>(List.of(o.text().trim().split("\n")));
        assertTrue(got.get(got.size() - 1), got.remove(got.size() - 1).equals("DUMP orders=3 lines=" + expected.size()));
        assertEquals("the subset is the full dump's lines, in its order", expected, got);
        Files.writeString(ids, "424242\n");
        assertEquals("an unknown order is an ERR", FeedHarnessMain.USAGE, t1(env, "t1-core", "dump", "--orders", ids.toString()).code());
    }

    // ------------------------------------------------------------------ item 2: the --extra writer

    @Test
    public void oracleWithExtraWithdrawAtRoundR() throws Exception {
        final Map<String, String> env = rig.cluster(1, 24_900, T1Keys.table(true));
        final Path dir = FeedHarnessClusterTest.story(24);
        final FeedStory.Story s = FeedStory.read(dir);
        final int r = 4;
        FeedHarnessClusterTest.Out o = t1(env, "t1-core", "genesis", "--story", dir.toString());
        assertEquals(o.text(), 0, o.code());
        o = t1(env, "t1-core", "run", dir.toString(), "--pump", "--to", Integer.toString(r));
        assertEquals(o.text(), 0, o.code());
        final Path extra = dir.resolve("extra.flog");
        o = t1(env, "t1-core", "withdraw", "2", "0", "777", "--record", extra.toString());
        assertTrue(o.text(), o.text().contains("result=ACCEPTED") && o.text().contains("RECORDED " + extra));
        final String upto = Long.toString(s.barriers()[r]);
        o = t1(env, "t1-core", "oracle", dir.toString(), "--upto", upto, "--extra", extra.toString());
        assertEquals("the model applies the recorded withdraw after round " + r + ": " + o.text(), 0, o.code());
        o = t1(env, "t1-core", "oracle", dir.toString(), "--upto", upto);
        assertEquals("without --extra: FAIL", 1, o.code());
        o = t1(env, "t1-core", "deposit", "2", "0", "777");
        assertTrue(o.text(), o.text().contains("result=ACCEPTED"));
        o = t1(env, "t1-core", "run", dir.toString(), "--pump", "--from", Integer.toString(r + 1));
        assertEquals(o.text(), 0, o.code());
        o = t1(env, "t1-core", "oracle", dir.toString());
        assertEquals("withdraw then re-deposit: the final oracle without --extra PASSes: " + o.text(), 0, o.code());
        FeedHarnessClusterTest.assertLatch(o, "L false 0");
    }

    // ------------------------------------------------------------------ items 7 and 8: gate --watch, suspend, the 4R core

    /** An in-process {@code t1-core gate --watch} whose output the test can read while it runs. */
    static final class Watch implements AutoCloseable {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final Thread thread;
        volatile int code = -1;

        Watch(final Map<String, String> env, final int seconds) {
            final PrintStream p = new PrintStream(bytes, true, StandardCharsets.UTF_8);
            thread = new Thread(() -> {
                try {
                    code = T1Core.run(new String[] {"t1-core", "gate", "--watch", "1", "--until", Integer.toString(seconds)}, env, p, p);
                } catch (Exception e) {
                    p.println("internal: " + e);
                    code = FeedHarnessMain.INTERNAL;
                }
            }, "t1-gate-watch");
            thread.start();
        }

        String text() {
            synchronized (bytes) {
                return bytes.toString(StandardCharsets.UTF_8);
            }
        }

        void awaitLine(final String part) throws InterruptedException {
            final long deadline = System.currentTimeMillis() + 30_000;
            while (!text().contains(part)) {
                assertTrue("gate --watch printed '" + part + "' within 30 s: " + text(), System.currentTimeMillis() < deadline);
                Thread.sleep(10);
            }
        }

        /** The transition lines (not INITIAL, not the end line). */
        List<String> transitions() {
            final List<String> out = new ArrayList<>();
            for (final String l : text().split("\n")) {
                if (l.startsWith("t=")) {
                    out.add(l);
                }
            }
            return out;
        }

        @Override
        public void close() throws InterruptedException {
            thread.join(60_000);
        }
    }

    /** The last watch's transition lines (set by {@link #runWatchScenario}). */
    private List<String> lastTransitions = List.of();
    private Map<String, String> rigEnv;

    /**
     * suspend (ADMIN), a FeedHalt GAP (+BRIDGE_HALT), the entry that reaches it (the 4R reopen clears BRIDGE_HALT only:
     * ADMIN stays, a probe is still refused), open: four transitions, each awaited on the CORE (never on the watch under
     * test, so a dropped line cannot stall the scenario).
     */
    void runWatchScenario(final int portBase) throws Exception {
        final Map<String, String> env = rig.cluster(1, portBase, T1Keys.table(true));
        rigEnv = env;
        final FeedStory.Story s = fullStory(env, FeedHarnessClusterTest.story(25));
        final T1Session b = bridge(env);
        final long[] p = position(b);
        assertEquals(s.length(), p[1]);
        final Watch w = new Watch(env, 15);
        try {
            w.awaitLine("INITIAL ");
            assertTrue(w.text(), line(w.text(), "INITIAL ").contains("state=OPEN reasons=0"));
            FeedHarnessClusterTest.Out o = t1(env, "t1-core", "suspend");
            assertTrue(o.text(), o.text().contains("ref=73 result=ACCEPTED"));
            awaitTransitions(w, 1);
            b.offer(new CoreFrames().feedHalt(0x66_11L, 1, 0, p[1] + 1, p[1] + 2, LANE));
            assertEquals("FeedHalt GAP ACCEPTED", 0, ackResult(b.reply(0x66_11L, 110)));
            awaitTransitions(w, 2);
            // the entry at c + 1 = seen - 1: the reach-clear; the 4R reopen clears BRIDGE_HALT, the operator's ADMIN stays
            b.offer(new CoreFrames().engineConfig(p[2] + 1, p[1] + 1, p[0], 1_000, 2, 4096, 12, 3));
            awaitTransitions(w, 3);
            o = t1(env, "t1-core", "probe-create", "1", "1");
            assertTrue("ADMIN still holds the gate after the reopen: " + o.text(), o.text().contains("OrderReject GATE_CLOSED"));
            o = t1(env, "t1-core", "open");
            assertTrue(o.text(), o.text().contains("ref=73 result=ACCEPTED"));
            awaitTransitions(w, 4);
        } finally {
            w.close();
        }
        assertTrue("the watch ends at --until: " + w.text(), w.text().contains("WATCH end lines="));
        assertEquals(w.text(), 0, w.code);
        lastTransitions = w.transitions();
    }

    /** Waits until the core has made {@code n} transitions (read from the core, never from the watch under test). */
    private void awaitTransitions(final Watch w, final int n) throws InterruptedException {
        final String[] states = {"names=ADMIN ", "names=ADMIN,BRIDGE_HALT ", "names=ADMIN ", "state=OPEN reasons=0 "};
        final long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            final FeedHarnessClusterTest.Out o = t1(rigEnv, "t1-core", "gate");
            if (o.text().contains(n == 4 ? "\"gateReasons\":0" : n == 2 ? "\"gateReasons\":5" : "\"gateReasons\":1")) {
                Thread.sleep(300); // the broadcast reaches the watch's session within the same egress drain
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("the core did not reach transition " + n + " (" + states[n - 1] + "): " + w.text());
    }

    static void assertFourTransitions(final List<String> t) {
        assertEquals("line count " + t.size(), 4, t.size());
        assertTrue(t.get(0), t.get(0).contains("state=SUSPENDED reasons=1 names=ADMIN "));
        assertTrue(t.get(1), t.get(1).contains("state=SUSPENDED reasons=5 names=ADMIN,BRIDGE_HALT "));
        assertTrue("the reopen keeps ADMIN: " + t.get(2), t.get(2).contains("state=SUSPENDED reasons=1 names=ADMIN "));
        assertTrue(t.get(3), t.get(3).contains("state=OPEN reasons=0 names=- "));
        long prev = 0;
        for (final String l : t) {
            final long ms = Long.parseLong(l.substring(2, l.indexOf(' ')));
            assertTrue("monotonic t: " + t, ms >= prev);
            prev = ms;
        }
    }

    @Test
    public void gateWatchPrintsEveryTransition() throws Exception {
        runWatchScenario(25_200);
        assertFourTransitions(lastTransitions);
    }

    @Test
    public void suspendThenAutoReopenKeepsAdmin() throws Exception {
        runWatchScenario(25_500); // the probe after the reopen answered GATE_CLOSED inside
        assertTrue("{ADMIN, BRIDGE_HALT} -> {ADMIN}: " + lastTransitions, lastTransitions.size() >= 3
                && lastTransitions.get(1).contains("reasons=5 ") && lastTransitions.get(2).contains("state=SUSPENDED reasons=1 names=ADMIN "));
    }

    @Test
    public void gateWatchDroppedTransitionIsRed() {
        FeedHarnessNegativeControlsTest.red(FeedHarnessSeams.GATE_WATCH_DROPS_ONE, "line count 3", () -> {
            try {
                runWatchScenario(25_800);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            assertFourTransitions(lastTransitions);
        });
    }

    // ------------------------------------------------------------------ DUPLICATE_IDENTITY and the session timeout

    /** A fresh BRIDGE session (key 10) registering {@code identity}; returns {session, AdminAck result, reason}. */
    static Object[] registerBridge(final Map<String, String> env, final long identity, final long corr) throws Exception {
        final T1Session s = new T1Session(env.get("T1_AERON_DIR"), env.get("T1_CORE_INGRESS"),
                com.openexchange.assets.auth.CoreCredentials.fromSpec(T1Keys.BRIDGE1));
        s.offer(new CoreFrames().register(corr, SessionClass.BRIDGE, identity, 0));
        final byte[] ack = s.reply(corr, 110);
        return new Object[] {s, ack[8 + 10] & 0xFF, ack[8 + 11] & 0xFF};
    }

    /**
     * RC2 S10' S2-3 with the coordinator's Q1 answer (the bridge retries DUPLICATE_IDENTITY for up to 2 x the core session
     * timeout): a lane whose process died without closing its session holds its identity until the CLUSTER session timeout
     * (10 s, {@code InfrastructureConstants.SESSION_TIMEOUT_NS}) closes that session; a restarted lane is REJECTED
     * DUPLICATE_IDENTITY until then and ACCEPTED after it, with no operator step.
     */
    @Test
    public void duplicateIdentityIsFreedWhenTheDeadSessionTimesOut() throws Exception {
        final Map<String, String> env = rig.cluster(1, 26_100, T1Keys.table(true));
        final long identity = 0xC1L;
        // the lane that will "die": registered, then never polled again (no keepalive, no close): a kill -9
        final T1Session dead = T1Session.connect(env, "T1_BRIDGE_KEY");
        dead.offer(new CoreFrames().register(0x80_01L, SessionClass.BRIDGE, identity, 0));
        assertEquals("the first lane registers", 0, ackResult(dead.reply(0x80_01L, 110)));
        final long diedAt = System.currentTimeMillis();
        final List<String> attempts = new ArrayList<>();
        long acceptedAt = 0;
        long corr = 0x80_10L;
        try {
            while (System.currentTimeMillis() - diedAt < 30_000) {
                final Object[] r = registerBridge(env, identity, corr++);
                final T1Session s = (T1Session) r[0];
                final int result = (int) r[1];
                final int reason = (int) r[2];
                attempts.add((System.currentTimeMillis() - diedAt) + "ms:" + result + "/" + reason);
                if (result == 0) {
                    acceptedAt = System.currentTimeMillis();
                    s.close();
                    break;
                }
                assertEquals("REJECTED (2) while the dead session lives: " + attempts, 2, result);
                assertEquals("by name, DUPLICATE_IDENTITY (44): " + attempts,
                        com.openexchange.assets.domain.CoreRejectReason.DUPLICATE_IDENTITY.code(), reason);
                s.close();
                Thread.sleep(1_000);
            }
        } finally {
            dead.close();
        }
        System.out.println("[S10p] DUPLICATE_IDENTITY attempts after the lane died: " + attempts);
        assertTrue("the identity registers once the dead session times out (within 30 s): " + attempts, acceptedAt != 0);
        final long waited = acceptedAt - diedAt;
        assertTrue("the first attempts were refused (the rule held while the session lived): " + attempts,
                attempts.size() >= 2 && attempts.get(0).endsWith(":2/44"));
        assertTrue("freed by the 10 s session timeout, not earlier: " + waited + " ms " + attempts, waited >= 8_000);
        assertTrue("within 2 x the session timeout (the bridge's retry bound): " + waited + " ms", waited <= 20_000 + 2_000);
    }
}

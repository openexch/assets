// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.cluster.CoreRestoreRig.Encoders;
import com.openexchange.assets.cluster.CoreRestoreRig.Listener;
import com.openexchange.assets.cluster.CoreRestoreRig.Node;
import com.openexchange.assets.cluster.CoreRestoreRig.Session;
import com.openexchange.assets.cluster.CoreRestoreRig.Tee;
import com.openexchange.assets.domain.Account;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.TimeInForce;
import com.openexchange.cluster.SnapshotEnvelope;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.openexchange.assets.cluster.CoreRestoreRig.ONE;
import static com.openexchange.assets.cluster.CoreRestoreRig.OP_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S8, harness B7 tests 3 and 4 (RC1-A05 evidence), behind {@code -Dcore.restore.100k=true}. A single real node on
 * a 4m snapshot term (RC0 finding 10) is loaded with 100k open LIMIT orders over the wire from a GATEWAY session
 * (offers batched in a bounded in-flight window, egress polled), plus one GTD order whose leg is learned from a BRIDGE
 * outcome and whose deadline falls after the restart. A snapshot is forced (cluster control toggle), the node is
 * cold-restarted on the same dirs and the test measures, from the moment the restart begins:
 * {@code [SNAPSHOT] reassembled} (reassembleMs), {@code [SNAPSHOT] restored} (restoreMs) and the first successful round
 * trip, a {@code RequestOrderSnapshot} reply for one user (readyMs); all three are cumulative from t(start). Asserts:
 * ready within 30 s, the chunk count is {@code ceil(bytes / maxMessageLength)}, the restored node holds every order, I2
 * holds on a sample of accounts, and the GTD order is cancelled exactly once after the restart (one {@code DISPATCH
 * kind=CANCEL} to a forwarder that connected after it). The second method flips one byte inside the recorded snapshot
 * payload and asserts the node refuses to restore (CRC32C refusal through the failFast seam) and does not serve within
 * 10 s.
 */
public class CoreChunkedRestore100kClusterTest {

    private static final String SNAPSHOT_TERM = "4m";
    private static final int MAX_MESSAGE_LENGTH = 4 * 1024 * 1024 / 8;
    private static final int ORDERS_PER_USER = 10;
    private static final int WINDOW = 4_000;
    private static final long READY_BUDGET_MS = 30_000;
    private static final long GTD_WINDOW_MS = 30_000;
    private static final long QUIET_AFTER_MS = 3_000;
    private static final long LEG = 77;
    private static final Pattern TAKEN = Pattern.compile("\\[SNAPSHOT\\] accounts=(\\d+) bytes=(\\d+) chunks=(\\d+) maxMessageLength=(\\d+) serializeMs=(\\d+)");
    private static final Pattern REASSEMBLED = Pattern.compile("\\[SNAPSHOT\\] reassembled (\\d+) byte");

    @Before
    public void onlyWithTheFlag() {
        Assume.assumeTrue("the 100k cluster restore runs only with -Dcore.restore.100k=true", Boolean.getBoolean("core.restore.100k"));
    }


    /** State of the build phase, shared by both methods. */
    private static final class Loaded {
        Node node;
        final List<Session> sessions = new ArrayList<>();
        long gtdOrderId;
        long gtdExpiresAtMs;
        long snapshotRecordingId;
        String takenLine;
        String buildLine;
        File archiveDir;
    }

    @Test(timeout = 600_000)
    public void oneHundredThousandOrdersRestoreFromAChunkedSnapshotWithinThirtySeconds() throws Exception {
        runRestore(100_000);
    }

    @Test(timeout = 300_000)
    public void aCorruptedSnapshotByteIsRefusedAndTheNodeNeverServes() throws Exception {
        final File tmp = tmp("corrupt");
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");
        final PrintStream out = System.out;
        final PrintStream err = System.err;
        final Tee tee = new Tee(out, "[SNAPSHOT]");
        final Tee errTee = new Tee(err, "snapshot");
        final AtomicInteger stops = new AtomicInteger();
        Loaded l = null;
        Node node = null;
        Session probe = null;
        try {
            System.setOut(tee);
            System.setErr(errTee);
            // 5,000 orders: a multi-chunk image (about 1.9 MB on 512 KiB messages) is enough for the CRC path
            l = load(aeronDir, baseDir, 5_000, tee, stops);
            final long recordingId = l.snapshotRecordingId;
            closeAll(l);
            l.node.close();
            l.node = null;
            final long flippedAt = flipOnePayloadByte(l.archiveDir, recordingId);
            System.out.println("[TEST] flipped one byte of snapshot recording " + recordingId + " at file offset " + flippedAt);

            tee.clear();
            errTee.clear();
            final long t0 = System.currentTimeMillis();
            node = CoreRestoreRig.boot(baseDir, aeronDir, stops::incrementAndGet);
            final long until = t0 + 10_000; // one round trip is attempted for 10 s: there must be none
            boolean served = false;
            try {
                final Listener in = new Listener();
                probe = new Session(CoreRestoreRig.connect(aeronDir, SessionClass.GATEWAY, in, until, 100), in);
                final List<Session> all = List.of(probe);
                final Encoders enc = new Encoders();
                CoreRestoreRig.register(probe, SessionClass.GATEWAY, false, false, all);
                CoreRestoreRig.offer(probe, enc.buf, enc.orderSnapshot(0xC0L, 1), all);
                CoreRestoreRig.await(all, () -> in.snapshotEnd(0xC0L) != null, until, "no reply");
                served = true;
            } catch (final IllegalStateException | AssertionError expected) {
                // no connect, no registration ack or no reply before the deadline: the node does not serve
            }
            assertTrue("the restart must not serve a round trip within 10 s on a corrupted snapshot", !served);
            assertTrue("the failFast seam fires on the refused restore (got " + stops.get() + "), stderr: " + errTee.lines,
                    stops.get() >= 1);
            final int crc = errTee.first("CRC32C mismatch");
            assertTrue("the refusal names the CRC32C mismatch; stderr lines: " + errTee.lines, crc >= 0);
            assertEquals("no [SNAPSHOT] restored line on a refused image; stdout: " + tee.lines, -1, tee.first("[SNAPSHOT] restored"));
            System.out.println("[RESTORE-CORRUPT] refused=true failFast=" + stops.get() + " servedWithin10s=false reason="
                    + errTee.lines.get(crc).trim());
        } finally {
            System.setOut(out);
            System.setErr(err);
            if (probe != null) {
                CloseHelper.quietClose(probe.client);
            }
            shutdown(l, node, tmp);
        }
    }

    private void runRestore(final int orders) throws Exception {
        final File tmp = tmp("100k");
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");
        final PrintStream out = System.out;
        final Tee tee = new Tee(out, "[SNAPSHOT]");
        final AtomicInteger stops = new AtomicInteger();
        Loaded l = null;
        Node node = null;
        final List<Session> after = new ArrayList<>();
        try {
            System.setOut(tee);
            l = load(aeronDir, baseDir, orders, tee, stops);
            final Matcher taken = TAKEN.matcher(l.takenLine);
            assertTrue("snapshot line format: " + l.takenLine, taken.find());
            final long bytes = Long.parseLong(taken.group(2));
            final int chunks = Integer.parseInt(taken.group(3));
            final int maxMessage = Integer.parseInt(taken.group(4));
            assertEquals("the snapshot publication runs on the 4m term (maxMessageLength = term / 8)", MAX_MESSAGE_LENGTH, maxMessage);
            assertEquals("chunk count = ceil(bytes / maxMessageLength)", (bytes + maxMessage - 1) / maxMessage, chunks);
            final long recorded = recordedSnapshotBytes(l);
            final long gtdOrderId = l.gtdOrderId;
            final long expiresAt = l.gtdExpiresAtMs;
            closeAll(l);
            l.node.close();
            l.node = null;
            tee.clear();

            // ---- cold restart on the same dirs; every milestone is measured from here
            final long t0 = System.nanoTime();
            node = CoreRestoreRig.boot(baseDir, aeronDir, stops::incrementAndGet);
            final Listener gin = new Listener();
            final Session gw = new Session(CoreRestoreRig.connect(aeronDir, SessionClass.GATEWAY, gin,
                    System.currentTimeMillis() + 60_000, 20), gin);
            after.add(gw);
            final Encoders enc = new Encoders();
            CoreRestoreRig.register(gw, SessionClass.GATEWAY, false, false, after);
            CoreRestoreRig.offer(gw, enc.buf, enc.orderSnapshot(0x51L, 1), after);
            CoreRestoreRig.await(after, () -> gin.snapshotEnd(0x51L) != null, System.currentTimeMillis() + 60_000,
                    "the first RequestOrderSnapshot after the restart must be answered");
            final long[] first = gin.snapshotEnd(0x51L);
            final int ir = tee.first("[SNAPSHOT] reassembled");
            final int id = tee.first("[SNAPSHOT] restored");
            assertTrue("the restart logs [SNAPSHOT] reassembled; lines: " + tee.lines, ir >= 0);
            assertTrue("the restart logs [SNAPSHOT] restored; lines: " + tee.lines, id >= 0);
            final Matcher re = REASSEMBLED.matcher(tee.lines.get(ir));
            assertTrue(re.find());
            assertEquals("reassembled bytes == the bytes the snapshot offered", bytes, Long.parseLong(re.group(1)));
            final long reassembleMs = (tee.at.get(ir) - t0) / 1_000_000;
            final long restoreMs = (tee.at.get(id) - t0) / 1_000_000;
            final long readyMs = (first[3] - t0) / 1_000_000;
            System.out.println("[RESTORE] orders=" + (orders + 1) + " bytes=" + bytes + " chunks=" + chunks + " reassembleMs="
                    + reassembleMs + " restoreMs=" + restoreMs + " readyMs=" + readyMs);
            System.out.println("[RESTORE-DETAIL] recordedBytes=" + recorded + " maxMessageLength=" + maxMessage + " " + l.buildLine
                    + " taken=\"" + l.takenLine + "\" restored=\"" + tee.lines.get(id) + "\"");
            assertEquals("no failFast on a healthy restore", 0, stops.get());
            assertTrue("restore-to-ready <= " + READY_BUDGET_MS + " ms (RC1-A05 gate), took " + readyMs, readyMs <= READY_BUDGET_MS);
            assertEquals("user 1 has 10 LIMIT orders + the GTD order", ORDERS_PER_USER + 1, first[1]);
            assertEquals("one page answers it", 0L, first[2]);

            // ---- a wire sample of two more users, and in-JVM checks on the restored node
            final int users = orders / ORDERS_PER_USER;
            for (final long u : new long[] {users / 2, users}) {
                final long corr = 0x52L + u;
                CoreRestoreRig.offer(gw, enc.buf, enc.orderSnapshot(corr, u), after);
                CoreRestoreRig.await(after, () -> gin.snapshotEnd(corr) != null, System.currentTimeMillis() + OP_TIMEOUT_MS,
                        "RequestOrderSnapshot for user " + u);
                assertEquals("user " + u + " orders after the restore", ORDERS_PER_USER, gin.snapshotEnd(corr)[1]);
            }
            assertEquals("every order is live on the restored node", orders + 1, node.service.engine().orders().orders().size());
            for (final long u : new long[] {1, users / 2, users}) {
                assertLockedEqualsHolds(node.service.engine().account(u), u);
            }
            final CoreOrder gtd = node.service.engine().orders().orders().get(gtdOrderId);
            assertNotNull("the GTD order survives the restore", gtd);
            assertEquals("its leg survives the restore", LEG, gtd.currentLegId);

            // ---- the GTD order is cancelled exactly once, after the restart, at its deadline
            final Listener fin = new Listener();
            final Session fwd = new Session(CoreRestoreRig.connect(aeronDir, SessionClass.FORWARDER, fin,
                    System.currentTimeMillis() + 60_000, 50), fin);
            after.add(fwd);
            CoreRestoreRig.register(fwd, SessionClass.FORWARDER, false, true, after);
            assertTrue("precondition: the forwarder is connected before the GTD deadline (deadline in "
                    + (expiresAt - System.currentTimeMillis()) + " ms)", System.currentTimeMillis() < expiresAt - 500);
            final Predicate<String> anyCancel = f -> f.startsWith("DISPATCH") && f.contains("kind=CANCEL");
            final Predicate<String> gtdCancel = f -> anyCancel.test(f) && f.contains(" order=" + gtdOrderId + " ");
            CoreRestoreRig.pollUntil(after, expiresAt - 250);
            assertEquals("no CANCEL before the GTD deadline; frames " + fin.frames, 0, fin.count(anyCancel));
            CoreRestoreRig.await(after, () -> fin.count(gtdCancel) > 0, expiresAt + 10_000,
                    "one DISPATCH kind=CANCEL for the GTD order is expected at its deadline after the restart; frames " + fin.frames);
            CoreRestoreRig.pollUntil(after, System.currentTimeMillis() + QUIET_AFTER_MS);
            assertEquals("the GTD order is cancelled exactly once (R-C9a across a restore); frames " + fin.frames, 1, fin.count(anyCancel));
            assertTrue("the cancel is attempt 1 on leg " + LEG + ": " + fin.frames, fin.frames.stream().anyMatch(f -> gtdCancel.test(f)
                    && f.contains("id=" + gtdOrderId + "/" + ((1L << 2) | 1) + " ") && f.contains("old=" + LEG)));
            System.out.println("[RESTORE-GTD] expiresAtMs=" + expiresAt + " cancels=" + fin.count(anyCancel) + " frame=\""
                    + fin.frames.stream().filter(gtdCancel).findFirst().orElse("") + "\"");
        } finally {
            System.setOut(out);
            for (final Session s : after) {
                CloseHelper.quietClose(s.client);
            }
            shutdown(l, node, tmp);
        }
    }

    // ---------------------------------------------------------------- build phase

    private static Loaded load(final String aeronDir, final File baseDir, final int orders, final Tee tee, final AtomicInteger stops)
            throws Exception {
        // before anything is constructed: the cadence reads its properties in a field initialiser and ClusterConfig
        // reads the snapshot term when it builds the channel string. Only the forced snapshot may happen.
        System.setProperty("snapshot.log.bytes", Long.toString(1L << 30));
        System.setProperty("snapshot.interval.minutes", "0");
        System.setProperty("transport.snapshot.term.length", SNAPSHOT_TERM);
        final Loaded l = new Loaded();
        l.node = CoreRestoreRig.boot(baseDir, aeronDir, stops::incrementAndGet);
        l.archiveDir = l.node.cfg.archiveContext().archiveDir();
        final long deadline = System.currentTimeMillis() + 60_000;
        final Session admin = session(l, aeronDir, SessionClass.ADMIN, deadline);
        final Session tool = session(l, aeronDir, SessionClass.TOOL, deadline);
        final Session gw = session(l, aeronDir, SessionClass.GATEWAY, deadline);
        final Session bridge = session(l, aeronDir, SessionClass.BRIDGE, deadline);
        CoreRestoreRig.register(gw, SessionClass.GATEWAY, false, false, l.sessions);
        CoreRestoreRig.register(bridge, SessionClass.BRIDGE, true, false, l.sessions);
        final Encoders enc = new Encoders();

        CoreRestoreRig.offer(admin, enc.buf, enc.gateOpen(0xAD1L), l.sessions);
        CoreRestoreRig.await(l.sessions, () -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0,
                System.currentTimeMillis() + OP_TIMEOUT_MS, "the ADMIN gate open must be ACCEPTED");

        final int users = orders / ORDERS_PER_USER;
        for (long u = 1; u <= users; u++) {
            CoreRestoreRig.offer(tool, enc.buf, enc.deposit(0, u, CoreRestoreRig.usd(), 1_000_000 * ONE), l.sessions);
            CoreRestoreRig.offer(tool, enc.buf, enc.deposit(0, u, CoreRestoreRig.baseAssetOf(CoreRestoreRig.marketOf(u)), 1_000 * ONE),
                    l.sessions);
        }
        CoreRestoreRig.offer(tool, enc.buf, enc.deposit(99, 1, CoreRestoreRig.usd(), ONE), l.sessions);
        CoreRestoreRig.await(l.sessions, () -> tool.in.lastDepositAckCorr == 99, System.currentTimeMillis() + 60_000,
                "the last funding deposit must be acked (all " + (2 * users) + " before it are applied)");

        // ---- the creates: a bounded in-flight window, egress polled, never a wait per order
        final Listener g = gw.in;
        final long b0 = System.nanoTime();
        for (int i = 0; i < orders; i++) {
            final long user = 1 + i / ORDERS_PER_USER;
            final int k = i % ORDERS_PER_USER;
            final boolean buy = k % 2 == 0;
            final int market = CoreRestoreRig.marketOf(user);
            while (i - (g.acks + g.rejects) >= WINDOW) {
                for (final Session s : l.sessions) {
                    s.poll();
                }
            }
            if (g.rejects > 0) {
                throw new AssertionError("create refused during the build: " + g.firstReject);
            }
            CoreRestoreRig.offer(gw, enc.buf, enc.limit(i + 1, user, i + 1, market, buy, CoreRestoreRig.priceOf(market, buy, k / 2),
                    ONE / 10, TimeInForce.GTC, 0L), l.sessions);
        }
        CoreRestoreRig.await(l.sessions, () -> g.acks + g.rejects >= orders, System.currentTimeMillis() + 120_000,
                "every create must be answered");
        final long buildMs = (System.nanoTime() - b0) / 1_000_000;
        assertNull("no create refused: " + g.firstReject, g.firstReject);
        assertEquals("the last ack is order " + orders, orders, g.lastAckOrderId);
        l.buildLine = "buildOrders=" + orders + " buildMs=" + buildMs + " buildRatePerSec=" + (orders * 1000L / Math.max(1, buildMs));
        System.out.println("[BUILD] " + l.buildLine);

        // ---- one GTD order due after the restart, its leg learned from a BRIDGE outcome
        l.gtdExpiresAtMs = g.lastAckAtMs + GTD_WINDOW_MS;
        final int m1 = CoreRestoreRig.marketOf(1);
        CoreRestoreRig.offer(gw, enc.buf, enc.limit(orders + 1, 1, orders + 1, m1, true, CoreRestoreRig.priceOf(m1, true, 9), ONE / 10,
                TimeInForce.GTD, l.gtdExpiresAtMs), l.sessions);
        CoreRestoreRig.await(l.sessions, () -> g.acks == orders + 1, System.currentTimeMillis() + OP_TIMEOUT_MS, "the GTD create is acked");
        l.gtdOrderId = g.lastAckOrderId;
        assertEquals("the GTD order id", orders + 1, l.gtdOrderId);
        CoreRestoreRig.offer(bridge, enc.buf, enc.createApplied(l.gtdOrderId, 1, m1, LEG), l.sessions);
        final Node n = l.node;
        CoreRestoreRig.await(l.sessions, () -> {
            final CoreOrder o = n.service.engine().orders().orders().get(l.gtdOrderId);
            return o != null && o.currentLegId == LEG;
        }, System.currentTimeMillis() + OP_TIMEOUT_MS, "the BRIDGE outcome APPLIED gives the GTD order its leg");

        // ---- force one snapshot through the cluster control toggle
        final File clusterDir = n.cfg.consensusModuleContext().clusterDir();
        final long before = CoreRestoreRig.latestServiceSnapshot(clusterDir);
        final AtomicCounter toggle = ClusterControl.findControlToggle(n.cfg.consensusModuleContext().aeron().countersReader(),
                n.cfg.consensusModuleContext().clusterId());
        assertTrue("the SNAPSHOT toggle is accepted", ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
        CoreRestoreRig.await(l.sessions, () -> CoreRestoreRig.latestServiceSnapshot(clusterDir) != before
                && tee.first("[SNAPSHOT] accounts=") >= 0, System.currentTimeMillis() + 120_000, "a valid service snapshot after the toggle");
        l.snapshotRecordingId = CoreRestoreRig.latestServiceSnapshot(clusterDir);
        l.takenLine = tee.lines.get(tee.first("[SNAPSHOT] accounts="));
        return l;
    }

    private static Session session(final Loaded l, final String aeronDir, final SessionClass cls, final long deadline)
            throws InterruptedException {
        final Listener in = new Listener();
        final Session s = new Session(CoreRestoreRig.connect(aeronDir, cls, in, deadline, 250), in);
        l.sessions.add(s);
        return s;
    }

    private static void closeAll(final Loaded l) {
        for (final Session s : l.sessions) {
            CloseHelper.quietClose(s.client);
        }
        l.sessions.clear();
    }

    private static void shutdown(final Loaded l, final Node node, final File tmp) {
        if (l != null) {
            closeAll(l);
            if (l.node != null) {
                l.node.close();
            }
        }
        if (node != null) {
            node.close();
        }
        IoUtil.delete(tmp, true);
        System.clearProperty("snapshot.log.bytes");
        System.clearProperty("snapshot.interval.minutes");
        System.clearProperty("transport.snapshot.term.length");
    }

    /** The snapshot recording on the archive: its term must be the 4m one; returns the recorded byte count. */
    private static long recordedSnapshotBytes(final Loaded l) {
        final long[] termAndBytes = new long[2];
        try (AeronArchive archive = AeronArchive.connect(l.node.cfg.consensusModuleContext().archiveContext().clone())) {
            final long found = archive.listRecording(l.snapshotRecordingId,
                    (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
                     initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                     originalChannel, sourceIdentity) -> {
                        termAndBytes[0] = termBufferLength;
                        termAndBytes[1] = stopPosition - startPosition;
                    });
            assertEquals("snapshot recording descriptor", 1L, found);
        }
        assertEquals("the snapshot recording runs on the 4m term", 4L * 1024 * 1024, termAndBytes[0]);
        return termAndBytes[1];
    }

    /**
     * Flip one byte of the v4 payload inside the recorded snapshot: find the envelope header (magic, version 1) in the
     * recording's segment files and flip the byte 100 bytes into the payload (inside the first fragment's data, so the
     * Aeron framing stays intact and only the CRC32C can catch it). Returns the file offset flipped.
     */
    private static long flipOnePayloadByte(final File archiveDir, final long recordingId) throws Exception {
        final File[] segments = archiveDir.listFiles((d, name) -> name.startsWith(recordingId + "-") && name.endsWith(".rec"));
        assertNotNull("archive dir " + archiveDir, segments);
        assertTrue("snapshot recording " + recordingId + " has a segment file in " + archiveDir, segments.length > 0);
        for (final File f : segments) {
            try (FileChannel ch = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                final ByteBuffer bb = ByteBuffer.allocate((int) ch.size()).order(ByteOrder.nativeOrder());
                while (bb.hasRemaining() && ch.read(bb, bb.position()) > 0) {
                    // read the whole segment
                }
                for (int p = 0; p + 8 <= bb.capacity(); p += 4) {
                    if (bb.getInt(p) == SnapshotEnvelope.MAGIC && bb.getInt(p + 4) == SnapshotEnvelope.VERSION) {
                        final int at = p + SnapshotEnvelope.HEADER_LENGTH + 100;
                        ch.write(ByteBuffer.wrap(new byte[] {(byte) (bb.get(at) ^ 0x5A)}), at);
                        ch.force(true);
                        return at;
                    }
                }
            }
        }
        throw new AssertionError("no snapshot envelope header found in recording " + recordingId);
    }

    private static void assertLockedEqualsHolds(final Account a, final long user) {
        assertNotNull("account " + user, a);
        final long[] sum = new long[Asset.count()];
        a.forEachHold((orderId, assetId, remaining, oms) -> sum[assetId] += remaining);
        for (int i = 0; i < sum.length; i++) {
            assertEquals("I2 user " + user + " asset " + i + ": locked == sum of holds", sum[i], a.locked(i));
        }
        assertTrue("user " + user + " holds something", a.holdCount() >= ORDERS_PER_USER);
    }

    private static File tmp(final String what) {
        final File t = new File(System.getProperty("java.io.tmpdir"), "ae-core-restore-" + what + "-" + CoreRestoreRig.PORT_BASE);
        IoUtil.delete(t, true);
        return t;
    }
}

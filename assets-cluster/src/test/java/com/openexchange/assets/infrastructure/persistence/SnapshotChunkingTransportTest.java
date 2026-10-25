// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.IdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A ledger snapshot LARGER than the snapshot channel's {@code maxMessageLength} must survive a full
 * transport round-trip. Until 2026-10-25 {@code onTakeSnapshot} offered the whole ledger as ONE Aeron
 * message, so a payload above {@code maxMessageLength} (snapshot term / 8: 8 MiB at the 64m default,
 * 128 KiB on the e2e fixture's 1m term) threw {@link IllegalArgumentException} inside the callback.
 * Every member snapshots at the same log position, so that fenced all three nodes at once and the log
 * was never pruned again. The fix chunks on the way out and accumulates on the way in; the
 * {@link BalanceSnapshotCodec} byte format is unchanged.
 *
 * <p>Drives the real production encoder ({@link AssetsClusteredService#offerSnapshotChunked}) and the
 * same FragmentAssembler+accumulate reassembly {@code loadSnapshot} uses, over a real Aeron IPC
 * publication whose 64 KiB term (maxMessageLength 8 KiB) and 1408 B MTU force both multi-chunk and
 * multi-fragment delivery. 400 accounts with two holds each serialize to about 62 KiB, roughly eight
 * chunks. The reassembled bytes are asserted identical, then decoded and compared account by account.
 * The in-process cluster tests keep covering the single-chunk path through the real cluster.</p>
 */
public class SnapshotChunkingTransportTest {

    private static final String CHANNEL = "aeron:ipc?term-length=64k|mtu=1408";
    private static final int STREAM_ID = 1301;
    private static final int ACCOUNTS = 400;
    private static final int QUOTE = Asset.USD.id();
    private static final int BASE = Asset.BTC.id();
    private static final long TRADE_HIGH_WATER = 123_456_789L;
    private static final long CONSUME_POSITION = 9_876_543_210L;

    private MediaDriver mediaDriver;
    private Aeron aeron;
    private File aeronDir;

    @Before
    public void setUp() {
        aeronDir = new File(System.getProperty("java.io.tmpdir"), "ae-snap-chunk-" + System.nanoTime());
        mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .aeronDirectoryName(aeronDir.getAbsolutePath())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
    }

    @After
    public void tearDown() {
        CloseHelper.closeAll(aeron, mediaDriver);
        if (aeronDir != null && aeronDir.exists()) {
            IoUtil.delete(aeronDir, true);
        }
    }

    @Test
    public void largeLedgerSnapshotSurvivesChunkedTransportRoundTrip() {
        final AssetsEngine orig = populated();
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer();
        // What onTakeSnapshot offers: the v4 payload sealed in the kit envelope (RC1 S2).
        final int pos = AssetsClusteredService.encodeSnapshot(orig, payload, new com.openexchange.cluster.SnapshotEnvelope());
        final byte[] expected = Arrays.copyOf(payload.byteArray(), pos);

        final Subscription sub = aeron.addSubscription(CHANNEL, STREAM_ID);
        final ExclusivePublication pub = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
        try {
            awaitConnected(pub, sub);
            final int maxLen = pub.maxMessageLength();
            assertTrue("precondition: snapshot (" + pos + " B) must exceed maxMessageLength (" + maxLen
                    + " B) so the chunking path is exercised", pos > maxLen);

            final ExpandableArrayBuffer accum = new ExpandableArrayBuffer();
            final int[] accumLen = {0};
            final FragmentAssembler assembler = new FragmentAssembler((buf, offset, length, header) -> {
                accum.putBytes(accumLen[0], buf, offset, length);
                accumLen[0] += length;
            });
            // Drain the subscriber whenever the publisher back-pressures (a 64 KiB term guarantees it),
            // which also exercises the production retry branch.
            final IdleStrategy drainingIdle = new IdleStrategy() {
                public void idle(final int workCount) { sub.poll(assembler, 64); }
                public void idle() { sub.poll(assembler, 64); }
                public void reset() { }
            };

            final int chunks = AssetsClusteredService.offerSnapshotChunked(pub, payload, pos, drainingIdle);
            assertTrue("payload must be sent as several chunks (got " + chunks + ")", chunks >= 2);

            final long deadline = System.currentTimeMillis() + 10_000;
            while (accumLen[0] < pos) {
                if (sub.poll(assembler, 64) == 0 && System.currentTimeMillis() > deadline) {
                    break;
                }
            }
            assertEquals("reassembled length", pos, accumLen[0]);
            assertArrayEquals("reassembled payload must be byte-identical to the serialized ledger",
                    expected, Arrays.copyOf(accum.byteArray(), accumLen[0]));

            final AssetsEngine restored = new AssetsEngine();
            restored.setEventSink(NOOP);
            // loadSnapshot's contract: open the envelope on the reassembled image, then decode the payload.
            final int payloadLength = new com.openexchange.cluster.SnapshotEnvelope().open(accum, accumLen[0]);
            final BalanceSnapshotCodec.Decoded decoded = BalanceSnapshotCodec.deserialize(
                    accum, com.openexchange.cluster.SnapshotEnvelope.payloadOffset(), payloadLength, restored);
            assertEquals("bytes consumed", payloadLength, decoded.bytesConsumed);
            assertEquals("account count", ACCOUNTS, decoded.accountCount);
            assertEquals("hold count", ACCOUNTS * 2, decoded.holdCount);
            assertEquals("trade high-water", TRADE_HIGH_WATER, restored.getLastAppliedTradeId());
            assertEquals("consume position", CONSUME_POSITION, restored.getConsumePosition());
            for (long user = 1; user <= ACCOUNTS; user++) {
                for (final int a : new int[] {QUOTE, BASE}) {
                    assertEquals("available user=" + user + " asset=" + a,
                            orig.account(user).available(a), restored.account(user).available(a));
                    assertEquals("locked user=" + user + " asset=" + a,
                            orig.account(user).locked(a), restored.account(user).locked(a));
                }
                assertEquals("hold remaining user=" + user,
                        orig.account(user).holdRemaining(user * 10), restored.account(user).holdRemaining(user * 10));
                assertEquals("hold remaining user=" + user + " second",
                        orig.account(user).holdRemaining(user * 10 + 1), restored.account(user).holdRemaining(user * 10 + 1));
                assertEquals("omsManagedRelease user=" + user,
                        orig.account(user).isOmsManagedRelease(user * 10 + 1),
                        restored.account(user).isOmsManagedRelease(user * 10 + 1));
            }
            System.out.println("[TEST] ledger snapshot " + pos + " B sent in " + chunks + " chunk(s) of <= "
                    + maxLen + " B, reassembled byte-identical, " + ACCOUNTS + " accounts restored.");
        } finally {
            CloseHelper.closeAll(sub, pub);
        }
    }

    private static AssetsEngine populated() {
        final AssetsEngine e = new AssetsEngine();
        e.setEventSink(NOOP);
        final DepositCommand dep = new DepositCommand();
        final HoldCommand hold = new HoldCommand();
        for (long user = 1; user <= ACCOUNTS; user++) {
            dep.reset();
            dep.setCorrelationId(0L); dep.setUserId(user); dep.setAssetId(QUOTE); dep.setAmount(1_000_000_000L * user);
            e.applyCommand(AssetsEngine.CMD_DEPOSIT, dep, 1L);
            dep.reset();
            dep.setCorrelationId(0L); dep.setUserId(user); dep.setAssetId(BASE); dep.setAmount(5_000_000L + user);
            e.applyCommand(AssetsEngine.CMD_DEPOSIT, dep, 1L);
            hold.reset();
            hold.setCorrelationId(0L); hold.setOrderId(user * 10); hold.setUserId(user); hold.setAssetId(QUOTE);
            hold.setAmount(250_000_000L); hold.setOmsManagedRelease(false);
            e.applyCommand(AssetsEngine.CMD_HOLD, hold, 1L);
            hold.reset();
            hold.setCorrelationId(0L); hold.setOrderId(user * 10 + 1); hold.setUserId(user); hold.setAssetId(BASE);
            hold.setAmount(1_000_000L); hold.setOmsManagedRelease(true);
            e.applyCommand(AssetsEngine.CMD_HOLD, hold, 1L);
        }
        e.setLastAppliedTradeId(TRADE_HIGH_WATER);
        e.setConsumePosition(CONSUME_POSITION);
        return e;
    }

    private static void awaitConnected(final ExclusivePublication pub, final Subscription sub) {
        final long deadline = System.currentTimeMillis() + 5_000;
        while (!(pub.isConnected() && sub.isConnected())) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("IPC pub/sub did not connect within 5s");
            }
            try {
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted awaiting IPC connection", e);
            }
        }
    }

    private static final AssetsEventSink NOOP = new AssetsEventSink() {
        public void onDepositAck(long c, long u, int a, long amt, long n) { }
        public void onWithdrawAck(long c, long u, int a, long amt, long n) { }
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
    };
}

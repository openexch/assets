// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.match.infrastructure.journal.generated.BooleanType;
import com.match.infrastructure.journal.generated.JournalTerminalEncoder;
import com.match.infrastructure.journal.generated.JournalTradeEncoder;
import com.match.infrastructure.journal.generated.TerminalStatus;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The whole feed, end to end, in-process: a REAL single-node AE cluster + a REAL journal
 * archive; journal entries written exactly as the ME's writer records them; the REAL
 * {@link BridgeAgent} queried, replaying, translating and offering; then a bridge RESTART
 * mid-stream proving the stateless resume protocol (query -> filter -> idempotent overlap).
 *
 * Isolated ports (AE 19400s, journal 18930, egress 19494/19495) and temp dirs; backoff idle
 * throughout so the test co-resides with the live stack.
 */
public class BridgeEndToEndTest {

    private static final int AE_PORT_BASE = 19400;
    private static final int JOURNAL_CONTROL_PORT = 18930;
    private static final long BUYER = 200L;
    private static final long SELLER = 100L;
    private static final long SELL_ORDER = 11L;
    private static final long BUY_ORDER = 22L;

    private File tmp;
    private ClusteredMediaDriver aeDriver;
    private ClusteredServiceContainer aeContainer;
    private MediaDriver journalDriver;
    private Archive journalArchive;
    private AeronArchive journalWriterClient;
    private ExclusivePublication journalPub;
    private TestAeClient fundingClient;

    /** Minimal AE client for funding + assertions (same package: reuses nothing from the bridge). */
    private static final class TestAeClient implements EgressListener, AutoCloseable {
        final AeronCluster cluster;
        final AtomicInteger holdAcks = new AtomicInteger();
        final AtomicInteger feedReports = new AtomicInteger();
        final Map<String, long[]> balances = new ConcurrentHashMap<>(); // "user:asset" -> {avail, locked}
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final HoldAckDecoder holdAck = new HoldAckDecoder();
        private final BalanceUpdateDecoder balance = new BalanceUpdateDecoder();
        private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
        private final DepositEncoder depositEnc = new DepositEncoder();
        private final HoldEncoder holdEnc = new HoldEncoder();
        private final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);

        TestAeClient(final String aeronDir, final String egressEndpoint) {
            final class Holder implements EgressListener {
                volatile EgressListener delegate;

                @Override
                public void onMessage(long sid, long ts, DirectBuffer b, int o, int l, Header h) {
                    final EgressListener d = delegate;
                    if (d != null) {
                        d.onMessage(sid, ts, b, o, l, h);
                    }
                }
            }
            final Holder holder = new Holder();
            this.cluster = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp?term-length=4m")
                    .ingressEndpoints("0=localhost:" + (AE_PORT_BASE + 2))
                    .egressChannel("aeron:udp?endpoint=" + egressEndpoint + "|term-length=4m")
                    .egressListener(holder));
            holder.delegate = this;
        }

        @Override
        public void onMessage(long sessionId, long timestamp, DirectBuffer buffer, int offset, int length, Header h) {
            header.wrap(buffer, offset);
            if (header.templateId() == 17) { // FeedPositionReport: proves the AE processed a query
                feedReports.incrementAndGet();
            }
            if (header.templateId() == HoldAckDecoder.TEMPLATE_ID) {
                holdAck.wrapAndApplyHeader(buffer, offset, header);
                holdAcks.incrementAndGet();
            } else if (header.templateId() == BalanceUpdateDecoder.TEMPLATE_ID) {
                balance.wrapAndApplyHeader(buffer, offset, header);
                balances.put(balance.userId() + ":" + balance.assetId(),
                        new long[] {balance.available(), balance.locked()});
            }
        }

        void deposit(long userId, int assetId, long amount) {
            depositEnc.wrapAndApplyHeader(buf, 0, headerEnc)
                    .correlationId(0).userId(userId).assetId(assetId).amount(amount);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength());
        }

        void hold(long orderId, long userId, int assetId, long amount) {
            holdEnc.wrapAndApplyHeader(buf, 0, headerEnc)
                    .correlationId(0).orderId(orderId).userId(userId).assetId(assetId).amount(amount);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + holdEnc.encodedLength());
        }

        private void offer(int length) {
            while (cluster.offer(buf, 0, length) < 0) {
                cluster.pollEgress();
                Thread.onSpinWait();
            }
        }

        void await(final LongSupplier value, final long expected, final long timeoutMs) {
            final long deadline = System.currentTimeMillis() + timeoutMs;
            while (value.getAsLong() != expected) {
                assertTrue("timed out waiting for value " + expected + ", have " + value.getAsLong(),
                        System.currentTimeMillis() < deadline);
                cluster.pollEgress();
                cluster.sendKeepAlive();
                sleepQuiet();
            }
        }

        long[] balance(long userId, int assetId) {
            return balances.getOrDefault(userId + ":" + assetId, new long[] {-1, -1});
        }

        @Override
        public void close() {
            CloseHelper.quietClose(cluster);
        }
    }

    private static void sleepQuiet() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Before
    public void setUp() {
        tmp = new File(System.getProperty("java.io.tmpdir"), "bridge-e2e-" + AE_PORT_BASE);
        IoUtil.delete(tmp, true);

        // Real single-node AE cluster (embedded driver, backoff idle) — ClusterBootSmokeTest pattern.
        final ClusterConfig cfg = ClusterConfig.create(0, List.of("localhost"), AE_PORT_BASE,
                new AssetsClusteredService());
        cfg.baseDir(new File(tmp, "node0"));
        cfg.aeronDirectoryName(new File(tmp, "ae-driver").getAbsolutePath());
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=4m")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        aeDriver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                cfg.archiveContext(),
                cfg.consensusModuleContext());
        aeContainer = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

        // Real journal archive (the ME side's second archive, in miniature) + writer publication.
        journalDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        journalArchive = Archive.launch(new Archive.Context()
                .aeronDirectoryName(journalDriver.aeronDirectoryName())
                .archiveDir(new File(tmp, "journal-archive"))
                .controlChannel("aeron:udp?endpoint=localhost:" + JOURNAL_CONTROL_PORT)
                .controlStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID)
                .localControlStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID + 1)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .fileSyncLevel(0)
                .segmentFileLength(1024 * 1024));
        journalWriterClient = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(journalDriver.aeronDirectoryName())
                .controlRequestChannel("aeron:udp?endpoint=localhost:" + JOURNAL_CONTROL_PORT)
                .controlRequestStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);

        fundingClient = new TestAeClient(new File(tmp, "ae-driver").getAbsolutePath(), "localhost:19495");
    }

    @After
    public void tearDown() {
        CloseHelper.quietCloseAll(fundingClient, journalPub, journalWriterClient, journalArchive,
                journalDriver, aeContainer, aeDriver);
        IoUtil.delete(tmp, true);
    }

    private final MessageHeaderEncoder jHeader = new MessageHeaderEncoder();
    private final com.match.infrastructure.journal.generated.MessageHeaderEncoder journalHeaderEnc =
            new com.match.infrastructure.journal.generated.MessageHeaderEncoder();
    private final JournalTradeEncoder jTrade = new JournalTradeEncoder();
    private final JournalTerminalEncoder jTerminal = new JournalTerminalEncoder();
    private final UnsafeBuffer jBuf = new UnsafeBuffer(new byte[160]);

    private void journalTrade(long egressSeq, long tradeId, long price, long qty) {
        jTrade.wrapAndApplyHeader(jBuf, 0, journalHeaderEnc)
                .egressSeq(egressSeq).tradeId(tradeId).marketId(1)
                .takerOrderId(BUY_ORDER).takerUserId(BUYER)
                .makerOrderId(SELL_ORDER).makerUserId(SELLER)
                .price(price).quantity(qty)
                .takerIsBuy(BooleanType.TRUE).timestamp(egressSeq);
        offerJournal(com.match.infrastructure.journal.generated.MessageHeaderEncoder.ENCODED_LENGTH
                + jTrade.encodedLength());
    }

    private void journalTerminal(long egressSeq, long orderId, long userId) {
        jTerminal.wrapAndApplyHeader(jBuf, 0, journalHeaderEnc)
                .egressSeq(egressSeq).orderId(orderId).userId(userId).marketId(1)
                .status(TerminalStatus.FILLED).timestamp(egressSeq);
        offerJournal(com.match.infrastructure.journal.generated.MessageHeaderEncoder.ENCODED_LENGTH
                + jTerminal.encodedLength());
    }

    private void offerJournal(int length) {
        while (journalPub.offer(jBuf, 0, length) < 0) {
            Thread.onSpinWait();
        }
    }

    private BridgeConfig bridgeConfig() {
        return new BridgeConfig(
                List.of("localhost:" + JOURNAL_CONTROL_PORT),
                List.of("localhost"), AE_PORT_BASE, "localhost:19494",
                true, 10_000);
    }

    @Test
    public void bridgeSettlesReleasesAndResumesAcrossARestart() throws Exception {
        // Fund + hold, and CONFIRM it landed before any settle can race it (cross-session
        // ingress has no ordering guarantee — production is protected by the hold-before-
        // submit gate; the test must protect itself the same way).
        fundingClient.deposit(SELLER, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        fundingClient.deposit(BUYER, Asset.USD.id(), FixedPoint.fromDouble(120000.0));
        fundingClient.hold(SELL_ORDER, SELLER, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        fundingClient.hold(BUY_ORDER, BUYER, Asset.USD.id(), FixedPoint.fromDouble(120000.0));
        fundingClient.await(fundingClient.holdAcks::get, 2, 10_000);

        // Journal: first trade (1 BTC @ 60k) before the bridge starts (bounded-replay path).
        journalTrade(1_000L, 1L, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0));

        // Bridge epoch 1.
        final BridgeState state1 = new BridgeState();
        final BridgeAgent agent1 = new BridgeAgent(bridgeConfig(), journalDriver.aeronDirectoryName(), state1);
        final Thread t1 = new Thread(agent1, "bridge-e2e-1");
        t1.start();

        // Trade 1 settles: buyer gets 1 BTC, seller gets 60k USD.
        fundingClient.await(() -> fundingClient.balance(BUYER, Asset.BTC.id())[0],
                FixedPoint.fromDouble(1.0), 15_000);
        assertEquals(FixedPoint.fromDouble(60000.0), fundingClient.balance(SELLER, Asset.USD.id())[0]);
        assertEquals(1L, state1.forwardedTrades);

        // Kill the bridge mid-stream (stateless: no handoff, no checkpoint files).
        agent1.stop();
        t1.interrupt();
        t1.join(10_000);

        // More journal while the bridge is down: trade 2 + both terminals.
        journalTrade(2_000L, 2L, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0));
        journalTerminal(2_100L, SELL_ORDER, SELLER);
        journalTerminal(2_200L, BUY_ORDER, BUYER);

        // Bridge epoch 2 (fresh instance): must resume via the AE's FeedPositionReport,
        // skip/no-op the overlap, and land ONLY the new money.
        final BridgeState state2 = new BridgeState();
        final BridgeAgent agent2 = new BridgeAgent(bridgeConfig(), journalDriver.aeronDirectoryName(), state2);
        final Thread t2 = new Thread(agent2, "bridge-e2e-2");
        t2.start();

        fundingClient.await(() -> fundingClient.balance(BUYER, Asset.BTC.id())[0],
                FixedPoint.fromDouble(2.0), 15_000);

        // Exact final books: no double-settle, residuals released by the terminals.
        assertEquals(FixedPoint.fromDouble(120000.0), fundingClient.balance(SELLER, Asset.USD.id())[0]);
        assertEquals(0L, fundingClient.balance(SELLER, Asset.BTC.id())[0]);
        assertEquals(0L, fundingClient.balance(SELLER, Asset.BTC.id())[1]); // hold fully consumed
        assertEquals(FixedPoint.fromDouble(2.0), fundingClient.balance(BUYER, Asset.BTC.id())[0]);
        assertEquals(0L, fundingClient.balance(BUYER, Asset.USD.id())[0]);  // exact-price buy, no residual
        assertEquals(0L, fundingClient.balance(BUYER, Asset.USD.id())[1]);

        // Epoch 2 resumed cleanly: trade 1 was skipped (already applied), trade 2 forwarded.
        assertTrue("epoch2 should have skipped the overlap", state2.skippedEntries >= 1);
        assertEquals(1L, state2.forwardedTrades);
        assertEquals(2L, state2.forwardedTerminals);
        assertEquals(0L, state2.gapsDetected);

        agent2.stop();
        t2.interrupt();
        t2.join(10_000);
    }
}

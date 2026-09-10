// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.match.infrastructure.journal.generated.BooleanType;
import com.match.infrastructure.journal.generated.JournalTerminalEncoder;
import com.match.infrastructure.journal.generated.JournalTradeEncoder;
import com.match.infrastructure.journal.generated.TerminalStatus;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.HoldAckBatchDecoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedBatchDecoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder;
import com.openexchange.assets.infrastructure.generated.SettleFaultDecoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
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
    private final List<BridgeAgent> runningAgents = new ArrayList<>();
    private final List<Thread> agentThreads = new ArrayList<>();

    /** Minimal AE client for funding + assertions (same package: reuses nothing from the bridge). */
    private static final class TestAeClient implements EgressListener, AutoCloseable {
        final AeronCluster cluster;
        final AtomicInteger holdAcks = new AtomicInteger();
        final AtomicInteger feedReports = new AtomicInteger();
        final AtomicInteger settleFaults = new AtomicInteger();
        final Map<String, long[]> balances = new ConcurrentHashMap<>(); // "user:asset" -> {avail, locked}
        final List<Long> settlements = new ArrayList<>();
        private final FeedPositionReportDecoder feedPosition = new FeedPositionReportDecoder();
        private final SettlementAppliedDecoder settlement = new SettlementAppliedDecoder();
        private final SettlementAppliedBatchDecoder settlementBatch = new SettlementAppliedBatchDecoder();
        private final QueryFeedPositionEncoder query = new QueryFeedPositionEncoder();
        private long queryCorrelationId;
        private long responseCorrelationId = -1;
        private long consumePosition;
        private long lastAppliedTradeId;
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final HoldAckDecoder holdAck = new HoldAckDecoder();
        private final HoldAckBatchDecoder holdAckBatch = new HoldAckBatchDecoder();
        private final BalanceUpdateDecoder balance = new BalanceUpdateDecoder();
        private final BalanceUpdateBatchDecoder balanceBatch = new BalanceUpdateBatchDecoder();
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
                    .ingressChannel("aeron:udp?term-length=4m|mtu=8192")
                    .ingressEndpoints("0=localhost:" + (AE_PORT_BASE + 2))
                    .egressChannel("aeron:udp?endpoint=" + egressEndpoint + "|term-length=4m")
                    .egressListener(holder));
            holder.delegate = this;
        }

        @Override
        public void onMessage(long sessionId, long timestamp, DirectBuffer buffer, int offset, int length, Header h) {
            header.wrap(buffer, offset);
            if (header.templateId() == FeedPositionReportDecoder.TEMPLATE_ID) {
                feedPosition.wrapAndApplyHeader(buffer, offset, header);
                if (feedPosition.correlationId() == queryCorrelationId) {
                    responseCorrelationId = feedPosition.correlationId();
                    consumePosition = feedPosition.consumePosition();
                    lastAppliedTradeId = feedPosition.lastAppliedTradeId();
                }
                feedReports.incrementAndGet();
            } else if (header.templateId() == SettlementAppliedDecoder.TEMPLATE_ID) {
                settlement.wrapAndApplyHeader(buffer, offset, header);
                settlements.add(settlement.tradeId());
            } else if (header.templateId() == SettlementAppliedBatchDecoder.TEMPLATE_ID) {
                settlementBatch.wrapAndApplyHeader(buffer, offset, header);
                for (final SettlementAppliedBatchDecoder.SettlementsDecoder s : settlementBatch.settlements()) {
                    settlements.add(s.tradeId());
                }
            } else if (header.templateId() == SettleFaultDecoder.TEMPLATE_ID) {
                settleFaults.incrementAndGet();
            }
            if (header.templateId() == HoldAckDecoder.TEMPLATE_ID) {
                holdAck.wrapAndApplyHeader(buffer, offset, header);
                holdAcks.incrementAndGet();
            } else if (header.templateId() == HoldAckBatchDecoder.TEMPLATE_ID) {
                // v5: live egress coalesces these; a lone ack arrives as a batch of one.
                holdAckBatch.wrapAndApplyHeader(buffer, offset, header);
                for (final HoldAckBatchDecoder.AcksDecoder a : holdAckBatch.acks()) {
                    holdAcks.incrementAndGet();
                }
            } else if (header.templateId() == BalanceUpdateDecoder.TEMPLATE_ID) {
                balance.wrapAndApplyHeader(buffer, offset, header);
                balances.put(balance.userId() + ":" + balance.assetId(),
                        new long[] {balance.available(), balance.locked()});
            } else if (header.templateId() == BalanceUpdateBatchDecoder.TEMPLATE_ID) {
                balanceBatch.wrapAndApplyHeader(buffer, offset, header);
                for (final BalanceUpdateBatchDecoder.UpdatesDecoder u : balanceBatch.updates()) {
                    balances.put(u.userId() + ":" + u.assetId(),
                            new long[] {u.available(), u.locked()});
                }
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
            final long deadline = System.currentTimeMillis() + 10_000;
            while (cluster.offer(buf, 0, length) < 0) {
                assertTrue("AE test client offer timed out", System.currentTimeMillis() < deadline);
                cluster.pollEgress();
                cluster.sendKeepAlive();
                Thread.onSpinWait();
            }
        }

        void assertApplied(final long expectedPosition, final long expectedTradeId) {
            final long deadline = System.currentTimeMillis() + 15_000;
            do {
                final long correlationId = ++queryCorrelationId;
                query.wrapAndApplyHeader(buf, 0, headerEnc).correlationId(correlationId);
                offer(MessageHeaderEncoder.ENCODED_LENGTH + query.encodedLength());
                await(() -> responseCorrelationId, correlationId, 5_000);
                if (consumePosition == expectedPosition && lastAppliedTradeId == expectedTradeId) {
                    return;
                }
                sleepQuiet();
            } while (System.currentTimeMillis() < deadline);
            assertEquals("actual AE consumePosition", expectedPosition, consumePosition);
            assertEquals("actual AE lastAppliedTradeId", expectedTradeId, lastAppliedTradeId);
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
                .ingressChannel("aeron:udp?term-length=4m|mtu=8192")
                .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(5))
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
        journalArchive = launchJournalArchive();
        journalWriterClient = connectJournalWriter();
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);

        fundingClient = new TestAeClient(new File(tmp, "ae-driver").getAbsolutePath(), "localhost:19495");
    }

    /** Same-dir, same-port archive: a relaunch recovers the catalog like a restarted node's does. */
    private Archive launchJournalArchive() {
        return launchJournalArchive(Archive.Configuration.maxConcurrentReplays());
    }

    private Archive launchJournalArchive(final int maxConcurrentReplays) {
        return Archive.launch(new Archive.Context()
                .aeronDirectoryName(journalDriver.aeronDirectoryName())
                .archiveDir(new File(tmp, "journal-archive"))
                .controlChannel("aeron:udp?endpoint=localhost:" + JOURNAL_CONTROL_PORT)
                .controlStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID)
                .localControlStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID + 1)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .maxConcurrentReplays(maxConcurrentReplays)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .fileSyncLevel(0)
                .segmentFileLength(1024 * 1024));
    }

    private AeronArchive connectJournalWriter() {
        return AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(journalDriver.aeronDirectoryName())
                .controlRequestChannel("aeron:udp?endpoint=localhost:" + JOURNAL_CONTROL_PORT)
                .controlRequestStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
    }

    @After
    public void tearDown() throws InterruptedException {
        for (final BridgeAgent agent : runningAgents) {
            agent.stop();
        }
        for (final Thread thread : agentThreads) {
            thread.interrupt();
            thread.join(10_000);
            assertTrue("bridge test thread must stop", !thread.isAlive());
        }
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
                .takerOrderId(900_000 + BUY_ORDER).takerUserId(BUYER)   // cluster ids differ from
                .makerOrderId(900_000 + SELL_ORDER).makerUserId(SELLER) // the OMS ids on purpose
                .price(price).quantity(qty)
                .takerIsBuy(BooleanType.TRUE).timestamp(egressSeq)
                .takerOmsOrderId(BUY_ORDER).makerOmsOrderId(SELL_ORDER);
        offerJournal(com.match.infrastructure.journal.generated.MessageHeaderEncoder.ENCODED_LENGTH
                + jTrade.encodedLength());
    }

    private void journalTerminal(long egressSeq, long orderId, long userId) {
        jTerminal.wrapAndApplyHeader(jBuf, 0, journalHeaderEnc)
                .egressSeq(egressSeq).orderId(orderId).userId(userId).marketId(1)
                .status(TerminalStatus.FILLED).timestamp(egressSeq)
                .omsOrderId(orderId);
        offerJournal(com.match.infrastructure.journal.generated.MessageHeaderEncoder.ENCODED_LENGTH
                + jTerminal.encodedLength());
    }

    private void offerJournal(int length) {
        offerJournal(journalPub, length);
    }

    private void offerJournal(final ExclusivePublication publication, final int length) {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (publication.offer(jBuf, 0, length) < 0) {
            assertTrue("journal test offer timed out", System.currentTimeMillis() < deadline);
            fundingClient.cluster.pollEgress();
            fundingClient.cluster.sendKeepAlive();
            Thread.onSpinWait();
        }
    }

    private BridgeConfig bridgeConfig() {
        return bridgeConfig(List.of("localhost:" + JOURNAL_CONTROL_PORT));
    }

    private BridgeConfig bridgeConfig(final List<String> endpoints) {
        return new BridgeConfig(
                endpoints,
                List.of("localhost"), AE_PORT_BASE, "localhost:19494",
                "localhost", true, 10_000, 0);
    }

    private BridgeState startBridge(final BridgeConfig config) {
        final BridgeState state = new BridgeState();
        final BridgeAgent agent = new BridgeAgent(config, journalDriver.aeronDirectoryName(), state);
        final Thread thread = new Thread(agent, "bridge-e2e-memo");
        runningAgents.add(agent);
        agentThreads.add(thread);
        thread.start();
        return state;
    }

    private void awaitCondition(final String description, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + 20_000;
        while (!condition.getAsBoolean()) {
            assertTrue("timed out: " + description, System.currentTimeMillis() < deadline);
            fundingClient.cluster.pollEgress();
            fundingClient.cluster.sendKeepAlive();
            sleepQuiet();
        }
    }

    private void fundAndHold(final double bitcoins, final double dollars) {
        fundingClient.deposit(SELLER, Asset.BTC.id(), FixedPoint.fromDouble(bitcoins));
        fundingClient.deposit(BUYER, Asset.USD.id(), FixedPoint.fromDouble(dollars));
        fundingClient.hold(SELL_ORDER, SELLER, Asset.BTC.id(), FixedPoint.fromDouble(bitcoins));
        fundingClient.hold(BUY_ORDER, BUYER, Asset.USD.id(), FixedPoint.fromDouble(dollars));
        fundingClient.await(fundingClient.holdAcks::get, 2, 10_000);
    }

    private void assertBooks(final double bought, final double proceeds,
                             final double releasedBitcoins, final double releasedDollars) {
        fundingClient.await(() -> fundingClient.balance(SELLER, Asset.BTC.id())[0],
                FixedPoint.fromDouble(releasedBitcoins), 10_000);
        fundingClient.await(() -> fundingClient.balance(BUYER, Asset.USD.id())[0],
                FixedPoint.fromDouble(releasedDollars), 10_000);
        assertEquals(FixedPoint.fromDouble(bought), fundingClient.balance(BUYER, Asset.BTC.id())[0]);
        assertEquals(FixedPoint.fromDouble(proceeds), fundingClient.balance(SELLER, Asset.USD.id())[0]);
        assertEquals(0L, fundingClient.balance(SELLER, Asset.BTC.id())[1]);
        assertEquals(0L, fundingClient.balance(BUYER, Asset.USD.id())[1]);
        assertEquals("terminals must not release the holds before their preceding settles",
                0, fundingClient.settleFaults.get());
    }

    private void restartReplay(final AeronArchive writer, final long recordingId,
                               final BridgeState state, final String endpoint,
                               final long frontier, final long expectedSkips) {
        final long nextEpoch = state.epochs + 1;
        writer.stopAllReplays(recordingId);
        awaitCondition("new epoch at " + endpoint + " frontier=" + frontier,
                () -> state.epochs >= nextEpoch && state.journalSource.equals(endpoint)
                        && state.connectedToAe && state.replayConsumedPosition == frontier
                        && state.skippedEntries == expectedSkips);
        // Allow the accepted replay's image to attach before injecting another replay loss;
        // also observe that the replacement epoch does not immediately fail at its offset.
        final long deadline = System.currentTimeMillis() + 200;
        while (System.currentTimeMillis() < deadline) {
            fundingClient.cluster.pollEgress();
            fundingClient.cluster.sendKeepAlive();
            sleepQuiet();
        }
        assertEquals("one deliberate replay loss should cause one retry", nextEpoch, state.epochs);
        assertTrue("replacement replay stays connected", state.connectedToAe);
    }

    private ArchiveJournalSource.Recording recording(final AeronArchive writer, final long recordingId) {
        final ArchiveJournalSource.Recording[] descriptor = {null};
        assertEquals(1, writer.listRecording(recordingId,
                (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, startPosition, stopPosition,
                 initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                 strippedChannel, originalChannel, sourceIdentity) ->
                        descriptor[0] = new ArchiveJournalSource.Recording(id, startPosition, stopPosition,
                                new ArchiveJournalSource.RecordingIncarnation(startTimestamp, initialTermId,
                                        sessionId, termBufferLength, mtuLength, streamId))));
        return descriptor[0];
    }

    /**
     * Real Aeron regression for the September cross-source alias: identical logical messages
     * and recording id/start/active stop, different byte histories. A legal PAD frame on B
     * shifts every subsequent frame by 96 bytes. A's 160-byte cursor lands inside B's trade;
     * B's 416-byte cursor is beyond A's 320-byte frontier. One agent carries the memo throughout.
     */
    @Test(timeout = 90_000)
    public void byteResumeSurvivesAToBToAWithDifferentRealFrameBoundaries() throws Exception {
        fundAndHold(4.0, 240000.0);
        final String endpointA = "localhost:" + JOURNAL_CONTROL_PORT;
        final String endpointB = "localhost:" + (JOURNAL_CONTROL_PORT + 1);
        final List<String> selectedEndpoint = new CopyOnWriteArrayList<>(List.of(endpointA));

        try (MediaDriver driverB = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             Archive archiveB = Archive.launch(new Archive.Context()
                    .aeronDirectoryName(driverB.aeronDirectoryName())
                    .archiveDir(new File(tmp, "journal-b"))
                    .controlChannel("aeron:udp?endpoint=" + endpointB)
                    .controlStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID)
                    .localControlStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID + 1)
                    .replicationChannel("aeron:udp?endpoint=localhost:0")
                    .recordingEventsEnabled(false).threadingMode(ArchiveThreadingMode.SHARED)
                    .fileSyncLevel(0).segmentFileLength(1024 * 1024));
             AeronArchive writerB = AeronArchive.connect(new AeronArchive.Context()
                    .aeronDirectoryName(driverB.aeronDirectoryName())
                    .controlRequestChannel("aeron:udp?endpoint=" + endpointB)
                    .controlRequestStreamId(JournalSource.JOURNAL_ARCHIVE_CONTROL_STREAM_ID)
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
             ExclusivePublication pubB = writerB.addRecordedExclusivePublication(
                    "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID)) {
            awaitCondition("journal B connected", pubB::isConnected);
            assertEquals("legal Aeron padding creates a different byte history", 96L, pubB.appendPadding(64));
            journalTrade(1_000, 1, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
            offerJournal(pubB, com.match.infrastructure.journal.generated.MessageHeaderEncoder.ENCODED_LENGTH
                    + jTrade.encodedLength());
            assertEquals(160L, journalPub.position());
            assertEquals(256L, pubB.position());
            final ArchiveJournalSource.Recording recordingA = recording(journalWriterClient, 0);
            final ArchiveJournalSource.Recording recordingB = recording(writerB, 0);
            assertEquals("each archive starts at local recording id zero", 0, recordingA.recordingId());
            assertEquals(recordingA.recordingId(), recordingB.recordingId());
            assertEquals(0, recordingA.startPosition());
            assertEquals(recordingA.startPosition(), recordingB.startPosition());
            assertEquals(AeronArchive.NULL_POSITION, recordingA.stopPosition());
            assertEquals(recordingA.stopPosition(), recordingB.stopPosition());

            final BridgeState state = startBridge(bridgeConfig(selectedEndpoint));
            fundingClient.assertApplied(1_000, 1);
            awaitCondition("A has consumed trade one", () -> state.replayConsumedPosition == 160);

            // First retry learns the safe SKIP prefix; another same-source retry must use it.
            restartReplay(journalWriterClient, 0, state, endpointA, 160, 1);
            restartReplay(journalWriterClient, 0, state, endpointA, 160, 1);

            selectedEndpoint.set(0, endpointB);
            restartReplay(journalWriterClient, 0, state, endpointB, 256, 2);
            journalTrade(2_000, 2, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
            offerJournal(pubB, com.match.infrastructure.journal.generated.MessageHeaderEncoder.ENCODED_LENGTH
                    + jTrade.encodedLength());
            fundingClient.assertApplied(2_000, 2);
            awaitCondition("B has consumed trade two", () -> state.replayConsumedPosition == 416);
            restartReplay(writerB, 0, state, endpointB, 416, 3);

            selectedEndpoint.set(0, endpointA);
            restartReplay(writerB, 0, state, endpointA, 320, 5);
            journalTrade(3_000, 3, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
            journalTerminal(3_100, SELL_ORDER, SELLER);
            journalTerminal(3_200, BUY_ORDER, BUYER);
            fundingClient.assertApplied(3_200, 3);
            assertBooks(3, 180000, 1, 60000);
            assertEquals("actual settlement acknowledgements are dense and ordered",
                    List.of(1L, 2L, 3L), fundingClient.settlements);
            assertEquals(3, state.forwardedTrades);
            assertEquals(0, state.gapsDetected);
            assertTrue(!state.halted);
            // Five explicit stopAllReplays calls; no extra invalid-position retry churn.
            assertEquals(6, state.epochs);
            assertEquals(5, state.errors);
        }
    }

    @Test(timeout = 90_000)
    public void emptyHeadStoppedEofAndActiveFrontierContinueIntoSuccessor() {
        fundAndHold(5, 300000);
        awaitCondition("empty journal recording is attached", journalPub::isConnected);
        journalWriterClient.stopRecording(journalPub);
        journalPub.close();
        awaitCondition("recording zero is stopped and empty", () -> journalWriterClient.getStopPosition(0) == 0);
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);
        journalTrade(1_000, 1, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        awaitCondition("recording one is archived", () -> journalWriterClient.getRecordingPosition(1) == 160);
        journalWriterClient.stopRecording(journalPub);
        journalPub.close();
        awaitCondition("recording one has EOF", () -> journalWriterClient.getStopPosition(1) == 160);
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);
        journalTrade(2_000, 2, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));

        final BridgeState state = startBridge(bridgeConfig());
        fundingClient.assertApplied(2_000, 2);
        final String endpoint = "localhost:" + JOURNAL_CONTROL_PORT;
        restartReplay(journalWriterClient, 2, state, endpoint, 160, 2);
        restartReplay(journalWriterClient, 2, state, endpoint, 160, 2);
        assertEquals("repeated epochs must preserve the drained stopped prefix", 2, state.skippedEntries);
        journalTrade(3_000, 3, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        fundingClient.assertApplied(3_000, 3);

        // Learn the live tail's applied byte frontier, then turn that exact frontier into EOF.
        restartReplay(journalWriterClient, 2, state, endpoint, 320, 3);
        final long beforeSuccessor = state.epochs;
        journalWriterClient.stopRecording(journalPub);
        journalPub.close();
        awaitCondition("old live tail is stopped", () -> journalWriterClient.getStopPosition(2) == 320);
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);
        journalTrade(4_000, 4, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        journalTerminal(4_100, SELL_ORDER, SELLER);
        journalTerminal(4_200, BUY_ORDER, BUYER);
        fundingClient.assertApplied(4_200, 4);
        assertBooks(4, 240000, 1, 60000);
        assertEquals(List.of(1L, 2L, 3L, 4L), fundingClient.settlements);
        assertTrue("successor required a fresh catalog listing", state.epochs > beforeSuccessor);
        assertEquals(4, state.forwardedTrades);
        assertEquals(0, state.gapsDetected);
        assertTrue(!state.halted);
    }

    @Test(timeout = 90_000)
    public void earlierInclusiveTerminalDoesNotForceRescanningLaterAppliedRecording() {
        fundAndHold(4, 240000);
        journalTrade(1_000, 1, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        // Same watermark group, absent hold: every retry must forward this terminal as an
        // idempotent no-op, without releasing either actual settlement hold.
        journalTerminal(1_000, 9_999, BUYER);
        final long stoppedEnd = journalPub.position();
        awaitCondition("first recording is archived",
                () -> journalWriterClient.getRecordingPosition(0) == stoppedEnd);
        journalWriterClient.stopRecording(journalPub);
        journalPub.close();
        awaitCondition("first recording has its terminal EOF",
                () -> journalWriterClient.getStopPosition(0) == stoppedEnd);
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);
        for (int duplicate = 0; duplicate < 512; duplicate++) {
            journalTrade(1_000, 1, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        }
        // The replay also crosses a real Aeron term boundary; take the publication's actual
        // frontier rather than assuming the journal frames have no intervening PAD frame.
        final long frontier = journalPub.position();
        awaitCondition("duplicate backlog is archived",
                () -> journalWriterClient.getRecordingPosition(1) == frontier);

        final BridgeState state = startBridge(bridgeConfig());
        fundingClient.assertApplied(1_000, 1);
        awaitCondition("initial epoch consumed the duplicate backlog",
                () -> state.replayConsumedPosition == frontier && state.skippedEntries == 512);
        fundingClient.await(() -> state.settleAckLatency.count, 1, 10_000);
        final String endpoint = "localhost:" + JOURNAL_CONTROL_PORT;

        // At the first retry the AE proves trade one applied. Recording zero skips its trade
        // and forwards its inclusive terminal; recording one must still learn its byte prefix.
        restartReplay(journalWriterClient, 1, state, endpoint, frontier, 1_025);
        // At the next retry recording zero resumes at its terminal, and recording one resumes
        // at its applied frontier. Repeating either head would increment this exact SKIP count.
        restartReplay(journalWriterClient, 1, state, endpoint, frontier, 1_025);
        assertEquals("the later applied backlog is not scanned every epoch", 1_025, state.skippedEntries);

        journalTrade(2_000, 2, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        journalTerminal(2_100, SELL_ORDER, SELLER);
        journalTerminal(2_200, BUY_ORDER, BUYER);
        fundingClient.assertApplied(2_200, 2);
        assertBooks(2, 120000, 2, 120000);
        assertEquals(List.of(1L, 2L), fundingClient.settlements);
        assertEquals(2, state.forwardedTrades);
        assertEquals(3, state.epochs);
        assertEquals("only the two injected replay losses caused errors", 2, state.errors);
        assertEquals(0, state.gapsDetected);
        assertTrue(!state.halted);
    }

    @Test(timeout = 60_000)
    public void backlogBatchesApplyInOrderAndSessionSurvivesIdlePastTimeout() {
        fundAndHold(2, 200);
        // More than two full 64-trade batches: a default 1408-byte ingress MTU breaks this
        // catch-up path while individual trade tests continue to pass.
        for (int trade = 1; trade <= 129; trade++) {
            journalTrade(trade * 1_000L, trade, FixedPoint.fromDouble(100), FixedPoint.fromDouble(0.01));
        }
        final long backlogEnd = journalPub.position();
        awaitCondition("full backlog is recorded before replay",
                () -> journalWriterClient.getRecordingPosition(0) == backlogEnd);
        final BridgeState state = startBridge(bridgeConfig());
        fundingClient.assertApplied(129_000, 129);
        fundingClient.await(() -> state.settleAckLatency.count, 129, 10_000);
        final long epoch = state.epochs;
        final long idleDeadline = System.currentTimeMillis() + 6_000;
        while (System.currentTimeMillis() < idleDeadline) {
            fundingClient.cluster.pollEgress();
            fundingClient.cluster.sendKeepAlive();
            sleepQuiet();
        }
        journalTrade(130_000, 130, FixedPoint.fromDouble(100), FixedPoint.fromDouble(0.01));
        journalTerminal(130_100, SELL_ORDER, SELLER);
        journalTerminal(130_200, BUY_ORDER, BUYER);
        fundingClient.assertApplied(130_200, 130);
        fundingClient.await(() -> state.settleAckLatency.count, 130, 10_000);
        assertBooks(1.3, 130, 0.7, 70);
        assertEquals(130, fundingClient.settlements.size());
        for (int trade = 1; trade <= 130; trade++) {
            assertEquals("AE settlement acknowledgement order", Long.valueOf(trade),
                    fundingClient.settlements.get(trade - 1));
        }
        assertEquals("keepalives preserve the same session beyond its five-second timeout", epoch, state.epochs);
        assertEquals(0, state.errors);
        assertEquals(0, state.gapsDetected);
    }

    @Test(timeout = 45_000)
    public void stoppedReplayEndingBeforeCatalogEofIsAVisibleFailure() throws Exception {
        journalTrade(1_000, 1, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        journalTrade(2_000, 2, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        awaitCondition("both frames archived", () -> journalWriterClient.getRecordingPosition(0) == 320);
        journalWriterClient.stopRecording(journalPub);
        journalPub.close();
        awaitCondition("catalog records full stopped extent", () -> journalWriterClient.getStopPosition(0) == 320);
        final ArchiveJournalSource.Recording stopped = recording(journalWriterClient, 0);
        final BridgeState state = new BridgeState();
        final BridgeAgent agent = new BridgeAgent(bridgeConfig(), journalDriver.aeronDirectoryName(), state);
        final Method drained = BridgeAgent.class.getDeclaredMethod("replayDrained", Subscription.class,
                ArchiveJournalSource.Recording.class, Image.class);
        drained.setAccessible(true);

        // Produce genuine Aeron EOS after the FIRST of two catalogued frames. EOS describes
        // the requested replay, so it cannot itself prove the catalogued recording was drained.
        final AtomicReference<Image> shortImage = new AtomicReference<>();
        try (Subscription replay = journalWriterClient.replay(0, 0, 160, "aeron:ipc", 9102,
                shortImage::set, image -> { })) {
            awaitCondition("short replay reaches EOS at the first frame", () -> {
                replay.poll((b, o, l, h) -> { }, 32);
                final Image image = shortImage.get();
                return image != null && image.position() == 160 && image.isEndOfStream();
            });
            final InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                    () -> drained.invoke(agent, replay, stopped, shortImage.get()));
            assertTrue(failure.getCause() instanceof IllegalStateException);
            assertTrue(failure.getCause().getMessage().contains("before catalog EOF"));
            assertEquals(1, state.sourceStalls);
        }

        final AtomicReference<Image> fullImage = new AtomicReference<>();
        try (Subscription replay = journalWriterClient.replay(0, 0, 320, "aeron:ipc", 9102,
                fullImage::set, image -> { })) {
            awaitCondition("full replay reaches catalog EOF", () -> {
                replay.poll((b, o, l, h) -> { }, 32);
                final Image image = fullImage.get();
                return image != null && image.position() == 320;
            });
            assertEquals(Boolean.TRUE, drained.invoke(agent, replay, stopped, fullImage.get()));
            assertEquals("normal EOF adds no source failure", 1, state.sourceStalls);
        }
    }

    @Test(timeout = 45_000)
    public void rejectedRememberedOffsetClearsBothProofsAndNextReplayStartsSafely() throws Exception {
        awaitCondition("empty recording is attached", journalPub::isConnected);
        journalWriterClient.stopRecording(journalPub);
        journalPub.close();
        awaitCondition("empty recording stopped", () -> journalWriterClient.getStopPosition(0) == 0);
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);
        journalTrade(1_000, 1, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        journalTrade(2_000, 2, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        awaitCondition("two frames archived", () -> journalWriterClient.getRecordingPosition(1) == 320);

        final BridgeAgent agent = new BridgeAgent(bridgeConfig(), journalDriver.aeronDirectoryName(), new BridgeState());
        final Field memoField = BridgeAgent.class.getDeclaredField("resumeMemo");
        memoField.setAccessible(true);
        final ChainResumeMemo memo = (ChainResumeMemo)memoField.get(agent);
        final Method open = BridgeAgent.class.getDeclaredMethod("openReplay", ArchiveJournalSource.class,
                ArchiveJournalSource.Recording.class, long.class);
        open.setAccessible(true);
        try (ArchiveJournalSource source = JournalSource.connectFirstHealthy(bridgeConfig(),
                journalDriver.aeronDirectoryName())) {
            final List<ArchiveJournalSource.Recording> chain = source.recordings();
            assertEquals(2, chain.size());
            final ArchiveJournalSource.Recording active = chain.get(1);
            memo.resumeIndex(source.identity(), chain, 2_000, 2);
            memo.noteDrained(chain, 0, false);
            // Simulate a remembered offset from an older implementation or unseen history
            // change: 96 is aligned but sits inside the genuine first 160-byte frame.
            memo.noteSkipHighWater(active, 96);
            assertEquals(1, memo.resumeIndex(source.identity(), chain, 2_000, 2));
            final InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                    () -> open.invoke(agent, source, active, 96L));
            assertTrue("invalid bytes remain a visible real Aeron failure",
                    failure.getCause() instanceof io.aeron.archive.client.ArchiveException);
            assertEquals("byte proof cleared", 0, memo.replayStartPosition(active));
            assertEquals("drained-recording proof cleared", 0,
                    memo.resumeIndex(source.identity(), chain, 2_000, 2));

            final AtomicInteger replayed = new AtomicInteger();
            try (Subscription replay = (Subscription)open.invoke(agent, source, active,
                    memo.replayStartPosition(active))) {
                awaitCondition("next replay reads both intact frames from catalog start", () -> {
                    source.poll(replay, (b, o, l, h) -> replayed.incrementAndGet(), 32);
                    return replayed.get() == 2;
                });
            }
        }
    }

    @Test(timeout = 45_000)
    public void replayCapacityRejectionPreservesValidByteAndDrainedPrefixProofs() throws Exception {
        awaitCondition("empty recording is attached", journalPub::isConnected);
        journalWriterClient.stopRecording(journalPub);
        awaitCondition("empty recording stopped", () -> journalWriterClient.getStopPosition(0) == 0);
        CloseHelper.quietCloseAll(journalPub, journalWriterClient, journalArchive);
        // Only this test's fresh fixture gets a one-replay limit. Other tests retain the normal
        // cap, including transient overlap between old/new replay sessions during epoch retries.
        journalArchive = launchJournalArchive(1);
        journalWriterClient = connectJournalWriter();
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);
        journalTrade(1_000, 1, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        journalTrade(2_000, 2, FixedPoint.fromDouble(60000), FixedPoint.fromDouble(1));
        awaitCondition("two frames archived", () -> journalWriterClient.getRecordingPosition(1) == 320);

        final BridgeAgent agent = new BridgeAgent(bridgeConfig(), journalDriver.aeronDirectoryName(), new BridgeState());
        final Field memoField = BridgeAgent.class.getDeclaredField("resumeMemo");
        memoField.setAccessible(true);
        final ChainResumeMemo memo = (ChainResumeMemo)memoField.get(agent);
        final Method open = BridgeAgent.class.getDeclaredMethod("openReplay", ArchiveJournalSource.class,
                ArchiveJournalSource.Recording.class, long.class);
        open.setAccessible(true);
        try (ArchiveJournalSource source = JournalSource.connectFirstHealthy(bridgeConfig(),
                journalDriver.aeronDirectoryName())) {
            final List<ArchiveJournalSource.Recording> chain = source.recordings();
            assertEquals(2, chain.size());
            final ArchiveJournalSource.Recording active = chain.get(1);
            memo.resumeIndex(source.identity(), chain, 2_000, 2);
            memo.noteDrained(chain, 0, false);
            memo.noteSkipHighWater(active, 160); // real boundary after the first trade
            try (Subscription heldReplay = source.openReplay(active)) {
                awaitCondition("first replay occupies the only replay slot", () -> heldReplay.imageCount() == 1);
                final InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                        () -> open.invoke(agent, source, active, 160L));
                assertTrue(failure.getCause() instanceof io.aeron.archive.client.ArchiveException);
                assertEquals(io.aeron.archive.client.ArchiveException.MAX_REPLAYS,
                        ((io.aeron.archive.client.ArchiveException)failure.getCause()).errorCode());
                assertEquals("resource pressure does not disprove valid byte history", 160,
                        memo.replayStartPosition(active));
                assertEquals("resource pressure does not discard the drained prefix", 1,
                        memo.resumeIndex(source.identity(), chain, 2_000, 2));
            }
        }
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
        runningAgents.add(agent1);
        agentThreads.add(t1);
        t1.start();

        // Trade 1 settles: buyer gets 1 BTC, seller gets 60k USD.
        fundingClient.await(() -> fundingClient.balance(BUYER, Asset.BTC.id())[0],
                FixedPoint.fromDouble(1.0), 15_000);
        assertEquals(FixedPoint.fromDouble(60000.0), fundingClient.balance(SELLER, Asset.USD.id())[0]);
        assertEquals(1L, state1.forwardedTrades);

        // The AE broadcasts SettlementApplied for the settle; the bridge's own egress session
        // sees it and must close the ack-latency loop (offer-return -> ack observed).
        fundingClient.await(() -> state1.settleAckLatency.count > 0 ? 1 : 0, 1, 10_000);

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
        runningAgents.add(agent2);
        agentThreads.add(t2);
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

    /**
     * The 2026-07-10 production incident, in miniature: the journal SOURCE dies mid-live-follow
     * (node SIGKILL took its archive, its recording, and the replay publication with it), then
     * comes back with a NEW recording. The old bridge idled at the dead recording's EOF forever,
     * reporting healthy while holds piled up. The fixed bridge must detect the loss, restart its
     * epoch, re-list the chain, and land the money written after the restart — one agent, no
     * operator action.
     */
    @Test
    public void bridgeRecoversWhenJournalSourceDiesMidLiveFollow() throws Exception {
        fundingClient.deposit(SELLER, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        fundingClient.deposit(BUYER, Asset.USD.id(), FixedPoint.fromDouble(120000.0));
        fundingClient.hold(SELL_ORDER, SELLER, Asset.BTC.id(), FixedPoint.fromDouble(2.0));
        fundingClient.hold(BUY_ORDER, BUYER, Asset.USD.id(), FixedPoint.fromDouble(120000.0));
        fundingClient.await(fundingClient.holdAcks::get, 2, 10_000);

        journalTrade(1_000L, 1L, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0));

        // ONE agent for the whole test: recovery must not need a process restart.
        final BridgeState state = new BridgeState();
        final BridgeAgent agent = new BridgeAgent(bridgeConfig(), journalDriver.aeronDirectoryName(), state);
        final Thread t = new Thread(agent, "bridge-e2e-source-restart");
        runningAgents.add(agent);
        agentThreads.add(t);
        t.start();

        // Trade 1 settles; the bridge is now live-following the ACTIVE recording 0.
        fundingClient.await(() -> fundingClient.balance(BUYER, Asset.BTC.id())[0],
                FixedPoint.fromDouble(1.0), 15_000);
        assertEquals(1L, state.forwardedTrades);

        // Kill the journal source under the live-follow: publication, writer client and archive
        // all die together, exactly as a node SIGKILL takes them (the shared test driver
        // survives, standing in for the network path).
        CloseHelper.quietCloseAll(journalPub, journalWriterClient, journalArchive);

        // The "restarted node": same archive dir + port (catalog recovery stops recording 0),
        // and a fresh recorded publication = recording 1, carrying the post-restart money.
        journalArchive = launchJournalArchive();
        journalWriterClient = connectJournalWriter();
        journalPub = journalWriterClient.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k", JournalSource.SETTLEMENT_JOURNAL_STREAM_ID);
        journalTrade(2_000L, 2L, FixedPoint.fromDouble(60000.0), FixedPoint.fromDouble(1.0));
        journalTerminal(2_100L, SELL_ORDER, SELLER);
        journalTerminal(2_200L, BUY_ORDER, BUYER);

        // The bridge must self-recover and land ONLY the new money. Generous deadline: detection
        // (image loss or the 10s source probe) + epoch teardown + archive reconnect all fit well
        // inside it; the OLD code fails this await by idling at recording 0's EOF forever.
        fundingClient.await(() -> fundingClient.balance(BUYER, Asset.BTC.id())[0],
                FixedPoint.fromDouble(2.0), 60_000);

        // Exact final books — same invariants as the restart test: no double-settle, no losses.
        assertEquals(FixedPoint.fromDouble(120000.0), fundingClient.balance(SELLER, Asset.USD.id())[0]);
        assertEquals(0L, fundingClient.balance(SELLER, Asset.BTC.id())[0]);
        assertEquals(0L, fundingClient.balance(SELLER, Asset.BTC.id())[1]);
        assertEquals(FixedPoint.fromDouble(2.0), fundingClient.balance(BUYER, Asset.BTC.id())[0]);
        assertEquals(0L, fundingClient.balance(BUYER, Asset.USD.id())[0]);
        assertEquals(0L, fundingClient.balance(BUYER, Asset.USD.id())[1]);

        // The recovery was a detected source loss + a fresh epoch, with a clean dense-id chain.
        assertTrue("expected a detected source stall/loss, got stalls=" + state.sourceStalls
                + " errors=" + state.errors, state.sourceStalls >= 1 || state.errors >= 1);
        assertTrue("expected a recovery epoch, got epochs=" + state.epochs, state.epochs >= 2);
        assertEquals(0L, state.gapsDetected);
        assertEquals(2L, state.forwardedTrades);
        assertEquals(2L, state.forwardedTerminals);

        agent.stop();
        t.interrupt();
        t.join(10_000);
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.InfrastructureConstants;
import com.openexchange.assets.infrastructure.feed.BalanceFeedRuntime;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Proves the balance-feed side-channel end-to-end through a real embedded cluster: with
 * {@code balance.feed.channel} set, live BalanceUpdates leave on a PLAIN Aeron publication —
 * conflated to latest-value — while snapshot-reply entries never enter it.
 *
 * <p>Assertions are about the feed's actual contract:</p>
 * <ol>
 *   <li><b>Conflation, not per-update delivery</b> — one user is hammered with N deposits; the feed
 *       must show the FINAL balance, while the number of feed entries may be anywhere from 1 to N
 *       (fewer frames than mutations is the feature working, not loss).</li>
 *   <li><b>Snapshot replies stay off the feed</b> — a RequestBalanceSnapshot streams its entries on
 *       the CLUSTER egress to the asker; the feed's per-key entry counts must not move. The negative
 *       is ordered, not timed: a follow-up deposit is the fence — the feed publication is ordered,
 *       so once the fence's frame is seen, a leaked snapshot entry would already have been counted.</li>
 * </ol>
 *
 * <p>Feature off (no channel configured) is the untouched default and is implicitly covered by every
 * other cluster test: nothing is wired, nothing starts.</p>
 */
public class BalanceFeedClusterTest {

    private static final int PORT_BASE = 19700; // disjoint: smoke 19300, cadence 19400, cap 19500, origin 19600
    private static final long USER_A = 8101L;
    private static final long USER_B = 8102L;
    private static final long PRIMER_USER = 8999L;
    private static final int HAMMER_DEPOSITS = 200;

    /** What the feed subscription observed: frames, and per-tracked-key entry counts + last pair. */
    private static final class FeedRecorder {
        final AtomicInteger frames = new AtomicInteger();
        final AtomicInteger entriesA = new AtomicInteger();   // (USER_A, USD)
        final AtomicInteger entriesB = new AtomicInteger();   // (USER_B, USD)
        final AtomicInteger entriesPrimer = new AtomicInteger();
        final AtomicLong lastAvailableA = new AtomicLong(-1);
        final AtomicLong lastLockedA = new AtomicLong(-1);
        final AtomicLong lastAvailableB = new AtomicLong(-1);

        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final BalanceUpdateDecoder single = new BalanceUpdateDecoder();
        private final BalanceUpdateBatchDecoder batch = new BalanceUpdateBatchDecoder();

        void onFrame(final DirectBuffer buffer, final int offset, final int length, final Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            if (header.templateId() == BalanceUpdateDecoder.TEMPLATE_ID) {
                frames.incrementAndGet();
                single.wrapAndApplyHeader(buffer, offset, header);
                onEntry(single.userId(), single.assetId(), single.available(), single.locked());
            } else if (header.templateId() == BalanceUpdateBatchDecoder.TEMPLATE_ID) {
                frames.incrementAndGet();
                batch.wrapAndApplyHeader(buffer, offset, header);
                for (final BalanceUpdateBatchDecoder.UpdatesDecoder u : batch.updates()) {
                    onEntry(u.userId(), u.assetId(), u.available(), u.locked());
                }
            }
        }

        private void onEntry(final long userId, final int assetId, final long available, final long locked) {
            if (assetId != Asset.USD.id()) {
                return;
            }
            if (userId == USER_A) {
                entriesA.incrementAndGet();
                lastAvailableA.set(available);
                lastLockedA.set(locked);
            } else if (userId == USER_B) {
                entriesB.incrementAndGet();
                lastAvailableB.set(available);
            } else if (userId == PRIMER_USER) {
                entriesPrimer.incrementAndGet();
            }
        }
    }

    /** Cluster-egress recorder: deposit acks + the snapshot terminator (proof the reply DID run). */
    private static final class ClusterEgress implements EgressListener {
        final AtomicInteger depositAcks = new AtomicInteger();
        final AtomicInteger snapshotEnds = new AtomicInteger();
        final AtomicInteger lastSnapshotEntryCount = new AtomicInteger(-1);

        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();
        private final BalanceSnapshotEndDecoder snapshotEnd = new BalanceSnapshotEndDecoder();

        @Override
        public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer,
                              int offset, int length, Header aeronHeader) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case DepositAckDecoder.TEMPLATE_ID:
                    depositAck.wrapAndApplyHeader(buffer, offset, header);
                    depositAcks.incrementAndGet();
                    break;
                case BalanceSnapshotEndDecoder.TEMPLATE_ID:
                    snapshotEnd.wrapAndApplyHeader(buffer, offset, header);
                    lastSnapshotEntryCount.set(snapshotEnd.entryCount());
                    snapshotEnds.incrementAndGet();
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    public void feedCarriesConflatedLiveUpdatesAndNeverSnapshotReplies() {
        System.setProperty("balance.feed.channel", "aeron:ipc");
        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-balance-feed-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final File baseDir = new File(tmp, "node0");

        final List<String> hosts = List.of("localhost");
        final AssetsClusteredService service = new AssetsClusteredService();
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        cfg.baseDir(baseDir);
        cfg.aeronDirectoryName(aeronDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new); // never busy-spin
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));

        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        BalanceFeedRuntime feedRuntime = null;
        Aeron feedAeron = null;
        Subscription feedSub = null;
        AeronCluster client = null;
        final FeedRecorder feed = new FeedRecorder();
        final ClusterEgress egress = new ClusterEgress();
        try {
            // Node wiring order mirrors AeronCluster's bootstrap: store armed on the service before
            // the container launches; the publisher thread starts once the media driver is up.
            feedRuntime = BalanceFeedRuntime.createIfEnabled(0);
            assertNotNull("balance.feed.channel is set: the feature must be ON", feedRuntime);
            service.setBalanceFeed(feedRuntime.store());

            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                            .mtuLength(8192),
                    cfg.archiveContext(),
                    cfg.consensusModuleContext());
            feedRuntime.start(aeronDir, Throwable::printStackTrace);
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

            // The measurement side: a PLAIN subscription on the same media driver — no cluster session.
            feedAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            feedSub = feedAeron.addSubscription("aeron:ipc", InfrastructureConstants.BALANCE_FEED_STREAM_ID);
            final FragmentAssembler feedHandler = new FragmentAssembler(feed::onFrame);
            final Subscription sub = feedSub;

            client = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(egress)
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                    .aeronDirectoryName(aeronDir)
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(
                            hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
            final AeronCluster c = client;

            // ---- primer: prove the feed pipe is live before anything is measured ----
            // The agent connects its publication lazily; a frame offered before the subscriber links
            // is dropped by contract (NOT_CONNECTED = conflated away). So: keep re-depositing a
            // throwaway user until the feed delivers it — each deposit re-marks the slot dirty, so
            // this converges the moment the pipe is up, without a single timed sleep.
            int primerSends = 0;
            final long primerDeadline = System.currentTimeMillis() + 15_000;
            while (feed.entriesPrimer.get() == 0) {
                assertTrue("timed out priming the balance feed",
                        System.currentTimeMillis() < primerDeadline);
                offer(c, encodeDeposit(0x900L + primerSends, PRIMER_USER, Asset.USD.id(),
                        FixedPoint.fromDouble(1.0)));
                primerSends++;
                final long spinUntil = System.currentTimeMillis() + 100;
                while (feed.entriesPrimer.get() == 0 && System.currentTimeMillis() < spinUntil) {
                    c.pollEgress();
                    sub.poll(feedHandler, 64);
                }
            }

            // ---- fund the uninvolved key: (USER_B, USD) ----
            offer(c, encodeDeposit(0xB0L, USER_B, Asset.USD.id(), FixedPoint.fromDouble(500.0)));
            pumpUntil(c, sub, feedHandler,
                    () -> feed.lastAvailableB.get() == FixedPoint.fromDouble(500.0),
                    "USER_B's funding on the feed");
            final int entriesBBaseline = feed.entriesB.get();

            // ---- (1) conflation: hammer USER_A with N deposits, expect the FINAL value ----
            for (int i = 0; i < HAMMER_DEPOSITS; i++) {
                offer(c, encodeDeposit(0xA000L + i, USER_A, Asset.USD.id(), FixedPoint.fromDouble(1.0)));
            }
            final long finalA = FixedPoint.fromDouble(HAMMER_DEPOSITS); // N x $1
            final int expectedAcks = HAMMER_DEPOSITS + 1 + primerSends; // hammer + B funding + primers
            pumpUntil(c, sub, feedHandler,
                    () -> egress.depositAcks.get() >= expectedAcks
                            && feed.lastAvailableA.get() == finalA,
                    "all deposits acked + final balance on the feed");
            assertEquals("feed must converge on the final truth", finalA, feed.lastAvailableA.get());
            assertEquals(0L, feed.lastLockedA.get());
            assertTrue("at least one feed entry for the hammered key", feed.entriesA.get() >= 1);
            // Fewer frames than deposits is the CONTRACT, not a failure; the (rare) +1 allows a
            // leadership-gain sweep republishing the key once.
            assertTrue("conflated feed must never exceed one entry per mutation (+1 sweep): "
                            + feed.entriesA.get(),
                    feed.entriesA.get() <= HAMMER_DEPOSITS + 1);

            // ---- (2) snapshot replies stay OFF the feed ----
            // The feed publication is ordered: once the post-query fence deposit's frame arrives,
            // any snapshot-reply leak would already have been counted. No timed negative.
            final int entriesABefore = feed.entriesA.get();
            offer(c, encodeRequestBalanceSnapshot(0xC1L));
            pumpUntil(c, sub, feedHandler, () -> egress.snapshotEnds.get() >= 1,
                    "the snapshot reply's terminator on the cluster egress");
            assertTrue("the reply did stream entries (on the cluster egress)",
                    egress.lastSnapshotEntryCount.get() >= 3); // A, B and the primer at least

            final long fencedA = finalA + FixedPoint.fromDouble(1.0);
            offer(c, encodeDeposit(0xF1L, USER_A, Asset.USD.id(), FixedPoint.fromDouble(1.0)));
            pumpUntil(c, sub, feedHandler, () -> feed.lastAvailableA.get() == fencedA,
                    "the fence deposit on the feed");
            assertEquals("snapshot-reply entries must not enter the feed (hammered key)",
                    entriesABefore + 1, feed.entriesA.get());
            assertEquals("snapshot-reply entries must not enter the feed (uninvolved key)",
                    entriesBBaseline, feed.entriesB.get());
        } finally {
            System.clearProperty("balance.feed.channel");
            CloseHelper.quietClose(client);
            CloseHelper.quietClose(feedSub);
            CloseHelper.quietClose(feedAeron);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(feedRuntime);
            CloseHelper.quietClose(mediaDriver);
            IoUtil.delete(tmp, true);
        }
    }

    /** Poll the cluster session AND the feed until the condition holds. */
    private static void pumpUntil(final AeronCluster client, final Subscription feedSub,
                                  final FragmentAssembler feedHandler,
                                  final BooleanSupplier condition, final String what) {
        final long deadline = System.currentTimeMillis() + 15_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (!condition.getAsBoolean()) {
            assertTrue("timed out waiting for: " + what, System.currentTimeMillis() < deadline);
            client.pollEgress();
            feedSub.poll(feedHandler, 64);
            idle.idle();
        }
    }

    // ---- ingress SBE encoding (same shapes as RequestOriginEgressClusterTest) ----

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final RequestBalanceSnapshotEncoder balanceSnapshotEnc = new RequestBalanceSnapshotEncoder();

    private int encodeDeposit(long correlationId, long userId, int assetId, long amount) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount);
        return MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();
    }

    private int encodeRequestBalanceSnapshot(long correlationId) {
        balanceSnapshotEnc.wrapAndApplyHeader(ingress, 0, headerEnc).correlationId(correlationId);
        return MessageHeaderEncoder.ENCODED_LENGTH + balanceSnapshotEnc.encodedLength();
    }

    private void offer(AeronCluster client, int length) {
        final long deadline = System.currentTimeMillis() + 10_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            long result = client.offer(ingress, 0, length);
            if (result > 0) {
                return;
            }
            client.pollEgress(); // keep the session serviced while backpressured
            idle.idle();
        }
        throw new IllegalStateException("failed to offer ingress within timeout (result kept <= 0)");
    }
}

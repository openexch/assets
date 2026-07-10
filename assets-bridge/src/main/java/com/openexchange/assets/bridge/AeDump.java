// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotEncoder;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * AeDump — a one-shot, READ-ONLY snapshot of the Assets Engine ledger, printed as a single JSON
 * document on stdout, for the {@code money-check} cutover-soak guardrail (tools repo).
 *
 * <p>It connects an Aeron cluster client (mirroring {@link AeFeedClient#connect}), sends three
 * read-only requests — {@code RequestBalanceSnapshot}, {@code RequestHoldSnapshot} and
 * {@code QueryFeedPosition} — and collects the streamed {@code BalanceUpdate} /
 * {@code HoldSnapshotEntry} egress up to the matching {@code *End} / {@code FeedPositionReport}
 * messages. All three requests are deterministic no-ops on AE state.</p>
 *
 * <p>Output (stdout, exactly one line):</p>
 * <pre>
 * {"balances":[{"userId":..,"assetId":..,"available":..,"locked":..}, ...],
 *  "holds":[{"orderId":..,"userId":..,"assetId":..,"remaining":..}, ...],
 *  "consumePosition":.., "lastAppliedTradeId":..}
 * </pre>
 *
 * <p>Exit codes: {@code 0} = snapshot printed; {@code 3} = the 30 s overall snapshot deadline
 * elapsed before all three responses arrived (e.g. no AE leader). Diagnostics go to stderr only,
 * so stdout is always clean JSON for the consumer to parse.</p>
 *
 * <p>Invoked as {@code java -jar assets-bridge.jar dump} (dispatched by {@link BridgeMain}).</p>
 */
public final class AeDump {

    private static final long OVERALL_TIMEOUT_MS = 30_000;
    private static final int EXIT_OK = 0;
    private static final int EXIT_TIMEOUT = 3;

    private AeDump() {
    }

    public static void main(final String[] args) {
        System.exit(run(args));
    }

    /** @return process exit code (0 ok, 3 timeout). Prints the JSON snapshot to stdout on success. */
    public static int run(final String[] args) {
        final BridgeConfig config = BridgeConfig.fromEnv();
        final SnapshotCollector collector = new SnapshotCollector();

        // Own embedded media driver (SHARED, backoff-idle by nature) in its own /dev/shm dir, so
        // AeDump can run alongside the bridge/OMS without touching their drivers.
        final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName("/dev/shm/aeron-aedump-" + ProcessHandle.current().pid())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .errorHandler(t -> t.printStackTrace(System.err)));

        AeronCluster cluster = null;
        try {
            cluster = AeronCluster.connect(new AeronCluster.Context()
                    .messageTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(8))
                    .aeronDirectoryName(driver.aeronDirectoryName())
                    .ingressChannel("aeron:udp?term-length=4m")
                    .ingressEndpoints(AeFeedClient.ingressEndpoints(config.aeClusterAddresses, config.aePortBase))
                    // Ephemeral egress port: never collides with the bridge (9394) or OMS (9393) client.
                    .egressChannel("aeron:udp?endpoint=127.0.0.1:0")
                    .egressListener(collector)
                    .errorHandler(t -> t.printStackTrace(System.err)));

            final long deadline = System.currentTimeMillis() + OVERALL_TIMEOUT_MS;
            sendRequests(cluster, collector, deadline);

            final IdleStrategy idle = new BackoffIdleStrategy();
            while (!collector.complete()) {
                if (System.currentTimeMillis() > deadline) {
                    System.err.println("AeDump: snapshot incomplete after " + OVERALL_TIMEOUT_MS + "ms "
                            + collector.progress());
                    return EXIT_TIMEOUT;
                }
                final int work = cluster.pollEgress();
                idle.idle(work);
            }
            System.out.println(collector.toJson());
            return EXIT_OK;
        } catch (final io.aeron.exceptions.TimeoutException | IllegalStateException e) {
            // No leader reachable / could not send or complete the snapshot within budget: a
            // "no snapshot" condition, not a crash. Same exit code as the poll-loop timeout.
            System.err.println("AeDump: could not obtain a snapshot within the timeout: " + e.getMessage());
            return EXIT_TIMEOUT;
        } finally {
            CloseHelper.quietClose(cluster);
            CloseHelper.quietClose(driver);
        }
    }

    /** Encode + offer the three read-only requests, retrying past backpressure within the deadline. */
    private static void sendRequests(final AeronCluster cluster, final SnapshotCollector collector,
                                     final long deadline) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[64]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();

        final RequestBalanceSnapshotEncoder bal = new RequestBalanceSnapshotEncoder();
        bal.wrapAndApplyHeader(buf, 0, header).correlationId(collector.balanceCorr);
        offerBlocking(cluster, buf, MessageHeaderEncoder.ENCODED_LENGTH + bal.encodedLength(), deadline);

        final RequestHoldSnapshotEncoder hold = new RequestHoldSnapshotEncoder();
        hold.wrapAndApplyHeader(buf, 0, header).correlationId(collector.holdCorr);
        offerBlocking(cluster, buf, MessageHeaderEncoder.ENCODED_LENGTH + hold.encodedLength(), deadline);

        final QueryFeedPositionEncoder pos = new QueryFeedPositionEncoder();
        pos.wrapAndApplyHeader(buf, 0, header).correlationId(collector.posCorr);
        offerBlocking(cluster, buf, MessageHeaderEncoder.ENCODED_LENGTH + pos.encodedLength(), deadline);
    }

    private static void offerBlocking(final AeronCluster cluster, final DirectBuffer buffer,
                                      final int length, final long deadline) {
        final IdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() <= deadline) {
            if (cluster.offer(buffer, 0, length) > 0) {
                return;
            }
            cluster.pollEgress(); // keep the session serviced while backpressured
            idle.idle();
        }
        throw new IllegalStateException("AeDump: failed to offer a snapshot request before the deadline");
    }

    // --------------------------------------------------------------------- //
    // Egress collector — decouples decode+accumulate+JSON from the Aeron plumbing so it is
    // unit-testable against hand-built SBE messages (see AeDumpTest).
    // --------------------------------------------------------------------- //
    static final class SnapshotCollector implements EgressListener {

        final long balanceCorr = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        final long holdCorr = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        final long posCorr = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final BalanceUpdateDecoder balanceDec = new BalanceUpdateDecoder();
        private final BalanceSnapshotEndDecoder balanceEndDec = new BalanceSnapshotEndDecoder();
        private final HoldSnapshotEntryDecoder holdDec = new HoldSnapshotEntryDecoder();
        private final HoldSnapshotEndDecoder holdEndDec = new HoldSnapshotEndDecoder();
        private final FeedPositionReportDecoder posDec = new FeedPositionReportDecoder();

        // BalanceUpdate carries no correlationId, so a live settle's update is indistinguishable
        // from a snapshot entry; dedup last-write-wins per (user,asset), insertion-ordered.
        private final Map<Long, long[]> balances = new LinkedHashMap<>(); // key -> {userId, assetId, available, locked}
        private final Map<Long, long[]> holds = new LinkedHashMap<>();     // key -> {orderId, userId, assetId, remaining}

        private boolean balancesEnd;
        private boolean holdsEnd;
        private boolean positionSeen;
        private long consumePosition;
        private long lastAppliedTradeId;

        private static long balanceKey(final long userId, final int assetId) {
            return (userId << 20) ^ (assetId & 0xFFFFFFFFL); // deterministic dedup key
        }

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                              final int offset, final int length, final Header aeronHeader) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case BalanceUpdateDecoder.TEMPLATE_ID: {
                    if (balancesEnd) {
                        return; // ignore any live update after the snapshot terminator
                    }
                    balanceDec.wrapAndApplyHeader(buffer, offset, header);
                    final long uid = balanceDec.userId();
                    final int asset = balanceDec.assetId();
                    balances.put(balanceKey(uid, asset),
                            new long[]{uid, asset, balanceDec.available(), balanceDec.locked()});
                    break;
                }
                case BalanceSnapshotEndDecoder.TEMPLATE_ID: {
                    balanceEndDec.wrapAndApplyHeader(buffer, offset, header);
                    if (balanceEndDec.correlationId() == balanceCorr) {
                        balancesEnd = true;
                    }
                    break;
                }
                case HoldSnapshotEntryDecoder.TEMPLATE_ID: {
                    if (holdsEnd) {
                        return;
                    }
                    holdDec.wrapAndApplyHeader(buffer, offset, header);
                    holds.put(holdDec.orderId(),
                            new long[]{holdDec.orderId(), holdDec.userId(), holdDec.assetId(), holdDec.remaining()});
                    break;
                }
                case HoldSnapshotEndDecoder.TEMPLATE_ID: {
                    holdEndDec.wrapAndApplyHeader(buffer, offset, header);
                    if (holdEndDec.correlationId() == holdCorr) {
                        holdsEnd = true;
                    }
                    break;
                }
                case FeedPositionReportDecoder.TEMPLATE_ID: {
                    posDec.wrapAndApplyHeader(buffer, offset, header);
                    if (posDec.correlationId() == posCorr) {
                        consumePosition = posDec.consumePosition();
                        lastAppliedTradeId = posDec.lastAppliedTradeId();
                        positionSeen = true;
                    }
                    break;
                }
                default:
                    break;
            }
        }

        boolean complete() {
            return balancesEnd && holdsEnd && positionSeen;
        }

        String progress() {
            return "(balances=" + balances.size() + (balancesEnd ? "/end" : "")
                    + " holds=" + holds.size() + (holdsEnd ? "/end" : "")
                    + " position=" + positionSeen + ")";
        }

        String toJson() {
            final StringBuilder sb = new StringBuilder(256);
            sb.append("{\"balances\":[");
            boolean first = true;
            for (final long[] b : balances.values()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append("{\"userId\":").append(b[0])
                        .append(",\"assetId\":").append(b[1])
                        .append(",\"available\":").append(b[2])
                        .append(",\"locked\":").append(b[3]).append('}');
            }
            sb.append("],\"holds\":[");
            first = true;
            for (final long[] h : holds.values()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append("{\"orderId\":").append(h[0])
                        .append(",\"userId\":").append(h[1])
                        .append(",\"assetId\":").append(h[2])
                        .append(",\"remaining\":").append(h[3]).append('}');
            }
            sb.append("],\"consumePosition\":").append(consumePosition)
                    .append(",\"lastAppliedTradeId\":").append(lastAppliedTradeId)
                    .append('}');
            return sb.toString();
        }
    }
}

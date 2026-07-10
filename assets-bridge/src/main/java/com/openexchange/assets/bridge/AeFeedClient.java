// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The bridge's Assets-Engine session: offers translated Settle/TerminalRelease commands as
 * cluster ingress (BLOCKING — the journal is lossless, so the bridge waits, never drops)
 * and answers the resume protocol (QueryFeedPosition -> FeedPositionReport).
 *
 * Session death surfaces as {@link SessionClosedException}: the bridge's response is always
 * the same — tear down the epoch, reconnect, re-query, rebuild the filter. Statelessness
 * keeps every failure mode boring.
 */
final class AeFeedClient implements AutoCloseable, EgressListener {

    static final class SessionClosedException extends RuntimeException {
        SessionClosedException(final String msg) {
            super(msg);
        }
    }

    record FeedPosition(long consumePosition, long lastAppliedTradeId) {
    }

    private static final long KEEPALIVE_INTERVAL_MS = 1_000;

    private final AeronCluster cluster;
    private final IdleStrategy idle = new BackoffIdleStrategy();

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final FeedPositionReportDecoder feedPositionDecoder = new FeedPositionReportDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final QueryFeedPositionEncoder queryEncoder = new QueryFeedPositionEncoder();
    private final UnsafeBuffer queryBuffer = new UnsafeBuffer(new byte[64]);

    private long lastKeepAliveMs;
    private long awaitedCorrelationId;
    private FeedPosition capturedPosition;

    private AeFeedClient(final AeronCluster cluster) {
        this.cluster = cluster;
    }

    /** Late-bound listener holder: the AeFeedClient instance cannot exist before connect(). */
    private static final class DelegatingListener implements EgressListener {
        volatile EgressListener delegate;

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp,
                              final DirectBuffer buffer, final int offset, final int length, final Header header) {
            final EgressListener d = delegate;
            if (d != null) {
                d.onMessage(clusterSessionId, timestamp, buffer, offset, length, header);
            }
        }
    }

    static AeFeedClient connect(final BridgeConfig config, final String aeronDirectoryName) {
        final DelegatingListener holder = new DelegatingListener();
        final AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .ingressChannel("aeron:udp?term-length=4m")
                .ingressEndpoints(ingressEndpoints(config.aeClusterAddresses, config.aePortBase))
                .egressChannel("aeron:udp?endpoint=" + config.aeEgressEndpoint + "|term-length=4m")
                .egressListener(holder));
        final AeFeedClient client = new AeFeedClient(cluster);
        holder.delegate = client;
        return client;
    }

    /** Cluster ingress endpoint list using the AE ClusterConfig port math (client port = base + n*100 + 2). */
    static String ingressEndpoints(final List<String> addresses, final int portBase) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < addresses.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(i).append('=').append(addresses.get(i).trim()).append(':')
                    .append(portBase + i * 100 + 2);
        }
        return sb.toString();
    }

    @Override
    public void onMessage(final long clusterSessionId, final long timestamp,
                          final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.templateId() == FeedPositionReportDecoder.TEMPLATE_ID) {
            feedPositionDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            if (feedPositionDecoder.correlationId() == awaitedCorrelationId) {
                capturedPosition = new FeedPosition(
                        feedPositionDecoder.consumePosition(), feedPositionDecoder.lastAppliedTradeId());
            }
        }
        // All other egress (acks, balance updates, snapshots) is for other consumers; ignore.
    }

    /** The resume protocol: ask the AE how far it has consumed; retries within the deadline. */
    FeedPosition queryFeedPosition(final long timeoutMs) {
        awaitedCorrelationId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        capturedPosition = null;
        queryEncoder.wrapAndApplyHeader(queryBuffer, 0, headerEncoder).correlationId(awaitedCorrelationId);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + queryEncoder.encodedLength();

        final long deadline = System.currentTimeMillis() + timeoutMs;
        long nextSendMs = 0;
        while (capturedPosition == null) {
            final long nowMs = System.currentTimeMillis();
            if (nowMs > deadline) {
                throw new SessionClosedException("FeedPositionReport not received within " + timeoutMs + "ms");
            }
            if (nowMs >= nextSendMs) {
                offerOnce(queryBuffer, length); // re-send is safe: read-only query
                nextSendMs = nowMs + 500;
            }
            duty();
            idle.idle();
        }
        idle.reset();
        return capturedPosition;
    }

    /** Blocking offer: waits out backpressure forever; session death throws (epoch restart). */
    void offerBlocking(final DirectBuffer buffer, final int length) {
        while (true) {
            final long result = cluster.offer(buffer, 0, length);
            if (result > 0) {
                idle.reset();
                return;
            }
            if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED
                    || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new SessionClosedException("AE ingress offer failed terminally: " + result);
            }
            duty();
            idle.idle();
        }
    }

    private void offerOnce(final DirectBuffer buffer, final int length) {
        final long result = cluster.offer(buffer, 0, length);
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            throw new SessionClosedException("AE ingress offer failed terminally: " + result);
        }
    }

    /** Keep the session healthy: consume egress + protocol keep-alive. Call often from any wait loop. */
    void duty() {
        cluster.pollEgress();
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastKeepAliveMs >= KEEPALIVE_INTERVAL_MS) {
            cluster.sendKeepAlive();
            lastKeepAliveMs = nowMs;
        }
    }

    @Override
    public void close() {
        CloseHelper.quietClose(cluster);
    }
}

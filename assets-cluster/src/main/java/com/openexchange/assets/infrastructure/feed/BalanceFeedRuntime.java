// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.feed;

import com.openexchange.assets.infrastructure.InfrastructureConstants;
import com.openexchange.assets.infrastructure.Logger;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;

/**
 * Owns the balance-feed side-channel's moving parts on one node: the {@link BalanceConflationStore}
 * (written by the cluster service thread) and the {@link BalanceFeedAgent} (its own AgentRunner
 * thread) that publishes conflated BalanceUpdates on a plain Aeron publication. Mirrors
 * {@code MoneyJournalRuntime}'s two-step lifecycle: {@link #createIfEnabled} parses env and builds
 * the driver-independent store (so it can be wired onto the service before launch), then
 * {@link #start} launches the publisher thread once the node's media driver is up.
 *
 * <p><b>Feature switch: {@code BALANCE_FEED_CHANNEL}</b> (property {@code balance.feed.channel}).
 * Empty or unset — the default — means OFF: {@link #createIfEnabled} returns {@code null}, nothing
 * is wired, nothing starts, and node behavior is byte-for-byte unchanged. The channel is a plain
 * Aeron URI and is expected to become a stack-profile field:</p>
 * <ul>
 *   <li><b>Colo</b> — a real UDP multicast group, e.g.
 *       {@code aeron:udp?endpoint=224.20.30.39:24326|interface=192.168.1.0/24}: one send serves
 *       every subscriber on the segment.</li>
 *   <li><b>Cloud</b> (no multicast) — Aeron multi-destination-cast in dynamic mode, e.g.
 *       {@code aeron:udp?control=<node-host>:24325|control-mode=dynamic}: subscribers join by
 *       dialing the control endpoint and the driver fans out.</li>
 *   <li><b>Same-box consumers / tests</b> — {@code aeron:ipc}.</li>
 * </ul>
 *
 * <p>{@code BALANCE_FEED_STREAM_ID} (property {@code balance.feed.stream.id}) overrides the stream
 * id, default {@link InfrastructureConstants#BALANCE_FEED_STREAM_ID}.</p>
 *
 * <p>A per-node env var is legitimate here, unlike the journal's arming: nothing about the feed is
 * replicated state. The store is derived on every node from the replicated event stream, and whether
 * this node PUBLISHES it is a transport concern — exactly like the egress queues themselves.</p>
 */
public final class BalanceFeedRuntime implements AutoCloseable {

    private static final Logger log = Logger.getLogger(BalanceFeedRuntime.class);

    private final int nodeId;
    private final String channel;
    private final int streamId;
    private final BalanceConflationStore store = new BalanceConflationStore();

    private BalanceFeedAgent agent;
    private AgentRunner runner;

    private BalanceFeedRuntime(final int nodeId, final String channel, final int streamId) {
        this.nodeId = nodeId;
        this.channel = channel;
        this.streamId = streamId;
    }

    /**
     * Build this node's feed machinery if {@code BALANCE_FEED_CHANNEL} names a channel; {@code null}
     * (feature off, the default) otherwise. Env-var-first with a system-property fallback, the same
     * idiom as the rest of the node's config (see {@code AeronCluster}).
     */
    public static BalanceFeedRuntime createIfEnabled(final int nodeId) {
        final String channel = envOrProp("BALANCE_FEED_CHANNEL", "balance.feed.channel");
        if (channel == null) {
            return null; // OFF: nothing wired, nothing started, zero behavior change
        }
        final String stream = envOrProp("BALANCE_FEED_STREAM_ID", "balance.feed.stream.id");
        final int streamId = stream == null
                ? InfrastructureConstants.BALANCE_FEED_STREAM_ID
                : Integer.parseInt(stream);
        log.info("Balance feed enabled: node=%d channel=%s stream=%d (conflated latest-value "
                + "side-channel; authoritative balances remain the cluster)", nodeId, channel, streamId);
        return new BalanceFeedRuntime(nodeId, channel, streamId);
    }

    /** The store to wire onto the service (safe before {@link #start}: it only conflates). */
    public BalanceConflationStore store() {
        return store;
    }

    /**
     * Launch the publisher thread. Call once the node's media driver is reachable at
     * {@code aeronDirectoryName}; the agent connects lazily with backoff, so calling this before the
     * driver is fully up is safe. Call once per process.
     */
    public BalanceFeedRuntime start(final String aeronDirectoryName, final ErrorHandler errorHandler) {
        agent = new BalanceFeedAgent(store, aeronDirectoryName, channel, streamId);
        runner = new AgentRunner(new BackoffIdleStrategy(), errorHandler, null, agent);
        AgentRunner.startOnThread(runner);
        log.info("Balance feed publisher started (node %d)", nodeId);
        return this;
    }

    /** The agent, for tests/diagnostics; {@code null} before {@link #start}. */
    public BalanceFeedAgent agent() {
        return agent;
    }

    @Override
    public void close() {
        CloseHelper.quietClose(runner);
    }

    private static String envOrProp(final String envName, final String propName) {
        final String env = System.getenv(envName);
        if (env != null && !env.isEmpty()) {
            return env;
        }
        final String prop = System.getProperty(propName);
        return prop == null || prop.isEmpty() ? null : prop;
    }
}

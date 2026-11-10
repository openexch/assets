// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.bridge;

import java.util.List;

/**
 * Env-driven bridge configuration. The bridge is stateless: everything here is connection
 * shape, not position state (positions come from the core's FeedPositionReport each epoch).
 */
public final class BridgeConfig {

    /** ME journal archive control endpoints, first-healthy-wins (all three nodes journal identically). */
    public final List<String> journalArchiveEndpoints;
    /** Core cluster member addresses (CSV) + port base for the ingress endpoint math. */
    public final List<String> coreClusterAddresses;
    public final int corePortBase;
    /** The bridge's own egress listen endpoint (must differ from the OMS client's 9393). */
    public final String coreEgressEndpoint;
    /**
     * The bridge's own routable host: the address the REMOTE ME journal archive should send
     * control responses and replay data back to. "localhost" only works co-resident; a cross-host
     * bridge MUST advertise its routable IP or the archive replay silently starves (0 forwarded
     * trades). Defaults to the host part of {@link #coreEgressEndpoint} so one setting (the bridge's
     * IP in BRIDGE_CORE_EGRESS_ENDPOINT) fixes both the core egress AND the archive replay.
     */
    public final String localHost;
    /** Latch HALT on a dense-tradeId gap (production default true; false = operator override). */
    public final boolean haltOnGap;
    public final long queryTimeoutMs;
    /** Port for the /metrics + /health HTTP endpoint (0 = ephemeral, for tests). */
    public final int metricsPort;

    public BridgeConfig(
            final List<String> journalArchiveEndpoints,
            final List<String> coreClusterAddresses,
            final int corePortBase,
            final String coreEgressEndpoint,
            final String localHost,
            final boolean haltOnGap,
            final long queryTimeoutMs,
            final int metricsPort) {
        this.journalArchiveEndpoints = journalArchiveEndpoints;
        this.coreClusterAddresses = coreClusterAddresses;
        this.corePortBase = corePortBase;
        this.coreEgressEndpoint = coreEgressEndpoint;
        this.localHost = localHost;
        this.haltOnGap = haltOnGap;
        this.queryTimeoutMs = queryTimeoutMs;
        this.metricsPort = metricsPort;
    }

    public static BridgeConfig fromEnv() {
        final String egressEndpoint = envOr("BRIDGE_CORE_EGRESS_ENDPOINT", "127.0.0.1:9394");
        return new BridgeConfig(
                List.of(envOr("BRIDGE_ME_JOURNAL_ARCHIVES", "localhost:9010,localhost:9110,localhost:9210").split(",")),
                List.of(envOr("BRIDGE_CORE_CLUSTER_ADDRESSES", "127.0.0.1").split(",")),
                Integer.parseInt(envOr("BRIDGE_CORE_PORT_BASE", "9300")),
                egressEndpoint,
                envOr("BRIDGE_LOCAL_HOST", hostOf(egressEndpoint)),
                !"false".equalsIgnoreCase(envOr("BRIDGE_HALT_ON_GAP", "true")),
                Long.parseLong(envOr("BRIDGE_QUERY_TIMEOUT_MS", "5000")),
                Integer.parseInt(envOr("BRIDGE_METRICS_PORT", "9600")));
    }

    /** Host part of a "host:port" endpoint (used to derive localHost from the egress endpoint). */
    private static String hostOf(final String endpoint) {
        final int colon = endpoint.lastIndexOf(':');
        return colon > 0 ? endpoint.substring(0, colon) : endpoint;
    }

    private static String envOr(final String key, final String fallback) {
        final String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }
}

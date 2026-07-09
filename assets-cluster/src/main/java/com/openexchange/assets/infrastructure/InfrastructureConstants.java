// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure;

/**
 * Cluster/transport constants for the Assets Engine. Deliberately <b>disjoint</b> from the matching
 * engine so the two Aeron clusters can co-reside without colliding on ports, shared-memory dirs, or
 * driver dirs.
 *
 * <ul>
 *   <li>Matching engine: cluster port base 9000 (node0 9000, node1 9100, node2 9200).</li>
 *   <li>Assets engine:   cluster port base <b>9300</b> (node0 9300, node1 9400, node2 9500).</li>
 * </ul>
 */
public final class InfrastructureConstants {

    private InfrastructureConstants() {
    }

    /** Cluster consensus port base; each node uses base + nodeId*100 + fixed offsets. */
    public static final int CLUSTER_PORT_BASE = 9300;

    /** Ports consumed per node (matches the Aeron ClusterConfig layout). */
    public static final int PORTS_PER_NODE = 100;

    /** Shared-memory base dir for a node's cluster + archive state. */
    public static final String NODE_BASE_DIR_PREFIX = "/dev/shm/aeron-assets/ae";

    /** Term buffer length for UDP channels (matches the ME for parity). */
    public static final int TERM_BUFFER_LENGTH = 16 * 1024 * 1024;

    /** Session timeout. */
    public static final long SESSION_TIMEOUT_NS = 10_000_000_000L;
}

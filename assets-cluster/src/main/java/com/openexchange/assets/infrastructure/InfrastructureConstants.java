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

    // ==================== MONEY JOURNAL ====================
    // The money journal (dark unless AE_MONEY_JOURNAL_ENABLED=true) is a dedicated publication
    // recorded by the node's OWN archive as a separate recording next to the consensus log: every
    // APPLIED money movement (money-journal-schema.xml, schema id=3), appended on the deterministic
    // service thread so all replicas produce byte-identical journal payloads. Substrate for the
    // external projector (Part 2). Mirrors the ME settlement journal (stream 4001 on the ME's own
    // driver); ids cannot collide across the two clusters (separate drivers) but stay disjoint
    // anyway for greppability.

    /** Aeron stream id the money journal is recorded/replayed on (node-local IPC). */
    public static final int MONEY_JOURNAL_STREAM_ID = 4101;

    /** Aeron IPC channel the money journal is published on. */
    public static final String MONEY_JOURNAL_CHANNEL = "aeron:ipc?term-length=4m";
}

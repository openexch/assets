// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.cluster.service.Cluster;

/**
 * Positive control for {@code ReplicaClosureNeverSchedulesTest} (test scope only): a replica-side class that reads
 * the wall clock and schedules a cluster timer, both of which the replica closure must never do.
 */
public final class ForbiddenReplicaClosureProbe {
    private ForbiddenReplicaClosureProbe() {
    }

    public static boolean probe(final Cluster cluster) {
        return cluster.scheduleTimer(1, System.currentTimeMillis() + 1_000);
    }
}

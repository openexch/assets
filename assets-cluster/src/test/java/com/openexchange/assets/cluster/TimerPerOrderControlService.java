// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RCS S4 contract 11.9, the control of {@code CmSnapshotSizeClusterTest} (RCS-A04): a TEST-ONLY clustered service that
 * schedules one cluster timer per ingress message (per "order"), far in the future, which is the design S4 exists to
 * avoid. The consensus module snapshots every pending timer, so the same CM-snapshot gate must FAIL on it at 100k. It
 * lives in test scope only: {@code src/main} keeps its two {@code scheduleTimer} sites ({@code
 * NoNewClusterTimerCallSiteTest}).
 */
final class TimerPerOrderControlService implements ClusteredService {

    /** One day: no timer fires during the test. */
    private static final long DEADLINE_MS = TimeUnit.DAYS.toMillis(1);

    final AtomicLong scheduled = new AtomicLong();
    private Cluster cluster;
    private long nextCorrelationId = 1;

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
    }

    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                 final int offset, final int length, final Header header) {
        // timestamps are milliseconds (the AE's cluster time unit)
        while (!cluster.scheduleTimer(nextCorrelationId, timestamp + DEADLINE_MS)) {
            cluster.idleStrategy().idle();
        }
        nextCorrelationId++;
        scheduled.incrementAndGet();
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        // the service image is irrelevant here; only the consensus module's snapshot is measured
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
    }

    @Override
    public void onTerminate(final Cluster cluster) {
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import io.aeron.ExclusivePublication;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * A stopped recording above L on a cluster snapshot stream that the backup
 * RecordingLog does not reference (the shape a failed backup cycle leaves: a retrieved set never recorded) is an
 * orphan; housekeeping purges it after two passes with the backup BACKING_UP and the same re-check, and never purges
 * L, a referenced recording or a shadow.
 */
public class ReplicaOrphanAboveLTest {

    private static final int PORT_BASE = 39000;
    /** {@code ClusteredServiceContainer.Configuration.SNAPSHOT_STREAM_ID_DEFAULT}, in REPLICA_SNAPSHOT_STREAM_IDS. */
    private static final int SERVICE_SNAPSHOT_STREAM = 106;

    @Test(timeout = 600_000)
    public void aStoppedUnreferencedRecordingAboveLIsPurged() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-orphan")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 2);
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long l = r.replica.follower().logRecordingId();
            final java.util.Set<Long> referenced = r.recordingLogIds("replica");

            final long orphan = recordStopped(r);
            assertTrue("the orphan sits above L=" + l + ": " + orphan, orphan > l);
            System.out.println("[orphan] a stopped, unreferenced recording rec=" + orphan + " on stream "
                    + SERVICE_SNAPSHOT_STREAM + " above L=" + l);
            try {
                r.rig.await(() -> !r.inReplicaArchive(orphan), 60_000, "v6");
            } catch (final AssertionError timeout) {
                throw new AssertionError("orphan recording rec=" + orphan + " above L=" + l + " not purged"
                        + " within 60 s (shadow_replica_orphan_recordings="
                        + r.metric("shadow_replica_orphan_recordings") + ", backup state "
                        + r.metric("shadow_replica_backup_state") + ")");
            }
            assertTrue("L survives", r.inReplicaArchive(l));
            for (final long id : referenced) {
                assertTrue("a referenced recording survives: " + id, r.inReplicaArchive(id));
            }
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            System.out.println("GREEN orphan rec=" + orphan + " above L=" + l + " purged; L and " + referenced
                    + " kept");
        }
    }

    /** A small recording on the service snapshot stream in the replica archive, stopped (a failed cycle's copy). */
    private static long recordStopped(final ReplicaRig r) {
        final String channel = "aeron:ipc?term-length=64k";
        final io.aeron.archive.client.AeronArchive archive = r.replicaArchive();
        final long subscriptionId = archive.startRecording(channel, SERVICE_SNAPSHOT_STREAM, SourceLocation.LOCAL);
        try (ExclusivePublication pub = r.replica.aeron().addExclusivePublication(channel, SERVICE_SNAPSHOT_STREAM)) {
            final CountersReader counters = r.replica.aeron().countersReader();
            final int[] counterId = {CountersReader.NULL_COUNTER_ID};
            r.rig.await(() -> (counterId[0] = RecordingPos.findCounterIdBySession(counters, pub.sessionId(),
                    archive.archiveId())) != CountersReader.NULL_COUNTER_ID, 30_000, "recording counter");
            final long recordingId = RecordingPos.getRecordingId(counters, counterId[0]);
            final UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
            for (int i = 0; i < 4; i++) {
                r.rig.await(() -> pub.offer(buf, 0, buf.capacity()) > 0, 10_000, "offer");
            }
            final long target = pub.position();
            r.rig.await(() -> counters.getCounterValue(counterId[0]) >= target, 10_000, "recorded");
            archive.stopRecording(subscriptionId);
            r.rig.await(() -> archive.getStopPosition(recordingId) >= 0, 10_000, "stopped");
            return recordingId;
        }
    }
}

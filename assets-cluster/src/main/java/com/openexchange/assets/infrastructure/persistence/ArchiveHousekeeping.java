// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reclaims the Assets Engine's archive disk after cluster snapshots.
 *
 * Aeron snapshots do NOT truncate the cluster log — they only ADD recordings
 * (consensus module state + service state) plus a recording.log entry. Disk
 * reclamation is a separate, explicit archive operation, and until this class
 * existed the AE had none: its log grew forever. On 2026-07-25 that filled
 * /dev/shm, the archive began failing writes with "No space left on device",
 * AE nodes terminated, the settlement bridge could no longer map a journal
 * replay image, and the money path stopped for seventeen hours. A bigger box
 * only moves that day further out. This is what removes it.
 *
 * What it does: purge whole log segment files below the latest valid snapshot
 * position (recovery = latest snapshot + log replay from its position, so the
 * log below that point is never needed by this node again).
 *
 * Snapshot recordings themselves are deliberately NOT purged: Aeron recovery
 * selects the snapshot to replay by the recording.log {@code isValid} flag without
 * checking the archive, so purging a recording while its recording.log entry remains
 * valid breaks recover-from-snapshot ("unknown recording id"). Snapshot recordings are
 * tiny (serialized state); the reclaimable disk is the log segments. A post-run check
 * verifies every referenced snapshot recording still resolves in the archive.
 *
 * Must run against each node's own archive. Operational caveats (document,
 * don't discover): a member that was offline during the snapshot cannot
 * log-catch-up across purged segments and must be reseeded from a snapshot.
 * This is match#35 and it applies here identically — NEVER housekeep while a
 * member is down, lagging, or recovering.
 *
 * Ported from the matching engine's tool (match-cluster) rather than shared:
 * the logic is pure Aeron (RecordingLog + AeronArchive) with nothing
 * engine-specific, and the AE must not take a compile dependency on
 * match-cluster. Keep the two in step when either changes.
 *
 * Loud-limits principle: every action and every skipped action is reported.
 */
public final class ArchiveHousekeeping {

    /** Outcome of a housekeeping run, for reporting and assertions. */
    public static final class Result {
        public final long logRecordingId;
        public final long previousStartPosition;
        public final long newStartPosition;
        public final long segmentsPurged;
        public final long logBytesReclaimed;
        public final int errors;

        Result(final long logRecordingId, final long previousStartPosition,
               final long newStartPosition, final long segmentsPurged, final int errors) {
            this.logRecordingId = logRecordingId;
            this.previousStartPosition = previousStartPosition;
            this.newStartPosition = newStartPosition;
            this.segmentsPurged = segmentsPurged;
            this.logBytesReclaimed = newStartPosition - previousStartPosition;
            this.errors = errors;
        }

        @Override
        public String toString() {
            return "Result{logRecordingId=" + logRecordingId
                    + ", previousStartPosition=" + previousStartPosition
                    + ", newStartPosition=" + newStartPosition
                    + ", segmentsPurged=" + segmentsPurged
                    + ", logBytesReclaimed=" + logBytesReclaimed
                    + ", errors=" + errors + "}";
        }
    }

    private ArchiveHousekeeping() {
    }

    /**
     * Purge whole log segments below the latest valid snapshot. Snapshot recordings are
     * never purged (doing so breaks recover-from-snapshot — see class javadoc); this run
     * also verifies that every snapshot recording referenced by recording.log still resolves.
     *
     * @param clusterDir the node's cluster directory (contains recording.log)
     * @param archive    connected client to this node's archive
     * @return what was reclaimed
     */
    public static Result purgeBelowLatestSnapshot(final File clusterDir, final AeronArchive archive) {
        final List<RecordingLog.Entry> snapshotEntries = new ArrayList<>();
        final long logRecordingId;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid) {
                    snapshotEntries.add(entry);
                }
            }
            logRecordingId = recordingLog.findLastTermRecordingId();
        }

        if (logRecordingId == -1) {
            throw new IllegalStateException(
                    "[AE-HOUSEKEEPING] No cluster log recording found in " + clusterDir);
        }

        final long[] descriptor = new long[3]; // startPosition, segmentFileLength, termBufferLength
        final int found = archive.listRecording(logRecordingId,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                    descriptor[0] = startPosition;
                    descriptor[1] = segmentFileLength;
                    descriptor[2] = termBufferLength;
                });
        if (found == 0) {
            throw new IllegalStateException(
                    "[AE-HOUSEKEEPING] Log recording " + logRecordingId + " not found in archive");
        }
        final long startPosition = descriptor[0];

        if (snapshotEntries.isEmpty()) {
            System.out.println("[AE-HOUSEKEEPING] No valid snapshot — nothing reclaimable. "
                    + "Take a snapshot first; the log below it can then be purged.");
            return new Result(logRecordingId, startPosition, startPosition, 0, 0);
        }

        // Snapshot groups: entries sharing a logPosition (consensus module + each service)
        final List<Long> groupPositions = snapshotEntries.stream()
                .map(e -> e.logPosition)
                .distinct()
                .sorted(Comparator.reverseOrder())
                .toList();
        final long latestSnapshotPosition = groupPositions.get(0);

        // Purge whole log segments below the latest snapshot position.
        final long newStartPosition = AeronArchive.segmentFileBasePosition(
                startPosition, latestSnapshotPosition, (int) descriptor[2], (int) descriptor[1]);

        long segmentsPurged = 0;
        if (newStartPosition > startPosition) {
            segmentsPurged = archive.purgeSegments(logRecordingId, newStartPosition);
            System.out.println("[AE-HOUSEKEEPING] Purged " + segmentsPurged + " log segment(s): "
                    + "recordingId=" + logRecordingId
                    + " startPosition " + startPosition + " -> " + newStartPosition
                    + " (" + (newStartPosition - startPosition) + " bytes reclaimed, "
                    + "snapshot at " + latestSnapshotPosition + ")");
        } else {
            System.out.println("[AE-HOUSEKEEPING] No whole log segment below snapshot position "
                    + latestSnapshotPosition + " (startPosition=" + startPosition
                    + ", segmentFileLength=" + descriptor[1] + ") — nothing purged. "
                    + "Smaller segmentFileLength reclaims sooner.");
        }

        // Safety net: every valid snapshot recording the recording.log still references MUST resolve
        // in the archive, or recover-from-snapshot will crash. Verify and report loudly — this is the
        // exact invariant whose violation == the recovery bug, and it catches an already-corrupted node.
        int errors = 0;
        for (final RecordingLog.Entry entry : snapshotEntries) {
            final int exists = archive.listRecording(entry.recordingId,
                    (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                     startPos, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                     mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                     sourceIdentity) -> {
                    });
            if (exists == 0) {
                errors++;
                System.err.println("[AE-HOUSEKEEPING] CRITICAL: recording.log references snapshot "
                        + "recordingId " + entry.recordingId + " (serviceId=" + entry.serviceId
                        + ", logPosition=" + entry.logPosition + ") that is NOT in the archive — "
                        + "recover-from-snapshot will fail on this node; it must be reseeded.");
            }
        }

        return new Result(logRecordingId, startPosition, newStartPosition, segmentsPurged, errors);
    }

    /**
     * CLI entry point, invoked per node by the admin gateway after a successful
     * snapshot. The trailing argument is accepted and ignored for call-site
     * compatibility with the matching engine's tool (the admin passes the same
     * argv shape to both).
     *
     * Usage: ArchiveHousekeeping &lt;clusterDir&gt; &lt;aeronDir&gt; [ignored]
     */
    public static void main(final String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ArchiveHousekeeping <clusterDir> <aeronDir> [ignored]");
            System.exit(2);
        }
        final File clusterDir = new File(args[0]);
        final String aeronDir = args[1];

        try (AeronArchive archive = AeronArchive.connect(
                new AeronArchive.Context()
                        .controlRequestChannel("aeron:ipc?term-length=16m")
                        .controlResponseChannel("aeron:ipc?term-length=16m")
                        .aeronDirectoryName(aeronDir))) {

            final Result result = purgeBelowLatestSnapshot(clusterDir, archive);
            System.out.println("[AE-HOUSEKEEPING] Done: " + result);
            if (result.errors > 0) {
                System.exit(1);
            }
        } catch (final Exception e) {
            System.err.println("[AE-HOUSEKEEPING] FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

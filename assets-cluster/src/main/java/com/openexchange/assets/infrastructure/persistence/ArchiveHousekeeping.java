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

    /** Named so a leftover positional argument can never be read as a position. */
    private static final String WATERMARK_FLAG = "--watermark=";

    /**
     * Extract {@code --watermark=N}, or {@link Long#MAX_VALUE} when absent.
     *
     * <p>Positional arguments are ignored outright, never guessed at: see the
     * class docs on why reading the legacy {@code "2"} as a position would
     * rebuild the outage.
     */
    static long watermarkFrom(final String[] args) {
        for (int i = 2; i < args.length; i++) {
            if (!args[i].startsWith(WATERMARK_FLAG)) {
                continue;
            }
            final String value = args[i].substring(WATERMARK_FLAG.length());
            try {
                final long parsed = Long.parseLong(value);
                if (parsed < 0) {
                    System.err.println("[AE-HOUSEKEEPING] negative watermark " + parsed
                            + " ignored; purging to the latest snapshot instead");
                    return Long.MAX_VALUE;
                }
                return parsed;
            } catch (final NumberFormatException e) {
                System.err.println("[AE-HOUSEKEEPING] FAILED: unparseable "
                        + WATERMARK_FLAG + "value: " + value);
                System.exit(2);
            }
        }
        return Long.MAX_VALUE;
    }

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
        return purgeBelow(clusterDir, archive, Long.MAX_VALUE);
    }

    /**
     * Purge whole log segments below {@code watermark}, never above the latest
     * valid snapshot.
     *
     * <p>The watermark is the caller's answer to "what has every consumer already
     * passed?" - the slowest live member, the backup, and the position durably
     * stored in S3. The snapshot bound stays because recovery on THIS node needs
     * everything above it; the watermark bounds it further for everyone else.
     *
     * <p>Passing {@link Long#MAX_VALUE} means "no external constraint", which is
     * the pre-watermark behaviour and what the deprecated entry point above
     * still asks for.
     *
     * @param watermark lowest position any consumer still needs
     */
    public static Result purgeBelow(final File clusterDir, final AeronArchive archive,
                                    final long watermark) {
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

        // The purge point is the LOWER of what this node needs (its newest
        // snapshot) and what every other consumer has already passed. Ignoring
        // the second is how housekeeping strands a member that was behind
        // (match#35) and how it can delete log that S3 has not stored yet.
        final long purgePosition = Math.min(latestSnapshotPosition, watermark);
        if (purgePosition < latestSnapshotPosition) {
            System.out.println("[AE-HOUSEKEEPING] Watermark holds the purge at " + purgePosition
                    + " (snapshot is at " + latestSnapshotPosition + "): a consumer has not"
                    + " passed it yet. Reclaiming less on purpose.");
        }

        // Purge whole log segments below the purge position.
        final long newStartPosition = AeronArchive.segmentFileBasePosition(
                startPosition, purgePosition, (int) descriptor[2], (int) descriptor[1]);

        long segmentsPurged = 0;
        if (newStartPosition > startPosition) {
            segmentsPurged = archive.purgeSegments(logRecordingId, newStartPosition);
            System.out.println("[AE-HOUSEKEEPING] Purged " + segmentsPurged + " log segment(s): "
                    + "recordingId=" + logRecordingId
                    + " startPosition " + startPosition + " -> " + newStartPosition
                    + " (" + (newStartPosition - startPosition) + " bytes reclaimed, "
                    + "purge point " + purgePosition
                    + ", snapshot at " + latestSnapshotPosition + ")");
        } else {
            System.out.println("[AE-HOUSEKEEPING] No whole log segment below purge position "
                    + purgePosition + " (startPosition=" + startPosition
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
     * snapshot.
     *
     * <p>The retention WATERMARK - the lowest position any consumer still needs,
     * computed by the gateway from the slowest live member, the backup, and what
     * is durably in S3 - arrives as a NAMED argument, {@code --watermark=N}.
     *
     * <p>Named rather than positional for a reason that nearly shipped as a bug:
     * the historical call site passes a bare {@code "2"} in this slot (a long
     * dead {@code snapshotsToKeep}), and {@code "2"} parses perfectly well as a
     * position. Read positionally it would mean "purge nothing", the log would
     * grow unbounded, and the disk would fill - the exact 2026-07-25 outage,
     * reintroduced by an argument nobody changed. A named flag cannot be
     * confused with a leftover.
     *
     * <p>Absent, the purge falls back to the latest snapshot alone, which is the
     * pre-watermark behaviour. Falling back rather than refusing is deliberate:
     * an operator running this by hand against a filling disk must not be
     * blocked by an argument they do not have.
     *
     * Usage: ArchiveHousekeeping &lt;clusterDir&gt; &lt;aeronDir&gt; [--watermark=N] [ignored]
     */
    public static void main(final String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ArchiveHousekeeping <clusterDir> <aeronDir> [ignored]");
            System.exit(2);
        }
        final File clusterDir = new File(args[0]);
        final String aeronDir = args[1];

        final long watermark = watermarkFrom(args);

        try (AeronArchive archive = AeronArchive.connect(
                new AeronArchive.Context()
                        .controlRequestChannel("aeron:ipc?term-length=16m")
                        .controlResponseChannel("aeron:ipc?term-length=16m")
                        .aeronDirectoryName(aeronDir))) {

            final Result result = purgeBelow(clusterDir, archive, watermark);
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

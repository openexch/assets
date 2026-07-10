// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * One epoch's attachment to an ME settlement-journal archive: discover the recording chain
 * for stream 4001, replay every stopped recording (older node incarnations) in recordingId
 * order, then live-follow the active recording (an unbounded archive replay keeps streaming
 * as the recording grows — the journal's only consumer transport is the archive, so this
 * also works across hosts unchanged).
 *
 * Deliberately epoch-scoped and disposable: any failure (source node dies, replay image
 * closes, purge race) is handled by the caller discarding this instance and building a new
 * one — the BridgeFilter's dedupe rules make re-reading from anywhere safe.
 */
final class JournalSource implements AutoCloseable {

    /** Mirrors match InfrastructureConstants (kept literal: the bridge must not depend on match-cluster). */
    static final int SETTLEMENT_JOURNAL_STREAM_ID = 4001;
    static final int JOURNAL_ARCHIVE_CONTROL_STREAM_ID = 4010;
    private static final String JOURNAL_CHANNEL_URI = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 9101;

    record Recording(long recordingId, long startPosition, long stopPosition) {
        boolean isActive() {
            return stopPosition == AeronArchive.NULL_POSITION;
        }
    }

    private final AeronArchive archive;
    private final String endpoint;

    private JournalSource(final AeronArchive archive, final String endpoint) {
        this.archive = archive;
        this.endpoint = endpoint;
    }

    /** First-healthy-wins across the configured journal archives (all nodes journal identically). */
    static JournalSource connectFirstHealthy(final BridgeConfig config, final String aeronDirectoryName) {
        Exception last = null;
        for (final String endpoint : config.journalArchiveEndpoints) {
            try {
                final AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .controlRequestChannel("aeron:udp?endpoint=" + endpoint.trim())
                        .controlRequestStreamId(JOURNAL_ARCHIVE_CONTROL_STREAM_ID)
                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
                return new JournalSource(archive, endpoint.trim());
            } catch (Exception e) {
                last = e;
            }
        }
        throw new IllegalStateException("no journal archive reachable from "
                + config.journalArchiveEndpoints, last);
    }

    String endpoint() {
        return endpoint;
    }

    /** The journal recording chain, ascending recordingId (chronological incarnations). */
    List<Recording> recordings() {
        final List<Recording> out = new ArrayList<>();
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, JOURNAL_CHANNEL_URI, SETTLEMENT_JOURNAL_STREAM_ID,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                 stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                 sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                        out.add(new Recording(recordingId, startPosition, stopPosition)));
        return out;
    }

    /**
     * Open a replay subscription over [recording.startPosition, ...): bounded for a stopped
     * recording, unbounded live-follow for the active one. Caller polls and closes.
     *
     * The archive CREATES a publication to the replay endpoint, so an ephemeral port must be
     * bound and resolved on our side FIRST — subscribe on port 0, wait for the OS-assigned
     * endpoint, then ask the archive to replay to the resolved address.
     */
    Subscription openReplay(final Recording recording) {
        final long length = recording.isActive()
                ? Long.MAX_VALUE
                : recording.stopPosition() - recording.startPosition();

        final Subscription sub = archive.context().aeron()
                .addSubscription("aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID);
        final long deadline = System.currentTimeMillis() + 10_000;
        String resolved;
        while ((resolved = sub.resolvedEndpoint()) == null) {
            if (System.currentTimeMillis() > deadline) {
                CloseHelper.quietClose(sub);
                throw new IllegalStateException("replay endpoint did not resolve within 10s");
            }
            Thread.onSpinWait();
        }
        try {
            archive.startReplay(recording.recordingId(), recording.startPosition(), length,
                    "aeron:udp?endpoint=" + resolved, REPLAY_STREAM_ID);
        } catch (RuntimeException e) {
            CloseHelper.quietClose(sub);
            throw e;
        }
        return sub;
    }

    /** Poll one batch of journal fragments into the handler. */
    int poll(final Subscription replay, final FragmentHandler handler, final int limit) {
        return replay.poll(handler, limit);
    }

    /**
     * Current recorded position of an ACTIVE recording, or {@link AeronArchive#NULL_POSITION}
     * once it has stopped. Doubles as an archive liveness probe: throws when the archive
     * (the source node) is gone, which the caller turns into an epoch restart.
     */
    long recordingPosition(final long recordingId) {
        return archive.getRecordingPosition(recordingId);
    }

    /** Final position of a stopped recording, or {@link AeronArchive#NULL_POSITION} while active. */
    long stopPosition(final long recordingId) {
        return archive.getStopPosition(recordingId);
    }

    @Override
    public void close() {
        CloseHelper.quietClose(archive);
    }
}

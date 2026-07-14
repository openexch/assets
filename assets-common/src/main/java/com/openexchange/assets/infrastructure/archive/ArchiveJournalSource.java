// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.archive;

import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameterized attachment to an Aeron Archive that RECORDS a journal stream: discover the
 * recording chain for a stream id, replay every stopped recording (older node incarnations)
 * in recordingId order, then live-follow the active recording (an unbounded archive replay
 * keeps streaming as the recording grows, which also works across hosts unchanged).
 *
 * Extracted verbatim from the settlement bridge's original JournalSource so the settlement
 * bridge (ME journal: control stream 4010, journal stream 4001) and the money projector
 * (AE money journal: control stream 10, journal stream 4101) share ONE follow implementation
 * instead of copy-forking it. The only differences between the two are the connection
 * parameters passed at construction; the replay/liveness mechanics are identical.
 *
 * Deliberately epoch-scoped and disposable: any failure (source node dies, replay image
 * closes, purge race) is handled by the caller discarding this instance and building a new
 * one. The caller's dense-sequence dedupe makes re-reading from anywhere safe.
 */
public final class ArchiveJournalSource implements AutoCloseable {

    public record Recording(long recordingId, long startPosition, long stopPosition) {
        public boolean isActive() {
            return stopPosition == AeronArchive.NULL_POSITION;
        }
    }

    private final AeronArchive archive;
    private final String endpoint;
    private final int journalStreamId;
    private final String channelUri;
    private final int replayStreamId;

    private ArchiveJournalSource(final AeronArchive archive, final String endpoint,
                                 final int journalStreamId, final String channelUri,
                                 final int replayStreamId) {
        this.archive = archive;
        this.endpoint = endpoint;
        this.journalStreamId = journalStreamId;
        this.channelUri = channelUri;
        this.replayStreamId = replayStreamId;
    }

    /**
     * First-healthy-wins across the configured archive control endpoints (all nodes journal
     * identically, so any reachable one is a valid source).
     *
     * @param archiveEndpoints   archive control endpoints, tried in order (host:port each)
     * @param controlStreamId    the archive's control request stream id
     * @param journalStreamId    the recorded journal stream id to discover/replay
     * @param channelUri         the (stripped) channel the journal is recorded on
     * @param replayStreamId     the stream id to open the local replay subscription on
     * @param aeronDirectoryName the embedded media driver dir to connect the archive client through
     */
    public static ArchiveJournalSource connectFirstHealthy(
            final List<String> archiveEndpoints,
            final int controlStreamId,
            final int journalStreamId,
            final String channelUri,
            final int replayStreamId,
            final String aeronDirectoryName) {
        Exception last = null;
        for (final String endpoint : archiveEndpoints) {
            try {
                final AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .controlRequestChannel("aeron:udp?endpoint=" + endpoint.trim())
                        .controlRequestStreamId(controlStreamId)
                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
                return new ArchiveJournalSource(archive, endpoint.trim(), journalStreamId,
                        channelUri, replayStreamId);
            } catch (Exception e) {
                last = e;
            }
        }
        throw new IllegalStateException("no journal archive reachable from " + archiveEndpoints, last);
    }

    public String endpoint() {
        return endpoint;
    }

    /** The journal recording chain, ascending recordingId (chronological incarnations). */
    public List<Recording> recordings() {
        final List<Recording> out = new ArrayList<>();
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, channelUri, journalStreamId,
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
     * bound and resolved on our side FIRST: subscribe on port 0, wait for the OS-assigned
     * endpoint, then ask the archive to replay to the resolved address.
     */
    public Subscription openReplay(final Recording recording) {
        final long length = recording.isActive()
                ? Long.MAX_VALUE
                : recording.stopPosition() - recording.startPosition();

        final Subscription sub = archive.context().aeron()
                .addSubscription("aeron:udp?endpoint=localhost:0", replayStreamId);
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
                    "aeron:udp?endpoint=" + resolved, replayStreamId);
        } catch (RuntimeException e) {
            CloseHelper.quietClose(sub);
            throw e;
        }
        return sub;
    }

    /** Poll one batch of journal fragments into the handler. */
    public int poll(final Subscription replay, final FragmentHandler handler, final int limit) {
        return replay.poll(handler, limit);
    }

    /**
     * Current recorded position of an ACTIVE recording, or {@link AeronArchive#NULL_POSITION}
     * once it has stopped. Doubles as an archive liveness probe: throws when the archive
     * (the source node) is gone, which the caller turns into an epoch restart.
     */
    public long recordingPosition(final long recordingId) {
        return archive.getRecordingPosition(recordingId);
    }

    /** Final position of a stopped recording, or {@link AeronArchive#NULL_POSITION} while active. */
    public long stopPosition(final long recordingId) {
        return archive.getStopPosition(recordingId);
    }

    @Override
    public void close() {
        CloseHelper.quietClose(archive);
    }
}

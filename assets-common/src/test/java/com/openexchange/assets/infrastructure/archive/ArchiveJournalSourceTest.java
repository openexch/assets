// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.archive;

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.DatagramSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.*;

/** Exercises the real Aeron catalog and replay validator, with isolated disk-backed dirs. */
public class ArchiveJournalSourceTest {
    private static final int CONTROL_STREAM_ID = 4010;
    private static final int JOURNAL_STREAM_ID = 4001;
    private static final int REPLAY_STREAM_ID = 4080;

    private File directory;
    private String endpoint;
    private MediaDriver driver;
    private Archive archive;
    private AeronArchive writer;
    private ArchiveJournalSource source;
    private ExclusivePublication publication;

    @Before
    public void setUp() throws Exception {
        directory = Files.createTempDirectory("archive-journal-source-").toFile();
        try (DatagramSocket socket = new DatagramSocket(0)) {
            endpoint = "localhost:" + socket.getLocalPort();
        }
        driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .aeronDirectoryName(new File(directory, "driver").getAbsolutePath())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        launchArchive();
    }

    private void launchArchive() {
        archive = Archive.launch(new Archive.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .archiveDir(new File(directory, "archive"))
                .controlChannel("aeron:udp?endpoint=" + endpoint)
                .controlStreamId(CONTROL_STREAM_ID)
                .localControlStreamId(CONTROL_STREAM_ID + 1)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .fileSyncLevel(0)
                .segmentFileLength(1024 * 1024));
        writer = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .controlRequestChannel("aeron:udp?endpoint=" + endpoint)
                .controlRequestStreamId(CONTROL_STREAM_ID)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
        source = connectSource();
    }

    private ArchiveJournalSource connectSource() {
        return ArchiveJournalSource.connectFirstHealthy(List.of(endpoint), CONTROL_STREAM_ID,
                JOURNAL_STREAM_ID, "aeron:ipc", REPLAY_STREAM_ID, driver.aeronDirectoryName(), "localhost");
    }

    private void startRecording(final int sessionId) {
        publication = writer.addRecordedExclusivePublication(
                "aeron:ipc?term-length=64k|session-id=" + sessionId, JOURNAL_STREAM_ID);
        await("recording to start", () -> publication.isConnected() && !source.recordings().isEmpty());
    }

    @After
    public void tearDown() {
        CloseHelper.quietCloseAll(publication, source, writer, archive, driver);
        if (directory != null) {
            IoUtil.delete(directory, true);
        }
    }

    @Test
    public void catalogIncarnationSurvivesStopAndReconnect() {
        startRecording(111);
        offer(1, 101);
        final ArchiveJournalSource.Recording active = recording();
        final ArchiveJournalSource.RecordingIncarnation incarnation = active.incarnation();
        assertEquals(new ArchiveJournalSource.SourceIdentity(endpoint, CONTROL_STREAM_ID,
                JOURNAL_STREAM_ID, "aeron:ipc"), source.identity());
        assertTrue(active.isActive());
        assertNotNull(incarnation);
        assertTrue(incarnation.startTimestamp() > 0);
        assertEquals(publication.initialTermId(), incarnation.initialTermId());
        assertEquals(publication.sessionId(), incarnation.sessionId());
        assertEquals(publication.termBufferLength(), incarnation.termBufferLength());
        assertEquals(publication.maxPayloadLength() + 32, incarnation.mtuLength());
        assertEquals(JOURNAL_STREAM_ID, incarnation.streamId());

        final ArchiveJournalSource.Recording stopped = stopRecording();
        assertEquals(incarnation, stopped.incarnation());
        assertEquals(active.recordingId(), stopped.recordingId());
        final ArchiveJournalSource.SourceIdentity identity = source.identity();
        source.close();
        source = connectSource();
        assertEquals(identity, source.identity());
        assertEquals(stopped, recording());
        assertNull(new ArchiveJournalSource.Recording(0, 0, 160).incarnation());
    }

    @Test
    public void recreatedArchiveAtSameEndpointHasNewIncarnationForReusedId() {
        startRecording(211);
        offer(1, 101);
        final ArchiveJournalSource.Recording before = recording();
        final ArchiveJournalSource.SourceIdentity identity = source.identity();
        CloseHelper.closeAll(publication, source, writer, archive);
        IoUtil.delete(new File(directory, "archive"), true);

        launchArchive();
        startRecording(212);
        offer(2, 53);
        final ArchiveJournalSource.Recording after = recording();
        assertEquals(identity, source.identity());
        assertEquals(before.recordingId(), after.recordingId());
        assertEquals(before.startPosition(), after.startPosition());
        assertEquals(before.stopPosition(), after.stopPosition());
        assertNotEquals(before.incarnation(), after.incarnation());
        assertEquals(212, after.incarnation().sessionId());
    }

    @Test
    public void realReplayHonorsByteBoundaryAndFollowsActiveFrontier() {
        startRecording(311);
        // Payload 101 produces a 133-byte frame aligned to 160; payload 53 produces 96.
        final long firstEnd = offer(1, 101);
        final long frontier = offer(2, 53);
        assertEquals(160, firstEnd);
        assertEquals(256, frontier);
        final ArchiveJournalSource.Recording active = recording();
        await("archive at frontier", () -> source.recordingPosition(active.recordingId()) == frontier);

        try (Subscription replay = source.openReplay(active, firstEnd)) {
            assertEquals(List.of(2), readMessages(replay, 1));
        }
        // Aligned byte 96 is inside the first payload. It must remain a visible Aeron error,
        // not silently restart or skip a frame; this is the production cross-source failure.
        assertThrows(ArchiveException.class, () -> source.openReplay(active, 96));
        assertThrows(ArchiveException.class, () -> source.openReplay(active, frontier + 32));
        assertThrows(IllegalArgumentException.class, () -> source.openReplay(active, -32));

        try (Subscription tail = source.openReplay(active, frontier)) {
            final long newEnd = offer(3, 53);
            assertTrue(newEnd > frontier);
            assertEquals(List.of(3), readMessages(tail, 1));
        }

        final ArchiveJournalSource.Recording stopped = stopRecording();
        assertThrows(IllegalArgumentException.class, () -> source.openReplay(stopped, stopped.stopPosition()));
        assertThrows(IllegalArgumentException.class, () -> source.openReplay(stopped, stopped.stopPosition() + 32));
        try (Subscription replay = source.openReplay(stopped, firstEnd)) {
            assertEquals(List.of(2, 3), readMessages(replay, 2));
        }
    }

    @Test
    public void emptyAndMalformedStoppedRangesNeverStartZeroLengthReplay() {
        startRecording(411);
        final ArchiveJournalSource.Recording empty = stopRecording();
        assertEquals(empty.startPosition(), empty.stopPosition());
        assertThrows(IllegalArgumentException.class, () -> source.openReplay(empty));

        final ArchiveJournalSource.Recording inverted = new ArchiveJournalSource.Recording(
                empty.recordingId(), 32, 0, empty.incarnation());
        assertThrows(IllegalArgumentException.class, () -> source.openReplay(inverted));
    }

    private ArchiveJournalSource.Recording recording() {
        final List<ArchiveJournalSource.Recording> recordings = source.recordings();
        assertEquals(1, recordings.size());
        return recordings.getFirst();
    }

    private ArchiveJournalSource.Recording stopRecording() {
        final long position = publication.position();
        final long recordingId = recording().recordingId();
        await("archive to record before stop", () -> source.recordingPosition(recordingId) == position);
        publication.close();
        await("recording to stop", () -> !recording().isActive());
        return recording();
    }

    private long offer(final int value, final int length) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        buffer.putInt(0, value);
        final long[] position = {-1};
        await("publication offer", () -> (position[0] = publication.offer(buffer)) >= 0);
        return position[0];
    }

    private List<Integer> readMessages(final Subscription replay, final int count) {
        final List<Integer> messages = new ArrayList<>();
        await("replay messages", () -> {
            source.poll(replay, (buffer, offset, length, header) -> messages.add(buffer.getInt(offset)), 10);
            return messages.size() >= count;
        });
        return messages;
    }

    private static void await(final String reason, final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (!condition.getAsBoolean()) {
            assertTrue("timed out waiting for " + reason, System.nanoTime() < deadline);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}

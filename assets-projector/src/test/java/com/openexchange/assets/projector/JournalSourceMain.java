// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import com.openexchange.assets.infrastructure.journal.generated.JournalDepositEncoder;
import com.openexchange.assets.infrastructure.journal.generated.MessageHeaderEncoder;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Test child (S17 W10): a stand-in for the AE's money journal. An archiving media driver of its own (the AE side) with
 * its control endpoint on UDP {@code localhost:<port>}, recording {@code aeron:ipc} stream 4101 as the AE does
 * ({@code InfrastructureConstants.MONEY_JOURNAL_CHANNEL}), publishing dense JournalDeposit records from journalSeq 1.
 * Each stdin line {@code publish <n>} appends n more; after each batch it prints {@code [SOURCE] recorded <lastSeq>}
 * once the archive has recorded it. Args: {@code <aeronDir> <archiveDir> <controlPort> <initialCount>}.
 */
public final class JournalSourceMain {

    private static final String CHANNEL = "aeron:ipc?term-length=64k";

    public static void main(final String[] args) throws Exception {
        final String aeronDir = args[0];
        final File archiveDir = new File(args[1]);
        final String control = "aeron:udp?endpoint=localhost:" + args[2];
        final int initial = Integer.parseInt(args[3]);

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(
                new MediaDriver.Context().aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                new Archive.Context().aeronDirectoryName(aeronDir).archiveDir(archiveDir).deleteArchiveOnStart(true)
                        .threadingMode(ArchiveThreadingMode.SHARED).controlChannel(control)
                        .replicationChannel("aeron:udp?endpoint=localhost:0").recordingEventsEnabled(false)
                        .segmentFileLength(1024 * 1024));
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(aeronDir)
                     .controlRequestChannel(control).controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {
            archive.startRecording(CHANNEL, ProjectorConfig.MONEY_JOURNAL_STREAM_ID, SourceLocation.LOCAL);
            final ExclusivePublication pub = archive.context().aeron()
                    .addExclusivePublication(CHANNEL, ProjectorConfig.MONEY_JOURNAL_STREAM_ID);
            final CountersReader counters = archive.context().aeron().countersReader();
            int counterId;
            while ((counterId = RecordingPos.findCounterIdBySession(counters, pub.sessionId(),
                    archive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
                Thread.sleep(10);
            }
            System.out.println("[SOURCE] archive at " + control + ", recording "
                    + RecordingPos.getRecordingId(counters, counterId));
            long seq = 0;
            seq = publish(pub, counters, counterId, seq, initial);
            final BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith("publish ")) {
                    seq = publish(pub, counters, counterId, seq, Integer.parseInt(line.substring(8).trim()));
                }
            }
            // stdin closed: keep the source up until the test kills it
            Thread.currentThread().join();
        }
    }

    private static long publish(final ExclusivePublication pub, final CountersReader counters, final int counterId,
                                final long fromSeq, final int count) throws InterruptedException {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final JournalDepositEncoder deposit = new JournalDepositEncoder();
        long seq = fromSeq;
        for (int i = 0; i < count; i++) {
            seq++;
            deposit.wrapAndApplyHeader(buffer, 0, header).journalSeq(seq).userId(1000 + seq).assetId(1)
                    .amount(100_000_000L).balanceAfter(100_000_000L * seq).clusterTimeMs(1_700_000_000_000L + seq);
            final int length = MessageHeaderEncoder.ENCODED_LENGTH + deposit.encodedLength();
            while (pub.offer(buffer, 0, length) < 0) {
                Thread.sleep(1);
            }
        }
        while (counters.getCounterValue(counterId) < pub.position()) {
            Thread.sleep(5);
        }
        System.out.println("[SOURCE] recorded " + seq);
        return seq;
    }

    private JournalSourceMain() {
    }
}

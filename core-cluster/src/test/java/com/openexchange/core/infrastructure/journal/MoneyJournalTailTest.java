// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit driver: {@link MoneyJournalTail} answers the MAX journalSeq over every journal recording of this
 * archive (review N3: a newer, shorter recording must not hide an older high-water), reads a still ACTIVE recording to
 * its current position (C2), handles both journal frame sizes (96 and 160 aligned bytes), and answers NoJournal for an
 * archive without journal bytes.
 */
public class MoneyJournalTailTest {

    private static final String CHANNEL = "aeron:ipc?term-length=64k";
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(20);

    /** A journal-shaped message: 8-byte SBE header, journalSeq at offset 8, then {@code length - 16} filler bytes. */
    private static void write(final ExclusivePublication pub, final long seq, final int length) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[length]);
        b.putShort(0, (short)(length - 8));
        b.putLong(MoneyJournalTail.JOURNAL_SEQ_OFFSET, seq);
        for (int i = 16; i < length; i++) {
            b.putByte(i, (byte)(seq + i));
        }
        while (pub.offer(b, 0, length) < 0) {
            Thread.onSpinWait();
        }
    }

    private static void awaitRecorded(final AeronArchive archive, final ExclusivePublication pub) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        long rec;
        while ((rec = archive.findLastMatchingRecording(0, "aeron:ipc", pub.streamId(), pub.sessionId())) < 0
                || archive.getRecordingPosition(rec) < pub.position()) {
            assertTrue("recording caught up", System.nanoTime() < deadline);
            Thread.onSpinWait();
        }
    }

    private static void awaitStopped(final AeronArchive archive, final int streamId, final int sessionId) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        final long rec = archive.findLastMatchingRecording(0, "aeron:ipc", streamId, sessionId);
        while (archive.getStopPosition(rec) == AeronArchive.NULL_POSITION) {
            assertTrue("recording stopped", System.nanoTime() < deadline);
            Thread.onSpinWait();
        }
    }

    @Test
    public void maxOverRecordingsIncludingAnActiveOne() {
        try (RecordedLogFixture fx = new RecordedLogFixture()) {
            final MoneyJournalTail tail = new MoneyJournalTail(fx.aeron(), fx.archive(), SystemNanoClock.INSTANCE);
            final MoneyJournalTail.Result empty = tail.read(TIMEOUT_NS);
            assertTrue("no journal recording: " + empty, empty instanceof MoneyJournalTail.NoJournal);

            // A: seq 1..50, ends with a settle-sized message (101 B, a 160-byte frame); stopped
            final ExclusivePublication a = fx.archive().addRecordedExclusivePublication(CHANNEL,
                    InfrastructureConstants.MONEY_JOURNAL_STREAM_ID);
            for (int s = 1; s <= 50; s++) {
                write(a, s, s % 2 == 0 ? 101 : 52);
            }
            awaitRecorded(fx.archive(), a);
            final int aSession = a.sessionId();
            a.close();
            awaitStopped(fx.archive(), InfrastructureConstants.MONEY_JOURNAL_STREAM_ID, aSession);

            // B: a newer, SHORTER recording (seq 1..9), ends with a deposit-sized message (52 B, a 96-byte frame)
            final ExclusivePublication b = fx.archive().addRecordedExclusivePublication(CHANNEL,
                    InfrastructureConstants.MONEY_JOURNAL_STREAM_ID);
            for (int s = 1; s <= 9; s++) {
                write(b, s, 52);
            }
            awaitRecorded(fx.archive(), b);
            final int bSession = b.sessionId();
            b.close();
            awaitStopped(fx.archive(), InfrastructureConstants.MONEY_JOURNAL_STREAM_ID, bSession);

            final MoneyJournalTail.Result stopped = tail.read(TIMEOUT_NS);
            assertEquals("max over recordings (N3): the newest recording alone would answer 9",
                    new MoneyJournalTail.Known(50, 2, 0), stopped);

            // C: still recording (a writer not stopped yet), high-water 77
            final ExclusivePublication c = fx.archive().addRecordedExclusivePublication(CHANNEL,
                    InfrastructureConstants.MONEY_JOURNAL_STREAM_ID);
            for (int s = 70; s <= 77; s++) {
                write(c, s, s == 77 ? 101 : 52);
            }
            awaitRecorded(fx.archive(), c);
            final MoneyJournalTail.Result active = tail.read(TIMEOUT_NS);
            assertEquals("an ACTIVE recording is read to its current position (C2)", new MoneyJournalTail.Known(77, 3,
                    1), active);
            c.close();
        }
    }
}

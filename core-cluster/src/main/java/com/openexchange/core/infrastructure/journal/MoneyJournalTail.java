// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.cluster.shadow.ShadowReplay;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import org.agrona.concurrent.NanoClock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The largest {@code journalSeq} THIS node's archive holds in its money-journal recordings, the
 * {@code localJournalDurableSeq} of the skip-safe rule (8.1). Bounded and read only: it lists every recording on
 * {@link InfrastructureConstants#MONEY_JOURNAL_STREAM_ID} over IPC, reads the LAST message of each with a positive
 * length through a session-scoped replay (the replay discipline, join and end positions checked) and answers the
 * {@code max} over them (review N3: a process that crashed during an earlier replay may have left its newest recording
 * short of an older recording's high-water, so the newest alone would under-read and refuse a legal SKIP). A
 * recording still ACTIVE (no stop position yet: a writer not yet stopped, or this process's own writer) is read to
 * its current recorded position and counted. Never writes, never purges.
 *
 * <p>Finding the last message without reading the whole recording: a journal message is one small SBE message
 * (header + one block, at most 128 bytes, {@code MoneyJournal}'s scratch buffer) under the MTU, so it is ONE
 * unfragmented frame of at most {@value #MAX_FRAME_LENGTH} aligned bytes, and {@code journalSeq} sits at
 * {@link #JOURNAL_SEQ_OFFSET} of every message ({@code MoneyJournalWriterAgent}). The reader asks the archive for a
 * replay of the last {@code w} bytes for {@code w = 32, 64, ..., MAX_FRAME_LENGTH}: the archive refuses a start that is
 * not a frame header ({@code INVALID_POSITION}), and an accepted window must deliver exactly one whole message whose
 * aligned frame length is {@code w}. If no window fits (the recording ends in padding, a crash between the pad and the
 * next message), the reader replays the term that holds the end, which starts on a frame boundary by construction, and
 * keeps its last message. A term with no message is {@code Unknown}: SKIP is then not taken (the safe direction).</p>
 *
 * <p>Answers: {@link Known} (seq, recordings read, active ones), {@link NoJournal} (no recording, or none with bytes:
 * skip-safe only when the shadow says journaling is off), {@link Unknown} (an archive or replay failure, or a deadline:
 * SKIP is not taken, counted by the caller). One owner thread per call; the replays are serial.</p>
 */
public final class MoneyJournalTail {

    /** Byte offset of {@code journalSeq} inside every journal message (SBE header 8 + the first field). */
    public static final int JOURNAL_SEQ_OFFSET = 8;
    /**
     * One journal message: header + JournalSettle (93) = 101 bytes, plus the 32-byte data header, aligned. A format
     * constant coupled to the largest journal message: if a future message is larger, no window up to this length fits
     * and the read falls through to the whole-last-term replay below (still correct, slower), so raise it with the
     * codec.
     */
    public static final int MAX_FRAME_LENGTH = 160;

    /** What a read found. */
    public sealed interface Result permits Known, NoJournal, Unknown {
    }

    /** The largest journalSeq over {@code recordings} recordings ({@code active} of them still recording). */
    public record Known(long seq, int recordings, int active) implements Result {
    }

    /** No journal recording with any bytes. */
    public record NoJournal(int recordings) implements Result {
    }

    /** Not readable now; the caller treats every SKIP as not skip-safe. */
    public record Unknown(String reason) implements Result {
    }

    private record Rec(long id, long start, long stop, int termLength) {
    }

    private final Aeron aeron;
    private final AeronArchive archive;
    private final NanoClock clock;
    private final String channelFragment;
    private final int streamId;

    public MoneyJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock) {
        this(aeron, archive, clock, "aeron:ipc", InfrastructureConstants.MONEY_JOURNAL_STREAM_ID);
    }

    MoneyJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                     final String channelFragment, final int streamId) {
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.archive = Objects.requireNonNull(archive, "archive");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.channelFragment = Objects.requireNonNull(channelFragment, "channelFragment");
        this.streamId = streamId;
    }

    /** @param timeoutNs the whole read's budget ({@code SHADOW_LOAD_TIMEOUT_MS}) */
    public Result read(final long timeoutNs) {
        final long deadlineNs = clock.nanoTime() + timeoutNs;
        final List<Rec> recs = new ArrayList<>();
        try {
            long from = 0;
            while (true) {
                final int before = recs.size();
                final int n = archive.listRecordingsForUri(from, 100, channelFragment, streamId,
                        (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                         stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId,
                         stream, strippedChannel, originalChannel, sourceIdentity) -> recs.add(
                                 new Rec(recordingId, startPosition, stopPosition, termBufferLength)));
                if (n == 0 || recs.size() == before) {
                    break;
                }
                from = recs.get(recs.size() - 1).id() + 1;
            }
        } catch (final RuntimeException listFailed) {
            return new Unknown("listing journal recordings failed: " + listFailed);
        }
        long max = -1;
        int read = 0;
        int active = 0;
        for (final Rec r : recs) {
            long stop = r.stop();
            if (stop == AeronArchive.NULL_POSITION) {
                try {
                    stop = archive.getRecordingPosition(r.id());
                } catch (final RuntimeException positionFailed) {
                    return new Unknown("recording position of active journal recording " + r.id() + ": "
                            + positionFailed);
                }
                if (stop == AeronArchive.NULL_POSITION) {
                    // stopped between the listing and now: read its descriptor stop position again
                    try {
                        stop = archive.getStopPosition(r.id());
                    } catch (final RuntimeException stopFailed) {
                        return new Unknown("stop position of journal recording " + r.id() + ": " + stopFailed);
                    }
                } else {
                    active++;
                }
            }
            if (stop <= r.start()) {
                continue;
            }
            final long seq;
            try {
                seq = lastSeq(r, stop, deadlineNs);
            } catch (final RuntimeException readFailed) {
                return new Unknown("last message of journal recording " + r.id() + " [" + r.start() + "," + stop
                        + "): " + readFailed);
            }
            if (seq == Long.MIN_VALUE) {
                return new Unknown("no journal message in the last term of recording " + r.id() + " [" + r.start()
                        + "," + stop + ")");
            }
            read++;
            max = Math.max(max, seq);
        }
        if (read == 0) {
            return new NoJournal(recs.size());
        }
        return new Known(max, read, active);
    }

    /** The journalSeq of the last message of {@code [r.start, stop)}, or {@code Long.MIN_VALUE} when there is none. */
    private long lastSeq(final Rec r, final long stop, final long deadlineNs) {
        final ShadowReplay replay = new ShadowReplay(aeron, archive, clock);
        for (int w = FrameDescriptor.FRAME_ALIGNMENT; w <= MAX_FRAME_LENGTH; w += FrameDescriptor.FRAME_ALIGNMENT) {
            final long from = stop - w;
            if (from < r.start()) {
                break;
            }
            final long[] seq = {Long.MIN_VALUE};
            final int[] messages = {0};
            final int[] frameLength = {0};
            try {
                replay.readRange(r.id(), from, w, (buffer, offset, length, header) -> {
                    messages[0]++;
                    frameLength[0] = header.frameLength();
                    if (length >= JOURNAL_SEQ_OFFSET + Long.BYTES) {
                        seq[0] = buffer.getLong(offset + JOURNAL_SEQ_OFFSET);
                    }
                }, deadlineNs);
            } catch (final ArchiveException notAFrame) {
                if (notAFrame.errorCode() == ArchiveException.INVALID_POSITION) {
                    continue; // not a frame header: a longer window
                }
                throw notAFrame;
            }
            if (messages[0] == 1 && BitUtil.align(frameLength[0], FrameDescriptor.FRAME_ALIGNMENT) == w
                    && seq[0] != Long.MIN_VALUE) {
                return seq[0];
            }
        }
        // the end is not one message frame (padding at a term end): read the term holding the end, frame aligned
        final long termStart = Math.max(r.start(), (stop - 1) & ~((long)r.termLength() - 1));
        final long[] seq = {Long.MIN_VALUE};
        replay.readRange(r.id(), termStart, stop - termStart, (buffer, offset, length, header) -> {
            if (length >= JOURNAL_SEQ_OFFSET + Long.BYTES) {
                seq[0] = buffer.getLong(offset + JOURNAL_SEQ_OFFSET);
            }
        }, deadlineNs);
        return seq[0];
    }
}

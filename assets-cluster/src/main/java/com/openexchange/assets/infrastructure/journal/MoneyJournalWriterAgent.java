// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.journal;

import com.openexchange.assets.infrastructure.Logger;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;

import java.util.function.Supplier;

/**
 * Drains the {@link MoneyJournal} ring into a RECORDED exclusive publication on the node's OWN
 * archive: the money journal lands as a separate recording next to the consensus log, in the same
 * durable archive dir.
 *
 * Runs on its own AgentRunner thread (backoff idle): the cluster service thread never touches
 * Aeron or the archive for journaling, it only writes the in-memory ring.
 *
 * Failure policy: never drop. If the archive connection or the publication is unavailable the
 * agent retries with backoff while the ring absorbs the burst; if the ring also fills,
 * {@link MoneyJournal} blocks the service thread (loudly) so the node degrades to a lagging
 * replica rather than losing a movement.
 *
 * Also the node's journal metrics point (there is no other AE-node metrics surface yet): volatile
 * getters for scrapers plus a periodic MONEY JOURNAL STATS log line carrying the journalSeq
 * high-water and recorded bytes.
 */
public final class MoneyJournalWriterAgent implements Agent {

    private static final Logger log = Logger.getLogger(MoneyJournalWriterAgent.class);

    /** Byte offset of journalSeq inside every journal message: SBE header(8) + first field. */
    private static final int JOURNAL_SEQ_OFFSET = 8;
    private static final int DRAIN_LIMIT = 64;
    private static final long CONNECT_RETRY_BACKOFF_MS = 1_000;
    private static final long STATS_INTERVAL_MS = 10_000;

    private final MoneyJournal journal;
    private final OneToOneRingBuffer ring;
    private final Supplier<AeronArchive.Context> archiveClientContext;
    private final String channel;
    private final int streamId;
    private final IdleStrategy offerIdle = new BackoffIdleStrategy();

    private AeronArchive archive;
    private ExclusivePublication publication;
    private long lastConnectAttemptMs;
    private long lastStatsMs;
    private long lastStatsEntries = -1;

    private volatile long writtenEntries;
    private volatile long lastJournalSeq;
    private volatile long journalPosition;
    private volatile long connectFailures;

    public MoneyJournalWriterAgent(
            final MoneyJournal journal,
            final Supplier<AeronArchive.Context> archiveClientContext,
            final String channel,
            final int streamId) {
        this.journal = journal;
        this.ring = journal.ringBuffer();
        this.archiveClientContext = archiveClientContext;
        this.channel = channel;
        this.streamId = streamId;
    }

    @Override
    public int doWork() {
        if (publication == null) {
            return tryConnect() ? 1 : 0;
        }
        final int drained = ring.read(this::onRingMessage, DRAIN_LIMIT);
        maybeLogStats();
        return drained;
    }

    /**
     * Connect lazily with a retry cadence so a slow-starting archive never blocks node boot; the
     * ring buffers entries in the meantime. Recording starts the moment this succeeds.
     */
    private boolean tryConnect() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastConnectAttemptMs < CONNECT_RETRY_BACKOFF_MS) {
            return false;
        }
        lastConnectAttemptMs = nowMs;
        try {
            archive = AeronArchive.connect(archiveClientContext.get());
            publication = archive.addRecordedExclusivePublication(channel, streamId);
            log.info("Money journal writer connected: channel=%s stream=%d session=%d",
                    channel, streamId, publication.sessionId());
            return true;
        } catch (Exception e) {
            connectFailures = connectFailures + 1;
            CloseHelper.quietClose(archive);
            archive = null;
            publication = null;
            log.error("Money journal archive connect failed (attempt %d): %s",
                    connectFailures, e.getMessage());
            return false;
        }
    }

    private void onRingMessage(
            final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
        // Blocking offer: the ring handler owns backpressure. A CLOSED/MAX_POSITION result is
        // unrecoverable for this publication; surface it hard (the errorHandler restarts us).
        long result;
        while ((result = publication.offer(buffer, index, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("money journal publication unusable: " + result);
            }
            offerIdle.idle();
        }
        offerIdle.reset();

        lastJournalSeq = buffer.getLong(index + JOURNAL_SEQ_OFFSET);
        writtenEntries = writtenEntries + 1;
        journalPosition = publication.position();
    }

    /** Periodic, change-gated stats line: the journal's high-water + bytes for operators/scrapers. */
    private void maybeLogStats() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastStatsMs < STATS_INTERVAL_MS) {
            return;
        }
        lastStatsMs = nowMs;
        final long entries = writtenEntries;
        if (entries != lastStatsEntries) {
            lastStatsEntries = entries;
            log.info("MONEY JOURNAL STATS: journalSeq=%d entries=%d bytes=%d backpressureEvents=%d",
                    lastJournalSeq, entries, journalPosition, journal.backpressureEvents());
        }
    }

    /** Entries written to the recorded publication since boot. */
    public long writtenEntries() {
        return writtenEntries;
    }

    /** journalSeq high-water written to the recorded publication. */
    public long lastJournalSeq() {
        return lastJournalSeq;
    }

    /** Recorded publication position = journal bytes written (this session's recording). */
    public long journalPosition() {
        return journalPosition;
    }

    @Override
    public void onClose() {
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(archive);
    }

    @Override
    public String roleName() {
        return "money-journal-writer";
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource;
import com.openexchange.assets.infrastructure.journal.generated.BoolFlag;
import com.openexchange.assets.infrastructure.journal.generated.JournalDepositDecoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalOpeningBalanceDecoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalSettleDecoder;
import com.openexchange.assets.infrastructure.journal.generated.JournalWithdrawDecoder;
import com.openexchange.assets.infrastructure.journal.generated.MessageHeaderDecoder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The money projector's engine: AE money journal -> filter -> Postgres.
 *
 * Stateless with respect to journal position. Every epoch reads the Postgres high-water
 * ({@code MAX(journal_seq)}, 0 on an empty ledger), builds a fresh {@link ProjectorFilter}, then
 * replays every money-journal recording from the earliest retained byte forward: the filter skips
 * what Postgres already has (dense journalSeq dedupe), and a dense journalSeq gap HALTS the
 * projector (correctness over liveness: a gap is a lost money movement). Postgres is authoritative;
 * a PG failure never skips, it rolls back the batch, tears down the epoch and retries from the
 * unchanged high-water.
 *
 * Source death is detected the same three ways as the settlement bridge, because a dead source
 * otherwise looks exactly like a quiet market: (1) a replay image that existed and vanished; (2) a
 * replay that never connects within {@link #CONNECT_TIMEOUT_MS}; (3) a periodic archive probe (the
 * recording stopped, or grew while the replay sat still, or the probe throws because the archive
 * process is gone). All three end the epoch; the next epoch re-lists the chain and re-attaches.
 */
public final class ProjectorAgent implements Runnable {

    private static final int POLL_LIMIT = 32;
    private static final long ERROR_BACKOFF_MS = 1_000;
    private static final long STATUS_LOG_INTERVAL_MS = 10_000;
    /** Cadence of the live-follow source-progress probe (also an archive liveness check). */
    private static final long SOURCE_CHECK_INTERVAL_MS = 10_000;
    /** A replay whose image never arrives is a dead source, not a slow start, past this. */
    private static final long CONNECT_TIMEOUT_MS = 15_000;
    /** Sentinel: no previous source-progress check this replay (never stall on the first probe). */
    private static final long NO_PRIOR_CHECK = Long.MIN_VALUE;
    /** Flush the pending batch once it reaches this many records mid-burst (bounds memory + latency). */
    private static final int FLUSH_MAX_BATCH = 256;
    private static final int SCHEMA_ID = JournalDepositDecoder.SCHEMA_ID;

    private final ProjectorConfig config;
    private final String aeronDirectoryName;
    private final ProjectorState state;
    private final PostgresMoneyJournalStore store;

    private final MessageHeaderDecoder journalHeader = new MessageHeaderDecoder();
    private final JournalOpeningBalanceDecoder openingDecoder = new JournalOpeningBalanceDecoder();
    private final JournalDepositDecoder depositDecoder = new JournalDepositDecoder();
    private final JournalWithdrawDecoder withdrawDecoder = new JournalWithdrawDecoder();
    private final JournalSettleDecoder settleDecoder = new JournalSettleDecoder();
    private final IdleStrategy idle = new BackoffIdleStrategy();

    /** Records projected this epoch but not yet committed (journalSeq order preserved). */
    private final List<MoneyJournalRecord> pending = new ArrayList<>();

    private volatile boolean running = true;
    private ProjectorFilter filter;
    private long lastStatusLogMs;

    public ProjectorAgent(final ProjectorConfig config, final String aeronDirectoryName,
                          final ProjectorState state, final PostgresMoneyJournalStore store) {
        this.config = config;
        this.aeronDirectoryName = aeronDirectoryName;
        this.state = state;
        this.store = store;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        System.out.println("[PROJECTOR] starting: aeJournalArchives=" + config.journalArchiveEndpoints
                + " pg=" + config.postgresUrl + " haltOnGap=" + config.haltOnGap);
        while (running) {
            if (state.halted) {
                // Parked after a gap: keep the process (and its metrics) alive; a human decides.
                sleep(ERROR_BACKOFF_MS);
                continue;
            }
            try {
                runEpoch();
            } catch (Exception e) {
                state.errors++;
                state.connectedToPg = false;
                System.err.println("[PROJECTOR] epoch failed (" + e.getClass().getSimpleName() + ": "
                        + e.getMessage() + ") - resyncing" + (running ? "" : " (stopping)"));
                sleep(ERROR_BACKOFF_MS);
            }
        }
        System.out.println("[PROJECTOR] stopped. " + state.render());
    }

    private void runEpoch() throws SQLException {
        final long hwm = store.highWater();
        state.connectedToPg = true;
        filter = new ProjectorFilter(hwm, config.haltOnGap);
        pending.clear();
        state.epochHighWater = hwm;
        state.sourceBacklogBytes = 0;
        state.epochs++;
        System.out.println("[PROJECTOR] epoch " + state.epochs + ": Postgres high-water journalSeq=" + hwm);

        try (ArchiveJournalSource source = ArchiveJournalSource.connectFirstHealthy(
                config.journalArchiveEndpoints,
                config.archiveControlStreamId,
                ProjectorConfig.MONEY_JOURNAL_STREAM_ID,
                ProjectorConfig.MONEY_JOURNAL_CHANNEL,
                ProjectorConfig.REPLAY_STREAM_ID,
                aeronDirectoryName)) {
            state.journalSource = source.endpoint();
            followChain(source);
        }
    }

    private void followChain(final ArchiveJournalSource source) {
        final List<ArchiveJournalSource.Recording> chain = source.recordings();
        if (chain.isEmpty()) {
            // Journal enabled but nothing recorded yet (or dark): wait and re-list next epoch.
            System.out.println("[PROJECTOR] no journal recordings at " + source.endpoint() + " yet - waiting");
            sleep(ERROR_BACKOFF_MS);
            return;
        }
        final FragmentHandler handler = (buffer, offset, length, header) -> onJournalEntry(buffer, offset);

        for (final ArchiveJournalSource.Recording recording : chain) {
            System.out.println("[PROJECTOR] following recording " + recording.recordingId()
                    + (recording.isActive() ? " (ACTIVE, live-follow)" : " (stopped)"));
            try (Subscription replay = source.openReplay(recording)) {
                final boolean liveFollow = recording.isActive();
                final long connectDeadlineMs = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
                long nextSourceCheckMs = System.currentTimeMillis() + SOURCE_CHECK_INTERVAL_MS;
                long consumedAtLastCheck = NO_PRIOR_CHECK;
                boolean imageWasLive = false;
                boolean followedRecordingEnded = false;
                while (running && !state.halted) {
                    final int fragments = source.poll(replay, handler, POLL_LIMIT);
                    maybeLogStatus();
                    imageWasLive |= replay.imageCount() > 0;
                    if (fragments == 0) {
                        // Caught up for now: durably commit whatever this batch projected.
                        flushPending();
                        if (!liveFollow && replayDrained(replay, recording)) {
                            break; // stopped recording fully consumed -> next in chain
                        }
                        if (replay.isClosed() || (imageWasLive && replay.imageCount() == 0)) {
                            // The image we HAD is gone (source node died / purge race):
                            // epoch restart re-lists the chain and re-attaches.
                            sourceStall("journal replay image lost on recording "
                                    + recording.recordingId());
                        }
                        if (!imageWasLive && System.currentTimeMillis() > connectDeadlineMs) {
                            // Replay accepted but no image ever arrived: dead source, not slow start.
                            sourceStall("journal replay never connected on recording "
                                    + recording.recordingId() + " within " + CONNECT_TIMEOUT_MS + "ms");
                        }
                        if (liveFollow && System.currentTimeMillis() >= nextSourceCheckMs) {
                            nextSourceCheckMs = System.currentTimeMillis() + SOURCE_CHECK_INTERVAL_MS;
                            final long consumed = replay.imageCount() > 0
                                    ? replay.imageAtIndex(0).position() : -1;
                            // Throws when the archive itself is gone -> epoch restart. This is
                            // what un-wedges a live-follow whose source process was killed.
                            final long recPos = source.recordingPosition(recording.recordingId());
                            if (recPos == AeronArchive.NULL_POSITION) {
                                // Recording stopped under our live-follow (source restarted; its
                                // successor recording isn't in this epoch's chain listing).
                                final long stop = source.stopPosition(recording.recordingId());
                                if (consumed >= stop) {
                                    System.out.println("[PROJECTOR] followed recording "
                                            + recording.recordingId() + " stopped and is fully "
                                            + "consumed - ending epoch to re-list the chain");
                                    followedRecordingEnded = true;
                                    break;
                                }
                                if (consumed == consumedAtLastCheck && consumedAtLastCheck != NO_PRIOR_CHECK) {
                                    sourceStall("recording " + recording.recordingId()
                                            + " stopped but replay is stuck at " + consumed
                                            + " short of stop position " + stop);
                                }
                            } else {
                                state.sourceBacklogBytes = Math.max(0, recPos - Math.max(consumed, 0));
                                if (recPos > consumed && consumed == consumedAtLastCheck
                                        && consumedAtLastCheck != NO_PRIOR_CHECK) {
                                    sourceStall("replay stalled on recording " + recording.recordingId()
                                            + ": recorded position " + recPos
                                            + " but replay stuck at " + consumed);
                                }
                            }
                            consumedAtLastCheck = consumed;
                        }
                        idle.idle();
                    } else {
                        if (pending.size() >= FLUSH_MAX_BATCH) {
                            flushPending();
                        }
                        idle.reset();
                    }
                }
                if (followedRecordingEnded) {
                    break; // end the epoch: the next one re-lists and follows the successor
                }
            }
            if (!running || state.halted) {
                return;
            }
        }
        // Chain exhausted without a live-follow in progress (source down or mid-restart):
        // pause before the next epoch so we don't hot-loop re-reading the whole chain.
        sleep(ERROR_BACKOFF_MS);
    }

    private void sourceStall(final String reason) {
        state.sourceStalls++;
        throw new IllegalStateException(reason);
    }

    private boolean replayDrained(final Subscription replay, final ArchiveJournalSource.Recording recording) {
        if (replay.imageCount() == 0) {
            return replay.isClosed(); // bounded replay closes its image at the bound
        }
        return replay.imageAtIndex(0).position()
                >= recording.stopPosition() || replay.imageAtIndex(0).isEndOfStream();
    }

    /**
     * Decode one journal fragment and hand its dense journalSeq to the filter. Only build the
     * (allocating) {@link MoneyJournalRecord} on PROJECT, so replay-overlap SKIPs stay cheap.
     */
    private void onJournalEntry(final DirectBuffer buffer, final int offset) {
        journalHeader.wrap(buffer, offset);
        if (journalHeader.schemaId() != SCHEMA_ID) {
            return; // foreign schema on the journal stream - ignore
        }
        switch (journalHeader.templateId()) {
            case JournalOpeningBalanceDecoder.TEMPLATE_ID -> {
                openingDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                final long seq = openingDecoder.journalSeq();
                switch (filter.onRecord(seq)) {
                    case PROJECT -> pending.add(MoneyJournalRecord.opening(seq,
                            openingDecoder.userId(), openingDecoder.assetId(), openingDecoder.amount()));
                    case SKIP -> state.skippedRecords++;
                    case HALT -> haltOnGap("opening", seq);
                }
            }
            case JournalDepositDecoder.TEMPLATE_ID -> {
                depositDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                final long seq = depositDecoder.journalSeq();
                switch (filter.onRecord(seq)) {
                    case PROJECT -> pending.add(MoneyJournalRecord.deposit(seq,
                            depositDecoder.userId(), depositDecoder.assetId(), depositDecoder.amount(),
                            depositDecoder.balanceAfter(), depositDecoder.clusterTimeMs()));
                    case SKIP -> state.skippedRecords++;
                    case HALT -> haltOnGap("deposit", seq);
                }
            }
            case JournalWithdrawDecoder.TEMPLATE_ID -> {
                withdrawDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                final long seq = withdrawDecoder.journalSeq();
                switch (filter.onRecord(seq)) {
                    case PROJECT -> pending.add(MoneyJournalRecord.withdraw(seq,
                            withdrawDecoder.userId(), withdrawDecoder.assetId(), withdrawDecoder.amount(),
                            withdrawDecoder.balanceAfter(), withdrawDecoder.clusterTimeMs()));
                    case SKIP -> state.skippedRecords++;
                    case HALT -> haltOnGap("withdraw", seq);
                }
            }
            case JournalSettleDecoder.TEMPLATE_ID -> {
                settleDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                final long seq = settleDecoder.journalSeq();
                switch (filter.onRecord(seq)) {
                    case PROJECT -> pending.add(MoneyJournalRecord.settle(seq,
                            settleDecoder.tradeId(), settleDecoder.marketId(), settleDecoder.price(),
                            settleDecoder.quantity(), settleDecoder.buyerUserId(), settleDecoder.sellerUserId(),
                            settleDecoder.takerIsBuy() == BoolFlag.TRUE,
                            settleDecoder.buyerBaseAfter(), settleDecoder.buyerQuoteAfter(),
                            settleDecoder.sellerBaseAfter(), settleDecoder.sellerQuoteAfter(),
                            settleDecoder.clusterTimeMs()));
                    case SKIP -> state.skippedRecords++;
                    case HALT -> haltOnGap("settle", seq);
                }
            }
            default -> { /* unknown template on schema 3 - ignore */ }
        }
        // Surface the gap counter live in both halt and haltOnGap=false override modes.
        state.gapsDetected = filter.gapsDetected();
    }

    /**
     * Commit the pending batch. On success advance the in-memory progress. On failure count it, drop
     * the PG-connected flag and THROW: the batch rolled back, the epoch tears down and retries from
     * the unchanged Postgres high-water (never skip). Preserves journalSeq order: the batch was built
     * in order and committed atomically, so the high-water only ever advances over committed records.
     */
    private void flushPending() {
        if (pending.isEmpty()) {
            return;
        }
        try {
            store.project(pending);
            state.connectedToPg = true;
            state.rowsProjected += pending.size();
            state.lastJournalSeq = pending.get(pending.size() - 1).journalSeq();
            pending.clear();
        } catch (SQLException e) {
            state.pgFlushFailures++;
            state.connectedToPg = false;
            throw new IllegalStateException("postgres flush failed (batch kept for retry): "
                    + e.getMessage(), e);
        }
    }

    private void haltOnGap(final String kind, final long seq) {
        // Persist everything strictly below the gap before parking (throws on PG failure -> the epoch
        // retries and re-detects the gap; nothing below it is lost).
        flushPending();
        state.gapsDetected = filter.gapsDetected();
        state.halted = true;
        System.err.println("[PROJECTOR] *** HALTED - LOST MONEY MOVEMENT SUSPECTED *** dense journalSeq gap: "
                + kind + " journalSeq=" + seq + " after high-water=" + filter.highWater()
                + " | a journal gap means a money movement Postgres will never see; NOT projecting past it. "
                + "Investigate the AE journal archives and the money_journal table, then restart the projector. "
                + state.render());
    }

    private void maybeLogStatus() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastStatusLogMs >= STATUS_LOG_INTERVAL_MS) {
            lastStatusLogMs = nowMs;
            System.out.println("[PROJECTOR] " + state.render());
        }
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

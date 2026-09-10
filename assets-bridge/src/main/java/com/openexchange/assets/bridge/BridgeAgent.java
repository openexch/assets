// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.match.infrastructure.journal.generated.JournalTerminalDecoder;
import com.match.infrastructure.journal.generated.JournalTradeDecoder;
import com.match.infrastructure.journal.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.List;

/**
 * The settlement bridge's engine: ME journal -> filter -> translate -> AE ingress.
 *
 * Stateless by design. Every epoch starts from scratch: connect the AE session, ask it how
 * far it has consumed (FeedPositionReport -> W/T), build a fresh {@link BridgeFilter}, then
 * read the journal chain from the earliest unproven byte forward — the filter skips what
 * the AE already has, the AE's idempotency absorbs the inclusive boundary, and a dense
 * tradeId gap HALTS the bridge (correctness over liveness: a gap is a lost settlement).
 *
 * Every failure (AE session death, journal source death, purge race) tears down the epoch
 * and starts a new one. A HALT parks the bridge with metrics alive until an operator acts.
 *
 * Source death is detected three ways, because a dead source otherwise looks exactly like a
 * quiet market: (1) a replay image that existed and vanished; (2) a replay that never
 * connects within {@link #CONNECT_TIMEOUT_MS}; (3) a periodic archive probe — the recording
 * stopped, or grew while the replay sat still, or the probe itself throws because the
 * archive process is gone. All three end the epoch; the next epoch re-lists the recording
 * chain and re-attaches (the 2026-07-10 incident: a node SIGKILL stopped the followed
 * recording and the bridge idled at EOF for two hours reporting healthy).
 */
public final class BridgeAgent implements Runnable {

    private static final int POLL_LIMIT = 32;
    private static final long ERROR_BACKOFF_MS = 1_000;
    private static final long STATUS_LOG_INTERVAL_MS = 10_000;
    /** Cadence of the live-follow source-progress probe (also an archive liveness check). */
    private static final long SOURCE_CHECK_INTERVAL_MS = 10_000;
    /** A replay whose image never arrives is a dead source, not a slow start, past this. */
    private static final long CONNECT_TIMEOUT_MS = 15_000;
    /** Sentinel: no previous source-progress check this replay (never stall on the first probe). */
    private static final long NO_PRIOR_CHECK = Long.MIN_VALUE;
    /** Bound on the settle-ack in-flight map: bounded memory beats perfect ack coverage. */
    private static final int IN_FLIGHT_SETTLE_CAP = 1_000_000;
    /** Missing-value sentinel for the in-flight map (a nanoTime can never plausibly be this). */
    private static final long NO_OFFER_TIME = Long.MIN_VALUE;

    private final BridgeConfig config;
    private final String aeronDirectoryName;
    private final BridgeState state;
    private final JournalToMoneyTranslator translator = new JournalToMoneyTranslator();
    private final MessageHeaderDecoder journalHeader = new MessageHeaderDecoder();
    private final JournalTradeDecoder tradeDecoder = new JournalTradeDecoder();
    private final JournalTerminalDecoder terminalDecoder = new JournalTerminalDecoder();
    /** Sized for one full SettleBatch frame (~4.4 KB at the 64-trade cap), not just a single. */
    private final UnsafeBuffer outBuffer = new UnsafeBuffer(new byte[8192]);
    private final IdleStrategy idle = new BackoffIdleStrategy();
    // Per-staged-trade metadata, parallel to the translator's SettleBatch staging: the metrics
    // (forward latency, ack in-flight arming, last-forwarded gauges) are recorded per trade at FLUSH
    // time, when the batch's offer has actually returned — not at stage time, when nothing has left.
    private final long[] stagedTradeIds = new long[JournalToMoneyTranslator.TRADE_BATCH_CAP];
    private final long[] stagedEgressSeqs = new long[JournalToMoneyTranslator.TRADE_BATCH_CAP];
    private final long[] stagedTimestamps = new long[JournalToMoneyTranslator.TRADE_BATCH_CAP];
    /**
     * Settle offers awaiting their SettlementApplied egress ack: tradeId -> offer-return
     * nanoTime. Agent-thread-only (egress is polled on this same thread), bounded at
     * {@link #IN_FLIGHT_SETTLE_CAP}. Purely observability — never gates forwarding.
     */
    private final Long2LongHashMap inFlightSettles = new Long2LongHashMap(NO_OFFER_TIME);

    private volatile boolean running = true;
    private long lastStatusLogMs;
    private ArchiveJournalSource.Recording currentRecording;

    /** How far up the chain an epoch may skip after an earlier one drained the head. */
    private final ChainResumeMemo resumeMemo = new ChainResumeMemo();
    /** Forward count at epoch start: an advance seals the memo at what it has proven. */
    private long forwardsAtEpochStart;

    public BridgeAgent(final BridgeConfig config, final String aeronDirectoryName, final BridgeState state) {
        this.config = config;
        this.aeronDirectoryName = aeronDirectoryName;
        this.state = state;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        System.out.println("[BRIDGE] starting: journalArchives=" + config.journalArchiveEndpoints
                + " ae=" + config.aeClusterAddresses + ":" + config.aePortBase
                + " haltOnGap=" + config.haltOnGap);
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
                state.connectedToAe = false;
                System.err.println("[BRIDGE] epoch failed (" + e.getClass().getSimpleName() + ": "
                        + e.getMessage() + ") — resyncing" + (running ? "" : " (stopping)"));
                sleep(ERROR_BACKOFF_MS);
            }
        }
        System.out.println("[BRIDGE] stopped. " + state.render());
    }

    private void runEpoch() {
        // Acks for settles offered on a previous epoch's session can never arrive (the AE
        // broadcasts SettlementApplied at apply time, only to sessions connected then) —
        // drop the stale in-flight entries rather than let them accumulate to the cap.
        inFlightSettles.clear();
        // Trades a dead epoch staged but never flushed are NOT carried over: the stateless resume
        // re-reads them from the journal and the AE's idempotency absorbs any overlap.
        translator.resetStagedTrades();
        try (AeFeedClient ae = AeFeedClient.connect(config, aeronDirectoryName)) {
            ae.onSettlementApplied(this::onSettlementAck);
            state.connectedToAe = true;
            final AeFeedClient.FeedPosition pos = ae.queryFeedPosition(config.queryTimeoutMs);
            final BridgeFilter filter = new BridgeFilter(
                    pos.consumePosition(), pos.lastAppliedTradeId(), config.haltOnGap);
            state.epochConsumePosition = pos.consumePosition();
            state.epochLastAppliedTradeId = pos.lastAppliedTradeId();
            state.sourceBacklogBytes = 0;
            state.replayConsumedPosition = AeronArchive.NULL_POSITION;
            state.replayRecordingPosition = AeronArchive.NULL_POSITION;
            state.epochs++;
            forwardsAtEpochStart = forwardCount();
            System.out.println("[BRIDGE] epoch " + state.epochs + ": AE at consumePosition="
                    + pos.consumePosition() + " lastAppliedTradeId=" + pos.lastAppliedTradeId());

            try (ArchiveJournalSource source = JournalSource.connectFirstHealthy(config, aeronDirectoryName)) {
                state.journalSource = source.endpoint();
                followChain(source, filter, ae, pos);
            }
        } finally {
            state.connectedToAe = false;
        }
    }

    /** A malformed stopped extent is an error, never an empty recording. */
    static boolean isEmptyStopped(final ArchiveJournalSource.Recording recording) {
        return recording.startPosition() >= 0 && !recording.isActive()
                && recording.stopPosition() == recording.startPosition();
    }

    private void followChain(final ArchiveJournalSource source, final BridgeFilter filter,
                            final AeFeedClient ae, final AeFeedClient.FeedPosition sync) {
        final List<ArchiveJournalSource.Recording> chain = source.recordings();
        final int startIndex = resumeMemo.resumeIndex(
                source.identity(), chain, sync.consumePosition(), sync.lastAppliedTradeId());
        if (resumeMemo.invalidationReason() != null) {
            System.out.println("[BRIDGE] replay memo invalidated: " + resumeMemo.invalidationReason());
        }
        if (chain.isEmpty()) {
            // Journal enabled but nothing recorded yet (or dark): wait and re-list next epoch.
            System.out.println("[BRIDGE] no journal recordings at " + source.endpoint() + " yet — waiting");
            sleep(ERROR_BACKOFF_MS);
            return;
        }
        if (startIndex > 0) {
            System.out.println("[BRIDGE] skipping proven recording prefix: source=" + source.identity()
                    + " count=" + startIndex + " chainSize=" + chain.size());
        }
        final FragmentHandler handler = (buffer, offset, length, header) ->
                onJournalEntry(buffer, offset, filter, ae, header.position());

        for (int i = startIndex; i < chain.size(); i++) {
            final ArchiveJournalSource.Recording recording = chain.get(i);
            final long replayFrom = resumeMemo.replayStartPosition(recording);
            if (isEmptyStopped(recording)
                    || (!recording.isActive() && replayFrom == recording.stopPosition())) {
                // Empty recordings belong to the drained prefix. Proven stopped EOF needs no
                // replay: Aeron rejects a zero-length bounded replay. Active frontier still tails.
                resumeMemo.noteDrained(chain, i, forwardCount() != forwardsAtEpochStart);
                ae.duty();
                continue;
            }
            boolean fullyDrained = false;
            currentRecording = recording;
            resumeMemo.beginRecording();
            System.out.println("[BRIDGE] following recording " + recording.recordingId()
                    + " source=" + source.identity() + " incarnation=" + recording.incarnation()
                    + (recording.isActive() ? " (ACTIVE, live-follow)" : " (stopped)")
                    + (replayFrom > recording.startPosition()
                        ? " resuming at position " + replayFrom + " (drained head skipped)" : ""));
            try (Subscription replay = openReplay(source, recording, replayFrom)) {
                final boolean liveFollow = recording.isActive();
                // Replay-progress gauges: a stopped recording's recorded position is its stop
                // position (bounded cold catch-up lag = recording - consumed); an active one
                // starts at its start position and is raised by the probe / consumed floor.
                state.replayConsumedPosition = replayFrom;
                state.replayRecordingPosition = liveFollow ? replayFrom : recording.stopPosition();
                final long connectDeadlineMs = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
                long nextSourceCheckMs = System.currentTimeMillis() + SOURCE_CHECK_INTERVAL_MS;
                long consumedAtLastCheck = NO_PRIOR_CHECK;
                boolean imageWasLive = false;
                Image replayImage = null;
                boolean followedRecordingEnded = false;
                while (running && !state.halted) {
                    if (replay.imageCount() > 0) {
                        replayImage = replay.imageAtIndex(0);
                    }
                    final int fragments = source.poll(replay, handler, POLL_LIMIT);
                    ae.duty();
                    maybeLogStatus();
                    if (replay.imageCount() > 0) {
                        replayImage = replay.imageAtIndex(0);
                    }
                    imageWasLive |= replayImage != null;
                    if (replayImage != null) {
                        // Retain the image through detach so a bounded EOF can be verified by
                        // its final position, never inferred from an early EOS/closed image.
                        final long consumedNow = replayImage.position();
                        state.replayConsumedPosition = consumedNow;
                        if (consumedNow > state.replayRecordingPosition) {
                            // Consumed bytes were necessarily recorded: keeps the recorded-position
                            // gauge from lagging below consumed between live-follow probes.
                            state.replayRecordingPosition = consumedNow;
                        }
                    }
                    if (fragments == 0) {
                        // The replay went quiet: whatever is staged leaves NOW. Batch fill must never
                        // hold settlement latency hostage — a partial batch on an empty poll is the
                        // normal quiet-market frame, not a failure to batch.
                        flushSettleBatch(ae);
                        if (!liveFollow && replayDrained(replay, recording, replayImage)) {
                            fullyDrained = true;
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
                            final long consumed = replayImage != null ? replayImage.position() : replayFrom;
                            // Throws when the archive itself is gone -> epoch restart. This is
                            // what un-wedges a live-follow whose source process was killed.
                            final long recPos = source.recordingPosition(recording.recordingId());
                            if (recPos == AeronArchive.NULL_POSITION) {
                                // Recording stopped under our live-follow (source restarted; its
                                // successor recording isn't in this epoch's chain listing).
                                final long stop = source.stopPosition(recording.recordingId());
                                state.replayRecordingPosition = stop;
                                if (consumed >= stop) {
                                    System.out.println("[BRIDGE] followed recording "
                                            + recording.recordingId() + " stopped and is fully "
                                            + "consumed — ending epoch to re-list the chain");
                                    followedRecordingEnded = true;
                                    break;
                                }
                                if (consumed == consumedAtLastCheck && consumedAtLastCheck != NO_PRIOR_CHECK) {
                                    sourceStall("recording " + recording.recordingId()
                                            + " stopped but replay is stuck at " + consumed
                                            + " short of stop position " + stop);
                                }
                            } else {
                                state.replayRecordingPosition = recPos;
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
                        idle.reset();
                    }
                }
                if (followedRecordingEnded) {
                    break; // end the epoch: the next one re-lists and follows the successor
                }
            }
            if (fullyDrained) {
                resumeMemo.noteDrained(chain, i, forwardCount() != forwardsAtEpochStart);
            }
            if (!running || state.halted) {
                return;
            }
        }
        // Chain exhausted without a live-follow in progress (source down or mid-restart):
        // pause before the next epoch so we don't hot-loop re-reading the whole chain.
        sleep(ERROR_BACKOFF_MS);
    }

    private Subscription openReplay(final ArchiveJournalSource source,
                                    final ArchiveJournalSource.Recording recording, final long from) {
        try {
            return source.openReplay(recording, from);
        } catch (ArchiveException e) {
            if (from > recording.startPosition() && e.errorCode() == ArchiveException.INVALID_POSITION) {
                // Reject the optimization, never skip past a bad byte. Keep this failure visible;
                // a later epoch re-reads from the catalog start against a fresh AE watermark.
                // Real corruption is still reported by Aeron during that replay. Resource and
                // connectivity failures retain the proof so retries do not rescan a large head.
                resumeMemo.invalidate("archive rejected remembered position: source=" + source.identity()
                        + " recording=" + recording + " position=" + from);
                System.err.println("[BRIDGE] replay memo invalidated: " + resumeMemo.invalidationReason());
            }
            throw e;
        }
    }

    private long forwardCount() {
        return state.forwardedTrades + state.forwardedTerminals;
    }

    private void sourceStall(final String reason) {
        state.sourceStalls++;
        throw new IllegalStateException(reason);
    }

    private boolean replayDrained(final Subscription replay, final ArchiveJournalSource.Recording recording,
                                  final Image image) {
        if (image != null && image.position() >= recording.stopPosition()) {
            return true;
        }
        if (replay.isClosed() || (image != null && (image.isClosed() || image.isEndOfStream()))) {
            sourceStall("stopped replay ended before catalog EOF: recording=" + recording
                    + " consumed=" + (image == null ? "unknown" : image.position()));
        }
        return false;
    }

    private void onJournalEntry(final DirectBuffer buffer, final int offset,
                                final BridgeFilter filter, final AeFeedClient ae,
                                final long fragmentEndPosition) {
        journalHeader.wrap(buffer, offset);
        if (journalHeader.schemaId() != JournalTradeDecoder.SCHEMA_ID) {
            resumeMemo.seal(); // unrecognized bytes cannot extend an applied-prefix proof
            return; // foreign schema on the journal stream — ignore
        }
        if (journalHeader.templateId() == JournalTradeDecoder.TEMPLATE_ID) {
            tradeDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
            final BridgeFilter.Action action = filter.onTrade(tradeDecoder.egressSeq(), tradeDecoder.tradeId());
            switch (action) {
                case FORWARD -> {
                    resumeMemo.seal();
                    // v5: trades leave as SettleBatch chunks. Stage now; the batch flushes at the
                    // cap (here), on an interleaved terminal (journal order!), on an empty replay
                    // poll, or on a halt — never sits waiting to fill.
                    if (translator.stagedTradeCount() == JournalToMoneyTranslator.TRADE_BATCH_CAP) {
                        flushSettleBatch(ae);
                    }
                    final int i = translator.stagedTradeCount();
                    translator.stageTrade(buffer, offset);
                    stagedTradeIds[i] = tradeDecoder.tradeId();
                    stagedEgressSeqs[i] = tradeDecoder.egressSeq();
                    stagedTimestamps[i] = tradeDecoder.timestamp();
                }
                case SKIP -> {
                    state.skippedEntries++;
                    // The filter also skips duplicates of trades staged earlier this epoch.
                    // Only ids already applied at the initial AE sync can prove a byte prefix.
                    resumeMemo.noteSkippedTrade(currentRecording, fragmentEndPosition, tradeDecoder.tradeId());
                }
                case HALT -> {
                    resumeMemo.seal();
                    // Everything staged precedes the gap and is legitimate money: flush it, THEN park.
                    flushSettleBatch(ae);
                    halt("dense tradeId gap: journal shows tradeId=" + tradeDecoder.tradeId()
                            + " after lastForwarded=" + filter.lastForwardedTradeId()
                            + " (egressSeq=" + tradeDecoder.egressSeq() + ")", filter);
                }
            }
        } else if (journalHeader.templateId() == JournalTerminalDecoder.TEMPLATE_ID) {
            terminalDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
            final BridgeFilter.Action action = filter.onTerminal(terminalDecoder.egressSeq());
            switch (action) {
                case FORWARD -> {
                    resumeMemo.seal();
                    // Journal order is the money order: the terminal must not overtake the staged
                    // trades that precede it (a terminal releases the residual the settles drew on).
                    flushSettleBatch(ae);
                    final int length = translator.translateTerminal(buffer, offset, outBuffer, 0);
                    ae.offerBlocking(outBuffer, length);
                    state.forwardedTerminals++;
                    state.lastForwardedEgressSeq = terminalDecoder.egressSeq();
                }
                case SKIP -> {
                    state.skippedEntries++;
                    // Terminal SKIP always uses the fixed initial AE consume watermark.
                    resumeMemo.noteSkipHighWater(currentRecording, fragmentEndPosition);
                }
                case HALT -> {
                    resumeMemo.seal(); // latched by a prior trade gap
                }
            }
        } else {
            resumeMemo.seal();
        }
    }

    /**
     * Offer the staged trades as one SettleBatch frame, then record the per-trade bookkeeping the
     * single-message path used to do at its offer: forward-latency samples, ack in-flight arming and
     * the last-forwarded gauges. All entries share one offer-return instant — they left together.
     * No-op when nothing is staged.
     */
    private void flushSettleBatch(final AeFeedClient ae) {
        final int count = translator.stagedTradeCount();
        if (count == 0) {
            return;
        }
        final int length = translator.encodeStagedTrades(outBuffer, 0);
        ae.offerBlocking(outBuffer, length);
        for (int i = 0; i < count; i++) {
            recordSettleForwardLatency(stagedTimestamps[i]);
            trackInFlightSettle(stagedTradeIds[i]);
            state.forwardedTrades++;
            state.lastForwardedTradeId = stagedTradeIds[i];
            state.lastForwardedEgressSeq = stagedEgressSeqs[i];
        }
    }

    /**
     * Forward-latency sample: journal trade timestamp (epoch ms, written by the ME leader's
     * clock) -> this host's clock at offer-return. CROSS-HOST clocks, so a non-positive or
     * future timestamp is possible garbage: count the anomaly, never record it.
     */
    private void recordSettleForwardLatency(final long journalTimestampMs) {
        final long nowMs = System.currentTimeMillis();
        if (journalTimestampMs <= 0 || journalTimestampMs > nowMs) {
            state.settleForwardClockAnomalies++;
            return;
        }
        state.settleForwardLatency.record(nowMs - journalTimestampMs);
    }

    /** Arm the ack-latency clock for a just-offered settle; skip (counted) when at the cap. */
    private void trackInFlightSettle(final long tradeId) {
        if (inFlightSettles.size() >= IN_FLIGHT_SETTLE_CAP) {
            state.settleAckMapSkips++;
            return;
        }
        inFlightSettles.put(tradeId, System.nanoTime());
    }

    /**
     * SettlementApplied egress observed (same thread: egress is polled by this agent).
     * Unknown tradeIds are acks for settles offered before a restart — ignore them.
     */
    private void onSettlementAck(final long tradeId) {
        final long offerReturnNanos = inFlightSettles.remove(tradeId);
        if (offerReturnNanos != NO_OFFER_TIME) {
            state.settleAckLatency.record((System.nanoTime() - offerReturnNanos) / 1_000_000);
        }
    }

    private void halt(final String reason, final BridgeFilter filter) {
        state.halted = true;
        state.gapsDetected = filter.gapsDetected();
        System.err.println("[BRIDGE] *** HALTED — LOST SETTLEMENT SUSPECTED *** " + reason
                + " | a journal gap means money the AE will never see; NOT forwarding past it. "
                + "Investigate the journal archives and the AE state, then restart the bridge. "
                + state.render());
    }

    private void maybeLogStatus() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastStatusLogMs >= STATUS_LOG_INTERVAL_MS) {
            lastStatusLogMs = nowMs;
            System.out.println("[BRIDGE] " + state.render());
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

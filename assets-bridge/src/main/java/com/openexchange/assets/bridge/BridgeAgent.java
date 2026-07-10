// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.match.infrastructure.journal.generated.JournalTerminalDecoder;
import com.match.infrastructure.journal.generated.JournalTradeDecoder;
import com.match.infrastructure.journal.generated.MessageHeaderDecoder;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.List;

/**
 * The settlement bridge's engine: ME journal -> filter -> translate -> AE ingress.
 *
 * Stateless by design. Every epoch starts from scratch: connect the AE session, ask it how
 * far it has consumed (FeedPositionReport -> W/T), build a fresh {@link BridgeFilter}, then
 * read the journal chain from the earliest retained byte forward — the filter skips what
 * the AE already has, the AE's idempotency absorbs the inclusive boundary, and a dense
 * tradeId gap HALTS the bridge (correctness over liveness: a gap is a lost settlement).
 *
 * Every failure (AE session death, journal source death, purge race) tears down the epoch
 * and starts a new one. A HALT parks the bridge with metrics alive until an operator acts.
 */
public final class BridgeAgent implements Runnable {

    private static final int POLL_LIMIT = 32;
    private static final long ERROR_BACKOFF_MS = 1_000;
    private static final long STATUS_LOG_INTERVAL_MS = 10_000;

    private final BridgeConfig config;
    private final String aeronDirectoryName;
    private final BridgeState state;
    private final JournalToMoneyTranslator translator = new JournalToMoneyTranslator();
    private final MessageHeaderDecoder journalHeader = new MessageHeaderDecoder();
    private final JournalTradeDecoder tradeDecoder = new JournalTradeDecoder();
    private final JournalTerminalDecoder terminalDecoder = new JournalTerminalDecoder();
    private final UnsafeBuffer outBuffer = new UnsafeBuffer(new byte[256]);
    private final IdleStrategy idle = new BackoffIdleStrategy();

    private volatile boolean running = true;
    private long lastStatusLogMs;

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
        try (AeFeedClient ae = AeFeedClient.connect(config, aeronDirectoryName)) {
            state.connectedToAe = true;
            final AeFeedClient.FeedPosition pos = ae.queryFeedPosition(config.queryTimeoutMs);
            final BridgeFilter filter = new BridgeFilter(
                    pos.consumePosition(), pos.lastAppliedTradeId(), config.haltOnGap);
            state.epochConsumePosition = pos.consumePosition();
            state.epochLastAppliedTradeId = pos.lastAppliedTradeId();
            state.epochs++;
            System.out.println("[BRIDGE] epoch " + state.epochs + ": AE at consumePosition="
                    + pos.consumePosition() + " lastAppliedTradeId=" + pos.lastAppliedTradeId());

            try (JournalSource source = JournalSource.connectFirstHealthy(config, aeronDirectoryName)) {
                state.journalSource = source.endpoint();
                followChain(source, filter, ae);
            }
        } finally {
            state.connectedToAe = false;
        }
    }

    private void followChain(final JournalSource source, final BridgeFilter filter, final AeFeedClient ae) {
        final List<JournalSource.Recording> chain = source.recordings();
        if (chain.isEmpty()) {
            // Journal enabled but nothing recorded yet (or dark): wait and re-list next epoch.
            System.out.println("[BRIDGE] no journal recordings at " + source.endpoint() + " yet — waiting");
            sleep(ERROR_BACKOFF_MS);
            return;
        }
        final FragmentHandler handler = (buffer, offset, length, header) ->
                onJournalEntry(buffer, offset, filter, ae);

        for (final JournalSource.Recording recording : chain) {
            System.out.println("[BRIDGE] following recording " + recording.recordingId()
                    + (recording.isActive() ? " (ACTIVE, live-follow)" : " (stopped)"));
            try (Subscription replay = source.openReplay(recording)) {
                while (running && !state.halted) {
                    final int fragments = source.poll(replay, handler, POLL_LIMIT);
                    ae.duty();
                    maybeLogStatus();
                    if (fragments == 0) {
                        if (!recording.isActive() && replayDrained(replay, recording)) {
                            break; // stopped recording fully consumed -> next in chain
                        }
                        if (replay.isClosed() || !replay.isConnected() && replay.imageCount() == 0
                                && replayStarted(replay)) {
                            // Live image lost (source node died / purge race): epoch restart re-lists.
                            throw new IllegalStateException("journal replay image lost on recording "
                                    + recording.recordingId());
                        }
                        idle.idle();
                    } else {
                        idle.reset();
                    }
                }
            }
            if (!running || state.halted) {
                return;
            }
        }
    }

    private boolean replayStarted(final Subscription replay) {
        // A live-follow that never connected yet is "starting", not "lost" — poll a while first.
        return replay.imageCount() > 0 || replay.isClosed();
    }

    private boolean replayDrained(final Subscription replay, final JournalSource.Recording recording) {
        if (replay.imageCount() == 0) {
            return replay.isClosed(); // bounded replay closes its image at the bound
        }
        return replay.imageAtIndex(0).position()
                >= recording.stopPosition() || replay.imageAtIndex(0).isEndOfStream();
    }

    private void onJournalEntry(final DirectBuffer buffer, final int offset,
                                final BridgeFilter filter, final AeFeedClient ae) {
        journalHeader.wrap(buffer, offset);
        if (journalHeader.schemaId() != JournalTradeDecoder.SCHEMA_ID) {
            return; // foreign schema on the journal stream — ignore
        }
        if (journalHeader.templateId() == JournalTradeDecoder.TEMPLATE_ID) {
            tradeDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
            final BridgeFilter.Action action = filter.onTrade(tradeDecoder.egressSeq(), tradeDecoder.tradeId());
            switch (action) {
                case FORWARD -> {
                    final int length = translator.translateTrade(buffer, offset, outBuffer, 0);
                    ae.offerBlocking(outBuffer, length);
                    state.forwardedTrades++;
                    state.lastForwardedTradeId = tradeDecoder.tradeId();
                    state.lastForwardedEgressSeq = tradeDecoder.egressSeq();
                }
                case SKIP -> state.skippedEntries++;
                case HALT -> halt("dense tradeId gap: journal shows tradeId=" + tradeDecoder.tradeId()
                        + " after lastForwarded=" + filter.lastForwardedTradeId()
                        + " (egressSeq=" + tradeDecoder.egressSeq() + ")", filter);
            }
        } else if (journalHeader.templateId() == JournalTerminalDecoder.TEMPLATE_ID) {
            terminalDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
            final BridgeFilter.Action action = filter.onTerminal(terminalDecoder.egressSeq());
            switch (action) {
                case FORWARD -> {
                    final int length = translator.translateTerminal(buffer, offset, outBuffer, 0);
                    ae.offerBlocking(outBuffer, length);
                    state.forwardedTerminals++;
                    state.lastForwardedEgressSeq = terminalDecoder.egressSeq();
                }
                case SKIP -> state.skippedEntries++;
                case HALT -> { /* latched by a prior trade gap; terminals just stop flowing */ }
            }
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

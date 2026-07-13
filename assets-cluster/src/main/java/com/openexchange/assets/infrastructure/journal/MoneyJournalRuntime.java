// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.journal;

import com.openexchange.assets.infrastructure.InfrastructureConstants;
import com.openexchange.assets.infrastructure.Logger;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;

/**
 * Owns the money journal's moving parts on one node: the SPSC ring ({@link MoneyJournal}, written
 * by the cluster service thread) and the writer agent ({@link MoneyJournalWriterAgent}, its own
 * AgentRunner thread) that records it via the node's OWN archive; unlike the ME settlement journal
 * there is no second archive to launch: the AE archive already lives on durable storage (BASE_DIR).
 *
 * Two-step lifecycle mirroring the ME runtime: {@link #createIfEnabled} parses env and builds the
 * driver-independent parts (so the ring can be armed on the engine before launch), then
 * {@link #start} launches the writer thread once the node's media driver and archive are up.
 *
 * Env:
 *   AE_MONEY_JOURNAL_ENABLED - "true" to enable (default false = dark, zero behavior change).
 *                              MUST be set uniformly across the cluster: the engine's journalSeq is
 *                              replicated (snapshotted) state, so replicas disagreeing on whether
 *                              it advances would diverge. Roll all nodes with the same value.
 */
public final class MoneyJournalRuntime implements AutoCloseable {

    private static final Logger log = Logger.getLogger(MoneyJournalRuntime.class);

    /** Ring capacity: movements are low-rate next to the ME trade stream; 16MB absorbs any burst. */
    private static final int RING_BYTES = 16 * 1024 * 1024;

    private final int nodeId;
    private final MoneyJournal journal;

    private AgentRunner writerRunner;

    private MoneyJournalRuntime(final int nodeId) {
        this.nodeId = nodeId;
        this.journal = new MoneyJournal(RING_BYTES);
    }

    /** @return the runtime, or null when AE_MONEY_JOURNAL_ENABLED is not "true" (journal dark). */
    public static MoneyJournalRuntime createIfEnabled(final int nodeId) {
        if (!"true".equalsIgnoreCase(System.getenv("AE_MONEY_JOURNAL_ENABLED"))) {
            return null;
        }
        log.info("Money journal ENABLED: node=%d channel=%s stream=%d ring=%d",
                nodeId, InfrastructureConstants.MONEY_JOURNAL_CHANNEL,
                InfrastructureConstants.MONEY_JOURNAL_STREAM_ID, RING_BYTES);
        return new MoneyJournalRuntime(nodeId);
    }

    /** The ring facade to arm on the engine (safe before {@link #start}: the ring buffers). */
    public MoneyJournal journal() {
        return journal;
    }

    /**
     * Launch the writer thread. {@code archiveClientContext} is a pristine (unconcluded) client
     * context for the node's own archive, e.g. {@code clusterConfig.aeronArchiveContext().clone()};
     * the agent clones it per connect attempt and retries with backoff, so calling this before the
     * archive is reachable is safe. Call once per process.
     */
    public MoneyJournalRuntime start(final AeronArchive.Context archiveClientContext, final ErrorHandler errorHandler) {
        final MoneyJournalWriterAgent agent = new MoneyJournalWriterAgent(
                journal,
                archiveClientContext::clone,
                InfrastructureConstants.MONEY_JOURNAL_CHANNEL,
                InfrastructureConstants.MONEY_JOURNAL_STREAM_ID);
        writerRunner = new AgentRunner(new BackoffIdleStrategy(), errorHandler, null, agent);
        AgentRunner.startOnThread(writerRunner);
        log.info("Money journal writer started (node %d)", nodeId);
        return this;
    }

    @Override
    public void close() {
        CloseHelper.quietClose(writerRunner);
    }
}

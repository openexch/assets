// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource;

/**
 * Bridge-specific wiring for the shared {@link ArchiveJournalSource} follow helper: pins the ME
 * settlement-journal connection shape (kept literal so the bridge does not depend on match-cluster)
 * and hands it to the shared implementation. The replay/liveness mechanics live in the shared
 * helper; only the connection parameters differ between the bridge and the money projector.
 */
final class JournalSource {

    /** Mirrors match InfrastructureConstants (kept literal: the bridge must not depend on match-cluster). */
    static final int SETTLEMENT_JOURNAL_STREAM_ID = 4001;
    static final int JOURNAL_ARCHIVE_CONTROL_STREAM_ID = 4010;
    private static final String JOURNAL_CHANNEL_URI = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 9101;

    private JournalSource() {
    }

    /** First-healthy-wins across the configured journal archives (all nodes journal identically). */
    static ArchiveJournalSource connectFirstHealthy(final BridgeConfig config, final String aeronDirectoryName) {
        return ArchiveJournalSource.connectFirstHealthy(
                config.journalArchiveEndpoints,
                JOURNAL_ARCHIVE_CONTROL_STREAM_ID,
                SETTLEMENT_JOURNAL_STREAM_ID,
                JOURNAL_CHANNEL_URI,
                REPLAY_STREAM_ID,
                aeronDirectoryName,
                config.localHost);
    }
}

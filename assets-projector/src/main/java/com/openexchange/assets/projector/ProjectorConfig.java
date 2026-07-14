// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Env-driven projector configuration. The projector is stateless with respect to journal
 * position: everything here is connection shape. The resume high-water is read from Postgres
 * (MAX(journal_seq)) on every epoch, never from a file and never from the AE.
 *
 * The money-journal stream id (4101), the archive default control stream id (10) and the IPC
 * channel are pinned literal (mirroring how the settlement bridge pins the ME journal's 4001 /
 * 4010) so the projector never has to depend on assets-cluster.
 */
public final class ProjectorConfig {

    /** The AE money journal is recorded on this node-local IPC stream (InfrastructureConstants). */
    public static final int MONEY_JOURNAL_STREAM_ID = 4101;
    /** Channel fragment for listRecordingsForUri; a prefix that matches "aeron:ipc?term-length=4m". */
    public static final String MONEY_JOURNAL_CHANNEL = "aeron:ipc";
    /** Local replay stream id (own media driver, so it cannot collide with the bridge's 9101). */
    public static final int REPLAY_STREAM_ID = 9161;

    /** AE consensus-archive control endpoints, first-healthy-wins (every node journals identically). */
    public final List<String> journalArchiveEndpoints;
    /** The AE consensus archive's control request stream id (Aeron default 10). */
    public final int archiveControlStreamId;
    public final String postgresUrl;
    public final String postgresUser;
    public final String postgresPassword;
    /** Latch HALT on a dense journalSeq gap (production default true; false = operator override). */
    public final boolean haltOnGap;
    /** Port for the /metrics + /health HTTP endpoint (0 = ephemeral, for tests). */
    public final int metricsPort;

    public ProjectorConfig(
            final List<String> journalArchiveEndpoints,
            final int archiveControlStreamId,
            final String postgresUrl,
            final String postgresUser,
            final String postgresPassword,
            final boolean haltOnGap,
            final int metricsPort) {
        this.journalArchiveEndpoints = journalArchiveEndpoints;
        this.archiveControlStreamId = archiveControlStreamId;
        this.postgresUrl = postgresUrl;
        this.postgresUser = postgresUser;
        this.postgresPassword = postgresPassword;
        this.haltOnGap = haltOnGap;
        this.metricsPort = metricsPort;
    }

    public static ProjectorConfig fromEnv() {
        return new ProjectorConfig(
                List.of(envOr("PROJECTOR_AE_JOURNAL_ARCHIVES",
                        "localhost:9301,localhost:9401,localhost:9501").split(",")),
                Integer.parseInt(envOr("PROJECTOR_AE_ARCHIVE_CONTROL_STREAM_ID", "10")),
                envOr("PROJECTOR_POSTGRES_URL", "jdbc:postgresql://localhost:5432/assets"),
                envOr("PROJECTOR_POSTGRES_USER", "assets"),
                // No default password: unset means Postgres auth fails loudly rather than shipping
                // a baked-in credential. Also accepts PROJECTOR_POSTGRES_PASSWORD_FILE (secret mount).
                secretOr("PROJECTOR_POSTGRES_PASSWORD", ""),
                !"false".equalsIgnoreCase(envOr("PROJECTOR_HALT_ON_GAP", "true")),
                Integer.parseInt(envOr("PROJECTOR_METRICS_PORT", "9601")));
    }

    private static String envOr(final String key, final String fallback) {
        final String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }

    /** Like envOr, but also accepts KEY_FILE pointing at a secret file (mirrors OMS OMS_*_FILE). */
    private static String secretOr(final String key, final String fallback) {
        final String direct = System.getenv(key);
        if (direct != null && !direct.isBlank()) {
            return direct;
        }
        final String file = System.getenv(key + "_FILE");
        if (file != null && !file.isBlank()) {
            try {
                return Files.readString(Path.of(file)).trim();
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot read " + key + "_FILE " + file + ": "
                        + e.getMessage(), e);
            }
        }
        return fallback;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * RCS S1 boot guard: this engine never accepts or replicates standby snapshots. Aeron 1.53 replicates
 * and promotes STANDBY_SNAPSHOT recording-log entries at CM start without consulting
 * {@code acceptStandbySnapshots}, so the guard refuses to boot when
 * <ol>
 * <li>the consensus-module context (G-A1) or the system property
 *     {@code aeron.cluster.accept.standby.snapshots} (G-A2) enables acceptance;</li>
 * <li>the cluster dir is unset (G-R0), or its {@code recording.log} cannot be read (G-R2);</li>
 * <li>any STANDBY_SNAPSHOT entry exists, valid or invalidated (G-R3).</li>
 * </ol>
 * An absent {@code recording.log} is genesis and passes (G-R1). The guard only reads the log.
 */
public final class StandbySnapshotBootGuard {

    static final String PREFIX = "ASSETS BOOT GUARD: ";
    static final String ACCEPT_PROPERTY = "aeron.cluster.accept.standby.snapshots";

    /** Boot refused; the node must not launch any Aeron component. */
    public static final class Refused extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Refused(final String message) {
            super(PREFIX + message);
        }

        Refused(final String message, final Throwable cause) {
            super(PREFIX + message, cause);
        }
    }

    private StandbySnapshotBootGuard() {
    }

    public static void enforce(final ConsensusModule.Context ctx) {
        try {
            check(ctx);
        } catch (final Refused refused) {
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    private static void check(final ConsensusModule.Context ctx) {
        if (ctx.acceptStandbySnapshots()) {
            throw new Refused("consensus module context has acceptStandbySnapshots=true");
        }
        if (Boolean.getBoolean(ACCEPT_PROPERTY)) {
            throw new Refused("system property " + ACCEPT_PROPERTY + " is true; this engine never accepts"
                    + " standby snapshots (unset it)");
        }
        final File clusterDir = ctx.clusterDir();
        if (clusterDir == null) {
            throw new Refused("consensus module clusterDir is not set");
        }
        if (new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            final List<String> standby = new ArrayList<>();
            try (RecordingLog log = new RecordingLog(clusterDir, false)) {
                for (final RecordingLog.Entry e : log.entries()) {
                    if (e.type == RecordingLog.ENTRY_TYPE_STANDBY_SNAPSHOT) {
                        standby.add("{index=" + e.entryIndex + " recordingId=" + e.recordingId
                                + " leadershipTermId=" + e.leadershipTermId + " logPosition=" + e.logPosition
                                + " serviceId=" + e.serviceId + " archiveEndpoint=" + e.archiveEndpoint
                                + " isValid=" + e.isValid + "}");
                    }
                }
            } catch (final RuntimeException ex) {
                throw new Refused("cannot read " + new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME)
                        + ": " + ex, ex);
            }
            if (!standby.isEmpty()) {
                throw new Refused(standby.size() + " STANDBY_SNAPSHOT recording-log entr"
                        + (standby.size() == 1 ? "y" : "ies") + " in " + clusterDir
                        + " (restore the cluster dir from a peer or a clean genesis): " + String.join(", ", standby));
            }
        }
        System.out.println("[BOOT-GUARD] standby=0 acceptStandbySnapshots=false clusterDir=" + clusterDir);
    }
}

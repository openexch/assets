// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.SnapshotCadence;

/**
 * The boot refusal of stub mode without a floor. Runs first in {@code AeronCluster.run}, before
 * the service is constructed from the settings it returns and before any Aeron component launches, so it reads
 * settings only: no archive is open here and a stub payload is inside an archive recording (review F1). The clusterId
 * of the newest stub is checked in {@code onStart}, where the stub bytes are readable (3.3).
 *
 * <table>
 *   <caption>Decision table</caption>
 *   <tr><td>guard row 1</td><td>any setting of {@link SnapshotModeSettings} fails its range</td><td>refuse with its 3.5 text (every
 *       mode)</td></tr>
 *   <tr><td>guard row 2</td><td>{@code SNAPSHOT_MODE=shadow}, {@code SNAPSHOT_PRUNE_ENABLED=true} and
 *       {@code SHADOW_FLOOR_ENABLED=false}</td><td>refuse, unless {@value #UNCHECKED_PROPERTY} is true (a
 *       test-only override, the {@code ShadowSelfCheck} pattern)</td></tr>
 *   <tr><td>guard row OK</td><td>otherwise</td><td>{@code [BOOT-GUARD] snapshotMode=<mode> prune=<bool> floor=wired}</td></tr>
 * </table>
 *
 * <p>The runtime second net is in {@code CoreClusteredService.startDurability}: in shadow mode the pruner's
 * watermark must be the {@code ShadowFloor} instance ({@code SnapshotLogPruner.watermarkIs}).</p>
 */
public final class StubModeBootGuard {

    /** Lets {@code SHADOW_FLOOR_ENABLED=false} through guard row 2; hosts never set it (11.6 control only). */
    public static final String UNCHECKED_PROPERTY = "oxs.shadow.floor.unchecked";

    private StubModeBootGuard() {
    }

    /** The guard over this process's environment. */
    public static SnapshotModeSettings enforce() {
        return enforce(SnapshotModeSettings.Source.ENVIRONMENT, SnapshotCadence.booleanFromEnv(
                "SNAPSHOT_PRUNE_ENABLED", "snapshot.prune.enabled", true));
    }

    /**
     * @return the parsed settings when the boot may proceed
     * @throws StandbySnapshotBootGuard.Refused with {@code CORE BOOT GUARD: } and the reason
     */
    public static SnapshotModeSettings enforce(final SnapshotModeSettings.Source source, final boolean pruneEnabled) {
        try {
            return check(source, pruneEnabled);
        } catch (final StandbySnapshotBootGuard.Refused refused) {
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    private static SnapshotModeSettings check(final SnapshotModeSettings.Source source, final boolean pruneEnabled) {
        final SnapshotModeSettings settings;
        try {
            settings = SnapshotModeSettings.parse(source);
        } catch (final IllegalArgumentException badSetting) {
            throw new StandbySnapshotBootGuard.Refused(badSetting.getMessage(), badSetting);
        }
        if (settings.mode() == SnapshotMode.SHADOW && pruneEnabled && !settings.floorEnabled()) {
            if (!Boolean.getBoolean(UNCHECKED_PROPERTY)) {
                throw new StandbySnapshotBootGuard.Refused("SNAPSHOT_MODE=shadow needs ShadowFloor on the pruner or"
                        + " SNAPSHOT_PRUNE_ENABLED=false");
            }
            System.err.println("[BOOT-GUARD] CRITICAL: SNAPSHOT_MODE=shadow with SHADOW_FLOOR_ENABLED=false let through by "
                    + UNCHECKED_PROPERTY + "=true (test only): this node may purge log below a stub it cannot restore");
        }
        System.out.println("[BOOT-GUARD] snapshotMode=" + settings.mode().text() + " prune=" + pruneEnabled
                + " floor=" + (settings.mode() == SnapshotMode.SHADOW && pruneEnabled
                ? (settings.floorEnabled() ? "wired" : "UNCHECKED-OFF") : "not-needed") + " " + settings);
        return settings;
    }
}

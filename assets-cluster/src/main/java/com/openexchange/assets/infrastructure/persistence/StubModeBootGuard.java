// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.cluster.SnapshotCadence;

/**
 * RCS S4 4.1 (MF-4): the boot refusal of stub mode without a floor. Runs first in {@code AeronCluster.run}, before
 * the service is constructed from the settings it returns and before any Aeron component launches, so it reads
 * settings only: no archive is open here and a stub payload is inside an archive recording (review F1). The clusterId
 * of the newest stub is checked in {@code onStart}, where the stub bytes are readable (3.3).
 *
 * <table>
 *   <caption>Decision table</caption>
 *   <tr><td>G-S1</td><td>any setting of {@link RcsSettings} fails its range</td><td>refuse with its 3.5 text (every
 *       mode)</td></tr>
 *   <tr><td>G-S2</td><td>{@code RCS_MODE=shadow}, {@code SNAPSHOT_PRUNE_ENABLED=true} and
 *       {@code SHADOW_FLOOR_ENABLED=false}</td><td>refuse (MF-4), unless {@value #UNCHECKED_PROPERTY} is true (a
 *       test-only override, the S3 {@code ShadowSelfCheck} pattern)</td></tr>
 *   <tr><td>G-OK</td><td>otherwise</td><td>{@code [BOOT-GUARD] rcsMode=<mode> prune=<bool> floor=wired}</td></tr>
 * </table>
 *
 * <p>The runtime second net is in {@code AssetsClusteredService.startDurability}: in shadow mode the pruner's
 * watermark must be the {@code ShadowFloor} instance ({@code SnapshotLogPruner.watermarkIs}).</p>
 */
public final class StubModeBootGuard {

    /** Lets {@code SHADOW_FLOOR_ENABLED=false} through G-S2; hosts never set it (11.6 control only). */
    public static final String UNCHECKED_PROPERTY = "oxs.shadow.floor.unchecked";

    private StubModeBootGuard() {
    }

    /** The guard over this process's environment. */
    public static RcsSettings enforce() {
        return enforce(RcsSettings.Source.ENVIRONMENT, SnapshotCadence.booleanFromEnv(
                "SNAPSHOT_PRUNE_ENABLED", "snapshot.prune.enabled", true));
    }

    /**
     * @return the parsed settings when the boot may proceed
     * @throws StandbySnapshotBootGuard.Refused with {@code ASSETS BOOT GUARD: } and the reason
     */
    public static RcsSettings enforce(final RcsSettings.Source source, final boolean pruneEnabled) {
        try {
            return check(source, pruneEnabled);
        } catch (final StandbySnapshotBootGuard.Refused refused) {
            System.err.println(refused.getMessage());
            throw refused;
        }
    }

    private static RcsSettings check(final RcsSettings.Source source, final boolean pruneEnabled) {
        final RcsSettings settings;
        try {
            settings = RcsSettings.parse(source);
        } catch (final IllegalArgumentException badSetting) {
            throw new StandbySnapshotBootGuard.Refused(badSetting.getMessage(), badSetting);
        }
        if (settings.mode() == RcsMode.SHADOW && pruneEnabled && !settings.floorEnabled()) {
            if (!Boolean.getBoolean(UNCHECKED_PROPERTY)) {
                throw new StandbySnapshotBootGuard.Refused("RCS_MODE=shadow needs ShadowFloor on the pruner or"
                        + " SNAPSHOT_PRUNE_ENABLED=false (MF-4)");
            }
            System.err.println("[BOOT-GUARD] CRITICAL: RCS_MODE=shadow with SHADOW_FLOOR_ENABLED=false let through by "
                    + UNCHECKED_PROPERTY + "=true (test only): this node may purge log below a stub it cannot restore");
        }
        System.out.println("[BOOT-GUARD] rcsMode=" + settings.mode().text() + " prune=" + pruneEnabled
                + " floor=" + (settings.mode() == RcsMode.SHADOW && pruneEnabled
                ? (settings.floorEnabled() ? "wired" : "UNCHECKED-OFF") : "not-needed") + " " + settings);
        return settings;
    }
}

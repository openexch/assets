// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.archive.client.AeronArchive;

/**
 * Positive control for {@code NoPurgeCallSiteInAssetsTest} and {@code RcsClosureNeverSchedulesTest} (test scope
 * only): a shadow purge path in the assets tree, outside the kit's one purge owner.
 */
public final class ForbiddenAssetsPurgeProbe {
    private ForbiddenAssetsPurgeProbe() {
    }

    public static void purge(final AeronArchive archive, final long recordingId) {
        archive.purgeRecording(recordingId);
    }
}

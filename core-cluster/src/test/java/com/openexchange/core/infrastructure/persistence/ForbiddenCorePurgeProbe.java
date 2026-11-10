// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import io.aeron.archive.client.AeronArchive;

/**
 * Positive control for {@code NoPurgeCallSiteInCoreTest} and {@code RestoreClosureNeverSchedulesTest} (test scope
 * only): a shadow purge path in the core tree, outside the kit's one purge owner.
 */
public final class ForbiddenCorePurgeProbe {
    private ForbiddenCorePurgeProbe() {
    }

    public static void purge(final AeronArchive archive, final long recordingId) {
        archive.purgeRecording(recordingId);
    }
}

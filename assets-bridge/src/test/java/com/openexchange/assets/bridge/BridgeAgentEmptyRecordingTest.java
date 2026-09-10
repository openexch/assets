// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource;
import io.aeron.archive.client.AeronArchive;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The 2026-08-23 live loop: a rolling update left an empty-stopped journal recording
 * (start == stop == 0); replaying it fails the archive's start-below-limit check and the
 * bridge resynced onto the same recording forever. The chain must step over it.
 */
public class BridgeAgentEmptyRecordingTest {

    @Test
    public void stoppedEmptyRecordingIsSkipped() {
        assertTrue(BridgeAgent.isEmptyStopped(new ArchiveJournalSource.Recording(9, 0, 0)));
        assertTrue(BridgeAgent.isEmptyStopped(new ArchiveJournalSource.Recording(3, 4096, 4096)));
    }

    @Test
    public void stoppedRecordingWithBytesIsFollowed() {
        assertFalse(BridgeAgent.isEmptyStopped(new ArchiveJournalSource.Recording(8, 0, 1024)));
    }

    @Test
    public void malformedStoppedExtentIsNotSilentlySkipped() {
        assertFalse(BridgeAgent.isEmptyStopped(new ArchiveJournalSource.Recording(9, 1024, 0)));
    }

    @Test
    public void activeRecordingIsNeverSkipped() {
        assertFalse(BridgeAgent.isEmptyStopped(
                new ArchiveJournalSource.Recording(10, 0, AeronArchive.NULL_POSITION)));
    }
}

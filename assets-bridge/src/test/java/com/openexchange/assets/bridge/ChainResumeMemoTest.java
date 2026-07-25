// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.Recording;
import io.aeron.archive.client.AeronArchive;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * The resume memo may only ever skip recordings that a previous epoch PROVED empty at the
 * SAME sync point. Every test here is a case where skipping would lose a settlement.
 */
public class ChainResumeMemoTest {

    private static final long W = 779_974_336L;
    private static final long T = 3_512_700L;

    private static Recording stopped(final long id, final long start, final long stop) {
        return new Recording(id, start, stop);
    }

    private static Recording active(final long id, final long start) {
        return new Recording(id, start, AeronArchive.NULL_POSITION);
    }

    private static List<Recording> chain() {
        return List.of(stopped(0, 0, 64), stopped(1, 0, 64), stopped(2, 0, 320), active(3, 0));
    }

    private final ChainResumeMemo memo = new ChainResumeMemo();

    @Test
    public void walksTheWholeChainWhenNothingIsRemembered() {
        assertEquals(0, memo.resumeIndex(chain(), W, T));
    }

    @Test
    public void resumesPastTheDrainedPrefixAtTheSameSyncPoint() {
        // Epoch 1: drains recordings 0 and 1, then dies on 2 (no image: /dev/shm full).
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // Epoch 2 syncs to the same position: the head is known-empty, start at 2.
        assertEquals(2, memo.resumeIndex(chain(), W, T));
    }

    @Test
    public void forgetsEverythingWhenTheSyncPointMoved() {
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // The AE applied settlements: the filter is now a different function of the bytes,
        // so the "these recordings hold nothing" proof no longer holds.
        assertEquals(0, memo.resumeIndex(chain(), W + 4096, T + 12));
    }

    @Test
    public void keepsThePrefixProvenBeforeAForwardButStopsExtending() {
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, true);  // recording 1 drained AFTER a forward: not proven
        memo.noteDrained(chain(), 2, true);  // sealed: nothing more is recorded either

        // Recording 0 drained with nothing forwarded, so it is still proven empty. Throwing it
        // away would send the next epoch back to the head — the very loop this memo breaks.
        assertEquals(1, memo.resumeIndex(chain(), W, T));
    }

    @Test
    public void doesNotRememberARecordingDrainedAfterAForward() {
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, true); // forwarded before recording 0 finished draining
        assertEquals(0, memo.resumeIndex(chain(), W, T));
    }

    @Test
    public void doesNotRememberAGapInTheDrainedPrefix() {
        // Recording 0 was NOT fully drained (the epoch died on it); 1 and 2 were. The prefix
        // is broken, so nothing may be skipped — 0 has never been read to its end.
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 1, false);
        memo.noteDrained(chain(), 2, false);
        assertEquals(0, memo.resumeIndex(chain(), W, T));
    }

    @Test
    public void forgetsEverythingWhenTheArchiveWasWipedAndRecordingIdsRestarted() {
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // Same recordingIds, DIFFERENT content: a re-genesised ME re-recorded from 0. Skipping
        // on id alone would silently drop every trade in the new recordings 0 and 1.
        final List<Recording> reborn = List.of(stopped(0, 0, 999), stopped(1, 0, 12), active(2, 0));
        assertEquals(0, memo.resumeIndex(reborn, W, T));
    }

    @Test
    public void forgetsEverythingWhenTheDrainedPrefixWasPurgedFromTheChain() {
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // Retention deleted recordings 0-1; the listing now starts at 2. The remembered prefix
        // is no longer a prefix of the chain, so walk it in full rather than guess.
        final List<Recording> purged = List.of(stopped(2, 0, 320), active(3, 0));
        assertEquals(0, memo.resumeIndex(purged, W, T));
    }

    @Test
    public void doesNotResumePastAChainThatShrankBelowTheRememberedPrefix() {
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);
        assertEquals(0, memo.resumeIndex(List.of(stopped(0, 0, 64)), W, T));
    }

    @Test
    public void keepsResumingAcrossRepeatedFailuresAtTheSameSyncPoint() {
        // The outage shape: each epoch clears one more recording before dying on the next.
        assertEquals(0, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        assertEquals(2, memo.resumeIndex(chain(), W, T));
        memo.noteDrained(chain(), 2, false);

        // Whole stopped prefix cleared: the next epoch goes straight to the ACTIVE recording,
        // which is where the 16 hours of unsettled trades actually are.
        assertEquals(3, memo.resumeIndex(chain(), W, T));
    }
}

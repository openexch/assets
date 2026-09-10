// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.Recording;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.RecordingIncarnation;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.SourceIdentity;
import io.aeron.archive.client.AeronArchive;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Memo safety and liveness across source, catalog and AE history transitions. Only applied
 * prefixes may survive an epoch; monotonic watermarks preserve those proofs.
 */
public class ChainResumeMemoTest {

    private static final SourceIdentity A = new SourceIdentity("localhost:9010", 4010, 4001, "aeron:ipc");
    private static final SourceIdentity B = new SourceIdentity("localhost:9110", 4010, 4001, "aeron:ipc");
    private static final RecordingIncarnation INC = new RecordingIncarnation(1000, 42, 7, 65536, 1408, 4001);

    private static final long W = 779_974_336L;
    private static final long T = 3_512_700L;

    private static Recording stopped(final long id, final long start, final long stop) {
        return new Recording(id, start, stop, INC);
    }

    private static Recording active(final long id, final long start) {
        return new Recording(id, start, AeronArchive.NULL_POSITION, INC);
    }

    private static List<Recording> chain() {
        return List.of(stopped(0, 0, 64), stopped(1, 0, 64), stopped(2, 0, 320), active(3, 0));
    }

    private final ChainResumeMemo memo = new ChainResumeMemo();

    @Test
    public void walksTheWholeChainWhenNothingIsRemembered() {
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
    }

    @Test
    public void resumesPastTheDrainedPrefixAtTheSameSyncPoint() {
        // Epoch 1: drains recordings 0 and 1, then dies on 2 (no image: /dev/shm full).
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // Epoch 2 syncs to the same position: the head is known-empty, start at 2.
        assertEquals(2, memo.resumeIndex(A, chain(), W, T));
    }

    @Test
    public void retainsProvenPrefixWhenBothWatermarksAdvance() {
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // Both SKIP predicates remain true when AE watermarks advance monotonically.
        assertEquals(2, memo.resumeIndex(A, chain(), W + 4096, T + 12));
    }

    @Test
    public void keepsThePrefixProvenBeforeAForwardButStopsExtending() {
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, true);  // recording 1 drained AFTER a forward: not proven
        memo.noteDrained(chain(), 2, true);  // sealed: nothing more is recorded either

        // Recording 0 drained with nothing forwarded, so it is still proven empty. Throwing it
        // away would send the next epoch back to the head — the very loop this memo breaks.
        assertEquals(1, memo.resumeIndex(A, chain(), W, T));
    }

    @Test
    public void doesNotRememberARecordingDrainedAfterAForward() {
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, true); // forwarded before recording 0 finished draining
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
    }

    @Test
    public void doesNotRememberAGapInTheDrainedPrefix() {
        // Recording 0 was NOT fully drained (the epoch died on it); 1 and 2 were. The prefix
        // is broken, so nothing may be skipped — 0 has never been read to its end.
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 1, false);
        memo.noteDrained(chain(), 2, false);
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
    }

    @Test
    public void forgetsEverythingWhenTheArchiveWasWipedAndRecordingIdsRestarted() {
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // Same recordingIds, DIFFERENT content: a re-genesised ME re-recorded from 0. Skipping
        // on id alone would silently drop every trade in the new recordings 0 and 1.
        final List<Recording> reborn = List.of(stopped(0, 0, 999), stopped(1, 0, 12), active(2, 0));
        assertEquals(0, memo.resumeIndex(A, reborn, W, T));
    }

    @Test
    public void forgetsEverythingWhenTheDrainedPrefixWasPurgedFromTheChain() {
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        // Retention deleted recordings 0-1; the listing now starts at 2. The remembered prefix
        // is no longer a prefix of the chain, so walk it in full rather than guess.
        final List<Recording> purged = List.of(stopped(2, 0, 320), active(3, 0));
        assertEquals(0, memo.resumeIndex(A, purged, W, T));
    }

    @Test
    public void doesNotResumePastAChainThatShrankBelowTheRememberedPrefix() {
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);
        assertEquals(0, memo.resumeIndex(A, List.of(stopped(0, 0, 64)), W, T));
    }

    @Test
    public void keepsResumingAcrossRepeatedFailuresAtTheSameSyncPoint() {
        // The outage shape: each epoch clears one more recording before dying on the next.
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 0, false);
        memo.noteDrained(chain(), 1, false);

        assertEquals(2, memo.resumeIndex(A, chain(), W, T));
        memo.noteDrained(chain(), 2, false);

        // Whole stopped prefix cleared: the next epoch goes straight to the ACTIVE recording,
        // which is where the 16 hours of unsettled trades actually are.
        assertEquals(3, memo.resumeIndex(A, chain(), W, T));
    }

    // --- within-recording byte resume (the 2026-09-02 stall: re-scanning an outage-inflated
    // recording's drained head every epoch outran the AE's 10s session timeout) ---

    @Test
    public void replayStartsAtTheRecordingStartWhenNoHeadHasBeenSkipped() {
        assertEquals(0L, memo.replayStartPosition(stopped(2, 0, 320)));
        assertEquals(128L, memo.replayStartPosition(stopped(5, 128, 640)));
    }

    @Test
    public void replayResumesPastTheSkippedHead() {
        final Recording r = stopped(2, 0, 320);
        memo.resumeIndex(A, chain(), W, T);
        memo.noteSkipHighWater(r, 256);
        assertEquals(256L, memo.replayStartPosition(r));
    }

    @Test
    public void theSkippedHeadHighWaterOnlyEverAdvances() {
        final Recording r = stopped(2, 0, 320);
        memo.resumeIndex(A, chain(), W, T);
        memo.noteSkipHighWater(r, 256);
        memo.noteSkipHighWater(r, 128); // a later epoch that got LESS far must not rewind the mark
        assertEquals(256L, memo.replayStartPosition(r));
    }

    @Test
    public void theSkippedHeadSurvivesASyncPointMoveForARecordingStillInTheChain() {
        // Both applied-prefix proofs stay valid while both AE watermarks only advance.
        final Recording r = stopped(2, 0, 320);
        memo.resumeIndex(A, chain(), W, T);
        memo.noteSkipHighWater(r, 256);
        memo.resumeIndex(A, chain(), W + 4096, T + 12); // sync moved; recording 2 still present
        assertEquals(256L, memo.replayStartPosition(r));
    }

    @Test
    public void theSkippedHeadIsForgottenWhenTheRecordingLeavesTheChain() {
        final Recording r = stopped(2, 0, 320);
        memo.resumeIndex(A, chain(), W, T);
        memo.noteSkipHighWater(r, 256);
        // Retention purged recordings up to 2; the listing no longer holds this exact recording.
        memo.resumeIndex(A, List.of(active(3, 0)), W, T);
        assertEquals(0L, memo.replayStartPosition(r)); // pruned -> back to the recording start
    }

    @Test
    public void aWipedRecordingWithTheSameIdDoesNotReuseAStaleSkippedHead() {
        final Recording old = stopped(2, 0, 320);
        memo.resumeIndex(A, chain(), W, T);
        memo.noteSkipHighWater(old, 256);
        // A re-genesised archive re-recorded id 2 with different bytes (different start/stop): the
        // key differs, so the stale 256 must not be reused — that would skip real, unapplied trades.
        memo.resumeIndex(A, List.of(stopped(2, 0, 999)), W, T);
        assertEquals(0L, memo.replayStartPosition(stopped(2, 0, 999)));
    }

    private void rememberBoth() {
        memo.resumeIndex(A, chain(), W, T);
        memo.noteDrained(chain(), 0, false);
        memo.noteSkipHighWater(chain().get(3), 160);
    }

    @Test
    public void differentSourceWithIdenticalDescriptorsInvalidatesBothAndNeverResurrectsA() {
        rememberBoth();
        assertEquals(0, memo.resumeIndex(B, chain(), W, T));
        assertEquals(0, memo.replayStartPosition(chain().get(3)));
        assertTrue(memo.invalidationReason().contains("source changed"));
        memo.noteDrained(chain(), 0, false);
        memo.noteSkipHighWater(chain().get(3), 96);
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        assertEquals(0, memo.replayStartPosition(chain().get(3)));
        assertEquals(0, memo.resumeIndex(B, chain(), W, T));
        assertEquals(0, memo.replayStartPosition(chain().get(3)));
    }

    @Test
    public void reusedIdAndIdenticalExtentsWithChangedIncarnationInvalidatesBoth() {
        for (RecordingIncarnation next : List.of(
                new RecordingIncarnation(1001, 42, 7, 65536, 1408, 4001),
                new RecordingIncarnation(1000, 43, 7, 65536, 1408, 4001),
                new RecordingIncarnation(1000, 42, 8, 65536, 1408, 4001))) {
            rememberBoth();
            final List<Recording> reborn = List.of(chain().get(0), chain().get(1), chain().get(2),
                    new Recording(3, 0, -1, next));
            assertEquals(0, memo.resumeIndex(A, reborn, W, T));
            assertEquals(0, memo.replayStartPosition(reborn.get(3)));
            assertTrue(memo.invalidationReason().contains("recording history changed"));
            // Old descriptors reappearing must not restore old marks.
            assertEquals(0, memo.resumeIndex(A, chain(), W, T));
            assertEquals(0, memo.replayStartPosition(chain().get(3)));
        }
    }

    @Test
    public void eitherWatermarkRegressingDropsBothEvenIfTheOtherAdvances() {
        for (long[] watermark : List.of(new long[]{W - 1, T + 1}, new long[]{W + 1, T - 1}, new long[]{0, 0})) {
            rememberBoth();
            assertEquals(0, memo.resumeIndex(A, chain(), watermark[0], watermark[1]));
            assertEquals(0, memo.replayStartPosition(chain().get(3)));
            assertTrue(memo.invalidationReason().contains("watermark regressed"));
        }
    }

    @Test
    public void emptyChainOnAnotherSourceAlsoInvalidatesBoth() {
        rememberBoth();
        assertEquals(0, memo.resumeIndex(B, List.of(), W, T));
        assertEquals(0, memo.resumeIndex(A, chain(), W, T));
        assertEquals(0, memo.replayStartPosition(chain().get(3)));
    }

    @Test
    public void legacyRecordingWithoutIncarnationCannotEnableMemo() {
        final List<Recording> legacy = List.of(new Recording(0, 0, 64), new Recording(1, 0, -1));
        memo.resumeIndex(A, legacy, W, T);
        memo.noteDrained(legacy, 0, false);
        memo.noteSkipHighWater(legacy.get(1), 64);
        assertEquals(0, memo.resumeIndex(A, legacy, W, T));
        assertEquals(0, memo.replayStartPosition(legacy.get(1)));
    }

    @Test
    public void activeToStoppedPreservesByteMarkButNeverTreatsActiveFrontierAsDrained() {
        final Recording active = active(0, 0);
        memo.resumeIndex(A, List.of(active), W, T);
        memo.noteSkipHighWater(active, 160);
        memo.noteDrained(List.of(active), 0, false);
        assertEquals(0, memo.resumeIndex(A, List.of(active), W, T));
        assertEquals(160, memo.replayStartPosition(active));
        final Recording stopped = stopped(0, 0, 160);
        assertEquals(0, memo.resumeIndex(A, List.of(stopped), W, T));
        assertEquals(160, memo.replayStartPosition(stopped));
        memo.noteDrained(List.of(stopped), 0, false);
        assertEquals(1, memo.resumeIndex(A, List.of(stopped, active(1, 0)), W, T));
    }

    @Test
    public void stopBeforeRememberedActiveFrontierInvalidatesBoth() {
        rememberBoth();
        final List<Recording> truncated = List.of(chain().get(0), chain().get(1), chain().get(2), stopped(3, 0, 96));
        assertEquals(0, memo.resumeIndex(A, truncated, W, T));
        assertEquals(0, memo.replayStartPosition(truncated.get(3)));
    }

    @Test
    public void emptyThenFullySkippedStoppedChainCanBeExhaustedAndGainSuccessor() {
        final List<Recording> stoppedChain = List.of(stopped(0, 0, 0), stopped(1, 0, 160));
        memo.resumeIndex(A, stoppedChain, W, T);
        memo.noteDrained(stoppedChain, 0, false);
        memo.noteSkipHighWater(stoppedChain.get(1), 160);
        memo.noteDrained(stoppedChain, 1, false);
        for (int epoch = 0; epoch < 5; epoch++) {
            assertEquals(2, memo.resumeIndex(A, stoppedChain, W, T));
        }
        assertEquals(2, memo.resumeIndex(A, List.of(stoppedChain.get(0), stoppedChain.get(1), active(2, 0)), W, T));
    }

    @Test
    public void forwardSealsBytePrefixBeforeUnacknowledgedDuplicateCanSkip() {
        final Recording r = active(0, 0);
        memo.resumeIndex(A, List.of(r), 100, 1);
        final BridgeFilter filter = new BridgeFilter(100, 1, true);
        assertEquals(BridgeFilter.Action.SKIP, filter.onTrade(100, 1));
        memo.noteSkipHighWater(r, 160);
        assertEquals(BridgeFilter.Action.FORWARD, filter.onTrade(200, 2));
        memo.seal();
        assertEquals(BridgeFilter.Action.SKIP, filter.onTrade(200, 2));
        memo.noteSkipHighWater(r, 480);
        memo.resumeIndex(A, List.of(r), 100, 1); // AE never applied the offered trade
        assertEquals(160, memo.replayStartPosition(r));
        assertEquals(BridgeFilter.Action.FORWARD, new BridgeFilter(100, 1, true).onTrade(200, 2));
    }

    @Test
    public void terminalOnlyAndTradeOnlyWatermarkProgressKeepBothProofs() {
        rememberBoth();
        assertEquals(1, memo.resumeIndex(A, chain(), W + 1, T));
        assertEquals(160, memo.replayStartPosition(chain().get(3)));
        assertEquals(1, memo.resumeIndex(A, chain(), W + 1, T + 1));
        assertEquals(160, memo.replayStartPosition(chain().get(3)));
    }

    @Test
    public void malformedBoundsRemainVisibleEvenIfTheRecordingLooksEmpty() {
        assertThrows(IllegalArgumentException.class,
                () -> memo.resumeIndex(A, List.of(stopped(0, -2, -2)), W, T));
        assertThrows(IllegalArgumentException.class,
                () -> memo.resumeIndex(A, List.of(stopped(0, 160, 96)), W, T));
    }
}

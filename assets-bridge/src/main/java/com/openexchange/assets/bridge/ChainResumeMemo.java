// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * How far up the journal recording chain an epoch may skip — a LIVENESS aid, never a
 * correctness input. Kept PURE (no Aeron, no I/O) so every boundary case is unit-testable,
 * for the same reason {@link BridgeFilter} is.
 *
 * Every epoch re-reads the recording chain from its head. That is correct (the filter skips
 * what the AE already has) but not free: each recording costs a replay image of 3x the
 * journal's term length in the media driver, and a failure part-way down the chain restarts
 * the walk at the head — so a DETERMINISTIC mid-chain failure loops forever without ever
 * reaching the tail where the new trades are. That is the 2026-07-25 outage: /dev/shm had no
 * room for a 192MB replay image, and the bridge re-read the head of the chain for 16 hours
 * while settlement stood still and holds piled up.
 *
 * The rule that makes skipping safe: when an epoch fully drains a STOPPED recording and has
 * forwarded NOTHING up to that point, that recording provably holds nothing for this sync
 * point. A stopped recording's bytes never change, and the filter is a pure function of
 * (consumePosition, lastAppliedTradeId) plus those bytes — so an epoch that syncs to the SAME
 * position must reach the same all-skip verdict. The memo is dropped when:
 *  - the sync point moved (the AE applied something, so the filter is a different function);
 *  - the drained prefix is no longer present in the listing byte-for-byte — same recordingIds
 *    AND the same start/stop positions. That last check is what distinguishes "the recordings
 *    we already read" from "an archive that was wiped and restarted its recordingIds at 0".
 *
 * A forward SEALS the memo rather than clearing it: once the epoch has sent something, later
 * recordings are no longer proven empty, but the prefix drained BEFORE that forward still is,
 * and throwing it away would send the next epoch back to recording 0 — which is the loop this
 * class exists to break.
 *
 * Should this ever be wrong anyway, the dense-tradeId gap detector HALTs the bridge — the
 * same guardrail that already covers every other skip decision.
 */
final class ChainResumeMemo {

    private final List<ArchiveJournalSource.Recording> drainedPrefix = new ArrayList<>();
    private long consumePosition;
    private long lastAppliedTradeId;
    /** Set once this epoch forwards or breaks the prefix: stop extending, keep what is proven. */
    private boolean sealed;

    /**
     * Per-recording byte position past which every entry was SKIPPED — a within-recording twin of
     * {@link #drainedPrefix} for the ONE recording that is only partially drained (the prefix skips
     * whole recordings; this skips the drained head of the next one). Lets the next epoch openReplay
     * from here instead of re-reading and re-skipping that head. The head-rescan of an
     * outage-inflated recording is exactly what raced — and lost to — the AE's 10s session timeout
     * in the 2026-09-02 stall: the scan never reached the tail before the session died, so nothing
     * new was ever forwarded.
     *
     * <p>Safe because it is only ever advanced by a SKIP, and an entry is SKIPPED precisely when its
     * egressSeq is at or below the AE's consumePosition — i.e. the AE has already applied it. So a
     * replay resumed here re-sends only entries the AE has NOT applied; the inclusive boundary is
     * absorbed by the AE's idempotency, and a dense-tradeId gap still HALTs. Monotonic (max only),
     * so a later epoch never rewinds.</p>
     *
     * <p>Keyed by the whole {@link ArchiveJournalSource.Recording} (id + start + stop), not just the
     * id: a head-purged or wiped-and-restarted archive presents a different start/stop, so its stale
     * high-water simply never matches and the epoch falls back to a full scan. {@link #resumeIndex}
     * prunes entries no longer in the chain, bounding the map to the live chain.</p>
     */
    private final Map<ArchiveJournalSource.Recording, Long> skipHighWater = new HashMap<>();

    /**
     * The index in {@code chain} at which this epoch may start walking: past the prefix an
     * earlier epoch already drained at this very sync point, or 0 when anything differs.
     * Re-arms the memo for the given sync point as a side effect.
     */
    int resumeIndex(final List<ArchiveJournalSource.Recording> chain,
                    final long syncConsumePosition, final long syncLastAppliedTradeId) {
        sealed = false;
        // Drop byte-resume marks for recordings no longer in the chain (head-purged, rolled, or a
        // wiped archive whose ids restarted): they can never match again, and this bounds the map
        // to the live chain. Kept across sync-point moves because a SKIP-derived mark stays valid
        // as consumePosition only advances — unlike the drainedPrefix, whose empty-drain proof is
        // specific to one sync point.
        skipHighWater.keySet().retainAll(new HashSet<>(chain));
        final boolean resumable = consumePosition == syncConsumePosition
                && lastAppliedTradeId == syncLastAppliedTradeId
                && !drainedPrefix.isEmpty()
                && chain.size() >= drainedPrefix.size()
                && chain.subList(0, drainedPrefix.size()).equals(drainedPrefix);
        if (resumable) {
            return drainedPrefix.size();
        }
        drainedPrefix.clear();
        consumePosition = syncConsumePosition;
        lastAppliedTradeId = syncLastAppliedTradeId;
        return 0;
    }

    /**
     * Where the next epoch should open {@code recording}'s replay: past the head this or an earlier
     * epoch already skipped, or the recording's start when nothing is proven for it. Always at or
     * before the first not-yet-applied entry (see {@link #skipHighWater}).
     */
    long replayStartPosition(final ArchiveJournalSource.Recording recording) {
        final Long mark = skipHighWater.get(recording);
        return mark == null ? recording.startPosition() : mark;
    }

    /**
     * Record that {@code recording} was skipped up to {@code position} (an Aeron fragment boundary).
     * Monotonic: a lower position never overwrites a higher one, so a failed epoch that got further
     * than a later one still wins. Call with a SKIP boundary only — never a forwarded entry.
     */
    void noteSkipHighWater(final ArchiveJournalSource.Recording recording, final long position) {
        skipHighWater.merge(recording, position, Math::max);
    }

    /**
     * {@code chain[index]} drained to its end. Extends the memo only while the epoch has
     * forwarded nothing and the drained recordings still form an unbroken prefix; otherwise
     * the memo is sealed at what it has already proven.
     */
    void noteDrained(final List<ArchiveJournalSource.Recording> chain, final int index,
                     final boolean forwardedThisEpoch) {
        if (sealed) {
            return;
        }
        if (forwardedThisEpoch || drainedPrefix.size() != index) {
            sealed = true;
            return;
        }
        drainedPrefix.add(chain.get(index));
    }
}

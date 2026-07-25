// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource;

import java.util.ArrayList;
import java.util.List;

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
     * The index in {@code chain} at which this epoch may start walking: past the prefix an
     * earlier epoch already drained at this very sync point, or 0 when anything differs.
     * Re-arms the memo for the given sync point as a side effect.
     */
    int resumeIndex(final List<ArchiveJournalSource.Recording> chain,
                    final long syncConsumePosition, final long syncLastAppliedTradeId) {
        sealed = false;
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

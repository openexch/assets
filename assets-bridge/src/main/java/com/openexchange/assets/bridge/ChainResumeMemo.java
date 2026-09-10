// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.Recording;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.RecordingIncarnation;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.SourceIdentity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Disposable replay acceleration, never an AE checkpoint. Only a recording's contiguous prefix
 * of SKIPs proven against the epoch's initial AE sync point is remembered. Seal BEFORE a forward
 * (including a staged/unacknowledged trade): a later duplicate can SKIP without reaching the AE.
 * A later recording can prove its own applied byte prefix even after an inclusive terminal in an
 * earlier recording was forwarded; only the whole-chain drained-prefix proof stays sealed.
 *
 * Both proofs belong to one source and catalog history. A source switch, changed/missing catalog
 * history or regression of EITHER AE watermark drops both. Monotonic AE progress preserves them:
 * previously applied trades and terminals strictly below the old watermark remain skippable.
 * Catalog incarnation is required; the legacy three-field Recording API cannot establish it.
 *
 * Active -> stopped preserves byte marks, but a stopped extent may never change. An active
 * frontier is not EOF. This avoids rescanning a large applied head after each failed epoch while
 * still re-reading every unproven event. The AE filter/idempotency and dense trade gap guard remain
 * authoritative. Catalog metadata cannot detect offline byte edits that preserve all metadata.
 */
final class ChainResumeMemo {
    private record RecordingKey(long id, long start, RecordingIncarnation incarnation) {
        static RecordingKey of(final Recording recording) {
            return new RecordingKey(recording.recordingId(), recording.startPosition(), recording.incarnation());
        }
    }

    private final List<Recording> drainedPrefix = new ArrayList<>();
    private final Map<RecordingKey, Long> skipHighWater = new HashMap<>();
    private SourceIdentity source;
    private List<Recording> previousChain = List.of();
    private long consumePosition;
    private long lastAppliedTradeId;
    private boolean initialized;
    private boolean prefixSealed;
    private boolean bytePrefixSealed;
    private boolean identityKnown;
    private String invalidationReason;

    int resumeIndex(final SourceIdentity currentSource, final List<Recording> chain,
                    final long syncConsumePosition, final long syncLastAppliedTradeId) {
        Objects.requireNonNull(currentSource, "source identity");
        invalidationReason = null;
        for (final Recording recording : chain) {
            if (recording.startPosition() < 0
                    || (!recording.isActive() && recording.stopPosition() < recording.startPosition())) {
                throw new IllegalArgumentException("invalid recording bounds: source=" + currentSource
                        + " recording=" + recording);
            }
        }
        identityKnown = chain.stream().allMatch(r -> r.incarnation() != null);
        if (initialized && !currentSource.equals(source)) {
            invalidate("source changed: old=" + source + " new=" + currentSource);
        } else if (initialized && (syncConsumePosition < consumePosition
                || syncLastAppliedTradeId < lastAppliedTradeId)) {
            invalidate("AE watermark regressed: consume=" + consumePosition + "->" + syncConsumePosition
                    + " tradeId=" + lastAppliedTradeId + "->" + syncLastAppliedTradeId);
        } else if (!identityKnown) {
            invalidate("recording incarnation unavailable; replay memo disabled");
        } else if (initialized) {
            if (chain.size() < previousChain.size()) {
                invalidate("recording chain shrank: " + previousChain.size() + "->" + chain.size());
            } else {
                for (int i = 0; i < previousChain.size(); i++) {
                    final Recording old = previousChain.get(i);
                    final Recording now = chain.get(i);
                    if (!RecordingKey.of(old).equals(RecordingKey.of(now))
                            || (!old.isActive() && old.stopPosition() != now.stopPosition())
                            || (!now.isActive() && replayStartPosition(now) > now.stopPosition())) {
                        invalidate("recording history changed: old=" + old + " new=" + now);
                        break;
                    }
                }
            }
        }
        source = currentSource;
        previousChain = List.copyOf(chain);
        consumePosition = syncConsumePosition;
        lastAppliedTradeId = syncLastAppliedTradeId;
        initialized = true;
        prefixSealed = false;
        bytePrefixSealed = false;
        return drainedPrefix.size();
    }

    /** Narrow diagnostic, containing transport/catalog identity and watermarks only. */
    String invalidationReason() {
        return invalidationReason;
    }

    void invalidate(final String reason) {
        drainedPrefix.clear();
        skipHighWater.clear();
        invalidationReason = reason;
    }

    long replayStartPosition(final Recording recording) {
        return skipHighWater.getOrDefault(RecordingKey.of(recording), recording.startPosition());
    }

    /** A different recording has an independent byte prefix, but cannot reopen the chain prefix. */
    void beginRecording() {
        bytePrefixSealed = false;
    }

    /** A filter SKIP can refer to an unacknowledged trade forwarded earlier in this epoch. */
    void noteSkippedTrade(final Recording recording, final long position, final long tradeId) {
        if (tradeId > lastAppliedTradeId) {
            seal();
            return;
        }
        noteSkipHighWater(recording, position);
    }

    /** Only call for a SKIP proven applied at sync, at a complete Aeron frame boundary. */
    void noteSkipHighWater(final Recording recording, final long position) {
        if (bytePrefixSealed || !identityKnown) {
            return;
        }
        if (position < recording.startPosition() || (position & 31) != 0
                || (!recording.isActive() && position > recording.stopPosition())) {
            throw new IllegalArgumentException("invalid skipped prefix position=" + position + " recording=" + recording);
        }
        skipHighWater.merge(RecordingKey.of(recording), position, Math::max);
    }

    /** A non-SKIP ends this recording's byte proof and the epoch's whole-chain prefix proof. */
    void seal() {
        prefixSealed = true;
        bytePrefixSealed = true;
    }

    void noteDrained(final List<Recording> chain, final int index, final boolean forwardedThisEpoch) {
        if (prefixSealed || !identityKnown) {
            return;
        }
        final Recording recording = chain.get(index);
        if (forwardedThisEpoch || drainedPrefix.size() != index || recording.isActive()) {
            seal();
            return;
        }
        drainedPrefix.add(recording);
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.domain.FeedHaltReason;

/**
 * RC2 S7 (contract {@code RC2-me-link/S7/contract.md} section 2.1): the core's replicated view of the ME feed under
 * behaviour v3. {@code lastAppliedJournalSeq} is THE cursor of the new path: no entry applies unless its journalSeq is
 * exactly the next one, so a lost entry can never be applied over (the G29 class). One latch slot records why the
 * feed is halted. Every field above the counters is replicated and snapshotted (layout section 10, 92 bytes, written
 * iff v3); under v1/v2 every field stays 0.
 *
 * <p>The counters are node-local diagnostics (not replicated, not snapshotted), like {@code OrderEngine}'s.</p>
 */
public final class FeedState {

    /** Section 10 head length (contract section 14): the fixed fields. */
    public static final int SNAPSHOT_BYTES = 92;
    /** Section 10 layout v2 (contract 3.8): the head, {@code freshDigest i64}, {@code digestCount u32}, then the digests. */
    public static final int DIGEST_HEAD_BYTES = SNAPSHOT_BYTES + 8 + 4;
    /**
     * Contract 3.8 (the duplicate belt): the digests of the last {@code DIGEST_WINDOW} applied entries are kept, so a
     * DUPLICATE inside the window must byte-match what was applied. Older duplicates cannot be compared and are counted.
     */
    public static final int DIGEST_WINDOW = 1 << 16;
    private static final int DIGEST_MIN_CAPACITY = 1 << 10;

    // ---- replicated (section 10) ----
    long boundMeEpoch;
    long lastAppliedJournalSeq;
    FeedHaltReason latchReason = FeedHaltReason.NONE;
    long latchExpectedSeq;
    long latchSeenSeq;
    long latchSkipSeq;
    long latchTradeId;
    long latchMeEpochSeen;
    long latchBridgeIdentity;
    long latchLogPosition;
    long meConfigVersion;
    long meBehaviorVersion;
    long meLedgerCapacity;
    int meMaxWireVersion;
    /** The digest of the FRESH record the epoch was bound on (journalSeq 1), kept for the life of the epoch; 0 = unbound. */
    long freshDigest;
    /** Digest ring indexed by {@code journalSeq & (length - 1)}; holds journalSeq {@code (c - min(c, W), c]}. 0 = unknown. */
    private long[] digests = new long[0];

    // ---- node-local counters (section 2.1) ----
    /** Per template: 61, 62, 63, 64, 67, 68. */
    final long[] duplicateCount = new long[6];
    long gapCount;
    long epochMismatchCount;
    long unboundCount;
    long settleRefusedCount;
    long sourceSchemaCount;
    long egressRegressionCount;
    long haltAppliedCount;
    long haltStaleCount;
    long haltDuplicateCount;
    long latchAutoClearCount;
    long latchClearCount;
    long latchSuppressedCount;
    long skippedEntryRedeliveredCount;
    /** Node-local, metrics only (D3b row 5, review 2 FIX-G): the journalSeq the last 76 skip consumed. */
    long lastSkippedJournalSeq;
    long unservedReleaseCount;
    long tickEpochDropCount;
    long tickUnboundCount;
    long epochRevokedCount;
    long duplicateMismatchCount;
    long duplicateUnverifiedCount;

    public long boundMeEpoch() { return boundMeEpoch; }
    public long lastAppliedJournalSeq() { return lastAppliedJournalSeq; }
    public FeedHaltReason latchReason() { return latchReason; }
    public boolean latched() { return latchReason != FeedHaltReason.NONE; }
    public long latchExpectedSeq() { return latchExpectedSeq; }
    public long latchSeenSeq() { return latchSeenSeq; }
    public long latchSkipSeq() { return latchSkipSeq; }
    public long latchTradeId() { return latchTradeId; }
    public long latchMeEpochSeen() { return latchMeEpochSeen; }
    public long latchBridgeIdentity() { return latchBridgeIdentity; }
    public long latchLogPosition() { return latchLogPosition; }
    public long meConfigVersion() { return meConfigVersion; }
    public long meBehaviorVersion() { return meBehaviorVersion; }
    public long meLedgerCapacity() { return meLedgerCapacity; }
    public int meMaxWireVersion() { return meMaxWireVersion; }
    public long freshDigest() { return freshDigest; }

    /** How many digests the snapshot carries: {@code min(cursor, DIGEST_WINDOW)}. */
    public int digestCount() {
        return (int) Math.min(lastAppliedJournalSeq, DIGEST_WINDOW);
    }

    /** True when the digest of applied entry {@code s} is still held (inside the window). */
    public boolean digestHeld(final long s) {
        return s >= 1 && s <= lastAppliedJournalSeq && s > lastAppliedJournalSeq - digestCount();
    }

    /** The digest recorded for {@code s} (0 = the entry was skipped by a 76, content unknown). Only for {@link #digestHeld}. */
    public long digestAt(final long s) {
        if (!digestHeld(s)) {
            throw new IllegalArgumentException("journalSeq " + s + " is outside the digest window");
        }
        return digests[(int) (s & (digests.length - 1))];
    }

    /** Record the digest of the entry the cursor just moved to ({@code s == cursor}); 0 for a 76 skip. */
    public void recordDigest(final long s, final long digest) {
        if (s != lastAppliedJournalSeq) {
            throw new IllegalStateException("digest for " + s + " recorded with the cursor at " + lastAppliedJournalSeq);
        }
        ensureCapacity(s);
        digests[(int) (s & (digests.length - 1))] = digest;
        if (s == 1) {
            freshDigest = digest;
        }
    }

    private void ensureCapacity(final long s) {
        final long need = Math.min(s, DIGEST_WINDOW);
        if (digests.length >= need) {
            return;
        }
        int cap = Math.max(DIGEST_MIN_CAPACITY, digests.length);
        while (cap < need) {
            cap <<= 1;
        }
        final long[] next = new long[cap];
        final int kept = digests.length;
        for (long q = Math.max(1, s - kept); q < s && kept > 0; q++) {
            next[(int) (q & (cap - 1))] = digests[(int) (q & (kept - 1))];
        }
        digests = next;
    }

    /** Restore only (section 10 layout v2): the digests of journalSeq {@code (c - count, c]} in order, and the FRESH digest. */
    public void restoreDigests(final long fresh, final long[] window) {
        this.freshDigest = fresh;
        this.digests = new long[0];
        final long c = lastAppliedJournalSeq;
        if (window.length > 0) {
            ensureCapacity(c);
            for (int i = 0; i < window.length; i++) {
                final long q = c - window.length + 1 + i;
                digests[(int) (q & (digests.length - 1))] = window[i];
            }
        }
    }

    public long duplicateMismatchCount() { return duplicateMismatchCount; }
    public long duplicateUnverifiedCount() { return duplicateUnverifiedCount; }
    public void countDuplicateMismatch() { duplicateMismatchCount++; }
    public void countDuplicateUnverified() { duplicateUnverifiedCount++; }

    /** Duplicates counted for {@code templateId} in {61, 62, 63, 64, 67, 68}; 0 for any other id. */
    public long duplicateCount(final int templateId) {
        final int i = duplicateSlot(templateId);
        return i < 0 ? 0L : duplicateCount[i];
    }

    static int duplicateSlot(final int templateId) {
        switch (templateId) {
            case 61: return 0;
            case 62: return 1;
            case 63: return 2;
            case 64: return 3;
            case 67: return 4;
            case 68: return 5;
            default: return -1;
        }
    }

    public long gapCount() { return gapCount; }
    public long epochMismatchCount() { return epochMismatchCount; }
    public long unboundCount() { return unboundCount; }
    public long settleRefusedCount() { return settleRefusedCount; }
    public long sourceSchemaCount() { return sourceSchemaCount; }
    public long egressRegressionCount() { return egressRegressionCount; }
    public long haltAppliedCount() { return haltAppliedCount; }
    public long haltStaleCount() { return haltStaleCount; }
    public long haltDuplicateCount() { return haltDuplicateCount; }
    public long latchAutoClearCount() { return latchAutoClearCount; }
    public long latchClearCount() { return latchClearCount; }
    public long latchSuppressedCount() { return latchSuppressedCount; }
    public long skippedEntryRedeliveredCount() { return skippedEntryRedeliveredCount; }
    public long lastSkippedJournalSeq() { return lastSkippedJournalSeq; }
    public long unservedReleaseCount() { return unservedReleaseCount; }
    public long tickEpochDropCount() { return tickEpochDropCount; }
    public long tickUnboundCount() { return tickUnboundCount; }
    public long epochRevokedCount() { return epochRevokedCount; }

    // ---- mutators (the engine's feed apply is the only caller; every one runs inside one replicated apply) ----

    /** D2 row 1: trust on first use of the FRESH record's epoch. */
    public void bind(final long meEpoch) {
        this.boundMeEpoch = meEpoch;
    }

    /** 3.3 commit, or D3b row 5 skip: the cursor moves by exactly one. */
    public void advanceCursorTo(final long journalSeq) {
        if (journalSeq != lastAppliedJournalSeq + 1) {
            throw new IllegalStateException("cursor " + lastAppliedJournalSeq + " cannot move to " + journalSeq);
        }
        this.lastAppliedJournalSeq = journalSeq;
    }

    /** 4.2: the last applied ME EngineConfig record. */
    public void recordMeConfig(final long configVersion, final long behaviorVersion, final long ledgerCapacity,
                               final int maxWireVersion) {
        this.meConfigVersion = configVersion;
        this.meBehaviorVersion = behaviorVersion;
        this.meLedgerCapacity = ledgerCapacity;
        this.meMaxWireVersion = maxWireVersion;
    }

    /** 5.1: a replacing or first latch zeroes every latch field, then writes its own. */
    public void writeLatch(final FeedHaltReason reason, final long expected, final long seen, final long skip,
                           final long tradeId, final long meEpochSeen, final long bridgeIdentity, final long logPosition) {
        zeroLatch();
        this.latchReason = reason;
        this.latchExpectedSeq = expected;
        this.latchSeenSeq = seen;
        this.latchSkipSeq = skip;
        this.latchTradeId = tradeId;
        this.latchMeEpochSeen = meEpochSeen;
        this.latchBridgeIdentity = bridgeIdentity;
        this.latchLogPosition = logPosition;
    }

    /** 3.3 reach-clear and D3b clears: every latch field back to 0. */
    public void zeroLatch() {
        this.latchReason = FeedHaltReason.NONE;
        this.latchExpectedSeq = 0L;
        this.latchSeenSeq = 0L;
        this.latchSkipSeq = 0L;
        this.latchTradeId = 0L;
        this.latchMeEpochSeen = 0L;
        this.latchBridgeIdentity = 0L;
        this.latchLogPosition = 0L;
    }

    public void countDuplicate(final int templateId) {
        final int i = duplicateSlot(templateId);
        if (i >= 0) {
            duplicateCount[i]++;
        }
    }

    public void countGap() { gapCount++; }
    public void countEpochMismatch() { epochMismatchCount++; }
    public void countUnbound() { unboundCount++; }
    public void countSettleRefused() { settleRefusedCount++; }
    public void countSourceSchema() { sourceSchemaCount++; }
    public void countEgressRegression() { egressRegressionCount++; }
    public void countHaltApplied() { haltAppliedCount++; }
    public void countHaltStale() { haltStaleCount++; }
    public void countHaltDuplicate() { haltDuplicateCount++; }
    public void countLatchAutoClear() { latchAutoClearCount++; }
    public void countLatchClear() { latchClearCount++; }
    public void countLatchSuppressed() { latchSuppressedCount++; }
    public void countSkippedEntryRedelivered() { skippedEntryRedeliveredCount++; }
    public void setLastSkippedJournalSeq(final long journalSeq) { lastSkippedJournalSeq = journalSeq; }
    public void countUnservedRelease() { unservedReleaseCount++; }
    public void countTickEpochDrop() { tickEpochDropCount++; }
    public void countTickUnbound() { tickUnboundCount++; }
    public void countEpochRevoked(final int n) { epochRevokedCount += n; }

    /** Restore only (section 10); the caller verifies the 2.1 invariants afterwards. */
    public void restore(final long boundMeEpoch, final long lastAppliedJournalSeq, final FeedHaltReason latchReason,
                        final long latchExpectedSeq, final long latchSeenSeq, final long latchSkipSeq, final long latchTradeId,
                        final long latchMeEpochSeen, final long latchBridgeIdentity, final long latchLogPosition,
                        final long meConfigVersion, final long meBehaviorVersion, final long meLedgerCapacity,
                        final int meMaxWireVersion) {
        this.boundMeEpoch = boundMeEpoch;
        this.lastAppliedJournalSeq = lastAppliedJournalSeq;
        this.latchReason = latchReason;
        this.latchExpectedSeq = latchExpectedSeq;
        this.latchSeenSeq = latchSeenSeq;
        this.latchSkipSeq = latchSkipSeq;
        this.latchTradeId = latchTradeId;
        this.latchMeEpochSeen = latchMeEpochSeen;
        this.latchBridgeIdentity = latchBridgeIdentity;
        this.latchLogPosition = latchLogPosition;
        this.meConfigVersion = meConfigVersion;
        this.meBehaviorVersion = meBehaviorVersion;
        this.meLedgerCapacity = meLedgerCapacity;
        this.meMaxWireVersion = meMaxWireVersion;
    }

    /**
     * The section 2.1 invariants that hold on the feed state alone, or null. {@code lastAppliedTradeId} comes from the
     * ledger. The gate-dependent invariant ("under v3, unbound implies GLOBAL SUSPENDED") is checked by the engine.
     */
    public String invariantBreach(final long lastAppliedTradeId) {
        // latchSeenSeq and latchMeEpochSeen are diagnostics of a refused frame (D1 row 1 records a raw s < 1): unchecked
        if (boundMeEpoch < 0 || lastAppliedJournalSeq < 0 || latchExpectedSeq < 0 || latchSkipSeq < 0
                || latchTradeId < 0 || meConfigVersion < 0 || meBehaviorVersion < 0 || meLedgerCapacity < 0 || meMaxWireVersion < 0) {
            return "negative feed field";
        }
        if (boundMeEpoch == 0 && (lastAppliedJournalSeq != 0 || meConfigVersion != 0 || meBehaviorVersion != 0
                || meLedgerCapacity != 0 || meMaxWireVersion != 0)) {
            return "unbound feed with cursor " + lastAppliedJournalSeq + " or ME config fields set";
        }
        if (boundMeEpoch == 0 && freshDigest != 0) {
            return "unbound feed with a FRESH digest";
        }
        if (boundMeEpoch != 0 && freshDigest == 0) {
            return "bound feed without the FRESH digest";
        }
        if (boundMeEpoch != 0 && meBehaviorVersion < 2) {
            return "bound epoch " + boundMeEpoch + " with meBehaviorVersion " + meBehaviorVersion;
        }
        final boolean none = latchReason == FeedHaltReason.NONE;
        final boolean fieldsZero = latchExpectedSeq == 0 && latchSeenSeq == 0 && latchSkipSeq == 0 && latchTradeId == 0
                && latchMeEpochSeen == 0 && latchBridgeIdentity == 0 && latchLogPosition == 0;
        if (none != fieldsZero) {
            return "latchReason " + latchReason + " disagrees with the latch fields";
        }
        if (latchSkipSeq != 0 && latchSkipSeq != lastAppliedJournalSeq + 1) {
            return "latchSkipSeq " + latchSkipSeq + " is not cursor + 1 (" + (lastAppliedJournalSeq + 1) + ")";
        }
        if ((latchReason == FeedHaltReason.GAP || latchReason == FeedHaltReason.EPOCH_MISMATCH
                || latchReason == FeedHaltReason.SOURCE_UNBOUND) && latchSkipSeq != 0) {
            return "latch " + latchReason + " names a skip " + latchSkipSeq;
        }
        if (latchTradeId != 0 && (latchSkipSeq == 0 || (latchReason != FeedHaltReason.SOURCE_SCHEMA
                && latchReason != FeedHaltReason.SETTLE_REFUSED))) {
            return "latchTradeId " + latchTradeId + " on latch " + latchReason + " skip " + latchSkipSeq;
        }
        if (latchTradeId != 0 && latchTradeId != lastAppliedTradeId + 1) {
            return "latchTradeId " + latchTradeId + " is not lastAppliedTradeId + 1 (" + (lastAppliedTradeId + 1) + ")";
        }
        return null;
    }
}

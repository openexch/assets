// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.domain.FeedHaltReason;

/**
 * RC2 S7 (contract {@code RC2-me-link/S7/contract.md} section 2.1): the core's replicated view of the ME feed under
 * behaviour v3. {@code lastAppliedJournalSeq} is THE cursor of the new path: no entry applies unless its journalSeq is
 * exactly the next one, so a lost entry can never be applied over (the G29 class). One latch slot records a non-GAP
 * halt; since RC2 S10' 4R a GAP lives in the per-lane halt table (8 records + one overflow record) with the holder set
 * that decides when the BRIDGE_HALT gate bit clears. Every field above the counters is replicated and snapshotted
 * (section 10, 92 bytes, and section 12 for the table, both written iff v3); under v1/v2 every field stays 0.
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
    /** The latch SLOT's reason (non-GAP only since RC2 S10' 4R: GAP lives in the lane-halt table). */
    public FeedHaltReason latchReason() { return latchReason; }
    /** The latch slot is set (SOURCE_SCHEMA, SETTLE_REFUSED, EPOCH_MISMATCH or SOURCE_UNBOUND). */
    public boolean slotLatched() { return latchReason != FeedHaltReason.NONE; }
    /** 4R.1 rule 6: the feed is halted = the slot is set, or a GAP record is open, or the overflow record is set. */
    public boolean latched() { return latchReason != FeedHaltReason.NONE || haltCount != 0 || haltOverflow; }
    /**
     * 4R.1 rule 6: the one {@code haltReason} the report and the latch gauge carry: the highest exit class present. A slot
     * reason is always class 2 or 3, above GAP's class 1; else GAP when a record or the overflow is open; else NONE.
     */
    public FeedHaltReason haltReason() {
        if (latchReason != FeedHaltReason.NONE) {
            return latchReason;
        }
        return haltCount != 0 || haltOverflow ? FeedHaltReason.GAP : FeedHaltReason.NONE;
    }
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

    /** RC2 S10' 4R.1 rule 7: every duplicate over the six feed templates ({@code assets_feed_duplicate_total}). */
    public long duplicateTotal() {
        long n = 0;
        for (final long d : duplicateCount) {
            n += d;
        }
        return n;
    }

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

    // ---- RC2 S10' 4R (contract S10p/contract.md 4R.1): the replicated lane-halt table, snapshot section 12 ----

    /** At most this many open GAP records (one per bridge identity); a further identity goes to the overflow record. */
    public static final int LANE_HALT_CAPACITY = 8;
    /** Holder bits (4R.1 rule 4): the BRIDGE_HALT gate bit is cleared only when no holder is left. */
    public static final int HOLDER_GAP = 1;
    public static final int HOLDER_NONGAP_AWAITING_OPEN = 2;
    public static final int HOLDER_OVERFLOW = 4;
    static final int HOLDER_KNOWN = HOLDER_GAP | HOLDER_NONGAP_AWAITING_OPEN | HOLDER_OVERFLOW;
    /** {@link #recordGapHalt} results. */
    public static final int HALT_DUPLICATE = -1;
    /** The overflow record's slot index in {@link #recordGapHalt}'s result. */
    public static final int HALT_OVERFLOW = LANE_HALT_CAPACITY;
    /**
     * The session marker of a record that two sessions have written (S2-3): from then on every write is a merge
     * ({@code seen = max}), whoever sends it, until the record clears. Never a real cluster session id.
     */
    public static final long SESSION_SHARED = Long.MIN_VALUE;
    /** The identity of a GAP the core detected itself (a feed frame above cursor + 1, D1 row 5 / D2 row 3). */
    public static final long CORE_DETECTED_IDENTITY = 0L;
    /** {@link #reopenAction} results. */
    public static final int REOPEN_NONE = 0;
    public static final int REOPEN_BRIDGE_HALT = 1;
    public static final int REOPEN_ALL = 2;

    // replicated, kept ascending by identity (the snapshot order is the state order)
    private final long[] haltIdentity = new long[LANE_HALT_CAPACITY];
    private final long[] haltExpected = new long[LANE_HALT_CAPACITY];
    private final long[] haltSeen = new long[LANE_HALT_CAPACITY];
    private final long[] haltSession = new long[LANE_HALT_CAPACITY];
    private final long[] haltLogPosition = new long[LANE_HALT_CAPACITY];
    private int haltCount;
    private boolean haltOverflow;
    /** Informational only (NOTE S2-2): the overflow decides nothing by reach; min expected, max seen of what it absorbed. */
    private long overflowExpected;
    private long overflowSeen;
    private int holders;
    /** Set by {@link #releaseNonGapOnReach} under the AUTO_REOPEN_ANY_REASON seam only. */
    private boolean nonGapReleased;
    // node-local
    long gateAutoReopenCount;

    public int laneHaltCount() { return haltCount; }
    public long laneHaltIdentity(final int i) { return haltIdentity[checkIndex(i)]; }
    public long laneHaltExpected(final int i) { return haltExpected[checkIndex(i)]; }
    public long laneHaltSeen(final int i) { return haltSeen[checkIndex(i)]; }
    public long laneHaltSession(final int i) { return haltSession[checkIndex(i)]; }
    public long laneHaltLogPosition(final int i) { return haltLogPosition[checkIndex(i)]; }
    public boolean haltOverflow() { return haltOverflow; }
    public long overflowExpected() { return overflowExpected; }
    public long overflowSeen() { return overflowSeen; }
    /** A GAP record or the overflow record is open (what {@code 76 {GAP, 0}} clears). */
    public boolean gapLatched() { return haltCount != 0 || haltOverflow; }
    /** The holder set {GAP, NONGAP_AWAITING_OPEN, OVERFLOW} as bits ({@code HOLDER_*}). */
    public int holders() { return holders; }
    public boolean holds(final int holder) { return (holders & holder) != 0; }
    public long gateAutoReopenCount() { return gateAutoReopenCount; }
    public void countGateAutoReopen() { gateAutoReopenCount++; }

    /** The index of {@code identity}'s record, or -1. */
    public int laneHaltIndex(final long identity) {
        for (int i = 0; i < haltCount; i++) {
            if (haltIdentity[i] == identity) {
                return i;
            }
        }
        return -1;
    }

    private int checkIndex(final int i) {
        if (i < 0 || i >= haltCount) {
            throw new IndexOutOfBoundsException("lane-halt record " + i + " of " + haltCount);
        }
        return i;
    }

    /**
     * 4R.1 rule 2, RC-2: a non-stale GAP FeedHalt (the caller has refused a stale one) writes or replaces its identity's
     * record. A replace from the record's own session takes the new values (the lane's current view); a replace from
     * ANOTHER session keeps {@code seen = max(old, new)} and {@code expected = min(old, new)}, so a second holder of an
     * identity never weakens the first's reach condition (S2-3). That write also marks the record {@link #SESSION_SHARED}:
     * from then on no session can lower it either, so neither holder weakens the other's (the property test found the
     * case "B's FeedHalt merged into A's record unchanged, then A lowered its own seen below B's hole"). A ninth identity
     * while 8 records are open merges into the overflow record. Nothing is written when the result equals what is held.
     *
     * @return the record's index ({@code 0..7}), {@link #HALT_OVERFLOW}, or {@link #HALT_DUPLICATE}
     */
    public int recordGapHalt(final long identity, final long sessionId, final long expected, final long seen,
                             final long logPosition) {
        final int i = laneHaltIndex(identity);
        if (i >= 0) {
            final long e;
            final long s;
            final long session;
            if (haltSession[i] != sessionId && !FeedSeams.REPLACE_TAKES_LAST_SEEN.on) {
                e = Math.min(haltExpected[i], expected);
                s = Math.max(haltSeen[i], seen);
                session = SESSION_SHARED;
            } else {
                e = expected;
                s = seen;
                session = sessionId;
            }
            if (e == haltExpected[i] && s == haltSeen[i] && session == haltSession[i]) {
                return HALT_DUPLICATE;
            }
            haltExpected[i] = e;
            haltSeen[i] = s;
            haltSession[i] = session;
            haltLogPosition[i] = logPosition;
            holders |= HOLDER_GAP;
            return i;
        }
        if (FeedSeams.AUTO_REOPEN_SINGLE_SLOT.on && haltCount != 0) {
            return HALT_DUPLICATE; // the S7 single slot: the second lane's hole is forgotten
        }
        return insertOrOverflow(identity, sessionId, expected, seen, logPosition);
    }

    /**
     * A GAP the core detected itself (D1 row 5, D2 row 3): the record of {@link #CORE_DETECTED_IDENTITY}. While that record
     * is open a later one MERGES into it ({@code seen = max}, {@code expected = min}, review FIX-1): under the automatic
     * reopen, S7's "suppress the second GAP" would let the record's reach open the gate below a hole the core itself
     * refused a frame at. The CORE_GAP_SUPPRESSED seam restores the S7 suppression.
     *
     * @return the record's index, {@link #HALT_OVERFLOW}, or {@link #HALT_DUPLICATE} when unchanged (or suppressed)
     */
    public int recordCoreGap(final long expected, final long seen, final long logPosition) {
        final int i = laneHaltIndex(CORE_DETECTED_IDENTITY);
        if (i >= 0) {
            if (FeedSeams.CORE_GAP_SUPPRESSED.on) {
                return HALT_DUPLICATE;
            }
            final long e = Math.min(haltExpected[i], expected);
            final long s = Math.max(haltSeen[i], seen);
            if (e == haltExpected[i] && s == haltSeen[i]) {
                return HALT_DUPLICATE;
            }
            haltExpected[i] = e;
            haltSeen[i] = s;
            haltLogPosition[i] = logPosition;
            return i;
        }
        if (FeedSeams.AUTO_REOPEN_SINGLE_SLOT.on && haltCount != 0) {
            return HALT_DUPLICATE;
        }
        return insertOrOverflow(CORE_DETECTED_IDENTITY, 0L, expected, seen, logPosition);
    }

    private int insertOrOverflow(final long identity, final long sessionId, final long expected, final long seen,
                                 final long logPosition) {
        if (haltCount < LANE_HALT_CAPACITY) {
            int at = haltCount;
            while (at > 0 && haltIdentity[at - 1] > identity) {
                haltIdentity[at] = haltIdentity[at - 1];
                haltExpected[at] = haltExpected[at - 1];
                haltSeen[at] = haltSeen[at - 1];
                haltSession[at] = haltSession[at - 1];
                haltLogPosition[at] = haltLogPosition[at - 1];
                at--;
            }
            haltIdentity[at] = identity;
            haltExpected[at] = expected;
            haltSeen[at] = seen;
            haltSession[at] = sessionId;
            haltLogPosition[at] = logPosition;
            haltCount++;
            holders |= HOLDER_GAP;
            return at;
        }
        if (haltOverflow) {
            final long e = Math.min(overflowExpected, expected);
            final long s = Math.max(overflowSeen, seen);
            if (e == overflowExpected && s == overflowSeen) {
                return HALT_DUPLICATE;
            }
            overflowExpected = e;
            overflowSeen = s;
            return HALT_OVERFLOW;
        }
        haltOverflow = true;
        overflowExpected = expected;
        overflowSeen = seen;
        holders |= HOLDER_OVERFLOW;
        return HALT_OVERFLOW;
    }

    /**
     * 4R.1 rule 2, RC-3: every record whose reach condition holds at cursor {@code c} ({@code c >= seen - 1}) is removed;
     * when this empties the table the GAP holder goes. The overflow never clears by reach.
     *
     * @return how many records were removed
     */
    public int reachClearRecords(final long c) {
        if (haltCount == 0) {
            return 0;
        }
        int w = 0;
        for (int i = 0; i < haltCount; i++) {
            if (c >= haltSeen[i] - 1) {
                continue;
            }
            haltIdentity[w] = haltIdentity[i];
            haltExpected[w] = haltExpected[i];
            haltSeen[w] = haltSeen[i];
            haltSession[w] = haltSession[i];
            haltLogPosition[w] = haltLogPosition[i];
            w++;
        }
        final int removed = haltCount - w;
        for (int i = w; i < haltCount; i++) {
            haltIdentity[i] = 0L;
            haltExpected[i] = 0L;
            haltSeen[i] = 0L;
            haltSession[i] = 0L;
            haltLogPosition[i] = 0L;
        }
        haltCount = w;
        if (removed > 0 && w == 0) {
            holders &= ~HOLDER_GAP;
        }
        return removed;
    }

    /** 4R.1 rule 4: a non-GAP slot write adds NONGAP_AWAITING_OPEN; it stays through that latch's reach-clear or 76. */
    public void holdAwaitingOpen() {
        holders |= HOLDER_NONGAP_AWAITING_OPEN;
    }

    /** A non-GAP reach-clear keeps NONGAP_AWAITING_OPEN (the operator's OPEN); the AUTO_REOPEN_ANY_REASON seam drops it. */
    public void releaseNonGapOnReach() {
        if (FeedSeams.AUTO_REOPEN_ANY_REASON.on && (holders & HOLDER_NONGAP_AWAITING_OPEN) != 0) {
            holders &= ~HOLDER_NONGAP_AWAITING_OPEN;
            nonGapReleased = true;
        }
    }

    /**
     * 4R.1 rule 2: {@code 76 {GAP, 0}} clears the whole table and the overflow record and adds NONGAP_AWAITING_OPEN, so the
     * operator's {@code open} is the next step. Returns false (nothing done) when no GAP record and no overflow is open.
     */
    public boolean clearGapTable() {
        if (haltCount == 0 && !haltOverflow) {
            return false;
        }
        for (int i = 0; i < haltCount; i++) {
            haltIdentity[i] = 0L;
            haltExpected[i] = 0L;
            haltSeen[i] = 0L;
            haltSession[i] = 0L;
            haltLogPosition[i] = 0L;
        }
        haltCount = 0;
        haltOverflow = false;
        overflowExpected = 0L;
        overflowSeen = 0L;
        holders = (holders & ~(HOLDER_GAP | HOLDER_OVERFLOW)) | HOLDER_NONGAP_AWAITING_OPEN;
        return true;
    }

    /** 4R.1 rule 4: ADMIN OPEN of GLOBAL ({@code clearAll}) empties the holder set. */
    public void clearHolders() {
        holders = 0;
    }

    /**
     * 4R.1 rules 1 and 4, after a reach-clear in the apply: whether the engine clears the BRIDGE_HALT bit now. The bit is
     * cleared only when a GAP or (seam) non-GAP holder was released in this apply AND the holder set is empty; the seams
     * OVERFLOW_READS_CLEARED and AUTO_REOPEN_OVER_AWAITING_OPEN ignore their holder, AUTO_REOPEN_CLEARS_ALL asks for
     * {@code clearAll}.
     *
     * @param gapReleased the table was emptied by this apply's reach-clear
     * @return {@link #REOPEN_NONE}, {@link #REOPEN_BRIDGE_HALT} or {@link #REOPEN_ALL}
     */
    public int reopenAction(final boolean gapReleased) {
        final boolean released = gapReleased || nonGapReleased;
        nonGapReleased = false;
        if (!released) {
            return REOPEN_NONE;
        }
        int h = holders;
        if (FeedSeams.OVERFLOW_READS_CLEARED.on) {
            h &= ~HOLDER_OVERFLOW;
        }
        if (FeedSeams.AUTO_REOPEN_OVER_AWAITING_OPEN.on) {
            h &= ~HOLDER_NONGAP_AWAITING_OPEN;
        }
        if (h != 0) {
            return REOPEN_NONE;
        }
        return FeedSeams.AUTO_REOPEN_CLEARS_ALL.on ? REOPEN_ALL : REOPEN_BRIDGE_HALT;
    }

    /**
     * 4R.1 rule 5 (restore, contract S2-4 breach checks on the set): a non-empty table implies GAP in the set, an overflow
     * record implies OVERFLOW, a set slot implies NONGAP_AWAITING_OPEN; no record may be at its reach (the apply would
     * have removed it); {@code bridgeHaltBit} is the GLOBAL gate's BRIDGE_HALT bit, set iff the holder set is non-empty.
     * Null when consistent.
     */
    public String haltBreach(final boolean bridgeHaltBit) {
        if ((holders & ~HOLDER_KNOWN) != 0) {
            return "holder set 0x" + Integer.toHexString(holders) + " has a bit this build does not know";
        }
        if (bridgeHaltBit != (holders != 0)) {
            return "BRIDGE_HALT bit " + (bridgeHaltBit ? "set" : "clear") + " with holder set 0x" + Integer.toHexString(holders);
        }
        if (haltCount != 0 && (holders & HOLDER_GAP) == 0) {
            return haltCount + " lane-halt records without GAP in the holder set";
        }
        if (haltOverflow && (holders & HOLDER_OVERFLOW) == 0) {
            return "overflow record without OVERFLOW in the holder set";
        }
        if (latchReason != FeedHaltReason.NONE && (holders & HOLDER_NONGAP_AWAITING_OPEN) == 0) {
            return "latch slot " + latchReason + " without NONGAP_AWAITING_OPEN in the holder set";
        }
        if (latchReason == FeedHaltReason.GAP) {
            return "latch slot GAP (GAP lives in the lane-halt table)";
        }
        for (int i = 0; i < haltCount; i++) {
            if (i > 0 && haltIdentity[i] <= haltIdentity[i - 1]) {
                return "lane-halt identities not strictly ascending at record " + i;
            }
            if (haltExpected[i] < 1 || haltSeen[i] <= haltExpected[i]) {
                return "lane-halt record " + haltIdentity[i] + " {" + haltExpected[i] + ", " + haltSeen[i] + "} breaks 1 <= expected < seen";
            }
            if (lastAppliedJournalSeq >= haltSeen[i] - 1) {
                return "lane-halt record " + haltIdentity[i] + " {" + haltExpected[i] + ", " + haltSeen[i]
                        + "} is at its reach with the cursor at " + lastAppliedJournalSeq;
            }
        }
        if (!haltOverflow && (overflowExpected != 0 || overflowSeen != 0)) {
            return "overflow fields set without the overflow record";
        }
        return null;
    }

    /** Below behaviour 3 the table, the overflow and the holder set are empty. */
    public boolean haltTableEmpty() {
        return haltCount == 0 && !haltOverflow && holders == 0;
    }

    /** Section 12 head and entry sizes. */
    public static final int HALT_SECTION_VERSION = 1;
    public static final int HALT_SECTION_HEAD_BYTES = 24;
    public static final int HALT_SECTION_ENTRY_BYTES = 40;

    /**
     * Section 12 FEED_HALTS payload (4R.1 rule 5), native byte order: {@code version u8 @0} (1), {@code holders u8 @1},
     * {@code overflow u8 @2} (0/1), reserved u8 @3, {@code count u32 @4}, {@code overflowExpected i64 @8},
     * {@code overflowSeen i64 @16}, then {@code count} x 40 B {identity, expected, seen, sessionId, logPosition} ascending by
     * identity. Returns the end position.
     */
    public int writeHaltSection(final org.agrona.MutableDirectBuffer dst, final int at) {
        final int count = FeedSeams.LANE_TABLE_NOT_SNAPSHOTTED.on ? 0 : haltCount;
        dst.putByte(at, (byte) HALT_SECTION_VERSION);
        dst.putByte(at + 1, (byte) holders);
        dst.putByte(at + 2, (byte) (haltOverflow ? 1 : 0));
        dst.putByte(at + 3, (byte) 0);
        dst.putInt(at + 4, count);
        dst.putLong(at + 8, overflowExpected);
        dst.putLong(at + 16, overflowSeen);
        int q = at + HALT_SECTION_HEAD_BYTES;
        for (int i = 0; i < count; i++) {
            dst.putLong(q, haltIdentity[i]);
            dst.putLong(q + 8, haltExpected[i]);
            dst.putLong(q + 16, haltSeen[i]);
            dst.putLong(q + 24, haltSession[i]);
            dst.putLong(q + 32, haltLogPosition[i]);
            q += HALT_SECTION_ENTRY_BYTES;
        }
        return q;
    }

    /** The payload length {@link #writeHaltSection} writes. */
    public int haltSectionLength() {
        return HALT_SECTION_HEAD_BYTES + HALT_SECTION_ENTRY_BYTES * (FeedSeams.LANE_TABLE_NOT_SNAPSHOTTED.on ? 0 : haltCount);
    }

    /**
     * Restore section 12 (fail closed). The version byte is read FIRST: a version this build does not know refuses the
     * restore naming the section id and the version (S2-4). Then the length, the byte ranges and the identity order; the
     * state breaches run in {@link #haltBreach} once the gate is restored.
     */
    public void readHaltSection(final org.agrona.DirectBuffer src, final int at, final int length, final int sectionId) {
        if (length < 1) {
            throw new IllegalStateException("balance snapshot FEED_HALTS section (" + sectionId + ") is empty: no version byte");
        }
        final int version = src.getByte(at) & 0xFF;
        if (version != HALT_SECTION_VERSION && !FeedSeams.UNKNOWN_SECTION_VERSION_ACCEPTED.on) {
            throw new IllegalStateException("balance snapshot section " + sectionId + " (FEED_HALTS) version " + version
                    + " is not known to this build (it reads version " + HALT_SECTION_VERSION + "); restore refused");
        }
        if (length < HALT_SECTION_HEAD_BYTES) {
            throw new IllegalStateException("balance snapshot FEED_HALTS section (" + sectionId + ") is " + length
                    + " bytes, below its " + HALT_SECTION_HEAD_BYTES + "-byte head");
        }
        final int h = src.getByte(at + 1) & 0xFF;
        final int overflow = src.getByte(at + 2) & 0xFF;
        final long count = src.getInt(at + 4) & 0xFFFFFFFFL;
        if ((h & ~HOLDER_KNOWN) != 0 || overflow > 1 || src.getByte(at + 3) != 0) {
            throw new IllegalStateException("balance snapshot FEED_HALTS section (" + sectionId + "): holders 0x"
                    + Integer.toHexString(h) + ", overflow " + overflow + " or a reserved byte this build does not know");
        }
        if (count > LANE_HALT_CAPACITY || length != HALT_SECTION_HEAD_BYTES + HALT_SECTION_ENTRY_BYTES * count) {
            throw new IllegalStateException("balance snapshot FEED_HALTS section (" + sectionId + "): " + count
                    + " records in " + length + " bytes (at most " + LANE_HALT_CAPACITY + ")");
        }
        haltCount = (int) count;
        int q = at + HALT_SECTION_HEAD_BYTES;
        for (int i = 0; i < haltCount; i++) {
            haltIdentity[i] = src.getLong(q);
            haltExpected[i] = src.getLong(q + 8);
            haltSeen[i] = src.getLong(q + 16);
            haltSession[i] = src.getLong(q + 24);
            haltLogPosition[i] = src.getLong(q + 32);
            q += HALT_SECTION_ENTRY_BYTES;
        }
        haltOverflow = overflow == 1;
        overflowExpected = src.getLong(at + 8);
        overflowSeen = src.getLong(at + 16);
        holders = h;
    }

    // ---- RC2 S8 3.10 item 3: the demuxer's bridge lane table, node-local (never replicated, never snapshotted) ----
    long lanesBehind;
    long feedLaneOverflowCount;

    /** BRIDGE lanes whose highest delivered journalSeq is below the cursor (written by the demuxer per feed frame). */
    public long lanesBehind() { return lanesBehind; }
    public void setLanesBehind(final long n) { lanesBehind = n; }
    /** BRIDGE sessions beyond the lane table's capacity (8). */
    public long feedLaneOverflowCount() { return feedLaneOverflowCount; }
    public void countFeedLaneOverflow() { feedLaneOverflowCount++; }

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

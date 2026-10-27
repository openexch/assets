// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.util.EnumSet;
import java.util.Set;

/**
 * RC2 S9h contract 11 preamble: the test-only seams the negative controls switch on. Every seam turns ONE rule of the
 * harness off (or wrong) so the test that proves the rule must fail for the stated reason. Read only by test-scope
 * code (this whole package is test scope, R1); the default set is empty, the CLI has no flag for them, and
 * {@code FeedHarnessMain.run} refuses to run (70) while any seam is on.
 */
public enum FeedHarnessSeams {
    /** Translator: takerOrderId and makerOrderId swapped (two i64 fields). */
    SWAP_TAKER_MAKER_ORDER,
    /** Translator: 61 written with block 110 and one trailing zero byte. */
    HEADER_BLOCK_PLUS_ONE,
    /** Translator: every frame header says version 6. */
    HEADER_VERSION_6,
    /** Translator: egressSeq / journalSeq / meEpoch copied into the wrong head slots (not permuted). */
    HEAD_NOT_PERMUTED,
    /** Translator: the S8 tail of a 64 (timeInForce .. behaviorVersion) zeroed. */
    DROP_64_TAIL,
    /** Translator: enums written through the generated setters (E.get(raw)), which throw or clamp on 2..255. */
    CLAMP_ENUMS,
    /** Translator: writes 1 for any non-zero takerIsBuy. */
    COLLAPSE_BOOL,
    /** Translator: the trade cap flushes at 63. */
    CAP_63,
    /** Translator: the 67 group header written as two u8 instead of two u16. */
    GROUP_HEADER_U8,
    /** Translator: the reference gate is skipped (a non-admitted record is translated). */
    GATE_OFF,
    /** RawDigests: one field left out of the trade digest. */
    RAWDIGEST_DROP_FIELD,
    /** RandomJournal: no extreme integer values. */
    RANDOM_NO_EXTREMES,
    /** Reverse encoder only: takerOrderId and makerOrderId swapped. */
    REVERSE_SWAP_TAKER_MAKER_ORDER,
    /** CoreApplyRunner: feed frames through dispatchTrusted instead of the BRIDGE session. */
    TRUSTED_FEED,
    /** CoreApplyRunner serve: the log time advances per frame instead of per round. */
    ROUND_TIME_PER_FRAME,
    /** CoreApplyRunner: a bridge restart opens a new session instead of reusing 701. */
    RESTART_NEW_SESSION,
    /** T1Journal: REGENESIS relabels the epoch and keeps the seqs (the EPOCH verb's shape). */
    REGENESIS_AS_RELABEL,
    /** FeedStory: the coverage item "62-sized run" is not forced. */
    STORY_NO_BATCH_RUN,
    /** CoreServe: the old catch order, a request error classified before the H4 apply-fault check (review FIX-1). */
    H4_CLASSIFY_FIRST,
    /** FeedStory: a round may carry no feed record. */
    STORY_FEEDLESS_ROUND;

    private static final Set<FeedHarnessSeams> ON = EnumSet.noneOf(FeedHarnessSeams.class);

    public static synchronized boolean on(final FeedHarnessSeams seam) {
        return ON.contains(seam);
    }

    public static synchronized void set(final FeedHarnessSeams seam, final boolean value) {
        if (value) {
            ON.add(seam);
        } else {
            ON.remove(seam);
        }
    }

    public static synchronized void clear() {
        ON.clear();
    }

    public static synchronized boolean any() {
        return !ON.isEmpty();
    }

    public static synchronized Set<FeedHarnessSeams> active() {
        return EnumSet.copyOf(ON.isEmpty() ? EnumSet.noneOf(FeedHarnessSeams.class) : ON);
    }

    /** Runs {@code body} with {@code seam} on, always switching it off again. */
    public static void with(final FeedHarnessSeams seam, final Runnable body) {
        set(seam, true);
        try {
            body.run();
        } finally {
            set(seam, false);
        }
    }
}

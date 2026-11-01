// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * RC2 S9h 4.3: the seeded generator behind {@code random}. {@code SplittableRandom(seed)}; record template weights trade
 * 1/2, every other 1/8; a trade pick emits a run whose length is drawn from {1, 2, 63, 64, 65, uniform 1..130}; a FLUSH
 * follows a record with probability 1/16 (a second one right behind it with 1/4, and the journal starts with one with
 * probability 1/2: review NOTE-5, inputs the differential must see); release entry counts from {0, 1, 47, 48, uniform 0..48}. Every integer field:
 * with probability 1/8 one of {0, 1, -1, MIN, MAX} of its type, else uniform over the type; every raw enum byte uniform
 * 0..255 with probability 1/2, else a valid value. journalSeq and meEpoch are drawn like any field but kept >= 1 (the gate's
 * HEAD row), so every record is admitted. Headers are always schema 4 v1 with the exact block and length. The output is a
 * pure function of {@code (seed, count)}.
 */
public final class RandomJournal {

    private final SplittableRandom rnd;
    private final JournalRecords j = new JournalRecords();

    public RandomJournal(final long seed) {
        this.rnd = new SplittableRandom(seed);
    }

    /** {@code count} records (FLUSH markers between them) as FJR entries. */
    public List<FeedFiles.Entry> generate(final int count) {
        final List<FeedFiles.Entry> out = new ArrayList<>();
        if (rnd.nextBoolean()) {
            out.add(FeedFiles.Entry.FLUSH); // review NOTE-5: a leading FLUSH
        }
        int records = 0;
        while (records < count) {
            final int pick = rnd.nextInt(8);
            if (pick < 4) {
                final int[] lengths = {1, 2, 63, 64, 65, -1};
                int run = lengths[rnd.nextInt(lengths.length)];
                if (run < 0) {
                    run = 1 + rnd.nextInt(130);
                }
                for (int i = 0; i < run && records < count; i++) {
                    records += add(out, trade());
                }
            } else {
                final byte[] r = switch (pick) {
                    case 4 -> terminal();
                    case 5 -> outcome();
                    case 6 -> release();
                    default -> config();
                };
                records += add(out, r);
            }
        }
        return out;
    }

    private int add(final List<FeedFiles.Entry> out, final byte[] r) {
        out.add(FeedFiles.Entry.record(r));
        if (rnd.nextInt(16) == 0) {
            out.add(FeedFiles.Entry.FLUSH);
            if (rnd.nextInt(4) == 0) {
                out.add(FeedFiles.Entry.FLUSH); // and two in a row
            }
        }
        return 1;
    }

    // ------------------------------------------------------------------ fields

    private boolean extreme() {
        return !FeedHarnessSeams.on(FeedHarnessSeams.RANDOM_NO_EXTREMES) && rnd.nextInt(8) == 0;
    }

    long i64() {
        if (extreme()) {
            return new long[] {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}[rnd.nextInt(5)];
        }
        return rnd.nextLong();
    }

    int i32() {
        if (extreme()) {
            return new int[] {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE}[rnd.nextInt(5)];
        }
        return rnd.nextInt();
    }

    /** An unsigned field of {@code bits} bits (8, 16, 32): MIN 0, MAX all ones, -1 all ones. */
    long unsigned(final int bits) {
        final long mask = (1L << bits) - 1;
        if (extreme()) {
            return new long[] {0L, 1L, mask, 0L, mask}[rnd.nextInt(5)];
        }
        return rnd.nextLong() & mask;
    }

    /** A raw enum byte: uniform 0..255 with probability 1/2, else one of {@code valid}. */
    int raw(final int... valid) {
        return rnd.nextBoolean() ? rnd.nextInt(256) : valid[rnd.nextInt(valid.length)];
    }

    /** journalSeq / meEpoch: drawn like any i64, kept >= 1. */
    long head() {
        final long v = i64();
        if (v >= 1) {
            return v;
        }
        return v == Long.MIN_VALUE ? Long.MAX_VALUE : v == 0 ? 1 : -v;
    }

    // ------------------------------------------------------------------ records

    private byte[] trade() {
        return j.trade(head(), head(), i64(), i64(), i32(), i64(), i64(), i64(), i64(), i64(), i64(), raw(0, 1), i64(), i64(),
                i64());
    }

    private byte[] terminal() {
        return j.terminal(head(), head(), i64(), i64(), i64(), i32(), raw(2, 3, 4), i64(), i64(), (int) unsigned(8));
    }

    private byte[] outcome() {
        return j.outcome(head(), head(), i64(), i64(), i64(), i64(), i64(), i64(), i64(), i64(), i64(), i32(),
                (int) unsigned(8), (int) unsigned(8), (int) unsigned(8), i64(), i64(), i64(), i32(), i32(), (int) unsigned(8),
                i32(), (int) unsigned(8), i64(), i32(), i32(), unsigned(32));
    }

    private byte[] release() {
        final int[] counts = {0, 1, 47, 48, -1};
        int n = counts[rnd.nextInt(counts.length)];
        if (n < 0) {
            n = rnd.nextInt(49);
        }
        final long js = head();
        final long ep = head();
        final long eg = i64();
        final long rs = i64();
        final int ls = i32();
        final int lc = i32();
        final int tr = i32();
        final long[][] e = new long[n][];
        for (int i = 0; i < n; i++) {
            e[i] = new long[] {i64(), i64(), raw(0, 1), raw(0, 1, 2), i32()};
        }
        return j.release(js, ep, eg, rs, ls, lc, tr, e);
    }

    private byte[] config() {
        return j.config(head(), head(), i64(), unsigned(32), unsigned(32), unsigned(32), (int) unsigned(16), raw(0, 1, 2, 3));
    }
}

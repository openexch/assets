// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import org.agrona.collections.Long2LongHashMap;

import java.util.Arrays;

/**
 * RC2 S8 3.8: the pending-dispatch index, derived (rebuilt at restore, never serialized). Append-only primitive arrays of
 * {@code (dispatchSeq, orderId, tag)} in ascending dispatchSeq with a dead mark. Live inserts come only from the trading
 * dispatch, the release mint and the orphan cancel mint, all of which take the next replicated dispatchSeq, so appends are
 * ascending; the restore rebuild sorts first. Content is a function of replicated state. Service thread only.
 *
 * <p>Review NOTE-1 (bounded work per apply): a slot is found by binary search over the ascending seqs (no seq -> slot map,
 * so a compaction re-puts nothing), and compaction is incremental: once the dead slots exceed {@code max(live, 64)} an
 * in-place two-region compaction starts, and every mutation advances it by at most {@link #COMPACT_STEP} slots. While it
 * runs, the slots are the compacted region {@code [0, w)} followed by the uncompacted region {@code [r, end)}; both are
 * ascending and every seq of the first is below every seq of the second, so the binary search and the reply walk
 * ({@link #firstSlotAfter}, {@link #nextSlot}) stay exact. The head of each tag (the lowest live seq) comes from a per-tag
 * min-heap: exact, O(log n). Zero allocation outside array growth.</p>
 */
public final class PendingDispatchIndex {

    public static final int TAG_TRADING = 0;
    public static final int TAG_RELEASE = 1;
    public static final int TAG_ORPHAN = 2;
    private static final int TAGS = 3;
    private static final int COMPACT_FLOOR = 64;
    /** Slots an incremental compaction advances per mutation (review NOTE-1: bounded work per apply). */
    public static final int COMPACT_STEP = 256;

    private long[] seq = new long[256];
    private long[] ref = new long[256];
    private byte[] tag = new byte[256];
    private boolean[] dead = new boolean[256];
    /** One past the last physical slot. */
    private int end;
    private boolean compacting;
    /** While compacting: the compacted region is {@code [0, w)}, the uncompacted one {@code [r, end)}, {@code w <= r}. */
    private int w;
    private int r;
    private long maxSeq;
    private int live;
    private int deadCount;
    private final int[] liveByTag = new int[TAGS];
    private final MinHeap[] heads = {new MinHeap(), new MinHeap(), new MinHeap()};
    /** seq -> its position in its tag's heap (seqs are unique across tags). */
    private final Long2LongHashMap heapPos = new Long2LongHashMap(-1L);
    private long compactions;

    /** Append {@code dispatchSeq} for {@code orderId}; {@code dispatchSeq} must be above every seq the index has held. */
    public void add(final long dispatchSeq, final long orderId, final int t) {
        if (dispatchSeq <= maxSeq) {
            throw new IllegalStateException("pending-dispatch index: dispatchSeq " + dispatchSeq + " is not above "
                    + maxSeq + " (dispatchSeq not unique or not ascending)");
        }
        if (end == seq.length) {
            grow();
        }
        seq[end] = dispatchSeq;
        ref[end] = orderId;
        tag[end] = (byte) t;
        dead[end] = false;
        end++;
        maxSeq = dispatchSeq;
        live++;
        liveByTag[t]++;
        heads[t].add(dispatchSeq);
        step();
    }

    /** Mark {@code dispatchSeq} dead; false when it is not in the index. */
    public boolean remove(final long dispatchSeq) {
        final int s = find(dispatchSeq);
        if (s < 0) {
            return false;
        }
        dead[s] = true;
        live--;
        liveByTag[tag[s]]--;
        deadCount++;
        heads[tag[s]].remove(dispatchSeq);
        step();
        return true;
    }

    public boolean contains(final long dispatchSeq) {
        return find(dispatchSeq) >= 0;
    }

    /** Live entries of every tag. */
    public int live() {
        return live;
    }

    /** Live entries of one tag (maintained, plain int). */
    public int live(final int t) {
        return liveByTag[t];
    }

    /** Slots held, dead ones included (test hook). */
    public int slots() {
        return compacting ? w + (end - r) : end;
    }

    /** The lowest live dispatchSeq of tag {@code t}, 0 when none (the tag's min-heap: exact, O(1)). */
    public long headSeq(final int t) {
        return heads[t].min();
    }

    /** The first slot whose seq is above {@code from} (dead ones included), or {@link #end()} when there is none. */
    public int firstSlotAfter(final long from) {
        if (compacting) {
            // a hit in the compacted region is below w; otherwise the answer is in [r, end] (end: none)
            return w > 0 && seq[w - 1] > from ? upper(0, w, from) : upper(r, end, from);
        }
        return upper(0, end, from);
    }

    /** The slot after {@code slot} in seq order (skips the compaction gap); {@link #end()} after the last one. */
    public int nextSlot(final int slot) {
        final int n = slot + 1;
        return compacting && n == w ? r : n;
    }

    /** One past the last slot ({@link #firstSlotAfter} and {@link #nextSlot} stop here). */
    public int end() {
        return end;
    }

    public boolean isDead(final int slot) {
        return dead[slot];
    }

    public long seqAt(final int slot) {
        return seq[slot];
    }

    public long refAt(final int slot) {
        return ref[slot];
    }

    public int tagAt(final int slot) {
        return tag[slot];
    }

    /** Completed compaction rounds (test hook). */
    public long compactions() {
        return compactions;
    }

    public boolean compacting() {
        return compacting;
    }

    /** Drop everything (restore rebuild). */
    public void clear() {
        end = 0;
        compacting = false;
        w = 0;
        r = 0;
        maxSeq = 0;
        live = 0;
        deadCount = 0;
        Arrays.fill(liveByTag, 0);
        for (final MinHeap h : heads) {
            h.n = 0;
        }
        heapPos.clear();
    }

    /**
     * Rebuild from unsorted {@code (seq, ref, tag)} triples (restore only): sorted by seq, then appended. A repeated seq is
     * refused ({@link IllegalStateException}), as a restored image whose dispatchSeqs are not unique is corrupt.
     */
    public void rebuild(final long[] seqs, final long[] refs, final int[] tags, final int n) {
        clear();
        final Long2LongHashMap at = new Long2LongHashMap(-1L);
        final long[] sorted = Arrays.copyOf(seqs, n);
        for (int i = 0; i < n; i++) {
            if (at.put(seqs[i], i) != -1L) {
                throw new IllegalStateException("pending-dispatch index: dispatchSeq " + seqs[i] + " held twice");
            }
        }
        Arrays.sort(sorted);
        for (int i = 0; i < n; i++) {
            final int k = (int) at.get(sorted[i]);
            add(seqs[k], refs[k], tags[k]);
        }
    }

    // ---------------------------------------------------------------- internals

    /** The live slot holding {@code s}, or -1. */
    private int find(final long s) {
        final int slot;
        if (compacting) {
            slot = w > 0 && s <= seq[w - 1] ? exact(0, w, s) : exact(r, end, s);
        } else {
            slot = exact(0, end, s);
        }
        return slot >= 0 && !dead[slot] ? slot : -1;
    }

    private int exact(final int lo0, final int hi0, final long s) {
        int lo = lo0;
        int hi = hi0 - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long v = seq[mid];
            if (v < s) {
                lo = mid + 1;
            } else if (v > s) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    /** The first slot in {@code [lo, hi)} whose seq is above {@code from}; {@code hi} when none. */
    private int upper(final int lo0, final int hi0, final long from) {
        int lo = lo0;
        int hi = hi0;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (seq[mid] <= from) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** Advance (or start) the incremental compaction by at most {@link #COMPACT_STEP} slots. */
    private void step() {
        if (!compacting) {
            if (deadCount <= Math.max(live, COMPACT_FLOOR)) {
                return;
            }
            compacting = true;
            w = 0;
            r = 0;
        }
        for (int i = 0; i < COMPACT_STEP && r < end; i++, r++) {
            if (dead[r]) {
                deadCount--;
                continue;
            }
            if (w != r) {
                seq[w] = seq[r];
                ref[w] = ref[r];
                tag[w] = tag[r];
                dead[w] = false;
            }
            w++;
        }
        if (r == end) {
            end = w;
            compacting = false;
            compactions++;
        }
    }

    private void grow() {
        final int n = seq.length * 2;
        seq = Arrays.copyOf(seq, n);
        ref = Arrays.copyOf(ref, n);
        tag = Arrays.copyOf(tag, n);
        dead = Arrays.copyOf(dead, n);
    }

    /** A min-heap of seqs; positions in {@link #heapPos}. */
    private final class MinHeap {
        private long[] a = new long[64];
        private int n;

        long min() {
            return n == 0 ? 0L : a[0];
        }

        void add(final long v) {
            if (n == a.length) {
                a = Arrays.copyOf(a, n * 2);
            }
            a[n] = v;
            heapPos.put(v, n);
            up(n++);
        }

        void remove(final long v) {
            final int i = (int) heapPos.remove(v);
            if (i < 0) {
                throw new IllegalStateException("pending-dispatch index: seq " + v + " not in its tag's heap");
            }
            final int last = --n;
            if (i != last) {
                a[i] = a[last];
                heapPos.put(a[i], i);
                down(i);
                up(i);
            }
        }

        private void up(final int at) {
            int i = at;
            final long v = a[i];
            while (i > 0) {
                final int p = (i - 1) >>> 1;
                if (a[p] <= v) {
                    break;
                }
                a[i] = a[p];
                heapPos.put(a[i], i);
                i = p;
            }
            a[i] = v;
            heapPos.put(v, i);
        }

        private void down(final int at) {
            int i = at;
            final long v = a[i];
            while (true) {
                final int c = 2 * i + 1;
                if (c >= n) {
                    break;
                }
                final int m = c + 1 < n && a[c + 1] < a[c] ? c + 1 : c;
                if (a[m] >= v) {
                    break;
                }
                a[i] = a[m];
                heapPos.put(a[i], i);
                i = m;
            }
            a[i] = v;
            heapPos.put(v, i);
        }
    }
}

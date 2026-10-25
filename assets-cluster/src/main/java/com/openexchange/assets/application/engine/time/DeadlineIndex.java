// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.time;

import java.util.TreeSet;

/**
 * The ONE sorted deadline index behind the core's single sweep timer (K3-D). Entries are
 * {@code (deadlineMs, id, kind)} ordered exactly that way, so a sweep pops them in a total order that
 * is a pure function of the entries: two replicas, or a replica and its restored twin, pop identically.
 *
 * <p>DERIVED state: it is rebuilt on restore from the replicated records that own the deadlines
 * (GTD {@code expiresAtMs}, replace windows, requestId TTLs) and is never serialized itself. The sweep
 * is one-shot per entry: a popped entry is removed before its handler runs, so firing the sweep twice
 * at the same time is idempotent by construction.</p>
 *
 * <p>Service-thread only. Allocation per insert is accepted: deadlines are a minority of orders and
 * the sweep path is off the admission hot path.</p>
 */
public final class DeadlineIndex {

    public static final int KIND_GTD = 1;
    public static final int KIND_REPLACE = 2;
    public static final int KIND_REQUEST_TTL = 3;

    /** Receives a popped or listed entry. */
    public interface Handler {
        void onDue(long deadlineMs, long id, int kind);
    }

    private record Entry(long deadlineMs, long id, int kind) implements Comparable<Entry> {
        @Override
        public int compareTo(final Entry o) {
            int c = Long.compare(deadlineMs, o.deadlineMs);
            if (c == 0) {
                c = Long.compare(id, o.id);
            }
            if (c == 0) {
                c = Integer.compare(kind, o.kind);
            }
            return c;
        }
    }

    private final TreeSet<Entry> entries = new TreeSet<>();
    private final Entry probe = new Entry(0, 0, 0); // never inserted; compareTo is all remove needs

    public void add(final long deadlineMs, final long id, final int kind) {
        entries.add(new Entry(deadlineMs, id, kind));
    }

    /** Remove the exact entry; {@code false} when it was not present (idempotent). */
    public boolean remove(final long deadlineMs, final long id, final int kind) {
        return entries.remove(new Entry(deadlineMs, id, kind));
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /** Earliest deadline, or {@code Long.MAX_VALUE} when empty. */
    public long firstDeadline() {
        return entries.isEmpty() ? Long.MAX_VALUE : entries.first().deadlineMs;
    }

    /**
     * Pop every entry with {@code deadline <= nowMs} in index order, removing each BEFORE its handler
     * runs (one-shot; a handler may add new entries, which are considered in the same pass if due).
     *
     * @return how many entries were popped
     */
    public int popDue(final long nowMs, final Handler handler) {
        int n = 0;
        while (!entries.isEmpty()) {
            final Entry first = entries.first();
            if (first.deadlineMs > nowMs) {
                break;
            }
            entries.pollFirst();
            n++;
            handler.onDue(first.deadlineMs, first.id, first.kind);
        }
        return n;
    }

    /** Visit every entry in index order without removing (state comparisons, diagnostics). */
    public void forEach(final Handler handler) {
        for (final Entry e : entries) {
            handler.onDue(e.deadlineMs, e.id, e.kind);
        }
    }

    public void clear() {
        entries.clear();
    }
}

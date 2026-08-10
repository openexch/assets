// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.feed;

import com.openexchange.assets.infrastructure.feed.BalanceConflationStore.BalanceSlot;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * The conflation store's contract, piece by piece: latest value wins, a hot key is enqueued once,
 * the seqlock never surrenders a torn pair, and the flat sweep view names every key exactly once.
 */
public class BalanceConflationStoreTest {

    private final long[] pair = new long[2];

    // ---- latest-wins ----

    @Test
    public void repeatedWritesToOneKeyConflateToTheLatestPair() {
        final BalanceConflationStore store = new BalanceConflationStore();
        for (int i = 1; i <= 1_000; i++) {
            store.onBalanceUpdate(7L, 1, i * 10L, i);
        }
        final BalanceSlot slot = store.pollDirty();
        assertNotNull(slot);
        assertEquals(7L, slot.userId());
        assertEquals(1, slot.assetId());
        slot.readInto(pair);
        assertEquals(10_000L, pair[0]);
        assertEquals(1_000L, pair[1]);
    }

    // ---- dirty de-dup ----

    @Test
    public void hotKeyIsEnqueuedOnceUntilTaken() {
        final BalanceConflationStore store = new BalanceConflationStore();
        for (int i = 0; i < 1_000; i++) {
            store.onBalanceUpdate(7L, 1, i, 0L);
        }
        assertNotNull("first poll takes the one queued slot", store.pollDirty());
        assertNull("a hot key must conflate, not flood the queue", store.pollDirty());
    }

    @Test
    public void writeAfterTakeReschedulesTheSlot() {
        final BalanceConflationStore store = new BalanceConflationStore();
        store.onBalanceUpdate(7L, 1, 100L, 0L);
        final BalanceSlot slot = store.pollDirty();
        assertNotNull(slot);
        assertNull(store.pollDirty());

        store.onBalanceUpdate(7L, 1, 200L, 0L);
        final BalanceSlot again = store.pollDirty();
        assertSame("same key, same slot identity", slot, again);
        again.readInto(pair);
        assertEquals(200L, pair[0]);
    }

    // ---- seqlock: torn pairs are impossible ----

    /**
     * Two-thread hammer: the writer flips one slot between (A,A) and (B,B); the reader must never
     * observe a mixed pair. Bounded iterations on both sides, no sleeps: the reader spins until the
     * writer is done AND it has a minimum number of overlapped reads, both of which terminate.
     */
    @Test
    public void seqlockNeverYieldsATornPair() throws InterruptedException {
        final long a = 0x1111_1111_1111_1111L;
        final long b = 0x2222_2222_2222_2222L;
        final int writes = 2_000_000;
        final int minReads = 100_000;

        final BalanceConflationStore store = new BalanceConflationStore();
        store.onBalanceUpdate(7L, 1, a, a);
        final BalanceSlot slot = store.pollDirty();
        assertNotNull(slot);

        final AtomicBoolean writerDone = new AtomicBoolean();
        final AtomicLong tornSeen = new AtomicLong();
        final AtomicLong reads = new AtomicLong();

        final Thread writer = new Thread(() -> {
            for (int i = 0; i < writes; i++) {
                final long v = (i & 1) == 0 ? b : a;
                store.onBalanceUpdate(7L, 1, v, v);
            }
            writerDone.set(true);
        }, "hammer-writer");

        final Thread reader = new Thread(() -> {
            final long[] p = new long[2];
            long n = 0;
            while (!writerDone.get() || n < minReads) {
                slot.readInto(p);
                if (p[0] != p[1] || (p[0] != a && p[0] != b)) {
                    tornSeen.incrementAndGet();
                    break;
                }
                n++;
            }
            reads.set(n);
        }, "hammer-reader");

        reader.start();
        writer.start();
        writer.join(60_000);
        reader.join(60_000);
        assertFalse("hammer did not finish in time", writer.isAlive() || reader.isAlive());
        assertEquals("reader observed a torn (available, locked) pair", 0, tornSeen.get());
        assertTrue("hammer must actually have read", reads.get() >= minReads);

        // And the final truth survived the storm: the last write is what the slot holds.
        slot.readInto(pair);
        assertEquals(pair[0], pair[1]);
    }

    // ---- mark-all sweep surface ----

    @Test
    public void leadershipGainRequestsOneSweepAndTheFlatViewNamesEveryKeyOnce() {
        final BalanceConflationStore store = new BalanceConflationStore();
        final int users = 100;
        for (long u = 0; u < users; u++) {
            store.onBalanceUpdate(u, 0, u * 10, 1L); // USD
            store.onBalanceUpdate(u, 1, u * 20, 2L); // BTC
        }
        while (store.pollDirty() != null) {
            // drain: the sweep view must not depend on queue state
        }

        assertFalse("no sweep before any role change", store.consumeSweepRequest());
        store.onRoleChange(true);
        assertTrue("gaining leadership requests a sweep", store.consumeSweepRequest());
        assertFalse("the request is one-shot", store.consumeSweepRequest());
        store.onRoleChange(true);
        assertFalse("re-affirming leadership is not a gain", store.consumeSweepRequest());
        store.onRoleChange(false);
        store.onRoleChange(true);
        assertTrue("a fresh gain requests a fresh sweep", store.consumeSweepRequest());

        // The sweep iterates [0, liveSlots()): every (userId, assetId) exactly once, values current.
        final int live = store.liveSlots();
        assertEquals(users * 2, live);
        final Set<Long> seen = new HashSet<>();
        for (int i = 0; i < live; i++) {
            final BalanceSlot slot = store.slotAt(i);
            assertTrue("duplicate key in sweep view",
                    seen.add((slot.userId() << 8) | slot.assetId()));
            slot.readInto(pair);
            assertEquals(slot.userId() * (slot.assetId() == 0 ? 10 : 20), pair[0]);
            assertEquals(slot.assetId() == 0 ? 1L : 2L, pair[1]);
        }
        assertEquals(users * 2, seen.size());
    }

    // ---- dirty-queue overflow degrades to a sweep, never a loss ----

    @Test
    public void queueOverflowRequestsASweepAndTheKeyStaysReschedulable() {
        final BalanceConflationStore store = new BalanceConflationStore();
        final int capacity = BalanceConflationStore.DIRTY_QUEUE_CAPACITY;
        for (long u = 0; u <= capacity; u++) { // capacity + 1 distinct keys, nothing drained
            store.onBalanceUpdate(u, 0, u, 0L);
        }
        assertTrue("overflow must fall back to a full sweep", store.consumeSweepRequest());
        // The overflowed key's value is reachable through the sweep view regardless.
        assertEquals(capacity + 1, store.liveSlots());

        // Its flag was un-claimed, so once there is room a fresh write re-schedules it.
        assertNotNull(store.pollDirty()); // make room
        store.onBalanceUpdate(capacity, 0, 42L, 0L);
        BalanceSlot found = null;
        BalanceSlot next;
        while ((next = store.pollDirty()) != null) {
            if (next.userId() == capacity) {
                found = next;
            }
        }
        assertNotNull("overflowed key must be reschedulable after the queue has room", found);
        found.readInto(pair);
        assertEquals(42L, pair[0]);
    }
}

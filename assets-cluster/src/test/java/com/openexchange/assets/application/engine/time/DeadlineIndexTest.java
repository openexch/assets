// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.time;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S2 (K3-D): the ONE sorted deadline index behind the single sweep timer. Pops are ordered by
 * (deadline, id, kind), one-shot, and the listing is a pure function of the entries, not of the order
 * they were inserted in (the property the restore rebuild relies on).
 */
public class DeadlineIndexTest {

    private static String pop(final DeadlineIndex idx, final long now) {
        final List<String> out = new ArrayList<>();
        idx.popDue(now, (d, id, k) -> out.add(d + ":" + id + ":" + k));
        return String.join(",", out);
    }

    @Test
    public void popsDueEntriesInDeadlineIdKindOrderAndOnlyOnce() {
        final DeadlineIndex idx = new DeadlineIndex();
        idx.add(300, 7, DeadlineIndex.KIND_GTD);
        idx.add(100, 9, DeadlineIndex.KIND_GTD);
        idx.add(100, 3, DeadlineIndex.KIND_REPLACE);
        idx.add(100, 3, DeadlineIndex.KIND_GTD);
        idx.add(500, 1, DeadlineIndex.KIND_REQUEST_TTL);
        assertEquals(5, idx.size());
        assertEquals(100, idx.firstDeadline());
        assertEquals("100:3:1,100:3:2,100:9:1,300:7:1", pop(idx, 300));
        assertEquals("a second sweep at the same time pops nothing", "", pop(idx, 300));
        assertEquals(1, idx.size());
        assertEquals(500, idx.firstDeadline());
        assertEquals("500:1:3", pop(idx, 500));
        assertTrue(idx.isEmpty());
        assertEquals(Long.MAX_VALUE, idx.firstDeadline());
    }

    @Test
    public void removeIsExactAndIdempotent() {
        final DeadlineIndex idx = new DeadlineIndex();
        idx.add(100, 1, DeadlineIndex.KIND_GTD);
        idx.add(100, 1, DeadlineIndex.KIND_REPLACE);
        assertTrue(idx.remove(100, 1, DeadlineIndex.KIND_GTD));
        assertFalse(idx.remove(100, 1, DeadlineIndex.KIND_GTD));
        assertFalse("different deadline is a different entry", idx.remove(101, 1, DeadlineIndex.KIND_REPLACE));
        assertEquals(1, idx.size());
        assertEquals("100:1:2", pop(idx, 100));
    }

    @Test
    public void theListingIsAFunctionOfTheEntriesNotTheInsertionOrder() {
        final long[][] entries = {{900, 5, 1}, {100, 2, 1}, {100, 2, 2}, {400, 9, 3}, {100, 1, 1}, {400, 3, 1}};
        final DeadlineIndex forward = new DeadlineIndex();
        for (final long[] e : entries) {
            forward.add(e[0], e[1], (int) e[2]);
        }
        final DeadlineIndex reverse = new DeadlineIndex();
        for (int i = entries.length - 1; i >= 0; i--) {
            reverse.add(entries[i][0], entries[i][1], (int) entries[i][2]);
        }
        final List<String> a = new ArrayList<>();
        final List<String> b = new ArrayList<>();
        forward.forEach((d, id, k) -> a.add(d + ":" + id + ":" + k));
        reverse.forEach((d, id, k) -> b.add(d + ":" + id + ":" + k));
        assertEquals(List.of("100:1:1", "100:2:1", "100:2:2", "400:3:1", "400:9:3", "900:5:1"), a);
        assertEquals(a, b);
    }
}

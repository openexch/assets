// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import org.junit.Test;

import static com.openexchange.assets.projector.ProjectorFilter.Action.HALT;
import static com.openexchange.assets.projector.ProjectorFilter.Action.PROJECT;
import static com.openexchange.assets.projector.ProjectorFilter.Action.SKIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pins the projector's money-critical projection semantics on the single dense journalSeq: resume
 * skip below the Postgres high-water, exact +1 advance, dense-gap HALT (latched), and the explicit
 * haltOnGap=false operator override. These rules are what make the projector safe to crash, restart
 * and re-run purely from the PG high-water.
 */
public class ProjectorFilterTest {

    @Test
    public void freshLedgerProjectsFromJournalSeqOne() {
        ProjectorFilter f = new ProjectorFilter(0L, true);
        assertEquals(PROJECT, f.onRecord(1L));
        assertEquals(PROJECT, f.onRecord(2L));
        assertEquals(PROJECT, f.onRecord(3L));
        assertEquals(3L, f.highWater());
        assertEquals(0L, f.gapsDetected());
        assertFalse(f.halted());
    }

    @Test
    public void resumeSkipsAtOrBelowHighWaterAndProjectsTheNext() {
        // Postgres already has journalSeq 1..100.
        ProjectorFilter f = new ProjectorFilter(100L, true);
        assertEquals(SKIP, f.onRecord(1L));    // replay-from-start overlap
        assertEquals(SKIP, f.onRecord(100L));  // the boundary itself is already projected
        assertEquals(PROJECT, f.onRecord(101L));
        assertEquals(PROJECT, f.onRecord(102L));
        assertEquals(2L, f.skipped());
        assertEquals(102L, f.highWater());
    }

    @Test
    public void duplicateReplayAcrossRecordingsSkips() {
        ProjectorFilter f = new ProjectorFilter(0L, true);
        assertEquals(PROJECT, f.onRecord(1L));
        assertEquals(PROJECT, f.onRecord(2L));
        // A stopped-recording replay re-delivers old records before the active one: dedupe by seq.
        assertEquals(SKIP, f.onRecord(1L));
        assertEquals(SKIP, f.onRecord(2L));
        assertEquals(PROJECT, f.onRecord(3L));
        assertEquals(2L, f.skipped());
        assertEquals(0L, f.gapsDetected());
    }

    @Test
    public void denseGapHaltsAndLatchesForever() {
        ProjectorFilter f = new ProjectorFilter(10L, true);
        assertEquals(PROJECT, f.onRecord(11L));
        // 13 skips 12: a lost money movement. Nothing may be projected past it.
        assertEquals(HALT, f.onRecord(13L));
        assertTrue(f.halted());
        assertEquals(1L, f.gapsDetected());
        // Latched: even the "missing" 12 arriving later must not resume a halted filter.
        assertEquals(HALT, f.onRecord(12L));
        assertEquals(HALT, f.onRecord(14L));
        assertEquals(11L, f.highWater());
    }

    @Test
    public void retentionRaceSurfacesAsAGapNotASilentSkip() {
        // PG at journalSeq 50; the earliest retained journal record is 60 (journal purged past the
        // projector's needs, or projector down too long): that MUST halt, not silently skip.
        ProjectorFilter f = new ProjectorFilter(50L, true);
        assertEquals(HALT, f.onRecord(60L));
        assertTrue(f.halted());
        assertEquals(50L, f.highWater());
    }

    @Test
    public void haltOnGapFalseProjectsPastTheGapButCountsIt() {
        ProjectorFilter f = new ProjectorFilter(10L, false);
        assertEquals(PROJECT, f.onRecord(11L));
        assertEquals(PROJECT, f.onRecord(13L)); // operator override: project, count
        assertEquals(1L, f.gapsDetected());
        assertFalse(f.halted());
        assertEquals(13L, f.highWater());
        // The late 12 is now a duplicate-by-ordering: skipped by the dense dedupe.
        assertEquals(SKIP, f.onRecord(12L));
    }
}

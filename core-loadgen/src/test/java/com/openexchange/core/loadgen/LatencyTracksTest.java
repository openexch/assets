// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Warm-up exclusion, per-second maxima and the clamp of the extracted latency tracks.
 */
public class LatencyTracksTest {

    private static final long S = 1_000_000_000L;

    @Test
    public void warmUpIsExcludedAndEachSecondKeepsItsMaximum() {
        final LatencyTracks t = new LatencyTracks(0, 2 * S);
        final LatencyTracks.Track create = t.track("create-ack");
        final LatencyTracks.Track cancel = t.track("cancel-ack");
        assertFalse("inside the warm-up", t.record(create, S, 9_000_000));
        assertFalse(t.record(create, 2 * S - 1, 9_000_000));
        assertTrue(t.record(create, 2 * S, 100_000));
        assertTrue(t.record(create, 2 * S + 5, 300_000));
        assertTrue(t.record(create, 3 * S + 1, 200_000));
        assertTrue(t.record(cancel, 3 * S + 2, 50_000));
        assertEquals("warm-up values never reach the histogram", 3, create.cumulative().getTotalCount());
        assertTrue(create.cumulative().getMaxValue() < 1_000_000);
        final List<LatencyTracks.SecondRow> rows = t.secondRows();
        assertEquals(List.of(new LatencyTracks.SecondRow(2, "create-ack", 2, 300_000),
                new LatencyTracks.SecondRow(3, "create-ack", 1, 200_000),
                new LatencyTracks.SecondRow(3, "cancel-ack", 1, 50_000)), rows);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        t.writeSecondsTsv(new PrintStream(out, true, StandardCharsets.UTF_8));
        assertEquals("second\ttrack\tn\tmax_us\n2\tcreate-ack\t2\t300.0\n3\tcreate-ack\t1\t200.0\n3\tcancel-ack\t1\t50.0\n",
                out.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void valuesAreClampedIntoTheHistogramRange() {
        final LatencyTracks t = new LatencyTracks(0, 0);
        final LatencyTracks.Track x = t.track("x");
        t.record(x, 0, -5);
        t.record(x, 0, LatencyTracks.HIGHEST_NS * 3);
        assertEquals(2, x.cumulative().getTotalCount());
        assertEquals(0, x.cumulative().getMinValue());
        assertTrue(x.cumulative().getMaxValue() >= LatencyTracks.HIGHEST_NS - LatencyTracks.HIGHEST_NS / 500);
        assertEquals(LatencyTracks.HIGHEST_NS - 1, LatencyTracks.clamp(Long.MAX_VALUE));
    }

    @Test
    public void theIntervalLineCoversOnlyTheIntervalSinceTheLastFlip() {
        final LatencyTracks.Track x = new LatencyTracks.Track("hold-ack");
        x.recordValue(2_000_000);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintStream ps = new PrintStream(out, true, StandardCharsets.UTF_8);
        x.printIntervalLine(ps, "ILOG ");
        x.printIntervalLine(ps, "ILOG ");
        final String[] lines = out.toString(StandardCharsets.UTF_8).split("\n");
        assertTrue(lines[0], lines[0].startsWith("ILOG track=hold-ack n=1 p50_ms=2.00"));
        assertTrue(lines[1], lines[1].startsWith("ILOG track=hold-ack n=0 "));
        assertEquals(1, x.cumulative().getTotalCount());
    }
}

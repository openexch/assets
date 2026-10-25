// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.metrics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** The service-thread cost histogram and its Prometheus rendering, off the cluster. */
public class AssetsNodeMetricsTest {

    private static int bucketOf(final long nanos) {
        int i = 0;
        while (i < AssetsNodeMetrics.BUCKET_UPPER_NS.length && nanos > AssetsNodeMetrics.BUCKET_UPPER_NS[i]) {
            i++;
        }
        return i;
    }

    @Test
    public void thresholdsFallOnExactBucketBoundaries() {
        final AssetsNodeMetrics m = new AssetsNodeMetrics(0);
        m.recordMessage(1, 5_000L, 100L, 10L);        // exactly 5 us: le="0.000005" bucket
        m.recordMessage(1, 5_001L, 100L, 11L);        // just over: next bucket
        m.recordMessage(1, 250L, 100L, 12L);          // first bucket
        m.recordMessage(1, 20_000_000_000L, 100L, 13L); // beyond the ladder: +Inf slot
        final AssetsNodeMetrics.Histogram h = m.dispatch();
        assertEquals(1, h.bucketCount(bucketOf(5_000L)));
        assertEquals(bucketOf(5_000L) + 1, bucketOf(5_001L));
        assertEquals(1, h.bucketCount(bucketOf(5_001L)));
        assertEquals(1, h.bucketCount(0));
        assertEquals(1, h.bucketCount(AssetsNodeMetrics.BUCKETS - 1));
        assertEquals(4, h.count());
        assertEquals(5_000L + 5_001L + 250L + 20_000_000_000L, h.sumNanos());
        assertEquals(20_000_000_000L, h.maxNanos());
        assertEquals("0.000005", AssetsNodeMetrics.BUCKET_LE[bucketOf(5_000L)]);
        assertEquals("0.00005", AssetsNodeMetrics.BUCKET_LE[bucketOf(50_000L)]);
        assertEquals("10", AssetsNodeMetrics.BUCKET_LE[AssetsNodeMetrics.BUCKET_LE.length - 1]);
    }

    @Test
    public void perTemplateCostAndOutOfRangeTemplatesShareTheLastSlot() {
        final AssetsNodeMetrics m = new AssetsNodeMetrics(0);
        m.recordMessage(1, 1_000L, 10L, 1L);
        m.recordMessage(1, 3_000L, 10L, 2L);
        m.recordMessage(5, 7_000L, 10L, 3L);
        m.recordMessage(999, 9_000L, 10L, 4L);
        m.recordMessage(-1, 11_000L, 10L, 5L);
        assertEquals(2, m.templateCount(1));
        assertEquals(4_000L, m.templateSumNanos(1));
        assertEquals(3_000L, m.templateMaxNanos(1));
        assertEquals(1, m.templateCount(5));
        assertEquals(2, m.templateCount(AssetsNodeMetrics.TEMPLATE_SLOTS - 1));
        assertEquals(11_000L, m.templateMaxNanos(AssetsNodeMetrics.TEMPLATE_SLOTS - 1));
        assertEquals(5L, m.lastLogPosition());
        assertEquals(5, m.dispatch().count());
        assertEquals(5, m.drain().count());
    }

    @Test
    public void samplingShiftRecordsOneInTwoToTheShift() {
        final AssetsNodeMetrics every = new AssetsNodeMetrics(0);
        for (int i = 0; i < 8; i++) {
            assertTrue(every.shouldSample());
        }
        final AssetsNodeMetrics quarter = new AssetsNodeMetrics(2);
        int sampled = 0;
        for (int i = 0; i < 16; i++) {
            if (quarter.shouldSample()) {
                sampled++;
            }
        }
        assertEquals(4, sampled);
        assertEquals(2, quarter.sampleShift());
    }

    @Test(expected = IllegalArgumentException.class)
    public void sampleShiftOutsideRangeIsRefused() {
        new AssetsNodeMetrics(17);
    }

    @Test
    public void publishSequenceAdvancesEveryPublishEveryMessagesAndOnTimerDrain() {
        final AssetsNodeMetrics m = new AssetsNodeMetrics(0);
        final long before = m.acquire();
        for (int i = 0; i < AssetsNodeMetrics.PUBLISH_EVERY - 1; i++) {
            m.recordMessage(1, 1L, 1L, i);
        }
        assertEquals(before, m.acquire());
        m.recordMessage(1, 1L, 1L, 99L);
        assertEquals(before + 1, m.acquire());
        m.recordTimerDrain(5L);
        assertEquals(before + 2, m.acquire());
    }

    @Test
    public void rendersPrometheusTextWithHistogramsTemplatesAndSnapshotSeries() {
        final AssetsNodeMetrics m = new AssetsNodeMetrics(0);
        m.setRole(2);
        m.setMemberId(1);
        m.recordMessage(1, 4_000L, 500L, 77L);
        m.recordMessage(3, 60_000L, 500L, 78L);
        m.recordTimerDrain(900L);
        final AssetsMetricsBody body = new AssetsMetricsBody(m)
                .counter("assets_test_total", "A counter", () -> 42L)
                .gauge("assets_test_gauge", "A gauge", () -> -7L);
        String out = body.render();
        assertTrue(out, out.contains("# TYPE assets_test_total counter\nassets_test_total 42\n"));
        assertTrue(out, out.contains("# TYPE assets_test_gauge gauge\nassets_test_gauge -7\n"));
        assertTrue(out, out.contains("assets_cluster_role 2\n"));
        assertTrue(out, out.contains("assets_member_id 1\n"));
        assertTrue(out, out.contains("assets_log_position 78\n"));
        assertTrue(out, out.contains("assets_apply_sample_shift 0\n"));
        assertTrue(out, out.contains("# TYPE assets_apply_dispatch_seconds histogram\n"));
        // 4 us is <= 5 us; 60 us is > 50 us, so the 5 us bucket holds 1 and the 50 us bucket still 1
        assertTrue(out, out.contains("assets_apply_dispatch_seconds_bucket{le=\"0.000005\"} 1\n"));
        assertTrue(out, out.contains("assets_apply_dispatch_seconds_bucket{le=\"0.00005\"} 1\n"));
        assertTrue(out, out.contains("assets_apply_dispatch_seconds_bucket{le=\"0.0001\"} 2\n"));
        assertTrue(out, out.contains("assets_apply_dispatch_seconds_bucket{le=\"+Inf\"} 2\n"));
        assertTrue(out, out.contains("assets_apply_dispatch_seconds_count 2\n"));
        assertTrue(out, out.contains("assets_apply_dispatch_seconds_max 6.0E-5\n"));
        assertTrue(out, out.contains("assets_apply_drain_seconds_count 2\n"));
        assertTrue(out, out.contains("assets_timer_drain_seconds_count 1\n"));
        assertTrue(out, out.contains("assets_apply_template_dispatch_count{template=\"1\"} 1\n"));
        assertTrue(out, out.contains("assets_apply_template_dispatch_count{template=\"3\"} 1\n"));
        assertFalse(out, out.contains("template=\"2\""));
        assertTrue(out, out.contains("assets_snapshot_age_seconds -1\n"));
        assertTrue(out, out.contains("assets_snapshot_last_bytes -1\n"));
        assertTrue(out, out.contains("assets_snapshot_last_record_seconds -1\n"));

        m.stampSnapshot(System.currentTimeMillis(), 178_537L, 22L, 8_000_000L, 1_000_000L, 17_000_000L, 261_856L);
        out = body.render();
        assertTrue(out, out.contains("assets_snapshot_age_seconds 0\n"));
        assertTrue(out, out.contains("assets_snapshot_last_bytes 178537\n"));
        assertTrue(out, out.contains("assets_snapshot_last_chunks 22\n"));
        assertTrue(out, out.contains("assets_snapshot_last_log_position 261856\n"));
        assertTrue(out, out.contains("assets_snapshot_last_serialize_seconds 0.008\n"));
        assertTrue(out, out.contains("assets_snapshot_last_record_seconds 0.017\n"));
    }
}

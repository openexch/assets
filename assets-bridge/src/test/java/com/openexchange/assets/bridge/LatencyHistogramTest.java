// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Pins the {@link BridgeState.LatencyHistogram} recording path: bucket selection (inclusive
 * upper bounds, ms), the +Inf overflow bucket, and the sum/count bookkeeping the metrics
 * renderer builds its cumulative Prometheus lines from.
 */
public class LatencyHistogramTest {

    @Test
    public void recordsKnownValuesIntoTheRightBuckets() {
        final BridgeState.LatencyHistogram h = new BridgeState.LatencyHistogram();
        h.record(0);     // le=1ms bucket
        h.record(3);     // le=5ms bucket
        h.record(700);   // le=1000ms bucket
        h.record(5_000); // +Inf bucket

        assertEquals(1, h.buckets.get(0)); // le 1
        assertEquals(0, h.buckets.get(1)); // le 2
        assertEquals(1, h.buckets.get(2)); // le 5
        assertEquals(0, h.buckets.get(3)); // le 10
        assertEquals(0, h.buckets.get(4)); // le 25
        assertEquals(0, h.buckets.get(5)); // le 50
        assertEquals(0, h.buckets.get(6)); // le 100
        assertEquals(0, h.buckets.get(7)); // le 250
        assertEquals(1, h.buckets.get(8)); // le 1000
        assertEquals(1, h.buckets.get(9)); // +Inf
        assertEquals(5_703, h.sumMs);
        assertEquals(4, h.count);
    }

    @Test
    public void bucketUpperBoundsAreInclusive() {
        final long[] uppers = BridgeState.LatencyHistogram.BUCKET_UPPER_MS;
        for (int i = 0; i < uppers.length; i++) {
            final BridgeState.LatencyHistogram h = new BridgeState.LatencyHistogram();
            h.record(uppers[i]);     // exactly the bound: this bucket
            h.record(uppers[i] + 1); // just past the bound: the next bucket
            assertEquals("bound " + uppers[i] + "ms in bucket " + i, 1, h.buckets.get(i));
            assertEquals("bound+1 in bucket " + (i + 1), 1, h.buckets.get(i + 1));
            assertEquals(2, h.count);
        }
    }

    @Test
    public void totalOfAllBucketsEqualsCount() {
        final BridgeState.LatencyHistogram h = new BridgeState.LatencyHistogram();
        final long[] samples = {0, 1, 2, 7, 33, 99, 250, 251, 999, 1000, 1001, 60_000};
        long expectedSum = 0;
        for (final long s : samples) {
            h.record(s);
            expectedSum += s;
        }
        long total = 0;
        for (int i = 0; i < h.buckets.length(); i++) {
            total += h.buckets.get(i);
        }
        assertEquals(samples.length, total);
        assertEquals(samples.length, h.count);
        assertEquals(expectedSum, h.sumMs);
    }
}

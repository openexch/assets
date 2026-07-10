// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import org.junit.Test;

import static com.openexchange.assets.bridge.BridgeFilter.Action.FORWARD;
import static com.openexchange.assets.bridge.BridgeFilter.Action.HALT;
import static com.openexchange.assets.bridge.BridgeFilter.Action.SKIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pins the bridge's money-critical forwarding semantics: startup skip below the watermark,
 * inclusive boundary re-delivery, dense-tradeId dedupe and gap-halt (latched), and the
 * explicit haltOnGap=false operator override. These rules are what make the stateless
 * bridge safe to crash, restart, duplicate, and re-point at a different journal source.
 */
public class BridgeFilterTest {

    @Test
    public void startupSkipsBelowWatermarkAndRedeliversTheBoundaryGroupInclusively() {
        // AE says: consumePosition=1000, lastAppliedTradeId=50.
        BridgeFilter f = new BridgeFilter(1000L, 50L, true);

        // Terminals strictly below the watermark were applied before this epoch.
        assertEquals(SKIP, f.onTerminal(999L));
        // The boundary command's event group (egressSeq == watermark) is re-delivered:
        // the AE's hold-scoped release makes it a no-op, losing it would be unrecoverable.
        assertEquals(FORWARD, f.onTerminal(1000L));
        assertEquals(FORWARD, f.onTerminal(1001L));

        // Trades dedupe on the dense id regardless of egressSeq.
        assertEquals(SKIP, f.onTrade(999L, 49L));
        assertEquals(SKIP, f.onTrade(1000L, 50L));
        assertEquals(FORWARD, f.onTrade(1000L, 51L));
        assertEquals(2L, f.skippedTrades());
        assertEquals(1L, f.skippedTerminals());
        assertEquals(51L, f.lastForwardedTradeId());
    }

    @Test
    public void denseTradeSequenceForwardsAndDuplicatesSkipEvenAfterProgress() {
        BridgeFilter f = new BridgeFilter(0L, 0L, true);
        assertEquals(FORWARD, f.onTrade(10L, 1L));
        assertEquals(FORWARD, f.onTrade(10L, 2L)); // ties in egressSeq are normal (one command, many trades)
        assertEquals(FORWARD, f.onTrade(12L, 3L));
        // Replay overlap / restart-duplicate recording re-delivers old trades: dedupe by id.
        assertEquals(SKIP, f.onTrade(10L, 2L));
        assertEquals(SKIP, f.onTrade(12L, 3L));
        assertEquals(FORWARD, f.onTrade(15L, 4L));
        assertFalse(f.halted());
        assertEquals(0L, f.gapsDetected());
    }

    @Test
    public void tradeGapHaltsAndLatchesForever() {
        BridgeFilter f = new BridgeFilter(0L, 10L, true);
        assertEquals(FORWARD, f.onTrade(100L, 11L));
        // 13 skips 12: a lost settlement. Nothing may be forwarded past it.
        assertEquals(HALT, f.onTrade(110L, 13L));
        assertTrue(f.halted());
        assertEquals(1L, f.gapsDetected());
        // Latched: even the "missing" 12 arriving later must not resume a halted filter
        // (order is broken; a human decides how to recover).
        assertEquals(HALT, f.onTrade(105L, 12L));
        assertEquals(HALT, f.onTerminal(120L));
        assertEquals(11L, f.lastForwardedTradeId());
    }

    @Test
    public void haltOnGapFalseForwardsPastTheGapButCountsIt() {
        BridgeFilter f = new BridgeFilter(0L, 10L, false);
        assertEquals(FORWARD, f.onTrade(100L, 11L));
        assertEquals(FORWARD, f.onTrade(110L, 13L)); // operator override: forward, count
        assertEquals(1L, f.gapsDetected());
        assertFalse(f.halted());
        assertEquals(13L, f.lastForwardedTradeId());
        // The late 12 is now a duplicate-by-ordering: skipped by the dense dedupe.
        assertEquals(SKIP, f.onTrade(105L, 12L));
    }

    @Test
    public void retentionRaceSurfacesAsAGapNotASilentSkip() {
        // AE at tradeId 50; the earliest retained journal trade is 60 (journal purged past
        // the bridge's needs, or bridge down too long): that MUST halt, not silently skip.
        BridgeFilter f = new BridgeFilter(1000L, 50L, true);
        assertEquals(HALT, f.onTrade(2000L, 60L));
        assertTrue(f.halted());
    }

    @Test
    public void freshLedgerEpochForwardsFromTradeOne() {
        BridgeFilter f = new BridgeFilter(0L, 0L, true);
        assertEquals(FORWARD, f.onTerminal(0L));
        assertEquals(FORWARD, f.onTrade(1L, 1L));
        assertEquals(1L, f.lastForwardedTradeId());
    }
}

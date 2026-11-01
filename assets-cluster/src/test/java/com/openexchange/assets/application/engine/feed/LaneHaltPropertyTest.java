// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S10' 4R (C1, C2, C4, C5): a seeded property over the lane-halt table, checked against an independent hole model
 * (what each identity's FeedHalts require, never the core's own fields). Per step one of: a GAP FeedHalt from one of ten
 * identities (two sessions each, some stale), the next feed entry, a SOURCE_SCHEMA FeedHalt, {@code 76 {GAP, 0}}, an
 * ADMIN SUSPEND or OPEN, a snapshot + restore with the next entries seconds later.
 *
 * <p>After every step: (safety) the BRIDGE_HALT bit is clear only when the model has no open hole, no overflow and no
 * slot; (liveness) with nothing open, no operator SUSPEND and nothing awaiting OPEN the gate is OPEN; the restore
 * invariants hold; the table equals the model's open identities (at most 8, the overflow beyond). Non-vacuity (C5):
 * automatic reopens, reopens after two concurrent records, operator-cleared overflows, cross-session merges, stale
 * FeedHalts, NONGAP-blocked reaches and restores are each > 0 over the run.</p>
 */
public class LaneHaltPropertyTest {

    private static final int SEEDS = Integer.getInteger("laneHalt.seeds", 2000);
    private static final int STEPS = 160;
    private static final int BRIDGE_BIT = 1 << GateReason.BRIDGE_HALT.code();

    private long autoReopens;
    private long twoRecordReopens;
    private long overflowOperatorClears;
    private long crossSessionMerges;
    private long stale;
    private long nonGapBlocked;
    private long restores;
    private long coreMerges;

    @Test
    public void lanesHolesAndTheGate() {
        for (int seed = 1; seed <= SEEDS; seed++) {
            run(seed);
        }
        System.out.printf("[LANE-HALT-PROPERTY] seeds=%d autoReopens=%d twoRecordReopens=%d overflowOperatorClears=%d "
                        + "crossSessionMerges=%d stale=%d nonGapBlocked=%d restores=%d coreMerges=%d%n", SEEDS, autoReopens, twoRecordReopens,
                overflowOperatorClears, crossSessionMerges, stale, nonGapBlocked, restores, coreMerges);
        assertTrue("non-vacuity: automatic reopens", autoReopens > 0);
        assertTrue("non-vacuity: reopens after two concurrent records", twoRecordReopens > 0);
        assertTrue("non-vacuity: overflows cleared by the operator", overflowOperatorClears > 0);
        assertTrue("non-vacuity: cross-session merges", crossSessionMerges > 0);
        assertTrue("non-vacuity: stale FeedHalts", stale > 0);
        assertTrue("non-vacuity: GAP reaches blocked by NONGAP_AWAITING_OPEN", nonGapBlocked > 0);
        assertTrue("non-vacuity: restores", restores > 0);
        assertTrue("non-vacuity: core-detected GAPs merged into the identity-0 record", coreMerges > 0);
    }

    /** One identity's requirement: its hole is open while {@code c < seen - 1}. */
    private static final class Need {
        long seen;
        long session;
        boolean shared;
    }

    private void run(final int seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        V3Rig r = V3Rig.open();
        final Map<Long, Need> need = new HashMap<>();
        boolean overflow = false;
        boolean slot = false;
        long slotExpected = 0;
        boolean awaitingOpen = false;
        boolean admin = false;
        peakOpen = 0;
        long counterBase = 0;
        long seedReopens = 0;
        final java.util.ArrayDeque<String> trace = new java.util.ArrayDeque<>();
        for (int step = 0; step < STEPS; step++) {
            final long c = r.cursor();
            final int openBefore = openCount(need, c);
            final int a = rnd.nextInt(100);
            final String what;
            final int bitsBefore = r.globalReasons();
            boolean adminOpened = false;
            if (a < 7) {
                // review FIX-1 / NOTE-9: a frame above c + 1, refused: the core's own GAP record (identity 0) merges seen = max
                final long s = c + 2 + rnd.nextInt(5);
                assertTrue(!r.configAt(s, V3Rig.EPOCH, ++r.egress, r.feed().meConfigVersion() + 1,
                        com.openexchange.assets.determinism.V3Genesis.ME_BEHAVIOR, com.openexchange.assets.determinism.V3Genesis.ME_CAPACITY,
                        com.openexchange.assets.determinism.V3Genesis.ME_MAX_WIRE, com.openexchange.assets.domain.ConfigDecision.BUMP));
                final Need n = open(need, FeedState.CORE_DETECTED_IDENTITY, c);
                if (n != null) {
                    n.seen = Math.max(n.seen, s);
                    coreMerges++;
                } else if (openCount(need, c) < FeedState.LANE_HALT_CAPACITY) {
                    final Need m = new Need();
                    m.seen = s;
                    m.session = -1;
                    need.put(FeedState.CORE_DETECTED_IDENTITY, m);
                } else {
                    overflow = true;
                }
                what = "core-detected GAP {" + (c + 1) + ", " + s + "}";
            } else if (a < 40) {
                final long identity = 1 + rnd.nextInt(10);
                final long session = identity * 1000 + (rnd.nextInt(7) == 0 ? 2 : 1);
                final long expected;
                final long seen;
                if (rnd.nextInt(8) == 0 && c >= 2) {
                    expected = c - 1;
                    seen = c + rnd.nextInt(2); // c >= seen - 1: stale
                } else {
                    expected = c + 1 + rnd.nextInt(3);
                    seen = expected + 1 + rnd.nextInt(6);
                }
                final AdminResult ack = AutoReopenScenarios.gap(r, session, identity, expected, seen);
                what = "GAP id " + identity + " session " + session + " {" + expected + ", " + seen + "} -> " + ack;
                if (c >= seen - 1) {
                    assertEquals(seed + "/" + step + " " + what + ": stale", AdminResult.DUPLICATE, ack);
                    stale++;
                } else {
                    final Need n = open(need, identity, c);
                    if (n != null) {
                        if (n.shared || n.session != session) {
                            crossSessionMerges++;
                            n.seen = Math.max(n.seen, seen); // what EVERY holder of the identity requires
                            n.shared = true;
                        } else {
                            n.seen = seen; // a lone lane's own current view
                        }
                    } else if (openCount(need, c) < FeedState.LANE_HALT_CAPACITY) {
                        final Need m = new Need();
                        m.seen = seen;
                        m.session = session;
                        need.put(identity, m);
                    } else {
                        overflow = true;
                    }
                }
            } else if (a < 78) {
                assertTrue(r.bump(r.feed().meConfigVersion() + 1));
                what = "entry " + r.cursor();
                if (slot && r.cursor() >= slotExpected) {
                    slot = false;
                }
            } else if (a < 82) {
                if (!slot) {
                    final AdminResult ack = AutoReopenScenarios.halt(r, 9_001, 99, FeedHaltReason.SOURCE_SCHEMA, c + 1, c + 1);
                    assertEquals(seed + "/" + step + " SOURCE_SCHEMA", AdminResult.ACCEPTED, ack);
                    slot = true;
                    slotExpected = c + 1;
                    awaitingOpen = true;
                }
                what = "SOURCE_SCHEMA {" + (c + 1) + "}";
            } else if (a < 86) {
                r.mark();
                r.clear(FeedHaltReason.GAP, 0);
                final AdminResult ack = r.onlyAck().result();
                final boolean any = openCount(need, c) > 0 || overflow;
                assertEquals(seed + "/" + step + " 76 {GAP, 0}", any ? AdminResult.ACCEPTED : AdminResult.DUPLICATE, ack);
                if (any) {
                    if (overflow) {
                        overflowOperatorClears++;
                    }
                    need.clear();
                    overflow = false;
                    awaitingOpen = true;
                }
                what = "76 {GAP, 0} -> " + ack;
            } else if (a < 89) {
                AutoReopenScenarios.admin(r, GateState.SUSPENDED, GateReason.ADMIN);
                admin = true;
                what = "ADMIN SUSPEND";
            } else if (a < 97) {
                final AdminResult ack = AutoReopenScenarios.admin(r, GateState.OPEN, GateReason.ADMIN).result();
                final boolean latched = openCount(need, c) > 0 || overflow || slot;
                if ((bitsBefore & BRIDGE_BIT) != 0 && latched) {
                    assertEquals(seed + "/" + step + " ADMIN OPEN while latched", AdminResult.REJECTED, ack);
                } else {
                    assertTrue(seed + "/" + step + " ADMIN OPEN", ack != AdminResult.REJECTED);
                    admin = false;
                    awaitingOpen = false;
                    adminOpened = true;
                }
                what = "ADMIN OPEN -> " + ack;
            } else {
                counterBase += r.feed().gateAutoReopenCount();
                r = r.fork();
                r.now += 2_000 + rnd.nextInt(5_000); // the next entries land seconds after the restore (C2)
                restores++;
                what = "snapshot + restore";
            }
            final long cNow = r.cursor();
            trace.addLast(step + ": " + what + " c=" + cNow);
            if (trace.size() > 16) {
                trace.removeFirst();
            }
            try {
                seedReopens += check(r, need, overflow, slot, awaitingOpen, admin, a, bitsBefore, adminOpened, openBefore,
                        "seed " + seed + " step " + step + " (" + what + ") c=" + cNow);
            } catch (final AssertionError e) {
                throw new AssertionError(e.getMessage() + "\n  last steps:\n    " + String.join("\n    ", trace)
                        + "\n  model " + need.keySet() + " -> " + seenOf(need) + " table " + table(r.feed()), e);
            }
        }
        assertEquals("seed " + seed + ": assets_gate_auto_reopen_total counts the model's automatic opens", seedReopens,
                r.feed().gateAutoReopenCount() + counterBase);
    }

    private static String seenOf(final Map<Long, Need> need) {
        final StringBuilder b = new StringBuilder();
        need.forEach((k, v) -> b.append(k).append(':').append(v.seen).append('/').append(v.session).append(' '));
        return b.toString();
    }

    private static String table(final FeedState f) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < f.laneHaltCount(); i++) {
            b.append(f.laneHaltIdentity(i)).append(':').append(f.laneHaltSeen(i)).append('/').append(f.laneHaltSession(i)).append(' ');
        }
        return b.append(f.haltOverflow() ? "+overflow" : "").toString();
    }

    private int peakOpen;

    /** The per-step checks; returns 1 when this step was an automatic reopen. */
    private int check(final V3Rig r, final Map<Long, Need> need, final boolean overflow, final boolean slot,
                      final boolean awaitingOpen, final boolean admin, final int a, final int bitsBefore,
                      final boolean adminOpened, final int openBefore, final String where) {
        final long cNow = r.cursor();
        int reopened = 0;
        {
            final int open = openCount(need, cNow);
            peakOpen = Math.max(peakOpen, open);
            final int bits = r.globalReasons();
            final boolean holesOpen = open > 0 || overflow || slot;
            // safety: the bit is never clear while the model holds a hole, an overflow or a slot latch
            if ((bits & BRIDGE_BIT) == 0 && holesOpen) {
                fail(where + ": BRIDGE_HALT clear with " + open + " open holes, overflow " + overflow + ", slot " + slot);
            }
            if (r.engine.gate().state(OrderEntryGate.GLOBAL) == GateState.OPEN && holesOpen) {
                fail(where + ": gate OPEN with a hole open");
            }
            // liveness: nothing open, no operator SUSPEND, nothing awaiting OPEN -> OPEN
            if (!holesOpen && !awaitingOpen && !admin) {
                assertEquals(where + ": nothing holds the gate", 0, bits);
            }
            if (openBefore > 0 && !holesOpen && awaitingOpen && (bits & BRIDGE_BIT) != 0 && a >= 40 && a < 78) {
                nonGapBlocked++; // the last GAP hole reached, NONGAP_AWAITING_OPEN kept the bit
            }
            // the table is the model's open set
            assertEquals(where + ": records", Math.min(open, FeedState.LANE_HALT_CAPACITY), r.feed().laneHaltCount());
            assertEquals(where + ": overflow", overflow, r.feed().haltOverflow());
            if ((bitsBefore & BRIDGE_BIT) != 0 && bits == 0 && !adminOpened) {
                autoReopens++;
                reopened = 1;
                if (peakOpen >= 2) {
                    twoRecordReopens++;
                }
            }
            if ((bits & BRIDGE_BIT) == 0) {
                peakOpen = 0;
            }
            r.engine.verifyRestoredInvariants();
        }
        return reopened;
    }

    private static Need open(final Map<Long, Need> need, final long identity, final long c) {
        final Need n = need.get(identity);
        return n != null && c < n.seen - 1 ? n : null;
    }

    private static int openCount(final Map<Long, Need> need, final long c) {
        need.values().removeIf(n -> c >= n.seen - 1);
        return need.size();
    }
}

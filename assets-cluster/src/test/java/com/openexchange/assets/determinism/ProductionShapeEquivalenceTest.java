// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.order.OrderEngine;
import com.openexchange.cluster.testkit.GoldenAssert;
import com.openexchange.cluster.testkit.ScenarioText;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Harness B4 around the production-shaped lifecycle corpus ({@link LifecycleTrafficGenerator}, seed
 * {@link LifecycleTrafficGenerator#SEED}, committed as {@code determinism/core_lifecycle_production_shape.scenario}).
 *
 * <p>The file sits in the corpus directory, so {@link DeterminismCorpusTest} (golden, run twice, wall clock) and
 * {@link SnapshotReplayEquivalenceTest} (the kit splice at EVERY cut: 1,504 lines, 1,505 cuts, measured 10.6 s) already
 * cover it. This class adds what those cannot see: the file is the generator's output, the generator's predictions hold
 * against the engine (so the stream exercises what it claims to), and the mix covers every lifecycle path.
 * {@code -Dcore.scenario.large=true} adds a 30,000-line stream from the same generator with a strided splice
 * (every {@code n / 48}-th cut plus every cut within 3 lines of a SNAPSHOT, CORECONFIG, GATE or RISKCFG line), using the
 * kit runner's four checks. {@code -Dcore.scenario.regenerate=true} rewrites the committed scenario first.</p>
 */
public class ProductionShapeEquivalenceTest {

    static final Path FILE = Path.of("src", "test", "resources", "determinism", LifecycleTrafficGenerator.NAME + ".scenario");
    private static final Set<String> BOUNDARY_VERBS = Set.of("SNAPSHOT", "CORECONFIG", "GATE", "RISKCFG");
    private static final int LARGE_LINES = 30_000;

    private static List<String> raw;
    private static List<String> lines;

    @BeforeClass
    public static void load() throws IOException {
        if (Boolean.getBoolean("core.scenario.regenerate")) {
            Files.write(FILE, LifecycleTrafficGenerator.production().lines(), StandardCharsets.UTF_8);
            System.out.println("[CORPUS] wrote " + FILE);
        }
        raw = Files.readAllLines(FILE, StandardCharsets.UTF_8);
        lines = ScenarioText.executableLines(raw);
    }

    /** The committed file is exactly the generator's output: nobody edited it by hand. */
    @Test
    public void committedScenarioIsTheGeneratorOutput() {
        assertEquals("regenerate with -Dcore.scenario.regenerate=true", LifecycleTrafficGenerator.production().lines(), raw);
        assertTrue("1,200 to 1,800 executable lines, got " + lines.size(), lines.size() >= 1_200 && lines.size() <= 1_800);
    }

    /**
     * The generator predicts the core without reading it; a prediction the engine does not share shows up as an
     * outcome that matches no pending dispatch, a trade gap, a different accept count or an unexpected order fault.
     */
    @Test
    public void generatorPredictionsHoldAgainstTheEngine() {
        final LifecycleTrafficGenerator.Generated g = LifecycleTrafficGenerator.production();
        final AssetsScenarioRunner runner = assertInStep(g);
        System.out.println("[B4] " + g.stats());
        System.out.println("[B4] events " + eventCounts(runner.renderedEvents()));
    }

    /** The production-shape mix is present: every lifecycle path the corpus is meant to cover appears. */
    @Test
    public void theMixCoversEveryLifecyclePath() {
        final AssetsScenarioRunner runner = AssetsScenarioRunner.newRunner();
        runner.execAll(lines);
        final Map<String, Integer> c = eventCounts(runner.renderedEvents());
        for (final String k : List.of("ORDEREVENT kind=ACCEPTED", "ORDEREVENT kind=ACK_NEW", "ORDEREVENT kind=FILL status=FILLED",
                "ORDEREVENT kind=FILL status=PARTIALLY_FILLED", "ORDEREVENT kind=CANCELLED", "ORDEREVENT kind=EXPIRED",
                "ORDEREVENT kind=TRIGGERED", "ORDEREVENT kind=SLICE_SUBMITTED", "ORDEREVENT kind=REPLACED", "ORDEREVENT kind=REJECTED",
                "ORDERREJECT reason=GATE_CLOSED", "ORDERREJECT reason=PRICE_OFF_TICK", "ORDERREJECT reason=REQUEST_ID_PAYLOAD_MISMATCH",
                "ORDERREJECT reason=INSUFFICIENT_FUNDS", "ORDERREJECT reason=UNKNOWN_ORDER", "ORDERREJECT reason=NOT_OWNER",
                "ORDERREJECT reason=QTY_OFF_LOT", "ORDERREJECT reason=PRICE_COLLAR", "ORDERREJECT reason=NO_REFERENCE_PRICE",
                "ORDERREJECT reason=STOP_WOULD_TRIGGER_IMMEDIATELY", "ORDERREJECT reason=MISSING_REQUEST_ID",
                "ORDERREJECT reason=DUPLICATE_CLIENT_ORDER_ID", "ORDERACK dup=1",
                "WITHDRAWACK", "WITHDRAWREJECT", "SETTLED", "SETTLEFAULT", "ORDERFAULT", "MEDISPATCH kind=CANCEL", "MEDISPATCH kind=AMEND",
                "ORDERSNAPEND", "PENDINGEND")) {
            assertTrue("no '" + k + "' in the production shape: " + c, c.getOrDefault(k, 0) > 0);
        }
        final LifecycleTrafficGenerator.Stats s = LifecycleTrafficGenerator.production().stats();
        assertTrue("duplicate trades " + s, s.duplicateTrades() > 0);
        assertTrue("terminals before the last trade " + s, s.earlyTerminals() > 0);
        assertTrue("stale ticks " + s, s.staleTicks() > 0);
        assertTrue("one-sided ticks " + s, s.oneSidedTicks() > 0);
        assertTrue("exact requestId replays " + s, s.replays() > 0);
        assertTrue("payload mismatches " + s, s.mismatches() > 0);
        assertEquals("no trade refused by the ledger", 0, (int) c.getOrDefault("SETTLEREJECTED", 0));
    }

    /** {@code -Dcore.scenario.large=true}: 30k lines, run twice, predictions held, strided splice. */
    @Test
    public void largeStreamIsDeterministicAcrossRestarts() {
        assumeTrue("30k-line run behind -Dcore.scenario.large=true", Boolean.getBoolean("core.scenario.large"));
        final long t0 = System.nanoTime();
        final LifecycleTrafficGenerator.Generated g = LifecycleTrafficGenerator.generate(LifecycleTrafficGenerator.SEED, LARGE_LINES);
        final List<String> large = ScenarioText.executableLines(g.lines());
        assertInStep(g);
        assertEquals(AssetsScenarioRunner.runLines(large), AssetsScenarioRunner.runLines(large));
        final SortedSet<Integer> cuts = stridedCuts(large, Math.max(1, large.size() / 48));
        splice(large, cuts);
        System.out.printf("[B4] large: %d lines, %d cuts, %.1f s, %s%n", large.size(), cuts.size(),
                (System.nanoTime() - t0) / 1e9, g.stats());
    }

    // ---------------------------------------------------------------- shared checks

    /** Run without the SNAPSHOT restarts (node-local counters restart at 0; splice tests prove SNAPSHOT transparent). */
    static AssetsScenarioRunner assertInStep(final LifecycleTrafficGenerator.Generated g) {
        final LifecycleTrafficGenerator.Stats s = g.stats();
        final AssetsScenarioRunner runner = AssetsScenarioRunner.newRunner();
        runner.execAll(ScenarioText.executableLines(g.lines()).stream().filter(l -> !"SNAPSHOT".equals(l)).toList());
        final OrderEngine e = runner.engine().orders();
        assertEquals("outcomes that matched no pending dispatch", 0, e.staleOutcomeCount());
        assertEquals("outcomes naming an already learned leg", 0, e.redundantOutcomeCount());
        assertEquals("trade gaps", 0, e.tradeGapCount());
        assertEquals("accepted creates", s.predictedAccepted(), e.acceptedCount());
        assertEquals("minted order ids", s.predictedAccepted() + 1, runner.engine().getNextOrderId());
        assertEquals("order faults = injected terminals before the last trade", s.earlyTerminals(), e.orderFaultCount());
        assertEquals("open orders at the end", s.openAtEnd(), e.orders().size());
        assertEquals("triggers", s.triggers(), e.triggeredCount());
        assertEquals("iceberg refills", s.slices(), e.sliceCount());
        assertEquals("stale ticks", s.staleTicks(), e.staleTicks());
        assertEquals("invalid ticks", 0, e.invalidTickCount());
        return runner;
    }

    /** Every {@code stride}-th cut, the last one, and every cut within 3 lines of a boundary line. */
    static SortedSet<Integer> stridedCuts(final List<String> lines, final int stride) {
        final SortedSet<Integer> cuts = new TreeSet<>();
        final int n = lines.size();
        for (int k = 0; k <= n; k += stride) {
            cuts.add(k);
        }
        cuts.add(n);
        for (int i = 0; i < n; i++) {
            if (BOUNDARY_VERBS.contains(ScenarioText.tokens(lines.get(i))[0])) {
                for (int k = Math.max(0, i - 3); k <= Math.min(n, i + 4); k++) {
                    cuts.add(k); // cut k = restart before line k: three lines either side of the boundary line
                }
            }
        }
        return cuts;
    }

    /** A+B vs A+SNAPSHOT+B at each cut on fresh replicas (SnapshotSpliceRunner's checks, with chosen cuts). */
    static void splice(final List<String> lines, final SortedSet<Integer> cuts) {
        final AssetsReplica baseline = new AssetsReplica();
        lines.forEach(baseline::apply);
        final List<String> expectedEvents = List.copyOf(baseline.events());
        final byte[] expectedBytes = baseline.snapshot();
        assertArrayEquals("baseline snapshot is not reproducible", expectedBytes, baseline.snapshot());
        for (final int k : cuts) {
            final AssetsReplica r = new AssetsReplica();
            for (int i = 0; i < k; i++) {
                r.apply(lines.get(i));
            }
            final byte[] atCut = r.snapshot();
            if (!Arrays.equals(atCut, r.snapshot())) {
                fail("snapshot at cut " + k + " is not reproducible");
            }
            r.restore(atCut);
            if (!Arrays.equals(atCut, r.snapshot())) {
                fail("restore at cut " + k + " does not reproduce the bytes it was restored from");
            }
            for (int i = k; i < lines.size(); i++) {
                r.apply(lines.get(i));
            }
            final List<String> got = r.events();
            if (!expectedEvents.equals(got)) {
                fail("events diverge with a snapshot spliced at cut " + k + " (before '" + (k < lines.size() ? lines.get(k) : "<end>")
                        + "'):\n" + GoldenAssert.diff(String.join("\n", expectedEvents), String.join("\n", got)));
            }
            if (!Arrays.equals(expectedBytes, r.snapshot())) {
                fail("final snapshot bytes diverge with a snapshot spliced at cut " + k);
            }
        }
    }

    /** Event lines bucketed by line type, plus kind/status/reason/dup sub-keys, for the mix report. */
    static Map<String, Integer> eventCounts(final List<String> events) {
        final Map<String, Integer> c = new TreeMap<>();
        for (final String e : events) {
            final String[] t = e.split(" ");
            c.merge(t[0], 1, Integer::sum);
            for (final String tok : t) {
                final boolean keyed = (tok.startsWith("reason=") && t[0].equals("ORDERREJECT"))
                        || (tok.equals("dup=1") && t[0].equals("ORDERACK"))
                        || (tok.startsWith("kind=") && (t[0].equals("MEDISPATCH") || t[0].equals("ORDEREVENT")));
                if (!keyed) {
                    continue;
                }
                c.merge(t[0] + " " + tok, 1, Integer::sum);
                if (tok.equals("kind=FILL")) {
                    for (final String st : t) {
                        if (st.startsWith("status=")) {
                            c.merge(t[0] + " " + tok + " " + st, 1, Integer::sum);
                        }
                    }
                }
            }
        }
        return c;
    }
}

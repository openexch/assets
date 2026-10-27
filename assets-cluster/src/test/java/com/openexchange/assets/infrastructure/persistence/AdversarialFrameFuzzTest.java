// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.infrastructure.persistence.FrameMutator.Category;
import com.openexchange.assets.infrastructure.persistence.FrameMutator.Judgement;
import com.openexchange.assets.infrastructure.persistence.FrameMutator.Mutant;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S8, harness B5 adversarial mode at the frame level. For seeds 1..4 the valid story of {@link CoreFrameStory} is
 * replayed into a fuzzed replica F and a reference replica R. Before steps of the story, hostile frames derived from the
 * story's own valid frames ({@link FrameMutator}) are dispatched into F through {@code dispatchTrusted}:
 * <ul>
 *   <li>MUST_DROP (truncation at every length, length beyond the buffer, short/oversized blockLength, wrong schemaId or
 *       version, unknown templateId, group count beyond the frame, short group block, invalid raw enum bytes, also in a
 *       wider-than-generated block) go straight into F: a drop counter must move, no exception may escape, the image and
 *       the event stream must not change.</li>
 *   <li>OBSERVED (bit flips in header or body, a templateId swapped to another known one) are judged in a sandbox
 *       restored from F's image first: no exception may escape; a dropped one must leave the sandbox unchanged and is
 *       then dispatched into F as well; an accepted one (a different but well-formed command) stays out of F.</li>
 *   <li>EQUIVALENT (wider blocks with the same values, trailing bytes) are applied in two sandboxes next to the original
 *       frame: when the gate admits the variant, its effect must equal the original's.</li>
 * </ul>
 * After the run F must equal R byte for byte (image) and event for event, with {@code applyErrorCount == 0}: malformed
 * input never partially mutates and never fences a healthy engine. Reject counters are printed per category.
 */
@RunWith(Parameterized.class)
public class AdversarialFrameFuzzTest {

    private static final int FRAMES = 300;
    private static final int MAX_FINDINGS_SHOWN = 20;

    /** Per category: dispatched, schema, decode, unknownTemplate, unhandled, accepted, equivalent, faults, findings. */
    private static final Map<Category, long[]> TOTAL = new EnumMap<>(Category.class);
    private static final String[] COLUMNS = {"dispatched", "schemaRej", "decodeRej", "unknownTpl", "unhandledTpl", "accepted",
        "equivalent", "faults", "findings"};

    /** Seeds 1..4 on the v1/v2 story and on the behaviour-3 story (RC2 S7 review F-1: the 66/67/68/76 serve path and the cursor). */
    @Parameters(name = "seed {0} v3 {1}")
    public static Collection<Object[]> seeds() {
        final List<Object[]> out = new ArrayList<>();
        for (final boolean v3 : new boolean[] {false, true}) {
            for (long seed = 1; seed <= 4; seed++) {
                out.add(new Object[] {seed, v3});
            }
        }
        return out;
    }

    private final long seed;
    private final boolean v3;
    private final Map<Category, long[]> tally = new EnumMap<>(Category.class);
    private final List<String> findings = new ArrayList<>();
    private boolean replaced;
    private int stepIndex;

    public AdversarialFrameFuzzTest(final long seed, final boolean v3) {
        this.seed = seed;
        this.v3 = v3;
    }

    /** v3 only (belt review F-1): 62 mutants have their own tally, the one category-free exemption of the drop count. */
    private final Map<Category, long[]> tally62 = new EnumMap<>(Category.class);

    private long[] row(final Mutant m) {
        if (v3 && FrameMutator.templateOf(m.original()) == com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder.TEMPLATE_ID) {
            return tally62.computeIfAbsent(m.category(), k -> new long[COLUMNS.length]);
        }
        return row(m.category());
    }

    private long[] row(final Category c) {
        return tally.computeIfAbsent(c, k -> new long[COLUMNS.length]);
    }

    private static long[] counters(final FrameReplica r) {
        return new long[] {r.demuxer.schemaRejectCount(), r.demuxer.decodeRejectCount(), r.demuxer.unknownTemplateCount(),
            r.demuxer.unhandledTemplateCount()};
    }

    /** Adds the counter deltas to the tally row; true when any drop counter moved. */
    private boolean recordDrops(final long[] row, final long[] before, final long[] after) {
        boolean dropped = false;
        for (int k = 0; k < 4; k++) {
            row[1 + k] += after[k] - before[k];
            dropped |= after[k] > before[k];
        }
        return dropped;
    }

    private void finding(final Mutant m, final String what) {
        row(m)[8]++;
        final int n = Math.min(m.length(), m.buffer().length);
        findings.add("seed=" + seed + " beforeStep=" + stepIndex + " category=" + m.category() + " template="
                + FrameMutator.templateOf(m.original()) + " (" + m.note() + ") length=" + m.length() + ": " + what
                + "\n    hex=" + HexFormat.of().formatHex(m.buffer(), 0, n)
                + "\n    original=" + HexFormat.of().formatHex(m.original()));
    }

    private static void dispatch(final FrameReplica r, final Mutant m, final long timestamp) {
        r.dispatchRaw(new UnsafeBuffer(m.buffer()), 0, m.length(), r.engine.getLogPosition(), timestamp);
    }

    private static String describe(final Throwable t) {
        final StackTraceElement[] st = t.getStackTrace();
        final StringBuilder b = new StringBuilder(t.toString());
        for (int i = 0; i < Math.min(4, st.length); i++) {
            b.append(" @ ").append(st[i]);
        }
        return b.toString();
    }

    /**
     * Dispatch into F, which must drop it: no throw, a counter moves, no byte and no event changes. On a violation the
     * finding is recorded and F is rebuilt from its pre-image so later checks stay meaningful.
     */
    private FrameReplica intoF(final FrameReplica f, final Mutant m, final long timestamp, final boolean countDispatch) {
        if (v3) {
            return intoFv3(f, m, timestamp, countDispatch);
        }
        final long[] row = row(m);
        if (countDispatch) {
            row[0]++;
        }
        final byte[] pre = f.image();
        final int events = f.eventCount();
        final long[] before = counters(f);
        try {
            dispatch(f, m, timestamp);
        } catch (final Throwable t) {
            row[7]++;
            finding(m, "exception escaped dispatch: " + describe(t));
            replaced = true;
            return FrameReplica.restore(pre);
        }
        final boolean dropped = recordDrops(countDispatch ? row : new long[COLUMNS.length], before, counters(f));
        if (!dropped) {
            row[5]++;
            finding(m, "a malformed frame was ACCEPTED (no drop counter moved)");
        }
        if (f.eventCount() != events || !Arrays.equals(pre, f.image())) {
            finding(m, "state or events changed: " + (f.eventCount() - events) + " new events " + f.rendered(events)
                    + (Arrays.equals(pre, f.image()) ? "" : "; snapshot image differs"));
            replaced = true;
            return FrameReplica.restore(pre);
        }
        return f;
    }

    /**
     * RC2 S7 (review F-1): the MUST_DROP judgement on a behaviour-3 replica, in a sandbox restored from F's image. Under v3
     * a decode poison reaches the engine (5.4) and a 62 batch is judged entry by entry (3.5), so the rule is the reviewer's:
     * no exception; a single-frame template must move a drop counter (a 62 may not, when an earlier entry stopped the batch);
     * the only events allowed are the refusal and latch events (SettleRejected, AdmissionState, OrderFault FEED_HALT,
     * FEEDLATCH); with the latch slot and GLOBAL's bitmask put back, the image must equal the pre-image, or, when the
     * cursor moved (a well-formed exactly-next entry of a partly poisoned 62 applied, 3.5), every restore invariant must hold
     * and every per-asset total must be the pre-image's. A mutant that changes nothing is also dispatched into F; any other
     * stays out of F (its latch would shut a gate the reference never shuts).
     */
    private FrameReplica intoFv3(final FrameReplica f, final Mutant m, final long timestamp, final boolean countDispatch) {
        final long[] row = row(m);
        if (countDispatch) {
            row[0]++;
        }
        final byte[] pre = f.image();
        final FrameReplica sandbox = FrameReplica.restore(pre);
        final long[] before = counters(sandbox);
        try {
            dispatch(sandbox, m, timestamp);
        } catch (final Throwable t) {
            row[7]++;
            finding(m, "exception escaped dispatch (v3 sandbox): " + describe(t));
            return f;
        }
        final boolean dropped = recordDrops(countDispatch ? row : new long[COLUMNS.length], before, counters(sandbox));
        final int template = FrameMutator.templateOf(m.original());
        if (!dropped && template != com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder.TEMPLATE_ID) {
            row[5]++;
            finding(m, "a malformed frame was ACCEPTED at v3 (no drop counter moved)");
            return f;
        }
        if (sandbox.eventCount() == 0 && Arrays.equals(pre, sandbox.image())) {
            if (dropped) {
                final int events = f.eventCount();
                dispatch(f, m, timestamp);
                if (f.eventCount() != events || !Arrays.equals(pre, f.image())) {
                    finding(m, "the sandbox was unchanged but F changed");
                    replaced = true;
                    return FrameReplica.restore(pre);
                }
            }
            return f;
        }
        final String bad = v3Breach(pre, sandbox);
        if (bad != null) {
            finding(m, bad);
        } else {
            row[6]++; // latched or partly applied by design (5.4, 3.5): kept out of F
        }
        return f;
    }

    /** The v3 judgement of a sandbox that changed after a malformed frame (see {@link #intoFv3}); null when it holds. */
    private static String v3Breach(final byte[] pre, final FrameReplica sandbox) {
        final FrameReplica preReplica = FrameReplica.restore(pre);
        final com.openexchange.assets.application.engine.feed.FeedState p = preReplica.engine.feed();
        final com.openexchange.assets.application.engine.feed.FeedState q = sandbox.engine.feed();
        for (final AssetsEngineEvent e : sandbox.sink.events()) {
            final boolean allowed = e instanceof AssetsEngineEvent.AdmissionState || e instanceof AssetsEngineEvent.FeedLatch
                    || e instanceof AssetsEngineEvent.SettleRejected
                    || (e instanceof AssetsEngineEvent.OrderFault of && of.kind() == com.openexchange.assets.domain.OrderFaultKind.FEED_HALT);
            if (!allowed && q.lastAppliedJournalSeq() == p.lastAppliedJournalSeq()) {
                return "a malformed frame emitted a non-refusal event at v3 without applying anything: " + e.render();
            }
        }
        if (q.lastAppliedJournalSeq() < p.lastAppliedJournalSeq()) {
            return "the cursor moved back at v3";
        }
        if (q.lastAppliedJournalSeq() > p.lastAppliedJournalSeq()) {
            // a well-formed exactly-next entry of a partly poisoned 62 applied (3.5): invariants and conservation must hold
            try {
                sandbox.engine.verifyRestoredInvariants();
            } catch (final IllegalStateException e) {
                return "a partly applied malformed batch broke an invariant: " + e.getMessage();
            }
            for (int asset = 0; asset < com.openexchange.assets.domain.Asset.count(); asset++) {
                if (total(preReplica, asset) != total(sandbox, asset)) {
                    return "a partly applied malformed batch changed the total of asset " + asset;
                }
            }
            return null;
        }
        if (p.latched()) {
            q.writeLatch(p.latchReason(), p.latchExpectedSeq(), p.latchSeenSeq(), p.latchSkipSeq(), p.latchTradeId(),
                    p.latchMeEpochSeen(), p.latchBridgeIdentity(), p.latchLogPosition());
        } else {
            q.zeroLatch();
        }
        sandbox.engine.gate().setReasons(com.openexchange.assets.application.engine.gate.OrderEntryGate.GLOBAL,
                preReplica.engine.gate().reasons(com.openexchange.assets.application.engine.gate.OrderEntryGate.GLOBAL));
        // a refused feed entry observes the log time (it is a log entry the core judged); put the pre-image's back too
        sandbox.engine.setLastLogTime(preReplica.engine.getLastLogTime());
        return Arrays.equals(pre, sandbox.image()) ? null
                : "a refused or latched malformed frame changed more than the latch slot and the BRIDGE_HALT bit at v3";
    }

    private static long total(final FrameReplica r, final int asset) {
        final long[] sum = {0L};
        r.engine.forEachAccount(a -> sum[0] += a.available(asset) + a.locked(asset));
        return sum[0];
    }

    /** OBSERVED: judged in a sandbox restored from F's image; a dropped one is then interleaved into F. */
    private FrameReplica observed(final FrameReplica f, final Mutant m, final long timestamp) {
        final long[] row = row(m);
        row[0]++;
        final byte[] pre = f.image();
        final FrameReplica sandbox = FrameReplica.restore(pre);
        final long[] before = counters(sandbox);
        try {
            dispatch(sandbox, m, timestamp);
        } catch (final Throwable t) {
            row[7]++;
            finding(m, "exception escaped dispatch (sandbox): " + describe(t));
            return f;
        }
        if (!recordDrops(row, before, counters(sandbox))) {
            row[5]++; // a different but well-formed command: kept out of F
            return f;
        }
        if (v3) {
            return intoFv3(f, m, timestamp, false); // a dropped flip of the next feed entry may latch by design (5.4)
        }
        if (sandbox.eventCount() != 0 || !Arrays.equals(pre, sandbox.image())) {
            finding(m, "dropped, but state or events changed: " + sandbox.rendered(0));
            return f;
        }
        return intoF(f, m, timestamp, false);
    }

    /** EQUIVALENT: the variant and its original in two sandboxes; when admitted, the effects must be identical. */
    private void equivalent(final FrameReplica f, final Mutant m, final long timestamp) {
        final long[] row = row(m);
        row[0]++;
        final byte[] pre = f.image();
        final FrameReplica plain = FrameReplica.restore(pre);
        final FrameReplica variant = FrameReplica.restore(pre);
        try {
            plain.dispatchRaw(new UnsafeBuffer(m.original()), 0, m.original().length, plain.engine.getLogPosition(), timestamp);
            dispatch(variant, m, timestamp);
        } catch (final Throwable t) {
            row[7]++;
            finding(m, "exception escaped dispatch (sandbox): " + describe(t));
            return;
        }
        if (recordDrops(row, new long[4], counters(variant))) {
            if (v3 && recordDrops(new long[COLUMNS.length], new long[4], counters(plain))) {
                // v3: the original is decode poison too (the story's poisoned 67); both must have the same effect (5.4)
                if (!plain.rendered(0).equals(variant.rendered(0)) || !Arrays.equals(plain.image(), variant.image())) {
                    finding(m, "a re-encoded poison applied DIFFERENTLY from the original at v3");
                }
                return;
            }
            if (v3 && v3Breach(pre, variant) == null) {
                return; // v3: a dropped re-encoding latched or was refused by design
            }
            if (variant.eventCount() != 0 || !Arrays.equals(pre, variant.image())) {
                finding(m, "dropped, but state or events changed: " + variant.rendered(0));
            }
            return; // a conservative drop of a non-canonical encoding is allowed; it is counted
        }
        row[5]++;
        if (!plain.rendered(0).equals(variant.rendered(0)) || !Arrays.equals(plain.image(), variant.image())) {
            finding(m, "an admitted re-encoding applied DIFFERENTLY from the original: original " + plain.rendered(0)
                    + " vs variant " + variant.rendered(0));
            return;
        }
        row[6]++;
    }

    @Test
    public void malformedFramesAreDroppedAndCountedAndNeverMutateTheReplica() {
        final long t0 = System.nanoTime();
        final List<CoreFrameStory.Step> story = CoreFrameStory.build(seed, FRAMES, v3);
        final List<byte[]> sources = new ArrayList<>();
        for (final CoreFrameStory.Step s : story) {
            if (s.kind() == CoreFrameStory.Kind.FRAME) {
                sources.add(s.bytes());
            }
        }
        sources.addAll(CoreFrameStory.v7FeedFrames()); // RC2 S7 16.4: 66, 67, 68 and 76 rows of the mutation corpus
        final Map<Integer, List<byte[]>> byTemplate = new java.util.TreeMap<>();
        for (final byte[] b : sources) {
            byTemplate.computeIfAbsent(FrameMutator.templateOf(b), k -> new ArrayList<>()).add(b);
        }
        final Random rnd = new Random(seed * 1_000_003L + 17);
        final FrameMutator mutator = new FrameMutator(rnd);
        // truncation at every length for three frames (one fixed block, one batch, one two-group config), injected at random steps
        final Map<Integer, List<byte[]>> truncateBefore = new java.util.TreeMap<>();
        for (final int template : new int[] {50, 62, 70, 67}) { // RC2 S7: 67 adds a v7 grouped frame
            sources.stream().filter(b -> FrameMutator.templateOf(b) == template).findFirst().ifPresent(b ->
                    truncateBefore.computeIfAbsent(rnd.nextInt(story.size()), k -> new ArrayList<>()).add(b));
        }
        // RC2 S8: V3_RANGE (the 3.9 mutants) is drawn only in the v3 run, so the v1/v2 random stream is the RC1 one
        final Category[] drawn = Arrays.stream(Category.values()).filter(c -> c != Category.TRUNCATE && (v3 || c != Category.V3_RANGE))
                .toArray(Category[]::new);
        FrameReplica f = new FrameReplica();
        final FrameReplica reference = new FrameReplica();
        for (stepIndex = 0; stepIndex < story.size(); stepIndex++) {
            final CoreFrameStory.Step step = story.get(stepIndex);
            for (final byte[] b : truncateBefore.getOrDefault(stepIndex, List.of())) {
                for (final Mutant m : mutator.truncations(b)) {
                    f = intoF(f, m, step.timestamp(), true);
                }
            }
            if (rnd.nextInt(10) < 8) {
                for (int k = 0, n = 1 + rnd.nextInt(4); k < n; k++) {
                    final Category c = drawn[rnd.nextInt(drawn.length)];
                    // stratified: a template first (uniform over the templates the category applies to), then a frame of it,
                    // so a rare template (the trade batch, CoreConfig) is mutated as often as CreateOrder
                    final List<Integer> eligible = new ArrayList<>();
                    byTemplate.forEach((t, frames) -> {
                        if (FrameMutator.applicable(c, frames.get(0), v3)) {
                            eligible.add(t);
                        }
                    });
                    final List<byte[]> pool = byTemplate.get(eligible.get(rnd.nextInt(eligible.size())));
                    final byte[] src = pool.get(rnd.nextInt(pool.size()));
                    final Mutant m = mutator.mutate(c, src);
                    switch (c.judgement) {
                        case MUST_DROP -> f = intoF(f, m, step.timestamp(), true);
                        case OBSERVED -> f = observed(f, m, step.timestamp());
                        default -> equivalent(f, m, step.timestamp());
                    }
                }
            }
            f.apply(step);
            reference.apply(step);
        }
        final long ms = (System.nanoTime() - t0) / 1_000_000;
        print("seed " + seed + " v3 " + v3 + " (" + story.size() + " steps, " + ms + " ms)", tally);
        if (!tally62.isEmpty()) {
            print("seed " + seed + " v3 62 mutants (exempt from the drop count, 3.5)", tally62);
        }
        synchronized (TOTAL) {
            tally.forEach((c, r) -> {
                final long[] t = TOTAL.computeIfAbsent(c, k -> new long[COLUMNS.length]);
                for (int i = 0; i < r.length; i++) {
                    t[i] += r[i];
                }
            });
        }
        assertTrue(findings.size() + " finding(s):\n" + String.join("\n", findings.subList(0, Math.min(MAX_FINDINGS_SHOWN,
                findings.size()))), findings.isEmpty());
        assertEquals("a healthy engine is never fenced by malformed input", 0, f.demuxer.applyErrorCount());
        assertEquals(0, reference.demuxer.applyErrorCount());
        assertArrayEquals("F (valid + dropped frames) == R (valid frames only), byte for byte", reference.image(), f.image());
        // belt review F-2: the story's verbatim resends never trip the 3.8 duplicate belt, in F or in R
        assertEquals("no belt mismatch in F", 0L, f.engine.feed().duplicateMismatchCount());
        assertEquals("no belt mismatch in R", 0L, reference.engine.feed().duplicateMismatchCount());
        if (!replaced) {
            assertEquals("F renders exactly R's events", reference.rendered(0), f.rendered(0));
        }
        for (final Category c : Category.values()) {
            // v3: a malformed 62 whose earlier entry stopped the batch is never looked at again (3.5), so it moves no counter;
            // its mutants live in tally62 and only they are exempt (belt review F-1)
            if (c.judgement == Judgement.MUST_DROP && tally.containsKey(c)) {
                final long[] r = tally.get(c);
                assertEquals(c + ": every malformed frame is dropped exactly once", r[0], r[1] + r[2] + r[3] + r[4]);
            }
        }
    }

    private static void print(final String title, final Map<Category, long[]> rows) {
        final StringBuilder b = new StringBuilder("[FUZZ] ").append(title).append('\n');
        b.append(String.format("[FUZZ] %-24s", "category"));
        for (final String c : COLUMNS) {
            b.append(String.format(" %12s", c));
        }
        rows.forEach((c, r) -> {
            b.append(String.format("%n[FUZZ] %-24s", c));
            for (final long v : r) {
                b.append(String.format(" %12d", v));
            }
        });
        System.out.println(b);
    }

    @AfterClass
    public static void printTotals() {
        print("TOTAL seeds 1..4, v1/v2 and v3", TOTAL);
    }
}

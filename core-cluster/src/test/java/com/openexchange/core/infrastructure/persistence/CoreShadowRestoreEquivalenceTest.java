// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.determinism.RecordingCoreSink;
import com.openexchange.core.domain.CoreEventSink;
import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.log.SkipWindow;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import com.openexchange.cluster.testkit.ShadowRestoreRunner;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (frame level, no Aeron cluster): restore-and-resume equivalence of the REAL applier on the
 * production corpus log (the recorded log: 2,089 steps behind one term event, written with the kit writer to a real
 * archive and read back with the kit reader). For every pair (s, c) on the strides, EQUAL / SKIP / TAIL end with the
 * baseline's final image bytes, final cursor and event suffix after {@code max(s, c)} ({@link ShadowRestoreRunner}).
 *
 * <p>New versus: the events the probe compares carry their {@code logPosition}, so a position difference on a
 * timer, open or close frame is a divergence; the per-frame oracle ({@code event logPosition <x> != frame end <e>})
 * checks every position-carrying event against the end of the frame whose application emitted it; both P_s shift
 * directions of 11.0 are fed as candidates and each must give {@code final image differs from baseline}.</p>
 *
 * <p>Negative controls (each prints {@code NC id -> text}): (a) the SKIP gate with {@code <} instead of {@code <=};
 * (b) the gate missing on TIMER; (c) a tail reader that drops the SessionOpen principal; (d) the stamp from a
 * constant instead of the frame end.</p>
 */
public class CoreShadowRestoreEquivalenceTest {

    static final int STRIDE = 199;

    private static RecordedLogFixture fixture;
    private static RecordedLog log;

    @BeforeClass
    public static void recordTheCorpus() {
        fixture = new RecordedLogFixture();
        final CoreReplayParityTest.Written w = CoreReplayParityTest.write(fixture,
                CoreReplayParityTest.coreSteps(CoreReplayParityTest.SEED, CoreReplayParityTest.FRAMES));
        log = RecordedLog.capture(fixture.replay(w.recordingId(), 0, AeronArchive.NULL_LENGTH), 0, w.stopPosition());
        assertEquals("one frame per step plus the term event", w.steps().size() + 1, log.size());
        System.out.println("[11.1] corpus: " + log.size() + " frames, end " + log.endPosition() + ", timers "
                + log.count(FrameKind.TIMER_EVENT) + ", opens " + log.count(FrameKind.SESSION_OPEN) + ", closes "
                + log.count(FrameKind.SESSION_CLOSE));
    }

    @AfterClass
    public static void closeFixture() {
        if (fixture != null) {
            fixture.close();
        }
    }

    // ------------------------------------------------------------------------------------------ positioned events

    /** Which argument of a position-carrying sink call is the log position (the StampSink set). */
    static int positionArg(final Method m) {
        return switch (m.getName()) {
            case "onOrderEvent" -> 1;
            case "onAdmissionState" -> 5;
            case "onConfigApplied" -> 3;
            case "onOrderFault" -> 6;
            case "onOrderSnapshotEnd" -> 3;
            default -> -1;
        };
    }

    /**
     * An event sink whose rendered stream carries the log position of every position-carrying call ("render @pos"):
     * the RecordingCoreSink renders omit positions on purpose, which would hide from the suffix comparison.
     */
    static final class PositionedSink {
        final RecordingCoreSink recording = new RecordingCoreSink();
        final List<String> rendered = new ArrayList<>();
        /** Every stamped position in call order. */
        final List<Long> positions = new ArrayList<>();
        final CoreEventSink proxy;

        PositionedSink() {
            proxy = (CoreEventSink)Proxy.newProxyInstance(CoreEventSink.class.getClassLoader(),
                    new Class<?>[] {CoreEventSink.class}, (p, m, args) -> {
                        if (m.getDeclaringClass() == Object.class) {
                            return m.invoke(this, args);
                        }
                        final int before = recording.events().size();
                        final Object r;
                        try {
                            r = m.invoke(recording, args);
                        } catch (final InvocationTargetException e) {
                            throw e.getCause();
                        }
                        final int at = positionArg(m);
                        final String tag = at >= 0 && args != null && at < args.length ? " @" + args[at] : "";
                        if (at >= 0 && args != null && at < args.length) {
                            positions.add((Long)args[at]);
                        }
                        final List<CoreEngineEvent> events = recording.events();
                        if (events.size() == before) {
                            rendered.add(m.getName() + tag);
                        }
                        for (int i = before; i < events.size(); i++) {
                            rendered.add(events.get(i).render() + tag);
                        }
                        return r;
                    });
        }
    }

    /** One fresh applier per call, each with its own positioned sink (the runner's probe reads it back). */
    static final class Engines implements Supplier<CoreLogApplier> {
        final Map<CoreLogApplier, PositionedSink> sinks = new IdentityHashMap<>();

        @Override
        public CoreLogApplier get() {
            final CoreLogApplier a = new CoreLogApplier(() -> {
                throw new AssertionError("the corpus must not trip the node-local CoreConfig stop");
            });
            final PositionedSink s = new PositionedSink();
            a.setEventSink(s.proxy);
            sinks.put(a, s);
            return a;
        }

        ShadowRestoreRunner.Probe<CoreLogApplier> probe() {
            return a -> sinks.get(a).rendered;
        }
    }

    /** The applier with the stamp switched off: open, close and timer receive a constant (control d). */
    static final class ConstantStamp implements ShadowEngine {
        static final long CONSTANT = 42;
        final CoreLogApplier d;

        ConstantStamp(final CoreLogApplier delegate) {
            this.d = delegate;
        }

        public int engineId() {
            return d.engineId();
        }

        public void genesis() {
            d.genesis();
        }

        public void restore(final DirectBuffer image, final int length) {
            d.restore(image, length);
        }

        public int serialize(final MutableDirectBuffer dst) {
            return d.serialize(dst);
        }

        public void onSessionOpen(final long id, final long p, final long ts, final byte[] principal) {
            d.onSessionOpen(id, CONSTANT, ts, principal);
        }

        public void onSessionClose(final long id, final long p, final long ts, final CloseReason reason) {
            d.onSessionClose(id, CONSTANT, ts, reason);
        }

        public boolean onSessionMessage(final long id, final long p, final long ts, final DirectBuffer buffer,
                                        final int offset, final int length) {
            return d.onSessionMessage(id, p, ts, buffer, offset, length);
        }

        public int onTimerEvent(final long correlationId, final long p, final long ts) {
            return d.onTimerEvent(correlationId, CONSTANT, ts);
        }

        public void onNewLeadershipTermEvent(final long term, final long p, final long ts, final TimeUnit unit) {
            d.onNewLeadershipTermEvent(term, p, ts, unit);
        }
    }

    // ------------------------------------------------------------------------------------------ the oracle

    /**
     * Applies the corpus frame by frame and asserts that every position-carrying event emitted while a frame of kind
     * MESSAGE, TIMER, OPEN or CLOSE was applied carries that frame's end. Returns how many such events the
     * timer, open and close frames emitted (non-vacuity: the stamp is new for exactly those kinds).
     */
    static long assertEventPositionsAreFrameEnds(final boolean stampOff) {
        final Engines engines = new Engines();
        final CoreLogApplier a = engines.get();
        final ShadowEngine engine = stampOff ? new ConstantStamp(a) : CoreReplayParityTest.shadowEngine(a);
        final ReplayContainer container = new ReplayContainer(engine, ShadowRestoreRunner.DEFAULT_APP_VERSION,
                ShadowRestoreRunner.DEFAULT_VALIDATOR, ShadowTriggerListener.NONE);
        container.genesis();
        final PositionedSink sink = engines.sinks.get(a);
        long timerOpenClose = 0;
        long previous = 0;
        for (final LogFrame frame : log.frames()) {
            final int mark = sink.positions.size();
            frame.deliver(container);
            final boolean checked = frame.kind() == FrameKind.SESSION_MESSAGE || frame.kind() == FrameKind.TIMER_EVENT
                    || frame.kind() == FrameKind.SESSION_OPEN || frame.kind() == FrameKind.SESSION_CLOSE;
            for (int i = mark; i < sink.positions.size(); i++) {
                final long x = sink.positions.get(i);
                if (checked && x != frame.position()) {
                    throw new AssertionError("event logPosition " + x + " != frame end " + frame.position() + " ("
                            + frame.kind() + ", previous frame end " + previous + ")");
                }
                if (frame.kind() != FrameKind.SESSION_MESSAGE && checked) {
                    timerOpenClose++;
                }
            }
            previous = frame.position();
        }
        return timerOpenClose;
    }

    @Test
    public void everyEventCarriesTheEndOfTheFrameThatEmittedIt() {
        final long n = assertEventPositionsAreFrameEnds(false);
        System.out.println("GREEN frame-end stamp: " + n + " position-carrying event(s) emitted by timer/open/close frames, each at its"
                + " frame end");
        assertTrue("non-vacuous: the corpus has timer/open/close frames that emit positioned events (" + n + ")", n > 0);
        CoreShadowRig.nc("11.1-d-RF3-constant", "event logPosition " + ConstantStamp.CONSTANT + " != frame end",
                () -> assertEventPositionsAreFrameEnds(true));
    }

    // ------------------------------------------------------------------------------------------ the three paths

    @Test
    public void everyPairOnTheStridesEndsWithTheBaselineBytesCursorAndPositionedEvents() {
        final Engines engines = new Engines();
        final ShadowRestoreRunner.Report r = ShadowRestoreRunner.run(engines, log, STRIDE,
                ShadowRestoreRunner.Variant.PRODUCTION, engines.probe());
        System.out.println("GREEN 11.1: " + r);
        assertTrue("every path exercised (C5): " + r, r.equal() > 0 && r.skip() > 0 && r.tail() > 0);
        assertEquals(log.size(), r.frames());
    }

    /**
     * (a) the SKIP gate with {@code <} instead of {@code <=}: the frame AT s reaches the engine a second time. Behind
     * the kit {@code ReplayContainer} its own APPLY_INSIDE_IMAGE guard stops it first (the container knows the image
     * position); the member gates in front of the applier with no container (9.2 divergence, {@link MemberModel}),
     * where the double apply reaches the image oracle.
     */
    @Test
    public void negativeControlAGateWithLessThanInsteadOfLessOrEqual() {
        final Engines engines = new Engines();
        final java.util.function.LongFunction<com.openexchange.cluster.log.SkipRule> lt = s -> (kind, end) ->
                SkipWindow.isEngineMutating(kind) && end < s;
        CoreShadowRig.nc("11.1-a-gate-lt-container", "APPLY_INSIDE_IMAGE: SESSION_MESSAGE",
                () -> ShadowRestoreRunner.run(engines, log, STRIDE,
                        ShadowRestoreRunner.Variant.PRODUCTION.withSkipRule(lt), engines.probe()));
        // the corpus dedupes by requestId (applying the frame at s twice changes nothing there); the 11.0 deposit story
        // carries no requestId, so a double apply of the frame at s is a real double credit
        final RecordedLog deposits = CoreShadowRig.write(fixture, TestShadowProducerForgeryTest.depositStory(240), -1, 0, 0)
                .frames();
        MemberModel.run(deposits, 23, SkipWindow::new);
        CoreShadowRig.nc("11.1-a-gate-lt-member", "final image differs from baseline",
                () -> MemberModel.run(deposits, 23, lt));
    }

    /** (b) the gate missing on TIMER: an old TimerEvent reaches the restored engine (same two legs as (a)). */
    @Test
    public void negativeControlBGateMissingOnTimer() {
        final Engines engines = new Engines();
        final java.util.function.LongFunction<com.openexchange.cluster.log.SkipRule> noTimer = s -> (kind, end) ->
                kind != FrameKind.TIMER_EVENT && SkipWindow.isEngineMutating(kind) && end <= s;
        CoreShadowRig.nc("11.1-b-gate-no-timer-container", "APPLY_INSIDE_IMAGE: TIMER_EVENT",
                () -> ShadowRestoreRunner.run(engines, log, STRIDE,
                        ShadowRestoreRunner.Variant.PRODUCTION.withSkipRule(noTimer), engines.probe()));
        CoreShadowRig.nc("11.1-b-gate-no-timer-member", "SKIP (s=", () -> MemberModel.run(log, STRIDE, noTimer));
    }

    @Test
    public void theMemberModelEveryPairEndsWithTheBaselineBytesAndPositionedEvents() {
        final int[] counts = MemberModel.run(log, STRIDE, SkipWindow::new);
        System.out.println("GREEN 11.1 member model: pairs=" + (counts[0] + counts[1] + counts[2]) + " equal=" + counts[0]
                + " skip=" + counts[1] + " tail=" + counts[2]);
        assertTrue("every path exercised (C5)", counts[0] > 0 && counts[1] > 0 && counts[2] > 0);
    }

    /**
     * The member's own restore shape (6.4, 9.2 divergence): no {@code ReplayContainer}. A fresh applier restores the
     * image, the image position is stamped, and frames reach the SAME applier entries the live wrappers call,
     * behind {@code SkipWindow} on SKIP, directly on EQUAL and TAIL. Same pair set and oracles as the runner: final
     * image bytes and the positioned event suffix after max(s, c).
     */
    static final class MemberModel {
        /** frames -> applier entries, what the service wrappers and the tail adapter do. */
        static ClusterLogListener entries(final CoreLogApplier a) {
            return new ClusterLogListener() {
                public void onSessionMessage(final long p, final long id, final long t, final DirectBuffer b, final int o,
                                             final int l) {
                    a.onSessionMessage(id, p, t, b, o, l);
                }

                public void onTimerEvent(final long p, final long c, final long t) {
                    a.onTimerEvent(c, p, t);
                }

                public void onSessionOpen(final long term, final long p, final long id, final long t, final int s,
                                          final String ch, final byte[] principal) {
                    a.onSessionOpen(id, p, t, principal);
                }

                public void onSessionClose(final long term, final long p, final long id, final long t,
                                           final CloseReason r) {
                    a.onSessionClose(id, p, t, r);
                }

                public boolean onClusterAction(final long term, final long p, final long t, final ClusterAction x,
                                               final int f) {
                    return false;
                }

                public void onNewLeadershipTermEvent(final long term, final long p, final long t, final long base,
                                                     final int leader, final int session, final TimeUnit u,
                                                     final int app) {
                    a.onNewLeadershipTermEvent(term, p, t, u);
                }

                public void onSkipped(final FrameKind kind, final long p, final long t) {
                }
            };
        }

        static byte[] image(final CoreLogApplier a, final ExpandableArrayBuffer buf) {
            return Arrays.copyOf(buf.byteArray(), a.serialize(buf));
        }

        /** @return {equal, skip, tail} pair counts; throws AssertionError naming the path and pair on a divergence */
        static int[] run(final RecordedLog log, final int stride,
                         final java.util.function.LongFunction<com.openexchange.cluster.log.SkipRule> rule) {
            final long[] ends = log.positions();
            final List<Integer> idx = new ArrayList<>();
            for (int i = 0; i < ends.length; i += stride) {
                idx.add(i);
            }
            if (idx.get(idx.size() - 1) != ends.length - 1) {
                idx.add(ends.length - 1);
            }
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(1 << 20);
            final Engines engines = new Engines();
            final CoreLogApplier base = engines.get();
            base.genesis();
            final ClusterLogListener baseIn = entries(base);
            final int n = idx.size();
            final long[] at = new long[n];
            final byte[][] img = new byte[n][];
            final int[] marks = new int[n];
            long prev = -1;
            for (int k = 0; k < n; k++) {
                at[k] = ends[idx.get(k)];
                log.replay(prev, at[k], baseIn);
                prev = at[k];
                img[k] = image(base, buf);
                marks[k] = engines.sinks.get(base).rendered.size();
            }
            final byte[] fin = img[n - 1];
            final List<String> baseEvents = engines.sinks.get(base).rendered;
            final int[] counts = new int[3];
            for (int si = 0; si < n; si++) {
                for (int ci = 0; ci < n; ci++) {
                    final long s = at[si];
                    final long c = at[ci];
                    final String path = s == c ? "EQUAL" : s > c ? "SKIP" : "TAIL";
                    final String where = path + " (s=" + s + ",c=" + c + ")";
                    final CoreLogApplier a = engines.get();
                    a.restore(new UnsafeBuffer(img[si]), img[si].length);
                    a.stampLogPosition(s);
                    final ClusterLogListener in = entries(a);
                    final int mark;
                    try {
                        if (s > c) {
                            final ClusterLogListener gated = SkipWindow.gate(rule.apply(s), in);
                            log.replay(c, s, gated);
                            mark = engines.sinks.get(a).rendered.size();
                            log.replay(s, Long.MAX_VALUE, gated);
                            counts[1]++;
                        } else {
                            log.replay(s, c, in); // the tail (s, c], empty on EQUAL
                            mark = engines.sinks.get(a).rendered.size();
                            log.replay(c, Long.MAX_VALUE, in);
                            counts[s == c ? 0 : 2]++;
                        }
                    } catch (final RuntimeException failure) {
                        final AssertionError e = new AssertionError(where + ": leg failed: " + failure);
                        e.initCause(failure);
                        throw e;
                    }
                    if (!Arrays.equals(fin, image(a, buf))) {
                        throw new AssertionError(where + ": final image differs from baseline");
                    }
                    final List<String> got = engines.sinks.get(a).rendered;
                    final List<String> suffix = got.subList(mark, got.size());
                    final List<String> expected = baseEvents.subList(marks[Math.max(si, ci)], baseEvents.size());
                    if (!expected.equals(suffix)) {
                        throw new AssertionError(where + ": events after max(s,c) differ: " + firstDifference(expected,
                                suffix));
                    }
                }
            }
            return counts;
        }

        static String firstDifference(final List<String> expected, final List<String> got) {
            final int n = Math.min(expected.size(), got.size());
            for (int i = 0; i < n; i++) {
                if (!expected.get(i).equals(got.get(i))) {
                    return "at " + i + " expected <" + expected.get(i) + "> got <" + got.get(i) + ">";
                }
            }
            return expected.size() > got.size() ? "missing <" + expected.get(n) + "> (" + (expected.size() - n)
                    + " fewer)" : "extra <" + got.get(n) + "> (" + (got.size() - n) + " more)";
        }
    }

    /**
     * (c) a tail reader that drops the SessionOpen principal: the applier's own principal gate refuses the open
     * (ENGINE_FAILED), so the session never reaches a state where its later frames are rejected NO_AUTHORITY.
     */
    @Test
    public void negativeControlCTailReaderDroppingTheSessionOpenPrincipal() {
        final Engines engines = new Engines();
        CoreShadowRig.nc("11.1-c-tail-drops-principal", "ENGINE_FAILED: onSessionOpen", () -> ShadowRestoreRunner.run(
                engines, log, STRIDE, ShadowRestoreRunner.Variant.PRODUCTION.withTailDecorator(
                        CoreShadowRestoreEquivalenceTest::dropPrincipal), engines.probe()));
    }

    @Test
    public void negativeControlDStampFromAConstantInTheThreePaths() {
        final Engines engines = new Engines();
        // the baseline and every leg use the defective applier, so only the per-frame oracle can see it
        final Supplier<ShadowEngine> constant = () -> new ConstantStamp(engines.get());
        final ShadowRestoreRunner.Report self = ShadowRestoreRunner.run(constant, log, STRIDE,
                ShadowRestoreRunner.Variant.PRODUCTION, e -> engines.sinks.get(((ConstantStamp)e).d).rendered);
        System.out.println("[11.1-d] a constant stamp is self-consistent across the paths (" + self + "): the per-frame"
                + " oracle is the one that must see it");
        CoreShadowRig.nc("11.1-d-RF3-constant-oracle", "event logPosition " + ConstantStamp.CONSTANT + " != frame end",
                () -> assertEventPositionsAreFrameEnds(true));
    }

    /** A tail decorator that hands every SessionOpen to the container without its principal (control c). */
    static ClusterLogListener dropPrincipal(final ClusterLogListener d) {
        return new ClusterLogListener() {
            public void onSessionMessage(final long p, final long id, final long t, final DirectBuffer b, final int o,
                                         final int l) {
                d.onSessionMessage(p, id, t, b, o, l);
            }

            public void onTimerEvent(final long p, final long c, final long t) {
                d.onTimerEvent(p, c, t);
            }

            public void onSessionOpen(final long term, final long p, final long id, final long t, final int s,
                                      final String ch, final byte[] principal) {
                d.onSessionOpen(term, p, id, t, s, ch, new byte[0]);
            }

            public void onSessionClose(final long term, final long p, final long id, final long t, final CloseReason r) {
                d.onSessionClose(term, p, id, t, r);
            }

            public boolean onClusterAction(final long term, final long p, final long t, final ClusterAction a,
                                           final int f) {
                return d.onClusterAction(term, p, t, a, f);
            }

            public void onNewLeadershipTermEvent(final long term, final long p, final long t, final long base,
                                                 final int leader, final int session, final TimeUnit u, final int app) {
                d.onNewLeadershipTermEvent(term, p, t, base, leader, session, u, app);
            }

            public void onSkipped(final FrameKind kind, final long p, final long t) {
                d.onSkipped(kind, p, t);
            }
        };
    }

    // ------------------------------------------------------------------------------------------ P_s shifts (11.0)

    /** Baseline image and cursor at every frame end (genesis, every frame in order). */
    private static final class Baseline {
        final long[] ends = log.positions();
        final byte[][] image = new byte[ends.length][];
        final LogCursor[] cursor = new LogCursor[ends.length];
        final byte[] finalImage;

        Baseline() {
            final Engines engines = new Engines();
            final ReplayContainer c = new ReplayContainer(engines.get(), ShadowRestoreRunner.DEFAULT_APP_VERSION,
                    ShadowRestoreRunner.DEFAULT_VALIDATOR, ShadowTriggerListener.NONE);
            c.genesis();
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(1 << 20);
            long prev = -1;
            for (int i = 0; i < ends.length; i++) {
                log.replay(prev, ends[i], c);
                prev = ends[i];
                image[i] = Arrays.copyOf(buf.byteArray(), c.serialize(buf));
                cursor[i] = c.cursor();
            }
            finalImage = image[ends.length - 1];
        }

        int index(final long position) {
            final int i = Arrays.binarySearch(ends, position);
            if (i < 0) {
                throw new IllegalArgumentException(position + " is not a frame end");
            }
            return i;
        }
    }

    /**
     * A candidate whose content is {@code S(content)} and whose header says {@code label} is restored on the path the
     * label and {@code pCm} pick (SKIP when label > pCm, TAIL when label < pCm), then the rest of the log; the final
     * image must equal the baseline's or the leg fails with {@code final image differs from baseline}.
     */
    private static void restoreCandidate(final Baseline b, final long content, final long label, final long pCm) {
        final Engines engines = new Engines();
        final ReplayContainer c = new ReplayContainer(engines.get(), ShadowRestoreRunner.DEFAULT_APP_VERSION,
                ShadowRestoreRunner.DEFAULT_VALIDATOR, ShadowTriggerListener.NONE);
        final byte[] img = b.image[b.index(content)];
        final UnsafeBuffer buffer = new UnsafeBuffer(img);
        final String where;
        if (label > pCm) {
            where = "SKIP (s=" + label + ",c=" + pCm + ")";
            c.restore(buffer, img.length, label, b.cursor[b.index(pCm)]);
            log.replay(pCm, Long.MAX_VALUE, SkipWindow.gate(new SkipWindow(label), c));
        } else {
            where = "TAIL (s=" + label + ",c=" + pCm + ")";
            c.restore(buffer, img.length, label, b.cursor[b.index(label)]);
            log.replay(label, Long.MAX_VALUE, c);
        }
        final ExpandableArrayBuffer out = new ExpandableArrayBuffer(1 << 20);
        final byte[] got = Arrays.copyOf(out.byteArray(), c.serialize(out));
        if (!Arrays.equals(b.finalImage, got)) {
            throw new AssertionError(where + ": final image differs from baseline (content S(" + content + ") under"
                    + " P_s=" + label + ")");
        }
    }

    /**
     * The corpus dedupes most frames by requestId, so applying one twice leaves the image unchanged and no shift there is
     * a content-changing forgery; the shift rows use the 11.0 deposit story (no requestId), as the W0 forgery table does.
     */
    @Test
    public void bothPsShiftDirectionsAreSeenOnlyByTheFrameLevelOracle() {
        final RecordedLog corpus = log;
        log = CoreShadowRig.write(fixture, TestShadowProducerForgeryTest.depositStory(240), -1, 0, 0).frames();
        try {
            shiftRows();
        } finally {
            log = corpus;
        }
    }

    private static void shiftRows() {
        final Baseline b = new Baseline();
        final TestShadowProducer.ShiftPair p = TestShadowProducer.findShiftPair(log, false, 0);
        final long early = b.ends[Math.max(0, b.index(p.x()) - 3)];
        final long late = b.ends[b.ends.length - 2];
        assertTrue("early < x < y < late: " + early + " " + p + " " + late, early < p.x() && p.y() < late);
        // genuine candidates at x and y pass on both paths (the oracle is not blind in the other direction)
        restoreCandidate(b, p.x(), p.x(), early);
        restoreCandidate(b, p.x(), p.x(), late);
        restoreCandidate(b, p.y(), p.y(), early);
        restoreCandidate(b, p.y(), p.y(), late);
        // backward shift: content S(y) under P_s = x
        CoreShadowRig.nc("11.1-backward-shift-SKIP", "final image differs from baseline",
                () -> restoreCandidate(b, p.y(), p.x(), early));
        CoreShadowRig.nc("11.1-backward-shift-TAIL", "final image differs from baseline",
                () -> restoreCandidate(b, p.y(), p.x(), late));
        // forward shift: content S(x) under P_s = y
        CoreShadowRig.nc("11.1-forward-shift-SKIP", "final image differs from baseline",
                () -> restoreCandidate(b, p.x(), p.y(), early));
        CoreShadowRig.nc("11.1-forward-shift-TAIL", "final image differs from baseline",
                () -> restoreCandidate(b, p.x(), p.y(), late));
        System.out.println("GREEN 11.1 shifts at x=" + p.x() + " y=" + p.y() + ": genuine passes, both shifts differ");
    }
}

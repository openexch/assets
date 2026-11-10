// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.NeverSchedulesScan;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import com.openexchange.cluster.testkit.ShadowEngineConformance;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The {@link CoreLogApplier} is a kit {@link ShadowEngine}.
 * <ol>
 *   <li>The seam: {@code CoreLogApplier implements ShadowEngine}, the nine methods are the interface's (RED before
 *       W8: the class implements nothing).</li>
 *   <li>Conformance: {@code ShadowEngineConformance.run} R1-R7 with sessions replicated (R6), the failure provoked by a
 *       clashing open, events counted by a {@link StampSink}, over a sample recorded through the kit writer and read
 *       back through the reader (sessioned story of 300 frames plus the 4.5 tail).</li>
 *   <li>{@code replayContainerNeverSchedules}, core half: the scan over the transitive closure from
 *       {@code CoreLogApplier} inside {@code com/openexchange/core/}, across the core-cluster and core-common
 *       class roots, is clean and not shrinking; the positive control {@code CoreClusteredService} is flagged.</li>
 * </ol>
 * Negative controls: each conformance rule switched off in a test-only {@link RuleOff} decorator around the applier
 * must make {@code run} throw the AssertionError of exactly that rule; the closure with the core-common root left out
 * must refuse (fail closed), not shrink.
 */
public class CoreShadowEngineConformanceTest {

    static final long SAMPLE_SEED = 0x5EED_0002L;
    static final int SAMPLE_FRAMES = 300;
    static final String ALA = "com/openexchange/core/infrastructure/persistence/CoreLogApplier";
    static final String ACS = "com/openexchange/core/infrastructure/persistence/CoreClusteredService";
    /**
     * Classes in the ALA closure at b05b8e8 (measured 206; the measured 183 at a9a865d +). A closure
     * below the floor means a module or a reference stopped being followed; a larger one is new code the scan covers.
     */
    static final int CLOSURE_FLOOR = 206;

    private static RecordedLogFixture fixture;
    private static RecordedLog sample;

    @BeforeClass
    public static void recordTheSample() {
        fixture = new RecordedLogFixture();
        final CoreReplayParityTest.Written w = CoreReplayParityTest.write(fixture,
                CoreReplayParityTest.coreSteps(SAMPLE_SEED, SAMPLE_FRAMES));
        sample = RecordedLog.capture(fixture.replay(w.recordingId(), 0, AeronArchive.NULL_LENGTH), 0, w.stopPosition());
        assertEquals("one frame per step plus the term event", w.steps().size() + 1, sample.size());
    }

    @AfterClass
    public static void closeFixture() {
        if (fixture != null) {
            fixture.close();
        }
    }

    // ------------------------------------------------------------------------------------------------ the seam

    @Test
    public void theApplierImplementsTheKitShadowEngine() throws NoSuchMethodException {
        assertTrue("CoreLogApplier implements com.openexchange.cluster.log.ShadowEngine",
                ShadowEngine.class.isAssignableFrom(CoreLogApplier.class));
        final Method[] seam = ShadowEngine.class.getDeclaredMethods();
        assertEquals("the seam is fixed at nine methods", 9, seam.length);
        for (final Method m : seam) {
            final Method impl = CoreLogApplier.class.getMethod(m.getName(), m.getParameterTypes());
            assertEquals(m.getName(), CoreLogApplier.class, impl.getDeclaringClass());
            assertEquals(m.getName(), m.getReturnType(), impl.getReturnType());
            assertTrue(m.getName(), Modifier.isPublic(impl.getModifiers()));
        }
        assertEquals(CoreLogApplier.ENGINE_ID_CORE,
                CoreReplayParityTest.shadowEngine(new CoreLogApplier(() -> { })).engineId());
        assertEquals(1, CoreLogApplier.ENGINE_ID_CORE);
    }

    // ----------------------------------------------------------------------------------------------- conformance

    /** One shared sink for every engine the supplier makes: {@code emittedEvents} is the total over all of them. */
    private static final class Engines {
        final StampSink sink = new StampSink();
        final List<CoreLogApplier> made = new ArrayList<>();

        CoreLogApplier fresh() {
            final CoreLogApplier a = new CoreLogApplier(() -> {
                throw new AssertionError("the sample must not trip the node-local CoreConfig stop");
            });
            a.setEventSink(sink);
            made.add(a);
            return a;
        }

        int emitted() {
            return (int)sink.calls;
        }
    }

    private static LogFrame firstOpen() {
        return sample.frames().stream().filter(f -> f.kind() == FrameKind.SESSION_OPEN).findFirst().orElseThrow();
    }

    /** The provocation: a clashing open of the sample's first session (open until the end of the sample). */
    private static Consumer<ShadowEngine> clashingOpen() {
        final LogFrame open = firstOpen();
        return e -> e.onSessionOpen(open.clusterSessionId(), open.position() + 1, open.timestamp(), open.data());
    }

    private static ShadowEngineConformance.Report run(final Engines engines, final Off off) {
        return ShadowEngineConformance.<ShadowEngine>run(() -> {
            final CoreLogApplier a = engines.fresh();
            final ShadowEngine e = CoreReplayParityTest.shadowEngine(a);
            return off == null ? e : new RuleOff(off, a, e, engines.sink);
        }, sample, new ShadowEngineConformance.Options<>(true, clashingOpen(), engines::emitted));
    }

    @Test
    public void theApplierSatisfiesTheKitConformanceRules() {
        final Engines engines = new Engines();
        final ShadowEngineConformance.Report report = run(engines, null);
        assertEquals(sample.size(), report.frames());
        assertEquals("R3 spliced at every frame and at the end", sample.size() + 1, report.splices());
        // Non-vacuity: the sample exercises every engine-mutating kind, emits, and the provocation latched.
        for (final FrameKind k : new FrameKind[] {FrameKind.SESSION_OPEN, FrameKind.SESSION_CLOSE,
                FrameKind.SESSION_MESSAGE, FrameKind.TIMER_EVENT, FrameKind.NEW_LEADERSHIP_TERM}) {
            assertTrue("sample has " + k, sample.count(k) > 0);
        }
        assertTrue("events emitted " + engines.emitted(), engines.emitted() > SAMPLE_FRAMES);
        assertTrue("the provoked engine latched", engines.made.stream().anyMatch(CoreLogApplier::failed));
        System.out.println("GREEN conformance: frames=" + report.frames() + " splices=" + report.splices()
                + " engines=" + engines.made.size() + " sinkCalls=" + engines.emitted());
    }

    /** Each rule switched off; the expected AssertionError message prefix. */
    enum Off {
        R1_GENESIS_TWICE("R1: second genesis was accepted", ""),
        R2_SERIALIZE_MUTATES("R2: two serializations of the same state differ", ""),
        R3_RESTORE_DRIFTS("R3: restore(serialize()) does not reproduce the bytes at frame 0", ""),
        R4_TERM_MUTATES("R4: a term event changed the image", ""),
        R4_TERM_EMITS("R4: a term event emitted an event", ""),
        R5_UNKNOWN_CLOSE_MUTATES("R5: a close of never-opened id", "changed the image"),
        R5_UNKNOWN_CLOSE_EMITS("R5: a close of never-opened id", "emitted an event"),
        R6_CLASH_ACCEPTED("R6: a clashing open of id", "was accepted"),
        R7_SERIALIZE_AFTER_FAILURE("R7: serialize after the provoked failure was accepted", "");

        final String expected;
        final String suffix;

        Off(final String expected, final String suffix) {
            this.expected = expected;
            this.suffix = suffix;
        }
    }

    /** The applier with one conformance rule switched off. Test-only; production has no such seam. */
    static final class RuleOff implements ShadowEngine {
        private final Off off;
        private final CoreLogApplier applier;
        private final ShadowEngine d;
        private final StampSink sink;

        RuleOff(final Off off, final CoreLogApplier applier, final ShadowEngine delegate, final StampSink sink) {
            this.off = off;
            this.applier = applier;
            this.d = delegate;
            this.sink = sink;
        }

        private void drift() {
            applier.engine().setConsumePosition(applier.engine().getConsumePosition() + 1);
        }

        private void emit() {
            sink.onBalanceUpdate(0, 0, 0, 0);
        }

        public int engineId() {
            return d.engineId();
        }

        public void genesis() {
            if (off == Off.R1_GENESIS_TWICE) {
                try {
                    d.genesis();
                } catch (final IllegalStateException swallowed) {
                    // the rule switched off: a second genesis is not refused
                }
                return;
            }
            d.genesis();
        }

        public void restore(final DirectBuffer image, final int length) {
            d.restore(image, length);
            if (off == Off.R3_RESTORE_DRIFTS) {
                drift();
            }
        }

        public int serialize(final MutableDirectBuffer dst) {
            if (off == Off.R7_SERIALIZE_AFTER_FAILURE && applier.failed()) {
                return CoreLogApplier.encode(applier.engine(), dst, new SnapshotEnvelope());
            }
            final int length = d.serialize(dst);
            if (off == Off.R2_SERIALIZE_MUTATES) {
                drift();
            }
            return length;
        }

        public void onSessionOpen(final long id, final long p, final long ts, final byte[] principal) {
            if (off == Off.R6_CLASH_ACCEPTED && applier.engine().sessions().get(id) != null) {
                return;
            }
            d.onSessionOpen(id, p, ts, principal);
        }

        public void onSessionClose(final long id, final long p, final long ts, final CloseReason reason) {
            if (applier.engine().sessions().get(id) == null) {
                if (off == Off.R5_UNKNOWN_CLOSE_MUTATES) {
                    drift();
                } else if (off == Off.R5_UNKNOWN_CLOSE_EMITS) {
                    emit();
                }
            }
            d.onSessionClose(id, p, ts, reason);
        }

        public boolean onSessionMessage(final long id, final long p, final long ts, final DirectBuffer buffer,
                                        final int offset, final int length) {
            return d.onSessionMessage(id, p, ts, buffer, offset, length);
        }

        public int onTimerEvent(final long correlationId, final long p, final long ts) {
            return d.onTimerEvent(correlationId, p, ts);
        }

        public void onNewLeadershipTermEvent(final long term, final long p, final long ts, final TimeUnit unit) {
            d.onNewLeadershipTermEvent(term, p, ts, unit);
            if (off == Off.R4_TERM_MUTATES) {
                drift();
            } else if (off == Off.R4_TERM_EMITS) {
                emit();
            }
        }
    }

    @Test
    public void negativeControlsEachRuleSwitchedOffIsCaughtByThatRule() {
        final List<String> blind = new ArrayList<>();
        for (final Off off : Off.values()) {
            String message = null;
            try {
                run(new Engines(), off);
            } catch (final AssertionError expected) {
                message = expected.getMessage();
            }
            System.out.println("NC " + off + " -> " + message);
            if (message == null || !message.startsWith(off.expected) || !message.endsWith(off.suffix)) {
                blind.add(off + " (got: " + message + ")");
            }
        }
        if (!blind.isEmpty()) {
            fail("conformance blind to, or caught by the wrong rule: " + blind);
        }
    }

    // ------------------------------------------------------------------------------- never schedules (core half)

    /** The class root (directory, or the root of a jar) a class was loaded from. */
    static Path rootOf(final Class<?> c) {
        try {
            final Path p = Path.of(c.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (Files.isDirectory(p)) {
                return p;
            }
            final FileSystem jar = FileSystems.newFileSystem(p);
            return jar.getPath("/");
        } catch (final URISyntaxException | IOException e) {
            throw new IllegalStateException("class root of " + c.getName(), e);
        }
    }

    /** core-cluster (the applier) and core-common (the generated codecs it imports). */
    static List<Path> coreRoots() {
        return List.of(rootOf(CoreLogApplier.class), rootOf(MessageHeaderDecoder.class));
    }

    @Test
    public void replayContainerNeverSchedules() {
        final List<Path> roots = coreRoots();
        assertTrue("two distinct module roots " + roots, !roots.get(0).equals(roots.get(1)));
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanClosure(roots, ALA, "com/openexchange/core/",
                NeverSchedulesScan.Rules.rf6());
        System.out.println("GREEN scan: closure from " + ALA + " = " + report.classesScanned() + " classes, findings "
                + report.findings().size() + ", roots " + roots);
        assertTrue("forbidden-call findings in the applier closure (resolve, never allowlist): " + report.findings(),
                report.clean());
        assertTrue("the closure shrank below the floor: " + report.classesScanned() + " < " + CLOSURE_FLOOR,
                report.classesScanned() >= CLOSURE_FLOOR);
        assertTrue("the closure crosses into core-common", report.classes().stream()
                .anyMatch(n -> n.startsWith("com/openexchange/core/infrastructure/generated/")));
        assertTrue("the closure reaches the engine", report.classes().stream()
                .anyMatch(n -> n.equals("com/openexchange/core/application/engine/CoreEngine")));
        assertTrue("the service wrapper is not in the applier closure", !report.classes().contains(ACS));
    }

    /**
     * Core leg: the closure from {@code CoreShadowIdentity} (named as a string, so this file compiles
     * before the class exists and the scan fails closed without it) is clean. It reaches the engine and the
     * generated codecs; its size is recorded and is the floor (76 classes measured at on 6fcf66b +).
     */
    @Test
    public void theShadowIdentityClosureNeverSchedules() {
        final String asi = "com/openexchange/core/infrastructure/persistence/CoreShadowIdentity";
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanClosure(coreRoots(), asi,
                "com/openexchange/core/", NeverSchedulesScan.Rules.rf6());
        System.out.println("GREEN scan: closure from " + asi + " = " + report.classesScanned() + " classes, findings "
                + report.findings().size());
        assertTrue("forbidden-call findings in the CoreShadowIdentity closure: " + report.findings(), report.clean());
        assertTrue("the closure reaches the engine", report.classes().stream()
                .anyMatch(n -> n.equals("com/openexchange/core/application/engine/CoreEngine")));
        assertTrue("the service wrapper is not in the closure", !report.classes().contains(ACS));
        assertTrue("the closure shrank below the floor: " + report.classesScanned() + " < 76",
                report.classesScanned() >= 76);
    }

    @Test
    public void positiveControlTheServiceWrapperIsFlagged() {
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanClosure(coreRoots(), ACS,
                "com/openexchange/core/", NeverSchedulesScan.Rules.rf6());
        final List<NeverSchedulesScan.Finding> own = report.findings().stream()
                .filter(f -> f.className().equals(ACS)).toList();
        System.out.println("NC ACS_POSITIVE_CONTROL -> " + own.size() + " findings in ACS itself, e.g. "
                + own.stream().map(NeverSchedulesScan.Finding::toString).limit(3).toList());
        assertTrue("scheduleTimer seen in ACS: " + own, own.stream()
                .anyMatch(f -> f.reference().equals("io/aeron/cluster/service/Cluster.scheduleTimer")));
        assertTrue("the Cluster type seen in ACS: " + own, own.stream()
                .anyMatch(f -> f.reference().equals("io/aeron/cluster/service/Cluster")));
        assertEquals(List.of(ACS), own.get(0).path());
    }

    @Test
    public void negativeControlAMissingModuleRootRefusesInsteadOfShrinking() {
        try {
            final NeverSchedulesScan.Report r = NeverSchedulesScan.scanClosure(List.of(rootOf(CoreLogApplier.class)),
                    ALA, "com/openexchange/core/", NeverSchedulesScan.Rules.rf6());
            fail("a closure without core-common was accepted with " + r.classesScanned() + " classes");
        } catch (final IllegalStateException expected) {
            System.out.println("NC SCAN_WITHOUT_CORE_COMMON -> " + expected.getMessage());
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("unresolved in-prefix class "
                    + "com/openexchange/core/"));
        }
    }
}

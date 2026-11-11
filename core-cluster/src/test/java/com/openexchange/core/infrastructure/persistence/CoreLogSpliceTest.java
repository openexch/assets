// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.LogSpliceRunner;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import com.openexchange.cluster.testkit.ShadowRestoreRunner;
import com.openexchange.core.infrastructure.persistence.SessionedLog.LogStep;
import com.openexchange.core.replica.ReplicaTestBindings;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Splice equivalence of the production applier at the positions a replica shadows at: the corpus log (the same
 * sessioned story {@link CoreShadowRestoreEquivalenceTest} replays) written with the kit writer, with SNAPSHOT
 * actions (three flags 0, two flags 1) interleaved through {@link ClusterLogWriter}, since the corpus itself holds no
 * action. {@link LogSpliceRunner} cuts at every action and every 199th frame end, restores a fresh applier from the
 * genesis replay's image there and checks the image at EVERY later action, the cursor and the event suffix.
 *
 * <p>Control: an applier that drops every session open, used only for the restored legs (the genesis replay keeps
 * the production applier) -> {@code splice at <k>: image at action <a> differs from the genesis replay}.</p>
 */
public class CoreLogSpliceTest {

    static final int STRIDE = 199;
    /**
     * Where a SNAPSHOT action is written, and its flags: {fraction of the corpus, flags}; fraction 0 is right after
     * the first session open (so later opens follow a cut).
     */
    private static final int[][] ACTIONS = {{0, 0}, {2, 1}, {3, 0}, {4, 1}, {5, 0}};

    private static RecordedLogFixture fixture;
    private static RecordedLog log;

    @BeforeClass
    public static void recordTheCorpusWithActions() {
        fixture = new RecordedLogFixture();
        final List<LogStep> steps = CoreReplayParityTest.coreSteps(CoreReplayParityTest.SEED, CoreReplayParityTest.FRAMES);
        final RecordedLogFixture.Recording rec = fixture.record(CoreReplayParityTest.TERM_LENGTH,
                CoreReplayParityTest.MTU, 0);
        final ClusterLogWriter w = rec.writer();
        w.newLeadershipTerm(0, steps.get(0).timestamp(), 0, 0, rec.publication().sessionId(), TimeUnit.MILLISECONDS,
                CoreReplayParityTest.APP_VERSION);
        int firstOpen = -1;
        for (int i = 0; i < steps.size() && firstOpen < 0; i++) {
            if (steps.get(i).kind() == SessionedLog.Kind.OPEN) {
                firstOpen = i;
            }
        }
        int next = 0;
        for (int i = 0; i < steps.size(); i++) {
            final LogStep s = steps.get(i);
            switch (s.kind()) {
                case OPEN -> w.sessionOpen(s.sessionId(), 7_000_000L + i, s.timestamp(), 102,
                        "aeron:udp?endpoint=localhost:20" + (s.sessionId() % 100), s.bytes());
                case CLOSE -> w.sessionClose(s.sessionId(), s.timestamp(), CloseReason.CLIENT_ACTION);
                case MESSAGE -> w.message(s.sessionId(), s.timestamp(), s.bytes());
                case TIMER -> w.timer(s.correlationId(), s.timestamp());
            }
            if (next < ACTIONS.length && i == (ACTIONS[next][0] == 0 ? firstOpen
                    : steps.size() * ACTIONS[next][0] / (ACTIONS.length + 1))) {
                w.action(ClusterAction.SNAPSHOT, s.timestamp(), ACTIONS[next][1]);
                next++;
            }
        }
        final long stop = rec.stopRecording();
        log = RecordedLog.capture(fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH), 0, stop);
        assertEquals("one frame per step, the term event and the actions", steps.size() + 1 + ACTIONS.length,
                log.size());
        System.out.println("[splice] corpus with actions: " + log.size() + " frames, end " + log.endPosition()
                + ", actions " + log.count(FrameKind.CLUSTER_ACTION) + ", opens " + log.count(FrameKind.SESSION_OPEN));
    }

    @AfterClass
    public static void closeFixture() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @Test(timeout = 600_000)
    public void everyActionCutReproducesEveryLaterActionImage() {
        final CoreShadowRestoreEquivalenceTest.Engines engines = new CoreShadowRestoreEquivalenceTest.Engines();
        final LogSpliceRunner.Report r = LogSpliceRunner.run(engines, log, LogSpliceRunner.Cuts.actionsAndStride(STRIDE),
                engines.probe());
        assertEquals("every interleaved action is a checkpoint", ACTIONS.length, r.actions());
        if (r.comparisons() < r.cuts()) {
            throw new AssertionError("vacuous: " + r.comparisons() + " comparisons for " + r.cuts() + " cuts");
        }
        assertTrue("cuts at the actions and the stride: " + r.cuts(), r.cuts() > ACTIONS.length);
        System.out.println("GREEN splice: frames=" + r.frames() + " actions=" + r.actions() + " cuts=" + r.cuts()
                + " checkpoints=" + r.checkpoints() + " comparisons=" + r.comparisons() + " finalImageBytes="
                + r.finalImageBytes());
    }

    /** The restored legs drop every session open; the genesis replay does not. */
    @Test(timeout = 600_000)
    public void aRestoredApplierThatSkipsSessionOpensFailsAtTheNextAction() {
        final CoreShadowRestoreEquivalenceTest.Engines engines = new CoreShadowRestoreEquivalenceTest.Engines();
        final AtomicInteger calls = new AtomicInteger();
        final Supplier<ShadowEngine> skipping = () -> calls.getAndIncrement() == 0 ? engines.get()
                : new ReplicaTestBindings.SkipSessionOpenEngine(engines.get());
        final ShadowRestoreRunner.Probe<ShadowEngine> probe = e -> List.of();
        try {
            LogSpliceRunner.run(skipping, log, LogSpliceRunner.Cuts.actionsAndStride(STRIDE), probe);
        } catch (final AssertionError e) {
            final String msg = String.valueOf(e.getMessage());
            if (!msg.startsWith("splice at ") || !msg.contains(": image at action ")
                    || !msg.contains(" differs from the genesis replay (first byte ")) {
                throw new AssertionError("negative control SKIP_SESSION_OPEN failed for the wrong reason: <" + msg + ">",
                        e);
            }
            System.out.println("NC SKIP_SESSION_OPEN -> " + msg);
            return;
        }
        throw new AssertionError("negative control SKIP_SESSION_OPEN did not fail: the splice runner cannot see a"
                + " skipped session open");
    }
}

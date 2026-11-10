// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.persistence.CoreShadowRig.Arm;
import com.openexchange.core.infrastructure.persistence.CoreShadowRig.Log;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.ShiftPair;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowIdentity;
import com.openexchange.cluster.shadow.ShadowImageLoader;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import org.agrona.IoUtil;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The {@link TestShadowProducer} scaffold and its forgery table, on the fixture (a real
 * archive, a real core log written with the kit's {@code ClusterLogWriter}). Every row asserts the outcome the contract
 * names for it; the outcomes that need production code (the W1 witness inside {@code ShadowRestore}, the SKIP window,
 * the tail reader) are asserted here against the test oracles ({@link TestShadowProducer#witnessFailure}, the frame
 * level baseline) so W2 can swap the oracle for the production check without changing the forged inputs.
 *
 * <p>Expected to go RED until W2: nothing in this class (it needs no production class). The rows that W2/W3 re-assert
 * through production code are marked {@code [W2-REASSERT]}: W1 inside {@code ShadowRestore} (backward shift, later ms),
 * the 11.1 frame-level {@code final image differs from baseline} (both shifts, same-ms), STRICT identity and loader
 * CORRUPT inside the restore selection, catalog CONFLICT handling in the selection (11.3).</p>
 *
 * <p>Negative controls (printed as {@code NC id -> message}): each runs the same assertion on an input with the forgery
 * switched off and must fail with the named text.</p>
 */
public class TestShadowProducerForgeryTest {

    static final long GENESIS = CoreShadowRig.GENESIS;
    static final long FOREIGN_GENESIS = 0xF00DL;

    private static RecordedLogFixture fixture;
    private static File archiveDir;
    private static Log log;
    private static TestShadowProducer producer;
    private static byte[] baselineFinal;
    /** One frame pair per row, allocated in a fixed order so no two rows ever share a header position (CONFLICT is per position). */
    private static ShiftPair laterMs;      // genuine round trip
    private static ShiftPair backLater;    // backward shift, later ms
    private static ShiftPair sameMs;       // backward shift, same ms
    private static ShiftPair forward;      // forward shift
    private static ShiftPair foreign;      // foreign genesis
    private static ShiftPair flip;         // payload bit flip
    private static ShiftPair conflict;     // second producer, different content
    private static ShiftPair control;      // the negative controls' own positions

    /**
     * ADMIN opens, ADOPTs the genesis nonce, then deposits without a requestId (no dedupe: applying a frame twice changes
     * the image, which is what a double apply after a backward shift must be able to show). Every third deposit moves the
     * clock by 1..7 ms, the other two share the millisecond, so both W1 cases exist in one log.
     */
    static java.util.List<SessionedLog.LogStep> depositStory(final int deposits) {
        final CoreFrameEncoders enc = new CoreFrameEncoders();
        final long admin = SessionedLog.sessionOf(com.openexchange.core.infrastructure.generated.SessionClass.ADMIN);
        final java.util.List<SessionedLog.LogStep> out = new java.util.ArrayList<>();
        long t = CoreFrameStory.T0;
        out.add(SessionedLog.open(admin, t, SessionedLog.principalOf(com.openexchange.core.infrastructure.generated.SessionClass.ADMIN)));
        out.add(SessionedLog.message(admin, t, enc.coreConfig(1, 1, 1, 1, 1, 1_000_000_000_000_000L, 0)));
        for (int i = 0; i < deposits; i++) {
            if (i % 3 == 0) {
                t += 1 + (i % 7);
            }
            out.add(SessionedLog.message(admin, t, enc.deposit(100 + i, 1 + (i % 5),
                    com.openexchange.core.domain.Asset.USD.id(), 1_000_00000000L + i, 0, 0)));
        }
        return SessionedLog.renumber(out);
    }

    @BeforeClass
    public static void writeLogAndBaseline() throws Exception {
        fixture = new RecordedLogFixture(null, CoreShadowRoundTripTest.freeUdpPort());
        final Field dir = RecordedLogFixture.class.getDeclaredField("dir");
        dir.setAccessible(true);
        archiveDir = new File((File)dir.get(fixture), "archive");
        log = CoreShadowRig.write(fixture, depositStory(240), -1, 0, 0);
        producer = new TestShadowProducer(fixture.aeron(), fixture.archive(), 0, archiveDir);
        final Arm base = new Arm().genesis().apply(log, -1, Long.MAX_VALUE);
        baselineFinal = base.image();
        laterMs = TestShadowProducer.findShiftPair(log.frames(), false, 0);
        backLater = TestShadowProducer.findShiftPair(log.frames(), false, laterMs.y());
        sameMs = TestShadowProducer.findShiftPair(log.frames(), true, backLater.y());
        forward = TestShadowProducer.findShiftPair(log.frames(), false, sameMs.y());
        foreign = TestShadowProducer.findShiftPair(log.frames(), false, forward.y());
        flip = TestShadowProducer.findShiftPair(log.frames(), false, foreign.y());
        conflict = TestShadowProducer.findShiftPair(log.frames(), false, flip.y());
        control = TestShadowProducer.findShiftPair(log.frames(), false, conflict.y());
        System.out.println("[W0] shift pairs (x,y,t(x)->t(y)): later " + laterMs + " backLater " + backLater + " same " + sameMs
                + " forward " + forward + " foreign " + foreign + " flip " + flip + " conflict " + conflict + " control " + control);
    }

    @AfterClass
    public static void close() {
        if (fixture != null) {
            fixture.close();
        }
        if (archiveDir != null) {
            IoUtil.delete(archiveDir.getParentFile(), true);
        }
    }

    // ------------------------------------------------------------------------------------------ host steps

    private static final java.util.concurrent.atomic.AtomicInteger NEXT_ID = new java.util.concurrent.atomic.AtomicInteger(10);

    /** A producer identity no earlier write used: the writer's monotone rule (W2) is per producer, tests are unordered. */
    private static Who fresh() {
        return new Who(com.openexchange.cluster.shadow.ProducerKind.FOLLOWER, NEXT_ID.getAndIncrement());
    }

    private final ShadowVerdicts verdicts = new ShadowVerdicts();
    private final ShadowCatalog catalog = CoreShadowRig.catalog(fixture.aeron(), fixture.archive(), verdicts, GENESIS);

    private ShadowEntry entryOf(final Written w) {
        final ShadowEntry e = catalog.scan().entry(w.recordingId());
        assertNotNull("catalog must list the written recording " + w.recordingId() + ": " + w, e);
        return e;
    }

    /** What a host does with one entry: load (digest), restore at the header's cursor, then the rest of the log. */
    private record Restored(Arm arm, ShadowImageLoader.LoadResult load, byte[] finalImage, String witness) {
    }

    private Restored restoreAndFinish(final ShadowEntry e) {
        final UnsafeBuffer into = new UnsafeBuffer(ByteBuffer.allocateDirect(TestShadowProducer.CAPACITY));
        final ShadowImageLoader.LoadResult r = new ShadowImageLoader(fixture.aeron(), fixture.archive(), verdicts,
                SystemNanoClock.INSTANCE, CoreShadowRig.TIMEOUT_NS, new ShadowCounters()).load(e, into);
        if (!r.ok()) {
            return new Restored(null, r, null, null);
        }
        final Arm restored = new Arm();
        restored.restore(r.loaded().payload(), r.loaded().length(), r.loaded().header().logPosition(), r.loaded().cursor());
        final String witness = TestShadowProducer.witnessFailure(restored.applier, r.loaded().header());
        restored.apply(log, r.loaded().header().logPosition(), Long.MAX_VALUE);
        return new Restored(restored, r, restored.image(), witness);
    }

    private static void assertFinalDiffers(final byte[] fin, final String what) {
        if (Arrays.equals(baselineFinal, fin)) {
            throw new AssertionError("final image equals baseline: " + what + " was not a content-changing forgery");
        }
        // the named text of 11.1 for a candidate whose P_s does not match its content
        System.out.println("final image differs from baseline (" + what + ")");
    }

    private static void assertWitnessFires(final String witness, final String what) {
        if (witness == null) {
            throw new AssertionError("W1 did not fire for " + what + ": shadow P_s witness failed expected");
        }
        assertTrue(witness, witness.startsWith("shadow P_s witness failed: lastLogTime="));
    }

    // ------------------------------------------------------------------------------------------ the rows

    @Test
    public void genuineShadowRoundTripsAndPassesW1() {
        final Written w = producer.produce(fresh(), log.frames(), laterMs.x());
        assertTrue(w.toString(), w.ok());
        final ShadowEntry e = entryOf(w);
        assertEquals(ShadowEntry.State.COMPLETE, e.state());
        assertEquals(laterMs.x(), e.header().logPosition());
        assertEquals(ShadowEntry.State.VERIFIED, catalog.verifyAllComplete().entry(w.recordingId()).state());
        assertEquals(ShadowIdentity.Outcome.OK, ShadowIdentity.check(e.header(), CoreShadowRig.member(GENESIS, log.terms())));
        final Restored r = restoreAndFinish(e);
        assertTrue(String.valueOf(r.load()), r.load().ok());
        assertNull("a genuine shadow passes W1", r.witness());
        assertTrue("a genuine shadow reproduces the baseline's final image", Arrays.equals(baselineFinal, r.finalImage()));
        CoreShadowRig.nc("forgery-control-1", "final image equals baseline", () -> assertFinalDiffers(r.finalImage(), "a genuine shadow"));
        System.out.println("GREEN genuine: " + w);
    }

    @Test
    public void backwardShiftToALaterMillisecondIsSeenByW1() {
        final Written w = producer.produceBackwardShift(fresh(), log.frames(), backLater.x(), backLater.y());
        assertTrue(w.toString(), w.ok());
        assertTrue("the extra frame has a later millisecond: content " + w.contentLastLogTime() + " header " + w.headerClusterTime(),
                w.contentLastLogTime() > w.headerClusterTime());
        final ShadowEntry e = entryOf(w);
        assertEquals("the catalog cannot tell (digest over what was written)", ShadowEntry.State.COMPLETE, e.state());
        assertEquals(ShadowIdentity.Outcome.OK, ShadowIdentity.check(e.header(), CoreShadowRig.member(GENESIS, log.terms())));
        final Restored r = restoreAndFinish(e);
        assertTrue(ShadowIdentity.matches(e.header(), CoreShadowRig.identityOf(r.arm().applier)));
        assertWitnessFires(r.witness(), "a backward shift to a later millisecond");
        System.out.println("GREEN backward shift (later ms) -> " + r.witness());
        assertFinalDiffers(r.finalImage(), "backward shift, later ms");
        // control: a genuine shadow at the same position is not flagged
        final Written genuine = producer.produce(fresh(), log.frames(), control.x());
        CoreShadowRig.nc("forgery-control-2", "W1 did not fire", () -> assertWitnessFires(restoreAndFinish(entryOf(genuine)).witness(),
                "a genuine shadow"));
    }

    @Test
    public void backwardShiftInTheSameMillisecondIsTheW1ResidueAndOnlyTheFrameLevelOracleSeesIt() {
        final Written w = producer.produceBackwardShift(fresh(), log.frames(), sameMs.x(), sameMs.y());
        assertTrue(w.toString(), w.ok());
        assertEquals("same millisecond: W1 cannot separate the two frames", w.contentLastLogTime(), w.headerClusterTime());
        final Restored r = restoreAndFinish(entryOf(w));
        assertNull("W1 does not fire (the documented residue of 6.3)", r.witness());
        assertFinalDiffers(r.finalImage(), "backward shift, same ms"); // [W2-REASSERT] 11.1 final image differs from baseline
        CoreShadowRig.nc("forgery-control-3", "final image equals baseline", () -> assertFinalDiffers(baselineFinal, "same-ms control"));
        System.out.println("GREEN backward shift (same ms): W1 silent, frame level differs, " + w);
    }

    @Test
    public void forwardShiftIsInvisibleToEveryS4CheckAndOnlyTheFrameLevelOracleSeesIt() {
        final ShiftPair p = forward;
        final Written w = producer.produceForwardShift(fresh(), log.frames(), p.x(), p.y());
        assertTrue(w.toString(), w.ok());
        final ShadowEntry e = entryOf(w);
        assertEquals(p.y(), e.header().logPosition());
        assertEquals(ShadowIdentity.Outcome.OK, ShadowIdentity.check(e.header(), CoreShadowRig.member(GENESIS, log.terms())));
        final Restored r = restoreAndFinish(e);
        assertTrue("matches (genesis only with journaling off) cannot see it",
                ShadowIdentity.matches(e.header(), CoreShadowRig.identityOf(r.arm().applier)));
        assertNull("W1 cannot see header-after-content", r.witness());
        assertFinalDiffers(r.finalImage(), "forward shift"); // [W2-REASSERT] 11.1 final image differs from baseline
        System.out.println("GREEN forward shift: loader, identity, matches and W1 silent; frame level differs, " + w);
    }

    @Test
    public void foreignGenesisIsRefusedByStrictIdentity() {
        final ShiftPair p = foreign;
        final Written w = producer.produceForeignGenesis(fresh(), log.frames(), p.x(), FOREIGN_GENESIS);
        assertTrue(w.toString(), w.ok());
        final ShadowEntry e = entryOf(w);
        assertEquals(FOREIGN_GENESIS, e.header().genesisId());
        final ShadowIdentity.Outcome o = ShadowIdentity.check(e.header(), CoreShadowRig.member(GENESIS, log.terms()));
        assertEquals(ShadowIdentity.Outcome.GENESIS_MISMATCH, o);
        assertTrue(!ShadowIdentity.accepted(o, ShadowIdentity.Policy.STRICT));
        CoreShadowRig.nc("forgery-control-4", "GENESIS_MISMATCH", () -> assertEquals("GENESIS_MISMATCH",
                String.valueOf(ShadowIdentity.check(entryOf(producer.produce(fresh(), log.frames(), control.y() + 0)).header(),
                        CoreShadowRig.member(GENESIS, log.terms())))));
        System.out.println("GREEN foreign genesis: " + o);
    }

    @Test
    public void aPayloadBitFlipAfterSealingIsCorruptAtTheLoaderDigest() {
        final ShiftPair p = flip;
        final Written w = producer.produce(fresh(), log.frames(), p.y());
        assertTrue(w.toString(), w.ok());
        final ShadowEntry before = entryOf(w);
        assertEquals(ShadowEntry.State.COMPLETE, before.state());
        producer.flipPayloadByte(w);
        final Restored r = restoreAndFinish(before);
        assertEquals("CORRUPT", r.load().kind().name());
        assertEquals(ShadowCatalog.DeepResult.Kind.CORRUPT, catalog.verifyDeep(before).kind());
        CoreShadowRig.nc("forgery-control-5", "CORRUPT", () -> assertEquals("CORRUPT", String.valueOf(
                restoreAndFinish(entryOf(producer.produce(fresh(), log.frames(), control.y()))).load().kind())));
        System.out.println("GREEN payload bit flip: " + r.load().kind() + " " + r.load().reason());
    }

    @Test
    public void aSecondProducersDifferentContentAtOnePositionIsAConflict() {
        final ShiftPair p = conflict;
        final Written a = producer.produce(fresh(), log.frames(), p.x());
        final Written b = producer.produceDifferentContent(fresh(), log.frames(), p.x(), p.y());
        assertTrue(a.toString(), a.ok());
        assertTrue(b.toString(), b.ok());
        final ShadowCatalog.Scan scan = catalog.scan();
        assertEquals("CONFLICT at " + p.x(), ShadowCatalog.Answer.Kind.CONFLICT, scan.equalAt(p.x()).kind());
        CoreShadowRig.nc("forgery-control-6", "CONFLICT at", () -> {
            final long last = log.positions()[log.positions().length - 1];
            final Written c = producer.produce(fresh(), log.frames(), last);
            assertTrue(c.toString(), c.ok());
            assertEquals("CONFLICT at " + last, ShadowCatalog.Answer.Kind.CONFLICT, catalog.scan().equalAt(last).kind());
        });
        System.out.println("GREEN conflict at " + p.x() + ": " + a.recordingId() + " vs " + b.recordingId());
    }
}

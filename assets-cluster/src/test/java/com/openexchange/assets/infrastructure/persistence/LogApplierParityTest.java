// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ApplierArm;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.Arm;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.Probe;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ServiceArm;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.Kind;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.LogStep;
import io.aeron.cluster.service.Cluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RCS S1 (contract 6.1): the {@link AssetsLogApplier}, driven by frame ids alone, is the live service. One sessioned
 * log per seed goes through the service as FOLLOWER and as LEADER (container emulation, stub cluster) and through one
 * applier; after EVERY entry the emitted events, every log position an event carries, the engine's log position and
 * the demuxer counters must match, and every 100 entries (and at the end) the snapshot image bytes and the session
 * table. Restored arms (service and applier, from the service's image at three cuts) must keep matching. Two negative
 * controls keep the comparator honest: an applier fed the log without its SessionOpens must diverge exactly at the
 * first message of such a session, and the re-homed log must refuse no more traffic than the trusted story did.
 */
public class LogApplierParityTest {

    private static final long[] SEEDS = {0x5EED_0001L, 0x5EED_0002L, 0x5EED_0003L};
    private static final int FRAMES = 2_000;
    private static final int[] CUTS = {500, 1_000, 1_500};
    private static final long TAIL_SESSION = SessionedLog.sessionOf(SessionClass.ADMIN);

    /** The corpus: the sessioned story, a drain TimerEvent every 50 entries, and a CLOSE of a re-homed session then a frame from it. */
    static List<LogStep> parityLog(final long seed) {
        final List<LogStep> base = SessionedLog.from(CoreFrameStory.build(seed, FRAMES));
        final List<LogStep> withDrains = new ArrayList<>();
        for (int i = 0; i < base.size(); i++) {
            withDrains.add(base.get(i));
            if ((i + 1) % 50 == 0) {
                withDrains.add(SessionedLog.timer(AssetsLogApplier.EGRESS_DRAIN_TIMER_BASE, base.get(i).timestamp()));
            }
        }
        final long ts = base.get(base.size() - 1).timestamp() + 1;
        withDrains.add(SessionedLog.close(TAIL_SESSION, ts));
        withDrains.add(SessionedLog.message(TAIL_SESSION, ts,
                new CoreFrameEncoders().deposit(7_700_001L, 1, 0, CoreFrameStory.ONE, 0xD0L, 7_700_001L)));
        return SessionedLog.renumber(withDrains);
    }

    /** First index >= n whose NEXT entry is a session message, so a restored arm stamps a position at once. */
    private static int cutAt(final List<LogStep> log, final int n) {
        for (int i = n; i + 1 < log.size(); i++) {
            if (log.get(i + 1).kind() == Kind.MESSAGE) {
                return i;
            }
        }
        throw new IllegalStateException("no cut after " + n);
    }

    private static String checkpoint(final Arm a) {
        return a.engine().sessions().sessionIds().toString();
    }

    @Test
    public void theApplierIsTheLiveServiceEntryByEntry() {
        for (final long seed : SEEDS) {
            run(seed);
        }
    }

    private static void run(final long seed) {
        final List<CoreFrameStory.Step> story = CoreFrameStory.build(seed, FRAMES);
        final List<LogStep> log = parityLog(seed);
        final int tailClose = log.size() - 2;
        final String tag = "seed=0x" + Long.toHexString(seed);

        final ServiceArm follower = new ServiceArm(Cluster.Role.FOLLOWER);
        final ServiceArm leader = new ServiceArm(Cluster.Role.LEADER);
        final ApplierArm applier = new ApplierArm();
        final ApplierArm skipOpens = new ApplierArm(true);
        final Probe pf = new Probe(follower);
        final Probe pl = new Probe(leader);
        final Probe pa = new Probe(applier);
        Probe ps = new Probe(skipOpens);

        final int[] cuts = new int[CUTS.length];
        for (int c = 0; c < CUTS.length; c++) {
            cuts[c] = cutAt(log, CUTS[c]);
        }
        final List<Arm> restored = new ArrayList<>();
        final List<Probe> restoredProbes = new ArrayList<>();

        // The skip-open control's expected first divergence: the first message from a session whose OPEN it never saw.
        final Set<Long> opened = new HashSet<>();
        int expectedSkipMismatch = -1;
        for (int i = 0; i < log.size() && expectedSkipMismatch < 0; i++) {
            final LogStep s = log.get(i);
            if (s.kind() == Kind.OPEN) {
                opened.add(s.sessionId());
            } else if (s.kind() == Kind.MESSAGE && opened.contains(s.sessionId())) {
                expectedSkipMismatch = i;
            }
        }
        int skipMismatch = -1;
        long unauthorisedBeforeTail = -1;

        for (int i = 0; i < log.size(); i++) {
            final LogStep s = log.get(i);
            final String at = tag + " step=" + i + " kind=" + s.kind() + " session=" + s.sessionId()
                    + " position=" + s.logPosition();
            follower.apply(s);
            leader.apply(s);
            applier.apply(s);
            final String expected = pa.step();
            assertEquals("FOLLOWER service vs applier, " + at, pf.step(), expected);
            assertEquals("LEADER service vs applier, " + at, pl.step(), expected);
            for (int r = 0; r < restored.size(); r++) {
                restored.get(r).apply(s);
                assertEquals("restored arm " + r + " vs uninterrupted applier, " + at, expected, restoredProbes.get(r).step());
            }
            if (ps != null) {
                try {
                    skipOpens.apply(s);
                    if (!ps.step().equals(expected) && skipMismatch < 0) {
                        skipMismatch = i;
                    }
                } catch (final RuntimeException diverged) {
                    if (skipMismatch < 0) {
                        skipMismatch = i;
                    }
                    ps = null; // a diverged instance is discarded, as any refused applier is
                }
            }

            if (i % 100 == 0 || i == log.size() - 1) {
                final byte[] image = applier.image();
                final String sessions = checkpoint(applier);
                assertArrayEquals("FOLLOWER image, " + at, follower.image(), image);
                assertArrayEquals("LEADER image, " + at, leader.image(), image);
                assertEquals("FOLLOWER section 9, " + at, checkpoint(follower), sessions);
                assertEquals("LEADER section 9, " + at, checkpoint(leader), sessions);
                for (int r = 0; r < restored.size(); r++) {
                    assertArrayEquals("restored arm " + r + " image, " + at, image, restored.get(r).image());
                    assertEquals("restored arm " + r + " section 9, " + at, sessions, checkpoint(restored.get(r)));
                }
            }

            for (final int cut : cuts) {
                if (i == cut) {
                    final byte[] image = follower.image();
                    final ServiceArm rs = new ServiceArm(Cluster.Role.FOLLOWER, follower.containerCopy());
                    rs.service.restoreFromImage(new UnsafeBuffer(image), image.length);
                    assertEquals("restore must not stop the node, " + at, 0, rs.failFastCalls[0]);
                    final ApplierArm ra = new ApplierArm();
                    ra.applier.restore(new UnsafeBuffer(image), image.length);
                    assertArrayEquals("restored images, " + at, image, rs.image());
                    assertArrayEquals("restored images, " + at, image, ra.image());
                    restored.add(rs);
                    restoredProbes.add(new Probe(rs));
                    restored.add(ra);
                    restoredProbes.add(new Probe(ra));
                }
            }
            if (i == tailClose - 1) {
                unauthorisedBeforeTail = applier.engine().getUnauthorisedCount();
            }
        }

        assertEquals(tag + ": all three cuts restored twice", 2 * CUTS.length, restored.size());
        // Non-vacuity: a long log, a rich event stream, stamped positions, and a leader that armed timers.
        assertTrue(tag + ": log length " + log.size(), log.size() > FRAMES);
        assertTrue(tag + ": events " + applier.sink.events().size(), applier.sink.events().size() > FRAMES);
        assertTrue(tag + ": stamped calls " + applier.sink.stamps.size(), applier.sink.stamps.size() > 100);
        assertTrue(tag + ": the leader armed timers", leader.cluster.scheduled > 0);
        assertEquals(tag + ": a follower never arms", 0, follower.cluster.scheduled);
        for (final ServiceArm a : new ServiceArm[] {follower, leader}) {
            assertEquals(tag + ": no node-local stop on a sane story", 0, a.failFastCalls[0]);
        }
        assertEquals(tag, 0, applier.failFastCalls[0]);
        assertFalse(tag, applier.applier.failed());

        // Coverage guard: re-homing created no new rejects, and the story's deliberate unauthorised traffic is there.
        final long trustedStoryUnauthorised = CoreFrameStory.replay(story).engine.getUnauthorisedCount();
        assertTrue(tag + ": the story carries unauthorised traffic", trustedStoryUnauthorised > 0);
        assertEquals(tag + ": re-homing created no new rejects", trustedStoryUnauthorised, unauthorisedBeforeTail);
        assertEquals(tag + ": the frame after its session's CLOSE is refused", unauthorisedBeforeTail + 1,
                applier.engine().getUnauthorisedCount());
        assertFalse(tag + ": the closed session left the table",
                applier.engine().sessions().sessionIds().contains(TAIL_SESSION));

        // Skip-open negative control: the comparator must see a missing SessionOpen, at the first frame it matters.
        if (skipMismatch < 0) {
            fail(tag + ": comparator blind: an applier fed no SessionOpen compared equal");
        }
        assertEquals(tag + ": first divergence of the skip-open arm", expectedSkipMismatch, skipMismatch);
        if (ps != null) {
            assertNotEquals(tag + ": section 9 must differ without the opens",
                    checkpoint(applier), checkpoint(skipOpens));
            assertFalse(tag + ": image must differ without the opens", Arrays.equals(applier.image(), skipOpens.image()));
        }
    }
}

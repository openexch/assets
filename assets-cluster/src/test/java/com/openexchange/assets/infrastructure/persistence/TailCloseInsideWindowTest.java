// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.auth.CorePrincipal;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ApplierArm;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.Kind;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.LogStep;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RCS S1 (contract 6.3, RRS RF-1): a session opened before a snapshot cut P_s and closed between P_s and the commit
 * position P_cm. Replaying the tail from the snapshot by FRAME id reproduces the uninterrupted run exactly; replaying
 * it through a container session map frozen at P_cm (the design RF-1 forbids) leaves the session OPEN in section 9 and
 * refuses its in-window commands. The second arm is a negative control: if it ever compares equal, the fixture no
 * longer exercises RF-1 and the test fails.
 */
public class TailCloseInsideWindowTest {

    private static final long SESSION = 777L;
    private static final int OPEN_AT = 100;
    private static final int CUT = 200;          // P_s = end of step 200
    private static final int CLOSE_AT = 250;
    private static final int LATE_CREATE_AT = 260;
    private static final int COMMIT = 300;       // P_cm = end of step 300

    /** Log T: the sessioned story with session 777 opened, registered, carrying the gateway traffic, closed in the window. */
    static List<LogStep> tailLog() {
        final List<LogStep> base = SessionedLog.from(CoreFrameStory.build(0x7A11L, 400));
        final CoreFrameEncoders enc = new CoreFrameEncoders();
        final List<LogStep> t = new ArrayList<>(base.subList(0, OPEN_AT));
        final long tsOpen = t.get(t.size() - 1).timestamp();
        t.add(SessionedLog.open(SESSION, tsOpen,
                CorePrincipal.forUsers(SessionClass.GATEWAY, 77, 1, 2, 3, 4, 5, 6).encode()));      // step 100
        t.add(SessionedLog.message(SESSION, tsOpen, enc.register(7_770_001L, SessionClass.GATEWAY))); // step 101
        int next = OPEN_AT;
        while (t.size() < CLOSE_AT) {                                                               // steps 102..249
            final LogStep s = base.get(next++);
            t.add(s.kind() == Kind.MESSAGE && s.sessionId() == CoreFrameStory.GATEWAY_SESSION ? s.from(SESSION) : s);
        }
        t.add(SessionedLog.close(SESSION, t.get(t.size() - 1).timestamp()));                          // step 250
        while (t.size() < LATE_CREATE_AT) {
            t.add(base.get(next++));
        }
        t.add(SessionedLog.message(SESSION, t.get(t.size() - 1).timestamp(),                        // step 260
                enc.create(7_770_002L, 1, 0, 0x7A11L, 7_770_002L, 1, 0, 0, 0, 100L * CoreFrameStory.ONE,
                        CoreFrameStory.ONE, 0, 0, 0, 0, 0)));
        while (next < base.size()) {
            t.add(base.get(next++));
        }
        if (t.size() <= COMMIT + 10) {
            throw new IllegalStateException("fixture too short: " + t.size());
        }
        return SessionedLog.renumber(t);
    }

    private static List<String> renders(final ApplierArm arm, final int from) {
        final List<String> out = new ArrayList<>();
        for (int i = from; i < arm.sink.events().size(); i++) {
            out.add(arm.sink.events().get(i).render());
        }
        return out;
    }

    private static ApplierArm restoredFrom(final byte[] image) {
        final ApplierArm arm = new ApplierArm();
        arm.applier.restore(new UnsafeBuffer(image), image.length);
        return arm;
    }

    @Test
    public void frameIdRoutingReplaysACloseInsideTheWindowAndAFrozenContainerMapDoesNot() {
        final List<LogStep> log = tailLog();
        assertEquals(Kind.OPEN, log.get(OPEN_AT).kind());
        assertEquals(Kind.CLOSE, log.get(CLOSE_AT).kind());
        assertEquals(SESSION, log.get(LATE_CREATE_AT).sessionId());
        int inWindow = 0;
        for (int i = CUT + 1; i < CLOSE_AT; i++) {
            if (log.get(i).kind() == Kind.MESSAGE && log.get(i).sessionId() == SESSION) {
                inWindow++;
            }
            if (log.get(i).kind() == Kind.OPEN) {
                fail("fixture: an OPEN inside the window would need its own container rule");
            }
        }
        assertTrue("fixture: session 777 must send inside (P_s, close)", inWindow > 0);

        // Baseline: one applier, every entry, by frame id.
        final ApplierArm baseline = new ApplierArm();
        byte[] imageAtCut = null;
        int eventsAtCut = -1;
        for (int i = 0; i < log.size(); i++) {
            baseline.apply(log.get(i));
            if (i == CUT) {
                imageAtCut = baseline.image();
                eventsAtCut = baseline.sink.events().size();
                assertTrue("777 is OPEN at P_s", baseline.engine().sessions().sessionIds().contains(SESSION));
            }
        }
        final byte[] imageEnd = baseline.image();
        final List<Long> sessionsEnd = baseline.engine().sessions().sessionIds();
        assertFalse("777 is closed at the end", sessionsEnd.contains(SESSION));
        final List<String> eventsAfterCut = renders(baseline, eventsAtCut);

        // Frame-id arm: restore at P_s, replay the tail by frame id.
        final ApplierArm frameId = restoredFrom(imageAtCut);
        int authorisedInWindow = 0; // 777's in-window commands the frame-id replay applies (the story also sends refused ones)
        for (int i = CUT + 1; i < log.size(); i++) {
            final long unauthorised = frameId.engine().getUnauthorisedCount();
            frameId.apply(log.get(i));
            if (i < CLOSE_AT && log.get(i).kind() == Kind.MESSAGE && log.get(i).sessionId() == SESSION
                    && frameId.engine().getUnauthorisedCount() == unauthorised) {
                authorisedInWindow++;
            }
        }
        assertTrue("fixture: 777 must have authorised commands inside (P_s, close)", authorisedInWindow > 0);
        assertArrayEquals("frame-id tail == uninterrupted run", imageEnd, frameId.image());
        assertEquals("events after the cut", eventsAfterCut, renders(frameId, 0));
        assertEquals("section 9", sessionsEnd, frameId.engine().sessions().sessionIds());

        // Container-map arm (negative control): sessions in (P_s, P_cm] resolve through the map as it stands at P_cm.
        final Set<Long> frozen = new HashSet<>();
        for (int i = 0; i <= COMMIT; i++) {
            final LogStep s = log.get(i);
            if (s.kind() == Kind.OPEN) {
                frozen.add(s.sessionId());
            } else if (s.kind() == Kind.CLOSE) {
                frozen.remove(s.sessionId());
            }
        }
        assertFalse("777 is absent from the container map at P_cm", frozen.contains(SESSION));
        final ApplierArm containerMap = restoredFrom(imageAtCut);
        final AssetsLogApplier a = containerMap.applier;
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[0]);
        for (int i = CUT + 1; i < log.size(); i++) {
            final LogStep s = log.get(i);
            if (i > COMMIT) {
                containerMap.apply(s); // past P_cm the container is current: live routing
                continue;
            }
            switch (s.kind()) {
                case MESSAGE -> {
                    frame.wrap(s.bytes());
                    a.onSessionMessage(frozen.contains(s.sessionId()) ? s.sessionId() : -1L, s.logPosition(), s.timestamp(),
                            frame, 0, s.bytes().length);
                }
                case CLOSE -> {
                    if (frozen.contains(s.sessionId())) { // an unknown close calls no service callback (CSA:547-553)
                        a.onSessionClose(s.sessionId(), s.logPosition(), s.timestamp(), CloseReason.CLIENT_ACTION);
                    }
                }
                default -> containerMap.apply(s);
            }
        }
        final boolean equal = java.util.Arrays.equals(imageEnd, containerMap.image())
                && sessionsEnd.equals(containerMap.engine().sessions().sessionIds());
        if (equal) {
            fail("RF-1 not reproduced: a container map frozen at P_cm replayed the tail correctly");
        }
        assertTrue("777 stays OPEN in section 9", containerMap.engine().sessions().sessionIds().contains(SESSION));
        assertEquals("exactly 777's authorised in-window commands are refused instead of applied", authorisedInWindow,
                containerMap.engine().getUnauthorisedCount() - frameId.engine().getUnauthorisedCount());
    }
}

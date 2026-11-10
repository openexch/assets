// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventBatchDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventDecoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.client.EgressListener;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The {@code SweepSurvivesRestoreClusterTest} story (a GTD order whose leg was learned from a
 * BRIDGE outcome, a snapshot, a cold restart, exactly one {@code MeDispatch{CANCEL}} at the deadline) in STUB mode on
 * each of the three restore paths, single member, embedded driver. New here: the cancel's {@code OrderEvent
 * DISPATCHED} carries the end of the sweep TimerEvent frame that minted it, checked against the member's own
 * log.
 *
 * <p>Controls: the stamp off ({@code cancel event logPosition <x> is not the TimerEvent frame end <e>}); the sweep
 * re-arm id from term 0 ({@code armed sweep id <id> belongs to term 0}).</p>
 *
 * <p>D5: the term-0 control used to observe the K3 shape through the deadline ({@code no CANCEL within <window>} or a
 * second CANCEL). That outcome depends on how many term-0 TimerEvents the consensus module replays after the restart
 * (each one makes it swallow a later {@code scheduleTimer} of that id), which the test does not control, so it passed
 * and failed by timing. The oracle now reads the armed id itself ({@code ServiceProbe.lastArmedSweepTimerId}) against
 * the current term's id, which is deterministic; the deadline assertions stay for the positive rows. G28 (the term id
 * across a TAIL) stays covered by 11.11's halt control.</p>
 */
public class SweepSurvivesStubRestoreClusterTest {

    private static final long USER = 7;
    private static final long LEG = 77;
    private static final long ORDER_ID = 1;
    private static final long GTD_WINDOW_MS = 15_000;
    private static final long QUIET_AFTER_MS = 3_000;

    /** OrderEvents as {@code {orderId, eventKind ordinal, logPosition, arrivedMs}} plus their text. */
    static final class Events implements EgressListener {
        final List<String> frames = new CopyOnWriteArrayList<>();
        final List<long[]> dispatched = new CopyOnWriteArrayList<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final OrderEventDecoder event = new OrderEventDecoder();
        private final OrderEventBatchDecoder batch = new OrderEventBatchDecoder();

        public void onMessage(final long sessionId, final long timestamp, final DirectBuffer buffer, final int offset,
                              final int length, final io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            if (header.templateId() == OrderEventDecoder.TEMPLATE_ID) {
                event.wrapAndApplyHeader(buffer, offset, header);
                add(event.orderId(), String.valueOf(event.eventKind()), event.logPosition());
            } else if (header.templateId() == OrderEventBatchDecoder.TEMPLATE_ID) {
                batch.wrapAndApplyHeader(buffer, offset, header);
                for (final OrderEventBatchDecoder.EventsDecoder e : batch.events()) {
                    add(e.orderId(), String.valueOf(e.eventKind()), e.logPosition());
                }
            }
        }

        private void add(final long orderId, final String kind, final long logPosition) {
            frames.add("EVENT order=" + orderId + " kind=" + kind + " logPosition=" + logPosition);
            if ("DISPATCHED".equals(kind)) {
                dispatched.add(new long[] {orderId, logPosition, System.currentTimeMillis()});
            }
        }
    }

    /** The GTD story up to the stub snapshot; returns {@code {pCm, expiresAtMs}} and the committed log. */
    private record Setup(long pCm, long expiresAtMs, RecordedLog log) { }

    private static void register(final ShadowClusterRig rig, final Client c, final SessionClass cls,
                                 final boolean settlements, final boolean orders, final boolean dispatch) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);
        final int len = TestAuth.encodeRegisterSession(buf, 0x8E6L, cls, true, true, false, settlements, false, orders,
                dispatch, false);
        c.offer(buf, len);
        rig.await(() -> c.in.registered.get() > 0, 30_000, cls + " RegisterSession must be acked");
    }

    private static Setup gtdStory(final StubStory s) throws Exception {
        final ShadowClusterRig rig = s.rig;
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        final Events gwEvents = new Events();
        final Client gateway = rig.connect(SessionClass.GATEWAY, s.m, gwEvents);
        register(rig, gateway, SessionClass.GATEWAY, false, true, false);
        final Client forwarder = rig.connect(SessionClass.FORWARDER, s.m);
        register(rig, forwarder, SessionClass.FORWARDER, false, false, true);
        s.tool.offer(enc.buf, enc.deposit(0x7001L, USER, CoreRestoreRig.usd(), 1_000 * ShadowClusterRig.ONE));
        rig.await(() -> s.tool.in.lastDepositAckCorr >= 0x7001L, 30_000, "the USER deposit must be acked");
        s.admin.offer(enc.buf, enc.gateOpen(0xAD1L));
        rig.await(() -> s.admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 30_000,
                () -> "the gate open must be accepted: " + s.admin.in.frames);
        final long expiresAtMs = System.currentTimeMillis() + GTD_WINDOW_MS; // cluster time is epoch ms (K3)
        gateway.offer(enc.buf, enc.limit(0xA2L, USER, 2, 1, true, 100 * ShadowClusterRig.ONE, ShadowClusterRig.ONE,
                TimeInForce.GTD, expiresAtMs));
        rig.await(() -> forwarder.in.count(f -> f.startsWith("DISPATCH seq=1 ") && f.contains("kind=CREATE")) > 0, 30_000,
                () -> "the forwarder must receive the create: " + forwarder.in.frames);
        final Client bridge = rig.connect(SessionClass.BRIDGE, s.m);
        register(rig, bridge, SessionClass.BRIDGE, true, false, false);
        bridge.offer(enc.buf, enc.createApplied(ORDER_ID, USER, 1, LEG));
        rig.await(() -> gwEvents.frames.stream().anyMatch(f -> f.contains("order=" + ORDER_ID + " kind=ACK_NEW")), 30_000,
                () -> "the BRIDGE outcome must give the order its leg (ACK_NEW): " + gwEvents.frames);
        assertEquals("the record carries the learned leg", LEG,
                s.m.service.engine().orders().orders().get(ORDER_ID).currentLegId);
        s.deposits(6); // frames between the leg and the snapshot: the TAIL base lies after the leg
        final long pCm = s.snapshot();
        s.deposits(6);
        return new Setup(pCm, expiresAtMs, s.log());
    }

    /** After the restart: a forwarder and a gateway connect, the deadline passes, the CANCEL and its event arrive. */
    private static void assertOneCancelAtTheTimerFrameEnd(final StubStory s, final Setup st) throws Exception {
        final ShadowClusterRig rig = s.rig;
        final Events gwEvents = new Events();
        final Client gateway = rig.connect(SessionClass.GATEWAY, s.m, gwEvents);
        register(rig, gateway, SessionClass.GATEWAY, false, true, false);
        final Client forwarder = rig.connect(SessionClass.FORWARDER, s.m);
        register(rig, forwarder, SessionClass.FORWARDER, false, false, true);
        assertTrue("the restart and reconnect finished before the deadline",
                System.currentTimeMillis() < st.expiresAtMs() - 500);
        // D5: the armed sweep id belongs to the CURRENT term (the restart elected a new one), checked before the deadline.
        // The sweep re-arms every SWEEP_PERIOD_MS while the GTD rests; an arm that ran before the new term event reached
        // the service carries the previous term, so the oracle waits for the current term's id over many re-arms (a
        // node that arms term-0 ids never gets there, whatever the timing).
        final long[] seen = {-1, -1, -1};
        final boolean current = awaitQuietly(rig, () -> {
            seen[0] = ServiceProbe.lastArmedSweepTimerId(s.m.service);
            seen[1] = ServiceProbe.expectedSweepTimerId(s.m.service);
            seen[2] = ServiceProbe.leadershipTermId(s.m.service);
            return seen[2] > 0 && seen[0] == seen[1];
        }, 5_000);
        if (!current) {
            assertTrue("the restart elected a term past 0 (the control needs it): " + seen[2], seen[2] > 0);
            throw new AssertionError("armed sweep id " + seen[0] + " belongs to term "
                    + (seen[0] < 0 ? "none" : String.valueOf((seen[0] - CoreLogApplier.SWEEP_TIMER_BASE)
                    / CoreLogApplier.TIMER_ID_TERM_STRIDE)) + ", the node is in term " + seen[2] + " (expected "
                    + seen[1] + ")");
        }
        final long until = st.expiresAtMs() + 5_000;
        rig.await(() -> forwarder.in.count(f -> f.contains("kind=CANCEL") && f.contains(" order=" + ORDER_ID + " ")) > 0,
                until - System.currentTimeMillis(), () -> "no CANCEL within " + (GTD_WINDOW_MS + 5_000) + " ms of the"
                        + " create (deadline " + st.expiresAtMs() + "); forwarder frames " + forwarder.in.frames);
        rig.pumpMs(QUIET_AFTER_MS);
        final List<String> cancels = forwarder.in.frames.stream()
                .filter(f -> f.contains("kind=CANCEL") && f.contains(" order=" + ORDER_ID + " ")).toList();
        if (cancels.size() != 1) {
            throw new AssertionError("second CANCEL after restart: " + cancels);
        }
        final String cancel = cancels.get(0);
        final long at = Long.parseLong(cancel.substring(cancel.indexOf(" at=") + 4).trim());
        assertTrue("no CANCEL before the deadline: " + cancel + " deadline " + st.expiresAtMs(), at >= st.expiresAtMs());
        rig.await(() -> !gwEvents.dispatched.isEmpty(), 10_000, () -> "the DISPATCHED event: " + gwEvents.frames);
        final long x = gwEvents.dispatched.get(0)[1];
        final RecordedLog log = s.log();
        LogFrame timer = null;
        for (final LogFrame f : log.frames()) {
            if (f.kind() == FrameKind.TIMER_EVENT && CoreLogApplier.isSweepTimerId(f.correlationId())
                    && f.timestamp() >= st.expiresAtMs() && timer == null) {
                timer = f;
            }
        }
        assertTrue("a sweep TimerEvent at or after the deadline exists in the member's log", timer != null);
        final long e = timer.position();
        if (x != e) {
            throw new AssertionError("cancel event logPosition " + x + " is not the TimerEvent frame end " + e);
        }
        System.out.println("[11.10] one CANCEL " + cancel + "; DISPATCHED logPosition " + x + " == sweep TimerEvent frame"
                + " end " + e + " (ts " + timer.timestamp() + ", deadline " + st.expiresAtMs() + ")");
    }

    /** {@code rig.await} that answers false at the deadline instead of throwing. */
    private static boolean awaitQuietly(final ShadowClusterRig rig, final java.util.function.BooleanSupplier done,
                                        final long timeoutMs) {
        try {
            rig.await(done, timeoutMs, "quiet");
            return true;
        } catch (final AssertionError timedOut) {
            return false;
        }
    }

    private enum PathRow { EQUAL, SKIP, TAIL }

    private static void run(final int port, final PathRow path, final String control, final String expected)
            throws Exception {
        try (StubStory s = new StubStory(port, "rcs-s4-11.10-" + path + (control == null ? "" : "-" + control),
                StubStory.shadow()).start(true)) {
            final Setup st = gtdStory(s);
            final int cm = StubStory.indexOf(st.log(), st.pCm());
            final long ps = switch (path) {
                case EQUAL -> st.pCm();
                case SKIP -> st.log().positions()[cm + 3];
                case TAIL -> st.log().positions()[cm - 3];
            };
            s.produce(st.log(), ps);
            s.rig.forgetAll(); // the image never rests here: the sweep ticks every 100 ms while the GTD rests
            s.kill();
            if (control == null) {
                s.restart();
            } else {
                s.restart(control);
            }
            s.awaitReady();
            RestoreDrillDryRun.requirePathLine(s.rig.tee, path + " P_s=" + ps, "11.10");
            assertEquals("the GTD order and its leg survived the " + path + " restore", LEG,
                    s.m.service.engine().orders().orders().get(ORDER_ID).currentLegId);
            assertTrue("the deadline index was rebuilt", s.m.service.engine().deadlines().size() > 0);
            s.connect(false);
            if (expected == null) {
                assertOneCancelAtTheTimerFrameEnd(s, st);
                System.out.println("GREEN 11.10 " + path + ": " + s.pathLine() + "; sweep fires "
                        + s.m.service.sweepTimerFires());
            } else {
                RestoreDrillDryRun.nc("11.10-" + control, expected, () -> {
                    try {
                        assertOneCancelAtTheTimerFrameEnd(s, st);
                    } catch (final AssertionError e) {
                        throw e;
                    } catch (final Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
            }
        }
    }

    @Test
    public void equalPath() throws Exception {
        run(26000, PathRow.EQUAL, null, null);
    }

    @Test
    public void skipPath() throws Exception {
        run(26100, PathRow.SKIP, null, null);
    }

    @Test
    public void tailPath() throws Exception {
        run(26200, PathRow.TAIL, null, null);
    }

    @Test
    public void controlRf3StampOff() throws Exception {
        run(26300, PathRow.TAIL, "rf3StampOff", "is not the TimerEvent frame end");
    }

    @Test
    public void controlSweepReArmIdFromTermZero() throws Exception {
        run(26400, PathRow.TAIL, "timerIdsFromTermZero", "armed sweep id");
    }
}

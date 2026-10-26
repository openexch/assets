// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.LogStep;
import com.openexchange.cluster.SnapshotEnvelope;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RCS S1 (contract 2.1/2.2 with review FIX-1, FIX-2, FIX-3): the entry rules a replayer relies on once the service's
 * own fence is not around it. A fresh applier applies without any wiring (default no-op sink); any exception latches
 * the instance and every later entry refuses; {@code restore} and {@code genesis} need a pristine instance, and a
 * candidate the envelope refuses (nothing decoded) leaves it pristine and retryable while a refusal after the CRC
 * latches it.
 */
public class AssetsLogApplierEntryContractTest {

    private static final long ADMIN = SessionedLog.sessionOf(SessionClass.ADMIN);
    private static final String DISCARD = "assets applier failed; discard the instance";

    private static List<LogStep> fundLog() {
        final CoreFrameEncoders enc = new CoreFrameEncoders();
        final List<LogStep> log = new ArrayList<>(SessionedLog.from(List.of()));
        log.add(SessionedLog.message(ADMIN, CoreFrameStory.T0, enc.deposit(1, 7, 0, 1_000 * CoreFrameStory.ONE, 0xD0L, 1)));
        log.add(SessionedLog.message(ADMIN, CoreFrameStory.T0 + 1, enc.withdraw(2, 7, 0, 400 * CoreFrameStory.ONE, 0xD0L, 2)));
        return SessionedLog.renumber(log);
    }

    private static void drive(final AssetsLogApplier applier, final List<LogStep> log) {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[0]);
        for (final LogStep s : log) {
            switch (s.kind()) {
                case OPEN -> applier.onSessionOpen(s.sessionId(), s.logPosition(), s.timestamp(), s.bytes());
                case MESSAGE -> {
                    frame.wrap(s.bytes());
                    applier.onSessionMessage(s.sessionId(), s.logPosition(), s.timestamp(), frame, 0, s.bytes().length);
                }
                default -> throw new IllegalStateException("kind " + s.kind());
            }
        }
    }

    private static byte[] image(final AssetsLogApplier applier) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = applier.serialize(buffer);
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    private static byte[] fundedImage() {
        final AssetsLogApplier source = new AssetsLogApplier(() -> { });
        drive(source, fundLog());
        return image(source);
    }

    private static void assertLatched(final AssetsLogApplier applier) {
        assertTrue(applier.failed());
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[64]);
        for (final org.junit.function.ThrowingRunnable entry : new org.junit.function.ThrowingRunnable[] {
                () -> applier.onSessionOpen(77, 64, 1, new byte[0]),
                () -> applier.onSessionClose(77, 64, 1, CloseReason.CLIENT_ACTION),
                () -> applier.onSessionMessage(77, 64, 1, frame, 0, 64),
                () -> applier.onTimerEvent(AssetsLogApplier.SWEEP_TIMER_BASE, 64, 1),
                () -> applier.onNewLeadershipTermEvent(1, 64, 1, java.util.concurrent.TimeUnit.MILLISECONDS),
                () -> applier.restore(frame, 64),
                applier::genesis,
                () -> applier.serialize(new ExpandableArrayBuffer())}) {
            assertEquals(DISCARD, assertThrows(IllegalStateException.class, entry).getMessage());
        }
    }

    @Test
    public void aFreshApplierAppliesWithoutAnySinkWired() {
        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        drive(applier, fundLog()); // the deposit and withdrawal emit acks and balance updates
        assertEquals(600 * CoreFrameStory.ONE, applier.engine().account(7).available(0));
        assertFalse(applier.failed());
        assertEquals(AssetsLogApplier.ENGINE_ID_ASSETS, applier.engineId());
        assertThrows(NullPointerException.class, () -> new AssetsLogApplier(null));
        assertThrows(NullPointerException.class, () -> applier.setEventSink(null));
    }

    @Test
    public void anApplyFailureLatchesEveryEntry() {
        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        final IllegalStateException injected = new IllegalStateException("injected sink");
        applier.setEventSink((com.openexchange.assets.domain.AssetsEventSink) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {com.openexchange.assets.domain.AssetsEventSink.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("onWithdrawReject")) {
                        throw injected;
                    }
                    return null;
                }));
        final List<LogStep> log = fundLog();
        drive(applier, log.subList(0, log.size() - 2)); // sessions only
        final CoreFrameEncoders enc = new CoreFrameEncoders();
        final byte[] tooMuch = enc.withdraw(3, 7, 0, 1L, 0xD0L, 3); // no balance: rejected, the sink throws
        assertSame(injected, assertThrows(IllegalStateException.class, () -> applier.onSessionMessage(ADMIN, 4_096,
                CoreFrameStory.T0, new UnsafeBuffer(tooMuch), 0, tooMuch.length)));
        assertLatched(applier);
    }

    @Test
    public void restoreAndGenesisNeedAPristineInstance() {
        final byte[] image = fundedImage();

        final AssetsLogApplier applied = new AssetsLogApplier(() -> { });
        applied.onSessionOpen(901, 64, CoreFrameStory.T0, SessionedLog.principalOf(SessionClass.BRIDGE));
        assertTrue(assertThrows(IllegalStateException.class, () -> applied.restore(new UnsafeBuffer(image), image.length))
                .getMessage().contains("pristine"));
        assertTrue(assertThrows(IllegalStateException.class, applied::genesis).getMessage().contains("pristine"));
        assertFalse("a precondition refusal decodes nothing and does not latch", applied.failed());

        final AssetsLogApplier twice = new AssetsLogApplier(() -> { });
        assertNull(twice.lastRestored());
        twice.restore(new UnsafeBuffer(image), image.length);
        assertNotNull(twice.lastRestored());
        assertArrayEquals("restore round-trips", image, image(twice));
        assertTrue("a second restore would merge two states", assertThrows(IllegalStateException.class,
                () -> twice.restore(new UnsafeBuffer(image), image.length)).getMessage().contains("pristine"));
        assertArrayEquals(image, image(twice));

        final AssetsLogApplier genesis = new AssetsLogApplier(() -> { });
        genesis.genesis();
        assertThrows(IllegalStateException.class, genesis::genesis);
        assertThrows(IllegalStateException.class, () -> genesis.restore(new UnsafeBuffer(image), image.length));
    }

    @Test
    public void aCandidateTheEnvelopeRefusesLeavesThePristineInstanceRetryable() {
        final byte[] image = fundedImage();
        final byte[] flipped = image.clone();
        flipped[SnapshotEnvelope.payloadOffset() + 20] ^= 1;
        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        assertTrue(assertThrows(IllegalStateException.class, () -> applier.restore(new UnsafeBuffer(flipped), flipped.length))
                .getMessage().contains("CRC32C"));
        assertThrows(IllegalStateException.class, () -> applier.restore(new UnsafeBuffer(new byte[8]), 0)); // empty
        assertFalse(applier.failed());
        assertEquals("nothing was decoded", 0, applier.engine().accountCount());
        applier.restore(new UnsafeBuffer(image), image.length); // the next candidate
        assertArrayEquals(image, image(applier));
    }

    @Test
    public void aRefusalAfterTheCrcLatchesTheInstance() {
        // A CRC-valid image that lies about the ledger (locked != sum of holds), as SnapshotEnvelopeAdoptionTest builds it.
        final com.openexchange.assets.application.engine.AssetsEngine source = new com.openexchange.assets.application.engine.AssetsEngine();
        source.setEventSink(new SnapshotEnvelopeAdoptionTest.NoopSink());
        final com.openexchange.assets.domain.commands.DepositCommand d = new com.openexchange.assets.domain.commands.DepositCommand();
        d.reset();
        d.setUserId(7);
        d.setAssetId(0);
        d.setAmount(1_000);
        source.applyCommand(com.openexchange.assets.application.engine.AssetsEngine.CMD_DEPOSIT, d, 10);
        final com.openexchange.assets.domain.commands.HoldCommand h = new com.openexchange.assets.domain.commands.HoldCommand();
        h.reset();
        h.setOrderId(1);
        h.setUserId(7);
        h.setAssetId(0);
        h.setAmount(400);
        source.applyCommand(com.openexchange.assets.application.engine.AssetsEngine.CMD_HOLD, h, 11);
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer();
        final int payloadLength = BalanceSnapshotCodec.serialize(source, payload, SnapshotEnvelope.payloadOffset());
        final int lockedOffset = SnapshotEnvelope.payloadOffset() + 8 + 7 * 8 + 1 + 4 + 2 + 4 + 4 + 8 + 4 + 8;
        payload.putLong(lockedOffset, payload.getLong(lockedOffset) + 1);
        final int length = new SnapshotEnvelope().seal(payload, payloadLength);

        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        assertTrue(assertThrows(IllegalStateException.class, () -> applier.restore(payload, length)).getMessage()
                .contains("locked"));
        assertNull(applier.lastRestored());
        assertLatched(applier);
    }

    @Test
    public void timerKindsAreTheServicesIds() {
        assertEquals(AssetsLogApplier.TIMER_NONE, AssetsLogApplier.timerKind(1));
        assertEquals(AssetsLogApplier.TIMER_NONE, AssetsLogApplier.timerKind(AssetsLogApplier.EGRESS_DRAIN_TIMER_BASE - 1));
        assertEquals(AssetsLogApplier.TIMER_DRAIN, AssetsLogApplier.timerKind(AssetsClusteredService.EGRESS_DRAIN_TIMER_ID));
        assertEquals(AssetsLogApplier.TIMER_DRAIN, AssetsLogApplier.timerKind(AssetsLogApplier.SWEEP_TIMER_BASE - 1));
        assertEquals(AssetsLogApplier.TIMER_SWEEP, AssetsLogApplier.timerKind(AssetsClusteredService.SWEEP_TIMER_ID));
        assertEquals(AssetsLogApplier.TIMER_SWEEP, AssetsLogApplier.timerKind(
                AssetsClusteredService.SWEEP_TIMER_BASE + 7 * AssetsClusteredService.TIMER_ID_TERM_STRIDE));
        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        assertEquals(AssetsLogApplier.TIMER_DRAIN, applier.onTimerEvent(AssetsLogApplier.EGRESS_DRAIN_TIMER_BASE, 64, 1));
        assertEquals("only a session message stamps the position (rule 1.3)", 0, applier.engine().getLogPosition());
    }
}

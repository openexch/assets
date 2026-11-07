// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import com.openexchange.assets.infrastructure.generated.SubscribeEncoder;
import com.openexchange.assets.infrastructure.metrics.AssetsNodeMetrics;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ApplierArm;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ServiceArm;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.LogStep;
import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RCS S1 (contract 6.2): a well-formed Subscribe is a transport concern the applier refuses to see. It returns false
 * and changes nothing: not the snapshot image, not the engine's log position, not the event stream, not a counter,
 * not the session table. A malformed Subscribe is an engine frame like any other: it stamps the position and the
 * demuxer drops and counts it. The classification is exactly the service's historical four conditions. The service
 * arm characterizes the live path (unchanged image, no metrics sample) and the one known edge: a LEADER handed a
 * Subscribe with no session dereferences it (the container never does that; S1 keeps the body byte-identical).
 */
public class ApplierSubscribeNoStateTest {

    private static final long SENTINEL = 12_345L;
    private static final long REGISTERED = SessionedLog.sessionOf(SessionClass.GATEWAY);
    private static final long UNKNOWN = 4_242L;
    private static final int HEADER = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int BLOCK = SubscribeDecoder.BLOCK_LENGTH;

    /** A Subscribe at {@code offset} in a larger buffer, with the header fields overridden and the frame cut to {@code length}. */
    private static UnsafeBuffer subscribe(final int offset, final int templateId, final int schemaId, final int version,
                                          final int blockLength) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[offset + 64]);
        final SubscribeEncoder e = new SubscribeEncoder().wrapAndApplyHeader(buffer, offset, new MessageHeaderEncoder());
        e.channels().clear().acks(true).balances(true);
        buffer.putShort(offset, (short) blockLength, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(offset + 2, (short) templateId, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(offset + 4, (short) schemaId, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(offset + 6, (short) version, java.nio.ByteOrder.LITTLE_ENDIAN);
        return buffer;
    }

    private static UnsafeBuffer wellFormed() {
        return subscribe(0, SubscribeDecoder.TEMPLATE_ID, SubscribeDecoder.SCHEMA_ID, MessageHeaderDecoder.SCHEMA_VERSION, BLOCK);
    }

    /** The service's classification at a9a865d (ACS:1187-1201), copied verbatim as the reference predicate. */
    private static boolean reference(final DirectBuffer buffer, final int offset, final int length) {
        final MessageHeaderDecoder peekHeaderDecoder = new MessageHeaderDecoder();
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return false;
        }
        peekHeaderDecoder.wrap(buffer, offset);
        if (peekHeaderDecoder.templateId() != SubscribeDecoder.TEMPLATE_ID
                || peekHeaderDecoder.schemaId() != SubscribeDecoder.SCHEMA_ID) {
            return false;
        }
        if (peekHeaderDecoder.version() != MessageHeaderDecoder.SCHEMA_VERSION
                || length < MessageHeaderDecoder.ENCODED_LENGTH + SubscribeDecoder.BLOCK_LENGTH
                || peekHeaderDecoder.blockLength() < SubscribeDecoder.BLOCK_LENGTH) {
            return false;
        }
        return true;
    }

    /** An applier with the prefix sessions open and registered and one deposit applied (a non-trivial image). */
    private static ApplierArm seeded() {
        final ApplierArm arm = new ApplierArm();
        for (final LogStep s : setupLog()) {
            arm.apply(s);
        }
        arm.engine().setLogPosition(SENTINEL);
        return arm;
    }

    private static List<LogStep> setupLog() {
        final List<LogStep> prefix = new java.util.ArrayList<>(SessionedLog.from(List.of()));
        prefix.add(SessionedLog.message(SessionedLog.sessionOf(SessionClass.ADMIN), CoreFrameStory.T0,
                new CoreFrameEncoders().deposit(1, 3, 0, 50 * CoreFrameStory.ONE, 0xD0L, 1)));
        return SessionedLog.renumber(prefix);
    }

    private record State(byte[] image, long logPosition, long calls, String counters, String sessions) { }

    private static State state(final ApplierArm a) {
        return new State(a.image(), a.engine().getLogPosition(), a.sink.calls, Arrays.toString(ApplierParityRig.counters(a)),
                a.engine().sessions().sessionIds().toString());
    }

    @Test
    public void aWellFormedSubscribeChangesNothingWhoeverSendsIt() {
        for (final long session : new long[] {REGISTERED, UNKNOWN, -1L}) {
            final ApplierArm arm = seeded();
            final State before = state(arm);
            assertTrue("fixture: the session table is not empty", before.sessions().contains(String.valueOf(REGISTERED)));
            final UnsafeBuffer frame = wellFormed();
            assertFalse("session " + session, arm.applier.onSessionMessage(session, 99_999_968L, CoreFrameStory.T0 + 5,
                    frame, 0, HEADER + BLOCK));
            final State after = state(arm);
            assertArrayEquals("image, session " + session, before.image(), after.image());
            assertEquals("logPosition, session " + session, SENTINEL, after.logPosition());
            assertEquals("sink calls, session " + session, before.calls(), after.calls());
            assertEquals("counters, session " + session, before.counters(), after.counters());
            assertEquals("sessions, session " + session, before.sessions(), after.sessions());
        }
    }

    @Test
    public void aMalformedSubscribeIsAnEngineFrameThatIsStampedAndDropped() {
        final int v = MessageHeaderDecoder.SCHEMA_VERSION;
        final int t = SubscribeDecoder.TEMPLATE_ID;
        final int sc = SubscribeDecoder.SCHEMA_ID;
        // {schemaRejects delta, decodeRejects delta}, frame, length
        final Object[][] cases = {
                {"version != SCHEMA_VERSION", 1, 0, subscribe(0, t, sc, v - 1, BLOCK), HEADER + BLOCK},
                {"foreign schemaId", 1, 0, subscribe(0, t, sc + 1, v, BLOCK), HEADER + BLOCK},
                {"blockLength < BLOCK_LENGTH", 0, 1, subscribe(0, t, sc, v, BLOCK - 1), HEADER + BLOCK},
                {"frame shorter than header + BLOCK_LENGTH", 0, 1, subscribe(0, t, sc, v, BLOCK), HEADER + BLOCK - 1},
        };
        long position = 100_000_032L;
        for (final Object[] c : cases) {
            final ApplierArm arm = seeded();
            final State before = state(arm);
            final long schema = arm.demuxer().schemaRejectCount();
            final long decode = arm.demuxer().decodeRejectCount();
            position += 32;
            assertTrue((String) c[0], arm.applier.onSessionMessage(REGISTERED, position, CoreFrameStory.T0 + 5,
                    (UnsafeBuffer) c[3], 0, (Integer) c[4]));
            assertEquals(c[0] + ": the position IS stamped", position, arm.engine().getLogPosition());
            assertEquals(c[0] + ": schema rejects", schema + (Integer) c[1], arm.demuxer().schemaRejectCount());
            assertEquals(c[0] + ": decode rejects", decode + (Integer) c[2], arm.demuxer().decodeRejectCount());
            assertArrayEquals(c[0] + ": image", before.image(), arm.image());
            assertEquals(c[0] + ": no event", before.calls(), arm.sink.calls);
            assertEquals(c[0] + ": sessions", before.sessions(), arm.engine().sessions().sessionIds().toString());
        }
    }

    @Test
    public void theClassificationIsExactlyTheServicesFourConditions() {
        final AssetsLogApplier applier = new AssetsLogApplier(() -> { });
        int positives = 0;
        for (int mask = 0; mask < 16; mask++) {
            final boolean fullHeader = (mask & 1) != 0;
            final boolean identity = (mask & 2) != 0;
            final boolean version = (mask & 4) != 0;
            final boolean block = (mask & 8) != 0;
            final int offset = 16;
            final UnsafeBuffer frame = subscribe(offset, identity ? SubscribeDecoder.TEMPLATE_ID : DepositDecoder.TEMPLATE_ID,
                    SubscribeDecoder.SCHEMA_ID, version ? MessageHeaderDecoder.SCHEMA_VERSION : MessageHeaderDecoder.SCHEMA_VERSION + 1,
                    block ? BLOCK : BLOCK - 1);
            final int length = fullHeader ? HEADER + BLOCK : HEADER - 1;
            final boolean expected = reference(frame, offset, length);
            assertEquals("combination " + mask, expected, applier.isTransportSubscribe(frame, offset, length));
            assertEquals("combination " + mask + ": only all four make a Subscribe", mask == 15, expected);
            positives += expected ? 1 : 0;
        }
        assertEquals(1, positives);
        // The two halves of the identity and of the size condition, each on its own.
        final UnsafeBuffer foreignSchema = subscribe(0, SubscribeDecoder.TEMPLATE_ID, SubscribeDecoder.SCHEMA_ID + 1,
                MessageHeaderDecoder.SCHEMA_VERSION, BLOCK);
        assertEquals(reference(foreignSchema, 0, HEADER + BLOCK), applier.isTransportSubscribe(foreignSchema, 0, HEADER + BLOCK));
        assertFalse(applier.isTransportSubscribe(foreignSchema, 0, HEADER + BLOCK));
        final UnsafeBuffer ok = wellFormed();
        assertEquals(reference(ok, 0, HEADER + BLOCK - 1), applier.isTransportSubscribe(ok, 0, HEADER + BLOCK - 1));
        assertFalse(applier.isTransportSubscribe(ok, 0, HEADER + BLOCK - 1));
        assertTrue(applier.isTransportSubscribe(ok, 0, HEADER + BLOCK + 8));
    }

    @Test
    public void aClashingOpenIsRefusedAndLatchesTheApplier() {
        final ApplierArm arm = seeded();
        final byte[] principal = SessionedLog.principalOf(SessionClass.GATEWAY);
        final IllegalStateException clash = assertThrows(IllegalStateException.class,
                () -> arm.applier.onSessionOpen(REGISTERED, 4_096, CoreFrameStory.T0, principal));
        assertTrue(clash.getMessage(), clash.getMessage().contains("clashing open clusterSessionId=" + REGISTERED));
        assertTrue("a refusal latches the instance", arm.applier.failed());
        assertThrows(IllegalStateException.class, () -> arm.applier.onSessionMessage(REGISTERED, 4_160, CoreFrameStory.T0,
                wellFormed(), 0, HEADER + BLOCK));
    }

    // ---------------------------------------------------------------- the service arm (characterization)

    private static int sampleTick(final ServiceArm arm) {
        final Object metrics = ApplierParityRig.read(arm.service, AssetsClusteredService.class, "nodeMetrics");
        return (Integer) ApplierParityRig.read(metrics, AssetsNodeMetrics.class, "sampleTick");
    }

    private static ServiceArm seededService(final Cluster.Role role) {
        final ServiceArm arm = new ServiceArm(role);
        for (final LogStep s : setupLog()) {
            arm.apply(s);
        }
        return arm;
    }

    @Test
    public void theServiceTakesTheSameFramesWithoutStateOrSample() {
        final long position = setupLog().get(setupLog().size() - 1).logPosition() + 64;
        for (final Cluster.Role role : new Cluster.Role[] {Cluster.Role.FOLLOWER, Cluster.Role.LEADER}) {
            // FIX-5: a session-less Subscribe only on the FOLLOWER (on a LEADER it is the characterized edge below).
            final long[] sessions = role == Cluster.Role.FOLLOWER ? new long[] {REGISTERED, UNKNOWN, -1L} : new long[] {REGISTERED};
            for (final long session : sessions) {
                final ServiceArm arm = seededService(role);
                final byte[] image = arm.image();
                final int ticks = sampleTick(arm);
                final long calls = arm.sink.calls;
                final byte[] bytes = new byte[HEADER + BLOCK];
                wellFormed().getBytes(0, bytes);
                arm.apply(new LogStep(SessionedLog.Kind.MESSAGE, session, CoreFrameStory.T0 + 5, bytes, position, 0L));
                assertArrayEquals(role + " session " + session, image, arm.image());
                assertEquals(role + " session " + session + ": no metrics sample for a Subscribe", ticks, sampleTick(arm));
                assertEquals(role + " session " + session, calls, arm.sink.calls);
                assertEquals(0, arm.failFastCalls[0]);
            }
        }
    }

    @Test
    public void aLeaderHandedASessionlessSubscribeDereferencesItAsAtTheBase() {
        final ServiceArm arm = seededService(Cluster.Role.LEADER);
        final byte[] image = arm.image();
        final byte[] bytes = new byte[HEADER + BLOCK];
        wellFormed().getBytes(0, bytes);
        final long position = setupLog().get(setupLog().size() - 1).logPosition() + 64;
        assertThrows(NullPointerException.class, () -> arm.apply(
                new LogStep(SessionedLog.Kind.MESSAGE, -1L, CoreFrameStory.T0 + 5, bytes, position, 0L)));
        assertEquals("the apply fence stops the node once", 1, arm.failFastCalls[0]);
        assertArrayEquals("no state was touched", image, arm.image());
        assertFalse("the applier never saw it", arm.service.applier().failed());
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.SettleBatchEncoder;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.Arm;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.Probe;
import com.openexchange.assets.infrastructure.persistence.ApplierParityRig.ServiceArm;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.Kind;
import com.openexchange.assets.infrastructure.persistence.SessionedLog.LogStep;
import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.ClusterLogReader;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SemanticVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RCS S2 (contract 4.5, RCS-A06 reader half for the AE): the kit's {@link ClusterLogReader} and
 * {@link ReplayContainer} in front of the {@link AssetsLogApplier} apply a real recorded cluster log exactly as the
 * live service does.
 *
 * <p>The log is the RCS S1 sessioned story ({@code SessionedLog.from(CoreFrameStory.build(0x5EED_0001L, 2_000))}) plus
 * one SettleBatch longer than the MTU (fragmented on the log) and four appended steps: a close of
 * {@code UNREGISTERED_BRIDGE}, a second close of the same id (unknown to the container), a close of an id never opened,
 * and a message from the closed id. It is written with the kit's {@link ClusterLogWriter} to a 64 KiB-term recording
 * (the term rotates many times, with padding) behind one NewLeadershipTermEvent, and the writer's returned frame-end
 * positions are kept.</p>
 * <ol>
 *   <li>Step 1 (not circular): the reader's trace (kind, position, id, timestamp, bytes) equals what was written, and
 *       its positions are the writer's own offer results.</li>
 *   <li>Step 2: arm R = reader -> container -> applier; arm A = the live service as FOLLOWER behind the S1 container
 *       emulation ({@link ServiceArm}), fed the same steps re-positioned at the writer's positions. Arm A skips the two
 *       unknown closes as ClusteredServiceAgent does; arm R forwards every close (no session map, RF-1). Compared per
 *       entry: the {@link StampSink} stream, the engine's log position and six demuxer counter deltas
 *       ({@link Probe}); every 100 entries and at the end: the image bytes and the session table.</li>
 * </ol>
 * Negative controls (each must make step 2 mismatch at the stated entry, and nowhere earlier): the 5th OPEN (the
 * story's first, {@code GATEWAY_SESSION}) dropped by a listener decorator mismatches at that session's Register frame;
 * every session message handed to the container 32 bytes past its frame end mismatches at the first message; the
 * fragmented SettleBatch handed over one fragment short mismatches at the SettleBatch.
 */
public class AssetsReplayParityTest {

    static final long SEED = 0x5EED_0001L;
    static final int FRAMES = 2_000;
    static final int TERM_LENGTH = 64 * 1024;
    static final int MTU = 1408;
    static final int APP_VERSION = SemanticVersion.compose(0, 0, 1);
    /** An id no story, prefix or tail step opens. */
    static final long NEVER_OPENED = 4_242_424L;
    static final int SETTLE_BATCH_ENTRIES = 40;
    /** The 5th OPEN of the log is the story's first (GATEWAY_SESSION): the SessionedLog prefix opens four class homes. */
    static final int GATEWAY_OPEN_ORDINAL = 5;

    private static RecordedLogFixture fixture;
    private static Written written;

    /** The cast helper (contract 4.5): the files compile before AssetsLogApplier implements the seam. */
    static ShadowEngine shadowEngine(final Object applier) {
        if (!(applier instanceof ShadowEngine)) {
            throw new AssertionError("AssetsLogApplier does not implement com.openexchange.cluster.log.ShadowEngine"
                    + " (RCS S2 W8)");
        }
        return (ShadowEngine) applier;
    }

    // ------------------------------------------------------------------------------------------------- the log

    /** The 4.5 log: the sessioned story, one SettleBatch above the MTU, then the four appended steps. */
    static List<LogStep> aeSteps(final long seed, final int frames) {
        final List<LogStep> out = new ArrayList<>(SessionedLog.from(CoreFrameStory.build(seed, frames)));
        final long ts = out.get(out.size() - 1).timestamp() + 1;
        out.add(SessionedLog.message(SessionedLog.sessionOf(SessionClass.BRIDGE), ts, settleBatch(SETTLE_BATCH_ENTRIES)));
        out.add(SessionedLog.close(CoreFrameStory.UNREGISTERED_BRIDGE, ts + 1));
        out.add(SessionedLog.close(CoreFrameStory.UNREGISTERED_BRIDGE, ts + 2));
        out.add(SessionedLog.close(NEVER_OPENED, ts + 3));
        out.add(SessionedLog.message(CoreFrameStory.UNREGISTERED_BRIDGE, ts + 4,
                new CoreFrameEncoders().terminal(9_999_001L, 10_001, 1, 1, 3, ts + 4, 1)));
        return out;
    }

    /** A v5 SettleBatch of fresh trades (no hold: every entry settles and emits), longer than one MTU. */
    static byte[] settleBatch(final int entries) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(8192);
        final SettleBatchEncoder e = new SettleBatchEncoder().wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder());
        final SettleBatchEncoder.SettlesEncoder g = e.settlesCount(entries);
        for (int i = 0; i < entries; i++) {
            g.next().tradeId(1_000_000L + i).marketId(1).takerOrderId(8_000_000L + i).takerUserId(1 + (i % 3))
                    .makerOrderId(8_100_000L + i).makerUserId(4 + (i % 3)).price(100L * CoreFrameStory.ONE)
                    .quantity(CoreFrameStory.ONE / 1000 * (1 + i)).takerIsBuy(i % 2 == 0 ? BoolFlag.TRUE : BoolFlag.FALSE)
                    .journalPosition(10_000_000L + i);
        }
        final byte[] bytes = new byte[MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength()];
        buf.getBytes(0, bytes);
        return bytes;
    }

    /** A recording of one log: the steps, the writer's frame end for each, the term event's end, the stop position. */
    record Written(List<LogStep> steps, long[] positions, long termEventEnd, long recordingId, long stopPosition) {
        /** The steps re-positioned at the writer's frame ends ({@code LogStep.at}). */
        List<LogStep> positioned() {
            final List<LogStep> out = new ArrayList<>(steps.size());
            for (int i = 0; i < steps.size(); i++) {
                out.add(steps.get(i).at(positions[i]));
            }
            return out;
        }
    }

    static Written write(final RecordedLogFixture fixture, final List<LogStep> steps) {
        final RecordedLogFixture.Recording rec = fixture.record(TERM_LENGTH, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final long t0 = steps.get(0).timestamp();
        final long termEnd = w.newLeadershipTerm(0, t0, 0, 0, rec.publication().sessionId(), TimeUnit.MILLISECONDS,
                APP_VERSION);
        final long[] positions = new long[steps.size()];
        for (int i = 0; i < steps.size(); i++) {
            final LogStep s = steps.get(i);
            positions[i] = switch (s.kind()) {
                case OPEN -> w.sessionOpen(s.sessionId(), 7_000_000L + i, s.timestamp(), 102,
                        "aeron:udp?endpoint=localhost:20" + (s.sessionId() % 100), s.bytes());
                case CLOSE -> w.sessionClose(s.sessionId(), s.timestamp(), CloseReason.CLIENT_ACTION);
                case MESSAGE -> w.message(s.sessionId(), s.timestamp(), s.bytes());
                case TIMER -> w.timer(s.correlationId(), s.timestamp());
            };
        }
        return new Written(List.copyOf(steps), positions, termEnd, rec.recordingId(), rec.stopRecording());
    }

    /** Polls a fresh reader over a replay of the whole recording into {@code listener}. */
    static ClusterLogReader readAll(final RecordedLogFixture fixture, final Written log, final ClusterLogListener listener) {
        final Image image = fixture.replay(log.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final ClusterLogReader reader = new ClusterLogReader(listener, 16, 0);
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (reader.consumedPosition() < log.stopPosition()) {
            if (reader.poll(image, log.stopPosition()) == 0) {
                if (image.isEndOfStream() || image.isClosed()) {
                    break;
                }
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("reader stuck at " + reader.consumedPosition() + " of " + log.stopPosition());
                }
                Thread.yield();
            }
        }
        return reader;
    }

    static FrameKind frameKind(final Kind kind) {
        return switch (kind) {
            case OPEN -> FrameKind.SESSION_OPEN;
            case CLOSE -> FrameKind.SESSION_CLOSE;
            case MESSAGE -> FrameKind.SESSION_MESSAGE;
            case TIMER -> FrameKind.TIMER_EVENT;
        };
    }

    /** Step index of the {@link #GATEWAY_OPEN_ORDINAL}th OPEN. */
    static int gatewayOpenIndex(final List<LogStep> steps) {
        int opens = 0;
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i).kind() == Kind.OPEN && ++opens == GATEWAY_OPEN_ORDINAL) {
                return i;
            }
        }
        throw new IllegalStateException("fewer than " + GATEWAY_OPEN_ORDINAL + " opens");
    }

    static boolean checkpoint(final int i, final int size) {
        return i % 100 == 0 || i == size - 1;
    }

    @BeforeClass
    public static void writeTheLog() {
        fixture = new RecordedLogFixture();
        written = write(fixture, aeSteps(SEED, FRAMES));
    }

    @AfterClass
    public static void closeFixture() {
        if (fixture != null) {
            fixture.close();
        }
    }

    // --------------------------------------------------------------------------------------------- arm R

    /** How arm R's decorator between reader and container changes the stream (NONE in production). */
    enum Variant { NONE, DROP_GATEWAY_OPEN, SHIFT_MESSAGE_POSITION, TRUNCATE_FRAGMENTED_MESSAGE }

    /** What the reader delivered for one entry, and what the applier did with it. */
    record Entry(FrameKind kind, long position, long id, long timestamp, byte[] data, String probe, int events,
                 byte[] image, String sessions) {
    }

    /** reader -> [decorator] -> ReplayContainer -> AssetsLogApplier, recording one {@link Entry} per log step. */
    static final class ArmR implements ClusterLogListener {
        final StampSink sink = new StampSink();
        final int[] failFastCalls = new int[1];
        final AssetsLogApplier applier = new AssetsLogApplier(() -> failFastCalls[0]++);
        final ReplayContainer container;
        final List<Entry> entries = new ArrayList<>();
        final ExpandableArrayBuffer imageBuffer = new ExpandableArrayBuffer(64 * 1024);
        final Variant variant;
        final long dropPosition;
        final int total;
        final Probe probe;
        int termEvents;
        int actions;

        ArmR(final Variant variant, final Written log) {
            this.variant = variant;
            this.dropPosition = log.positions()[gatewayOpenIndex(log.steps())];
            this.total = log.steps().size();
            applier.setEventSink(sink);
            container = new ReplayContainer(shadowEngine(applier), APP_VERSION,
                    AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR, ShadowTriggerListener.NONE);
            container.genesis();
            probe = new Probe(new Arm() {
                public void apply(final LogStep step) {
                    throw new UnsupportedOperationException("arm R is driven by the reader");
                }

                public AssetsEngine engine() {
                    return applier.engine();
                }

                public AssetsSbeDemuxer demuxer() {
                    return applier.demuxer();
                }

                public StampSink sink() {
                    return sink;
                }

                public byte[] image() {
                    return imageNow();
                }
            });
        }

        byte[] imageNow() {
            return ApplierParityRig.copy(imageBuffer, container.serialize(imageBuffer));
        }

        private void record(final FrameKind kind, final long position, final long id, final long timestamp,
                            final byte[] data, final int eventsBefore) {
            final int i = entries.size();
            final String step = probe.step();
            final int events = sink.events().size() - eventsBefore;
            final boolean check = checkpoint(i, total);
            entries.add(new Entry(kind, position, id, timestamp, data, step, events, check ? imageNow() : null,
                    check ? applier.engine().sessions().sessionIds().toString() : null));
        }

        @Override
        public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                     final DirectBuffer buffer, final int offset, final int length) {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            final int before = sink.events().size();
            final long forwarded = variant == Variant.SHIFT_MESSAGE_POSITION ? logPosition + 32 : logPosition;
            // A reader that lost the last fragment of a reassembled message would hand over a shorter payload.
            final int forwardedLength = variant == Variant.TRUNCATE_FRAGMENTED_MESSAGE && length > MTU
                    ? length - (MTU - 64) : length;
            container.onSessionMessage(forwarded, clusterSessionId, timestamp, buffer, offset, forwardedLength);
            record(FrameKind.SESSION_MESSAGE, logPosition, clusterSessionId, timestamp, data, before);
        }

        @Override
        public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
            final int before = sink.events().size();
            container.onTimerEvent(logPosition, correlationId, timestamp);
            record(FrameKind.TIMER_EVENT, logPosition, correlationId, timestamp, null, before);
        }

        @Override
        public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                  final long timestamp, final int responseStreamId, final String responseChannel,
                                  final byte[] encodedPrincipal) {
            final int before = sink.events().size();
            if (!(variant == Variant.DROP_GATEWAY_OPEN && logPosition == dropPosition)) {
                container.onSessionOpen(leadershipTermId, logPosition, clusterSessionId, timestamp, responseStreamId,
                        responseChannel, encodedPrincipal);
            }
            record(FrameKind.SESSION_OPEN, logPosition, clusterSessionId, timestamp, encodedPrincipal.clone(), before);
        }

        @Override
        public void onSessionClose(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                   final long timestamp, final CloseReason closeReason) {
            final int before = sink.events().size();
            container.onSessionClose(leadershipTermId, logPosition, clusterSessionId, timestamp, closeReason);
            record(FrameKind.SESSION_CLOSE, logPosition, clusterSessionId, timestamp, null, before);
        }

        @Override
        public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                       final ClusterAction action, final int flags) {
            actions++;
            return container.onClusterAction(leadershipTermId, logPosition, timestamp, action, flags);
        }

        @Override
        public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                             final long termBaseLogPosition, final int leaderMemberId,
                                             final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
            termEvents++;
            container.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, termBaseLogPosition,
                    leaderMemberId, logSessionId, timeUnit, appVersion);
            // The term event is not an entry of arm A: absorb what it did into the probe mark (nothing, per R4).
            final String absorbed = probe.step();
            if (!absorbed.equals("P=0 C=0,0,0,0,0,0,")) {
                throw new AssertionError("the term event changed the applier: " + absorbed);
            }
        }
    }

    /** First step where arm A and arm R differ, or null. */
    record Mismatch(int index, String what, String expected, String actual) {
        @Override
        public String toString() {
            return "step " + index + " " + what + ": arm A <" + expected + "> arm R <" + actual + ">";
        }
    }

    /** Arm A (the live service as FOLLOWER) over the re-positioned steps, against arm R's entries. */
    static Mismatch compare(final Written log, final ArmR r) {
        final List<LogStep> steps = log.positioned();
        final ServiceArm a = new ServiceArm(Cluster.Role.FOLLOWER);
        final Probe pa = new Probe(a);
        for (int i = 0; i < steps.size(); i++) {
            a.apply(steps.get(i));
            final String expected = pa.step();
            final Entry e = r.entries.get(i);
            if (!expected.equals(e.probe())) {
                return new Mismatch(i, "entry (events, stamps, P, counters)", expected, e.probe());
            }
            if (checkpoint(i, steps.size())) {
                final byte[] image = a.image();
                if (!Arrays.equals(image, e.image())) {
                    return new Mismatch(i, "image", image.length + " bytes", e.image().length + " bytes");
                }
                final String sessions = a.engine().sessions().sessionIds().toString();
                if (!sessions.equals(e.sessions())) {
                    return new Mismatch(i, "session table", sessions, e.sessions());
                }
            }
        }
        if (a.failFastCalls[0] != 0) {
            return new Mismatch(steps.size(), "arm A fail-fast calls", "0", String.valueOf(a.failFastCalls[0]));
        }
        return null;
    }

    static ArmR runArmR(final Variant variant) {
        final ArmR r = new ArmR(variant, written);
        final ClusterLogReader reader = readAll(fixture, written, r);
        assertFalse(variant + ": reader stopped " + reader.stop(), reader.stopped());
        assertEquals(variant + ": reader consumed the whole recording", written.stopPosition(), reader.consumedPosition());
        assertEquals(variant + ": one entry per written step", written.steps().size(), r.entries.size());
        return r;
    }

    // --------------------------------------------------------------------------------------------------- tests

    @Test
    public void theReaderAndContainerApplyTheRecordedLogAsTheLiveServiceDoes() {
        final List<LogStep> steps = written.steps();
        final long[] positions = written.positions();
        final ArmR r = new ArmR(Variant.NONE, written);
        final ClusterLogReader reader = readAll(fixture, written, r);

        // Reader end state: the whole recording, no stop, no partial, the frames counted by kind.
        assertFalse("reader stopped: " + reader.stop(), reader.stopped());
        assertEquals(written.stopPosition(), reader.consumedPosition());
        assertEquals(positions[positions.length - 1], reader.position());
        assertFalse(reader.midMessage());
        assertEquals(0, reader.abandonedPartialCount());
        assertEquals(0, reader.unknownTemplateCount());
        assertEquals(1, r.termEvents);
        assertEquals(0, r.actions);

        // Step 1 (not circular): the reader's trace is what was written, at the writer's own frame-end positions.
        assertEquals("one reader entry per written step", steps.size(), r.entries.size());
        long previous = written.termEventEnd();
        for (int i = 0; i < steps.size(); i++) {
            final LogStep s = steps.get(i);
            final Entry e = r.entries.get(i);
            final String at = "step " + i + " " + s.kind();
            assertEquals(at, frameKind(s.kind()), e.kind());
            assertEquals(at + ": reader position = the writer's offer result", positions[i], e.position());
            assertTrue(at + ": positions strictly increase", e.position() > previous);
            previous = e.position();
            assertEquals(at, s.timestamp(), e.timestamp());
            switch (s.kind()) {
                case OPEN -> {
                    assertEquals(at, s.sessionId(), e.id());
                    assertArrayEquals(at + ": principal", s.bytes(), e.data());
                }
                case MESSAGE -> {
                    assertEquals(at, s.sessionId(), e.id());
                    assertArrayEquals(at + ": payload (session header stripped)", s.bytes(), e.data());
                }
                case CLOSE -> assertEquals(at, s.sessionId(), e.id());
                case TIMER -> assertEquals(at, s.correlationId(), e.id());
            }
        }

        // Step 2: arm A (live service, FOLLOWER) vs arm R entry by entry.
        final Mismatch mismatch = compare(written, r);
        assertNull("arm A vs arm R: " + mismatch, mismatch);

        // Container: every close forwarded (arm A skipped two of the three), nothing failed, nothing ignored.
        assertEquals(3, r.container.count(FrameKind.SESSION_CLOSE));
        assertEquals(steps.stream().filter(s -> s.kind() == Kind.OPEN).count(), r.container.count(FrameKind.SESSION_OPEN));
        assertEquals(1, r.container.count(FrameKind.NEW_LEADERSHIP_TERM));
        assertEquals(positions[positions.length - 1], r.container.logPosition());
        assertEquals(steps.get(steps.size() - 1).timestamp(), r.container.clusterTime());
        assertEquals(0, r.container.leadershipTermId());
        assertFalse(r.container.failed());
        assertFalse(r.applier.failed());
        assertEquals(0, r.failFastCalls[0]);

        // Non-vacuity: a long log over many terms, a rich event stream, stamped positions, a fragmented message that
        // did its work, the tail closes.
        assertTrue("terms rotated: stop " + written.stopPosition(), written.stopPosition() > 4L * TERM_LENGTH);
        assertTrue("log length " + steps.size(), steps.size() > FRAMES);
        assertTrue("events " + r.sink.events().size(), r.sink.events().size() > FRAMES);
        assertTrue("stamped calls " + r.sink.stamps.size(), r.sink.stamps.size() > 100);
        final int batch = steps.size() - 5;
        assertEquals(Kind.MESSAGE, steps.get(batch).kind());
        assertTrue("the SettleBatch spans fragments: " + (positions[batch] - positions[batch - 1]) + " bytes",
                positions[batch] - positions[batch - 1] > MTU);
        assertTrue("every SettleBatch entry emitted (events " + r.entries.get(batch).events() + ")",
                r.entries.get(batch).events() >= SETTLE_BATCH_ENTRIES);
        assertEquals(1_000_000L + SETTLE_BATCH_ENTRIES - 1, r.applier.engine().getLastAppliedTradeId());
        assertFalse("the closed session left the table",
                r.applier.engine().sessions().sessionIds().contains(CoreFrameStory.UNREGISTERED_BRIDGE));
        System.out.println("GREEN parity: steps=" + steps.size() + " stop=" + written.stopPosition() + " events="
                + r.sink.events().size() + " stamps=" + r.sink.stamps.size() + " settleBatchEvents="
                + r.entries.get(batch).events() + " settleBatchFrame=" + (positions[batch] - positions[batch - 1]));
    }

    @Test
    public void negativeControlDroppedGatewayOpenMismatchesAtItsRegisterFrame() {
        final List<LogStep> steps = written.steps();
        final int openIndex = gatewayOpenIndex(steps);
        final LogStep open = steps.get(openIndex);
        assertEquals("the 5th OPEN is the story's first, GATEWAY_SESSION", CoreFrameStory.GATEWAY_SESSION, open.sessionId());
        for (int i = 0; i < 4; i++) {
            assertEquals("the four class-home opens come first", Kind.OPEN, steps.get(i).kind());
            assertTrue(steps.get(i).sessionId() >= 900);
        }
        int register = -1;
        for (int i = openIndex + 1; i < steps.size() && register < 0; i++) {
            if (steps.get(i).kind() == Kind.MESSAGE && steps.get(i).sessionId() == CoreFrameStory.GATEWAY_SESSION) {
                register = i;
            }
        }
        final byte[] frame = steps.get(register).bytes();
        assertEquals("the session's first frame is its RegisterSession", RegisterSessionDecoder.TEMPLATE_ID,
                (frame[2] & 0xFF) | (frame[3] & 0xFF) << 8);

        final Mismatch mismatch = compare(written, runArmR(Variant.DROP_GATEWAY_OPEN));
        System.out.println("NC DROP_GATEWAY_OPEN -> " + mismatch);
        if (mismatch == null) {
            throw new AssertionError("comparator blind: arm R without the GATEWAY_SESSION open compared equal");
        }
        // RCS S4 RF-3: a SessionOpen stamps its frame end, so arm R differs in P at the dropped OPEN itself, before the
        // Register frame that showed it before RF-3
        assertTrue("the Register frame follows the open", register > openIndex);
        assertEquals("first mismatch at GATEWAY_SESSION's dropped OPEN (" + mismatch + ")", openIndex, mismatch.index());
    }

    @Test
    public void negativeControlShiftedMessagePositionMismatchesAtTheFirstMessage() {
        final List<LogStep> steps = written.steps();
        int firstMessage = -1;
        for (int i = 0; i < steps.size() && firstMessage < 0; i++) {
            if (steps.get(i).kind() == Kind.MESSAGE) {
                firstMessage = i;
            }
        }
        final Mismatch mismatch = compare(written, runArmR(Variant.SHIFT_MESSAGE_POSITION));
        System.out.println("NC SHIFT_MESSAGE_POSITION -> " + mismatch);
        if (mismatch == null) {
            throw new AssertionError("comparator blind: message positions 32 bytes past the frame end compared equal");
        }
        assertEquals("first mismatch at the first stamped message (" + mismatch + ")", firstMessage, mismatch.index());
        assertTrue("the divergence is the stamped position: " + mismatch, mismatch.expected().contains("P="));
    }

    @Test
    public void negativeControlTruncatedFragmentedMessageMismatchesAtTheSettleBatch() {
        final int batch = written.steps().size() - 5;
        final Mismatch mismatch = compare(written, runArmR(Variant.TRUNCATE_FRAGMENTED_MESSAGE));
        System.out.println("NC TRUNCATE_FRAGMENTED_MESSAGE -> " + mismatch);
        if (mismatch == null) {
            throw new AssertionError("comparator blind: a SettleBatch one fragment short compared equal");
        }
        assertEquals("first mismatch at the SettleBatch (" + mismatch + ")", batch, mismatch.index());
    }
}

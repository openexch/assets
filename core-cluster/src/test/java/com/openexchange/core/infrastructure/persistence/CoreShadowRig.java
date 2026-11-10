// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.persistence.SessionedLog.LogStep;
import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentity;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowProducer;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Core rig: a core cluster log written with the kit's {@link ClusterLogWriter} (64 KiB terms, MTU 1408, one
 * {@code ClusterAction(SNAPSHOT, 0)} inserted), captured through the kit reader, applied by
 * {@link ReplayContainer} + {@link CoreLogApplier}, shadowed through the kit writer into a real archive.
 */
final class CoreShadowRig {

    static final int TERM = 64 * 1024;
    static final int MTU = 1408;
    static final long GENESIS = CoreFrameStory.GENESIS;
    static final int ENGINE = ShadowHeader.ENGINE_CORE;
    static final ShadowProducer REPLICA = new ShadowProducer(0, ProducerKind.REPLICA, 3);
    static final long TIMEOUT_NS = 30_000_000_000L;
    static final String SHADOW_TERM = "1m";

    /** The cast helper (contract 11 preamble): the files compile before CoreShadowIdentity exists. */
    static ShadowIdentitySource identityOf(final CoreLogApplier applier) {
        try {
            final Class<?> type = Class.forName("com.openexchange.core.infrastructure.persistence.CoreShadowIdentity");
            final Constructor<?> c = type.getConstructor(com.openexchange.core.application.engine.CoreEngine.class);
            return (ShadowIdentitySource)c.newInstance(applier.engine());
        } catch (final ClassNotFoundException e) {
            throw new AssertionError("CoreShadowIdentity is absent");
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError("CoreShadowIdentity cannot be built: " + e);
        }
    }

    static ShadowEngine engine(final CoreLogApplier applier) {
        return CoreReplayParityTest.shadowEngine(applier);
    }

    /** One written log: its captured frames (through the kit reader), the action's frame end, the term base. */
    record Log(RecordedLog frames, long actionPosition, long termId, long termBase, long recordingId, long stop) {
        /** {term -> termBase} as the reader observed it from the term events. */
        Map<Long, Long> terms() {
            final Map<Long, Long> out = new HashMap<>();
            frames.replay(-1, Long.MAX_VALUE, new Terms(out));
            return out;
        }

        /** Frame ends in order. */
        long[] positions() {
            return frames.positions();
        }
    }

    private static final class Terms implements ClusterLogListener {
        final Map<Long, Long> out;

        Terms(final Map<Long, Long> out) {
            this.out = out;
        }

        public void onSessionMessage(final long p, final long id, final long t, final DirectBuffer b, final int o,
                                     final int l) {
        }

        public void onTimerEvent(final long p, final long c, final long t) {
        }

        public void onSessionOpen(final long term, final long p, final long id, final long t, final int s,
                                  final String ch, final byte[] pr) {
        }

        public void onSessionClose(final long term, final long p, final long id, final long t, final CloseReason r) {
        }

        public boolean onClusterAction(final long term, final long p, final long t, final ClusterAction a,
                                       final int f) {
            return false;
        }

        public void onNewLeadershipTermEvent(final long term, final long p, final long t, final long base,
                                             final int leader, final int session, final TimeUnit u, final int app) {
            out.put(term, base);
        }

        public void onSkipped(final FrameKind kind, final long p, final long t) {
        }
    }

    /**
     * Writes {@code steps} behind one term event ({@code termId} at {@code initialPosition}, its own term base), with a
     * {@code ClusterAction(SNAPSHOT, 0)} after step {@code actionAfter} (-1: none), then captures it with the kit reader.
     */
    static Log write(final RecordedLogFixture fixture, final List<LogStep> steps, final int actionAfter,
                     final long termId, final long initialPosition) {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, initialPosition);
        final ClusterLogWriter w = rec.writer();
        final long t0 = steps.get(0).timestamp();
        w.newLeadershipTerm(termId, t0, initialPosition, 0, rec.publication().sessionId(), TimeUnit.MILLISECONDS,
                CoreReplayParityTest.APP_VERSION);
        long action = -1;
        for (int i = 0; i < steps.size(); i++) {
            final LogStep s = steps.get(i);
            switch (s.kind()) {
                case OPEN -> w.sessionOpen(s.sessionId(), 7_000_000L + i, s.timestamp(), 102,
                        "aeron:udp?endpoint=localhost:20" + (s.sessionId() % 100), s.bytes());
                case CLOSE -> w.sessionClose(s.sessionId(), s.timestamp(), CloseReason.CLIENT_ACTION);
                case MESSAGE -> w.message(s.sessionId(), s.timestamp(), s.bytes());
                case TIMER -> w.timer(s.correlationId(), s.timestamp());
            }
            if (i == actionAfter) {
                action = w.action(ClusterAction.SNAPSHOT, s.timestamp(), 0);
            }
        }
        final long stop = rec.stopRecording();
        final RecordedLog frames = RecordedLog.capture(fixture.replay(rec.recordingId(), initialPosition,
                AeronArchive.NULL_LENGTH), initialPosition, stop);
        return new Log(frames, action, termId, initialPosition, rec.recordingId(), stop);
    }

    /** reader -> container -> applier over a pristine applier, from genesis. */
    static final class Arm {
        final StampSink sink = new StampSink();
        final int[] failFast = new int[1];
        final CoreLogApplier applier = new CoreLogApplier(() -> failFast[0]++);
        final ReplayContainer container;
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1 << 20);

        Arm() {
            this(CoreReplayParityTest.APP_VERSION);
        }

        /** A member's real log carries the cluster's own app version; the scaffold replays it under that. */
        Arm(final int appVersion) {
            applier.setEventSink(sink);
            container = new ReplayContainer(engine(applier), appVersion,
                    AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR, ShadowTriggerListener.NONE);
        }

        Arm genesis() {
            container.genesis();
            return this;
        }

        Arm restore(final DirectBuffer payload, final int length, final long imagePosition, final LogCursor from) {
            container.restore(payload, length, imagePosition, from);
            return this;
        }

        Arm apply(final Log log, final long afterExclusive, final long throughInclusive) {
            log.frames().replay(afterExclusive, throughInclusive, container);
            return this;
        }

        byte[] image() {
            final int n = container.serialize(buffer);
            return Arrays.copyOf(buffer.byteArray(), n);
        }

        int events() {
            return sink.events().size();
        }
    }

    /** Over a scratch applier: restore, re-serialize, byte compare, the engine's restore invariants. */
    static final class ApplierSelfCheck implements ShadowSelfCheck {
        int calls;

        @Override
        public boolean verify(final DirectBuffer payload, final int length) {
            calls++;
            final CoreLogApplier scratch = new CoreLogApplier(() -> { });
            scratch.restore(payload, length);
            scratch.engine().verifyRestoredInvariants();
            final ExpandableArrayBuffer again = new ExpandableArrayBuffer(length + 64);
            final int n = scratch.serialize(again);
            if (n != length) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (again.getByte(i) != payload.getByte(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Serializes the arm's state behind the header slot. */
    static ShadowImageBuffer image(final Arm arm, final int capacity) {
        final ShadowImageBuffer image = new ShadowImageBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(capacity)));
        image.setPayloadLength(arm.container.serialize(image.payloadView()));
        return image;
    }

    static ShadowWriter writer(final RecordedLogFixture fixture, final ShadowCounters counters) {
        return new ShadowWriter(fixture.aeron(), fixture.archive(), REPLICA, new ApplierSelfCheck(), SHADOW_TERM,
                SystemNanoClock.INSTANCE, TIMEOUT_NS, TIMEOUT_NS, TIMEOUT_NS, 1, -1, counters);
    }

    static ShadowCatalog catalog(final io.aeron.Aeron aeron, final AeronArchive archive, final ShadowVerdicts verdicts,
                                 final long genesis) {
        return new ShadowCatalog(aeron, archive, ENGINE, 0, genesis, verdicts, ShadowCatalog.DEFAULT_LIST_BATCH,
                SystemNanoClock.INSTANCE, TIMEOUT_NS, new ShadowCounters());
    }

    static ShadowIdentity.LocalIdentity member(final long genesis, final Map<Long, Long> terms) {
        return new ShadowIdentity.LocalIdentity(ENGINE, 0, genesis, t -> terms.getOrDefault(t, -1L));
    }

    /** The steps of a story in which the ADMIN session ADOPTs {@link #GENESIS} early (A7 fixture). */
    static List<LogStep> story(final long seed, final int frames) {
        return CoreReplayParityTest.coreSteps(seed, frames);
    }

    /** A negative control: {@code body} with one rule off must fail with {@code named}; prints {@code NC id -> msg}. */
    static String nc(final String id, final String named, final Runnable body) {
        try {
            body.run();
        } catch (final AssertionError e) {
            final String msg = String.valueOf(e.getMessage());
            if (!msg.contains(named)) {
                throw new AssertionError("negative control " + id + " failed for the wrong reason: <" + msg
                        + ">, expected <" + named + ">", e);
            }
            System.out.println("NC " + id + " -> " + msg);
            return msg;
        }
        throw new AssertionError("negative control " + id + " did not fail: the assertion cannot see its rule");
    }

    static List<LogFrame> framesOf(final Log log) {
        return new ArrayList<>(log.frames().frames());
    }
}

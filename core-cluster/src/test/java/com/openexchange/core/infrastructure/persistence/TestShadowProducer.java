// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.persistence.CoreShadowRig.Arm;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowProducer;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SystemNanoClock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The test producer. has no production producer (the replica is, the follower producer is
 * ), so the restore tests obtain shadows at chosen positions here: a member's (or a fixture's) log, captured as a
 * {@link RecordedLog}, is replayed through {@code ReplayContainer} + a fresh {@link CoreLogApplier} up to a chosen
 * frame end, the state is serialized, and the image is written through the kit's {@link ShadowWriter} into the given
 * archive. That is the replica's job done by test code, legitimate because the archive catalog is the only registry.
 *
 * <p>It also forges the controls of the 11.0 table, each with the outcome the contract names:</p>
 * <ul>
 *   <li>{@link #produceBackwardShift}: content {@code S(y)} under a header that says {@code P_s = x < y}. Detected by
 *       W1 (6.3) when {@code y} has a later millisecond; with the same millisecond W1 cannot see it and only the
 *       frame-level equivalence (11.1) does: {@code final image differs from baseline}.</li>
 *   <li>{@link #produceForwardShift}: content {@code S(x)} under a header that says {@code P_s = y > x}. Nothing in
 *       detects it (6.3); 11.1 only. Prevented structurally (hard rule 12.13).</li>
 *   <li>{@link #produceForeignGenesis}: STRICT identity, {@code GENESIS_MISMATCH}.</li>
 *   <li>{@link #flipPayloadByte}: a byte flipped in the recorded payload after sealing, loader digest {@code CORRUPT}.</li>
 *   <li>{@link #produceDifferentContent}: a second producer's different content at one position, catalog {@code CONFLICT}.</li>
 * </ul>
 *
 * <p>Public because the cluster-level tests live in another package. The rule it models is hard rule 12.13: the
 * header's {@code P_s} comes from the cursor of the frame the container just applied; only the forgeries break it, on
 * purpose.</p>
 */
public final class TestShadowProducer {

    public static final int CAPACITY = 4 << 20;
    private static final long TIMEOUT_NS = 30_000_000_000L;
    private static final int FALLBACK_APP_VERSION = CoreReplayParityTest.APP_VERSION;

    /** A producer identity: kind and id (the alias and the CONFLICT distinction come from it). */
    public record Who(ProducerKind kind, int id) {
        public static final Who REPLICA_0 = new Who(ProducerKind.REPLICA, 0);
        public static final Who FOLLOWER_1 = new Who(ProducerKind.FOLLOWER, 1);
        public static final Who FOLLOWER_2 = new Who(ProducerKind.FOLLOWER, 2);
    }

    /** What one write did. {@code headerPosition != contentPosition} only for the shift forgeries. */
    public record Written(String label, ShadowWriter.WriteResult result, long headerPosition, long contentPosition,
                          long headerClusterTime, long contentLastLogTime, long recordingId, int payloadLength,
                          long genesisId) {
        public boolean ok() {
            return result.ok();
        }

        @Override
        public String toString() {
            return label + " P_s=" + headerPosition + " content=S(" + contentPosition + ") rec=" + recordingId
                    + " payload=" + payloadLength + " B " + result;
        }
    }

    /** A client of a member's media driver and archive (the producer writes into that archive). */
    public record MemberArchive(Aeron aeron, AeronArchive archive) {
    }

    /** A frame pair (x, y = the next frame) for the shift forgeries. */
    public record ShiftPair(long x, long y, long xTime, long yTime) {
    }

    private final Aeron aeron;
    private final AeronArchive archive;
    private final int clusterId;
    private final File archiveDir;
    private final ShadowCounters counters = new ShadowCounters();
    private final Map<Who, ShadowWriter> writers = new HashMap<>();
    /** The image buffer size (the 100k leg's images exceed {@link #CAPACITY}). */
    private int capacity = CAPACITY;

    /**
     * @param aeron      a client on the driver whose archive receives the shadows
     * @param archive    a client of that archive
     * @param clusterId  {@code cluster.context().clusterId()} (0 on the fixtures)
     * @param archiveDir the archive's directory, only for {@link #flipPayloadByte}; null when no flip is needed
     */
    public TestShadowProducer(final Aeron aeron, final AeronArchive archive, final int clusterId, final File archiveDir) {
        this.aeron = aeron;
        this.archive = archive;
        this.clusterId = clusterId;
        this.archiveDir = archiveDir;
    }

    /** A larger image buffer for big engines (the 100k leg). */
    public TestShadowProducer capacity(final int bytes) {
        this.capacity = bytes;
        return this;
    }

    public ShadowCounters counters() {
        return counters;
    }

    // ---------------------------------------------------------------------------------------------- capture

    /**
     * A member's log recording as frames, {@code [from, to]}, read over its archive (the member's own log, 11.0). The
     * caller bounds {@code to} by the commit position so only committed frames are shadowed.
     */
    public static RecordedLog captureLog(final Aeron aeron, final AeronArchive archive, final long logRecordingId,
                                         final long from, final long to) {
        final String channel = "aeron:ipc";
        final int streamId = 9_100 + (int)(System.nanoTime() & 0xFF);
        final int sessionId = (int)archive.startReplay(logRecordingId, from, to - from, channel, streamId);
        final Subscription subscription = aeron.addSubscription(
                io.aeron.ChannelUri.addSessionId(channel, sessionId), streamId);
        try {
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            Image image;
            while ((image = subscription.imageBySessionId(sessionId)) == null) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("no replay image for log recording " + logRecordingId);
                }
                Thread.onSpinWait();
            }
            return RecordedLog.capture(image, from, to);
        } finally {
            CloseHelper.quietClose(subscription);
        }
    }

    /** The app version the log's own term events carry (the container must replay under it), or a fallback. */
    static int appVersionOf(final RecordedLog log) {
        for (final LogFrame f : log.frames()) {
            if (f.kind() == FrameKind.NEW_LEADERSHIP_TERM) {
                return f.appVersion();
            }
        }
        return FALLBACK_APP_VERSION;
    }

    // ---------------------------------------------------------------------------------------------- producing

    /** A genuine ALIGNED shadow of {@code S(p)} with {@code P_s = p}, {@code p} a frame end of {@code log}. */
    public Written produce(final Who who, final RecordedLog log, final long p) {
        return write("GENUINE", who, log, p, p, null);
    }

    /**
     * The trailer-less recording a {@code kill -9} mid-write leaves: genuine header and
     * payload chunks of {@code S(p)} from {@code who} at {@code producerSeq}, no trailer, no cleanup purge.
     */
    public Written produceCrashed(final Who who, final RecordedLog log, final long p, final long producerSeq) {
        final Arm content = new Arm(appVersionOf(log)).genesis();
        log.replay(-1, p, content.container);
        requireAt(content, p, "content");
        final ShadowImageBuffer image = CoreShadowRig.image(content, capacity);
        final ShadowWriter w = com.openexchange.cluster.shadow.CrashedShadowWriters.crashingBeforeTrailer(aeron, archive,
                new ShadowProducer(clusterId, who.kind(), who.id()), new CoreShadowRig.ApplierSelfCheck(),
                CoreShadowRig.SHADOW_TERM, producerSeq);
        final ShadowWriter.WriteResult r = w.write(image, ShadowHeader.ENGINE_CORE, ShadowKind.ALIGNED,
                content.container.cursor(), CoreShadowRig.identityOf(content.applier));
        return new Written("CRASHED_BEFORE_TRAILER", r, p, p, content.container.cursor().clusterTime(),
                content.applier.engine().getLastLogTime(), r.recordingId(), image.payloadLength(),
                CoreShadowRig.identityOf(content.applier).genesisId());
    }

    /**
     * For the two-restores control: a sealed, digest-valid shadow of {@code who} whose payload is NOT an engine
     * image (the envelope refuses it), under the genuine identity and cursor of {@code S(p)}. Written without the
     * self-check ({@code ShadowSelfCheck.UNCHECKED_TEST_ONLY}, which needs {@code -Doxs.shadow.unchecked=true}): the
     * loader accepts it, {@code restore} throws.
     */
    public Written produceUndecodable(final Who who, final RecordedLog log, final long p) {
        final Arm content = new Arm(appVersionOf(log)).genesis();
        log.replay(-1, p, content.container);
        requireAt(content, p, "content");
        final ShadowImageBuffer image = CoreShadowRig.image(content, capacity);
        final int n = image.payloadLength();
        for (int i = 0; i < n; i++) {
            image.payloadView().putByte(i, (byte)(0xA5 ^ i));
        }
        final ShadowWriter w = new ShadowWriter(aeron, archive, new ShadowProducer(clusterId, who.kind(), who.id()),
                com.openexchange.cluster.shadow.ShadowSelfCheck.UNCHECKED_TEST_ONLY, CoreShadowRig.SHADOW_TERM,
                SystemNanoClock.INSTANCE, TIMEOUT_NS, TIMEOUT_NS, TIMEOUT_NS, 1, -1, counters);
        final ShadowWriter.WriteResult r = w.write(image, ShadowHeader.ENGINE_CORE, ShadowKind.ALIGNED,
                content.container.cursor(), CoreShadowRig.identityOf(content.applier));
        return new Written("UNDECODABLE", r, p, p, content.container.cursor().clusterTime(),
                content.applier.engine().getLastLogTime(), r.recordingId(), n,
                CoreShadowRig.identityOf(content.applier).genesisId());
    }

    /** Backward shift: content {@code S(y)} under a header {@code P_s = x} ({@code x < y}). */
    public Written produceBackwardShift(final Who who, final RecordedLog log, final long x, final long y) {
        if (y <= x) {
            throw new IllegalArgumentException("backward shift needs y > x: x=" + x + " y=" + y);
        }
        return write("BACKWARD_SHIFT", who, log, y, x, null);
    }

    /** Forward shift: content {@code S(x)} under a header {@code P_s = y} ({@code y > x}). */
    public Written produceForwardShift(final Who who, final RecordedLog log, final long x, final long y) {
        if (y <= x) {
            throw new IllegalArgumentException("forward shift needs y > x: x=" + x + " y=" + y);
        }
        return write("FORWARD_SHIFT", who, log, x, y, null);
    }

    /** Genuine content under another cluster's genesis nonce: STRICT identity answers {@code GENESIS_MISMATCH}. */
    public Written produceForeignGenesis(final Who who, final RecordedLog log, final long p, final long foreignGenesis) {
        if (foreignGenesis <= 0) {
            throw new IllegalArgumentException("a foreign genesis must be a real nonce (> 0): " + foreignGenesis);
        }
        return write("FOREIGN_GENESIS", who, log, p, p, foreignGenesis);
    }

    /**
     * A second producer's different content at one position: content {@code S(y)} at {@code P_s = x}. Together with a
     * genuine shadow at {@code x} from another producer this is a catalog {@code CONFLICT} at {@code x}.
     */
    public Written produceDifferentContent(final Who who, final RecordedLog log, final long x, final long y) {
        final Written w = produceBackwardShift(who, log, x, y);
        return new Written("DIFFERENT_CONTENT", w.result(), w.headerPosition(), w.contentPosition(),
                w.headerClusterTime(), w.contentLastLogTime(), w.recordingId(), w.payloadLength(), w.genesisId());
    }

    /**
     * (11.3 skip-safe row): genuine content whose header claims the money journal is armed at
     * {@code journalSeq}. On a node whose own journal never reached that seq the candidate is not skip-safe (8.1).
     */
    public Written produceJournalClaim(final Who who, final RecordedLog log, final long p, final long journalSeq) {
        return write("JOURNAL_CLAIM", who, log, p, p, null, journalSeq);
    }

    private Written write(final String label, final Who who, final RecordedLog log, final long contentPosition,
                          final long headerPosition, final Long genesisOverride) {
        return write(label, who, log, contentPosition, headerPosition, genesisOverride, null);
    }

    private Written write(final String label, final Who who, final RecordedLog log, final long contentPosition,
                          final long headerPosition, final Long genesisOverride, final Long journalSeqOverride) {
        final int appVersion = appVersionOf(log);
        final Arm content = new Arm(appVersion).genesis();
        log.replay(-1, contentPosition, content.container);
        requireAt(content, contentPosition, "content");
        final LogCursor cursor;
        if (headerPosition == contentPosition) {
            cursor = content.container.cursor();
        } else {
            final Arm header = new Arm(appVersion).genesis();
            log.replay(-1, headerPosition, header.container);
            requireAt(header, headerPosition, "header");
            cursor = header.container.cursor();
        }
        final ShadowImageBuffer image = CoreShadowRig.image(content, capacity);
        final ShadowIdentitySource real = CoreShadowRig.identityOf(content.applier);
        final ShadowIdentitySource identity = genesisOverride == null && journalSeqOverride == null ? real
                : new ShadowIdentitySource() {
            public long genesisId() { return genesisOverride == null ? real.genesisId() : genesisOverride; }
            public boolean journalEnabled() { return journalSeqOverride != null || real.journalEnabled(); }
            public long journalSeq() { return journalSeqOverride == null ? real.journalSeq() : journalSeqOverride; }
            public int payloadLayoutHint() { return real.payloadLayoutHint(); }
        };
        final ShadowWriter.WriteResult r = writer(who).write(image, ShadowHeader.ENGINE_CORE, ShadowKind.ALIGNED,
                cursor, identity);
        return new Written(label, r, headerPosition, contentPosition, cursor.clusterTime(),
                content.applier.engine().getLastLogTime(), r.recordingId(), image.payloadLength(), identity.genesisId());
    }

    private static void requireAt(final Arm arm, final long position, final String what) {
        final long at = arm.container.cursor().logPosition();
        if (at != position) {
            throw new IllegalArgumentException(what + " position " + position + " is not a frame end of the log (the container"
                    + " stopped at " + at + ")");
        }
    }

    private ShadowWriter writer(final Who who) {
        return writers.computeIfAbsent(who, w -> new ShadowWriter(aeron, archive,
                new ShadowProducer(clusterId, w.kind(), w.id()), new CoreShadowRig.ApplierSelfCheck(),
                CoreShadowRig.SHADOW_TERM, SystemNanoClock.INSTANCE, TIMEOUT_NS, TIMEOUT_NS, TIMEOUT_NS, 1, -1, counters));
    }

    /** The oracle image {@code S(p)}: a fresh applier over {@code log} up to the frame end {@code p}. */
    public static byte[] imageAt(final RecordedLog log, final long p) {
        final Arm arm = new Arm(appVersionOf(log)).genesis();
        log.replay(-1, p, arm.container);
        requireAt(arm, p, "oracle");
        return arm.image();
    }

    // ---------------------------------------------------------------------------------------------- forgeries

    /**
     * Flips one byte inside the first payload chunk of a written shadow, in the archive's segment file (the payload
     * bit flip of 11.0, after sealing). The recording stays COMPLETE for the catalog's cheap scan; the loader's digest
     * and {@code verifyDeep} answer CORRUPT.
     */
    public void flipPayloadByte(final Written w) {
        if (archiveDir == null) {
            throw new IllegalStateException("flipPayloadByte needs the archive directory");
        }
        final File segment = new File(archiveDir, w.recordingId() + "-0.rec");
        if (!segment.isFile()) {
            throw new IllegalStateException("segment file " + segment + " not found");
        }
        // header message (FRAME_LENGTH) + the first chunk's 32 byte data header, then 64 bytes into the payload
        final long offset = ShadowHeader.FRAME_LENGTH + 32 + 64;
        try (RandomAccessFile f = new RandomAccessFile(segment, "rw")) {
            f.seek(offset);
            final int b = f.read();
            f.seek(offset);
            f.write(b ^ 0x01);
        } catch (final IOException e) {
            throw new IllegalStateException("cannot flip a byte of " + segment, e);
        }
    }

    // ---------------------------------------------------------------------------------------------- oracles

    /**
     * The first frame pair {@code (x, y)} after {@code fromExclusive} with the genesis already ADOPTed at {@code x}, the
     * state changed by {@code y}, and {@code y}'s millisecond later than (or, with {@code sameMs}, equal to) {@code x}'s.
     * That is the pair on which a shift is a real forgery (the images differ, and applying {@code y} on top of an image that
     * already holds it changes the image again, so a double apply is observable) and on which W1 does or does not see it.
     */
    public static ShiftPair findShiftPair(final RecordedLog log, final boolean sameMs, final long fromExclusive) {
        final List<LogFrame> frames = log.frames();
        final Arm arm = new Arm(appVersionOf(log)).genesis();
        byte[] previous = null;
        LogFrame previousFrame = null;
        boolean previousAdopted = false;
        for (final LogFrame frame : frames) {
            frame.deliver(arm.container);
            final byte[] image = arm.image();
            final boolean adopted = CoreShadowRig.identityOf(arm.applier).genesisId() > 0;
            if (previousFrame != null && previousAdopted && previousFrame.position() > fromExclusive
                    && frame.kind() != FrameKind.CLUSTER_ACTION && previousFrame.kind() != FrameKind.CLUSTER_ACTION
                    && !Arrays.equals(previous, image)
                    && (sameMs ? frame.timestamp() == previousFrame.timestamp()
                    : frame.timestamp() > previousFrame.timestamp())
                    && replayingTheFrameAgainChangesTheImage(log, previousFrame.position(), frame.position())) {
                return new ShiftPair(previousFrame.position(), frame.position(), previousFrame.timestamp(),
                        frame.timestamp());
            }
            previous = image;
            previousFrame = frame;
            previousAdopted = adopted;
        }
        throw new IllegalStateException("no " + (sameMs ? "same-ms" : "later-ms") + " state-changing frame pair after "
                + fromExclusive + " in " + frames.size() + " frames");
    }

    /** A backward-shifted restore (content {@code S(y)} at cursor {@code x}) followed by the frames {@code (x, y]}. */
    static boolean replayingTheFrameAgainChangesTheImage(final RecordedLog log, final long x, final long y) {
        final int appVersion = appVersionOf(log);
        final Arm content = new Arm(appVersion).genesis();
        log.replay(-1, y, content.container);
        final byte[] sy = content.image();
        final Arm header = new Arm(appVersion).genesis();
        log.replay(-1, x, header.container);
        final Arm restored = new Arm(appVersion);
        restored.restore(new org.agrona.concurrent.UnsafeBuffer(sy), sy.length, x, header.container.cursor());
        log.replay(x, y, restored.container);
        return !Arrays.equals(sy, restored.image());
    }

    /**
     * W1 (contract 6.3), the oracle the production witness must match: the restored engine's running maximum of observed
     * cluster times must not exceed the header's cluster time. Null when it holds. One-directional: it sees an image that
     * holds a frame AFTER the header's P_s with a later millisecond, and nothing else.
     */
    public static String witnessFailure(final CoreLogApplier restored, final ShadowHeader header) {
        final long lastLogTime = restored.engine().getLastLogTime();
        if (lastLogTime <= header.clusterTime()) {
            return null;
        }
        return "shadow P_s witness failed: lastLogTime=" + lastLogTime + " > header.clusterTime=" + header.clusterTime();
    }
}

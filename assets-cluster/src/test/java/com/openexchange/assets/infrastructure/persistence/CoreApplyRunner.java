// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.feed.FeedState;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.feedharness.CoreFrames;
import com.openexchange.assets.feedharness.FeedFiles;
import com.openexchange.assets.feedharness.FeedHarnessSeams;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.auth.ClusterPrincipal;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RC2 S9h section 6: a real behaviour-3 core in process (the production {@link AssetsEngine} + {@link SettlementProjector}
 * + {@link AssetsSbeDemuxer} + a {@link RecordingAssetsSink}, no Aeron), driven entry by entry as the clustered service
 * drives it: the log position is set, then the session's {@code dispatch}. Feed frames always go through a registered
 * BRIDGE session (class mask, registration-first and the lane table run as in production); the trusted dispatch is never
 * used for a feed frame. Every verdict (apply, duplicate, refuse, latch) is the production engine's (R3).
 *
 * <p>Log positions follow the {@link SessionedLog} rule (a message occupies 64 + its 32-aligned length, an open, close or
 * timer 64), so the runner reproduces the image of a re-homed {@link CoreFrameStory} exactly.</p>
 */
public final class CoreApplyRunner implements AutoCloseable {

    /** The genesis of a run: the core's genesisId (= the ME epoch it binds), the funded users and t0 (ms). */
    public record Genesis(long genesisId, int users, long depositEach, long t0) { }

    /** The ADMIN, TREASURY sessions boot opens (fixed ids; the story's other sessions open in its round 0 block). */
    public static final long ADMIN_SESSION = 801L;
    public static final long TREASURY_SESSION = 804L;
    /** The pinned clean-run BRIDGE session (6.3, review FIX-5). */
    public static final long BRIDGE_SESSION = 701L;
    public static final long BRIDGE_IDENTITY = 0xB1L;

    /** One ORIGIN event of an entry (the decoded money v7 egress frame and its rendering). */
    public record OriginEvent(int templateId, byte[] frame, String render) { }

    /** The ORIGIN events one entry produced for its session. */
    public record Egress(List<OriginEvent> events) {
        public long settleRejected() {
            return events.stream().filter(e -> e.templateId() == 114).count();
        }
    }

    /** QueryFeedPosition answer (the v7 FeedPositionReport fields). */
    public record Report(long correlationId, long meEpoch, long lastAppliedJournalSeq, boolean feedHalted, int haltReason,
                         long consumePosition, long lastAppliedTradeId, byte[] frame) { }

    /** 6.1 Result. */
    public record Result(String snapshotSha256, long lastAppliedJournalSeq, long boundMeEpoch, long lastAppliedTradeId,
                         long consumePosition, String latchReason, long latchExpected, long latchSeen, long latchSkip,
                         int globalGateReasons, Map<String, Long> counters, boolean invariantsHold, boolean applyFailed,
                         long lastLogTime) {

        /** Deterministic JSON (fixed key order). */
        public String toJson() {
            final StringBuilder sb = new StringBuilder("{");
            sb.append("\"snapshotSha256\":\"").append(snapshotSha256).append('"');
            sb.append(",\"lastAppliedJournalSeq\":").append(lastAppliedJournalSeq);
            sb.append(",\"boundMeEpoch\":").append(boundMeEpoch);
            sb.append(",\"lastAppliedTradeId\":").append(lastAppliedTradeId);
            sb.append(",\"consumePosition\":").append(consumePosition);
            sb.append(",\"latch\":{\"reason\":\"").append(latchReason).append("\",\"expected\":").append(latchExpected)
                    .append(",\"seen\":").append(latchSeen).append(",\"skip\":").append(latchSkip).append('}');
            sb.append(",\"globalGateReasons\":").append(globalGateReasons);
            sb.append(",\"lastLogTime\":").append(lastLogTime);
            sb.append(",\"counters\":{");
            boolean first = true;
            for (final Map.Entry<String, Long> e : counters.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append('"').append(e.getKey()).append("\":").append(e.getValue());
            }
            sb.append('}');
            sb.append(",\"invariantsHold\":").append(invariantsHold);
            sb.append(",\"applyFailed\":").append(applyFailed);
            return sb.append('}').toString();
        }
    }

    private final AssetsEngine engine = new AssetsEngine();
    private final RecordingAssetsSink sink = new RecordingAssetsSink();
    private final AssetsSbeDemuxer demuxer;
    private final CoreFrames enc = new CoreFrames();
    private final UnsafeBuffer frame = new UnsafeBuffer(new byte[0]);
    private final long[] treasuryNet = new long[Asset.count()];
    private long position;
    private long corr = 1;
    private long request = 1;
    private boolean applyFailed;
    /** Review NOTE-4 b: the CoreConfig cross-check hook production requires (AssetsLogApplier); a call fails invariantsHold. */
    private long configFailFast;
    /** Every entry this runner applied, as FLOG (review NOTE-9: a story's boot prefix is replayable by {@code apply}). */
    private final List<FeedFiles.LogEntry> log = new ArrayList<>();

    private CoreApplyRunner() {
        engine.setConfigFailFast(() -> configFailFast++);
        engine.setEventSink(sink);
        demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
    }

    /** A bare core (no session, no config): {@code apply} replays a complete core log onto it. */
    public static CoreApplyRunner empty() {
        return new CoreApplyRunner();
    }

    /**
     * Fresh core; behaviour 3 is reached by CoreConfig 1 -> 2 -> 3 FRAMES on an ADMIN session (the production supported
     * set is {1, 2, 3} since S8; no seam), GLOBAL stays SUSPENDED until the bind. Users 1..users get {@code depositEach}
     * of every asset through a TREASURY session.
     */
    public static CoreApplyRunner boot(final Genesis g) {
        final CoreApplyRunner r = new CoreApplyRunner();
        r.openSession(ADMIN_SESSION, SessionClass.ADMIN, 1L, g.t0());
        for (long v = 1; v <= 3; v++) {
            r.apply(ADMIN_SESSION, r.enc.coreConfig(r.corr++, v, v, g.genesisId()), g.t0());
        }
        if (r.engine.coreConfig().behaviorVersion() != 3 || r.engine.coreConfig().recorded().genesisId != g.genesisId()) {
            throw new IllegalStateException("CoreApplyRunner.boot: behaviour 3 with genesisId " + g.genesisId() + " not recorded");
        }
        r.openSession(TREASURY_SESSION, SessionClass.TREASURY, 9L, g.t0());
        for (int u = 1; u <= g.users(); u++) {
            for (final Asset a : Asset.values()) {
                if (g.depositEach() > 0) {
                    r.apply(TREASURY_SESSION, r.enc.deposit(r.corr++, u, a.id(), g.depositEach(), 0xB007L, r.request++), g.t0());
                }
            }
        }
        return r;
    }

    // ------------------------------------------------------------------ log entries

    private void advance(final long size) {
        position += size;
        engine.setLogPosition(position);
    }

    /** SessionOpen with {@code ClusterPrincipal.anyUser(class, keyId)}. */
    public void openSession(final long sessionId, final SessionClass c, final long keyId, final long timestamp) {
        openSession(sessionId, ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(c), (int) keyId).encode(), timestamp);
    }

    public void openSession(final long sessionId, final SessionClass c, final long keyId) {
        openSession(sessionId, c, keyId, engine.getLastLogTime());
    }

    /** SessionOpen with an explicit principal (FLOG SESSION_OPEN). */
    public void openSession(final long sessionId, final byte[] principal) {
        openSession(sessionId, principal, engine.getLastLogTime());
    }

    public void openSession(final long sessionId, final byte[] principal, final long timestamp) {
        advance(64);
        log.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.SESSION_OPEN, sessionId, timestamp, principal));
        engine.sessions().open(sessionId, principal);
    }

    public void closeSession(final long sessionId) {
        advance(64);
        log.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.SESSION_CLOSE, sessionId, engine.getLastLogTime(), new byte[0]));
        engine.sessions().close(sessionId);
    }

    public void sweep(final long timestamp) {
        advance(64);
        log.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.SWEEP, 0L, timestamp, new byte[0]));
        engine.sweep(timestamp);
    }

    /** RegisterSession{BRIDGE, acks|settlements, resumable TRUE, identity, sourceEpoch 0} on an open session. */
    public Egress registerBridge(final long sessionId, final long identity, final long timestamp) {
        return apply(sessionId, enc.register(corr++, SessionClass.BRIDGE, identity, 0L), timestamp);
    }

    /** Open (as BRIDGE, key 3) and register a bridge session. */
    public Egress openBridge(final long sessionId, final long identity, final long timestamp) {
        if (engine.sessions().get(sessionId) == null) {
            openSession(sessionId, SessionClass.BRIDGE, 3L, timestamp);
        }
        return registerBridge(sessionId, identity, timestamp);
    }

    public boolean isFeedTemplate(final int templateId) {
        return templateId >= 61 && templateId <= 68 && templateId != 65 && templateId != 66;
    }

    /** One log entry on {@code sessionId}; returns the ORIGIN egress this entry produced. A core apply fault rethrows (H4). */
    public Egress apply(final long sessionId, final byte[] bytes, final long timestamp) {
        if (applyFailed) {
            throw new IllegalStateException("the core's apply fence latched; restart required");
        }
        advance(64 + ((bytes.length + 31) & ~31));
        log.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.FRAME, sessionId, timestamp, bytes.clone()));
        final int from = sink.events().size();
        frame.wrap(bytes);
        final int templateId = bytes.length >= 4 ? (bytes[2] & 0xFF) | (bytes[3] & 0xFF) << 8 : -1;
        try {
            if (FeedHarnessSeams.on(FeedHarnessSeams.TRUSTED_FEED) && isFeedTemplate(templateId)) {
                demuxer.dispatchTrusted(frame, 0, bytes.length, timestamp);
            } else {
                demuxer.dispatch(sessionId, frame, 0, bytes.length, timestamp);
            }
        } catch (RuntimeException | Error e) {
            applyFailed = true;
            throw e;
        }
        return new Egress(origin(from));
    }

    public Report query(final long sessionId, final long correlationId, final long timestamp) {
        final Egress e = apply(sessionId, enc.query(correlationId), timestamp);
        for (final OriginEvent o : e.events()) {
            if (o.templateId() == 17) {
                final org.agrona.concurrent.UnsafeBuffer b = new UnsafeBuffer(o.frame());
                final var d = new com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder()
                        .wrapAndApplyHeader(b, 0, new com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder());
                return new Report(d.correlationId(), d.meEpoch(), d.lastAppliedJournalSeq(), d.feedHaltedRaw() == 1,
                        d.haltReasonRaw(), d.consumePosition(), d.lastAppliedTradeId(), o.frame());
            }
        }
        throw new IllegalStateException("QueryFeedPosition produced no FeedPositionReport");
    }

    /** One FLOG entry. */
    public Egress apply(final FeedFiles.LogEntry e) {
        switch (e.kind()) {
            case FeedFiles.LogEntry.FRAME:
                return apply(e.sessionId(), e.bytes(), e.timestamp());
            case FeedFiles.LogEntry.SESSION_OPEN:
                openSession(e.sessionId(), e.bytes(), e.timestamp());
                return new Egress(List.of());
            case FeedFiles.LogEntry.SESSION_CLOSE:
                closeSession(e.sessionId());
                return new Egress(List.of());
            case FeedFiles.LogEntry.SWEEP:
                sweep(e.timestamp());
                return new Egress(List.of());
            default:
                throw new IllegalArgumentException("FLOG kind " + e.kind());
        }
    }

    /** The ORIGIN events emitted since {@code from}, re-encoded as their money v7 egress frames. */
    private List<OriginEvent> origin(final int from) {
        final List<AssetsEngineEvent> events = sink.events();
        final List<OriginEvent> out = new ArrayList<>();
        for (int i = from; i < events.size(); i++) {
            final AssetsEngineEvent ev = events.get(i);
            if (ev instanceof AssetsEngineEvent.SettleRejected s) {
                out.add(new OriginEvent(114, enc.settleRejected(s.tradeId(), s.marketId(), s.journalPosition(),
                        com.openexchange.assets.infrastructure.generated.SettleRejectReason.valueOf(s.reason().name()).value(),
                        s.detail()), s.render()));
            } else if (ev instanceof AssetsEngineEvent.AdminAck a) {
                out.add(new OriginEvent(110, enc.adminAck(a.correlationId(), a.refTemplateId(), a.result().code(), a.reason(),
                        a.configVersion()), a.render()));
            } else if (ev instanceof AssetsEngineEvent.FeedPositionReport f) {
                out.add(new OriginEvent(17, enc.feedPositionReport(f.correlationId(), f.consumePosition(),
                        f.lastAppliedTradeId(), f.journalEnabled(), f.meEpoch(), f.lastAppliedJournalSeq(), f.feedHalted(),
                        f.haltReason().code()), f.render()));
            } else if (ev instanceof AssetsEngineEvent.DepositAck d && !d.duplicate()) {
                treasuryNet[d.assetId()] += d.amount();
            } else if (ev instanceof AssetsEngineEvent.WithdrawAck w && !w.duplicate()) {
                treasuryNet[w.assetId()] -= w.amount();
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ reads

    public AssetsEngine engine() {
        return engine;
    }

    public AssetsSbeDemuxer demuxer() {
        return demuxer;
    }

    public RecordingAssetsSink sink() {
        return sink;
    }

    public long position() {
        return position;
    }

    public boolean applyFailed() {
        return applyFailed;
    }

    /** The entries applied so far, as FLOG entries (the boot prefix of a story, for {@code apply}). */
    public List<FeedFiles.LogEntry> log() {
        return List.copyOf(log);
    }

    /** The production snapshot image: the payload sealed in the kit envelope (what onTakeSnapshot offers). */
    public byte[] image() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64 * 1024);
        final int payloadLength = BalanceSnapshotCodec.serialize(engine, buffer, SnapshotEnvelope.payloadOffset());
        final int length = new SnapshotEnvelope().seal(buffer, payloadLength);
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    public Result result() {
        final byte[] image = image();
        final FeedState f = engine.feed();
        final Map<String, Long> c = new LinkedHashMap<>();
        for (final int t : new int[] {61, 62, 63, 64, 67, 68}) {
            c.put("feedDuplicateCount" + t, f.duplicateCount(t));
        }
        c.put("feedGapCount", f.gapCount());
        c.put("feedEpochMismatchCount", f.epochMismatchCount());
        c.put("feedUnboundCount", f.unboundCount());
        c.put("feedSettleRefusedCount", f.settleRefusedCount());
        c.put("feedSourceSchemaCount", f.sourceSchemaCount());
        c.put("feedDuplicateMismatchCount", f.duplicateMismatchCount());
        c.put("feedHaltApplied", f.haltAppliedCount());
        c.put("feedHaltStale", f.haltStaleCount());
        c.put("feedHaltDuplicate", f.haltDuplicateCount());
        c.put("lanesBehind", f.lanesBehind());
        c.put("schemaRejectCount", demuxer.schemaRejectCount());
        c.put("decodeRejectCount", demuxer.decodeRejectCount());
        c.put("unknownTemplateCount", demuxer.unknownTemplateCount());
        c.put("unhandledTemplateCount", demuxer.unhandledTemplateCount());
        c.put("settleRejectCount", engine.getSettleRejectCount());
        c.put("configFailFast", configFailFast);
        return new Result(FeedFiles.sha256Hex(image), f.lastAppliedJournalSeq(), f.boundMeEpoch(), engine.getLastAppliedTradeId(),
                engine.getConsumePosition(), f.latchReason().name(), f.latchExpectedSeq(), f.latchSeenSeq(), f.latchSkipSeq(),
                engine.gate().reasons(OrderEntryGate.GLOBAL), c, invariantsHold(image), applyFailed, engine.getLastLogTime());
    }

    /** A snapshot round trip (restore + verifyRestoredInvariants, same bytes again) and asset conservation. */
    private boolean invariantsHold(final byte[] image) {
        try {
            if (configFailFast != 0) {
                return false;
            }
            final FrameReplica restored = FrameReplica.restore(image);
            if (restored.failFastCalls != 0 || !java.util.Arrays.equals(image, restored.image())) {
                return false;
            }
        } catch (RuntimeException | AssertionError e) {
            return false;
        }
        final long[] total = new long[Asset.count()];
        engine.forEachAccount(a -> {
            for (int asset = 0; asset < total.length; asset++) {
                total[asset] += a.available(asset) + a.locked(asset);
            }
        });
        return java.util.Arrays.equals(total, treasuryNet);
    }

    @Override
    public void close() {
        // nothing to release: no Aeron, no files
    }
}

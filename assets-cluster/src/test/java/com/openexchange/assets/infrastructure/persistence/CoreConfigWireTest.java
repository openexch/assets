// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.auth.CorePrincipal;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.CoreConfigEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S7 (contract section 6, Wire): the demuxer serves CoreConfig (70). Both groups are decoded and reach
 * {@link AssetsEngine#applyCoreConfig}; the class mask admits ADMIN only; malformed group bytes (a raw count
 * beyond the frame, a non-printable symbol byte) are decode rejects, never an apply. Rig modelled on
 * IngressFenceTest's.
 */
public class CoreConfigWireTest {

    private static final long TS = 1_000L;
    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final long ONE = 100_000_000L;
    private static final long TICK = ONE / 100;
    private static final long MIN = TICK;
    private static final long MAX = 1_000_000 * ONE;
    private static final long LOT = ONE / 1_000;

    private static final class Rig {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final SettlementProjector projector = new SettlementProjector(engine);
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, projector);
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[2048]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        int failFastCalls;
        int mark;

        Rig() {
            engine.setEventSink(sink);
            engine.setConfigFailFast(() -> failFastCalls++);
        }

        /** Encode {@code c} with the generated CoreConfigEncoder (markets and assets groups); returns the frame length. */
        int coreConfig(final CoreConfigCommand c) {
            final CoreConfigEncoder e = new CoreConfigEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(c.correlationId).configVersion(c.configVersion).behaviorVersion(c.behaviorVersion);
            final CoreConfigEncoder.MarketsEncoder m = e.marketsCount(c.markets.length);
            for (final CoreConfigCommand.MarketDef d : c.markets) {
                m.next().marketId(d.marketId()).symbol(d.symbol()).baseAssetId(d.baseAssetId())
                        .quoteAssetId(d.quoteAssetId()).tickSize(d.tickSize()).minPrice(d.minPrice())
                        .maxPrice(d.maxPrice()).lotSize(d.lotSize());
            }
            final CoreConfigEncoder.AssetsEncoder a = e.assetsCount(c.assets.length);
            for (final CoreConfigCommand.AssetDef d : c.assets) {
                a.next().assetId(d.assetId()).code(d.code()).scale((short) d.scale());
            }
            return HEADER + e.encodedLength();
        }

        int register(final long corr, final SessionClass cls) {
            final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(corr).sessionClass(cls).readerKind((short) 0)
                    .resumable(BoolFlag.TRUE).identity(0).sourceEpoch(0);
            e.channels().clear().acks(true).settlements(true);
            return HEADER + e.encodedLength();
        }

        long open(final long sessionId, final CorePrincipal principal) {
            engine.sessions().open(sessionId, principal.encode());
            return sessionId;
        }

        void dispatchTrusted(final int length) {
            demuxer.dispatchTrusted(buf, 0, length, TS);
        }

        void dispatch(final long sessionId, final int length) {
            demuxer.dispatch(sessionId, buf, 0, length, TS);
        }

        void markNow() {
            mark = sink.events().size();
        }

        List<AssetsEngineEvent> events() {
            return sink.events().subList(mark, sink.events().size());
        }

        long count(final Class<? extends AssetsEngineEvent> type) {
            return events().stream().filter(type::isInstance).count();
        }

        <T extends AssetsEngineEvent> T first(final Class<T> type) {
            for (final AssetsEngineEvent e : events()) {
                if (type.isInstance(e)) {
                    return type.cast(e);
                }
            }
            throw new AssertionError("no " + type.getSimpleName() + " in " + events());
        }
    }

    private static CoreConfigCommand v1(final long corr) {
        return AssetsScenarioRunner.coreConfigFor(1, 1, TICK, MIN, MAX, LOT, corr);
    }

    private static void assertAdoptedFromTheWire(final Rig r, final CoreConfigCommand sent) {
        assertEquals(0, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertEquals("70 is served, not counted as unhandled", 0, r.demuxer.unhandledTemplateCount());
        final CoreConfigCommand got = r.engine.coreConfig().recorded();
        assertNotNull("the frame reached applyCoreConfig", got);
        assertEquals(sent.configVersion, got.configVersion);
        assertEquals(sent.behaviorVersion, got.behaviorVersion);
        assertArrayEquals("markets group decoded field by field", sent.markets, got.markets);
        assertArrayEquals("assets group decoded field by field", sent.assets, got.assets);
        final AssetsEngineEvent.AdminAck ack = r.first(AssetsEngineEvent.AdminAck.class);
        assertEquals(70, ack.refTemplateId());
        assertEquals(AdminResult.ADOPTED, ack.result());
        assertEquals(sent.correlationId, ack.correlationId());
        assertEquals(1, ack.configVersion());
        assertEquals("the compiled tables match: no halt", 0, r.failFastCalls);
    }

    @Test
    public void trustedCoreConfigFrameDecodesBothGroupsAndAdopts() {
        final Rig r = new Rig();
        final CoreConfigCommand sent = v1(41);
        r.markNow();
        r.dispatchTrusted(r.coreConfig(sent));
        assertAdoptedFromTheWire(r, sent);
    }

    @Test
    public void adminClassSessionCoreConfigReachesApplyAndAdopts() {
        final Rig r = new Rig();
        final long admin = r.open(61, CorePrincipal.anyUser(SessionClass.ADMIN, 2));
        final CoreConfigCommand sent = v1(42);
        r.markNow();
        r.dispatch(admin, r.coreConfig(sent));
        assertEquals(0, r.demuxer.unauthorisedCount());
        assertAdoptedFromTheWire(r, sent);
    }

    @Test
    public void gatewayClassSessionCoreConfigIsRefusedUnauthorizedClassAndNothingIsRecorded() {
        final Rig r = new Rig();
        final long gateway = r.open(10, CorePrincipal.anyUser(SessionClass.GATEWAY, 1));
        r.markNow();
        r.dispatch(gateway, r.register(1, SessionClass.GATEWAY));
        assertEquals("the gateway is registered, so the refusal is the class mask",
                AdminResult.ACCEPTED, r.first(AssetsEngineEvent.AdminAck.class).result());
        r.markNow();
        final CoreConfigCommand sent = v1(43);
        r.dispatch(gateway, r.coreConfig(sent));
        final AssetsEngineEvent.OrderReject rej = r.first(AssetsEngineEvent.OrderReject.class);
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
        assertEquals(70, rej.refTemplateId());
        assertEquals(43, rej.correlationId());
        assertEquals(0, r.count(AssetsEngineEvent.AdminAck.class));
        assertEquals(1, r.demuxer.unauthorisedCount());
        assertNull(r.engine.coreConfig().recorded());
        assertEquals("never a fence event", 0, r.demuxer.applyErrorCount());
        // the refusal consumed nothing: the same frame from an ADMIN session is the first ADOPT
        final long admin = r.open(61, CorePrincipal.anyUser(SessionClass.ADMIN, 2));
        r.markNow();
        r.dispatch(admin, r.coreConfig(sent));
        assertAdoptedFromTheWire(r, sent);
    }

    @Test
    public void symbolWithANonPrintableByteIsADecodeRejectNeverAnApply() {
        final Rig r = new Rig();
        final int len = r.coreConfig(v1(44));
        final int symbol = HEADER + CoreConfigEncoder.BLOCK_LENGTH + CoreConfigEncoder.MarketsEncoder.HEADER_SIZE
                + CoreConfigEncoder.MarketsEncoder.symbolEncodingOffset();
        assertEquals('B', r.buf.getByte(symbol));
        r.buf.putByte(symbol + 1, (byte) 0x07); // "B\u0007C-USD": a control byte inside the symbol
        r.markNow();
        r.dispatchTrusted(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertNull("never an apply", r.engine.coreConfig().recorded());
        assertTrue("no ack, no ConfigApplied: " + r.events(), r.events().isEmpty());
        r.dispatchTrusted(r.coreConfig(v1(45))); // the healthy engine continues
        assertNotNull(r.engine.coreConfig().recorded());
    }

    @Test
    public void marketsGroupCountBeyondTheFrameIsADecodeReject() {
        final Rig r = new Rig();
        final int len = r.coreConfig(v1(46));
        r.buf.putShort(HEADER + CoreConfigEncoder.BLOCK_LENGTH + 2, (short) 0xFFFF, ByteOrder.LITTLE_ENDIAN);
        r.markNow();
        r.dispatchTrusted(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertNull(r.engine.coreConfig().recorded());
        assertTrue(r.events().isEmpty());
    }

    @Test
    public void assetsGroupCountBeyondTheFrameIsADecodeReject() {
        final Rig r = new Rig();
        final CoreConfigCommand sent = v1(47);
        final int len = r.coreConfig(sent);
        final int assetsHeader = HEADER + CoreConfigEncoder.BLOCK_LENGTH + CoreConfigEncoder.MarketsEncoder.HEADER_SIZE
                + sent.markets.length * CoreConfigEncoder.MarketsEncoder.sbeBlockLength();
        assertEquals(sent.assets.length, r.buf.getShort(assetsHeader + 2, ByteOrder.LITTLE_ENDIAN));
        r.buf.putShort(assetsHeader + 2, (short) 0xFFFF, ByteOrder.LITTLE_ENDIAN);
        r.markNow();
        r.dispatchTrusted(len);
        assertEquals(1, r.demuxer.decodeRejectCount());
        assertEquals(0, r.demuxer.applyErrorCount());
        assertNull(r.engine.coreConfig().recorded());
        assertTrue(r.events().isEmpty());
    }
}

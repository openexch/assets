// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.auth.CorePrincipal;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.SetMoneyJournalEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S3 (K6, RED-F17..F20 at the demuxer boundary): every ingress frame is checked against its
 * session's authority BEFORE any apply: class mask per template, user scope for user-scoped templates,
 * registration-first for the production classes (GATEWAY, FORWARDER, BRIDGE). A failure is a
 * deterministic ORIGIN OrderReject naming the template, counted, never a fence event.
 */
public class SessionAuthorityDemuxerTest {

    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final long TS = 1_000L;

    private static final class Rig implements AssetsSbeDemuxer.TransportHooks {
        final AssetsEngine engine = new AssetsEngine();
        final RecordingAssetsSink sink = new RecordingAssetsSink();
        final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[512]);
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final List<long[]> registered = new ArrayList<>(); // {sessionId, mask, resumable}
        final List<Long> rejectedSessions = new ArrayList<>();
        int mark;

        Rig() {
            engine.setEventSink(sink);
            demuxer.setTransportHooks(this);
        }

        public void onSessionRegistered(long sessionId, int channelMask, boolean resumable) {
            registered.add(new long[] {sessionId, channelMask, resumable ? 1 : 0});
        }

        public void onSessionRejected(long sessionId) {
            rejectedSessions.add(sessionId);
        }

        long open(long sessionId, CorePrincipal principal) {
            engine.sessions().open(sessionId, principal.encode());
            return sessionId;
        }

        void dispatch(long sessionId, int length) {
            demuxer.dispatch(sessionId, buf, 0, length, TS);
        }

        int deposit(long corr, long user, long amount) {
            final DepositEncoder e = new DepositEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(corr).userId(user).assetId(0).amount(amount).requestIdHigh(1).requestIdLow(corr);
            return HEADER + e.encodedLength();
        }

        int hold(long corr, long user, long order, long amount) {
            final HoldEncoder e = new HoldEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(corr).orderId(order).userId(user).assetId(0).amount(amount).omsManagedRelease(BoolFlag.FALSE);
            return HEADER + e.encodedLength();
        }

        int settle(long tradeId) {
            final SettleEncoder e = new SettleEncoder().wrapAndApplyHeader(buf, 0, header)
                    .tradeId(tradeId).marketId(1).takerOrderId(1).takerUserId(1).makerOrderId(2).makerUserId(2)
                    .price(100).quantity(100).takerIsBuy(BoolFlag.TRUE).journalPosition(0);
            return HEADER + e.encodedLength();
        }

        int setJournal(long corr) {
            final SetMoneyJournalEncoder e = new SetMoneyJournalEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(corr).enabled(BoolFlag.TRUE);
            return HEADER + e.encodedLength();
        }

        int queryFeed(long corr) {
            final QueryFeedPositionEncoder e = new QueryFeedPositionEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr);
            return HEADER + e.encodedLength();
        }

        int register(long corr, SessionClass cls, boolean resumable) {
            final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(corr).sessionClass(cls).readerKind((short) 0)
                    .resumable(resumable ? BoolFlag.TRUE : BoolFlag.FALSE).identity(0).sourceEpoch(0);
            e.channels().clear().acks(true).settlements(true);
            return HEADER + e.encodedLength();
        }

        int gate(long corr, int marketId, com.openexchange.assets.infrastructure.generated.GateState st,
                 com.openexchange.assets.infrastructure.generated.GateReason reason) {
            final OrderEntryGateEncoder e = new OrderEntryGateEncoder().wrapAndApplyHeader(buf, 0, header)
                    .correlationId(corr).marketId(marketId).state(st).reason(reason);
            return HEADER + e.encodedLength();
        }

        void markNow() {
            mark = sink.events().size();
        }

        List<AssetsEngineEvent> events() {
            return sink.events().subList(mark, sink.events().size());
        }

        AssetsEngineEvent.OrderReject onlyReject() {
            assertEquals("exactly one event: " + events(), 1, events().size());
            return (AssetsEngineEvent.OrderReject) events().get(0);
        }
    }

    @Test
    public void redF17_aTemplateOutsideTheSessionClassIsRejectedNotApplied() {
        final Rig r = new Rig();
        final long bridge = r.open(10, CorePrincipal.anyUser(SessionClass.BRIDGE, 1));
        r.dispatch(bridge, r.register(1, SessionClass.BRIDGE, true));
        r.markNow();
        r.dispatch(bridge, r.deposit(7, 1, 1_000)); // BRIDGE may not deposit
        final AssetsEngineEvent.OrderReject rej = r.onlyReject();
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, rej.reason());
        assertEquals(DepositEncoder.TEMPLATE_ID, rej.refTemplateId());
        assertEquals(7, rej.correlationId());
        assertEquals(0, r.engine.accountCount());
        assertEquals(1, r.demuxer.unauthorisedCount());
        assertEquals("never a fence event", 0, r.demuxer.applyErrorCount());

        r.markNow();
        final long treasury = r.open(11, CorePrincipal.anyUser(SessionClass.TREASURY, 2));
        r.dispatch(treasury, r.settle(1)); // TREASURY may not settle
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.onlyReject().reason());
        r.markNow();
        r.dispatch(treasury, r.setJournal(3)); // TREASURY may not touch the journal switch
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.onlyReject().reason());
        assertFalse(r.engine.isMoneyJournalEnabled());
        r.markNow();
        r.dispatch(treasury, r.deposit(8, 5, 1_000)); // ...but it may deposit
        assertEquals(1, r.engine.accountCount());
        assertTrue(r.events().get(0) instanceof AssetsEngineEvent.DepositAck);
    }

    @Test
    public void redF18_aUserOutsideTheSessionScopeIsRejected() {
        final Rig r = new Rig();
        final long scoped = r.open(20, CorePrincipal.forUsers(SessionClass.TREASURY, 4, 7));
        r.markNow();
        r.dispatch(scoped, r.deposit(1, 8, 1_000));
        final AssetsEngineEvent.OrderReject rej = r.onlyReject();
        assertEquals(CoreRejectReason.NOT_OWNER, rej.reason());
        assertEquals(8, rej.userId());
        assertEquals(0, r.engine.accountCount());
        r.markNow();
        r.dispatch(scoped, r.deposit(2, 7, 1_000));
        assertEquals(1, r.engine.accountCount());
        assertEquals(1_000, r.engine.account(7).available(0));
        assertEquals(1, r.demuxer.unauthorisedCount());
    }

    @Test
    public void redF19_productionClassesMustRegisterBeforeAnythingButAQuery() {
        final Rig r = new Rig();
        final long bridge = r.open(30, CorePrincipal.anyUser(SessionClass.BRIDGE, 1));
        r.markNow();
        r.dispatch(bridge, r.settle(1));
        assertEquals(CoreRejectReason.UNREGISTERED_SESSION, r.onlyReject().reason());
        assertEquals(0, r.engine.getLastAppliedTradeId());
        r.markNow();
        r.dispatch(bridge, r.queryFeed(5)); // a query is allowed before registration
        assertTrue(r.events().get(0) instanceof AssetsEngineEvent.FeedPositionReport);
        r.markNow();
        r.dispatch(bridge, r.register(6, SessionClass.BRIDGE, true));
        assertEquals(2, r.events().size());
        final AssetsEngineEvent.AdminAck ack = (AssetsEngineEvent.AdminAck) r.events().get(0);
        assertEquals(AdminResult.ACCEPTED, ack.result());
        assertEquals(80, ack.refTemplateId());
        final AssetsEngineEvent.AdmissionState adm = (AssetsEngineEvent.AdmissionState) r.events().get(1);
        assertEquals(GateState.SUSPENDED, adm.gateState());
        assertEquals(GateReason.GENESIS, adm.gateReason());
        assertTrue(adm.toOrigin());
        assertEquals(1, r.registered.size());
        assertEquals(30, r.registered.get(0)[0]);
        assertEquals(1, r.registered.get(0)[2]);
        // ADMIN and TOOL are exempt from registration-first (operators and the in-process harness).
        final long tool = r.open(31, CorePrincipal.anyUser(SessionClass.TOOL, 2));
        r.markNow();
        r.dispatch(tool, r.deposit(9, 1, 50));
        assertTrue(r.events().get(0) instanceof AssetsEngineEvent.DepositAck);
    }

    @Test
    public void redF20_aClassMismatchOrANonResumableRegistrationIsRejectedAndTheSessionClosed() {
        final Rig r = new Rig();
        final long bridge = r.open(40, CorePrincipal.anyUser(SessionClass.BRIDGE, 1));
        r.markNow();
        r.dispatch(bridge, r.register(1, SessionClass.GATEWAY, true));
        AssetsEngineEvent.AdminAck ack = (AssetsEngineEvent.AdminAck) r.events().get(0);
        assertEquals(AdminResult.REJECTED, ack.result());
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS.code(), ack.reason());
        assertEquals(List.of(40L), r.rejectedSessions);
        assertFalse(r.engine.sessions().get(40).isRegistered());
        r.markNow();
        r.dispatch(bridge, r.register(2, SessionClass.BRIDGE, false));
        ack = (AssetsEngineEvent.AdminAck) r.events().get(0);
        assertEquals(AdminResult.REJECTED, ack.result());
        assertEquals(CoreRejectReason.NOT_RESUMABLE.code(), ack.reason());
        assertEquals(2, r.rejectedSessions.size());
        // re-registering with the same declaration is a DUPLICATE, not a change
        r.dispatch(bridge, r.register(3, SessionClass.BRIDGE, true));
        r.markNow();
        r.dispatch(bridge, r.register(4, SessionClass.BRIDGE, true));
        assertEquals(AdminResult.DUPLICATE, ((AssetsEngineEvent.AdminAck) r.events().get(0)).result());
        assertEquals("the leader still re-applies the mask on a duplicate", 2, r.registered.size());
    }

    @Test
    public void aSessionWithoutAPrincipalAuthorisesNothing() {
        final Rig r = new Rig();
        r.engine.sessions().open(50, new byte[0]);
        r.markNow();
        r.dispatch(50, r.deposit(1, 1, 10));
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.onlyReject().reason());
        r.markNow();
        r.dispatch(50, r.register(2, SessionClass.TOOL, true));
        assertEquals(AdminResult.REJECTED, ((AssetsEngineEvent.AdminAck) r.events().get(0)).result());
        r.markNow();
        r.dispatch(99, r.deposit(3, 1, 10)); // a session this registry never saw
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.onlyReject().reason());
        assertEquals(0, r.engine.accountCount());
    }

    @Test
    public void theGateIsAdminOnlyAndItsStateIsBroadcast() {
        final Rig r = new Rig();
        final long tool = r.open(60, CorePrincipal.anyUser(SessionClass.TOOL, 1));
        final long admin = r.open(61, CorePrincipal.anyUser(SessionClass.ADMIN, 2));
        assertFalse("genesis: suspended", r.engine.gate().admits(1));
        r.markNow();
        r.dispatch(tool, r.gate(1, OrderEntryGate.GLOBAL, com.openexchange.assets.infrastructure.generated.GateState.OPEN,
                com.openexchange.assets.infrastructure.generated.GateReason.ADMIN));
        assertEquals(CoreRejectReason.UNAUTHORIZED_CLASS, r.onlyReject().reason());
        assertFalse(r.engine.gate().admits(1));
        r.markNow();
        r.dispatch(admin, r.gate(2, OrderEntryGate.GLOBAL, com.openexchange.assets.infrastructure.generated.GateState.OPEN,
                com.openexchange.assets.infrastructure.generated.GateReason.ADMIN));
        assertEquals(AdminResult.ACCEPTED, ((AssetsEngineEvent.AdminAck) r.events().get(0)).result());
        final AssetsEngineEvent.AdmissionState adm = (AssetsEngineEvent.AdmissionState) r.events().get(1);
        assertEquals(GateState.OPEN, adm.gateState());
        assertFalse(adm.toOrigin());
        assertTrue(r.engine.gate().admits(1));
        r.markNow();
        r.dispatch(admin, r.gate(3, OrderEntryGate.GLOBAL, com.openexchange.assets.infrastructure.generated.GateState.OPEN,
                com.openexchange.assets.infrastructure.generated.GateReason.ADMIN));
        assertEquals(AdminResult.DUPLICATE, ((AssetsEngineEvent.AdminAck) r.events().get(0)).result());
        assertEquals("no AdmissionState on a duplicate", 1, r.events().size());
        r.markNow();
        r.dispatch(admin, r.gate(4, 2, com.openexchange.assets.infrastructure.generated.GateState.SUSPENDED,
                com.openexchange.assets.infrastructure.generated.GateReason.BRIDGE_HALT));
        assertTrue(r.engine.gate().admits(1));
        assertFalse(r.engine.gate().admits(2));
        r.markNow();
        r.dispatch(admin, r.gate(5, 9, com.openexchange.assets.infrastructure.generated.GateState.OPEN,
                com.openexchange.assets.infrastructure.generated.GateReason.ADMIN));
        assertEquals(AdminResult.REJECTED, ((AssetsEngineEvent.AdminAck) r.events().get(0)).result());
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.session.SessionAuthority;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.ConfigDecision;
import com.openexchange.assets.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.assets.infrastructure.generated.MarketTickEncoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.util.List;

import static com.openexchange.assets.application.engine.feed.V3Rig.EPOCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 section 10 (D8a, D8b; M4): a forwarder may register before the ME epoch is bound (no crash loop), a mismatching
 * pre-bind registration is revoked and closed at the bind, a mismatching post-bind registration is REJECTED.
 */
public class ForwarderEpochTest {

    private static final int T80 = 80;

    private static int register(final V3Rig r, final SessionClass cls, final long sourceEpoch) {
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .correlationId(8_000 + sourceEpoch % 100).sessionClass(cls).readerKind((short) 0).resumable(BoolFlag.TRUE)
                .identity(0).sourceEpoch(sourceEpoch);
        final EgressChannelsEncoder ch = e.channels().clear();
        ch.acks(true).control(true).dispatch(true);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int fresh(final V3Rig r) {
        final MeEngineConfigEncoder e = new MeEngineConfigEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .egressSeq(1).journalSeq(1).meEpoch(EPOCH).configVersion(1).behaviorVersion(2).ledgerCapacity(4096)
                .maxWireVersion(12).decision(ConfigDecision.FRESH);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int tick(final V3Rig r) {
        final MarketTickEncoder e = new MarketTickEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .marketId(1).bookVersion(5).bestBid(1).bestAsk(2).lastTradePrice(0).lastTradeId(0).meEpoch(EPOCH);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static AdminResult registerResult(final V3Rig r, final long session, final SessionClass cls, final long epoch) {
        r.mark();
        r.dispatch(session, register(r, cls, epoch));
        for (final AssetsEngineEvent e : r.events()) {
            if (e instanceof AssetsEngineEvent.AdminAck a && a.refTemplateId() == T80) {
                return a.result();
            }
        }
        throw new AssertionError("no RegisterSession AdminAck in " + r.events());
    }

    @Test
    public void forwarderRegistersBeforeEpochBound() {
        final V3Rig r = V3Rig.unbound();
        final long wrong = r.openSession(11, SessionClass.FORWARDER);
        final long right = r.openSession(12, SessionClass.FORWARDER);
        final long bridge = r.openSession(13, SessionClass.BRIDGE);
        final long zero = r.openSession(14, SessionClass.FORWARDER);
        // pre-bind: every registration follows the registry rules, the epoch is only recorded
        assertEquals("D8a B = 0, e != 0", AdminResult.ACCEPTED, registerResult(r, wrong, SessionClass.FORWARDER, 0xBAD));
        assertEquals(AdminResult.ACCEPTED, registerResult(r, right, SessionClass.FORWARDER, EPOCH));
        assertEquals("D8a e = 0", AdminResult.ACCEPTED, registerResult(r, bridge, SessionClass.BRIDGE, 0));
        assertEquals(AdminResult.ACCEPTED, registerResult(r, zero, SessionClass.FORWARDER, 0));
        assertTrue(r.rejectedSessions.isEmpty());

        // the bind (68 FRESH from the registered bridge) revokes every FORWARDER/BRIDGE registration with e not in {0, B}
        r.mark();
        r.dispatch(bridge, fresh(r));
        assertEquals("bound", EPOCH, r.feed().boundMeEpoch());
        final SessionAuthority w = r.engine.sessions().get(wrong);
        assertFalse("D8b: revoked", w.isRegistered());
        assertEquals("D8b: channelMask 0", 0, w.channelMask());
        assertEquals(List.of(wrong), r.rejectedSessions);
        assertEquals(1L, r.feed().epochRevokedCount());
        assertEquals("no AdminAck for a revocation", 0, r.count(AssetsEngineEvent.AdminAck.class));
        assertTrue("the matching registration stays", r.engine.sessions().get(right).isRegistered());
        assertTrue("sourceEpoch 0 stays", r.engine.sessions().get(zero).isRegistered());
        assertTrue(r.engine.sessions().get(bridge).isRegistered());

        // the revoked session's later frames are UNREGISTERED_SESSION
        r.mark();
        r.dispatch(wrong, tick(r));
        assertEquals(CoreRejectReason.UNREGISTERED_SESSION, r.first(AssetsEngineEvent.OrderReject.class).reason());

        // post-bind: a mismatching registration is REJECTED INVALID and closed; a matching one is ACCEPTED
        final long late = r.openSession(15, SessionClass.FORWARDER);
        r.mark();
        r.dispatch(late, register(r, SessionClass.FORWARDER, 0xBAD));
        final AssetsEngineEvent.AdminAck ack = r.onlyAck();
        assertEquals(AdminResult.REJECTED, ack.result());
        assertEquals(CoreRejectReason.INVALID.code(), ack.reason());
        assertEquals("REJECTED closes the session", List.of(wrong, late), r.rejectedSessions);
        assertFalse("the registry was not called", r.engine.sessions().get(late).isRegistered());
        final long good = r.openSession(16, SessionClass.FORWARDER);
        assertEquals(AdminResult.ACCEPTED, registerResult(r, good, SessionClass.FORWARDER, EPOCH));
    }
}

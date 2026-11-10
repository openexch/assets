// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.gate.OrderEntryGate;
import com.openexchange.core.application.engine.session.SessionAuthority;
import com.openexchange.core.auth.CoreSessionClasses;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The order-entry gate (section 5) and the session authority table (section 9) ride the v4
 * snapshot, so a member restored from a snapshot admits and authorises exactly like one that replayed.
 */
public class GateAndSessionSnapshotTest {

    private static byte[] bytes(final CoreEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    private static CoreEngine restore(final byte[] image) {
        final CoreEngine e = new CoreEngine();
        e.setEventSink(new RecordingCoreSink());
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(
                new org.agrona.concurrent.UnsafeBuffer(image), 0, image.length, e);
        assertEquals(image.length, d.bytesConsumed);
        return e;
    }

    @Test
    public void gatesSurviveRestoreAndGenesisIsSuspended() {
        final CoreEngine a = new CoreEngine();
        a.setEventSink(new RecordingCoreSink());
        assertEquals(GateState.SUSPENDED, a.gate().state(OrderEntryGate.GLOBAL));
        assertEquals(GateReason.GENESIS, a.gate().reason(OrderEntryGate.GLOBAL));
        assertFalse(a.gate().admits(1));
        a.gate().set(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        a.gate().set(3, GateState.SUSPENDED, GateReason.ME_CAPACITY);
        final CoreEngine b = restore(bytes(a));
        assertTrue(b.gate().admits(1));
        assertFalse(b.gate().admits(3));
        assertEquals(GateReason.ME_CAPACITY, b.gate().reason(3));
        assertArrayEquals("bytes are a function of state", bytes(a), bytes(b));
    }

    @Test
    public void sessionAuthoritiesSurviveRestoreInSessionIdOrder() {
        final CoreEngine a = new CoreEngine();
        a.setEventSink(new RecordingCoreSink());
        a.sessions().open(30, ClusterPrincipal.anyUser(CoreSessionClasses.toKit(SessionClass.BRIDGE), 3).encode());
        a.sessions().open(10, ClusterPrincipal.forUsers(CoreSessionClasses.toKit(SessionClass.TREASURY), 4, 7, 9).encode());
        // A restored row needs a principal; the class-0 row is the refusal below
        a.sessions().open(20, ClusterPrincipal.anyUser(CoreSessionClasses.toKit(SessionClass.GATEWAY), 2).encode());
        a.sessions().register(30, SessionClass.BRIDGE, 0b101, true, 2, 77, 5);
        final byte[] image = bytes(a);
        final CoreEngine b = restore(image);
        assertEquals(List.of(10L, 20L, 30L), b.sessions().sessionIds());
        final SessionAuthority bridge = b.sessions().get(30);
        assertTrue(bridge.isRegistered());
        assertTrue(bridge.isResumable());
        assertEquals(0b101, bridge.channelMask());
        assertEquals(2, bridge.readerKind());
        assertEquals(77, bridge.identity());
        assertEquals(5, bridge.sourceEpoch());
        assertEquals(SessionClass.BRIDGE, bridge.sessionClass());
        final SessionAuthority treasury = b.sessions().get(10);
        assertFalse(treasury.isRegistered());
        assertTrue(treasury.allowsUser(9));
        assertFalse(treasury.allowsUser(8));
        assertEquals(SessionClass.GATEWAY, b.sessions().get(20).sessionClass());
        assertNull(b.sessions().get(40));
        assertArrayEquals(image, bytes(b));
        // a closed session leaves the table and the bytes
        b.sessions().close(20);
        assertEquals(List.of(10L, 30L), b.sessions().sessionIds());

        // (R0, restored session): a row whose principal is unreadable is history from before session principals and refused
        final CoreEngine c = new CoreEngine();
        c.setEventSink(new RecordingCoreSink());
        c.sessions().open(20, new byte[0]);
        final byte[] principalLess = bytes(c);
        final IllegalStateException refused = org.junit.Assert.assertThrows(IllegalStateException.class,
                () -> restore(principalLess));
        assertTrue(refused.getMessage(), refused.getMessage().startsWith(
                "CRITICAL: principal-less session in log: history from before session principals, clean genesis required (restored session, "
                        + "clusterSessionId=20"));
    }

    @Test
    public void theSpliceRunnerStillHoldsWithGateAndSessionSections() {
        // No DSL verbs touch gates or sessions yet; the sections must simply ride every cut unchanged.
        com.openexchange.cluster.testkit.SnapshotSpliceRunner.run(CoreReplica::new, List.of(
                "CLOCK 1000", "DEPOSIT u=1 asset=0 amt=10.0", "HOLD order=1 u=1 asset=0 amt=4.0", "RELEASE order=1 u=1"));
    }
}

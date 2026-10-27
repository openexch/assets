// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.engine.session.SessionAuthority;
import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S3: the order-entry gate (section 5) and the session authority table (section 9) ride the v4
 * snapshot, so a member restored from a snapshot admits and authorises exactly like one that replayed.
 */
public class GateAndSessionSnapshotTest {

    private static byte[] bytes(final AssetsEngine e) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(e, buf);
        final byte[] out = new byte[len];
        buf.getBytes(0, out);
        return out;
    }

    private static AssetsEngine restore(final byte[] image) {
        final AssetsEngine e = new AssetsEngine();
        e.setEventSink(new RecordingAssetsSink());
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(
                new org.agrona.concurrent.UnsafeBuffer(image), 0, image.length, e);
        assertEquals(image.length, d.bytesConsumed);
        return e;
    }

    @Test
    public void gatesSurviveRestoreAndGenesisIsSuspended() {
        final AssetsEngine a = new AssetsEngine();
        a.setEventSink(new RecordingAssetsSink());
        assertEquals(GateState.SUSPENDED, a.gate().state(OrderEntryGate.GLOBAL));
        assertEquals(GateReason.GENESIS, a.gate().reason(OrderEntryGate.GLOBAL));
        assertFalse(a.gate().admits(1));
        a.gate().set(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        a.gate().set(3, GateState.SUSPENDED, GateReason.ME_CAPACITY);
        final AssetsEngine b = restore(bytes(a));
        assertTrue(b.gate().admits(1));
        assertFalse(b.gate().admits(3));
        assertEquals(GateReason.ME_CAPACITY, b.gate().reason(3));
        assertArrayEquals("bytes are a function of state", bytes(a), bytes(b));
    }

    @Test
    public void sessionAuthoritiesSurviveRestoreInSessionIdOrder() {
        final AssetsEngine a = new AssetsEngine();
        a.setEventSink(new RecordingAssetsSink());
        a.sessions().open(30, ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(SessionClass.BRIDGE), 3).encode());
        a.sessions().open(10, ClusterPrincipal.forUsers(AssetsSessionClasses.toKit(SessionClass.TREASURY), 4, 7, 9).encode());
        a.sessions().open(20, new byte[0]); // a session whose principal was unreadable
        a.sessions().register(30, SessionClass.BRIDGE, 0b101, true, 2, 77, 5);
        final byte[] image = bytes(a);
        final AssetsEngine b = restore(image);
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
        assertEquals(SessionClass.UNSET, b.sessions().get(20).sessionClass());
        assertNull(b.sessions().get(40));
        assertArrayEquals(image, bytes(b));
        // a closed session leaves the table and the bytes
        b.sessions().close(20);
        assertEquals(List.of(10L, 30L), b.sessions().sessionIds());
    }

    @Test
    public void theSpliceRunnerStillHoldsWithGateAndSessionSections() {
        // No DSL verbs touch gates or sessions yet; the sections must simply ride every cut unchanged.
        com.openexchange.cluster.testkit.SnapshotSpliceRunner.run(AssetsReplica::new, List.of(
                "CLOCK 1000", "DEPOSIT u=1 asset=0 amt=10.0", "HOLD order=1 u=1 asset=0 amt=4.0", "RELEASE order=1 u=1"));
    }
}

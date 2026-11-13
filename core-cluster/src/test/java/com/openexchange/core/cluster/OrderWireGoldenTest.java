// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.application.engine.order.RequestTable;
import com.openexchange.core.infrastructure.generated.CancelOrderDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.loadgen.CoreOrderLoadGenerator;
import com.openexchange.core.loadgen.OrderWire;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * The order load generator's frames are the rigs' frames: CreateOrder, Deposit and the gate OPEN byte-equal to
 * {@link CoreRestoreRig.Encoders} for the same fields, RegisterSession byte-equal to {@link TestAuth}'s, CancelOrder
 * decoded field by field; and the generator's user-count rule uses the core's requestId table constants.
 */
public class OrderWireGoldenTest {

    private static byte[] bytes(final UnsafeBuffer b, final int len) {
        return Arrays.copyOf(b.byteArray(), len);
    }

    @Test
    public void theGeneratorEncodesWhatTheRigsEncode() {
        final CoreRestoreRig.Encoders rig = new CoreRestoreRig.Encoders();
        final OrderWire wire = new OrderWire();
        final UnsafeBuffer mine = new UnsafeBuffer(new byte[512]);

        final int rigCreate = rig.limit(42, 7, 9_001, 3, true, 49_950 * ShadowClusterRig.ONE, ShadowClusterRig.ONE / 10,
                TimeInForce.GTC, 0);
        final byte[] rigCreateBytes = bytes(rig.buf, rigCreate);
        final int myCreate = wire.limit(mine, 42, 7, 0, 0, 0x100L, 9_001, 3, true, 49_950 * ShadowClusterRig.ONE,
                ShadowClusterRig.ONE / 10, TimeInForce.GTC, 0);
        assertArrayEquals("CreateOrder", rigCreateBytes, bytes(mine, myCreate));

        final int rigDeposit = rig.deposit(5, 11, CoreRestoreRig.usd(), 1_000 * ShadowClusterRig.ONE);
        final byte[] rigDepositBytes = bytes(rig.buf, rigDeposit);
        final int myDeposit = wire.deposit(mine, 5, 11, CoreRestoreRig.usd(), 1_000 * ShadowClusterRig.ONE, 0, 0);
        assertArrayEquals("Deposit", rigDepositBytes, bytes(mine, myDeposit));

        final int rigGate = rig.gateOpen(0xAD1L);
        final byte[] rigGateBytes = bytes(rig.buf, rigGate);
        final int myGate = wire.gateOpen(mine, 0xAD1L);
        assertArrayEquals("OrderEntryGate OPEN", rigGateBytes, bytes(mine, myGate));

        final UnsafeBuffer ta = new UnsafeBuffer(new byte[128]);
        final int taLen = TestAuth.encodeRegisterSession(ta, 0x8E6L, SessionClass.GATEWAY, true, true, false, false,
                false, false, false, false);
        final int myReg = wire.registerSession(mine, 0x8E6L, SessionClass.GATEWAY, false, false);
        assertArrayEquals("RegisterSession", bytes(ta, taLen), bytes(mine, myReg));

        final int myCancel = wire.cancel(mine, 43, 7, 0, 0x77L, 9_002, 0x77L, 9_001);
        final CancelOrderDecoder c = new CancelOrderDecoder().wrapAndApplyHeader(mine, 0,
                new MessageHeaderDecoder().wrap(mine, 0));
        assertEquals(myCancel, MessageHeaderDecoder.ENCODED_LENGTH + c.encodedLength());
        assertEquals(43L, c.correlationId());
        assertEquals(7L, c.userId());
        assertEquals(0L, c.orderId());
        assertEquals(0x77L, c.requestIdHigh());
        assertEquals(9_002L, c.requestIdLow());
        assertEquals(0x77L, c.origClientOrderIdHigh());
        assertEquals(9_001L, c.origClientOrderIdLow());
    }

    @Test
    public void theUserCountRuleUsesTheCoreRequestTable() {
        assertEquals(RequestTable.TTL_MS, CoreOrderLoadGenerator.REQUEST_TTL_MS);
        assertEquals(RequestTable.PER_USER_CAP, CoreOrderLoadGenerator.REQUESTS_PER_USER_CAP);
    }
}

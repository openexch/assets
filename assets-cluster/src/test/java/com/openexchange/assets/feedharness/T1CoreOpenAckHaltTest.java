// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.generated.AdminAckEncoder;
import com.openexchange.assets.infrastructure.generated.AdminResult;
import com.openexchange.assets.infrastructure.generated.GateReason;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * RC2 S17 W12 (G1, A11): {@code t1-core open [market] [--ack-halt]}. The default OPEN names ADMIN; {@code --ack-halt}
 * names BRIDGE_HALT, the operator's acknowledgement of a halt holder; the ack printer names 45 HALT_HOLDER_PENDING.
 */
public class T1CoreOpenAckHaltTest {

    private static GateReason reasonOf(final byte[] frame) {
        return new OrderEntryGateDecoder().wrapAndApplyHeader(new UnsafeBuffer(frame), 0, new MessageHeaderDecoder())
                .reason();
    }

    @Test
    public void theDefaultOpenNamesAdminAndAckHaltNamesBridgeHalt() {
        assertEquals(GateReason.ADMIN, reasonOf(new CoreFrames().gate(1, 0, true)));
        assertEquals(GateReason.ADMIN, reasonOf(new CoreFrames().gate(1, 0, true, GateReason.ADMIN)));
        assertEquals(GateReason.BRIDGE_HALT, reasonOf(new CoreFrames().gate(1, 0, true, GateReason.BRIDGE_HALT)));
    }

    @Test
    public void theOpenArgumentsParseMarketAndAckHalt() {
        assertEquals(0, T1Core.openMarket(new String[] {"t1-core", "open"}));
        assertEquals(GateReason.ADMIN, T1Core.openReason(new String[] {"t1-core", "open"}));
        assertEquals(3, T1Core.openMarket(new String[] {"t1-core", "open", "3"}));
        assertEquals(GateReason.BRIDGE_HALT, T1Core.openReason(new String[] {"t1-core", "open", "--ack-halt"}));
        assertEquals(0, T1Core.openMarket(new String[] {"t1-core", "open", "--ack-halt"}));
        assertEquals(2, T1Core.openMarket(new String[] {"t1-core", "open", "2", "--ack-halt"}));
        assertEquals(GateReason.BRIDGE_HALT, T1Core.openReason(new String[] {"t1-core", "open", "2", "--ack-halt"}));
    }

    @Test
    public void theAckPrinterNamesHaltHolderPending() {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[64]);
        final AdminAckEncoder e = new AdminAckEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder());
        e.correlationId(1).refTemplateId(73).result(AdminResult.REJECTED).reason((short) 45);
        final byte[] f = new byte[MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength()];
        b.getBytes(0, f);
        assertEquals("ADMINACK ref=73 result=REJECTED reason=HALT_HOLDER_PENDING", T1Core.ack(f));
    }
}

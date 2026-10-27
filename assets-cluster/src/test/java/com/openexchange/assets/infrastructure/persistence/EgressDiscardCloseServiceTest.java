// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import io.aeron.Publication;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Review FIX-2 (RC2 S8 3.13): {@code assets_egress_discard_closes_total} through a real queue lifecycle on the service: a
 * resumable session that had a delivery gets NOT_CONNECTED, its queue closes it (3.13), the container then closes the session
 * and the queue is retired; the series stays counted and only grows.
 */
public class EgressDiscardCloseServiceTest {

    @Test
    public void theDiscardCloseOutlivesTheRetiredQueue() {
        final ServiceEgressRig r = new ServiceEgressRig().v3();
        r.deposit(1);
        final ServiceEgressRig.RecSession fwd = r.registered(91, SessionClass.FORWARDER); // resumable, AdminAck delivered
        final ServiceEgressRig.RecSession gw = r.registered(92, SessionClass.GATEWAY);
        assertEquals("nothing yet", 0L, r.series("assets_egress_discard_closes_total"));
        fwd.failWith = Publication.NOT_CONNECTED;
        r.create(gw, 1); // the BROADCAST 104 meets NOT_CONNECTED after a delivery
        assertTrue("3.13 closed the session", fwd.closed);
        assertEquals(1L, r.series("assets_egress_discard_closes_total"));
        r.containerClose(fwd); // onSessionClose: the queue is dropped
        r.create(gw, 1);       // the session array is rebuilt without it
        assertEquals("still counted after the queue retired", 1L, r.series("assets_egress_discard_closes_total"));
        // a second resumable session, MAX_POSITION_EXCEEDED this time: the total grows
        final ServiceEgressRig.RecSession fwd2 = r.registered(93, SessionClass.FORWARDER);
        fwd2.failWith = Publication.MAX_POSITION_EXCEEDED;
        r.create(gw, 1);
        assertTrue(fwd2.closed);
        r.containerClose(fwd2);
        r.create(gw, 1);
        assertEquals("monotone", 2L, r.series("assets_egress_discard_closes_total"));
    }
}

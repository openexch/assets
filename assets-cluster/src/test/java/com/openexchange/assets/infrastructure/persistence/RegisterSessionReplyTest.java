// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S8 7 G11 (S12 13.1 A-1 test 4), guards: RegisterSession is answered AdminAck then the ORIGIN AdmissionState; a new
 * leader's queue lacks the dispatch channel until the forwarder re-registers; a duplicate registration re-applies the
 * mask (the T1 dependency, {@code AssetsSbeDemuxer} RegisterSession handler).
 */
public class RegisterSessionReplyTest {

    @Test
    public void adminAckThenOriginAdmission() {
        final ServiceEgressRig r = new ServiceEgressRig().v3();
        final ServiceEgressRig.RecSession fwd = r.registered(61, SessionClass.FORWARDER);
        final List<Integer> t = fwd.templates();
        assertEquals("AdminAck then AdmissionState", List.of(AdminAckDecoder.TEMPLATE_ID, AdmissionStateDecoder.TEMPLATE_ID), t);
    }

    @Test
    public void newLeaderMaskLacksDispatchUntilReRegistration() {
        final ServiceEgressRig r = new ServiceEgressRig().v3();
        r.deposit(1);
        final ServiceEgressRig.RecSession fwd = r.registered(71, SessionClass.FORWARDER);
        final ServiceEgressRig.RecSession gw = r.registered(72, SessionClass.GATEWAY);
        r.create(gw, 1);
        assertEquals("the registered leader delivers the dispatch", List.of("D1"), fwd.dispatchStream());
        // a leadership change: this node's transport state (the queues and their masks) is gone; the new term's queue
        // starts from the default mask, which carries no dispatch channel
        r.service.onRoleChange(Cluster.Role.FOLLOWER);
        ApplierParityRig.inject(r.service, "egressQueues", new com.openexchange.assets.infrastructure.publisher.SessionEgressQueue[0]);
        ((java.util.Map<?, ?>) ApplierParityRig.read(r.service, AssetsClusteredService.class, "egressBySessionId")).clear();
        r.service.onRoleChange(Cluster.Role.LEADER);
        fwd.delivered.clear();
        r.create(gw, 1);
        assertTrue("no dispatch before re-registration", fwd.dispatchStream().isEmpty());
        r.send(fwd, r.enc.register(r.nextCorr(), SessionClass.FORWARDER));
        fwd.delivered.clear();
        r.create(gw, 1);
        assertEquals("delivered after re-registration", List.of("D3"), fwd.dispatchStream());
    }

    @Test
    public void duplicateReappliesMask() {
        final ServiceEgressRig r = new ServiceEgressRig().v3();
        r.deposit(1);
        final ServiceEgressRig.RecSession fwd = r.registered(81, SessionClass.FORWARDER);
        final ServiceEgressRig.RecSession gw = r.registered(82, SessionClass.GATEWAY);
        ApplierParityRig.inject(r.service, "egressQueues", new com.openexchange.assets.infrastructure.publisher.SessionEgressQueue[0]);
        ((java.util.Map<?, ?>) ApplierParityRig.read(r.service, AssetsClusteredService.class, "egressBySessionId")).clear();
        fwd.delivered.clear();
        r.send(fwd, r.enc.register(r.nextCorr(), SessionClass.FORWARDER)); // the same declaration again: DUPLICATE
        final AdminAckDecoder ack = new AdminAckDecoder().wrapAndApplyHeader(new org.agrona.concurrent.UnsafeBuffer(fwd.delivered.get(0)), 0,
                new com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder().wrap(new org.agrona.concurrent.UnsafeBuffer(fwd.delivered.get(0)), 0));
        assertEquals("a duplicate registration", com.openexchange.assets.infrastructure.generated.AdminResult.DUPLICATE, ack.result());
        fwd.delivered.clear();
        r.create(gw, 1);
        assertEquals("the duplicate re-applied the dispatch mask on the fresh queue", List.of("D1"), fwd.dispatchStream());
    }
}

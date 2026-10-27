// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.order.CoreOrder;
import com.openexchange.assets.application.engine.order.OrderEngine;
import com.openexchange.assets.application.engine.order.ReleaseBacklog;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S8 7 G11 (S12 13.1 A-1 tests 1 and 2), against {@link AssetsClusteredService} with real egress queues: the reply of
 * a RequestPendingDispatch is the last {@code count} entries before its PendingDispatchEnd in the forwarder's queue, also
 * when 105 coalescing merges live and reply entries and when a back-pressured batch parks; a query apply mints nothing.
 */
public class PendingReplyContiguityTest {

    private static List<String> pending(final OrderEngine oe, final long from) {
        final TreeSet<Long> s = new TreeSet<>();
        oe.orders().forEachByOrderId(o -> {
            if (o.has(CoreOrder.F_DISPATCH_PENDING) && o.pendingDispatchSeq > from) {
                s.add(o.pendingDispatchSeq);
            }
        });
        oe.backlog().forEachByOrderId(b -> {
            if (b.stage != ReleaseBacklog.STAGE_PARKED && b.dispatchSeq > from) {
                s.add(b.dispatchSeq);
            }
        });
        final List<String> out = new ArrayList<>();
        for (final long seq : s) {
            out.add("D" + seq);
        }
        return out;
    }

    /** At every end frame, the {@code count} entries right before it are exactly {@code expected[i]}. */
    private static void assertReplyBlocks(final List<String> stream, final List<List<String>> expected) {
        int q = 0;
        for (int i = 0; i < stream.size(); i++) {
            if (stream.get(i).startsWith("E")) {
                final int count = Integer.parseInt(stream.get(i).substring(1, stream.get(i).indexOf('/')));
                final List<String> exp = expected.get(q++);
                assertEquals("query " + q + ": count", exp.size(), count);
                assertEquals("query " + q + ": the reply block is the last " + count + " entries before its end",
                        exp, stream.subList(i - count, i));
            }
        }
        assertEquals("every query answered", expected.size(), q);
    }

    @Test
    public void replyBlockIsLastBeforeEnd() {
        final ServiceEgressRig r = new ServiceEgressRig().v3();
        r.deposit(1);
        r.deposit(2);
        final ServiceEgressRig.RecSession fwd = r.registered(31, SessionClass.FORWARDER);
        final ServiceEgressRig.RecSession gw = r.registered(32, SessionClass.GATEWAY);
        final ServiceEgressRig.RecSession bridge = r.registered(33, SessionClass.BRIDGE);
        fwd.delivered.clear();
        final List<List<String>> expected = new ArrayList<>();
        // (1) plain interleave: live creates, a query, more live creates and acks (UP_TO releases), a second query
        r.create(gw, 1);
        r.create(gw, 2);
        expected.add(pending(r.engine().orders(), 0));
        r.send(fwd, r.enc.pendingDispatch(r.nextCorr(), 0, 4096));
        r.create(gw, 1);
        r.ack(bridge, 1, 1, 101);
        expected.add(pending(r.engine().orders(), 1));
        r.send(fwd, r.enc.pendingDispatch(r.nextCorr(), 1, 4096));
        r.create(gw, 2);
        // (2) coalescing: the forwarder back-pressures while live 104s and then a whole reply queue up; one drain merges them
        fwd.accepts = 0;
        for (int i = 0; i < 5; i++) {
            r.create(gw, 1 + (i & 1));
        }
        expected.add(pending(r.engine().orders(), 0));
        r.send(fwd, r.enc.pendingDispatch(r.nextCorr(), 0, 4096));
        // (3) a parked batch: the merged batch is refused once, parks, then leaves whole
        fwd.accepts = 0;
        r.create(gw, 1);
        fwd.accepts = Integer.MAX_VALUE;
        r.create(gw, 2); // this apply's drain delivers the parked frames, in order
        expected.add(pending(r.engine().orders(), 3));
        r.send(fwd, r.enc.pendingDispatch(r.nextCorr(), 3, 4096));
        final List<String> stream = fwd.dispatchStream();
        assertReplyBlocks(stream, expected);
        assertTrue("105 coalescing merged entries", fwd.sawBatch());
    }

    @Test
    public void queryApplyMintsNothing() {
        final ServiceEgressRig r = new ServiceEgressRig().v3();
        r.deposit(1);
        final ServiceEgressRig.RecSession fwd = r.registered(41, SessionClass.FORWARDER);
        final ServiceEgressRig.RecSession other = r.registered(42, SessionClass.FORWARDER);
        final ServiceEgressRig.RecSession gw = r.registered(43, SessionClass.GATEWAY);
        final ServiceEgressRig.RecSession bridge = r.registered(44, SessionClass.BRIDGE);
        r.create(gw, 1);
        r.ack(bridge, 1, 1, 101);
        r.create(gw, 1);
        final long head = r.engine().getDispatchSeq();
        final int otherBefore = other.delivered.size();
        final int gwBefore = gw.delivered.size();
        fwd.delivered.clear();
        r.send(fwd, r.enc.pendingDispatch(r.nextCorr(), 0, 4096));
        r.send(fwd, r.enc.pendingDispatch(r.nextCorr(), 0, 0)); // the head probe
        assertEquals("the 65 apply (endApply included) leaves dispatchSeq", head, r.engine().getDispatchSeq());
        assertEquals("no BROADCAST to another forwarder", otherBefore, other.delivered.size());
        assertEquals("nor to the gateway", gwBefore, gw.delivered.size());
        final List<String> stream = fwd.dispatchStream();
        assertEquals("the asker gets its reply and its ends only", List.of("D2", "D3", "E2/" + head, "E0/" + head), stream);
    }

    static com.openexchange.assets.domain.commands.DepositCommand deposit(final long user) {
        final var d = new com.openexchange.assets.domain.commands.DepositCommand();
        d.reset();
        d.setUserId(user);
        d.setAssetId(0);
        d.setAmount(1_000_000_00000000L);
        return d;
    }
}

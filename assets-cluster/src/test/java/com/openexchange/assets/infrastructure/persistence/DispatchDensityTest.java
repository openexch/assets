// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S8 7 G11 (S12 13.1 A-1 test 3): every dispatchSeq is BROADCAST once, in order, per apply: the 104 seqs a registered
 * forwarder receives from one apply equal {@code (old, new dispatchSeq]} in ascending emission order, with releases,
 * orphan cancels and deferred UP_TO flushes in the mix (through the service and its real queue). The per-apply density
 * over seeded v3 fuzz runs with every fault verb is asserted by {@code V3CoreFuzzDriver} after every apply.
 */
public class DispatchDensityTest {

    @Test
    public void everyDispatchSeqIsBroadcastOnceInOrder() {
        final ServiceEgressRig r = new ServiceEgressRig().v3();
        r.deposit(1);
        r.deposit(2);
        final ServiceEgressRig.RecSession fwd = r.registered(51, SessionClass.FORWARDER);
        final ServiceEgressRig.RecSession gw = r.registered(52, SessionClass.GATEWAY);
        final ServiceEgressRig.RecSession bridge = r.registered(53, SessionClass.BRIDGE);
        final List<Runnable> applies = new ArrayList<>();
        applies.add(() -> r.create(gw, 1));                                                     // CREATE 1
        applies.add(() -> r.create(gw, 2));                                                     // CREATE 2
        applies.add(() -> r.ack(bridge, 1, 1, 101));                                            // M6 deferred flush
        applies.add(() -> r.send(gw, r.enc.cancel(r.nextCorr(), 2, 2, 0xCC, r.nextCorr())));   // flag only (pending create)
        applies.add(() -> r.ack(bridge, 2, 2, 102));                                            // afterAck cancel, then M6 flush
        applies.add(() -> r.send(gw, r.enc.cancel(r.nextCorr(), 1, 1, 0xCC, r.nextCorr())));   // CANCEL 1
        applies.add(() -> r.send(bridge, r.enc.terminal(++r.egress, 101, 1, 1, 3, r.now, 1, ++r.jseq))); // M11 ORDER for 1
        applies.add(() -> r.send(bridge, r.enc.release(++r.egress, ++r.jseq, r.enc.meEpoch, 1, 7, 1, 2, 0))); // LEG_LIVE: orphan
        applies.add(() -> r.send(bridge, r.enc.release(++r.egress, ++r.jseq, r.enc.meEpoch, 2, 0, 0, 0, 0))); // C2: no mint
        long total = 0;
        for (int i = 0; i < applies.size(); i++) {
            final long before = r.engine().getDispatchSeq();
            fwd.delivered.clear();
            applies.get(i).run();
            final long after = r.engine().getDispatchSeq();
            final List<String> got = fwd.dispatchStream();
            final List<String> want = new ArrayList<>();
            for (long s = before + 1; s <= after; s++) {
                want.add("D" + s);
            }
            assertEquals("apply " + i + ": the BROADCAST seqs are (old, new] in order", want, got);
            total += got.size();
        }
        assertEquals("every seq once", r.engine().getDispatchSeq(), total);
        assertTrue("a release, an orphan cancel and a flush were in the mix", r.engine().orders().orphanCancelCount() == 1
                && r.engine().orders().releaseMintedOrderCount() == 1 && r.engine().orders().releaseMintedUpToCount() == 2);
    }
}

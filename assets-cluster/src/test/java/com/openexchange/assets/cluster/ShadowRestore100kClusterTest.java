// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.infrastructure.generated.TimeInForce;
import com.openexchange.assets.infrastructure.persistence.ServiceProbe;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 contract 11.2, the 100k leg (nightly, {@code -Drcs.nightly=true}; D-S4-11: the single-node
 * {@code CoreRestoreRig} shape, cold restart on the same directories). 100k resting orders in stub mode, then one
 * restart per path (TAIL, SKIP, EQUAL), each {@code readyMs <= RCS_READY_BUDGET_MS} (3.5, default 30000, the RC0
 * budget; a failed budget is a failed test, never a relaxed default). The per-path numbers are the S10 input.
 *
 * <p>The TAIL row (review F3): the tail range holds 50k deposits to existing accounts (events, no new engine state);
 * {@code egressBySessionId} is sampled every 10k tail frames and must stay 0, and the used heap after the tail must be
 * within 10% of the heap at its first frame ({@code TailEgressSink}).</p>
 */
public class ShadowRestore100kClusterTest {

    private static final int ORDERS = 100_000;
    private static final int USERS = ORDERS / OrderLoad.ORDERS_PER_USER;
    private static final int TAIL_DEPOSITS = 50_000;
    private static final int SAMPLE_EVERY = 10_000;
    private static final int IMAGE_CAPACITY = 256 << 20;

    @Before
    public void nightlyOnly() {
        Assume.assumeTrue("the 100k leg runs only with -Drcs.nightly=true", Boolean.getBoolean("rcs.nightly"));
    }

    /** What one sampled TAIL restart observed on the service thread. */
    private static final class TailSample {
        final List<long[]> queues = new CopyOnWriteArrayList<>(); // {frame, egressBySessionId.size()}
        final AtomicLong heapBefore = new AtomicLong(-1);
        final AtomicLong heapAfter = new AtomicLong(-1);

        long maxQueues() {
            long max = 0;
            for (final long[] q : queues) {
                max = Math.max(max, q[1]);
            }
            return max;
        }
    }

    private static long usedHeap() {
        System.gc();
        final Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    @Test(timeout = 3_600_000)
    public void oneHundredThousandOrdersRestoreOnEveryPathWithinTheBudget() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(25100, "rcs-s4-11.2-100k", 1, "16m", 0)) {
            final Member m = rig.members.get(0);
            m.settings = StubStory.shadow();
            final long budget = m.settings.readyBudgetMs();
            rig.boot(m);
            rig.awaitLeader(null);
            final OrderLoad load = OrderLoad.prepare(rig, m, USERS);
            load.orders(rig, 1, ORDERS, TimeInForce.GTC, 0L);
            final long pS = m.cfg.consensusModuleContext().commitPositionCounter().get();
            load.depositsToExisting(rig, TAIL_DEPOSITS, USERS);
            final long pCm = rig.snapshotAll();
            load.depositsToExisting(rig, 1_000, USERS);
            final RecordedLog log = rig.captureCommitted(m);
            final long[] pos = log.positions();
            final int is = Arrays.binarySearch(pos, pS);
            final int cm = Arrays.binarySearch(pos, pCm);
            assertTrue("P_s " + pS + " and P_cm " + pCm + " are frame ends", is >= 0 && cm >= 0 && cm + 500 < pos.length);
            long entries = 0; // frames the tail hands to applier entries (actions and term events are not entries)
            for (final com.openexchange.cluster.testkit.LogFrame fr : log.frames()) {
                if (fr.position() > pS && fr.position() <= pCm
                        && fr.kind() != com.openexchange.cluster.log.FrameKind.CLUSTER_ACTION
                        && fr.kind() != com.openexchange.cluster.log.FrameKind.NEW_LEADERSHIP_TERM) {
                    entries++;
                }
            }
            final long tailFrames = entries;
            assertTrue("the tail holds the deposits: " + tailFrames, tailFrames >= TAIL_DEPOSITS);
            final long pGt = pos[cm + 500];
            System.out.println("[11.2-100k] orders=" + ORDERS + " P_s=" + pS + " P_cm=" + pCm + " P_s>P_cm=" + pGt
                    + " tailFrames=" + tailFrames + " frames=" + pos.length);

            // ---- TAIL
            produce(rig, m, log, pS);
            rig.forgetAll();
            final TailSample sample = new TailSample();
            m.beforeLaunch = svc -> ServiceProbe.tailObserver(svc, frame -> {
                if (frame == 1) {
                    sample.heapBefore.set(usedHeap());
                }
                if (frame % SAMPLE_EVERY == 0) {
                    sample.queues.add(new long[] {frame, ServiceProbe.egressQueues(svc)});
                }
                if (frame == tailFrames) {
                    sample.heapAfter.set(usedHeap());
                }
            });
            final long tailMs = restart(rig, m, "TAIL P_s=" + pS, budget);
            assertEquals("frames sampled", tailFrames / SAMPLE_EVERY, sample.queues.size());
            if (sample.maxQueues() > 0) {
                throw new AssertionError("egress queue grew to " + sample.maxQueues() + " during TAIL: " + render(sample));
            }
            final long before = sample.heapBefore.get();
            final long after = sample.heapAfter.get();
            assertTrue("heap sampled at both ends: " + before + " " + after, before > 0 && after > 0);
            assertTrue("used heap after the tail " + after + " within 10% of before " + before,
                    after <= before + before / 10);
            System.out.println("[11.2-100k] TAIL egressBySessionId samples " + render(sample) + " heapBefore=" + before
                    + " heapAfter=" + after + " discarded=" + ServiceProbe.rcs(m.service, "rcs_tail_events_discarded_total"));

            m.beforeLaunch = null;

            // ---- SKIP, then EQUAL
            produce(rig, m, log, pGt);
            final long skipMs = restart(rig, m, "SKIP P_s=" + pGt, budget);
            produce(rig, m, log, pCm);
            final long equalMs = restart(rig, m, "EQUAL P_s=" + pCm, budget);
            System.out.println("GREEN 11.2-100k readyMs TAIL=" + tailMs + " SKIP=" + skipMs + " EQUAL=" + equalMs
                    + " budget=" + budget + " (RCS_READY_BUDGET_MS)");
        }
    }

    private static String render(final TailSample s) {
        final List<String> out = new ArrayList<>();
        for (final long[] q : s.queues) {
            out.add(q[0] + ":" + q[1]);
        }
        return out.toString();
    }

    private static void produce(final ShadowClusterRig rig, final Member m, final RecordedLog log, final long p) {
        final Written w = rig.producer(m).capacity(IMAGE_CAPACITY).produce(new Who(ProducerKind.FOLLOWER, (int)(p % 10_000) + 700),
                log, p);
        assertTrue(w.toString(), w.ok());
        System.out.println("[11.2-100k] shadow " + w);
    }

    /**
     * The restored node's image equals the oracle {@code S(p)} at the engine's own log position {@code p} (read twice
     * around two equal image reads: the leader's sweep timer keeps the image moving every 100 ms while request ids live,
     * so equality is asserted at a position, not at rest).
     */
    private static void assertImageAtItsPosition(final ShadowClusterRig rig, final Member m, final String path) {
        final long deadline = System.currentTimeMillis() + 120_000;
        while (true) {
            final long p1 = m.service.engine().getLogPosition();
            final byte[] a = ServiceProbe.image(m.service);
            final byte[] b = ServiceProbe.image(m.service);
            final long p2 = m.service.engine().getLogPosition();
            if (p1 == p2 && p1 > 0 && Arrays.equals(a, b)) {
                final RecordedLog now = rig.captureCommitted(m);
                if (Arrays.binarySearch(now.positions(), p1) >= 0) {
                    final byte[] oracle = TestShadowProducer.imageAt(now, p1);
                    if (!Arrays.equals(oracle, a)) {
                        throw new AssertionError(path + ": final image differs from baseline at " + p1 + " (sizes "
                                + a.length + " vs " + oracle.length + ")");
                    }
                    System.out.println("[11.2-100k] " + path + ": image == S(" + p1 + ") (" + a.length + " B)");
                    return;
                }
            }
            assertTrue(path + ": no stable image read within 120 s", System.currentTimeMillis() < deadline);
        }
    }

    private static long restart(final ShadowClusterRig rig, final Member m, final String path, final long budgetMs) {
        rig.kill(m);
        rig.tee.clear();
        rig.errTee.clear();
        rig.restart(m);
        final long readyMs = rig.awaitReady(m, 300_000);
        assertTrue(m.halts.toString(), m.halts.isEmpty());
        RcsDryRun.requirePathLine(rig.tee, path, "11.2 100k");
        assertImageAtItsPosition(rig, m, path);
        System.out.println("[11.2-100k] " + path + " readyMs=" + readyMs + " " + rig.tee.all("restored path="));
        assertTrue(path + ": readyMs " + readyMs + " <= RCS_READY_BUDGET_MS " + budgetMs, readyMs <= budgetMs);
        return readyMs;
    }
}

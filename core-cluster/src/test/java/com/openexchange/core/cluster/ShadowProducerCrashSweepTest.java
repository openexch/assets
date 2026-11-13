// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A crashed write of the member's own producer is swept, and the restarted writer never reuses its seq (two members,
 * embedded drivers, nightly). The designated follower writes its first ALIGNED (seq 1); a trailer-less
 * {@code F<id>} recording with seq 2 (the shape a {@code kill -9} after the first chunk leaves, written through the kit
 * writer's crash seam) is planted in its archive; the member restarts on the same directories: its writer is armed with
 * the next seq above the crashed one (the seq counts header-bearing and alias-only crashed copies), the position from
 * usable copies only, and the kit's sweep purges the orphan through retention within two housekeeping scans. The next
 * ALIGNED carries seq 3 and is not refused NOT_MONOTONE.
 *
 * <p>Control {@code -Dcore.producer.nc=SWEEP_OFF} must fail with {@code a crashed write of this producer survived two
 * scans}.</p>
 */
public class ShadowProducerCrashSweepTest {

    private static final int PORT_BASE = 63000;

    @Test
    public void aCrashedWriteOfThisProducerIsSweptAndItsSeqNeverReused() throws Exception {
        final boolean sweepOff = "SWEEP_OFF".equals(System.getProperty("core.producer.nc", ""));
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "producer-crash-sweep", 2, "1m", 0)) {
            for (final Member m : rig.members) {
                m.settings = ShadowProducerRoleTest.producerSettings();
                if (sweepOff) {
                    m.beforeLaunch = svc -> ServiceProbe.producerFault(svc, "sweepOff", true);
                }
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member d = rig.members.get(ShadowProducerRoleTest.designated(rig, leader.id));
            final Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            // identity: a loaded runner can elect right after a snapshot, which drops the deposits not yet committed
            final Client tool = rig.connect(SessionClass.TOOL, leader).withIdentity();
            tool.deposits(100);
            ShadowProducerRoleTest.awaitArmed(rig);
            final long p1 = rig.snapshotAll();
            rig.await(() -> ShadowProducerRoleTest.written(rig, d, p1) != null, 60_000, "the first ALIGNED");
            assertEquals("the first ALIGNED is seq 1", 1, ShadowProducerRoleTest.written(rig, d, p1).header()
                    .producerSeq());

            // the crash: a trailer-less F<d> seq 2 at a later committed position
            tool.deposits(30);
            final RecordedLog log = rig.captureCommitted(leader);
            final long pc = log.positions()[log.positions().length - 1];
            final TestShadowProducer.Written crashed = rig.producer(d).produceCrashed(
                    new TestShadowProducer.Who(ProducerKind.FOLLOWER, d.id), log, pc, 2);
            final ShadowEntry orphan = rig.scan(d).entry(crashed.recordingId());
            assertTrue("the orphan is trailer-less: " + orphan, orphan != null
                    && (orphan.state() == ShadowEntry.State.NO_TRAILER || orphan.state() == ShadowEntry.State.TOO_SHORT));

            // restart on the same directories: armed above the crashed seq, the orphan swept
            rig.forgetAll();
            rig.kill(d);
            rig.restart(d);
            rig.awaitReady(d, 120_000);
            ShadowProducerRoleTest.awaitArmed(rig);
            final long swept0 = System.nanoTime();
            try {
                rig.await(() -> ServiceProbe.shadowCounter(d.service, ShadowCounters.Counter.CRASHED_WRITES_PURGED_TOTAL)
                        == 1 && rig.scan(d).entry(crashed.recordingId()) == null, 2 * 5_000 + 10_000, "swept");
            } catch (final AssertionError notSwept) {
                throw new AssertionError("a crashed write of this producer survived two scans: rec="
                        + crashed.recordingId() + " purged_total="
                        + ServiceProbe.shadowCounter(d.service, ShadowCounters.Counter.CRASHED_WRITES_PURGED_TOTAL));
            }
            final long sweptMs = (System.nanoTime() - swept0) / 1_000_000;
            final Client tool2 = rig.connect(SessionClass.TOOL, rig.awaitLeader(null)).withIdentity();
            tool2.deposits(20);
            final Member leaderNow = rig.awaitLeader(null);
            final Member dNow = rig.members.get(ShadowProducerRoleTest.designated(rig, leaderNow.id));
            final long p2 = rig.snapshotAll();
            rig.await(() -> ShadowProducerRoleTest.written(rig, dNow, p2) != null, 60_000, "the next ALIGNED");
            if (dNow.id == d.id) {
                final long seq = ShadowProducerRoleTest.written(rig, d, p2).header().producerSeq();
                assertTrue("next seq " + seq + " > crashed seq 2", seq > 2);
                assertEquals("NOT_MONOTONE not hit", 0, ServiceProbe.shadowCounter(d.service,
                        ShadowCounters.Counter.WRITE_SKIPPED_NOT_MONOTONE_TOTAL));
            }
            assertEquals("the sweep purges nothing more", 1, ServiceProbe.shadowCounter(d.service,
                    ShadowCounters.Counter.CRASHED_WRITES_PURGED_TOTAL));
            System.out.println("GREEN crash sweep: member " + d.id + " swept rec=" + crashed.recordingId() + " (seq 2) in "
                    + sweptMs + " ms after armed; next ALIGNED at " + p2 + " by member " + dNow.id);
        }
    }
}

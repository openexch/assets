// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.shadow.Digests;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Who produces, on a real cluster in {@code shadow} mode (embedded drivers, the real ClusterAction, the real readiness
 * and the real {@code CommitPosition} the leader sends): at every P_cm exactly the designated follower (the next member
 * id after the leader, wrapping) has an {@code F<id>} ALIGNED at exactly P_cm in its own archive whose payload digest is
 * the oracle image's {@code S(P_cm)}; the leader and the other follower write nothing; every serialize is one written
 * copy; after a leader change the new designated produces at the next P_cm; one node reads designated at a time.
 *
 * <p>(b), three members: the designated follower restarted mid-term into an EQUAL shadow restore, with no leader
 * change. Its log never replays the term's {@code NewLeadershipTermEvent}, so the log alone cannot name the leader; the
 * leader's commit message does, and the next P_cm has an F copy without an election.</p>
 *
 * <p>Variants: {@code -Dcore.producer.members} 2 or 3 (default 2, the PR gate; the nightly job runs 3, which adds (b)
 * and the leader mismatch row), {@code -Dcore.producer.100k=true} (100 000 accounts before the first snapshot).
 * Negative controls ({@code -Dcore.producer.nc=<seam>}): {@code DESIGNATION_OFF}, {@code PRODUCER_ON_LEADER},
 * {@code PRODUCE_OUTSIDE_CALLBACK}, {@code LEADER_FROM_LOG_ONLY}; each makes the test fail with its text.</p>
 */
public class ShadowProducerRoleTest {

    private static final int PORT_BASE = 61000;

    static int members() {
        final int n = Integer.getInteger("core.producer.members", 2);
        if (n < 2 || n > 3) {
            throw new IllegalArgumentException("the test refuses: core.producer.members must be 2..3: " + n);
        }
        return n;
    }

    static String control() {
        return System.getProperty("core.producer.nc", "");
    }

    /** Shadow mode with the real producer and a small image buffer (several members share one JVM). */
    static com.openexchange.core.infrastructure.persistence.SnapshotModeSettings producerSettings(final String... extra) {
        final String[] base = {"SHADOW_PRODUCER", "designated-follower", "SHADOW_PRODUCER_BUFFER_MB", "16",
                "SHADOW_SCAN_PERIOD_MS", "500", "SHADOW_DEEP_READ_PACE_MS", "0"};
        final String[] all = Arrays.copyOf(base, base.length + extra.length);
        System.arraycopy(extra, 0, all, base.length, extra.length);
        return ShadowClusterRig.shadowSettings(all);
    }

    /** The designated member for a term led by {@code leader}: the next id, wrapping. */
    static int designated(final ShadowClusterRig rig, final int leader) {
        return (leader + 1) % rig.members.size();
    }

    /** Applies the named negative-control seam to every member before its boot. */
    static void applyControl(final Member m) {
        final String nc = control();
        if (nc.isEmpty()) {
            return;
        }
        final String seam = switch (nc) {
            case "DESIGNATION_OFF" -> "designationOff";
            case "PRODUCER_ON_LEADER" -> "producerOnLeader";
            case "PRODUCE_OUTSIDE_CALLBACK" -> "produceOutsideCallback";
            case "LEADER_FROM_LOG_ONLY" -> "leaderFromLogOnly";
            default -> throw new IllegalArgumentException("the test refuses: unknown control " + nc);
        };
        final java.util.function.Consumer<com.openexchange.core.infrastructure.persistence.CoreClusteredService> prior =
                m.beforeLaunch;
        m.beforeLaunch = s -> {
            if (prior != null) {
                prior.accept(s);
            }
            ServiceProbe.producerFault(s, seam, true);
        };
    }

    @Test
    public void exactlyTheDesignatedFollowerWritesAnAlignedAtEveryCommittedSnapshot() throws Exception {
        final int n = members();
        final boolean big = Boolean.getBoolean("core.producer.100k");
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + n * 100 + (big ? 500 : 0), "producer-role-" + n
                + (big ? "-100k" : ""), n, "1m", 0)) {
            for (final Member m : rig.members) {
                m.settings = producerSettings();
                applyControl(m);
            }
            rig.bootAll();
            Member leader = rig.awaitLeader(null);
            final Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            // identity: a loaded runner can elect right after a snapshot, which drops the deposits not yet committed
            final Client tool = rig.connect(SessionClass.TOOL, leader).withIdentity();
            if (big) {
                accounts(rig, tool, 100_000);
            }
            tool.deposits(200);
            awaitArmed(rig);

            for (int s = 0; s < 3; s++) {
                tool.deposits(50);
                // a loaded runner can elect between snapshots: the designation follows the leader of this term
                leader = rig.awaitLeader(null);
                final long[] before = writes(rig);
                final long p = rig.snapshotAll();
                assertProducedBy(rig, leader, p, before);
            }
            System.out.println("GREEN role: 3 snapshots, each written by member " + designated(rig, leader.id)
                    + " only (leader " + leader.id + ")");

            // a leader change: the new leader's designated produces at the next P_cm
            final Member old = leader;
            rig.forgetAll();
            rig.kill(old);
            if (n == 2) {
                rig.restart(old); // two members: no quorum until it is back
            }
            leader = rig.awaitLeader(n == 2 ? null : old);
            final Client tool2 = rig.connect(SessionClass.TOOL, leader).withIdentity();
            if (n > 2) {
                rig.restart(old);
            }
            rig.awaitReady(old, 120_000);
            awaitArmed(rig);
            for (int s = 0; s < 2; s++) {
                tool2.deposits(50);
                leader = rig.awaitLeader(null);
                final long[] before = writes(rig);
                final long p = rig.snapshotAll();
                assertProducedBy(rig, leader, p, before);
            }
            System.out.println("GREEN role after a leader change: leader " + leader.id + ", designated member "
                    + designated(rig, leader.id));

            if (n >= 3) {
                // the mismatch row first: it needs a node that saw the term frame (the restart below never does)
                leaderMismatch(rig, leader, tool2);
                midTermRestart(rig, leader, tool2);
            }
        }
    }

    /** {@code shadow_write_total{kind=ALIGNED}} of every member (0 for a killed one), indexed by member id. */
    static long[] writes(final ShadowClusterRig rig) {
        final long[] out = new long[rig.members.size()];
        for (final Member m : rig.live()) {
            out[m.id] = ServiceProbe.shadowWriteTotal(m.service, ShadowKind.ALIGNED);
        }
        return out;
    }

    /** {@code n} accounts (one deposit each), so the image and its shadow are several MB. */
    static void accounts(final ShadowClusterRig rig, final Client tool, final int n) {
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        long last = 0;
        for (int i = 0; i < n; i++) {
            last = tool.corr++;
            tool.offer(enc.buf, enc.deposit(last, 1_000 + i, com.openexchange.core.domain.Asset.USD.id(),
                    ShadowClusterRig.ONE));
        }
        final long target = last;
        rig.await(() -> tool.in.lastDepositAckCorr >= target, 600_000, "100k account deposits");
    }

    /** Every live member armed its writer (the first housekeeping wake after the ADOPT with a scan). */
    static void awaitArmed(final ShadowClusterRig rig) {
        for (final Member m : rig.live()) {
            rig.await(() -> ServiceProbe.producerArmed(m.service), 60_000, () -> "member " + m.id
                    + "'s writer was never armed");
        }
    }

    /**
     * At P_cm {@code p} under {@code leader}: the designated follower writes exactly one more ALIGNED, at exactly
     * {@code p}, {@code F<id>}, with the oracle's payload digest; no other node's writer moves.
     */
    static void assertProducedBy(final ShadowClusterRig rig, final Member leader, final long p, final long[] before) {
        final int d = designated(rig, leader.id);
        final Member dm = rig.members.get(d);
        rig.await(() -> written(rig, dm, p) != null, 60_000,
                () -> "shadow_write_total{kind=ALIGNED} == 1 on member " + d + " at P_cm=" + p + " (written "
                        + ServiceProbe.producerWritten(dm.service) + ", skipped leader_unknown "
                        + ServiceProbe.producerSkipped(dm.service, "leader_unknown") + ", not_ready "
                        + ServiceProbe.producerSkipped(dm.service, "not_ready") + ", unarmed "
                        + ServiceProbe.producerSkipped(dm.service, "unarmed") + ")");
        final ShadowEntry e = written(rig, dm, p);
        assertEquals("alias position == the snapshot position", p, e.alias().logPosition());
        assertEquals("producerKind FOLLOWER", ProducerKind.FOLLOWER.code(), e.header().producerKind());
        assertEquals("producerId == memberId", d, e.header().producerId());
        final RecordedLog log = rig.captureCommitted(leader);
        final byte[] oracle = TestShadowProducer.imageAt(log, p);
        if (Digests.of(oracle) != e.header().payloadDigest()) {
            throw new AssertionError("the producer's image differs from S(P_cm) at " + p);
        }
        for (final Member m : rig.live()) {
            if (m.id == d) {
                rig.await(() -> ServiceProbe.producerSerialized(m.service) == ServiceProbe.producerWritten(m.service),
                        10_000, () -> "serialize_total == written_total on member " + d + ": "
                                + ServiceProbe.producerSerialized(m.service) + " vs "
                                + ServiceProbe.producerWritten(m.service));
                if (ServiceProbe.producerSerializedOutsideCallback(m.service) != 0) {
                    throw new AssertionError("a shadow was serialized at depth 0");
                }
                continue;
            }
            final long w = ServiceProbe.shadowWriteTotal(m.service, ShadowKind.ALIGNED) - before[m.id];
            if (w != 0) {
                throw new AssertionError(m.id == leader.id ? "the leader produced an ALIGNED at " + p
                        : "a non-designated follower produced: member " + m.id + " at " + p);
            }
        }
        // exactly one node per TERM (contract 4.4): an election after this P_cm moves the designation with the leader,
        // so the expected node is the current leader's designated, not the one of the term that wrote at p
        final int[] seen = new int[] {-1, -1};
        rig.await(() -> {
            Member now = null;
            int ones = 0;
            for (final Member m : rig.live()) {
                ones += ServiceProbe.producerDesignated(m.service) == 1 ? 1 : 0;
                if (m.role() == io.aeron.cluster.service.Cluster.Role.LEADER) {
                    now = m;
                }
            }
            if (now == null) {
                return false;
            }
            seen[0] = now.id;
            seen[1] = ones;
            return ones == 1 && ServiceProbe.producerDesignated(rig.members.get(designated(rig, now.id)).service) == 1;
        }, 10_000, () -> "shadow_producer_designated reads 1 on exactly one node (member " + d + " under leader "
                + leader.id + "; now leader " + seen[0] + ", nodes reading 1: " + seen[1] + ")");
        if (seen[0] != leader.id) {
            System.out.println("[role] an election after P_cm=" + p + ": leader " + leader.id + " -> " + seen[0]
                    + ", the gauge moved to member " + designated(rig, seen[0]));
        }
    }

    /** The usable F copy of {@code m} at exactly {@code p} in its own archive, or null. */
    static ShadowEntry written(final ShadowClusterRig rig, final Member m, final long p) {
        final ShadowCatalog.Scan scan = rig.scan(m);
        for (final ShadowEntry e : scan.all()) {
            if (e.state().usable() && e.alias() != null && e.alias().logPosition() == p
                    && e.alias().producerKind() == ProducerKind.FOLLOWER.code() && e.alias().producerId() == m.id) {
                return e;
            }
        }
        return null;
    }

    /**
     * (b): the designated follower restarted mid-term into an EQUAL restore of its own F copy, no leader change: the
     * log never names the leader again (the term frame lies below the restore position), the commit wire does.
     */
    static void midTermRestart(final ShadowClusterRig rig, final Member leader, final Client tool) {
        final Member d = rig.members.get(designated(rig, leader.id));
        final long termBefore = ServiceProbe.leadershipTermId(leader.service);
        rig.tee.clear();
        rig.kill(d);
        rig.restart(d);
        rig.await(() -> !rig.tee.all("restored path=").isEmpty(), 120_000, "no restore path line after the restart");
        RestoreDrillDryRun.requirePathLine(rig.tee, "EQUAL", "own F copy");
        rig.awaitReady(d, 120_000);
        assertEquals("no leader change", termBefore, ServiceProbe.leadershipTermId(leader.service));
        final long[] logLeader = ServiceProbe.logTermLeader(d.service);
        assertEquals("the restarted node never saw the term frame (it lies below the restore position)", -1L,
                logLeader[1]);
        final long t0 = System.nanoTime();
        awaitArmed(rig);
        tool.deposits(50);
        final long p = rig.snapshotAll();
        rig.await(() -> written(rig, d, p) != null, 60_000, () -> "designation stayed unknown after a mid-term restart:"
                + " skipped leader_unknown at " + p + ", no F copy (leader_unknown "
                + ServiceProbe.producerSkipped(d.service, "leader_unknown") + ")");
        assertEquals("leader_unknown skips at that P_cm", 0, ServiceProbe.producerSkipped(d.service, "leader_unknown"));
        rig.await(() -> ServiceProbe.producerDesignated(d.service) == 1, 30_000,
                () -> "designated never read 1 after the restart: " + ServiceProbe.producerDesignated(d.service));
        final long designatedMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.println("GREEN mid-term restart: member " + d.id + " restored EQUAL, log term leader -1, designated"
                + " read 1 within " + designatedMs + " ms of ready (a snapshot in between), F copy at P_cm=" + p + " without an election");
    }

    /** A wrong wire id on the designated (the seam): leader_mismatch, no copy, one CRITICAL line. */
    static void leaderMismatch(final ShadowClusterRig rig, final Member leader, final Client tool) {
        if (!control().isEmpty()) {
            return;
        }
        final Member d = rig.members.get(designated(rig, leader.id));
        ServiceProbe.producerFault(d.service, "wireLeaderOverride", d.id);
        final long before = ServiceProbe.producerWritten(d.service);
        final CoreRestoreRig.Tee err = new CoreRestoreRig.Tee(System.err, "[SHADOW-PRODUCER]");
        final java.io.PrintStream prior = System.err;
        System.setErr(err);
        try {
            tool.deposits(20);
        final long p = rig.snapshotAll();
            rig.await(() -> ServiceProbe.producerSkipped(d.service, "leader_mismatch") >= 1, 30_000,
                    "skipped_total{leader_mismatch} 1");
            rig.pumpMs(2_000);
            assertEquals("no copy at " + p, before, ServiceProbe.producerWritten(d.service));
            assertNotNull(err.lines.toString(), StubStory.line(err.lines, "leader id disagrees: commit wire says "
                    + d.id));
            assertTrue(written(rig, d, p) == null);
        } finally {
            System.setErr(prior);
        }
        ServiceProbe.producerFault(d.service, "wireLeaderOverride", -1);
        // the next P_cm is produced again (recovery), so the restart below restores EQUAL from it
        tool.deposits(20);
        final long[] beforeNext = writes(rig);
        final long next = rig.snapshotAll();
        assertProducedBy(rig, leader, next, beforeNext);
        System.out.println("GREEN leader mismatch: member " + d.id + " skipped leader_mismatch, then produced at "
                + next);
    }
}

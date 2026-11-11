// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.domain.Asset;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.testkit.RecordedLog;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The member puller on a real cluster (every member in {@code shadow} mode, embedded drivers, real UDP archive control
 * channels and real replications): every member lists every other member's archive ({@code SHADOW_PULL_SOURCES}),
 * a genuine ALIGNED written into the leader's archive after the pullers started is pulled by every follower, verified
 * locally (alias and payload digest equal to the source's); a byte-flipped copy at a second position is refused for
 * its content on every follower and never pulled again in three periods; a follower whose shadows were wiped and whose
 * log was purged below P_cm restores at restart through the startup pull (3b): {@code restored path=EQUAL}, image
 * equal to the leader's, the time to ready recorded. Then the same follower with a source that answers nothing halts
 * with the pull named in the runbook text.
 *
 * <p>Variants: {@code -Dcore.pull.members} 2 or 3 (default 3, the nightly job's), {@code -Dcore.pull.100k=true}
 * (100 000 accounts before the snapshot, a shadow of several MB).</p>
 */
public class ShadowPullerClusterTest {

    private static final int PORT_BASE = 48000;
    private static final long PERIOD_MS = 1000;
    private static final int DEPOSITS_BEFORE = 900;
    private static final int DEPOSITS_AFTER = 120;

    static int members() {
        final int n = Integer.getInteger("core.pull.members", 3);
        if (n < 2 || n > 3) {
            throw new IllegalArgumentException("the test refuses: core.pull.members must be 2..3: " + n);
        }
        return n;
    }

    private static int nextProducerId = 300;

    private static Who who() {
        return new Who(ProducerKind.FOLLOWER, nextProducerId++);
    }

    @Test
    public void followersPullAVerifiedCopyRefuseACorruptOneAndAWipedMemberRestoresFromAPull() throws Exception {
        final int n = members();
        final boolean big = Boolean.getBoolean("core.pull.100k");
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + (big ? 500 : 0) + n * 100,
                "pull-cluster-" + n + (big ? "-100k" : ""), n, "64k", StubStory.SEGMENT)) {
            for (final Member m : rig.members) {
                final List<Member> others = new ArrayList<>(rig.members);
                others.remove(m);
                m.settings = ShadowClusterRig.shadowSettings("SHADOW_PULL_SOURCES",
                        rig.sources(others.toArray(new Member[0])), "SHADOW_PULL_PERIOD_MS", Long.toString(PERIOD_MS),
                        "SHADOW_PULL_RETRY_BACKOFF_MS", "500");
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, leader);
            if (big) {
                accounts(rig, tool, 100_000);
            }
            tool.deposits(DEPOSITS_BEFORE);
            final long pCm = rig.snapshotAll();
            tool.deposits(DEPOSITS_AFTER);
            rig.awaitImagesEqual(f, leader, 60_000);
            // every follower's puller runs (started by its first housekeeping wake after the ADOPT)
            for (final Member m : followers(rig, leader)) {
                rig.await(() -> ServiceProbe.puller(m.service) != null, 30_000,
                        "member " + m.id + "'s puller never started");
            }

            // a genuine ALIGNED at P_cm appears on the leader's archive after the pullers started (C2)
            final RecordedLog log = rig.captureCommitted(leader);
            StubStory.indexOf(log, pCm);
            final TestShadowProducer producer = rig.producer(leader);
            final Written good = producer.produce(who(), log, pCm);
            assertTrue(good.toString(), good.ok());
            final ShadowEntry source = rig.scan(leader).entry(good.recordingId());
            assertNotNull("the source copy is listed on the leader", source);
            final long t0 = System.nanoTime();
            for (final Member m : followers(rig, leader)) {
                rig.await(() -> ServiceProbe.pull(m.service, "shadow_pull_total") >= 1, 2 * PERIOD_MS + 30_000,
                        () -> "member " + m.id + " never pulled the ALIGNED at P_cm=" + pCm + ": "
                                + ServiceProbe.puller(m.service).counters().snapshot());
            }
            final long pulledMs = (System.nanoTime() - t0) / 1_000_000;
            for (final Member m : followers(rig, leader)) {
                assertEquals("member " + m.id + " pulled exactly once", 1, ServiceProbe.pull(m.service, "shadow_pull_total"));
                final ShadowEntry copy = copyOf(rig.scan(m), source);
                assertNotNull("member " + m.id + " holds a usable copy with the source's full alias "
                        + source.alias().format(), copy);
                assertEquals("member " + m.id + "'s copy has the source's payload digest",
                        source.header().payloadDigest(), copy.header().payloadDigest());
            }
            assertEquals("the leader never pulls", 0, ServiceProbe.pull(leader.service, "shadow_pull_total"));
            System.out.println("GREEN pull: " + (n - 1) + " follower(s) hold " + source.alias().format() + " in "
                    + pulledMs + " ms (period " + PERIOD_MS + " ms), " + source.stopPosition() + " B");

            // a corrupted copy at a second position (above P_cm): refused for its content, never pulled again
            final long p2 = log.positions()[StubStory.indexOf(log, pCm) + 40];
            final Written bad = producer.produce(who(), log, p2);
            assertTrue(bad.toString(), bad.ok());
            producer.flipPayloadByte(bad);
            for (final Member m : followers(rig, leader)) {
                rig.await(() -> ServiceProbe.pullRefused(m.service, "CORRUPT") >= 1, 2 * PERIOD_MS + 30_000,
                        () -> "member " + m.id + " never refused the corrupt copy at " + p2 + ": "
                                + ServiceProbe.puller(m.service).counters().snapshot());
            }
            final long[] started = new long[rig.members.size()];
            for (final Member m : followers(rig, leader)) {
                started[m.id] = ServiceProbe.pull(m.service, "shadow_pull_started_total");
            }
            rig.pumpMs(3 * PERIOD_MS + 500);
            for (final Member m : followers(rig, leader)) {
                // every member lists every peer: a peer's just-pulled copy of the corrupt source (same full alias,
                // its own recording) is listed there until its purge, so a member may refuse it a second time as a
                // second source recording. The rule is per source recording: each refused once, none re-pulled.
                final long corrupt = ServiceProbe.pullRefused(m.service, "CORRUPT");
                final java.util.Set<String> refusedSources = ServiceProbe.puller(m.service).contentRefused();
                assertTrue("member " + m.id + ": the corrupt source copy was refused: " + corrupt, corrupt >= 1);
                assertEquals("member " + m.id + ": one CORRUPT refusal per source recording " + refusedSources,
                        refusedSources.size(), corrupt);
                if (ServiceProbe.pull(m.service, "shadow_pull_started_total") != started[m.id]) {
                    throw new AssertionError("a corrupt source copy was pulled again: started "
                            + ServiceProbe.pull(m.service, "shadow_pull_started_total") + " on member " + m.id);
                }
                assertEquals("still one pull", 1, ServiceProbe.pull(m.service, "shadow_pull_total"));
            }
            System.out.println("GREEN refused: CORRUPT at " + p2 + " on every follower, not retried in 3 periods");

            // a wiped follower: its puller stopped, its shadows purged, its log purged below P_cm; the restart pulls
            ServiceProbe.puller(f.service).close();
            assertTrue("f's shadows were purged", rig.purgeShadows(f) >= 1);
            final long boundary = pCm / StubStory.SEGMENT * StubStory.SEGMENT;
            assertTrue("P_cm " + pCm + " lies past the first segment", boundary > 0);
            assertEquals("f's log purged to " + boundary, boundary, rig.purgeLogTo(f, boundary));
            rig.await(() -> rig.scan(f).all().isEmpty(), 10_000, "f still lists shadows after the purge");
            rig.forgetAll();
            rig.tee.clear();
            rig.errTee.clear();
            rig.kill(f);
            final long restartNs = System.nanoTime();
            rig.restart(f);
            // the service acks the consensus module when onStart returns, right after its restore path line
            rig.await(() -> !rig.tee.all("restored path=").isEmpty(), 180_000, "no restore path line after the restart");
            final long toAckMs = (System.nanoTime() - restartNs) / 1_000_000;
            final long readyMs = rig.awaitReady(f, 180_000);
            final long toReadyMs = (System.nanoTime() - restartNs) / 1_000_000;
            assertTrue(f.halts.toString(), f.halts.isEmpty());
            RestoreDrillDryRun.requirePathLine(rig.tee, "EQUAL", "startup pull");
            assertEquals("core_restore_path EQUAL", 2, ServiceProbe.rcs(f.service, "core_restore_path"));
            assertTrue("the startup pull pulled", ServiceProbe.pull(f.service, "shadow_pull_total") >= 1);
            rig.awaitImagesEqual(f, leader, 60_000);
            System.out.println("GREEN 3b: wiped member " + f.id + " restored EQUAL from a pull; restart to ack "
                    + toAckMs + " ms, time to ready " + readyMs + " ms, restart to ready " + toReadyMs
                    + " ms (SNAPSHOT_RESTORE_READY_BUDGET_MS "
                    + f.settings.readyBudgetMs() + " is a measurement)");

            // the same member, its shadows wiped again, with a source that answers nothing: the halt names the pull
            rig.await(() -> ServiceProbe.puller(f.service) != null, 30_000, "f's puller never restarted");
            ServiceProbe.puller(f.service).close();
            rig.purgeShadows(f);
            rig.await(() -> rig.scan(f).all().isEmpty(), 10_000, "f still lists shadows after the second purge");
            f.settings = ShadowClusterRig.shadowSettings("SHADOW_PULL_SOURCES", "localhost:"
                    + (PORT_BASE + 999), "SHADOW_PULL_PERIOD_MS", "500", "SHADOW_PULL_STARTUP_WAIT_MS", "1000");
            rig.tee.clear();
            rig.errTee.clear();
            rig.kill(f);
            rig.restart(f);
            rig.await(() -> !f.halts.isEmpty()
                            && StubStory.line(rig.errTee.lines, "FATAL shadow restore: stub at P_cm=") != null, 120_000,
                    () -> "f must halt; halts " + f.halts + " stderr " + rig.errTee.lines);
            final String halt = StubStory.line(rig.errTee.lines, "FATAL shadow restore: stub at P_cm=");
            assertTrue(halt, halt.contains("(3) pull: tried 1 sources, 0 candidates, refused 0, last: no candidate"));
            System.out.println("GREEN 3b unreachable source -> " + halt.trim());
        }
    }

    /** The live members other than {@code leader}. */
    static List<Member> followers(final ShadowClusterRig rig, final Member leader) {
        final List<Member> out = new ArrayList<>();
        for (final Member m : rig.live()) {
            if (m.id != leader.id) {
                out.add(m);
            }
        }
        return out;
    }

    /** The usable local copy carrying {@code source}'s full alias, or null. */
    static ShadowEntry copyOf(final ShadowCatalog.Scan scan, final ShadowEntry source) {
        for (final ShadowEntry e : scan.all()) {
            if (source.alias().equals(e.alias()) && e.state().usable()) {
                return e;
            }
        }
        return null;
    }

    /** {@code n} accounts (one deposit each), so the image and its shadow are several MB. */
    private static void accounts(final ShadowClusterRig rig, final Client tool, final int n) {
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        long last = 0;
        for (int i = 0; i < n; i++) {
            last = tool.corr++;
            tool.offer(enc.buf, enc.deposit(last, 1_000 + i, Asset.USD.id(), ShadowClusterRig.ONE));
        }
        final long target = last;
        rig.await(() -> tool.in.lastDepositAckCorr >= target, 600_000, "100k account deposits");
    }
}

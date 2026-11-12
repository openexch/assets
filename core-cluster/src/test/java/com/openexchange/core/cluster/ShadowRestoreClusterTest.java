// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The DRY RUN (C14) of the three-member restore drill on 84b088b with {@code SNAPSHOT_MODE}
 * absent. Production restore code does not exist yet, so the drill proves the rig, the sizes and the oracles, and may
 * fail only on the path line (see {@link RestoreDrillDryRun}).
 *
 * <p>The story: three members, ADOPT, deposits, one cluster snapshot (P_cm), more deposits; shadows are produced into the
 * follower's own archive with the contract 11.0 producer at {@code P_s < P_cm}, {@code P_s > P_cm} and {@code P_s == P_cm}
 * (the last one 10 s after the snapshot, C2); the follower is killed (no shutdown hook), the survivors keep committing
 * while it is down (the kill window), it is restarted on the same directories, and its image must equal the never
 * restarted leader's, within the ready budget. Four restarts: TAIL, SKIP, SKIP again (C4: the window opens twice in one
 * JVM), EQUAL. On the base every restart restores the full snapshot at P_cm and ignores the shadows; the expected
 * production path of each restart is recorded for W3/W4.</p>
 *
 * <p>The production rows (stub mode on every member): the same story asserts the restore path line of each
 * restart, the image equality with the never-restarted leader and {@code /ready} within {@code SNAPSHOT_RESTORE_READY_BUDGET_MS}
 * (read from the member's settings, 3.5). Controls: no shadow and a pruned log
 * ({@code no restore path line; node halted: FATAL shadow restore: stub at P_cm}), the backward-shifted shadow of 11.0
 * ({@code shadow P_s witness failed}). The embedded leg of the 11.14 journal-writer row (rule 12.14) runs here too. The
 * 100k leg (nightly, {@code -Dsnapshot.nightly=true}) is {@link ShadowRestore100kClusterTest}.</p>
 */
public class ShadowRestoreClusterTest {

    private static final int PORT_BASE = 21000;
    private static int nextProducerId = 100;

    private static Who who() {
        return new Who(ProducerKind.FOLLOWER, nextProducerId++);
    }

    private static void produce(final ShadowClusterRig rig, final Member f, final RecordedLog log, final long p,
                                final String what) {
        final TestShadowProducer producer = rig.producer(f);
        final Written w = producer.produce(who(), log, p);
        assertTrue(w.toString(), w.ok());
        System.out.println("[restore-dryrun] shadow " + what + " written into member " + f.id + "'s archive: " + w);
    }

    @Test
    public void dryRunStoryAndKillWindowsOnTheBase() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "rcs-s4-dry-restore")) {
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            System.out.println("[restore-dryrun] leader member " + leader.id + ", restarting follower " + f.id + ", client via member " + g.id);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(300);
            final long pCm = rig.snapshotAll();
            tool.deposits(80);
            rig.awaitImagesEqual(f, leader, 60_000);
            final byte[] image = ServiceProbe.image(f.service);
            System.out.println("[restore-dryrun] P_cm=" + pCm + " engine image " + image.length + " B; snapshot lines: "
                    + rig.tee.all("[SNAPSHOT] accounts="));

            // the committed frames of f's own log recording, and the three shadow positions around P_cm
            final RecordedLog log = rig.captureCommitted(f);
            final long[] pos = log.positions();
            final int cm = Arrays.binarySearch(pos, pCm);
            assertTrue("P_cm " + pCm + " is a frame end of f's log (" + pos.length + " frames, last " + pos[pos.length - 1] + ")",
                    cm >= 0);
            assertTrue("frames exist on both sides of P_cm", cm >= 6 && cm + 6 < pos.length);
            final long pLt = pos[cm - 6];
            final long pGt = pos[cm + 6];
            System.out.println("[restore-dryrun] positions P_s<P_cm=" + pLt + " P_cm=" + pCm + " P_s>P_cm=" + pGt + " of " + pos.length + " frames");

            // R1 TAIL: only the older shadow exists
            produce(rig, f, log, pLt, "TAIL(P_s<P_cm)");
            cycle(rig, leader, f, tool, "R1", "TAIL");
            // R2 SKIP: a shadow above P_cm, no EQUAL
            produce(rig, f, log, pGt, "SKIP(P_s>P_cm)");
            cycle(rig, leader, f, tool, "R2", "SKIP");
            // R3 SKIP again in the same JVM (C4)
            cycle(rig, leader, f, tool, "R3", "SKIP");
            // R4 EQUAL: the shadow at P_cm is written late (C2), ten seconds after the snapshot
            rig.pumpMs(10_000);
            produce(rig, f, log, pCm, "EQUAL(P_s==P_cm)");
            cycle(rig, leader, f, tool, "R4", "EQUAL");

            // C6: the image oracle must be able to fail
            RestoreDrillDryRun.nc("dry-11.2-image-oracle", "images never became equal", () ->
                    rig.awaitImagesEqual(f, leader, 3_000, bytes -> {
                        final byte[] flipped = bytes.clone();
                        flipped[flipped.length / 2] ^= 1;
                        return flipped;
                    }));
        }
    }

    /** kill -9 equivalent of {@code f}, a commit burst while it is down, restart on the same directories, oracles. */
    private static void cycle(final ShadowClusterRig rig, final Member leader, final Member f, final Client tool,
                              final String label, final String expectedPath) {
        rig.tee.clear();
        rig.kill(f);
        tool.deposits(40);
        rig.restart(f);
        final long readyMs = rig.awaitReady(f, 120_000);
        rig.awaitImagesEqual(f, leader, 60_000);
        assertTrue(f.halts.toString(), f.halts.isEmpty());
        System.out.println("[restore-dryrun] " + label + " restart #" + f.starts + " readyMs=" + readyMs + " (budget "
                + RestoreDrillDryRun.READY_BUDGET_MS + ") image==leader image (" + ServiceProbe.image(f.service).length + " B); "
                + "restore lines: " + rig.tee.lines);
        assertTrue(label + ": ready within the budget, took " + readyMs, readyMs <= RestoreDrillDryRun.READY_BUDGET_MS);
        RestoreDrillDryRun.pathLine(rig.tee, expectedPath, label);
        // the producer's clients died with the driver; the next produce() reconnects
    }

    // ---------------------------------------------------------------------------------------- W4: production rows

    private static void allShadow(final ShadowClusterRig rig) {
        for (final Member m : rig.members) {
            m.settings = StubStory.shadow();
        }
    }

    /** 11.2: TAIL, SKIP, SKIP again (C4), EQUAL written 10 s late (C2), all members in stub mode. */
    @Test
    public void stubModeRestoresEveryPathToTheLeadersImageWithinTheBudget() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(21100, "rcs-s4-11.2-paths")) {
            allShadow(rig);
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(300);
            final long pCm = rig.snapshotAll();
            assertTrue("the snapshot is a stub on every member: " + rig.tee.all("kind="),
                    rig.tee.all("logPosition=" + pCm + " kind=stub").size() >= 3);
            tool.deposits(80);
            rig.awaitImagesEqual(f, leader, 60_000);
            final RecordedLog log = rig.captureCommitted(f);
            final long[] pos = log.positions();
            final int cm = Arrays.binarySearch(pos, pCm);
            assertTrue("P_cm is a frame end with frames on both sides", cm >= 6 && cm + 6 < pos.length);
            final long pLt = pos[cm - 6];
            final long pGt = pos[cm + 6];
            final long budget = f.settings.readyBudgetMs();

            produce(rig, f, log, pLt, "TAIL(P_s<P_cm)");
            final long tailMs = production(rig, leader, f, tool, "R1", "TAIL P_s=" + pLt, budget);
            assertEquals("core_restore_path TAIL", 4, ServiceProbe.rcs(f.service, "core_restore_path"));
            assertTrue("the tail replayed frames", ServiceProbe.rcs(f.service, "core_tail_frames_total") > 0);
            produce(rig, f, log, pGt, "SKIP(P_s>P_cm)");
            final long skipMs = production(rig, leader, f, tool, "R2", "SKIP P_s=" + pGt, budget);
            assertEquals("core_skip_window_closed_total after the first SKIP", 1,
                    ServiceProbe.rcs(f.service, "core_skip_window_closed_total"));
            final long skip2Ms = production(rig, leader, f, tool, "R3", "SKIP P_s=" + pGt, budget);
            assertEquals("the window opened and closed again in the second process life (C4)", 1,
                    ServiceProbe.rcs(f.service, "core_skip_window_closed_total"));
            rig.pumpMs(10_000);
            produce(rig, f, log, pCm, "EQUAL(P_s==P_cm), 10 s late (C2)");
            final long equalMs = production(rig, leader, f, tool, "R4", "EQUAL P_s=" + pCm, budget);
            assertEquals("core_restore_path EQUAL", 2, ServiceProbe.rcs(f.service, "core_restore_path"));
            System.out.println("GREEN 11.2 readyMs TAIL=" + tailMs + " SKIP=" + skipMs + " SKIP2=" + skip2Ms + " EQUAL="
                    + equalMs + " budget=" + budget);
        }
    }

    /** 11.2 control: stub mode, NO shadow, the follower's log pruned below P_cm: the path assertion must fail. */
    @Test
    public void controlWithoutShadowsAPrunedFollowerHaltsAndThePathAssertionFails() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(21200, "rcs-s4-11.2-noshadow", 3, "64k", StubStory.SEGMENT)) {
            allShadow(rig);
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(900);
            final long pCm = rig.snapshotAll();
            rig.awaitImagesEqual(f, leader, 60_000);
            final long boundary = pCm / StubStory.SEGMENT * StubStory.SEGMENT;
            assertTrue("P_cm past the first segment: " + pCm, boundary > 0);
            assertEquals("f's log pruned to " + boundary, boundary, rig.purgeLogTo(f, boundary));
            rig.tee.clear();
            rig.errTee.clear();
            rig.kill(f);
            tool.deposits(20);
            rig.restart(f);
            rig.await(() -> !f.halts.isEmpty() && StubStory.line(rig.errTee.lines, "FATAL shadow restore: stub at P_cm=") != null,
                    120_000, () -> "f must halt; halts " + f.halts + " stderr " + rig.errTee.lines);
            RestoreDrillDryRun.nc("11.2-no-shadows", "no restore path line; node halted: FATAL shadow restore: stub at P_cm",
                    () -> pathLineOrHalt(rig, f, "TAIL"));
            assertEquals("never genesis", 0, ServiceProbe.rcs(f.service, "core_virtual_genesis_total"));
        }
    }

    /** 11.2 control: the backward-shifted shadow of 11.0 (a later-millisecond extra frame) fails W1 and halts. */
    @Test
    public void controlABackwardShiftedShadowFailsTheWitness() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(21300, "rcs-s4-11.2-witness")) {
            allShadow(rig);
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(200);
            final long pCm = rig.snapshotAll();
            for (int i = 0; i < 20; i++) { // spread frames over milliseconds so a later-ms pair exists past P_cm
                tool.deposits(5);
                rig.pumpMs(3);
            }
            rig.awaitImagesEqual(f, leader, 60_000);
            final RecordedLog log = rig.captureCommitted(f);
            final TestShadowProducer.ShiftPair pair = TestShadowProducer.findShiftPair(log, false, pCm);
            final Written w = rig.producer(f).produceBackwardShift(who(), log, pair.x(), pair.y());
            assertTrue(w.toString(), w.ok());
            System.out.println("[11.2] backward shift " + w + " pair " + pair);
            rig.tee.clear();
            rig.errTee.clear();
            rig.kill(f);
            rig.restart(f);
            rig.await(() -> !f.halts.isEmpty() && StubStory.line(rig.errTee.lines, "shadow P_s witness failed") != null,
                    120_000, () -> "the witness must halt f; halts " + f.halts + " stderr " + rig.errTee.lines);
            final String halt = StubStory.line(rig.errTee.lines, "shadow P_s witness failed");
            assertTrue(halt, halt.contains("shadow P_s witness failed: lastLogTime=" + pair.yTime() + " > header.clusterTime="
                    + pair.xTime()));
            assertEquals("core_witness_failed_total", 1, ServiceProbe.rcs(f.service, "core_witness_failed_total"));
            System.out.println("NC 11.2-backward-shift -> " + halt.trim());
        }
    }

    /**
     * 11.14 journal-writer row, EMBEDDED leg (rule 12.14, review F3): f journals money (armed through the log), its
     * restart takes a TAIL that deposits more than its ring holds, and a Faults.afterTailFrames hook stops the writer
     * after the first tail frame: the tail must end in the SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS halt, never hang. Paired control:
     * the same restart with the writer left running completes {@code restored path=TAIL}.
     */
    @Test
    public void aTailWithTheJournalWriterStoppedTimesOutLoudly() throws Exception {
        journalRow(21400, true);
    }

    @Test
    public void controlTheSameTailWithTheWriterRunningCompletes() throws Exception {
        journalRow(21500, false);
    }

    static final int SMALL_RING = 64 * 1024;

    private static void journalRow(final int portBase, final boolean stopWriter) throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(portBase, "rcs-s4-11.14-emb-" + stopWriter)) {
            allShadow(rig);
            final Member[] fRef = new Member[1];
            for (final Member m : rig.members) {
                m.beforeLaunch = svc -> svc.setMoneyJournalSink(
                        new com.openexchange.core.infrastructure.journal.MoneyJournal(16 * 1024 * 1024));
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            fRef[0] = f;
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            rig.armMoneyJournal(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(50);
            rig.awaitImagesEqual(f, leader, 60_000);
            final RecordedLog early = rig.captureCommitted(f);
            final long pS = early.positions()[early.positions().length - 1];
            produce(rig, f, early, pS, "TAIL base");
            // more journal entries in (P_s, P_cm] than the restart's ring holds (a deposit entry is 64 ring bytes)
            final int deposits = SMALL_RING / 64 + 600;
            tool.deposits(deposits);
            final long pCm = rig.snapshotAll();
            rig.awaitImagesEqual(f, leader, 60_000);
            final com.openexchange.core.infrastructure.persistence.SnapshotModeSettings settings =
                    com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.with(java.util.Map.of(
                            com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.Key.SNAPSHOT_MODE, "shadow",
                            com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.Key.SHADOW_PRODUCER, "none",
                            com.openexchange.core.infrastructure.persistence.SnapshotModeSettings.Key.SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS,
                            "3000"));
            final com.openexchange.core.infrastructure.journal.MoneyJournal small =
                    new com.openexchange.core.infrastructure.journal.MoneyJournal(SMALL_RING);
            final org.agrona.concurrent.AgentRunner[] writer = new org.agrona.concurrent.AgentRunner[1];
            rig.kill(f);
            f.settings = settings;
            f.beforeLaunch = svc -> {
                svc.setMoneyJournalSink(small);
                if (stopWriter) {
                    // the Faults hook of 6.4 (review F8): the writer agent stops after the first tail frame
                    ServiceProbe.afterTailFrames(svc, 1, () -> {
                        System.out.println("[11.14-emb] Faults.afterTailFrames(1): stopping the journal writer agent");
                        org.agrona.CloseHelper.quietClose(writer[0]);
                    });
                }
            };
            // AC:208-212 order: the writer starts after the driver, archive and consensus module, before the container
            f.beforeContainer = m -> {
                writer[0] = new org.agrona.concurrent.AgentRunner(new org.agrona.concurrent.BackoffIdleStrategy(),
                        Throwable::printStackTrace, null,
                        new com.openexchange.core.infrastructure.journal.MoneyJournalWriterAgent(small,
                                m.cfg.aeronArchiveContext().clone()::clone,
                                com.openexchange.core.infrastructure.InfrastructureConstants.MONEY_JOURNAL_CHANNEL,
                                com.openexchange.core.infrastructure.InfrastructureConstants.MONEY_JOURNAL_STREAM_ID));
                org.agrona.concurrent.AgentRunner.startOnThread(writer[0]);
            };
            rig.tee.clear();
            rig.errTee.clear();
            final long t0 = System.currentTimeMillis();
            rig.restart(f);
            final Thread unblock = new Thread(() -> {
                // after the verdict, drain the abandoned ring so the blocked service thread can reach its close
                while (!Thread.currentThread().isInterrupted()) {
                    if (!f.halts.isEmpty()) {
                        small.ringBuffer().read((type, buf, off, len) -> { }, 1024);
                    }
                    Thread.onSpinWait();
                }
            }, "11.14-unblock");
            unblock.setDaemon(true);
            try {
                if (stopWriter) {
                    rig.await(() -> !f.halts.isEmpty()
                                    && StubStory.line(rig.errTee.lines, "tail replay timed out at") != null, 60_000,
                            () -> "the tail must time out loudly; halts " + f.halts + " stderr " + rig.errTee.lines
                                    + " stdout " + rig.tee.lines);
                    final long ms = System.currentTimeMillis() - t0;
                    unblock.start();
                    final String halt = StubStory.line(rig.errTee.lines, "tail replay timed out at");
                    final java.util.regex.Matcher at = java.util.regex.Pattern.compile("timed out at (\\d+) \\(journal ring full\\?\\)")
                            .matcher(halt);
                    assertTrue(halt, at.find());
                    final long p = Long.parseLong(at.group(1));
                    assertTrue("the tail stopped inside (P_s, P_cm): " + pS + " < " + p + " < " + pCm, pS < p && p < pCm);
                    assertTrue("no restore path line on a timed-out tail: " + rig.tee.all("restored path="),
                            rig.tee.all("restored path=").isEmpty());
                    System.out.println("GREEN 11.14 embedded journal-writer row: halt after " + ms + " ms: " + halt.trim());
                } else {
                    final long readyMs = rig.awaitReady(f, 120_000);
                    rig.awaitImagesEqual(f, leader, 60_000);
                    assertTrue(f.halts.toString(), f.halts.isEmpty());
                    RestoreDrillDryRun.requirePathLine(rig.tee, "TAIL P_s=" + pS, "11.14 embedded writer running");
                    System.out.println("CONTROL 11.14 embedded, writer running: TAIL completed, readyMs=" + readyMs + " "
                            + rig.tee.all("[shadow restore] tail"));
                }
            } finally {
                unblock.interrupt();
                org.agrona.CloseHelper.quietClose(writer[0]);
            }
        }
    }

    /** 11.2 path assertion of a production restart: the path line, or the halt text when the node halted. */
    static void pathLineOrHalt(final ShadowClusterRig rig, final Member f, final String expected) {
        if (rig.tee.all("restored path=").stream().noneMatch(l -> l.contains("restored path=" + expected))
                && !f.halts.isEmpty()) {
            throw new AssertionError("no restore path line; node halted: "
                    + String.valueOf(StubStory.line(rig.errTee.lines, "FATAL shadow restore")).trim());
        }
        RestoreDrillDryRun.requirePathLine(rig.tee, expected, "11.2");
    }

    /**
     * 11.2 egress oracle (the event-stream half of 6.6): with both members at rest and equal, tap the
     * frames each member's publisher emits (before the leader gate, so the follower's would-be egress is visible), run
     * a burst, and require the restarted follower's frames to equal the leader's for the same committed frames.
     */
    private static int egressEqualsTheLeaders(final ShadowClusterRig rig, final Member leader, final Member f,
                                              final Client tool, final String label) {
        final java.util.List<String> lf = ServiceProbe.tapEgress(leader.service);
        final java.util.List<String> ff = ServiceProbe.tapEgress(f.service);
        try {
            tool.deposits(20);
            rig.awaitImagesEqual(f, leader, 60_000);
            rig.await(() -> !lf.isEmpty() && snapshot(ff).size() >= snapshot(lf).size(), 30_000,
                    () -> label + ": the follower emitted " + ff.size() + " frames, the leader " + lf.size());
            rig.pumpMs(200);
        } finally {
            ServiceProbe.untapEgress(leader.service);
            ServiceProbe.untapEgress(f.service);
        }
        final java.util.List<String> leaderFrames = snapshot(lf);
        final java.util.List<String> followerFrames = snapshot(ff);
        assertEgressEqual(label, leaderFrames, followerFrames);
        if (label.equals("R1")) {
            // C6: the oracle must be able to fail
            final java.util.List<String> short1 = new java.util.ArrayList<>(followerFrames);
            short1.remove(short1.size() - 1);
            RestoreDrillDryRun.nc("11.2-egress-oracle", label + ": egress after the restart differs from the leader's",
                    () -> assertEgressEqual(label, leaderFrames, short1));
        }
        return leaderFrames.size();
    }

    private static java.util.List<String> snapshot(final java.util.List<String> frames) {
        synchronized (frames) {
            return new java.util.ArrayList<>(frames);
        }
    }

    private static void assertEgressEqual(final String label, final java.util.List<String> leader,
                                          final java.util.List<String> follower) {
        if (leader.isEmpty() || !leader.equals(follower)) {
            int i = 0;
            while (i < Math.min(leader.size(), follower.size()) && leader.get(i).equals(follower.get(i))) {
                i++;
            }
            throw new AssertionError(label + ": egress after the restart differs from the leader's: leader "
                    + leader.size() + " frames, follower " + follower.size() + ", first difference at frame " + i);
        }
    }

    /** One production restart of {@code f}: kill, a commit burst while it is down, restart, path, image, budget. */
    private static long production(final ShadowClusterRig rig, final Member leader, final Member f, final Client tool,
                                   final String label, final String expectedPath, final long budgetMs) {
        rig.tee.clear();
        rig.errTee.clear();
        rig.kill(f);
        tool.deposits(40);
        rig.restart(f);
        final long readyMs = rig.awaitReady(f, 120_000);
        rig.awaitImagesEqual(f, leader, 60_000);
        assertTrue(f.halts.toString(), f.halts.isEmpty());
        pathLineOrHalt(rig, f, expectedPath);
        final int frames = egressEqualsTheLeaders(rig, leader, f, tool, label);
        System.out.println("[11.2] " + label + " restart #" + f.starts + " readyMs=" + readyMs + " (budget " + budgetMs
                + ") image==leader image, egress==leader egress over " + frames + " frames; "
                + rig.tee.all("restored path="));
        assertTrue(label + ": ready within SNAPSHOT_RESTORE_READY_BUDGET_MS=" + budgetMs + ", took " + readyMs, readyMs <= budgetMs);
        return readyMs;
    }
}

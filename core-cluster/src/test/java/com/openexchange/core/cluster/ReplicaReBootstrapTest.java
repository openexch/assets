// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaProcess;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowPullSettings;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.RecordingLog;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A replica whose log the members pruned past (the members took two snapshots and purged their logs above the
 * replica's L.stop while it was down) re-bootstraps instead of looping: B2 from its old shadow, the supplier refuses
 * every live log replay below its start ({@code shadow_replica_backup_replay_below_start_total}), L's live recording is
 * not alive, a source lists an ALIGNED above L.stop and the pull verifies it: the CRITICAL line,
 * {@code shadow_replica_rebootstrap_total 1}, {@code host.restart(3)}; the process close moves {@code cluster/} aside
 * after the backup and the archive closed (the backup's mark file and RecordingLog inside the moved dir, none left in
 * {@code BASE_DIR/cluster}); the second start records a new L from the supplier's start and restores LOCAL_SHADOW
 * from the pulled shadow, then follows and writes an ALIGNED equal to the members'. The loop guard: a marker already
 * holding three re-bootstraps inside the window refuses the fourth with the loop text (exit 1).
 *
 * <p>Negative controls, each on its own rig (one rule switched off, the row's oracle fails with its text): the
 * re-bootstrap decided without a pull, {@code cluster/} moved before the backup closed, the loop guard off. The
 * replay-refusal storm (MAX_REPLAYS held on every member, the backup window shape): with the production rules no
 * re-bootstrap and a re-attach after the release; with the pull precondition switched off alone, still none (the
 * replay-below-start rule alone holds); with every supplier replay refusal counted as well, the storm re-bootstraps
 * the replica.</p>
 *
 * <p>A restart that lost the move (the process stopped between the decision and the move in close: kill -9, or a
 * driver loss, which exits 3 without the close) keeps the decision in the marker, the pulled shadow in the archive and
 * {@code cluster/} in place. The next start restores from the pulled shadow above L.stop and the pull finds nothing
 * new; the VERIFIED local shadow above L.stop decides the re-bootstrap once, the start after restores from it. With the
 * local branch switched off the replica idles ({@code re-bootstrap not taken} on {@code /ready}). A replica without
 * sources whose local shadows all lie at or below L.stop refuses with the re-bootstrap prefix.</p>
 */
public class ReplicaReBootstrapTest {

    private static final int PORT_BASE = 50400;
    /** One rig per negative control: 53000, 53600, 54200 (members, the replica at slot 3). */
    private static final int PORT_BASE_NC = 53000;
    private static final int PORT_BASE_STORM = 54800;
    private static final int PORT_BASE_LOST_MOVE = 56000;
    private static final int SEGMENT = 64 * 1024;
    private static final long CYCLE_MS = 300_000;

    @Test(timeout = 1_800_000)
    public void aReplicaPrunedPastRebootstrapsThroughAPullAndTheLoopGuardBoundsIt() throws Exception {
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE, "replica-rebootstrap", 3, "64k",
                SEGMENT))) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            final long copy = r.rig.liveLogCopy(r.rig.members.get(1), r.rig.members.get(2));
            final List<Member> ms = r.rig.members;
            final ShadowPullSettings pull = ShadowPullSettings.parse(Map.of(
                    ShadowPullSettings.SOURCES, r.rig.sources(ms.get(0), ms.get(1), ms.get(2)),
                    ShadowPullSettings.PERIOD_MS, "1000",
                    ShadowPullSettings.STARTUP_WAIT_MS, "20000")::get, null);
            final File base = r.replicaBase("replica");

            // the replica follows from genesis and writes a VERIFIED ALIGNED, then stops
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 60_000);
            final long lStop = r.replica.follower().recordedPosition();
            r.stopReplica();

            // while it is down the members take two snapshots and purge their logs past its L.stop
            final long pB = pruneBeyond(r, copy, lStop, true);
            r.windowGap();

            // restart: B2, three replay-below-start refusals, a pull above L.stop, exit 3
            final long t0 = System.nanoTime();
            final ReplicaProcess first = r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull)
                    .build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            r.rig.await(() -> !r.restarts.isEmpty() || !r.fatals.isEmpty(), CYCLE_MS,
                    () -> "exit code == 3 within " + CYCLE_MS + " ms: no restart; replay below start "
                            + r.metric("shadow_replica_backup_replay_below_start_total") + ", replay failures "
                            + r.metric("shadow_replica_backup_replay_failures_total") + ", " + r.probe().detail());
            final long decidedMs = (System.nanoTime() - t0) / 1_000_000;
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            final String restart = r.restarts.get(0);
            assertTrue(restart, restart.startsWith("3 replica re-bootstrap: members pruned past L.stop="));
            assertPulledBeforeTheRestart(r, restart, pB);
            assertTrue(restart, restart.endsWith("; restarting"));
            assertEquals(1, r.metric("shadow_replica_rebootstrap_total"));
            assertTrue("replay below start counted", r.metric("shadow_replica_backup_replay_below_start_total") >= 3);
            assertTrue(r.probe().detail(), r.probe().detail().contains("rebootstrap: restarting"));
            r.stopReplica();
            final File moved = assertMovedAfterTheBackupClosed(first, base);
            System.out.println("GREEN re-bootstrap decided in " + decidedMs + " ms: " + restart + "; moved to " + moved);

            // the second start: a new L from the supplier's start, LOCAL_SHADOW from the pulled shadow
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            assertEquals("restored from the pulled shadow", pB, r.replica.follower().restoredPosition());
            assertTrue("the new L starts at the supplier's start", r.replica.follower().logStartPositionAtBootstrap()
                    > lStop);
            r.awaitCaughtUp(120_000);
            final long next = r.rig.snapshotAll();
            r.awaitVerified(next, ShadowKind.ALIGNED, 60_000);
            ReplicaRig.assertPayloadEqualsMember(next, r.payload(r.awaitVerified(next, ShadowKind.ALIGNED, 1_000)),
                    r.memberImage(host.id, next));
            final long lStop2 = r.replica.follower().recordedPosition();
            r.stopReplica();
            System.out.println("GREEN second start: LOCAL_SHADOW at " + pB + ", the next ALIGNED at " + next
                    + " equals the members'");

            // the loop guard: three re-bootstraps already inside the window refuse the fourth, a newer shadow there
            threeInTheWindow(base);
            pruneBeyond(r, copy, lStop2, false); // the guard refuses before any pull
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
            r.rig.await(() -> !r.fatals.isEmpty() || !r.restarts.isEmpty(), CYCLE_MS,
                    () -> "the loop guard never refused: " + r.probe().detail());
            assertTheLoopGuardRefused(r);
            System.out.println("GREEN loop guard: " + r.fatals.get(0));
        }
    }

    // ---------------------------------------------------------------------------------------------- controls

    /** One negative control per rig: the first re-bootstrap cycle with the rule off, the row's oracle on it. */
    @Test(timeout = 1_800_000)
    public void eachReBootstrapRuleSwitchedOffFailsItsOracle() throws Exception {
        final ReplicaNegativeControl[] controls = {ReplicaNegativeControl.REBOOTSTRAP_WITHOUT_PULL,
            ReplicaNegativeControl.RENAME_BEFORE_BACKUP_CLOSE, ReplicaNegativeControl.REBOOTSTRAP_LOOP_GUARD_OFF};
        for (int i = 0; i < controls.length; i++) {
            final ReplicaNegativeControl nc = controls[i];
            try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE_NC + 600 * i,
                    "replica-rebootstrap-nc-" + i, 3, "64k", SEGMENT))) {
                r.bootAll();
                final Member leader = r.rig.awaitLeader(null);
                r.story(r.rig.followerOtherThan(leader, null), 4);
                final long copy = r.rig.liveLogCopy(r.rig.members.get(1), r.rig.members.get(2));
                final ShadowPullSettings pull = pullSettings(r);
                final File base = r.replicaBase("replica");
                r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                r.awaitCaughtUp(60_000);
                final long p1 = r.rig.snapshotAll();
                r.awaitVerified(p1, ShadowKind.ALIGNED, 60_000);
                final long lStop = r.replica.follower().recordedPosition();
                r.stopReplica();
                final long pB = pruneBeyond(r, copy, lStop, true);
                if (nc == ReplicaNegativeControl.REBOOTSTRAP_LOOP_GUARD_OFF) {
                    threeInTheWindow(base);
                }
                r.windowGap();
                final ReplicaProcess first = r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull)
                        .testHooks(r.faults, ReplicaRig.off(nc)).build());
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                r.rig.await(() -> !r.restarts.isEmpty() || !r.fatals.isEmpty(), CYCLE_MS,
                        () -> "no restart and no refusal with " + nc + ": " + r.probe().detail());
                switch (nc) {
                    case REBOOTSTRAP_WITHOUT_PULL -> RestoreDrillDryRun.nc(nc.name(),
                            "re-bootstrap exited with no newer shadow pulled",
                            () -> assertPulledBeforeTheRestart(r, r.restarts.isEmpty() ? String.valueOf(r.fatals)
                                    : r.restarts.get(0), pB));
                    case RENAME_BEFORE_BACKUP_CLOSE -> {
                        assertTrue("restarted: " + r.fatals, !r.restarts.isEmpty());
                        r.stopReplica();
                        RestoreDrillDryRun.nc(nc.name(), "cluster/ moved while the backup held it",
                                () -> assertMovedAfterTheBackupClosed(first, base));
                    }
                    case REBOOTSTRAP_LOOP_GUARD_OFF -> RestoreDrillDryRun.nc(nc.name(), "re-bootstrap cycled 4 times",
                            () -> assertTheLoopGuardRefused(r));
                    default -> throw new IllegalStateException(nc.name());
                }
                r.stopReplica();
            }
        }
    }

    /**
     * A MAX_REPLAYS storm on every member (transient replay refusals that are not below the supplier's start) never
     * re-bootstraps: the production rules, then the pull precondition switched off alone; the re-attach after each
     * release. Control: every supplier replay refusal counted too (with the pull precondition off, so only the
     * replay-below-start rule separates the runs) -> {@code a transient replay refusal re-bootstrapped the replica}.
     * {@code SHADOW_REBOOTSTRAP_REPLAY_FAILURES=2} here: the rows run at a tighter threshold than the default.
     */
    @Test(timeout = 1_800_000)
    public void aReplayRefusalStormNeverReBootstraps() throws Exception {
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE_STORM, "replica-rebootstrap-storm", 3, "64k",
                SEGMENT))) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            r.story(r.rig.followerOtherThan(leader, null), 4);
            final ShadowPullSettings pull = pullSettings(r);
            final List<Set<ReplicaNegativeControl>> runs = List.of(Set.of(),
                    Set.of(ReplicaNegativeControl.REBOOTSTRAP_WITHOUT_PULL),
                    Set.of(ReplicaNegativeControl.REBOOTSTRAP_WITHOUT_PULL,
                            ReplicaNegativeControl.REBOOTSTRAP_ON_ANY_REPLAY_FAILURE));
            for (int i = 0; i < runs.size(); i++) {
                final Set<ReplicaNegativeControl> off = runs.get(i);
                if (i > 0) {
                    r.stopReplica();
                    r.windowGap();
                }
                final ReplicaConfig cfg = r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull)
                        .rebootstrapReplayFailures(2).testHooks(r.faults, off.isEmpty()
                                ? java.util.EnumSet.noneOf(ReplicaNegativeControl.class) : java.util.EnumSet.copyOf(off))
                        .build();
                r.startReplica(cfg);
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                r.awaitCaughtUp(60_000);
                if (i == 0) {
                    final long p = r.rig.snapshotAll(); // a local shadow for the later starts
                    r.awaitVerified(p, ShadowKind.ALIGNED, 60_000);
                }
                final long failures = stormUntil(r, 3, 120_000);
                final Runnable oracle = () -> {
                    if (!r.restarts.isEmpty() || r.metric("shadow_replica_rebootstrap_total") > 0) {
                        throw new AssertionError("a transient replay refusal re-bootstrapped the replica: "
                                + r.restarts);
                    }
                };
                if (off.contains(ReplicaNegativeControl.REBOOTSTRAP_ON_ANY_REPLAY_FAILURE)) {
                    RestoreDrillDryRun.nc(ReplicaNegativeControl.REBOOTSTRAP_ON_ANY_REPLAY_FAILURE.name(),
                            "a transient replay refusal re-bootstrapped the replica", oracle);
                    continue;
                }
                oracle.run();
                assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
                assertEquals("no replay below start in a MAX_REPLAYS storm", 0,
                        r.metric("shadow_replica_backup_replay_below_start_total"));
                final long lift = System.currentTimeMillis();
                ReplicaBackupWindowTest.awaitReattach(r, lift);
                System.out.println("GREEN storm" + (off.isEmpty() ? "" : " with " + off) + ": " + failures
                        + " replay refusals, no re-bootstrap, re-attached after " + (System.currentTimeMillis() - lift)
                        + " ms");
            }
        }
    }

    /**
     * The on-disk state a process leaves when it stops between the re-bootstrap decision and the move: the first
     * decision runs and its close moves {@code cluster/}; the test moves it back (the same files the backup left, the
     * decision recorded in {@code rebootstrap.count}, the pulled shadow in the archive). Then, on the same state: the
     * local branch switched off idles; the production rules re-bootstrap once from the local shadow; the start after
     * restores from it and follows. Last, without sources and with every local shadow at or below L.stop, the refusal
     * carries the re-bootstrap prefix.
     */
    @Test(timeout = 1_800_000)
    public void aRestartThatLostTheMoveReBootstrapsOnceFromTheLocalShadow() throws Exception {
        try (ReplicaRig r = new ReplicaRig(new ShadowClusterRig(PORT_BASE_LOST_MOVE, "replica-rebootstrap-lost-move", 3,
                "64k", SEGMENT))) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);
            final long copy = r.rig.liveLogCopy(r.rig.members.get(1), r.rig.members.get(2));
            final ShadowPullSettings pull = pullSettings(r);
            final File base = r.replicaBase("replica");
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long p1 = r.rig.snapshotAll();
            r.awaitVerified(p1, ShadowKind.ALIGNED, 60_000);
            final long lStop = r.replica.follower().recordedPosition();
            r.stopReplica();
            final long pB = pruneBeyond(r, copy, lStop, true);

            // the first decision (pulled), then the state a lost move leaves
            r.windowGap();
            final ReplicaProcess first = r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull)
                    .build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.rig.await(() -> !r.restarts.isEmpty() || !r.fatals.isEmpty(), CYCLE_MS,
                    () -> "the first re-bootstrap was never decided: " + r.probe().detail());
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            assertPulledBeforeTheRestart(r, r.restarts.get(0), pB);
            r.stopReplica();
            final File moved = assertMovedAfterTheBackupClosed(first, base);
            final File cluster = new File(base, "cluster");
            Files.move(moved.toPath(), cluster.toPath());
            assertEquals("the decision is recorded", 1, markerEntries(base));
            System.out.println("[rebootstrap] lost move: " + moved.getName() + " moved back to cluster/, pulled shadow"
                    + " at " + pB + " kept, L.stop=" + lStop);

            // control: only a shadow pulled in this attempt decides -> the replica idles
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull)
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.REBOOTSTRAP_LOCAL_SHADOW_OFF)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertLostMoveRestore(r, pB);
            r.rig.await(() -> r.metric("shadow_replica_rebootstrap_not_taken_total") >= 2 || !r.restarts.isEmpty()
                    || !r.fatals.isEmpty(), CYCLE_MS, () -> "no re-bootstrap attempt: " + r.probe().detail());
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            final String idle = r.probe().detail();
            RestoreDrillDryRun.nc(ReplicaNegativeControl.REBOOTSTRAP_LOCAL_SHADOW_OFF.name(),
                    "a restart that lost the re-bootstrap move never re-bootstrapped",
                    () -> assertReBootstrappedFromTheLocalShadow(r, pB));
            assertTrue("the plain not-taken reason on /ready: " + idle, idle.contains(
                    "recovery=re-bootstrap not taken: no source lists a shadow of kind ALIGNED above L.stop="));
            r.stopReplica();
            assertTrue("no move without a decision", cluster.isDirectory());
            assertEquals("no decision recorded by the control", 1, markerEntries(base));

            // the production rules on the same state: once, from the local shadow
            r.windowGap();
            final long t0 = System.nanoTime();
            final ReplicaProcess second = r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull)
                    .build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertLostMoveRestore(r, pB);
            r.rig.await(() -> !r.restarts.isEmpty() || !r.fatals.isEmpty(), CYCLE_MS,
                    () -> "no re-bootstrap after the lost move: " + r.probe().detail());
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            assertReBootstrappedFromTheLocalShadow(r, pB);
            assertEquals(1, r.metric("shadow_replica_rebootstrap_total"));
            assertEquals("the second decision is recorded", 2, markerEntries(base));
            r.stopReplica();
            final File moved2 = assertMovedAfterTheBackupClosed(second, base);
            System.out.println("GREEN lost move re-bootstrapped in " + (System.nanoTime() - t0) / 1_000_000 + " ms: "
                    + r.restarts.get(0) + "; moved to " + moved2.getName());

            // the start after: LOCAL_SHADOW from the pulled shadow, a new L, follows, no further re-bootstrap
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
            assertEquals("restored from the pulled shadow", pB, r.replica.follower().restoredPosition());
            assertTrue("the new L starts at the supplier's start", r.replica.follower().logStartPositionAtBootstrap()
                    > lStop);
            r.awaitCaughtUp(120_000);
            final long next = r.rig.snapshotAll();
            r.awaitVerified(next, ShadowKind.ALIGNED, 60_000);
            assertTrue("no further re-bootstrap: " + r.restarts + " " + r.fatals, r.restarts.isEmpty()
                    && r.fatals.isEmpty());
            final long lStop3 = r.replica.follower().recordedPosition();
            r.stopReplica();
            System.out.println("GREEN after the lost move: LOCAL_SHADOW at " + pB + ", the next ALIGNED at " + next);

            // no sources, every local shadow at or below L.stop: the re-bootstrap refusal, never the bootstrap runbook
            pruneBeyond(r, copy, lStop3, false);
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").pullSettings(ShadowPullSettings.disabled()).build());
            r.rig.await(() -> !r.fatals.isEmpty() || !r.restarts.isEmpty(), CYCLE_MS,
                    () -> "the replica without sources never refused: " + r.probe().detail());
            assertTrue("never re-bootstrapped from a shadow at or below L.stop: " + r.restarts, r.restarts.isEmpty());
            final String refusal = r.fatals.get(0);
            assertTrue(refusal, refusal.startsWith(
                    "FATAL replica: re-bootstrap refused: re-bootstrap needs SHADOW_PULL_SOURCES ("));
            assertTrue(refusal, refusal.contains(", no local VERIFIED shadow above it). Runbook: set"
                    + " SHADOW_PULL_SOURCES"));
            assertFalse(refusal, refusal.contains("no local shadow and the cluster log starts at"));
            System.out.println("GREEN no sources: " + refusal);
        }
    }

    // ---------------------------------------------------------------------------------------------- oracles

    /** A decided re-bootstrap exits only after a VERIFIED pull of the newer shadow at {@code pB}. */
    static void assertPulledBeforeTheRestart(final ReplicaRig r, final String restart, final long pB) {
        if (r.metric("shadow_pull_total") < 1 || !restart.contains("; pulled ALIGNED at " + pB + " from ")) {
            throw new AssertionError("re-bootstrap exited with no newer shadow pulled: " + restart
                    + " (shadow_pull_total " + r.metric("shadow_pull_total") + ")");
        }
    }

    /** {@code cluster/} moved by the close after the backup and the archive closed; the backup's files inside. */
    static File assertMovedAfterTheBackupClosed(final ReplicaProcess closed, final File base) {
        final File moved = closed.movedClusterDir();
        assertTrue("cluster/ was moved aside: " + moved, moved != null && moved.isDirectory());
        if (!closed.movedAfterBackupClosed()) {
            throw new AssertionError("cluster/ moved while the backup held it: " + moved);
        }
        assertTrue("the backup RecordingLog moved", new File(moved, RecordingLog.RECORDING_LOG_FILE_NAME).isFile());
        assertTrue("the backup mark file moved", new File(moved, "cluster-mark.dat").isFile());
        assertFalse("BASE_DIR/cluster is gone", new File(base, "cluster").exists());
        return moved;
    }

    /** With three re-bootstraps in the window, the fourth is refused (exit 1) with the loop text, never taken. */
    static void assertTheLoopGuardRefused(final ReplicaRig r) {
        if (!r.restarts.isEmpty()) {
            throw new AssertionError("re-bootstrap cycled " + (3 + r.restarts.size()) + " times: " + r.restarts);
        }
        assertTrue("loop text: " + r.fatals, !r.fatals.isEmpty() && r.fatals.get(0).startsWith(
                "FATAL replica: re-bootstrap refused: re-bootstrap loop: 4 in 3600000 ms; members prune faster than"
                        + " this replica catches up. Runbook: "));
    }

    /** The lost-move start: LOCAL_SHADOW from the pulled shadow at {@code pB}, above the old L. */
    private static void assertLostMoveRestore(final ReplicaRig r, final long pB) {
        assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, r.replica.readiness().restorePath());
        assertEquals("restored from the pulled shadow", pB, r.replica.follower().restoredPosition());
    }

    /** After a lost move the VERIFIED local shadow at {@code pB} decides the re-bootstrap (exit 3), no pull needed. */
    static void assertReBootstrappedFromTheLocalShadow(final ReplicaRig r, final long pB) {
        if (r.restarts.isEmpty()) {
            throw new AssertionError("a restart that lost the re-bootstrap move never re-bootstrapped: "
                    + r.metric("shadow_replica_rebootstrap_not_taken_total") + " attempts not taken, "
                    + r.probe().detail());
        }
        final String restart = r.restarts.get(0);
        assertTrue(restart, restart.startsWith("3 replica re-bootstrap: members pruned past L.stop="));
        assertTrue(restart, restart.contains("; local VERIFIED ALIGNED at " + pB + " already held; backup state will"
                + " move to cluster.rebootstrap-"));
    }

    private static int markerEntries(final File base) throws java.io.IOException {
        final File f = new File(base, "rebootstrap.count");
        return f.isFile() ? (int)Files.readAllLines(f.toPath(), StandardCharsets.US_ASCII).stream()
                .filter(l -> !l.isBlank()).count() : 0;
    }

    /** The node-local marker with three re-bootstraps inside the window. */
    private static void threeInTheWindow(final File base) throws java.io.IOException {
        final long now = System.currentTimeMillis();
        Files.writeString(new File(base, "rebootstrap.count").toPath(), (now - 3000) + "\n" + (now - 2000) + "\n"
                + (now - 1000) + "\n", StandardCharsets.US_ASCII);
    }

    private static ShadowPullSettings pullSettings(final ReplicaRig r) {
        final List<Member> ms = r.rig.members;
        return ShadowPullSettings.parse(Map.of(
                ShadowPullSettings.SOURCES, r.rig.sources(ms.get(0), ms.get(1), ms.get(2)),
                ShadowPullSettings.PERIOD_MS, "1000",
                ShadowPullSettings.STARTUP_WAIT_MS, "20000")::get, null);
    }

    /**
     * Every member's replays held (the backup window storm), the supplier's live replay stopped: runs until the
     * backup counted {@code failures} more replay refusals, a restart or a refusal, or {@code maxMs}; a few seconds
     * more for the follower's detector; then releases. Returns the refusals counted.
     */
    private static long stormUntil(final ReplicaRig r, final long failures, final long maxMs) {
        final List<ReplicaBackupWindowTest.Hold> holds = new ArrayList<>();
        final int supplier = r.replica.events().supplierMemberId();
        try {
            for (final Member m : r.rig.live()) {
                if (m.id != supplier) {
                    holds.add(ReplicaBackupWindowTest.fill(r, m));
                }
            }
            final Member s = r.rig.members.get(supplier);
            r.rig.archive(s).stopAllReplays(r.rig.logRecordingId(s));
            holds.add(ReplicaBackupWindowTest.fill(r, s));
            final long f0 = r.replica.events().replayFailures();
            r.rig.await(() -> !r.replica.follower().liveRecordingAlive(), 30_000,
                    "the replica's live recording survived the supplier's replay stop");
            final long deadline = System.currentTimeMillis() + maxMs;
            while (System.currentTimeMillis() < deadline && r.restarts.isEmpty() && r.fatals.isEmpty()
                    && r.replica.events().replayFailures() - f0 < failures) {
                r.rig.pumpMs(200);
            }
            r.rig.pumpMs(3_000);
            final long counted = r.replica.events().replayFailures() - f0;
            assertTrue("the storm refused " + counted + " replays, wanted " + failures, counted >= failures
                    || !r.restarts.isEmpty());
            System.out.println("[rebootstrap] storm: " + counted + " replay refusals, restarts " + r.restarts);
            return counted;
        } finally {
            for (final ReplicaBackupWindowTest.Hold h : holds) {
                h.release(r);
            }
        }
    }

    /**
     * Two member snapshots, every member's log purged to the segment boundary below the second, which must lie past
     * {@code lStop}; with {@code withShadow} a genuine ALIGNED at the second snapshot position on member 1 (built
     * from member 1's log read from 0 out of the live copy {@code copy} on member 2: the members' pruner purges their
     * own logs after each snapshot). Returns that position.
     */
    private static long pruneBeyond(final ReplicaRig r, final long copy, final long lStop, final boolean withShadow) {
        r.tool.deposits(900);
        r.rig.snapshotAll();
        r.tool.deposits(300);
        final long pB = r.rig.snapshotAll();
        r.tool.deposits(60);
        final long boundary = pB / SEGMENT * SEGMENT;
        assertTrue("the purge boundary " + boundary + " lies past L.stop=" + lStop, boundary > lStop);
        final Member m1 = r.rig.members.get(1);
        final RecordedLog log = withShadow ? r.rig.captureCopy(m1, r.rig.members.get(2), copy) : null;
        for (final Member m : r.rig.live()) {
            assertEquals("member " + m.id + " log start after the purge", boundary, r.rig.purgeLogTo(m, boundary));
        }
        if (!withShadow) {
            return pB;
        }
        final TestShadowProducer.Written w = r.rig.producer(m1).produce(new Who(ProducerKind.FOLLOWER, 600 + (int)(pB
                % 100)), log, pB);
        assertTrue(w.toString(), w.ok());
        System.out.println("[rebootstrap] members purged to " + boundary + " past L.stop=" + lStop + "; ALIGNED at "
                + pB + " on member 1");
        return pB;
    }
}

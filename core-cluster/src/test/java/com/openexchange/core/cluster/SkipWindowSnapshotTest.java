// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.RecordingLog;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The DRY RUN (C14) of the skip-window snapshot story on 84b088b with {@code SNAPSHOT_MODE}
 * absent. It proves the setup the production test needs, before any code exists: a follower that restarts from a
 * snapshot at {@code P_cm} and has a shadow at {@code P_s} far above, replays a {@code ClusterAction(SNAPSHOT, 0)} frame
 * at {@code P_x} with {@code P_cm < P_x < P_s} in the SAME leadership term, and takes a snapshot there (the base takes a
 * FULL one: the {@code [SNAPSHOT]} line's {@code logPosition} lies inside the future skip window).
 *
 * <p>How the situation is made: three members take snapshots #1 (P_cm) and #2 (P_x); the follower's archive receives a
 * shadow at {@code P_s > P_x} (contract 11.0 producer); the follower is killed and its newest snapshot group (#2) is
 * invalidated in its {@code RecordingLog} exactly as {@code ClusterTool invalidate-latest-snapshot} does, which is the
 * operator step of the 7.3 runbook; the restart therefore starts from #1 and replays the action frame of #2 as a replay
 * of an earlier-term-equal action. W3 may instead take snapshot #2 while the follower is down; both reach the same
 * state.</p>
 *
 * <p>Expected RED until W2: only the path line ({@code restored path=SKIP}). The window itself
 * ({@code core_snapshot_in_skip_window_total}, a stub at P_x) is W3.</p>
 */
public class SkipWindowSnapshotTest {

    private static final int PORT_BASE = 22000;
    private static final Pattern LOG_POSITION = Pattern.compile("logPosition=(\\d+)");
    /**
     * No row kills a leader, so nothing here needs the rig's fast 1 s failure detection, and every row offers deposits
     * right after a snapshot: a runner pause of about a second there must not become an election that drops them.
     */
    private static final long LEADER_HEARTBEAT_TIMEOUT_MS = 5_000;

    @Test
    public void dryRunAFollowerSnapshotsAtASameTermActionInsideItsReplayWindow() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "rcs-s4-dry-skipwindow")) {
            rig.leaderHeartbeatTimeoutMs = LEADER_HEARTBEAT_TIMEOUT_MS;
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            System.out.println("[restore-dryrun] leader member " + leader.id + ", restarting follower " + f.id + ", client via member " + g.id);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(200);
            final long pCm = rig.snapshotAll();
            tool.deposits(40);
            final long pX = rig.snapshotAll();
            tool.deposits(40);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertTrue("two snapshots, P_cm < P_x: " + pCm + " " + pX, pCm < pX);

            // the shadow above P_x, from f's own committed log
            final RecordedLog log = rig.captureCommitted(f);
            final long[] pos = log.positions();
            final int ix = Arrays.binarySearch(pos, pX);
            assertTrue("P_x " + pX + " is a frame end of f's log", ix >= 0);
            assertTrue("frames exist above P_x", ix + 8 < pos.length);
            final long pS = pos[ix + 8];
            final Written w = rig.producer(f).produce(new Who(ProducerKind.FOLLOWER, 100), log, pS);
            assertTrue(w.toString(), w.ok());
            assertTrue("P_cm < P_x < P_s: " + pCm + " " + pX + " " + pS, pCm < pX && pX < pS);
            System.out.println("[restore-dryrun] P_cm=" + pCm + " P_x=" + pX + " P_s=" + pS + " shadow " + w);

            final int groupsBefore = rig.snapshots(f).size();
            assertEquals("f holds both snapshots", 2, groupsBefore);
            final long secondRecordingId = rig.snapshots(f).get(1)[1];
            rig.kill(f);
            try (RecordingLog recordingLog = new RecordingLog(f.clusterDir(), true)) {
                assertTrue("invalidate the newest snapshot group (ClusterTool invalidate-latest-snapshot)",
                        recordingLog.invalidateLatestSnapshot());
            }
            final List<long[]> valid = rig.snapshots(f);
            assertEquals("only snapshot #1 stays valid", 1, valid.size());
            assertEquals(pCm, valid.get(0)[0]);

            tool.deposits(20); // the kill window
            rig.tee.clear();
            rig.restart(f);
            final long readyMs = rig.awaitReady(f, 120_000);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertTrue(f.halts.toString(), f.halts.isEmpty());

            // the follower replayed the action frame of #2 and snapshotted there
            final List<String> lines = rig.tee.all("[SNAPSHOT] accounts=");
            final long lineAt = assertLineInWindow(lines, pX, pCm, pS);
            RestoreDrillDryRun.nc("dry-11.4-line-oracle", "a [SNAPSHOT] line at P_x", () -> assertLineInWindow(lines, pX + 32, pCm, pS));
            boolean newGroupAtPx = false;
            for (final long[] s : rig.snapshots(f)) {
                if (s[0] == pX && s[1] != secondRecordingId) {
                    newGroupAtPx = true;
                }
            }
            assertTrue("f's RecordingLog holds a NEW valid snapshot group at P_x (the invalidated one was recording "
                    + secondRecordingId + "): " + dump(rig.snapshots(f)), newGroupAtPx);
            System.out.println("[restore-dryrun] readyMs=" + readyMs + " snapshot line at P_x inside the window (" + pCm + ", " + pS
                    + "); image==leader image (" + ServiceProbe.image(f.service).length + " B); lines " + rig.tee.lines);
            assertTrue("ready within the budget, took " + readyMs, readyMs <= RestoreDrillDryRun.READY_BUDGET_MS);
            RestoreDrillDryRun.pathLine(rig.tee, "SKIP", "11.4");
        }
    }

    // ------------------------------------------------------------------------------------------ W3: the production rows

    /** What one 11.4 run leaves for its assertions. */
    private record Window(ShadowClusterRig rig, Member leader, Member f, Client tool, long pCm, long pX, long pS) { }

    /**
     * The 11.4 situation in stub mode: three members in shadow mode take stubs #1 (P_cm) and #2 (P_x); a shadow at P_s
     * above P_x lands in follower f's archive; f is killed, its #2 group invalidated, and it restarts with SNAPSHOT_MODE=off:
     * SKIP from P_s, and the replayed same-term action at P_x falls inside its skip window.
     */
    private static Window skipWindowStory(final ShadowClusterRig rig, final String... faultsOnRestart) throws Exception {
        for (final Member m : rig.members) {
            m.settings = StubStory.shadow();
        }
        rig.leaderHeartbeatTimeoutMs = LEADER_HEARTBEAT_TIMEOUT_MS;
        rig.bootAll();
        final Member leader = rig.awaitLeader(null);
        final Member f = rig.followerOtherThan(leader, null);
        final Member g = rig.followerOtherThan(leader, f);
        final Client admin = rig.connect(SessionClass.ADMIN, g);
        rig.adopt(admin);
        final Client tool = rig.connect(SessionClass.TOOL, g);
        tool.deposits(200);
        final long pCm = rig.snapshotAll();
        tool.deposits(40);
        final long pX = rig.snapshotAll();
        tool.deposits(40);
        rig.awaitImagesEqual(f, leader, 60_000);
        final RecordedLog log = rig.captureCommitted(f);
        final long[] pos = log.positions();
        final int ix = Arrays.binarySearch(pos, pX);
        assertTrue("P_x " + pX + " is a frame end of f's log", ix >= 0 && ix + 8 < pos.length);
        // eight deposits past P_x, not eight frames: timer frames alone in (P_x, P_s] make a double apply invisible
        final long pS = afterSessionMessages(log, pX, 8);
        final Written w = rig.producer(f).produce(new Who(ProducerKind.FOLLOWER, 300), log, pS);
        assertTrue(w.toString(), w.ok());
        assertTrue("P_cm < P_x < P_s: " + pCm + " " + pX + " " + pS, pCm < pX && pX < pS);
        rig.kill(f);
        try (RecordingLog recordingLog = new RecordingLog(f.clusterDir(), true)) {
            assertTrue("invalidate the P_x group", recordingLog.invalidateLatestSnapshot());
        }
        tool.deposits(20);
        f.settings = StubStory.off(); // SNAPSHOT_MODE=off on the restarting node
        f.faults = faultsOnRestart;
        rig.tee.clear();
        rig.restart(f);
        rig.awaitReady(f, 120_000);
        rig.awaitImagesEqual(f, leader, 60_000);
        assertTrue(f.halts.toString(), f.halts.isEmpty());
        System.out.println("[11.4] P_cm=" + pCm + " P_x=" + pX + " P_s=" + pS + " lines " + rig.tee.lines);
        return new Window(rig, leader, f, tool, pCm, pX, pS);
    }

    /** The restart of f from its newest group (the one written at P_x) must end equal to the leader. */
    private static void restartFromPx(final Window w, final String label) {
        w.rig().kill(w.f());
        w.tool().deposits(10);
        w.f().faults = new String[0];
        w.rig().tee.clear();
        w.rig().restart(w.f());
        w.rig().awaitReady(w.f(), 120_000);
        try {
            w.rig().awaitImagesEqual(w.f(), w.leader(), 20_000);
        } catch (final AssertionError differs) {
            throw new AssertionError("final image differs from baseline after restore from P_x (double apply) [" + label
                    + "]: " + differs.getMessage() + "; restore lines " + w.rig().tee.lines);
        }
    }

    @Test
    public void inTheSkipWindowTheSnapshotIsAStubEvenWithSnapshotModeOff() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(22100, "rcs-s4-11.4-window")) {
            final Window w = skipWindowStory(rig);
            RestoreDrillDryRun.requirePathLine(rig.tee, "SKIP P_s=" + w.pS(), "11.4");
            String atPx = null;
            for (final String l : rig.tee.all("[SNAPSHOT] accounts=")) {
                if (l.startsWith("[SNAPSHOT] accounts=") && l.contains("logPosition=" + w.pX() + " ")) {
                    atPx = l;
                }
            }
            assertTrue("a [SNAPSHOT] line at P_x: " + rig.tee.lines, atPx != null);
            assertTrue("the snapshot at P_x is a stub inside the window although SNAPSHOT_MODE=off: " + atPx,
                    atPx.contains("kind=stub inSkipWindow=true"));
            assertEquals("core_snapshot_in_skip_window_total", 1, ServiceProbe.rcs(w.f().service,
                    "core_snapshot_in_skip_window_total"));
            assertEquals("the window closed once at the first frame past P_s (C4)", 1, ServiceProbe.rcs(w.f().service,
                    "core_skip_window_closed_total"));
            assertFalse("skipWindow.active() is false after the first frame past P_s",
                    ServiceProbe.skipWindowActive(w.f().service));
            boolean groupAtPx = false;
            for (final long[] e : rig.snapshots(w.f())) {
                groupAtPx |= e[0] == w.pX();
            }
            assertTrue("f holds a new valid group at P_x", groupAtPx);
            System.out.println("GREEN 11.4 window: " + atPx);
            restartFromPx(w, "GREEN");
            System.out.println("GREEN 11.4 restart from the P_x stub group: " + rig.tee.all("restored"));
            RestoreDrillDryRun.requirePathLine(rig.tee, "SKIP P_s=" + w.pS(), "11.4 restart from P_x");
        }
    }

    @Test
    public void controlTheWindowClauseOffDoublesTheWindowFromAFullImageAtPx() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(22200, "rcs-s4-11.4-noclause")) {
            final Window w = skipWindowStory(rig, "windowClauseOff");
            final List<String> atPx = rig.tee.all("logPosition=" + w.pX() + " ").stream()
                    .filter(l -> l.startsWith("[SNAPSHOT] accounts=")).toList();
            assertTrue("with the clause off the snapshot at P_x is FULL: " + atPx, atPx.toString().contains("kind=full"));
            RestoreDrillDryRun.nc("11.4-window-clause-off", "final image differs from baseline after restore from P_x (double apply)",
                    () -> restartFromPx(w, "window clause off"));
        }
    }

    /** The frame end of the {@code n}-th session message after {@code from} (each one a deposit in these stories). */
    private static long afterSessionMessages(final RecordedLog log, final long from, final int n) {
        int seen = 0;
        for (final com.openexchange.cluster.testkit.LogFrame frame : log.frames()) {
            if (frame.position() > from && frame.kind() == com.openexchange.cluster.log.FrameKind.SESSION_MESSAGE
                    && ++seen == n) {
                return frame.position();
            }
        }
        throw new AssertionError("fewer than " + n + " session messages after " + from + " in f's log");
    }

    /** The {@code [SNAPSHOT]} line whose logPosition is {@code pX}, and that it lies strictly inside {@code (pCm, pS)}. */
    private static long assertLineInWindow(final List<String> lines, final long pX, final long pCm, final long pS) {
        long lineAt = -1;
        for (final String line : lines) {
            final Matcher m = LOG_POSITION.matcher(line);
            if (m.find() && Long.parseLong(m.group(1)) == pX) {
                lineAt = pX;
            }
        }
        assertTrue("a [SNAPSHOT] line at P_x=" + pX + " from the replayed action; lines " + lines, lineAt == pX);
        assertTrue("the [SNAPSHOT] line position lies in (P_cm, P_s): " + pCm + " < " + lineAt + " < " + pS,
                pCm < lineAt && lineAt < pS);
        return lineAt;
    }

    private static String dump(final List<long[]> snapshots) {
        final StringBuilder sb = new StringBuilder();
        for (final long[] s : snapshots) {
            sb.append("{pos=").append(s[0]).append(" rec=").append(s[1]).append(" term=").append(s[2]).append("} ");
        }
        return sb.toString();
    }
}

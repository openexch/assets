// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaConfig;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import org.agrona.CloseHelper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Backup window row: a transient replay-refusal storm on EVERY member. Each member
 * archive's {@code maxConcurrentReplays} (10, CFG:151) is held by replays of its own log the test opens and never
 * drains, so the backup's {@code boundedReplay} is refused with MAX_REPLAYS on whichever member it queries; releasing
 * them lifts the fault.
 *
 * <ul>
 *   <li>with the default cool-down (12 000) the backup re-attaches within 3 x cool-down after the release, and
 *       {@code backup_auth_rejected_total} stays 0 (one query per member per backup window);</li>
 *   <li>no replay storm on the replica's own archive while the backup cycles: the follower opens no replay of L
 *       while L's counter is dead and L's stop equals what it consumed ({@code replica_attach_attempts_total}
 *       bounded);</li>
 *   <li>the storm is visible: {@code backup_replay_failures_total} grows (a re-bootstrap detector's input).</li>
 * </ul>
 *
 * <p>Control: cool-down 1 000 -> {@code backup locked out: <n> AUTHENTICATION_REJECTED, no response in 60 s} (proves
 * the window rule on a real rig: the per-member window answers one {@code -1} request per 11 s, a fast reset loop
 * renews it).</p>
 */
public class ReplicaBackupWindowTest {

    private static final int PORT_BASE = 16000; // members 16000..16200, replica slots 16300, 16400
    private static final int HOLD_STREAM = 9_700;
    private static final long STORM_MS = 40_000;
    private static final long LOCKOUT_WINDOW_MS = 60_000;

    @Test(timeout = 900_000)
    public void aReplayRefusalStormEndsInAReattachWithoutLockout() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-window")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);

            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final long cooldown = r.replica.config().backupCooldownMs();
            assertEquals("the default cool-down", ReplicaConfig.DEFAULT_BACKUP_COOLDOWN_MS, cooldown);
            final Storm storm = storm(r);
            System.out.println("[window] window: " + storm);
            assertTrue("the storm must refuse replays (backup_replay_failures_total +" + storm.replayFailures + ")",
                    storm.replayFailures >= 2);
            assertTrue("replay attempts on the replica archive during the storm: " + storm.attachAttempts,
                    storm.attachAttempts <= 2);
            final long lift = System.currentTimeMillis();
            awaitReattach(r, lift);
            final long tookMs = System.currentTimeMillis() - lift;
            assertTrue("re-attach took " + tookMs + " ms > 3 x cool-down " + 3 * cooldown, tookMs <= 3 * cooldown);
            assertEquals("backup_auth_rejected_total", 0, r.metric("shadow_replica_backup_auth_rejected_total"));
            r.traffic(20, 4);
            r.awaitCaughtUp(60_000);
            assertTrue("no fatal: " + r.fatals, r.fatals.isEmpty());
            System.out.println("GREEN window reattachMs=" + tookMs + " replayFailures=" + storm.replayFailures
                    + " attachAttemptsDuringStorm=" + storm.attachAttempts + " reattachTotal="
                    + r.replica.follower().reattachTotal());

            // control: a 1 s cool-down re-queries members inside their backup window and locks itself out
            r.stopReplica();
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-nc").backupCooldownMs(1_000).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(60_000);
            final Storm ncStorm = storm(r);
            System.out.println("[window] window control: " + ncStorm);
            final long ncLift = System.currentTimeMillis();
            RestoreDrillDryRun.nc("COOLDOWN_1000", "backup locked out", () -> awaitReattach(r, ncLift));
        }
    }

    record Storm(long replayFailures, long attachAttempts, long authRejected, long queries) {
    }

    /** Holds every member's replays, forces a backup reset on the supplier, lets the storm run, then releases. */
    private static Storm storm(final ReplicaRig r) {
        final List<Hold> holds = new ArrayList<>();
        final int supplier = r.replica.events().supplierMemberId();
        for (final Member m : r.rig.live()) {
            if (m.id != supplier) {
                holds.add(fill(r, m));
            }
        }
        // the supplier: end the replica's live replay with ours, then take every slot before the next query
        final Member s = r.rig.members.get(supplier);
        r.rig.archive(s).stopAllReplays(r.rig.logRecordingId(s));
        holds.add(fill(r, s));
        final long failures0 = r.replica.events().replayFailures();
        final long attempts0 = r.replica.follower().attachAttemptsTotal();
        final long rejected0 = r.replica.events().authRejected();
        final long queries0 = r.replica.events().queries();
        r.rig.await(() -> !r.replica.follower().liveRecordingAlive(), 30_000,
                "the replica's live recording survived the supplier's replay stop");
        r.rig.pumpMs(STORM_MS);
        final Storm out = new Storm(r.replica.events().replayFailures() - failures0,
                r.replica.follower().attachAttemptsTotal() - attempts0,
                r.replica.events().authRejected() - rejected0, r.replica.events().queries() - queries0);
        for (final Hold h : holds) {
            h.release(r);
        }
        return out;
    }

    /** Open replays of {@code m}'s log (following live, never drained) until its archive refuses one. */
    private static Hold fill(final ReplicaRig r, final Member m) {
        final AeronArchive a = r.rig.archive(m);
        final Subscription sink = r.rig.aeron(m).addSubscription("aeron:ipc", HOLD_STREAM);
        final long rec = r.rig.logRecordingId(m);
        final Hold h = new Hold(m, sink);
        for (int i = 0; i < 32; i++) {
            try {
                a.startReplay(rec, 0, AeronArchive.NULL_LENGTH, "aeron:ipc", HOLD_STREAM);
                h.held++;
            } catch (final ArchiveException refused) {
                h.refusal = refused.getMessage();
                break;
            }
        }
        if (h.refusal == null) {
            throw new AssertionError("member " + m.id + " archive accepted " + h.held + " replays and never refused");
        }
        System.out.println("[window] member " + m.id + ": holding " + h.held + " replays (" + h.refusal + ")");
        return h;
    }

    static final class Hold {
        final Member m;
        final Subscription sink;
        int held;
        String refusal;

        Hold(final Member m, final Subscription sink) {
            this.m = m;
            this.sink = sink;
        }

        void release(final ReplicaRig r) {
            r.rig.archive(m).stopAllReplays(r.rig.logRecordingId(m));
            CloseHelper.quietClose(sink);
        }
    }

    /** A backup response after {@code liftMs}, then L live again with a successful re-attach poll. */
    private static void awaitReattach(final ReplicaRig r, final long liftMs) {
        final long responses0 = r.replica.events().responses();
        final long reattach0 = r.replica.follower().reattachTotal();
        final long rejected0 = r.replica.events().authRejected();
        final long deadline = liftMs + LOCKOUT_WINDOW_MS;
        while (System.currentTimeMillis() < deadline) {
            if (r.replica.events().responses() > responses0 && r.replica.follower().liveRecordingAlive()
                    && r.replica.follower().reattachTotal() > reattach0) {
                return;
            }
            r.rig.pumpMs(100);
        }
        throw new AssertionError("backup locked out: " + (r.replica.events().authRejected() - rejected0)
                + " AUTHENTICATION_REJECTED, no response in " + LOCKOUT_WINDOW_MS / 1000 + " s (responses "
                + r.replica.events().responses() + ", live " + r.replica.follower().liveRecordingAlive() + ")");
    }
}

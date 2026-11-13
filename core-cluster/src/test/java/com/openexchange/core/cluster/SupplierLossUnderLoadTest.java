// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Supplier loss under order load (nightly): the member the replica's backup names as its supplier
 * ({@code shadow_replica_supplier_member_id}, a FOLLOWER) is stopped while the generator runs. Pass:
 * the backup leaves BACKING_UP ({@code shadow_replica_backup_state} reads RESET_BACKUP or BACKUP_QUERY after the stop),
 * a new supplier id, the applied position passes the commit position at the stop within
 * {@code core.lag.peerloss.bound.s} (the bytes after the stop reach the replica only through the new supplier), and the
 * next ALIGNED is VERIFIED and equal to the members' image. Recorded, not asserted (measured on Aeron 1.53.1):
 * {@code shadow_replica_backup_resets_total} (it counts the backup's possible-failure events, which a supplier stop
 * does not raise), {@code shadow_replica_reader_reattach_total} and the local log recording L (the backup extends the
 * same L from the new supplier, so the reader has nothing to re-attach), and the lag through the stop (it grows with
 * the load until the new supplier answers). The stopped member is restarted before the end.
 *
 * <p>Control in the row: a second replica with {@code REPLICA_BACKUP_COOLDOWN_MS=30000} exceeds the bound,
 * {@code supplier loss recovery <n> s above <bound> s}. Vacuity: a supplier id unchanged after the kill fails with
 * {@code killed member was not the supplier}.</p>
 */
public class SupplierLossUnderLoadTest {

    private static final int PORT_BASE = 57600; // members 57600..57800, the replica slots 57900, 58000
    private static final long CONTROL_COOLDOWN_MS = 30_000;

    @Test(timeout = 1_800_000)
    public void theReplicaMovesToTheOtherFollowerWithinTheBound() throws Exception {
        try (ReplicaLoadRig x = new ReplicaLoadRig(PORT_BASE, "supplier-loss", 3, "1m", 0)) {
            x.bootMembers();
            x.startReplica(x.builder(ReplicaRig.SLOT, "replica").build());
            x.startLoad();
            x.rig.pumpMs(5_000);
            final Map<String, Object> extra = new LinkedHashMap<>();

            final long recoveryMs = supplierLoss(x, "default cool-down");
            extra.put("recovery_ms", recoveryMs);
            if (recoveryMs > ReplicaLoadRig.PEERLOSS_BOUND_S * 1_000) {
                throw new AssertionError(recoveryText(recoveryMs));
            }
            final long p = x.snapshot();
            x.assertAlignedEqual(p);
            // the lag through the stop is recorded, not bounded by the term: until the new supplier answers it grows
            // with the load (the row's bound is the recovery time)
            x.sampler.assertObserved();
            extra.put("window_with_the_stop", x.sampler.summary());
            restartKilled(x);

            // control: a 30 s cool-down exceeds the bound
            x.sampler.close();
            x.r.stopReplica();
            x.r.windowGap();
            x.startReplica(x.builder(ReplicaRig.SLOT + 1, "replica-cooldown")
                    .backupCooldownMs(CONTROL_COOLDOWN_MS).build());
            final long ncMs = supplierLoss(x, "cool-down " + CONTROL_COOLDOWN_MS + " ms");
            extra.put("control_cooldown_ms", CONTROL_COOLDOWN_MS);
            extra.put("control_recovery_ms", ncMs);
            RestoreDrillDryRun.nc("COOLDOWN_30000", "supplier loss recovery", () -> {
                if (ncMs > ReplicaLoadRig.PEERLOSS_BOUND_S * 1_000) {
                    throw new AssertionError(recoveryText(ncMs));
                }
            });
            restartKilled(x);

            extra.put("generator", x.endLoad());
            x.sampler.writeSummary(extra);
            assertTrue("no fatal: " + x.r.fatals, x.r.fatals.isEmpty());
            System.out.println("GREEN supplier loss under load: recovery " + recoveryMs + " ms (bound "
                    + ReplicaLoadRig.PEERLOSS_BOUND_S + " s), control " + ncMs + " ms");
        }
    }

    private static String recoveryText(final long ms) {
        return "supplier loss recovery " + ms / 1000.0 + " s above " + ReplicaLoadRig.PEERLOSS_BOUND_S + " s";
    }

    private static Member killed;

    /**
     * Stops the supplier and measures, from the stop, until the applied position passes the commit position at the
     * stop and the supplier id changed; asserts the backup state left BACKING_UP and records the reset and the
     * re-attach counters (neither moves on a supplier stop). Returns the milliseconds.
     */
    private static long supplierLoss(final ReplicaLoadRig x, final String label) {
        final var events = x.r.replica.events();
        final int supplier = events.supplierMemberId();
        if (supplier < 0) {
            throw new AssertionError("no supplier named before the kill (" + label + ")");
        }
        final Member m = x.rig.members.get(supplier);
        if (m.killed || m.role() == Cluster.Role.LEADER) {
            throw new AssertionError("the supplier " + supplier + " is not a live follower (roles " + x.rig.roles()
                    + "; " + label + ")");
        }
        final long resets0 = events.resets();
        final long reattach0 = x.r.replica.follower().reattachTotal();
        final long l0 = events.liveRecordingId();
        final long commitAtKill = x.commit();
        // shadow_replica_backup_state (ClusterBackup.State codes) as the replica's /metrics reads it, from the kill on
        final java.util.Set<Long> states = new java.util.TreeSet<>();
        final Runnable stateSampler = () -> {
            if (x.r.replica != null) {
                states.add(x.r.metric("shadow_replica_backup_state"));
            }
        };
        x.rig.duringAwait.add(stateSampler);
        final long t0 = System.nanoTime();
        x.kill(m);
        killed = m;
        final long limitMs = 3 * ReplicaLoadRig.PEERLOSS_BOUND_S * 1_000 + x.r.replica.config().backupCooldownMs()
                + 30_000;
        x.rig.await(() -> x.r.replica.follower().appliedPosition() > commitAtKill
                        && events.supplierMemberId() >= 0 && events.supplierMemberId() != supplier, limitMs,
                () -> "the replica never resumed after the supplier " + supplier + " stopped (" + label + "): applied "
                        + x.r.replica.follower().appliedPosition() + " commit at the kill " + commitAtKill
                        + " supplier " + events.supplierMemberId() + " " + x.r.probe().detail());
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        x.rig.duringAwait.remove(stateSampler);
        final int next = events.supplierMemberId();
        if (next == supplier) {
            throw new AssertionError("killed member was not the supplier: " + supplier);
        }
        assertNotEquals("the new supplier is the stopped member", supplier, next);
        // the backup went back through its reset or its query to find the new supplier (RESET_BACKUP 6, BACKUP_QUERY 0)
        assertTrue("the backup never left BACKING_UP after the supplier stopped: states " + states,
                states.contains(6L) || states.contains(0L));
        // recorded, not asserted (measured): shadow_replica_backup_resets_total counts the backup's possible-failure
        // events, which a supplier stop does not raise; the reader re-attaches only when the local log recording L
        // changes, and the backup extends the same L from the new supplier
        final long liveRecording = events.liveRecordingId();
        System.out.println("[lag] supplier loss (" + label + "): supplier " + supplier + " -> " + next
                + ", backup states seen " + states + ", backup_resets_total " + resets0 + " -> " + events.resets()
                + ", reader_reattach_total " + reattach0 + " -> " + x.r.replica.follower().reattachTotal()
                + ", L before " + l0 + " after " + liveRecording
                + ", applied past the commit at the kill after " + ms + " ms");
        return ms;
    }

    private static void restartKilled(final ReplicaLoadRig x) {
        if (killed != null && killed.killed) {
            final long commitAtRestart = x.commit();
            x.restart(killed);
            final Member back = killed;
            x.rig.await(() -> back.cfg.consensusModuleContext().commitPositionCounter().get() >= commitAtRestart
                    && back.role() == Cluster.Role.FOLLOWER, 120_000, "member " + back.id + " never rejoined");
        }
        killed = null;
    }
}

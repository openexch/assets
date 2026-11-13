// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.ShadowPullSettings;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The replica re-bootstrap under order load (nightly): three members in {@code shadow} mode with 1 MiB log segments;
 * the replica follows and writes its ALIGNED at two P_cm, then it is stopped (its L.stop recorded); under the load the
 * designated follower writes its ALIGNED at two more P_cm and the members' logs are purged past L.stop below the newest
 * P_cm (the operator stand-in, as {@link DualProducerLiveTest}). The replica restarts with the members as its pull
 * sources: the supplier refuses its live log replays below the start, it pulls the follower's ALIGNED above L.stop and
 * restarts with code 3 ({@code shadow_replica_rebootstrap_total 1}); the next start restores LOCAL_SHADOW from the
 * pulled shadow, follows and its next ALIGNED is VERIFIED and equal to the members' image.
 *
 * <p>Control in the row, on a copy of the stopped replica's directory: {@code SHADOW_REBOOTSTRAP_REPLAY_FAILURES=0}
 * (the kit's detector-only setting: it counts the refusals and never acts) counts three replay-below-start refusals
 * and the row's oracle, the restart, fails with {@code re-bootstrap not taken within <n> ms}; its
 * {@code rebootstrap_not_taken_total} and {@code /ready} are recorded (measured: the not-taken reading belongs to the
 * branch that found no shadow above L.stop, not to this setting).</p>
 */
public class ReBootstrapUnderLoadTest {

    private static final int PORT_BASE = 60000; // members 60000..60200, the replica slot 60300
    /**
     * 1 MiB log segments on the 1m term, so the members' logs purge within the row. Not the 64k term of the unloaded
     * rows: under the load the designated follower's window guard skips every ALIGNED on a 64k term (W = 32k), and the
     * floor then never has its second witness.
     */
    private static final int SEGMENT = 1024 * 1024;
    private static final long CYCLE_MS = 300_000;
    /** The control's wait for a decision after three refusals were counted (the default decides at the third). */
    private static final long CONTROL_WINDOW_MS = 60_000;

    @Test(timeout = 2_400_000)
    public void aReplicaPrunedPastUnderLoadReBootstrapsAndFollows() throws Exception {
        try (ReplicaLoadRig x = new ReplicaLoadRig(PORT_BASE, "rebootstrap-load", 3, "1m", SEGMENT)) {
            x.bootMembers();
            x.startReplica(x.builder(ReplicaRig.SLOT, "replica").build());
            x.startLoad();
            x.rig.pumpMs(3_000);
            for (int i = 0; i < 2; i++) {
                x.assertAlignedEqual(x.snapshot());
            }
            x.sampler.assertInsideTheTerm();
            final Map<String, Object> extra = new LinkedHashMap<>();
            extra.put("steady_window", x.sampler.summary());
            x.sampler.close();

            // the replica down: the designated follower's ALIGNED at two more P_cm, then the purge past L.stop
            final long lStop = x.r.replica.follower().recordedPosition();
            x.r.stopReplica();
            final Member leader = x.rig.awaitLeader(null);
            final Member d = x.rig.members.get(ShadowProducerRoleTest.designated(x.rig, leader.id));
            long pB = -1;
            for (int i = 0; i < 2; i++) {
                x.rig.pumpMs(4_000);
                final long p = x.snapshot();
                x.rig.await(() -> ShadowProducerRoleTest.written(x.rig, d, p) != null, 60_000,
                        "the designated follower's ALIGNED at " + p);
                pB = p;
            }
            final long boundary = pB / SEGMENT * SEGMENT;
            assertTrue("the purge boundary " + boundary + " lies past L.stop=" + lStop, boundary > lStop);
            x.rig.pumpMs(1_000);
            for (final Member m : x.rig.live()) {
                final long start = x.rig.purgeLogTo(m, boundary);
                assertTrue("member " + m.id + " log start " + start + " at or past " + boundary, start >= boundary);
            }
            System.out.println("[lag] members purged to " + boundary + " past L.stop=" + lStop + "; F" + d.id
                    + " ALIGNED at " + pB);
            final ShadowPullSettings pull = ShadowPullSettings.parse(Map.of(
                    ShadowPullSettings.SOURCES, x.rig.sources(x.rig.members.toArray(new Member[0])),
                    ShadowPullSettings.PERIOD_MS, "1000",
                    ShadowPullSettings.STARTUP_WAIT_MS, "20000")::get, null);
            copyTree(x.r.replicaBase("replica").toPath(), x.r.replicaBase("replica-detector-only").toPath());

            // control: the detector without the decision (SHADOW_REBOOTSTRAP_REPLAY_FAILURES=0 counts the refusals and
            // never acts): the row's oracle, a restart with code 3, must not be met
            x.r.windowGap();
            x.r.startReplica(x.builder(ReplicaRig.SLOT, "replica-detector-only").pullSettings(pull)
                    .rebootstrapReplayFailures(0).build());
            x.r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            x.rig.await(() -> x.r.metric("shadow_replica_backup_replay_below_start_total") >= 3, CYCLE_MS,
                    () -> "the detector-only replica never counted three replay-below-start refusals: "
                            + x.r.metric("shadow_replica_backup_replay_below_start_total"));
            RestoreDrillDryRun.nc("REPLAY_FAILURES_0", "re-bootstrap not taken within",
                    () -> awaitRestart(x, CONTROL_WINDOW_MS));
            final String controlText = "replay_below_start_total "
                    + x.r.metric("shadow_replica_backup_replay_below_start_total") + ", rebootstrap_total "
                    + x.r.metric("shadow_replica_rebootstrap_total") + ", rebootstrap_not_taken_total "
                    + x.r.metric("shadow_replica_rebootstrap_not_taken_total") + ", /ready " + String.join(" ", x.ready());
            System.out.println("[lag] detector-only control: " + controlText);
            extra.put("control", controlText);
            x.r.stopReplica();

            // the re-bootstrap decision
            x.r.windowGap();
            x.r.startReplica(x.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
            x.r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            awaitRestart(x, CYCLE_MS);
            assertTrue("no fatal: " + x.r.fatals, x.r.fatals.isEmpty());
            final String restart = x.r.restarts.get(0);
            assertTrue(restart, restart.startsWith("3 replica re-bootstrap: members pruned past L.stop="));
            ReplicaReBootstrapTest.assertPulledBeforeTheRestart(x.r, restart, pB);
            assertEquals("rebootstrap_total", 1, x.r.metric("shadow_replica_rebootstrap_total"));
            final long belowStart = x.r.metric("shadow_replica_backup_replay_below_start_total");
            assertTrue("replay-below-start refusals before the decision: " + belowStart, belowStart >= 3);
            extra.put("restart", restart);
            extra.put("replay_below_start_total", belowStart);
            x.r.stopReplica();

            // the second life
            x.r.windowGap();
            x.r.startReplica(x.builder(ReplicaRig.SLOT, "replica").pullSettings(pull).build());
            x.r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, x.r.replica.readiness().restorePath());
            assertEquals("restored from the pulled shadow", pB, x.r.replica.follower().restoredPosition());
            x.r.awaitCaughtUp(120_000);
            final long next = x.snapshot();
            x.assertAlignedEqual(next);
            extra.put("second_life_restored", pB);
            extra.put("next_aligned", next);
            extra.put("generator", x.endLoad());
            x.sampler.writeSummary(extra);
            System.out.println("GREEN re-bootstrap under load: " + restart + "; LOCAL_SHADOW at " + pB
                    + ", the next ALIGNED at " + next + " equal");
        }
    }

    /** The re-bootstrap decision: {@code host.restart} (code 3 in production) or a fatal, within {@code ms}. */
    private static void awaitRestart(final ReplicaLoadRig x, final long ms) {
        x.rig.await(() -> !x.r.restarts.isEmpty() || !x.r.fatals.isEmpty(), ms,
                () -> "re-bootstrap not taken within " + ms + " ms: rebootstrap_total "
                        + x.r.metric("shadow_replica_rebootstrap_total") + ", replay_below_start_total "
                        + x.r.metric("shadow_replica_backup_replay_below_start_total") + ", " + x.r.probe().detail());
    }

    private static void copyTree(final Path from, final Path to) {
        try (java.util.stream.Stream<Path> walk = Files.walk(from)) {
            for (final Path p : (Iterable<Path>)walk::iterator) {
                final Path target = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

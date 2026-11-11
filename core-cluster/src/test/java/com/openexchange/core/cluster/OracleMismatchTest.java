// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.shadow.ShadowKind;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The live oracle against a forged MEMBER snapshot, on a real three-member core with the in-JVM replica (the mirror
 * of the mutated-replica row of {@link ReplicaAlignmentTrustTest}). The replica is VERIFIED from genesis; right after
 * a SNAPSHOT toggle, before the backup's next query, the test rewrites the service snapshot of every follower (the
 * FOLLOWER source can only retrieve from a follower) in its archive segment: one payload byte flipped, the envelope
 * re-sealed, same length, genesis untouched. The backup retrieves the forged copy; the held ALIGNED compares
 * DIFFERENT and the replica HOLDS:
 *
 * <ul>
 *   <li>{@code shadow_oracle_mismatch_total 1}, {@code shadow_oracle_mismatch_position P}, no other genesis, nothing
 *       unreadable; {@code /ready} 503 whose readiness field is exactly {@code recovery=fenced:oracle mismatch at <P>:
 *       first difference at byte <n> of <len>}; no ALIGNED shadow at P; the process alive after 2 x {@code REPLICA_VERIFY_WAIT_MS};
 *       {@code shadow_log_purge_position} frozen;</li>
 *   <li>the dump: {@code report.txt} with both digests, {@code replica.image} equal to the unforged leader's image at
 *       P (the member is the odd one, and the replica decided nothing);</li>
 *   <li>the pin, with {@code REPLICA_RETRIEVED_SETS_KEEP=1}: one more member snapshot is retrieved after the latch and
 *       the disputed set at P is still present beside it ({@code shadow_retrieved_sets == 2});</li>
 *   <li>a restart on the same {@code BASE_DIR} re-latches at P with the same readiness text and the report gains
 *       {@code re-latched at}.</li>
 * </ul>
 *
 * <p>Controls: {@code HOLD_OFF} (the exit-1 fence of before) -> {@code the process exited before the metrics were
 * read}; {@code ALIGN_COMPARE_OFF} -> {@code /ready 200 after a forged member snapshot at <P>}; a TORN forgery (the
 * CRC not re-sealed) must read UNREADABLE and recover at the next good set, else {@code torn member copy counted as a
 * mismatch}; {@code MISMATCH_PIN_OFF} -> {@code disputed set at <P> purged after a newer set arrived}.</p>
 */
public class OracleMismatchTest {

    private static final int PORT_BASE = 43200;
    /** The smallest verify wait the rig's backup cycle allows (2 x 12 000 + the 12 000 cool-down). */
    private static final long VERIFY_WAIT_MS = 2 * 12_000 + ReplicaConfig.DEFAULT_BACKUP_COOLDOWN_MS;
    private static final long LATCH_WAIT_MS = 120_000;
    private static final int FORGE_ATTEMPTS = 3;

    @Test(timeout = 2_400_000)
    public void aForgedMemberSnapshotHoldsTheReplicaWithItsEvidence() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-oracle-mismatch")) {
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 4);

            // ------------------------------------------------------------------------------------------ the hold
            start(r, "replica", 1, ReplicaRig.off());
            final long[] forged = forgeUntilLatched(r, leader, true);
            final long p = forged[0];
            final int len = (int)forged[1];
            final int at = SnapshotEnvelope.payloadOffset() + 8;
            final String text = "oracle mismatch at " + p + ": first difference at byte " + at + " of " + len;
            assertEquals("hold reason", List.of(text), r.holds);
            assertEquals("mismatch counted once", 1, r.metric("shadow_oracle_mismatch_total"));
            assertEquals("mismatch position", p, r.metric("shadow_oracle_mismatch_position"));
            assertEquals("latched", 1, r.metric("shadow_oracle_mismatch_latched"));
            assertEquals("a same-genesis forgery is not another history", 0,
                    r.metric("shadow_oracle_other_genesis_total"));
            assertEquals("a re-sealed forgery opens", 0, r.metric("shadow_oracle_unreadable_total"));
            final String[] ready = r.httpReady();
            assertEquals("/ready status while held", "503", ready[0]);
            assertReadyFenced(ready, text);
            assertTrue("no ALIGNED shadow at the disputed position " + p,
                    !r.verified(ShadowKind.ALIGNED).contains(p));

            // alive, frozen, nothing written, for 2 x the verify wait while the members move on
            final long purgeAt = r.metric("shadow_log_purge_position");
            final List<Long> shadowsAtLatch = r.allShadowPositions();
            final long end = System.currentTimeMillis() + 2 * VERIFY_WAIT_MS;
            while (System.currentTimeMillis() < end) {
                r.traffic(10, 2);
                r.rig.pumpMs(3_000);
            }
            if (!r.fatals.isEmpty()) {
                throw new AssertionError("the process exited before the metrics were read: " + r.fatals);
            }
            assertEquals("/metrics still served while held", 1, r.metric("shadow_oracle_mismatch_total"));
            assertTrue("held for at least 2 x the verify wait: " + r.metric("shadow_oracle_hold_ms"),
                    r.metric("shadow_oracle_hold_ms") >= 2 * VERIFY_WAIT_MS);
            assertEquals("the log purge point is frozen while held", purgeAt, r.metric("shadow_log_purge_position"));
            assertEquals("nothing written while held", shadowsAtLatch, r.allShadowPositions());
            assertEquals("still 503 after 2 x the verify wait", "503", r.httpReady()[0]);

            // the dump: both digests; the replica's image is the unforged leader's
            final File dir = new File(r.replicaBase("replica"), "mismatch/" + p);
            final String report = Files.readString(new File(dir, "report.txt").toPath(), StandardCharsets.UTF_8);
            assertTrue("report names both digests: " + report, report.contains("replica_sha256=")
                    && report.contains("member_sha256="));
            assertTrue("report position: " + report, report.contains("position=" + p + "\n"));
            assertEquals("the dumped member image is the forged copy",
                    Arrays.hashCode(r.retrievedCopy("replica", p)),
                    Arrays.hashCode(Files.readAllBytes(new File(dir, "member.image").toPath())));
            final byte[] replicaImage = Files.readAllBytes(new File(dir, "replica.image").toPath());
            ReplicaRig.assertPayloadEqualsMember(p, replicaImage, r.memberImage(leader.id, p));
            System.out.println("[oracle] held at " + p + ": " + text + "; the dumped replica image equals the unforged"
                    + " leader's (" + replicaImage.length + " B); report:\n" + report);

            // the pin: K=1, a newer set arrives, the disputed set stays
            assertPinned(r, "replica", p);

            // a restart on the same BASE_DIR re-latches at P
            r.stopReplica();
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(VERIFY_WAIT_MS).retrievedSetsKeep(1)
                    .build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.rig.await(() -> !r.holds.isEmpty() || !r.fatals.isEmpty(), 4 * LATCH_WAIT_MS,
                    () -> "the restarted replica never re-latched at " + p + ": " + r.probe().detail());
            assertEquals("re-latched with the same text", List.of(text), r.holds);
            assertReadyFenced(r.httpReady(), text);
            final String again = Files.readString(new File(dir, "report.txt").toPath(), StandardCharsets.UTF_8);
            assertTrue("the report gains a re-latched line: " + again, again.contains("re-latched at "));
            System.out.println("GREEN oracle mismatch hold at " + p + "; pin kept; re-latched after a restart");
            r.stopReplica();

            // ------------------------------------------------------------------------------- HOLD_OFF control
            r.windowGap();
            start(r, "replica-nc-hold", 2, ReplicaRig.off(ReplicaNegativeControl.HOLD_OFF));
            forgeUntilLatched(r, leader, true);
            RestoreDrillDryRun.nc("HOLD_OFF", "the process exited before the metrics were read", () -> {
                if (!r.fatals.isEmpty()) {
                    throw new AssertionError("the process exited before the metrics were read: " + r.fatals);
                }
            });
            r.stopReplica();

            // ------------------------------------------------------------------------- ALIGN_COMPARE_OFF control
            r.windowGap();
            start(r, "replica-nc-compare", 2, ReplicaRig.off(ReplicaNegativeControl.ALIGN_COMPARE_OFF));
            final long pOff = r.rig.snapshotAll();
            forgeFollowers(r, leader, pOff, true);
            r.awaitVerified(pOff, ShadowKind.ALIGNED, LATCH_WAIT_MS);
            r.windowGap();
            r.rig.pumpMs(VERIFY_WAIT_MS);
            RestoreDrillDryRun.nc("ALIGN_COMPARE_OFF", "/ready 200 after a forged member snapshot at", () -> {
                if ("200".equals(r.httpReady()[0])) {
                    throw new AssertionError("/ready 200 after a forged member snapshot at " + pOff);
                }
            });
            r.stopReplica();

            // ------------------------------------------------------------------------------- the torn variant
            r.windowGap();
            start(r, "replica-torn", 2, ReplicaRig.off());
            final long pTorn = r.rig.snapshotAll();
            forgeFollowers(r, leader, pTorn, false);
            r.rig.await(() -> r.metric("shadow_oracle_unreadable_total") > 0 || !r.holds.isEmpty()
                    || !r.fatals.isEmpty(), LATCH_WAIT_MS, () -> "the torn copy at " + pTorn + " was never compared: "
                    + r.probe().detail());
            assertTornIsUnreadable(r, pTorn);
            final long recoveriesBefore = r.metric("shadow_oracle_match_total");
            r.windowGap();
            final long pGood = r.rig.snapshotAll();
            r.awaitVerified(pGood, ShadowKind.ALIGNED, LATCH_WAIT_MS);
            assertTrue("recovered at the next good set", r.metric("shadow_oracle_match_total") > recoveriesBefore);
            assertEquals("ready after the next good set", "200", r.httpReady()[0]);
            System.out.println("GREEN torn member copy at " + pTorn + " read UNREADABLE (unreadable_total="
                    + r.metric("shadow_oracle_unreadable_total") + "), recovered at " + pGood);
            r.stopReplica();

            // control: the envelope check off, the same torn forgery is read as a byte mismatch
            r.windowGap();
            start(r, "replica-nc-envelope", 2, ReplicaRig.off(ReplicaNegativeControl.ENVELOPE_CHECK_OFF));
            final long pTornOff = forgeUntilLatched(r, leader, false)[0];
            RestoreDrillDryRun.nc("ENVELOPE_CHECK_OFF", "torn member copy counted as a mismatch",
                    () -> assertTornIsUnreadable(r, pTornOff));
            r.stopReplica();

            // ------------------------------------------------------------------------------- MISMATCH_PIN_OFF
            r.windowGap();
            start(r, "replica-nc-pin", 1, ReplicaRig.off(ReplicaNegativeControl.MISMATCH_PIN_OFF));
            final long pPin = forgeUntilLatched(r, leader, true)[0];
            RestoreDrillDryRun.nc("MISMATCH_PIN_OFF", "disputed set at", () -> assertPinned(r, "replica-nc-pin", pPin));
        }
    }

    /** {@code /ready} 503 whose readiness field is exactly {@code recovery=fenced:<text>} (then the next field). */
    private static void assertReadyFenced(final String[] ready, final String text) {
        if (!"503".equals(ready[0]) || !ready[1].contains(" recovery=fenced:" + text + " restore=")) {
            throw new AssertionError("/ready while held: expected 503 with recovery=fenced:" + text + ", got "
                    + ready[0] + " " + ready[1]);
        }
    }

    private static void start(final ReplicaRig r, final String name, final int keep,
                              final Set<ReplicaNegativeControl> off) {
        // a clean newest member snapshot first: an earlier step's forged copy is still in the members' archives, and
        // a replica that starts from genesis compares the newest one the backup names
        r.rig.snapshotAll();
        r.startReplica(r.builder(ReplicaRig.SLOT, name).verifyWaitMs(VERIFY_WAIT_MS).retrievedSetsKeep(keep)
                .testHooks(r.faults, off).build());
        r.awaitBootstrapped(ReplicaRig.WAIT_MS);
        r.awaitCaughtUp(60_000);
        final long p0 = r.rig.snapshotAll();
        r.awaitVerified(p0, ShadowKind.ALIGNED, LATCH_WAIT_MS);
        r.traffic(10, 4);
        // the next snapshot must be named by a query that comes after the forge: start right after one
        r.windowGap();
    }

    /** Forges every follower's service snapshot at {@code p} (the FOLLOWER source retrieves from a follower). */
    private static byte[] forgeFollowers(final ReplicaRig r, final Member leader, final long p, final boolean reseal) {
        byte[] out = null;
        for (final Member m : r.rig.live()) {
            if (m.id != leader.id) {
                out = r.forgeMemberSnapshot(m, p, reseal);
            }
        }
        return out;
    }

    /**
     * SNAPSHOT toggle, forge at once, wait for the hold (or a fatal). The forgery races the backup's next query; a
     * replica that compared EQUAL (the query came first) is retried at the next snapshot. Returns {P, image length}.
     */
    private static long[] forgeUntilLatched(final ReplicaRig r, final Member leader, final boolean reseal) {
        for (int attempt = 1; attempt <= FORGE_ATTEMPTS; attempt++) {
            final long p = r.rig.snapshotAll();
            final byte[] image = forgeFollowers(r, leader, p, reseal);
            r.rig.await(() -> !r.holds.isEmpty() || !r.fatals.isEmpty() || r.verified(ShadowKind.ALIGNED).contains(p),
                    LATCH_WAIT_MS, () -> "neither a latch nor an ALIGNED at the forged " + p + ": "
                            + r.probe().detail());
            if (!r.holds.isEmpty() || !r.fatals.isEmpty()) {
                System.out.println("[oracle] forge attempt " + attempt + " latched at " + p + " (holds " + r.holds
                        + ", fatals " + r.fatals + ")");
                return new long[] {p, image.length};
            }
            System.out.println("[oracle] forge attempt " + attempt + " at " + p + " lost the race with the backup query;"
                    + " retrying at the next snapshot");
            r.windowGap();
        }
        throw new AssertionError("the forgery never reached the backup before its query in " + FORGE_ATTEMPTS
                + " attempts");
    }

    private static void assertTornIsUnreadable(final ReplicaRig r, final long p) {
        assertTornIsUnreadable(r.metric("shadow_oracle_mismatch_total"), r.metric("shadow_oracle_unreadable_total"), p);
        if (!r.holds.isEmpty() || !r.fatals.isEmpty()) {
            throw new AssertionError("torn member copy counted as a mismatch at " + p + ": holds " + r.holds
                    + ", fatals " + r.fatals);
        }
    }

    private static void assertTornIsUnreadable(final long mismatches, final long unreadable, final long p) {
        if (mismatches != 0 || unreadable < 1) {
            throw new AssertionError("torn member copy counted as a mismatch at " + p + " (mismatch_total "
                    + mismatches + ", unreadable_total " + unreadable + ")");
        }
    }

    /** K=1: one more member snapshot retrieved after the latch, two housekeeping passes, the set at P still there. */
    private static void assertPinned(final ReplicaRig r, final String name, final long p) {
        r.windowGap();
        final long newer = r.rig.snapshotAll();
        r.rig.await(() -> r.retrievedCopy(name, newer) != null, LATCH_WAIT_MS,
                () -> "the backup never retrieved the newer set at " + newer);
        // two housekeeping passes (the scan period is 500 ms in the rig)
        r.rig.pumpMs(3_000);
        final byte[] disputed = r.retrievedCopy(name, p);
        if (disputed == null) {
            throw new AssertionError("disputed set at " + p + " purged after a newer set arrived at " + newer
                    + " (shadow_retrieved_sets " + r.metric("shadow_retrieved_sets") + ")");
        }
        assertEquals("K=1 plus the pinned set", 2, r.metric("shadow_retrieved_sets"));
        System.out.println("[oracle] pin: the disputed set at " + p + " kept beside the newer set at " + newer);
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.CoreConfigEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer;
import com.openexchange.cluster.SnapshotCadence;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.replica.BackupEvents;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ReplicaReadiness;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.LogSpliceRunner;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.codecs.ClusterAction;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The replica's shadows equal the members' snapshots, on a real three-member core with the in-JVM replica (FOLLOWER
 * source, a real backup retrieving every member snapshot) under churn. The story: ADMIN adopt and gate open, TOOL
 * deposits, GATEWAY and BRIDGE registrations, two REFUSED registrations (a GATEWAY asserting BRIDGE, a non-resumable
 * BRIDGE), GATEWAY session churn between toggles, GTD orders expiring three seconds out in two batches (the deadline
 * sweep fires), a CoreConfig switch to behaviour 2, a leader kill after an ALIGNED, and (with at least four
 * ALIGNED) a leader kill while an ALIGNED is held; SNAPSHOT and STANDBY_SNAPSHOT toggles interleaved, every SNAPSHOT at
 * least one backup query after the previous one.
 *
 * <p>Oracles: (1) at every flags-0 action P the VERIFIED ALIGNED shadow's payload equals every live member's image
 * captured at (member, P) and the retrieved member copy, {@code shadow_oracle_match_total} counts every compare,
 * nothing mismatched, nothing of another genesis, nothing unverifiable; (2) at every flags-1 action Q a STANDBY shadow
 * equal to the offline replay to Q, the members' snapshot state unchanged across the toggle; (3) the members agree
 * at every P; (4) the recorded log carries every frame kind of the story; (5) no fatal, no hold, no leader supplier;
 * the held kill ends EQUAL or with exactly one verify timeout, never a fence; (6) {@link LogSpliceRunner} on the
 * recorded log with the production applier.</p>
 *
 * <p>Variants (system properties): {@code core.equivalence.aligned}, {@code core.equivalence.standby} (default 5 and
 * 5; the PR leg passes 2 and 2), {@code core.equivalence.users} (default 4), {@code core.equivalence.100k} (100k open
 * GTC orders before the first toggle), {@code core.equivalence.million} (1M; the dev box only). With 100k or 1M the
 * test refuses fewer than 5 toggles of a kind.</p>
 *
 * <p>Controls ({@code NC <id> -> <text>}): (a) {@code SKIP_SESSION_OPEN} -> {@code payload differs from member
 * snapshot at <P>} (the replica holds an oracle mismatch); (b) {@code TRIGGER_IGNORES_STANDBY} ->
 * {@code no STANDBY shadow at <Q> within <t>}; (c) a forward-shift forgery offered to oracle 1 ->
 * {@code payload differs from member snapshot at}; (d) {@code STANDBY_FRESHNESS_OFF} with a named, uncompared member
 * snapshot -> {@code STANDBY written while a named member snapshot was uncompared at <P>}; (e) member images keyed by
 * member only -> {@code no member image captured at <P> for member <id>}. The PR variant runs (b), (c) and (e).</p>
 */
public class ShadowEquivalenceClusterTest {

    private static final int PORT_BASE = 28000; // members 28000/28100/28200, replica slots 3..5 at 28300..28500
    private static final long STANDBY_WAIT_MS = 20_000;
    /** The smallest verify wait the rig's backup cycle allows (2 x 12 000 + the 12 000 cool-down). */
    private static final long VERIFY_WAIT_MS = 2 * 12_000 + ReplicaConfig.DEFAULT_BACKUP_COOLDOWN_MS;
    /** GTD orders of the sweep batches expire this far out (cluster time is wall time in the rig). */
    private static final long GTD_EXPIRY_MS = 3_000;

    /** The variant, from the system properties, refused like a setting. */
    record Variant(int aligned, int standby, int users, int orders, String name) {
        static Variant fromProperties() {
            final boolean hundredK = bool("core.equivalence.100k");
            final boolean million = bool("core.equivalence.million");
            final int aligned = atLeast2("core.equivalence.aligned", 5);
            final int standby = atLeast2("core.equivalence.standby", 5);
            final String users = System.getProperty("core.equivalence.users", "4").trim();
            final long u;
            try {
                u = Long.parseLong(users);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("the test refuses: core.equivalence.users='" + users
                        + "' is not an integer");
            }
            if (u < 1 || u > 100_000) {
                throw new IllegalArgumentException("the test refuses: core.equivalence.users must be 1..100000: " + u);
            }
            if (hundredK || million) {
                if (aligned < 5) {
                    throw new IllegalArgumentException("the plan requires >= 5 SNAPSHOT toggles on the nightly leg: "
                            + aligned);
                }
                if (standby < 5) {
                    throw new IllegalArgumentException("the plan requires >= 5 STANDBY_SNAPSHOT toggles on the"
                            + " nightly leg: " + standby);
                }
            }
            final int orders = million ? 1_000_000 : hundredK ? 100_000 : 0;
            final String name = (million ? "million" : hundredK ? "100k" : aligned >= 5 ? "default" : "pr") + "-"
                    + aligned + "x" + standby;
            return new Variant(aligned, standby, (int)u, orders, name);
        }

        private static boolean bool(final String name) {
            final String v = System.getProperty(name);
            if (v == null || v.isBlank() || v.trim().equals("false")) {
                return false;
            }
            if (v.trim().equals("true")) {
                return true;
            }
            throw new IllegalArgumentException("the test refuses: " + name + " must be true or false: " + v);
        }

        private static int atLeast2(final String name, final int dflt) {
            final String v = System.getProperty(name);
            if (v == null || v.isBlank()) {
                return dflt;
            }
            final int n;
            try {
                n = Integer.parseInt(v.trim());
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("the test refuses: " + name + "='" + v + "' is not an integer");
            }
            if (n < 2) {
                throw new IllegalArgumentException("the test refuses: " + name + " must be >= 2: " + n);
            }
            return n;
        }

        boolean full() {
            return aligned >= 5;
        }

        /** The replica's serialize buffer: the rig's 16 MB, 128 MB at 100k (about 40 MB images), 1 GB at 1M. */
        int shadowBufferMb() {
            return orders >= 1_000_000 ? 1024 : orders > 0 ? 128 : 16;
        }
    }

    /** One row of {@code positions.tsv}. */
    private record Row(long position, int flags, long term, String members, int bytes, boolean equal, String kind,
                       boolean setPresent) {
    }

    @Test
    public void theVariantKnobsAreRefusedWithTheirTexts() {
        final String[][] rows = {
                {"core.equivalence.users", "0", "the test refuses: core.equivalence.users must be 1..100000: 0"},
                {"core.equivalence.users", "100001",
                        "the test refuses: core.equivalence.users must be 1..100000: 100001"},
                {"core.equivalence.users", "4x", "the test refuses: core.equivalence.users='4x' is not an integer"},
                {"core.equivalence.aligned", "1", "the test refuses: core.equivalence.aligned must be >= 2: 1"},
                {"core.equivalence.100k", "yes", "the test refuses: core.equivalence.100k must be true or false: yes"},
                {"core.equivalence.million", "1",
                        "the test refuses: core.equivalence.million must be true or false: 1"},
        };
        final Map<String, String> saved = new HashMap<>();
        for (final String k : List.of("core.equivalence.users", "core.equivalence.aligned", "core.equivalence.standby",
                "core.equivalence.100k", "core.equivalence.million")) {
            saved.put(k, System.getProperty(k));
            System.clearProperty(k);
        }
        try {
            for (final String[] r : rows) {
                System.setProperty(r[0], r[1]);
                try {
                    Variant.fromProperties();
                    throw new AssertionError("accepted " + r[0] + "=" + r[1]);
                } catch (final IllegalArgumentException refused) {
                    assertEquals(r[2], refused.getMessage());
                } finally {
                    System.clearProperty(r[0]);
                }
            }
            System.setProperty("core.equivalence.100k", "true");
            System.setProperty("core.equivalence.aligned", "2");
            try {
                Variant.fromProperties();
                throw new AssertionError("accepted 2 SNAPSHOT toggles on the nightly leg");
            } catch (final IllegalArgumentException refused) {
                assertEquals("the plan requires >= 5 SNAPSHOT toggles on the nightly leg: 2", refused.getMessage());
            }
            System.clearProperty("core.equivalence.aligned");
            assertEquals("100k with the defaults", 100_000, Variant.fromProperties().orders());
        } finally {
            for (final Map.Entry<String, String> e : saved.entrySet()) {
                if (e.getValue() == null) {
                    System.clearProperty(e.getKey());
                } else {
                    System.setProperty(e.getKey(), e.getValue());
                }
            }
        }
    }

    @Test(timeout = 7_200_000)
    public void shadowsEqualMemberSnapshotsUnderChurnKillsAndASwitch() throws Exception {
        final Variant v = Variant.fromProperties();
        System.out.println("[equivalence] variant " + v);
        final long t0 = System.currentTimeMillis();
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-equivalence")) {
            r.bootAll();
            final Member firstLeader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(firstLeader, null);
            r.story(host, v.users());
            if (v.orders() > 0) {
                bulk(r, v.orders());
            }
            r.rejectedRegistrations();

            r.startReplica(r.builder(ReplicaRig.SLOT, "replica").verifyWaitMs(VERIFY_WAIT_MS)
                    .shadowBufferMb(v.shadowBufferMb()).retrievedSetsKeep(Math.min(100, v.aligned() + 1)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            assertEquals("a fresh replica against a complete log starts at genesis",
                    ReplicaReadiness.RestorePath.GENESIS, r.replica.readiness().restorePath());
            r.awaitCaughtUp(v.orders() >= 1_000_000 ? 1_800_000 : 600_000);

            final List<Row> rows = new ArrayList<>();
            final Set<Integer> killed = new HashSet<>();
            final Map<Long, Integer> wentDown = new HashMap<>(); // P -> member down at P (no image legitimately)
            final List<Long> alignedAt = new ArrayList<>();
            final List<Long> standbyAt = new ArrayList<>();
            final int killAfter = Math.min(3, v.aligned() - 1);
            final int heldKillRound = v.aligned() >= 4 ? 4 : -1;
            String heldKill = "not run (fewer than 4 ALIGNED)";
            long heldP = -1;
            long equalCompares = 0;
            final int rounds = Math.max(v.aligned(), v.standby());
            for (int round = 1; round <= rounds; round++) {
                churn(r, 2);
                if (round <= 2) {
                    r.orders(v.users(), 2 * v.users(), System.currentTimeMillis() + GTD_EXPIRY_MS);
                    r.rig.pumpMs(GTD_EXPIRY_MS + 2_000);
                }
                if (round == 2) {
                    r.configSwitch(2);
                }
                r.traffic(10, 2);
                if (round == heldKillRound) {
                    if (round <= v.standby()) {
                        standbyAt.add(standby(r, stable(r, killed), rows));
                    }
                    final long[] held = heldKill(r, killed, wentDown);
                    heldP = held[0];
                    heldKill = held[1] == 1 ? "EQUAL at " + held[0] : "one verify timeout at " + held[0];
                    if (held[1] == 1) {
                        equalCompares++;
                        alignedAt.add(held[0]);
                        aligned(r, held[0], rows, wentDown);
                    }
                    continue;
                }
                if (round <= v.aligned()) {
                    r.windowGap();
                    final long p = r.rig.snapshotAll();
                    alignedAt.add(p);
                    aligned(r, p, rows, wentDown);
                    equalCompares++;
                    if (round == 1) {
                        // (c) the oracle sees a forward shift: content S(x), x < P, labelled P
                        final RecordedLog log = r.rig.captureCommitted(stable(r, killed));
                        final byte[] atP = r.memberImage(stable(r, killed).id, p);
                        final long x = forgeryPosition(log, p, atP);
                        final byte[] forged = TestShadowProducer.imageAt(log, x);
                        RestoreDrillDryRun.nc("FORWARD_SHIFT_FORGERY", "payload differs from member snapshot at",
                                () -> ReplicaRig.assertPayloadEqualsMember(p, forged, atP));
                    }
                }
                if (round <= v.standby()) {
                    standbyAt.add(standby(r, stable(r, killed), rows));
                }
                if (round == killAfter) {
                    kill(r, killed, wentDown, -1);
                }
            }

            // (3) the members agree at every P; (1) the retrieved sets and the counters
            for (final long p : alignedAt) {
                byte[] first = null;
                int firstId = -1;
                for (final Member m : r.rig.live()) {
                    if (Integer.valueOf(m.id).equals(wentDown.get(p))) {
                        continue;
                    }
                    final byte[] img = r.memberImage(m.id, p);
                    if (first == null) {
                        first = img;
                        firstId = m.id;
                    } else if (!java.util.Arrays.equals(first, img)) {
                        throw new AssertionError("the members disagree at " + p + ": member " + firstId + " vs member "
                                + m.id + " (the members, not the replica, diverged)");
                    }
                }
            }
            final int flags0 = alignedAt.size() + (heldKill.startsWith("one verify timeout")
                    && r.retrievedCopy("replica", heldP) != null ? 1 : 0);
            assertEquals("shadow_oracle_match_total counts every EQUAL compare", equalCompares,
                    r.metric("shadow_oracle_match_total"));
            assertEquals("no oracle mismatch", 0, r.metric("shadow_oracle_mismatch_total"));
            assertEquals("no other genesis", 0, r.metric("shadow_oracle_other_genesis_total"));
            assertEquals("no UNVERIFIABLE compare (the toggles are spaced by a query interval)", 0,
                    r.metric("shadow_align_unverifiable_total"));
            r.rig.await(() -> r.metric("shadow_retrieved_sets") == flags0, 60_000,
                    () -> "shadow_retrieved_sets " + r.metric("shadow_retrieved_sets") + " != " + flags0);

            // (4) the recorded log carries the story
            final Member stable = stable(r, killed);
            final RecordedLog log = r.rig.captureCommitted(stable);
            storyIsInTheLog(log, v, killed.size());

            // (5) no fatal, no hold, no fence, never a leader supplier
            assertTrue("no fatal on the replica: " + r.fatals, r.fatals.isEmpty());
            assertTrue("no hold on the replica: " + r.holds, r.holds.isEmpty());
            assertTrue("the replica fenced: " + r.replica.follower().fenced(), r.replica.follower().fenced() == null);
            assertSupplierNotLeader(r.replica.events());

            // (6) the splice runner on the recorded log with the production applier
            final LogSpliceRunner.Report splice = LogSpliceRunner.run(() -> new CoreLogApplier(() -> {
                throw new AssertionError("the recorded log must not trip the CoreConfig stop");
            }), log, LogSpliceRunner.Cuts.actions(), e -> List.of());
            if (splice.comparisons() < splice.cuts()) {
                throw new AssertionError("vacuous: " + splice.comparisons() + " comparisons for " + splice.cuts()
                        + " cuts");
            }
            final File evidence = evidenceDir(v);
            writePositions(evidence, rows);
            write(new File(evidence, "splice.tsv"), "frames\tactions\tcuts\tcheckpoints\tcomparisons\tfinal_bytes\n"
                    + splice.frames() + "\t" + splice.actions() + "\t" + splice.cuts() + "\t" + splice.checkpoints() + "\t"
                    + splice.comparisons() + "\t" + splice.finalImageBytes() + "\n");
            write(new File(evidence, "held-kill.txt"), heldKill + "\n");
            System.out.println("GREEN equivalence " + v.name() + ": ALIGNED " + alignedAt + ", STANDBY " + standbyAt
                    + ", held kill " + heldKill + ", splice " + splice + ", " + rows.size() + " rows in " + evidence
                    + ", " + (System.currentTimeMillis() - t0) / 1000 + " s");

            // ------------------------------------------------------------------------------------ controls
            // (e) the image map keyed by member only keeps the last image: the first P has none
            final long firstP = alignedAt.get(0);
            final Map<ReplicaRig.MemberPosition, byte[]> lastOnly = new HashMap<>();
            final Map<Integer, Long> newest = new HashMap<>();
            for (final ReplicaRig.MemberPosition k : r.imageAt.keySet()) {
                newest.merge(k.memberId(), k.logPosition(), Math::max);
            }
            for (final Map.Entry<Integer, Long> e : newest.entrySet()) {
                lastOnly.put(new ReplicaRig.MemberPosition(e.getKey(), e.getValue()),
                        r.imageAt.get(new ReplicaRig.MemberPosition(e.getKey(), e.getValue())));
            }
            r.imageAt.clear();
            r.imageAt.putAll(lastOnly);
            RestoreDrillDryRun.nc("IMAGE_MAP_KEYED_BY_MEMBER", "no member image captured at " + firstP + " for member",
                    () -> r.memberImage(stable.id, firstP));

            if (v.full()) {
                freshness(r);
            }

            // (b) the trigger listener ignoring flags 1: no STANDBY shadow
            r.stopReplica();
            r.windowGap();
            r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-nc-b").verifyWaitMs(VERIFY_WAIT_MS).shadowBufferMb(v.shadowBufferMb())
                    .testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.TRIGGER_IGNORES_STANDBY)).build());
            r.awaitBootstrapped(ReplicaRig.WAIT_MS);
            r.awaitCaughtUp(600_000);
            final Member hostB = stable(r, killed);
            final int framesB = r.actionFrames(hostB, 1).size();
            r.toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
            final long qB = awaitStandbyFrame(r, hostB, framesB);
            RestoreDrillDryRun.nc("TRIGGER_IGNORES_STANDBY", "no STANDBY shadow at",
                    () -> awaitStandbyShadow(r, qB, STANDBY_WAIT_MS));

            if (v.full()) {
                // (a) the replica drops every session open: the next ALIGNED latches an oracle mismatch. A replica
                // that dropped every open from genesis would refuse the ADMIN ADOPT too and never hold a genesis (no
                // compare at all), so the control restarts a VERIFIED replica from its local shadow first
                r.stopReplica();
                r.windowGap();
                r.startReplica(r.builder(ReplicaRig.SLOT + 2, "replica-nc-a").verifyWaitMs(VERIFY_WAIT_MS)
                        .shadowBufferMb(v.shadowBufferMb()).build());
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                r.awaitCaughtUp(600_000);
                final long pA0 = r.rig.snapshotAll();
                r.awaitVerified(pA0, ShadowKind.ALIGNED, ReplicaRig.WAIT_MS);
                r.stopReplica();
                r.windowGap();
                r.startReplica(r.builder(ReplicaRig.SLOT + 2, "replica-nc-a").verifyWaitMs(VERIFY_WAIT_MS)
                        .shadowBufferMb(v.shadowBufferMb()).testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.SKIP_SESSION_OPEN)).build());
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                r.awaitCaughtUp(600_000);
                // a session that stays open: the members hold it, the replica never saw it open
                final Client kept = r.rig.connect(SessionClass.GATEWAY, r.clientHost);
                r.register(kept, SessionClass.GATEWAY, true);
                r.traffic(10, 2);
                final long pA = r.rig.snapshotAll();
                r.rig.await(() -> !r.holds.isEmpty() || !r.fatals.isEmpty(), ReplicaRig.WAIT_MS,
                        () -> "SKIP_SESSION_OPEN: no oracle mismatch at " + pA + ": " + r.probe().detail());
                assertEquals("the control latched one mismatch", 1, r.metric("shadow_oracle_mismatch_total"));
                final String[] readyA = r.httpReady();
                assertTrue("503 fenced:oracle mismatch: " + readyA[0] + " " + readyA[1], "503".equals(readyA[0])
                        && readyA[1].contains("recovery=fenced:oracle mismatch at " + pA + ": "));
                final byte[] heldImage = Files.readAllBytes(new File(r.replicaBase("replica-nc-a"),
                        "mismatch/" + pA + "/replica.image").toPath());
                RestoreDrillDryRun.nc("SKIP_SESSION_OPEN", "payload differs from member snapshot at " + pA,
                        () -> ReplicaRig.assertPayloadEqualsMember(pA, heldImage, r.memberImage(stable.id, pA)));

                // (d) the freshness rule off: a STANDBY is written while a named member snapshot is uncompared
                r.stopReplica();
                r.windowGap();
                r.startReplica(r.builder(ReplicaRig.SLOT, "replica-nc-d").verifyWaitMs(VERIFY_WAIT_MS)
                        .shadowBufferMb(v.shadowBufferMb()).testHooks(r.faults, ReplicaRig.off(ReplicaNegativeControl.STANDBY_FRESHNESS_OFF)).build());
                r.awaitBootstrapped(ReplicaRig.WAIT_MS);
                r.awaitCaughtUp(600_000);
                final long p0 = r.rig.snapshotAll();
                r.awaitVerified(p0, ShadowKind.ALIGNED, ReplicaRig.WAIT_MS);
                final long[] named = nameUncompared(r);
                RestoreDrillDryRun.nc("STANDBY_FRESHNESS_OFF", "STANDBY written while a named member snapshot was"
                        + " uncompared at " + named[0], () -> assertNoStandbyWhileUncompared(r, named[0], named[1]));
                r.faults.wedgeSnapshotRetrieval(false);

                // FOLLOWER source: a LEADER-source replica is seen by the supplier oracle
                r.stopReplica();
                r.windowGap();
                r.startReplica(r.builder(ReplicaRig.SLOT + 1, "replica-nc-leader").shadowBufferMb(v.shadowBufferMb())
                        .sourceType(ClusterBackup.SourceType.LEADER).build());
                r.rig.await(() -> r.replica.events().responses() > 0, ReplicaRig.WAIT_MS,
                        "the LEADER-source replica never got a backup response");
                RestoreDrillDryRun.nc("SOURCE_LEADER", "supplier is the leader",
                        () -> assertSupplierNotLeader(r.replica.events()));
            }
            System.out.println("[equivalence] " + v.name() + " done in " + (System.currentTimeMillis() - t0) / 1000
                    + " s");
        }
    }

    // ---------------------------------------------------------------------------------------------- the story

    /** {@code n} GATEWAY sessions opened, registered and closed (open, RegisterSession and close frames). */
    private static void churn(final ReplicaRig r, final int n) throws InterruptedException {
        for (int i = 0; i < n; i++) {
            final Client c = r.rig.connect(SessionClass.GATEWAY, r.clientHost);
            final String ack = r.register(c, SessionClass.GATEWAY, true);
            if (!ack.contains("result=ACCEPTED")) {
                throw new AssertionError("a churn GATEWAY registration was not accepted: " + ack);
            }
            r.rig.forget(c);
        }
    }

    /** {@code orders} open GTC orders, ten per user, through a windowed GATEWAY (users funded first). */
    private static void bulk(final ReplicaRig r, final int orders) {
        final int users = orders / OrderLoad.ORDERS_PER_USER;
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        final long base = 10_000; // users above the story's
        for (long u = 1; u <= users; u++) {
            r.tool.offer(enc.buf, enc.deposit(0, base + u, CoreRestoreRig.usd(), 1_000_000 * ShadowClusterRig.ONE));
            r.tool.offer(enc.buf, enc.deposit(0, base + u, CoreRestoreRig.baseAssetOf(CoreRestoreRig.marketOf(base + u)),
                    1_000 * ShadowClusterRig.ONE));
        }
        r.tool.offer(enc.buf, enc.deposit(98, 1, CoreRestoreRig.usd(), ShadowClusterRig.ONE));
        r.rig.await(() -> r.tool.in.lastDepositAckCorr == 98, 1_800_000, "the bulk funding must be applied");
        final CoreRestoreRig.Listener g = r.gw.in;
        final int before = g.acks + g.rejects;
        final long t = System.currentTimeMillis();
        for (int n = 0; n < orders; n++) {
            final long user = base + 1 + n / OrderLoad.ORDERS_PER_USER;
            final int k = n % OrderLoad.ORDERS_PER_USER;
            final boolean buy = k % 2 == 0;
            final int market = CoreRestoreRig.marketOf(user);
            while (n - (g.acks + g.rejects - before) >= 4_000) {
                for (final CoreRestoreRig.Session s : r.rig.sessions()) {
                    s.poll();
                }
            }
            r.gw.offer(enc.buf, enc.limit(5_000_000L + n, user, 5_000_000L + n, market, buy,
                    CoreRestoreRig.priceOf(market, buy, k / 2), ShadowClusterRig.ONE / 10, TimeInForce.GTC, 0L));
        }
        r.rig.await(() -> g.acks + g.rejects - before >= orders, 3_600_000, "every bulk create must be answered");
        System.out.println("[equivalence] bulk: " + orders + " open orders over " + users + " users in "
                + (System.currentTimeMillis() - t) + " ms (acks " + g.acks + ", rejects " + g.rejects + ")");
    }

    /** A live member never killed in this run (its log recording covers the whole history). */
    private static Member stable(final ReplicaRig r, final Set<Integer> killed) {
        for (final Member m : r.rig.live()) {
            if (!killed.contains(m.id)) {
                return m;
            }
        }
        throw new AssertionError("every live member was killed once");
    }

    /**
     * Kills the leader (clients moved off it first), waits for the new leader and the backup's re-validated
     * supplier, restarts the killed member (so the next kill keeps a quorum) and records it as down at
     * {@code downAt} (-1: at no snapshot position).
     */
    private static Member kill(final ReplicaRig r, final Set<Integer> killed, final Map<Long, Integer> wentDown,
                               final long downAt) throws InterruptedException {
        final Member leader = r.rig.awaitLeader(null);
        if (r.clientHost.id == leader.id) {
            r.moveClientsTo(r.rig.followerOtherThan(leader, null));
        }
        r.rig.kill(leader);
        killed.add(leader.id);
        if (downAt >= 0) {
            wentDown.put(downAt, leader.id);
        }
        final Member newLeader = r.rig.awaitLeader(leader);
        System.out.println("[equivalence] killed leader " + leader.id + "; new leader " + newLeader.id
                + "; supplier was " + r.replica.events().supplierMemberId());
        r.moveClientsTo(r.clientHost);
        final long queriesAtKill = r.replica.events().queries();
        final long responsesAtKill = r.replica.events().responses();
        r.rig.await(() -> r.replica.events().queries() > queriesAtKill + 1
                        || (r.replica.events().responses() > responsesAtKill
                        && r.replica.events().supplierMemberId() != newLeader.id),
                ReplicaRig.WAIT_MS, () -> "no post-election backup query: queries=" + r.replica.events().queries());
        r.rig.await(() -> r.replica.events().supplierMemberId() != newLeader.id
                        && r.replica.follower().liveRecordingAlive(), ReplicaRig.WAIT_MS,
                () -> "after the election the supplier is still the new leader " + newLeader.id + " or L is not live: "
                        + r.probe().detail());
        r.rig.restart(leader);
        r.hook(leader);
        r.rig.await(() -> leader.cfg.consensusModuleContext().commitPositionCounter().get() >= r.commit(), 120_000,
                () -> "the restarted member " + leader.id + " never caught up");
        r.traffic(10, 0);
        return leader;
    }

    /**
     * SNAPSHOT toggle, the ALIGNED held ({@code shadow_align_held 1}), then the leader killed before the backup's next
     * query. Returns {P, 1 when the ALIGNED at P compared EQUAL, 0 after exactly one verify timeout}.
     */
    private static long[] heldKill(final ReplicaRig r, final Set<Integer> killed, final Map<Long, Integer> wentDown)
            throws InterruptedException {
        r.windowGap();
        final Member leader = r.rig.awaitLeader(null);
        final Member probeHost = stable(r, Set.of(leader.id));
        final int before = r.actionFrames(probeHost, 0).size();
        final long timeoutsBefore = r.metric("shadow_align_verify_timeouts_total");
        r.toggle(ClusterControl.ToggleState.SNAPSHOT);
        r.rig.await(() -> r.metric("shadow_align_held") == 1 || !r.fatals.isEmpty(), 30_000,
                () -> "no ALIGNED held after the SNAPSHOT toggle: " + r.probe().detail());
        final List<Long> frames = r.actionFrames(probeHost, 0);
        assertTrue("one more flags-0 action", frames.size() > before);
        final long p = frames.get(frames.size() - 1);
        kill(r, killed, wentDown, p);
        final long[] out = {p, -1};
        r.rig.await(() -> {
            if (r.verified(ShadowKind.ALIGNED).contains(p)) {
                out[1] = 1;
            } else if (r.metric("shadow_align_verify_timeouts_total") > timeoutsBefore) {
                out[1] = 0;
            }
            return out[1] >= 0 || !r.fatals.isEmpty() || !r.holds.isEmpty();
        }, VERIFY_WAIT_MS + ReplicaRig.WAIT_MS, () -> "the held ALIGNED at " + p + " neither compared nor timed out: "
                + r.probe().detail());
        assertTrue("no fence after a leader kill while an ALIGNED was held: fatals " + r.fatals + " holds " + r.holds,
                r.fatals.isEmpty() && r.holds.isEmpty());
        if (out[1] == 0) {
            assertEquals("exactly one verify timeout", timeoutsBefore + 1,
                    r.metric("shadow_align_verify_timeouts_total"));
        }
        System.out.println("[equivalence] held kill at " + p + ": " + (out[1] == 1 ? "EQUAL" : "one verify timeout"));
        return out;
    }

    /** Oracle 1 at P: the VERIFIED ALIGNED equals every member image captured at P and the retrieved member copy. */
    private static void aligned(final ReplicaRig r, final long p, final List<Row> rows,
                                final Map<Long, Integer> wentDown) {
        final LogicalShadow s = r.awaitVerified(p, ShadowKind.ALIGNED, ReplicaRig.WAIT_MS);
        final byte[] image = r.payload(s);
        final StringBuilder members = new StringBuilder();
        for (final Member m : r.rig.live()) {
            if (Integer.valueOf(m.id).equals(wentDown.get(p))) {
                continue; // killed at P: no image legitimately
            }
            ReplicaRig.assertPayloadEqualsMember(p, image, r.memberImage(m.id, p));
            members.append(members.length() == 0 ? "" : ",").append(m.id);
        }
        final byte[] copy = r.retrievedCopy("replica", p);
        if (copy == null) {
            throw new AssertionError("no retrieved member copy at " + p + " (REPLICA_RETRIEVED_SETS_KEEP too small?)");
        }
        ReplicaRig.assertPayloadEqualsMember(p, image, copy);
        rows.add(new Row(p, 0, s.representative().header().leadershipTermId(), members.toString(), image.length, true,
                "ALIGNED", true));
        System.out.println("[equivalence] ALIGNED at " + p + " equals members " + members + " and the retrieved copy ("
                + image.length + " B)");
    }

    /** Oracle 2: a STANDBY toggle; the STANDBY shadow equals the offline replay; the members' snapshot state is still. */
    private static long standby(final ReplicaRig r, final Member host, final List<Row> rows) {
        final Map<Integer, long[]> before = settledMemberState(r);
        final int framesBefore = r.actionFrames(host, 1).size();
        r.toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
        final long q = awaitStandbyFrame(r, host, framesBefore);
        awaitStandbyShadow(r, q, STANDBY_WAIT_MS);
        final LogicalShadow s = r.scan().atPosition(q);
        final byte[] image = r.payload(s);
        final byte[] offline = TestShadowProducer.imageAt(r.rig.captureCommitted(host), q);
        if (!java.util.Arrays.equals(image, offline)) {
            int at = 0;
            while (at < Math.min(image.length, offline.length) && image[at] == offline[at]) {
                at++;
            }
            throw new AssertionError("STANDBY at " + q + " differs from the offline replay (first byte " + at + ")");
        }
        r.traffic(5, 0);
        final Map<Integer, long[]> after = memberState(r);
        for (final Member m : r.rig.live()) {
            final long[] b = before.get(m.id);
            final long[] a = after.get(m.id);
            if (b == null || a == null) {
                continue;
            }
            assertEquals("member " + m.id + " RecordingLog snapshot count moved on a STANDBY toggle", b[0], a[0]);
            assertEquals("member " + m.id + " core_snapshot_stub_total moved on a STANDBY toggle", b[1], a[1]);
            assertEquals("member " + m.id + " core_snapshot_full_total moved on a STANDBY toggle", b[2], a[2]);
            assertEquals("member " + m.id + " onTakeSnapshot ran on a STANDBY toggle", b[3], a[3]);
            assertEquals("member " + m.id + " cadence byte baseline moved on a STANDBY toggle", b[4], a[4]);
        }
        rows.add(new Row(q, 1, s.representative().header().leadershipTermId(), "offline", image.length, true,
                "STANDBY", false));
        System.out.println("[equivalence] STANDBY at " + q + " equals the offline replay (" + image.length + " B);"
                + " members unchanged");
        return q;
    }

    /**
     * The freshness rule on the main replica: a named member snapshot the replica has not compared yet (its retrieval
     * wedged) suspends STANDBY; the next EQUAL resumes it.
     */
    private static void freshness(final ReplicaRig r) {
        final long staleBefore = r.metric("shadow_oracle_stale_named_total");
        final long[] named = nameUncompared(r);
        assertNoStandbyWhileUncompared(r, named[0], named[1]);
        assertTrue("the suspended STANDBY was counted", r.metric("shadow_oracle_stale_named_total") > staleBefore);
        r.faults.wedgeSnapshotRetrieval(false);
        r.windowGap();
        final long p = r.rig.snapshotAll();
        r.awaitVerified(p, ShadowKind.ALIGNED, VERIFY_WAIT_MS + ReplicaRig.WAIT_MS);
        final Member host = r.clientHost;
        final int frames = r.actionFrames(host, 1).size();
        r.toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
        final long q = awaitStandbyFrame(r, host, frames);
        awaitStandbyShadow(r, q, STANDBY_WAIT_MS);
        System.out.println("[equivalence] freshness: STANDBY at " + named[1] + " suspended while the member snapshot at "
                + named[0] + " was uncompared; resumed after the EQUAL at " + p + " (STANDBY at " + q + ")");
    }

    /**
     * A member snapshot named but never compared: the retrieval wedged, a member snapshot at P, named by the backup,
     * the held ALIGNED at P dropped after the verify wait; then the retrieval restored (the log follows again, P's
     * copy arrives, but no ALIGNED is held there any more, so nothing compares EQUAL at P) and a STANDBY toggle at Q
     * that the replica applies. Returns {P, Q}.
     */
    private static long[] nameUncompared(final ReplicaRig r) {
        r.windowGap();
        r.faults.wedgeSnapshotRetrieval(true);
        final long p = r.rig.snapshotAll();
        r.rig.await(() -> r.replica.events().newestReportedServiceSnapshot() >= p, ReplicaRig.WAIT_MS,
                () -> "the backup never named the member snapshot at " + p);
        // the held ALIGNED at P is dropped after the verify wait (the copy never arrives); only then is the image
        // buffer free for a STANDBY, which the freshness rule must then refuse
        r.rig.await(() -> r.metric("shadow_align_held") == 0, VERIFY_WAIT_MS + ReplicaRig.WAIT_MS,
                () -> "the ALIGNED at " + p + " was never dropped: " + r.probe().detail());
        r.faults.wedgeSnapshotRetrieval(false);
        r.awaitCaughtUp(ReplicaRig.WAIT_MS);
        final Member host = r.clientHost;
        final int frames = r.actionFrames(host, 1).size();
        r.toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
        final long q = awaitStandbyFrame(r, host, frames);
        r.rig.await(() -> r.replica.follower().appliedPosition() >= q, ReplicaRig.WAIT_MS,
                () -> "the replica never applied the STANDBY action at " + q + ": " + r.probe().detail());
        return new long[] {p, q};
    }

    private static void assertNoStandbyWhileUncompared(final ReplicaRig r, final long p, final long q) {
        final long end = System.currentTimeMillis() + STANDBY_WAIT_MS;
        while (System.currentTimeMillis() < end) {
            if (r.verified(ShadowKind.STANDBY).contains(q) && r.replica.follower().trust().verifiedAt() < p) {
                throw new AssertionError("STANDBY written while a named member snapshot was uncompared at " + p
                        + " (STANDBY at " + q + ", verified at " + r.replica.follower().trust().verifiedAt() + ")");
            }
            r.rig.pumpMs(250);
        }
    }

    /** Oracle 4: the recorded log carries every frame kind of the story. */
    private static void storyIsInTheLog(final RecordedLog log, final Variant v, final int kills) {
        int registers = 0;
        int configs = 0;
        int timers = 0;
        int flags0 = 0;
        int flags1 = 0;
        int terms = 0;
        final MessageHeaderDecoder hd = new MessageHeaderDecoder();
        for (final LogFrame f : log.frames()) {
            switch (f.kind()) {
                case SESSION_MESSAGE -> {
                    final byte[] d = f.data();
                    if (d != null && d.length >= MessageHeaderDecoder.ENCODED_LENGTH) {
                        hd.wrap(new UnsafeBuffer(d), 0);
                        if (hd.templateId() == RegisterSessionEncoder.TEMPLATE_ID) {
                            registers++;
                        } else if (hd.templateId() == CoreConfigEncoder.TEMPLATE_ID) {
                            configs++;
                        }
                    }
                }
                case TIMER_EVENT -> timers++;
                case CLUSTER_ACTION -> {
                    if (f.action() == ClusterAction.SNAPSHOT && f.flags() == 0) {
                        flags0++;
                    } else if (f.action() == ClusterAction.SNAPSHOT && f.flags() == 1) {
                        flags1++;
                    }
                }
                case NEW_LEADERSHIP_TERM -> terms++;
                default -> {
                }
            }
        }
        final List<String> missing = new ArrayList<>();
        // two accepted story registrations, two refused ones, two churn sessions per round
        if (registers < 4) {
            missing.add("RegisterSession frames incl. the refused ones (" + registers + ")");
        }
        if (configs < 2) {
            missing.add("CoreConfig ADOPT and switch frames (" + configs + ")");
        }
        if (timers < 2) {
            missing.add("TimerEvent frames (" + timers + ")");
        }
        if (flags0 < v.aligned()) {
            missing.add("flags-0 SNAPSHOT actions (" + flags0 + " < " + v.aligned() + ")");
        }
        if (flags1 < v.standby()) {
            missing.add("flags-1 SNAPSHOT actions (" + flags1 + " < " + v.standby() + ")");
        }
        if (terms < 1 + kills) {
            missing.add("term events (" + terms + " < " + (1 + kills) + ")");
        }
        if (!missing.isEmpty()) {
            throw new AssertionError("the recorded log is missing story frames: " + missing);
        }
        System.out.println("[equivalence] log: registers=" + registers + " configs=" + configs + " timers=" + timers
                + " flags0=" + flags0 + " flags1=" + flags1 + " terms=" + terms + " frames=" + log.size());
    }

    // ---------------------------------------------------------------------------------------------- helpers

    /** A frame end x < P whose image differs from S(P): the content a forward shift would label P. */
    private static long forgeryPosition(final RecordedLog log, final long p, final byte[] atP) {
        final long[] pos = log.positions();
        for (int i = pos.length - 1; i >= 0; i--) {
            if (pos[i] < p && !java.util.Arrays.equals(TestShadowProducer.imageAt(log, pos[i]), atP)) {
                return pos[i];
            }
        }
        throw new AssertionError("no frame before P=" + p + " with a different image");
    }

    /**
     * {@link #memberState} once two reads 500 ms apart agree: a member's cadence baseline moves when its own
     * snapshot completes, which can trail the RecordingLog entry {@code snapshotAll} waits for.
     */
    private static Map<Integer, long[]> settledMemberState(final ReplicaRig r) {
        Map<Integer, long[]> last = memberState(r);
        for (int i = 0; i < 40; i++) {
            r.rig.pumpMs(500);
            final Map<Integer, long[]> now = memberState(r);
            boolean same = now.keySet().equals(last.keySet());
            for (final Map.Entry<Integer, long[]> e : now.entrySet()) {
                same &= java.util.Arrays.equals(e.getValue(), last.get(e.getKey()));
            }
            if (same) {
                return now;
            }
            last = now;
        }
        throw new AssertionError("the members' snapshot state never settled before a STANDBY toggle");
    }

    /** {RecordingLog snapshots, rcs stub, rcs full, onTakeSnapshot calls, cadence byte baseline} per live member. */
    private static Map<Integer, long[]> memberState(final ReplicaRig r) {
        final Map<Integer, long[]> out = new HashMap<>();
        for (final Member m : r.rig.live()) {
            final SnapshotCadence cadence = ServiceProbe.cadence(m.service);
            final long probe = 1L << 40; // any fixed position: baseline = probe - bytesSince(probe)
            out.put(m.id, new long[] {
                    r.rig.snapshots(m).size(),
                    ServiceProbe.rcs(m.service, "core_snapshot_stub_total"),
                    ServiceProbe.rcs(m.service, "core_snapshot_full_total"),
                    r.snapshotsTaken.getOrDefault(m.id, 0L),
                    probe - cadence.logBytesSinceSnapshot(probe)});
        }
        return out;
    }

    /** The newest flags-1 SNAPSHOT action frame end in {@code host}'s committed log once one more exists. */
    static long awaitStandbyFrame(final ReplicaRig r, final Member host, final int before) {
        final long[] found = {-1};
        r.rig.await(() -> {
            final List<Long> frames = r.actionFrames(host, 1);
            if (frames.size() > before) {
                found[0] = frames.get(frames.size() - 1);
                return true;
            }
            return false;
        }, 30_000, "no flags-1 SNAPSHOT action frame appeared in member " + host.id + "'s log");
        return found[0];
    }

    static void awaitStandbyShadow(final ReplicaRig r, final long p, final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!r.verified(ShadowKind.STANDBY).contains(p)) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("no STANDBY shadow at " + p + " within " + timeoutMs + " ms (written "
                        + r.replica.follower().policy().writtenTotal(ShadowKind.STANDBY) + ", last "
                        + r.replica.follower().policy().lastPosition(ShadowKind.STANDBY) + ")");
            }
            r.rig.pumpMs(100);
        }
    }

    static void assertSupplierNotLeader(final BackupEvents events) {
        if (events.supplierWasLeaderEver() || events.supplierIsLeader()) {
            throw new AssertionError("supplier is the leader (member " + events.supplierMemberId()
                    + "): the replay loads the leader");
        }
    }

    private static File evidenceDir(final Variant v) {
        final File dir = new File(System.getProperty("user.dir"), "target/replica-evidence/equivalence-" + v.name());
        if (!dir.isDirectory() && !dir.mkdirs()) {
            throw new IllegalStateException("cannot create " + dir);
        }
        return dir;
    }

    private static void writePositions(final File dir, final List<Row> rows) {
        final StringBuilder sb = new StringBuilder(
                "position\tflags\tterm\tmembers_compared\tbytes\tequal\treplica_kind\tset_present\n");
        for (final Row row : rows) {
            sb.append(row.position()).append('\t').append(row.flags()).append('\t').append(row.term()).append('\t')
                    .append(row.members()).append('\t').append(row.bytes()).append('\t').append(row.equal()).append('\t')
                    .append(row.kind()).append('\t').append(row.setPresent()).append('\n');
        }
        write(new File(dir, "positions.tsv"), sb.toString());
    }

    private static void write(final File file, final String text) {
        try {
            Files.writeString(file.toPath(), text, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

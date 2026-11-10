// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.replica.CoreShadowReplicaMain;
import com.openexchange.cluster.replica.ReplicaFollower;
import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * The core half of the replica closure scans: the closures over {@code CoreShadowReplicaMain} (the core
 * classes it reaches: the binding, the applier, the engine, the settings) and over the kit's {@code ReplicaFollower}
 * and {@code ShadowTriggerPolicy} (the kit classes they reach, read from the cluster-kit jar this module builds
 * against) hold no {@code Cluster}, {@code ClientSession}, timer port, cluster client or cluster clock, call no
 * {@code scheduleTimer}/{@code cancelTimer} or scheduler, read neither {@code System} clock nor {@code java.time}
 * {@code now}, and launch no driver, archive, backup or cluster component. The replica's timeouts run on the clock
 * {@code ReplicaProcess} injects, so the injected {@code NanoClock} type is allowed here; the settings
 * read the environment, so {@code getenv} and {@code getProperty} are allowed in the core closure only.
 * The core closure must not reach the member stack ({@code AeronCluster}, {@code ClusterConfig},
 * {@code CoreClusteredService}). And {@code cluster.shadow} in the jar still never references the backup classes.
 * Controls: the member service wrapper and a probe class are flagged by the same rules.
 */
public class ReplicaClosureNeverSchedulesTest {

    static final String MAIN = "com/openexchange/core/replica/CoreShadowReplicaMain";
    static final String FOLLOWER = "com/openexchange/cluster/replica/ReplicaFollower";
    static final String POLICY = "com/openexchange/cluster/replica/ShadowTriggerPolicy";
    static final String ACS = RestoreClosureNeverSchedulesTest.ACS;
    static final String MEMBER_ENTRY = RestoreClosureNeverSchedulesTest.PKG + "AeronCluster";
    static final String MEMBER_CONFIG = RestoreClosureNeverSchedulesTest.PKG + "ClusterConfig";

    private static final Set<String> LAUNCH = Set.of("launch", "launchEmbedded");

    /** The replica closure rules; {@code engineSpecific} adds the engine prefixes for the kit closures. */
    static NeverSchedulesScan.Rules rules(final boolean engineSpecific) {
        final Map<String, Set<String>> members = new HashMap<>();
        members.put("java/lang/System", Set.of("currentTimeMillis", "nanoTime"));
        for (final String owner : List.of("io/aeron/driver/MediaDriver", "io/aeron/archive/Archive",
                "io/aeron/archive/ArchivingMediaDriver", "io/aeron/cluster/ClusterBackup",
                "io/aeron/cluster/ClusterBackupMediaDriver", "io/aeron/cluster/ClusteredMediaDriver",
                "io/aeron/cluster/ConsensusModule", "io/aeron/cluster/service/ClusteredServiceContainer")) {
            members.put(owner, LAUNCH);
        }
        return new NeverSchedulesScan.Rules(
                Set.of("io/aeron/cluster/service/Cluster",
                        "io/aeron/cluster/service/ClientSession",
                        "com/openexchange/cluster/TimerPort",
                        "io/aeron/cluster/client/AeronCluster",
                        "io/aeron/cluster/service/ClusterClock",
                        "java/util/Timer",
                        "java/util/TimerTask",
                        "java/util/concurrent/ScheduledExecutorService",
                        "java/util/concurrent/ScheduledThreadPoolExecutor"),
                engineSpecific ? List.of("com/openexchange/core/", "com/match/") : List.of(),
                Set.of("scheduleTimer", "cancelTimer", "newScheduledThreadPool", "newSingleThreadScheduledExecutor"),
                Map.copyOf(members),
                Map.of("java/time/", Set.of("now")),
                Map.of());
    }

    static Path kitRoot() {
        return CoreShadowEngineConformanceTest.rootOf(ReplicaFollower.class);
    }

    static NeverSchedulesScan.Report coreClosure(final String root) {
        return NeverSchedulesScan.scanClosure(CoreShadowEngineConformanceTest.coreRoots(), root,
                "com/openexchange/core/", rules(false));
    }

    static NeverSchedulesScan.Report kitClosure(final String root) {
        return NeverSchedulesScan.scanClosure(List.of(kitRoot()), root, "com/openexchange/cluster/", rules(true));
    }

    static void assertClean(final String root, final NeverSchedulesScan.Report r) {
        if (!r.clean()) {
            throw new AssertionError("closure from " + root + " schedules, holds a cluster handle, reads a clock or"
                    + " launches a driver: " + r.findings());
        }
    }

    @Test
    public void theReplicaMainClosureIsCleanAndNeverReachesTheMemberStack() {
        final NeverSchedulesScan.Report r = coreClosure(MAIN);
        System.out.println("GREEN scan: closure from " + MAIN + " = " + r.classesScanned() + " classes, findings "
                + r.findings().size());
        assertClean(MAIN, r);
        assertTrue("the closure reaches the applier", r.classes().contains(RestoreClosureNeverSchedulesTest.PKG
                + "CoreLogApplier"));
        assertTrue("the closure reaches the engine",
                r.classes().contains("com/openexchange/core/application/engine/CoreEngine"));
        assertTrue("the closure reaches the settings",
                r.classes().contains("com/openexchange/core/replica/ReplicaSettings"));
        final List<String> memberStack = new ArrayList<>();
        for (final String m : List.of(ACS, MEMBER_ENTRY, MEMBER_CONFIG)) {
            if (r.classes().contains(m)) {
                memberStack.add(m);
            }
        }
        assertTrue("the replica closure reaches the member stack " + memberStack + " (a replica must never be able to"
                + " start a member, schedule its timers or launch its driver)", memberStack.isEmpty());
    }

    /**
     * The member readiness class: the follower's closure reaches it only as the outer class of the
     * {@code NodeReadiness.Probe} value record the replica's readiness returns (a nested type names its outer class in
     * the InnerClasses attribute, so the closure follows it). Its code (a {@code Cluster.Role} and the system nano
     * clock, the member's own readiness) never runs on the replica: {@link #noMemberOfTheMemberReadinessIsCalled}
     * proves no class of the closure calls one of its members.
     */
    static final String NODE_READINESS = "com/openexchange/cluster/NodeReadiness";

    static boolean throughNodeReadiness(final NeverSchedulesScan.Finding f) {
        for (final String hop : f.path()) {
            if (hop.equals(NODE_READINESS) || hop.startsWith(NODE_READINESS + "$Evidence")) {
                return true;
            }
        }
        return false;
    }

    static void noMemberOfTheMemberReadinessIsCalled(final String root, final NeverSchedulesScan.Report closure) {
        final NeverSchedulesScan.Rules noCalls = new NeverSchedulesScan.Rules(Set.of(), List.of(), Set.of(), Map.of(),
                Map.of(), Map.of(NODE_READINESS, Map.of()));
        final List<NeverSchedulesScan.Finding> calls = new ArrayList<>();
        for (final String c : closure.classes()) {
            if (c.equals(NODE_READINESS) || c.startsWith(NODE_READINESS + "$")) {
                continue;
            }
            final Path file = kitRoot().resolve(c + ".class");
            try {
                calls.addAll(NeverSchedulesScan.scanClass(c, java.nio.file.Files.readAllBytes(file), noCalls));
            } catch (final java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        assertTrue("closure from " + root + " calls the member readiness: " + calls, calls.isEmpty());
    }

    @Test
    public void theFollowerAndTriggerPolicyClosuresAreClean() {
        for (final String root : List.of(FOLLOWER, POLICY)) {
            final NeverSchedulesScan.Report all = kitClosure(root);
            final List<NeverSchedulesScan.Finding> own = new ArrayList<>();
            int viaProbe = 0;
            for (final NeverSchedulesScan.Finding f : all.findings()) {
                if (throughNodeReadiness(f)) {
                    viaProbe++;
                } else {
                    own.add(f);
                }
            }
            final NeverSchedulesScan.Report r = new NeverSchedulesScan.Report(all.classesScanned(), all.classes(),
                    List.copyOf(own));
            System.out.println("GREEN scan: closure from " + root + " = " + r.classesScanned() + " kit classes, findings "
                    + own.size() + " (" + viaProbe + " inside " + NODE_READINESS + ", reached as the Probe record's"
                    + " outer class, never called), root " + kitRoot().toUri());
            assertClean(root, r);
            noMemberOfTheMemberReadinessIsCalled(root, all);
            assertTrue("the closure from " + root + " reaches the replay container",
                    root.equals(POLICY) || r.classes().contains("com/openexchange/cluster/log/ReplayContainer"));
            assertTrue("the closure from " + root + " does not reach ReplicaProcess (the one launcher)",
                    !r.classes().contains("com/openexchange/cluster/replica/ReplicaProcess"));
        }
    }

    @Test
    public void theShadowPackageInTheKitJarNeverReferencesTheBackup() {
        final NeverSchedulesScan.Rules backupOnly = new NeverSchedulesScan.Rules(
                Set.of("io/aeron/cluster/ClusterBackup", "io/aeron/cluster/ClusterBackupEventsListener",
                        "io/aeron/cluster/ClusterBackupMediaDriver"),
                List.of("com/openexchange/cluster/replica/"), Set.of(), Map.of(), Map.of(), Map.of());
        final NeverSchedulesScan.Report shadow = NeverSchedulesScan.scanDirectory(kitRoot(),
                n -> n.startsWith("com/openexchange/cluster/shadow/"), backupOnly);
        System.out.println("GREEN scan: cluster.shadow in the kit jar = " + shadow.classesScanned()
                + " classes, backup references " + shadow.findings().size());
        assertTrue("scanned " + shadow.classesScanned(), shadow.classesScanned() >= 20);
        assertTrue(shadow.findings().toString(), shadow.clean());
    }

    @Test
    public void controlTheMemberServiceAndAProbeAreFlagged() {
        RestoreClosureNeverSchedulesTest.nc("replica closure rules see scheduleTimer", "closure from " + ACS,
                () -> assertClean(ACS, coreClosure(ACS)));
        final String probe = RestoreClosureNeverSchedulesTest.PKG + "ForbiddenReplicaClosureProbe";
        final NeverSchedulesScan.Report r = new NeverSchedulesScan.Report(1, List.of(probe),
                NeverSchedulesScan.scanClass(probe, NoPurgeCallSiteInCoreTest.bytes(ForbiddenReplicaClosureProbe.class),
                        rules(false)));
        final String msg = RestoreClosureNeverSchedulesTest.nc("replica closure rules see the wall clock",
                "java/lang/System.currentTimeMillis", () -> assertClean(probe, r));
        assertTrue(msg, msg.contains("io/aeron/cluster/service/Cluster.scheduleTimer"));
        // the binding itself is in the main closure (a renamed root would shrink it to nothing silently)
        assertTrue(coreClosure(MAIN).classes().contains(MAIN + "$Binding"));
        assertTrue(CoreShadowReplicaMain.class.getName().replace('.', '/').equals(MAIN));
    }
}

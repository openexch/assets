// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * RCS S4 11.16 item 5 (hard rules 12.x: the restore never schedules, never publishes, never writes
 * {@code RecordingLog}, never purges), the closures over {@code ShadowRestore} and {@code MemberShadowHousekeeping}
 * (review FIX-4). Both hold an Aeron and an archive client and a clock by design (they replay the local archive), so
 * the RF-6 list of the applier closure does not apply; these rules name what they must never do. The closure follows
 * every assets class they reach (the applier and the engine included); kit and Aeron classes are leaves. Control: the
 * same rules over the service wrapper (which schedules the sweep) and over a purge probe must flag them.
 */
public class RcsClosureNeverSchedulesTest {

    static final String PKG = "com/openexchange/assets/infrastructure/persistence/";
    static final String SR = PKG + "ShadowRestore";
    static final String MSH = PKG + "MemberShadowHousekeeping";
    static final String ACS = PKG + "AssetsClusteredService";

    static NeverSchedulesScan.Rules restoreRules() {
        return new NeverSchedulesScan.Rules(
                Set.of("io/aeron/cluster/service/Cluster",
                        "io/aeron/cluster/service/ClientSession",
                        "com/openexchange/cluster/TimerPort",
                        "io/aeron/cluster/client/AeronCluster",
                        "io/aeron/Publication",
                        "io/aeron/ExclusivePublication",
                        "io/aeron/ConcurrentPublication"),
                List.of(),
                Set.of("scheduleTimer", "cancelTimer", "purgeRecording", "purgeSegments", "detachSegments",
                        "deleteDetachedSegments", "addPublication", "addExclusivePublication"),
                Map.of("io/aeron/cluster/RecordingLog", Set.of("appendTerm", "appendSnapshot",
                                "appendStandbySnapshot", "invalidateEntry", "invalidateLatestSnapshot",
                                "commitLogPosition", "removeEntry", "force"),
                        "com/openexchange/cluster/shadow/ShadowCatalog", Set.of("purge"),
                        "com/openexchange/cluster/shadow/ShadowRetention", Set.of("purge")),
                Map.of(),
                Map.of());
    }

    static String nc(final String id, final String named, final Runnable body) {
        try {
            body.run();
        } catch (final AssertionError e) {
            final String msg = String.valueOf(e.getMessage());
            if (!msg.contains(named)) {
                throw new AssertionError("negative control " + id + " failed for the wrong reason: <" + msg
                        + ">, expected <" + named + ">", e);
            }
            System.out.println("NC " + id + " -> " + msg);
            return msg;
        }
        throw new AssertionError("negative control " + id + " did not fail: the assertion cannot see its rule");
    }

    static NeverSchedulesScan.Report closure(final String root) {
        return NeverSchedulesScan.scanClosure(AssetsShadowEngineConformanceTest.assetsRoots(), root,
                "com/openexchange/assets/", restoreRules());
    }

    static void assertClean(final String root, final NeverSchedulesScan.Report r) {
        if (!r.clean()) {
            throw new AssertionError("closure from " + root + " schedules, publishes, writes RecordingLog or purges: "
                    + r.findings());
        }
    }

    @Test
    public void theRestoreClosureIsClean() {
        final NeverSchedulesScan.Report r = closure(SR);
        System.out.println("GREEN scan: closure from " + SR + " = " + r.classesScanned() + " classes, findings "
                + r.findings().size());
        assertClean(SR, r);
        assertTrue("the closure reaches the applier", r.classes().contains(PKG + "AssetsLogApplier"));
        assertTrue("the service wrapper is not in the restore closure", !r.classes().contains(ACS));
    }

    @Test
    public void theHousekeepingClosureIsClean() {
        final NeverSchedulesScan.Report r = closure(MSH);
        System.out.println("GREEN scan: closure from " + MSH + " = " + r.classesScanned() + " classes, findings "
                + r.findings().size());
        assertClean(MSH, r);
        assertTrue("the closure reaches the restore (skipSafe)", r.classes().contains(SR));
        assertTrue("the service wrapper is not in the housekeeping closure", !r.classes().contains(ACS));
    }

    @Test
    public void controlTheServiceWrapperAndAPurgeProbeAreFlagged() {
        nc("S4-11.16 closure rules see scheduleTimer", "closure from " + ACS + " schedules",
                () -> assertClean(ACS, closure(ACS)));
        final String probe = PKG + "ForbiddenAssetsPurgeProbe";
        final NeverSchedulesScan.Report r = new NeverSchedulesScan.Report(1, List.of(probe),
                NeverSchedulesScan.scanClass(probe, NoPurgeCallSiteInAssetsTest.bytes(ForbiddenAssetsPurgeProbe.class),
                        restoreRules()));
        nc("S4-11.16 closure rules see purgeRecording", "purgeRecording", () -> assertClean(probe, r));
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RCS S4 9.2 / 11.15 (review F6, D-S4-6), the assets half of the one-purge-owner rule: every main class of
 * assets-cluster ({@code target/classes}, never the test tree) is scanned for calls to
 * {@code AeronArchive.purgeRecording} and {@code ShadowCatalog.purge}, and the count must be ZERO. The kit's
 * {@code ShadowRetention} is the only production path that deletes a shadow ({@code ShadowPurgeCallSiteTest} holds the
 * kit side). Control: a probe class calling {@code purgeRecording} is flagged with the 11.15 text.
 */
public class NoPurgeCallSiteInAssetsTest {

    static final String PREFIX = "com/openexchange/assets/";

    static NeverSchedulesScan.Rules purgeRules() {
        return new NeverSchedulesScan.Rules(Set.of(), List.of(), Set.of(),
                Map.of("io/aeron/archive/client/AeronArchive", Set.of("purgeRecording"),
                        "com/openexchange/cluster/shadow/ShadowCatalog", Set.of("purge")),
                Map.of(), Map.of());
    }

    static Path mainRoot() {
        final Path root = AssetsShadowEngineConformanceTest.rootOf(AssetsLogApplier.class);
        final Path testRoot = AssetsShadowEngineConformanceTest.rootOf(NoPurgeCallSiteInAssetsTest.class);
        assertTrue("the main class root is not the test root: " + root, !root.equals(testRoot));
        return root;
    }

    /** Fails naming the first purge call site; zero is the only passing count. */
    static void check(final List<NeverSchedulesScan.Finding> findings) {
        for (final NeverSchedulesScan.Finding f : findings) {
            fail("purge call site in assets: " + f.className() + " (" + f.reference() + ")");
        }
    }

    static List<NeverSchedulesScan.Finding> mainFindings() {
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanDirectory(mainRoot(),
                n -> n.startsWith(PREFIX), purgeRules());
        assertTrue("the scan saw only " + report.classesScanned() + " assets-cluster main classes (expected >= 130)",
                report.classesScanned() >= 130); // 138 measured (assets-cluster main, S4)
        assertTrue("the scan reached the shadow housekeeping",
                report.classes().contains(PREFIX + "infrastructure/persistence/MemberShadowHousekeeping"));
        assertTrue("the scan reached the restore", report.classes().contains(PREFIX
                + "infrastructure/persistence/ShadowRestore"));
        System.out.println("GREEN scan: " + report.classesScanned() + " assets-cluster main classes, purge findings "
                + report.findings().size());
        return report.findings();
    }

    static byte[] bytes(final Class<?> c) {
        try (InputStream in = c.getResourceAsStream(c.getSimpleName() + ".class")) {
            return in.readAllBytes();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void assetsMainHasZeroPurgeCallSites() {
        check(mainFindings());
    }

    @Test
    public void aProbeCallingPurgeRecordingIsFlagged() {
        final List<NeverSchedulesScan.Finding> withProbe = new ArrayList<>(mainFindings());
        withProbe.addAll(NeverSchedulesScan.scanClass(PREFIX + "infrastructure/persistence/ForbiddenAssetsPurgeProbe",
                bytes(ForbiddenAssetsPurgeProbe.class), purgeRules()));
        RcsClosureNeverSchedulesTest.nc("S4-11.15 assets purge probe", "purge call site in assets: " + PREFIX
                + "infrastructure/persistence/ForbiddenAssetsPurgeProbe", () -> check(withProbe));
    }
}

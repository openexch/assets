// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.cluster.testkit.ScenarioText;
import com.openexchange.cluster.testkit.SnapshotSpliceRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * The determinism gate for point-in-time recovery: <b>restoring a snapshot and replaying the rest of
 * the log must reproduce the uninterrupted run exactly, byte for byte.</b>
 *
 * <p>Every committed {@code .scenario} is run twice for every position in it — once straight through,
 * once with a real snapshot/restore spliced in at that position — and both the final state bytes and
 * the emitted event stream are compared. A restore is a genuine round trip through
 * {@link BalanceSnapshotCodec} into a brand-new engine, which is what a node coming up from a bundle
 * does.</p>
 *
 * <p>This is the property the durable ledger archive rests on. A bundle carries a snapshot plus the
 * log after it; replaying it answers "what was the ledger at position P" only if the answer does not
 * depend on <em>where</em> the last snapshot happened to fall. Splicing at every position is the point:
 * the interesting divergences hide at the boundaries (a hold placed before and released after, a
 * settle whose high-water rides through, an account first touched on either side of the cut).</p>
 *
 * <p><b>Scope, stated honestly.</b> This proves the ENGINE is deterministic given (snapshot, log). It
 * does not exercise Aeron's replay transport, so it is not by itself proof that a real bundle restores
 * — that is the debug-cluster work, and it depends on this holding first.</p>
 *
 * <p>Distinct from {@link EngineSnapshotReplayTest}, which compares only the event stream on five
 * hand-written cases: an engine restored from a snapshot can emit identical events while holding a
 * differently-ordered map, and it is the bytes that a replay is verified against.</p>
 */
@RunWith(Parameterized.class)
public class SnapshotReplayEquivalenceTest {

    private static final Path DIR = Path.of("src", "test", "resources", "determinism");

    @Parameters(name = "{0}")
    public static Collection<Object[]> scenarios() throws IOException {
        try (Stream<Path> s = Files.list(DIR)) {
            return s.filter(p -> p.toString().endsWith(".scenario"))
                    .sorted()
                    .map(p -> new Object[]{stripExt(p), p})
                    .collect(Collectors.toList());
        }
    }

    private static String stripExt(Path p) {
        final String n = p.getFileName().toString();
        return n.substring(0, n.length() - ".scenario".length());
    }

    private final String name;
    private final Path scenarioFile;

    public SnapshotReplayEquivalenceTest(String name, Path scenarioFile) {
        this.name = name;
        this.scenarioFile = scenarioFile;
    }

    @Test
    public void restoringAtAnyPointReproducesTheUninterruptedRun() throws IOException {
        // The kit runner: for every cut, a fresh replica applies the prefix, snapshots (twice: the bytes
        // must be reproducible), restores from those bytes (which must re-serialize identically), applies
        // the suffix, and must end with the uninterrupted run's events and final image.
        final SnapshotSpliceRunner.Report report = SnapshotSpliceRunner.run(AssetsReplica::new,
                Files.readAllLines(scenarioFile, StandardCharsets.UTF_8));
        assertEquals("every boundary is a cut", report.lines() + 1, report.cuts());
    }

    /**
     * A snapshot taken twice from the same engine must be identical: the codec reads state, it must
     * not consume or reorder anything as a side effect.
     */
    @Test
    public void serializingTwiceYieldsTheSameBytes() throws IOException {
        final AssetsScenarioRunner runner = AssetsScenarioRunner.newRunner();
        runner.execAll(ScenarioText.executableLines(Files.readAllLines(scenarioFile, StandardCharsets.UTF_8)));
        assertArrayEquals("serialize is not side-effect free in '" + name + "'",
                runner.snapshotBytes(), runner.snapshotBytes());
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The retired-name guard on the real entry point: {@code com.openexchange.core.Main} in a child JVM with a retired
 * environment variable, then with a retired system property, exits 1 with exactly the one refusal line on stderr and
 * starts nothing. The source check proves every other process entry point calls the same guard first.
 */
public class RetiredSettingsProcessTest {

    private static final String ENV_LINE = "FATAL: core-cluster.jar refused to start: the environment variable"
            + " ASSETS_ROLE is retired; it is now CORE_ROLE (rename the setting; the old name is never read)";
    private static final String PROPERTY_LINE = "FATAL: core-cluster.jar refused to start: the system property"
            + " assets.node.port is retired; it is now core.node.port (rename the setting; the old name is never read)";

    /** Entry point sources (relative to the reactor root) and the process name each passes to the guard. */
    private static final Map<String, String> ENTRY_POINTS = Map.of(
            "core-cluster/src/main/java/com/openexchange/core/Main.java", "core-cluster.jar",
            "core-cluster/src/main/java/com/openexchange/core/infrastructure/persistence/BundleCaptureMain.java",
            "core-cluster bundle capture",
            "core-cluster/src/test/java/com/openexchange/core/feedharness/FeedHarnessMain.java", "core-feed-harness.jar",
            "core-bridge/src/main/java/com/openexchange/core/bridge/BridgeMain.java", "core-bridge.jar",
            "core-bridge/src/main/java/com/openexchange/core/bridge/CoreDump.java", "core-bridge.jar dump",
            "core-bridge/src/main/java/com/openexchange/core/bridge/CoreSetMoneyJournal.java",
            "core-bridge.jar set-money-journal",
            "core-projector/src/main/java/com/openexchange/core/projector/ProjectorMain.java", "core-projector.jar",
            "core-loadgen/src/main/java/com/openexchange/core/loadgen/MoneyLoadGenerator.java", "core-loadgen.jar");

    @Test
    public void aRetiredEnvironmentVariableRefusesTheMemberWithOneLine() throws Exception {
        final Run r = run(Map.of("ASSETS_ROLE", "member"), List.of());
        assertEquals("exit code; stderr=" + r.err, 1, r.exit);
        assertEquals("stderr", ENV_LINE, r.err.strip());
        assertEquals("nothing started: stdout=" + r.out, "", r.out.strip());
    }

    @Test
    public void aRetiredSystemPropertyRefusesTheMemberWithOneLine() throws Exception {
        final Run r = run(Map.of(), List.of("-Dassets.node.port=9790"));
        assertEquals("exit code; stderr=" + r.err, 1, r.exit);
        assertEquals("stderr", PROPERTY_LINE, r.err.strip());
        assertEquals("nothing started: stdout=" + r.out, "", r.out.strip());
    }

    @Test
    public void everyProcessEntryPointCallsTheGuardFirst() throws IOException {
        final Path root = reactorRoot();
        final Pattern firstStatement = Pattern.compile(
                "static void main\\((?:final )?String\\[] args\\) \\{\\s*(?://[^\\n]*\\n\\s*)*"
                        + "com\\.openexchange\\.core\\.config\\.RetiredSettings\\.refuseRetired\\(\"([^\"]+)\"");
        for (final Map.Entry<String, String> e : ENTRY_POINTS.entrySet()) {
            final String src = Files.readString(root.resolve(e.getKey()), StandardCharsets.UTF_8);
            final Matcher m = firstStatement.matcher(src);
            assertTrue(e.getKey() + ": main does not call RetiredSettings.refuseRetired first", m.find());
            assertEquals(e.getKey(), e.getValue(), m.group(1));
        }
    }

    private static Path reactorRoot() {
        Path p = Paths.get("").toAbsolutePath();
        while (p != null && !Files.isDirectory(p.resolve("core-common"))) {
            p = p.getParent();
        }
        if (p == null) {
            throw new IllegalStateException("no reactor root above " + Paths.get("").toAbsolutePath());
        }
        return p;
    }

    private record Run(int exit, String out, String err) { }

    private static Run run(final Map<String, String> env, final List<String> jvmArgs) throws Exception {
        final Path dir = Files.createTempDirectory("core-retired-settings-");
        final List<String> cmd = new ArrayList<>();
        cmd.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        cmd.addAll(jvmArgs);
        cmd.addAll(List.of("-cp", System.getProperty("java.class.path"), "com.openexchange.core.Main"));
        final ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir.toFile());
        // A clean child: no engine setting from the developer's shell, then the row's.
        pb.environment().keySet().removeIf(k -> k.startsWith("CORE_") || k.startsWith("ASSETS_")
                || k.startsWith("AE_") || k.startsWith("RCS_") || k.startsWith("SNAPSHOT_") || k.startsWith("CLUSTER_")
                || k.startsWith("TRANSPORT_") || k.startsWith("AERON_") || k.startsWith("BASE_DIR"));
        pb.environment().putAll(env);
        final File out = dir.resolve("out.txt").toFile();
        final File err = dir.resolve("err.txt").toFile();
        pb.redirectOutput(out).redirectError(err);
        final Process p = pb.start();
        if (!p.waitFor(60, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("the child did not exit within 60 s: it started instead of refusing; stderr="
                    + Files.readString(err.toPath()));
        }
        return new Run(p.exitValue(), Files.readString(out.toPath()), Files.readString(err.toPath()));
    }
}

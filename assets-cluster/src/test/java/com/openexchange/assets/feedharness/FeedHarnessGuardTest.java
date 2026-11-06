// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S9h 11.6 (R1) and 9.2 (review FIX-6): the harness stays out of every runtime path. No production source imports
 * the harness; the production classes and jar carry no harness class; the pump imports no archive client; the build's
 * production code equals {@code money_schema_ref}'s (b05b8e8) file for file; and a built harness jar names the pins its
 * consumers check.
 */
public class FeedHarnessGuardTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** The assets repository root (surefire runs in the module directory). */
    static Path root() {
        Path p = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (p != null && !Files.isRegularFile(p.resolve("assets-cluster/pom.xml"))) {
            p = p.getParent();
        }
        if (p == null) {
            throw new IllegalStateException("no assets root above " + System.getProperty("user.dir"));
        }
        return p;
    }

    /** Production sources under {@code <module>/src/main/} that name the harness (the R1 violations). */
    public static List<String> harnessReferencesInMain(final Path repo) throws IOException {
        final List<String> bad = new ArrayList<>();
        try (Stream<Path> modules = Files.list(repo)) {
            for (final Path m : modules.toList()) {
                final Path main = m.resolve("src/main");
                if (!Files.isDirectory(main)) {
                    continue;
                }
                try (Stream<Path> files = Files.walk(main)) {
                    for (final Path f : files.filter(x -> x.toString().endsWith(".java")).toList()) {
                        final String text = Files.readString(f, StandardCharsets.UTF_8);
                        if (text.contains("com.openexchange.assets.feedharness") || text.contains("CoreApplyRunner")) {
                            bad.add(repo.relativize(f).toString());
                        }
                    }
                }
            }
        }
        return bad;
    }

    @Test
    public void noProductionSourceImportsTheHarness() throws Exception {
        assertEquals("R1: production sources naming the harness", List.of(), harnessReferencesInMain(root()));
    }

    /** The RED as a check: a probe import in a scratch main class is found (recorded in evidence, never in the tree). */
    @Test
    public void aProbeImportInAScratchMainClassIsFound() throws Exception {
        final Path repo = tmp.newFolder("repo").toPath();
        final Path probe = repo.resolve("assets-cluster/src/main/java/com/openexchange/assets/Probe.java");
        Files.createDirectories(probe.getParent());
        Files.writeString(probe, "package com.openexchange.assets;\nimport com.openexchange.assets.feedharness.FeedHarnessMain;\n"
                + "final class Probe { }\n");
        assertEquals(List.of("assets-cluster/src/main/java/com/openexchange/assets/Probe.java"), harnessReferencesInMain(repo));
    }

    @Test
    public void theProductionClassesCarryNoHarnessClass() throws Exception {
        final Path classes = root().resolve("assets-cluster/target/classes");
        assertTrue("target/classes exists", Files.isDirectory(classes));
        assertTrue("no feedharness package in the production classes",
                !Files.exists(classes.resolve("com/openexchange/assets/feedharness")));
        assertTrue("no CoreApplyRunner in the production classes", !Files.exists(
                classes.resolve("com/openexchange/assets/infrastructure/persistence/CoreApplyRunner.class")));
        final Path jar = root().resolve("assets-cluster/target/assets-cluster.jar");
        if (Files.isRegularFile(jar)) {
            try (JarFile j = new JarFile(jar.toFile())) {
                final long harness = j.stream().filter(e -> e.getName().contains("feedharness")
                        || e.getName().contains("CoreApplyRunner") || e.getName().startsWith("com/match/")).count();
                assertEquals("the production assets-cluster.jar carries no harness class and no match-common", 0, harness);
            }
        }
    }

    @Test
    public void thePumpImportsNoArchiveClient() throws Exception {
        final Path pump = root().resolve("assets-cluster/src/test/java/com/openexchange/assets/feedharness/ReferencePump.java");
        final String text = Files.readString(pump, StandardCharsets.UTF_8);
        assertTrue("D-S9h-6: the pump reads no archive", !text.contains("io.aeron.archive"));
        assertTrue("and uses no T1 journal host", !text.contains("T1Journal"));
    }

    /** FIX-6: this build's production code is b05b8e8's, file for file (the empty {@code src/main} diff). */
    @Test
    public void theMainTreeEqualsTheMoneySchemaRef() throws Exception {
        final Map<String, String> work = MainTree.workingTree(root());
        final Map<String, String> ref = MainTree.atRef(root(), MainTree.MONEY_SCHEMA_REF);
        assertTrue("non-vacuous: the ref has main files", ref.size() > 100);
        assertEquals("src/main differs from " + MainTree.MONEY_SCHEMA_REF, List.of(), MainTree.diff(work, ref));
        assertEquals(MainTree.id(ref), MainTree.id(work));
    }

    /** The harness jar (when the feed-harness profile built it) names the pins of 2.3 and 9.2. */
    @Test
    public void aBuiltHarnessJarNamesItsPins() throws Exception {
        final Path jar = root().resolve("assets-cluster/target/assets-feed-harness.jar");
        org.junit.Assume.assumeTrue("the feed-harness profile built the jar", Files.isRegularFile(jar));
        try (JarFile j = new JarFile(jar.toFile())) {
            assertEquals("com.openexchange.assets.feedharness.FeedHarnessMain",
                    j.getManifest().getMainAttributes().getValue("Main-Class"));
            assertEquals("Add-Opens (review FIX-2)", FeedHarnessJarBuilder.ADD_OPENS,
                    j.getManifest().getMainAttributes().getValue("Add-Opens"));
            final Properties p = new Properties();
            try (InputStream s = j.getInputStream(j.getEntry(FeedHarnessMain.PROPERTIES_RESOURCE))) {
                p.load(s);
            }
            assertEquals(FeedFiles.sha256Hex(Files.readAllBytes(root().resolve(
                    "assets-common/src/main/resources/sbe/money-schema.xml"))), p.getProperty("money_schema_sha256"));
            assertEquals(FeedHarnessMain.JOURNAL_SCHEMA_SHA256, p.getProperty("me_journal_schema_sha256"));
            assertEquals("true", p.getProperty("main_tree_equals_ref"));
            assertEquals(MainTree.MONEY_SCHEMA_REF, p.getProperty("main_tree_ref"));
            assertEquals("1.0-runtime.20260929.r02h", p.getProperty("match_journal_version"));
            assertTrue("the jar carries match-common's schema 4", j.getEntry(FeedHarnessMain.JOURNAL_SCHEMA_RESOURCE) != null);
            assertTrue("and the production main", j.getEntry("com/openexchange/assets/Main.class") != null);
        }
    }

    /**
     * Review FIX-2: the built jar starts with plain {@code java -jar} and no JVM flag ({@code Add-Opens} in the manifest),
     * for {@code random} and {@code serve}. The RED as a check: the same jar with the attribute removed fails on its
     * first buffer access (agrona's UnsafeApi), exit 70.
     */
    @Test
    public void theJarStartsWithPlainJavaJar() throws Exception {
        final Path jar = root().resolve("assets-cluster/target/assets-feed-harness.jar");
        org.junit.Assume.assumeTrue("the feed-harness profile built the jar", Files.isRegularFile(jar));
        assertEquals("random, plain java -jar", 0, plain(jar, "random", "3", "500", tmp.newFolder("r").toString()));
        assertEquals("serve, plain java -jar (QUIT)", 0, plainServe(jar));
        final Path stripped = tmp.getRoot().toPath().resolve("stripped.jar");
        stripAddOpens(jar, stripped);
        assertEquals("without Add-Opens the first buffer access fails: 70", 70, plain(stripped, "random", "3", "500",
                tmp.newFolder("r2").toString()));
    }

    static int plain(final Path jar, final String... args) throws Exception {
        final List<String> cmd = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", jar.toString()));
        cmd.addAll(List.of(args));
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertTrue(p.waitFor(120, java.util.concurrent.TimeUnit.SECONDS));
        return p.exitValue();
    }

    static int plainServe(final Path jar) throws Exception {
        final Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-jar",
                jar.toString(), "serve").redirectError(ProcessBuilder.Redirect.DISCARD).start();
        p.getOutputStream().write(CoreServe.request(CoreServe.QUIT, new byte[0]));
        p.getOutputStream().close();
        final byte[] reply = p.getInputStream().readAllBytes();
        assertTrue(p.waitFor(60, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals("BYE", CoreServe.QUIT | 0x80, reply[4] & 0xFF);
        return p.exitValue();
    }

    /** A copy of the jar whose manifest has no Add-Opens attribute. */
    static void stripAddOpens(final Path from, final Path to) throws Exception {
        try (java.util.zip.ZipFile z = new java.util.zip.ZipFile(from.toFile());
             java.util.zip.ZipOutputStream out = new java.util.zip.ZipOutputStream(Files.newOutputStream(to))) {
            final var en = z.entries();
            while (en.hasMoreElements()) {
                final java.util.zip.ZipEntry e = en.nextElement();
                byte[] b = z.getInputStream(e).readAllBytes();
                if (e.getName().equals("META-INF/MANIFEST.MF")) {
                    final java.util.jar.Manifest m = new java.util.jar.Manifest(new java.io.ByteArrayInputStream(b));
                    m.getMainAttributes().remove(new java.util.jar.Attributes.Name("Add-Opens"));
                    final java.io.ByteArrayOutputStream mb = new java.io.ByteArrayOutputStream();
                    m.write(mb);
                    b = mb.toByteArray();
                }
                out.putNextEntry(new java.util.zip.ZipEntry(e.getName()));
                out.write(b);
                out.closeEntry();
            }
        }
    }
}

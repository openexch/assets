// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * RC2 S9h 2.3: builds {@code assets-feed-harness.jar} from the test classpath it runs on (the {@code feed-harness}
 * profile runs it with exec:exec and {@code <classpath/>}). Reproducible by construction (review N-5): every entry is
 * collected first (the first classpath element wins on a duplicate path, as the JVM's lookup does), written in sorted path
 * order, with the commit time ({@code git log -1 --format=%ct}) as the entry time and nothing time- or host-dependent in
 * any file; signature files and {@code module-info.class} are dropped. The manifest is the first entry and names
 * {@code Main-Class: com.openexchange.assets.feedharness.FeedHarnessMain} and {@code Add-Opens} (plain {@code java -jar}).
 *
 * <p>Usage: {@code FeedHarnessJarBuilder <out.jar> <assets-root> <match.journal.version> <aeron.version> <sbe.version>}</p>
 */
public final class FeedHarnessJarBuilder {

    public static final String ADD_OPENS = "java.base/jdk.internal.misc java.base/java.nio java.base/sun.nio.ch";

    private FeedHarnessJarBuilder() {
    }

    public static void main(final String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("usage: FeedHarnessJarBuilder <out.jar> <assets-root> <match.journal.version> <aeron.version>"
                    + " <sbe.version>");
            System.exit(64);
        }
        final Path out = Path.of(args[0]);
        final Path root = Path.of(args[1]).toRealPath();
        final Map<String, byte[]> entries = new TreeMap<>();
        for (final String element : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (element.isEmpty()) {
                continue;
            }
            final Path p = Path.of(element);
            if (Files.isDirectory(p)) {
                collectDirectory(p, entries);
            } else if (Files.isRegularFile(p) && element.endsWith(".jar") && !p.toAbsolutePath().equals(out.toAbsolutePath())) {
                collectJar(p, entries);
            }
        }
        final String journalSha = FeedHarnessMain.journalSchemaSha256(FeedHarnessJarBuilder.class.getClassLoader());
        if (!FeedHarnessMain.JOURNAL_SCHEMA_SHA256.equals(journalSha)) {
            throw new IllegalStateException("H1: the test classpath's match-common carries schema sha " + journalSha);
        }
        final String commit = MainTree.git(root, null, "rev-parse", "HEAD").trim();
        final long commitTime = Long.parseLong(MainTree.git(root, null, "log", "-1", "--format=%ct", "HEAD").trim());
        final boolean dirty = !MainTree.git(root, null, "status", "--porcelain").isBlank();
        final Map<String, String> work = MainTree.workingTree(root);
        final Map<String, String> ref = MainTree.atRef(root, MainTree.MONEY_SCHEMA_REF);
        final Map<String, String> props = new LinkedHashMap<>();
        props.put("assets_commit", commit);
        props.put("assets_worktree_dirty", Boolean.toString(dirty));
        props.put("money_schema_sha256", FeedFiles.sha256Hex(
                Files.readAllBytes(root.resolve("assets-common/src/main/resources/sbe/money-schema.xml"))));
        props.put("me_journal_schema_sha256", journalSha);
        props.put("match_journal_version", args[2]);
        props.put("aeron_version", args[3]);
        props.put("sbe_version", args[4]);
        props.put("main_tree_id", MainTree.id(work));
        props.put("main_tree_ref", MainTree.MONEY_SCHEMA_REF);
        props.put("main_tree_ref_id", MainTree.id(ref));
        props.put("main_tree_equals_ref", Boolean.toString(MainTree.diff(work, ref).isEmpty()));
        final StringBuilder p = new StringBuilder("# RC2 S9h feed harness build properties (contract 2.3)\n");
        for (final Map.Entry<String, String> e : props.entrySet()) {
            p.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        entries.put(FeedHarnessMain.PROPERTIES_RESOURCE, p.toString().getBytes(StandardCharsets.UTF_8));
        // review FIX-2: plain `java -jar` must work; agrona's UnsafeApi needs jdk.internal.misc, Aeron's driver
        // java.nio and sun.nio.ch (the opens every launcher of this repo passes as flags)
        final String manifest = "Manifest-Version: 1.0\r\nMain-Class: com.openexchange.assets.feedharness.FeedHarnessMain\r\n"
                + manifestLine("Add-Opens: " + ADD_OPENS) + "Created-By: FeedHarnessJarBuilder\r\n\r\n";
        final LocalDateTime time = LocalDateTime.ofEpochSecond(commitTime, 0, ZoneOffset.UTC);
        final Path tmp = out.resolveSibling(out.getFileName() + ".tmp");
        try (OutputStream fo = Files.newOutputStream(tmp); ZipOutputStream zip = new ZipOutputStream(fo)) {
            write(zip, "META-INF/MANIFEST.MF", manifest.getBytes(StandardCharsets.UTF_8), time);
            for (final Map.Entry<String, byte[]> e : entries.entrySet()) {
                write(zip, e.getKey(), e.getValue(), time);
            }
        }
        Files.move(tmp, out, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        System.out.println("feed-harness jar " + out + " sha256 " + FeedFiles.sha256Hex(Files.readAllBytes(out)) + " entries "
                + (entries.size() + 1));
        for (final Map.Entry<String, String> e : props.entrySet()) {
            System.out.println("  " + e.getKey() + "=" + e.getValue());
        }
    }

    /** One manifest attribute, wrapped at 72 bytes with continuation lines (JAR spec). */
    static String manifestLine(final String line) {
        final StringBuilder sb = new StringBuilder();
        String rest = line;
        int width = 72;
        while (rest.length() > width) {
            sb.append(rest, 0, width).append("\r\n ");
            rest = rest.substring(width);
            width = 71;
        }
        return sb.append(rest).append("\r\n").toString();
    }

    private static void write(final ZipOutputStream zip, final String name, final byte[] bytes, final LocalDateTime time)
            throws IOException {
        final ZipEntry z = new ZipEntry(name);
        z.setTimeLocal(time);
        zip.putNextEntry(z);
        zip.write(bytes);
        zip.closeEntry();
    }

    private static boolean skipped(final String name) {
        final String upper = name.toUpperCase(java.util.Locale.ROOT);
        return name.endsWith("/") || upper.equals("META-INF/MANIFEST.MF") || name.equals("module-info.class")
                || name.endsWith("/module-info.class")
                || (upper.startsWith("META-INF/") && (upper.endsWith(".SF") || upper.endsWith(".DSA") || upper.endsWith(".RSA")
                        || upper.endsWith(".EC")))
                || name.equals(FeedHarnessMain.PROPERTIES_RESOURCE);
    }

    private static void collectDirectory(final Path dir, final Map<String, byte[]> entries) throws IOException {
        try (Stream<Path> s = Files.walk(dir)) {
            for (final Path f : (Iterable<Path>) s.filter(Files::isRegularFile).sorted()::iterator) {
                final String name = dir.relativize(f).toString().replace(File.separatorChar, '/');
                if (!skipped(name)) {
                    entries.putIfAbsent(name, Files.readAllBytes(f));
                }
            }
        }
    }

    private static void collectJar(final Path jar, final Map<String, byte[]> entries) throws IOException {
        try (ZipFile z = new ZipFile(jar.toFile())) {
            final Enumeration<? extends ZipEntry> en = z.entries();
            while (en.hasMoreElements()) {
                final ZipEntry e = en.nextElement();
                if (e.isDirectory() || skipped(e.getName())) {
                    continue;
                }
                try (InputStream in = z.getInputStream(e)) {
                    final ByteArrayOutputStream b = new ByteArrayOutputStream();
                    in.transferTo(b);
                    entries.putIfAbsent(e.getName(), b.toByteArray());
                }
            }
        }
    }
}

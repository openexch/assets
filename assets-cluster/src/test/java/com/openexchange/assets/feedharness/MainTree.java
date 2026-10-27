// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * RC2 S9h 9.2 (review FIX-6): the identity of the production code a harness build runs. {@code id} is the sha256 of the
 * sorted lines {@code <path> <git blob id>} of every file under {@code <module>/src/main/} (tracked and untracked, the
 * working tree as it is), so a harness jar can prove its core equals {@code money_schema_ref}'s: the same function over
 * {@code git ls-tree -r <ref>} must give the same id.
 */
public final class MainTree {

    /** The core the harness is graded against (cluster-tools {@code money_schema_ref}). */
    public static final String MONEY_SCHEMA_REF = "b05b8e8";

    private MainTree() {
    }

    static boolean isMain(final String path) {
        final int i = path.indexOf("/src/main/");
        return i > 0 && path.indexOf('/') == i;
    }

    /** path -> blob id of the working tree's main files. */
    public static Map<String, String> workingTree(final Path root) throws IOException {
        final List<String> files = new ArrayList<>();
        for (final String f : git(root, null, "ls-files", "-co", "--exclude-standard").split("\n")) {
            if (!f.isEmpty() && isMain(f) && root.resolve(f).toFile().isFile()) {
                files.add(f);
            }
        }
        final String ids = git(root, String.join("\n", files) + "\n", "hash-object", "--stdin-paths");
        final String[] lines = ids.split("\n");
        if (lines.length != files.size()) {
            throw new IOException("hash-object returned " + lines.length + " ids for " + files.size() + " files");
        }
        final Map<String, String> out = new TreeMap<>();
        for (int i = 0; i < files.size(); i++) {
            out.put(files.get(i), lines[i]);
        }
        return out;
    }

    /** path -> blob id of the main files at {@code ref}. */
    public static Map<String, String> atRef(final Path root, final String ref) throws IOException {
        final Map<String, String> out = new TreeMap<>();
        for (final String line : git(root, null, "ls-tree", "-r", ref).split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            final int tab = line.indexOf('\t');
            final String path = line.substring(tab + 1);
            final String[] meta = line.substring(0, tab).split(" ");
            if (isMain(path) && meta[1].equals("blob")) {
                out.put(path, meta[2]);
            }
        }
        return out;
    }

    public static String id(final Map<String, String> tree) {
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, String> e : tree.entrySet()) {
            sb.append(e.getKey()).append(' ').append(e.getValue()).append('\n');
        }
        return FeedFiles.sha256Hex(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** The paths whose blob differs between {@code a} and {@code b} (added, removed or changed). */
    public static List<String> diff(final Map<String, String> a, final Map<String, String> b) {
        final TreeMap<String, String> all = new TreeMap<>(a);
        all.putAll(b);
        final List<String> out = new ArrayList<>();
        for (final String p : all.keySet()) {
            if (!java.util.Objects.equals(a.get(p), b.get(p))) {
                out.add(p);
            }
        }
        return out;
    }

    public static String git(final Path root, final String stdin, final String... args) throws IOException {
        final List<String> cmd = new ArrayList<>();
        cmd.add("git");
        cmd.add("-C");
        cmd.add(root.toString());
        cmd.addAll(List.of(args));
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(false).start();
        if (stdin != null) {
            try (OutputStream o = p.getOutputStream()) {
                o.write(stdin.getBytes(StandardCharsets.UTF_8));
            }
        } else {
            p.getOutputStream().close();
        }
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream s = p.getInputStream()) {
            s.transferTo(out);
        }
        final byte[] errBytes = p.getErrorStream().readAllBytes();
        try {
            if (p.waitFor() != 0) {
                throw new IOException("git " + String.join(" ", args) + " failed: " + new String(errBytes, StandardCharsets.UTF_8));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        return out.toString(StandardCharsets.UTF_8);
    }
}

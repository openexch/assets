// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The production entry point {@code com.openexchange.assets.Main} in a child JVM on the test
 * classpath (the {@code EngineDriverLifecycleProcessTest} shape), stdout and stderr in separate files so a row can
 * assert what reached stderr. The child's environment is the test's minus every engine setting family, plus the row's.
 */
final class ReplicaChild {

    static final String MAIN = "com.openexchange.assets.Main";
    private static final List<String> SCRUBBED_PREFIXES = List.of("REPLICA_", "SHADOW_", "CLUSTER_", "TRANSPORT_",
            "AERON_", "ASSETS_", "AE_AUTH_", "RCS_", "SNAPSHOT_", "BASE_DIR", "ENGINE_");

    final Process process;
    final Path out;
    final Path err;
    final String name;

    private ReplicaChild(final String name, final Process process, final Path out, final Path err) {
        this.name = name;
        this.process = process;
        this.out = out;
        this.err = err;
    }

    static List<String> java() {
        return new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx1g", "-Daeron.threading.mode=SHARED", "-Daeron.archive.threading.mode=SHARED"));
    }

    /** {@code java ... com.openexchange.assets.Main <args>} with {@code env} on top of the scrubbed environment. */
    static ReplicaChild start(final Path dir, final String name, final Map<String, String> env,
                              final List<String> args) throws IOException {
        final List<String> cmd = java();
        cmd.add("-XX:ErrorFile=" + dir.resolve(name + "-hs_err_pid%p.log"));
        cmd.addAll(List.of("-cp", System.getProperty("java.class.path"), MAIN));
        cmd.addAll(args);
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().keySet().removeIf(k -> SCRUBBED_PREFIXES.stream().anyMatch(k::startsWith));
        pb.environment().putAll(env);
        final Path out = dir.resolve(name + ".out");
        final Path err = dir.resolve(name + ".err");
        final Process p = pb.redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        System.out.println("[REPLICA-CHILD] " + name + " pid " + p.pid() + " args " + args + " env " + env.keySet());
        return new ReplicaChild(name, p, out, err);
    }

    String out() throws IOException {
        return Files.exists(out) ? Files.readString(out) : "";
    }

    String err() throws IOException {
        return Files.exists(err) ? Files.readString(err) : "";
    }

    /** Both streams' last lines, for failure texts. */
    String tails() throws IOException {
        return "\n--- " + name + " stdout:" + tail(out()) + "\n--- " + name + " stderr:" + tail(err());
    }

    /** Waits for exit; returns the exit status, or throws naming the child and its output. */
    int awaitExit(final long timeoutMs) throws Exception {
        if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new AssertionError(name + " did not exit within " + timeoutMs + " ms" + tails());
        }
        return process.exitValue();
    }

    void awaitLine(final String needle, final long timeoutMs) throws Exception {
        awaitLine(needle, timeoutMs, () -> {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * @param pause one wait step: the in-process rigs pass their client pump, so the test's cluster sessions keep
     *              their keepalives while a child starts (a session idle past the members' session timeout is closed)
     */
    void awaitLine(final String needle, final long timeoutMs, final Runnable pause) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!(out() + err()).contains(needle)) {
            if (!process.isAlive()) {
                throw new AssertionError(name + " exited (" + process.exitValue() + ") before <" + needle + ">"
                        + tails());
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no <" + needle + "> from " + name + " within " + timeoutMs + " ms" + tails());
            }
            pause.run();
        }
    }

    void destroyForcibly() throws InterruptedException {
        process.destroyForcibly();
        process.waitFor(20, TimeUnit.SECONDS);
    }

    static String line(final String text, final String needle) {
        for (final String l : text.split("\n")) {
            if (l.contains(needle)) {
                return l.trim();
            }
        }
        return null;
    }

    static String tail(final String text) {
        final String[] lines = text.split("\n");
        final StringBuilder sb = new StringBuilder();
        for (int i = Math.max(0, lines.length - 30); i < lines.length; i++) {
            sb.append('\n').append(lines[i]);
        }
        return sb.toString();
    }
}

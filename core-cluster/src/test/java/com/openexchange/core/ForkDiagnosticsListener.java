// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

/**
 * The ci-diagnostics profile's surefire listener: appends {@code <pid> TAB <test class>} once per class to the file
 * {@code core.diagnostics.pids} names, so a CI step can upload the GC and safepoint log ({@code jvm-<pid>.log}) of
 * only the classes that failed. Without the property it does nothing.
 */
public final class ForkDiagnosticsListener extends RunListener {

    private final Set<String> seen = new HashSet<>();

    private void record(final Description d) {
        final String file = System.getProperty("core.diagnostics.pids");
        final String cls = d == null ? null : d.getClassName();
        if (file == null || file.isBlank() || cls == null || cls.isBlank() || "null".equals(cls)
                || !seen.add(cls)) {
            return;
        }
        try {
            Files.writeString(Path.of(file), ProcessHandle.current().pid() + "\t" + cls + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (final IOException e) {
            System.err.println("[ci-diagnostics] cannot record the fork pid in " + file + ": " + e);
        }
    }

    @Override
    public void testSuiteStarted(final Description description) {
        record(description);
    }

    @Override
    public void testStarted(final Description description) {
        record(description);
    }

    @Override
    public void testFailure(final Failure failure) {
        record(failure.getDescription());
    }
}

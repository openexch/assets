// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 section 9 (G4, K5): S7 adds no cluster timer, no timer kind, no deadline kind and no wall-clock read; the feed
 * latch has no deadline and every staleness rule is decided by the cursor. A structural guard over the comment- and
 * literal-stripped main sources ({@link ApplierOwnershipGuardTest#strip}). It does not fail on the base by design; its
 * RED is a mutation probe: {@code -Ds7.guard.root=<copy of src/main/java>} points the same scan at a mutated tree (a third
 * {@code scheduleTimer}, a fourth {@code KIND_}), which must fail.
 */
public class NoNewClusterTimerCallSiteTest {

    static final Path ROOT = Paths.get(System.getProperty("s7.guard.root", "src/main/java"));
    private static final String PKG = "com/openexchange/assets/";

    private static List<Path> sources(final Path root) throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".java")).filter(p -> !p.toString().contains("/generated/"))
                    .sorted().collect(Collectors.toList());
        }
    }

    private static String stripped(final Path p) throws IOException {
        return ApplierOwnershipGuardTest.strip(Files.readString(p, StandardCharsets.UTF_8));
    }

    private static int count(final Pattern pattern, final String text) {
        final Matcher m = pattern.matcher(text);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    @Test
    public void noNewClusterTimerCallSite() throws IOException {
        final Pattern schedule = Pattern.compile("\\bscheduleTimer\\s*\\(");
        final Pattern cancel = Pattern.compile("\\bcancelTimer\\s*\\(");
        final List<String> scheduleSites = new ArrayList<>();
        int cancels = 0;
        for (final Path p : sources(ROOT)) {
            final String src = stripped(p);
            final Matcher m = schedule.matcher(src);
            while (m.find()) {
                final int eol = src.indexOf('\n', m.end());
                scheduleSites.add(p.getFileName() + ": " + src.substring(m.start(), eol < 0 ? src.length() : eol).trim());
            }
            cancels += count(cancel, src);
        }
        assertEquals("exactly two scheduleTimer( call sites (sweep and egress drain), got " + scheduleSites, 2, scheduleSites.size());
        for (final String site : scheduleSites) {
            assertTrue("every scheduleTimer is in AssetsClusteredService: " + site, site.startsWith("AssetsClusteredService.java: "));
        }
        assertTrue("one per-term sweep timer: " + scheduleSites,
                scheduleSites.stream().anyMatch(s -> s.contains("termTimerId(SWEEP_TIMER_BASE)")));
        assertTrue("one per-term egress drain timer: " + scheduleSites,
                scheduleSites.stream().anyMatch(s -> s.contains("termTimerId(EGRESS_DRAIN_TIMER_BASE)")));
        assertEquals("no cancelTimer(", 0, cancels);

        final String applier = stripped(ROOT.resolve(PKG + "infrastructure/persistence/AssetsLogApplier.java"));
        assertEquals("the applier knows three timer kinds", 3,
                count(Pattern.compile("\\bstatic\\s+final\\s+int\\s+TIMER_[A-Z_]+\\s*="), applier));
        final String deadlines = stripped(ROOT.resolve(PKG + "application/engine/time/DeadlineIndex.java"));
        assertEquals("DeadlineIndex has three kinds", 3, count(Pattern.compile("\\bstatic\\s+final\\s+int\\s+KIND_[A-Z_]+\\s*="),
                deadlines));

        final Pattern clock = Pattern.compile("\\bcurrentTimeMillis\\b|\\bnanoTime\\b|\\bjava\\.util\\.Timer\\b|\\bScheduledExecutor");
        final List<String> clockReads = new ArrayList<>();
        for (final Path p : sources(ROOT.resolve(PKG + "application"))) {
            if (count(clock, stripped(p)) > 0) {
                clockReads.add(p.toString());
            }
        }
        assertTrue("no wall-clock read or timer under application/: " + clockReads, clockReads.isEmpty());
    }
}

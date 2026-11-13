// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The generator's one refusal shape (exit 2, one line naming the flag), shared by the command line and the in-JVM
 * session, and the user-count rule against the core's requestId cap.
 */
public class CoreOrderLoadGeneratorSettingsTest {

    private static String refusal(final String... args) {
        return assertThrows(CoreOrderLoadGenerator.Refused.class, () -> CoreOrderLoadGenerator.Settings.parse(args))
                .getMessage();
    }

    @Test
    public void theUserCountRuleFollowsTheRequestCap() {
        assertEquals(7_032, CoreOrderLoadGenerator.minUsers(1_000));
        assertEquals(70_313, CoreOrderLoadGenerator.minUsers(10_000));
        assertEquals(8, CoreOrderLoadGenerator.minUsers(1));
        final CoreOrderLoadGenerator.Settings d = CoreOrderLoadGenerator.Settings.parse(new String[0]);
        assertEquals("the defaults pass their own rule", 1_000, d.rate);
        assertTrue(d.users >= CoreOrderLoadGenerator.minUsers(d.rate));
        // the ramp's creates and, with prefunding, two deposits per user come out of the same cap
        assertEquals(7_422, CoreOrderLoadGenerator.minUsers(1_000, 100_000, false));
        assertEquals(7_087, CoreOrderLoadGenerator.minUsers(1_000, 0, true));
        assertEquals(7_481, CoreOrderLoadGenerator.minUsers(1_000, 100_000, true));
        assertEquals("--users 7031 below the minimum 7422 for --rate 1000: the core keeps 256 requests per user for 15"
                + " min (two per slot, 100000 ramp creates), so TOO_MANY_PENDING_REQUESTS would appear within 849 s",
                refusal("--users", "7031"));
        CoreOrderLoadGenerator.Settings.parse(new String[] {"--users", "7422"});
        assertEquals("--users 7480 below the minimum 7481 for --rate 1000: the core keeps 256 requests per user for 15"
                + " min (two per slot, 100000 ramp creates, 2 prefund deposits per user), so TOO_MANY_PENDING_REQUESTS"
                + " would appear within 899 s", refusal("--users", "7480", "--prefund"));
        CoreOrderLoadGenerator.Settings.parse(new String[] {"--users", "7481", "--prefund"});
    }

    @Test
    public void eachFlagIsCheckedWithOneLine() {
        assertEquals("unknown flag --rat (--help lists them)", refusal("--rat", "5"));
        assertEquals("--rate must be at least 1 slot/s: 0", refusal("--rate", "0"));
        assertEquals("--rate is not an integer: fast", refusal("--rate", "fast"));
        assertEquals("--duration needs a value", refusal("--duration"));
        assertEquals("--open-orders 100001 above --users x --orders-per-user-max (10000 x 10)", refusal(
                "--open-orders", "100001", "--orders-per-user-max", "10"));
        assertEquals("--mix must be create-cancel or create-only: both", refusal("--mix", "both"));
        assertEquals("--idle must be busy_spin or backoff: yield", refusal("--idle", "yield"));
        assertEquals("--markets must be 1..5: 8", refusal("--markets", "8"));
        assertEquals("--warmup must be 0..59 s: 60", refusal("--warmup", "60"));
        assertEquals("--reoffer-after-ms must be at least 100: 10", refusal("--reoffer-after-ms", "10"));
        final String dir = System.getProperty("java.io.tmpdir") + "/no-driver-" + System.nanoTime();
        assertEquals("--aeron-dir " + dir + " has no cnc.dat (no media driver there)", refusal("--aeron-dir", dir));
        assertEquals("--open-orders must be at least 1 with --mix create-cancel (a slot cancels the oldest open order)",
                refusal("--open-orders", "0"));
        final List<String> ok = new ArrayList<>(List.of("--rate", "10", "--users", "100", "--open-orders", "0",
                "--mix", "create-only", "--core-hosts", "a,b,c", "--stride", "100"));
        final CoreOrderLoadGenerator.Settings s = CoreOrderLoadGenerator.Settings.parse(ok.toArray(new String[0]));
        assertTrue(s.createOnly);
        assertEquals(List.of("a", "b", "c"), s.coreHosts);
        assertEquals(100, s.stride);
    }

    @Test
    public void theLeaderLogPositionIsReadFromTheMetricsText() {
        assertEquals(123_456L, CoreOrderLoadGenerator.parseLogPosition("# HELP x\ncore_log_position 123456\n"));
        assertEquals(-1L, CoreOrderLoadGenerator.parseLogPosition("core_cluster_role 2\n"));
    }
}

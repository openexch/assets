// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.metrics;

import com.openexchange.core.application.engine.feed.V3Rig;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * {@code grafana/alerts.yml} and {@code prometheus.yml}: the scrape job, the
 * {@code rule_files} reference, every alert of the contract table, and every expression naming a registered series.
 */
public class AlertsFileTest {

    private static final Path GRAFANA = Path.of("..", "grafana");

    @Test
    public void scrapeJobAndRuleFiles() throws Exception {
        final String prom = Files.readString(GRAFANA.resolve("prometheus.yml"));
        assertTrue("rule_files: [alerts.yml]", Pattern.compile("rule_files:\\s*\\n\\s*-\\s*alerts\\.yml").matcher(prom).find());
        assertTrue("the core-nodes job", prom.contains("job_name: core-nodes"));
        assertTrue("on /metrics", prom.contains("metrics_path: /metrics"));
        assertTrue("9700 + nodeId", prom.contains("localhost:9700") && prom.contains("localhost:9701") && prom.contains("localhost:9702"));
    }

    @Test
    public void everyAlertOfTheTableAndEverySeriesRegistered() throws Exception {
        final String alerts = Files.readString(GRAFANA.resolve("alerts.yml"));
        for (final String name : new String[] {"FeedDuplicateMismatch", "FeedUnverifiedNoLaneBehind", "FeedUnverifiedLong", "FeedLatched",
                "GlobalGateClosed", "MeLinkFault", "MeCapacityWhileOpen", "ReleaseBacklogParked", "DispatchStuck", "ReleasePathStuck",
                "MeCapacityParked", "CoreOrderMoneyRefused", "CoreTickEpochDrops", "CoreTickInvalid"}) {
            assertTrue("alert " + name, alerts.contains("alert: " + name));
        }
        final Map<String, Long> registered = OrderEngineMetricsTest.scrape(V3Rig.open().engine.metricsView());
        final TreeSet<String> named = new TreeSet<>();
        final Matcher m = Pattern.compile("\\bcore_[a-z_]+").matcher(alerts.replaceAll("(?m)^\\s*#.*$", "")
                .replaceAll("summary: \".*\"", ""));
        while (m.find()) {
            named.add(m.group());
        }
        assertTrue("the expressions name series", named.size() > 15);
        for (final String s : named) {
            assertTrue("alerts.yml names " + s + ", which the registrar does not register", registered.containsKey(s));
        }
    }

    @Test
    public void tickAlertsPageAndUnboundIsNotAlerted() throws Exception {
        // Epoch drops and invalid ticks page; unbound drops are expected at genesis and never alerted
        final String alerts = Files.readString(GRAFANA.resolve("alerts.yml")).replaceAll("(?m)^\\s*#.*$", "");
        final Map<String, String> exprs = Map.of("CoreTickEpochDrops", "expr: increase(core_ticks_epoch_drop_total[5m]) > 0",
                "CoreTickInvalid", "expr: increase(core_ticks_invalid_total[5m]) > 0");
        for (final Map.Entry<String, String> e : exprs.entrySet()) {
            final String name = e.getKey();
            final int at = alerts.indexOf("alert: " + name);
            assertTrue("alert " + name + " present", at >= 0);
            final int next = alerts.indexOf("- alert:", at + 5);
            final String block = alerts.substring(at, next < 0 ? alerts.length() : next);
            assertTrue(name + " pages", block.contains("severity: page"));
            assertTrue(name + " reads exactly: " + e.getValue(), block.contains(e.getValue()));
        }
        assertTrue("core_ticks_unbound_total is exported", OrderEngineMetricsTest.scrape(V3Rig.open().engine.metricsView())
                .containsKey("core_ticks_unbound_total"));
        assertTrue("and not named by any alert", !alerts.contains("core_ticks_unbound_total"));
    }

    @Test
    public void dispatchStuckRow() throws Exception {
        // Verbatim
        final String alerts = Files.readString(GRAFANA.resolve("alerts.yml"));
        assertTrue(alerts.contains("expr: changes(core_pending_oldest_seq[2m]) == 0 and core_pending_commands > 0 and "
                + "core_me_capacity_parked == 0"));
        final int at = alerts.indexOf("alert: DispatchStuck");
        assertTrue("severity page", alerts.indexOf("severity: page", at) < alerts.indexOf("- alert:", at + 5));
    }
}

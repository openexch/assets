// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.metrics;

import com.openexchange.assets.application.engine.feed.V3Rig;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * RC2 S8 7 G9/G11 (3.10 item 3, D-S8-9): {@code assets/grafana/alerts.yml} and {@code prometheus.yml}: the scrape job, the
 * {@code rule_files} reference, every alert of the contract table, and every expression naming a registered series.
 */
public class AlertsFileTest {

    private static final Path GRAFANA = Path.of("..", "grafana");

    @Test
    public void scrapeJobAndRuleFiles() throws Exception {
        final String prom = Files.readString(GRAFANA.resolve("prometheus.yml"));
        assertTrue("rule_files: [alerts.yml]", Pattern.compile("rule_files:\\s*\\n\\s*-\\s*alerts\\.yml").matcher(prom).find());
        assertTrue("the assets-nodes job", prom.contains("job_name: assets-nodes"));
        assertTrue("on /metrics", prom.contains("metrics_path: /metrics"));
        assertTrue("9700 + nodeId", prom.contains("localhost:9700") && prom.contains("localhost:9701") && prom.contains("localhost:9702"));
    }

    @Test
    public void everyAlertOfTheTableAndEverySeriesRegistered() throws Exception {
        final String alerts = Files.readString(GRAFANA.resolve("alerts.yml"));
        for (final String name : new String[] {"FeedDuplicateMismatch", "FeedUnverifiedNoLaneBehind", "FeedUnverifiedLong", "FeedLatched",
                "GlobalGateClosed", "MeLinkFault", "MeCapacityWhileOpen", "ReleaseBacklogParked", "DispatchStuck", "ReleasePathStuck",
                "MeCapacityParked", "CoreOrderMoneyRefused"}) {
            assertTrue("alert " + name, alerts.contains("alert: " + name));
        }
        final Map<String, Long> registered = OrderEngineMetricsTest.scrape(V3Rig.open().engine.metricsView());
        final TreeSet<String> named = new TreeSet<>();
        final Matcher m = Pattern.compile("\\bassets_[a-z_]+").matcher(alerts.replaceAll("(?m)^\\s*#.*$", "")
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
    public void dispatchStuckRow() throws Exception {
        // S12 A-3, verbatim
        final String alerts = Files.readString(GRAFANA.resolve("alerts.yml"));
        assertTrue(alerts.contains("expr: changes(assets_pending_oldest_seq[2m]) == 0 and assets_pending_commands > 0 and "
                + "assets_me_capacity_parked == 0"));
        final int at = alerts.indexOf("alert: DispatchStuck");
        assertTrue("severity page", alerts.indexOf("severity: page", at) < alerts.indexOf("- alert:", at + 5));
    }
}

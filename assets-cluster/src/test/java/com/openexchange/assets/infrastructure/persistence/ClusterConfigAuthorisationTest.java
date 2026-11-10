// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.cluster.auth.ClassAuthorisationService;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.cluster.auth.ClusterSessionClass;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The authorisation seam and the standby cadence, unit level (the cluster-level row is
 * {@code ReplicaBackupAuthorityTest}): {@code ClusterConfig.create} installs the
 * {@code AssetsAuth} table on the consensus module as ONE instance the service exports
 * {@code assets_backup_queries_denied_total} from, and {@code metricsBody()} carries the standby cadence counters
 *. RED on 98be859: no authorisation supplier is installed ({@code CFG:185-189}), so Aeron's default allows any
 * authenticated class to BackupQuery.
 */
public class ClusterConfigAuthorisationTest {

    private static ClusterConfig config(final AssetsClusteredService service) {
        return ClusterConfig.create(0, List.of("localhost"), List.of("localhost"), 29_300, service);
    }

    static String metrics(final AssetsClusteredService service) throws Exception {
        final Method body = AssetsClusteredService.class.getDeclaredMethod("metricsBody");
        body.setAccessible(true);
        final Object b = body.invoke(service);
        return (String)b.getClass().getMethod("render").invoke(b);
    }

    static long series(final String body, final String name) {
        final Matcher m = Pattern.compile("(?m)^" + Pattern.quote(name) + " (-?\\d+)$").matcher(body);
        if (!m.find()) {
            fail("metric " + name + " is not exported by metricsBody()");
        }
        return Long.parseLong(m.group(1));
    }

    @Test
    public void consensusModuleGetsTheAssetsTableAsOneSharedInstance() {
        final ClusterConfig cfg = config(new AssetsClusteredService());
        final AuthorisationServiceSupplier supplier = cfg.consensusModuleContext().authorisationServiceSupplier();
        if (supplier == null) {
            fail("ClusterConfig installs no authorisation service: any authenticated class may BackupQuery (CFG:185-189)");
        }
        final AuthorisationService first = supplier.get();
        assertTrue("the installed service is the kit class table, got " + first,
                first instanceof ClassAuthorisationService);
        assertSame("Aeron calls the supplier once per CM; the metrics must read that same instance", first,
                supplier.get());
        final ClassAuthorisationService table = (ClassAuthorisationService)first;
        assertTrue("REPLICA BackupQuery", table.allows(ClusterSessionClass.REPLICA,
                ClassAuthorisationService.CLUSTER_PROTOCOL_ID, ClassAuthorisationService.BACKUP_QUERY));
        assertFalse("GATEWAY BackupQuery", table.allows(ClusterSessionClass.GATEWAY,
                ClassAuthorisationService.CLUSTER_PROTOCOL_ID, ClassAuthorisationService.BACKUP_QUERY));
    }

    @Test
    public void deniedBackupQueriesAreExportedFromTheInstalledInstance() throws Exception {
        final AssetsClusteredService service = new AssetsClusteredService();
        final ClusterConfig cfg = config(service);
        assertEquals("no denial yet", 0, series(metrics(service), "assets_backup_queries_denied_total"));
        final AuthorisationServiceSupplier supplier = cfg.consensusModuleContext().authorisationServiceSupplier();
        assertNotNull("ClusterConfig installs no authorisation service", supplier);
        final AuthorisationService table = supplier.get();
        assertFalse("GATEWAY BackupQuery must be denied", table.isAuthorised(
                ClassAuthorisationService.CLUSTER_PROTOCOL_ID, ClassAuthorisationService.BACKUP_QUERY, null,
                ClusterPrincipal.anyUser(ClusterSessionClass.GATEWAY, 1).encode()));
        assertTrue("REPLICA BackupQuery must be allowed", table.isAuthorised(
                ClassAuthorisationService.CLUSTER_PROTOCOL_ID, ClassAuthorisationService.BACKUP_QUERY, null,
                ClusterPrincipal.anyUser(ClusterSessionClass.REPLICA, 2).encode()));
        assertEquals("the GATEWAY denial is visible on /metrics", 1,
                series(metrics(service), "assets_backup_queries_denied_total"));
    }

    @Test
    public void metricsBodyCarriesTheStandbyCadenceCounters() throws Exception {
        final String body = metrics(new AssetsClusteredService());
        for (final String name : new String[] {
                "assets_standby_requests_total",
                "assets_standby_toggle_deferred_total",
                "assets_standby_toggle_lost_total",
                "assets_standby_toggle_ambiguous_total",
        }) {
            assertEquals(name + " starts at 0", 0, series(body, name));
        }
        assertEquals("assets_standby_interval_minutes is the member's SNAPSHOT_STANDBY_INTERVAL_MINUTES (default 15)",
                15, series(body, "assets_standby_interval_minutes"));
    }
}

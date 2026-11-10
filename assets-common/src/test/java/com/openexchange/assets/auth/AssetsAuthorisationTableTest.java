// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.ClassAuthorisationService;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.cluster.auth.ClusterSessionClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The AE consensus-module authorisation table.
 * REPLICA may send BackupQuery; HeartbeatRequest, StandbySnapshot and AdminRequest are denied for every class; every
 * other class is denied BackupQuery; the table's class codes are the money-schema's ({@link AssetsSessionClasses}).
 * Control: a table that also grants GATEWAY BackupQuery (today's effective behaviour, ABS:39-48) is flagged with
 * {@code GATEWAY allowed BackupQuery}.
 */
public class AssetsAuthorisationTableTest {

    private static final int[] DENIED_FOR_ALL = {
            ClassAuthorisationService.HEARTBEAT_REQUEST,
            ClassAuthorisationService.STANDBY_SNAPSHOT,
            ClassAuthorisationService.ADMIN_REQUEST,
    };

    private static String name(final int actionId) {
        if (actionId == ClassAuthorisationService.BACKUP_QUERY) {
            return "BackupQuery";
        }
        if (actionId == ClassAuthorisationService.HEARTBEAT_REQUEST) {
            return "HeartbeatRequest";
        }
        if (actionId == ClassAuthorisationService.STANDBY_SNAPSHOT) {
            return "StandbySnapshot";
        }
        return actionId == ClassAuthorisationService.ADMIN_REQUEST ? "AdminRequest" : "action " + actionId;
    }

    /** The first rule of 5.1 the table breaks, or null. */
    static String violation(final ClassAuthorisationService table) {
        for (final ClusterSessionClass c : ClusterSessionClass.values()) {
            if (c == ClusterSessionClass.UNSET) {
                continue;
            }
            final boolean backup = table.allows(c, ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                    ClassAuthorisationService.BACKUP_QUERY);
            if (c == ClusterSessionClass.REPLICA && !backup) {
                return "REPLICA denied BackupQuery";
            }
            if (c != ClusterSessionClass.REPLICA && backup) {
                return c + " allowed BackupQuery";
            }
            for (final int action : DENIED_FOR_ALL) {
                if (table.allows(c, ClassAuthorisationService.CLUSTER_PROTOCOL_ID, action)) {
                    return c + " allowed " + name(action);
                }
            }
        }
        return null;
    }

    @Test
    public void replicaMayBackupQueryAndNothingElseIsAllowed() {
        final String v = violation(AssetsAuth.authorisationService());
        assertNull("authorisation table: " + v, v);
    }

    @Test
    public void everyClusterActionIsDecidedOnThePrincipalClass() {
        final ClassAuthorisationService table = AssetsAuth.authorisationService();
        for (final ClusterSessionClass c : ClusterSessionClass.values()) {
            if (c == ClusterSessionClass.UNSET) {
                continue;
            }
            final byte[] principal = ClusterPrincipal.anyUser(c, 42).encode();
            final boolean expected = c == ClusterSessionClass.REPLICA;
            assertEquals(c + " BackupQuery", expected, table.isAuthorised(ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                    ClassAuthorisationService.BACKUP_QUERY, null, principal));
            for (final int action : DENIED_FOR_ALL) {
                assertEquals(c + " " + name(action) + " must be denied", false,
                        table.isAuthorised(ClassAuthorisationService.CLUSTER_PROTOCOL_ID, action, null, principal));
            }
        }
        // a session without a principal (none, or not a v1 principal) is denied
        assertEquals("no principal BackupQuery", false, table.isAuthorised(ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                ClassAuthorisationService.BACKUP_QUERY, null, new byte[0]));
        // six non-REPLICA classes and the empty principal were denied BackupQuery once each
        assertEquals("BackupQuery denials counted for the metrics export", 7,
                table.deniedCount(ClassAuthorisationService.BACKUP_QUERY));
    }

    @Test
    public void tableCodesMatchTheMoneySchemaClasses() {
        for (final ClusterSessionClass c : ClusterSessionClass.values()) {
            final SessionClass schema = AssetsSessionClasses.of(c);
            if (c == ClusterSessionClass.UNSET) {
                assertEquals("UNSET maps to no authority", SessionClass.UNSET, schema);
                continue;
            }
            assertEquals(c + " code parity with the money schema", c.code(), schema.value());
            assertTrue(c + " is a class the AE policy accepts", AssetsAuth.POLICY.allows(c));
        }
        assertEquals("REPLICA is money-schema class 7 (MSX:161)", 7, SessionClass.REPLICA.value());
    }

    @Test
    public void eachCallReturnsAFreshTableWithItsOwnCounters() {
        final ClassAuthorisationService a = AssetsAuth.authorisationService();
        final ClassAuthorisationService b = AssetsAuth.authorisationService();
        assertNotNull(a);
        assertNotSame("ClusterConfig shares ONE instance between the CM supplier and the metrics; the factory must not"
                + " hand out a shared static", a, b);
    }

    @Test
    public void controlTodaysAllowAllTableIsFlagged() {
        final ClassAuthorisationService allowAll = ClassAuthorisationService.builder()
                .allow(ClusterSessionClass.REPLICA, ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                        ClassAuthorisationService.BACKUP_QUERY)
                .allow(ClusterSessionClass.GATEWAY, ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                        ClassAuthorisationService.BACKUP_QUERY)
                .build();
        final String v = violation(allowAll);
        if (v == null) {
            fail("control: a GATEWAY BackupQuery row was not flagged");
        }
        assertEquals("GATEWAY allowed BackupQuery", v);
        System.out.println("NC TABLE_ALLOWS_GATEWAY -> " + v);
        final ClassAuthorisationService heartbeat = ClassAuthorisationService.builder()
                .allow(ClusterSessionClass.REPLICA, ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                        ClassAuthorisationService.BACKUP_QUERY)
                .allow(ClusterSessionClass.REPLICA, ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                        ClassAuthorisationService.HEARTBEAT_REQUEST)
                .build();
        assertEquals("REPLICA allowed HeartbeatRequest", violation(heartbeat));
        System.out.println("NC TABLE_ALLOWS_HEARTBEAT -> " + violation(heartbeat));
    }
}

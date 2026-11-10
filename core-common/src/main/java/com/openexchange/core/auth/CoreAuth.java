// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.auth;

import com.openexchange.cluster.auth.AuthPolicy;
import com.openexchange.cluster.auth.ClassAuthorisationService;
import com.openexchange.cluster.auth.ClusterSessionClass;

/**
 * The core's use of the shared K6 v1 authority: the node-local key table is
 * {@code -Dcore.auth.keys} / {@code CORE_AUTH_KEYS}, clients present {@code -Dcore.auth.credential} /
 * {@code CORE_AUTH_CREDENTIAL} (the names, unchanged). Every class 1..7 may hold a key; a key's users
 * field is {@code users=*} (any user) or an explicit id list (TREASURY, per-firm GATEWAY). Decision A16: a key
 * line with no users field or an empty {@code users=} fails the load naming the key; it never grants any-user.
 */
public final class CoreAuth {

    public static final AuthPolicy POLICY = AuthPolicy.of("core",
            "core.auth.keys", "CORE_AUTH_KEYS",
            "core.auth.credential", "CORE_AUTH_CREDENTIAL",
            true,
            ClusterSessionClass.GATEWAY, ClusterSessionClass.FORWARDER, ClusterSessionClass.BRIDGE,
            ClusterSessionClass.ADMIN, ClusterSessionClass.TOOL, ClusterSessionClass.TREASURY,
            ClusterSessionClass.REPLICA);

    /**
     * The consensus-module authorisation table: REPLICA
     * may send BackupQuery; every other (class, action), HeartbeatRequest, StandbySnapshot and AdminRequest included,
     * is denied. Without it Aeron's default allows BackupQuery, HeartbeatRequest and StandbySnapshot for any
     * authenticated class (ABS:39-48). A new instance per call: {@code ClusterConfig} installs ONE and exports its
     * denial counters.
     */
    public static ClassAuthorisationService authorisationService() {
        return ClassAuthorisationService.builder()
                .allow(ClusterSessionClass.REPLICA, ClassAuthorisationService.CLUSTER_PROTOCOL_ID,
                        ClassAuthorisationService.BACKUP_QUERY)
                .build();
    }

    private CoreAuth() {
    }
}

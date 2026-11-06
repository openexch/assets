// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.cluster.auth.AuthPolicy;
import com.openexchange.cluster.auth.ClusterSessionClass;

/**
 * The assets engine's use of the shared K6 v1 authority (RC2 S6 contract 2.7): the node-local key table is
 * {@code -Dassets.auth.keys} / {@code ASSETS_AUTH_KEYS}, clients present {@code -Dassets.auth.credential} /
 * {@code AE_AUTH_CREDENTIAL} (the RC1 S3 names, unchanged). Every class 1..7 may hold a key; a key's users
 * field is {@code users=*} (any user) or an explicit id list (TREASURY, per-firm GATEWAY). Decision A16: a key
 * line with no users field or an empty {@code users=} fails the load naming the key; it never grants any-user.
 */
public final class AssetsAuth {

    public static final AuthPolicy POLICY = AuthPolicy.of("assets",
            "assets.auth.keys", "ASSETS_AUTH_KEYS",
            "assets.auth.credential", "AE_AUTH_CREDENTIAL",
            true,
            ClusterSessionClass.GATEWAY, ClusterSessionClass.FORWARDER, ClusterSessionClass.BRIDGE,
            ClusterSessionClass.ADMIN, ClusterSessionClass.TOOL, ClusterSessionClass.TREASURY,
            ClusterSessionClass.REPLICA);

    private AssetsAuth() {
    }
}

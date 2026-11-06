// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.cluster.auth.ClusterSessionClass;
import com.openexchange.assets.infrastructure.generated.SessionClass;

/**
 * The one mapping between the kit's class codes and the money-schema {@link SessionClass} (RC2 S6 contract 2.1,
 * 4.3). Codes are equal by construction; {@code SessionClassParityTest} guards it. Allocation-free.
 */
public final class AssetsSessionClasses {

    private AssetsSessionClasses() {
    }

    /** Wire class code to enum; unknown codes map to UNSET (no authority). 7 is REPLICA (money-schema v7). */
    public static SessionClass of(final int code) {
        switch (code) {
            case 1: return SessionClass.GATEWAY;
            case 2: return SessionClass.FORWARDER;
            case 3: return SessionClass.BRIDGE;
            case 4: return SessionClass.ADMIN;
            case 5: return SessionClass.TOOL;
            case 6: return SessionClass.TREASURY;
            case 7: return SessionClass.REPLICA;
            default: return SessionClass.UNSET;
        }
    }

    public static SessionClass of(final ClusterSessionClass c) {
        return of(c.code());
    }

    /** Enum to the kit class; UNSET and NULL_VAL map to {@link ClusterSessionClass#UNSET}. */
    public static ClusterSessionClass toKit(final SessionClass c) {
        return c == null || c == SessionClass.NULL_VAL ? ClusterSessionClass.UNSET : ClusterSessionClass.of(c.value());
    }
}

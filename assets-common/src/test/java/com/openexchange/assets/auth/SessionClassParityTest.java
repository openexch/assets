// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.ClusterSessionClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * RC2 S6 9.3 guard: the kit's class registry and the money-schema {@code SessionClass} agree on every code and
 * name 1..7, and {@link AssetsSessionClasses} is the identity between them (contract 2.1, A4).
 */
public class SessionClassParityTest {

    @Test
    public void kitCodesAndNamesEqualTheWireEnum() {
        int checked = 0;
        for (SessionClass s : SessionClass.values()) {
            if (s == SessionClass.NULL_VAL) {
                continue;
            }
            final ClusterSessionClass k = ClusterSessionClass.of(s.value());
            assertEquals("code " + s.value(), s.name(), k.name());
            assertEquals(s, AssetsSessionClasses.of(k));
            assertEquals(k, AssetsSessionClasses.toKit(s));
            checked++;
        }
        assertEquals("UNSET + 7 classes", 8, checked);
        assertEquals(ClusterSessionClass.MAX_CODE, SessionClass.REPLICA.value());
        for (ClusterSessionClass k : ClusterSessionClass.values()) {
            assertEquals(k.code(), AssetsSessionClasses.of(k).value());
        }
    }
}

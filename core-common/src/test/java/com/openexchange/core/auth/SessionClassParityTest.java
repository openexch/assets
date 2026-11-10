// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.auth;

import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.ClusterSessionClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Guard: the kit's class registry and the money-schema {@code SessionClass} agree on every code and
 * name 1..7, and {@link CoreSessionClasses} is the identity between them (contract 2.1, A4).
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
            assertEquals(s, CoreSessionClasses.of(k));
            assertEquals(k, CoreSessionClasses.toKit(s));
            checked++;
        }
        assertEquals("UNSET + 7 classes", 8, checked);
        assertEquals(ClusterSessionClass.MAX_CODE, SessionClass.REPLICA.value());
        for (ClusterSessionClass k : ClusterSessionClass.values()) {
            assertEquals(k.code(), CoreSessionClasses.of(k).value());
        }
    }
}

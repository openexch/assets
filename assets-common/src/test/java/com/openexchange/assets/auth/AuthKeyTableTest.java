// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.AuthKeyTable;
import com.openexchange.cluster.auth.ClusterCredentials;
import com.openexchange.cluster.auth.ClusterPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC1 S3 (K6 v1), rewritten in RC2 S6 against the kit key table with the AE policy (users field per A16). */
public class AuthKeyTableTest {

    private static final String S1 = "11".repeat(32);
    private static final String S2 = "22".repeat(32);

    private static AuthKeyTable table(final String spec) {
        return AuthKeyTable.fromSpec(spec, AssetsAuth.POLICY);
    }

    @Test
    public void parsesEntriesAndMintsTheRightPrincipal() {
        final AuthKeyTable t = table("1:ADMIN:" + S1 + "::users=*; 2:TREASURY:" + S2 + ":77:users=7|9|8 ;");
        assertEquals(2, t.size());
        final ClusterPrincipal admin = t.authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + S1));
        assertNotNull(admin);
        assertEquals(SessionClass.ADMIN, AssetsSessionClasses.of(admin.classCode()));
        assertTrue(admin.anyUser());
        assertEquals(1, admin.identity());
        final ClusterPrincipal treasury = t.authenticate(ClusterCredentials.fromSpec("2:TREASURY:" + S2));
        assertNotNull(treasury);
        assertEquals(77, treasury.identity());
        assertArrayEquals(new long[] {7, 8, 9}, treasury.userIds());
        assertTrue(treasury.allowsUser(8));
    }

    @Test
    public void rejectsUnknownKeyWrongSecretWrongClassAndNullInput() {
        final AuthKeyTable t = table("1:ADMIN:" + S1 + "::users=*");
        assertNull(t.authenticate(null));
        assertNull(t.authenticate(ClusterCredentials.fromSpec("2:ADMIN:" + S1)));
        assertNull(t.authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + S2)));
        assertNull(t.authenticate(ClusterCredentials.fromSpec("1:TOOL:" + S1)));
        assertNotNull(t.authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + S1)));
    }

    @Test
    public void anEmptyTableAuthenticatesNobodyAndBadSpecsFailLoudly() {
        assertEquals(0, table(null).size());
        assertEquals(0, table("  ").size());
        assertNull(AuthKeyTable.empty(AssetsAuth.POLICY).authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + S1)));
        assertThrows(IllegalArgumentException.class, () -> table("1:ADMIN"));
        assertThrows(IllegalArgumentException.class, () -> table("1:ADMIN:" + S1 + "::users=*;1:TOOL:" + S2 + "::users=*"));
        assertThrows(IllegalArgumentException.class, () -> table("1:ADMIN:" + S1 + ":1:scope=7"));
        assertThrows(IllegalArgumentException.class, () -> table("1:ADMIN:abcd::users=*"));
    }
}

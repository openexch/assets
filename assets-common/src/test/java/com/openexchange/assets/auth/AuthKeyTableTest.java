// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC1 S3 (K6 v1): the node-local key table that mints principals. */
public class AuthKeyTableTest {

    private static final String S1 = "11".repeat(32);
    private static final String S2 = "22".repeat(32);

    @Test
    public void parsesEntriesAndMintsTheRightPrincipal() {
        final AuthKeyTable t = AuthKeyTable.fromSpec("1:ADMIN:" + S1 + "; 2:TREASURY:" + S2 + ":77:users=7|9|8 ;");
        assertEquals(2, t.size());
        final CorePrincipal admin = t.authenticate(CoreCredentials.fromSpec("1:ADMIN:" + S1));
        assertNotNull(admin);
        assertEquals(SessionClass.ADMIN, admin.sessionClass());
        assertTrue(admin.anyUser());
        assertEquals(1, admin.identity());
        final CorePrincipal treasury = t.authenticate(CoreCredentials.fromSpec("2:TREASURY:" + S2));
        assertNotNull(treasury);
        assertEquals(77, treasury.identity());
        assertArrayEquals(new long[] {7, 8, 9}, treasury.userIds());
        assertTrue(treasury.allowsUser(8));
    }

    @Test
    public void rejectsUnknownKeyWrongSecretWrongClassAndNullInput() {
        final AuthKeyTable t = AuthKeyTable.fromSpec("1:ADMIN:" + S1);
        assertNull(t.authenticate(null));
        assertNull(t.authenticate(CoreCredentials.fromSpec("2:ADMIN:" + S1)));
        assertNull(t.authenticate(CoreCredentials.fromSpec("1:ADMIN:" + S2)));
        assertNull(t.authenticate(CoreCredentials.fromSpec("1:TOOL:" + S1)));
        assertNotNull(t.authenticate(CoreCredentials.fromSpec("1:ADMIN:" + S1)));
    }

    @Test
    public void anEmptyTableAuthenticatesNobodyAndBadSpecsFailLoudly() {
        assertEquals(0, AuthKeyTable.fromSpec(null).size());
        assertEquals(0, AuthKeyTable.fromSpec("  ").size());
        assertNull(AuthKeyTable.empty().authenticate(CoreCredentials.fromSpec("1:ADMIN:" + S1)));
        assertThrows(IllegalArgumentException.class, () -> AuthKeyTable.fromSpec("1:ADMIN"));
        assertThrows(IllegalArgumentException.class, () -> AuthKeyTable.fromSpec("1:ADMIN:" + S1 + ";1:TOOL:" + S2));
        assertThrows(IllegalArgumentException.class, () -> AuthKeyTable.fromSpec("1:ADMIN:" + S1 + ":1:scope=7"));
        assertThrows(IllegalArgumentException.class, () -> AuthKeyTable.fromSpec("1:ADMIN:abcd"));
    }
}

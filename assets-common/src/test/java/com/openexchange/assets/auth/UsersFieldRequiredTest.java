// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.cluster.auth.AuthKeyTable;
import com.openexchange.cluster.auth.AuthPolicy;
import com.openexchange.cluster.auth.ClusterCredentials;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.cluster.auth.ClusterSessionClass;
import org.junit.Test;

import java.util.HexFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S6 decision A16 (strict mode): any-user scope needs an explicit {@code users=*}. A key line with no users
 * field, or an empty {@code users=}, fails the load with an error naming the key and its entry and pointing at
 * {@code users=*}; {@code users=*} mints the any-user principal whose bytes equal RC1's implicit form; malformed
 * id lists are named refusals; the ME policy refuses an id list; no secret appears in a message. RED form ran
 * against the RC1 S3 table of the base (evidence); this is the kit table with the AE policy.
 */
public class UsersFieldRequiredTest {

    private static final String S = "11".repeat(32);
    /** The ME's policy as match declares it (MatchAuth.POLICY): no user scope. */
    private static final AuthPolicy ME = AuthPolicy.of("match", "match.auth.keys", "ME_AUTH_KEYS",
            "match.auth.credential", "ME_AUTH_CREDENTIAL", false, ClusterSessionClass.GATEWAY,
            ClusterSessionClass.FORWARDER, ClusterSessionClass.ADMIN, ClusterSessionClass.REPLICA);

    private static IllegalArgumentException refused(final String spec, final AuthPolicy policy) {
        final IllegalArgumentException e = assertThrows(spec, IllegalArgumentException.class,
                () -> AuthKeyTable.fromSpec(spec, policy));
        assertFalse(e.getMessage(), String.valueOf(e.getMessage()).contains(S));
        return e;
    }

    private static void namesKeyAndEntry(final IllegalArgumentException e) {
        final String m = String.valueOf(e.getMessage());
        assertTrue(m, m.contains("entry 1") && m.contains("keyId 1"));
    }

    @Test
    public void aKeyWithNoUsersFieldIsRefusedNamingTheKey() {
        final IllegalArgumentException e = refused("1:ADMIN:" + S, AssetsAuth.POLICY);
        namesKeyAndEntry(e);
        assertTrue(e.getMessage(), e.getMessage().contains("users=*"));
        assertTrue(e.getMessage(), e.getMessage().startsWith("ASSETS_AUTH_KEYS entry 1 (keyId 1)"));
    }

    @Test
    public void anEmptyUsersFieldIsRefusedNamingTheKey() {
        final IllegalArgumentException e = refused("1:ADMIN:" + S + "::users=", AssetsAuth.POLICY);
        namesKeyAndEntry(e);
        assertTrue(e.getMessage(), e.getMessage().contains("users=*"));
    }

    @Test
    public void usersStarMintsTheImplicitAnyUserBytes() {
        final AuthKeyTable t = AuthKeyTable.fromSpec("1:ADMIN:" + S + "::users=*", AssetsAuth.POLICY);
        final ClusterPrincipal p = t.authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + S));
        assertEquals("the RC1 implicit any-user bytes", "01040100010000000000", HexFormat.of().formatHex(p.encode()));
    }

    @Test
    public void malformedIdListsAreNamedRefusals() {
        for (String users : new String[] {"users=7||9", "users=*|7", "users=x"}) {
            namesKeyAndEntry(refused("1:ADMIN:" + S + "::" + users, AssetsAuth.POLICY));
        }
    }

    @Test
    public void theMePolicyRefusesAnIdListAndAcceptsUsersStar() {
        final IllegalArgumentException e = refused("1:FORWARDER:" + S + "::users=7", ME);
        namesKeyAndEntry(e);
        assertTrue(e.getMessage(), e.getMessage().startsWith("ME_AUTH_KEYS entry 1 (keyId 1)"));
        assertTrue(AuthKeyTable.fromSpec("1:FORWARDER:" + S + "::users=*", ME).principalOf(1).anyUser());
        namesKeyAndEntry(refused("1:FORWARDER:" + S, ME));
    }
}

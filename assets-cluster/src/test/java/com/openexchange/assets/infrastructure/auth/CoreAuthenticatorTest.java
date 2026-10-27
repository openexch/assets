// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.auth;

import com.openexchange.assets.auth.AssetsAuth;
import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.assets.auth.CoreCredentials;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.AuthKeyTable;
import com.openexchange.cluster.auth.ClusterAuthenticator;
import com.openexchange.cluster.auth.ClusterPrincipal;
import io.aeron.security.SessionProxy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S3 (K6 v1): the consensus-module authenticator, driven the way SessionManager drives it. RC2 S6: the AE now
 * installs the kit's {@link ClusterAuthenticator} with the AE policy; these are the RC1 cases against it (the
 * credentials are built with the CoreCredentials shim, as the AE's clients build them).
 */
public class CoreAuthenticatorTest {

    private static final String SECRET = "ab".repeat(32);

    /** Records what the authenticator decided for one session. */
    private static final class Proxy implements SessionProxy {
        final long id;
        byte[] principal;
        boolean rejected;
        boolean refuseAuthenticateOnce;

        Proxy(final long id) {
            this.id = id;
        }

        public long sessionId() { return id; }
        public boolean challenge(final byte[] c) { throw new UnsupportedOperationException(); }

        public boolean authenticate(final byte[] encodedPrincipal) {
            if (refuseAuthenticateOnce) {
                refuseAuthenticateOnce = false;
                return false;
            }
            principal = encodedPrincipal;
            return true;
        }

        public void reject() { rejected = true; }
    }

    private static ClusterAuthenticator authenticator() {
        return new ClusterAuthenticator(AuthKeyTable.fromSpec("1:ADMIN:" + SECRET + "::users=*;2:TREASURY:" + SECRET
                + ":9:users=7", AssetsAuth.POLICY));
    }

    @Test
    public void aKnownKeyMintsItsPrincipalIntoTheSession() {
        final ClusterAuthenticator a = authenticator();
        final Proxy p = new Proxy(11);
        a.onConnectRequest(11, CoreCredentials.fromSpec("2:TREASURY:" + SECRET).encode(), 1_000);
        a.onConnectedSession(p, 1_001);
        assertFalse(p.rejected);
        final ClusterPrincipal minted = ClusterPrincipal.decode(p.principal);
        assertEquals(SessionClass.TREASURY, AssetsSessionClasses.of(minted.classCode()));
        assertEquals(9, minted.identity());
        assertTrue(minted.allowsUser(7));
        assertFalse(minted.allowsUser(8));
        assertEquals(1, a.acceptedCount());
    }

    @Test
    public void unknownKeyWrongSecretMissingCredentialsAndChallengesAreRejected() {
        final ClusterAuthenticator a = authenticator();
        final Proxy unknown = new Proxy(1);
        a.onConnectRequest(1, CoreCredentials.fromSpec("5:ADMIN:" + SECRET).encode(), 0);
        a.onConnectedSession(unknown, 0);
        assertTrue(unknown.rejected);
        final Proxy wrong = new Proxy(2);
        a.onConnectRequest(2, CoreCredentials.fromSpec("1:ADMIN:" + "cd".repeat(32)).encode(), 0);
        a.onConnectedSession(wrong, 0);
        assertTrue(wrong.rejected);
        final Proxy none = new Proxy(3);
        a.onConnectRequest(3, new byte[0], 0); // Aeron's NullCredentialsSupplier
        a.onConnectedSession(none, 0);
        assertTrue(none.rejected);
        final Proxy challenged = new Proxy(4);
        a.onChallengedSession(challenged, 0);
        assertTrue(challenged.rejected);
        assertEquals(4, a.rejectedCount());
        assertEquals(0, a.acceptedCount());
        assertNull(unknown.principal);
    }

    @Test
    public void anAuthenticateThatIsNotYetPossibleIsRetriedOnTheNextCycle() {
        final ClusterAuthenticator a = authenticator();
        final Proxy p = new Proxy(5);
        p.refuseAuthenticateOnce = true;
        a.onConnectRequest(5, CoreCredentials.fromSpec("1:ADMIN:" + SECRET).encode(), 0);
        a.onConnectedSession(p, 0);
        assertNull("not yet", p.principal);
        assertFalse(p.rejected);
        a.onConnectedSession(p, 1);
        assertEquals(SessionClass.ADMIN, AssetsSessionClasses.of(ClusterPrincipal.decode(p.principal).classCode()));
        assertEquals(1, a.acceptedCount());
    }

    @Test
    public void anEmptyTableRejectsEveryoneInsteadOfDegrading() {
        final ClusterAuthenticator a = new ClusterAuthenticator(AuthKeyTable.empty(AssetsAuth.POLICY));
        final Proxy p = new Proxy(6);
        a.onConnectRequest(6, CoreCredentials.fromSpec("1:ADMIN:" + SECRET).encode(), 0);
        a.onConnectedSession(p, 0);
        assertTrue(p.rejected);
    }
}

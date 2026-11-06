// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.ClusterCredentials;
import org.junit.After;
import org.junit.Test;

import java.util.HexFormat;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/**
 * RC2 S6 9.3 (contract 4.2): every member of the RC1 CoreCredentials surface the consumers use behaves as RC1 for
 * the parity matrix rows (auth-parity-v1.txt in the kit): constructor checks, fromSpec, fromEnvironment, decode,
 * encode, supplier, sessionClass, keyId, secret and the constants.
 */
public class CoreCredentialsShimTest {

    private static final String SA = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    private static final String SB = "a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf";

    @After
    public void clear() {
        System.clearProperty(CoreCredentials.PROPERTY);
    }

    /** A guard, not a RED: the constants are inlined at compile time, so no stub shim can make it fail. */
    @Test
    public void constantsAreTheRc1Values() {
        assertEquals(1, CoreCredentials.VERSION);
        assertEquals(32, CoreCredentials.SECRET_LENGTH);
        assertEquals(38, CoreCredentials.ENCODED_LENGTH);
        assertEquals("assets.auth.credential", CoreCredentials.PROPERTY);
        assertEquals("AE_AUTH_CREDENTIAL", CoreCredentials.ENV);
        assertEquals(AssetsAuth.POLICY.credentialProperty(), CoreCredentials.PROPERTY);
        assertEquals(AssetsAuth.POLICY.credentialEnv(), CoreCredentials.ENV);
    }

    @Test
    public void parityRowsEncodeToTheRc1Bytes() {
        // rows of auth-parity-v1.txt (cred, ROOT): spec -> the RC1 hex
        final String[][] rows = {
                {"6:FORWARDER:" + SA, "010206000000" + SA},
                {"6:forwarder:" + SA, "010206000000" + SA},
                {"6:2:" + SA, "010206000000" + SA},
                {" 6 : FORWARDER : " + SA + " ", "010206000000" + SA},
                {"-2:ADMIN:" + SB, "0104feffffff" + SB},
                {"2147483647:REPLICA:" + SB, "0107ffffff7f" + SB},
                {"1:ADMIN:" + SA + ":", "010401000000" + SA}};
        for (String[] r : rows) {
            final CoreCredentials c = CoreCredentials.fromSpec(r[0]);
            assertEquals(r[0], r[1], HexFormat.of().formatHex(c.encode()));
            assertArrayEquals(c.encode(), c.supplier().encodedCredentials());
            assertEquals(0, c.supplier().onChallenge(new byte[3]).length);
            assertArrayEquals(ClusterCredentials.fromSpec(r[0]).encode(), c.encode());
        }
        for (String bad : new String[] {"1:TOOL", "1:NOBODY:" + SA, "1:9:" + SA, "1:0:" + SA, "1:UNSET:" + SA,
                "1:NULL_VAL:" + SA, "1:ADMIN:abcd", "x:ADMIN:" + SA, "1:ADMIN:" + SA + ":9", "1:-1:" + SA}) {
            assertThrows(bad, IllegalArgumentException.class, () -> CoreCredentials.fromSpec(bad));
        }
    }

    @Test
    public void constructorAccessorsAndDecodeAsRc1() {
        final byte[] secret = HexFormat.of().parseHex(SA);
        final CoreCredentials c = new CoreCredentials(SessionClass.BRIDGE, 3, secret);
        assertEquals(SessionClass.BRIDGE, c.sessionClass());
        assertEquals(3, c.keyId());
        assertArrayEquals(secret, c.secret());
        c.secret()[0] = 9; // a copy
        assertArrayEquals(secret, c.secret());
        final CoreCredentials d = CoreCredentials.decode(c.encode());
        assertEquals(SessionClass.BRIDGE, d.sessionClass());
        assertNull(CoreCredentials.decode(new byte[37]));
        assertNull(CoreCredentials.decode(null));
        assertThrows(IllegalArgumentException.class, () -> new CoreCredentials(SessionClass.UNSET, 1, secret));
        assertThrows(IllegalArgumentException.class, () -> new CoreCredentials(SessionClass.NULL_VAL, 1, secret));
        assertThrows(IllegalArgumentException.class, () -> new CoreCredentials(null, 1, secret));
        assertThrows(IllegalArgumentException.class, () -> new CoreCredentials(SessionClass.TOOL, 1, new byte[16]));
        assertEquals(SessionClass.REPLICA, new CoreCredentials(SessionClass.REPLICA, 7, secret).sessionClass());
    }

    @Test
    public void fromEnvironmentReadsThePropertyThenTheEnv() {
        // review FIX-2: a developer shell that carries AE_AUTH_CREDENTIAL would otherwise turn this red
        org.junit.Assume.assumeTrue(System.getenv(CoreCredentials.ENV) == null);
        assertNull(CoreCredentials.fromEnvironment());
        System.setProperty(CoreCredentials.PROPERTY, "3:BRIDGE:" + SA);
        assertEquals(SessionClass.BRIDGE, CoreCredentials.fromEnvironment().sessionClass());
        System.setProperty(CoreCredentials.PROPERTY, "  ");
        assertNull(CoreCredentials.fromEnvironment());
    }
}

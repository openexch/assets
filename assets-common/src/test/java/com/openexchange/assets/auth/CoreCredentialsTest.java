// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/** RC1 S3 (K6 v1): what a client presents on connect. */
public class CoreCredentialsTest {

    private static final byte[] SECRET = new byte[32];

    static {
        for (int i = 0; i < 32; i++) {
            SECRET[i] = (byte) (0xA0 + i);
        }
    }

    @Test
    public void roundTripAndSpecParsing() {
        final CoreCredentials c = new CoreCredentials(SessionClass.BRIDGE, 3, SECRET);
        assertEquals(CoreCredentials.ENCODED_LENGTH, c.encode().length);
        final CoreCredentials d = CoreCredentials.decode(c.encode());
        assertEquals(SessionClass.BRIDGE, d.sessionClass());
        assertEquals(3, d.keyId());
        assertArrayEquals(SECRET, d.secret());
        final String hex = java.util.HexFormat.of().formatHex(SECRET);
        assertArrayEquals(c.encode(), CoreCredentials.fromSpec("3:BRIDGE:" + hex).encode());
        assertArrayEquals(c.encode(), CoreCredentials.fromSpec("3:bridge:" + hex).encode());
        assertArrayEquals(c.encode(), CoreCredentials.fromSpec(" 3 : 3 : " + hex).encode());
        assertArrayEquals(c.encode(), c.supplier().encodedCredentials());
        assertEquals(0, c.supplier().onChallenge(new byte[4]).length);
    }

    @Test
    public void foreignBytesAndBadSpecsAreRefused() {
        assertNull(CoreCredentials.decode(null));
        assertNull(CoreCredentials.decode(new byte[0]));
        assertNull(CoreCredentials.decode(new byte[CoreCredentials.ENCODED_LENGTH - 1]));
        final byte[] badVersion = new CoreCredentials(SessionClass.TOOL, 1, SECRET).encode();
        badVersion[0] = 2;
        assertNull(CoreCredentials.decode(badVersion));
        final byte[] badClass = new CoreCredentials(SessionClass.TOOL, 1, SECRET).encode();
        badClass[1] = 0;
        assertNull(CoreCredentials.decode(badClass));
        assertThrows(IllegalArgumentException.class, () -> new CoreCredentials(SessionClass.UNSET, 1, SECRET));
        assertThrows(IllegalArgumentException.class, () -> new CoreCredentials(SessionClass.TOOL, 1, Arrays.copyOf(SECRET, 16)));
        assertThrows(IllegalArgumentException.class, () -> CoreCredentials.fromSpec("1:TOOL"));
        assertThrows(IllegalArgumentException.class, () -> CoreCredentials.fromSpec("1:NOBODY:" + "00".repeat(32)));
        assertThrows(IllegalArgumentException.class, () -> CoreCredentials.fromSpec("1:9:" + "00".repeat(32)));
    }
}

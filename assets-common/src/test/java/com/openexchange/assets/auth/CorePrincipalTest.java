// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC1 S3 (K6): the principal every replica reads from the SessionOpen event. */
public class CorePrincipalTest {

    @Test
    public void anyUserRoundTripsAndAllowsEveryone() {
        final CorePrincipal p = CorePrincipal.anyUser(SessionClass.GATEWAY, 42);
        final CorePrincipal d = CorePrincipal.decode(p.encode());
        assertEquals(SessionClass.GATEWAY, d.sessionClass());
        assertEquals(42, d.identity());
        assertTrue(d.anyUser());
        assertTrue(d.allowsUser(1) && d.allowsUser(Long.MAX_VALUE));
        assertEquals(10, p.encode().length);
    }

    @Test
    public void scopedPrincipalSortsDedupesAndBinarySearches() {
        final CorePrincipal p = CorePrincipal.forUsers(SessionClass.TREASURY, 7, 30, 10, 20);
        final CorePrincipal d = CorePrincipal.decode(p.encode());
        assertArrayEquals(new long[] {10, 20, 30}, d.userIds());
        assertTrue(d.allowsUser(20));
        assertFalse(d.allowsUser(25));
        assertFalse(d.anyUser());
        assertThrows(IllegalArgumentException.class, () -> CorePrincipal.forUsers(SessionClass.TREASURY, 7, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> CorePrincipal.forUsers(SessionClass.TREASURY, 7));
        final long[] tooMany = new long[CorePrincipal.MAX_USERS + 1];
        for (int i = 0; i < tooMany.length; i++) {
            tooMany[i] = i;
        }
        assertThrows(IllegalArgumentException.class, () -> CorePrincipal.forUsers(SessionClass.TREASURY, 7, tooMany));
    }

    @Test
    public void foreignBytesDecodeToNoneWhichAuthorisesNothing() {
        assertSame(CorePrincipal.NONE, CorePrincipal.decode(null));
        assertSame(CorePrincipal.NONE, CorePrincipal.decode(new byte[0]));
        assertSame(CorePrincipal.NONE, CorePrincipal.decode(new byte[] {9, 1, 1, 0, 0, 0, 0, 0, 0, 0})); // version 9
        assertSame(CorePrincipal.NONE, CorePrincipal.decode(new byte[] {1, 0, 1, 0, 0, 0, 0, 0, 0, 0})); // class UNSET
        assertSame(CorePrincipal.NONE, CorePrincipal.decode(new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0})); // no scope, not ANY
        final byte[] truncated = Arrays.copyOf(CorePrincipal.forUsers(SessionClass.TOOL, 1, 5, 6).encode(), 20);
        assertSame(CorePrincipal.NONE, CorePrincipal.decode(truncated));
        final byte[] unsorted = CorePrincipal.forUsers(SessionClass.TOOL, 1, 5, 6).encode();
        unsorted[10] = 9; // first user 9 > second user 6
        assertSame(CorePrincipal.NONE, CorePrincipal.decode(unsorted));
        assertFalse(CorePrincipal.NONE.allowsUser(1));
        assertEquals(SessionClass.UNSET, CorePrincipal.NONE.sessionClass());
    }

    @Test
    public void classCodesFollowTheWireEnum() {
        assertEquals(SessionClass.GATEWAY, CorePrincipal.classOf(1));
        assertEquals(SessionClass.TREASURY, CorePrincipal.classOf(6));
        assertEquals(SessionClass.UNSET, CorePrincipal.classOf(7));
        assertEquals(SessionClass.UNSET, CorePrincipal.classOf(0));
    }
}

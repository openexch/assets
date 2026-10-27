// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.cluster.auth.ClusterSessionClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S3 (K6): the principal every replica reads from SessionOpen. RC2 S6: the class is the kit principal, the
 * engine enum comes from {@link AssetsSessionClasses}; these cases pin the AE's use of it.
 */
public class CorePrincipalTest {

    private static ClusterPrincipal any(final SessionClass c, final int identity) {
        return ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(c), identity);
    }

    private static ClusterPrincipal scoped(final SessionClass c, final int identity, final long... users) {
        return ClusterPrincipal.forUsers(AssetsSessionClasses.toKit(c), identity, users);
    }

    @Test
    public void anyUserRoundTripsAndAllowsEveryone() {
        final ClusterPrincipal p = any(SessionClass.GATEWAY, 42);
        final ClusterPrincipal d = ClusterPrincipal.decode(p.encode());
        assertEquals(SessionClass.GATEWAY, AssetsSessionClasses.of(d.classCode()));
        assertEquals(42, d.identity());
        assertTrue(d.anyUser());
        assertTrue(d.allowsUser(1) && d.allowsUser(Long.MAX_VALUE));
        assertEquals(10, p.encode().length);
    }

    @Test
    public void scopedPrincipalSortsDedupesAndBinarySearches() {
        final ClusterPrincipal d = ClusterPrincipal.decode(scoped(SessionClass.TREASURY, 7, 30, 10, 20).encode());
        assertArrayEquals(new long[] {10, 20, 30}, d.userIds());
        assertTrue(d.allowsUser(20));
        assertFalse(d.allowsUser(25));
        assertFalse(d.anyUser());
        assertThrows(IllegalArgumentException.class, () -> scoped(SessionClass.TREASURY, 7, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> scoped(SessionClass.TREASURY, 7));
        assertThrows(IllegalArgumentException.class,
                () -> scoped(SessionClass.TREASURY, 7, new long[ClusterPrincipal.MAX_USERS + 1]));
    }

    @Test
    public void foreignBytesDecodeToNoneWhichAuthorisesNothing() {
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(null));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(new byte[0]));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(new byte[] {9, 1, 1, 0, 0, 0, 0, 0, 0, 0}));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(new byte[] {1, 0, 1, 0, 0, 0, 0, 0, 0, 0}));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0}));
        final byte[] truncated = Arrays.copyOf(scoped(SessionClass.TOOL, 1, 5, 6).encode(), 20);
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(truncated));
        final byte[] unsorted = scoped(SessionClass.TOOL, 1, 5, 6).encode();
        unsorted[10] = 9;
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(unsorted));
        assertFalse(ClusterPrincipal.NONE.allowsUser(1));
        assertEquals(SessionClass.UNSET, AssetsSessionClasses.of(ClusterPrincipal.NONE.classCode()));
    }

    @Test
    public void classCodesFollowTheWireEnum() {
        assertEquals(SessionClass.GATEWAY, AssetsSessionClasses.of(1));
        assertEquals(SessionClass.TREASURY, AssetsSessionClasses.of(6));
        assertEquals(SessionClass.REPLICA, AssetsSessionClasses.of(7));
        assertEquals(SessionClass.UNSET, AssetsSessionClasses.of(8));
        assertEquals(SessionClass.UNSET, AssetsSessionClasses.of(0));
        assertEquals(ClusterSessionClass.UNSET, AssetsSessionClasses.toKit(SessionClass.NULL_VAL));
        final ClusterPrincipal replica = any(SessionClass.REPLICA, 3);
        assertEquals(SessionClass.REPLICA, AssetsSessionClasses.of(ClusterPrincipal.decode(replica.encode()).classCode()));
    }
}

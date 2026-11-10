// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * {@code java -jar core-cluster.jar [member|replica]}, {@code CORE_ROLE} an alias for
 * images that cannot pass arguments; both present and different is refused, an unknown role is refused, no argument
 * keeps today's member start.
 */
public class MainRoleTest {

    private static String refused(final String alias, final String... args) {
        return assertThrows(IllegalArgumentException.class, () -> Main.role(args, alias)).getMessage();
    }

    @Test
    public void noArgumentAndNoAliasStartsAMember() {
        assertEquals(Main.Role.MEMBER, Main.role(new String[0], null));
        assertEquals(Main.Role.MEMBER, Main.role(new String[0], ""));
    }

    @Test
    public void theSubcommandPicksTheRole() {
        assertEquals(Main.Role.MEMBER, Main.role(new String[] {"member"}, null));
        assertEquals(Main.Role.REPLICA, Main.role(new String[] {"replica"}, null));
        assertEquals(Main.Role.REPLICA, Main.role(new String[] {" Replica "}, null));
    }

    @Test
    public void theAliasPicksTheRoleWithoutAnArgument() {
        assertEquals(Main.Role.REPLICA, Main.role(new String[0], "replica"));
        assertEquals(Main.Role.MEMBER, Main.role(new String[0], "member"));
        assertEquals("agreeing alias and argument", Main.Role.REPLICA, Main.role(new String[] {"replica"}, "REPLICA"));
    }

    @Test
    public void aliasAndArgumentThatDisagreeAreRefused() {
        assertEquals("CORE_ROLE=replica conflicts with the argument 'member'", refused("replica", "member"));
        assertEquals("CORE_ROLE=member conflicts with the argument 'replica'", refused("member", "replica"));
    }

    @Test
    public void unknownRolesAndExtraArgumentsAreRefused() {
        assertEquals("unknown role argument 'aer' (expected member|replica)", refused(null, "aer"));
        assertEquals("unknown role CORE_ROLE='shadow0' (expected member|replica)", refused("shadow0"));
        assertEquals("expected at most one argument (member|replica), got 2: [replica, extra]",
                refused(null, "replica", "extra"));
    }
}

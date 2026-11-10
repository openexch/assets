// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core;

import com.openexchange.core.infrastructure.TransportConfig;
import com.openexchange.core.infrastructure.persistence.AeronCluster;
import com.openexchange.core.replica.CoreShadowReplicaMain;

import java.util.Arrays;
import java.util.Locale;

/**
 * Entry point of the core jar. {@code java -jar core-cluster.jar} or {@code java -jar core-cluster.jar member} starts a
 * cluster node: a media driver (embedded by default) + archive + consensus module + clustered service on core-isolated
 * ports ({@code 9300+}) and dirs, idle mode BACKOFF by default, all config env-var-first (see {@code AeronCluster}).
 * {@code java -jar core-cluster.jar replica} starts the shadow replica, a fourth process that is not a member.
 *
 * <p>{@code CORE_ROLE} ({@code -Dcore.role}) is an alias for images that cannot pass arguments; an argument and an
 * alias that disagree, an unknown role or a second argument are refused with exit 1 before anything starts.</p>
 */
public final class Main {

    /** What this process runs. */
    enum Role { MEMBER, REPLICA }

    static final String ROLE_ENV = "CORE_ROLE";
    static final String ROLE_PROPERTY = "core.role";

    private Main() {
    }

    public static void main(String[] args) {
        // First: a deployment written for the retired names refuses before any setting is read.
        com.openexchange.core.config.RetiredSettings.refuseRetired("core-cluster.jar");
        final Role role;
        try {
            role = role(args, TransportConfig.envOrProp(ROLE_ENV, ROLE_PROPERTY, null));
        } catch (final IllegalArgumentException refused) {
            System.err.println("FATAL: core-cluster.jar refused to start: " + refused.getMessage());
            System.exit(1);
            return;
        }
        if (role == Role.REPLICA) {
            CoreShadowReplicaMain.run();
        } else {
            new AeronCluster();
        }
    }

    /**
     * The role from the subcommand and the {@code CORE_ROLE} alias.
     *
     * @throws IllegalArgumentException naming the conflict, the unknown value or the extra argument
     */
    static Role role(final String[] args, final String alias) {
        if (args.length > 1) {
            throw new IllegalArgumentException("expected at most one argument (member|replica), got " + args.length
                    + ": " + Arrays.toString(args));
        }
        final Role fromArg = args.length == 0 ? null : parse(args[0], "argument '" + args[0] + "'");
        final boolean aliasSet = alias != null && !alias.isBlank();
        final Role fromAlias = aliasSet ? parse(alias, ROLE_ENV + "='" + alias + "'") : null;
        if (fromArg != null && fromAlias != null && fromArg != fromAlias) {
            throw new IllegalArgumentException(ROLE_ENV + "=" + alias.trim() + " conflicts with the argument '"
                    + args[0].trim() + "'");
        }
        if (fromArg != null) {
            return fromArg;
        }
        return fromAlias != null ? fromAlias : Role.MEMBER;
    }

    private static Role parse(final String value, final String what) {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "member":
                return Role.MEMBER;
            case "replica":
                return Role.REPLICA;
            default:
                throw new IllegalArgumentException("unknown role " + what + " (expected member|replica)");
        }
    }
}

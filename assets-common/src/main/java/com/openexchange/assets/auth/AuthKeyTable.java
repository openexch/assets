// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Node-local key table for the v1 authenticator: {@code keyId -> (class, secret, principal)}. Node-local
 * configuration is legitimate here because the table is NOT replicated state: only the principal it
 * mints enters the log (the SessionOpen event). Every member must carry the same table; a mismatch
 * shows as a reconnect rejection after a leader change, never as divergence.
 *
 * <p>Spec: entries separated by {@code ;}, each {@code keyId:class:secretHex[:identity[:users=1|2|3]]}.
 * No {@code users} part means the key may act for ANY user. Read from {@code -Dassets.auth.keys}, else
 * {@code ASSETS_AUTH_KEYS}. An absent or empty table authenticates NOBODY: the cluster refuses every
 * session until it is configured, loudly, rather than falling back to an unauthenticated mode.</p>
 */
public final class AuthKeyTable {

    public static final String PROPERTY = "assets.auth.keys";
    public static final String ENV = "ASSETS_AUTH_KEYS";

    /** One row: the class the key may claim, its secret and the principal it mints. */
    public record Key(int keyId, SessionClass sessionClass, byte[] secret, CorePrincipal principal) { }

    private final Map<Integer, Key> keys = new TreeMap<>();

    public static AuthKeyTable empty() {
        return new AuthKeyTable();
    }

    public static AuthKeyTable fromSpec(final String spec) {
        final AuthKeyTable t = new AuthKeyTable();
        if (spec == null || spec.isBlank()) {
            return t;
        }
        for (final String raw : spec.split(";")) {
            final String entry = raw.trim();
            if (entry.isEmpty()) {
                continue;
            }
            final String[] p = entry.split(":");
            if (p.length < 3 || p.length > 5) {
                throw new IllegalArgumentException("key entry must be keyId:class:secretHex[:identity[:users=a|b]]: " + entry);
            }
            final int keyId = Integer.parseInt(p[0].trim());
            final SessionClass cls = CoreCredentials.parseClass(p[1]);
            final byte[] secret = HexFormat.of().parseHex(p[2].trim());
            final int identity = p.length >= 4 && !p[3].isBlank() ? Integer.parseInt(p[3].trim()) : keyId;
            long[] users = new long[0];
            if (p.length == 5) {
                final String u = p[4].trim();
                if (!u.startsWith("users=")) {
                    throw new IllegalArgumentException("fifth field must be users=a|b|c: " + entry);
                }
                final List<Long> list = new ArrayList<>();
                for (final String s : u.substring("users=".length()).split("\\|")) {
                    if (!s.isBlank()) {
                        list.add(Long.parseLong(s.trim()));
                    }
                }
                users = list.stream().mapToLong(Long::longValue).toArray();
            }
            t.add(keyId, cls, secret, identity, users);
        }
        return t;
    }

    /** From {@code -Dassets.auth.keys}, else {@code ASSETS_AUTH_KEYS}; empty when neither is set. */
    public static AuthKeyTable fromEnvironment() {
        String spec = System.getProperty(PROPERTY);
        if (spec == null || spec.isBlank()) {
            spec = System.getenv(ENV);
        }
        return fromSpec(spec);
    }

    public AuthKeyTable add(final int keyId, final SessionClass sessionClass, final byte[] secret,
                            final int identity, final long... users) {
        if (secret == null || secret.length != CoreCredentials.SECRET_LENGTH) {
            throw new IllegalArgumentException("key " + keyId + ": secret must be " + CoreCredentials.SECRET_LENGTH + " bytes");
        }
        final CorePrincipal principal = users.length == 0
                ? CorePrincipal.anyUser(sessionClass, identity)
                : CorePrincipal.forUsers(sessionClass, identity, users);
        if (keys.put(keyId, new Key(keyId, sessionClass, secret.clone(), principal)) != null) {
            throw new IllegalArgumentException("duplicate keyId " + keyId);
        }
        return this;
    }

    public int size() {
        return keys.size();
    }

    /**
     * Authenticate presented credentials: known key, claimed class equals the key's class, secret equal
     * in constant time. Returns the principal to mint, or null to reject.
     */
    public CorePrincipal authenticate(final CoreCredentials presented) {
        if (presented == null) {
            return null;
        }
        final Key key = keys.get(presented.keyId());
        if (key == null) {
            return null;
        }
        if (key.sessionClass != presented.sessionClass()) {
            return null;
        }
        if (!MessageDigest.isEqual(key.secret, presented.secret())) {
            return null;
        }
        return key.principal;
    }
}

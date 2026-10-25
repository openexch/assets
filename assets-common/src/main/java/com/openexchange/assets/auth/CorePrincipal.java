// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * What the authenticator mints for an accepted session and what every replica reads from the
 * SessionOpen log event (K6, hardening B3.2): the session CLASS, its user scope and an identity for
 * logs. Encoded as
 * {@code [u8 version=1][u8 class][u8 flags][u8 reserved][i32 identity][u16 userCount][i64 userIds...]}
 * (little endian), at most 4096 bytes (Aeron's principal limit), so at most 510 explicit user ids.
 *
 * <p>Decoding never throws on foreign bytes: a malformed principal decodes to {@link #NONE} (class
 * UNSET, no scope), which authorises nothing. The authenticator is the place that rejects bad input;
 * the service only defends.</p>
 */
public final class CorePrincipal {

    public static final int VERSION = 1;
    public static final int FLAG_USER_SCOPE_ANY = 1;
    public static final int MAX_USERS = 510;
    private static final int FIXED = 1 + 1 + 1 + 1 + 4 + 2;

    /** The principal of a session whose bytes were missing or malformed: authorises nothing. */
    public static final CorePrincipal NONE = new CorePrincipal(SessionClass.UNSET, 0, 0, new long[0]);

    private final SessionClass sessionClass;
    private final int flags;
    private final int identity;
    private final long[] userIds; // ascending, unique

    private CorePrincipal(final SessionClass sessionClass, final int flags, final int identity, final long[] userIds) {
        this.sessionClass = sessionClass;
        this.flags = flags;
        this.identity = identity;
        this.userIds = userIds;
    }

    /** A principal that may act for ANY user (the retail gateway, admin, tools, feeds). */
    public static CorePrincipal anyUser(final SessionClass sessionClass, final int identity) {
        requireClass(sessionClass);
        return new CorePrincipal(sessionClass, FLAG_USER_SCOPE_ANY, identity, new long[0]);
    }

    /** A principal bound to an explicit user set (an HFT or per-firm session). */
    public static CorePrincipal forUsers(final SessionClass sessionClass, final int identity, final long... users) {
        requireClass(sessionClass);
        if (users.length == 0 || users.length > MAX_USERS) {
            throw new IllegalArgumentException("user scope needs 1.." + MAX_USERS + " ids, got " + users.length);
        }
        final long[] sorted = users.clone();
        Arrays.sort(sorted);
        for (int i = 1; i < sorted.length; i++) {
            if (sorted[i] == sorted[i - 1]) {
                throw new IllegalArgumentException("duplicate userId " + sorted[i]);
            }
        }
        return new CorePrincipal(sessionClass, 0, identity, sorted);
    }

    private static void requireClass(final SessionClass sessionClass) {
        if (sessionClass == null || sessionClass == SessionClass.UNSET || sessionClass == SessionClass.NULL_VAL) {
            throw new IllegalArgumentException("a principal needs a real session class, got " + sessionClass);
        }
    }

    public byte[] encode() {
        final ByteBuffer b = ByteBuffer.allocate(FIXED + 8 * userIds.length).order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) VERSION).put((byte) sessionClass.value()).put((byte) flags).put((byte) 0)
                .putInt(identity).putShort((short) userIds.length);
        for (final long u : userIds) {
            b.putLong(u);
        }
        return b.array();
    }

    /** Never throws: anything this build cannot read is {@link #NONE}. */
    public static CorePrincipal decode(final byte[] bytes) {
        if (bytes == null || bytes.length < FIXED) {
            return NONE;
        }
        final ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        if ((b.get() & 0xFF) != VERSION) {
            return NONE;
        }
        final int classCode = b.get() & 0xFF;
        final SessionClass sessionClass = classOf(classCode);
        if (sessionClass == SessionClass.UNSET) {
            return NONE;
        }
        final int flags = b.get() & 0xFF;
        b.get(); // reserved
        final int identity = b.getInt();
        final int count = b.getShort() & 0xFFFF;
        if (count > MAX_USERS || bytes.length != FIXED + 8L * count) {
            return NONE;
        }
        if ((flags & FLAG_USER_SCOPE_ANY) != 0 && count != 0) {
            return NONE;
        }
        if ((flags & FLAG_USER_SCOPE_ANY) == 0 && count == 0) {
            return NONE;
        }
        final long[] users = new long[count];
        for (int i = 0; i < count; i++) {
            users[i] = b.getLong();
            if (i > 0 && users[i] <= users[i - 1]) {
                return NONE; // not ascending-unique: foreign bytes
            }
        }
        return new CorePrincipal(sessionClass, flags, identity, users);
    }

    /** Wire class code to enum; unknown codes map to UNSET (no authority). */
    public static SessionClass classOf(final int code) {
        switch (code) {
            case 1: return SessionClass.GATEWAY;
            case 2: return SessionClass.FORWARDER;
            case 3: return SessionClass.BRIDGE;
            case 4: return SessionClass.ADMIN;
            case 5: return SessionClass.TOOL;
            case 6: return SessionClass.TREASURY;
            default: return SessionClass.UNSET;
        }
    }

    public SessionClass sessionClass() {
        return sessionClass;
    }

    public int flags() {
        return flags;
    }

    public int identity() {
        return identity;
    }

    public boolean anyUser() {
        return (flags & FLAG_USER_SCOPE_ANY) != 0;
    }

    /** Explicit scope (empty when ANY). Copy: callers must not mutate the principal. */
    public long[] userIds() {
        return userIds.clone();
    }

    public boolean allowsUser(final long userId) {
        if (sessionClass == SessionClass.UNSET) {
            return false;
        }
        return anyUser() || Arrays.binarySearch(userIds, userId) >= 0;
    }

    @Override
    public String toString() {
        return "CorePrincipal{" + sessionClass + " identity=" + identity
                + (anyUser() ? " users=ANY" : " users=" + Arrays.toString(userIds)) + "}";
    }
}

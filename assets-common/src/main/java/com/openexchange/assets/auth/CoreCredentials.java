// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import io.aeron.security.CredentialsSupplier;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;

/**
 * What a client presents on connect (K6 v1, static shared secret):
 * {@code [u8 version=1][u8 class][i32 keyId][32 bytes secret]} = 38 bytes, little endian. The class is
 * the client's ASSERTION; the key table decides what the key is actually allowed to be, and the
 * authenticator rejects a mismatch. v2 adds an HMAC challenge through {@code onChallenge}; v1 rejects a
 * challenge outright.
 *
 * <p>Clients configure one credential as the string {@code keyId:class:secretHex} (class by name or
 * code), e.g. {@code 3:BRIDGE:0f1e...}, through {@link #fromSpec}; the AE's own tools read it from
 * {@code -Dassets.auth.credential} / {@code AE_AUTH_CREDENTIAL}.</p>
 */
public final class CoreCredentials {

    public static final int VERSION = 1;
    public static final int SECRET_LENGTH = 32;
    public static final int ENCODED_LENGTH = 1 + 1 + 4 + SECRET_LENGTH;
    public static final String PROPERTY = "assets.auth.credential";
    public static final String ENV = "AE_AUTH_CREDENTIAL";

    private final SessionClass sessionClass;
    private final int keyId;
    private final byte[] secret;

    public CoreCredentials(final SessionClass sessionClass, final int keyId, final byte[] secret) {
        if (sessionClass == null || sessionClass == SessionClass.UNSET || sessionClass == SessionClass.NULL_VAL) {
            throw new IllegalArgumentException("credentials need a real session class, got " + sessionClass);
        }
        if (secret == null || secret.length != SECRET_LENGTH) {
            throw new IllegalArgumentException("secret must be exactly " + SECRET_LENGTH + " bytes");
        }
        this.sessionClass = sessionClass;
        this.keyId = keyId;
        this.secret = secret.clone();
    }

    /** Parse {@code keyId:class:secretHex}; class by enum name (case-insensitive) or numeric code. */
    public static CoreCredentials fromSpec(final String spec) {
        final String[] parts = spec.trim().split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("credential spec must be keyId:class:secretHex");
        }
        return new CoreCredentials(parseClass(parts[1]), Integer.parseInt(parts[0].trim()),
                HexFormat.of().parseHex(parts[2].trim()));
    }

    /** From {@code -Dassets.auth.credential}, else {@code AE_AUTH_CREDENTIAL}; null when neither is set. */
    public static CoreCredentials fromEnvironment() {
        String spec = System.getProperty(PROPERTY);
        if (spec == null || spec.isBlank()) {
            spec = System.getenv(ENV);
        }
        return spec == null || spec.isBlank() ? null : fromSpec(spec);
    }

    static SessionClass parseClass(final String text) {
        final String t = text.trim();
        if (!t.isEmpty() && Character.isDigit(t.charAt(0))) {
            final SessionClass c = CorePrincipal.classOf(Integer.parseInt(t));
            if (c == SessionClass.UNSET) {
                throw new IllegalArgumentException("unknown session class code " + t);
            }
            return c;
        }
        final SessionClass c = SessionClass.valueOf(t.toUpperCase());
        if (c == SessionClass.UNSET || c == SessionClass.NULL_VAL) {
            throw new IllegalArgumentException("unknown session class " + text);
        }
        return c;
    }

    public byte[] encode() {
        final ByteBuffer b = ByteBuffer.allocate(ENCODED_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) VERSION).put((byte) sessionClass.value()).putInt(keyId).put(secret);
        return b.array();
    }

    /** Null when the bytes are not a v1 credential frame. */
    public static CoreCredentials decode(final byte[] bytes) {
        if (bytes == null || bytes.length != ENCODED_LENGTH) {
            return null;
        }
        final ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        if ((b.get() & 0xFF) != VERSION) {
            return null;
        }
        final SessionClass c = CorePrincipal.classOf(b.get() & 0xFF);
        if (c == SessionClass.UNSET) {
            return null;
        }
        final int keyId = b.getInt();
        final byte[] secret = new byte[SECRET_LENGTH];
        b.get(secret);
        return new CoreCredentials(c, keyId, secret);
    }

    /** The Aeron client-side supplier: presents these bytes, refuses challenges (v1). */
    public CredentialsSupplier supplier() {
        final byte[] encoded = encode();
        return new CredentialsSupplier() {
            public byte[] encodedCredentials() {
                return encoded.clone();
            }

            public byte[] onChallenge(final byte[] encodedChallenge) {
                return new byte[0];
            }
        };
    }

    public SessionClass sessionClass() {
        return sessionClass;
    }

    public int keyId() {
        return keyId;
    }

    /** Copy. */
    public byte[] secret() {
        return secret.clone();
    }
}

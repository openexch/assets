// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.auth;

import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.cluster.auth.ClusterCredentials;
import io.aeron.security.CredentialsSupplier;

/**
 * RC2 S6 contract 4.2: a final, logic-free shim over the kit's {@link ClusterCredentials} with the AE policy,
 * keeping the RC1 S3 public surface so the bridge tools, the loadgen, BridgeEndToEndTest and the fixture's
 * CoreProbe compile unchanged. The bytes are the kit's (the same 38 bytes RC1 wrote). Removed when S15 moves the
 * probe to the kit class (S17 at the latest).
 */
public final class CoreCredentials {

    public static final int VERSION = ClusterCredentials.VERSION;
    public static final int SECRET_LENGTH = ClusterCredentials.SECRET_LENGTH;
    public static final int ENCODED_LENGTH = ClusterCredentials.ENCODED_LENGTH;
    public static final String PROPERTY = "assets.auth.credential";
    public static final String ENV = "AE_AUTH_CREDENTIAL";

    private final ClusterCredentials kit;

    /** Same checks as RC1: a real session class (not UNSET, not NULL_VAL) and a 32-byte secret. */
    public CoreCredentials(final SessionClass sessionClass, final int keyId, final byte[] secret) {
        if (sessionClass == null || sessionClass == SessionClass.UNSET || sessionClass == SessionClass.NULL_VAL) {
            throw new IllegalArgumentException("credentials need a real session class, got " + sessionClass);
        }
        this.kit = new ClusterCredentials(AssetsSessionClasses.toKit(sessionClass), keyId, secret);
    }

    private CoreCredentials(final ClusterCredentials kit) {
        this.kit = kit;
    }

    /** Parse {@code keyId:class:secretHex}; class by name (Locale.ROOT) or numeric code. */
    public static CoreCredentials fromSpec(final String spec) {
        return new CoreCredentials(ClusterCredentials.fromSpec(spec));
    }

    /** From {@code -Dassets.auth.credential}, else {@code AE_AUTH_CREDENTIAL}; null when neither is set. */
    public static CoreCredentials fromEnvironment() {
        final ClusterCredentials c = ClusterCredentials.fromEnvironment(AssetsAuth.POLICY);
        return c == null ? null : new CoreCredentials(c);
    }

    /** Null when the bytes are not a v1 credential frame. */
    public static CoreCredentials decode(final byte[] bytes) {
        final ClusterCredentials c = ClusterCredentials.decode(bytes);
        return c == null ? null : new CoreCredentials(c);
    }

    public byte[] encode() {
        return kit.encode();
    }

    /** The Aeron client-side supplier: presents these bytes, refuses challenges (v1). */
    public CredentialsSupplier supplier() {
        return kit.supplier();
    }

    public SessionClass sessionClass() {
        return AssetsSessionClasses.of(kit.classCode());
    }

    public int keyId() {
        return kit.keyId();
    }

    /** Copy. */
    public byte[] secret() {
        return kit.secret();
    }

    /** The kit credential this shim wraps. */
    public ClusterCredentials kit() {
        return kit;
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.auth.AuthKeyTable;
import com.openexchange.assets.auth.CoreCredentials;
import com.openexchange.assets.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.security.CredentialsSupplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.HexFormat;

/**
 * The test key table (K6 v1) and the client-side helpers every in-process cluster test uses: one key per
 * session class, a TREASURY key scoped to user 7 only, and a RegisterSession encoder. {@link #install()}
 * publishes the table through the same system property the production launcher reads, so
 * {@code ClusterConfig.create} installs the real authenticator in tests.
 */
public final class TestAuth {

    private static final String SECRET_ADMIN = "11".repeat(32);
    private static final String SECRET_TOOL = "22".repeat(32);
    private static final String SECRET_BRIDGE = "33".repeat(32);
    private static final String SECRET_TREASURY_7 = "44".repeat(32);
    private static final String SECRET_GATEWAY = "55".repeat(32);
    private static final String SECRET_FORWARDER = "66".repeat(32);
    private static final String SECRET_TREASURY_ANY = "77".repeat(32);

    /** keyId:class:secret[:identity[:users=...]] */
    public static final String KEYS = String.join(";",
            "1:ADMIN:" + SECRET_ADMIN,
            "2:TOOL:" + SECRET_TOOL,
            "3:BRIDGE:" + SECRET_BRIDGE,
            "4:TREASURY:" + SECRET_TREASURY_7 + ":4:users=7",
            "5:GATEWAY:" + SECRET_GATEWAY,
            "6:FORWARDER:" + SECRET_FORWARDER,
            "7:TREASURY:" + SECRET_TREASURY_ANY);

    private TestAuth() {
    }

    /** Publish the test key table where ClusterConfig reads it. Idempotent. */
    public static void install() {
        System.setProperty(AuthKeyTable.PROPERTY, KEYS);
    }

    public static CredentialsSupplier credentials(final SessionClass sessionClass) {
        return credentialsOf(sessionClass).supplier();
    }

    /** The TREASURY key scoped to user 7 only (RED-F18). */
    public static CredentialsSupplier treasuryForUser7() {
        return new CoreCredentials(SessionClass.TREASURY, 4, HexFormat.of().parseHex(SECRET_TREASURY_7)).supplier();
    }

    /** A key the table does not hold. */
    public static CredentialsSupplier unknownKey() {
        return new CoreCredentials(SessionClass.ADMIN, 99, HexFormat.of().parseHex("99".repeat(32))).supplier();
    }

    /** The right key presented under the wrong class. */
    public static CredentialsSupplier wrongClass() {
        return new CoreCredentials(SessionClass.ADMIN, 2, HexFormat.of().parseHex(SECRET_TOOL)).supplier();
    }

    public static CoreCredentials credentialsOf(final SessionClass sessionClass) {
        switch (sessionClass) {
            case ADMIN: return new CoreCredentials(sessionClass, 1, HexFormat.of().parseHex(SECRET_ADMIN));
            case TOOL: return new CoreCredentials(sessionClass, 2, HexFormat.of().parseHex(SECRET_TOOL));
            case BRIDGE: return new CoreCredentials(sessionClass, 3, HexFormat.of().parseHex(SECRET_BRIDGE));
            case GATEWAY: return new CoreCredentials(sessionClass, 5, HexFormat.of().parseHex(SECRET_GATEWAY));
            case FORWARDER: return new CoreCredentials(sessionClass, 6, HexFormat.of().parseHex(SECRET_FORWARDER));
            case TREASURY: return new CoreCredentials(sessionClass, 7, HexFormat.of().parseHex(SECRET_TREASURY_ANY));
            default: throw new IllegalArgumentException("no test key for " + sessionClass);
        }
    }

    /** Encode a RegisterSession at offset 0; returns the frame length. */
    public static int encodeRegisterSession(final MutableDirectBuffer buf, final long correlationId,
                                            final SessionClass sessionClass, final boolean resumable,
                                            final boolean acks, final boolean balances, final boolean settlements,
                                            final boolean snapshots, final boolean orders, final boolean control) {
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .correlationId(correlationId).sessionClass(sessionClass).readerKind((short) 0)
                .resumable(resumable ? com.openexchange.assets.infrastructure.generated.BoolFlag.TRUE
                        : com.openexchange.assets.infrastructure.generated.BoolFlag.FALSE)
                .identity(0L).sourceEpoch(0L);
        final EgressChannelsEncoder ch = e.channels().clear();
        ch.acks(acks).balances(balances).settlements(settlements).snapshots(snapshots).orders(orders).control(control);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    /** Offer a resumable RegisterSession for {@code sessionClass} with every channel, blocking until accepted. */
    public static void register(final AeronCluster client, final SessionClass sessionClass) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);
        final int length = encodeRegisterSession(buf, 0x8E6L, sessionClass, true, true, true, true, true, true, true);
        final long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            final long result = client.offer(buf, 0, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("RegisterSession offer failed terminally: " + result);
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("RegisterSession not accepted within 10s");
            }
            Thread.onSpinWait();
        }
    }
}

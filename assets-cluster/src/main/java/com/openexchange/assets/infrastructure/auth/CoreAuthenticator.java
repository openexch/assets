// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.auth;

import com.openexchange.assets.auth.AuthKeyTable;
import com.openexchange.assets.auth.CoreCredentials;
import com.openexchange.assets.auth.CorePrincipal;
import io.aeron.security.Authenticator;
import io.aeron.security.SessionProxy;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * The consensus module's authenticator for the core (K6 v1): looks the presented credentials up in the
 * node-local {@link AuthKeyTable} and mints the session's {@link CorePrincipal}, which Aeron appends to
 * the SessionOpen log event so every member derives the same session authority. Node-local code,
 * replicated result. No challenge in v1: a challenged session is rejected. An empty key table rejects
 * everyone, loudly, instead of degrading to an unauthenticated mode.
 *
 * <p>Runs on the consensus module agent thread; one instance per node.</p>
 */
public final class CoreAuthenticator implements Authenticator {

    private final AuthKeyTable keys;
    private final Long2ObjectHashMap<byte[]> acceptedPrincipals = new Long2ObjectHashMap<>();
    private long accepted;
    private long rejected;
    private boolean warnedEmptyTable;

    public CoreAuthenticator(final AuthKeyTable keys) {
        this.keys = keys;
    }

    @Override
    public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
        final CorePrincipal principal = keys.authenticate(CoreCredentials.decode(encodedCredentials));
        if (principal != null) {
            acceptedPrincipals.put(sessionId, principal.encode());
        } else {
            acceptedPrincipals.remove(sessionId);
            if (keys.size() == 0 && !warnedEmptyTable) {
                warnedEmptyTable = true;
                System.err.println("AUTH: the key table is EMPTY (" + AuthKeyTable.PROPERTY + " / " + AuthKeyTable.ENV
                        + " not set); every session is rejected until it is configured");
            }
        }
    }

    @Override
    public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
        acceptedPrincipals.remove(sessionId); // v1 never challenges, so a response is foreign input
    }

    @Override
    public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs) {
        final byte[] principal = acceptedPrincipals.remove(sessionProxy.sessionId());
        if (principal != null) {
            if (sessionProxy.authenticate(principal)) {
                accepted++;
            } else {
                acceptedPrincipals.put(sessionProxy.sessionId(), principal); // not yet: retried next cycle
            }
        } else {
            rejected++;
            sessionProxy.reject();
        }
    }

    @Override
    public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs) {
        rejected++;
        sessionProxy.reject();
    }

    public long acceptedCount() {
        return accepted;
    }

    public long rejectedCount() {
        return rejected;
    }
}

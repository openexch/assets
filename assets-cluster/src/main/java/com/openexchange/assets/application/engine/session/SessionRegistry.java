// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.session;

import com.openexchange.assets.auth.CorePrincipal;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Every live session's {@link SessionAuthority}, keyed by cluster session id. Replicated-derived: fed by
 * the SessionOpen/SessionClose callbacks (which every member sees) and by RegisterSession log entries;
 * snapshotted as layout v4 section 9 so a member restored from a snapshot holds the same table as one
 * that replayed. Deterministic on every member by construction: no node-local input enters it.
 */
public final class SessionRegistry {

    /** Outcome of a RegisterSession apply. */
    public record Registration(AdminResult result, CoreRejectReason reason, boolean changed) { }

    private final Long2ObjectHashMap<SessionAuthority> byId = new Long2ObjectHashMap<>();
    private final ArrayList<SessionAuthority> scratch = new ArrayList<>();

    /** SessionOpen: parse the principal once. A malformed or missing principal becomes NONE (no authority). */
    public SessionAuthority open(final long sessionId, final byte[] encodedPrincipal) {
        final SessionAuthority a = new SessionAuthority(sessionId, CorePrincipal.decode(encodedPrincipal));
        byId.put(sessionId, a);
        return a;
    }

    /** SessionClose. */
    public void close(final long sessionId) {
        byId.remove(sessionId);
    }

    /** The authority of a session, or null when the session is unknown to this registry. */
    public SessionAuthority get(final long sessionId) {
        return byId.get(sessionId);
    }

    public int size() {
        return byId.size();
    }

    /**
     * Apply a RegisterSession (replicated). The asserted class must equal the principal's; the session must
     * declare itself resumable (every core session can be closed at its egress ceiling, critic D16);
     * re-registering is allowed and idempotent (DUPLICATE when nothing changed), which is how a client
     * re-declares after a leader change.
     */
    public Registration register(final long sessionId, final SessionClass assertedClass, final int channelMask,
                                 final boolean resumable, final int readerKind, final long identity,
                                 final long sourceEpoch) {
        final SessionAuthority a = byId.get(sessionId);
        if (a == null || a.sessionClass() == SessionClass.UNSET) {
            return new Registration(AdminResult.REJECTED, CoreRejectReason.UNAUTHORIZED_CLASS, false);
        }
        if (assertedClass != a.sessionClass()) {
            return new Registration(AdminResult.REJECTED, CoreRejectReason.UNAUTHORIZED_CLASS, false);
        }
        if (!resumable) {
            return new Registration(AdminResult.REJECTED, CoreRejectReason.NOT_RESUMABLE, false);
        }
        final boolean changed = a.register(channelMask, true, readerKind, identity, sourceEpoch);
        return new Registration(changed ? AdminResult.ACCEPTED : AdminResult.DUPLICATE, CoreRejectReason.NONE, changed);
    }

    /**
     * RC2 S7 D8b, at the bind: every registered FORWARDER or BRIDGE session whose {@code sourceEpoch} is not in
     * {0, {@code boundEpoch}} is revoked, in ascending session id; {@code revoked} receives each id (the demuxer closes
     * it through the transport hook). Returns how many were revoked.
     */
    public int revokeForeignEpoch(final long boundEpoch, final java.util.function.LongConsumer revoked) {
        final List<SessionAuthority> hit = new ArrayList<>();
        forEachBySessionId(a -> {
            if (a.isRegistered() && (a.sessionClass() == SessionClass.FORWARDER || a.sessionClass() == SessionClass.BRIDGE)
                    && a.sourceEpoch() != 0 && a.sourceEpoch() != boundEpoch) {
                hit.add(a);
            }
        });
        for (final SessionAuthority a : hit) {
            a.revoke();
            revoked.accept(a.sessionId());
        }
        return hit.size();
    }

    /** Restore one row (snapshot section 9). */
    public void restore(final long sessionId, final byte[] encodedPrincipal, final boolean registered,
                        final int channelMask, final boolean resumable, final int readerKind,
                        final long identity, final long sourceEpoch) {
        final SessionAuthority a = open(sessionId, encodedPrincipal);
        a.restore(registered, channelMask, resumable, readerKind, identity, sourceEpoch);
    }

    /** Visit in ascending session id (the canonical snapshot order). Not reentrant. */
    public void forEachBySessionId(final Consumer<SessionAuthority> visitor) {
        scratch.clear();
        scratch.addAll(byId.values());
        scratch.sort(Comparator.comparingLong(SessionAuthority::sessionId));
        for (int i = 0; i < scratch.size(); i++) {
            visitor.accept(scratch.get(i));
        }
        scratch.clear();
    }

    /** Snapshot-visible rows for tests. */
    public List<Long> sessionIds() {
        final List<Long> ids = new ArrayList<>();
        forEachBySessionId(a -> ids.add(a.sessionId()));
        return ids;
    }
}

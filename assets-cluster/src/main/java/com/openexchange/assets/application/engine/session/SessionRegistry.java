// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.session;

import com.openexchange.cluster.auth.ClusterPrincipal;
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
    /**
     * RC2 S10' 4R.1 rule 7: registered live sessions per class code ({@code assets_sessions{class}}), maintained at every
     * transition so a metrics scrape reads a plain int and never walks the table. Derived from the replicated table.
     */
    private final int[] registeredByClass = new int[CLASS_SLOTS];
    private static final int CLASS_SLOTS = 16;

    /** SessionOpen: parse the principal once. A malformed or missing principal becomes NONE (no authority). */
    public SessionAuthority open(final long sessionId, final byte[] encodedPrincipal) {
        final SessionAuthority a = new SessionAuthority(sessionId, ClusterPrincipal.decode(encodedPrincipal));
        uncount(byId.put(sessionId, a));
        return a;
    }

    /** SessionClose. */
    public void close(final long sessionId) {
        uncount(byId.remove(sessionId));
    }

    /** Registered live sessions of {@code cls} (RC2 S10' 4R.1 rule 7). */
    public int registeredCount(final SessionClass cls) {
        final int code = cls.value();
        return code >= 0 && code < CLASS_SLOTS ? registeredByClass[code] : 0;
    }

    private void uncount(final SessionAuthority a) {
        if (a != null && a.isRegistered()) {
            registeredByClass[slot(a)]--;
        }
    }

    private static int slot(final SessionAuthority a) {
        final int code = a.sessionClass().value();
        return code >= 0 && code < CLASS_SLOTS ? code : 0;
    }

    /**
     * RC2 S10' 4R.1 rule 2 (S2-3): another REGISTERED, live BRIDGE session already holds {@code identity}. A restarted
     * lane whose old session has closed finds none (its old row left the table at SessionClose).
     */
    private boolean bridgeIdentityHeldByAnother(final long sessionId, final long identity) {
        for (final SessionAuthority other : byId.values()) {
            if (other.sessionId() != sessionId && other.isRegistered() && other.sessionClass() == SessionClass.BRIDGE
                    && other.identity() == identity) {
                return true;
            }
        }
        return false;
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
     * re-declares after a leader change. Identities are not exclusive here (the behaviour 1/2 rule).
     */
    public Registration register(final long sessionId, final SessionClass assertedClass, final int channelMask,
                                 final boolean resumable, final int readerKind, final long identity,
                                 final long sourceEpoch) {
        return register(sessionId, assertedClass, channelMask, resumable, readerKind, identity, sourceEpoch, false);
    }

    /**
     * {@link #register} with RC2 S10' S2-3: when {@code exclusiveBridgeIdentity} (the engine passes behaviour >= 3, so a
     * v1/v2 log replays with the result it was recorded with, review FIX-2), a BRIDGE registration whose identity another
     * registered, live session holds is REJECTED {@code DUPLICATE_IDENTITY}.
     */
    public Registration register(final long sessionId, final SessionClass assertedClass, final int channelMask,
                                 final boolean resumable, final int readerKind, final long identity,
                                 final long sourceEpoch, final boolean exclusiveBridgeIdentity) {
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
        if (exclusiveBridgeIdentity && a.sessionClass() == SessionClass.BRIDGE
                && bridgeIdentityHeldByAnother(sessionId, identity)) {
            // RC2 S10' S2-3: two live lanes with one identity would share one lane-halt record; the second is refused
            return new Registration(AdminResult.REJECTED, CoreRejectReason.DUPLICATE_IDENTITY, false);
        }
        final boolean was = a.isRegistered();
        final boolean changed = a.register(channelMask, true, readerKind, identity, sourceEpoch);
        if (!was) {
            registeredByClass[slot(a)]++;
        }
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
            registeredByClass[slot(a)]--;
            a.revoke();
            revoked.accept(a.sessionId());
        }
        return hit.size();
    }

    /**
     * Restore one row (snapshot section 9). RC2 S17 W12 (R0): a row without a valid principal is pre-S6 history; the
     * kit's {@link com.openexchange.cluster.auth.PrincipalGate} throws before the row is written.
     */
    public void restore(final long sessionId, final byte[] encodedPrincipal, final boolean registered,
                        final int channelMask, final boolean resumable, final int readerKind,
                        final long identity, final long sourceEpoch) {
        com.openexchange.cluster.auth.PrincipalGate.requireClass(encodedPrincipal, "restored session", sessionId, -1L);
        final SessionAuthority a = open(sessionId, encodedPrincipal);
        a.restore(registered, channelMask, resumable, readerKind, identity, sourceEpoch);
        if (registered) {
            registeredByClass[slot(a)]++;
        }
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

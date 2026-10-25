// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.session;

import com.openexchange.assets.auth.CorePrincipal;
import com.openexchange.assets.infrastructure.generated.SessionClass;

/**
 * One cluster session as the core sees it (K6): the principal the authenticator minted (class, user
 * scope, identity) plus what the session declared through {@code RegisterSession} (registered, egress
 * channels, resumable, reader kind, source epoch). Replicated-derived state: the principal arrives with
 * the SessionOpen log event on every member, the registration is a log entry, and the table is
 * snapshotted (layout v4 section 9) so a restored member agrees with its peers about every live session.
 */
public final class SessionAuthority {

    /** The in-process authority tests and tools dispatch under: every class bit, every user. */
    public static final SessionAuthority TRUSTED = new SessionAuthority(-1L, CorePrincipal.anyUser(SessionClass.ADMIN, 0));

    static {
        TRUSTED.registered = true;
        TRUSTED.resumable = true;
        TRUSTED.trusted = true;
    }

    private final long sessionId;
    private CorePrincipal principal;
    private boolean registered;
    private boolean resumable;
    private int channelMask;
    private int readerKind;
    private long identity;
    private long sourceEpoch;
    private boolean trusted;

    public SessionAuthority(final long sessionId, final CorePrincipal principal) {
        this.sessionId = sessionId;
        this.principal = principal == null ? CorePrincipal.NONE : principal;
    }

    public long sessionId() {
        return sessionId;
    }

    public CorePrincipal principal() {
        return principal;
    }

    public SessionClass sessionClass() {
        return principal.sessionClass();
    }

    /** Bit for the class-mask table: {@code 1 << class code}; UNSET has no bit. */
    public int classBit() {
        final int code = principal.sessionClass().value();
        return code <= 0 || code > 6 ? 0 : 1 << code;
    }

    public boolean isTrusted() {
        return trusted;
    }

    public boolean isRegistered() {
        return registered;
    }

    public boolean isResumable() {
        return resumable;
    }

    public int channelMask() {
        return channelMask;
    }

    public int readerKind() {
        return readerKind;
    }

    public long identity() {
        return identity;
    }

    public long sourceEpoch() {
        return sourceEpoch;
    }

    public boolean allowsUser(final long userId) {
        return trusted || principal.allowsUser(userId);
    }

    /** Record a RegisterSession that the registry accepted. Returns true when something changed. */
    boolean register(final int channelMask, final boolean resumable, final int readerKind, final long identity,
                     final long sourceEpoch) {
        final boolean changed = !registered || this.channelMask != channelMask || this.resumable != resumable
                || this.readerKind != readerKind || this.identity != identity || this.sourceEpoch != sourceEpoch;
        this.registered = true;
        this.channelMask = channelMask;
        this.resumable = resumable;
        this.readerKind = readerKind;
        this.identity = identity;
        this.sourceEpoch = sourceEpoch;
        return changed;
    }

    /** Restore only (snapshot section 9). */
    void restore(final boolean registered, final int channelMask, final boolean resumable, final int readerKind,
                 final long identity, final long sourceEpoch) {
        this.registered = registered;
        this.channelMask = channelMask;
        this.resumable = resumable;
        this.readerKind = readerKind;
        this.identity = identity;
        this.sourceEpoch = sourceEpoch;
    }

    @Override
    public String toString() {
        return "SessionAuthority{session=" + sessionId + " " + principal + " registered=" + registered
                + " resumable=" + resumable + " mask=" + Integer.toBinaryString(channelMask) + "}";
    }
}

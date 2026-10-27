// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.assets.infrastructure.generated.AmendOrderDecoder;
import com.openexchange.assets.infrastructure.generated.CancelOrderDecoder;
import com.openexchange.assets.infrastructure.generated.CircuitBreakerDecoder;
import com.openexchange.assets.infrastructure.generated.CoreConfigDecoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderDecoder;
import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.HoldDecoder;
import com.openexchange.assets.infrastructure.generated.InitTradeHighWaterDecoder;
import com.openexchange.assets.infrastructure.generated.MarketTickDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateDecoder;
import com.openexchange.assets.infrastructure.generated.QuarantineOrderDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.assets.infrastructure.generated.ReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestOrderSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestPendingDispatchDecoder;
import com.openexchange.assets.infrastructure.generated.RequestRiskConfigDecoder;
import com.openexchange.assets.infrastructure.generated.RiskConfigSetDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.SetMoneyJournalDecoder;
import com.openexchange.assets.infrastructure.generated.SettleBatchDecoder;
import com.openexchange.assets.infrastructure.generated.SettleDecoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * RCS S1 test helper: a {@link CoreFrameStory} as the log a real member sees. The story's TRUSTED frames (which only
 * the in-process {@code dispatchTrusted} can apply, and which the service would dispatch as an unknown session) are
 * re-homed to one production session per class: the first class allowed for the template, in the order ADMIN, BRIDGE,
 * FORWARDER, GATEWAY (the demuxer's class-mask table). Each such session has id {@code 900 + class code} and principal
 * {@code ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(class), 90 + class code)}; a prefix opens all four and registers the three classes that
 * must register before sending. Every original step keeps its kind, time and bytes and is shifted by the prefix length;
 * a story SWEEP becomes a TimerEvent with the term-0 sweep id.
 */
final class SessionedLog {

    enum Kind { OPEN, CLOSE, MESSAGE, TIMER }

    /** One log entry with its frame-end position; {@code bytes} is the principal for OPEN, the frame for MESSAGE. */
    record LogStep(Kind kind, long sessionId, long timestamp, byte[] bytes, long logPosition, long correlationId) {
        LogStep at(final long position) {
            return new LogStep(kind, sessionId, timestamp, bytes, position, correlationId);
        }

        LogStep from(final long newSessionId) {
            return new LogStep(kind, newSessionId, timestamp, bytes, logPosition, correlationId);
        }
    }

    static final SessionClass[] HOMES = {SessionClass.ADMIN, SessionClass.BRIDGE, SessionClass.FORWARDER, SessionClass.GATEWAY};

    private SessionedLog() {
    }

    static long sessionOf(final SessionClass c) {
        return 900L + c.value();
    }

    static byte[] principalOf(final SessionClass c) {
        return ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(c), 90 + c.value()).encode();
    }

    /** The class a TRUSTED frame of this template is re-homed to (strict: an unmapped template throws). */
    static SessionClass homeOf(final int templateId) {
        switch (templateId) {
            case DepositDecoder.TEMPLATE_ID:
            case WithdrawDecoder.TEMPLATE_ID:
            case HoldDecoder.TEMPLATE_ID:
            case ReleaseDecoder.TEMPLATE_ID:
            case QueryFeedPositionDecoder.TEMPLATE_ID:
            case RequestBalanceSnapshotDecoder.TEMPLATE_ID:
            case RequestHoldSnapshotDecoder.TEMPLATE_ID:
            case InitTradeHighWaterDecoder.TEMPLATE_ID:
            case SetMoneyJournalDecoder.TEMPLATE_ID:
            case RequestOrderSnapshotDecoder.TEMPLATE_ID:
            case CoreConfigDecoder.TEMPLATE_ID:
            case RiskConfigSetDecoder.TEMPLATE_ID:
            case CircuitBreakerDecoder.TEMPLATE_ID:
            case OrderEntryGateDecoder.TEMPLATE_ID:
            case QuarantineOrderDecoder.TEMPLATE_ID:
            case RequestRiskConfigDecoder.TEMPLATE_ID:
                return SessionClass.ADMIN;
            case SettleDecoder.TEMPLATE_ID:
            case SettleBatchDecoder.TEMPLATE_ID:
            case TerminalReleaseDecoder.TEMPLATE_ID:
            case MeJournalTradeDecoder.TEMPLATE_ID:
            case MeJournalTradeBatchDecoder.TEMPLATE_ID:
            case MeJournalTerminalDecoder.TEMPLATE_ID:
            case MeCommandOutcomeDecoder.TEMPLATE_ID:
                return SessionClass.BRIDGE;
            case MarketTickDecoder.TEMPLATE_ID:
            case RequestPendingDispatchDecoder.TEMPLATE_ID:
                return SessionClass.FORWARDER;
            case CreateOrderDecoder.TEMPLATE_ID:
            case CancelOrderDecoder.TEMPLATE_ID:
            case AmendOrderDecoder.TEMPLATE_ID:
                return SessionClass.GATEWAY;
            default:
                throw new IllegalArgumentException("no production home for TRUSTED templateId " + templateId);
        }
    }

    /** The log space an entry occupies (the story's own rule: a frame is 64 + its 32-aligned length, the rest 64). */
    static long sizeOf(final LogStep s) {
        return s.kind() == Kind.MESSAGE ? 64 + ((s.bytes().length + 31) & ~31) : 64;
    }

    static List<LogStep> from(final List<CoreFrameStory.Step> story) {
        final long t0 = story.isEmpty() ? CoreFrameStory.T0 : story.get(0).timestamp();
        final CoreFrameEncoders enc = new CoreFrameEncoders();
        final List<LogStep> prefix = new ArrayList<>();
        for (final SessionClass c : HOMES) {
            prefix.add(open(sessionOf(c), t0, principalOf(c)));
        }
        long corr = 9_000_001L;
        for (final SessionClass c : HOMES) {
            if (c == SessionClass.GATEWAY || c == SessionClass.FORWARDER || c == SessionClass.BRIDGE) {
                prefix.add(message(sessionOf(c), t0, enc.register(corr++, c)));
            }
        }
        final List<LogStep> out = renumber(prefix);
        final long shift = out.get(out.size() - 1).logPosition();
        for (final CoreFrameStory.Step s : story) {
            final long position = s.logPosition() + shift;
            switch (s.kind()) {
                case FRAME -> {
                    final long session = s.sessionId() == CoreFrameStory.TRUSTED ? sessionOf(homeOf(s.templateId())) : s.sessionId();
                    out.add(new LogStep(Kind.MESSAGE, session, s.timestamp(), s.bytes(), position, 0L));
                }
                case SWEEP -> out.add(new LogStep(Kind.TIMER, 0L, s.timestamp(), null, position,
                        AssetsLogApplier.SWEEP_TIMER_BASE));
                case OPEN_SESSION -> out.add(new LogStep(Kind.OPEN, s.sessionId(), s.timestamp(), s.bytes(), position, 0L));
                default -> throw new IllegalStateException("step kind " + s.kind());
            }
        }
        // Self-check: the shift equals the cumulative rule, so renumber() keeps positions of an unmodified log.
        final List<LogStep> check = renumber(out);
        for (int i = 0; i < out.size(); i++) {
            if (check.get(i).logPosition() != out.get(i).logPosition()) {
                throw new IllegalStateException("SessionedLog position self-check failed at step " + i);
            }
        }
        return out;
    }

    /** Positions recomputed cumulatively from 0 (for logs with inserted entries). */
    static List<LogStep> renumber(final List<LogStep> steps) {
        final List<LogStep> out = new ArrayList<>(steps.size());
        long position = 0;
        for (final LogStep s : steps) {
            position += sizeOf(s);
            out.add(s.at(position));
        }
        return out;
    }

    static LogStep open(final long sessionId, final long timestamp, final byte[] principal) {
        return new LogStep(Kind.OPEN, sessionId, timestamp, principal, 0L, 0L);
    }

    static LogStep close(final long sessionId, final long timestamp) {
        return new LogStep(Kind.CLOSE, sessionId, timestamp, null, 0L, 0L);
    }

    static LogStep message(final long sessionId, final long timestamp, final byte[] bytes) {
        return new LogStep(Kind.MESSAGE, sessionId, timestamp, bytes, 0L, 0L);
    }

    static LogStep timer(final long correlationId, final long timestamp) {
        return new LogStep(Kind.TIMER, 0L, timestamp, null, 0L, correlationId);
    }
}

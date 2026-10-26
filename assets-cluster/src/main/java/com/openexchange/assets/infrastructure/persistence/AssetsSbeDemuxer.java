// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.session.SessionAuthority;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.infrastructure.generated.EgressChannelsDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateDecoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.commands.AmendOrderCommand;
import com.openexchange.assets.domain.commands.CancelOrderCommand;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.RiskConfigSetCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.InitTradeHighWaterCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SetMoneyJournalCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import com.openexchange.assets.infrastructure.generated.AmendOrderDecoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.CancelOrderDecoder;
import com.openexchange.assets.infrastructure.generated.CircuitBreakerDecoder;
import com.openexchange.assets.infrastructure.generated.CoreConfigDecoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderDecoder;
import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.FeedHaltDecoder;
import com.openexchange.assets.infrastructure.generated.FeedLatchClearDecoder;
import com.openexchange.assets.infrastructure.generated.HoldDecoder;
import com.openexchange.assets.infrastructure.generated.InitTradeHighWaterDecoder;
import com.openexchange.assets.infrastructure.generated.MarketTickDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateDecoder;
import com.openexchange.assets.infrastructure.generated.QuarantineOrderDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder;
import com.openexchange.assets.infrastructure.generated.ReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestOrderSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestPendingDispatchDecoder;
import com.openexchange.assets.infrastructure.generated.RequestRiskConfigDecoder;
import com.openexchange.assets.infrastructure.generated.RiskConfigSetDecoder;
import com.openexchange.assets.infrastructure.generated.SetMoneyJournalDecoder;
import com.openexchange.assets.infrastructure.generated.SettleBatchDecoder;
import com.openexchange.assets.infrastructure.generated.SettleDecoder;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/**
 * Inbound adapter: decodes an SBE ingress message into a pooled domain command and drives the
 * {@link AssetsEngine}. Zero allocation, zero string parsing on the hot path; decoders and command
 * objects are reused. The domain never sees a byte buffer; it only ever receives commands.
 *
 * <p><b>The ingress fence (RC1 S1, port of the ME's 23090c6).</b> Before any body decode the frame
 * passes: physical bounds, an exact schema/version gate, a per-template minimum blockLength, group
 * bounds for grouped templates, and the known-template table. Enum bytes are read RAW and range-checked
 * so an out-of-range byte is a deterministic drop, never an {@code IllegalArgumentException} out of a
 * generated accessor. Decoders wrap a view limited to the frame, so stale bytes in the backing receive
 * buffer cannot become money. Everything dropped here is counted (node-local diagnostics, never
 * replicated truth; identical bytes drop identically on every replica, so consensus is unaffected).</p>
 *
 * <p>Once a decoded command has entered the engine ({@code applying = true}) any {@code RuntimeException}
 * or {@code Error} is an APPLICATION FAULT: the demuxer latches, refuses every later dispatch, and
 * rethrows so the service closes readiness and halts. A partially applied command must never become an
 * applied prefix; recovery is a fresh process restoring verified state and replaying the log.</p>
 */
public final class AssetsSbeDemuxer {

    /**
     * The wire identity every ingress frame must carry. The version gate is EXACT (critic D12, RC2 D2): the core
     * speaks v7 from RC2 S1 on (single-manifest clean-genesis cut); a v6 producer is dropped and counted rather
     * than decoded with its {@code sinceVersion="7"} fields silently absent, and no v7 frame reaches a v6 core.
     * A later additive bump turns this into a range again, with the ME's rule: the floor advances only when
     * pre-floor producers and pre-floor frames in any replayable log are provably gone.
     */
    static final int EXPECTED_SCHEMA_ID = MessageHeaderDecoder.SCHEMA_ID;
    static final int EXPECTED_SCHEMA_VERSION = MessageHeaderDecoder.SCHEMA_VERSION;
    static final int MIN_SUPPORTED_SCHEMA_VERSION = MessageHeaderDecoder.SCHEMA_VERSION;

    private static final int HEADER = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int GROUP_HEADER = 4;
    /** Loud-log the first reject of each kind, then one in every REJECT_LOG_INTERVAL. */
    private static final long REJECT_LOG_INTERVAL = 1000L;

    private final AssetsEngine engine;
    private final SettlementProjector projector;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    /** The frame view: decoders see exactly [offset, offset + length), never the backing buffer. */
    private final UnsafeBuffer frame = new UnsafeBuffer();

    private final DepositDecoder depositDecoder = new DepositDecoder();
    private final WithdrawDecoder withdrawDecoder = new WithdrawDecoder();
    private final HoldDecoder holdDecoder = new HoldDecoder();
    private final ReleaseDecoder releaseDecoder = new ReleaseDecoder();
    private final SettleDecoder settleDecoder = new SettleDecoder();
    private final SettleBatchDecoder settleBatchDecoder = new SettleBatchDecoder();
    private final TerminalReleaseDecoder terminalReleaseDecoder = new TerminalReleaseDecoder();
    private final QueryFeedPositionDecoder queryFeedPositionDecoder = new QueryFeedPositionDecoder();
    private final InitTradeHighWaterDecoder initHighWaterDecoder = new InitTradeHighWaterDecoder();
    private final SetMoneyJournalDecoder setMoneyJournalDecoder = new SetMoneyJournalDecoder();
    private final RequestBalanceSnapshotDecoder balanceSnapshotDecoder = new RequestBalanceSnapshotDecoder();
    private final RequestHoldSnapshotDecoder holdSnapshotDecoder = new RequestHoldSnapshotDecoder();

    private final DepositCommand depositCommand = new DepositCommand();
    private final WithdrawCommand withdrawCommand = new WithdrawCommand();
    private final HoldCommand holdCommand = new HoldCommand();
    private final ReleaseCommand releaseCommand = new ReleaseCommand();
    private final InitTradeHighWaterCommand initHighWaterCommand = new InitTradeHighWaterCommand();
    private final SetMoneyJournalCommand setMoneyJournalCommand = new SetMoneyJournalCommand();

    // Ingress-reject counters. Plain longs: written only on the cluster service thread, read by the
    // /metrics scraper. NOT replicated, NOT snapshotted: pure local observability.
    private long schemaRejectCount;    // header schemaId/version outside this build's supported set
    private long decodeRejectCount;    // frame bounds / group bounds / invalid enum or field byte
    private long unknownTemplateCount; // templateId not in this schema at all
    private long unhandledTemplateCount; // in the schema, not served by this build (v7 66/67/68/76 until RC2 S7/S8)
    private long applyErrorCount;
    // Service-thread only. Once a command may have partially applied, this instance cannot establish
    // another applied prefix. Decode-only poison never enters this state.
    private boolean applying;
    private boolean applyFailed;

    public AssetsSbeDemuxer(AssetsEngine engine, SettlementProjector projector) {
        this.engine = engine;
        this.projector = projector;
    }

    /**
     * Is this templateId one of the read-only QUERY commands? A query mutates nothing and emits only
     * reply frames, so while {@link #dispatch} is handling one, <em>every</em> emission is scoped to the
     * asking session. Kept next to the dispatch switch so a new query message cannot be added to one
     * without the other.
     */
    public static boolean isQuery(final int templateId) {
        switch (templateId) {
            case QueryFeedPositionDecoder.TEMPLATE_ID:
            case RequestBalanceSnapshotDecoder.TEMPLATE_ID:
            case RequestHoldSnapshotDecoder.TEMPLATE_ID:
            case RequestOrderSnapshotDecoder.TEMPLATE_ID:
            case RequestPendingDispatchDecoder.TEMPLATE_ID:
            case RequestRiskConfigDecoder.TEMPLATE_ID:
                return true;
            default:
                return false;
        }
    }

    /**
     * Minimum root block length per known template, {@code -1} for a templateId this schema does not
     * define. Generated constants, so a schema change moves the table with it.
     */
    static int minimumBlockLength(final int templateId) {
        switch (templateId) {
            case DepositDecoder.TEMPLATE_ID:                return DepositDecoder.BLOCK_LENGTH;
            case WithdrawDecoder.TEMPLATE_ID:               return WithdrawDecoder.BLOCK_LENGTH;
            case HoldDecoder.TEMPLATE_ID:                   return HoldDecoder.BLOCK_LENGTH;
            case ReleaseDecoder.TEMPLATE_ID:                return ReleaseDecoder.BLOCK_LENGTH;
            case SettleDecoder.TEMPLATE_ID:                 return SettleDecoder.BLOCK_LENGTH;
            case SettleBatchDecoder.TEMPLATE_ID:            return SettleBatchDecoder.BLOCK_LENGTH;
            case TerminalReleaseDecoder.TEMPLATE_ID:        return TerminalReleaseDecoder.BLOCK_LENGTH;
            case QueryFeedPositionDecoder.TEMPLATE_ID:      return QueryFeedPositionDecoder.BLOCK_LENGTH;
            case RequestBalanceSnapshotDecoder.TEMPLATE_ID: return RequestBalanceSnapshotDecoder.BLOCK_LENGTH;
            case RequestHoldSnapshotDecoder.TEMPLATE_ID:    return RequestHoldSnapshotDecoder.BLOCK_LENGTH;
            case InitTradeHighWaterDecoder.TEMPLATE_ID:     return InitTradeHighWaterDecoder.BLOCK_LENGTH;
            case SubscribeDecoder.TEMPLATE_ID:              return SubscribeDecoder.BLOCK_LENGTH;
            case SetMoneyJournalDecoder.TEMPLATE_ID:        return SetMoneyJournalDecoder.BLOCK_LENGTH;
            case CreateOrderDecoder.TEMPLATE_ID:            return CreateOrderDecoder.BLOCK_LENGTH;
            case CancelOrderDecoder.TEMPLATE_ID:            return CancelOrderDecoder.BLOCK_LENGTH;
            case AmendOrderDecoder.TEMPLATE_ID:             return AmendOrderDecoder.BLOCK_LENGTH;
            case RequestOrderSnapshotDecoder.TEMPLATE_ID:   return RequestOrderSnapshotDecoder.BLOCK_LENGTH;
            case MarketTickDecoder.TEMPLATE_ID:             return MarketTickDecoder.BLOCK_LENGTH;
            case MeJournalTradeDecoder.TEMPLATE_ID:         return MeJournalTradeDecoder.BLOCK_LENGTH;
            case MeJournalTradeBatchDecoder.TEMPLATE_ID:    return MeJournalTradeBatchDecoder.BLOCK_LENGTH;
            case MeJournalTerminalDecoder.TEMPLATE_ID:      return MeJournalTerminalDecoder.BLOCK_LENGTH;
            case MeCommandOutcomeDecoder.TEMPLATE_ID:       return MeCommandOutcomeDecoder.BLOCK_LENGTH;
            case RequestPendingDispatchDecoder.TEMPLATE_ID: return RequestPendingDispatchDecoder.BLOCK_LENGTH;
            case CoreConfigDecoder.TEMPLATE_ID:             return CoreConfigDecoder.BLOCK_LENGTH;
            case RiskConfigSetDecoder.TEMPLATE_ID:          return RiskConfigSetDecoder.BLOCK_LENGTH;
            case CircuitBreakerDecoder.TEMPLATE_ID:         return CircuitBreakerDecoder.BLOCK_LENGTH;
            case OrderEntryGateDecoder.TEMPLATE_ID:         return OrderEntryGateDecoder.BLOCK_LENGTH;
            case QuarantineOrderDecoder.TEMPLATE_ID:        return QuarantineOrderDecoder.BLOCK_LENGTH;
            case RequestRiskConfigDecoder.TEMPLATE_ID:      return RequestRiskConfigDecoder.BLOCK_LENGTH;
            case RegisterSessionDecoder.TEMPLATE_ID:        return RegisterSessionDecoder.BLOCK_LENGTH;
            // v7 (RC2 S1 4.3/4.3a): defined and bounds-checked here, served by S7/S8.
            case FeedHaltDecoder.TEMPLATE_ID:               return FeedHaltDecoder.BLOCK_LENGTH;
            case MeCommandReleaseDecoder.TEMPLATE_ID:       return MeCommandReleaseDecoder.BLOCK_LENGTH;
            case MeEngineConfigDecoder.TEMPLATE_ID:         return MeEngineConfigDecoder.BLOCK_LENGTH;
            case FeedLatchClearDecoder.TEMPLATE_ID:         return FeedLatchClearDecoder.BLOCK_LENGTH;
            default: return -1;
        }
    }

    /** Leader-local transport effects of replicated session decisions; set by the service. */
    public interface TransportHooks {
        TransportHooks NONE = new TransportHooks() {
            public void onSessionRegistered(long sessionId, int channelMask, boolean resumable) { }
            public void onSessionRejected(long sessionId) { }
        };

        void onSessionRegistered(long sessionId, int channelMask, boolean resumable);

        void onSessionRejected(long sessionId);
    }

    private TransportHooks hooks = TransportHooks.NONE;

    public void setTransportHooks(final TransportHooks hooks) {
        this.hooks = hooks == null ? TransportHooks.NONE : hooks;
    }

    // ---- RC1 S3 (K6): the class-mask table, one int of class bits per templateId; zero allocation ----
    private static final int BIT_GATEWAY = 1 << SessionClass.GATEWAY.value();
    private static final int BIT_FORWARDER = 1 << SessionClass.FORWARDER.value();
    private static final int BIT_BRIDGE = 1 << SessionClass.BRIDGE.value();
    private static final int BIT_ADMIN = 1 << SessionClass.ADMIN.value();
    private static final int BIT_TOOL = 1 << SessionClass.TOOL.value();
    private static final int BIT_TREASURY = 1 << SessionClass.TREASURY.value();
    // REPLICA (7, money-schema v7) has no bit in any template's mask, ANY_CLASS included: every ingress but
    // RegisterSession (judged by the registry, never by this table) from a REPLICA session is UNAUTHORIZED_CLASS.
    private static final int ANY_CLASS = BIT_GATEWAY | BIT_FORWARDER | BIT_BRIDGE | BIT_ADMIN | BIT_TOOL | BIT_TREASURY;
    private static final int[] CLASS_MASK = new int[256];
    /** Templates a production-class session may send BEFORE it has registered (queries and the registration itself). */
    private static final boolean[] PRE_REGISTRATION = new boolean[256];
    /** Templates whose body carries a userId the session's scope must allow. */
    private static final boolean[] USER_SCOPED = new boolean[256];
    /** Templates that start with a correlationId (echoed on the ORIGIN reject). */
    private static final boolean[] HAS_CORRELATION = new boolean[256];

    static {
        CLASS_MASK[DepositDecoder.TEMPLATE_ID] = BIT_TREASURY | BIT_ADMIN | BIT_TOOL;
        CLASS_MASK[WithdrawDecoder.TEMPLATE_ID] = BIT_TREASURY | BIT_ADMIN | BIT_TOOL;
        CLASS_MASK[HoldDecoder.TEMPLATE_ID] = BIT_TOOL | BIT_ADMIN;
        CLASS_MASK[ReleaseDecoder.TEMPLATE_ID] = BIT_TOOL | BIT_ADMIN;
        CLASS_MASK[SettleDecoder.TEMPLATE_ID] = BIT_BRIDGE | BIT_TOOL;
        CLASS_MASK[SettleBatchDecoder.TEMPLATE_ID] = BIT_BRIDGE | BIT_TOOL;
        CLASS_MASK[TerminalReleaseDecoder.TEMPLATE_ID] = BIT_BRIDGE | BIT_TOOL;
        CLASS_MASK[QueryFeedPositionDecoder.TEMPLATE_ID] = ANY_CLASS;
        CLASS_MASK[RequestBalanceSnapshotDecoder.TEMPLATE_ID] = BIT_ADMIN | BIT_TOOL;
        CLASS_MASK[RequestHoldSnapshotDecoder.TEMPLATE_ID] = BIT_ADMIN | BIT_TOOL;
        CLASS_MASK[InitTradeHighWaterDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[SubscribeDecoder.TEMPLATE_ID] = ANY_CLASS;
        CLASS_MASK[SetMoneyJournalDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[CreateOrderDecoder.TEMPLATE_ID] = BIT_GATEWAY;
        CLASS_MASK[CancelOrderDecoder.TEMPLATE_ID] = BIT_GATEWAY;
        CLASS_MASK[AmendOrderDecoder.TEMPLATE_ID] = BIT_GATEWAY;
        CLASS_MASK[RequestOrderSnapshotDecoder.TEMPLATE_ID] = BIT_GATEWAY | BIT_ADMIN;
        CLASS_MASK[MarketTickDecoder.TEMPLATE_ID] = BIT_FORWARDER;
        CLASS_MASK[MeJournalTradeDecoder.TEMPLATE_ID] = BIT_BRIDGE;
        CLASS_MASK[MeJournalTradeBatchDecoder.TEMPLATE_ID] = BIT_BRIDGE;
        CLASS_MASK[MeJournalTerminalDecoder.TEMPLATE_ID] = BIT_BRIDGE;
        CLASS_MASK[MeCommandOutcomeDecoder.TEMPLATE_ID] = BIT_BRIDGE;
        CLASS_MASK[RequestPendingDispatchDecoder.TEMPLATE_ID] = BIT_FORWARDER;
        CLASS_MASK[CoreConfigDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[RiskConfigSetDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[CircuitBreakerDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[OrderEntryGateDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[QuarantineOrderDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[RequestRiskConfigDecoder.TEMPLATE_ID] = BIT_ADMIN;
        CLASS_MASK[RegisterSessionDecoder.TEMPLATE_ID] = ANY_CLASS;
        // v7 (RC2 S1 4.5): feed templates 66-68 are BRIDGE (registration required), the latch exit 76 is ADMIN.
        CLASS_MASK[FeedHaltDecoder.TEMPLATE_ID] = BIT_BRIDGE;
        CLASS_MASK[MeCommandReleaseDecoder.TEMPLATE_ID] = BIT_BRIDGE;
        CLASS_MASK[MeEngineConfigDecoder.TEMPLATE_ID] = BIT_BRIDGE;
        CLASS_MASK[FeedLatchClearDecoder.TEMPLATE_ID] = BIT_ADMIN;
        PRE_REGISTRATION[QueryFeedPositionDecoder.TEMPLATE_ID] = true;
        PRE_REGISTRATION[SubscribeDecoder.TEMPLATE_ID] = true;
        PRE_REGISTRATION[RegisterSessionDecoder.TEMPLATE_ID] = true;
        for (final int t : new int[] {DepositDecoder.TEMPLATE_ID, WithdrawDecoder.TEMPLATE_ID, HoldDecoder.TEMPLATE_ID,
                ReleaseDecoder.TEMPLATE_ID, CreateOrderDecoder.TEMPLATE_ID, CancelOrderDecoder.TEMPLATE_ID,
                AmendOrderDecoder.TEMPLATE_ID, RequestOrderSnapshotDecoder.TEMPLATE_ID}) {
            USER_SCOPED[t] = true;
        }
        // v7: 66 FeedHalt and 76 FeedLatchClear start with a correlationId; 67 and 68 do not (correlationIdOf = 0).
        for (final int t : new int[] {1, 2, 3, 7, 8, 9, 32, 50, 51, 52, 53, 65, 66, 70, 71, 72, 73, 74, 75, 76, 80}) {
            HAS_CORRELATION[t] = true;
        }
    }

    /** Classes that must RegisterSession before anything but a query: the production consumers. */
    private static boolean requiresRegistration(final SessionClass c) {
        return c == SessionClass.GATEWAY || c == SessionClass.FORWARDER || c == SessionClass.BRIDGE;
    }

    private final RegisterSessionDecoder registerSessionDecoder = new RegisterSessionDecoder();
    private final CreateOrderDecoder createOrderDecoder = new CreateOrderDecoder();
    private final CancelOrderDecoder cancelOrderDecoder = new CancelOrderDecoder();
    private final MarketTickDecoder marketTickDecoder = new MarketTickDecoder();
    private final RiskConfigSetDecoder riskConfigSetDecoder = new RiskConfigSetDecoder();
    private final CircuitBreakerDecoder circuitBreakerDecoder = new CircuitBreakerDecoder();
    private final RequestRiskConfigDecoder requestRiskConfigDecoder = new RequestRiskConfigDecoder();
    // RC1 S5
    private final AmendOrderDecoder amendOrderDecoder = new AmendOrderDecoder();
    private final RequestOrderSnapshotDecoder requestOrderSnapshotDecoder = new RequestOrderSnapshotDecoder();
    private final MeJournalTradeDecoder meJournalTradeDecoder = new MeJournalTradeDecoder();
    private final MeJournalTradeBatchDecoder meJournalTradeBatchDecoder = new MeJournalTradeBatchDecoder();
    private final MeJournalTerminalDecoder meJournalTerminalDecoder = new MeJournalTerminalDecoder();
    private final MeCommandOutcomeDecoder meCommandOutcomeDecoder = new MeCommandOutcomeDecoder();
    private final RequestPendingDispatchDecoder requestPendingDispatchDecoder = new RequestPendingDispatchDecoder();
    private final QuarantineOrderDecoder quarantineOrderDecoder = new QuarantineOrderDecoder();
    private final CoreConfigDecoder coreConfigDecoder = new CoreConfigDecoder();
    // RC2 S1 (v7), decoded for range checks only
    private final FeedHaltDecoder feedHaltDecoder = new FeedHaltDecoder();
    private final MeEngineConfigDecoder meEngineConfigDecoder = new MeEngineConfigDecoder();
    private final FeedLatchClearDecoder feedLatchClearDecoder = new FeedLatchClearDecoder();
    private final AmendOrderCommand amendOrderCommand = new AmendOrderCommand();
    private final CreateOrderCommand createOrderCommand = new CreateOrderCommand();
    private final CancelOrderCommand cancelOrderCommand = new CancelOrderCommand();
    private final RiskConfigSetCommand riskConfigSetCommand = new RiskConfigSetCommand();
    private final OrderEntryGateDecoder orderEntryGateDecoder = new OrderEntryGateDecoder();
    /** A session this registry never saw (or whose principal was unreadable): authorises nothing. */
    private final SessionAuthority unknownSession = new SessionAuthority(-2L, null);

    /**
     * Dispatch a frame from a cluster session: the frame gate, then the session's authority (class mask,
     * registration-first for production classes, user scope), then the handler. Every authority failure is
     * a deterministic ORIGIN OrderReject naming the template, counted, never a mutation or a fence event.
     */
    public void dispatch(final long sessionId, final DirectBuffer buffer, final int offset, final int length,
                         final long timestamp) {
        final SessionAuthority authority = engine.sessions().get(sessionId);
        dispatchChecked(authority == null ? unknownSession : authority, buffer, offset, length, timestamp);
    }

    /** Commands refused for session authority (class, user scope, registration). */
    public long unauthorisedCount() {
        return engine.getUnauthorisedCount();
    }

    /**
     * Dispatch under the TRUSTED authority (every class, every user): in-process tests and tools only.
     * The service never calls this; it dispatches by session id.
     */
    public void dispatchTrusted(final DirectBuffer buffer, final int offset, final int length, final long timestamp) {
        dispatchChecked(SessionAuthority.TRUSTED, buffer, offset, length, timestamp);
    }

    private void dispatchChecked(final SessionAuthority authority, final DirectBuffer buffer, final int offset,
                                 final int length, final long timestamp) {
        if (applyFailed) {
            throw new IllegalStateException("Ingress application already failed; restart required");
        }
        // 1. Physical bounds: explicit, so they stay effective with Agrona bounds checks disabled.
        if (offset < 0 || length < HEADER || offset > buffer.capacity() || length > buffer.capacity() - offset) {
            rejectFrameBounds();
            return;
        }
        frame.wrap(buffer, offset, length);
        headerDecoder.wrap(frame, 0);

        // 2. Header gate, before any body decode: a stale or foreign frame never reaches an accessor.
        final int schemaId = headerDecoder.schemaId();
        final int version = headerDecoder.version();
        if (schemaId != EXPECTED_SCHEMA_ID
                || version < MIN_SUPPORTED_SCHEMA_VERSION
                || version > EXPECTED_SCHEMA_VERSION) {
            final long n = ++schemaRejectCount;
            if (shouldLogReject(n)) {
                System.err.println("INGRESS DROP (schema): schemaId=" + schemaId + " version=" + version
                        + " templateId=" + headerDecoder.templateId() + "; this build speaks schemaId="
                        + EXPECTED_SCHEMA_ID + " version " + MIN_SUPPORTED_SCHEMA_VERSION + ".."
                        + EXPECTED_SCHEMA_VERSION + "; schemaRejects=" + n);
            }
            return;
        }

        // 3. Known template, declared block within the frame and at least the generated minimum.
        final int templateId = headerDecoder.templateId();
        final int minimum = minimumBlockLength(templateId);
        if (minimum < 0) {
            final long n = ++unknownTemplateCount;
            if (shouldLogReject(n)) {
                System.err.println("INGRESS DROP (unknown template): templateId=" + templateId
                        + " length=" + length + "; unknownTemplates=" + n);
            }
            return;
        }
        final int block = headerDecoder.blockLength();
        if (block < minimum || block > length - HEADER) {
            rejectFrameBounds();
            return;
        }
        // 4. Group bounds for every grouped template this build decodes.
        if (templateId == SettleBatchDecoder.TEMPLATE_ID
                && !groupWithinFrame(HEADER + block, SettleBatchDecoder.SettlesDecoder.sbeBlockLength(), length)) {
            rejectFrameBounds();
            return;
        }
        if (templateId == MeJournalTradeBatchDecoder.TEMPLATE_ID
                && !groupWithinFrame(HEADER + block, MeJournalTradeBatchDecoder.TradesDecoder.sbeBlockLength(), length)) {
            rejectFrameBounds(); // RC1 S7: the S5 batch was missing from this list
            return;
        }
        if (templateId == CoreConfigDecoder.TEMPLATE_ID && !coreConfigGroupsWithinFrame(HEADER + block, length)) {
            rejectFrameBounds();
            return;
        }
        if (templateId == MeCommandReleaseDecoder.TEMPLATE_ID
                && !releaseGroupWithinFrame(HEADER + block, length)) {
            rejectFrameBounds(); // v7: bounds as 62, plus the frozen cap of 48 entries (RC2 S1 4.3)
            return;
        }

        // 5. Session authority (K6): class mask, registration-first, user scope. RegisterSession itself is
        //    judged by the registry (so a session with no usable principal gets a REJECTED ack, not silence).
        if (!authority.isTrusted() && templateId != RegisterSessionDecoder.TEMPLATE_ID) {
            // Order (hardening B3.4): registration first, then class, then user scope.
            if (requiresRegistration(authority.sessionClass()) && !authority.isRegistered()
                    && !PRE_REGISTRATION[templateId]) {
                engine.rejectUnauthorised(templateId, correlationIdOf(templateId), userIdOf(templateId),
                        CoreRejectReason.UNREGISTERED_SESSION, authority.sessionId());
                return;
            }
            if ((CLASS_MASK[templateId] & authority.classBit()) == 0) {
                engine.rejectUnauthorised(templateId, correlationIdOf(templateId), userIdOf(templateId),
                        CoreRejectReason.UNAUTHORIZED_CLASS, authority.sessionClass().value());
                return;
            }
            if (USER_SCOPED[templateId]) {
                final long userId = userIdOf(templateId);
                if (!authority.allowsUser(userId)) {
                    engine.rejectUnauthorised(templateId, correlationIdOf(templateId), userId,
                            CoreRejectReason.NOT_OWNER, userId);
                    return;
                }
            }
        }

        // 6. Body decode and apply. Decode poison (IAE/IOOBE before any application callback) is dropped
        //    deterministically; any failure after `applying = true` latches and escapes to the service.
        try {
            switch (templateId) {
                case DepositDecoder.TEMPLATE_ID:                handleDeposit(authority, timestamp); break;
                case WithdrawDecoder.TEMPLATE_ID:               handleWithdraw(authority, timestamp); break;
                case CreateOrderDecoder.TEMPLATE_ID:            handleCreateOrder(timestamp); break;
                case CancelOrderDecoder.TEMPLATE_ID:            handleCancelOrder(timestamp); break;
                case MarketTickDecoder.TEMPLATE_ID:             handleMarketTick(timestamp); break;
                case RiskConfigSetDecoder.TEMPLATE_ID:          handleRiskConfigSet(timestamp); break;
                case CircuitBreakerDecoder.TEMPLATE_ID:         handleCircuitBreaker(timestamp); break;
                case RequestRiskConfigDecoder.TEMPLATE_ID:      handleRequestRiskConfig(); break;
                case AmendOrderDecoder.TEMPLATE_ID:             handleAmendOrder(timestamp); break;
                case RequestOrderSnapshotDecoder.TEMPLATE_ID:   handleRequestOrderSnapshot(authority); break;
                case MeJournalTradeDecoder.TEMPLATE_ID:         handleMeJournalTrade(timestamp); break;
                case MeJournalTradeBatchDecoder.TEMPLATE_ID:    handleMeJournalTradeBatch(timestamp); break;
                case MeJournalTerminalDecoder.TEMPLATE_ID:      handleMeJournalTerminal(timestamp); break;
                case MeCommandOutcomeDecoder.TEMPLATE_ID:       handleMeCommandOutcome(timestamp); break;
                case RequestPendingDispatchDecoder.TEMPLATE_ID: handleRequestPendingDispatch(); break;
                case QuarantineOrderDecoder.TEMPLATE_ID:        handleQuarantineOrder(timestamp); break;
                case CoreConfigDecoder.TEMPLATE_ID:             handleCoreConfig(timestamp); break;
                case HoldDecoder.TEMPLATE_ID:                   handleHold(timestamp); break;
                case ReleaseDecoder.TEMPLATE_ID:                handleRelease(timestamp); break;
                case SettleDecoder.TEMPLATE_ID:                 handleSettle(timestamp); break;
                case SettleBatchDecoder.TEMPLATE_ID:            handleSettleBatch(timestamp); break;
                case TerminalReleaseDecoder.TEMPLATE_ID:        handleTerminalRelease(timestamp); break;
                case QueryFeedPositionDecoder.TEMPLATE_ID:      handleQueryFeedPosition(); break;
                case InitTradeHighWaterDecoder.TEMPLATE_ID:     handleInitHighWater(timestamp); break;
                case RequestBalanceSnapshotDecoder.TEMPLATE_ID: handleRequestBalanceSnapshot(); break;
                case RequestHoldSnapshotDecoder.TEMPLATE_ID:    handleRequestHoldSnapshot(); break;
                case SetMoneyJournalDecoder.TEMPLATE_ID:        handleSetMoneyJournal(timestamp); break;
                case RegisterSessionDecoder.TEMPLATE_ID:     handleRegisterSession(authority, timestamp); break;
                case OrderEntryGateDecoder.TEMPLATE_ID:      handleOrderEntryGate(timestamp); break;
                // v7 (RC2 S1): raw ranges validated, then counted unserved (no mutation, no egress) until S7/S8.
                case FeedHaltDecoder.TEMPLATE_ID:            handleFeedHalt(); break;
                case MeCommandReleaseDecoder.TEMPLATE_ID:    handleMeCommandRelease(); break;
                case MeEngineConfigDecoder.TEMPLATE_ID:      handleMeEngineConfig(); break;
                case FeedLatchClearDecoder.TEMPLATE_ID:      handleFeedLatchClear(); break;
                case SubscribeDecoder.TEMPLATE_ID:
                    // Transport-level; consumed at the service boundary before dispatch. Reaching
                    // here means the service peek declined it (bad length/version): drop, count.
                    rejectDecode("Subscribe frame reached the engine");
                    break;
                default:
                    // Templates defined by this schema and not served by this build. Counted so a producer speaking
                    // ahead of this build is visible, never silent. Every known template has a case above today.
                    unhandled(templateId);
                    break;
            }
        } catch (final RuntimeException | Error failure) {
            if (!applying && (failure instanceof IllegalArgumentException
                    || failure instanceof IndexOutOfBoundsException)) {
                rejectDecode(failure.getClass().getSimpleName());
            } else {
                // Even IllegalArgumentException/IndexOutOfBoundsException can come from a publisher or a
                // mutated engine. Never relabel those as malformed wire input.
                applyFailed = true;
                final long n = ++applyErrorCount;
                System.err.println("INGRESS APPLY FAILED: templateId=" + templateId
                        + " type=" + failure.getClass().getName() + "; applyErrors=" + n
                        + "; restart required before any further application");
                throw failure;
            }
        } finally {
            applying = false;
        }
    }

    // ---- handlers: validate the raw bytes, THEN mark applying, THEN enter the engine ----

    /** The production money boundaries (GATEWAY, TREASURY) must carry a requestId (critic D5); tools may omit it. */
    private static boolean requestIdMandatory(final SessionAuthority authority) {
        return !authority.isTrusted() && (authority.sessionClass() == SessionClass.GATEWAY
                || authority.sessionClass() == SessionClass.TREASURY);
    }

    private void handleDeposit(final SessionAuthority authority, long timestamp) {
        depositDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int assetId = depositDecoder.assetId();
        if (!validAsset(assetId)) {
            rejectDecode("Deposit assetId " + assetId);
            return;
        }
        final long rh = depositDecoder.requestIdHigh();
        final long rl = depositDecoder.requestIdLow();
        if (rh == 0 && rl == 0 && requestIdMandatory(authority)) {
            applying = true;
            engine.rejectUnauthorised(DepositDecoder.TEMPLATE_ID, depositDecoder.correlationId(), depositDecoder.userId(),
                    CoreRejectReason.MISSING_REQUEST_ID, 0L);
            return;
        }
        depositCommand.reset();
        depositCommand.setCorrelationId(depositDecoder.correlationId());
        depositCommand.setUserId(depositDecoder.userId());
        depositCommand.setAssetId(assetId);
        depositCommand.setAmount(depositDecoder.amount());
        depositCommand.setRequestIdHigh(rh);
        depositCommand.setRequestIdLow(rl);
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, depositCommand, timestamp);
    }

    private void handleWithdraw(final SessionAuthority authority, long timestamp) {
        withdrawDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int assetId = withdrawDecoder.assetId();
        if (!validAsset(assetId)) {
            rejectDecode("Withdraw assetId " + assetId);
            return;
        }
        final long rh = withdrawDecoder.requestIdHigh();
        final long rl = withdrawDecoder.requestIdLow();
        if (rh == 0 && rl == 0 && requestIdMandatory(authority)) {
            applying = true;
            engine.rejectUnauthorised(WithdrawDecoder.TEMPLATE_ID, withdrawDecoder.correlationId(), withdrawDecoder.userId(),
                    CoreRejectReason.MISSING_REQUEST_ID, 0L);
            return;
        }
        withdrawCommand.reset();
        withdrawCommand.setCorrelationId(withdrawDecoder.correlationId());
        withdrawCommand.setUserId(withdrawDecoder.userId());
        withdrawCommand.setAssetId(assetId);
        withdrawCommand.setAmount(withdrawDecoder.amount());
        withdrawCommand.setRequestIdHigh(rh);
        withdrawCommand.setRequestIdLow(rl);
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_WITHDRAW, withdrawCommand, timestamp);
    }

    // ---- RC1 S4: order ingress (wire validity here, semantics in the order engine) ----

    private void handleCreateOrder(long timestamp) {
        createOrderDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int side = createOrderDecoder.sideRaw();
        final int type = createOrderDecoder.orderTypeRaw();
        final int tif = createOrderDecoder.timeInForceRaw();
        if (side < 0 || side > 1 || type < 0 || type > 6 || tif < 0 || tif > 3) {
            rejectDecode("CreateOrder side " + side + " type " + type + " tif " + tif);
            return;
        }
        final CreateOrderCommand c = createOrderCommand;
        c.reset();
        c.correlationId = createOrderDecoder.correlationId();
        c.userId = createOrderDecoder.userId();
        c.clientOrderIdHigh = createOrderDecoder.clientOrderIdHigh();
        c.clientOrderIdLow = createOrderDecoder.clientOrderIdLow();
        c.requestIdHigh = createOrderDecoder.requestIdHigh();
        c.requestIdLow = createOrderDecoder.requestIdLow();
        c.marketId = createOrderDecoder.marketId();
        c.side = side;
        c.orderType = type;
        c.timeInForce = tif;
        c.price = createOrderDecoder.price();
        c.quantity = createOrderDecoder.quantity();
        c.budget = createOrderDecoder.budget();
        c.stopPrice = createOrderDecoder.stopPrice();
        c.trailingDelta = createOrderDecoder.trailingDelta();
        c.displayQty = createOrderDecoder.displayQty();
        c.expiresAtMs = createOrderDecoder.expiresAtMs();
        c.flags = (int) frame.getInt(HEADER + CreateOrderDecoder.flagsEncodingOffset());
        applying = true;
        engine.orders().applyCreate(c, timestamp);
    }

    private void handleCancelOrder(long timestamp) {
        cancelOrderDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final CancelOrderCommand c = cancelOrderCommand;
        c.reset();
        c.correlationId = cancelOrderDecoder.correlationId();
        c.userId = cancelOrderDecoder.userId();
        c.orderId = cancelOrderDecoder.orderId();
        c.requestIdHigh = cancelOrderDecoder.requestIdHigh();
        c.requestIdLow = cancelOrderDecoder.requestIdLow();
        c.origClientOrderIdHigh = cancelOrderDecoder.origClientOrderIdHigh();
        c.origClientOrderIdLow = cancelOrderDecoder.origClientOrderIdLow();
        applying = true;
        engine.orders().applyCancel(c, timestamp);
    }

    private void handleMarketTick(long timestamp) {
        marketTickDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        engine.orders().applyMarketTick(marketTickDecoder.marketId(), marketTickDecoder.bookVersion(),
                marketTickDecoder.bestBid(), marketTickDecoder.bestAsk(), marketTickDecoder.lastTradePrice(),
                marketTickDecoder.lastTradeId(), marketTickDecoder.meEpoch(), timestamp);
    }

    private void handleRiskConfigSet(long timestamp) {
        riskConfigSetDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final RiskConfigSetCommand c = riskConfigSetCommand;
        c.reset();
        c.correlationId = riskConfigSetDecoder.correlationId();
        c.configVersion = riskConfigSetDecoder.configVersion();
        c.marketId = riskConfigSetDecoder.marketId();
        c.minQuantity = riskConfigSetDecoder.minQuantity();
        c.maxQuantity = riskConfigSetDecoder.maxQuantity();
        c.minNotional = riskConfigSetDecoder.minNotional();
        c.maxNotional = riskConfigSetDecoder.maxNotional();
        c.priceCollarBps = (int) riskConfigSetDecoder.priceCollarBps();
        c.maxOrdersPerSec = riskConfigSetDecoder.maxOrdersPerSec();
        c.maxOrdersPerMin = riskConfigSetDecoder.maxOrdersPerMin();
        c.maxOpenOrders = riskConfigSetDecoder.maxOpenOrders();
        c.maxPositionPerMarket = riskConfigSetDecoder.maxPositionPerMarket();
        c.marketBuySlippageBps = (int) riskConfigSetDecoder.marketBuySlippageBps();
        applying = true;
        engine.orders().applyRiskConfigSet(c, timestamp);
    }

    private void handleCircuitBreaker(long timestamp) {
        circuitBreakerDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int action = circuitBreakerDecoder.actionRaw();
        if (action < 0 || action > 1) {
            rejectDecode("CircuitBreaker action " + action);
            return;
        }
        applying = true;
        engine.orders().applyCircuitBreaker(circuitBreakerDecoder.correlationId(), circuitBreakerDecoder.marketId(),
                action == 1, timestamp);
    }

    // ---- RC1 S5: amend, quarantine, feed events, queries ----

    private void handleAmendOrder(long timestamp) {
        amendOrderDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final AmendOrderCommand c = amendOrderCommand;
        c.reset();
        c.correlationId = amendOrderDecoder.correlationId();
        c.userId = amendOrderDecoder.userId();
        c.orderId = amendOrderDecoder.orderId();
        c.requestIdHigh = amendOrderDecoder.requestIdHigh();
        c.requestIdLow = amendOrderDecoder.requestIdLow();
        c.newPrice = amendOrderDecoder.newPrice();
        c.newQuantity = amendOrderDecoder.newQuantity();
        applying = true;
        engine.orders().applyAmend(c, timestamp);
    }

    private void handleRequestOrderSnapshot(final SessionAuthority authority) {
        requestOrderSnapshotDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final boolean allUsers = authority.isTrusted() || authority.sessionClass() == SessionClass.ADMIN;
        applying = true;
        engine.orders().applyRequestOrderSnapshot(requestOrderSnapshotDecoder.correlationId(), requestOrderSnapshotDecoder.userId(),
                requestOrderSnapshotDecoder.fromOrderId(), allUsers);
    }

    /** 61 (and 62 per entry). v7 {@code journalSeq} is accepted with any value and not passed to the engine until RC2 S7. */
    private void handleMeJournalTrade(long timestamp) {
        meJournalTradeDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int buy = meJournalTradeDecoder.takerIsBuyRaw();
        if (!validBool(buy)) {
            rejectDecode("MeJournalTrade takerIsBuy " + buy);
            return;
        }
        applying = true;
        engine.orders().applyTrade(meJournalTradeDecoder.egressSeq(), meJournalTradeDecoder.tradeId(), meJournalTradeDecoder.marketId(),
                meJournalTradeDecoder.takerOrderId(), meJournalTradeDecoder.takerUserId(), meJournalTradeDecoder.makerOrderId(),
                meJournalTradeDecoder.makerUserId(), meJournalTradeDecoder.price(), meJournalTradeDecoder.quantity(),
                buy == BoolFlag.TRUE.value(), meJournalTradeDecoder.meTimestamp(), meJournalTradeDecoder.takerOmsOrderId(),
                meJournalTradeDecoder.makerOmsOrderId(), meJournalTradeDecoder.meEpoch(), timestamp);
    }

    private void handleMeJournalTradeBatch(long timestamp) {
        meJournalTradeBatchDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        // Raw enum bytes are validated for the WHOLE group before the first entry is applied (K5: no partial batch).
        // The frame's OWN dimensions (root block, group entry block) locate the bytes, exactly as the generated decoder
        // will read them (RC1 S8 fuzz finding: the generated constants misread a widened block).
        final int groupOffset = HEADER + headerDecoder.blockLength();
        final int blockLength = Short.toUnsignedInt(frame.getShort(groupOffset, ByteOrder.LITTLE_ENDIAN));
        final int count = Short.toUnsignedInt(frame.getShort(groupOffset + 2, ByteOrder.LITTLE_ENDIAN));
        final int groupStart = groupOffset + GROUP_HEADER;
        for (int i = 0; i < count; i++) {
            final int raw = frame.getByte(groupStart + i * blockLength + MeJournalTradeBatchDecoder.TradesDecoder.takerIsBuyEncodingOffset()) & 0xFF;
            if (!validBool(raw)) {
                rejectDecode("MeJournalTradeBatch takerIsBuy " + raw + " at " + i);
                return;
            }
        }
        meJournalTradeBatchDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        for (final MeJournalTradeBatchDecoder.TradesDecoder t : meJournalTradeBatchDecoder.trades()) {
            engine.orders().applyTrade(t.egressSeq(), t.tradeId(), t.marketId(), t.takerOrderId(), t.takerUserId(), t.makerOrderId(),
                    t.makerUserId(), t.price(), t.quantity(), t.takerIsBuyRaw() == BoolFlag.TRUE.value(), t.meTimestamp(),
                    t.takerOmsOrderId(), t.makerOmsOrderId(), t.meEpoch(), timestamp);
        }
    }

    /**
     * 63. v7 {@code rejectReason} is the ME code verbatim ({@code 0 = NONE}); 255 is never written by the bridge and is
     * decode poison. It and {@code journalSeq} are not passed to the engine until RC2 S7.
     */
    private void handleMeJournalTerminal(long timestamp) {
        meJournalTerminalDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int status = meJournalTerminalDecoder.statusRaw();
        final int meReject = meJournalTerminalDecoder.rejectReason();
        if (status < 2 || status > 5 || meReject < 0 || meReject > 254) {
            rejectDecode("MeJournalTerminal status " + status + " rejectReason " + meReject);
            return;
        }
        applying = true;
        engine.orders().applyTerminal(meJournalTerminalDecoder.egressSeq(), meJournalTerminalDecoder.orderId(), meJournalTerminalDecoder.userId(),
                meJournalTerminalDecoder.marketId(), status, meJournalTerminalDecoder.meTimestamp(), meJournalTerminalDecoder.omsOrderId(),
                meJournalTerminalDecoder.meEpoch(), timestamp);
    }

    /**
     * 64. v7 ranges: {@code commandKind 0..2}, {@code result 0..8} (7 RELEASED and 8 INVALID reach applyOutcome and land
     * in its default branch, counted stale with no mutation, until RC2 S8), {@code timeInForce 0..2}. The v7 tail
     * (journalSeq, timeInForce, expectedOldRemaining, ledgerSize, ledgerCapacity, behaviorVersion) is not passed (S8).
     */
    private void handleMeCommandOutcome(long timestamp) {
        meCommandOutcomeDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int kind = meCommandOutcomeDecoder.commandKind();
        final int result = meCommandOutcomeDecoder.result();
        final int tif = meCommandOutcomeDecoder.timeInForce();
        if (kind < 0 || kind > 2 || result < 0 || result > 8 || tif < 0 || tif > 2) {
            rejectDecode("MeCommandOutcome kind " + kind + " result " + result + " timeInForce " + tif);
            return;
        }
        applying = true;
        engine.orders().applyOutcome(meCommandOutcomeDecoder.egressSeq(), meCommandOutcomeDecoder.commandIdHigh(),
                meCommandOutcomeDecoder.commandIdLow(), meCommandOutcomeDecoder.userId(), meCommandOutcomeDecoder.omsOrderId(),
                meCommandOutcomeDecoder.oldOrderId(), meCommandOutcomeDecoder.price(), meCommandOutcomeDecoder.quantity(),
                meCommandOutcomeDecoder.budget(), meCommandOutcomeDecoder.marketId(), kind, result, meCommandOutcomeDecoder.orderId(),
                meCommandOutcomeDecoder.reason(), meCommandOutcomeDecoder.meTimestamp(), meCommandOutcomeDecoder.meEpoch(), timestamp);
    }

    private void handleRequestPendingDispatch() {
        requestPendingDispatchDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        engine.orders().applyRequestPendingDispatch(requestPendingDispatchDecoder.correlationId(),
                requestPendingDispatchDecoder.fromDispatchSeq(), requestPendingDispatchDecoder.maxEntries());
    }

    /** CoreConfig (70, ADMIN): groups decoded with raw character checks first; the decision table is the engine's. */
    private void handleCoreConfig(long timestamp) {
        coreConfigDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final com.openexchange.assets.domain.commands.CoreConfigCommand c = new com.openexchange.assets.domain.commands.CoreConfigCommand();
        c.reset();
        c.correlationId = coreConfigDecoder.correlationId();
        c.configVersion = coreConfigDecoder.configVersion();
        c.behaviorVersion = coreConfigDecoder.behaviorVersion();
        // v7 (RC2 S1 4.4): decoded only; excluded from the DUPLICATE equality and not recorded until RC2 S7.
        c.genesisId = coreConfigDecoder.genesisId();
        final java.util.List<com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef> markets = new java.util.ArrayList<>();
        for (final CoreConfigDecoder.MarketsDecoder m : coreConfigDecoder.markets()) {
            final String symbol = fixedAscii(m::symbol, CoreConfigDecoder.MarketsDecoder.symbolLength());
            if (symbol == null) {
                rejectDecode("CoreConfig market " + m.marketId() + " symbol bytes");
                return;
            }
            markets.add(new com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef(m.marketId(), symbol, m.baseAssetId(),
                    m.quoteAssetId(), m.tickSize(), m.minPrice(), m.maxPrice(), m.lotSize()));
        }
        final java.util.List<com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef> assets = new java.util.ArrayList<>();
        for (final CoreConfigDecoder.AssetsDecoder a : coreConfigDecoder.assets()) {
            final String code = fixedAscii(a::code, CoreConfigDecoder.AssetsDecoder.codeLength());
            if (code == null) {
                rejectDecode("CoreConfig asset " + a.assetId() + " code bytes");
                return;
            }
            assets.add(new com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef(a.assetId(), code, a.scale()));
        }
        c.markets = markets.toArray(new com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef[0]);
        c.assets = assets.toArray(new com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef[0]);
        applying = true;
        engine.applyCoreConfig(c, timestamp);
    }

    private void handleQuarantineOrder(long timestamp) {
        quarantineOrderDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        engine.orders().applyQuarantine(quarantineOrderDecoder.correlationId(), quarantineOrderDecoder.orderId(), timestamp);
    }

    private void handleRequestRiskConfig() {
        requestRiskConfigDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        engine.orders().applyRequestRiskConfig(requestRiskConfigDecoder.correlationId(), requestRiskConfigDecoder.marketId());
    }

    private void handleHold(long timestamp) {
        holdDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int assetId = holdDecoder.assetId();
        final int flag = holdDecoder.omsManagedReleaseRaw();
        if (!validAsset(assetId) || !validBool(flag)) {
            rejectDecode("Hold assetId " + assetId + " omsManagedRelease " + flag);
            return;
        }
        holdCommand.reset();
        holdCommand.setCorrelationId(holdDecoder.correlationId());
        holdCommand.setOrderId(holdDecoder.orderId());
        holdCommand.setUserId(holdDecoder.userId());
        holdCommand.setAssetId(assetId);
        holdCommand.setAmount(holdDecoder.amount());
        holdCommand.setOmsManagedRelease(flag == BoolFlag.TRUE.value());
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_HOLD, holdCommand, timestamp);
    }

    private void handleRelease(long timestamp) {
        releaseDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        releaseCommand.reset();
        releaseCommand.setOrderId(releaseDecoder.orderId());
        releaseCommand.setUserId(releaseDecoder.userId());
        releaseCommand.setAmount(releaseDecoder.amount());
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_RELEASE, releaseCommand, timestamp);
    }

    /**
     * All settles route through the {@link SettlementProjector} so the journal cursor (consumePosition)
     * advances atomically with the money mutation. Semantic validity (market, price, quantity, notional)
     * is the engine's deterministic, loud refusal ({@code SettleRejected}); only wire validity is checked
     * here. Direct ingress (tests/tools) carries journalPosition=0, which never advances the cursor.
     */
    private void handleSettle(long timestamp) {
        settleDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int takerIsBuy = settleDecoder.takerIsBuyRaw();
        if (!validBool(takerIsBuy)) {
            rejectDecode("Settle takerIsBuy " + takerIsBuy);
            return;
        }
        applying = true;
        projector.onTrade(
                settleDecoder.journalPosition(),
                settleDecoder.tradeId(),
                settleDecoder.marketId(),
                settleDecoder.takerOrderId(),
                settleDecoder.takerUserId(),
                settleDecoder.makerOrderId(),
                settleDecoder.makerUserId(),
                settleDecoder.price(),
                settleDecoder.quantity(),
                takerIsBuy == BoolFlag.TRUE.value(),
                timestamp);
    }

    /**
     * A {@link SettleBatchDecoder SettleBatch}: the WHOLE group is wire-validated before the first entry
     * is applied, so a poison byte in entry k never leaves entries 0..k-1 applied with the cursor
     * advanced (atomic at the decode boundary). Entries then apply strictly in group order through the
     * identical {@link SettlementProjector#onTrade} path as a single Settle. An entry the engine REFUSES
     * (unknown market, bad amounts) stops the batch there: the entries behind it stay unapplied and the
     * cursor stays below the refused position, so the feed resumes exactly at the refusal.
     */
    private void handleSettleBatch(long timestamp) {
        settleBatchDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int groupOffset = HEADER + headerDecoder.blockLength();
        final int entryBlock = Short.toUnsignedInt(frame.getShort(groupOffset, ByteOrder.LITTLE_ENDIAN));
        final int count = Short.toUnsignedInt(frame.getShort(groupOffset + 2, ByteOrder.LITTLE_ENDIAN));
        final int flagOffset = SettleBatchDecoder.SettlesDecoder.takerIsBuyEncodingOffset();
        for (int i = 0; i < count; i++) {
            final int flag = frame.getByte(groupOffset + GROUP_HEADER + i * entryBlock + flagOffset) & 0xFF;
            if (!validBool(flag)) {
                rejectDecode("SettleBatch entry " + i + " takerIsBuy " + flag);
                return;
            }
        }
        applying = true;
        for (final SettleBatchDecoder.SettlesDecoder s : settleBatchDecoder.settles()) {
            final boolean applied = projector.onTrade(
                    s.journalPosition(),
                    s.tradeId(),
                    s.marketId(),
                    s.takerOrderId(),
                    s.takerUserId(),
                    s.makerOrderId(),
                    s.makerUserId(),
                    s.price(),
                    s.quantity(),
                    s.takerIsBuyRaw() == BoolFlag.TRUE.value(),
                    timestamp);
            if (!applied) {
                return; // refused loudly by the engine: stop here, the feed resumes at this position
            }
        }
    }

    /** Feed-forward terminal from the ME journal: release the order's full residual hold. */
    private void handleTerminalRelease(long timestamp) {
        terminalReleaseDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        projector.onTerminal(
                terminalReleaseDecoder.journalPosition(),
                terminalReleaseDecoder.orderId(),
                terminalReleaseDecoder.userId(),
                timestamp);
    }

    /** Read-only: the engine answers with a FeedPositionReport through the sink. */
    private void handleQueryFeedPosition() {
        queryFeedPositionDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        engine.reportFeedPosition(queryFeedPositionDecoder.correlationId());
    }

    private void handleInitHighWater(long timestamp) {
        initHighWaterDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        initHighWaterCommand.reset();
        initHighWaterCommand.setTradeId(initHighWaterDecoder.tradeId());
        initHighWaterCommand.setConsumePosition(initHighWaterDecoder.consumePosition());
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_INIT_HIGH_WATER, initHighWaterCommand, timestamp);
    }

    /**
     * Arm/disarm the money journal cluster-wide. A COMMAND, not a query: it is applied on every replica
     * from the log, which is what keeps journalSeq (replicated, snapshotted) identical everywhere.
     */
    private void handleSetMoneyJournal(long timestamp) {
        setMoneyJournalDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int enabled = setMoneyJournalDecoder.enabledRaw();
        if (!validBool(enabled)) {
            rejectDecode("SetMoneyJournal enabled " + enabled);
            return;
        }
        setMoneyJournalCommand.reset();
        setMoneyJournalCommand.setCorrelationId(setMoneyJournalDecoder.correlationId());
        setMoneyJournalCommand.setEnabled(enabled == BoolFlag.TRUE.value());
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_SET_MONEY_JOURNAL, setMoneyJournalCommand, timestamp);
    }

    private void handleRequestBalanceSnapshot() {
        balanceSnapshotDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        engine.requestBalanceSnapshot(balanceSnapshotDecoder.correlationId());
    }

    private void handleRequestHoldSnapshot() {
        holdSnapshotDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        applying = true;
        engine.requestHoldSnapshot(holdSnapshotDecoder.correlationId());
    }

    /**
     * RegisterSession (replicated): the registry asserts the class against the principal and requires a
     * resumable declaration; the leader-local effects (egress mask, resumable mark, or closing a refused
     * session) go through the transport hooks the service installed.
     */
    private void handleRegisterSession(final SessionAuthority authority, final long timestamp) {
        registerSessionDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int classRaw = registerSessionDecoder.sessionClassRaw();
        final int resumableRaw = registerSessionDecoder.resumableRaw();
        if (classRaw < 0 || classRaw > 7 || !validBool(resumableRaw)) { // v7: 7 = REPLICA
            rejectDecode("RegisterSession class " + classRaw + " resumable " + resumableRaw);
            return;
        }
        final SessionClass asserted = com.openexchange.assets.auth.CorePrincipal.classOf(classRaw);
        final EgressChannelsDecoder ch = registerSessionDecoder.channels();
        int mask = 0;
        if (ch.acks())        mask |= AssetsEventPublisher.CH_ACKS;
        if (ch.balances())    mask |= AssetsEventPublisher.CH_BALANCES;
        if (ch.settlements()) mask |= AssetsEventPublisher.CH_SETTLEMENTS;
        if (ch.snapshots())   mask |= AssetsEventPublisher.CH_SNAPSHOTS;
        if (ch.orders())      mask |= AssetsEventPublisher.CH_ORDERS;
        if (ch.dispatch())    mask |= AssetsEventPublisher.CH_DISPATCH;
        if (ch.control())     mask |= AssetsEventPublisher.CH_CONTROL;
        final long correlationId = registerSessionDecoder.correlationId();
        final int readerKind = registerSessionDecoder.readerKind();
        final long identity = registerSessionDecoder.identity();
        final long sourceEpoch = registerSessionDecoder.sourceEpoch();
        applying = true;
        final var registration = engine.applyRegisterSession(authority.sessionId(), correlationId, asserted, mask,
                resumableRaw == BoolFlag.TRUE.value(), readerKind, identity, sourceEpoch, timestamp);
        if (registration.result() == com.openexchange.assets.domain.AdminResult.REJECTED) {
            hooks.onSessionRejected(authority.sessionId());
        } else {
            hooks.onSessionRegistered(authority.sessionId(), mask, true);
        }
    }

    /** OrderEntryGate (ADMIN, replicated): raw enum bytes are validated before anything is applied. */
    private void handleOrderEntryGate(final long timestamp) {
        orderEntryGateDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final GateState state = GateState.fromCode(orderEntryGateDecoder.stateRaw());
        final GateReason reason = GateReason.fromCode(orderEntryGateDecoder.reasonRaw());
        if (state == null || reason == null) {
            rejectDecode("OrderEntryGate state " + orderEntryGateDecoder.stateRaw() + " reason " + orderEntryGateDecoder.reasonRaw());
            return;
        }
        applying = true;
        engine.applyOrderEntryGate(orderEntryGateDecoder.correlationId(), orderEntryGateDecoder.marketId(), state, reason, timestamp);
    }

    // ---- v7 (RC2 S1 4.3 / 4.3a): defined, bounds- and range-checked, not served (S7 / S8) ----

    /** FeedHalt (66, BRIDGE): {@code reason 1..5}; NONE (0) is not a halt. Served by RC2 S7. */
    private void handleFeedHalt() {
        feedHaltDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int reason = feedHaltDecoder.reasonRaw();
        if (reason < 1 || reason > 5) {
            rejectDecode("FeedHalt reason " + reason);
            return;
        }
        unhandled(FeedHaltDecoder.TEMPLATE_ID);
    }

    /**
     * MeCommandRelease (67, BRIDGE): every entry's {@code scope 0..1} and {@code result 0..2}, located by the frame's
     * own dimensions as for 62. Served by RC2 S8.
     */
    private void handleMeCommandRelease() {
        final int groupOffset = HEADER + headerDecoder.blockLength();
        final int entryBlock = Short.toUnsignedInt(frame.getShort(groupOffset, ByteOrder.LITTLE_ENDIAN));
        final int count = Short.toUnsignedInt(frame.getShort(groupOffset + 2, ByteOrder.LITTLE_ENDIAN));
        final int groupStart = groupOffset + GROUP_HEADER;
        for (int i = 0; i < count; i++) {
            final int at = groupStart + i * entryBlock;
            final int scope = frame.getByte(at + MeCommandReleaseDecoder.ReleasesDecoder.scopeEncodingOffset()) & 0xFF;
            final int result = frame.getByte(at + MeCommandReleaseDecoder.ReleasesDecoder.resultEncodingOffset()) & 0xFF;
            if (scope > 1 || result > 2) {
                rejectDecode("MeCommandRelease scope " + scope + " result " + result + " at " + i);
                return;
            }
        }
        unhandled(MeCommandReleaseDecoder.TEMPLATE_ID);
    }

    /** MeEngineConfig (68, BRIDGE): {@code decision 0..3}. Served by RC2 S7. */
    private void handleMeEngineConfig() {
        meEngineConfigDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int decision = meEngineConfigDecoder.decisionRaw();
        if (decision > 3) {
            rejectDecode("MeEngineConfig decision " + decision);
            return;
        }
        unhandled(MeEngineConfigDecoder.TEMPLATE_ID);
    }

    /** FeedLatchClear (76, ADMIN): {@code haltReason 1..5}. Served by RC2 S7. */
    private void handleFeedLatchClear() {
        feedLatchClearDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int reason = feedLatchClearDecoder.haltReasonRaw();
        if (reason < 1 || reason > 5) {
            rejectDecode("FeedLatchClear haltReason " + reason);
            return;
        }
        unhandled(FeedLatchClearDecoder.TEMPLATE_ID);
    }

    /** The frame's correlationId when its template starts with one, else 0. Frame already bounds-checked. */
    private long correlationIdOf(final int templateId) {
        return HAS_CORRELATION[templateId] ? frame.getLong(HEADER) : 0L;
    }

    /** The frame's userId for a user-scoped template, else 0. Offsets come from the generated codecs. */
    private long userIdOf(final int templateId) {
        switch (templateId) {
            case DepositDecoder.TEMPLATE_ID:  return frame.getLong(HEADER + DepositDecoder.userIdEncodingOffset());
            case WithdrawDecoder.TEMPLATE_ID: return frame.getLong(HEADER + WithdrawDecoder.userIdEncodingOffset());
            case HoldDecoder.TEMPLATE_ID:     return frame.getLong(HEADER + HoldDecoder.userIdEncodingOffset());
            case ReleaseDecoder.TEMPLATE_ID:  return frame.getLong(HEADER + ReleaseDecoder.userIdEncodingOffset());
            case CreateOrderDecoder.TEMPLATE_ID: return frame.getLong(HEADER + CreateOrderDecoder.userIdEncodingOffset());
            case CancelOrderDecoder.TEMPLATE_ID: return frame.getLong(HEADER + CancelOrderDecoder.userIdEncodingOffset());
            case AmendOrderDecoder.TEMPLATE_ID:  return frame.getLong(HEADER + AmendOrderDecoder.userIdEncodingOffset());
            case RequestOrderSnapshotDecoder.TEMPLATE_ID: return frame.getLong(HEADER + RequestOrderSnapshotDecoder.userIdEncodingOffset());
            default: return 0L;
        }
    }

    // ---- validity helpers (no allocation, no throw) ----

    private static boolean validAsset(final int assetId) {
        return assetId >= 0 && assetId < Asset.count();
    }

    /** A money boolean is 0 or 1. 255 (NULL_VAL) is not a value for money either. */
    private static boolean validBool(final int raw) {
        return raw == BoolFlag.FALSE.value() || raw == BoolFlag.TRUE.value();
    }

    /** CoreConfig's two consecutive groups (markets, then assets) both inside the frame. */
    private boolean coreConfigGroupsWithinFrame(final int marketsOffset, final int length) {
        if (!groupWithinFrame(marketsOffset, CoreConfigDecoder.MarketsDecoder.sbeBlockLength(), length)) {
            return false;
        }
        final int block = Short.toUnsignedInt(frame.getShort(marketsOffset, ByteOrder.LITTLE_ENDIAN));
        final int count = Short.toUnsignedInt(frame.getShort(marketsOffset + 2, ByteOrder.LITTLE_ENDIAN));
        final int assetsOffset = marketsOffset + GROUP_HEADER + block * count;
        return groupWithinFrame(assetsOffset, CoreConfigDecoder.AssetsDecoder.sbeBlockLength(), length);
    }

    /** Frozen v7 cap of MeCommandRelease (67) entries per frame (RC2 S1 4.3; keeps the journal frame unfragmented). */
    static final int RELEASE_GROUP_CAP = 48;

    /** MeCommandRelease's group inside the frame (as 62) and at most {@link #RELEASE_GROUP_CAP} entries. */
    private boolean releaseGroupWithinFrame(final int groupOffset, final int length) {
        if (!groupWithinFrame(groupOffset, MeCommandReleaseDecoder.ReleasesDecoder.sbeBlockLength(), length)) {
            return false;
        }
        return Short.toUnsignedInt(frame.getShort(groupOffset + 2, ByteOrder.LITTLE_ENDIAN)) <= RELEASE_GROUP_CAP;
    }

    /** A fixed-length SBE char field as a String up to the first NUL; null when a byte is not printable ASCII. */
    private static String fixedAscii(final java.util.function.IntUnaryOperator byteAt, final int length) {
        final StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            final int b = byteAt.applyAsInt(i) & 0xFF;
            if (b == 0) {
                break;
            }
            if (b < 0x21 || b > 0x7E) {
                return null;
            }
            sb.append((char) b);
        }
        return sb.toString();
    }

    /** One repeating group starting at {@code groupOffset}: header present, entry block sane, count fits. */
    private boolean groupWithinFrame(final int groupOffset, final int generatedEntryBlock, final int length) {
        if (length - groupOffset < GROUP_HEADER) {
            return false;
        }
        final int groupBlock = Short.toUnsignedInt(frame.getShort(groupOffset, ByteOrder.LITTLE_ENDIAN));
        final int count = Short.toUnsignedInt(frame.getShort(groupOffset + 2, ByteOrder.LITTLE_ENDIAN));
        return groupBlock >= generatedEntryBlock
                && (long) groupBlock * count <= length - groupOffset - GROUP_HEADER;
    }

    private static boolean shouldLogReject(final long tally) {
        return tally == 1 || tally % REJECT_LOG_INTERVAL == 0;
    }

    private void rejectFrameBounds() {
        final long n = ++decodeRejectCount;
        if (shouldLogReject(n)) {
            System.err.println("INGRESS DROP (frame bounds); decodeRejects=" + n);
        }
    }

    private void rejectDecode(final String what) {
        final long n = ++decodeRejectCount;
        if (shouldLogReject(n)) {
            System.err.println("INGRESS DROP (decode): templateId=" + headerDecoder.templateId()
                    + " " + what + "; decodeRejects=" + n);
        }
    }

    private void unhandled(final int templateId) {
        final long n = ++unhandledTemplateCount;
        if (shouldLogReject(n)) {
            System.err.println("INGRESS DROP (unhandled template): templateId=" + templateId + " is in money-schema v"
                    + EXPECTED_SCHEMA_VERSION + " but not served by this build; unhandledTemplates=" + n);
        }
    }

    // ---- scrapeable counters ----

    /** Frames dropped for a schemaId mismatch or a version outside the supported set, before any body decode. */
    public long schemaRejectCount() { return schemaRejectCount; }

    /** Frames dropped for frame/group bounds or an invalid enum/field byte. Expected under poison input. */
    public long decodeRejectCount() { return decodeRejectCount; }

    /** Apply faults. A nonzero count fences dispatch until process restart. */
    public long applyErrorCount() { return applyErrorCount; }

    /** Frames whose templateId this schema does not define. */
    public long unknownTemplateCount() { return unknownTemplateCount; }

    /** Frames whose templateId is in the schema but not served by this build (later RC1 slices). */
    public long unhandledTemplateCount() { return unhandledTemplateCount; }
}

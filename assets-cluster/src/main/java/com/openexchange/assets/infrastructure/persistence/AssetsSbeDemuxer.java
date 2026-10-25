// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.commands.DepositCommand;
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
import com.openexchange.assets.infrastructure.generated.HoldDecoder;
import com.openexchange.assets.infrastructure.generated.InitTradeHighWaterDecoder;
import com.openexchange.assets.infrastructure.generated.MarketTickDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder;
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
     * The wire identity every ingress frame must carry. The version gate is EXACT (critic D12): the core
     * speaks v6 from RC1 on; a v5 producer (the v0.5.0-beta bridge and tools) is dropped and counted
     * rather than decoded with its {@code sinceVersion="6"} fields silently absent. A later additive
     * bump turns this into a range again, with the ME's rule: the floor advances only when pre-floor
     * producers and pre-floor frames in any replayable log are provably gone.
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
    private long unhandledTemplateCount; // in the schema, not served by this build (v6 order/feed/admin, RC1 S4+)
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
            default: return -1;
        }
    }

    public void dispatch(final DirectBuffer buffer, final int offset, final int length, final long timestamp) {
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

        // 5. Body decode and apply. Decode poison (IAE/IOOBE before any application callback) is dropped
        //    deterministically; any failure after `applying = true` latches and escapes to the service.
        try {
            switch (templateId) {
                case DepositDecoder.TEMPLATE_ID:                handleDeposit(timestamp); break;
                case WithdrawDecoder.TEMPLATE_ID:               handleWithdraw(timestamp); break;
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
                case SubscribeDecoder.TEMPLATE_ID:
                    // Transport-level; consumed at the service boundary before dispatch. Reaching
                    // here means the service peek declined it (bad length/version): drop, count.
                    rejectDecode("Subscribe frame reached the engine");
                    break;
                default:
                    // v6 order / feed / admin / session templates: defined by this schema, served from
                    // RC1 S3 (RegisterSession, OrderEntryGate) and S4+ (orders, feed, config). Counted
                    // so a gateway speaking ahead of this build is visible, never silent.
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

    private void handleDeposit(long timestamp) {
        depositDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int assetId = depositDecoder.assetId();
        if (!validAsset(assetId)) {
            rejectDecode("Deposit assetId " + assetId);
            return;
        }
        depositCommand.reset();
        depositCommand.setCorrelationId(depositDecoder.correlationId());
        depositCommand.setUserId(depositDecoder.userId());
        depositCommand.setAssetId(assetId);
        depositCommand.setAmount(depositDecoder.amount());
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, depositCommand, timestamp);
    }

    private void handleWithdraw(long timestamp) {
        withdrawDecoder.wrapAndApplyHeader(frame, 0, headerDecoder);
        final int assetId = withdrawDecoder.assetId();
        if (!validAsset(assetId)) {
            rejectDecode("Withdraw assetId " + assetId);
            return;
        }
        withdrawCommand.reset();
        withdrawCommand.setCorrelationId(withdrawDecoder.correlationId());
        withdrawCommand.setUserId(withdrawDecoder.userId());
        withdrawCommand.setAssetId(assetId);
        withdrawCommand.setAmount(withdrawDecoder.amount());
        applying = true;
        engine.applyCommand(AssetsEngine.CMD_WITHDRAW, withdrawCommand, timestamp);
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

    // ---- validity helpers (no allocation, no throw) ----

    private static boolean validAsset(final int assetId) {
        return assetId >= 0 && assetId < Asset.count();
    }

    /** A money boolean is 0 or 1. 255 (NULL_VAL) is not a value for money either. */
    private static boolean validBool(final int raw) {
        return raw == BoolFlag.FALSE.value() || raw == BoolFlag.TRUE.value();
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
            System.err.println("INGRESS DROP (unhandled v6 template): templateId=" + templateId
                    + " is in money-schema v6 but not served by this build; unhandledTemplates=" + n);
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

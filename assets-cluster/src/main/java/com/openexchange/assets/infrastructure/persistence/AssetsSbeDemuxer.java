// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.InitTradeHighWaterCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.HoldDecoder;
import com.openexchange.assets.infrastructure.generated.InitTradeHighWaterDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.assets.infrastructure.generated.ReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.SettleDecoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;
import org.agrona.DirectBuffer;

/**
 * Inbound adapter: decodes an SBE ingress message into a pooled domain command and drives the
 * {@link AssetsEngine}. Zero allocation, zero string parsing on the hot path — decoders and command
 * objects are reused. The domain never sees a byte buffer; it only ever receives commands.
 */
public final class AssetsSbeDemuxer {

    private final AssetsEngine engine;
    private final SettlementProjector projector;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    private final DepositDecoder depositDecoder = new DepositDecoder();
    private final WithdrawDecoder withdrawDecoder = new WithdrawDecoder();
    private final HoldDecoder holdDecoder = new HoldDecoder();
    private final ReleaseDecoder releaseDecoder = new ReleaseDecoder();
    private final SettleDecoder settleDecoder = new SettleDecoder();
    private final TerminalReleaseDecoder terminalReleaseDecoder = new TerminalReleaseDecoder();
    private final QueryFeedPositionDecoder queryFeedPositionDecoder = new QueryFeedPositionDecoder();
    private final InitTradeHighWaterDecoder initHighWaterDecoder = new InitTradeHighWaterDecoder();
    private final RequestBalanceSnapshotDecoder balanceSnapshotDecoder = new RequestBalanceSnapshotDecoder();
    private final RequestHoldSnapshotDecoder holdSnapshotDecoder = new RequestHoldSnapshotDecoder();

    private final DepositCommand depositCommand = new DepositCommand();
    private final WithdrawCommand withdrawCommand = new WithdrawCommand();
    private final HoldCommand holdCommand = new HoldCommand();
    private final ReleaseCommand releaseCommand = new ReleaseCommand();
    private final InitTradeHighWaterCommand initHighWaterCommand = new InitTradeHighWaterCommand();

    public AssetsSbeDemuxer(AssetsEngine engine, SettlementProjector projector) {
        this.engine = engine;
        this.projector = projector;
    }

    public void dispatch(final DirectBuffer buffer, final int offset, final int length, final long timestamp) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        headerDecoder.wrap(buffer, offset);

        switch (headerDecoder.templateId()) {
            case DepositDecoder.TEMPLATE_ID:  handleDeposit(buffer, offset, timestamp); break;
            case WithdrawDecoder.TEMPLATE_ID: handleWithdraw(buffer, offset, timestamp); break;
            case HoldDecoder.TEMPLATE_ID:     handleHold(buffer, offset, timestamp); break;
            case ReleaseDecoder.TEMPLATE_ID:  handleRelease(buffer, offset, timestamp); break;
            case SettleDecoder.TEMPLATE_ID:   handleSettle(buffer, offset, timestamp); break;
            case TerminalReleaseDecoder.TEMPLATE_ID:        handleTerminalRelease(buffer, offset, timestamp); break;
            case QueryFeedPositionDecoder.TEMPLATE_ID:      handleQueryFeedPosition(buffer, offset); break;
            case InitTradeHighWaterDecoder.TEMPLATE_ID:     handleInitHighWater(buffer, offset, timestamp); break;
            case RequestBalanceSnapshotDecoder.TEMPLATE_ID: handleRequestBalanceSnapshot(buffer, offset); break;
            case RequestHoldSnapshotDecoder.TEMPLATE_ID:    handleRequestHoldSnapshot(buffer, offset); break;
            default: break; // unknown message — ignore on the hot path
        }
    }

    // Decoders read each field at its own fixed SBE offset, so read order is not load-bearing; we still
    // decode in the v2 schema field order (correlationId first on Deposit/Withdraw/Hold) for clarity.
    private void handleDeposit(DirectBuffer buffer, int offset, long timestamp) {
        depositDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        depositCommand.reset();
        depositCommand.setCorrelationId(depositDecoder.correlationId());
        depositCommand.setUserId(depositDecoder.userId());
        depositCommand.setAssetId(depositDecoder.assetId());
        depositCommand.setAmount(depositDecoder.amount());
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, depositCommand, timestamp);
    }

    private void handleWithdraw(DirectBuffer buffer, int offset, long timestamp) {
        withdrawDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        withdrawCommand.reset();
        withdrawCommand.setCorrelationId(withdrawDecoder.correlationId());
        withdrawCommand.setUserId(withdrawDecoder.userId());
        withdrawCommand.setAssetId(withdrawDecoder.assetId());
        withdrawCommand.setAmount(withdrawDecoder.amount());
        engine.applyCommand(AssetsEngine.CMD_WITHDRAW, withdrawCommand, timestamp);
    }

    private void handleHold(DirectBuffer buffer, int offset, long timestamp) {
        holdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        holdCommand.reset();
        holdCommand.setCorrelationId(holdDecoder.correlationId());
        holdCommand.setOrderId(holdDecoder.orderId());
        holdCommand.setUserId(holdDecoder.userId());
        holdCommand.setAssetId(holdDecoder.assetId());
        holdCommand.setAmount(holdDecoder.amount());
        holdCommand.setOmsManagedRelease(holdDecoder.omsManagedRelease() == BoolFlag.TRUE);
        engine.applyCommand(AssetsEngine.CMD_HOLD, holdCommand, timestamp);
    }

    private void handleRelease(DirectBuffer buffer, int offset, long timestamp) {
        releaseDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        releaseCommand.reset();
        releaseCommand.setOrderId(releaseDecoder.orderId());
        releaseCommand.setUserId(releaseDecoder.userId());
        releaseCommand.setAmount(releaseDecoder.amount());
        engine.applyCommand(AssetsEngine.CMD_RELEASE, releaseCommand, timestamp);
    }

    /**
     * All settles route through the {@link SettlementProjector} so the journal cursor
     * (consumePosition) advances atomically with the money mutation. Direct ingress
     * (tests/tools) carries journalPosition=0, which never advances the cursor.
     */
    private void handleSettle(DirectBuffer buffer, int offset, long timestamp) {
        settleDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
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
                settleDecoder.takerIsBuy() == BoolFlag.TRUE,
                timestamp);
    }

    /** Feed-forward terminal from the ME journal: release the order's full residual hold. */
    private void handleTerminalRelease(DirectBuffer buffer, int offset, long timestamp) {
        terminalReleaseDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        projector.onTerminal(
                terminalReleaseDecoder.journalPosition(),
                terminalReleaseDecoder.orderId(),
                terminalReleaseDecoder.userId(),
                timestamp);
    }

    /** Read-only: the engine answers with a FeedPositionReport through the sink. */
    private void handleQueryFeedPosition(DirectBuffer buffer, int offset) {
        queryFeedPositionDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        engine.reportFeedPosition(queryFeedPositionDecoder.correlationId());
    }

    private void handleInitHighWater(DirectBuffer buffer, int offset, long timestamp) {
        initHighWaterDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        initHighWaterCommand.reset();
        initHighWaterCommand.setTradeId(initHighWaterDecoder.tradeId());
        initHighWaterCommand.setConsumePosition(initHighWaterDecoder.consumePosition());
        engine.applyCommand(AssetsEngine.CMD_INIT_HIGH_WATER, initHighWaterCommand, timestamp);
    }

    // Read-only queries: no domain command, no timestamp; the engine streams the answer via the sink.
    private void handleRequestBalanceSnapshot(DirectBuffer buffer, int offset) {
        balanceSnapshotDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        engine.requestBalanceSnapshot(balanceSnapshotDecoder.correlationId());
    }

    private void handleRequestHoldSnapshot(DirectBuffer buffer, int offset) {
        holdSnapshotDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        engine.requestHoldSnapshot(holdSnapshotDecoder.correlationId());
    }
}

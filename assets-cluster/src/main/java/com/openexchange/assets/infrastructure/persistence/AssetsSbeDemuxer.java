// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.HoldDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.ReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.SettleDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;
import org.agrona.DirectBuffer;

/**
 * Inbound adapter: decodes an SBE ingress message into a pooled domain command and drives the
 * {@link AssetsEngine}. Zero allocation, zero string parsing on the hot path — decoders and command
 * objects are reused. The domain never sees a byte buffer; it only ever receives commands.
 */
public final class AssetsSbeDemuxer {

    private final AssetsEngine engine;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    private final DepositDecoder depositDecoder = new DepositDecoder();
    private final WithdrawDecoder withdrawDecoder = new WithdrawDecoder();
    private final HoldDecoder holdDecoder = new HoldDecoder();
    private final ReleaseDecoder releaseDecoder = new ReleaseDecoder();
    private final SettleDecoder settleDecoder = new SettleDecoder();

    private final DepositCommand depositCommand = new DepositCommand();
    private final WithdrawCommand withdrawCommand = new WithdrawCommand();
    private final HoldCommand holdCommand = new HoldCommand();
    private final ReleaseCommand releaseCommand = new ReleaseCommand();
    private final SettleCommand settleCommand = new SettleCommand();

    public AssetsSbeDemuxer(AssetsEngine engine) {
        this.engine = engine;
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
            default: break; // unknown message — ignore on the hot path
        }
    }

    private void handleDeposit(DirectBuffer buffer, int offset, long timestamp) {
        depositDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        depositCommand.reset();
        depositCommand.setUserId(depositDecoder.userId());
        depositCommand.setAssetId(depositDecoder.assetId());
        depositCommand.setAmount(depositDecoder.amount());
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, depositCommand, timestamp);
    }

    private void handleWithdraw(DirectBuffer buffer, int offset, long timestamp) {
        withdrawDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        withdrawCommand.reset();
        withdrawCommand.setUserId(withdrawDecoder.userId());
        withdrawCommand.setAssetId(withdrawDecoder.assetId());
        withdrawCommand.setAmount(withdrawDecoder.amount());
        engine.applyCommand(AssetsEngine.CMD_WITHDRAW, withdrawCommand, timestamp);
    }

    private void handleHold(DirectBuffer buffer, int offset, long timestamp) {
        holdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        holdCommand.reset();
        holdCommand.setOrderId(holdDecoder.orderId());
        holdCommand.setUserId(holdDecoder.userId());
        holdCommand.setAssetId(holdDecoder.assetId());
        holdCommand.setAmount(holdDecoder.amount());
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

    private void handleSettle(DirectBuffer buffer, int offset, long timestamp) {
        settleDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        settleCommand.reset();
        settleCommand.setTradeId(settleDecoder.tradeId());
        settleCommand.setMarketId(settleDecoder.marketId());
        settleCommand.setTakerOrderId(settleDecoder.takerOrderId());
        settleCommand.setTakerUserId(settleDecoder.takerUserId());
        settleCommand.setMakerOrderId(settleDecoder.makerOrderId());
        settleCommand.setMakerUserId(settleDecoder.makerUserId());
        settleCommand.setPrice(settleDecoder.price());
        settleCommand.setQuantity(settleDecoder.quantity());
        settleCommand.setTakerIsBuy(settleDecoder.takerIsBuy() == BoolFlag.TRUE);
        engine.applyCommand(AssetsEngine.CMD_SETTLE, settleCommand, timestamp);
    }
}

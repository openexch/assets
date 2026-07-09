// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.projection;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SettleCommand;

/**
 * The settlement projector — the anti-corruption layer that turns the matching engine's recorded
 * trade + terminal-status egress into money-domain SETTLE/RELEASE, feed-forward, off the OMS hot path.
 * This is what structurally removes the MONEY-A bug class: the OMS is never in the money path after
 * submit, so it cannot "miss a trade".
 *
 * <p>Two feed events, each carrying its position in the ME recorded stream:</p>
 * <ul>
 *   <li><b>{@link #onTrade}</b> — a {@code TradeExecution}: buyer/seller are derived from
 *       {@code takerIsBuy}; base/quote and amounts are derived from the market and price×qty inside the
 *       engine. Idempotent on {@code tradeId} (the engine's monotonic high-water), so a re-delivered
 *       trade never double-settles.</li>
 *   <li><b>{@link #onTerminal}</b> — a terminal {@code OrderStatus} (FILLED/CANCELLED/REJECTED):
 *       releases the order's residual hold. Idempotent because releasing a gone/empty hold is a no-op.</li>
 * </ul>
 *
 * <p>Each applied event advances the engine's {@code consumePosition} <b>monotonically</b>, snapshotted
 * atomically with the balances it produced — so on restart the AE replays the ME archive from
 * {@code consumePosition + 1} (ReplayMerge) and converges with no skew and no double-application.</p>
 *
 * <p>Runs on the AE leader, which submits these into the AE's own replicated log so followers apply
 * them deterministically. Phase 1 proves the translation + idempotency + replay-gap recovery in-process
 * against a replayed trade tape; the live Aeron archive subscription lands in Phase 2.</p>
 */
public final class SettlementProjector {

    private final AssetsEngine engine;
    private final SettleCommand settleCommand = new SettleCommand();
    private final ReleaseCommand releaseCommand = new ReleaseCommand();

    public SettlementProjector(AssetsEngine engine) {
        this.engine = engine;
    }

    /** A {@code TradeExecution} consumed from the ME recorded stream at {@code streamPosition}. */
    public void onTrade(long streamPosition, long tradeId, int marketId,
                        long takerOrderId, long takerUserId, long makerOrderId, long makerUserId,
                        long price, long quantity, boolean takerIsBuy, long timestamp) {
        settleCommand.reset();
        settleCommand.setTradeId(tradeId);
        settleCommand.setMarketId(marketId);
        settleCommand.setTakerOrderId(takerOrderId);
        settleCommand.setTakerUserId(takerUserId);
        settleCommand.setMakerOrderId(makerOrderId);
        settleCommand.setMakerUserId(makerUserId);
        settleCommand.setPrice(price);
        settleCommand.setQuantity(quantity);
        settleCommand.setTakerIsBuy(takerIsBuy);
        engine.applyCommand(AssetsEngine.CMD_SETTLE, settleCommand, timestamp);
        advance(streamPosition);
    }

    /** A terminal {@code OrderStatus} for {@code orderId} consumed at {@code streamPosition}. */
    public void onTerminal(long streamPosition, long orderId, long userId, long timestamp) {
        releaseCommand.reset();
        releaseCommand.setOrderId(orderId);
        releaseCommand.setUserId(userId);
        releaseCommand.setAmount(-1L); // release the full residual reservation
        engine.applyCommand(AssetsEngine.CMD_RELEASE, releaseCommand, timestamp);
        advance(streamPosition);
    }

    /** How far into the ME recorded stream this projector has applied. */
    public long consumePosition() {
        return engine.getConsumePosition();
    }

    /** consumePosition only ever moves forward — a real ReplayMerge starts at consumePosition+1. */
    private void advance(long streamPosition) {
        if (streamPosition > engine.getConsumePosition()) {
            engine.setConsumePosition(streamPosition);
        }
    }
}

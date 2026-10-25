// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.publisher;

import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreOrderView;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.infrastructure.generated.AdminAckEncoder;
import com.openexchange.assets.infrastructure.generated.ConfigAppliedEncoder;
import com.openexchange.assets.infrastructure.generated.DepositRejectEncoder;
import com.openexchange.assets.infrastructure.generated.MeDispatchEncoder;
import com.openexchange.assets.infrastructure.generated.OrderAckEncoder;
import com.openexchange.assets.infrastructure.generated.OrderEventEncoder;
import com.openexchange.assets.infrastructure.generated.AdmissionStateEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.OrderRejectEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateEncoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositAckEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckEncoder;
import com.openexchange.assets.infrastructure.generated.HoldRejectEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SettleFaultEncoder;
import com.openexchange.assets.infrastructure.generated.SettleRejectedEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawAckEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawRejectEncoder;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.function.BooleanSupplier;

/**
 * Outbound adapter: implements the domain {@link AssetsEventSink} port by encoding each event to SBE
 * and handing the bytes to an {@link Egress} transport. The engine emits money events through the port
 * with no knowledge of SBE or Aeron; this adapter is the only place the wire format lives.
 *
 * <p>Single-threaded (the cluster service thread); one reusable buffer, no per-event allocation. Money
 * egress is <b>reliable</b> — it must never be shed — so {@link Egress} is expected to apply
 * backpressure rather than drop.</p>
 *
 * <p><b>Routing.</b> Each flush also names its {@link Routing}: request-scoped events (acks, rejects,
 * query replies) are {@code ORIGIN} — intended only for the session whose command produced them —
 * while genuinely shared streams (live balance updates, settlements) are {@code BROADCAST}. The
 * classification lives here, next to the channel classification, because this is the only class that
 * knows which encoder it just used; the publisher stays transport-free — it signals <em>intent</em>,
 * and the transport resolves which session the origin actually is. The one context-dependent method is
 * {@link #onBalanceUpdate}: a balance-snapshot reply streams its entries through it (see
 * {@code SessionEgressQueue#subscribe}), so it asks {@link #queryReplyContext} — query commands mutate
 * nothing and emit only reply frames, so while one is being dispatched every emission is
 * origin-scoped.</p>
 */
public final class AssetsEventPublisher implements AssetsEventSink {

    /** Who a frame is for. Transport-side only: it decides which queue a frame lands in, never bytes. */
    public enum Routing {
        /** Request-scoped: deliver only to the session whose command is currently being applied. */
        ORIGIN,
        /** Shared stream: deliver to every session subscribed to the frame's channel. */
        BROADCAST
    }

    /** The reliable egress transport (a positioned, flow-controlled channel — never lossy). */
    public interface Egress {
        /**
         * @param channel one of the {@code CH_*} constants — which class of egress this frame is, so the
         *                transport can skip sessions that did not subscribe to it
         * @param routing {@link Routing#ORIGIN} for request-scoped frames (only the commanding session),
         *                {@link Routing#BROADCAST} for shared streams (every subscribed session)
         */
        void emit(MutableDirectBuffer buffer, int offset, int length, int channel, Routing routing);
    }

    /**
     * Optional second sink for LIVE balance updates only — the balance-feed side-channel's intake.
     * Called from {@link #onBalanceUpdate} precisely when the emission is a live ({@code BROADCAST})
     * update, never for snapshot-reply entries: a query reply is one session's answer, not a change.
     * Decoded values rather than bytes, because the tap's consumer conflates by (userId, assetId)
     * and re-encodes on its own thread. Like {@link Egress}, this is a signal — the publisher stays
     * transport-free; wiring lives at the service/runtime boundary. Runs during apply on EVERY node
     * (followers too), deliberately: the feed's store stays current everywhere, so a follower that
     * gains leadership can replay it as a feed baseline.
     */
    public interface LiveBalanceTap {
        LiveBalanceTap NONE = (userId, assetId, available, locked) -> { };

        void onLiveBalance(long userId, int assetId, long available, long locked);
    }

    // Egress channels. These mirror the EgressChannels bit set in money-schema.xml and are the unit a
    // session subscribes to. Classification lives here because this is the only class that knows which
    // encoder it just used; the engine stays free of transport concepts.
    /** Responses to the client's own commands: acks, rejects, feed position. */
    public static final int CH_ACKS = 1;
    /** The BalanceUpdate firehose: one frame per changed (user, asset) line. */
    public static final int CH_BALANCES = 1 << 1;
    /** SettlementApplied and SettleFault. */
    public static final int CH_SETTLEMENTS = 1 << 2;
    /** Snapshot streams and their terminators. */
    public static final int CH_SNAPSHOTS = 1 << 3;
    /** v6: OrderEvent / OrderEventBatch (gateways). */
    public static final int CH_ORDERS = 1 << 4;
    /** v6: MeDispatch / MeDispatchBatch (forwarder sessions). */
    public static final int CH_DISPATCH = 1 << 5;
    /** v6: AdmissionState (gate and breaker changes). */
    public static final int CH_CONTROL = 1 << 6;
    /**
     * What a session gets when it never subscribes or registers: the four pre-v6 channels. The three
     * v6 streams are opt-in (RegisterSession), so a v5-era tool never receives the order firehose.
     */
    public static final int CH_ALL = CH_ACKS | CH_BALANCES | CH_SETTLEMENTS | CH_SNAPSHOTS;

    private static final int BUFFER_CAPACITY = 256;

    private final Egress egress;
    /**
     * Whether a read-only query command is currently being dispatched (see the class doc): true makes
     * {@link #onBalanceUpdate} emissions snapshot-reply entries ({@code ORIGIN}) instead of live
     * updates ({@code BROADCAST}). Supplied by the transport boundary, which owns the dispatch.
     */
    private final BooleanSupplier queryReplyContext;
    private final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[BUFFER_CAPACITY]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

    private final DepositAckEncoder depositAckEncoder = new DepositAckEncoder();
    private final WithdrawAckEncoder withdrawAckEncoder = new WithdrawAckEncoder();
    private final HoldAckEncoder holdAckEncoder = new HoldAckEncoder();
    private final HoldRejectEncoder holdRejectEncoder = new HoldRejectEncoder();
    private final BalanceUpdateEncoder balanceUpdateEncoder = new BalanceUpdateEncoder();
    private final SettlementAppliedEncoder settlementAppliedEncoder = new SettlementAppliedEncoder();
    private final WithdrawRejectEncoder withdrawRejectEncoder = new WithdrawRejectEncoder();
    private final BalanceSnapshotEndEncoder balanceSnapshotEndEncoder = new BalanceSnapshotEndEncoder();
    private final FeedPositionReportEncoder feedPositionReportEncoder = new FeedPositionReportEncoder();
    private final SettleFaultEncoder settleFaultEncoder = new SettleFaultEncoder();
    private final SettleRejectedEncoder settleRejectedEncoder = new SettleRejectedEncoder();
    private final OrderRejectEncoder orderRejectEncoder = new OrderRejectEncoder();
    private final AdminAckEncoder adminAckEncoder = new AdminAckEncoder();
    private final AdmissionStateEncoder admissionStateEncoder = new AdmissionStateEncoder();
    private final OrderAckEncoder orderAckEncoder = new OrderAckEncoder();
    private final OrderEventEncoder orderEventEncoder = new OrderEventEncoder();
    private final MeDispatchEncoder meDispatchEncoder = new MeDispatchEncoder();
    private final DepositRejectEncoder depositRejectEncoder = new DepositRejectEncoder();
    private final ConfigAppliedEncoder configAppliedEncoder = new ConfigAppliedEncoder();
    private final HoldSnapshotEntryEncoder holdSnapshotEntryEncoder = new HoldSnapshotEntryEncoder();
    private final HoldSnapshotEndEncoder holdSnapshotEndEncoder = new HoldSnapshotEndEncoder();

    /** See {@link LiveBalanceTap}: fed live balance updates; {@code NONE} when no feed is wired. */
    private final LiveBalanceTap liveBalanceTap;

    public AssetsEventPublisher(Egress egress, BooleanSupplier queryReplyContext) {
        this(egress, queryReplyContext, LiveBalanceTap.NONE);
    }

    public AssetsEventPublisher(Egress egress, BooleanSupplier queryReplyContext, LiveBalanceTap liveBalanceTap) {
        this.egress = egress;
        this.queryReplyContext = queryReplyContext;
        this.liveBalanceTap = liveBalanceTap;
    }

    @Override
    public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) {
        depositAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount).newAvailable(newAvailable)
                .duplicate(duplicate ? BoolFlag.TRUE : BoolFlag.FALSE);
        flush(depositAckEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable, boolean duplicate) {
        withdrawAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount).newAvailable(newAvailable)
                .duplicate(duplicate ? BoolFlag.TRUE : BoolFlag.FALSE);
        flush(withdrawAckEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) {
        holdAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).orderId(orderId).userId(userId).assetId(assetId).amount(amount);
        flush(holdAckEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason) {
        holdRejectEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).orderId(orderId).userId(userId).assetId(assetId)
                .amount(amount).reason(toSbe(reason));
        flush(holdRejectEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    /**
     * Live updates are the shared firehose ({@code BROADCAST}); the same frames streamed as a
     * balance-snapshot reply are for the asker only ({@code ORIGIN}). {@link #queryReplyContext}
     * separates the two — see the class doc. Only the live branch feeds {@link #liveBalanceTap}:
     * snapshot-reply entries must never enter the side-channel feed.
     */
    @Override
    public void onBalanceUpdate(long userId, int assetId, long available, long locked) {
        final boolean queryReply = queryReplyContext.getAsBoolean();
        if (!queryReply) {
            liveBalanceTap.onLiveBalance(userId, assetId, available, locked);
        }
        balanceUpdateEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .userId(userId).assetId(assetId).available(available).locked(locked);
        flush(balanceUpdateEncoder.encodedLength(), CH_BALANCES,
                queryReply ? Routing.ORIGIN : Routing.BROADCAST);
    }

    @Override
    public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) {
        settlementAppliedEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .tradeId(tradeId).buyerUserId(buyerUserId).sellerUserId(sellerUserId);
        flush(settlementAppliedEncoder.encodedLength(), CH_SETTLEMENTS, Routing.BROADCAST);
    }

    @Override
    public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) {
        withdrawRejectEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount).reason(toSbe(reason));
        flush(withdrawRejectEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onBalanceSnapshotEnd(long correlationId, int entryCount) {
        balanceSnapshotEndEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).entryCount(entryCount);
        flush(balanceSnapshotEndEncoder.encodedLength(), CH_SNAPSHOTS, Routing.ORIGIN);
    }

    @Override
    public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) {
        holdSnapshotEntryEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .orderId(orderId).userId(userId).assetId(assetId).remaining(remaining);
        flush(holdSnapshotEntryEncoder.encodedLength(), CH_SNAPSHOTS, Routing.ORIGIN);
    }

    @Override
    public void onHoldSnapshotEnd(long correlationId, int entryCount) {
        holdSnapshotEndEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).entryCount(entryCount);
        flush(holdSnapshotEndEncoder.encodedLength(), CH_SNAPSHOTS, Routing.ORIGIN);
    }

    @Override
    public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId,
                                     boolean journalEnabled) {
        feedPositionReportEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).consumePosition(consumePosition)
                .lastAppliedTradeId(lastAppliedTradeId)
                .journalEnabled(journalEnabled ? BoolFlag.TRUE : BoolFlag.FALSE);
        flush(feedPositionReportEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onSettleFault(long tradeId, long orderId, long userId, int assetId,
                              long drawnFromAvailable, long uncovered) {
        settleFaultEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .tradeId(tradeId).orderId(orderId).userId(userId).assetId(assetId)
                .drawnFromAvailable(drawnFromAvailable).uncovered(uncovered);
        flush(settleFaultEncoder.encodedLength(), CH_SETTLEMENTS, Routing.BROADCAST);
    }

    @Override
    public void onSettleRejected(long tradeId, int marketId, long journalPosition, SettleRejectReason reason, long detail) {
        settleRejectedEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .tradeId(tradeId).marketId(marketId).journalPosition(journalPosition)
                .reason(toWire(reason)).detail(detail);
        // ORIGIN: the feed session that delivered the refused trade is the one that has to act on it.
        flush(settleRejectedEncoder.encodedLength(), CH_SETTLEMENTS, Routing.ORIGIN);
    }

    private static com.openexchange.assets.infrastructure.generated.SettleRejectReason toWire(SettleRejectReason r) {
        switch (r) {
            case UNKNOWN_MARKET:    return com.openexchange.assets.infrastructure.generated.SettleRejectReason.UNKNOWN_MARKET;
            case INVALID_PRICE:     return com.openexchange.assets.infrastructure.generated.SettleRejectReason.INVALID_PRICE;
            case INVALID_QUANTITY:  return com.openexchange.assets.infrastructure.generated.SettleRejectReason.INVALID_QUANTITY;
            case NOTIONAL_OVERFLOW: return com.openexchange.assets.infrastructure.generated.SettleRejectReason.NOTIONAL_OVERFLOW;
            case INVALID_PARTY:     return com.openexchange.assets.infrastructure.generated.SettleRejectReason.INVALID_PARTY;
            default: throw new IllegalArgumentException("unmapped SettleRejectReason " + r);
        }
    }

    @Override
    public void onOrderReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, long orderId,
                              int marketId, int refTemplateId, CoreRejectReason reason, int meReason, long detail,
                              long rejectedAtMs) {
        orderRejectEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).requestIdHigh(requestIdHigh).requestIdLow(requestIdLow)
                .userId(userId).orderId(orderId).marketId(marketId).refTemplateId(refTemplateId)
                .reason(com.openexchange.assets.infrastructure.generated.CoreRejectReason.get((short) reason.code()))
                .meReason((short) meReason).detail(detail).rejectedAtMs(rejectedAtMs);
        flush(orderRejectEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onAdminAck(long correlationId, int refTemplateId, AdminResult result, int reason, long configVersion) {
        adminAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).refTemplateId(refTemplateId)
                .result(com.openexchange.assets.infrastructure.generated.AdminResult.get((short) result.code()))
                .reason((short) reason).configVersion(configVersion);
        flush(adminAckEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onAdmissionState(int marketId, GateState gateState, GateReason gateReason, boolean circuitBreaker,
                                 long atMs, long logPosition, boolean toOrigin) {
        admissionStateEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .marketId(marketId)
                .gateState(com.openexchange.assets.infrastructure.generated.GateState.get((short) gateState.code()))
                .gateReason(com.openexchange.assets.infrastructure.generated.GateReason.get((short) gateReason.code()))
                .circuitBreaker(circuitBreaker ? BoolFlag.TRUE : BoolFlag.FALSE)
                .atMs(atMs).logPosition(logPosition);
        flush(admissionStateEncoder.encodedLength(), CH_CONTROL, toOrigin ? Routing.ORIGIN : Routing.BROADCAST);
    }

    @Override
    public void onOrderAck(long correlationId, long requestIdHigh, long requestIdLow, long orderId, long userId, int marketId,
                           OrderStatus status, boolean duplicate, int holdAssetId, long holdAmount, long acceptedAtMs, long eventSeq) {
        orderAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).requestIdHigh(requestIdHigh).requestIdLow(requestIdLow)
                .orderId(orderId).userId(userId).marketId(marketId)
                .status(com.openexchange.assets.infrastructure.generated.OrderStatus.get((short) status.code()))
                .duplicate(duplicate ? BoolFlag.TRUE : BoolFlag.FALSE)
                .holdAssetId(holdAssetId).holdAmount(holdAmount).acceptedAtMs(acceptedAtMs).eventSeq(eventSeq);
        flush(orderAckEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onOrderEvent(long eventSeq, long logPosition, CoreOrderView o, EventKind kind, CoreRejectReason reason,
                             long lastPrice, long lastQty, long tradeId, long holdRemaining, long updatedAtMs) {
        orderEventEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .eventSeq(eventSeq).logPosition(logPosition).orderId(o.orderId()).userId(o.userId()).marketId(o.marketId())
                .eventKind(com.openexchange.assets.infrastructure.generated.EventKind.get((short) kind.code()))
                .status(com.openexchange.assets.infrastructure.generated.OrderStatus.get((short) o.statusCode()))
                .reason(com.openexchange.assets.infrastructure.generated.CoreRejectReason.get((short) reason.code()))
                .side(com.openexchange.assets.infrastructure.generated.Side.get((short) o.sideCode()))
                .orderType(com.openexchange.assets.infrastructure.generated.OrderType.get((short) o.orderTypeCode()))
                .timeInForce(com.openexchange.assets.infrastructure.generated.TimeInForce.get((short) o.timeInForceCode()))
                .price(o.price()).stopPrice(o.stopPrice()).quantity(o.quantity()).filledQty(o.filledQty())
                .remainingQty(o.remainingQty()).lastPrice(lastPrice).lastQty(lastQty).tradeId(tradeId)
                .meOrderId(o.meOrderId()).holdAssetId(o.holdAssetId()).holdAmount(o.holdAmount()).holdRemaining(holdRemaining)
                .clientOrderIdHigh(o.clientOrderIdHigh()).clientOrderIdLow(o.clientOrderIdLow())
                .createdAtMs(o.createdAtMs()).updatedAtMs(updatedAtMs);
        flush(orderEventEncoder.encodedLength(), CH_ORDERS, Routing.BROADCAST);
    }

    @Override
    public void onMeDispatch(long dispatchSeq, long commandIdHigh, long commandIdLow, DispatchKind kind, long userId,
                             long omsOrderId, long oldOrderId, int marketId, int orderTypeCode, int sideCode, long price,
                             long quantity, long budget, int timeInForceCode) {
        meDispatchEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .dispatchSeq(dispatchSeq).commandIdHigh(commandIdHigh).commandIdLow(commandIdLow)
                .kind(com.openexchange.assets.infrastructure.generated.DispatchKind.get((short) kind.code()))
                .userId(userId).omsOrderId(omsOrderId).oldOrderId(oldOrderId).marketId(marketId)
                .orderType((short) orderTypeCode).side((short) sideCode).price(price).quantity(quantity).budget(budget)
                .timeInForce((short) timeInForceCode);
        flush(meDispatchEncoder.encodedLength(), CH_DISPATCH, Routing.BROADCAST);
    }

    @Override
    public void onDepositReject(long correlationId, long requestIdHigh, long requestIdLow, long userId, int assetId, long amount,
                                RejectReason reason) {
        depositRejectEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).requestIdHigh(requestIdHigh).requestIdLow(requestIdLow)
                .userId(userId).assetId(assetId).amount(amount).reason(toSbe(reason));
        flush(depositRejectEncoder.encodedLength(), CH_ACKS, Routing.ORIGIN);
    }

    @Override
    public void onConfigApplied(ConfigKind kind, int marketId, long configVersion, long logPosition) {
        configAppliedEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .kind(com.openexchange.assets.infrastructure.generated.ConfigKind.get((short) kind.code()))
                .marketId(marketId).configVersion(configVersion).logPosition(logPosition);
        flush(configAppliedEncoder.encodedLength(), CH_CONTROL, Routing.BROADCAST);
    }

    private void flush(int bodyLength, int channel, Routing routing) {
        egress.emit(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + bodyLength, channel, routing);
    }

    private static com.openexchange.assets.infrastructure.generated.RejectReason toSbe(RejectReason r) {
        switch (r) {
            case NONE:               return com.openexchange.assets.infrastructure.generated.RejectReason.NONE;
            case INSUFFICIENT_FUNDS: return com.openexchange.assets.infrastructure.generated.RejectReason.INSUFFICIENT_FUNDS;
            case INVALID_AMOUNT:     return com.openexchange.assets.infrastructure.generated.RejectReason.INVALID_AMOUNT;
            case UNKNOWN_HOLD:       return com.openexchange.assets.infrastructure.generated.RejectReason.UNKNOWN_HOLD;
            case BALANCE_OVERFLOW:   return com.openexchange.assets.infrastructure.generated.RejectReason.BALANCE_OVERFLOW;
            case REQUEST_ID_MISMATCH: return com.openexchange.assets.infrastructure.generated.RejectReason.REQUEST_ID_MISMATCH;
            // Mapped exhaustively above; a new reason without a wire value must fail loudly here, never
            // decode as "accepted" (NONE), which is the silent-wrong-value class of bug.
            default: throw new IllegalArgumentException("unmapped RejectReason " + r);
        }
    }
}

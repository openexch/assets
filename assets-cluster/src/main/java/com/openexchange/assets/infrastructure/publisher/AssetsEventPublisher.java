// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.publisher;

import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateEncoder;
import com.openexchange.assets.infrastructure.generated.DepositAckEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckEncoder;
import com.openexchange.assets.infrastructure.generated.HoldRejectEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SettleFaultEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawAckEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawRejectEncoder;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Outbound adapter: implements the domain {@link AssetsEventSink} port by encoding each event to SBE
 * and handing the bytes to an {@link Egress} transport. The engine emits money events through the port
 * with no knowledge of SBE or Aeron; this adapter is the only place the wire format lives.
 *
 * <p>Single-threaded (the cluster service thread); one reusable buffer, no per-event allocation. Money
 * egress is <b>reliable</b> — it must never be shed — so {@link Egress} is expected to apply
 * backpressure rather than drop.</p>
 */
public final class AssetsEventPublisher implements AssetsEventSink {

    /** The reliable egress transport (a positioned, flow-controlled channel — never lossy). */
    public interface Egress {
        /**
         * @param channel one of the {@code CH_*} constants — which class of egress this frame is, so the
         *                transport can skip sessions that did not subscribe to it
         */
        void emit(MutableDirectBuffer buffer, int offset, int length, int channel);
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
    /** What a session gets when it never subscribes: everything, i.e. the pre-subscription behaviour. */
    public static final int CH_ALL = CH_ACKS | CH_BALANCES | CH_SETTLEMENTS | CH_SNAPSHOTS;

    private static final int BUFFER_CAPACITY = 256;

    private final Egress egress;
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
    private final HoldSnapshotEntryEncoder holdSnapshotEntryEncoder = new HoldSnapshotEntryEncoder();
    private final HoldSnapshotEndEncoder holdSnapshotEndEncoder = new HoldSnapshotEndEncoder();

    public AssetsEventPublisher(Egress egress) {
        this.egress = egress;
    }

    @Override
    public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable) {
        depositAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount).newAvailable(newAvailable);
        flush(depositAckEncoder.encodedLength(), CH_ACKS);
    }

    @Override
    public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable) {
        withdrawAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount).newAvailable(newAvailable);
        flush(withdrawAckEncoder.encodedLength(), CH_ACKS);
    }

    @Override
    public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) {
        holdAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).orderId(orderId).userId(userId).assetId(assetId).amount(amount);
        flush(holdAckEncoder.encodedLength(), CH_ACKS);
    }

    @Override
    public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, RejectReason reason) {
        holdRejectEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).orderId(orderId).userId(userId).assetId(assetId)
                .amount(amount).reason(toSbe(reason));
        flush(holdRejectEncoder.encodedLength(), CH_ACKS);
    }

    @Override
    public void onBalanceUpdate(long userId, int assetId, long available, long locked) {
        balanceUpdateEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .userId(userId).assetId(assetId).available(available).locked(locked);
        flush(balanceUpdateEncoder.encodedLength(), CH_BALANCES);
    }

    @Override
    public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) {
        settlementAppliedEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .tradeId(tradeId).buyerUserId(buyerUserId).sellerUserId(sellerUserId);
        flush(settlementAppliedEncoder.encodedLength(), CH_SETTLEMENTS);
    }

    @Override
    public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, RejectReason reason) {
        withdrawRejectEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount).reason(toSbe(reason));
        flush(withdrawRejectEncoder.encodedLength(), CH_ACKS);
    }

    @Override
    public void onBalanceSnapshotEnd(long correlationId, int entryCount) {
        balanceSnapshotEndEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).entryCount(entryCount);
        flush(balanceSnapshotEndEncoder.encodedLength(), CH_SNAPSHOTS);
    }

    @Override
    public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) {
        holdSnapshotEntryEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .orderId(orderId).userId(userId).assetId(assetId).remaining(remaining);
        flush(holdSnapshotEntryEncoder.encodedLength(), CH_SNAPSHOTS);
    }

    @Override
    public void onHoldSnapshotEnd(long correlationId, int entryCount) {
        holdSnapshotEndEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).entryCount(entryCount);
        flush(holdSnapshotEndEncoder.encodedLength(), CH_SNAPSHOTS);
    }

    @Override
    public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId) {
        feedPositionReportEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .correlationId(correlationId).consumePosition(consumePosition)
                .lastAppliedTradeId(lastAppliedTradeId);
        flush(feedPositionReportEncoder.encodedLength(), CH_ACKS);
    }

    @Override
    public void onSettleFault(long tradeId, long orderId, long userId, int assetId,
                              long drawnFromAvailable, long uncovered) {
        settleFaultEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .tradeId(tradeId).orderId(orderId).userId(userId).assetId(assetId)
                .drawnFromAvailable(drawnFromAvailable).uncovered(uncovered);
        flush(settleFaultEncoder.encodedLength(), CH_SETTLEMENTS);
    }

    private void flush(int bodyLength, int channel) {
        egress.emit(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + bodyLength, channel);
    }

    private static com.openexchange.assets.infrastructure.generated.RejectReason toSbe(RejectReason r) {
        switch (r) {
            case NONE:               return com.openexchange.assets.infrastructure.generated.RejectReason.NONE;
            case INSUFFICIENT_FUNDS: return com.openexchange.assets.infrastructure.generated.RejectReason.INSUFFICIENT_FUNDS;
            case INVALID_AMOUNT:     return com.openexchange.assets.infrastructure.generated.RejectReason.INVALID_AMOUNT;
            case UNKNOWN_HOLD:       return com.openexchange.assets.infrastructure.generated.RejectReason.UNKNOWN_HOLD;
            // No wire value of its own yet, and it can only be produced by deposit, which emits no
            // reject message at all. Mapped explicitly so it can never fall through to NONE: a
            // rejection that decodes as "accepted" is exactly the silent-wrong-value class of bug
            // this reason was added to close.
            case BALANCE_OVERFLOW:   return com.openexchange.assets.infrastructure.generated.RejectReason.INVALID_AMOUNT;
            default:                 return com.openexchange.assets.infrastructure.generated.RejectReason.NONE;
        }
    }
}

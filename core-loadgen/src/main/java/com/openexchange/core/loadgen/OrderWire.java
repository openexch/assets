// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

import com.openexchange.core.infrastructure.generated.BoolFlag;
import com.openexchange.core.infrastructure.generated.CancelOrderEncoder;
import com.openexchange.core.infrastructure.generated.CreateOrderEncoder;
import com.openexchange.core.infrastructure.generated.DepositEncoder;
import com.openexchange.core.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.core.infrastructure.generated.GateReason;
import com.openexchange.core.infrastructure.generated.GateState;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.OrderEntryGateEncoder;
import com.openexchange.core.infrastructure.generated.OrderType;
import com.openexchange.core.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.Side;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import org.agrona.MutableDirectBuffer;

/**
 * The v7 frames the order load generator may send: the ones a GATEWAY, TOOL or ADMIN client sends (CreateOrder,
 * CancelOrder, Deposit, RegisterSession, OrderEntryGate). It never encodes a forwarder, bridge or replica frame. Each
 * method encodes at offset 0 of {@code buf} and returns the frame length.
 */
public final class OrderWire {

    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final CreateOrderEncoder create = new CreateOrderEncoder();
    private final CancelOrderEncoder cancel = new CancelOrderEncoder();
    private final DepositEncoder deposit = new DepositEncoder();
    private final RegisterSessionEncoder register = new RegisterSessionEncoder();
    private final OrderEntryGateEncoder gate = new OrderEntryGateEncoder();

    /** A resting LIMIT create with a client order id and a request id. */
    public int limit(final MutableDirectBuffer buf, final long corr, final long user, final long clientOrderIdHigh,
                     final long clientOrderIdLow, final long requestIdHigh, final long requestIdLow, final int market,
                     final boolean buy, final long price, final long qty, final TimeInForce tif,
                     final long expiresAtMs) {
        create.wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).clientOrderIdHigh(clientOrderIdHigh)
                .clientOrderIdLow(clientOrderIdLow).requestIdHigh(requestIdHigh).requestIdLow(requestIdLow)
                .marketId(market).side(buy ? Side.BUY : Side.SELL).orderType(OrderType.LIMIT).timeInForce(tif)
                .price(price).quantity(qty).budget(0).stopPrice(0).trailingDelta(0).displayQty(0)
                .expiresAtMs(expiresAtMs);
        create.flags().clear();
        return MessageHeaderEncoder.ENCODED_LENGTH + create.encodedLength();
    }

    /** A cancel by core order id, or by the original client order id when {@code orderId} is 0. */
    public int cancel(final MutableDirectBuffer buf, final long corr, final long user, final long orderId,
                      final long requestIdHigh, final long requestIdLow, final long origClientOrderIdHigh,
                      final long origClientOrderIdLow) {
        cancel.wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).orderId(orderId)
                .requestIdHigh(requestIdHigh).requestIdLow(requestIdLow).origClientOrderIdHigh(origClientOrderIdHigh)
                .origClientOrderIdLow(origClientOrderIdLow);
        return MessageHeaderEncoder.ENCODED_LENGTH + cancel.encodedLength();
    }

    /** A deposit (TOOL) with its request id. */
    public int deposit(final MutableDirectBuffer buf, final long corr, final long user, final int asset,
                       final long amount, final long requestIdHigh, final long requestIdLow) {
        deposit.wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user).assetId(asset).amount(amount)
                .requestIdHigh(requestIdHigh).requestIdLow(requestIdLow);
        return MessageHeaderEncoder.ENCODED_LENGTH + deposit.encodedLength();
    }

    /**
     * RegisterSession: resumable, acks always; {@code settlements} and {@code dispatch} as given; no balances,
     * snapshots, orders or control channel (the shape a gateway load client needs).
     */
    public int registerSession(final MutableDirectBuffer buf, final long corr, final SessionClass cls,
                               final boolean settlements, final boolean dispatch) {
        register.wrapAndApplyHeader(buf, 0, header).correlationId(corr).sessionClass(cls).readerKind((short)0)
                .resumable(BoolFlag.TRUE).identity(0L).sourceEpoch(0L);
        final EgressChannelsEncoder ch = register.channels().clear();
        ch.acks(true).balances(false).settlements(settlements).snapshots(false).orders(false).dispatch(dispatch)
                .control(false);
        return MessageHeaderEncoder.ENCODED_LENGTH + register.encodedLength();
    }

    /** The ADMIN global order entry gate OPEN. */
    public int gateOpen(final MutableDirectBuffer buf, final long corr) {
        gate.wrapAndApplyHeader(buf, 0, header).correlationId(corr).marketId(0).state(GateState.OPEN)
                .reason(GateReason.ADMIN);
        return MessageHeaderEncoder.ENCODED_LENGTH + gate.encodedLength();
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

/**
 * RC2 S8 section 2: every field of one MeCommandOutcome (64) as the core applies it under behaviour v3. Reusable and
 * mutable (the demuxer fills one instance per frame, zero allocation); the engine reads it during one apply and never
 * keeps a reference.
 */
public final class MeOutcomeView {

    public long egressSeq;
    public long journalSeq;
    public long meEpoch;
    public long commandIdHigh;
    public long commandIdLow;
    public long userId;
    public long omsOrderId;
    public long oldOrderId;
    public long price;
    public long quantity;
    public long budget;
    public int marketId;
    public int commandKind;
    public int orderType;
    public int orderSide;
    public long appliedPosition;
    public long meTimestamp;
    /** The ME leg id (wire {@code orderId}). */
    public long orderId;
    public int status;
    public int reason;
    public boolean oldCancelled;
    public int result;
    public int timeInForce;
    public long expectedOldRemaining;
    public long ledgerSize;
    public long ledgerCapacity;
    public long behaviorVersion;

    public MeOutcomeView reset() {
        egressSeq = 0; journalSeq = 0; meEpoch = 0; commandIdHigh = 0; commandIdLow = 0; userId = 0; omsOrderId = 0;
        oldOrderId = 0; price = 0; quantity = 0; budget = 0; marketId = 0; commandKind = 0; orderType = 0; orderSide = 0;
        appliedPosition = 0; meTimestamp = 0; orderId = 0; status = 0; reason = 0; oldCancelled = false; result = 0;
        timeInForce = 0; expectedOldRemaining = 0; ledgerSize = 0; ledgerCapacity = 0; behaviorVersion = 0;
        return this;
    }

    @Override
    public String toString() {
        return "MeOutcomeView{jseq=" + journalSeq + " id=" + commandIdHigh + "/" + commandIdLow + " kind=" + commandKind
                + " result=" + result + " leg=" + orderId + " status=" + status + " reason=" + reason + " ledger=" + ledgerSize
                + "/" + ledgerCapacity + " behavior=" + behaviorVersion + "}";
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.domain.AdminResult;
import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.ConfigKind;
import com.openexchange.assets.domain.CoreOrderView;
import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.OrderStatus;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettleRejectReason;

import java.util.ArrayList;
import java.util.List;

/**
 * RCS S1 test helper: an {@link AssetsEventSink} that delegates every call (defaults included) to a
 * {@link RecordingAssetsSink} for the rendered stream, counts every call, and records {@code (callIndex, logPosition)}
 * for every call that carries a log position. Renders deliberately omit positions, so this is what makes the
 * position rule (1.3) visible to a comparator.
 */
final class StampSink implements AssetsEventSink {

    final RecordingAssetsSink recording = new RecordingAssetsSink();
    /** {@code {callIndex, logPosition}} per position-carrying call, in call order. */
    final List<long[]> stamps = new ArrayList<>();
    long calls;

    private void call() {
        calls++;
    }

    private void stamp(final long logPosition) {
        stamps.add(new long[] {calls, logPosition});
        calls++;
    }

    List<AssetsEngineEvent> events() {
        return recording.events();
    }

    public void onDepositAck(long c, long u, int a, long amt, long n, boolean dup) {
        call();
        recording.onDepositAck(c, u, a, amt, n, dup);
    }

    public void onWithdrawAck(long c, long u, int a, long amt, long n, boolean dup) {
        call();
        recording.onWithdrawAck(c, u, a, amt, n, dup);
    }

    public void onHoldAck(long c, long o, long u, int a, long amt) {
        call();
        recording.onHoldAck(c, o, u, a, amt);
    }

    public void onHoldReject(long c, long o, long u, int a, long amt, RejectReason r) {
        call();
        recording.onHoldReject(c, o, u, a, amt, r);
    }

    public void onBalanceUpdate(long u, int a, long av, long l) {
        call();
        recording.onBalanceUpdate(u, a, av, l);
    }

    public void onSettlementApplied(long t, long b, long s) {
        call();
        recording.onSettlementApplied(t, b, s);
    }

    public void onWithdrawReject(long c, long u, int a, long amt, RejectReason r) {
        call();
        recording.onWithdrawReject(c, u, a, amt, r);
    }

    public void onBalanceSnapshotEnd(long c, int n) {
        call();
        recording.onBalanceSnapshotEnd(c, n);
    }

    public void onHoldSnapshotEntry(long o, long u, int a, long rem) {
        call();
        recording.onHoldSnapshotEntry(o, u, a, rem);
    }

    public void onHoldSnapshotEnd(long c, int n) {
        call();
        recording.onHoldSnapshotEnd(c, n);
    }

    public void onFeedPositionReport(long c, long p, long t, boolean j) {
        call();
        recording.onFeedPositionReport(c, p, t, j);
    }

    public void onSettleFault(long t, long o, long u, int a, long d, long un) {
        call();
        recording.onSettleFault(t, o, u, a, d, un);
    }

    public void onSettleRejected(long t, int m, long p, SettleRejectReason r, long d) {
        call(); // journalPosition is the ME journal's, not a log position
        recording.onSettleRejected(t, m, p, r, d);
    }

    public void onOrderReject(long c, long rh, long rl, long u, long o, int m, int ref, CoreRejectReason r, int me, long d,
                              long t) {
        call();
        recording.onOrderReject(c, rh, rl, u, o, m, ref, r, me, d, t);
    }

    public void onAdminAck(long c, int ref, AdminResult r, int reason, long cfg) {
        call();
        recording.onAdminAck(c, ref, r, reason, cfg);
    }

    public void onAdmissionState(int m, GateState gs, GateReason gr, boolean b, long at, long lp, boolean o) {
        stamp(lp);
        recording.onAdmissionState(m, gs, gr, b, at, lp, o);
    }

    public void onOrderAck(long c, long rh, long rl, long o, long u, int m, OrderStatus st, boolean dup, int ha, long hm,
                           long at, long seq) {
        call();
        recording.onOrderAck(c, rh, rl, o, u, m, st, dup, ha, hm, at, seq);
    }

    public void onOrderEvent(long seq, long lp, CoreOrderView o, EventKind k, CoreRejectReason r, long lpx, long lq, long t,
                             long hr, long up) {
        stamp(lp);
        recording.onOrderEvent(seq, lp, o, k, r, lpx, lq, t, hr, up);
    }

    public void onMeDispatch(long ds, long ih, long il, DispatchKind k, long u, long o, long old, int m, int ot, int sd,
                             long px, long q, long b, int tif) {
        call();
        recording.onMeDispatch(ds, ih, il, k, u, o, old, m, ot, sd, px, q, b, tif);
    }

    public void onDepositReject(long c, long rh, long rl, long u, int a, long amt, RejectReason r) {
        call();
        recording.onDepositReject(c, rh, rl, u, a, amt, r);
    }

    public void onConfigApplied(ConfigKind k, int m, long v, long lp) {
        stamp(lp);
        recording.onConfigApplied(k, m, v, lp);
    }

    public void onOrderFault(OrderFaultKind k, long o, long u, int m, long t, long amt, long lp) {
        stamp(lp);
        recording.onOrderFault(k, o, u, m, t, amt, lp);
    }

    public void onOrderSnapshotBatch(long c, CoreOrderView[] page, int count) {
        call();
        recording.onOrderSnapshotBatch(c, page, count);
    }

    public void onOrderSnapshotEnd(long c, int n, long next, long asOf) {
        stamp(asOf);
        recording.onOrderSnapshotEnd(c, n, next, asOf);
    }

    public void onPendingDispatchEnd(long c, int n, long head) {
        call();
        recording.onPendingDispatchEnd(c, n, head);
    }

    public void onMeDispatchReply(long ds, long ih, long il, DispatchKind k, long u, long o, long old, int m, int ot, int sd,
                                  long px, long q, long b, int tif) {
        call();
        recording.onMeDispatchReply(ds, ih, il, k, u, o, old, m, ot, sd, px, q, b, tif);
    }
}

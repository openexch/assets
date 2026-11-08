// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.CoreRestoreRig.Encoders;
import com.openexchange.assets.cluster.CoreRestoreRig.Listener;
import com.openexchange.assets.cluster.CoreRestoreRig.Session;
import com.openexchange.assets.cluster.ShadowClusterRig.Client;
import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.TimeInForce;

import java.util.List;

import static org.junit.Assert.assertNull;

/**
 * RCS S4 W4: resting LIMIT orders over the wire on a {@link ShadowClusterRig} (the {@code CoreChunkedRestore100kClusterTest}
 * build: ADMIN gate open, funding deposits, a registered GATEWAY session, creates in a bounded in-flight window with
 * egress polled). Used by 11.2's 100k leg, 11.9 and 11.13.
 */
final class OrderLoad {

    static final int ORDERS_PER_USER = 10;
    private static final int WINDOW = 4_000;

    final Client admin;
    final Client tool;
    final Client gw;

    private OrderLoad(final Client admin, final Client tool, final Client gw) {
        this.admin = admin;
        this.tool = tool;
        this.gw = gw;
    }

    /** ADOPT, gate open, a registered gateway, the funding of {@code users} users. Sessions ride {@code via}'s driver. */
    static OrderLoad prepare(final ShadowClusterRig rig, final Member via, final int users) throws InterruptedException {
        final Client admin = rig.connect(SessionClass.ADMIN, via);
        rig.adopt(admin);
        final Client tool = rig.connect(SessionClass.TOOL, via);
        final Client gw = rig.connect(SessionClass.GATEWAY, via);
        final List<Session> all = rig.sessions();
        CoreRestoreRig.register(gw.session, SessionClass.GATEWAY, false, false, all);
        final Encoders enc = new Encoders();
        CoreRestoreRig.offer(admin.session, enc.buf, enc.gateOpen(0xAD1L), all);
        rig.await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 60_000,
                "the ADMIN gate open must be ACCEPTED");
        for (long u = 1; u <= users; u++) {
            CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(0, u, CoreRestoreRig.usd(), 1_000_000 * ShadowClusterRig.ONE),
                    rig.sessions());
            CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(0, u,
                    CoreRestoreRig.baseAssetOf(CoreRestoreRig.marketOf(u)), 1_000 * ShadowClusterRig.ONE), rig.sessions());
        }
        CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(99, 1, CoreRestoreRig.usd(), ShadowClusterRig.ONE), rig.sessions());
        rig.await(() -> tool.in.lastDepositAckCorr == 99, 300_000, "the funding deposits must be applied");
        return new OrderLoad(admin, tool, gw);
    }

    /** {@code orders} creates (ids {@code first..first+orders-1}), ten per user; waits for every answer. */
    void orders(final ShadowClusterRig rig, final int first, final int orders, final TimeInForce tif, final long expiresAtMs) {
        final Encoders enc = new Encoders();
        final Listener g = gw.in;
        final int answeredBefore = g.acks + g.rejects;
        for (int n = 0; n < orders; n++) {
            final int i = first - 1 + n;
            final long user = 1 + i / ORDERS_PER_USER;
            final int k = i % ORDERS_PER_USER;
            final boolean buy = k % 2 == 0;
            final int market = CoreRestoreRig.marketOf(user);
            while (n - (g.acks + g.rejects - answeredBefore) >= WINDOW) {
                for (final Session s : rig.sessions()) {
                    s.poll();
                }
            }
            CoreRestoreRig.offer(gw.session, enc.buf, enc.limit(i + 1, user, i + 1, market, buy,
                    CoreRestoreRig.priceOf(market, buy, k / 2), ShadowClusterRig.ONE / 10, tif, expiresAtMs), rig.sessions());
        }
        rig.await(() -> g.acks + g.rejects - answeredBefore >= orders, 600_000, "every create must be answered");
        assertNull("no create refused: " + g.firstReject, g.firstReject);
    }

    /** {@code n} deposits to the existing users {@code 1..users} (balance changes only: no new engine state). */
    void depositsToExisting(final ShadowClusterRig rig, final int n, final int users) {
        final Encoders enc = new Encoders();
        long corr = 1_000_000;
        for (int i = 0; i < n; i++) {
            corr++;
            CoreRestoreRig.offer(tool.session, enc.buf, enc.deposit(corr, 1 + (i % users), CoreRestoreRig.usd(),
                    ShadowClusterRig.ONE), rig.sessions());
        }
        final long last = corr;
        rig.await(() -> tool.in.lastDepositAckCorr >= last, 300_000, "deposit ack " + last);
    }
}

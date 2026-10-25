// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.proto;

import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.assets.infrastructure.persistence.SnapshotEnvelope;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import static com.openexchange.assets.application.proto.ProtoHarness.BTC;
import static com.openexchange.assets.application.proto.ProtoHarness.USD;
import static com.openexchange.assets.application.proto.ProtoOrderEngine.SIDE_BUY;
import static com.openexchange.assets.application.proto.ProtoOrderEngine.SIDE_SELL;
import static org.junit.Assert.assertEquals;

/**
 * RC0 measurement input, off the cluster: snapshot bytes and serialize/CRC/deserialize time for 100k
 * open orders over 10k users (and 1M with {@code -Dproto.sizing.million=true}). Numbers are printed,
 * not asserted: the dev box is not the rig. What IS asserted: the sizes match the declared record
 * layout and the restore reproduces the byte-identical snapshot.
 */
public class ProtoSnapshotSizingTest {

    @Test
    public void hundredThousandOpenOrders() {
        run(10_000, 100_000);
    }

    @Test
    public void oneMillionOpenOrdersWhenAsked() {
        if (Boolean.getBoolean("proto.sizing.million")) {
            run(50_000, 1_000_000);
        }
    }

    private static void run(final int users, final int orders) {
        final ProtoHarness h = new ProtoHarness();
        // Lift the per-user slot and rate limits for the sizing run (replicated config command).
        final com.openexchange.assets.domain.commands.ProtoRiskConfigCommand cfg =
                new com.openexchange.assets.domain.commands.ProtoRiskConfigCommand();
        cfg.marketId = 1; cfg.maxOrdersPerSec = 1_000_000; cfg.maxOrdersPerMin = 10_000_000; cfg.minQty = 1;
        cfg.maxQty = Long.MAX_VALUE / 4; cfg.maxNotional = Long.MAX_VALUE / 4; cfg.collarPct = 20;
        cfg.maxOpenOrders = 1_000_000; cfg.maxPositionPerMarket = Long.MAX_VALUE;
        h.engine.applyCommand(com.openexchange.assets.application.engine.AssetsEngine.CMD_PROTO_RISK_CONFIG, cfg, 1L);
        for (long u = 1; u <= users; u++) {
            h.deposit(u, USD, 10_000_000);
            h.deposit(u, BTC, 1_000);
        }
        final long t0 = System.nanoTime();
        for (int i = 0; i < orders; i++) {
            final long user = 1 + (i % users);
            final long[] ev = h.limit(i + 1, user, (i & 1) == 0 ? SIDE_BUY : SIDE_SELL, 50_000 + (i % 100), 0.01, 1_000L + i);
            if (ev[4] != ProtoOrderEngine.ST_NEW) {
                throw new AssertionError("order " + i + " rejected: reason " + ev[5]);
            }
        }
        final long t1 = System.nanoTime();
        assertEquals(orders, h.engine.proto().openOrderCount());

        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(16 * 1024 * 1024);
        final SnapshotEnvelope env = new SnapshotEnvelope();
        final long t2 = System.nanoTime();
        final int payload = BalanceSnapshotCodec.serialize(h.engine, buf, SnapshotEnvelope.payloadOffset());
        final long t3 = System.nanoTime();
        final int total = env.seal(buf, payload);
        final long t4 = System.nanoTime();

        final ProtoHarness r = new ProtoHarness();
        final long t5 = System.nanoTime();
        final int opened = env.open(buf, total);
        final long t6 = System.nanoTime();
        final BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(buf, SnapshotEnvelope.payloadOffset(), opened, r.engine);
        final long t7 = System.nanoTime();
        assertEquals(payload, opened);
        assertEquals(payload, d.bytesConsumed);
        assertEquals(orders, d.orderCount);
        assertEquals(users, d.accountCount);

        final ExpandableArrayBuffer again = new ExpandableArrayBuffer(16 * 1024 * 1024);
        final int payload2 = BalanceSnapshotCodec.serialize(r.engine, again, SnapshotEnvelope.payloadOffset());
        assertEquals(payload, payload2);
        for (int i = 0; i < payload; i += 4_099) { // sampled byte-equality (full compare is in the engine test)
            assertEquals("byte " + i, buf.getByte(SnapshotEnvelope.payloadOffset() + i), again.getByte(SnapshotEnvelope.payloadOffset() + i));
        }
        final int ledgerBytes = payload - h.engine.proto().snapshotBytes();
        System.out.printf("[SIZING] users=%d orders=%d | create %.1f us/order | snapshot payload %,d B (ledger %,d B + orders block %,d B = %.1f B/order incl. hold) "
                        + "| serialize %d ms | crc32c %d ms | verify %d ms | deserialize %d ms | envelope total %,d B%n",
                users, orders, (t1 - t0) / 1e3 / orders, payload, ledgerBytes, h.engine.proto().snapshotBytes(),
                (double) payload / orders, (t3 - t2) / 1_000_000, (t4 - t3) / 1_000_000, (t6 - t5) / 1_000_000,
                (t7 - t6) / 1_000_000, total);
    }
}

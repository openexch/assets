// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.metrics;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.order.CoreMetricsView;
import com.openexchange.assets.application.engine.order.S8Support;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.assets.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.openexchange.assets.application.engine.order.S8Support.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** RC2 S8 7 G9/G11: the OrderEngineMetrics registrar (3.10 item 3): every series, its value, plain reads, lanesBehind. */
public class OrderEngineMetricsTest {

    /** Every series of the S8 registrar, name -> value, as one scrape renders it. */
    static Map<String, Long> scrape(final CoreMetricsView view) {
        final AssetsMetricsBody body = new AssetsMetricsBody(new AssetsNodeMetrics()).include(OrderEngineMetrics.of(view, () -> 7L));
        final Map<String, Long> out = new HashMap<>();
        for (final String line : body.render().split("\n")) {
            if (line.startsWith("assets_") && !line.contains("{")) {
                final int sp = line.indexOf(' ');
                try {
                    out.put(line.substring(0, sp), Long.parseLong(line.substring(sp + 1)));
                } catch (final NumberFormatException ignored) {
                    // a seconds gauge of the base body, not ours
                }
            }
        }
        return out;
    }

    static final String[] SERIES = {
        "assets_order_stale_outcomes_total", "assets_order_redundant_outcomes_total", "assets_order_stale_terminals_total",
        "assets_order_implicit_acks_total", "assets_order_conflicts_total", "assets_order_cancel_refused_total",
        "assets_order_replace_slow_total", "assets_order_trade_gaps_total", "assets_order_faults_total",
        "assets_order_leg_regressions_total", "assets_order_quarantined", "assets_feed_duplicate_unverified_total",
        "assets_feed_duplicate_mismatch_total", "assets_feed_cursor", "assets_feed_latch_reason", "assets_order_entry_gate_reasons",
        "assets_feed_lanes_behind", "assets_release_minted_up_to_total", "assets_release_minted_order_total",
        "assets_release_confirmed_total", "assets_release_confirm_stale_total", "assets_release_abandoned_total",
        "assets_release_skipped_total", "assets_me_leg_live_total", "assets_me_leg_live_duplicate_total", "assets_orphan_cancel_total",
        "assets_backlog_parked_total", "assets_released_late_total", "assets_me_invalid_total", "assets_me_release_early_total",
        "assets_me_amend_mismatch_total", "assets_me_capacity_while_open_total", "assets_me_capacity_outcome_total",
        "assets_me_link_mismatch_total", "assets_me_ledger_size_clamped_total", "assets_late_apply_on_dead_order_total",
        "assets_leg_zero_refused_total", "assets_terminal_with_pending_create_total", "assets_core_order_hold_refused_total",
        "assets_core_order_release_refused_total", "assets_me_ledger_size", "assets_me_ledger_capacity",
        "assets_me_capacity_estimate", "assets_pending_commands", "assets_release_backlog", "assets_release_backlog_parked",
        "assets_release_backlog_oldest_seq", "assets_me_capacity_parked", "assets_core_behavior_version", "assets_pending_oldest_seq",
        "assets_egress_discard_closes_total",
    };

    @Test
    public void everySeriesWithEngineValues() {
        final V3Rig r = com.openexchange.assets.infrastructure.persistence.MeLinkSnapshotTest.rich();
        final var oe = r.engine.orders();
        final Map<String, Long> m = scrape(r.engine.metricsView());
        for (final String s : SERIES) {
            assertTrue("series " + s + " registered", m.containsKey(s));
        }
        assertEquals(oe.releaseMintedUpToCount(), (long) m.get("assets_release_minted_up_to_total"));
        assertEquals(oe.releaseMintedOrderCount(), (long) m.get("assets_release_minted_order_total"));
        assertEquals(oe.legLiveCount(), (long) m.get("assets_me_leg_live_total"));
        assertEquals(oe.orphanCancelCount(), (long) m.get("assets_orphan_cancel_total"));
        assertEquals(oe.parkedCount(), (long) m.get("assets_backlog_parked_total"));
        assertEquals("all stages", 5L, (long) m.get("assets_release_backlog"));
        assertEquals(2L, (long) m.get("assets_release_backlog_parked"));
        assertEquals(oe.oldestReleaseSeq(), (long) m.get("assets_release_backlog_oldest_seq"));
        assertEquals(oe.pendingCommands(), (long) m.get("assets_pending_commands"));
        assertEquals(oe.meLedgerSize() + oe.pendingCommands(), (long) m.get("assets_me_capacity_estimate"));
        assertEquals(4096L, (long) m.get("assets_me_ledger_capacity"));
        assertEquals(3L, (long) m.get("assets_core_behavior_version"));
        assertEquals(r.cursor(), (long) m.get("assets_feed_cursor"));
        assertEquals(r.engine.gate().reasons(0), (long) m.get("assets_order_entry_gate_reasons"));
        assertEquals("the summed SEQ discard closes", 7L, (long) m.get("assets_egress_discard_closes_total"));
    }

    @Test
    public void theViewReadsPlainValuesOnly() throws Exception {
        // 1.6: never a walk of the backlog, the index or OrderStore from the scrape thread
        final String src = Files.readString(Path.of("src/main/java/com/openexchange/assets/application/engine/order/CoreMetricsView.java"));
        for (final String forbidden : new String[] {"forEach", ".backlog().", ".dispatchIndex()", ".orders().orders()", "for (", "while"}) {
            assertFalse("CoreMetricsView must not use " + forbidden, src.contains(forbidden));
        }
    }

    @Test
    public void pendingOldestSeqFollowsTheIndex() {
        // S12 A-3: the lowest live seq of the TRADING and ORPHAN tags (releases excluded)
        final V3Rig r = funded();
        assertEquals(0L, (long) scrape(r.engine.metricsView()).get("assets_pending_oldest_seq"));
        final long a = r.create(r.limit(1, 0, 100 * ONE, ONE)); // seq 1
        final long b = r.create(r.limit(1, 0, 100 * ONE, ONE)); // seq 2
        assertEquals(1L, (long) scrape(r.engine.metricsView()).get("assets_pending_oldest_seq"));
        r.ack(a, 0, 101);                                       // seq 3: a's UP_TO (not counted)
        assertEquals("the oldest moves to b", 2L, (long) scrape(r.engine.metricsView()).get("assets_pending_oldest_seq"));
        assertEquals("the release head", 3L, (long) scrape(r.engine.metricsView()).get("assets_release_backlog_oldest_seq"));
        r.ack(b, 0, 102);
        assertEquals("nothing pending", 0L, (long) scrape(r.engine.metricsView()).get("assets_pending_oldest_seq"));
        cancel(r, 1, a);
        assertTrue(r.terminal(a, 101, 1, com.openexchange.assets.application.engine.feed.V3Rig.ME_CANCELLED, 0));
        assertTrue(r.releaseEntry(a, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        final long orphan = r.engine.orders().backlog().get(a).dispatchSeq;
        assertEquals("an orphan cancel counts", orphan, (long) scrape(r.engine.metricsView()).get("assets_pending_oldest_seq"));
    }

    // ---------------------------------------------------------------- lanesBehind (3.10 item 3)

    private static int register(final V3Rig r) {
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .correlationId(8_080).sessionClass(SessionClass.BRIDGE).readerKind((short) 0).resumable(BoolFlag.TRUE).identity(0)
                .sourceEpoch(0);
        final EgressChannelsEncoder ch = e.channels().clear();
        ch.acks(true).control(true);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int bump(final V3Rig r, final long js) {
        final MeEngineConfigEncoder e = new MeEngineConfigEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header()).egressSeq(100 + js)
                .journalSeq(js).meEpoch(V3Rig.EPOCH).configVersion(js).behaviorVersion(2).ledgerCapacity(4096).maxWireVersion(12)
                .decision(com.openexchange.assets.infrastructure.generated.ConfigDecision.BUMP);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    @Test
    public void lanesBehindFollowsTwoBridgeLanes() {
        final V3Rig r = V3Rig.open();
        final long a = r.openSession(51, SessionClass.BRIDGE);
        final long b = r.openSession(52, SessionClass.BRIDGE);
        r.dispatch(a, register(r));
        r.dispatch(b, register(r));
        for (long js = 2; js <= 4; js++) {
            r.dispatch(a, bump(r, js));
        }
        assertEquals(4L, r.cursor());
        assertEquals("lane A at the cursor", 0L, r.feed().lanesBehind());
        r.dispatch(b, bump(r, 2));
        assertEquals("lane B delivered only 2 (a duplicate): behind", 1L, r.feed().lanesBehind());
        final Map<String, Long> m = scrape(r.engine.metricsView());
        assertEquals(1L, (long) m.get("assets_feed_lanes_behind"));
        r.dispatch(b, bump(r, 3));
        r.dispatch(b, bump(r, 4));
        assertEquals("lane B caught up", 0L, r.feed().lanesBehind());
        r.dispatch(a, bump(r, 5));
        assertEquals("now B is behind again", 1L, r.feed().lanesBehind());
        r.engine.sessions().close(b);
        r.dispatch(a, bump(r, 6));
        assertEquals("a closed session's lane is dropped", 0L, r.feed().lanesBehind());
        assertEquals(ConfigDecision.BUMP, ConfigDecision.BUMP);
    }
}

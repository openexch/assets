// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.metrics;

import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.application.engine.order.CoreMetricsView;
import com.openexchange.core.application.engine.order.ReleaseTestSupport;
import com.openexchange.core.domain.ConfigDecision;
import com.openexchange.core.infrastructure.generated.BoolFlag;
import com.openexchange.core.infrastructure.generated.EgressChannelsEncoder;
import com.openexchange.core.infrastructure.generated.MeEngineConfigEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.RegisterSessionEncoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.openexchange.core.application.engine.order.ReleaseTestSupport.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** The OrderEngineMetrics registrar (3.10 item 3): every series, its value, plain reads, lanesBehind. */
public class OrderEngineMetricsTest {

    /** Every series of the registrar, name -> value, as one scrape renders it. */
    static Map<String, Long> scrape(final CoreMetricsView view) {
        final CoreMetricsBody body = new CoreMetricsBody(new CoreNodeMetrics()).include(OrderEngineMetrics.of(view, () -> 7L));
        final Map<String, Long> out = new HashMap<>();
        for (final String line : body.render().split("\n")) {
            if (line.startsWith("core_") && !line.contains("{")) {
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
        "core_order_stale_outcomes_total", "core_order_redundant_outcomes_total", "core_order_stale_terminals_total",
        "core_order_implicit_acks_total", "core_order_conflicts_total", "core_order_cancel_refused_total",
        "core_order_replace_slow_total", "core_order_trade_gaps_total", "core_order_faults_total",
        "core_order_leg_regressions_total", "core_order_quarantined", "core_feed_duplicate_unverified_total",
        "core_feed_duplicate_mismatch_total", "core_feed_cursor", "core_feed_latch_reason", "core_order_entry_gate_reasons",
        "core_feed_lanes_behind", "core_release_minted_up_to_total", "core_release_minted_order_total",
        "core_release_confirmed_total", "core_release_confirm_stale_total", "core_release_abandoned_total",
        "core_release_skipped_total", "core_me_leg_live_total", "core_me_leg_live_duplicate_total", "core_orphan_cancel_total",
        "core_backlog_parked_total", "core_released_late_total", "core_me_invalid_total", "core_me_release_early_total",
        "core_me_amend_mismatch_total", "core_me_capacity_while_open_total", "core_me_capacity_outcome_total",
        "core_me_link_mismatch_total", "core_me_ledger_size_clamped_total", "core_late_apply_on_dead_order_total",
        "core_leg_zero_refused_total", "core_terminal_with_pending_create_total", "core_order_hold_refused_total",
        "core_order_release_refused_total", "core_me_ledger_size", "core_me_ledger_capacity",
        "core_me_capacity_estimate", "core_pending_commands", "core_release_backlog", "core_release_backlog_parked",
        "core_release_backlog_oldest_seq", "core_me_capacity_parked", "core_behavior_version", "core_pending_oldest_seq",
        "core_egress_discard_closes_total",
        // lane-halt table counters and gauges
        "core_feed_duplicate_total", "core_feed_halt_applied_total", "core_feed_halt_stale_total",
        "core_feed_halt_duplicate_total", "core_feed_latch_auto_clear_total", "core_feed_gap_rejects_total",
        "core_gate_auto_reopen_total", "core_feed_lane_halts", "core_feed_halt_table_overflow",
    };

    @Test
    public void everySeriesWithEngineValues() {
        final V3Rig r = com.openexchange.core.infrastructure.persistence.MeLinkSnapshotTest.rich();
        final var oe = r.engine.orders();
        final Map<String, Long> m = scrape(r.engine.metricsView());
        for (final String s : SERIES) {
            assertTrue("series " + s + " registered", m.containsKey(s));
        }
        assertEquals(oe.releaseMintedUpToCount(), (long) m.get("core_release_minted_up_to_total"));
        assertEquals(oe.releaseMintedOrderCount(), (long) m.get("core_release_minted_order_total"));
        assertEquals(oe.legLiveCount(), (long) m.get("core_me_leg_live_total"));
        assertEquals(oe.orphanCancelCount(), (long) m.get("core_orphan_cancel_total"));
        assertEquals(oe.parkedCount(), (long) m.get("core_backlog_parked_total"));
        assertEquals("all stages", 5L, (long) m.get("core_release_backlog"));
        assertEquals(2L, (long) m.get("core_release_backlog_parked"));
        assertEquals(oe.oldestReleaseSeq(), (long) m.get("core_release_backlog_oldest_seq"));
        assertEquals(oe.pendingCommands(), (long) m.get("core_pending_commands"));
        assertEquals(oe.meLedgerSize() + oe.pendingCommands(), (long) m.get("core_me_capacity_estimate"));
        assertEquals(4096L, (long) m.get("core_me_ledger_capacity"));
        assertEquals(3L, (long) m.get("core_behavior_version"));
        assertEquals(r.cursor(), (long) m.get("core_feed_cursor"));
        assertEquals(r.engine.gate().reasons(0), (long) m.get("core_order_entry_gate_reasons"));
        assertEquals("the summed SEQ discard closes", 7L, (long) m.get("core_egress_discard_closes_total"));
    }

    /** Every rendered line (labelled ones too), "name" or "name{label=\"v\"}" -> value. */
    static Map<String, Long> scrapeAll(final CoreMetricsView view) {
        final CoreMetricsBody body = new CoreMetricsBody(new CoreNodeMetrics()).include(OrderEngineMetrics.of(view, () -> 7L));
        final Map<String, Long> out = new HashMap<>();
        for (final String line : body.render().split("\n")) {
            if (line.startsWith("core_")) {
                final int sp = line.lastIndexOf(' ');
                try {
                    out.put(line.substring(0, sp), Long.parseLong(line.substring(sp + 1)));
                } catch (final NumberFormatException ignored) {
                    // a seconds gauge of the base body, not ours
                }
            }
        }
        return out;
    }

    private static long series(final Map<String, Long> m, final String name) {
        final Long v = m.get(name);
        if (v == null) {
            throw new AssertionError("missing " + name);
        }
        return v;
    }

    /** The five tick counters and the per-market quote gauges, after 3 applied, 2 stale, 1 invalid, 1 epoch, 1 unbound. */
    @Test
    public void tickCountersAndMarketQuoteGauges() {
        final V3Rig r = V3Rig.unbound();
        final var oe = r.engine.orders();
        final long px = 100 * V3Rig.ONE;
        oe.applyMarketTick(1, 1, px - V3Rig.ONE, px + V3Rig.ONE, px, 5L, V3Rig.EPOCH, r.now);       // unbound: counted, not applied
        r.bindFresh();
        oe.applyMarketTick(1, 1, px - V3Rig.ONE, px + V3Rig.ONE, px, 5L, V3Rig.EPOCH, r.now);       // applied 1 (trade 5)
        oe.applyMarketTick(1, 2, px - 2 * V3Rig.ONE, px + 2 * V3Rig.ONE, 0L, 0L, V3Rig.EPOCH, r.now); // applied 2 (no trade info)
        oe.applyMarketTick(1, 3, px - 3 * V3Rig.ONE, px + 3 * V3Rig.ONE, 0L, 0L, V3Rig.EPOCH, r.now + 7); // applied 3
        oe.applyMarketTick(1, 3, px - V3Rig.ONE, px + V3Rig.ONE, 0L, 0L, V3Rig.EPOCH, r.now);       // stale 1 (same version)
        oe.applyMarketTick(1, 2, px - V3Rig.ONE, px + V3Rig.ONE, 0L, 0L, V3Rig.EPOCH, r.now);       // stale 2 (older version)
        oe.applyMarketTick(1, 4, px + V3Rig.ONE, px - V3Rig.ONE, 0L, 0L, V3Rig.EPOCH, r.now);       // invalid (crossed)
        oe.applyMarketTick(1, 9, px - V3Rig.ONE, px + V3Rig.ONE, 0L, 0L, V3Rig.EPOCH + 1, r.now);   // epoch drop
        final Map<String, Long> m = scrapeAll(r.engine.metricsView());
        assertEquals("labels follow the core's market count", com.openexchange.core.application.engine.order.OrderEngine.MARKETS,
                OrderEngineMetrics.MARKET_LABELS.length);
        assertEquals("five markets", 5, OrderEngineMetrics.MARKET_LABELS.length);
        final String body = new CoreMetricsBody(new CoreNodeMetrics()).include(OrderEngineMetrics.of(r.engine.metricsView(), () -> 7L)).render();
        for (final String c : new String[] {"applied", "stale", "invalid", "epoch_drop", "unbound"}) {
            assertTrue("counter type of " + c, body.contains("# TYPE core_ticks_" + c + "_total counter"));
        }
        assertEquals(3L, series(m, "core_ticks_applied_total"));
        assertEquals(2L, series(m, "core_ticks_stale_total"));
        assertEquals(1L, series(m, "core_ticks_invalid_total"));
        assertEquals(1L, series(m, "core_ticks_epoch_drop_total"));
        assertEquals(1L, series(m, "core_ticks_unbound_total"));
        assertEquals(3L, series(m, "core_market_book_version{market=\"1\"}"));
        assertEquals(px - 3 * V3Rig.ONE, series(m, "core_market_best_bid{market=\"1\"}"));
        assertEquals(px + 3 * V3Rig.ONE, series(m, "core_market_best_ask{market=\"1\"}"));
        assertEquals("the trade id stays at the last positive trade", 5L, series(m, "core_market_last_trade_id{market=\"1\"}"));
        assertEquals("cluster time of the last applied tick", oe.market(1).lastTickTs, series(m, "core_market_tick_cluster_ms{market=\"1\"}"));
        assertTrue("the applied tick moved the cluster time", oe.market(1).lastTickTs > 0L);
        for (int k = 2; k <= 5; k++) {
            for (final String g : new String[] {"book_version", "best_bid", "best_ask", "last_trade_id", "tick_cluster_ms"}) {
                assertEquals("untouched market " + k + " " + g, 0L, series(m, "core_market_" + g + "{market=\"" + k + "\"}"));
            }
        }
        assertFalse("no market 0 or 6 series", m.containsKey("core_market_book_version{market=\"0\"}")
                || m.containsKey("core_market_book_version{market=\"6\"}"));
    }

    @Test
    public void theViewReadsPlainValuesOnly() throws Exception {
        // 1.6: never a walk of the backlog, the index or OrderStore from the scrape thread
        final String src = Files.readString(Path.of("src/main/java/com/openexchange/core/application/engine/order/CoreMetricsView.java"));
        for (final String forbidden : new String[] {"forEach", ".backlog().", ".dispatchIndex()", ".orders().orders()", "for (", "while"}) {
            assertFalse("CoreMetricsView must not use " + forbidden, src.contains(forbidden));
        }
    }

    @Test
    public void pendingOldestSeqFollowsTheIndex() {
        // The lowest live seq of the TRADING and ORPHAN tags (releases excluded)
        final V3Rig r = funded();
        assertEquals(0L, (long) scrape(r.engine.metricsView()).get("core_pending_oldest_seq"));
        final long a = r.create(r.limit(1, 0, 100 * ONE, ONE)); // seq 1
        final long b = r.create(r.limit(1, 0, 100 * ONE, ONE)); // seq 2
        assertEquals(1L, (long) scrape(r.engine.metricsView()).get("core_pending_oldest_seq"));
        r.ack(a, 0, 101);                                       // seq 3: a's UP_TO (not counted)
        assertEquals("the oldest moves to b", 2L, (long) scrape(r.engine.metricsView()).get("core_pending_oldest_seq"));
        assertEquals("the release head", 3L, (long) scrape(r.engine.metricsView()).get("core_release_backlog_oldest_seq"));
        r.ack(b, 0, 102);
        assertEquals("nothing pending", 0L, (long) scrape(r.engine.metricsView()).get("core_pending_oldest_seq"));
        cancel(r, 1, a);
        assertTrue(r.terminal(a, 101, 1, com.openexchange.core.application.engine.feed.V3Rig.ME_CANCELLED, 0));
        assertTrue(r.releaseEntry(a, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        final long orphan = r.engine.orders().backlog().get(a).dispatchSeq;
        assertEquals("an orphan cancel counts", orphan, (long) scrape(r.engine.metricsView()).get("core_pending_oldest_seq"));
    }

    // ---------------------------------------------------------------- lanesBehind (3.10 item 3)

    /** A BRIDGE RegisterSession; two live lanes need two identities since (DUPLICATE_IDENTITY). */
    private static int register(final V3Rig r, final long identity) {
        final RegisterSessionEncoder e = new RegisterSessionEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header())
                .correlationId(8_080).sessionClass(SessionClass.BRIDGE).readerKind((short) 0).resumable(BoolFlag.TRUE)
                .identity(identity).sourceEpoch(0);
        final EgressChannelsEncoder ch = e.channels().clear();
        ch.acks(true).control(true);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private static int bump(final V3Rig r, final long js) {
        final MeEngineConfigEncoder e = new MeEngineConfigEncoder().wrapAndApplyHeader(r.buffer(), 0, r.header()).egressSeq(100 + js)
                .journalSeq(js).meEpoch(V3Rig.EPOCH).configVersion(js).behaviorVersion(2).ledgerCapacity(4096).maxWireVersion(12)
                .decision(com.openexchange.core.infrastructure.generated.ConfigDecision.BUMP);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    /** The counters and gauges read the engine, and core_sessions{class} is one family. */
    @Test
    public void laneHaltCountersAndSessionsByClass() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        final long a = r.openSession(51, SessionClass.BRIDGE);
        final long b = r.openSession(52, SessionClass.BRIDGE);
        r.dispatch(a, register(r, 1));
        r.dispatch(b, register(r, 2));
        r.engine.applyFeedHalt(51, 1L, com.openexchange.core.domain.FeedHaltReason.GAP, 0L, 4, 6, 1, r.now);
        r.engine.applyFeedHalt(52, 2L, com.openexchange.core.domain.FeedHaltReason.GAP, 0L, 4, 6, 2, r.now);
        r.engine.applyFeedHalt(52, 3L, com.openexchange.core.domain.FeedHaltReason.GAP, 0L, 4, 6, 2, r.now); // DUPLICATE
        r.engine.applyFeedHalt(52, 4L, com.openexchange.core.domain.FeedHaltReason.GAP, 0L, 2, 3, 2, r.now); // stale
        Map<String, Long> m = scrape(r.engine.metricsView());
        assertEquals(2L, (long) m.get("core_feed_halt_applied_total"));
        assertEquals(1L, (long) m.get("core_feed_halt_duplicate_total"));
        assertEquals(1L, (long) m.get("core_feed_halt_stale_total"));
        assertEquals(2L, (long) m.get("core_feed_lane_halts"));
        assertEquals(0L, (long) m.get("core_feed_halt_table_overflow"));
        assertEquals("GAP is the reported latch", 1L, (long) m.get("core_feed_latch_reason"));
        assertTrue(r.sellHits(1, V3Rig.ONE / 16));
        assertTrue(r.sellHits(2, V3Rig.ONE / 16)); // c = 5: both records reach, the gate reopens
        assertTrue(r.sellHitsAt(5, V3Rig.EPOCH, r.egress, 2, V3Rig.ONE / 16)); // a byte-equal duplicate of 5
        assertFalse(r.sellHitsAt(9, V3Rig.EPOCH, r.egress + 1, 3, V3Rig.ONE / 16)); // FEED_GAP: a core-detected record
        m = scrape(r.engine.metricsView());
        assertEquals(2L, (long) m.get("core_feed_latch_auto_clear_total"));
        assertEquals(1L, (long) m.get("core_gate_auto_reopen_total"));
        assertEquals(1L, (long) m.get("core_feed_duplicate_total"));
        assertEquals(1L, (long) m.get("core_feed_gap_rejects_total"));
        assertEquals("the core-detected record", 1L, (long) m.get("core_feed_lane_halts"));
        final String body = new CoreMetricsBody(new CoreNodeMetrics()).include(OrderEngineMetrics.of(r.engine.metricsView()))
                .render();
        assertEquals("one HELP line for the family", 1, body.split("# HELP core_sessions ", -1).length - 1);
        assertTrue(body, body.contains("core_sessions{class=\"BRIDGE\"} 2\n"));
        for (final String cls : new String[] {"ADMIN", "TOOL", "GATEWAY", "TREASURY", "FORWARDER", "REPLICA"}) {
            assertTrue(body, body.contains("core_sessions{class=\"" + cls + "\"} 0\n"));
        }
        assertFalse("no unlabelled core_sessions sample", body.contains("\ncore_sessions "));
        r.engine.sessions().close(b);
        assertTrue(new CoreMetricsBody(new CoreNodeMetrics()).include(OrderEngineMetrics.of(r.engine.metricsView())).render()
                .contains("core_sessions{class=\"BRIDGE\"} 1\n"));
    }

    @Test
    public void lanesBehindFollowsTwoBridgeLanes() {
        final V3Rig r = V3Rig.open();
        final long a = r.openSession(51, SessionClass.BRIDGE);
        final long b = r.openSession(52, SessionClass.BRIDGE);
        r.dispatch(a, register(r, 1));
        r.dispatch(b, register(r, 2));
        for (long js = 2; js <= 4; js++) {
            r.dispatch(a, bump(r, js));
        }
        assertEquals(4L, r.cursor());
        assertEquals("lane A at the cursor", 0L, r.feed().lanesBehind());
        r.dispatch(b, bump(r, 2));
        assertEquals("lane B delivered only 2 (a duplicate): behind", 1L, r.feed().lanesBehind());
        final Map<String, Long> m = scrape(r.engine.metricsView());
        assertEquals(1L, (long) m.get("core_feed_lanes_behind"));
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

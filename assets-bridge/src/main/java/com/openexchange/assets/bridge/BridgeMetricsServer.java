// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Minimal Prometheus {@code /metrics} + JSON {@code /health} endpoint for the settlement bridge.
 *
 * JDK-built-in HTTP server on its own daemon thread — zero dependencies, zero interaction with
 * the bridge agent thread beyond reading {@link BridgeState}'s volatile fields (single-writer:
 * the agent thread writes plain values under the covers of the class's own volatile fields, so
 * a normal read here already sees a consistent snapshot; no lock needed).
 */
public final class BridgeMetricsServer {

    private final BridgeState state;
    private HttpServer server;

    public BridgeMetricsServer(final BridgeState state) {
        this.state = state;
    }

    /** Starts the server. Port 0 binds an OS-assigned ephemeral port; read it back via {@link #port()}. */
    public void start(final int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/health", this::handleHealth);
        server.setExecutor(runnable -> {
            final Thread t = new Thread(runnable, "bridge-metrics-http");
            t.setDaemon(true);
            t.start();
        });
        server.start();
        System.out.println("[BRIDGE] metrics /metrics + /health on port " + port());
    }

    /** The actual bound port — resolves a requested port 0 to the ephemeral port the OS picked. */
    public int port() {
        return server.getAddress().getPort();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handleMetrics(final HttpExchange exchange) throws IOException {
        final byte[] body = render().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private void handleHealth(final HttpExchange exchange) throws IOException {
        // Single-writer volatile fields: read each once so the pair reported is self-consistent.
        final boolean halted = state.halted;
        final boolean aeConnected = state.connectedToAe;
        final byte[] body = ("{\"halted\":" + halted + ",\"aeConnected\":" + aeConnected + "}")
                .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(halted ? 503 : 200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    String render() {
        final StringBuilder sb = new StringBuilder(1024);
        gauge(sb, "bridge_halted", "1 if the bridge is halted on a detected gap, else 0", state.halted ? 1 : 0);
        gauge(sb, "bridge_ae_connected", "1 if the bridge currently holds an Assets Engine feed session, else 0",
                state.connectedToAe ? 1 : 0);
        counter(sb, "bridge_epochs_total", "Bridge epochs started since process start", state.epochs);
        counter(sb, "bridge_errors_total", "Epoch failures since process start", state.errors);
        counter(sb, "bridge_forwarded_trades_total", "Trades forwarded to the Assets Engine", state.forwardedTrades);
        counter(sb, "bridge_forwarded_terminals_total", "Terminal order statuses forwarded to the Assets Engine",
                state.forwardedTerminals);
        counter(sb, "bridge_skipped_entries_total",
                "Journal entries skipped as already-applied (below the AE watermark, or a dense-id duplicate)",
                state.skippedEntries);
        counter(sb, "bridge_gaps_detected_total",
                "Dense tradeId gaps detected (a lost settlement) - MUST stay 0; alert on any increase",
                state.gapsDetected);
        counter(sb, "bridge_source_stalls_total",
                "Journal-source stalls/losses detected and recovered by epoch restart - investigate if climbing",
                state.sourceStalls);
        gauge(sb, "bridge_source_backlog_bytes",
                "Bytes recorded in the followed journal recording not yet delivered to the bridge; "
                        + "sustained >0 while forwards are flat = the release path is behind",
                state.sourceBacklogBytes);
        gauge(sb, "bridge_last_forwarded_trade_id", "Dense tradeId of the last trade forwarded",
                state.lastForwardedTradeId);
        gauge(sb, "bridge_last_forwarded_egress_seq", "egressSeq of the last journal entry forwarded",
                state.lastForwardedEgressSeq);
        gauge(sb, "bridge_epoch_consume_position",
                "AE-reported journal consume position (W) as of the start of the current epoch",
                state.epochConsumePosition);
        gauge(sb, "bridge_epoch_last_applied_trade_id",
                "AE-reported last-applied tradeId (T) as of the start of the current epoch",
                state.epochLastAppliedTradeId);
        gauge(sb, "bridge_replay_recording_position",
                "Recorded position of the journal recording currently replayed/followed; "
                        + "with bridge_replay_consumed_position this makes cold catch-up lag observable",
                state.replayRecordingPosition);
        gauge(sb, "bridge_replay_consumed_position",
                "Replay position the bridge has consumed of the current recording "
                        + "(lag bytes = recording position - consumed position)",
                state.replayConsumedPosition);
        counter(sb, "bridge_settle_forward_clock_anomalies_total",
                "Journal trade timestamps rejected as garbage (<= 0 or in the future of the bridge "
                        + "clock); the forward-latency sample was skipped instead of recorded",
                state.settleForwardClockAnomalies);
        counter(sb, "bridge_settle_ack_map_skips_total",
                "Settle-ack tracking inserts skipped because the in-flight map hit its bound "
                        + "(bounded memory beats perfect ack coverage)",
                state.settleAckMapSkips);
        histogram(sb, "bridge_settle_forward_latency",
                "Seconds from the journal trade timestamp (written by the ME leader's clock) to the "
                        + "bridge's blocking offer returning. CROSS-HOST CLOCKS: supporting evidence "
                        + "only, ms resolution - not a single-clock measurement",
                state.settleForwardLatency);
        histogram(sb, "bridge_settle_ack_latency",
                "Seconds from the bridge's blocking offer returning to the SettlementApplied egress "
                        + "for that tradeId being observed (single host: bridge clock only)",
                state.settleAckLatency);
        return sb.toString();
    }

    private static void gauge(final StringBuilder sb, final String name, final String help, final long value) {
        series(sb, name, "gauge", help, value);
    }

    private static void counter(final StringBuilder sb, final String name, final String help, final long value) {
        series(sb, name, "counter", help, value);
    }

    private static void series(final StringBuilder sb, final String name, final String type, final String help,
                                final long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        sb.append(name).append(' ').append(value).append('\n');
    }

    /** {@code le} labels in SECONDS (Prometheus duration convention), derived from the ms bounds. */
    private static final String[] BUCKET_LE_SECONDS = bucketLeSeconds();

    private static String[] bucketLeSeconds() {
        final long[] uppersMs = BridgeState.LatencyHistogram.BUCKET_UPPER_MS;
        final String[] labels = new String[uppersMs.length];
        for (int i = 0; i < uppersMs.length; i++) {
            labels[i] = Double.toString(uppersMs[i] / 1000.0);
        }
        return labels;
    }

    /**
     * Prometheus text-format histogram: cumulative {@code _bucket{le="..."}} lines (le in
     * seconds), {@code _sum} in seconds, {@code _count}. {@code _count} is derived from the
     * bucket total so {@code le="+Inf"} always equals {@code _count}, even if the agent
     * thread records a sample mid-render.
     */
    private static void histogram(final StringBuilder sb, final String name, final String help,
                                  final BridgeState.LatencyHistogram histogram) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" histogram\n");
        long cumulative = 0;
        for (int i = 0; i < BUCKET_LE_SECONDS.length; i++) {
            cumulative += histogram.buckets.get(i);
            sb.append(name).append("_bucket{le=\"").append(BUCKET_LE_SECONDS[i]).append("\"} ")
                    .append(cumulative).append('\n');
        }
        cumulative += histogram.buckets.get(BUCKET_LE_SECONDS.length);
        sb.append(name).append("_bucket{le=\"+Inf\"} ").append(cumulative).append('\n');
        sb.append(name).append("_sum ").append(histogram.sumMs / 1000.0).append('\n');
        sb.append(name).append("_count ").append(cumulative).append('\n');
    }
}

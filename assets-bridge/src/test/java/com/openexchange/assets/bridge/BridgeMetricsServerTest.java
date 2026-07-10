// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins the /metrics + /health wire contract: series names/types render with current
 * {@link BridgeState} values, and /health flips to 503 the instant the bridge halts
 * (the alert-facing signal an operator's monitoring depends on).
 */
public class BridgeMetricsServerTest {

    private BridgeState state;
    private BridgeMetricsServer server;
    private final HttpClient client = HttpClient.newHttpClient();

    @Before
    public void setUp() throws Exception {
        state = new BridgeState();
        server = new BridgeMetricsServer(state);
        server.start(0); // ephemeral port
    }

    @After
    public void tearDown() {
        server.stop();
    }

    @Test
    public void metricsRendersAllSeriesWithCurrentValues() throws Exception {
        state.halted = false;
        state.connectedToAe = true;
        state.epochs = 3;
        state.errors = 1;
        state.forwardedTrades = 100;
        state.forwardedTerminals = 42;
        state.skippedEntries = 5;
        state.gapsDetected = 0;
        state.lastForwardedTradeId = 99;
        state.lastForwardedEgressSeq = 12_345;
        state.epochConsumePosition = 6_789;
        state.epochLastAppliedTradeId = 98;
        state.sourceStalls = 2;
        state.sourceBacklogBytes = 4_096;

        final HttpResponse<String> resp = send("/metrics");
        assertEquals(200, resp.statusCode());
        final String body = resp.body();

        assertTrue(body.contains("# TYPE bridge_halted gauge"));
        assertTrue(body.contains("bridge_halted 0"));
        assertTrue(body.contains("# TYPE bridge_ae_connected gauge"));
        assertTrue(body.contains("bridge_ae_connected 1"));
        assertTrue(body.contains("# TYPE bridge_epochs_total counter"));
        assertTrue(body.contains("bridge_epochs_total 3"));
        assertTrue(body.contains("# TYPE bridge_errors_total counter"));
        assertTrue(body.contains("bridge_errors_total 1"));
        assertTrue(body.contains("bridge_forwarded_trades_total 100"));
        assertTrue(body.contains("bridge_forwarded_terminals_total 42"));
        assertTrue(body.contains("bridge_skipped_entries_total 5"));
        assertTrue(body.contains("# TYPE bridge_gaps_detected_total counter"));
        assertTrue(body.contains("bridge_gaps_detected_total 0"));
        assertTrue(body.contains("bridge_last_forwarded_trade_id 99"));
        assertTrue(body.contains("bridge_last_forwarded_egress_seq 12345"));
        assertTrue(body.contains("bridge_epoch_consume_position 6789"));
        assertTrue(body.contains("bridge_epoch_last_applied_trade_id 98"));
        assertTrue(body.contains("# TYPE bridge_source_stalls_total counter"));
        assertTrue(body.contains("bridge_source_stalls_total 2"));
        assertTrue(body.contains("# TYPE bridge_source_backlog_bytes gauge"));
        assertTrue(body.contains("bridge_source_backlog_bytes 4096"));
    }

    @Test
    public void healthReports200WhenHealthyAnd503WhenHalted() throws Exception {
        state.connectedToAe = true;

        final HttpResponse<String> healthy = send("/health");
        assertEquals(200, healthy.statusCode());
        assertTrue(healthy.body().contains("\"halted\":false"));
        assertTrue(healthy.body().contains("\"aeConnected\":true"));

        state.halted = true;
        final HttpResponse<String> halted = send("/health");
        assertEquals(503, halted.statusCode());
        assertTrue(halted.body().contains("\"halted\":true"));
        assertTrue(halted.body().contains("\"aeConnected\":true"));
    }

    private HttpResponse<String> send(final String path) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
                .GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}

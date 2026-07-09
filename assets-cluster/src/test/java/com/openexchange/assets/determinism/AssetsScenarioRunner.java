// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.InitTradeHighWaterCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses and executes a money {@code .scenario} against a real {@link AssetsEngine}, capturing the
 * deterministic output stream via {@link RecordingAssetsSink} and rendering it to canonical text.
 *
 * <p>The {@code SNAPSHOT} verb performs a real {@link BalanceSnapshotCodec} round-trip into a fresh
 * engine — an in-scenario "restart" — proving snapshot/restore (including the settlement
 * high-water) is invisible to subsequent commands. The sink persists across the restart, exactly as
 * production's egress publisher persists while engine state is rebuilt.</p>
 *
 * <h3>DSL</h3>
 * <pre>
 *   # comment                              (also: trailing  # ... is stripped)
 *   CLOCK 1000                             set absolute logical timestamp
 *   DEPOSIT  u=100 asset=0 amt=1000.0
 *   WITHDRAW u=100 asset=0 amt=50.0
 *   HOLD     order=1 u=100 asset=0 amt=600.0
 *   RELEASE  order=1 u=100 [amt=..]        (no amt = release full residual)
 *   SETTLE   tradeId=1 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=0.5 takerBuy=1
 *   SNAPSHOT                               serialize -> restore into a fresh engine
 * </pre>
 * Amounts/prices (amt, px) are decimals converted to 8dp fixed-point; asset/market/order/user ids are
 * integers; {@code takerBuy} is 0/1.
 */
public final class AssetsScenarioRunner {

    private AssetsEngine engine;
    private SettlementProjector projector;
    private long feedPosition = 0L;
    private final RecordingAssetsSink sink = new RecordingAssetsSink();
    private final LogicalClock clock = new LogicalClock();
    // Pooled once and re-populated per INIT_HIGH_WATER call, mirroring the projector's pooled commands.
    private final InitTradeHighWaterCommand initHighWaterCommand = new InitTradeHighWaterCommand();

    private AssetsScenarioRunner() {
        this.engine = new AssetsEngine();
        engine.setEventSink(sink);
        this.projector = new SettlementProjector(engine);
    }

    /** Run a scenario file and return its canonical rendered output. */
    public static String run(Path scenarioFile) throws IOException {
        return runLines(Files.readAllLines(scenarioFile, StandardCharsets.UTF_8));
    }

    /** Run a scenario given as raw lines and return its canonical rendered output. */
    public static String runLines(List<String> lines) {
        AssetsScenarioRunner runner = new AssetsScenarioRunner();
        runner.execAll(lines);
        return runner.output();
    }

    /** Fresh runner — for tests that need to inspect engine state after running (conservation, codec). */
    static AssetsScenarioRunner newRunner() {
        return new AssetsScenarioRunner();
    }

    /** Execute all lines against this runner's engine (accumulating output). */
    void execAll(List<String> lines) {
        int lineNo = 0;
        for (String raw : lines) {
            lineNo++;
            String line = stripComment(raw).trim();
            if (line.isEmpty()) {
                continue;
            }
            try {
                exec(line);
            } catch (RuntimeException ex) {
                throw new IllegalArgumentException(
                        "scenario error at line " + lineNo + ": '" + raw + "' — " + ex.getMessage(), ex);
            }
        }
    }

    /** The current engine (post-restart if a SNAPSHOT ran). */
    AssetsEngine engine() {
        return engine;
    }

    String output() {
        return sink.render();
    }

    private void exec(String line) {
        String[] tokens = line.split("\\s+");
        String verb = tokens[0].toUpperCase();
        Map<String, String> kv = parseKv(tokens);

        switch (verb) {
            case "CLOCK":
                clock.set(Long.parseLong(tokens[1]));
                break;
            case "DEPOSIT":
                engine.applyCommand(AssetsEngine.CMD_DEPOSIT, buildDeposit(kv), clock.now());
                break;
            case "WITHDRAW":
                engine.applyCommand(AssetsEngine.CMD_WITHDRAW, buildWithdraw(kv), clock.now());
                break;
            case "HOLD":
                engine.applyCommand(AssetsEngine.CMD_HOLD, buildHold(kv), clock.now());
                break;
            case "RELEASE":
                engine.applyCommand(AssetsEngine.CMD_RELEASE, buildRelease(kv), clock.now());
                break;
            case "SETTLE":
                engine.applyCommand(AssetsEngine.CMD_SETTLE, buildSettle(kv), clock.now());
                break;
            case "INIT_HIGH_WATER":
                // Cutover primer (CMD_INIT_HIGH_WATER): seed the settlement high-water + consume position
                // on a virgin ledger. Positional args: <tradeId> <consumePosition>. A strict no-op (no
                // egress) on any non-virgin ledger, so a refusal leaves the stream unchanged.
                initHighWaterCommand.reset();
                initHighWaterCommand.setTradeId(Long.parseLong(tokens[1]));
                initHighWaterCommand.setConsumePosition(Long.parseLong(tokens[2]));
                engine.applyCommand(AssetsEngine.CMD_INIT_HIGH_WATER, initHighWaterCommand, clock.now());
                break;
            case "TRADE":
                // A TradeExecution from the ME recorded stream, fed through the settlement projector.
                projector.onTrade(++feedPosition, reqLong(kv, "tradeId"), reqInt(kv, "market"),
                        reqLong(kv, "takerOrder"), reqLong(kv, "takerUser"),
                        reqLong(kv, "makerOrder"), reqLong(kv, "makerUser"),
                        reqAmount(kv, "px"), reqAmount(kv, "qty"), reqLong(kv, "takerBuy") != 0, clock.now());
                break;
            case "TERMINAL":
                // A terminal OrderStatus from the ME recorded stream -> release the order's residual hold.
                projector.onTerminal(++feedPosition, reqLong(kv, "order"), reqLong(kv, "u"), clock.now());
                break;
            case "QUERY_FEED":
                // Read-only feed-position query: emit the journal consume position + settlement high-water
                // through the sink (a FEEDPOS line). Positional arg: <correlationId>.
                engine.reportFeedPosition(Long.parseLong(tokens[1]));
                break;
            case "SNAPSHOT":
                snapshotRoundTrip();
                break;
            default:
                throw new IllegalArgumentException("unknown verb '" + verb + "'");
        }
    }

    private DepositCommand buildDeposit(Map<String, String> kv) {
        DepositCommand c = new DepositCommand();
        c.setUserId(reqLong(kv, "u"));
        c.setAssetId(reqInt(kv, "asset"));
        c.setAmount(reqAmount(kv, "amt"));
        return c;
    }

    private WithdrawCommand buildWithdraw(Map<String, String> kv) {
        WithdrawCommand c = new WithdrawCommand();
        c.setUserId(reqLong(kv, "u"));
        c.setAssetId(reqInt(kv, "asset"));
        c.setAmount(reqAmount(kv, "amt"));
        return c;
    }

    private HoldCommand buildHold(Map<String, String> kv) {
        HoldCommand c = new HoldCommand();
        c.setOrderId(reqLong(kv, "order"));
        c.setUserId(reqLong(kv, "u"));
        c.setAssetId(reqInt(kv, "asset"));
        c.setAmount(reqAmount(kv, "amt"));
        return c;
    }

    private ReleaseCommand buildRelease(Map<String, String> kv) {
        ReleaseCommand c = new ReleaseCommand();
        c.setOrderId(reqLong(kv, "order"));
        c.setUserId(reqLong(kv, "u"));
        // No amt = release the full residual (sentinel -1).
        c.setAmount(kv.containsKey("amt") ? FixedPoint.fromDouble(Double.parseDouble(kv.get("amt"))) : -1L);
        return c;
    }

    private SettleCommand buildSettle(Map<String, String> kv) {
        SettleCommand c = new SettleCommand();
        c.setTradeId(reqLong(kv, "tradeId"));
        c.setMarketId(reqInt(kv, "market"));
        c.setTakerOrderId(reqLong(kv, "takerOrder"));
        c.setTakerUserId(reqLong(kv, "takerUser"));
        c.setMakerOrderId(reqLong(kv, "makerOrder"));
        c.setMakerUserId(reqLong(kv, "makerUser"));
        c.setPrice(reqAmount(kv, "px"));
        c.setQuantity(reqAmount(kv, "qty"));
        c.setTakerIsBuy(reqLong(kv, "takerBuy") != 0);
        return c;
    }

    /**
     * Real snapshot round-trip: serialize current engine state, restore into a FRESH engine, swap it
     * in — simulating a node restart mid-scenario. The engine's own scalars (lastAppliedTradeId,
     * consumePosition) ride through the codec; the sink (accumulated output) persists.
     */
    private void snapshotRoundTrip() {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        int length = BalanceSnapshotCodec.serialize(engine, buffer);

        AssetsEngine fresh = new AssetsEngine();
        BalanceSnapshotCodec.deserialize(buffer, 0, length, fresh);
        fresh.setEventSink(sink);
        engine = fresh;
        // Re-wire the projector onto the restored engine (its consumePosition/high-water rode through).
        projector = new SettlementProjector(fresh);
    }

    // ---- parsing helpers ----

    private static String stripComment(String line) {
        int hash = line.indexOf('#');
        return hash >= 0 ? line.substring(0, hash) : line;
    }

    private static Map<String, String> parseKv(String[] tokens) {
        Map<String, String> kv = new LinkedHashMap<>();
        for (int i = 1; i < tokens.length; i++) {
            int eq = tokens[i].indexOf('=');
            if (eq > 0) {
                kv.put(tokens[i].substring(0, eq), tokens[i].substring(eq + 1));
            }
        }
        return kv;
    }

    private static String req(Map<String, String> kv, String key) {
        String v = kv.get(key);
        if (v == null) {
            throw new IllegalArgumentException("missing required field '" + key + "'");
        }
        return v;
    }

    private static long reqLong(Map<String, String> kv, String key) {
        return Long.parseLong(req(kv, key));
    }

    private static int reqInt(Map<String, String> kv, String key) {
        return Integer.parseInt(req(kv, key));
    }

    /** A decimal money amount converted to 8dp fixed-point. */
    private static long reqAmount(Map<String, String> kv, String key) {
        return FixedPoint.fromDouble(Double.parseDouble(req(kv, key)));
    }
}

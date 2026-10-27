// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.HoldCommand;
import com.openexchange.assets.domain.commands.InitTradeHighWaterCommand;
import com.openexchange.assets.domain.commands.ReleaseCommand;
import com.openexchange.assets.domain.commands.SetMoneyJournalCommand;
import com.openexchange.assets.domain.commands.SettleCommand;
import com.openexchange.assets.domain.commands.WithdrawCommand;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.testkit.LogicalClock;
import com.openexchange.cluster.testkit.ScenarioText;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
 *   TICK 100                               advance logical time and run the deadline sweep at the new time
 *   SWEEP                                  run the deadline sweep at the current time
 *   DEPOSIT  u=100 asset=0 amt=1000.0
 *   WITHDRAW u=100 asset=0 amt=50.0
 *   HOLD     order=1 u=100 asset=0 amt=600.0
 *   RELEASE  order=1 u=100 [amt=..]        (no amt = release full residual)
 *   SETTLE   tradeId=1 market=1 takerOrder=2 takerUser=200 makerOrder=1 makerUser=100 px=60000.0 qty=0.5 takerBuy=1
 *   SET_JOURNAL 1                          arm (1) / disarm (0) the money journal, via the log
 *   GATE market=0 open=1                   order-entry gate (ADMIN); RISKCFG market= ver= [maxOpen= perSec= perMin= collarBps= slipBps= ...]
 *   BREAKER market= trip=1|0 ; MARKETTICK market= bid= ask= ver= [last= lastTradeId=]
 *   CREATE u= market= side= type= [px=] qty= [budget=] [tif=] [exp=] [cid=] [rid=] [stop=] [delta=] [display=] ; CANCEL u= [order=|cid=] [rid=]
 *   METRADE id= market= taker= maker= px= qty= takerBuy= [takerLeg= makerLeg= takerUser= makerUser= seq=] ; METERMINAL order= leg= status=
 *   MEOUTCOME order= kind= attempt= result= [leg= reason= old=] ; AMEND u= order= [px=] [qty=] [rid=] ; QUARANTINE order=
 *   ORDERSNAP u= [from=] ; PENDINGDISPATCH [from=] [max=]
 *   RC2 S7 (behaviour v3 feed; seq= stays the egressSeq): METRADE/METERMINAL/MEOUTCOME [jseq=] [epoch=],
 *   METERMINAL [meReason=], MEOUTCOME [oldCancelled=], MARKETTICK [epoch=], GATE [reason=] [corr=],
 *   V3GENESIS [bind=0], MECONFIG jseq= epoch= cfg= behavior= cap= maxWire= decision=, MERELEASE jseq= epoch=,
 *   FEEDHALT reason= expected= seen= [epoch=] [bridge=] [corr=], LATCHCLEAR reason= jseq= [corr=],
 *   FEEDPOISON tpl= jseq= epoch= [tradeId=]
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
    private final SetMoneyJournalCommand setMoneyJournalCommand = new SetMoneyJournalCommand();

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
            String line = ScenarioText.stripComment(raw).trim();
            if (line.isEmpty()) {
                continue;
            }
            try {
                exec(line);
            } catch (RuntimeException ex) {
                throw new IllegalArgumentException(
                        "scenario error at line " + lineNo + ": '" + raw + "': " + ex.getMessage(), ex);
            }
        }
    }

    /** One executable line (the {@link com.openexchange.cluster.testkit.Replica} hook). */
    void applyLine(String line) {
        exec(line);
    }

    /** Rendered events so far, one per element, in emission order. */
    List<String> renderedEvents() {
        return sink.events().stream().map(AssetsEngineEvent::render).toList();
    }

    /**
     * The production snapshot image of the current state: the v4 payload sealed in the kit envelope,
     * exactly what {@code onTakeSnapshot} offers (before chunking) and {@code loadSnapshot} opens.
     */
    byte[] snapshotBytes() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int payloadLength = BalanceSnapshotCodec.serialize(engine, buffer, SnapshotEnvelope.payloadOffset());
        final int length = new SnapshotEnvelope().seal(buffer, payloadLength);
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    /** Restart from an image: open the envelope, restore into a FRESH engine, verify, swap it in. */
    void restoreFrom(byte[] image) {
        final UnsafeBuffer buffer = new UnsafeBuffer(image);
        final int payloadLength = new SnapshotEnvelope().open(buffer, image.length);
        final AssetsEngine fresh = new AssetsEngine();
        // the jar's supported behaviour set is node-local (the V3Genesis test seam); a restarted node keeps its own
        fresh.coreConfig().setSupportedBehaviorVersions(engine.coreConfig().supportedBehaviorVersions());
        final BalanceSnapshotCodec.Decoded decoded =
                BalanceSnapshotCodec.deserialize(buffer, SnapshotEnvelope.payloadOffset(), payloadLength, fresh);
        if (decoded.bytesConsumed != payloadLength) {
            throw new IllegalStateException("snapshot payload length mismatch: " + decoded.bytesConsumed
                    + " consumed of " + payloadLength);
        }
        fresh.verifyRestoredInvariants();
        fresh.setEventSink(sink);
        engine = fresh;
        projector = new SettlementProjector(fresh);
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
            case "TICK":
                // Advance logical time; the deadline sweep fires as a log entry at the new time (K3-D).
                clock.advance(Long.parseLong(tokens[1]));
                engine.sweep(clock.now());
                break;
            case "SWEEP":
                engine.sweep(clock.now());
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
            case "SET_JOURNAL":
                // Arm/disarm the money journal through the log, exactly as production does. Replicated
                // state: it rides the snapshot, so a restore mid-scenario must keep it.
                setMoneyJournalCommand.reset();
                setMoneyJournalCommand.setEnabled(Long.parseLong(tokens[1]) != 0);
                engine.applyCommand(AssetsEngine.CMD_SET_MONEY_JOURNAL, setMoneyJournalCommand, clock.now());
                break;
            case "SNAPSHOT":
                snapshotRoundTrip();
                break;
            case "CORECONFIG":
                // RC1 S7: the compiled markets and assets as a CoreConfig; tick/min/max/lot apply to every market
                engine.applyCoreConfig(coreConfigFor(reqLong(kv, "ver"), reqLong(kv, "behavior"),
                        kv.containsKey("tick") ? reqAmount(kv, "tick") : 1L, kv.containsKey("min") ? reqAmount(kv, "min") : 1L,
                        kv.containsKey("max") ? reqAmount(kv, "max") : 1_000_000_000_000_000L, kv.containsKey("lot") ? reqAmount(kv, "lot") : 0L,
                        kv.containsKey("corr") ? reqLong(kv, "corr") : 0L), clock.now());
                break;
            // ---- RC1 S5: ME feed events (bridge class), amend, quarantine, queries ----
            case "METRADE": {
                final long taker = reqLong(kv, "taker");
                final long maker = reqLong(kv, "maker");
                final com.openexchange.assets.application.engine.order.CoreOrder to = engine.orders().orders().get(taker);
                final com.openexchange.assets.application.engine.order.CoreOrder mo = engine.orders().orders().get(maker);
                final long takerUser = kv.containsKey("takerUser") ? reqLong(kv, "takerUser") : (to == null ? 0L : to.userId);
                final long makerUser = kv.containsKey("makerUser") ? reqLong(kv, "makerUser") : (mo == null ? 0L : mo.userId);
                final long takerLeg = kv.containsKey("takerLeg") ? reqLong(kv, "takerLeg") : (to == null ? 0L : to.currentLegId);
                final long makerLeg = kv.containsKey("makerLeg") ? reqLong(kv, "makerLeg") : (mo == null ? 0L : mo.currentLegId);
                // RC2 S7: through the 2.4 entry point (under v1/v2 it delegates to the unchanged applyTrade)
                engine.onMeTrade(kv.containsKey("seq") ? reqLong(kv, "seq") : ++feedPosition, optLong(kv, "jseq", 0L),
                        reqLong(kv, "id"), reqInt(kv, "market"),
                        takerLeg, takerUser, makerLeg, makerUser, reqAmount(kv, "px"), reqAmount(kv, "qty"), reqLong(kv, "takerBuy") != 0,
                        clock.now(), taker, maker, optLong(kv, "epoch", 0L), clock.now());
                break;
            }
            case "METERMINAL": {
                final long order = reqLong(kv, "order");
                final com.openexchange.assets.application.engine.order.CoreOrder o = engine.orders().orders().get(order);
                engine.onMeTerminal(kv.containsKey("seq") ? reqLong(kv, "seq") : ++feedPosition, optLong(kv, "jseq", 0L),
                        reqLong(kv, "leg"),
                        kv.containsKey("u") ? reqLong(kv, "u") : (o == null ? 0L : o.userId),
                        kv.containsKey("market") ? reqInt(kv, "market") : (o == null ? 0 : o.marketId),
                        reqInt(kv, "status"), (int) optLong(kv, "meReason", 0L), clock.now(), order, optLong(kv, "epoch", 0L),
                        clock.now());
                break;
            }
            case "MEOUTCOME": {
                final long order = reqLong(kv, "order");
                final int kind = reqInt(kv, "kind");
                final int attempt = reqInt(kv, "attempt");
                final com.openexchange.assets.application.engine.order.CoreOrder o = engine.orders().orders().get(order);
                engine.onMeOutcome(kv.containsKey("seq") ? reqLong(kv, "seq") : ++feedPosition, optLong(kv, "jseq", 0L), order,
                        ((long) attempt << 2) | kind,
                        o == null ? 0L : o.userId, order, kv.containsKey("old") ? reqLong(kv, "old") : 0L, 0L, 0L, 0L,
                        o == null ? 0 : o.marketId, kind, reqInt(kv, "result"), kv.containsKey("leg") ? reqLong(kv, "leg") : 0L,
                        kv.containsKey("reason") ? reqInt(kv, "reason") : 0, optLong(kv, "oldCancelled", 0L) != 0, clock.now(),
                        optLong(kv, "epoch", 0L), clock.now());
                break;
            }
            case "AMEND": {
                final com.openexchange.assets.domain.commands.AmendOrderCommand c = new com.openexchange.assets.domain.commands.AmendOrderCommand();
                c.reset();
                c.correlationId = kv.containsKey("corr") ? reqLong(kv, "corr") : 0L;
                c.userId = reqLong(kv, "u");
                c.orderId = reqLong(kv, "order");
                c.newPrice = kv.containsKey("px") ? reqAmount(kv, "px") : 0L;
                c.newQuantity = kv.containsKey("qty") ? reqAmount(kv, "qty") : 0L;
                c.requestIdLow = kv.containsKey("rid") ? reqLong(kv, "rid") : 0L;
                engine.orders().applyAmend(c, clock.now());
                break;
            }
            // ---- RC2 S7: the v3 feed verbs (contract section 15) ----
            case "V3GENESIS":
                // CoreConfig 1 -> 2 -> 3 (genesisId 0x5EED, test seam widened) and, unless bind=0, 68 FRESH jseq 1
                if (optLong(kv, "bind", 1L) != 0) {
                    V3Genesis.boot(engine, clock.now());
                } else {
                    V3Genesis.switchTo3(engine, clock.now());
                }
                break;
            case "MECONFIG":
                engine.onMeEngineConfig(kv.containsKey("seq") ? reqLong(kv, "seq") : ++feedPosition, reqLong(kv, "jseq"),
                        reqLong(kv, "epoch"), reqLong(kv, "cfg"), reqLong(kv, "behavior"), reqLong(kv, "cap"),
                        reqInt(kv, "maxWire"), com.openexchange.assets.domain.ConfigDecision.valueOf(req(kv, "decision")),
                        clock.now(), id -> { });
                break;
            case "MERELEASE":
                engine.onMeRelease(kv.containsKey("seq") ? reqLong(kv, "seq") : ++feedPosition, reqLong(kv, "jseq"),
                        reqLong(kv, "epoch"), clock.now());
                break;
            case "FEEDHALT":
                engine.applyFeedHalt(optLong(kv, "session", 0L), optLong(kv, "corr", 0L),
                        com.openexchange.assets.domain.FeedHaltReason.valueOf(req(kv, "reason")), optLong(kv, "epoch", 0L),
                        reqLong(kv, "expected"), reqLong(kv, "seen"), optLong(kv, "bridge", 0L), clock.now());
                break;
            case "LATCHCLEAR":
                engine.applyFeedLatchClear(optLong(kv, "corr", 0L),
                        com.openexchange.assets.domain.FeedHaltReason.valueOf(req(kv, "reason")), reqLong(kv, "jseq"), clock.now());
                break;
            case "FEEDPOISON":
                engine.onFeedPoison(reqInt(kv, "tpl"), reqLong(kv, "jseq"), reqLong(kv, "epoch"), optLong(kv, "tradeId", 0L),
                        clock.now());
                break;
            case "QUARANTINE":
                engine.orders().applyQuarantine(kv.containsKey("corr") ? reqLong(kv, "corr") : 0L, reqLong(kv, "order"), clock.now());
                break;
            case "ORDERSNAP":
                engine.orders().applyRequestOrderSnapshot(kv.containsKey("corr") ? reqLong(kv, "corr") : 0L, reqLong(kv, "u"),
                        kv.containsKey("from") ? reqLong(kv, "from") : 0L, true);
                break;
            case "PENDINGDISPATCH":
                engine.orders().applyRequestPendingDispatch(kv.containsKey("corr") ? reqLong(kv, "corr") : 0L,
                        kv.containsKey("from") ? reqLong(kv, "from") : 0L, kv.containsKey("max") ? reqInt(kv, "max") : 0);
                break;
            // ---- RC1 S4: order lifecycle verbs (admission, cancel, risk config, breaker, tick, gate) ----
            case "GATE": {
                final int market = reqInt(kv, "market");
                final boolean open = reqLong(kv, "open") != 0;
                engine.applyOrderEntryGate(optLong(kv, "corr", 0L), market, open ? com.openexchange.assets.domain.GateState.OPEN
                        : com.openexchange.assets.domain.GateState.SUSPENDED, kv.containsKey("reason")
                        ? com.openexchange.assets.domain.GateReason.valueOf(req(kv, "reason"))
                        : com.openexchange.assets.domain.GateReason.ADMIN, clock.now());
                break;
            }
            case "RISKCFG": {
                final com.openexchange.assets.domain.commands.RiskConfigSetCommand c =
                        new com.openexchange.assets.domain.commands.RiskConfigSetCommand();
                c.reset();
                c.correlationId = 0L;
                c.marketId = reqInt(kv, "market");
                c.configVersion = reqLong(kv, "ver");
                final com.openexchange.assets.application.engine.order.MarketRisk cur = engine.orders().market(
                        c.marketId >= 0 && c.marketId < com.openexchange.assets.application.engine.order.UserRisk.MARKET_SLOTS ? c.marketId : 0);
                c.minQuantity = kv.containsKey("minQty") ? reqAmount(kv, "minQty") : cur.minQuantity;
                c.maxQuantity = kv.containsKey("maxQty") ? reqAmount(kv, "maxQty") : cur.maxQuantity;
                c.minNotional = kv.containsKey("minNotional") ? reqAmount(kv, "minNotional") : cur.minNotional;
                c.maxNotional = kv.containsKey("maxNotional") ? reqAmount(kv, "maxNotional") : cur.maxNotional;
                c.priceCollarBps = kv.containsKey("collarBps") ? reqInt(kv, "collarBps") : cur.priceCollarBps;
                c.maxOrdersPerSec = kv.containsKey("perSec") ? reqInt(kv, "perSec") : cur.maxOrdersPerSec;
                c.maxOrdersPerMin = kv.containsKey("perMin") ? reqInt(kv, "perMin") : cur.maxOrdersPerMin;
                c.maxOpenOrders = kv.containsKey("maxOpen") ? reqInt(kv, "maxOpen") : cur.maxOpenOrders;
                c.maxPositionPerMarket = kv.containsKey("maxPos") ? reqAmount(kv, "maxPos") : cur.maxPositionPerMarket;
                c.marketBuySlippageBps = kv.containsKey("slipBps") ? reqInt(kv, "slipBps") : cur.marketBuySlippageBps;
                engine.orders().applyRiskConfigSet(c, clock.now());
                break;
            }
            case "BREAKER":
                engine.orders().applyCircuitBreaker(0L, reqInt(kv, "market"), reqLong(kv, "trip") != 0, clock.now());
                break;
            case "MARKETTICK":
                engine.orders().applyMarketTick(reqInt(kv, "market"), reqLong(kv, "ver"), reqAmount(kv, "bid"), reqAmount(kv, "ask"),
                        kv.containsKey("last") ? reqAmount(kv, "last") : 0L, kv.containsKey("lastTradeId") ? reqLong(kv, "lastTradeId") : 0L,
                        optLong(kv, "epoch", 0L), clock.now());
                break;
            case "CREATE": {
                final com.openexchange.assets.domain.commands.CreateOrderCommand c =
                        new com.openexchange.assets.domain.commands.CreateOrderCommand();
                c.reset();
                c.correlationId = kv.containsKey("corr") ? reqLong(kv, "corr") : 0L;
                c.userId = reqLong(kv, "u");
                c.marketId = reqInt(kv, "market");
                c.side = reqInt(kv, "side");
                c.orderType = reqInt(kv, "type");
                c.timeInForce = kv.containsKey("tif") ? reqInt(kv, "tif") : 0;
                c.price = kv.containsKey("px") ? reqAmount(kv, "px") : 0L;
                c.quantity = reqAmount(kv, "qty");
                c.budget = kv.containsKey("budget") ? reqAmount(kv, "budget") : 0L;
                c.expiresAtMs = kv.containsKey("exp") ? reqLong(kv, "exp") : 0L;
                c.stopPrice = kv.containsKey("stop") ? reqAmount(kv, "stop") : 0L;
                c.trailingDelta = kv.containsKey("delta") ? reqAmount(kv, "delta") : 0L;
                c.displayQty = kv.containsKey("display") ? reqAmount(kv, "display") : 0L;
                c.clientOrderIdLow = kv.containsKey("cid") ? reqLong(kv, "cid") : 0L;
                c.requestIdLow = kv.containsKey("rid") ? reqLong(kv, "rid") : 0L;
                c.flags = kv.containsKey("flags") ? reqInt(kv, "flags") : 0;
                engine.orders().applyCreate(c, clock.now());
                break;
            }
            case "CANCEL": {
                final com.openexchange.assets.domain.commands.CancelOrderCommand c =
                        new com.openexchange.assets.domain.commands.CancelOrderCommand();
                c.reset();
                c.correlationId = kv.containsKey("corr") ? reqLong(kv, "corr") : 0L;
                c.userId = reqLong(kv, "u");
                c.orderId = kv.containsKey("order") ? reqLong(kv, "order") : 0L;
                c.origClientOrderIdLow = kv.containsKey("cid") ? reqLong(kv, "cid") : 0L;
                c.requestIdLow = kv.containsKey("rid") ? reqLong(kv, "rid") : 0L;
                engine.orders().applyCancel(c, clock.now());
                break;
            }
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
        c.setOmsManagedRelease("1".equals(kv.get("omsManaged")));
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
        restoreFrom(snapshotBytes());
    }

    // ---- parsing helpers (the text conventions live in the kit) ----

    /** The compiled Market.ALL / Asset tables as a CoreConfig command (RC1 S7 DSL and tests). */
    public static com.openexchange.assets.domain.commands.CoreConfigCommand coreConfigFor(long version, long behavior, long tick,
            long min, long max, long lot, long correlationId) {
        final com.openexchange.assets.domain.commands.CoreConfigCommand c = new com.openexchange.assets.domain.commands.CoreConfigCommand();
        c.reset();
        c.correlationId = correlationId;
        c.configVersion = version;
        c.behaviorVersion = behavior;
        final java.util.List<com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef> ms = new java.util.ArrayList<>();
        for (final com.openexchange.assets.domain.Market m : com.openexchange.assets.domain.Market.ALL) {
            ms.add(new com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef(m.marketId(), m.symbol(),
                    m.baseAsset().id(), m.quoteAsset().id(), tick, min, max, lot));
        }
        c.markets = ms.toArray(new com.openexchange.assets.domain.commands.CoreConfigCommand.MarketDef[0]);
        final java.util.List<com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef> as = new java.util.ArrayList<>();
        for (final com.openexchange.assets.domain.Asset a : com.openexchange.assets.domain.Asset.values()) {
            as.add(new com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef(a.id(), a.name(), 8));
        }
        c.assets = as.toArray(new com.openexchange.assets.domain.commands.CoreConfigCommand.AssetDef[0]);
        return c;
    }

    private static Map<String, String> parseKv(String[] tokens) {
        return ScenarioText.parseKv(tokens);
    }

    private static String req(Map<String, String> kv, String key) {
        return ScenarioText.req(kv, key);
    }

    private static long reqLong(Map<String, String> kv, String key) {
        return ScenarioText.reqLong(kv, key);
    }

    private static long optLong(Map<String, String> kv, String key, long fallback) {
        return kv.containsKey(key) ? reqLong(kv, key) : fallback;
    }

    private static int reqInt(Map<String, String> kv, String key) {
        return ScenarioText.reqInt(kv, key);
    }

    /** A decimal money amount converted to 8dp fixed-point. */
    private static long reqAmount(Map<String, String> kv, String key) {
        return FixedPoint.fromDouble(Double.parseDouble(req(kv, key)));
    }
}

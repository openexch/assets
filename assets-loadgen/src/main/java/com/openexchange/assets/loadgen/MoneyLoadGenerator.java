package com.openexchange.assets.loadgen;

import com.match.infrastructure.generated.CreateOrderEncoder;
import com.match.infrastructure.generated.OrderSide;
import com.match.infrastructure.generated.OrderStatusBatchDecoder;
import com.match.infrastructure.generated.OrderStatusUpdateDecoder;
import com.match.infrastructure.generated.OrderType;
import com.match.infrastructure.generated.TradeExecutionBatchDecoder;
import com.openexchange.assets.domain.FixedPoint;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;

import java.io.PrintStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark driver for the full money-critical hot path, with no OMS in the loop:
 *
 *   AE Hold -> HoldAck -> ME CreateOrder(omsOrderId) -> trade -> settlement journal
 *     -> bridge -> AE Settle -> (feed terminal releases residual hold)
 *
 * The generator performs the OMS's synchronous fail-closed hold gate itself: no order is
 * sent to the matching engine before its fund lock is acknowledged by the Assets Engine.
 * The Hold's orderId and the ME order's omsOrderId are the SAME value - that is the key
 * the settlement feed uses to draw down and release the hold.
 *
 * Measurement rules (see BENCH methodology):
 *  - Single clock: every latency is recorded on this process's clock. The order->settled
 *    interval joins ME TradeExecution egress (tradeId, taker/makerOmsOrderId) to AE
 *    SettlementApplied egress (tradeId), both observed here.
 *  - Coordinated-omission safe: each order's t0 is its SCHEDULED send slot on the fixed
 *    rate timeline. The schedule is never reset when the generator falls behind, so delay
 *    caused by a slow system lands in the tail percentiles instead of being hidden.
 *  - Histograms only record inside the measurement window (after warmup); counters always run.
 *
 * Single-threaded by design: one duty thread owns both AeronCluster sessions (they are not
 * thread-safe), which is also what makes the single-clock rule trivially true.
 */
public final class MoneyLoadGenerator implements AutoCloseable {

    // ---- fixed-point (8dp) ----
    private static final double FP_SCALE = 1e8;
    private static long fp(final double v) { return Math.round(v * FP_SCALE); }

    // ---- market/scenario constants (mirrors match-loadtest AGGRESSIVE, bias configurable) ----
    private static final int MARKET_ID = 1;              // BTC-USD
    private static final int ASSET_USD = 0;
    private static final int ASSET_BTC = 1;
    private static final double MID_PRICE = 120_000.0;
    /**
     * Market-BUY budget price: must clear the WORST possible crossing (asks go up to
     * mid*(1+0.01) in this scenario). The hold equals the budget, and the engine caps the
     * fill cost at the budget, so budget >= worst ask price makes a settle shortfall
     * impossible. Dry-run 2026-07-24 proved budget==mid faults ~13% of trades (AE's D5
     * shortfall hardening absorbed them - conservation held - but a benchmark run must be
     * fault-free to be publishable).
     */
    private static final double MARKET_BUY_BUDGET_PRICE = MID_PRICE * 1.02;

    private static final long PREFUND_CORR_BASE = 1L << 62;
    private static final int OFFER_RETRIES = 10;

    // ---- config ----
    private final int rate;
    private final int durationSec;          // measurement window, EXCLUDES warmup (unlike match-loadtest)
    private final int warmupSec;
    private final int users;
    private final int window;               // max in-flight lifecycles (hold-sent .. first-status)
    private final double bidBias;           // 0.5 = no net USD->BTC drift (long-run safe)
    private final double limitRatio;
    private final int sampleShift;          // order->settled lifecycle sampling: 1 in 2^shift
    private final Path outDir;
    private final boolean prefund;
    private final long prefundUsdFp;
    private final long prefundBtcFp;

    // ---- transport ----
    private final MediaDriver driver;
    private final AeronCluster me;
    private final AeronCluster ae;

    // ---- SBE (ME side imported, AE side fully qualified to avoid header-codec name clash) ----
    private final com.match.infrastructure.generated.MessageHeaderEncoder meHeaderEnc =
            new com.match.infrastructure.generated.MessageHeaderEncoder();
    private final com.match.infrastructure.generated.MessageHeaderDecoder meHeaderDec =
            new com.match.infrastructure.generated.MessageHeaderDecoder();
    private final CreateOrderEncoder createOrderEnc = new CreateOrderEncoder();
    private final OrderStatusUpdateDecoder statusDec = new OrderStatusUpdateDecoder();
    private final OrderStatusBatchDecoder statusBatchDec = new OrderStatusBatchDecoder();
    private final TradeExecutionBatchDecoder tradeBatchDec = new TradeExecutionBatchDecoder();

    private final com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder aeHeaderEnc =
            new com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder();
    private final com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder aeHeaderDec =
            new com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder();
    private final com.openexchange.assets.infrastructure.generated.HoldEncoder holdEnc =
            new com.openexchange.assets.infrastructure.generated.HoldEncoder();
    private final com.openexchange.assets.infrastructure.generated.ReleaseEncoder releaseEnc =
            new com.openexchange.assets.infrastructure.generated.ReleaseEncoder();
    private final com.openexchange.assets.infrastructure.generated.DepositEncoder depositEnc =
            new com.openexchange.assets.infrastructure.generated.DepositEncoder();
    private final com.openexchange.assets.infrastructure.generated.HoldAckDecoder holdAckDec =
            new com.openexchange.assets.infrastructure.generated.HoldAckDecoder();
    private final com.openexchange.assets.infrastructure.generated.HoldRejectDecoder holdRejectDec =
            new com.openexchange.assets.infrastructure.generated.HoldRejectDecoder();
    private final com.openexchange.assets.infrastructure.generated.DepositAckDecoder depositAckDec =
            new com.openexchange.assets.infrastructure.generated.DepositAckDecoder();
    private final com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder settleDec =
            new com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder();
    private final com.openexchange.assets.infrastructure.generated.SettleFaultDecoder settleFaultDec =
            new com.openexchange.assets.infrastructure.generated.SettleFaultDecoder();

    private final ExpandableDirectByteBuffer meBuffer = new ExpandableDirectByteBuffer(256);
    private final ExpandableDirectByteBuffer aeBuffer = new ExpandableDirectByteBuffer(256);

    // ---- lifecycle tracking (duty thread only) ----
    private static final class Pending {
        long omsOrderId;
        long scheduledNs;
        long userId;
        boolean isBid;
        boolean isLimit;
        long priceFp;
        long qtyFp;
        long totalPriceFp;
        long holdAmountFp;
        int holdAssetId;
        boolean orderSent;
        boolean sampled;
    }

    private final ArrayDeque<Pending> pool = new ArrayDeque<>();
    /** hold-sent (awaiting HoldAck) OR order-sent (awaiting first status). Bounded by {@link #window}. */
    private final Long2ObjectHashMap<Pending> pending = new Long2ObjectHashMap<>();
    /** sampled orders awaiting their first settle: omsOrderId -> scheduledNs. */
    private final Long2LongHashMap sampledOrders = new Long2LongHashMap(-1);
    /** tradeId -> first-seen-on-ME-egress ns. Removed when its settle is observed. */
    private final Long2LongHashMap tradeSeenNs = new Long2LongHashMap(-1);
    /**
     * tradeId -> SettlementApplied-seen ns for settles that arrived BEFORE the ME egress
     * trade observation. Dry-run finding: the settlement path (journal -> bridge -> AE,
     * ~1.5ms) is usually FASTER than the ME market-data egress batch flush (~20ms), so the
     * settle routinely wins the race; the join must work in both directions.
     */
    private final Long2LongHashMap settleSeenNs = new Long2LongHashMap(-1);
    /** tradeId -> scheduledNs of a SAMPLED parent order (taker preferred). */
    private final Long2LongHashMap tradeToSampledOrder = new Long2LongHashMap(-1);
    private static final int TRADE_MAP_CAP = 4_000_000;

    // ---- histograms (ns) ----
    private final Histogram holdAckHist = newHist();
    private final Histogram orderAckHist = newHist();
    private final Histogram tradeToSettledHist = newHist();   // trade observed first (settle lag)
    private final Histogram settleLeadHist = newHist();       // settle observed first (its lead)
    private final Histogram orderToSettledHist = newHist();
    private static Histogram newHist() { return new Histogram(TimeUnit.MINUTES.toNanos(2), 3); }

    // ---- counters ----
    private long holdsSent, holdAcks, holdRejects, ordersSent, orderSendFailures,
            firstStatuses, tradesSeen, settlesSeen, settleFaults, depositAcks,
            meBackpressure, aeBackpressure, tradeMapSkips, settleLedJoins, tradeLedJoins,
            scheduleBacklogMax;

    private long measureStartNs;
    private boolean measuring;
    private final Random rand = new Random(42); // fixed seed: reproducible order stream

    /**
     * omsOrderId base: epoch-millis shifted so CONSECUTIVE RUNS against the same (not
     * re-genesis'd) clusters can never collide ids - a collision merges this run's holds
     * with a previous run's leftovers (dry-run 2: 34k hold rejects + garbage SettleFaults).
     * ~2^20 ids per ms of headroom; stays far below 2^63.
     */
    private final long orderIdBase = System.currentTimeMillis() << 20;

    private MoneyLoadGenerator(final String[] args) {
        this.rate = intArg(args, "--rate", 10_000);
        this.durationSec = intArg(args, "--duration", 60);
        this.warmupSec = intArg(args, "--warmup", 30);
        this.users = intArg(args, "--users", 16);
        this.window = intArg(args, "--window", 8192);
        this.bidBias = doubleArg(args, "--bid-bias", 0.5);
        this.limitRatio = doubleArg(args, "--limit-ratio", 0.40);
        this.sampleShift = intArg(args, "--sample-shift", 6); // 1/64 lifecycle sampling
        this.outDir = Path.of(strArg(args, "--out", "results"));
        this.prefund = !hasFlag(args, "--no-prefund");
        this.prefundUsdFp = fp(doubleArg(args, "--prefund-usd", 50_000_000_000.0)); // $50B/user
        this.prefundBtcFp = fp(doubleArg(args, "--prefund-btc", 10_000_000.0));     // 10M BTC/user

        final List<String> meHosts = List.of(strArg(args, "--me-hosts", "127.0.0.1").split(","));
        final List<String> aeHosts = List.of(strArg(args, "--ae-hosts", "127.0.0.1").split(","));
        final int mePortBase = intArg(args, "--me-port-base", 9000);
        final int aePortBase = intArg(args, "--ae-port-base", 9300);
        final String egressHost = resolveEgressHost(strArg(args, "--egress-host", null), meHosts.get(0));

        this.driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName("/dev/shm/aeron-moneyload-" + ProcessHandle.current().pid())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .errorHandler(t -> t.printStackTrace(System.err)));

        System.out.printf("connecting ME %s (base %d) / AE %s (base %d), egressHost=%s%n",
                meHosts, mePortBase, aeHosts, aePortBase, egressHost);

        this.me = connect(driver, ingressEndpoints(meHosts, mePortBase), egressHost, new MeEgress());
        this.ae = connect(driver, ingressEndpoints(aeHosts, aePortBase), egressHost, new AeEgress());
    }

    /** {@code i=host:port} CSV; port = base + i*100 + 2 (client-facing offset, both clusters). */
    static String ingressEndpoints(final List<String> hosts, final int portBase) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hosts.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(i).append('=').append(hosts.get(i).trim()).append(':').append(portBase + i * 100 + 2);
        }
        return sb.toString();
    }

    /**
     * The egress endpoint must be reachable from the cluster leader (match#151 class of bug:
     * an iface-name guess silently falls back to loopback on cloud hosts). Resolution order:
     * explicit arg/env, then the OS's routed source address toward the first ME host, then loopback.
     */
    static String resolveEgressHost(final String explicit, final String firstMeHost) {
        if (explicit != null && !explicit.isBlank()) return explicit;
        final String env = System.getenv("EGRESS_HOST");
        if (env != null && !env.isBlank()) return env;
        try (DatagramSocket probe = new DatagramSocket()) {
            probe.connect(new InetSocketAddress(InetAddress.getByName(firstMeHost), 9)); // discard port
            final InetAddress local = probe.getLocalAddress();
            if (local != null && !local.isAnyLocalAddress()) return local.getHostAddress();
        } catch (final Exception ignore) {
            // fall through
        }
        return "127.0.0.1";
    }

    private static AeronCluster connect(final MediaDriver driver, final String ingressEndpoints,
                                        final String egressHost, final EgressListener listener) {
        return AeronCluster.connect(new AeronCluster.Context()
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10))
                .aeronDirectoryName(driver.aeronDirectoryName())
                .ingressChannel("aeron:udp?term-length=16m|mtu=8k")
                .ingressEndpoints(ingressEndpoints)
                .egressChannel("aeron:udp?endpoint=" + egressHost + ":0")
                .egressListener(listener)
                .errorHandler(t -> t.printStackTrace(System.err)));
    }

    // ------------------------------------------------------------------ pre-fund

    private void prefundUsers() {
        System.out.printf("pre-funding %d users (USD %.0f, BTC %.0f each)...%n",
                users, prefundUsdFp / FP_SCALE, prefundBtcFp / FP_SCALE);
        final int expected = users * 2;
        for (int u = 0; u < users; u++) {
            sendDeposit(PREFUND_CORR_BASE + u * 2L, u, ASSET_USD, prefundUsdFp);
            sendDeposit(PREFUND_CORR_BASE + u * 2L + 1, u, ASSET_BTC, prefundBtcFp);
        }
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (depositAcks < expected) {
            pollBoth();
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("pre-fund incomplete: " + depositAcks + "/" + expected
                        + " DepositAcks in 30s");
            }
        }
        System.out.println("pre-fund complete: " + depositAcks + " deposits acked");
    }

    private void sendDeposit(final long corrId, final long userId, final int assetId, final long amountFp) {
        depositEnc.wrapAndApplyHeader(aeBuffer, 0, aeHeaderEnc)
                .correlationId(corrId).userId(userId).assetId(assetId).amount(amountFp);
        final int len = aeHeaderEnc.encodedLength() + depositEnc.encodedLength();
        offerWithRetry(ae, aeBuffer, len, true);
    }

    // ------------------------------------------------------------------ hot loop

    private void run() {
        if (prefund) prefundUsers();

        final long nanosPerOrder = 1_000_000_000L / rate;
        final long startNs = System.nanoTime();
        this.measureStartNs = startNs + TimeUnit.SECONDS.toNanos(warmupSec);
        final long sendEndNs = measureStartNs + TimeUnit.SECONDS.toNanos(durationSec);
        long nextScheduledNs = startNs;
        long lastKeepAliveNs = startNs;
        long orderSeq = 0;

        System.out.printf("running: %d orders/s, warmup %ds, measured window %ds, window %d, users %d%n",
                rate, warmupSec, durationSec, window, users);

        while (true) {
            final long now = System.nanoTime();

            if (!measuring && now >= measureStartNs) {
                measuring = true;
                System.out.println("warmup done, measurement window open");
            }

            if (now >= sendEndNs) break;

            // Send exactly on the fixed timeline. The schedule NEVER resets to `now`:
            // if we are behind (window full / backpressure), orders go out late and their
            // latency is charged from the slot they SHOULD have occupied (CO-safe).
            if (now >= nextScheduledNs) {
                final long backlog = (now - nextScheduledNs) / nanosPerOrder;
                if (backlog > scheduleBacklogMax) scheduleBacklogMax = backlog;
                if (pending.size() < window) {
                    ++orderSeq;
                    startLifecycle(orderIdBase + orderSeq, orderSeq, nextScheduledNs);
                    nextScheduledNs += nanosPerOrder;
                }
                // window full: keep polling; the slot stays owed and is sent as soon as
                // capacity frees, still stamped with its original scheduled time.
            }

            pollBoth();

            if (now - lastKeepAliveNs > 250_000_000L) {
                lastKeepAliveNs = now;
                try { me.sendKeepAlive(); } catch (final Exception ignore) { }
                try { ae.sendKeepAlive(); } catch (final Exception ignore) { }
            }
        }

        drain();
        report(startNs);
    }

    private void startLifecycle(final long omsOrderId, final long seq, final long scheduledNs) {
        final Pending p = pool.isEmpty() ? new Pending() : pool.pop();
        p.omsOrderId = omsOrderId;
        p.scheduledNs = scheduledNs;
        p.userId = seq % users;
        p.isLimit = rand.nextDouble() < limitRatio;
        p.isBid = rand.nextDouble() < bidBias;
        p.orderSent = false;
        p.sampled = (seq & ((1L << sampleShift) - 1)) == 0;

        final double spread = 0.002 + rand.nextDouble() * 0.008;
        final double price = p.isBid ? MID_PRICE * (1.0 - spread) : MID_PRICE * (1.0 + spread);
        final double qty = 0.1 + rand.nextDouble() * 1.9;
        final double tickPrice = Math.rint(price); // $1 tick, engine rejects off-tick

        p.priceFp = p.isLimit ? fp(tickPrice) : 0L;
        p.qtyFp = fp(qty);
        // LIMIT: compute the notional with the AE's OWN FixedPoint.multiply (truncating),
        // NOT double math - the settle draws exactly multiply(price, qty) per fill
        // (AssetsEngine quote leg), and a double-rounded hold is 1 unit short ~13% of the
        // time (dry-run 3: 8026 deterministic SettleFaults, all uncovered=0).
        // Truncation sums safely across partial fills (floor(a)+floor(b) <= floor(a+b)).
        p.totalPriceFp = p.isLimit ? FixedPoint.multiply(p.priceFp, p.qtyFp)
                : (p.isBid ? fp(MARKET_BUY_BUDGET_PRICE * qty) : 0L);
        if (p.isBid) {
            p.holdAssetId = ASSET_USD;
            p.holdAmountFp = p.totalPriceFp;
        } else {
            p.holdAssetId = ASSET_BTC;
            p.holdAmountFp = p.qtyFp;
        }

        // Hold.orderId == ME omsOrderId == correlationId (one id, three roles).
        holdEnc.wrapAndApplyHeader(aeBuffer, 0, aeHeaderEnc)
                .correlationId(p.omsOrderId)
                .orderId(p.omsOrderId)
                .userId(p.userId)
                .assetId(p.holdAssetId)
                .amount(p.holdAmountFp)
                .omsManagedRelease(com.openexchange.assets.infrastructure.generated.BoolFlag.FALSE);
        final int len = aeHeaderEnc.encodedLength() + holdEnc.encodedLength();
        if (offerWithRetry(ae, aeBuffer, len, true)) {
            holdsSent++;
            pending.put(p.omsOrderId, p);
        } else {
            pool.push(p); // could not even queue the hold; slot abandoned
        }
    }

    /** HoldAck arrived: the gate opens, the ME order goes out (same duty thread). */
    private void onHoldAck(final long corrId) {
        final Pending p = pending.get(corrId);
        if (p == null || p.orderSent) return;
        holdAcks++;
        if (measuring) holdAckHist.recordValue(clamp(System.nanoTime() - p.scheduledNs));
        sendMeOrder(p);
    }

    private void onHoldReject(final long corrId) {
        final Pending p = pending.remove(corrId);
        holdRejects++;
        if (p != null) pool.push(p);
    }

    private void sendMeOrder(final Pending p) {
        createOrderEnc.wrapAndApplyHeader(meBuffer, 0, meHeaderEnc);
        createOrderEnc.userId(p.userId);
        createOrderEnc.price(p.priceFp);
        createOrderEnc.quantity(p.qtyFp);
        createOrderEnc.totalPrice(p.totalPriceFp);
        createOrderEnc.marketId(MARKET_ID);
        createOrderEnc.orderType(p.isLimit ? OrderType.LIMIT : OrderType.MARKET);
        createOrderEnc.orderSide(p.isBid ? OrderSide.BID : OrderSide.ASK);
        createOrderEnc.omsOrderId(p.omsOrderId);
        final int len = meHeaderEnc.encodedLength() + createOrderEnc.encodedLength();

        if (offerWithRetry(me, meBuffer, len, false)) {
            ordersSent++;
            p.orderSent = true;
            // Lifecycle sampling is armed at SEND time: market orders trade at the engine
            // before their first status reaches us (egress batching), so arming at
            // first-status missed nearly all taker lifecycles (dry-run n=2).
            if (p.sampled) sampledOrders.put(p.omsOrderId, p.scheduledNs);
        } else {
            // The hold is placed but the order will never exist: release it so the run
            // ends with clean conservation instead of a leaked lock.
            orderSendFailures++;
            releaseEnc.wrapAndApplyHeader(aeBuffer, 0, aeHeaderEnc)
                    .orderId(p.omsOrderId).userId(p.userId).amount(-1L);
            offerWithRetry(ae, aeBuffer, aeHeaderEnc.encodedLength() + releaseEnc.encodedLength(), true);
            pending.remove(p.omsOrderId);
            pool.push(p);
        }
    }

    /** First ME status for the order: ack latency, lifecycle slot freed. */
    private void onFirstStatus(final long omsOrderId) {
        final Pending p = pending.get(omsOrderId);
        if (p == null || !p.orderSent) return;
        firstStatuses++;
        if (measuring) orderAckHist.recordValue(clamp(System.nanoTime() - p.scheduledNs));
        pending.remove(omsOrderId);
        pool.push(p);
    }

    private void onTrade(final long tradeId, final long takerOmsOrderId, final long makerOmsOrderId) {
        final long now = System.nanoTime();
        // Bidirectional join. If the settle already arrived (usual case: journal->bridge->AE
        // beats the ME egress batch flush), close the pair now and record the settle's LEAD.
        final long settleAt = settleSeenNs.remove(tradeId);
        if (settleAt != -1) {
            tradesSeen++;
            settleLedJoins++; // pair closed here; the SETTLE had led
            if (measuring) settleLeadHist.recordValue(clamp(now - settleAt));
        } else if (tradeSeenNs.get(tradeId) == -1) {
            tradesSeen++;
            if (tradeSeenNs.size() < TRADE_MAP_CAP) tradeSeenNs.put(tradeId, now); else tradeMapSkips++;
        } else {
            return; // duplicate observation of an already-mapped trade
        }
        // A sampled order's lifecycle closes at its FIRST fill's settle; remove so the
        // sampled map only holds not-yet-filled sampled orders (bounded).
        long sampledScheduled = sampledOrders.remove(takerOmsOrderId);
        if (sampledScheduled == -1) sampledScheduled = sampledOrders.remove(makerOmsOrderId);
        if (sampledScheduled != -1) {
            if (settleAt != -1) {
                // settle already observed: lifecycle closes here
                if (measuring) orderToSettledHist.recordValue(clamp(now - sampledScheduled));
            } else if (tradeToSampledOrder.size() < TRADE_MAP_CAP) {
                tradeToSampledOrder.put(tradeId, sampledScheduled);
            }
        }
    }

    private void onSettlementApplied(final long tradeId) {
        settlesSeen++;
        final long now = System.nanoTime();
        final long seen = tradeSeenNs.remove(tradeId);
        if (seen != -1) {
            tradeLedJoins++; // pair closed here; the TRADE observation had led
            if (measuring) tradeToSettledHist.recordValue(clamp(now - seen));
        } else if (settleSeenNs.size() < TRADE_MAP_CAP) {
            settleSeenNs.put(tradeId, now); // settle first; trade observation will close it
        }
        final long scheduled = tradeToSampledOrder.remove(tradeId);
        if (scheduled != -1 && measuring) orderToSettledHist.recordValue(clamp(now - scheduled));
    }

    // ------------------------------------------------------------------ egress adapters

    private final class MeEgress implements EgressListener {
        @Override
        public void onMessage(final long clusterSessionId, final long timestamp,
                              final DirectBuffer buffer, final int offset, final int length,
                              final Header header) {
            meHeaderDec.wrap(buffer, offset);
            final int templateId = meHeaderDec.templateId();
            final int actingBlockLength = meHeaderDec.blockLength();
            final int actingVersion = meHeaderDec.version();
            final int body = offset + meHeaderDec.encodedLength();
            switch (templateId) {
                case OrderStatusUpdateDecoder.TEMPLATE_ID -> {
                    statusDec.wrap(buffer, body, actingBlockLength, actingVersion);
                    onFirstStatus(statusDec.omsOrderId());
                }
                case OrderStatusBatchDecoder.TEMPLATE_ID -> {
                    statusBatchDec.wrap(buffer, body, actingBlockLength, actingVersion);
                    for (final OrderStatusBatchDecoder.OrdersDecoder o : statusBatchDec.orders()) {
                        onFirstStatus(o.omsOrderId());
                    }
                }
                // TradeExecution singles (id 4) are deliberately IGNORED: the same trades also
                // arrive on the reliable TradeExecutionBatch stream (dry-run showed exactly 2x
                // observations), and consuming one stream keeps the tradeId join dedupe-free.
                case TradeExecutionBatchDecoder.TEMPLATE_ID -> {
                    tradeBatchDec.wrap(buffer, body, actingBlockLength, actingVersion);
                    for (final TradeExecutionBatchDecoder.TradesDecoder t : tradeBatchDec.trades()) {
                        onTrade(t.tradeId(), t.takerOmsOrderId(), t.makerOmsOrderId());
                    }
                }
                default -> { } // book updates, heartbeats: not our concern
            }
        }
    }

    private final class AeEgress implements EgressListener {
        @Override
        public void onMessage(final long clusterSessionId, final long timestamp,
                              final DirectBuffer buffer, final int offset, final int length,
                              final Header header) {
            aeHeaderDec.wrap(buffer, offset);
            final int templateId = aeHeaderDec.templateId();
            final int actingBlockLength = aeHeaderDec.blockLength();
            final int actingVersion = aeHeaderDec.version();
            final int body = offset + aeHeaderDec.encodedLength();
            if (templateId == holdAckDec.sbeTemplateId()) {
                holdAckDec.wrap(buffer, body, actingBlockLength, actingVersion);
                onHoldAck(holdAckDec.correlationId());
            } else if (templateId == holdRejectDec.sbeTemplateId()) {
                holdRejectDec.wrap(buffer, body, actingBlockLength, actingVersion);
                if (holdRejects < 5) {
                    System.err.printf("HOLD REJECT: orderId=%d userId=%d asset=%d amount=%d reason=%s%n",
                            holdRejectDec.orderId(), holdRejectDec.userId(), holdRejectDec.assetId(),
                            holdRejectDec.amount(), holdRejectDec.reason());
                }
                onHoldReject(holdRejectDec.correlationId());
            } else if (templateId == settleDec.sbeTemplateId()) {
                settleDec.wrap(buffer, body, actingBlockLength, actingVersion);
                onSettlementApplied(settleDec.tradeId());
            } else if (templateId == settleFaultDec.sbeTemplateId()) {
                settleFaultDec.wrap(buffer, body, actingBlockLength, actingVersion);
                settleFaults++;
                if (settleFaults <= 5) {
                    System.err.printf("SETTLE FAULT: tradeId=%d orderId=%d userId=%d "
                                    + "drawnFromAvailable=%d uncovered=%d "
                                    + "(pre-fund/hold sizing bug - run is INVALID)%n",
                            settleFaultDec.tradeId(), settleFaultDec.orderId(),
                            settleFaultDec.userId(), settleFaultDec.drawnFromAvailable(),
                            settleFaultDec.uncovered());
                }
            } else if (templateId == depositAckDec.sbeTemplateId()) {
                depositAckDec.wrap(buffer, body, actingBlockLength, actingVersion);
                if (depositAckDec.correlationId() >= PREFUND_CORR_BASE) depositAcks++;
            }
            // BalanceUpdate and snapshots: high-volume, ignored by design.
        }
    }

    // ------------------------------------------------------------------ plumbing

    private void pollBoth() {
        me.pollEgress();
        ae.pollEgress();
    }

    /**
     * Offer with bounded spin retries. Deliberately does NOT poll egress between retries:
     * this method is reached from inside egress callbacks (HoldAck -> order send), and a
     * reentrant pollEgress on the same AeronCluster from within its own callback is unsafe.
     * The main loop is the only place egress is drained.
     */
    private boolean offerWithRetry(final AeronCluster cluster, final DirectBuffer buffer,
                                   final int length, final boolean isAe) {
        for (int retry = 0; retry < OFFER_RETRIES; retry++) {
            final long result = cluster.offer(buffer, 0, length);
            if (result > 0) return true;
            if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED) return false;
            if (isAe) aeBackpressure++; else meBackpressure++;
            Thread.onSpinWait();
        }
        return false;
    }

    /** Post-send drain: give in-flight trades time to settle before the final report. */
    private void drain() {
        System.out.println("send window closed, draining in-flight settles...");
        final long grace = TimeUnit.SECONDS.toNanos(30);
        final long start = System.nanoTime();
        long lastSettles = -1;
        long lastChangeNs = start;
        while (System.nanoTime() - start < grace) {
            pollBoth();
            if (settlesSeen != lastSettles) {
                lastSettles = settlesSeen;
                lastChangeNs = System.nanoTime();
            } else if (System.nanoTime() - lastChangeNs > TimeUnit.SECONDS.toNanos(5)) {
                break; // settles stable for 5s: pipeline is dry
            }
        }
    }

    private static long clamp(final long ns) { return Math.max(0, Math.min(ns, TimeUnit.MINUTES.toNanos(2) - 1)); }

    private void report(final long startNs) {
        final double elapsedSec = (System.nanoTime() - startNs) / 1e9;
        final PrintStream out = System.out;
        out.println();
        out.println("==== money-path load report ====");
        out.printf("elapsed %.1fs (warmup %ds excluded from histograms)%n", elapsedSec, warmupSec);
        out.printf("holds sent %d | acks %d | rejects %d | orders sent %d | send-failures %d%n",
                holdsSent, holdAcks, holdRejects, ordersSent, orderSendFailures);
        out.printf("first-statuses %d | trades seen %d | settles seen %d | SETTLE FAULTS %d%n",
                firstStatuses, tradesSeen, settlesSeen, settleFaults);
        out.printf("backpressure me/ae %d/%d | trade-map skips %d | max schedule backlog %d%n",
                meBackpressure, aeBackpressure, tradeMapSkips, scheduleBacklogMax);
        out.printf("join direction: settle-led %d / trade-led %d (settle-led = settlement path "
                        + "beat the ME egress batch flush)%n", settleLedJoins, tradeLedJoins);
        out.printf("unpaired at exit: trades %d / settles %d | pending lifecycles: %d%n",
                tradeSeenNs.size(), settleSeenNs.size(), pending.size());
        printHist(out, "hold->holdAck (from scheduled send)", holdAckHist);
        printHist(out, "order lifecycle start->first ME status", orderAckHist);
        printHist(out, "trade obs->settled (trade-led pairs)", tradeToSettledHist);
        printHist(out, "settle lead over trade obs (settle-led)", settleLeadHist);
        printHist(out, "scheduled send->settled (sampled 1/" + (1 << sampleShift) + ")", orderToSettledHist);
        if (settleFaults > 0) {
            out.println("!! RUN INVALID: settle faults observed - fix pre-fund/hold sizing before publishing");
        }
        try {
            Files.createDirectories(outDir);
            writeHgrm("hold-ack", holdAckHist);
            writeHgrm("order-ack", orderAckHist);
            writeHgrm("trade-to-settled", tradeToSettledHist);
            writeHgrm("settle-lead", settleLeadHist);
            writeHgrm("order-to-settled", orderToSettledHist);
            out.println("hgrm files written to " + outDir.toAbsolutePath());
        } catch (final Exception e) {
            out.println("WARN: could not write hgrm files: " + e);
        }
    }

    private static void printHist(final PrintStream out, final String name, final Histogram h) {
        if (h.getTotalCount() == 0) {
            out.printf("%-45s  (no samples)%n", name);
            return;
        }
        out.printf("%-45s n=%-9d p50=%s p90=%s p99=%s p99.9=%s p99.99=%s max=%s%n",
                name, h.getTotalCount(),
                us(h.getValueAtPercentile(50)), us(h.getValueAtPercentile(90)),
                us(h.getValueAtPercentile(99)), us(h.getValueAtPercentile(99.9)),
                us(h.getValueAtPercentile(99.99)), us(h.getMaxValue()));
    }

    private static String us(final long ns) {
        return ns < 1_000_000 ? String.format("%.1fus", ns / 1e3) : String.format("%.2fms", ns / 1e6);
    }

    private void writeHgrm(final String name, final Histogram h) throws Exception {
        try (PrintStream ps = new PrintStream(Files.newOutputStream(outDir.resolve(name + ".hgrm")))) {
            h.outputPercentileDistribution(ps, 1000.0); // report in microseconds
        }
    }

    @Override
    public void close() {
        org.agrona.CloseHelper.quietClose(me);
        org.agrona.CloseHelper.quietClose(ae);
        org.agrona.CloseHelper.quietClose(driver);
    }

    // ------------------------------------------------------------------ args

    private static String strArg(final String[] args, final String key, final String def) {
        for (int i = 0; i < args.length - 1; i++) if (args[i].equals(key)) return args[i + 1];
        return def;
    }

    private static int intArg(final String[] args, final String key, final int def) {
        final String v = strArg(args, key, null);
        return v == null ? def : Integer.parseInt(v);
    }

    private static double doubleArg(final String[] args, final String key, final double def) {
        final String v = strArg(args, key, null);
        return v == null ? def : Double.parseDouble(v);
    }

    private static boolean hasFlag(final String[] args, final String key) {
        for (final String a : args) if (a.equals(key)) return true;
        return false;
    }

    public static void main(final String[] args) {
        if (hasFlag(args, "--help")) {
            System.out.println("""
                MoneyLoadGenerator - full money-path benchmark driver (no OMS)
                  --rate N            orders/s on the fixed timeline (default 10000)
                  --duration S        measured window seconds, EXCLUDES warmup (default 60)
                  --warmup S          warmup seconds before the window opens (default 30)
                  --users N           distinct userIds 0..N-1 (default 16)
                  --window N          max in-flight lifecycles (default 8192)
                  --bid-bias F        P(bid) (default 0.5 = no net balance drift)
                  --limit-ratio F     P(limit) vs market (default 0.40)
                  --sample-shift N    order->settled sampling 1/2^N (default 6)
                  --me-hosts CSV      matching-engine node hosts (default 127.0.0.1)
                  --ae-hosts CSV      assets-engine node hosts (default 127.0.0.1)
                  --me-port-base N    default 9000
                  --ae-port-base N    default 9300
                  --egress-host H     advertised egress address (default: routed source addr)
                  --out DIR           hgrm output dir (default results)
                  --no-prefund        skip the deposit phase
                  --prefund-usd F     USD per user (default 5e10)
                  --prefund-btc F     BTC per user (default 1e7)""");
            return;
        }
        try (MoneyLoadGenerator gen = new MoneyLoadGenerator(args)) {
            gen.run();
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.loadgen;

import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderCancelEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderCreateEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderEventDecoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderFillEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoOrderType;
import com.openexchange.assets.infrastructure.generated.ProtoRiskConfigEncoder;
import com.openexchange.assets.infrastructure.generated.ProtoSide;
import com.openexchange.assets.infrastructure.generated.ProtoTimeInForce;
import com.openexchange.assets.infrastructure.generated.SubscribeEncoder;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * RC0 spike driver: the prototype order lifecycle against the Assets Engine alone (no ME, no OMS).
 *
 * <p>Fixed-rate, coordinated-omission-safe timeline (the slot's scheduled time is every command's
 * t0, never reset when behind), single clock, histograms recording only inside the measurement
 * window: the {@link MoneyLoadGenerator} rules. One duty thread owns the session.</p>
 *
 * <p>Each slot sends one {@code ProtoOrderCreate}; in steady state it adds a cancel of a random open
 * order with probability {@code --cancel-ratio} and a fill of a random open buy against a random open
 * sell with probability {@code --fill-ratio}, with a band controller that keeps the open-order count
 * near {@code --keep-open}. So the snapshot size, the per-order apply cost and the egress bytes are
 * measured at a chosen resident state (100k, 1M), which is the RC0 question.</p>
 *
 * <p>Tracks: {@code create-ack} (slot -> NEW/REJECTED on this session), {@code cancel-ack}
 * (slot -> CANCELLED), {@code fill-event} (slot -> the buy order's FILLED broadcast).</p>
 */
public final class ProtoOrderLoadGenerator implements AutoCloseable {

    private static final double FP_SCALE = 1e8;
    private static long fp(final double v) { return Math.round(v * FP_SCALE); }

    private static final int ASSET_USD = 0;
    private static final int ASSET_BTC = 1;
    private static final long PREFUND_CORR_BASE = 1L << 62;
    private static final int OFFER_RETRIES = 10;
    private static final int ST_NEW = 0, ST_PARTIALLY_FILLED = 1, ST_FILLED = 2, ST_CANCELLED = 3, ST_REJECTED = 4, ST_EXPIRED = 5;

    // ---- config ----
    private final int rate;
    private final int durationSec;
    private final int warmupSec;
    private final int users;
    private final int keepOpen;
    private final int window;
    private final double cancelRatio;
    private final double fillRatio;
    private final double gtdRatio;
    private final long gtdMs;
    private final int marketId;
    private final double midPrice;
    private final int priceLevels;
    private final double qty;
    private final Path outDir;
    private final int genId;
    private final boolean intervalLog;
    private final boolean prefund;
    private final long prefundUsdFp;
    private final long prefundBtcFp;
    private final boolean subscribe;

    // ---- transport ----
    private final MediaDriver driver;
    private final AeronCluster ae;
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDec = new MessageHeaderDecoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final DepositAckDecoder depositAckDec = new DepositAckDecoder();
    private final SubscribeEncoder subscribeEnc = new SubscribeEncoder();
    private final ProtoRiskConfigEncoder riskEnc = new ProtoRiskConfigEncoder();
    private final ProtoOrderCreateEncoder createEnc = new ProtoOrderCreateEncoder();
    private final ProtoOrderCancelEncoder cancelEnc = new ProtoOrderCancelEncoder();
    private final ProtoOrderFillEncoder fillEnc = new ProtoOrderFillEncoder();
    private final ProtoOrderEventDecoder eventDec = new ProtoOrderEventDecoder();
    private final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(256);

    // ---- resident order set (duty thread only) ----
    private static final class Open {
        long orderId;
        long userId;
        boolean isBid;
        long remainingQty;
    }

    private final ArrayDeque<Open> openPool = new ArrayDeque<>();
    private final Long2ObjectHashMap<Open> open = new Long2ObjectHashMap<>();
    private final LongArrayList openBuys = new LongArrayList();
    private final LongArrayList openSells = new LongArrayList();
    private final Long2LongHashMap indexInSide = new Long2LongHashMap(-1L);

    // ---- in-flight correlated commands: correlationId -> scheduledNs (bounded by window) ----
    private final Long2LongHashMap createPending = new Long2LongHashMap(-1L);
    private final Long2LongHashMap cancelPending = new Long2LongHashMap(-1L);
    /** buy orderId of an in-flight fill -> scheduledNs. */
    private final Long2LongHashMap fillPending = new Long2LongHashMap(-1L);

    // ---- tracks ----
    private final LatencyTrack createAck = new LatencyTrack("create-ack");
    private final LatencyTrack cancelAck = new LatencyTrack("cancel-ack");
    private final LatencyTrack fillEvent = new LatencyTrack("fill-event");
    private final LatencyTrack[] tracks = {createAck, cancelAck, fillEvent};
    private static final DateTimeFormatter ILOG_TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    // ---- counters ----
    private long createsSent, createAcks, createRejects, cancelsSent, cancelAcks, cancelRejects, fillsSent, fillsClosed,
            fillEventsSeen, expiredSeen, depositAcks, backpressure, scheduleBacklogMax, offerGiveUps, eventsSeen;
    private final long[] rejectsByReason = new long[16];
    private long openAtWindowOpen = -1;
    private long maxOpen;

    private long measureStartNs;
    private boolean measuring;
    private final Random rand;
    private final long userBase;
    private long nextCorr = 1;
    private long nextTradeId = 1;
    private long nextClientOrderId;

    private ProtoOrderLoadGenerator(final String[] args) {
        this.rate = intArg(args, "--rate", 5_000);
        this.durationSec = intArg(args, "--duration", 60);
        this.warmupSec = intArg(args, "--warmup", 30);
        this.users = intArg(args, "--users", 1_000);
        this.keepOpen = intArg(args, "--keep-open", 100_000);
        this.window = intArg(args, "--window", 16_384);
        this.cancelRatio = doubleArg(args, "--cancel-ratio", 0.4);
        this.fillRatio = doubleArg(args, "--fill-ratio", 0.3);
        this.gtdRatio = doubleArg(args, "--gtd-ratio", 0.0);
        this.gtdMs = intArg(args, "--gtd-ms", 600_000);
        this.marketId = intArg(args, "--market", 1);
        this.midPrice = doubleArg(args, "--mid", 50_000.0);
        this.priceLevels = intArg(args, "--price-levels", 100);
        this.qty = doubleArg(args, "--qty", 0.01);
        this.outDir = Path.of(strArg(args, "--out", "results"));
        this.genId = intArg(args, "--gen-id", 0);
        this.userBase = 1L + (long) this.genId * this.users;
        this.rand = new Random(42 + this.genId);
        this.nextClientOrderId = ((System.currentTimeMillis() + this.genId) << 20);
        this.intervalLog = hasFlag(args, "--interval-log");
        this.prefund = !hasFlag(args, "--no-prefund");
        this.prefundUsdFp = fp(doubleArg(args, "--prefund-usd", 50_000_000_000.0));
        this.prefundBtcFp = fp(doubleArg(args, "--prefund-btc", 10_000_000.0));
        this.subscribe = !hasFlag(args, "--no-subscribe");

        final List<String> aeHosts = List.of(strArg(args, "--ae-hosts", "127.0.0.1").split(","));
        final int aePortBase = intArg(args, "--ae-port-base", 9300);
        final String egressHost = MoneyLoadGenerator.resolveEgressHost(strArg(args, "--egress-host", null), aeHosts.get(0));
        final String externalAeronDir = strArg(args, "--aeron-dir", null);
        this.driver = externalAeronDir != null ? null : MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName("/dev/shm/aeron-protoload-" + ProcessHandle.current().pid())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .errorHandler(t -> t.printStackTrace(System.err)));
        final String aeronDir = driver != null ? driver.aeronDirectoryName() : externalAeronDir;
        System.out.printf("connecting AE %s (base %d), egressHost=%s%n", aeHosts, aePortBase, egressHost);
        AeronCluster client = null;
        try {
            client = AeronCluster.connect(new AeronCluster.Context()
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10))
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp?term-length=16m|mtu=8k")
                    .ingressEndpoints(MoneyLoadGenerator.ingressEndpoints(aeHosts, aePortBase))
                    .egressChannel("aeron:udp?endpoint=" + egressHost + ":0")
                    .egressListener(new AeEgress())
                    .errorHandler(t -> t.printStackTrace(System.err)));
        } catch (final RuntimeException e) {
            org.agrona.CloseHelper.quietClose(client);
            org.agrona.CloseHelper.quietClose(driver);
            throw e;
        }
        this.ae = client;
    }

    // ------------------------------------------------------------------ setup

    private void sendSubscribe() {
        subscribeEnc.wrapAndApplyHeader(buffer, 0, headerEnc)
                .correlationId(0L)
                .channels().clear().acks(true).balances(false).settlements(false).snapshots(false).orders(true);
        final int len = headerEnc.encodedLength() + subscribeEnc.encodedLength();
        final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            final long result = ae.offer(buffer, 0, len);
            if (result > 0) return;
            if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("Subscribe offer failed terminally: " + result);
            }
            if (System.nanoTime() > deadlineNs) {
                throw new IllegalStateException("Subscribe not accepted within 10s");
            }
            ae.pollEgress();
            Thread.onSpinWait();
        }
    }

    private void sendRiskConfig() {
        // Per-user slot budget sized to the resident target, limits lifted so admission cost is
        // measured without artificial rejects (the checks still RUN on every create).
        riskEnc.wrapAndApplyHeader(buffer, 0, headerEnc)
                .correlationId(0L).marketId(marketId).maxOrdersPerSec(10_000_000).maxOrdersPerMin(100_000_000)
                .minQty(1L).maxQty(Long.MAX_VALUE / 4).maxNotional(Long.MAX_VALUE / 4).collarPct(50)
                .maxOpenOrders(Math.max(500, 4 * keepOpen / users + 16)).maxPositionPerMarket(Long.MAX_VALUE)
                .circuitBreaker(BoolFlag.FALSE);
        offerWithRetry(headerEnc.encodedLength() + riskEnc.encodedLength());
    }

    private void prefundUsers() {
        System.out.printf("pre-funding %d users (USD %.0f, BTC %.0f each)...%n", users, prefundUsdFp / FP_SCALE, prefundBtcFp / FP_SCALE);
        final int expected = users * 2;
        for (int u = 0; u < users; u++) {
            final long uid = userBase + u;
            depositEnc.wrapAndApplyHeader(buffer, 0, headerEnc).correlationId(PREFUND_CORR_BASE + uid * 2L).userId(uid).assetId(ASSET_USD).amount(prefundUsdFp);
            offerWithRetry(headerEnc.encodedLength() + depositEnc.encodedLength());
            depositEnc.wrapAndApplyHeader(buffer, 0, headerEnc).correlationId(PREFUND_CORR_BASE + uid * 2L + 1).userId(uid).assetId(ASSET_BTC).amount(prefundBtcFp);
            offerWithRetry(headerEnc.encodedLength() + depositEnc.encodedLength());
            if ((u & 255) == 255) ae.pollEgress();
        }
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (depositAcks < expected) {
            ae.pollEgress();
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("pre-fund incomplete: " + depositAcks + "/" + expected + " DepositAcks in 60s");
            }
        }
        System.out.println("pre-fund complete: " + depositAcks + " deposits acked");
    }

    // ------------------------------------------------------------------ hot loop

    private void run() {
        if (subscribe) sendSubscribe();
        sendRiskConfig();
        if (prefund) prefundUsers();

        final long nanosPerSlot = 1_000_000_000L / rate;
        final long startNs = System.nanoTime();
        this.measureStartNs = startNs + TimeUnit.SECONDS.toNanos(warmupSec);
        final long sendEndNs = measureStartNs + TimeUnit.SECONDS.toNanos(durationSec);
        long nextScheduledNs = startNs;
        long lastKeepAliveNs = startNs;

        System.out.printf("running: %d slots/s, warmup %ds, window %ds, keep-open %d, users %d, cancel %.2f fill %.2f gtd %.2f%n",
                rate, warmupSec, durationSec, keepOpen, users, cancelRatio, fillRatio, gtdRatio);

        Thread intervalLogger = null;
        if (intervalLog) {
            intervalLogger = new Thread(() -> {
                while (true) {
                    try { Thread.sleep(1_000); } catch (final InterruptedException e) { return; }
                    final long epochMs = System.currentTimeMillis();
                    final String stamp = "ILOG epoch_ms=" + epochMs + " ts=" + ILOG_TS.format(Instant.ofEpochMilli(epochMs))
                            + " open=" + open.size() + " ";
                    for (final LatencyTrack t : tracks) t.printIntervalLine(System.out, stamp);
                }
            }, "interval-log");
            intervalLogger.setDaemon(true);
            intervalLogger.start();
        }

        while (true) {
            final long now = System.nanoTime();
            if (!measuring && now >= measureStartNs) {
                measuring = true;
                openAtWindowOpen = open.size();
                System.out.println("warmup done, measurement window open, open orders=" + openAtWindowOpen);
            }
            if (now >= sendEndNs) break;
            if (now >= nextScheduledNs) {
                final long backlog = (now - nextScheduledNs) / nanosPerSlot;
                if (backlog > scheduleBacklogMax) scheduleBacklogMax = backlog;
                if (inFlight() < window) {
                    slot(nextScheduledNs);
                    nextScheduledNs += nanosPerSlot;
                }
            }
            ae.pollEgress();
            if (now - lastKeepAliveNs > 250_000_000L) {
                lastKeepAliveNs = now;
                try { ae.sendKeepAlive(); } catch (final Exception ignore) { }
            }
        }
        drain();
        if (intervalLogger != null) {
            intervalLogger.interrupt();
            try { intervalLogger.join(); } catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        report(startNs);
    }

    private int inFlight() {
        return createPending.size() + cancelPending.size() + fillPending.size();
    }

    /** One timeline slot: a create, then the mix that keeps the resident set near keep-open. */
    private void slot(final long scheduledNs) {
        final int resident = open.size();
        final boolean above = resident > keepOpen + keepOpen / 50;
        final boolean below = resident < keepOpen - keepOpen / 50;
        sendCreate(scheduledNs);
        if (!below && (above || rand.nextDouble() < cancelRatio)) {
            sendCancel(scheduledNs);
        }
        if (!below && rand.nextDouble() < fillRatio) {
            sendFill(scheduledNs);
        }
    }

    private void sendCreate(final long scheduledNs) {
        final long corr = nextCorr++;
        final long user = userBase + rand.nextInt(users);
        final boolean isBid = rand.nextBoolean();
        final int level = rand.nextInt(priceLevels);
        final double price = isBid ? midPrice - 1 - level : midPrice + 1 + level; // never crossing: fills come from ProtoOrderFill
        final boolean gtd = gtdRatio > 0 && rand.nextDouble() < gtdRatio;
        createEnc.wrapAndApplyHeader(buffer, 0, headerEnc)
                .correlationId(corr).userId(user).clientOrderIdHigh(0x5A5AL).clientOrderIdLow(nextClientOrderId++)
                .requestIdHigh(0L).requestIdLow(corr).marketId(marketId)
                .side(isBid ? ProtoSide.BUY : ProtoSide.SELL).orderType(ProtoOrderType.LIMIT)
                .timeInForce(gtd ? ProtoTimeInForce.GTD : ProtoTimeInForce.GTC)
                .price(fp(price)).quantity(fp(qty)).stopPrice(0L).displayQty(0L)
                .expiresAtMs(gtd ? System.currentTimeMillis() + gtdMs : 0L).flags(0L);
        if (offerWithRetry(headerEnc.encodedLength() + createEnc.encodedLength())) {
            createsSent++;
            createPending.put(corr, scheduledNs);
            sideByCorr.put(corr, isBid ? 1L : 0L); // the NEW event does not carry the side
        } else {
            offerGiveUps++;
        }
    }

    private void sendCancel(final long scheduledNs) {
        final LongArrayList side = rand.nextBoolean() ? openBuys : openSells;
        final LongArrayList pick = side.isEmpty() ? (side == openBuys ? openSells : openBuys) : side;
        if (pick.isEmpty()) return;
        final long orderId = pick.getLong(rand.nextInt(pick.size()));
        final Open o = open.get(orderId);
        if (o == null) return;
        final long corr = nextCorr++;
        cancelEnc.wrapAndApplyHeader(buffer, 0, headerEnc).correlationId(corr).userId(o.userId).orderId(orderId);
        if (offerWithRetry(headerEnc.encodedLength() + cancelEnc.encodedLength())) {
            cancelsSent++;
            cancelPending.put(corr, scheduledNs);
            // Take it out of the pick lists now so a second cancel / fill cannot target it; the
            // CANCELLED event only confirms. (Rejected cancels never happen for live own orders.)
            removeOpen(orderId);
        } else {
            offerGiveUps++;
        }
    }

    private void sendFill(final long scheduledNs) {
        if (openBuys.isEmpty() || openSells.isEmpty()) return;
        final long buyId = openBuys.getLong(rand.nextInt(openBuys.size()));
        final long sellId = openSells.getLong(rand.nextInt(openSells.size()));
        final Open b = open.get(buyId), s = open.get(sellId);
        if (b == null || s == null) return;
        final long fillQty = Math.min(b.remainingQty, s.remainingQty);
        fillEnc.wrapAndApplyHeader(buffer, 0, headerEnc)
                .correlationId(0L).tradeId(nextTradeId++).marketId(marketId).buyOrderId(buyId).sellOrderId(sellId)
                .price(fp(midPrice)).quantity(fillQty).takerIsBuy(BoolFlag.TRUE);
        if (offerWithRetry(headerEnc.encodedLength() + fillEnc.encodedLength())) {
            fillsSent++;
            fillPending.put(buyId, scheduledNs);
            removeOpen(buyId);  // both legs fill fully (qty = min remaining, equal sizes): closed on the wire soon
            removeOpen(sellId);
        } else {
            offerGiveUps++;
        }
    }

    // ------------------------------------------------------------------ egress

    private void onEvent(final long corr, final long orderId, final long userId, final int status, final int reason,
                         final long remainingQty, final boolean isBid, final long nowNs) {
        eventsSeen++;
        if (corr != 0) {
            final long scheduled = createPending.remove(corr);
            if (scheduled != -1L) {
                if (status == ST_NEW) {
                    createAcks++;
                    if (measuring) createAck.record(nowNs - scheduled);
                    addOpen(orderId, userId, remainingQty, isBid);
                } else {
                    createRejects++;
                    rejectsByReason[Math.min(reason, 15)]++;
                }
                return;
            }
            final long cancelScheduled = cancelPending.remove(corr);
            if (cancelScheduled != -1L) {
                if (status == ST_CANCELLED) {
                    cancelAcks++;
                    if (measuring) cancelAck.record(nowNs - cancelScheduled);
                } else {
                    cancelRejects++;
                }
                return;
            }
            return; // not ours (another generator's ack would not reach this session anyway)
        }
        // Broadcast: fill-driven, expiry.
        switch (status) {
            case ST_FILLED -> {
                final long scheduled = fillPending.remove(orderId);
                if (scheduled != -1L) {
                    fillsClosed++;
                    if (measuring) fillEvent.record(nowNs - scheduled);
                }
                fillEventsSeen++;
                removeOpen(orderId);
            }
            case ST_PARTIALLY_FILLED -> {
                fillEventsSeen++;
                final Open o = open.get(orderId);
                if (o != null) o.remainingQty = remainingQty;
            }
            case ST_EXPIRED -> {
                expiredSeen++;
                removeOpen(orderId);
            }
            case ST_CANCELLED -> removeOpen(orderId);
            default -> { }
        }
    }

    private void addOpen(final long orderId, final long userId, final long remainingQty, final boolean isBid) {
        if (open.containsKey(orderId)) return;
        final Open o = openPool.isEmpty() ? new Open() : openPool.pop();
        o.orderId = orderId;
        o.userId = userId;
        o.remainingQty = remainingQty;
        o.isBid = isBid;
        open.put(orderId, o);
        final LongArrayList list = o.isBid ? openBuys : openSells;
        indexInSide.put(orderId, list.size());
        list.addLong(orderId);
        if (open.size() > maxOpen) maxOpen = open.size();
    }

    private void removeOpen(final long orderId) {
        final Open o = open.remove(orderId);
        if (o == null) return;
        final LongArrayList list = o.isBid ? openBuys : openSells;
        final int idx = (int) indexInSide.remove(orderId);
        if (idx >= 0 && idx < list.size()) {
            final long last = list.getLong(list.size() - 1);
            list.setLong(idx, last);
            list.removeAt(list.size() - 1);
            if (last != orderId) indexInSide.put(last, idx);
        }
        openPool.push(o);
    }

    /** The side of a create is known only to the generator; the NEW event carries orderId and userId. */
    private final Long2LongHashMap sideByCorr = new Long2LongHashMap(-1L);

    private final class AeEgress implements EgressListener {
        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buf,
                              final int offset, final int length, final Header header) {
            final long nowNs = System.nanoTime();
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) return;
            headerDec.wrap(buf, offset);
            final int body = offset + headerDec.encodedLength();
            final int templateId = headerDec.templateId();
            if (templateId == ProtoOrderEventDecoder.TEMPLATE_ID) {
                eventDec.wrap(buf, body, headerDec.blockLength(), headerDec.version());
                final long corr = eventDec.correlationId();
                final long side = corr != 0 ? sideByCorr.remove(corr) : -1L;
                onEvent(corr, eventDec.orderId(), eventDec.userId(), eventDec.status().value(),
                        eventDec.reason().value(), eventDec.remainingQty(), side == 1L, nowNs);
            } else if (templateId == DepositAckDecoder.TEMPLATE_ID) {
                depositAckDec.wrap(buf, body, headerDec.blockLength(), headerDec.version());
                if (depositAckDec.correlationId() >= PREFUND_CORR_BASE) depositAcks++;
            }
        }

        @Override
        public void onNewLeader(final long clusterSessionId, final long leadershipTermId, final int leaderMemberId,
                                final String ingressEndpoints) {
            System.out.printf("%s [AE] NEW LEADER member=%d term=%d session=%d endpoints=%s%n",
                    ILOG_TS.format(Instant.now()), leaderMemberId, leadershipTermId, clusterSessionId, ingressEndpoints);
        }

        @Override
        public void onSessionEvent(final long correlationId, final long clusterSessionId, final long leadershipTermId,
                                   final int leaderMemberId, final EventCode code, final String detail) {
            System.out.printf("%s [AE] SESSION EVENT code=%s member=%d term=%d session=%d detail=%s%n",
                    ILOG_TS.format(Instant.now()), code, leaderMemberId, leadershipTermId, clusterSessionId, detail);
        }
    }

    // ------------------------------------------------------------------ plumbing

    private boolean offerWithRetry(final int length) {
        for (int retry = 0; retry < OFFER_RETRIES; retry++) {
            final long result = ae.offer(buffer, 0, length);
            if (result > 0) return true;
            if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED) return false;
            backpressure++;
            Thread.onSpinWait();
        }
        return false;
    }

    private void drain() {
        System.out.println("send window closed, draining in-flight acks...");
        final long start = System.nanoTime();
        long lastProgress = -1;
        long lastChangeNs = start;
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(30)) {
            ae.pollEgress();
            final long progress = createAcks + cancelAcks + fillsClosed;
            if (progress != lastProgress) {
                lastProgress = progress;
                lastChangeNs = System.nanoTime();
            } else if (System.nanoTime() - lastChangeNs > TimeUnit.SECONDS.toNanos(5)) {
                break;
            }
        }
    }

    private void report(final long startNs) {
        final double elapsedSec = (System.nanoTime() - startNs) / 1e9;
        final PrintStream out = System.out;
        out.println();
        out.println("==== proto-order report (AE core only: create + cancel + fill, no ME) ====");
        out.printf("elapsed %.1fs (warmup %ds excluded from histograms) | measured window %ds%n", elapsedSec, warmupSec, durationSec);
        out.printf("creates sent %d | NEW %d | rejects %d | cancels sent %d | CANCELLED %d | cancel rejects %d%n",
                createsSent, createAcks, createRejects, cancelsSent, cancelAcks, cancelRejects);
        out.printf("fills sent %d | closed (buy FILLED seen) %d | fill events %d | expired %d | events %d%n",
                fillsSent, fillsClosed, fillEventsSeen, expiredSeen, eventsSeen);
        out.printf("open at window open %d | open at exit %d | max open %d | keep-open target %d%n",
                openAtWindowOpen, open.size(), maxOpen, keepOpen);
        out.printf("backpressure %d | offer give-ups %d | max schedule backlog %d | in-flight at exit %d%n",
                backpressure, offerGiveUps, scheduleBacklogMax, inFlight());
        final StringBuilder rj = new StringBuilder();
        for (int i = 0; i < rejectsByReason.length; i++) if (rejectsByReason[i] > 0) rj.append(" reason").append(i).append('=').append(rejectsByReason[i]);
        if (rj.length() > 0) out.println("create rejects by reason:" + rj);
        final double commands = (createAcks + cancelAcks + 1.0 * fillsClosed) / Math.max(1e-9, elapsedSec - warmupSec);
        out.printf("ACKED COMMANDS ~%.0f/s over elapsed-warmup (creates + cancels + fills)%n", commands);
        printHist(out, "create -> NEW ack (from scheduled slot)", createAck);
        printHist(out, "cancel -> CANCELLED ack (from scheduled slot)", cancelAck);
        printHist(out, "fill -> buy FILLED event (from scheduled slot)", fillEvent);
        try {
            Files.createDirectories(outDir);
            for (final LatencyTrack t : tracks) {
                try (PrintStream ps = new PrintStream(Files.newOutputStream(outDir.resolve(t.name + ".hgrm")))) {
                    t.cumulative().outputPercentileDistribution(ps, 1000.0);
                }
            }
            out.println("hgrm files written to " + outDir.toAbsolutePath());
        } catch (final Exception e) {
            out.println("WARN: could not write hgrm files: " + e);
        }
    }

    private static void printHist(final PrintStream out, final String name, final LatencyTrack t) {
        final Histogram h = t.cumulative();
        if (h.getTotalCount() == 0) {
            out.printf("%-48s  (no samples)%n", name);
            return;
        }
        out.printf("%-48s n=%-9d p50=%s p90=%s p99=%s p99.9=%s p99.99=%s max=%s%n",
                name, h.getTotalCount(), us(h.getValueAtPercentile(50)), us(h.getValueAtPercentile(90)),
                us(h.getValueAtPercentile(99)), us(h.getValueAtPercentile(99.9)), us(h.getValueAtPercentile(99.99)), us(h.getMaxValue()));
    }

    private static String us(final long ns) {
        return ns < 1_000_000 ? String.format(Locale.ROOT, "%.1fus", ns / 1e3) : String.format(Locale.ROOT, "%.2fms", ns / 1e6);
    }

    @Override
    public void close() {
        org.agrona.CloseHelper.quietClose(ae);
        org.agrona.CloseHelper.quietClose(driver);
    }

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
                ProtoOrderLoadGenerator - RC0 spike: order lifecycle against the AE core (no ME)
                  --rate N           timeline slots/s; each slot = 1 create (+ cancel/fill mix) (default 5000)
                  --duration S       measured window seconds, excludes warmup (default 60)
                  --warmup S         warmup seconds; the resident set is built during it (default 30)
                  --users N          distinct users (default 1000)
                  --keep-open N      resident open-order target (default 100000)
                  --window N         max in-flight correlated commands (default 16384)
                  --cancel-ratio F   P(cancel per slot) in steady state (default 0.4)
                  --fill-ratio F     P(fill per slot) in steady state; a fill closes 2 orders (default 0.3)
                  --gtd-ratio F      P(GTD) per create, expires after --gtd-ms (default 0)
                  --gtd-ms N         GTD horizon (default 600000)
                  --qty F            order quantity (default 0.01)
                  --mid F            mid price (default 50000); limits sit 1..--price-levels away, never cross
                  --ae-hosts CSV --ae-port-base N --egress-host H --aeron-dir DIR   as MoneyLoadGenerator
                  --no-subscribe     keep the balance firehose (default: subscribe acks,orders only)
                  --no-prefund --prefund-usd F --prefund-btc F --out DIR --gen-id N --interval-log""");
            return;
        }
        try (ProtoOrderLoadGenerator gen = new ProtoOrderLoadGenerator(args)) {
            gen.run();
        }
    }
}

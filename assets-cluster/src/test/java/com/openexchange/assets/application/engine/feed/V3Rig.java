// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.auth.AssetsSessionClasses;
import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.RecordingAssetsSink;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.domain.FeedHaltReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.OrderFaultKind;
import com.openexchange.assets.domain.commands.CreateOrderCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.AssetsSbeDemuxer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 test rig: one in-process core (engine + projector + demuxer + recording sink) booted through {@link V3Genesis},
 * with a feed driver that numbers journalSeq and egressSeq like the ME does (dense journalSeq, increasing egressSeq), so a
 * test can deliver "the next entry" or any explicit (jseq, epoch, egressSeq) it needs. Public so the gate, config, order and
 * persistence tests share one fixture.
 */
public final class V3Rig implements AssetsSbeDemuxer.TransportHooks {

    public static final long ONE = 100_000_000L;
    public static final int USD = 0;
    public static final int BTC = 1;
    public static final int MKT = 1;
    public static final long EPOCH = V3Genesis.GENESIS_ID;
    public static final int ME_FILLED = 2;
    public static final int ME_CANCELLED = 3;
    public static final int ME_REJECTED = 4;
    public static final int KIND_CREATE = 0;
    public static final int KIND_CANCEL = 1;
    public static final int KIND_AMEND = 2;

    public final AssetsEngine engine = new AssetsEngine();
    public final RecordingAssetsSink sink = new RecordingAssetsSink();
    public final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, new SettlementProjector(engine));
    public final List<Long> rejectedSessions = new ArrayList<>();
    public long now = 1_700_000_000_000L;
    /** The last journalSeq this rig's ME produced (1 = the FRESH record). */
    public long jseq;
    /** The last egressSeq this rig's ME produced. */
    public long egress;
    private long nextRequest = 1;
    private int mark;

    private V3Rig() {
        engine.setEventSink(sink);
        demuxer.setTransportHooks(this);
    }

    public void onSessionRegistered(final long sessionId, final int channelMask, final boolean resumable) {
    }

    public void onSessionRejected(final long sessionId) {
        rejectedSessions.add(sessionId);
    }

    /** Behaviour 3 recorded, feed unbound (between the switch and the bind). */
    public static V3Rig unbound() {
        final V3Rig r = new V3Rig();
        V3Genesis.switchTo3(r.engine, r.now);
        return r;
    }

    /** Behaviour 3 and the FRESH record applied at journalSeq 1 (egressSeq 1). */
    public static V3Rig bound() {
        final V3Rig r = unbound();
        r.bindFresh();
        return r;
    }

    /** {@link #bound()} and ADMIN OPEN of the global gate. */
    public static V3Rig open() {
        final V3Rig r = bound();
        r.engine.applyOrderEntryGate(1L, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, r.now);
        return r;
    }

    /** Behaviour 2 recorded (the RC1 rules), for the v1/v2 contrast rows. */
    public static V3Rig v2() {
        final V3Rig r = new V3Rig();
        V3Genesis.widen(r.engine);
        r.engine.applyCoreConfig(V3Genesis.config(1, 1), r.now);
        r.engine.applyCoreConfig(V3Genesis.config(2, 2), r.now);
        return r;
    }

    public boolean bindFresh() {
        jseq = 1;
        return V3Genesis.bind(engine, ++egress, now);
    }

    // ---------------------------------------------------------------- reads

    public FeedState feed() {
        return engine.feed();
    }

    public long cursor() {
        return engine.feed().lastAppliedJournalSeq();
    }

    public int globalReasons() {
        return engine.gate().reasons(OrderEntryGate.GLOBAL);
    }

    public boolean globalHas(final GateReason r) {
        return (globalReasons() & (1 << r.code())) != 0;
    }

    public void mark() {
        mark = sink.events().size();
    }

    public List<AssetsEngineEvent> events() {
        return sink.events().subList(mark, sink.events().size());
    }

    public List<String> renders() {
        final List<String> out = new ArrayList<>();
        for (final AssetsEngineEvent e : events()) {
            out.add(e.render());
        }
        return out;
    }

    public <T extends AssetsEngineEvent> List<T> all(final Class<T> type) {
        final List<T> out = new ArrayList<>();
        for (final AssetsEngineEvent e : events()) {
            if (type.isInstance(e)) {
                out.add(type.cast(e));
            }
        }
        return out;
    }

    public <T extends AssetsEngineEvent> T first(final Class<T> type) {
        for (final AssetsEngineEvent e : events()) {
            if (type.isInstance(e)) {
                return type.cast(e);
            }
        }
        throw new AssertionError("no " + type.getSimpleName() + " in " + events());
    }

    public long count(final Class<? extends AssetsEngineEvent> type) {
        return events().stream().filter(type::isInstance).count();
    }

    /** The FEED_HALT OrderFaults since the mark. */
    public List<AssetsEngineEvent.OrderFault> feedHaltFaults() {
        final List<AssetsEngineEvent.OrderFault> out = new ArrayList<>();
        for (final AssetsEngineEvent.OrderFault f : all(AssetsEngineEvent.OrderFault.class)) {
            if (f.kind() == OrderFaultKind.FEED_HALT) {
                out.add(f);
            }
        }
        return out;
    }

    public AssetsEngineEvent.AdminAck onlyAck() {
        final List<AssetsEngineEvent.AdminAck> acks = all(AssetsEngineEvent.AdminAck.class);
        assertEquals("exactly one AdminAck, got " + events(), 1, acks.size());
        return acks.get(0);
    }

    /** Assert the latch slot. */
    public void assertLatch(final String what, final FeedHaltReason reason, final long expected, final long seen,
                            final long skip, final long tradeId) {
        final FeedState f = engine.feed();
        assertEquals(what + ": latch reason", reason, f.latchReason());
        assertEquals(what + ": latchExpectedSeq", expected, f.latchExpectedSeq());
        assertEquals(what + ": latchSeenSeq", seen, f.latchSeenSeq());
        assertEquals(what + ": latchSkipSeq", skip, f.latchSkipSeq());
        assertEquals(what + ": latchTradeId", tradeId, f.latchTradeId());
    }

    public void assertNotLatched(final String what) {
        assertEquals(what + ": not latched", FeedHaltReason.NONE, engine.feed().latchReason());
    }

    public void assertGloballyHalted(final String what) {
        assertEquals(what + ": GLOBAL SUSPENDED", GateState.SUSPENDED, engine.gate().state(OrderEntryGate.GLOBAL));
        assertTrue(what + ": GLOBAL carries BRIDGE_HALT, reasons 0x" + Integer.toHexString(globalReasons()),
                globalHas(GateReason.BRIDGE_HALT));
    }

    // ---------------------------------------------------------------- money and orders

    public void deposit(final long user, final int asset, final long amount) {
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setUserId(user);
        d.setAssetId(asset);
        d.setAmount(amount);
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, now);
    }

    public CreateOrderCommand limit(final long user, final int side, final long price, final long qty) {
        final CreateOrderCommand c = new CreateOrderCommand();
        c.reset();
        c.correlationId = 7_000 + nextRequest;
        c.userId = user;
        c.marketId = MKT;
        c.side = side;
        c.orderType = 0;
        c.timeInForce = 0;
        c.price = price;
        c.quantity = qty;
        c.requestIdHigh = 0xABCDL;
        c.requestIdLow = nextRequest++;
        return c;
    }

    public CreateOrderCommand iceberg(final long user, final int side, final long price, final long qty, final long display) {
        final CreateOrderCommand c = limit(user, side, price, qty);
        c.orderType = 6;
        c.displayQty = display;
        return c;
    }

    /** Create and return the orderId (the order must be admitted). */
    public long create(final CreateOrderCommand c) {
        final long before = engine.getNextOrderId() <= 0 ? 1L : engine.getNextOrderId();
        engine.orders().applyCreate(c, now);
        final var o = engine.orders().orders().get(before);
        if (o == null) {
            throw new AssertionError("order not admitted: " + sink.events().get(sink.events().size() - 1));
        }
        return before;
    }

    /** AmendOrder (52) with a fresh requestId; {@code 0} keeps the current price or quantity. */
    public void amend(final long user, final long orderId, final long newPrice, final long newQty) {
        final com.openexchange.assets.domain.commands.AmendOrderCommand c = new com.openexchange.assets.domain.commands.AmendOrderCommand();
        c.reset();
        c.correlationId = 5_200 + nextRequest;
        c.userId = user;
        c.orderId = orderId;
        c.requestIdHigh = 0xABCDL;
        c.requestIdLow = nextRequest++;
        c.newPrice = newPrice;
        c.newQuantity = newQty;
        engine.orders().applyAmend(c, now);
    }

    /** A MarketTick with {@code meEpoch}. */
    public void tick(final long version, final long bid, final long ask, final long meEpoch) {
        engine.orders().applyMarketTick(MKT, version, bid, ask, 0L, 0L, meEpoch, now);
    }

    public com.openexchange.assets.application.engine.order.CoreOrder order(final long orderId) {
        return engine.orders().orders().get(orderId);
    }

    public long locked(final long user, final int asset) {
        final var a = engine.account(user);
        return a == null ? 0L : a.locked(asset);
    }

    public long total(final long user, final int asset) {
        final var a = engine.account(user);
        return a == null ? 0L : a.available(asset) + a.locked(asset);
    }

    // ---------------------------------------------------------------- the ME feed (next journalSeq, bound epoch)

    public long nextJseq() {
        return ++jseq;
    }

    public boolean trade(final long tradeId, final long takerOrder, final long takerLeg, final long takerUser,
                         final long makerOrder, final long makerLeg, final long makerUser, final long px, final long qty,
                         final boolean takerIsBuy) {
        return tradeAt(nextJseq(), EPOCH, ++egress, tradeId, takerOrder, takerLeg, takerUser, makerOrder, makerLeg, makerUser, px,
                qty, takerIsBuy);
    }

    public boolean tradeAt(final long js, final long epoch, final long g, final long tradeId, final long takerOrder,
                           final long takerLeg, final long takerUser, final long makerOrder, final long makerLeg,
                           final long makerUser, final long px, final long qty, final boolean takerIsBuy) {
        return engine.onMeTrade(g, js, tradeId, MKT, takerLeg, takerUser, makerLeg, makerUser, px, qty, takerIsBuy, now,
                takerOrder, makerOrder, epoch, now);
    }

    public boolean terminal(final long orderId, final long leg, final long user, final int status, final int meReason) {
        return terminalAt(nextJseq(), EPOCH, ++egress, orderId, leg, user, status, meReason);
    }

    public boolean terminalAt(final long js, final long epoch, final long g, final long orderId, final long leg, final long user,
                              final int status, final int meReason) {
        return engine.onMeTerminal(g, js, leg, user, MKT, status, meReason, now, orderId, epoch, now);
    }

    /** An outcome for {@code attempt} of {@code kind} on {@code orderId}. */
    public boolean outcome(final long orderId, final int attempt, final int kind, final int result, final long leg,
                           final int reason, final boolean oldCancelled) {
        return outcomeAt(nextJseq(), EPOCH, ++egress, orderId, attempt, kind, result, leg, reason, oldCancelled);
    }

    public boolean outcomeAt(final long js, final long epoch, final long g, final long orderId, final int attempt, final int kind,
                             final int result, final long leg, final int reason, final boolean oldCancelled) {
        final var o = engine.orders().orders().get(orderId);
        return engine.onMeOutcome(g, js, orderId, ((long) attempt << 2) | kind, o == null ? 0L : o.userId, orderId, 0L, 0L, 0L,
                0L, MKT, kind, result, leg, reason, oldCancelled, now, epoch, now);
    }

    /** CREATE APPLIED for attempt {@code attempt}: the core learns {@code leg}. */
    public boolean ack(final long orderId, final int attempt, final long leg) {
        return outcome(orderId, attempt, KIND_CREATE, 0, leg, 0, false);
    }

    public boolean release() {
        return engine.onMeRelease(++egress, nextJseq(), EPOCH, now);
    }

    public boolean releaseAt(final long js, final long epoch, final long g) {
        return engine.onMeRelease(g, js, epoch, now);
    }

    /** A BUMP 68 at the next journalSeq. */
    public boolean bump(final long configVersion) {
        return configAt(nextJseq(), EPOCH, ++egress, configVersion, V3Genesis.ME_BEHAVIOR, V3Genesis.ME_CAPACITY,
                V3Genesis.ME_MAX_WIRE, ConfigDecision.BUMP);
    }

    public boolean configAt(final long js, final long epoch, final long g, final long configVersion, final long behavior,
                            final long capacity, final int maxWire, final ConfigDecision decision) {
        return engine.onMeEngineConfig(g, js, epoch, configVersion, behavior, capacity, maxWire, decision, now,
                demuxerRevocations::add);
    }

    /** Sessions the 68 handler revoked when called through this rig directly (the demuxer path uses the hook). */
    public final List<Long> demuxerRevocations = new ArrayList<>();

    public void poison(final int template, final long js, final long epoch, final long rawTradeId) {
        engine.onFeedPoison(template, js, epoch, rawTradeId, now);
    }

    public void halt(final FeedHaltReason reason, final long epochSeen, final long expected, final long seen,
                     final long bridge) {
        engine.applyFeedHalt(77L, 6_600L, reason, epochSeen, expected, seen, bridge, now);
    }

    public void clear(final FeedHaltReason reason, final long js) {
        engine.applyFeedLatchClear(7_600L, reason, js, now);
    }

    public void adminGate(final int market, final GateState state, final GateReason reason) {
        engine.applyOrderEntryGate(7_300L, market, state, reason, now);
    }

    // ---------------------------------------------------------------- the two-party book used by most feed tests

    /** Buyer 1 BUY 2 BTC @ 100 (orderId 1, leg 101), seller 2 SELL 2 BTC @ 100 (orderId 2, leg 202); acks at jseq 2, 3. */
    public void twoParty() {
        deposit(1, USD, 1_000_000 * ONE);
        deposit(2, BTC, 1_000 * ONE);
        final long buy = create(limit(1, 0, 100 * ONE, 2 * ONE));
        final long sell = create(limit(2, 1, 100 * ONE, 2 * ONE));
        if (buy != 1 || sell != 2) {
            throw new AssertionError("fixture order ids " + buy + "/" + sell);
        }
        ack(1, 0, 101);
        ack(2, 0, 202);
    }

    /** Seller (order 2, leg 202) takes against buyer (order 1, leg 101) at 100 for {@code qty}. */
    public boolean sellHits(final long tradeId, final long qty) {
        return trade(tradeId, 2, 202, 2, 1, 101, 1, 100 * ONE, qty, false);
    }

    public boolean sellHitsAt(final long js, final long epoch, final long g, final long tradeId, final long qty) {
        return tradeAt(js, epoch, g, tradeId, 2, 202, 2, 1, 101, 1, 100 * ONE, qty, false);
    }

    // ---------------------------------------------------------------- frames and sessions (demuxer-level rows)

    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[16 * 1024]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    public UnsafeBuffer buffer() {
        return buf;
    }

    public MessageHeaderEncoder header() {
        return header;
    }

    public void dispatchTrusted(final int length) {
        demuxer.dispatchTrusted(buf, 0, length, now);
    }

    public void dispatch(final long sessionId, final int length) {
        demuxer.dispatch(sessionId, buf, 0, length, now);
    }

    public long openSession(final long sessionId, final SessionClass cls) {
        engine.sessions().open(sessionId, ClusterPrincipal.anyUser(AssetsSessionClasses.toKit(cls), (int) sessionId).encode());
        return sessionId;
    }

    /** One 62 entry: {jseq, epoch, egressSeq, tradeId, takerIsBuyRaw}. */
    public record BatchEntry(long jseq, long epoch, long egressSeq, long tradeId, int takerIsBuyRaw, long qty) { }

    /** A MeJournalTradeBatch of seller-hits-buyer trades (the {@link #twoParty} book). */
    public int tradeBatch(final List<BatchEntry> entries) {
        return tradeBatch(entries, 100 * ONE);
    }

    /** As {@link #tradeBatch(List)} at {@code price}. */
    public int tradeBatch(final List<BatchEntry> entries, final long price) {
        final MeJournalTradeBatchEncoder e = new MeJournalTradeBatchEncoder().wrapAndApplyHeader(buf, 0, header);
        final MeJournalTradeBatchEncoder.TradesEncoder g = e.tradesCount(entries.size());
        for (final BatchEntry t : entries) {
            g.next().egressSeq(t.egressSeq()).tradeId(t.tradeId()).marketId(MKT).takerOrderId(202).takerUserId(2)
                    .makerOrderId(101).makerUserId(1).price(price).quantity(t.qty()).takerIsBuy(BoolFlag.FALSE)
                    .meTimestamp(now).takerOmsOrderId(2).makerOmsOrderId(1).meEpoch(t.epoch()).journalSeq(t.jseq());
        }
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
        // raw takerIsBuy bytes (poison rows) written after the fact, at the generated group-relative offset
        final int groupStart = MessageHeaderEncoder.ENCODED_LENGTH + MeJournalTradeBatchEncoder.BLOCK_LENGTH + 4;
        for (int i = 0; i < entries.size(); i++) {
            buf.putByte(groupStart + i * MeJournalTradeBatchEncoder.TradesEncoder.sbeBlockLength()
                    + MeJournalTradeBatchEncoder.TradesEncoder.takerIsBuyEncodingOffset(), (byte) entries.get(i).takerIsBuyRaw());
        }
        return length;
    }
}

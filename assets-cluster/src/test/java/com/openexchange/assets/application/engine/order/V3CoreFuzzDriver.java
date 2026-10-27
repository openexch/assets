// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.application.engine.gate.OrderEntryGate;
import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.determinism.V3Genesis;
import com.openexchange.assets.domain.DispatchKind;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.GateState;
import com.openexchange.assets.domain.ReleaseScope;
import com.openexchange.assets.domain.commands.CreateOrderCommand;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S8 G10 (and the G1 reference model, the A-1 density guard): a seeded driver of behaviour 3 against a reference ME
 * that follows S4 (B2 floors and tombstones, the B5-B7 inflight record, 4.2 release semantics, all-NOOP frames not
 * journaled, record 3 tail with the true {@code ledgerSize}), S5 (native IOC/FOK, the expectedOldRemaining check, reason
 * 16) and the dense journal. The core's MeDispatch stream is the ME's inbox (FIFO, the forwarder); the ME journal is
 * delivered FIFO with dense journalSeq. With {@code faults} the ME also answers INVALID, CAPACITY (re-offered by a
 * RequestPendingDispatch resync), RELEASED (as if a floor covered the id), REJECTED, LEG_LIVE on an ORDER release with no
 * leg, and a 63 naming leg 0. After every core apply: the S8 invariant set, dispatch density ({@code (old, new]} in
 * order), and every resync reply equal to its minted frame; a snapshot round trip at random steps.
 */
final class V3CoreFuzzDriver {

    private static final long ONE = V3Rig.ONE;
    private static final int C = (int) V3Genesis.ME_CAPACITY;

    final V3Rig r = V3Rig.open();
    private final Random rng;
    private final boolean faults;
    private final long seed;
    private int step;

    // ---- the reference ME
    private final Map<Long, TreeSet<Long>> inflight = new HashMap<>();
    private final Map<Long, Long> floor = new HashMap<>();
    private final Set<Long> tomb = new HashSet<>();
    /** h -> {leg, qty, price, side, user}. */
    private final Map<Long, long[]> rest = new TreeMap<>();
    private long nextLeg = 1_000;
    private long nextTrade = 1;
    /** Ids for which the core has applied a resolving journal entry (record 3, or a trade/terminal of a CREATE: G1). */
    private final Set<String> resolvedAtCore = new HashSet<>();
    /** Every trading or orphan id the core minted, per order: {idLow, kind}. */
    private final Map<Long, List<long[]>> mintedIds = new HashMap<>();

    // ---- the forwarder and the journal
    private final ArrayDeque<AssetsEngineEvent.MeDispatch> inbox = new ArrayDeque<>();
    private final Map<Long, AssetsEngineEvent.MeDispatch> minted = new HashMap<>();
    private final ArrayDeque<Runnable> journal = new ArrayDeque<>();
    private boolean resyncWanted;

    // ---- what the fuzz observed (the tests read these)
    final Set<Long> diedFlagged = new HashSet<>();
    long resyncs;
    long roundTrips;
    long capacityInjected;
    long invalidInjected;
    long releasedInjected;
    long legLiveInjected;
    long legZeroInjected;
    long releaseMints;

    V3CoreFuzzDriver(final long seed, final boolean faults) {
        this.seed = seed;
        this.faults = faults;
        this.rng = new Random(seed);
        for (long u = 1; u <= 3; u++) {
            r.deposit(u, V3Rig.USD, 10_000_000 * ONE);
            r.deposit(u, V3Rig.BTC, 100_000 * ONE);
        }
    }

    private String tag() {
        return "seed " + seed + (faults ? " (faults)" : "") + " step " + step;
    }

    void run(final int steps) {
        for (int i = 0; i < steps; i++) {
            step = i;
            final int roll = rng.nextInt(100);
            if (roll < 18) {
                userCreate();
            } else if (roll < 26) {
                userCancel();
            } else if (roll < 32) {
                userAmend();
            } else if (roll < 56) {
                meProcess();
            } else if (roll < 78) {
                deliver(1 + rng.nextInt(3));
            } else if (roll < 88) {
                meTrade();
            } else if (roll < 91) {
                resyncWanted = true;
                resync();
            } else if (roll < 94) {
                adminOpen();
            } else if (roll < 96) {
                sweep();
            } else if (roll < 98 && faults) {
                legZero();
            } else {
                roundTrips++;
                S8Support.roundTrip(r.engine, tag());
            }
        }
    }

    // ---------------------------------------------------------------- the core side

    /** Run one core apply: mark, apply, then the invariant set, density and the inbox capture. */
    private void core(final Runnable apply) {
        final long before = r.engine.getDispatchSeq();
        final Set<Long> flagged = new HashSet<>();
        r.engine.orders().orders().forEachByOrderId(o -> {
            if (o.has(CoreOrder.F_ME_RELEASED)) {
                flagged.add(o.orderId);
            }
        });
        r.mark();
        apply.run();
        final long after = r.engine.getDispatchSeq();
        final List<AssetsEngineEvent.MeDispatch> out = r.all(AssetsEngineEvent.MeDispatch.class);
        assertEquals(tag() + ": every dispatchSeq of the apply is broadcast once (A-1 density)", after - before, out.size());
        for (int i = 0; i < out.size(); i++) {
            assertEquals(tag() + ": broadcast in ascending order", before + 1 + i, out.get(i).dispatchSeq());
            final AssetsEngineEvent.MeDispatch d = out.get(i);
            minted.put(d.dispatchSeq(), d);
            inbox.addLast(d);
            if (d.kind() == DispatchKind.RELEASE_ID) {
                releaseMints++;
                mintedNotEarly(d);
            } else {
                mintedIds.computeIfAbsent(d.commandIdHigh(), k -> new ArrayList<>()).add(new long[] {d.commandIdLow(), d.kind().code()});
            }
        }
        for (final long h : flagged) {
            if (r.engine.orders().orders().get(h) == null) {
                diedFlagged.add(h);
            }
        }
        S8Support.checkInvariants(r, tag());
        assertEquals(tag() + ": the reference ME never makes the core latch", com.openexchange.assets.domain.FeedHaltReason.NONE,
                r.feed().latchReason());
    }

    private void userCreate() {
        final long user = 1 + rng.nextInt(3);
        final int side = rng.nextInt(2);
        final long px = (95 + rng.nextInt(11)) * ONE;
        final long qty = (1 + rng.nextInt(10)) * (ONE / 10);
        final CreateOrderCommand c = r.limit(user, side, px, qty);
        final int t = rng.nextInt(100);
        if (t < 8) {
            c.timeInForce = OrderEngine.TIF_IOC;
        } else if (t < 12) {
            c.timeInForce = OrderEngine.TIF_FOK;
        } else if (t < 22) {
            c.timeInForce = OrderEngine.TIF_GTD;
            c.expiresAtMs = r.now + 1_000 + rng.nextInt(20_000);
        }
        core(() -> r.engine.orders().applyCreate(c, r.now));
    }

    private List<CoreOrder> live() {
        final List<CoreOrder> out = new ArrayList<>();
        r.engine.orders().orders().forEachByOrderId(out::add);
        return out;
    }

    private void userCancel() {
        final List<CoreOrder> l = live();
        if (l.isEmpty()) {
            return;
        }
        final CoreOrder o = l.get(rng.nextInt(l.size()));
        final long user = o.userId;
        final long id = o.orderId;
        core(() -> S8Support.cancel(r, user, id));
    }

    private void userAmend() {
        final List<CoreOrder> l = live();
        l.removeIf(o -> o.currentLegId == 0 || o.has(CoreOrder.F_DISPATCH_PENDING) || o.has(CoreOrder.F_CANCEL_PENDING)
                || o.has(CoreOrder.F_REPLACE_PENDING) || o.has(CoreOrder.F_IOC) || o.has(CoreOrder.F_FOK));
        if (l.isEmpty()) {
            return;
        }
        final CoreOrder o = l.get(rng.nextInt(l.size()));
        final long newQty = o.filledQty + (1 + rng.nextInt(10)) * (ONE / 10);
        final long user = o.userId;
        final long id = o.orderId;
        core(() -> r.amend(user, id, 0, newQty));
    }

    private void adminOpen() {
        core(() -> {
            if (r.engine.gate().state(V3Rig.MKT) != GateState.OPEN) {
                r.adminGate(V3Rig.MKT, GateState.OPEN, GateReason.ADMIN);
            }
            if (r.engine.gate().state(OrderEntryGate.GLOBAL) != GateState.OPEN) {
                r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
            }
        });
    }

    private void sweep() {
        r.now += 1 + rng.nextInt(5_000);
        core(() -> r.engine.sweep(r.now));
    }

    private void legZero() {
        final List<CoreOrder> l = live();
        if (l.isEmpty()) {
            return;
        }
        final CoreOrder o = l.get(rng.nextInt(l.size()));
        final long id = o.orderId;
        final long user = o.userId;
        legZeroInjected++;
        final long js = ++r.jseq; // journal order: the seq is taken when the ME journals the entry
        final long g = ++r.egress;
        journal.addLast(() -> core(() -> r.engine.onMeTerminal(g, js, 0L, user, V3Rig.MKT, V3Rig.ME_CANCELLED, 0, r.now, id,
                V3Rig.EPOCH, r.now)));
    }

    /** The forwarder's resync: every pending entry, each reply equal to its minted frame, appended to the inbox. */
    private void resync() {
        if (!resyncWanted) {
            return;
        }
        resyncWanted = false;
        resyncs++;
        core(() -> r.engine.orders().applyRequestPendingDispatch(0x5E, 0L, 4096));
        for (final AssetsEngineEvent.MeDispatchReply d : r.all(AssetsEngineEvent.MeDispatchReply.class)) {
            final AssetsEngineEvent.MeDispatch m = minted.get(d.dispatchSeq());
            assertEquals(tag() + ": the reply equals the minted frame", m.render().replace("MEDISPATCH", "DISPATCHREPLY"), d.render());
            inbox.addLast(m);
        }
    }

    /** Deliver up to {@code n} ME journal entries to the core, in order. */
    private void deliver(final int n) {
        for (int i = 0; i < n && !journal.isEmpty(); i++) {
            journal.pollFirst().run();
        }
    }

    // ---------------------------------------------------------------- the reference ME

    private static String key(final long h, final long low) {
        return h + "/" + low;
    }

    private long ledgerSize() {
        long n = 0;
        for (final TreeSet<Long> s : inflight.values()) {
            n += s.size();
        }
        return n;
    }

    private boolean recorded(final long h, final long low) {
        final TreeSet<Long> s = inflight.get(h);
        return s != null && s.contains(low);
    }

    private void record(final long h, final long low, final int kind) {
        inflight.computeIfAbsent(h, k -> new TreeSet<>()).add(low);
    }

    /** Journal a record 3 for {@code d} (the S4 3.3 tail; the CAPACITY row reports ledgerSize = C). */
    private void outcome(final AssetsEngineEvent.MeDispatch d, final int result, final long leg, final int reason) {
        final long size = result == S8Support.CAPACITY ? C : ledgerSize();
        final long js = ++r.jseq;
        final long g = ++r.egress;
        final int kind = d.kind().code();
        journal.addLast(() -> {
            final MeOutcomeView v = new MeOutcomeView();
            v.egressSeq = g;
            v.journalSeq = js;
            v.meEpoch = V3Rig.EPOCH;
            v.commandIdHigh = d.commandIdHigh();
            v.commandIdLow = d.commandIdLow();
            v.userId = Math.max(1L, d.userId());
            v.omsOrderId = Math.max(1L, d.omsOrderId());
            v.oldOrderId = d.oldOrderId();
            v.price = d.price();
            v.quantity = d.quantity();
            v.budget = d.budget();
            v.marketId = Math.max(1, d.marketId());
            v.commandKind = kind;
            v.orderType = d.orderType();
            v.orderSide = d.side();
            v.appliedPosition = g;
            v.meTimestamp = r.now;
            v.orderId = result <= 1 ? leg : 0L;
            v.status = result == 0 ? 0 : result == 1 ? 4 : -1;
            v.reason = reason;
            v.result = result;
            v.timeInForce = d.timeInForce();
            v.expectedOldRemaining = d.expectedOldRemaining();
            v.ledgerSize = size;
            v.ledgerCapacity = C;
            v.behaviorVersion = V3Genesis.ME_BEHAVIOR;
            resolvedAtCore.add(key(d.commandIdHigh(), d.commandIdLow())); // applied in the next line, before any mint it causes
            core(() -> r.apply(v));
        });
    }

    private void terminal(final long h, final long leg, final long user, final int status, final int reason) {
        final long js = ++r.jseq;
        final long g = ++r.egress;
        journal.addLast(() -> {
            implicitEvidence(h);
            core(() -> r.engine.onMeTerminal(g, js, leg, user, V3Rig.MKT, status, reason, r.now, h, V3Rig.EPOCH, r.now));
        });
    }

    /** A trade or terminal of {@code h} applied by the core resolves its CREATE ids (M1/M4 implicit acks). */
    private void implicitEvidence(final long h) {
        for (final long[] id : mintedIds.getOrDefault(h, List.of())) {
            if (id[1] == 0) {
                resolvedAtCore.add(key(h, id[0]));
            }
        }
    }

    /**
     * G1 "minted strictly after its resolving entry": every id of h at or below an UP_TO floor (every CREATE id for an
     * ORDER; rule 3 makes an ORDER over a CANCEL or AMEND in flight moot) has a resolving entry the core already applied.
     */
    private void mintedNotEarly(final AssetsEngineEvent.MeDispatch d) {
        final long h = d.commandIdHigh();
        for (final long[] id : mintedIds.getOrDefault(h, List.of())) {
            final boolean covered = d.releaseScope() == ReleaseScope.ORDER ? id[1] == 0 : id[0] <= d.commandIdLow();
            if (covered && !resolvedAtCore.contains(key(h, id[0]))) {
                fail(tag() + ": release " + d.render() + " minted before the core applied a resolution of id " + key(h, id[0]));
            }
        }
    }

    /** The ME applies the oldest inbox entry (S4 3.2 rows, 4.2 for a release). */
    private void meProcess() {
        if (inbox.isEmpty()) {
            return;
        }
        final AssetsEngineEvent.MeDispatch d = inbox.pollFirst();
        final long h = d.commandIdHigh();
        final long low = d.commandIdLow();
        if (d.kind() == DispatchKind.RELEASE_ID) {
            release(h, low, d.releaseScope());
            return;
        }
        if (tomb.contains(h) || (floor.containsKey(h) && low <= floor.get(h))) {
            outcome(d, S8Support.RELEASED, 0L, 0); // B2
            return;
        }
        if (recorded(h, low)) {
            return; // B3: an equal-intent duplicate is suppressed, nothing journaled
        }
        if (faults) {
            final int f = rng.nextInt(100);
            if (f < 4) {
                invalidInjected++;
                outcome(d, S8Support.INVALID, 0L, 0);
                return;
            }
            if (f < 7) {
                capacityInjected++;
                outcome(d, S8Support.CAPACITY, 0L, 0);
                resyncWanted = true;
                return;
            }
            if (f < 9 && d.kind() != DispatchKind.CANCEL) {
                // as if a floor at this id existed (another release path): RELEASED, every id of h at or below it gone
                releasedInjected++;
                floor.merge(h, low, Math::max);
                final TreeSet<Long> s = inflight.get(h);
                if (s != null) {
                    s.headSet(low, true).clear();
                }
                outcome(d, S8Support.RELEASED, 0L, 0);
                return;
            }
        }
        final long[] leg = rest.get(h);
        switch (d.kind()) {
            case CREATE: {
                record(h, low, 0);
                final long newLeg = ++nextLeg;
                if (faults && rng.nextInt(100) < 5) {
                    outcome(d, S8Support.REJECTED, newLeg, 3);
                } else if (d.timeInForce() == OrderEngine.TIF_IOC || d.timeInForce() == OrderEngine.TIF_FOK) {
                    terminal(h, newLeg, d.userId(), V3Rig.ME_CANCELLED, 12); // S5: the ME ends it inside the create
                    outcome(d, S8Support.APPLIED, newLeg, 0);
                } else {
                    rest.put(h, new long[] {newLeg, d.quantity(), d.price(), d.side(), d.userId()});
                    outcome(d, S8Support.APPLIED, newLeg, 0);
                }
                break;
            }
            case CANCEL: {
                record(h, low, 1);
                if (leg != null && leg[0] == d.oldOrderId()) {
                    rest.remove(h);
                    terminal(h, leg[0], leg[4], V3Rig.ME_CANCELLED, 0);
                    outcome(d, S8Support.APPLIED, leg[0], 0);
                } else {
                    outcome(d, S8Support.UNKNOWN_LEG, 0L, 0);
                }
                break;
            }
            default: { // AMEND (S5: the old leg's remaining must equal expectedOldRemaining)
                record(h, low, 2);
                if (leg == null || leg[0] != d.oldOrderId()) {
                    outcome(d, S8Support.UNKNOWN_LEG, 0L, 0);
                } else if (leg[1] != d.expectedOldRemaining()) {
                    outcome(d, S8Support.REJECTED, leg[0], 16);
                } else {
                    final long newLeg = ++nextLeg;
                    rest.put(h, new long[] {newLeg, d.quantity(), d.price(), leg[3], leg[4]});
                    outcome(d, S8Support.APPLIED, newLeg, 0);
                }
                break;
            }
        }
    }

    private void release(final long h, final long f, final ReleaseScope scope) {
        final int result;
        int removed = 0;
        if (scope == ReleaseScope.UP_TO) {
            if (tomb.contains(h) || (floor.containsKey(h) && f <= floor.get(h))) {
                return; // NOOP: an all-NOOP frame is never journaled (S4 4.3)
            }
            final TreeSet<Long> s = inflight.get(h);
            if (s != null) {
                removed = s.headSet(f, true).size();
                s.headSet(f, true).clear();
            }
            floor.put(h, f);
            result = S8Support.R_APPLIED;
        } else {
            if (tomb.contains(h)) {
                return; // NOOP
            }
            if (rest.containsKey(h) || (faults && rng.nextInt(100) < 15)) {
                if (!rest.containsKey(h)) {
                    legLiveInjected++;
                }
                result = S8Support.R_LEG_LIVE;
            } else {
                final TreeSet<Long> s = inflight.remove(h);
                if (s != null) {
                    removed = s.size();
                }
                floor.remove(h);
                tomb.add(h);
                result = S8Support.R_APPLIED;
            }
        }
        final long js = ++r.jseq;
        final long g = ++r.egress;
        final long size = ledgerSize();
        final int rm = removed;
        journal.addLast(() -> {
            final MeReleaseView v = new MeReleaseView();
            v.egressSeq = g;
            v.journalSeq = js;
            v.meEpoch = V3Rig.EPOCH;
            v.releaseSeq = js;
            v.ledgerSize = size;
            v.ledgerCapacity = C;
            v.tombstoneRanges = tomb.size();
            v.add(h, f, scope == ReleaseScope.ORDER ? 1 : 0, result, rm);
            core(() -> r.apply(v));
        });
    }

    /** The ME trades two resting legs that cross (different users); the exhausted legs journal FILLED. */
    private void meTrade() {
        long[] buy = null, sell = null;
        long bh = 0, sh = 0;
        final List<Long> hs = new ArrayList<>(rest.keySet());
        java.util.Collections.shuffle(hs, rng);
        for (final long h : hs) {
            final long[] l = rest.get(h);
            if (l[3] == 0 && buy == null) {
                buy = l;
                bh = h;
            } else if (l[3] == 1 && sell == null) {
                sell = l;
                sh = h;
            }
        }
        if (buy == null || sell == null || buy[2] < sell[2] || buy[4] == sell[4]) {
            return;
        }
        final long qty = Math.min(Math.min(buy[1], sell[1]), (1 + rng.nextInt(5)) * (ONE / 10));
        final long px = sell[2];
        final long tradeId = nextTrade++;
        final long js = ++r.jseq;
        final long g = ++r.egress;
        final long[] b = buy, s = sell;
        final long buyH = bh, sellH = sh;
        final boolean takerIsBuy = rng.nextBoolean();
        journal.addLast(() -> implicitEvidence2(buyH, sellH));
        journal.addLast(() -> core(() -> {
            if (takerIsBuy) {
                r.engine.onMeTrade(g, js, tradeId, V3Rig.MKT, b[0], b[4], s[0], s[4], px, qty, true, r.now, buyH, sellH, V3Rig.EPOCH,
                        r.now);
            } else {
                r.engine.onMeTrade(g, js, tradeId, V3Rig.MKT, s[0], s[4], b[0], b[4], px, qty, false, r.now, sellH, buyH, V3Rig.EPOCH,
                        r.now);
            }
        }));
        buy[1] -= qty;
        sell[1] -= qty;
        if (buy[1] == 0) {
            rest.remove(bh);
            terminal(bh, buy[0], buy[4], V3Rig.ME_FILLED, 0);
        }
        if (sell[1] == 0) {
            rest.remove(sh);
            terminal(sh, sell[0], sell[4], V3Rig.ME_FILLED, 0);
        }
    }

    private void implicitEvidence2(final long a, final long b) {
        // bookkeeping only (no core apply, no journalSeq): the trade right behind this marker is the evidence
        implicitEvidence(a);
        implicitEvidence(b);
    }

    // ---------------------------------------------------------------- the end

    /**
     * End every order and run the ME until quiescent: user cancels, a counter order to trade out an order whose cancel the
     * core will not mint again (F_CANCEL_PENDING kept after INVALID/RELEASED, D-S8-3), resyncs for CAPACITY'd commands.
     * Then (review 2 FIX-1, G10): the backlog holds only PARKED entries and the reference ME ledger holds no entry of a
     * dead order that is not PARKED, orders that died with F_ME_RELEASED set included (review Q1, option (d)).
     */
    void drainAndCheckLedger() {
        for (int round = 0; round < 400; round++) {
            adminOpen();
            for (final CoreOrder o : live()) {
                if (!o.has(CoreOrder.F_CANCEL_PENDING)) {
                    final long user = o.userId, id = o.orderId;
                    core(() -> S8Support.cancel(r, user, id));
                } else if (!o.has(CoreOrder.F_DISPATCH_PENDING) && rest.containsKey(o.orderId) && rng.nextInt(4) == 0) {
                    counterTrade(o.orderId);
                }
            }
            for (int i = 0; i < 200 && (!inbox.isEmpty() || !journal.isEmpty()); i++) {
                meProcess();
                meTrade();
                deliver(3);
            }
            resyncWanted = true;
            resync();
            if (live().isEmpty() && inbox.isEmpty() && journal.isEmpty() && r.engine.orders().pendingCommands() == 0
                    && r.engine.orders().backlogRelease() + r.engine.orders().backlogOrphan() == 0) {
                break;
            }
        }
        final StringBuilder left = new StringBuilder();
        for (final CoreOrder o : live()) {
            left.append("\n  order ").append(o.orderId).append(" u=").append(o.userId).append(" status=").append(o.status)
                    .append(" flags=0x").append(Integer.toHexString(o.stateFlags)).append(" pendingKind=").append(o.pendingKind)
                    .append(" seq=").append(o.pendingDispatchSeq).append(" attempt=").append(o.attempt).append(" leg=")
                    .append(o.currentLegId).append(" rest=").append(java.util.Arrays.toString(rest.get(o.orderId)))
                    .append(" type=").append(o.orderType).append(" tif=").append(o.timeInForce);
        }
        assertTrue(tag() + ": every order ended; left: " + left + "\n gate global=0x"
                + Integer.toHexString(r.engine.gate().reasons(OrderEntryGate.GLOBAL)) + " market=0x"
                + Integer.toHexString(r.engine.gate().reasons(V3Rig.MKT)) + " latch=" + r.feed().latchReason(), live().isEmpty());
        assertEquals(tag() + ": no pending command", 0L, r.engine.orders().pendingCommands());
        assertEquals(tag() + ": the backlog drained (PARKED excepted)", r.engine.orders().backlogParked(),
                r.engine.orders().backlog().size());
        for (final Map.Entry<Long, TreeSet<Long>> e : inflight.entrySet()) {
            final ReleaseBacklog.Entry b = r.engine.orders().backlog().get(e.getKey());
            // review Q1 option (d): no exclusion for orders that died flagged (the fault fuzz must show no leak)
            if (e.getValue().isEmpty() || (b != null && b.stage == ReleaseBacklog.STAGE_PARKED)) {
                continue;
            }
            fail(tag() + ": the ME ledger still holds " + e.getValue() + " of dead order " + e.getKey());
        }
    }

    /** Orders that died with F_ME_RELEASED set and still own ME ledger entries (open question Q1 of the S8 README). */
    long flaggedLeaks() {
        long n = 0;
        for (final long h : diedFlagged) {
            final TreeSet<Long> s = inflight.get(h);
            if (s != null && !s.isEmpty()) {
                n++;
            }
        }
        return n;
    }

    /** A user-3 order at the same price on the other side, rested at the ME and traded against {@code h}'s leg. */
    private void counterTrade(final long h) {
        final long[] leg = rest.get(h);
        final long user = leg[4] == 3 ? 1 : 3;
        final CreateOrderCommand c = r.limit(user, leg[3] == 0 ? 1 : 0, leg[2], leg[1]);
        core(() -> r.engine.orders().applyCreate(c, r.now));
    }
}

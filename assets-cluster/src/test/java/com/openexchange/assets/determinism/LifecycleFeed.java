// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.assets.determinism.LifecycleFakeMe.Fill;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * The ME side of {@link LifecycleTrafficGenerator}: the fake ME's books, the core state the generator predicts from
 * what it journalled (live orders, the market top of book and last trade as the core applied them), and the feed lines
 * that follow every dispatch: {@code MEOUTCOME}, {@code METRADE}, {@code METERMINAL}, {@code MARKETTICK}, plus the
 * trigger book and the iceberg refill mirrored from the S6 rules. It shares the generator's {@link Random}, so the
 * stream is one sequence of draws whichever class makes them.
 */
final class LifecycleFeed {

    private static final int MARKETS = LifecycleTrafficGenerator.MARKETS;
    private static final long[] STEP = LifecycleTrafficGenerator.STEP;

    private final Random rnd;
    private final Consumer<String> sink;
    final LifecycleFakeMe me = new LifecycleFakeMe(MARKETS, LifecycleTrafficGenerator.LOT);
    final TreeMap<Long, LifecycleModelOrder> live = new TreeMap<>();
    final List<LifecycleModelOrder> terminated = new ArrayList<>();
    final Map<Long, boolean[]> received = new HashMap<>();
    final Map<Long, Set<Long>> activeCids = new HashMap<>();
    final Map<Long, List<Long>> freeCids = new HashMap<>();
    // the core's market state as the generator predicts it (applied ticks and settled trades only)
    final long[] bid = new long[MARKETS + 1];
    final long[] ask = new long[MARKETS + 1];
    final long[] ver = new long[MARKETS + 1];
    final long[] lastPx = new long[MARKETS + 1];
    final long[] lastTid = new long[MARKETS + 1];
    long tradeId;
    long legSeq = 100_000;
    int outcomes, trades, duplicateTrades, journalTerminals, earlyTerminals, marketTicks, staleTicks, oneSidedTicks;
    int triggers, slices;

    LifecycleFeed(final Random rnd, final Consumer<String> sink) {
        this.rnd = rnd;
        this.sink = sink;
    }


    void ack(final LifecycleModelOrder o) {
        o.leg = ++legSeq;
        outcomes++;
        emit("MEOUTCOME order=" + o.orderId + " kind=0 attempt=" + o.attempt + " result=0 leg=" + o.leg);
    }

    /** The ME processes the leg: trades, the ME's end of an unfilled MARKET rest, then any iceberg refills. */
    void work(final LifecycleModelOrder o) {
        final List<LifecycleModelOrder> refills = new ArrayList<>();
        settle(me.submit(o), refills);
        if (o.meType == LifecycleModelOrder.MARKET && !o.terminal && o.legRemaining > 0) {
            journalTerminals++;
            emit("METERMINAL order=" + o.orderId + " leg=" + o.leg + " status=3");
            terminal(o);
        }
        for (final LifecycleModelOrder r : refills) {
            if (!r.terminal) {
                ack(r);
                work(r);
            }
        }
    }

    private void settle(final List<Fill> fills, final List<LifecycleModelOrder> refills) {
        if (fills.isEmpty()) {
            return;
        }
        final LifecycleModelOrder taker = fills.get(0).taker();
        long total = 0;
        for (final Fill f : fills) {
            total += f.quantity();
        }
        final boolean early = fills.size() >= 2 && taker.type != LifecycleModelOrder.ICEBERG
                && taker.filled + total == taker.quantity && rnd.nextInt(100) < 8;
        final long takerLeg = taker.leg;
        final List<long[]> exhausted = new ArrayList<>();
        for (int i = 0; i < fills.size(); i++) {
            final Fill f = fills.get(i);
            final LifecycleModelOrder mk = f.maker();
            if (early && i == fills.size() - 1) {
                earlyTerminals++;
                journalTerminals++;
                emit("METERMINAL order=" + taker.orderId + " leg=" + takerLeg + " status=2"); // before the last trade
                terminal(taker);
            }
            final String line = "METRADE id=" + (++tradeId) + " market=" + taker.market + " taker=" + taker.orderId
                    + " maker=" + mk.orderId + " px=" + fmt(f.price()) + " qty=" + fmt(f.quantity())
                    + " takerBuy=" + (taker.buy() ? 1 : 0) + " takerLeg=" + takerLeg + " makerLeg=" + mk.leg
                    + " takerUser=" + taker.user + " makerUser=" + mk.user;
            trades++;
            emit(line);
            if (rnd.nextInt(100) == 0) {
                duplicateTrades++;
                emit(line); // redelivered: same tradeId, a no-op
            }
            lastPx[taker.market] = f.price();
            lastTid[taker.market] = tradeId;
            final LifecycleModelOrder buyer = taker.buy() ? taker : mk;
            final LifecycleModelOrder seller = taker.buy() ? mk : taker;
            received.computeIfAbsent(buyer.user, k -> new boolean[6])[buyer.market] = true;
            received.computeIfAbsent(seller.user, k -> new boolean[6])[0] = true;
            if (mk.legRemaining == 0) {
                exhausted.add(new long[]{mk.orderId, mk.leg});
            }
            fill(taker, f.quantity(), refills);
            fill(mk, f.quantity(), refills);
        }
        if (taker.legRemaining == 0) {
            exhausted.add(new long[]{taker.orderId, takerLeg});
        }
        for (final long[] e : exhausted) {
            journalTerminals++;
            emit("METERMINAL order=" + e[0] + " leg=" + e[1] + " status=2"); // the leg's FILLED journal entry
        }
    }

    private void fill(final LifecycleModelOrder o, final long q, final List<LifecycleModelOrder> refills) {
        if (o.terminal) {
            return; // the core has no record: money settles, order accounting is skipped
        }
        o.filled += q;
        if (o.filled == o.quantity) {
            terminal(o);
            return;
        }
        if (o.type == LifecycleModelOrder.ICEBERG) {
            o.sliceRemaining -= q;
            if (o.sliceRemaining == 0 && o.hidden > 0) {
                final long next = Math.min(o.display, o.hidden);
                o.hidden -= next;
                o.sliceRemaining = next;
                o.attempt++;
                o.leg = 0;
                o.legRemaining = next;
                o.legPrice = o.price;
                slices++;
                refills.add(o);
            }
        }
    }

    /** Tell the core the new top of book after a change, then run what its trigger book fires, until quiet. */
    void publish(final int m) {
        for (int guard = 0; guard < 40; guard++) {
            final long b = me.bestBid(m);
            final long a = me.bestAsk(m);
            if (b == bid[m] && a == ask[m]) {
                return;
            }
            final boolean oneSided = b > 0 && a > 0 && rnd.nextInt(50) == 0;
            if (oneSided) {
                oneSidedTicks++;
            }
            marketTick(m, b, oneSided ? 0L : a);
            final List<LifecycleModelOrder> fired = evaluate(m);
            if (rnd.nextInt(50) == 0) {
                final long sb = Math.max(0L, b - STEP[m]);
                final long sa = a > 0 ? a + STEP[m] : 0L;
                staleTicks++;
                emit("MARKETTICK market=" + m + " bid=" + fmt(sb) + " ask=" + fmt(sa) + " ver=" + ver[m]); // not newer
            }
            if (fired.isEmpty()) {
                return;
            }
            for (final LifecycleModelOrder o : fired) {
                fire(o);
            }
        }
    }

    private void marketTick(final int m, final long b, final long a) {
        marketTicks++;
        ver[m]++;
        bid[m] = b;
        ask[m] = a;
        emit("MARKETTICK market=" + m + " bid=" + fmt(b) + " ask=" + fmt(a) + " ver=" + ver[m]
                + (lastPx[m] > 0 ? " last=" + fmt(lastPx[m]) + " lastTradeId=" + lastTid[m] : ""));
    }

    /** S6 3.1 #4-5 mirrored: stops on the quote they watch, trailing arms advance, fired in ascending orderId. */
    private List<LifecycleModelOrder> evaluate(final int m) {
        final List<LifecycleModelOrder> fired = new ArrayList<>();
        for (final LifecycleModelOrder o : live.values()) {
            if (o.market != m || !o.armed) {
                continue;
            }
            final boolean sell = !o.buy();
            if (o.isStop()) {
                if (sell ? bid[m] > 0 && bid[m] <= o.stop : ask[m] > 0 && ask[m] >= o.stop) {
                    fired.add(o);
                }
                continue;
            }
            final long quote = sell ? bid[m] : ask[m];
            if (quote <= 0) {
                continue;
            }
            if (o.trailArm == 0) {
                o.trailArm = quote;
            } else if (sell ? quote > o.trailArm : quote < o.trailArm) {
                o.trailArm = quote;
            } else if ((sell ? o.trailArm - quote : quote - o.trailArm) >= o.delta) {
                fired.add(o);
            }
        }
        return fired;
    }

    private void fire(final LifecycleModelOrder o) {
        triggers++;
        o.armed = false;
        o.attempt++;
        final boolean marketChild = o.type != LifecycleModelOrder.STOP_LIMIT;
        o.meType = marketChild ? LifecycleModelOrder.MARKET : LifecycleModelOrder.LIMIT;
        o.legPrice = marketChild ? 0L : o.price;
        o.legRemaining = o.remaining();
        o.legBudget = marketChild && o.buy() ? o.hold : 0L;
        ack(o);
        work(o);
    }


    void terminal(final LifecycleModelOrder o) {
        o.terminal = true;
        live.remove(o.orderId);
        me.remove(o);
        if (o.cid != 0) {
            activeCids.getOrDefault(o.user, new HashSet<>()).remove(o.cid);
            freeCids.computeIfAbsent(o.user, k -> new ArrayList<>()).add(o.cid);
        }
        terminated.add(o);
    }

    /** One CREATE line in the runner's DSL; zero-valued optional fields are left out. */
    static String createLine(final long user, final int market, final int side, final int type, final long px, final long qty,
                             final long budget, final int tif, final long exp, final long stop, final long delta,
                             final long display, final long cid, final long rid) {
        final StringBuilder sb = new StringBuilder("CREATE u=").append(user).append(" market=").append(market)
                .append(" side=").append(side).append(" type=").append(type);
        amount(sb, " px=", px);
        sb.append(" qty=").append(fmt(qty));
        amount(sb, " budget=", budget);
        if (tif > 0) {
            sb.append(" tif=").append(tif);
        }
        if (exp > 0) {
            sb.append(" exp=").append(exp);
        }
        amount(sb, " stop=", stop);
        amount(sb, " delta=", delta);
        amount(sb, " display=", display);
        if (cid > 0) {
            sb.append(" cid=").append(cid);
        }
        if (rid > 0) {
            sb.append(" rid=").append(rid);
        }
        return sb.toString();
    }

    private static void amount(final StringBuilder sb, final String key, final long value) {
        if (value > 0) {
            sb.append(key).append(fmt(value));
        }
    }

    private void emit(final String line) {
        sink.accept(line);
    }

    private static String fmt(final long raw) {
        return LifecycleTrafficGenerator.fmt(raw);
    }
}

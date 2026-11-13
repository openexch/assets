// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.loadgen;

import com.openexchange.core.infrastructure.generated.AdminAckDecoder;
import com.openexchange.core.infrastructure.generated.BoolFlag;
import com.openexchange.core.infrastructure.generated.CoreRejectReason;
import com.openexchange.core.infrastructure.generated.DepositAckDecoder;
import com.openexchange.core.infrastructure.generated.DepositRejectDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.OrderAckDecoder;
import com.openexchange.core.infrastructure.generated.OrderRejectDecoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Order load against the core alone: one authenticated GATEWAY session places resting LIMIT GTC orders (a ramp to
 * {@code --open-orders}), then runs a steady open-loop phase at {@code --rate} slots per second, a slot being one
 * CreateOrder plus one CancelOrder of the oldest open order (the open count stays constant; {@code --mix create-only}
 * sends the create alone). Every CreateOrder, CancelOrder and Deposit carries a fresh 128-bit requestId; the
 * clientOrderId is the requestId; the correlationId is the slot number. Latency is create (or cancel) to its OrderAck
 * from the slot's SCHEDULED time (coordinated-omission safe, {@link OpenLoopSchedule}).
 *
 * <p>Elections: on a NewLeader event, or when a command's answer is older than {@code --reoffer-after-ms}, the
 * unanswered commands are offered again with the same requestIds and correlationIds, at most once per election plus
 * once by age. The core answers a replayed command with its stored decision ({@code duplicate} set) and applies
 * nothing; a replayed answer is counted as {@code replayed}, never as new work. A command still unanswered when the run
 * drains is {@code unanswered} and fails the accounting.</p>
 *
 * <p>Two uses: the command line (its own driver or {@code --aeron-dir}, its own sessions) and {@link Session} inside a
 * test JVM against a rig's sessions. It sends only frames a GATEWAY, TOOL or ADMIN client may send ({@link OrderWire}).
 * The requestId table of the core keeps {@value #REQUESTS_PER_USER_CAP} decisions per user for
 * {@value #REQUEST_TTL_MS} ms, so the run needs
 * {@code users >= ceil((2 x rate x ttl + openOrders) / (cap - deposits))}, deposits being 2 per user with prefunding
 * and 0 without; below that it is refused,
 * because the run would measure the cap ({@code TOO_MANY_PENDING_REQUESTS}), not the system.</p>
 */
public final class CoreOrderLoadGenerator {

    /** The core's requestId table TTL (equal to {@code RequestTable.TTL_MS}; a core test asserts it). */
    public static final long REQUEST_TTL_MS = 15 * 60_000L;
    /** The core's per-user requestId cap (equal to {@code RequestTable.PER_USER_CAP}; a core test asserts it). */
    public static final int REQUESTS_PER_USER_CAP = 256;
    /** Fixed-point scale of prices and quantities (8 decimals). */
    public static final long ONE = 100_000_000L;
    /** The default base price per market id 1..5 (the rigs' table); a resting order never crosses it. */
    static final long[] DEFAULT_BASE_PRICE = {0, 50_000 * ONE, 3_000 * ONE, 150 * ONE, 2 * ONE, ONE};
    /** Base asset per market id 1..5 (BTC, ETH, SOL, XRP, DOGE); USD is asset 0. */
    static final int[] DEFAULT_BASE_ASSET = {-1, 1, 2, 3, 4, 5};
    static final int USD = 0;

    /** Deposits per user when prefunding (one USD, one base asset); each takes a requestId out of the user's cap. */
    public static final int PREFUND_DEPOSITS_PER_USER = 2;

    /** The minimum user count for {@code rate} slots per second, two requests per slot, no ramp, no prefunding. */
    public static long minUsers(final long rate) {
        return minUsers(rate, 0, false);
    }

    /**
     * The minimum user count for {@code rate} slots per second: two requests per slot over the TTL plus the ramp's
     * {@code openOrders} creates, spread over the users, each user's cap less the two prefund deposits when
     * {@code prefund} (the core counts a Deposit's requestId against the same cap).
     */
    public static long minUsers(final long rate, final long openOrders, final boolean prefund) {
        final long requests = 2L * rate * (REQUEST_TTL_MS / 1000) + openOrders;
        final long perUser = REQUESTS_PER_USER_CAP - (prefund ? PREFUND_DEPOSITS_PER_USER : 0);
        return (requests + perUser - 1) / perUser;
    }

    /** A refused setting: exit 2 with the message as the one line. */
    public static final class Refused extends IllegalArgumentException {
        public Refused(final String message) {
            super(message);
        }
    }

    // ------------------------------------------------------------------------------------------------ settings

    /**
     * The run's settings, from the command line or built by a test. Every value is checked by {@link #check()}, the
     * one refusal shape for the CLI and the in-JVM session.
     */
    public static final class Settings {
        public long rate = 1_000;
        public int durationSec = 60;
        public int warmupSec = 10;
        public int openOrders = 100_000;
        public int users = 10_000;
        public int markets = 5;
        public int window = 4_096;
        public boolean createOnly;
        public long reofferAfterMs = 2_000;
        /** Re-offer on; false only in the negative control (the unanswered tail after an election shows). */
        public boolean reoffer = true;
        public String aeronDir;
        public String idle = "backoff";
        public String out = "results";
        public String metricsUrl;
        public List<String> coreHosts = List.of("127.0.0.1");
        public int corePortBase = 19_300;
        public int stride;
        public int ordersPerUserMax = 500;
        public boolean prefund;
        public boolean gateOpen;
        public boolean register = true;
        public long prefundUsd = 1_000_000 * ONE;
        public long prefundBase = 1_000 * ONE;
        public long qty = ONE / 10;
        public String egressHost;
        /** The requestId high word of this generator (distinct per run; the low word is a sequence). */
        public long requestIdHigh = (System.currentTimeMillis() << 8) | 0x10;
        /** The first user id; users are {@code firstUser .. firstUser + users - 1}. */
        public long firstUser = 1;
        public long drainMs = 10_000;

        /** Refuses a setting outside its range with one line naming it. */
        public Settings check() {
            if (rate < 1) {
                throw new Refused("--rate must be at least 1 slot/s: " + rate);
            }
            if (durationSec < 1) {
                throw new Refused("--duration must be at least 1 s: " + durationSec);
            }
            if (warmupSec < 0 || warmupSec >= durationSec) {
                throw new Refused("--warmup must be 0.." + (durationSec - 1) + " s: " + warmupSec);
            }
            if (markets < 1 || markets >= DEFAULT_BASE_PRICE.length) {
                throw new Refused("--markets must be 1.." + (DEFAULT_BASE_PRICE.length - 1) + ": " + markets);
            }
            if (window < 2) {
                throw new Refused("--window must be at least 2: " + window);
            }
            if (reofferAfterMs < 100) {
                throw new Refused("--reoffer-after-ms must be at least 100: " + reofferAfterMs);
            }
            if (!"backoff".equals(idle) && !"busy_spin".equals(idle)) {
                throw new Refused("--idle must be busy_spin or backoff: " + idle);
            }
            if (openOrders < 0) {
                throw new Refused("--open-orders must be at least 0: " + openOrders);
            }
            if (!createOnly && openOrders < 1) {
                throw new Refused("--open-orders must be at least 1 with --mix create-cancel (a slot cancels the"
                        + " oldest open order)");
            }
            if (ordersPerUserMax < 1) {
                throw new Refused("--orders-per-user-max must be at least 1: " + ordersPerUserMax);
            }
            final long min = minUsers();
            if (users < min) {
                final long budget = (long)(REQUESTS_PER_USER_CAP - (prefund ? PREFUND_DEPOSITS_PER_USER : 0)) * users
                        - openOrders;
                throw new Refused("--users " + users + " below the minimum " + min + " for --rate " + rate
                        + ": the core keeps " + REQUESTS_PER_USER_CAP + " requests per user for "
                        + REQUEST_TTL_MS / 60_000 + " min (two per slot, " + openOrders + " ramp creates"
                        + (prefund ? ", " + PREFUND_DEPOSITS_PER_USER + " prefund deposits per user" : "")
                        + "), so TOO_MANY_PENDING_REQUESTS would appear within " + Math.max(0, budget / (2 * rate))
                        + " s");
            }
            if ((long)openOrders > (long)users * ordersPerUserMax) {
                throw new Refused("--open-orders " + openOrders + " above --users x --orders-per-user-max ("
                        + users + " x " + ordersPerUserMax + ")");
            }
            if (aeronDir != null && !Files.exists(Path.of(aeronDir, "cnc.dat"))) {
                throw new Refused("--aeron-dir " + aeronDir + " has no cnc.dat (no media driver there)");
            }
            return this;
        }

        /** The user-count rule for these settings: the rate, the ramp and the prefund deposits. */
        public long minUsers() {
            return CoreOrderLoadGenerator.minUsers(rate, openOrders, prefund);
        }

        /** The settings as name to value, written with every run. */
        public Map<String, Object> asMap() {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("rate", rate);
            m.put("duration_s", durationSec);
            m.put("warmup_s", warmupSec);
            m.put("open_orders", openOrders);
            m.put("users", users);
            m.put("markets", markets);
            m.put("window", window);
            m.put("mix", createOnly ? "create-only" : "create-cancel");
            m.put("reoffer_after_ms", reofferAfterMs);
            m.put("reoffer", reoffer);
            m.put("aeron_dir", aeronDir == null ? "embedded" : aeronDir);
            m.put("idle", idle);
            m.put("core_hosts", String.join(",", coreHosts));
            m.put("core_port_base", corePortBase);
            m.put("stride", stride);
            m.put("orders_per_user_max", ordersPerUserMax);
            m.put("min_users", minUsers());
            m.put("qty", qty);
            return m;
        }

        private static final List<String> FLAGS = List.of("--rate", "--duration", "--warmup", "--open-orders",
                "--users", "--markets", "--window", "--mix", "--reoffer-after-ms", "--aeron-dir", "--idle", "--out",
                "--metrics-url", "--core-hosts", "--core-port-base", "--stride", "--orders-per-user-max",
                "--egress-host", "--first-user", "--drain-ms");
        private static final List<String> SWITCHES = List.of("--prefund", "--gate-open", "--version", "--help");

        /** Parses and checks the command line. */
        public static Settings parse(final String[] args) {
            final Settings s = new Settings();
            for (int i = 0; i < args.length; i++) {
                final String a = args[i];
                if (SWITCHES.contains(a)) {
                    if ("--prefund".equals(a)) {
                        s.prefund = true;
                    } else if ("--gate-open".equals(a)) {
                        s.gateOpen = true;
                    }
                    continue;
                }
                if (!FLAGS.contains(a)) {
                    throw new Refused("unknown flag " + a + " (--help lists them)");
                }
                if (i + 1 >= args.length) {
                    throw new Refused(a + " needs a value");
                }
                final String v = args[++i];
                switch (a) {
                    case "--rate" -> s.rate = longOf(a, v);
                    case "--duration" -> s.durationSec = intOf(a, v);
                    case "--warmup" -> s.warmupSec = intOf(a, v);
                    case "--open-orders" -> s.openOrders = intOf(a, v);
                    case "--users" -> s.users = intOf(a, v);
                    case "--markets" -> s.markets = intOf(a, v);
                    case "--window" -> s.window = intOf(a, v);
                    case "--mix" -> {
                        if (!"create-cancel".equals(v) && !"create-only".equals(v)) {
                            throw new Refused("--mix must be create-cancel or create-only: " + v);
                        }
                        s.createOnly = "create-only".equals(v);
                    }
                    case "--reoffer-after-ms" -> s.reofferAfterMs = longOf(a, v);
                    case "--aeron-dir" -> s.aeronDir = v;
                    case "--idle" -> s.idle = v;
                    case "--out" -> s.out = v;
                    case "--metrics-url" -> s.metricsUrl = v;
                    case "--core-hosts" -> s.coreHosts = List.of(v.split(","));
                    case "--core-port-base" -> s.corePortBase = intOf(a, v);
                    case "--stride" -> s.stride = intOf(a, v);
                    case "--orders-per-user-max" -> s.ordersPerUserMax = intOf(a, v);
                    case "--egress-host" -> s.egressHost = v;
                    case "--first-user" -> s.firstUser = longOf(a, v);
                    case "--drain-ms" -> s.drainMs = longOf(a, v);
                    default -> throw new Refused("unknown flag " + a);
                }
            }
            return s.check();
        }

        private static long longOf(final String flag, final String v) {
            try {
                return Long.parseLong(v.trim());
            } catch (final NumberFormatException e) {
                throw new Refused(flag + " is not an integer: " + v);
            }
        }

        private static int intOf(final String flag, final String v) {
            final long l = longOf(flag, v);
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                throw new Refused(flag + " out of range: " + v);
            }
            return (int)l;
        }
    }

    // ------------------------------------------------------------------------------------------------ session

    /** Where a frame goes: an AeronCluster's {@code offer}; a position above 0 means accepted. */
    @FunctionalInterface
    public interface Ingress {
        long offer(DirectBuffer buffer, int offset, int length);
    }

    /** The counts of a run. */
    public record Result(long creates, long cancels, long deposits, long ackFrames, long acks, long rejects,
                         long replayed, long reoffered, long reofferRounds, long unanswered, long elections,
                         long unansweredAtElections,
                         Map<String, Long> rejectsByReason, long maxBacklog, long createP50Ns, long createP99Ns,
                         long createP999Ns, long createMaxNs, long cancelP50Ns, long cancelP99Ns, long cancelP999Ns,
                         long cancelMaxNs, long measuredCreates, long measuredCancels) {

        /** Commands issued (each requestId once). */
        public long issued() {
            return creates + cancels;
        }
    }

    /**
     * The generator's state machine, driven by {@link #tick(long)} on the thread that polls the sessions' egress.
     * Register the session as the egress listener (or a tap) of the GATEWAY client and, with prefunding, of the TOOL
     * client. Phases: register, prefund, ramp (creates only up to the open count), steady (the timed slots), drain
     * (answers of the last slots), done.
     */
    public static final class Session implements EgressListener {

        enum Phase { REGISTER, PREFUND, RAMP, STEADY, DRAIN, DONE }

        private static final int CREATE = 1;
        private static final int CANCEL = 2;
        private static final int DEPOSIT = 3;
        private static final long REGISTER_CORR = 0x8E6L;
        /** Deposit correlation ids: this bit plus the requestId low word (a DepositAck carries no requestId). */
        private static final long DEPOSIT_CORR = 1L << 62;

        private static final class Pending {
            int kind;
            long slot;
            long requestLow;
            long scheduledNs;
            long lastOfferNs;
            long reofferedEpoch = -1;
            boolean reofferedByAge;
            byte[] frame;
        }

        private final Settings s;
        private final Ingress gateway;
        private final Ingress tool;
        private final OrderWire wire = new OrderWire();
        private final UnsafeBuffer buf = new UnsafeBuffer(new byte[512]);
        private final Long2ObjectHashMap<Pending> pending = new Long2ObjectHashMap<>();
        private final ArrayDeque<Pending> outbound = new ArrayDeque<>();
        /** Open orders in creation order: {user, clientOrderIdLow}. */
        private final ArrayDeque<long[]> open = new ArrayDeque<>();
        private final Map<String, Long> rejectsByReason = new TreeMap<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final OrderAckDecoder ack = new OrderAckDecoder();
        private final OrderRejectDecoder reject = new OrderRejectDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();
        private final DepositRejectDecoder depositReject = new DepositRejectDecoder();
        private final AdminAckDecoder adminAck = new AdminAckDecoder();
        private final PrintStream log;

        private Phase phase;
        private long requestSeq;
        private long epoch;
        private long lastLeadershipTermId = Long.MIN_VALUE;
        private OpenLoopSchedule schedule;
        private LatencyTracks tracks;
        private LatencyTracks.Track createTrack;
        private LatencyTracks.Track cancelTrack;
        private long steadyStartNs;
        private long steadyEndNs;
        private long drainStartNs;
        private long rampPlaced;
        private long prefundSent;
        private long prefundAcked;
        private boolean registerSent;
        private volatile boolean registered;
        private long creates;
        private long cancels;
        private long deposits;
        private long ackFrames;
        private long acks;
        private long rejects;
        private long replayed;
        private long reoffered;
        private long reofferRounds;
        private long elections;
        /** Commands unanswered when a new term's leader was announced (the election's stranded tail), summed. */
        private long unansweredAtElections;
        private long unanswered;
        private long nextUser;

        /**
         * @param settings checked here
         * @param gateway the GATEWAY session's offer
         * @param tool the TOOL session's offer for {@link Settings#prefund}; null without prefunding
         * @param log progress lines (one per phase change and per election)
         */
        public Session(final Settings settings, final Ingress gateway, final Ingress tool, final PrintStream log) {
            this.s = settings.check();
            this.gateway = gateway;
            this.tool = tool;
            this.log = log;
            if (s.prefund && tool == null) {
                throw new Refused("--prefund needs a TOOL session");
            }
            this.phase = s.register ? Phase.REGISTER : s.prefund ? Phase.PREFUND : Phase.RAMP;
        }

        public boolean done() {
            return phase == Phase.DONE;
        }

        public String phase() {
            return phase.name();
        }

        /** The last steady slot's scheduled time has passed (the load stopped). */
        public boolean steadyOver(final long nowNs) {
            return phase == Phase.DRAIN || phase == Phase.DONE || (phase == Phase.STEADY && nowNs >= steadyEndNs);
        }

        /**
         * Ends the steady phase at {@code nowNs} instead of after {@code --duration} (an in-JVM row whose faults decide
         * its length); slots scheduled before it still go out, then the drain runs as usual.
         */
        public void endSteady(final long nowNs) {
            if (phase == Phase.STEADY && nowNs < steadyEndNs) {
                steadyEndNs = nowNs;
            }
        }

        /** Commands sent and not yet answered. */
        public int inFlight() {
            return pending.size();
        }

        public LatencyTracks tracks() {
            return tracks;
        }

        /** One step: re-offers by age, then sends what is due. Returns the frames offered. */
        public int tick(final long nowNs) {
            if (phase == Phase.RAMP && schedule == null) {
                enter(Phase.RAMP, nowNs);
            }
            int work = flushOutbound(nowNs);
            if (!outbound.isEmpty()) {
                return work;
            }
            switch (phase) {
                case REGISTER -> {
                    if (!registerSent) {
                        final int len = wire.registerSession(buf, REGISTER_CORR, SessionClass.GATEWAY, false, false);
                        if (gateway.offer(buf, 0, len) > 0) {
                            registerSent = true;
                            work++;
                        }
                    } else if (registered) {
                        enter(s.prefund ? Phase.PREFUND : Phase.RAMP, nowNs);
                    }
                }
                case PREFUND -> work += prefund(nowNs);
                case RAMP -> work += ramp(nowNs);
                case STEADY -> work += steady(nowNs);
                case DRAIN -> {
                    reofferByAge(nowNs);
                    if (pending.isEmpty() || nowNs - drainStartNs > TimeUnit.MILLISECONDS.toNanos(s.drainMs)) {
                        unanswered = pending.size();
                        enter(Phase.DONE, nowNs);
                    }
                }
                case DONE -> { }
            }
            return work;
        }

        private void enter(final Phase next, final long nowNs) {
            phase = next;
            if (next == Phase.STEADY) {
                steadyStartNs = nowNs;
                steadyEndNs = nowNs + TimeUnit.SECONDS.toNanos(s.durationSec);
                schedule = new OpenLoopSchedule(nowNs, s.rate, false);
                tracks = new LatencyTracks(nowNs, nowNs + TimeUnit.SECONDS.toNanos(s.warmupSec));
                createTrack = tracks.track("create-ack");
                cancelTrack = tracks.track("cancel-ack");
            } else if (next == Phase.RAMP) {
                schedule = new OpenLoopSchedule(nowNs, s.rate, false);
            } else if (next == Phase.DRAIN) {
                drainStartNs = nowNs;
            }
            if (log != null) {
                log.printf(Locale.ROOT, "[loadgen] phase %s (open %d, in flight %d, issued %d)%n", next, open.size(),
                        pending.size(), creates + cancels);
            }
        }

        private int prefund(final long nowNs) {
            final long users = s.users;
            int work = 0;
            while (prefundSent < 2 * users && pending.size() < s.window) {
                final long u = s.firstUser + prefundSent / 2;
                final boolean usd = prefundSent % 2 == 0;
                final int market = marketOf(u);
                final Pending p = newPending(DEPOSIT, prefundSent, nowNs);
                final int len = wire.deposit(buf, corrOf(p), u, usd ? USD : DEFAULT_BASE_ASSET[market],
                        usd ? s.prefundUsd : s.prefundBase, s.requestIdHigh, p.requestLow);
                if (!send(p, len, nowNs)) {
                    break;
                }
                prefundSent++;
                deposits++;
                work++;
            }
            reofferByAge(nowNs);
            if (prefundSent == 2 * users && prefundAcked == 2 * users) {
                enter(Phase.RAMP, nowNs);
            }
            return work;
        }

        private int ramp(final long nowNs) {
            int work = 0;
            while (rampPlaced < s.openOrders && pending.size() < s.window && schedule.due(nowNs)) {
                final long slot = schedule.nextSlot();
                if (!create(slot, schedule.t0(slot), nowNs)) {
                    break;
                }
                schedule.take(nowNs);
                rampPlaced++;
                work++;
            }
            reofferByAge(nowNs);
            if (rampPlaced == s.openOrders && pending.isEmpty()) {
                enter(Phase.STEADY, nowNs);
            }
            return work;
        }

        private int steady(final long nowNs) {
            int work = 0;
            while (pending.size() < s.window - 1 && schedule.due(nowNs) && schedule.t0(schedule.nextSlot()) < steadyEndNs) {
                final long slot = schedule.nextSlot();
                final long t0 = schedule.t0(slot);
                if (!create(slot, t0, nowNs)) {
                    break;
                }
                schedule.take(nowNs);
                work++;
                if (!s.createOnly) {
                    cancelOldest(slot, t0, nowNs);
                    work++;
                }
            }
            reofferByAge(nowNs);
            if (nowNs >= steadyEndNs && (schedule.t0(schedule.nextSlot()) >= steadyEndNs)) {
                enter(Phase.DRAIN, nowNs);
            }
            return work;
        }

        private boolean create(final long slot, final long t0, final long nowNs) {
            final long u = nextUser();
            final int market = marketOf(u);
            final boolean buy = (slot & 1) == 0;
            final int level = (int)((slot >> 1) % 7); // within the rigs' resting ladder
            final Pending p = newPending(CREATE, slot, t0);
            final int len = wire.limit(buf, slot, u, s.requestIdHigh, p.requestLow, s.requestIdHigh, p.requestLow,
                    market, buy, priceOf(market, buy, level), s.qty, TimeInForce.GTC, 0);
            if (!send(p, len, nowNs)) {
                requestSeq--; // nothing was sent: the id is reused by the retry
                nextUser--;
                return false;
            }
            creates++;
            open.addLast(new long[] {u, p.requestLow});
            return true;
        }

        private void cancelOldest(final long slot, final long t0, final long nowNs) {
            final long[] oldest = open.pollFirst();
            final Pending p = newPending(CANCEL, slot, t0);
            final int len = wire.cancel(buf, slot, oldest[0], 0, s.requestIdHigh, p.requestLow, s.requestIdHigh,
                    oldest[1]);
            p.frame = copy(len);
            p.lastOfferNs = nowNs;
            pending.put(p.requestLow, p);
            cancels++;
            if (gateway.offer(buf, 0, len) <= 0) {
                outbound.addLast(p); // sent by the next ticks; the create of the slot is already out
            }
        }

        private long nextUser() {
            return s.firstUser + (nextUser++ % s.users);
        }

        private int marketOf(final long user) {
            return 1 + (int)(user % s.markets);
        }

        private long priceOf(final int market, final boolean buy, final int level) {
            final long base = DEFAULT_BASE_PRICE[market];
            final long step = base / 1000;
            return buy ? base - (1 + level) * step : base + (1 + level) * step;
        }

        private Pending newPending(final int kind, final long slot, final long scheduledNs) {
            final Pending p = new Pending();
            p.kind = kind;
            p.slot = slot;
            p.requestLow = ++requestSeq;
            p.scheduledNs = scheduledNs;
            return p;
        }

        private long corrOf(final Pending p) {
            return p.kind == DEPOSIT ? DEPOSIT_CORR | p.requestLow : p.slot;
        }

        private byte[] copy(final int len) {
            final byte[] b = new byte[len];
            buf.getBytes(0, b);
            return b;
        }

        /** Offers a new command; on acceptance it becomes pending. */
        private boolean send(final Pending p, final int len, final long nowNs) {
            final Ingress to = p.kind == DEPOSIT ? tool : gateway;
            if (to.offer(buf, 0, len) <= 0) {
                return false;
            }
            p.frame = copy(len);
            p.lastOfferNs = nowNs;
            pending.put(p.requestLow, p);
            return true;
        }

        private int flushOutbound(final long nowNs) {
            int work = 0;
            while (!outbound.isEmpty()) {
                final Pending p = outbound.peekFirst();
                if (pending.get(p.requestLow) != p) {
                    outbound.pollFirst(); // answered meanwhile
                    continue;
                }
                final Ingress to = p.kind == DEPOSIT ? tool : gateway;
                final UnsafeBuffer b = new UnsafeBuffer(p.frame);
                if (to.offer(b, 0, p.frame.length) <= 0) {
                    break;
                }
                outbound.pollFirst();
                p.lastOfferNs = nowNs;
                work++;
            }
            return work;
        }

        /** The unanswered commands older than the re-offer bound go out again once (same ids). */
        private void reofferByAge(final long nowNs) {
            if (!s.reoffer || pending.isEmpty()) {
                return;
            }
            final long bound = TimeUnit.MILLISECONDS.toNanos(s.reofferAfterMs);
            List<Pending> due = null;
            for (final Pending p : pending.values()) {
                if (!p.reofferedByAge && p.reofferedEpoch < 0 && nowNs - p.lastOfferNs > bound) {
                    if (due == null) {
                        due = new ArrayList<>();
                    }
                    due.add(p);
                }
            }
            if (due != null) {
                for (final Pending p : due) {
                    p.reofferedByAge = true;
                }
                queueReoffer(due, "an answer older than " + s.reofferAfterMs + " ms");
            }
        }

        private void queueReoffer(final List<Pending> list, final String why) {
            list.sort((a, b) -> Long.compare(a.requestLow, b.requestLow));
            for (final Pending p : list) {
                if (!outbound.contains(p)) {
                    outbound.addLast(p);
                }
            }
            reoffered += list.size();
            reofferRounds++;
            if (log != null) {
                log.printf(Locale.ROOT, "[loadgen] re-offer %d unanswered commands with their requestIds (%s)%n",
                        list.size(), why);
            }
        }

        // -------------------------------------------------------------------------------------------- egress

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                              final int offset, final int length, final Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            final long nowNs = System.nanoTime();
            switch (header.templateId()) {
                case OrderAckDecoder.TEMPLATE_ID -> {
                    ack.wrapAndApplyHeader(buffer, offset, header);
                    if (ack.requestIdHigh() != s.requestIdHigh) {
                        return;
                    }
                    ackFrames++;
                    final boolean duplicate = ack.duplicate() == BoolFlag.TRUE;
                    if (duplicate) {
                        replayed++;
                    }
                    answered(ack.requestIdLow(), nowNs, true, null);
                }
                case OrderRejectDecoder.TEMPLATE_ID -> {
                    reject.wrapAndApplyHeader(buffer, offset, header);
                    if (reject.requestIdHigh() != s.requestIdHigh) {
                        return;
                    }
                    final CoreRejectReason r = reject.reason();
                    answered(reject.requestIdLow(), nowNs, false, String.valueOf(r));
                }
                case DepositAckDecoder.TEMPLATE_ID -> {
                    depositAck.wrapAndApplyHeader(buffer, offset, header);
                    final long corr = depositAck.correlationId();
                    if ((corr & DEPOSIT_CORR) == 0 || corr < 0) {
                        return;
                    }
                    if (depositAck.duplicate() == BoolFlag.TRUE) {
                        replayed++;
                    }
                    final Pending p = pending.get(corr & (DEPOSIT_CORR - 1));
                    if (p != null && p.kind == DEPOSIT) {
                        pending.remove(p.requestLow);
                        prefundAcked++;
                    }
                }
                case DepositRejectDecoder.TEMPLATE_ID -> {
                    depositReject.wrapAndApplyHeader(buffer, offset, header);
                    if (depositReject.requestIdHigh() != s.requestIdHigh) {
                        return;
                    }
                    final Pending p = pending.remove(depositReject.requestIdLow());
                    if (p != null) {
                        rejects++;
                        rejectsByReason.merge("DEPOSIT_" + depositReject.reason(), 1L, Long::sum);
                        prefundAcked++;
                    }
                }
                case AdminAckDecoder.TEMPLATE_ID -> {
                    adminAck.wrapAndApplyHeader(buffer, offset, header);
                    if (adminAck.refTemplateId() == 80 && adminAck.correlationId() == REGISTER_CORR) {
                        registered = true;
                    }
                }
                default -> { }
            }
        }

        private void answered(final long requestLow, final long nowNs, final boolean accepted, final String reason) {
            final Pending p = pending.remove(requestLow);
            if (p == null) {
                return; // a second answer (the original after its re-offer, or the replay after the original)
            }
            if (accepted) {
                acks++;
            } else {
                rejects++;
                rejectsByReason.merge(reason, 1L, Long::sum);
            }
            if (tracks != null && p.scheduledNs >= steadyStartNs && phase != Phase.RAMP) {
                tracks.record(p.kind == CREATE ? createTrack : cancelTrack, nowNs, nowNs - p.scheduledNs);
            }
        }

        @Override
        public void onNewLeader(final long clusterSessionId, final long leadershipTermId, final int leaderMemberId,
                                final String ingressEndpoints) {
            if (leadershipTermId <= lastLeadershipTermId) {
                return; // the same election announced again: its re-offer already went out
            }
            lastLeadershipTermId = leadershipTermId;
            elections++;
            epoch++;
            unansweredAtElections += pending.size();
            if (log != null) {
                log.printf(Locale.ROOT, "[loadgen] new leader member %d term %d; %d commands unanswered%n",
                        leaderMemberId, leadershipTermId, pending.size());
            }
            if (!s.reoffer || pending.isEmpty()) {
                return;
            }
            final List<Pending> due = new ArrayList<>();
            for (final Pending p : pending.values()) {
                if (p.reofferedEpoch < epoch) {
                    p.reofferedEpoch = epoch;
                    due.add(p);
                }
            }
            queueReoffer(due, "new leader " + leaderMemberId);
        }

        /** The counts and the latency summary of the run so far. */
        public Result result() {
            final Histogram c = createTrack == null ? LatencyTracks.newHist() : createTrack.cumulative();
            final Histogram x = cancelTrack == null ? LatencyTracks.newHist() : cancelTrack.cumulative();
            return new Result(creates, cancels, deposits, ackFrames, acks, rejects, replayed, reoffered, reofferRounds,
                    phase == Phase.DONE ? unanswered : pending.size(), elections, unansweredAtElections,
                    Map.copyOf(rejectsByReason),
                    schedule == null ? 0 : schedule.maxBacklog(), c.getValueAtPercentile(50),
                    c.getValueAtPercentile(99), c.getValueAtPercentile(99.9), c.getMaxValue(),
                    x.getValueAtPercentile(50), x.getValueAtPercentile(99), x.getValueAtPercentile(99.9),
                    x.getMaxValue(), c.getTotalCount(), x.getTotalCount());
        }
    }

    // ------------------------------------------------------------------------------------------------ CLI

    /** The build sha stamped into the jar's manifest ({@code unknown} outside the shaded jar). */
    public static String buildSha() {
        final String v = CoreOrderLoadGenerator.class.getPackage() == null ? null
                : readManifestSha();
        return v == null || v.isBlank() ? "unknown" : v;
    }

    private static String readManifestSha() {
        try (var in = CoreOrderLoadGenerator.class.getResourceAsStream("/META-INF/MANIFEST.MF")) {
            if (in == null) {
                return null;
            }
            return new java.util.jar.Manifest(in).getMainAttributes().getValue("Build-Sha");
        } catch (final java.io.IOException e) {
            return null;
        }
    }

    private static final String HELP = """
            CoreOrderLoadGenerator: resting LIMIT creates and cancels against the core (GATEWAY session), open loop
              --rate N                slots/s in the steady phase, a slot = one create + one cancel (default 1000)
              --duration S            steady phase seconds (default 60)
              --warmup S              seconds of the steady phase excluded from the histograms (default 10)
              --open-orders N         resting orders placed by the ramp before the steady phase (default 100000)
              --users N               user ids first-user..first-user+N-1 (default 10000; at least
                                      ceil(2 x rate x 900 / 256), the core's requestId cap)
              --markets N             market ids 1..N (default 5)
              --window N              commands in flight at most (default 4096)
              --mix M                 create-cancel (default) or create-only
              --reoffer-after-ms N    re-offer an unanswered command once after N ms (default 2000)
              --orders-per-user-max N open orders per user at most (default 500, the core's default limit)
              --core-hosts CSV        core member hosts (default 127.0.0.1)
              --core-port-base N      cluster port base (default 19300)
              --stride N              port stride per member, 0 = the kit's (default 0)
              --egress-host H         advertised egress address (default: routed source address)
              --aeron-dir DIR         attach to an external media driver (default: embedded)
              --idle busy_spin|backoff  embedded driver and duty loop idle (default backoff)
              --metrics-url URL       the leader's /metrics: core_log_position growth is reported
              --prefund               deposit to every user first (TOOL credential CORE_TOOL_AUTH_CREDENTIAL)
              --gate-open             open the global order entry gate (ADMIN credential CORE_ADMIN_AUTH_CREDENTIAL)
              --first-user N          the first user id (default 1)
              --drain-ms N            wait for the last answers at most N ms (default 10000)
              --out DIR               summary.json and seconds.tsv (default results)
              --version               print the build sha
            The GATEWAY credential is CORE_AUTH_CREDENTIAL (or -Dcore.auth.credential).""";

    public static void main(final String[] args) {
        com.openexchange.core.config.RetiredSettings.refuseRetired("core-loadgen.jar", args);
        for (final String a : args) {
            if ("--help".equals(a)) {
                System.out.println(HELP);
                return;
            }
            if ("--version".equals(a)) {
                System.out.println(buildSha());
                return;
            }
        }
        final Settings s;
        final com.openexchange.core.auth.CoreCredentials gw;
        com.openexchange.core.auth.CoreCredentials toolCred = null;
        com.openexchange.core.auth.CoreCredentials adminCred = null;
        try {
            s = Settings.parse(args);
            gw = com.openexchange.core.auth.CoreCredentials.fromEnvironment();
            if (gw == null) {
                throw new Refused("no GATEWAY credential: set CORE_AUTH_CREDENTIAL");
            }
            if (s.prefund) {
                toolCred = credential("CORE_TOOL_AUTH_CREDENTIAL", "--prefund");
            }
            if (s.gateOpen) {
                adminCred = credential("CORE_ADMIN_AUTH_CREDENTIAL", "--gate-open");
            }
        } catch (final Refused r) {
            System.err.println("core order loadgen: " + r.getMessage());
            System.exit(2);
            return;
        }
        System.exit(run(s, gw, toolCred, adminCred));
    }

    private static com.openexchange.core.auth.CoreCredentials credential(final String env, final String flag) {
        final String v = System.getenv(env);
        if (v == null || v.isBlank()) {
            throw new Refused(flag + " needs " + env);
        }
        return com.openexchange.core.auth.CoreCredentials.fromSpec(v.trim());
    }

    private static int run(final Settings s, final com.openexchange.core.auth.CoreCredentials gw,
                           final com.openexchange.core.auth.CoreCredentials toolCred,
                           final com.openexchange.core.auth.CoreCredentials adminCred) {
        final boolean busy = "busy_spin".equals(s.idle);
        final MediaDriver driver = s.aeronDir != null ? null : MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName("/dev/shm/aeron-coreorderload-" + ProcessHandle.current().pid())
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(busy ? new BusySpinIdleStrategy() : new BackoffIdleStrategy())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        final String aeronDir = driver != null ? driver.aeronDirectoryName() : s.aeronDir;
        final String egressHost = MoneyLoadGenerator.resolveEgressHost(s.egressHost, s.coreHosts.get(0));
        final StringBuilder endpoints = new StringBuilder();
        for (int i = 0; i < s.coreHosts.size(); i++) {
            endpoints.append(i == 0 ? "" : ",").append(i).append('=').append(s.coreHosts.get(i).trim()).append(':')
                    .append(s.stride == 0
                            ? com.openexchange.cluster.ClusterPorts.port(i, s.corePortBase,
                                    com.openexchange.cluster.ClusterPorts.CLIENT_FACING_OFFSET)
                            : com.openexchange.cluster.ClusterPorts.port(i, s.corePortBase,
                                    com.openexchange.cluster.ClusterPorts.CLIENT_FACING_OFFSET, s.stride));
        }
        final Session[] holder = new Session[1];
        final EgressListener relay = new EgressListener() {
            @Override
            public void onMessage(final long sid, final long ts, final DirectBuffer b, final int o, final int l,
                                  final Header h) {
                if (holder[0] != null) {
                    holder[0].onMessage(sid, ts, b, o, l, h);
                }
            }

            @Override
            public void onNewLeader(final long sid, final long term, final int leader, final String ingress) {
                if (holder[0] != null) {
                    holder[0].onNewLeader(sid, term, leader, ingress);
                }
            }
        };
        AeronCluster gateway = null;
        AeronCluster toolSession = null;
        AeronCluster adminSession = null;
        try {
            gateway = connect(aeronDir, endpoints.toString(), egressHost, gw, relay);
            if (toolCred != null) {
                toolSession = connect(aeronDir, endpoints.toString(), egressHost, toolCred, relay);
            }
            if (adminCred != null) {
                adminSession = connect(aeronDir, endpoints.toString(), egressHost, adminCred, relay);
                openGate(adminSession);
            }
            final AeronCluster g = gateway;
            final AeronCluster t = toolSession;
            final Session session = new Session(s, g::offer, t == null ? null : t::offer, System.out);
            holder[0] = session;
            final IdleStrategy idle = busy ? new BusySpinIdleStrategy() : new BackoffIdleStrategy();
            long lastKeepAliveNs = System.nanoTime();
            long logStart = -1;
            long logStartNs = 0;
            long logEnd = -1;
            long logEndNs = 0;
            while (!session.done()) {
                final long now = System.nanoTime();
                int work = session.tick(now);
                work += g.pollEgress();
                if (t != null) {
                    work += t.pollEgress();
                }
                if (now - lastKeepAliveNs > 250_000_000L) {
                    lastKeepAliveNs = now;
                    g.sendKeepAlive();
                    if (t != null) {
                        t.sendKeepAlive();
                    }
                }
                if (s.metricsUrl != null && logStart < 0 && "STEADY".equals(session.phase())) {
                    logStart = logPosition(s.metricsUrl);
                    logStartNs = System.nanoTime();
                }
                if (s.metricsUrl != null && logEnd < 0 && session.steadyOver(now) && logStart >= 0) {
                    logEnd = logPosition(s.metricsUrl);
                    logEndNs = System.nanoTime();
                }
                idle.idle(work);
            }
            final Result r = session.result();
            final double logBps = logStart >= 0 && logEnd >= logStart && logEndNs > logStartNs
                    ? (logEnd - logStart) / ((logEndNs - logStartNs) / 1e9) : -1;
            report(System.out, s, r, logBps);
            write(s, session, r, logBps);
            return r.unanswered() == 0 ? 0 : 1;
        } catch (final Exception e) {
            System.err.println("core order loadgen: " + e);
            return 1;
        } finally {
            org.agrona.CloseHelper.quietCloseAll(adminSession, toolSession, gateway, driver);
        }
    }

    private static AeronCluster connect(final String aeronDir, final String endpoints, final String egressHost,
                                        final com.openexchange.core.auth.CoreCredentials cred,
                                        final EgressListener listener) {
        return AeronCluster.connect(new AeronCluster.Context()
                .credentialsSupplier(cred.supplier())
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10))
                .newLeaderTimeoutNs(TimeUnit.SECONDS.toNanos(10))
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:udp?term-length=16m|mtu=8k")
                .ingressEndpoints(endpoints)
                .egressChannel("aeron:udp?endpoint=" + egressHost + ":0")
                .egressListener(listener)
                .errorHandler(t -> t.printStackTrace(System.err)));
    }

    private static void openGate(final AeronCluster admin) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        final int len = new OrderWire().gateOpen(b, 0xAD1L);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (admin.offer(b, 0, len) <= 0) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("the gate OPEN was not accepted by the ingress within 10 s");
            }
            admin.pollEgress();
        }
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < until) {
            admin.pollEgress();
        }
    }

    /** {@code core_log_position} from a Prometheus text page; -1 when absent or unreachable. */
    static long logPosition(final String url) {
        try {
            final HttpResponse<String> r = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
                    .send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).build(),
                            HttpResponse.BodyHandlers.ofString());
            return parseLogPosition(r.body());
        } catch (final Exception e) {
            return -1;
        }
    }

    static long parseLogPosition(final String text) {
        for (final String line : text.split("\n")) {
            if (line.startsWith("core_log_position ") || line.startsWith("core_log_position{")) {
                final String v = line.substring(line.lastIndexOf(' ') + 1).trim();
                try {
                    return (long)Double.parseDouble(v);
                } catch (final NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    static void report(final PrintStream out, final Settings s, final Result r, final double logBps) {
        out.println("==== core order load report ====");
        out.printf(Locale.ROOT, "settings %s%n", s.asMap());
        out.printf(Locale.ROOT, "creates %d | cancels %d | deposits %d | ack frames %d | answered acks %d | rejects %d"
                + " | replayed %d | re-offered %d in %d rounds | unanswered %d | elections %d (%d unanswered at"
                + " the announcements)%n", r.creates(), r.cancels(), r.deposits(), r.ackFrames(), r.acks(),
                r.rejects(), r.replayed(), r.reoffered(), r.reofferRounds(), r.unanswered(), r.elections(),
                r.unansweredAtElections());
        out.printf(Locale.ROOT, "rejects by reason %s | max schedule backlog %d%n", r.rejectsByReason(), r.maxBacklog());
        out.printf(Locale.ROOT, "create->ack n=%d p50=%.1fus p99=%.1fus p99.9=%.1fus max=%.1fus%n", r.measuredCreates(),
                r.createP50Ns() / 1e3, r.createP99Ns() / 1e3, r.createP999Ns() / 1e3, r.createMaxNs() / 1e3);
        out.printf(Locale.ROOT, "cancel->ack n=%d p50=%.1fus p99=%.1fus p99.9=%.1fus max=%.1fus%n", r.measuredCancels(),
                r.cancelP50Ns() / 1e3, r.cancelP99Ns() / 1e3, r.cancelP999Ns() / 1e3, r.cancelMaxNs() / 1e3);
        if (logBps >= 0) {
            out.printf(Locale.ROOT, "core_log_position growth %.0f B/s%n", logBps);
        }
    }

    /** {@code summary.json} and {@code seconds.tsv} under {@code --out}. */
    static void write(final Settings s, final Session session, final Result r, final double logBps)
            throws java.io.IOException {
        final Path dir = Path.of(s.out);
        Files.createDirectories(dir);
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("build_sha", buildSha());
        m.put("settings", s.asMap());
        m.put("creates", r.creates());
        m.put("cancels", r.cancels());
        m.put("deposits", r.deposits());
        m.put("ack_frames", r.ackFrames());
        m.put("acks", r.acks());
        m.put("rejects", r.rejects());
        m.put("rejects_by_reason", r.rejectsByReason());
        m.put("replayed", r.replayed());
        m.put("reoffered", r.reoffered());
        m.put("reoffer_rounds", r.reofferRounds());
        m.put("unanswered", r.unanswered());
        m.put("elections", r.elections());
        m.put("unanswered_at_elections", r.unansweredAtElections());
        m.put("max_schedule_backlog", r.maxBacklog());
        m.put("create_ack_ns", Map.of("n", r.measuredCreates(), "p50", r.createP50Ns(), "p99", r.createP99Ns(),
                "p99_9", r.createP999Ns(), "max", r.createMaxNs()));
        m.put("cancel_ack_ns", Map.of("n", r.measuredCancels(), "p50", r.cancelP50Ns(), "p99", r.cancelP99Ns(),
                "p99_9", r.cancelP999Ns(), "max", r.cancelMaxNs()));
        m.put("core_log_position_bytes_per_s", logBps);
        Files.writeString(dir.resolve("summary.json"), json(m) + "\n");
        if (session.tracks() != null) {
            try (PrintStream ps = new PrintStream(Files.newOutputStream(dir.resolve("seconds.tsv")))) {
                session.tracks().writeSecondsTsv(ps);
            }
        }
    }

    /** A minimal JSON rendering of maps, strings, numbers and booleans. */
    static String json(final Object v) {
        if (v instanceof Map<?, ?> m) {
            final StringBuilder sb = new StringBuilder("{");
            final Map<?, ?> sorted = m instanceof LinkedHashMap ? m : new TreeMap<>(m);
            for (final Map.Entry<?, ?> e : sorted.entrySet()) {
                sb.append(sb.length() == 1 ? "" : ", ").append(json(String.valueOf(e.getKey()))).append(": ")
                        .append(json(e.getValue()));
            }
            return sb.append('}').toString();
        }
        if (v instanceof String str) {
            return '"' + str.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
        }
        if (v instanceof Double d) {
            return d.isNaN() || d.isInfinite() ? "null" : String.format(Locale.ROOT, "%.1f", d);
        }
        return String.valueOf(v);
    }
}

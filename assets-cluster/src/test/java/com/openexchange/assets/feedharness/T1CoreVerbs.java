// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.domain.CoreRejectReason;
import com.openexchange.assets.domain.GateReason;
import com.openexchange.assets.domain.Market;
import com.openexchange.assets.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositRejectDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.OrderAckDecoder;
import com.openexchange.assets.infrastructure.generated.OrderRejectDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.WithdrawRejectDecoder;
import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * RC2 S10' 4.5 (items 1, 2, 4, 6, 7, 8): the {@code t1-core} verbs the S10' halt and N = 2 drills need. Test scope (the
 * harness), every one against a live core through {@link T1Session}s of the invocation's {@link T1Core.Ctx}:
 *
 * <ul>
 *   <li>{@code probe-create <user> <market> [--side buy|sell] [--price P] [--qty Q]}, {@code probe-amend <orderId>
 *       [--price P] [--qty Q]}, {@code probe-cancel <orderId>}: one GATEWAY command; prints {@code OrderAck ...} or
 *       {@code OrderReject <REASON>} by name; exit 0 either way; an unknown user, market or order is an ERR (usage, 64)
 *       before anything is sent (C7). At T1-BR only refused probes are oracle-safe (contract B-5).</li>
 *   <li>{@code invariants [--seam BREAK_LOCKED]}: per (user, asset) {@code available, locked, sum(H)} from the live dump;
 *       PASS only when every {@code locked == sum(H)} and nothing is negative (exit 0), else FAIL naming the user and asset
 *       (exit 1). {@code --seam BREAK_LOCKED} is the 4.3 negative control: it adds 1 to the first non-zero locked before the
 *       check, so the check must FAIL naming that user and asset.</li>
 *   <li>{@code dump --orders <file>}: the canonical dump lines of the listed orders (one id per line), their H lines and
 *       their users' B lines, exactly as the full dump prints them.</li>
 *   <li>{@code gate --watch <ms> [--until <s>]}: one GATEWAY session; the initial state (the RegisterSession's ORIGIN
 *       AdmissionState) as {@code INITIAL ...}, then every GLOBAL AdmissionState broadcast as
 *       {@code t=<monotonic ms> state=<OPEN|SUSPENDED> reasons=<bitmask> names=<A,B> wall=<epoch ms>}; {@code t} is
 *       {@code System.nanoTime} in ms (CLOCK_MONOTONIC on Linux, comparable with a C++ steady clock on the same host);
 *       polls every {@code ms}; exits 0 after {@code --until} seconds of wall time (none: until SIGTERM).</li>
 *   <li>{@code suspend [market]}: ADMIN {@code OrderEntryGate{SUSPENDED, ADMIN}}; prints the AdminAck.</li>
 * </ul>
 */
final class T1CoreVerbs {

    private T1CoreVerbs() {
    }

    static final long PROBE_REQUEST_HIGH = 0x5150L;
    static final long ONE = FeedStory.ONE;

    static String rejectName(final int code) {
        for (final CoreRejectReason r : CoreRejectReason.values()) {
            if (r.code() == code) {
                return r.name();
            }
        }
        return Integer.toString(code);
    }

    static long longOpt(final String[] args, final String name, final long dflt) {
        final String v = T1Core.opt(args, name);
        return v == null ? dflt : Long.parseLong(v);
    }

    // ------------------------------------------------------------------ the live dump, parsed

    /** {B lines by user -> asset -> {available, locked}, H lines, O lines by order} of one live dump. */
    record Dump(List<String> lines, Map<Long, TreeMap<Integer, long[]>> balances, List<long[]> holds, Map<Long, String> orders) {

        static Dump of(final List<String> lines) {
            final Map<Long, TreeMap<Integer, long[]>> b = new TreeMap<>();
            final List<long[]> h = new ArrayList<>();
            final Map<Long, String> o = new TreeMap<>();
            for (final String l : lines) {
                final String[] f = l.split(" ");
                switch (f[0]) {
                    case "B" -> b.computeIfAbsent(Long.parseLong(f[1]), k -> new TreeMap<>()).put(Integer.parseInt(f[2]),
                            new long[] {Long.parseLong(f[3]), Long.parseLong(f[4])});
                    case "H" -> h.add(new long[] {Long.parseLong(f[1]), Long.parseLong(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4])});
                    case "O" -> o.put(Long.parseLong(f[1]), l);
                    default -> { }
                }
            }
            return new Dump(lines, b, h, o);
        }

        long userOf(final long order) {
            final String l = orders.get(order);
            if (l == null) {
                throw new FeedHarnessMain.Usage("ERR unknown order " + order);
            }
            return Long.parseLong(l.split(" ")[2]);
        }
    }

    static Dump dump(final T1Core.Ctx c) throws InterruptedException {
        return Dump.of(CanonicalDump.of(c.session(SessionClass.ADMIN)));
    }

    // ------------------------------------------------------------------ probes (item 1)

    static String orderReply(final byte[] f) {
        final UnsafeBuffer b = new UnsafeBuffer(f);
        final MessageHeaderDecoder h = new MessageHeaderDecoder();
        if (T1Session.templateOf(f) == 100) {
            final OrderAckDecoder d = new OrderAckDecoder().wrapAndApplyHeader(b, 0, h);
            return "OrderAck order=" + d.orderId() + " status=" + d.status() + " duplicate=" + d.duplicate() + " hold="
                    + d.holdAssetId() + ":" + d.holdAmount();
        }
        final OrderRejectDecoder d = new OrderRejectDecoder().wrapAndApplyHeader(b, 0, h);
        return "OrderReject " + rejectName(d.reasonRaw()) + " ref=" + d.refTemplateId() + " order=" + d.orderId() + " detail="
                + d.detail();
    }

    static int probeCreate(final T1Core.Ctx c, final String[] args) throws Exception {
        if (args.length < 4) {
            throw new FeedHarnessMain.Usage("t1-core probe-create <user> <market> [--side buy|sell] [--price P] [--qty Q]");
        }
        final long user = Long.parseLong(args[2]);
        final int market = Integer.parseInt(args[3]);
        if (market < 1 || market > Market.ALL.length || Market.fromId(market) == null) {
            throw new FeedHarnessMain.Usage("ERR unknown market " + market);
        }
        if (user < 1 || !dump(c).balances().containsKey(user)) {
            throw new FeedHarnessMain.Usage("ERR unknown user " + user + " (no B line in the live dump)");
        }
        final String side = T1Core.opt(args, "--side");
        if (side != null && !side.equals("buy") && !side.equals("sell")) {
            throw new FeedHarnessMain.Usage("ERR --side " + side + " (buy | sell)");
        }
        final long price = longOpt(args, "--price", ONE);
        final long qty = longOpt(args, "--qty", ONE / 100);
        if (price < 1 || qty < 1) {
            throw new FeedHarnessMain.Usage("ERR --price and --qty must be >= 1");
        }
        final T1Session gw = c.session(SessionClass.GATEWAY);
        final long k = c.corr++;
        gw.offer(new CoreFrames().create(k, user, PROBE_REQUEST_HIGH, c.request++, market, "sell".equals(side) ? 1 : 0, 0, 0, price,
                qty, 0, 0, 0, 0, 0));
        c.out.println(orderReply(gw.reply(k, 100, 101)));
        return 0;
    }

    static int probeAmend(final T1Core.Ctx c, final String[] args) throws Exception {
        if (args.length < 3) {
            throw new FeedHarnessMain.Usage("t1-core probe-amend <orderId> [--price P] [--qty Q]");
        }
        final long order = Long.parseLong(args[2]);
        final long user = dump(c).userOf(order);
        final T1Session gw = c.session(SessionClass.GATEWAY);
        final long k = c.corr++;
        gw.offer(new CoreFrames().amend(k, user, order, PROBE_REQUEST_HIGH, c.request++, longOpt(args, "--price", 0),
                longOpt(args, "--qty", 0)));
        c.out.println(orderReply(gw.reply(k, 100, 101)));
        return 0;
    }

    static int probeCancel(final T1Core.Ctx c, final String[] args) throws Exception {
        if (args.length < 3) {
            throw new FeedHarnessMain.Usage("t1-core probe-cancel <orderId>");
        }
        final long order = Long.parseLong(args[2]);
        final long user = dump(c).userOf(order);
        final T1Session gw = c.session(SessionClass.GATEWAY);
        final long k = c.corr++;
        gw.offer(new CoreFrames().cancel(k, user, order, PROBE_REQUEST_HIGH, c.request++));
        c.out.println(orderReply(gw.reply(k, 100, 101)));
        return 0;
    }

    // ------------------------------------------------------------------ money (item 2)

    /** The result and reason of a Deposit / Withdraw reply, by name. */
    static String moneyReply(final byte[] r) {
        final UnsafeBuffer b = new UnsafeBuffer(r);
        final MessageHeaderDecoder h = new MessageHeaderDecoder();
        return switch (T1Session.templateOf(r)) {
            case 15, 16 -> "result=ACCEPTED";
            case 14 -> "result=REJECTED reason=" + new WithdrawRejectDecoder().wrapAndApplyHeader(b, 0, h).reason();
            case 111 -> "result=REJECTED reason=" + new DepositRejectDecoder().wrapAndApplyHeader(b, 0, h).reason();
            case 101 -> "result=REJECTED reason=" + rejectName(new OrderRejectDecoder().wrapAndApplyHeader(b, 0, h).reasonRaw());
            default -> "result=UNKNOWN";
        };
    }

    /** The {@code --record <flog>} writer: appends the TREASURY frame the core was sent, for {@code oracle --extra}. */
    static void record(final Path flog, final byte[] frame) throws java.io.IOException {
        final List<FeedFiles.LogEntry> entries = new ArrayList<>();
        if (Files.exists(flog)) {
            entries.addAll(FeedFiles.decodeFlog(Files.readAllBytes(flog)));
        }
        entries.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.FRAME, CoreApplyRunner.TREASURY_SESSION, FeedStory.T0, frame));
        Files.write(flog, FeedFiles.encodeFlog(entries));
    }

    // ------------------------------------------------------------------ invariants (item 2)

    static int invariants(final T1Core.Ctx c, final String[] args) throws Exception {
        final String seam = T1Core.opt(args, "--seam");
        if (seam != null && !seam.equals("BREAK_LOCKED")) {
            throw new FeedHarnessMain.Usage("ERR --seam " + seam + " (BREAK_LOCKED)");
        }
        final Dump d = dump(c);
        final Map<Long, Map<Integer, Long>> sumH = new HashMap<>();
        for (final long[] h : d.holds()) {
            sumH.computeIfAbsent(h[1], k -> new HashMap<>()).merge((int) h[2], h[3], Long::sum);
        }
        boolean broken = seam == null;
        int rows = 0;
        String fail = null;
        final Set<String> seen = new HashSet<>();
        for (final Map.Entry<Long, TreeMap<Integer, long[]>> u : d.balances().entrySet()) {
            for (final Map.Entry<Integer, long[]> a : u.getValue().entrySet()) {
                long locked = a.getValue()[1];
                if (!broken && locked != 0) {
                    locked += 1; // BREAK_LOCKED: the negative control of 4.3
                    broken = true;
                }
                final long available = a.getValue()[0];
                final long held = sumH.getOrDefault(u.getKey(), Map.of()).getOrDefault(a.getKey(), 0L);
                final boolean ok = locked == held && available >= 0 && locked >= 0;
                seen.add(u.getKey() + " " + a.getKey());
                rows++;
                c.out.println("I " + u.getKey() + " " + a.getKey() + " available=" + available + " locked=" + locked + " sumH=" + held
                        + (ok ? " OK" : " FAIL"));
                if (!ok && fail == null) {
                    fail = "user=" + u.getKey() + " asset=" + a.getKey() + " available=" + available + " locked=" + locked + " sumH=" + held;
                }
            }
        }
        for (final Map.Entry<Long, Map<Integer, Long>> u : sumH.entrySet()) {
            for (final Map.Entry<Integer, Long> a : u.getValue().entrySet()) {
                if (!seen.contains(u.getKey() + " " + a.getKey()) && fail == null) {
                    fail = "user=" + u.getKey() + " asset=" + a.getKey() + " has holds " + a.getValue() + " and no B line";
                }
            }
        }
        if (fail != null) {
            c.out.println("INVARIANTS FAIL " + fail);
            return FeedHarnessMain.FAIL;
        }
        if (!broken) {
            c.out.println("INVARIANTS FAIL --seam BREAK_LOCKED found no locked balance to break");
            return FeedHarnessMain.FAIL;
        }
        c.out.println("INVARIANTS PASS rows=" + rows + " holds=" + d.holds().size());
        return 0;
    }

    // ------------------------------------------------------------------ dump --orders (item 6)

    static int dumpOrders(final T1Core.Ctx c, final String[] args) throws Exception {
        final String file = T1Core.opt(args, "--orders");
        if (file == null) {
            throw new FeedHarnessMain.Usage("t1-core dump --orders <file>");
        }
        final Set<Long> ids = new HashSet<>();
        for (final String l : Files.readAllLines(Path.of(file), StandardCharsets.US_ASCII)) {
            if (!l.isBlank()) {
                ids.add(Long.parseLong(l.trim()));
            }
        }
        final Dump d = dump(c);
        final Set<Long> users = new HashSet<>();
        for (final long id : ids) {
            users.add(d.userOf(id)); // an unknown order is an ERR (C7)
        }
        int n = 0;
        for (final String l : d.lines()) {
            final String[] f = l.split(" ");
            final boolean keep = switch (f[0]) {
                case "O", "H" -> ids.contains(Long.parseLong(f[1]));
                case "B" -> users.contains(Long.parseLong(f[1]));
                default -> false;
            };
            if (keep) {
                c.out.println(l);
                n++;
            }
        }
        c.out.println("DUMP orders=" + ids.size() + " lines=" + n);
        return 0;
    }

    // ------------------------------------------------------------------ gate --watch (item 7) and suspend (item 8)

    static String names(final int bits) {
        final StringBuilder sb = new StringBuilder();
        for (final GateReason r : GateReason.values()) {
            if ((bits & (1 << r.code())) != 0) {
                sb.append(sb.length() == 0 ? "" : ",").append(r.name());
            }
        }
        return sb.length() == 0 ? "-" : sb.toString();
    }

    static String admissionLine(final byte[] f) {
        final AdmissionStateDecoder d = new AdmissionStateDecoder().wrapAndApplyHeader(new UnsafeBuffer(f), 0,
                new MessageHeaderDecoder());
        final int bits = new UnsafeBuffer(f).getInt(8 + AdmissionStateDecoder.gateReasonsEncodingOffset(), ByteOrder.LITTLE_ENDIAN);
        return "t=" + System.nanoTime() / 1_000_000 + " state=" + d.gateState() + " reasons=" + Integer.toUnsignedLong(bits)
                + " names=" + names(bits) + " wall=" + System.currentTimeMillis();
    }

    static boolean globalAdmission(final byte[] f) {
        return T1Session.templateOf(f) == 109 && new UnsafeBuffer(f).getInt(8, ByteOrder.LITTLE_ENDIAN) == 0;
    }

    static int gateWatch(final T1Core.Ctx c, final String[] args) throws Exception {
        final long ms = Long.parseLong(T1Core.opt(args, "--watch"));
        final String until = T1Core.opt(args, "--until");
        if (ms < 1 || ms > 1000) {
            throw new FeedHarnessMain.Usage("ERR gate --watch <ms>: 1..1000");
        }
        final long deadline = until == null ? Long.MAX_VALUE : System.currentTimeMillis() + 1000 * Long.parseLong(until);
        final T1Session gw = T1Session.connect(c.env, "T1_GATEWAY_KEY");
        try {
            final long k = c.corr++;
            gw.offer(new CoreFrames().register(k, SessionClass.GATEWAY, 0, 0));
            final byte[] ack = gw.reply(k, 110);
            final int result = ack[8 + 10] & 0xFF;
            if (result != 0 && result != 1) {
                throw new IllegalStateException("RegisterSession GATEWAY refused (result " + result + ")");
            }
            int from = gw.frames.indexOf(ack) + 1;
            boolean initial = true;
            int lines = 0;
            int transitions = 0;
            while (System.currentTimeMillis() < deadline && !Thread.currentThread().isInterrupted()) {
                gw.poll();
                for (; from < gw.frames.size(); from++) {
                    final byte[] f = gw.frames.get(from);
                    if (!globalAdmission(f)) {
                        continue;
                    }
                    if (initial) {
                        c.out.println("INITIAL " + admissionLine(f));
                        initial = false;
                        continue;
                    }
                    transitions++;
                    if (FeedHarnessSeams.on(FeedHarnessSeams.GATE_WATCH_DROPS_ONE) && transitions == 2) {
                        continue; // the negative control: one transition not printed
                    }
                    c.out.println(admissionLine(f));
                    c.out.flush();
                    lines++;
                }
                gw.frames.clear(); // a long watch keeps nothing it printed
                from = 0;
                try {
                    Thread.sleep(ms);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            c.out.println("WATCH end lines=" + lines);
            c.out.flush();
            return 0;
        } finally {
            gw.close();
        }
    }

    static int suspend(final T1Core.Ctx c, final String[] args) throws Exception {
        final int market = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        final T1Session admin = c.session(SessionClass.ADMIN);
        final long k = c.corr++;
        admin.offer(new CoreFrames().gate(k, market, false));
        c.out.println(T1Core.ack(admin.reply(k, 110)));
        return 0;
    }
}

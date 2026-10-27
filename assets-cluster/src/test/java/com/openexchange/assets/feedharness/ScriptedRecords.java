// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RC2 S9h 4.2: the six committed ME journal fixtures (match {@code MeJournalFixtureDump}, verified against their pinned
 * SUMS first) and the named scripted cases built from them: the S9a 9.4 staging sequences, the release group sizes, the
 * raw enum vectors with the expected core outcome of each (review FIX-3), the head extremes and the S12 INVALID carrier.
 * A case name is what a C++ golden failure prints.
 */
public final class ScriptedRecords {

    /** sha256 of the fixtures' {@code SHA256SUMS} at match 421226c (S9a 9.2 pin). */
    public static final String FIXTURE_SUMS_SHA256 = "127739bfb6c0b5da5d0e8cf5b5b52706e8c58ce075d8bad33ca25e696f3da5c4";
    /** The meEpoch every fixture carries ({@code MeJournalFixtureDump.G}). */
    public static final long FIXTURE_EPOCH = 0x1234_5678_9ABC_DEF1L;
    /** The fixtures in producer order (config FRESH seq 1 .. release seq 6). */
    public static final List<String> FIXTURE_ORDER = List.of("java-config.bin", "java-trade.bin", "java-terminal.bin",
            "java-command.first.bin", "java-command.conflict.bin", "java-release.bin");

    /** Input class of 5 (coherent cases come from FeedStory, not from here). */
    public enum InputClass { VALID, ARBITRARY }

    /** The 4.2 outcome column a raw case must land in when applied as the exactly-next entry on a bound v3 core. */
    public enum Expect { NONE, POISON, APPLIES, REFUSED_4_2 }

    /** One named case: its FJR input, class and (raw cases) expected core outcome. */
    public record Case(String name, List<FeedFiles.Entry> input, InputClass inputClass, Expect expect) { }

    /**
     * The S12 INVALID carrier (cpp: amendment 7, S12 5.5 / 13.2): a record 3 with result 8, orderType 2, timeInForce 1,
     * orderId 0 and status -1, otherwise the command.first fixture's command. Translated like any record 3.
     */
    public static final byte[] S12_INVALID_CARRIER = new JournalRecords().outcome(2, FIXTURE_EPOCH, 192, 17, 1, 100, 9001, 0,
            5_900_000_000_000L, 100_000_000L, 0, 1, 0, 2, 0, 192, 1_000L, 0L, -1, 0, 0, 8, 1, 0L, 0, 100_000, 2);

    private ScriptedRecords() {
    }

    /** The six fixtures from {@code dir}, refused unless every file matches the SUMS and the SUMS matches the pin. */
    public static Map<String, byte[]> loadFixtures(final Path dir) throws IOException {
        final Map<String, byte[]> files = FeedFiles.verifySums(dir, FIXTURE_SUMS_SHA256);
        final Map<String, byte[]> out = new LinkedHashMap<>();
        for (final String name : FIXTURE_ORDER) {
            final byte[] b = files.get(name);
            if (b == null) {
                throw new FeedFiles.Refused("fixture " + name + " missing from the SUMS");
            }
            out.put(name, b);
        }
        if (files.size() != FIXTURE_ORDER.size()) {
            throw new FeedFiles.Refused("the fixture SUMS names " + files.size() + " files, expected 6");
        }
        return out;
    }

    /** {@code java-command.first.bin} -> {@code fx_command_first}. */
    public static String fixtureCaseName(final String file) {
        return "fx_" + file.substring("java-".length(), file.length() - ".bin".length()).replace('.', '_');
    }

    private static List<FeedFiles.Entry> records(final byte[]... rs) {
        final List<FeedFiles.Entry> l = new ArrayList<>();
        for (final byte[] r : rs) {
            l.add(FeedFiles.Entry.record(r));
        }
        return l;
    }

    /** Trades {@code first .. first + n - 1} (journalSeq and tradeId dense), every field valid under S8 3.9. */
    static List<byte[]> tradeRun(final JournalRecords j, final int n, final long firstSeq, final long firstTradeId) {
        final List<byte[]> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(trade(j, firstSeq + i, firstTradeId + i, 1));
        }
        return out;
    }

    static byte[] trade(final JournalRecords j, final long seq, final long tradeId, final int takerIsBuyRaw) {
        return j.trade(seq, FIXTURE_EPOCH, 128 + seq, tradeId, 1, 91, 11, 92, 12, 12_345_678_901_234L, 123_456_789L,
                takerIsBuyRaw, 1_700_000_000_123L + seq, 101, 102);
    }

    static byte[] terminal(final JournalRecords j, final long seq, final int statusRaw, final int rejectReasonRaw) {
        return j.terminal(seq, FIXTURE_EPOCH, 128 + seq, 91, 11, 1, statusRaw, 1_700_000_000_123L, 101, rejectReasonRaw);
    }

    /** A release of {@code n} entries (UP_TO APPLIED, high 17 + i, floor i, removed 1). */
    static byte[] release(final JournalRecords j, final long seq, final int n) {
        final long[][] e = new long[n][];
        for (int i = 0; i < n; i++) {
            e[i] = new long[] {17 + i, i, 0, 0, 1};
        }
        return j.release(seq, FIXTURE_EPOCH, 640 + seq, 7, 1, 100_000, 1, e);
    }

    static byte[] releaseRaw(final JournalRecords j, final long seq, final int scopeRaw, final int resultRaw) {
        return j.release(seq, FIXTURE_EPOCH, 640 + seq, 7, 1, 100_000, 1, new long[] {17, 2, scopeRaw, resultRaw, 1});
    }

    static byte[] config(final JournalRecords j, final long seq, final int decisionRaw) {
        return j.config(seq, FIXTURE_EPOCH, 64 + seq, 1 + seq, 2, 100_000, 12, decisionRaw);
    }

    /** Every named case of 4.2, in a fixed order. */
    public static List<Case> cases(final Map<String, byte[]> fixtures) {
        final JournalRecords j = new JournalRecords();
        final List<Case> out = new ArrayList<>();
        for (final String f : FIXTURE_ORDER) {
            out.add(new Case(fixtureCaseName(f), records(fixtures.get(f)), InputClass.VALID, Expect.NONE));
        }
        final List<FeedFiles.Entry> all = new ArrayList<>();
        for (final String f : FIXTURE_ORDER) {
            all.add(FeedFiles.Entry.record(fixtures.get(f)));
        }
        out.add(new Case("fx_all", all, InputClass.VALID, Expect.NONE));

        out.add(new Case("two_trades", records(tradeRun(j, 2, 2, 1).toArray(new byte[0][])), InputClass.VALID, Expect.NONE));
        out.add(new Case("trades_64", records(tradeRun(j, 64, 2, 1).toArray(new byte[0][])), InputClass.VALID, Expect.NONE));
        out.add(new Case("trades_65", records(tradeRun(j, 65, 2, 1).toArray(new byte[0][])), InputClass.VALID, Expect.NONE));
        final List<FeedFiles.Entry> f64 = records(tradeRun(j, 64, 2, 1).toArray(new byte[0][]));
        f64.add(FeedFiles.Entry.FLUSH);
        f64.add(FeedFiles.Entry.record(trade(j, 66, 65, 1)));
        out.add(new Case("trades_64_flush_1", f64, InputClass.VALID, Expect.NONE));
        final List<FeedFiles.Entry> tft = records(trade(j, 2, 1, 1));
        tft.add(FeedFiles.Entry.FLUSH);
        tft.add(FeedFiles.Entry.record(trade(j, 3, 2, 0)));
        out.add(new Case("trade_flush_trade", tft, InputClass.VALID, Expect.NONE));
        // review NOTE-5: a leading FLUSH and two FLUSHes in a row (an empty flush emits nothing, never an empty 62)
        final List<FeedFiles.Entry> ff = new ArrayList<>();
        ff.add(FeedFiles.Entry.FLUSH);
        ff.add(FeedFiles.Entry.record(trade(j, 2, 1, 1)));
        ff.add(FeedFiles.Entry.record(trade(j, 3, 2, 0)));
        out.add(new Case("flush_first", ff, InputClass.VALID, Expect.NONE));
        final List<FeedFiles.Entry> f2 = records(trade(j, 2, 1, 1));
        f2.add(FeedFiles.Entry.FLUSH);
        f2.add(FeedFiles.Entry.FLUSH);
        f2.add(FeedFiles.Entry.record(trade(j, 3, 2, 0)));
        f2.add(FeedFiles.Entry.FLUSH);
        f2.add(FeedFiles.Entry.FLUSH);
        out.add(new Case("flush_flush", f2, InputClass.VALID, Expect.NONE));
        out.add(new Case("trade_terminal_trade", records(trade(j, 2, 1, 1), terminal(j, 3, 2, 0), trade(j, 4, 2, 0)),
                InputClass.VALID, Expect.NONE));
        out.add(new Case("release_48", records(release(j, 2, 48)), InputClass.VALID, Expect.NONE));
        out.add(new Case("release_0", records(release(j, 2, 0)), InputClass.VALID, Expect.NONE));
        out.add(new Case("release_1", records(release(j, 2, 1)), InputClass.VALID, Expect.NONE));

        // raw enum vectors (4.2 table, review FIX-3); each is ONE record (or one 2-run) at journalSeq 2
        for (final int v : new int[] {0, 2, 254, 255}) {
            final Expect e = v == 0 ? Expect.APPLIES : Expect.POISON;
            out.add(new Case("raw_takerIsBuy_" + v, records(trade(j, 2, 1, v)), InputClass.ARBITRARY, e));
            out.add(new Case("raw62_takerIsBuy_" + v, records(trade(j, 2, 1, v), trade(j, 3, 2, 1)), InputClass.ARBITRARY, e));
        }
        for (final int v : new int[] {0, 2, 254, 255}) {
            out.add(new Case("raw_status_" + v, records(terminal(j, 2, v, 0)), InputClass.ARBITRARY,
                    v == 2 ? Expect.APPLIES : Expect.POISON));
        }
        for (final int v : new int[] {0, 2, 254, 255}) {
            out.add(new Case("raw_rejectReason_" + v, records(terminal(j, 2, 2, v)), InputClass.ARBITRARY,
                    v == 255 ? Expect.POISON : Expect.APPLIES));
        }
        for (final int v : new int[] {0, 2, 254, 255}) {
            out.add(new Case("raw_scope_" + v, records(releaseRaw(j, 2, v, 0)), InputClass.ARBITRARY,
                    v == 0 ? Expect.APPLIES : Expect.POISON));
        }
        for (final int v : new int[] {0, 2, 254, 255}) {
            out.add(new Case("raw_result_" + v, records(releaseRaw(j, 2, 0, v)), InputClass.ARBITRARY,
                    v == 0 ? Expect.APPLIES : Expect.POISON)); // 2 with scope 0 is poison (DX:1324)
        }
        out.add(new Case("raw_result_2_scope_1", records(releaseRaw(j, 2, 1, 2)), InputClass.ARBITRARY, Expect.APPLIES));
        for (final int v : new int[] {0, 2, 254, 255}) {
            out.add(new Case("raw_decision_" + v, records(config(j, 2, v)), InputClass.ARBITRARY,
                    v >= 254 ? Expect.POISON : Expect.REFUSED_4_2));
        }

        // head extremes: journalSeq, meEpoch, egressSeq at 1 and INT64_MAX on every template (all admitted by HEAD)
        final List<FeedFiles.Entry> head = new ArrayList<>();
        for (final long v : new long[] {1L, Long.MAX_VALUE}) {
            head.add(FeedFiles.Entry.record(JournalRecords.withHead(fixtures.get("java-trade.bin"), v, v, v)));
            head.add(FeedFiles.Entry.FLUSH);
        }
        for (final String f : List.of("java-terminal.bin", "java-command.first.bin", "java-release.bin", "java-config.bin")) {
            for (final long v : new long[] {1L, Long.MAX_VALUE}) {
                head.add(FeedFiles.Entry.record(JournalRecords.withHead(fixtures.get(f), v, v, v)));
            }
        }
        head.add(FeedFiles.Entry.record(JournalRecords.withHead(fixtures.get("java-trade.bin"), Long.MAX_VALUE, 1L, 1L)));
        head.add(FeedFiles.Entry.record(JournalRecords.withHead(fixtures.get("java-trade.bin"), 1L, Long.MAX_VALUE, 1L)));
        head.add(FeedFiles.Entry.record(JournalRecords.withHead(fixtures.get("java-trade.bin"), 1L, 1L, Long.MAX_VALUE)));
        out.add(new Case("head_extremes", head, InputClass.ARBITRARY, Expect.NONE));

        out.add(new Case("s12_invalid_carrier", records(S12_INVALID_CARRIER), InputClass.VALID, Expect.NONE));
        return out;
    }

    /** The names of {@link #cases}, in order (the list {@code fixturesCasesMatchTheNamedList} pins). */
    public static List<String> caseNames(final Map<String, byte[]> fixtures) {
        final List<String> n = new ArrayList<>();
        for (final Case c : cases(fixtures)) {
            n.add(c.name());
        }
        return n;
    }

    /**
     * 5.1 input shaping (review N-3): the records of {@code input} renumbered densely behind a FRESH 68 at journalSeq 1
     * under {@code epoch} (the FRESH is the input's own first record when it already is one at seq 1), every record's
     * meEpoch set to {@code epoch} and egressSeq kept, raised only where needed to stay non-decreasing. FLUSH markers are kept.
     */
    public static List<FeedFiles.Entry> renumber(final List<FeedFiles.Entry> input, final long epoch) {
        final JournalRecords j = new JournalRecords();
        final List<FeedFiles.Entry> out = new ArrayList<>();
        long seq = 1;
        long egress = 1;
        boolean first = true;
        for (final FeedFiles.Entry e : input) {
            if (e.isFlush()) {
                out.add(e);
                continue;
            }
            final byte[] r = e.bytes();
            final boolean fresh = JournalRecords.templateId(r) == 5 && JournalRecords.journalSeq(r) == 1
                    && (r[8 + 38] & 0xFF) == 0;
            if (first && !fresh) {
                out.add(FeedFiles.Entry.record(j.config(1, epoch, 1, 1, 2, 100_000, 12, 0)));
                seq = 2;
            }
            if (first && fresh) {
                seq = 1;
            }
            first = false;
            egress = Math.max(egress, JournalRecords.egressSeq(r)); // ranges like appliedPosition <= egressSeq hold
            out.add(FeedFiles.Entry.record(JournalRecords.withHead(r, seq, epoch, egress)));
            seq++;
        }
        if (first) {
            out.add(FeedFiles.Entry.record(j.config(1, epoch, 1, 1, 2, 100_000, 12, 0)));
        }
        return out;
    }
}

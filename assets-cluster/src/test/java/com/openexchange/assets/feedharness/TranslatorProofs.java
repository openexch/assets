// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S9h section 5 and 11.1: the proofs of the reference translator, each a static check that throws
 * {@link AssertionError} naming what failed, so the tests run them with every seam off and the negative controls run them
 * with ONE seam on and require the stated failure. Three formulations share no code with the translator's mapping:
 * the core's own decode (the S7 demuxer on a registered BRIDGE session), the digests (the core's, and RawDigests over an
 * independent decoder run) and the literal byte table of S9a 6.2 / 6.3.
 */
public final class TranslatorProofs {

    public static final long T0 = 1_700_000_000_000L;

    private TranslatorProofs() {
    }

    // ------------------------------------------------------------------ inputs

    public static Path fixtureDir() {
        try {
            return Path.of(TranslatorProofs.class.getClassLoader().getResource("feedharness/mejournal/SHA256SUMS").toURI())
                    .getParent();
        } catch (java.net.URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Map<String, byte[]> fixtures() {
        try {
            return ScriptedRecords.loadFixtures(fixtureDir());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static List<ScriptedRecords.Case> cases() {
        return ScriptedRecords.cases(fixtures());
    }

    public static ScriptedRecords.Case named(final String name) {
        for (final ScriptedRecords.Case c : cases()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        throw new IllegalArgumentException("no case " + name);
    }

    /** Records only (FLUSH markers dropped). */
    public static List<byte[]> recordsOf(final List<FeedFiles.Entry> input) {
        final List<byte[]> r = new ArrayList<>();
        for (final FeedFiles.Entry e : input) {
            if (!e.isFlush()) {
                r.add(e.bytes());
            }
        }
        return r;
    }

    /** One input record and the output it became: the frame and, for a 62, the entry index (else -1). */
    public record Pair(byte[] record, byte[] frame, int entry) { }

    /** Pairs records with the frames (and 62 entries) they translated into, in order. */
    public static List<Pair> pairs(final List<FeedFiles.Entry> input) {
        final List<byte[]> records = recordsOf(input);
        final List<byte[]> frames = MeJournalReferenceTranslator.translate(input);
        final List<Pair> out = new ArrayList<>();
        int r = 0;
        for (final byte[] f : frames) {
            if (FeedDigests.templateOf(f) == 62) {
                final int n = u16(f, 8 + 2);
                for (int i = 0; i < n; i++) {
                    out.add(new Pair(records.get(r++), f, i));
                }
            } else {
                out.add(new Pair(records.get(r++), f, -1));
            }
        }
        if (r != records.size()) {
            throw new AssertionError("translate emitted " + r + " entries for " + records.size() + " records");
        }
        return out;
    }

    static int u16(final byte[] b, final int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8;
    }

    // ------------------------------------------------------------------ 2.2 stale-jar trap

    public static void schemaPin(final ClassLoader loader) {
        final String sha = FeedHarnessMain.journalSchemaSha256(loader);
        assertEquals("the match-common on the classpath carries the schema 4 pin (" + FeedHarnessMain.JOURNAL_SCHEMA_RESOURCE + ")",
                FeedHarnessMain.JOURNAL_SCHEMA_SHA256, sha);
        try (InputStream s = loader.getResourceAsStream(FeedHarnessMain.JOURNAL_SCHEMA_RESOURCE)) {
            final String xml = new String(s.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertTrue("schema root id=\"4\"", xml.contains("id=\"4\""));
            assertTrue("schema root version=\"1\"", xml.contains("version=\"1\""));
            assertTrue("package com.match.infrastructure.mejournal.generated",
                    xml.contains("package=\"com.match.infrastructure.mejournal.generated\""));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        try {
            Class.forName("com.match.infrastructure.mejournal.generated.TradeDecoder", false, loader);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("no schema 4 TradeDecoder on the classpath");
        }
    }

    // ------------------------------------------------------------------ 5.3 the literal table (S9a 6.2)

    /** {outOffset, inOffset, width} block offsets per template; 67 and 62 entries separately. */
    static final int[][] T61 = {{0, 16, 8}, {8, 24, 8}, {16, 32, 4}, {20, 36, 8}, {28, 44, 8}, {36, 52, 8}, {44, 60, 8},
            {52, 68, 8}, {60, 76, 8}, {68, 84, 1}, {69, 85, 8}, {77, 93, 8}, {85, 101, 8}, {93, 8, 8}, {101, 0, 8}};
    static final int[][] T63 = {{0, 16, 8}, {8, 24, 8}, {16, 32, 8}, {24, 40, 4}, {28, 44, 1}, {29, 45, 8}, {37, 53, 8},
            {45, 8, 8}, {53, 0, 8}, {61, 61, 1}};
    static final int[][] T64 = {{0, 16, 8}, {8, 24, 8}, {16, 32, 8}, {24, 40, 8}, {32, 48, 8}, {40, 56, 8}, {48, 64, 8},
            {56, 72, 8}, {64, 80, 8}, {72, 88, 4}, {76, 92, 1}, {77, 93, 1}, {78, 94, 1}, {79, 95, 8}, {87, 103, 8},
            {95, 111, 8}, {103, 119, 4}, {107, 123, 4}, {111, 127, 1}, {112, 128, 4}, {116, 8, 8}, {124, 0, 8}, {132, 132, 1},
            {133, 133, 8}, {141, 141, 4}, {145, 145, 4}, {149, 149, 4}};
    static final int[][] T67 = {{0, 16, 8}, {8, 0, 8}, {16, 8, 8}, {24, 24, 8}, {32, 32, 4}, {36, 36, 4}, {40, 40, 4}};
    static final int[][] T67E = {{0, 0, 8}, {8, 8, 8}, {16, 16, 1}, {17, 17, 1}, {18, 18, 4}};
    static final int[][] T68 = {{0, 16, 8}, {8, 0, 8}, {16, 8, 8}, {24, 24, 4}, {28, 28, 4}, {32, 32, 4}, {36, 36, 2},
            {38, 38, 1}};

    /** The table covers every out byte of the block exactly once (and the source side likewise). */
    public static void tableIsComplete() {
        coverage("61", T61, 109);
        coverage("63", T63, 62);
        coverage("64", T64, 153);
        coverage("67 root", T67, 44);
        coverage("67 entry", T67E, 22);
        coverage("68", T68, 39);
    }

    private static void coverage(final String what, final int[][] table, final int block) {
        for (final int side : new int[] {0, 1}) {
            final int[] hits = new int[block];
            for (final int[] row : table) {
                for (int i = 0; i < row[2]; i++) {
                    hits[row[side] + i]++;
                }
            }
            for (int i = 0; i < block; i++) {
                assertEquals(what + " table covers " + (side == 0 ? "out" : "in") + " byte " + i + " once", 1, hits[i]);
            }
        }
    }

    private static void fields(final String what, final byte[] out, final int outBase, final byte[] in, final int inBase,
                               final int[][] table) {
        for (final int[] row : table) {
            for (int i = 0; i < row[2]; i++) {
                if (out[outBase + row[0] + i] != in[inBase + row[1] + i]) {
                    fail(what + ": out block @" + row[0] + " (width " + row[2] + ") != in @" + row[1] + " at byte " + i
                            + " (S9a 6.2 table)");
                }
            }
        }
    }

    /** everyFieldLandsWhereTheTableSays: the S9a 6.2 table as literal offset pairs, raw enum bytes included. */
    public static void everyFieldLands(final List<FeedFiles.Entry> input, final String what) {
        for (final Pair p : pairs(input)) {
            final int tin = JournalRecords.templateId(p.record());
            final int tout = FeedDigests.templateOf(p.frame());
            switch (tin) {
                case 1 -> {
                    if (p.entry() < 0) {
                        assertEquals(what + ": record 1 -> 61", 61, tout);
                        fields(what + " 61", p.frame(), 8, p.record(), 8, T61);
                    } else {
                        assertEquals(what + ": record 1 -> 62 entry", 62, tout);
                        fields(what + " 62 entry " + p.entry(), p.frame(), 12 + 109 * p.entry(), p.record(), 8, T61);
                    }
                }
                case 2 -> {
                    assertEquals(what + ": record 2 -> 63", 63, tout);
                    fields(what + " 63", p.frame(), 8, p.record(), 8, T63);
                }
                case 3 -> {
                    assertEquals(what + ": record 3 -> 64", 64, tout);
                    fields(what + " 64", p.frame(), 8, p.record(), 8, T64);
                }
                case 4 -> {
                    assertEquals(what + ": record 4 -> 67", 67, tout);
                    fields(what + " 67", p.frame(), 8, p.record(), 8, T67);
                    final int n = u16(p.record(), 8 + 44 + 2);
                    for (int i = 0; i < n; i++) {
                        fields(what + " 67 entry " + i, p.frame(), 56 + 22 * i, p.record(), 56 + 22 * i, T67E);
                    }
                }
                case 5 -> {
                    assertEquals(what + ": record 5 -> 68", 68, tout);
                    fields(what + " 68", p.frame(), 8, p.record(), 8, T68);
                }
                default -> fail(what + ": record template " + tin);
            }
        }
    }

    /** headersAndGroupHeadersAreExact (5.3, review FIX-2). */
    public static void headersExact(final List<FeedFiles.Entry> input, final String what) {
        final List<byte[]> frames = MeJournalReferenceTranslator.translate(input);
        // the expected output entries, restated from the input alone: {template, n}
        final List<int[]> expected = new ArrayList<>();
        int run = 0;
        for (final FeedFiles.Entry e : input) {
            if (e.isFlush() || JournalRecords.templateId(e.bytes()) != 1) {
                if (run > 0) {
                    expected.add(new int[] {run == 1 ? 61 : 62, run});
                    run = 0;
                }
                if (!e.isFlush()) {
                    final int t = JournalRecords.templateId(e.bytes());
                    expected.add(new int[] {61 + t + (t >= 4 ? 2 : 0), t == 4 ? u16(e.bytes(), 8 + 44 + 2) : 1});
                }
                continue;
            }
            if (++run == 64) {
                expected.add(new int[] {62, 64});
                run = 0;
            }
        }
        if (run > 0) {
            expected.add(new int[] {run == 1 ? 61 : 62, run});
        }
        assertEquals(what + ": frame count", expected.size(), frames.size());
        for (int k = 0; k < frames.size(); k++) {
            final byte[] f = frames.get(k);
            final int t = FeedDigests.templateOf(f);
            assertEquals(what + ": frame " + k + " template", expected.get(k)[0], t);
            final int block = switch (t) {
                case 61 -> 109;
                case 62 -> 0;
                case 63 -> 62;
                case 64 -> 153;
                case 67 -> 44;
                case 68 -> 39;
                default -> -1;
            };
            final byte[] head = {(byte) block, (byte) (block >>> 8), (byte) t, 0, 2, 0, 7, 0};
            assertArrayEquals(what + ": out[0, 8) of a " + t + " is {" + block + ", " + t + ", 2, 7}", head,
                    Arrays.copyOf(f, 8));
            final int n = expected.get(k)[1];
            switch (t) {
                case 62 -> {
                    assertEquals(what + ": 62 group header blockLength", 109, u16(f, 8));
                    assertEquals(what + ": 62 numInGroup equals the run length", n, u16(f, 10));
                    assertEquals(what + ": 62 frame length 12 + 109 n", 12 + 109 * n, f.length);
                }
                case 67 -> {
                    assertEquals(what + ": 67 group header blockLength", 22, u16(f, 52));
                    assertEquals(what + ": 67 numInGroup equals the input's", n, u16(f, 54));
                    assertEquals(what + ": 67 frame length 56 + 22 n", 56 + 22 * n, f.length);
                }
                default -> assertEquals(what + ": " + t + " frame length 8 + BLOCK_LENGTH", 8 + block, f.length);
            }
        }
    }

    /** memcpyIdentitiesHold: the S9a 6.3 byte ranges, no codec. */
    public static void memcpyIdentities(final List<FeedFiles.Entry> input, final String what) {
        for (final Pair p : pairs(input)) {
            final byte[] in = p.record();
            final byte[] out = p.frame();
            final int t = FeedDigests.templateOf(out);
            final int base = p.entry() < 0 ? 0 : 12 + 109 * p.entry() - 8; // a 62 entry: the 61 frame offsets shifted
            switch (t) {
                case 61, 62 -> {
                    range(what + " 61 out[16,101) == in[32,117)", out, base + 16, in, 32, 85);
                    head(what + " 61", out, base + 8, base + 8 + 101, base + 8 + 93, in);
                }
                case 63 -> {
                    range(what + " 63 out[16,53) == in[32,69)", out, 16, in, 32, 37);
                    range(what + " 63 out[69] == in[69]", out, 69, in, 69, 1);
                    head(what + " 63", out, 8, 8 + 53, 8 + 45, in);
                }
                case 64 -> {
                    range(what + " 64 out[16,124) == in[32,140)", out, 16, in, 32, 108);
                    range(what + " 64 out[140,161) == in[140,161)", out, 140, in, 140, 21);
                    head(what + " 64", out, 8, 8 + 124, 8 + 116, in);
                }
                case 67, 68 -> {
                    range(what + " " + t + " out[32,end) == in[32,end)", out, 32, in, 32, out.length - 32);
                    assertEquals(what + " " + t + " same length", in.length, out.length);
                    head(what + " " + t, out, 8, 16, 24, in);
                }
                default -> fail(what + ": template " + t);
            }
        }
    }

    private static void range(final String what, final byte[] out, final int o, final byte[] in, final int i, final int n) {
        for (int k = 0; k < n; k++) {
            if (out[o + k] != in[i + k]) {
                fail(what + ": differs at +" + k);
            }
        }
    }

    /** The permuted head: out egressSeq / journalSeq / meEpoch at the given frame offsets equal in @24 / @8 / @16. */
    private static void head(final String what, final byte[] out, final int egressAt, final int journalAt, final int epochAt,
                             final byte[] in) {
        range(what + " head egressSeq", out, egressAt, in, 24, 8);
        range(what + " head journalSeq", out, journalAt, in, 8, 8);
        range(what + " head meEpoch", out, epochAt, in, 16, 8);
    }

    // ------------------------------------------------------------------ 5.2 b digests over an independent decoder run

    public static void decodedDigestEqualsSource(final List<FeedFiles.Entry> input, final String what) {
        final List<byte[]> records = recordsOf(input);
        final List<Long> out = new ArrayList<>();
        for (final byte[] f : MeJournalReferenceTranslator.translate(input)) {
            out.addAll(FeedDigests.rawOfFrame(f));
        }
        assertEquals(what + ": one digest per record", records.size(), out.size());
        for (int i = 0; i < records.size(); i++) {
            assertEquals(what + ": decoded digest of entry " + i + " equals the source digest (template "
                    + JournalRecords.templateId(records.get(i)) + ")", FeedDigests.rawOfRecord(records.get(i)), (long) out.get(i));
        }
    }

    /** rawDigestsEqualProductionInRange: RawDigests == the production 3.8 functions on in-range records. */
    public static void rawDigestsEqualProduction(final List<byte[]> inRange, final String what) {
        for (final byte[] r : inRange) {
            assertEquals(what + ": RawDigests equals AssetsEngine's digest (template " + JournalRecords.templateId(r) + ")",
                    FeedDigests.productionOfRecord(r), FeedDigests.rawOfRecord(r));
        }
    }

    /** outOfRangeRawBytesSurviveExactly: every raw 0..255 on the enum fields and rejectReason. */
    public static void rawBytesSurvive() {
        final JournalRecords j = new JournalRecords();
        for (int v = 0; v < 256; v++) {
            // 61 takerIsBuy, 62 entry takerIsBuy (second entry), 63 status and rejectReason, 67 scope / result, 68 decision
            final byte[] t = ScriptedRecords.trade(j, 2, 1, v);
            final byte[] f61 = single(t);
            assertEquals("61 takerIsBuy raw " + v, (byte) v, f61[8 + 68]);
            final List<FeedFiles.Entry> run = List.of(FeedFiles.Entry.record(ScriptedRecords.trade(j, 2, 1, 1)),
                    FeedFiles.Entry.record(ScriptedRecords.trade(j, 3, 2, v)));
            final byte[] f62 = MeJournalReferenceTranslator.translate(run).get(0);
            assertEquals("62 entry 1 takerIsBuy raw " + v, (byte) v, f62[12 + 109 + 68]);
            final byte[] s = single(ScriptedRecords.terminal(j, 2, v, 0));
            assertEquals("63 status raw " + v, (byte) v, s[8 + 28]);
            final byte[] rr = single(ScriptedRecords.terminal(j, 2, 2, v));
            assertEquals("63 rejectReason raw " + v, (byte) v, rr[8 + 61]);
            final byte[] sc = single(ScriptedRecords.releaseRaw(j, 2, v, 0));
            assertEquals("67 scope raw " + v, (byte) v, sc[56 + 16]);
            final byte[] re = single(ScriptedRecords.releaseRaw(j, 2, 0, v));
            assertEquals("67 result raw " + v, (byte) v, re[56 + 17]);
            final byte[] de = single(ScriptedRecords.config(j, 2, v));
            assertEquals("68 decision raw " + v, (byte) v, de[8 + 38]);
            for (final List<FeedFiles.Entry> in : List.of(List.of(FeedFiles.Entry.record(t)), run,
                    List.of(FeedFiles.Entry.record(ScriptedRecords.terminal(j, 2, v, v))),
                    List.of(FeedFiles.Entry.record(ScriptedRecords.releaseRaw(j, 2, v, 255 - v))),
                    List.of(FeedFiles.Entry.record(ScriptedRecords.config(j, 2, v))))) {
                decodedDigestEqualsSource(in, "raw " + v);
            }
        }
    }

    static byte[] single(final byte[] record) {
        final List<byte[]> f = MeJournalReferenceTranslator.translate(List.of(FeedFiles.Entry.record(record)));
        assertEquals("one record, one frame", 1, f.size());
        return f.get(0);
    }

    // ------------------------------------------------------------------ 3.3 staging

    /** stagingFunctionTable: the frame sequence (template:entries) of every 4.2 staging case. */
    public static void stagingTable() {
        final Map<String, String> expected = new java.util.LinkedHashMap<>();
        expected.put("two_trades", "62:2");
        expected.put("trades_64", "62:64");
        expected.put("trades_65", "62:64 61");
        expected.put("trades_64_flush_1", "62:64 61");
        expected.put("trade_flush_trade", "61 61");
        expected.put("trade_terminal_trade", "61 63 61");
        expected.put("flush_first", "62:2");
        expected.put("flush_flush", "61 61");
        expected.put("fx_all", "68 61 63 64 64 67");
        expected.put("release_0", "67");
        for (final Map.Entry<String, String> e : expected.entrySet()) {
            assertEquals("staging of " + e.getKey(), e.getValue(), shape(named(e.getKey()).input()));
        }
        // the cap flush happens when the 64th trade is staged, not when the 65th arrives
        final JournalRecords j = new JournalRecords();
        final List<byte[]> emitted = new ArrayList<>();
        final MeJournalReferenceTranslator t = new MeJournalReferenceTranslator((b, o, l) -> emitted.add(new byte[l]));
        for (final byte[] r : ScriptedRecords.tradeRun(j, 64, 2, 1)) {
            t.record(r);
        }
        assertEquals("the 64th staged trade flushes at once", 1, emitted.size());
        assertEquals("nothing staged after the cap flush", 0, t.staged());
    }

    public static String shape(final List<FeedFiles.Entry> input) {
        final StringBuilder sb = new StringBuilder();
        for (final byte[] f : MeJournalReferenceTranslator.translate(input)) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            final int t = FeedDigests.templateOf(f);
            sb.append(t);
            if (t == 62) {
                sb.append(':').append(u16(f, 10));
            }
        }
        return sb.toString();
    }

    /** releaseGroupBounds: 0, 1, 48 entries; frame 56 + 22 n, group header {22, n}. */
    public static void releaseGroupBounds() {
        final JournalRecords j = new JournalRecords();
        for (final int n : new int[] {0, 1, 48}) {
            final byte[] f = single(ScriptedRecords.release(j, 2, n));
            assertEquals("67 of " + n + ": frame length 56 + 22 n", 56 + 22 * n, f.length);
            assertEquals("67 of " + n + ": group blockLength 22", 22, u16(f, 52));
            assertEquals("67 of " + n + ": numInGroup", n, u16(f, 54));
        }
    }

    // ------------------------------------------------------------------ gate

    /** nonAdmittedRecordIsRefused: rows 2-7 refuse with IllegalArgumentException, nothing emitted, staging untouched. */
    public static void nonAdmittedRefused() {
        final JournalRecords j = new JournalRecords();
        final byte[] trade = ScriptedRecords.trade(j, 2, 1, 1);
        final List<byte[]> bad = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        bad.add(Arrays.copyOf(trade, 7));
        names.add("LENGTH 7");
        bad.add(new byte[1113]);
        names.add("LENGTH 1113");
        final byte[] schema3 = trade.clone();
        schema3[4] = 3;
        bad.add(schema3);
        names.add("SCHEMA 3");
        final byte[] version2 = trade.clone();
        version2[6] = 2;
        bad.add(version2);
        names.add("SCHEMA version 2");
        final byte[] template6 = trade.clone();
        template6[2] = 6;
        bad.add(template6);
        names.add("TEMPLATE 6");
        final byte[] block = trade.clone();
        block[0] = 110;
        bad.add(block);
        names.add("BLOCK 110");
        bad.add(Arrays.copyOf(trade, trade.length + 1));
        names.add("FRAME_LENGTH +1");
        final byte[] rel = ScriptedRecords.release(j, 3, 1);
        final byte[] rel49 = rel.clone();
        rel49[8 + 44 + 2] = 49;
        bad.add(rel49);
        names.add("FRAME_LENGTH release n 49");
        bad.add(Arrays.copyOf(rel, 55));
        names.add("FRAME_LENGTH release below 56");
        bad.add(JournalRecords.withHead(trade, 0, 1, 1));
        names.add("HEAD journalSeq 0");
        bad.add(JournalRecords.withHead(trade, 1, 0, 1));
        names.add("HEAD meEpoch 0");
        bad.add(JournalRecords.withHead(trade, 1, -5, 1));
        names.add("HEAD meEpoch -5");
        for (int i = 0; i < bad.size(); i++) {
            final List<byte[]> emitted = new ArrayList<>();
            final MeJournalReferenceTranslator t = new MeJournalReferenceTranslator((b, o, l) -> emitted.add(new byte[l]));
            t.record(trade);
            boolean refused = false;
            try {
                t.record(bad.get(i));
            } catch (IllegalArgumentException e) {
                refused = true;
            } catch (RuntimeException e) {
                // with the gate off a bad record may also break the decoder: that is not the gate's refusal
            }
            assertTrue("non-admitted record " + names.get(i) + " refused with IllegalArgumentException", refused);
            assertEquals(names.get(i) + ": nothing emitted", 0, emitted.size());
            assertEquals(names.get(i) + ": staging untouched", 1, t.staged());
            assertTrue(names.get(i) + ": the gate refuses it", !ReferenceFrameGate.check(bad.get(i)).admitted());
        }
    }

    // ------------------------------------------------------------------ 5.1 the core decodes it

    /** A behaviour-3 core bound to {@code epoch} with the pinned BRIDGE session registered. */
    public static CoreApplyRunner core(final long epoch) {
        final CoreApplyRunner r = CoreApplyRunner.boot(new CoreApplyRunner.Genesis(epoch, 0, 0, T0));
        r.openBridge(CoreApplyRunner.BRIDGE_SESSION, CoreApplyRunner.BRIDGE_IDENTITY, T0);
        return r;
    }

    public static long drops(final CoreApplyRunner r) {
        return r.demuxer().schemaRejectCount() + r.demuxer().decodeRejectCount() + r.demuxer().unknownTemplateCount()
                + r.demuxer().unhandledTemplateCount();
    }

    /** everyFrameDecodesThroughTheS7Demuxer for a valid input: translate(renumber(input)) leaves every drop counter. */
    public static CoreApplyRunner decodesThroughTheDemuxer(final List<FeedFiles.Entry> input, final long epoch, final String what) {
        final CoreApplyRunner r = core(epoch);
        final long before = drops(r);
        final long unauthorised = r.engine().getUnauthorisedCount();
        for (final byte[] f : MeJournalReferenceTranslator.translate(ScriptedRecords.renumber(input, epoch))) {
            r.apply(CoreApplyRunner.BRIDGE_SESSION, f, T0 + 500);
            assertEquals(what + ": a " + FeedDigests.templateOf(f) + " frame moved a demuxer drop counter (schema "
                    + r.demuxer().schemaRejectCount() + ", decode/bounds " + r.demuxer().decodeRejectCount() + ", unknown "
                    + r.demuxer().unknownTemplateCount() + ", unhandled " + r.demuxer().unhandledTemplateCount() + ")",
                    before, drops(r));
        }
        assertEquals(what + ": no authority refusal on the BRIDGE session", unauthorised, r.engine().getUnauthorisedCount());
        assertTrue(what + ": the FRESH bound the epoch", r.engine().feed().boundMeEpoch() == epoch);
        return r;
    }

    /** rawValuesReachTheCoreAsTheTableSays (4.2 table, review FIX-3). */
    public static void rawValuesReachTheCore() {
        int checked = 0;
        for (final ScriptedRecords.Case c : cases()) {
            if (c.expect() == ScriptedRecords.Expect.NONE) {
                continue;
            }
            final CoreApplyRunner r = core(ScriptedRecords.FIXTURE_EPOCH);
            final List<byte[]> frames;
            try {
                frames = MeJournalReferenceTranslator.translate(ScriptedRecords.renumber(c.input(), ScriptedRecords.FIXTURE_EPOCH));
            } catch (IllegalArgumentException e) {
                throw new AssertionError(c.name() + ": the translator threw " + e + " instead of copying the raw byte");
            }
            r.apply(CoreApplyRunner.BRIDGE_SESSION, frames.get(0), T0 + 500); // the FRESH at seq 1
            assertEquals(c.name() + ": bound", ScriptedRecords.FIXTURE_EPOCH, r.engine().feed().boundMeEpoch());
            final long d0 = r.demuxer().decodeRejectCount();
            for (int i = 1; i < frames.size(); i++) {
                r.apply(CoreApplyRunner.BRIDGE_SESSION, frames.get(i), T0 + 500);
            }
            final long d = r.demuxer().decodeRejectCount() - d0;
            final var f = r.engine().feed();
            switch (c.expect()) {
                case POISON -> {
                    assertEquals(c.name() + ": decode poison (decodeReject + 1)", 1, d);
                    assertEquals(c.name() + ": latch SOURCE_SCHEMA", "SOURCE_SCHEMA", f.latchReason().name());
                    assertEquals(c.name() + ": skip s", 2, f.latchSkipSeq());
                }
                case APPLIES -> {
                    assertEquals(c.name() + ": no decode reject", 0, d);
                    assertTrue(c.name() + ": no SOURCE_SCHEMA latch (" + f.latchReason() + ")",
                            !f.latchReason().name().equals("SOURCE_SCHEMA"));
                }
                case REFUSED_4_2 -> {
                    assertEquals(c.name() + ": no decode reject", 0, d);
                    assertEquals(c.name() + ": refused by 4.2: SOURCE_SCHEMA", "SOURCE_SCHEMA", f.latchReason().name());
                    assertEquals(c.name() + ": skip s", 2, f.latchSkipSeq());
                }
                default -> fail(c.name());
            }
            checked++;
        }
        assertEquals("every raw case of 4.2 checked", 29, checked);
    }

    // ------------------------------------------------------------------ purity

    public static void translateIsPure(final Path scratch) throws IOException {
        for (final ScriptedRecords.Case c : cases()) {
            final List<byte[]> a = MeJournalReferenceTranslator.translate(c.input());
            final List<byte[]> b = MeJournalReferenceTranslator.translate(c.input());
            assertEquals(c.name() + ": two runs", a.size(), b.size());
            for (int i = 0; i < a.size(); i++) {
                assertArrayEquals(c.name() + ": two runs, frame " + i, a.get(i), b.get(i));
            }
            final Path in = scratch.resolve(c.name() + ".fjr");
            final Path out = scratch.resolve(c.name() + ".ffr");
            Files.write(in, FeedFiles.encodeFjr(c.input()));
            final java.io.ByteArrayOutputStream sink = new java.io.ByteArrayOutputStream();
            final int code = FeedHarnessMain.run(new String[] {"translate", in.toString(), out.toString()},
                    new java.io.ByteArrayInputStream(new byte[0]), new java.io.PrintStream(sink), new java.io.PrintStream(sink));
            assertEquals(c.name() + ": CLI exit", 0, code);
            assertArrayEquals(c.name() + ": CLI bytes equal the in-process function", FeedFiles.encodeFfr(a),
                    Files.readAllBytes(out));
        }
    }
}

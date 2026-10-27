// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.application.engine.AssetsEngine;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** RC2 S9h 11.2: the CLI ({@code translate}, {@code random}, {@code fixtures}, {@code e1} framing), exit codes, H1, H2. */
public class FeedHarnessCliTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @After
    public void seamsOff() {
        FeedHarnessSeams.clear();
    }

    /** {exit code, stdout, stderr}. */
    record Run(int code, String out, String err) { }

    static Run cli(final String... args) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ByteArrayOutputStream err = new ByteArrayOutputStream();
        final int code = FeedHarnessMain.run(args, new ByteArrayInputStream(new byte[0]),
                new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Run(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    /** Parses {@code static const std::vector<std::uint8_t> NAME = {...};} back into bytes. */
    static byte[] incArray(final String inc, final String name) {
        final Matcher m = Pattern.compile("std::vector<std::uint8_t> " + Pattern.quote(name) + " = \\{([^}]*)\\};").matcher(inc);
        assertTrue("array " + name + " in the .inc", m.find());
        final ByteArrayOutputStream b = new ByteArrayOutputStream();
        final Matcher h = Pattern.compile("0x([0-9a-f]{2})").matcher(m.group(1));
        while (h.find()) {
            b.write(Integer.parseInt(h.group(1), 16));
        }
        return b.toByteArray();
    }

    @Test
    public void translateWritesFfrAndInc() throws Exception {
        final ScriptedRecords.Case c = TranslatorProofs.named("fx_all");
        final Path in = tmp.getRoot().toPath().resolve("in.fjr");
        final Path out = tmp.getRoot().toPath().resolve("out.ffr");
        final byte[] fjr = FeedFiles.encodeFjr(c.input());
        Files.write(in, fjr);
        final Run r = cli("translate", in.toString(), out.toString(), "--inc", "FX_ALL");
        assertEquals(r.err(), 0, r.code());
        final byte[] ffr = FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(c.input()));
        assertArrayEquals(ffr, Files.readAllBytes(out));
        assertEquals("translate lists out.ffr in the SHA256SUMS next to it", FeedFiles.sha256Hex(ffr),
                FeedFiles.parseSums(Files.readString(out.resolveSibling("SHA256SUMS"))).get("out.ffr"));
        assertArrayEquals("NAME_IN is the FJR file", fjr, incArray(r.out(), "FX_ALL_IN"));
        assertArrayEquals("NAME_OUT is the FFR file", ffr, incArray(r.out(), "FX_ALL_OUT"));
        assertTrue("the build is named on stderr", r.err().contains("feed-harness "));
    }

    @Test
    public void randomIsSeedDeterministic() throws Exception {
        final Path a = tmp.newFolder("a").toPath();
        final Path b = tmp.newFolder("b").toPath();
        final Path c = tmp.newFolder("c").toPath();
        assertEquals(0, cli("random", "1", "3000", a.toString()).code());
        assertEquals(0, cli("random", "1", "3000", b.toString()).code());
        assertEquals(0, cli("random", "2", "3000", c.toString()).code());
        assertEquals("random lists its outputs in SHA256SUMS", 3, FeedFiles.verifySums(a, null).size());
        for (final String f : List.of("in.fjr", "out.ffr", "seed.txt", "SHA256SUMS")) {
            assertArrayEquals("seed 1 twice: " + f, Files.readAllBytes(a.resolve(f)), Files.readAllBytes(b.resolve(f)));
        }
        assertFalse("seeds 1 and 2 differ", java.util.Arrays.equals(Files.readAllBytes(a.resolve("in.fjr")),
                Files.readAllBytes(c.resolve("in.fjr"))));
        final List<FeedFiles.Entry> input = FeedFiles.decodeFjr(Files.readAllBytes(a.resolve("in.fjr")));
        assertEquals(3000, TranslatorProofs.recordsOf(input).size());
        assertArrayEquals("out.ffr is translate(in.fjr)", FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(input)),
                Files.readAllBytes(a.resolve("out.ffr")));
    }

    /** The 4.3 distribution, as a check the RANDOM_NO_EXTREMES control must fail. */
    public static void randomCoverage(final List<FeedFiles.Entry> input) {
        final Set<Integer> templates = new HashSet<>();
        final Set<Integer> runs = new HashSet<>();
        final Set<Integer> releaseSizes = new HashSet<>();
        final Set<String> raw = new HashSet<>();
        final Set<Long> extremes = new HashSet<>();
        int run = 0;
        int flushes = 0;
        int leadingFlush = 0;
        int doubleFlush = 0;
        boolean prevFlush = false;
        boolean first = true;
        for (final FeedFiles.Entry e : input) {
            if (e.isFlush()) {
                flushes++;
                leadingFlush += first ? 1 : 0;
                doubleFlush += prevFlush ? 1 : 0;
                prevFlush = true;
                first = false;
                if (run > 0) {
                    runs.add(run); // a FLUSH ends a run (review NOTE-5): the staged run the translator sees
                }
                run = 0;
                continue;
            }
            prevFlush = false;
            first = false;
            final byte[] r = e.bytes();
            final int t = JournalRecords.templateId(r);
            templates.add(t);
            if (t == 1) {
                run++;
            } else {
                if (run > 0) {
                    runs.add(run);
                }
                run = 0;
            }
            assertTrue("journalSeq >= 1", JournalRecords.journalSeq(r) >= 1);
            assertTrue("meEpoch >= 1", JournalRecords.meEpoch(r) >= 1);
            assertTrue("every random record is admitted", ReferenceFrameGate.check(r).admitted());
            switch (t) {
                case 1 -> raw.add("takerIsBuy:" + (r[8 + 84] & 0xFF));
                case 2 -> {
                    raw.add("status:" + (r[8 + 44] & 0xFF));
                    raw.add("rejectReason:" + (r[8 + 61] & 0xFF));
                }
                case 4 -> {
                    final int n = TranslatorProofs.u16(r, 54);
                    releaseSizes.add(n);
                    for (int i = 0; i < n; i++) {
                        raw.add("scope:" + (r[56 + 22 * i + 16] & 0xFF));
                        raw.add("result:" + (r[56 + 22 * i + 17] & 0xFF));
                    }
                }
                case 5 -> raw.add("decision:" + (r[8 + 38] & 0xFF));
                default -> { }
            }
            // i64 fields at block offset 24 of a trade (tradeId) and of every template's egressSeq @16
            for (final int at : new int[] {16, 24}) {
                extremes.add(new org.agrona.concurrent.UnsafeBuffer(r).getLong(8 + at, java.nio.ByteOrder.LITTLE_ENDIAN));
            }
        }
        assertEquals("every template", Set.of(1, 2, 3, 4, 5), templates);
        if (run > 0) {
            runs.add(run);
        }
        for (final int n : new int[] {1, 2, 63, 64, 65}) {
            assertTrue("a trade run of exactly " + n, runs.contains(n));
        }
        assertTrue("at most one leading FLUSH", leadingFlush <= 1);
        assertTrue("two FLUSHes in a row", doubleFlush > 0);
        assertTrue("a release of 0", releaseSizes.contains(0));
        assertTrue("a release of 48", releaseSizes.contains(48));
        for (final String f : List.of("takerIsBuy", "status", "rejectReason", "scope", "result", "decision")) {
            assertTrue("raw 0 on " + f, raw.contains(f + ":0"));
            assertTrue("raw 255 on " + f, raw.contains(f + ":255"));
        }
        for (final long x : new long[] {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}) {
            assertTrue("every extreme: " + x, extremes.contains(x));
        }
        assertTrue("FLUSH markers", flushes > 0);
    }

    @Test
    public void randomCoversTheDistribution() {
        randomCoverage(new RandomJournal(42).generate(1_000_000)); // the CI count of 4.3
        int leading = 0;
        for (long seed = 1; seed <= 16; seed++) {
            leading += new RandomJournal(seed).generate(10).get(0).isFlush() ? 1 : 0;
        }
        assertTrue("some journals start with a FLUSH, some do not: " + leading, leading > 0 && leading < 16);
    }

    /** The named list of 4.2, written out literally (a new or lost case fails here). */
    static final List<String> NAMED = List.of("fx_config", "fx_trade", "fx_terminal", "fx_command_first",
            "fx_command_conflict", "fx_release", "fx_all", "two_trades", "trades_64", "trades_65", "trades_64_flush_1",
            "trade_flush_trade", "flush_first", "flush_flush", "trade_terminal_trade", "release_48", "release_0", "release_1",
            "raw_takerIsBuy_0", "raw62_takerIsBuy_0", "raw_takerIsBuy_2", "raw62_takerIsBuy_2", "raw_takerIsBuy_254",
            "raw62_takerIsBuy_254", "raw_takerIsBuy_255", "raw62_takerIsBuy_255",
            "raw_status_0", "raw_status_2", "raw_status_254", "raw_status_255",
            "raw_rejectReason_0", "raw_rejectReason_2", "raw_rejectReason_254", "raw_rejectReason_255",
            "raw_scope_0", "raw_scope_2", "raw_scope_254", "raw_scope_255",
            "raw_result_0", "raw_result_2", "raw_result_254", "raw_result_255", "raw_result_2_scope_1",
            "raw_decision_0", "raw_decision_2", "raw_decision_254", "raw_decision_255",
            "head_extremes", "s12_invalid_carrier");

    @Test
    public void fixturesCasesMatchTheNamedList() throws Exception {
        assertEquals(NAMED, ScriptedRecords.caseNames(TranslatorProofs.fixtures()));
        final Path out = tmp.newFolder("fixtures").toPath();
        final Run r = cli("fixtures", TranslatorProofs.fixtureDir().toString(), out.toString());
        assertEquals(r.err(), 0, r.code());
        final Set<String> expected = new TreeSet<>();
        for (final String n : NAMED) {
            expected.add(n + ".fjr");
            expected.add(n + ".ffr");
        }
        expected.add("feed_cases.inc");
        expected.add("SHA256SUMS");
        final Set<String> got = new TreeSet<>();
        try (var s = Files.list(out)) {
            s.forEach(p -> got.add(p.getFileName().toString()));
        }
        assertEquals(expected, got);
        assertEquals(expected.size() - 1, FeedFiles.verifySums(out, null).size());
        final String inc = Files.readString(out.resolve("feed_cases.inc"));
        for (final ScriptedRecords.Case c : TranslatorProofs.cases()) {
            final byte[] fjr = FeedFiles.encodeFjr(c.input());
            assertArrayEquals(c.name() + "_IN", fjr, incArray(inc, c.name() + "_IN"));
            assertArrayEquals(c.name() + "_OUT", FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(c.input())),
                    incArray(inc, c.name() + "_OUT"));
            assertTrue(c.name() + " pinned by hash in the header", inc.contains("// case " + c.name() + " fjr="
                    + FeedFiles.sha256Hex(fjr) + " ffr="));
            assertTrue(c.name() + " in the case table", inc.contains("{\"" + c.name() + "\", &" + c.name() + "_IN, &"));
        }
        // a second run is byte-identical (R4)
        final Path again = tmp.newFolder("again").toPath();
        assertEquals(0, cli("fixtures", TranslatorProofs.fixtureDir().toString(), again.toString()).code());
        assertArrayEquals(Files.readAllBytes(out.resolve("SHA256SUMS")), Files.readAllBytes(again.resolve("SHA256SUMS")));
    }

    @Test
    public void exitCodes() throws Exception {
        assertEquals("no command", 64, cli().code());
        assertEquals("unknown command", 64, cli("nope").code());
        assertEquals("translate arity", 64, cli("translate", "a").code());
        assertEquals("random arity", 64, cli("random", "1").code());
        assertEquals("random not a number", 64, cli("random", "x", "1", tmp.getRoot().toString()).code());
        final Path dir = tmp.getRoot().toPath();
        // H2: bad magic, trailing bytes, a non-admitted record: 65 and no output file
        final Path bad = dir.resolve("bad.fjr");
        Files.write(bad, new byte[] {1, 2, 3, 4, 0, 0, 0, 0});
        final Path out = dir.resolve("o.ffr");
        assertEquals("bad magic", 65, cli("translate", bad.toString(), out.toString()).code());
        assertFalse("no output after a refusal", Files.exists(out));
        final byte[] good = FeedFiles.encodeFjr(TranslatorProofs.named("fx_all").input());
        final byte[] trailing = java.util.Arrays.copyOf(good, good.length + 1);
        Files.write(bad, trailing);
        assertEquals("trailing bytes", 65, cli("translate", bad.toString(), out.toString()).code());
        final byte[] schema3 = ScriptedRecords.trade(new JournalRecords(), 2, 1, 1);
        schema3[4] = 3;
        Files.write(bad, FeedFiles.encodeFjr(List.of(FeedFiles.Entry.record(schema3))));
        final Run refused = cli("translate", bad.toString(), out.toString());
        assertEquals("a non-admitted record", 65, refused.code());
        assertTrue(refused.err(), refused.err().contains("SCHEMA"));
        assertFalse("no output after a refusal", Files.exists(out));
        assertFalse("no tmp left", Files.exists(dir.resolve("o.ffr.tmp")));
        // H2: a fixture SUMS mismatch
        final Path fx = tmp.newFolder("fx").toPath();
        for (final Path p : Files.list(TranslatorProofs.fixtureDir()).toList()) {
            Files.copy(p, fx.resolve(p.getFileName()));
        }
        Files.write(fx.resolve("java-config.bin"), new byte[47]);
        final Path fxOut = tmp.newFolder("fxout").toPath();
        assertEquals("fixture SUMS", 65, cli("fixtures", fx.toString(), fxOut.toString()).code());
        assertEquals("nothing written", 0, Files.list(fxOut).count());
        // 70: an internal error (the output directory does not exist)
        Files.write(bad, good);
        assertEquals("internal", 70, cli("translate", bad.toString(), dir.resolve("no/such/dir/o.ffr").toString()).code());
        // review NOTE-3: a leaked seam refuses every run (70), so no golden can be written under one
        FeedHarnessSeams.set(FeedHarnessSeams.CAP_63, true);
        try {
            final Run leaked = cli("version");
            assertEquals("a seam on: 70", 70, leaked.code());
            assertTrue(leaked.err(), leaked.err().contains("CAP_63"));
        } finally {
            FeedHarnessSeams.clear();
        }
        // version
        final Run v = cli("version");
        assertEquals(0, v.code());
        assertTrue(v.out(), v.out().contains("me_journal_schema_sha256=" + FeedHarnessMain.JOURNAL_SCHEMA_SHA256));
    }

    /** H1 in a fresh JVM whose classpath carries the R00 match-common instead of the schema 4 one: 65, sha named. */
    @Test
    public void staleMatchCommonExits65() throws Exception {
        final Path pinned = Path.of(com.match.infrastructure.mejournal.generated.TradeDecoder.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        final Path r00 = pinned.getParent().getParent().resolve("1.0-runtime.20260927.r00.g7ea66bf")
                .resolve("match-common-1.0-runtime.20260927.r00.g7ea66bf.jar");
        org.junit.Assume.assumeTrue(Files.isRegularFile(r00));
        final String cp = String.join(File.pathSeparator, location(FeedHarnessMain.class), r00.toString());
        final Process p = new ProcessBuilder(javaBin(), "-cp", cp, FeedHarnessMain.class.getName(), "version")
                .redirectErrorStream(true).start();
        final String text = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("exits", p.waitFor(60, TimeUnit.SECONDS));
        assertEquals(text, 65, p.exitValue());
        assertTrue(text, text.contains("H1") && text.contains("<absent>"));
    }

    /** A killed run leaves no partial output file: every output present decodes completely. */
    @Test
    public void outputsAreAtomic() throws Exception {
        final Path out = tmp.newFolder("killed").toPath();
        final String cp = String.join(File.pathSeparator, location(FeedHarnessMain.class), location(AssetsEngine.class),
                location(com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder.class),
                location(org.agrona.DirectBuffer.class),
                location(com.match.infrastructure.mejournal.generated.TradeDecoder.class));
        // a complete earlier output exists: a killed later run must leave it (or a complete new one), never a torn file
        assertEquals(0, cli("random", "9", "1000", out.toString()).code());
        final List<Long> killedAfter = new ArrayList<>();
        for (final long ms : new long[] {400, 900, 1600}) {
            final Process p = new ProcessBuilder(javaBin(), "-cp", cp, FeedHarnessMain.class.getName(), "random", "9",
                    "2000000", out.toString()).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
            Thread.sleep(ms);
            p.destroyForcibly();
            p.waitFor(30, TimeUnit.SECONDS);
            killedAfter.add(ms);
            try (var s = Files.list(out)) {
                for (final Path f : s.toList()) {
                    final String n = f.getFileName().toString();
                    if (n.endsWith(".tmp")) {
                        continue; // a tmp is never an output
                    }
                    final byte[] b = Files.readAllBytes(f);
                    switch (n) {
                        case "in.fjr" -> FeedFiles.decodeFjr(b);
                        case "out.ffr" -> FeedFiles.decodeFfr(b);
                        case "SHA256SUMS" -> FeedFiles.parseSums(new String(b, StandardCharsets.US_ASCII));
                        case "seed.txt" -> assertTrue(new String(b, StandardCharsets.US_ASCII).matches("seed=9\ncount=(1000|2000000)\n"));
                        default -> throw new AssertionError("unexpected output " + n);
                    }
                }
            }
        }
        assertEquals(3, killedAfter.size());
    }

    /** 6.2 {@code apply}: a bare core replays a complete core log (FLOG), then the feed (an FFR on 701), Result as JSON. */
    @Test
    public void applyReplaysACoreLogAndAFeed() throws Exception {
        final long g = ScriptedRecords.FIXTURE_EPOCH;
        final CoreFrames enc = new CoreFrames();
        final List<FeedFiles.LogEntry> core = new ArrayList<>();
        core.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.SESSION_OPEN, 801, 1, com.openexchange.cluster.auth.ClusterPrincipal
                .anyUser(com.openexchange.assets.auth.AssetsSessionClasses.toKit(
                        com.openexchange.assets.infrastructure.generated.SessionClass.ADMIN), 1).encode()));
        for (long v = 1; v <= 3; v++) {
            core.add(new FeedFiles.LogEntry(FeedFiles.LogEntry.FRAME, 801, 2, enc.coreConfig(v, v, v, g)));
        }
        final Path dir = tmp.newFolder("apply").toPath();
        Files.write(dir.resolve("core.flog"), FeedFiles.encodeFlog(core));
        final List<FeedFiles.Entry> feed = ScriptedRecords.renumber(TranslatorProofs.named("fx_all").input(), g);
        Files.write(dir.resolve("feed.ffr"), FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(feed)));
        final Run r = cli("apply", dir.resolve("core.flog").toString(), dir.resolve("feed.ffr").toString(),
                dir.resolve("out.json").toString());
        assertEquals(r.err(), 0, r.code());
        final String json = Files.readString(dir.resolve("out.json")).trim();
        assertEquals("stdout and out.json agree", json, r.out().trim());
        assertTrue("bound to the fixtures' epoch: " + json, json.contains("\"boundMeEpoch\":" + g));
        assertTrue("the core applied the six fixtures in order (its own verdict, R3): " + json,
                json.contains("\"lastAppliedJournalSeq\":6,") && json.contains("\"reason\":\"NONE\""));
        assertTrue("no decode reject", json.contains("\"decodeRejectCount\":0"));
        assertEquals("a refused input file", 65, cli("apply", dir.resolve("feed.ffr").toString(),
                dir.resolve("feed.ffr").toString(), dir.resolve("o2.json").toString()).code());
    }

    static String location(final Class<?> c) throws Exception {
        return Path.of(c.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    }

    static String javaBin() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }
}

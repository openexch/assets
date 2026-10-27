// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * RC2 S9h sections 4, 6, 7: the harness CLI ({@code java -jar assets-feed-harness.jar <command> ...}). Every command
 * first prints the build properties on stderr (so a log names the build that produced it) and refuses to run unless the
 * match-common on the classpath carries the pinned schema 4 XML (rule H1, the stale-jar trap). Exit codes: 0 done,
 * 64 usage, 65 an input refused (bad magic, a SUMS mismatch, a non-admitted record given to translate, H1, H2, H3),
 * 70 an internal error (H4), 1 an oracle or barrier failure, 78 configuration (H5). Outputs are written to
 * {@code <name>.tmp} and renamed: never a partial output file.
 */
public final class FeedHarnessMain {

    /** sha256 of {@code sbe/me-journal-schema.xml} at match 421226c (unchanged since e371592). */
    public static final String JOURNAL_SCHEMA_SHA256 = "a7c58d87e4f1cfaa642014e4c92f46492c5fe20d36bd6b1d127dd7fa05a3541a";
    public static final String JOURNAL_SCHEMA_RESOURCE = "sbe/me-journal-schema.xml";
    public static final String PROPERTIES_RESOURCE = "META-INF/feed-harness.properties";

    public static final int OK = 0;
    public static final int FAIL = 1;
    public static final int USAGE = 64;
    public static final int REFUSED = 65;
    public static final int INTERNAL = 70;
    public static final int CONFIG = 78;

    /** A usage error (exit 64). */
    public static final class Usage extends RuntimeException {
        public Usage(final String message) {
            super(message);
        }
    }

    /** A command's own exit status (oracle FAIL 1, H5 78, ...), with a message for stderr. */
    public static final class Exit extends RuntimeException {
        public final int code;

        public Exit(final int code, final String message) {
            super(message);
            this.code = code;
        }
    }

    private FeedHarnessMain() {
    }

    public static void main(final String[] args) {
        System.exit(run(args, System.in, System.out, System.err));
    }

    public static int run(final String[] args, final InputStream in, final PrintStream out, final PrintStream err) {
        if (args.length == 0) {
            err.println(usage());
            return USAGE;
        }
        if (FeedHarnessSeams.any()) {
            err.println("refusing to run with a test seam on: " + FeedHarnessSeams.active());
            return INTERNAL; // review NOTE-3: a leaked seam can never produce goldens
        }
        final Map<String, String> props = buildProperties();
        if (!args[0].equals("version")) {
            for (final Map.Entry<String, String> e : props.entrySet()) {
                err.println("feed-harness " + e.getKey() + "=" + e.getValue());
            }
        }
        final String found = journalSchemaSha256(FeedHarnessMain.class.getClassLoader());
        if (!JOURNAL_SCHEMA_SHA256.equals(found)) {
            err.println("H1: the match-common on the classpath does not carry the schema 4 pin: " + JOURNAL_SCHEMA_RESOURCE
                    + " sha256 " + (found == null ? "<absent>" : found) + ", pinned " + JOURNAL_SCHEMA_SHA256);
            return REFUSED;
        }
        try {
            switch (args[0]) {
                case "version":
                    for (final Map.Entry<String, String> e : props.entrySet()) {
                        out.println(e.getKey() + "=" + e.getValue());
                    }
                    return OK;
                case "translate":
                    return translate(args, out);
                case "fixtures":
                    return fixtures(args);
                case "random":
                    return random(args);
                case "e1":
                    return e1(args, err);
                default:
                    return HarnessCommands.run(args, in, out, err);
            }
        } catch (Usage u) {
            err.println("usage: " + u.getMessage());
            return USAGE;
        } catch (FeedFiles.Refused r) {
            err.println("refused: " + r.getMessage());
            return REFUSED;
        } catch (Exit x) {
            err.println(x.getMessage());
            return x.code;
        } catch (Throwable t) {
            err.println("internal error: " + t);
            t.printStackTrace(err);
            return INTERNAL;
        }
    }

    static String usage() {
        return String.join("\n", "usage: feed-harness <command>",
                "  version",
                "  translate <in.fjr> <out.ffr> [--inc NAME]",
                "  fixtures <mejournal-dir> <out-dir>",
                "  random <seed> <count> <out-dir>",
                "  e1 <e1-dir> <out-dir>",
                "  story <seed> <rounds> <out-dir>",
                "  apply <core.flog> <feed.ffr-or-flog> <out.json>",
                "  serve",
                "  t1-journal",
                "  t1-core <command> ...");
    }

    // ------------------------------------------------------------------ H1 and the build properties

    /** sha256 of the schema 4 XML the class loader sees, or null when the resource is absent. */
    public static String journalSchemaSha256(final ClassLoader loader) {
        try (InputStream s = loader.getResourceAsStream(JOURNAL_SCHEMA_RESOURCE)) {
            return s == null ? null : FeedFiles.sha256Hex(s.readAllBytes());
        } catch (IOException e) {
            return null;
        }
    }

    /** The packaged properties (2.3) in file order, or {@code build=unpackaged} plus the live journal pin. */
    public static Map<String, String> buildProperties() {
        final Map<String, String> out = new LinkedHashMap<>();
        try (InputStream s = FeedHarnessMain.class.getClassLoader().getResourceAsStream(PROPERTIES_RESOURCE)) {
            if (s == null) {
                out.put("build", "unpackaged");
                out.put("me_journal_schema_sha256", String.valueOf(journalSchemaSha256(FeedHarnessMain.class.getClassLoader())));
                return out;
            }
            final Properties p = new Properties();
            p.load(new java.io.InputStreamReader(s, StandardCharsets.UTF_8));
            for (final String k : new TreeMap<>(toMap(p)).keySet()) {
                out.put(k, p.getProperty(k));
            }
            return out;
        } catch (IOException e) {
            out.put("build", "unreadable: " + e);
            return out;
        }
    }

    private static Map<String, String> toMap(final Properties p) {
        final Map<String, String> m = new TreeMap<>();
        for (final String k : p.stringPropertyNames()) {
            m.put(k, p.getProperty(k));
        }
        return m;
    }

    // ------------------------------------------------------------------ translate

    static int translate(final String[] args, final PrintStream out) throws IOException {
        if (args.length != 3 && !(args.length == 5 && args[3].equals("--inc"))) {
            throw new Usage("translate <in.fjr> <out.ffr> [--inc NAME]");
        }
        final byte[] fjr = Files.readAllBytes(Path.of(args[1]));
        final List<FeedFiles.Entry> input = FeedFiles.decodeFjr(fjr);
        final List<byte[]> frames;
        try {
            frames = MeJournalReferenceTranslator.translate(input);
        } catch (MeJournalReferenceTranslator.GateRefusal e) {
            throw new FeedFiles.Refused(e.getMessage()); // only the gate's refusal is an input error (anything else: 70)
        }
        final byte[] ffr = FeedFiles.encodeFfr(frames);
        final Path outPath = Path.of(args[2]).toAbsolutePath();
        FeedFiles.writeAtomic(outPath, ffr);
        FeedFiles.listInSums(outPath, ffr); // review NOTE-7 / 3.4: listed in the SHA256SUMS next to it
        if (args.length == 5) {
            out.print(IncWriter.pair(args[4], fjr, ffr));
        }
        return OK;
    }

    // ------------------------------------------------------------------ fixtures

    /** Writes every 4.2 case as {@code <name>.fjr} / {@code <name>.ffr}, the {@code feed_cases.inc} body and a SUMS. */
    static int fixtures(final String[] args) throws IOException {
        if (args.length != 3) {
            throw new Usage("fixtures <mejournal-dir> <out-dir>");
        }
        final Map<String, byte[]> fixtures = ScriptedRecords.loadFixtures(Path.of(args[1]));
        final Path dir = Path.of(args[2]);
        Files.createDirectories(dir);
        final Map<String, byte[]> written = new TreeMap<>();
        final StringBuilder hashes = new StringBuilder();
        final StringBuilder arrays = new StringBuilder();
        final StringBuilder table = new StringBuilder();
        table.append("struct FeedFrameCase { const char *name; const std::vector<std::uint8_t> *in;"
                + " const std::vector<std::uint8_t> *out; };\n");
        table.append("static const FeedFrameCase FEED_FRAME_CASES[] = {\n");
        for (final ScriptedRecords.Case c : ScriptedRecords.cases(fixtures)) {
            final byte[] fjr = FeedFiles.encodeFjr(c.input());
            final byte[] ffr = FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(c.input()));
            written.put(c.name() + ".fjr", fjr);
            written.put(c.name() + ".ffr", ffr);
            hashes.append("// case ").append(c.name()).append(" fjr=").append(FeedFiles.sha256Hex(fjr)).append(" ffr=")
                    .append(FeedFiles.sha256Hex(ffr)).append('\n');
            arrays.append(IncWriter.pair(c.name(), fjr, ffr));
            table.append("    {\"").append(c.name()).append("\", &").append(c.name()).append("_IN, &").append(c.name())
                    .append("_OUT},\n");
        }
        table.append("};\n");
        final byte[] inc = (hashes + arrays.toString() + table).getBytes(StandardCharsets.US_ASCII);
        written.put("feed_cases.inc", inc);
        for (final Map.Entry<String, byte[]> e : written.entrySet()) {
            FeedFiles.writeAtomic(dir.resolve(e.getKey()), e.getValue());
        }
        FeedFiles.writeAtomic(dir.resolve("SHA256SUMS"), FeedFiles.sums(written).getBytes(StandardCharsets.US_ASCII));
        return OK;
    }

    // ------------------------------------------------------------------ random

    static int random(final String[] args) throws IOException {
        if (args.length != 4) {
            throw new Usage("random <seed> <count> <out-dir>");
        }
        final long seed;
        final int count;
        try {
            seed = Long.parseLong(args[1]);
            count = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new Usage("random <seed> <count> <out-dir>: " + e.getMessage());
        }
        if (count < 0) {
            throw new Usage("random: count < 0");
        }
        final List<FeedFiles.Entry> input = new RandomJournal(seed).generate(count);
        final Path dir = Path.of(args[3]);
        Files.createDirectories(dir);
        final Map<String, byte[]> written = new TreeMap<>();
        written.put("in.fjr", FeedFiles.encodeFjr(input));
        written.put("out.ffr", FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(input)));
        written.put("seed.txt", ("seed=" + seed + "\ncount=" + count + "\n").getBytes(StandardCharsets.US_ASCII));
        for (final Map.Entry<String, byte[]> e : written.entrySet()) {
            FeedFiles.writeAtomic(dir.resolve(e.getKey()), e.getValue());
        }
        FeedFiles.writeAtomic(dir.resolve("SHA256SUMS"), FeedFiles.sums(written).getBytes(StandardCharsets.US_ASCII));
        return OK;
    }

    // ------------------------------------------------------------------ e1

    /** The E1 pin (section 8): {@code match_commit} and {@code sums_sha256} from the test resource {@code feedharness/e1.pin}. */
    public static Map<String, String> e1Pin() throws IOException {
        try (InputStream s = FeedHarnessMain.class.getClassLoader().getResourceAsStream("feedharness/e1.pin")) {
            if (s == null) {
                throw new FeedFiles.Refused("feedharness/e1.pin missing from the classpath");
            }
            final Map<String, String> m = new TreeMap<>();
            for (final String line : new String(s.readAllBytes(), StandardCharsets.US_ASCII).split("\n")) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                final int eq = line.indexOf('=');
                if (eq < 0) {
                    throw new FeedFiles.Refused("e1.pin line malformed: " + line);
                }
                m.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
            }
            if (!m.containsKey("sums_sha256") || !m.containsKey("match_commit")) {
                throw new FeedFiles.Refused("e1.pin lacks match_commit or sums_sha256");
            }
            return m;
        }
    }

    /** One E1 member stream after the gate: the admitted FJR entries and the refused frames. */
    public record E1Member(List<FeedFiles.Entry> admitted, List<String> refused) { }

    /**
     * The admitted records of an FJC stream as FJR (a FLUSH at each recording STOP and at the end) and
     * {@code refused.txt} lines {@code <frameIndex> <reason> <length> <hex>} for every frame the gate refused
     * ({@code frameIndex} counts FRAME entries of the member's FJC from 0).
     */
    public static E1Member gateMember(final List<FeedFiles.CaptureEntry> fjc) {
        final List<FeedFiles.Entry> admitted = new ArrayList<>();
        final List<String> refused = new ArrayList<>();
        int index = 0;
        for (final FeedFiles.CaptureEntry e : fjc) {
            switch (e.type()) {
                case FeedFiles.CaptureEntry.FRAME -> {
                    final ReferenceFrameGate.Verdict v = ReferenceFrameGate.check(e.bytes());
                    if (v.admitted()) {
                        admitted.add(FeedFiles.Entry.record(e.bytes()));
                    } else {
                        refused.add(index + " " + v.reason() + " " + e.bytes().length + " "
                                + java.util.HexFormat.of().formatHex(e.bytes()));
                    }
                    index++;
                }
                case FeedFiles.CaptureEntry.STOP -> admitted.add(FeedFiles.Entry.FLUSH);
                default -> { }
            }
        }
        if (admitted.isEmpty() || !admitted.get(admitted.size() - 1).isFlush()) {
            admitted.add(FeedFiles.Entry.FLUSH);
        }
        return new E1Member(admitted, refused);
    }

    static int e1(final String[] args, final PrintStream err) throws IOException {
        if (args.length != 3) {
            throw new Usage("e1 <e1-dir> <out-dir>");
        }
        final Map<String, String> pin = e1Pin();
        final Path src = Path.of(args[1]);
        final Map<String, byte[]> files = FeedFiles.verifySums(src, pin.get("sums_sha256"));
        final Path dir = Path.of(args[2]);
        final Map<String, byte[]> written = new TreeMap<>();
        for (final Map.Entry<String, byte[]> f : files.entrySet()) {
            if (!f.getKey().endsWith(".fjc")) {
                continue;
            }
            final String base = f.getKey().substring(0, f.getKey().length() - ".fjc".length());
            final E1Member m = gateMember(FeedFiles.decodeFjc(f.getValue()));
            written.put(base + ".fjr", FeedFiles.encodeFjr(m.admitted()));
            written.put(base + ".ffr", FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(m.admitted())));
            final StringBuilder sb = new StringBuilder();
            for (final String r : m.refused()) {
                sb.append(r).append('\n');
            }
            written.put(base + ".refused.txt", sb.toString().getBytes(StandardCharsets.US_ASCII));
        }
        written.put("e1.pin", ("match_commit=" + pin.get("match_commit") + "\nsums_sha256=" + pin.get("sums_sha256") + "\n")
                .getBytes(StandardCharsets.US_ASCII));
        for (final Map.Entry<String, byte[]> e : written.entrySet()) {
            final Path p = dir.resolve(e.getKey());
            Files.createDirectories(p.getParent());
            FeedFiles.writeAtomic(p, e.getValue());
        }
        FeedFiles.writeAtomic(dir.resolve("SHA256SUMS"), FeedFiles.sums(written).getBytes(StandardCharsets.US_ASCII));
        err.println("e1: " + (written.size() - 1) / 3 + " member streams written to " + dir);
        return OK;
    }
}

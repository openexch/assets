// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.persistence.CoreApplyRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S9h 11.5: the E1 corpus (match {@code mejournal-e1/}, pinned by {@code feedharness/e1.pin}). Every admitted record
 * of every member translates; its frames pass 5.1 (zero drops on a core bound to the scenario's first epoch, records in
 * the member's order; SETTLE_REFUSED and the GAP / EPOCH_MISMATCH refusals that follow are not decode outcomes and are
 * not asserted) and 5.2 b; the refused set equals the MANIFESTs' expected refusals (the schema 3 frame, row 3 SCHEMA).
 */
public class E1CorpusTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    static Path corpus() throws Exception {
        return Path.of(E1CorpusTest.class.getClassLoader().getResource("feedharness/mejournal-e1/SHA256SUMS").toURI()).getParent();
    }

    /** The corpus files, verified against the pin (SUMS sha256) and their SUMS. */
    public static Map<String, byte[]> files(final Path dir) throws Exception {
        return FeedFiles.verifySums(dir, FeedHarnessMain.e1Pin().get("sums_sha256"));
    }

    /** scenario/memberK -> the gated stream. */
    static Map<String, FeedHarnessMain.E1Member> gated(final Map<String, byte[]> files) {
        final Map<String, FeedHarnessMain.E1Member> out = new TreeMap<>();
        for (final Map.Entry<String, byte[]> e : files.entrySet()) {
            if (e.getKey().endsWith(".fjc")) {
                out.put(e.getKey().substring(0, e.getKey().length() - 4),
                        FeedHarnessMain.gateMember(FeedFiles.decodeFjc(e.getValue())));
            }
        }
        return out;
    }

    /** The refusals {@code scenario/memberK frameIndex REASON} the gate made, and those the MANIFESTs expect. */
    public static void refusalsMatchTheManifests(final Map<String, byte[]> files) {
        final TreeSet<String> got = new TreeSet<>();
        for (final Map.Entry<String, FeedHarnessMain.E1Member> e : gated(files).entrySet()) {
            for (final String line : e.getValue().refused()) {
                final String[] p = line.split(" ");
                got.add(e.getKey() + " " + p[0] + " " + p[1]);
            }
        }
        final TreeSet<String> expected = new TreeSet<>();
        for (final Map.Entry<String, byte[]> e : files.entrySet()) {
            if (!e.getKey().endsWith("/MANIFEST")) {
                continue;
            }
            final String scenario = e.getKey().substring(0, e.getKey().indexOf('/'));
            for (final String line : new String(e.getValue(), StandardCharsets.US_ASCII).split("\n")) {
                final int dot = line.indexOf(".refused=");
                if (line.startsWith("member") && dot > 0) {
                    final String[] p = line.substring(dot + ".refused=".length()).split(" ");
                    expected.add(scenario + "/" + line.substring(0, dot) + " " + p[0] + " " + p[1]);
                }
            }
        }
        assertTrue("the corpus expects at least one refusal (the schema 3 record)", !expected.isEmpty());
        assertEquals("the reference gate's refusals equal the MANIFESTs' expected refusals", expected, got);
    }

    @Test
    public void thePinHolds() throws Exception {
        final Map<String, byte[]> f = files(corpus());
        assertEquals("15 scenarios x (3 members + MANIFEST)", 60, f.size());
        assertEquals("the match commit holding the corpus", "ed984774007ccb7bac49218048749452ee7001dc", FeedHarnessMain.e1Pin().get("match_commit"));
    }

    @Test
    public void aFlippedFjcByteFailsTheSums() throws Exception {
        final Path copy = tmp.newFolder("e1").toPath();
        copyTree(corpus(), copy);
        final Path victim = copy.resolve("plain_fill/member1.fjc");
        final byte[] b = Files.readAllBytes(victim);
        b[20] ^= 0x10;
        Files.write(victim, b);
        try {
            files(copy);
            fail("a flipped FJC byte passed the SUMS");
        } catch (FeedFiles.Refused expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("plain_fill/member1.fjc"));
        }
    }

    @Test
    public void refusedSetEqualsTheManifests() throws Exception {
        refusalsMatchTheManifests(files(corpus()));
    }

    @Test
    public void aRemovedManifestRefusalFails() throws Exception {
        final Map<String, byte[]> f = new TreeMap<>(files(corpus()));
        final String m = new String(f.get("schema3_record/MANIFEST"), StandardCharsets.US_ASCII);
        f.put("schema3_record/MANIFEST", m.replaceAll("member0\\.refused=[^\n]*\n", "").getBytes(StandardCharsets.US_ASCII));
        try {
            refusalsMatchTheManifests(f);
            fail("a MANIFEST without its refusal passed");
        } catch (AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("refusal"));
        }
    }

    @Test
    public void everyAdmittedRecordTranslatesDecodesAndDigests() throws Exception {
        int streams = 0;
        long records = 0;
        for (final Map.Entry<String, FeedHarnessMain.E1Member> e : gated(files(corpus())).entrySet()) {
            final List<FeedFiles.Entry> admitted = e.getValue().admitted();
            TranslatorProofs.everyFieldLands(admitted, e.getKey());
            TranslatorProofs.headersExact(admitted, e.getKey());
            TranslatorProofs.memcpyIdentities(admitted, e.getKey());
            TranslatorProofs.decodedDigestEqualsSource(admitted, e.getKey());
            // 5.1: a core bound to the stream's first epoch (its FRESH at seq 1), the records in the member's order
            final byte[] first = admitted.get(0).bytes();
            assertEquals(e.getKey() + ": a FRESH 68 opens the stream", 5, JournalRecords.templateId(first));
            final long epoch = JournalRecords.meEpoch(first);
            final CoreApplyRunner r = TranslatorProofs.core(epoch);
            final long before = TranslatorProofs.drops(r);
            for (final byte[] f : MeJournalReferenceTranslator.translate(admitted)) {
                r.apply(CoreApplyRunner.BRIDGE_SESSION, f, TranslatorProofs.T0 + 500);
                assertEquals(e.getKey() + ": a " + FeedDigests.templateOf(f) + " moved a drop counter", before,
                        TranslatorProofs.drops(r));
            }
            assertEquals(e.getKey() + ": bound", epoch, r.engine().feed().boundMeEpoch());
            records += TranslatorProofs.recordsOf(admitted).size();
            streams++;
        }
        assertEquals("45 member streams", 45, streams);
        assertTrue("records: " + records, records > 200);
    }

    @Test
    public void theE1CommandWritesWhatTheTestComputes() throws Exception {
        final Path out = tmp.newFolder("out").toPath();
        final FeedHarnessCliTest.Run r = FeedHarnessCliTest.cli("e1", corpus().toString(), out.toString());
        assertEquals(r.err(), 0, r.code());
        for (final Map.Entry<String, FeedHarnessMain.E1Member> e : gated(files(corpus())).entrySet()) {
            assertArrayEquals(e.getKey() + ".fjr", FeedFiles.encodeFjr(e.getValue().admitted()),
                    Files.readAllBytes(out.resolve(e.getKey() + ".fjr")));
            assertArrayEquals(e.getKey() + ".ffr", FeedFiles.encodeFfr(MeJournalReferenceTranslator.translate(
                    e.getValue().admitted())), Files.readAllBytes(out.resolve(e.getKey() + ".ffr")));
            final List<String> lines = new ArrayList<>();
            for (final String l : Files.readString(out.resolve(e.getKey() + ".refused.txt")).split("\n")) {
                if (!l.isEmpty()) {
                    lines.add(l);
                }
            }
            assertEquals(e.getKey() + ".refused.txt", e.getValue().refused(), lines);
        }
        assertEquals("the output SUMS covers every file", 45 * 3 + 1, FeedFiles.verifySums(out, null).size());
        // a corrupted corpus is refused before anything is written (H2)
        final Path copy = tmp.newFolder("bad").toPath();
        copyTree(corpus(), copy);
        Files.write(copy.resolve("ioc/MANIFEST"), "tampered\n".getBytes(StandardCharsets.US_ASCII));
        final Path out2 = tmp.newFolder("out2").toPath();
        assertEquals(65, FeedHarnessCliTest.cli("e1", copy.toString(), out2.toString()).code());
        assertEquals(0, Files.list(out2).count());
    }

    static void copyTree(final Path from, final Path to) throws Exception {
        try (var s = Files.walk(from)) {
            for (final Path p : s.toList()) {
                final Path t = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) {
                    Files.createDirectories(t);
                } else {
                    Files.copy(p, t);
                }
            }
        }
    }
}

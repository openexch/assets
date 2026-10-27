// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.match.infrastructure.mejournal.generated.CommandOutcomeDecoder;
import com.match.infrastructure.mejournal.generated.CommandReleaseDecoder;
import com.match.infrastructure.mejournal.generated.EngineConfigDecoder;
import com.match.infrastructure.mejournal.generated.TerminalDecoder;
import com.match.infrastructure.mejournal.generated.TradeDecoder;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S9h 11.1: the reference translator, proved without C++ (R2) three independent ways (section 5): the core decodes
 * every frame, the digests of what the core reads equal the digests of the source, and the S9a 6.2 / 6.3 byte table
 * holds. Every RED of the contract is a negative control in {@link FeedHarnessNegativeControlsTest}.
 */
public class MeJournalReferenceTranslatorTest {

    static final int RANDOM_RECORDS = 10_000;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @After
    public void seamsOff() {
        FeedHarnessSeams.clear();
    }

    static List<FeedFiles.Entry> random(final long seed, final int n) {
        return new RandomJournal(seed).generate(n);
    }

    /** Every input class of 11.1: the six fixtures alone and together, every scripted case, 10^4 random records. */
    static List<ScriptedRecords.Case> allInputs() {
        final List<ScriptedRecords.Case> l = new ArrayList<>(TranslatorProofs.cases());
        l.add(new ScriptedRecords.Case("random", random(0x5_9AL, RANDOM_RECORDS), ScriptedRecords.InputClass.ARBITRARY,
                ScriptedRecords.Expect.NONE));
        return l;
    }

    @Test
    public void matchCommonCarriesTheSchemaFourPin() {
        TranslatorProofs.schemaPin(getClass().getClassLoader());
    }

    /** The RED of the pin, kept as a check: the R00 match-common (match.version, schema 3 only) has no schema 4 resource. */
    @Test
    public void theR00MatchCommonIsRefused() throws Exception {
        // the R00 jar sits next to the schema 4 jar in the same local repository (match-common/<version>/)
        final Path pinned = Path.of(TradeDecoder.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final Path r00 = pinned.getParent().getParent().resolve("1.0-runtime.20260927.r00.g7ea66bf")
                .resolve("match-common-1.0-runtime.20260927.r00.g7ea66bf.jar");
        org.junit.Assume.assumeTrue("the R00 jar is in the local repository: " + r00, Files.isRegularFile(r00));
        try (URLClassLoader l = new URLClassLoader(new URL[] {r00.toUri().toURL()}, ClassLoader.getPlatformClassLoader())) {
            assertNull("R00 carries no " + FeedHarnessMain.JOURNAL_SCHEMA_RESOURCE, FeedHarnessMain.journalSchemaSha256(l));
            try {
                TranslatorProofs.schemaPin(l);
                fail("the pin check passed against the R00 jar");
            } catch (AssertionError expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("schema 4 pin"));
            }
        }
    }

    @Test
    public void gateConstantsMatchTheGeneratedJournalCodecs() {
        assertEquals(TradeDecoder.BLOCK_LENGTH, ReferenceFrameGate.BLOCK[1]);
        assertEquals(TerminalDecoder.BLOCK_LENGTH, ReferenceFrameGate.BLOCK[2]);
        assertEquals(CommandOutcomeDecoder.BLOCK_LENGTH, ReferenceFrameGate.BLOCK[3]);
        assertEquals(CommandReleaseDecoder.BLOCK_LENGTH, ReferenceFrameGate.BLOCK[4]);
        assertEquals(EngineConfigDecoder.BLOCK_LENGTH, ReferenceFrameGate.BLOCK[5]);
        assertEquals(CommandReleaseDecoder.ReleasesDecoder.sbeBlockLength(), ReferenceFrameGate.RELEASE_ENTRY_BLOCK);
        assertEquals(TradeDecoder.SCHEMA_ID, ReferenceFrameGate.SCHEMA_ID);
        assertEquals(TradeDecoder.SCHEMA_VERSION, ReferenceFrameGate.SCHEMA_VERSION);
        assertEquals(0, TradeDecoder.journalSeqEncodingOffset());
        assertEquals(8, TradeDecoder.meEpochEncodingOffset());
        assertEquals(16, TradeDecoder.egressSeqEncodingOffset());
        assertEquals(8 + 44 + 4 + 22 * 48, ReferenceFrameGate.MAX_LENGTH);
    }

    @Test
    public void sixFixturesMatchThePinnedSums() throws Exception {
        assertEquals(6, TranslatorProofs.fixtures().size());
        // RED as a check: one byte flipped in a scratch copy is refused
        final Path copy = tmp.newFolder("fx").toPath();
        for (final Path p : Files.list(TranslatorProofs.fixtureDir()).toList()) {
            Files.copy(p, copy.resolve(p.getFileName()));
        }
        final Path trade = copy.resolve("java-trade.bin");
        final byte[] b = Files.readAllBytes(trade);
        b[40] ^= 1;
        Files.write(trade, b);
        try {
            ScriptedRecords.loadFixtures(copy);
            fail("a flipped fixture byte was accepted");
        } catch (FeedFiles.Refused expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("java-trade.bin"));
        }
    }

    @Test
    public void theTableIsCompleteAndLiteral() {
        TranslatorProofs.tableIsComplete();
    }

    @Test
    public void everyFieldLandsWhereTheTableSays() {
        for (final ScriptedRecords.Case c : allInputs()) {
            TranslatorProofs.everyFieldLands(c.input(), c.name());
        }
    }

    @Test
    public void headersAndGroupHeadersAreExact() {
        for (final ScriptedRecords.Case c : allInputs()) {
            TranslatorProofs.headersExact(c.input(), c.name());
        }
    }

    @Test
    public void rawDigestsEqualProductionInRange() {
        final List<byte[]> inRange = new ArrayList<>();
        for (final byte[] r : TranslatorProofs.recordsOf(random(0xD16E5L, RANDOM_RECORDS))) {
            if (inRange(r)) {
                inRange.add(r);
            }
        }
        for (final ScriptedRecords.Case c : TranslatorProofs.cases()) {
            for (final byte[] r : TranslatorProofs.recordsOf(c.input())) {
                if (inRange(r)) {
                    inRange.add(r);
                }
            }
        }
        assertTrue("enough in-range records: " + inRange.size(), inRange.size() > 2_000);
        TranslatorProofs.rawDigestsEqualProduction(inRange, "in-range");
    }

    /** The values the production digests take without collapsing (takerIsBuy, oldCancelled 0..1, decision 0..3). */
    static boolean inRange(final byte[] r) {
        return switch (JournalRecords.templateId(r)) {
            case 1 -> (r[8 + 84] & 0xFF) <= 1;
            case 3 -> (r[8 + 127] & 0xFF) <= 1;
            case 5 -> (r[8 + 38] & 0xFF) <= 3;
            default -> true;
        };
    }

    @Test
    public void outOfRangeRawBytesSurviveExactly() {
        TranslatorProofs.rawBytesSurvive();
    }

    @Test
    public void memcpyIdentitiesHold() {
        for (final ScriptedRecords.Case c : allInputs()) {
            TranslatorProofs.memcpyIdentities(c.input(), c.name());
        }
    }

    @Test
    public void everyFrameDecodesThroughTheS7Demuxer() {
        int n = 0;
        for (final ScriptedRecords.Case c : TranslatorProofs.cases()) {
            if (c.inputClass() == ScriptedRecords.InputClass.VALID) {
                TranslatorProofs.decodesThroughTheDemuxer(c.input(), ScriptedRecords.FIXTURE_EPOCH, c.name());
                n++;
            }
        }
        assertTrue("valid cases decoded: " + n, n >= 15);
    }

    @Test
    public void decodedDigestEqualsSourceDigest() {
        for (final ScriptedRecords.Case c : allInputs()) {
            TranslatorProofs.decodedDigestEqualsSource(c.input(), c.name());
        }
    }

    @Test
    public void rawValuesReachTheCoreAsTheTableSays() {
        TranslatorProofs.rawValuesReachTheCore();
    }

    @Test
    public void stagingFunctionTable() {
        TranslatorProofs.stagingTable();
    }

    @Test
    public void releaseGroupBounds() {
        TranslatorProofs.releaseGroupBounds();
    }

    @Test
    public void s12InvalidCarrierTranslatesVerbatim() {
        final List<FeedFiles.Entry> in = List.of(FeedFiles.Entry.record(ScriptedRecords.S12_INVALID_CARRIER));
        TranslatorProofs.everyFieldLands(in, "s12_invalid_carrier");
        TranslatorProofs.headersExact(in, "s12_invalid_carrier");
        TranslatorProofs.decodedDigestEqualsSource(in, "s12_invalid_carrier");
        TranslatorProofs.decodesThroughTheDemuxer(in, ScriptedRecords.FIXTURE_EPOCH, "s12_invalid_carrier");
        final byte[] f = TranslatorProofs.single(ScriptedRecords.S12_INVALID_CARRIER);
        assertEquals("result 8", 8, f[8 + 112]);
        assertEquals("orderType 2", 2, f[8 + 77]);
        assertEquals("timeInForce 1", 1, f[8 + 132]);
    }

    @Test
    public void nonAdmittedRecordIsRefused() {
        TranslatorProofs.nonAdmittedRefused();
    }

    @Test
    public void translateIsPure() throws Exception {
        TranslatorProofs.translateIsPure(tmp.newFolder("pure").toPath());
        final List<FeedFiles.Entry> r = random(77, 2_000);
        final List<byte[]> a = MeJournalReferenceTranslator.translate(r);
        final List<byte[]> b = MeJournalReferenceTranslator.translate(random(77, 2_000));
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertArrayEquals("random frame " + i, a.get(i), b.get(i));
        }
    }
}

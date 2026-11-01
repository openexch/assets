// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import org.junit.After;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S9h 11 preamble: every behavioural RED of the contract as a negative control, re-proved on every run. One seam of
 * {@link FeedHarnessSeams} switches one rule off; the proof that guards the rule must then FAIL, and fail for the stated
 * reason (the expected text must be in the assertion message). A proof that passes with its rule off, or fails for
 * another reason, fails this test. Each line {@code NC <seam> -> <message>} goes to stdout for the evidence log.
 */
public class FeedHarnessNegativeControlsTest {

    @After
    public void seamsOff() {
        FeedHarnessSeams.clear();
    }

    /** Runs {@code proof} with {@code seam} on and requires an AssertionError containing {@code expected}. */
    public static void red(final FeedHarnessSeams seam, final String expected, final Runnable proof) {
        FeedHarnessSeams.set(seam, true);
        try {
            proof.run();
        } catch (AssertionError e) {
            final String m = String.valueOf(e.getMessage());
            assertTrue("NC " + seam + " failed, but not for the stated reason (" + expected + "): " + m, m.contains(expected));
            System.out.println("NC " + seam + " -> " + m.replace('\n', ' '));
            return;
        } finally {
            FeedHarnessSeams.set(seam, false);
        }
        fail("NC " + seam + ": the proof PASSED with its rule off (expected a failure: " + expected + ")");
    }

    /** The proof must still PASS with {@code seam} on (it cannot see the defect; the point of another proof). */
    public static void green(final FeedHarnessSeams seam, final Runnable proof) {
        FeedHarnessSeams.set(seam, true);
        try {
            proof.run();
            System.out.println("NC " + seam + " (blind proof stays green, as the contract says)");
        } finally {
            FeedHarnessSeams.set(seam, false);
        }
    }

    private static List<FeedFiles.Entry> fxAll() {
        return TranslatorProofs.named("fx_all").input();
    }

    // ------------------------------------------------------------------ 11.1

    @Test
    public void swapTakerMakerOrderFailsTheTableAndTheDigest() {
        red(FeedHarnessSeams.SWAP_TAKER_MAKER_ORDER, "S9a 6.2 table", () -> TranslatorProofs.everyFieldLands(fxAll(), "fx_all"));
        red(FeedHarnessSeams.SWAP_TAKER_MAKER_ORDER, "decoded digest",
                () -> TranslatorProofs.decodedDigestEqualsSource(TranslatorProofs.named("trades_65").input(), "trades_65"));
    }

    @Test
    public void headerBlockPlusOneFailsOnlyTheHeaderProof() {
        final List<FeedFiles.Entry> in = TranslatorProofs.named("trade_flush_trade").input();
        // review FIX-2: 5.1 and 5.2 cannot see a padded 61 (the core accepts block >= minimum) ...
        green(FeedHarnessSeams.HEADER_BLOCK_PLUS_ONE,
                () -> TranslatorProofs.decodesThroughTheDemuxer(in, ScriptedRecords.FIXTURE_EPOCH, "trade_flush_trade"));
        green(FeedHarnessSeams.HEADER_BLOCK_PLUS_ONE, () -> TranslatorProofs.decodedDigestEqualsSource(in, "trade_flush_trade"));
        // ... 5.3's header proof does
        red(FeedHarnessSeams.HEADER_BLOCK_PLUS_ONE, "out[0, 8)", () -> TranslatorProofs.headersExact(in, "trade_flush_trade"));
    }

    @Test
    public void rawDigestDropFieldFailsTheProductionPin() {
        red(FeedHarnessSeams.RAWDIGEST_DROP_FIELD, "RawDigests equals AssetsEngine's digest",
                () -> TranslatorProofs.rawDigestsEqualProduction(TranslatorProofs.recordsOf(fxAll()), "fx_all"));
    }

    @Test
    public void collapseBoolFailsTheRawByteProof() {
        red(FeedHarnessSeams.COLLAPSE_BOOL, "takerIsBuy raw", TranslatorProofs::rawBytesSurvive);
    }

    @Test
    public void headNotPermutedFailsTheIdentities() {
        red(FeedHarnessSeams.HEAD_NOT_PERMUTED, "head", () -> TranslatorProofs.memcpyIdentities(fxAll(), "fx_all"));
    }

    @Test
    public void headerVersion6IsASchemaRejectAtTheCore() {
        red(FeedHarnessSeams.HEADER_VERSION_6, "moved a demuxer drop counter (schema 1",
                () -> TranslatorProofs.decodesThroughTheDemuxer(fxAll(), ScriptedRecords.FIXTURE_EPOCH, "fx_all"));
    }

    @Test
    public void drop64TailFailsTheDecodedDigest() {
        red(FeedHarnessSeams.DROP_64_TAIL, "decoded digest", () -> TranslatorProofs.decodedDigestEqualsSource(fxAll(), "fx_all"));
    }

    @Test
    public void clampEnumsNeverReachesThePoison() {
        red(FeedHarnessSeams.CLAMP_ENUMS, "the translator threw", TranslatorProofs::rawValuesReachTheCore);
    }

    @Test
    public void cap63FailsTheStagingTable() {
        red(FeedHarnessSeams.CAP_63, "staging of trades_64", TranslatorProofs::stagingTable);
    }

    @Test
    public void groupHeaderU8FailsTheReleaseBounds() {
        red(FeedHarnessSeams.GROUP_HEADER_U8, "frame length 56 + 22 n", TranslatorProofs::releaseGroupBounds);
    }

    @Test
    public void gateOffEmitsANonAdmittedRecord() {
        red(FeedHarnessSeams.GATE_OFF, "refused with IllegalArgumentException", TranslatorProofs::nonAdmittedRefused);
    }

    // ------------------------------------------------------------------ 11.2

    @Test
    public void randomNoExtremesFailsTheDistribution() {
        red(FeedHarnessSeams.RANDOM_NO_EXTREMES, "every extreme",
                () -> FeedHarnessCliTest.randomCoverage(new RandomJournal(42).generate(1_000_000)));
    }

    // ------------------------------------------------------------------ 11.1 on coherent stories (5.1, 5.2 a)

    @Test
    public void storyProofsFailWithTheTranslatorWrong() {
        final FeedStory.Story s = FeedStoryTest.story(1); // built before any seam is on
        red(FeedHarnessSeams.SWAP_TAKER_MAKER_ORDER, "digest of seq", () -> FeedStoryTest.decodesAndDigests(s, "seed 1"));
        red(FeedHarnessSeams.DROP_64_TAIL, "no demuxer drop", () -> FeedStoryTest.decodesAndDigests(s, "seed 1"));
        red(FeedHarnessSeams.HEADER_VERSION_6, "no demuxer drop", () -> FeedStoryTest.decodesAndDigests(s, "seed 1"));
    }

    // ------------------------------------------------------------------ 11.3

    @Test
    public void trustedFeedSkipsRegistration() {
        red(FeedHarnessSeams.TRUSTED_FEED, "UNREGISTERED_SESSION",
                com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest::bridgeNeedsRegistration);
    }

    @Test
    public void roundTimePerFrameBreaksTheDuplicateHash() throws Exception {
        final java.nio.file.Path dir = com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest.storyDir(1);
        red(FeedHarnessSeams.ROUND_TIME_PER_FRAME, "snapshot hash", () -> {
            try {
                com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest.duplicatesKeepTheSnapshot(dir, "seed 1");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Test
    public void restartOnANewSessionChangesTheHash() throws Exception {
        final java.nio.file.Path dir = com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest.storyDir(2);
        red(FeedHarnessSeams.RESTART_NEW_SESSION, "701", () -> {
            try {
                com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest.restartKeepsTheHash(dir, "seed 2");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Test
    public void reverseSwapFailsTheIdentity() {
        final FeedStory.Story s = FeedStoryTest.story(1);
        red(FeedHarnessSeams.REVERSE_SWAP_TAKER_MAKER_ORDER, "translate(reverse(frame)) == frame",
                () -> FeedStoryTest.reverseIdentity(s, "seed 1"));
    }

    @Test
    public void storyWithoutTheBatchBeatIsRefused() {
        red(FeedHarnessSeams.STORY_NO_BATCH_RUN, "coverage: missing [batchRun]", FeedStoryTest::coverageHolds);
    }

    /** Characterisation, not a RED: the TRUSTED_FEED seam also changes the image of the re-homed v3 story. */
    @Test
    public void trustedFeedIsVisibleInTheReproducedImage() {
        red(FeedHarnessSeams.TRUSTED_FEED, "snapshotSha256 equals FrameReplica.image()",
                () -> com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest.reproduces(1));
    }

    // ------------------------------------------------------------------ review fixes

    @Test
    public void classifyingFirstReportsAnApplyFaultAsMalformed() throws Exception {
        final java.nio.file.Path dir = com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest.storyDir(2);
        red(FeedHarnessSeams.H4_CLASSIFY_FIRST, "H4: RESULT, not ERR (malformed request", () -> {
            try {
                com.openexchange.assets.infrastructure.persistence.CoreApplyRunnerTest.h4(dir);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }
}

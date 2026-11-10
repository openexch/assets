// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.feedharness;

import org.junit.After;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code oracle --genesis G} models the second epoch of the dual genesis (4.1 rule 6):
 * the CoreConfig records G, every journal record the model translates carries G (seqs and egressSeqs unchanged, what
 * {@code REGENESIS --fresh-only} then {@code WRITE} writes) and so do the story's MarketTicks ({@code run --genesis G}).
 */
public class DualGenesisModelTest {

    static final long G = 777_001L;

    @After
    public void seamsOff() {
        FeedHarnessSeams.clear();
    }

    static String lineOf(final List<String> dump, final String prefix) {
        for (final String l : dump) {
            if (l.startsWith(prefix)) {
                return l;
            }
        }
        throw new AssertionError("no " + prefix + "line in " + dump);
    }

    /** The F line of the model under G equals the clean one's with the epoch replaced; every money line is equal. */
    static void genesisOverrideShape(final FeedStory.Story s) {
        final List<String> clean = T1Core.model(s, s.length(), List.of());
        final List<String> second = T1Core.model(s, s.length(), List.of(), G);
        final String[] f = lineOf(clean, "F ").split(" ");
        final String expectedF = "F " + f[1] + " " + f[2] + " " + G + " " + f[4];
        assertEquals("the F line differs", expectedF, lineOf(second, "F "));
        assertEquals("the second epoch applied the whole story: not halted", "L false 0", lineOf(second, "L "));
        assertEquals("cursor N", Long.toString(s.length()), f[4]);
        final List<String> moneyClean = CanonicalDump.money(clean);
        final List<String> moneySecond = CanonicalDump.money(second);
        moneyClean.removeIf(l -> l.startsWith("F "));
        moneySecond.removeIf(l -> l.startsWith("F "));
        assertEquals("B, H and O lines are the clean run's", moneyClean, moneySecond);
    }

    @Test
    public void oracleGenesisOverrideMatchesTheReport() {
        final FeedStory.Story s = FeedStoryTest.story(1);
        assertNotEquals("G is not the story's own genesis", G, s.genesis().genesisId());
        genesisOverrideShape(s);
    }

    @Test
    public void genesisIgnoredIsRed() {
        FeedHarnessNegativeControlsTest.red(FeedHarnessSeams.GENESIS_OVERRIDE_IGNORED, "the F line differs",
                () -> genesisOverrideShape(FeedStoryTest.story(1)));
    }

    @Test
    public void underGenesisRewritesOnlyTheTickEpoch() {
        final CoreFrames enc = new CoreFrames();
        final byte[] tick = enc.tick(1, 7, 100, 101, 100, 0x51_0000_0001L);
        final byte[] t = T1Core.underGenesis(tick, G);
        assertEquals(tick.length, t.length);
        int diff = 0;
        for (int i = 0; i < t.length; i++) {
            diff += tick[i] != t[i] ? 1 : 0;
        }
        assertTrue("only the 8 meEpoch bytes may differ: " + diff, diff > 0 && diff <= 8);
        final byte[] deposit = enc.deposit(1, 1, 0, 5, 1, 1);
        assertTrue("an epoch-free frame is unchanged", deposit == T1Core.underGenesis(deposit, G));
        assertTrue("genesis 0 is unchanged", tick == T1Core.underGenesis(tick, 0));
    }
}

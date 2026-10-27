// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S7 16.4: A + B == A + SNAPSHOT + B under behaviour v3, at every split point of a story that uses every feed verb:
 * the bind, applied 61/63/64/67/68 entries, a GAP latch and its reach-clear, a FeedHalt 66, a decode-poison latch, a 76
 * skip, a MarketTick with the bound epoch and the FeedPositionReport. The restore must be invisible: same rendered stream,
 * same final image.
 */
public class FeedSpliceTest {

    private static final String E = "epoch=" + V3Genesis.GENESIS_ID;

    static List<String> story() {
        final List<String> s = new ArrayList<>();
        s.add("CLOCK 1000");
        s.add("V3GENESIS bind=0");
        s.add("QUERY_FEED 1");
        s.add("MECONFIG seq=1 jseq=1 " + E + " cfg=1 behavior=2 cap=4096 maxWire=12 decision=FRESH");
        s.add("GATE market=0 open=1 corr=2");
        s.add("DEPOSIT u=1 asset=0 amt=100000.0");
        s.add("DEPOSIT u=2 asset=1 amt=100.0");
        s.add("CREATE u=1 market=1 side=0 type=0 px=100.0 qty=2.0 rid=1");
        s.add("CREATE u=2 market=1 side=1 type=0 px=100.0 qty=2.0 rid=2");
        s.add("MEOUTCOME order=1 kind=0 attempt=0 result=0 leg=101 seq=2 jseq=2 " + E);
        s.add("MEOUTCOME order=2 kind=0 attempt=0 result=0 leg=202 seq=3 jseq=3 " + E);
        s.add("METRADE id=1 market=1 taker=2 maker=1 px=100.0 qty=0.25 takerBuy=0 seq=4 jseq=4 " + E);
        s.add("METRADE id=3 market=1 taker=2 maker=1 px=100.0 qty=0.25 takerBuy=0 seq=6 jseq=6 " + E); // GAP latch
        s.add("QUERY_FEED 2");
        s.add("METRADE id=2 market=1 taker=2 maker=1 px=100.0 qty=0.25 takerBuy=0 seq=5 jseq=5 " + E); // reach-clear
        s.add("METRADE id=3 market=1 taker=2 maker=1 px=100.0 qty=0.25 takerBuy=0 seq=6 jseq=6 " + E);
        s.add("MERELEASE seq=7 jseq=7 " + E);
        s.add("FEEDHALT reason=SOURCE_SCHEMA expected=8 seen=8 corr=5");
        s.add("MECONFIG seq=8 jseq=8 " + E + " cfg=2 behavior=2 cap=4096 maxWire=12 decision=BUMP"); // reach-clear
        s.add("FEEDPOISON tpl=61 jseq=9 " + E + " tradeId=77");
        s.add("QUERY_FEED 3");
        s.add("LATCHCLEAR reason=SOURCE_SCHEMA jseq=9 corr=6"); // skip: L := 4
        s.add("METRADE id=5 market=1 taker=2 maker=1 px=100.0 qty=0.25 takerBuy=0 seq=10 jseq=10 " + E);
        s.add("MARKETTICK market=1 ver=1 bid=99.0 ask=101.0 " + E);
        s.add("METERMINAL order=2 leg=202 status=3 seq=11 jseq=11 meReason=0 " + E);
        s.add("GATE market=0 open=1 corr=7");
        s.add("QUERY_FEED 4");
        return s;
    }

    @Test
    public void splicedRunEqualsTheUnsplitRunAtEverySplitPoint() {
        final List<String> lines = story();
        final AssetsScenarioRunner whole = AssetsScenarioRunner.newRunner();
        whole.execAll(lines);
        final String expected = whole.output();
        final byte[] image = whole.snapshotBytes();
        assertTrue("the story latches and clears: " + expected, expected.contains("FEEDLATCH reason=GAP")
                && expected.contains("FEEDCLEAR reason=GAP jseq=0 auto=1") && expected.contains("FEEDCLEAR reason=SOURCE_SCHEMA jseq=9 auto=0"));
        assertTrue("the report carries the v7 tail", expected.contains("epoch=" + V3Genesis.GENESIS_ID + " jseq=11 halted=FALSE"));
        assertEquals("the story ends bound, cursor 11", 11L, whole.engine().feed().lastAppliedJournalSeq());
        assertEquals(5L, whole.engine().getLastAppliedTradeId());
        assertEquals("the story has no mismatching copy", 0L, whole.engine().feed().duplicateMismatchCount());
        for (int k = 1; k < lines.size(); k++) {
            final List<String> spliced = new ArrayList<>(lines.subList(0, k));
            spliced.add("SNAPSHOT");
            spliced.addAll(lines.subList(k, lines.size()));
            final AssetsScenarioRunner r = AssetsScenarioRunner.newRunner();
            r.execAll(spliced);
            assertEquals("A + SNAPSHOT + B == A + B, split before line " + k + " (" + lines.get(k) + ")", expected, r.output());
            assertArrayEquals("the same final image, split before line " + k, image, r.snapshotBytes());
            assertEquals("no belt mismatch after the restore, split before line " + k, 0L,
                    r.engine().feed().duplicateMismatchCount());
        }
    }
}

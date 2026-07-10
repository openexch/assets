// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Drives {@link AeDump.SnapshotCollector} with hand-built SBE egress and pins the JSON contract
 * the money-check tool consumes. This is the AE-mode proof we can make without a live cluster
 * (the AE nodes carry no meaningful state in the dev box): it exercises decode, per-(user,asset)
 * dedup, the correlationId-gated terminators, and the exact stdout shape.
 */
public class AeDumpTest {

    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);

    private void feedBalance(AeDump.SnapshotCollector c, long userId, int assetId, long avail, long locked) {
        BalanceUpdateEncoder e = new BalanceUpdateEncoder();
        e.wrapAndApplyHeader(buf, 0, header).userId(userId).assetId(assetId).available(avail).locked(locked);
        c.onMessage(0, 0, buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength(), null);
    }

    private void feedBalanceEnd(AeDump.SnapshotCollector c, long corr, int count) {
        BalanceSnapshotEndEncoder e = new BalanceSnapshotEndEncoder();
        e.wrapAndApplyHeader(buf, 0, header).correlationId(corr).entryCount(count);
        c.onMessage(0, 0, buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength(), null);
    }

    private void feedHold(AeDump.SnapshotCollector c, long orderId, long userId, int assetId, long remaining) {
        HoldSnapshotEntryEncoder e = new HoldSnapshotEntryEncoder();
        e.wrapAndApplyHeader(buf, 0, header).orderId(orderId).userId(userId).assetId(assetId).remaining(remaining);
        c.onMessage(0, 0, buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength(), null);
    }

    private void feedHoldEnd(AeDump.SnapshotCollector c, long corr, int count) {
        HoldSnapshotEndEncoder e = new HoldSnapshotEndEncoder();
        e.wrapAndApplyHeader(buf, 0, header).correlationId(corr).entryCount(count);
        c.onMessage(0, 0, buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength(), null);
    }

    private void feedPosition(AeDump.SnapshotCollector c, long corr, long consume, long lastTrade) {
        FeedPositionReportEncoder e = new FeedPositionReportEncoder();
        e.wrapAndApplyHeader(buf, 0, header).correlationId(corr).consumePosition(consume).lastAppliedTradeId(lastTrade);
        c.onMessage(0, 0, buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength(), null);
    }

    @Test
    public void collectsBalancesHoldsAndPositionIntoTheExpectedJson() {
        AeDump.SnapshotCollector c = new AeDump.SnapshotCollector();

        feedBalance(c, 900001L, 0, 900L, 100L);
        feedBalance(c, 900001L, 1, 50L, 0L);
        feedBalance(c, 900002L, 0, 1000L, 0L);
        assertFalse("not complete before any terminator", c.complete());
        feedBalanceEnd(c, c.balanceCorr, 3);

        feedHold(c, 111L, 900001L, 0, 100L);
        assertFalse("not complete before the hold + position terminators", c.complete());
        feedHoldEnd(c, c.holdCorr, 1);

        feedPosition(c, c.posCorr, 88231L, 45012L);
        assertTrue("complete once all three terminators arrive", c.complete());

        assertEquals(
                "{\"balances\":["
                        + "{\"userId\":900001,\"assetId\":0,\"available\":900,\"locked\":100},"
                        + "{\"userId\":900001,\"assetId\":1,\"available\":50,\"locked\":0},"
                        + "{\"userId\":900002,\"assetId\":0,\"available\":1000,\"locked\":0}],"
                        + "\"holds\":[{\"orderId\":111,\"userId\":900001,\"assetId\":0,\"remaining\":100}],"
                        + "\"consumePosition\":88231,\"lastAppliedTradeId\":45012}",
                c.toJson());
    }

    @Test
    public void dedupsRepeatedBalanceForSameUserAssetLastWriteWins() {
        AeDump.SnapshotCollector c = new AeDump.SnapshotCollector();
        feedBalance(c, 900001L, 0, 900L, 100L);
        feedBalance(c, 900001L, 0, 800L, 200L); // e.g. a live settle update mid-snapshot
        feedBalanceEnd(c, c.balanceCorr, 1);
        feedHoldEnd(c, c.holdCorr, 0);
        feedPosition(c, c.posCorr, 1L, 2L);

        assertEquals(
                "{\"balances\":[{\"userId\":900001,\"assetId\":0,\"available\":800,\"locked\":200}],"
                        + "\"holds\":[],\"consumePosition\":1,\"lastAppliedTradeId\":2}",
                c.toJson());
    }

    @Test
    public void terminatorsWithForeignCorrelationIdsDoNotComplete() {
        AeDump.SnapshotCollector c = new AeDump.SnapshotCollector();
        feedBalanceEnd(c, c.balanceCorr + 1, 0); // someone else's snapshot terminator
        feedHoldEnd(c, c.holdCorr + 1, 0);
        feedPosition(c, c.posCorr + 1, 1L, 2L);
        assertFalse("foreign correlationIds must not satisfy completion", c.complete());

        feedBalanceEnd(c, c.balanceCorr, 0);
        feedHoldEnd(c, c.holdCorr, 0);
        feedPosition(c, c.posCorr, 1L, 2L);
        assertTrue(c.complete());
    }
}

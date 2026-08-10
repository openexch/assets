// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.HoldDecoder;
import com.openexchange.assets.infrastructure.generated.InitTradeHighWaterDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.assets.infrastructure.generated.ReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.SetMoneyJournalDecoder;
import com.openexchange.assets.infrastructure.generated.SettleBatchDecoder;
import com.openexchange.assets.infrastructure.generated.SettleDecoder;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The query classification is what routes a query's entire reply (including the BalanceUpdate frames
 * a balance-snapshot reply streams) to the asking session alone, so its membership is asserted
 * template by template: a mutating command misclassified as a query would narrow live broadcast
 * frames to one session — silently wrong for every other consumer.
 */
public class AssetsSbeDemuxerQueryTest {

    @Test
    public void readOnlyQueriesAreQueries() {
        assertTrue(AssetsSbeDemuxer.isQuery(QueryFeedPositionDecoder.TEMPLATE_ID));
        assertTrue(AssetsSbeDemuxer.isQuery(RequestBalanceSnapshotDecoder.TEMPLATE_ID));
        assertTrue(AssetsSbeDemuxer.isQuery(RequestHoldSnapshotDecoder.TEMPLATE_ID));
    }

    @Test
    public void mutatingCommandsAreNot() {
        assertFalse(AssetsSbeDemuxer.isQuery(DepositDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(WithdrawDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(HoldDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(ReleaseDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(SettleDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(SettleBatchDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(TerminalReleaseDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(InitTradeHighWaterDecoder.TEMPLATE_ID));
        assertFalse(AssetsSbeDemuxer.isQuery(SetMoneyJournalDecoder.TEMPLATE_ID));
        // Subscribe never reaches the demuxer (handled at the service boundary), but a bug that let
        // it through must not also flip the query context.
        assertFalse(AssetsSbeDemuxer.isQuery(SubscribeDecoder.TEMPLATE_ID));
    }
}

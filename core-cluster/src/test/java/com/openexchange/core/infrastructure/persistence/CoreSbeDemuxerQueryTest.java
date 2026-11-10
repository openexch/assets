// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.generated.DepositDecoder;
import com.openexchange.core.infrastructure.generated.HoldDecoder;
import com.openexchange.core.infrastructure.generated.InitTradeHighWaterDecoder;
import com.openexchange.core.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.core.infrastructure.generated.ReleaseDecoder;
import com.openexchange.core.infrastructure.generated.RequestBalanceSnapshotDecoder;
import com.openexchange.core.infrastructure.generated.RequestHoldSnapshotDecoder;
import com.openexchange.core.infrastructure.generated.SetMoneyJournalDecoder;
import com.openexchange.core.infrastructure.generated.SettleBatchDecoder;
import com.openexchange.core.infrastructure.generated.SettleDecoder;
import com.openexchange.core.infrastructure.generated.SubscribeDecoder;
import com.openexchange.core.infrastructure.generated.TerminalReleaseDecoder;
import com.openexchange.core.infrastructure.generated.WithdrawDecoder;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The query classification is what routes a query's entire reply (including the BalanceUpdate frames
 * a balance-snapshot reply streams) to the asking session alone, so its membership is asserted
 * template by template: a mutating command misclassified as a query would narrow live broadcast
 * frames to one session, silently wrong for every other consumer.
 */
public class CoreSbeDemuxerQueryTest {

    @Test
    public void readOnlyQueriesAreQueries() {
        assertTrue(CoreSbeDemuxer.isQuery(QueryFeedPositionDecoder.TEMPLATE_ID));
        assertTrue(CoreSbeDemuxer.isQuery(RequestBalanceSnapshotDecoder.TEMPLATE_ID));
        assertTrue(CoreSbeDemuxer.isQuery(RequestHoldSnapshotDecoder.TEMPLATE_ID));
        // v6: the three core queries, served from + but classified now so their replies are
        // origin-scoped from the first build that answers them.
        assertTrue(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.RequestOrderSnapshotDecoder.TEMPLATE_ID));
        assertTrue(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.RequestPendingDispatchDecoder.TEMPLATE_ID));
        assertTrue(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.RequestRiskConfigDecoder.TEMPLATE_ID));
    }

    @Test
    public void mutatingCommandsAreNot() {
        assertFalse(CoreSbeDemuxer.isQuery(DepositDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(WithdrawDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(HoldDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(ReleaseDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(SettleDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(SettleBatchDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(TerminalReleaseDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(InitTradeHighWaterDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(SetMoneyJournalDecoder.TEMPLATE_ID));
        // Subscribe never reaches the demuxer (handled at the service boundary), but a bug that let
        // it through must not also flip the query context.
        assertFalse(CoreSbeDemuxer.isQuery(SubscribeDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.CreateOrderDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.CancelOrderDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.MeJournalTradeDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.RegisterSessionDecoder.TEMPLATE_ID));
        assertFalse(CoreSbeDemuxer.isQuery(com.openexchange.core.infrastructure.generated.OrderEntryGateDecoder.TEMPLATE_ID));
    }
}

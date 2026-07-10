// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.match.infrastructure.journal.generated.BooleanType;
import com.match.infrastructure.journal.generated.JournalTerminalEncoder;
import com.match.infrastructure.journal.generated.JournalTradeEncoder;
import com.match.infrastructure.journal.generated.MessageHeaderEncoder;
import com.match.infrastructure.journal.generated.TerminalStatus;

import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.SettleDecoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseDecoder;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Golden field-for-field tests for {@link JournalToMoneyTranslator}: every journal input field lands on
 * the expected money output field, with no silent drift, no swapped taker/maker, no truncation of large
 * {@code egressSeq} values, and no leakage of fields the money schema does not have (terminal status).
 */
public class JournalToMoneyTranslatorTest {

    private final JournalToMoneyTranslator translator = new JournalToMoneyTranslator();

    private final UnsafeBuffer journalBuf = new UnsafeBuffer(new byte[256]);
    private final UnsafeBuffer moneyBuf = new UnsafeBuffer(new byte[256]);

    // Journal-side (match-common) fixture encoders.
    private final MessageHeaderEncoder journalHeaderEnc = new MessageHeaderEncoder();
    private final JournalTradeEncoder journalTradeEnc = new JournalTradeEncoder();
    private final JournalTerminalEncoder journalTerminalEnc = new JournalTerminalEncoder();

    // Money-side (assets-common) assertion decoders.
    private final MessageHeaderDecoder moneyHeaderDec = new MessageHeaderDecoder();
    private final SettleDecoder settleDec = new SettleDecoder();
    private final TerminalReleaseDecoder terminalReleaseDec = new TerminalReleaseDecoder();

    // ---- JournalTrade -> Settle ----

    @Test
    public void translatesTradeFieldForField_takerIsBuyTrue() {
        writeJournalTrade(7_777L, 555L, 3, 11L, 100L, 22L, 200L,
                6_000_000L, 100_000L, BooleanType.TRUE, 123_456_789L);

        int len = translator.translateTrade(journalBuf, 0, moneyBuf, 0);

        moneyHeaderDec.wrap(moneyBuf, 0);
        assertEquals(SettleDecoder.TEMPLATE_ID, moneyHeaderDec.templateId());
        assertEquals(SettleDecoder.SCHEMA_ID, moneyHeaderDec.schemaId());
        settleDec.wrapAndApplyHeader(moneyBuf, 0, moneyHeaderDec);

        assertEquals(555L, settleDec.tradeId());
        assertEquals(3, settleDec.marketId());
        assertEquals(500_011L, settleDec.takerOrderId());
        assertEquals(100L, settleDec.takerUserId());
        assertEquals(500_022L, settleDec.makerOrderId());
        assertEquals(200L, settleDec.makerUserId());
        assertEquals(6_000_000L, settleDec.price());
        assertEquals(100_000L, settleDec.quantity());
        assertEquals(BoolFlag.TRUE, settleDec.takerIsBuy());
        assertEquals(7_777L, settleDec.journalPosition());
        assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + SettleDecoder.BLOCK_LENGTH, len);
    }

    @Test
    public void translatesTradeFieldForField_takerIsBuyFalse() {
        writeJournalTrade(9_001L, 556L, 4, 33L, 300L, 44L, 400L,
                7_500_000L, 250_000L, BooleanType.FALSE, 987_654_321L);

        int len = translator.translateTrade(journalBuf, 0, moneyBuf, 0);

        moneyHeaderDec.wrap(moneyBuf, 0);
        settleDec.wrapAndApplyHeader(moneyBuf, 0, moneyHeaderDec);

        assertEquals(556L, settleDec.tradeId());
        assertEquals(4, settleDec.marketId());
        assertEquals(500_033L, settleDec.takerOrderId());
        assertEquals(300L, settleDec.takerUserId());
        assertEquals(500_044L, settleDec.makerOrderId());
        assertEquals(400L, settleDec.makerUserId());
        assertEquals(7_500_000L, settleDec.price());
        assertEquals(250_000L, settleDec.quantity());
        assertEquals(BoolFlag.FALSE, settleDec.takerIsBuy());
        assertEquals(9_001L, settleDec.journalPosition());
        assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + SettleDecoder.BLOCK_LENGTH, len);
    }

    @Test
    public void translateTradeCarriesLargeEgressSeqAsJournalPositionVerbatim() {
        writeJournalTrade(Long.MAX_VALUE - 1, 1L, 1, 1L, 1L, 2L, 2L, 1L, 1L, BooleanType.TRUE, 1L);

        translator.translateTrade(journalBuf, 0, moneyBuf, 0);

        moneyHeaderDec.wrap(moneyBuf, 0);
        settleDec.wrapAndApplyHeader(moneyBuf, 0, moneyHeaderDec);
        assertEquals(Long.MAX_VALUE - 1, settleDec.journalPosition());
    }

    // ---- JournalTerminal -> TerminalRelease (deliberately status-blind) ----

    @Test
    public void translatesTerminalFilledToTerminalReleaseShape() {
        assertTerminalReleaseShape(TerminalStatus.FILLED);
    }

    @Test
    public void translatesTerminalCancelledToTerminalReleaseShape() {
        assertTerminalReleaseShape(TerminalStatus.CANCELLED);
    }

    @Test
    public void translatesTerminalRejectedToTerminalReleaseShape() {
        assertTerminalReleaseShape(TerminalStatus.REJECTED);
    }

    /**
     * FILLED, CANCELLED and REJECTED all produce the exact same TerminalRelease shape
     * (journalPosition/orderId/userId/timestamp only). This is deliberate, not an oversight: releasing
     * an order's residual hold means the same thing regardless of which terminal status caused it, and
     * the money-schema TerminalRelease message has no field to carry status even if we wanted to (see
     * money-schema.xml). The three call sites above (one per {@link TerminalStatus}) all route through
     * this one assertion, proving none of the other fields shift with status either.
     */
    private void assertTerminalReleaseShape(TerminalStatus status) {
        writeJournalTerminal(8_400L, 42L, 900L, 5, status, 111_222L);

        int len = translator.translateTerminal(journalBuf, 0, moneyBuf, 0);

        moneyHeaderDec.wrap(moneyBuf, 0);
        assertEquals(TerminalReleaseDecoder.TEMPLATE_ID, moneyHeaderDec.templateId());
        assertEquals(TerminalReleaseDecoder.SCHEMA_ID, moneyHeaderDec.schemaId());
        terminalReleaseDec.wrapAndApplyHeader(moneyBuf, 0, moneyHeaderDec);

        assertEquals(8_400L, terminalReleaseDec.journalPosition());
        assertEquals(500_042L, terminalReleaseDec.orderId());
        assertEquals(900L, terminalReleaseDec.userId());
        assertEquals(111_222L, terminalReleaseDec.timestamp());
        assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + TerminalReleaseDecoder.BLOCK_LENGTH, len);
    }

    @Test
    public void translateTerminalCarriesLargeEgressSeqAsJournalPositionVerbatim() {
        writeJournalTerminal(Long.MAX_VALUE, 1L, 1L, 1, TerminalStatus.FILLED, 1L);

        translator.translateTerminal(journalBuf, 0, moneyBuf, 0);

        moneyHeaderDec.wrap(moneyBuf, 0);
        terminalReleaseDec.wrapAndApplyHeader(moneyBuf, 0, moneyHeaderDec);
        assertEquals(Long.MAX_VALUE, terminalReleaseDec.journalPosition());
    }

    // ---- header sniffers ----

    @Test
    public void isTradeAndIsTerminalRecognizeTheirOwnMessages() {
        writeJournalTrade(1L, 1L, 1, 1L, 1L, 1L, 1L, 1L, 1L, BooleanType.TRUE, 1L);
        assertTrue(translator.isTrade(journalBuf, 0));
        assertFalse(translator.isTerminal(journalBuf, 0));

        writeJournalTerminal(1L, 1L, 1L, 1, TerminalStatus.FILLED, 1L);
        assertTrue(translator.isTerminal(journalBuf, 0));
        assertFalse(translator.isTrade(journalBuf, 0));
    }

    @Test
    public void sniffersRejectMatchingTemplateIdOnTheWrongSchema() {
        // Same templateId as JournalTrade (1), but schemaId=2 (the money schema) instead of 3 (the
        // journal schema). A templateId-only check would wrongly accept this; the sniffer must not.
        journalHeaderEnc.wrap(journalBuf, 0)
                .blockLength(JournalTradeEncoder.BLOCK_LENGTH)
                .templateId(JournalTradeEncoder.TEMPLATE_ID)
                .schemaId(2)
                .version(JournalTradeEncoder.SCHEMA_VERSION);

        assertFalse(translator.isTrade(journalBuf, 0));
        assertFalse(translator.isTerminal(journalBuf, 0));
    }

    @Test
    public void sniffersRejectUnrelatedTemplateIdOnTheRightSchema() {
        // Correct schema (3), but a templateId that is neither JournalTrade (1) nor JournalTerminal (2).
        journalHeaderEnc.wrap(journalBuf, 0)
                .blockLength(0)
                .templateId(99)
                .schemaId(JournalTradeEncoder.SCHEMA_ID)
                .version(JournalTradeEncoder.SCHEMA_VERSION);

        assertFalse(translator.isTrade(journalBuf, 0));
        assertFalse(translator.isTerminal(journalBuf, 0));
    }

    // ---- journal-side fixture builders ----

    private void writeJournalTrade(long egressSeq, long tradeId, int marketId, long takerOrderId,
            long takerUserId, long makerOrderId, long makerUserId, long price, long quantity,
            BooleanType takerIsBuy, long timestamp) {
        journalTradeEnc.wrapAndApplyHeader(journalBuf, 0, journalHeaderEnc)
                .egressSeq(egressSeq)
                .tradeId(tradeId)
                .marketId(marketId)
                .takerOrderId(takerOrderId)
                .takerUserId(takerUserId)
                .makerOrderId(makerOrderId)
                .makerUserId(makerUserId)
                .price(price)
                .quantity(quantity)
                .takerIsBuy(takerIsBuy)
                .timestamp(timestamp)
                // Distinct from the cluster ids on purpose: the tests prove the OMS ids (the
                // money key) are what reach the Settle, not the cluster ids.
                .takerOmsOrderId(takerOrderId + 500_000L)
                .makerOmsOrderId(makerOrderId + 500_000L);
    }

    private void writeJournalTerminal(long egressSeq, long orderId, long userId, int marketId,
            TerminalStatus status, long timestamp) {
        journalTerminalEnc.wrapAndApplyHeader(journalBuf, 0, journalHeaderEnc)
                .egressSeq(egressSeq)
                .orderId(orderId)
                .userId(userId)
                .marketId(marketId)
                .status(status)
                .timestamp(timestamp)
                .omsOrderId(orderId + 500_000L);
    }
}

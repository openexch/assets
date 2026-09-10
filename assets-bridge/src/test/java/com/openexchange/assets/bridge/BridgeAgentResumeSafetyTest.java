// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.match.infrastructure.journal.generated.BooleanType;
import com.match.infrastructure.journal.generated.JournalTerminalEncoder;
import com.match.infrastructure.journal.generated.JournalTradeEncoder;
import com.match.infrastructure.journal.generated.MessageHeaderEncoder;
import com.match.infrastructure.journal.generated.TerminalStatus;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.Recording;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.RecordingIncarnation;
import com.openexchange.assets.infrastructure.archive.ArchiveJournalSource.SourceIdentity;
import io.aeron.archive.client.AeronArchive;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Drive the real handler/filter/staging boundary without letting a staged batch reach the AE.
 * These failures are deterministic complements to the real-cluster tests: a duplicate can be a
 * filter SKIP even though the preceding trade exists only in the bridge's unsent staging buffer.
 */
public class BridgeAgentResumeSafetyTest {
    private static final SourceIdentity SOURCE = new SourceIdentity("localhost:9010", 4010, 4001, "aeron:ipc");
    private static final Recording RECORDING = new Recording(0, 0, AeronArchive.NULL_POSITION,
            new RecordingIncarnation(1_000, 42, 7, 65536, 1408, 4001));
    private static final Recording LATER_RECORDING = new Recording(1, 0, AeronArchive.NULL_POSITION,
            new RecordingIncarnation(2_000, 43, 8, 65536, 1408, 4001));
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final JournalTradeEncoder trade = new JournalTradeEncoder();
    private final JournalTerminalEncoder terminal = new JournalTerminalEncoder();

    @Test
    public void duplicateOfUnsentTradeDoesNotTurnStagingIntoAnAppliedByteCheckpoint() throws Exception {
        final BridgeAgent agent = agent();
        final ChainResumeMemo memo = field(agent, "resumeMemo", ChainResumeMemo.class);
        final BridgeFilter filter = new BridgeFilter(1_000, 1, true);
        memo.resumeIndex(SOURCE, List.of(RECORDING), 1_000, 1);
        beginRecording(agent, RECORDING);

        trade(1, 1_000);
        dispatch(agent, filter, 160);
        assertEquals(160, memo.replayStartPosition(RECORDING));
        trade(2, 2_000);
        dispatch(agent, filter, 320); // stage only: neither AE offer nor acknowledgement has happened
        dispatch(agent, filter, 480); // real filter calls this duplicate SKIP

        assertEquals(1, field(agent, "translator", JournalToMoneyTranslator.class).stagedTradeCount());
        assertEquals(0, field(agent, "state", BridgeState.class).forwardedTrades);
        assertEquals("the duplicate was SKIPped by the actual filter", 2,
                field(agent, "state", BridgeState.class).skippedEntries);
        memo.resumeIndex(SOURCE, List.of(RECORDING), 1_000, 1);
        assertEquals("retry must replay the unacknowledged trade, including after its duplicate", 160,
                memo.replayStartPosition(RECORDING));
    }

    @Test
    public void failedInclusiveTerminalOfferSealsMemoBeforeALaterSkip() throws Exception {
        final BridgeAgent agent = agent();
        final ChainResumeMemo memo = field(agent, "resumeMemo", ChainResumeMemo.class);
        final BridgeFilter filter = new BridgeFilter(1_000, 1, true);
        memo.resumeIndex(SOURCE, List.of(RECORDING), 1_000, 1);
        beginRecording(agent, RECORDING);
        trade(1, 900);
        dispatch(agent, filter, 160);

        terminal(1_000); // inclusive AE watermark must still be forwarded, not memoized
        final InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> dispatch(agent, filter, 256));
        // No client is supplied: the first attempted AE offer fails. Memo safety must already
        // be established before that side effect, independently of the transport failure type.
        assertTrue(failure.getCause() instanceof NullPointerException);
        terminal(950);
        dispatch(agent, filter, 352);
        memo.resumeIndex(SOURCE, List.of(RECORDING), 1_000, 1);
        assertEquals("an offer failure must not hide the inclusive terminal on retry", 160,
                memo.replayStartPosition(RECORDING));
    }

    @Test
    public void inclusiveTerminalInEarlierRecordingDoesNotBlockLaterAppliedByteResume() throws Exception {
        final Recording earlier = new Recording(0, 0, 96, RECORDING.incarnation());
        final List<Recording> chain = List.of(earlier, LATER_RECORDING);
        final BridgeAgent agent = agent();
        final ChainResumeMemo memo = field(agent, "resumeMemo", ChainResumeMemo.class);
        final BridgeFilter firstFilter = new BridgeFilter(1_000, 2, true);
        memo.resumeIndex(SOURCE, chain, 1_000, 2);

        beginRecording(agent, earlier);
        terminal(1_000); // inclusive redelivery is a no-op economically, but still a FORWARD
        assertFailedTerminalOffer(agent, firstFilter, 96);
        memo.noteDrained(chain, 0, false);

        // Exercise the next recording boundary after the handler sealed on that forward. The
        // absent client above makes the offer fail deterministically; the byte-proof requirement
        // is the same when that inclusive terminal offer succeeds as an AE idempotent no-op.
        beginRecording(agent, LATER_RECORDING);
        trade(1, 800);
        dispatch(agent, firstFilter, 160);
        assertEquals("the later recording can remember a trade already applied by the AE", 160,
                memo.replayStartPosition(LATER_RECORDING));

        assertEquals("the inclusive terminal still prevents a drained chain prefix", 0,
                memo.resumeIndex(SOURCE, chain, 1_000, 2));
        assertEquals("retry keeps the later recording's independent byte proof", 160,
                memo.replayStartPosition(LATER_RECORDING));

        final BridgeFilter retryFilter = new BridgeFilter(1_000, 2, true);
        beginRecording(agent, earlier);
        terminal(1_000);
        assertFailedTerminalOffer(agent, retryFilter, 96);
        beginRecording(agent, LATER_RECORDING);
        trade(2, 900);
        dispatch(agent, retryFilter, 320);
        assertEquals("another inclusive redelivery must not force a large applied-head rescan", 320,
                memo.replayStartPosition(LATER_RECORDING));
        assertEquals(0, memo.replayStartPosition(earlier));
        assertEquals(0, memo.resumeIndex(SOURCE, chain, 1_000, 2));
        assertEquals(2, field(agent, "state", BridgeState.class).skippedEntries);
        assertEquals(0, field(agent, "translator", JournalToMoneyTranslator.class).stagedTradeCount());
    }

    @Test
    public void duplicateOfEarlierUnsentTradeCannotStartALaterRecordingByteProof() throws Exception {
        final Recording earlier = new Recording(0, 0, 160, RECORDING.incarnation());
        final List<Recording> chain = List.of(earlier, LATER_RECORDING);
        final BridgeAgent agent = agent();
        final ChainResumeMemo memo = field(agent, "resumeMemo", ChainResumeMemo.class);
        final BridgeFilter filter = new BridgeFilter(1_000, 1, true);
        memo.resumeIndex(SOURCE, chain, 1_000, 1);

        beginRecording(agent, earlier);
        trade(2, 2_000);
        dispatch(agent, filter, 160); // pending only in staging: the AE still reports trade one
        assertEquals(1, field(agent, "translator", JournalToMoneyTranslator.class).stagedTradeCount());

        beginRecording(agent, LATER_RECORDING);
        dispatch(agent, filter, 160); // duplicate two is a real filter SKIP across recording boundaries
        assertEquals("filter-forwarded is not the AE's applied watermark", 0,
                memo.replayStartPosition(LATER_RECORDING));
        trade(1, 1_000);
        dispatch(agent, filter, 320);
        assertEquals("a later applied duplicate cannot extend a prefix across unacknowledged trade two", 0,
                memo.replayStartPosition(LATER_RECORDING));
        assertEquals(2, field(agent, "state", BridgeState.class).skippedEntries);
        assertEquals(0, field(agent, "state", BridgeState.class).forwardedTrades);
        assertEquals(0, memo.resumeIndex(SOURCE, chain, 1_000, 1));
        assertEquals("retry must reread the unacknowledged trade in the later recording", 0,
                memo.replayStartPosition(LATER_RECORDING));
    }

    private void assertFailedTerminalOffer(final BridgeAgent agent, final BridgeFilter filter, final long position) {
        final InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> dispatch(agent, filter, position));
        assertTrue(failure.getCause() instanceof NullPointerException);
    }

    private void beginRecording(final BridgeAgent agent, final Recording current) throws ReflectiveOperationException {
        final Field recording = BridgeAgent.class.getDeclaredField("currentRecording");
        recording.setAccessible(true);
        recording.set(agent, current);
        field(agent, "resumeMemo", ChainResumeMemo.class).beginRecording();
    }

    private BridgeAgent agent() throws ReflectiveOperationException {
        final BridgeAgent agent = new BridgeAgent(new BridgeConfig(List.of("localhost:9010"),
                List.of("localhost"), 9300, "localhost:9394", "localhost", true, 5_000, 0),
                "unused-by-handler", new BridgeState());
        final Field recording = BridgeAgent.class.getDeclaredField("currentRecording");
        recording.setAccessible(true);
        recording.set(agent, RECORDING);
        return agent;
    }

    private void dispatch(final BridgeAgent agent, final BridgeFilter filter, final long position)
            throws ReflectiveOperationException {
        final Method method = BridgeAgent.class.getDeclaredMethod("onJournalEntry", DirectBuffer.class,
                int.class, BridgeFilter.class, AeFeedClient.class, long.class);
        method.setAccessible(true);
        method.invoke(agent, buffer, 0, filter, null, position);
    }

    private static <T> T field(final BridgeAgent agent, final String name, final Class<T> type)
            throws ReflectiveOperationException {
        final Field field = BridgeAgent.class.getDeclaredField(name);
        field.setAccessible(true);
        return type.cast(field.get(agent));
    }

    private void trade(final long tradeId, final long egressSeq) {
        trade.wrapAndApplyHeader(buffer, 0, header).egressSeq(egressSeq).tradeId(tradeId).marketId(1)
                .takerOrderId(22).takerUserId(200).makerOrderId(11).makerUserId(100)
                .price(100).quantity(1).takerIsBuy(BooleanType.TRUE).timestamp(egressSeq)
                .takerOmsOrderId(22).makerOmsOrderId(11);
    }

    private void terminal(final long egressSeq) {
        terminal.wrapAndApplyHeader(buffer, 0, header).egressSeq(egressSeq).orderId(11).userId(100)
                .marketId(1).status(TerminalStatus.FILLED).timestamp(egressSeq).omsOrderId(11);
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.SetMoneyJournalEncoder;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Arm or disarm the Assets Engine's money journal: {@code java -jar assets-bridge.jar
 * set-money-journal on|off}.
 *
 * <p>Whether the journal is armed is <b>replicated state</b>, not node configuration — it gates
 * {@code journalSeq}, which is snapshotted. It therefore travels through the log like any other
 * command, which is what stops two replicas disagreeing and what makes a replayed log arm the journal
 * exactly where the original did. This tool is how an operator sends it.</p>
 *
 * <p><b>It verifies.</b> The command produces no egress of its own, so the tool follows it with a
 * {@code QueryFeedPosition} and checks the reported setting before reporting success. A command that
 * was silently dropped — an old engine that does not know the message, a leader that went away between
 * offer and commit — would otherwise look exactly like one that worked.</p>
 *
 * <p>Exit codes: {@code 0} applied and confirmed; {@code 3} no leader / timed out; {@code 4} the
 * engine reported a setting other than the one requested; {@code 64} bad usage.</p>
 */
public final class AeSetMoneyJournal {

    private static final long OVERALL_TIMEOUT_MS = 30_000;

    private static final int EXIT_OK = 0;
    private static final int EXIT_TIMEOUT = 3;
    private static final int EXIT_NOT_CONFIRMED = 4;
    private static final int EXIT_USAGE = 64;

    private AeSetMoneyJournal() {
    }

    public static void main(final String[] args) {
        System.exit(run(args));
    }

    public static int run(final String[] args) {
        if (args.length != 1 || !("on".equals(args[0]) || "off".equals(args[0]))) {
            System.err.println("usage: set-money-journal on|off");
            return EXIT_USAGE;
        }
        final boolean enable = "on".equals(args[0]);

        final BridgeConfig config = BridgeConfig.fromEnv();
        final Confirmation confirmation = new Confirmation();

        // Own embedded media driver in its own /dev/shm dir, so this can run alongside the bridge/OMS
        // without touching their drivers. Mirrors AeDump.
        final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName("/dev/shm/aeron-aesetjournal-" + ProcessHandle.current().pid())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .errorHandler(t -> t.printStackTrace(System.err)));

        AeronCluster cluster = null;
        try {
            cluster = AeronCluster.connect(new AeronCluster.Context()
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(8))
                    .aeronDirectoryName(driver.aeronDirectoryName())
                    .ingressChannel("aeron:udp?term-length=4m")
                    .ingressEndpoints(AeFeedClient.ingressEndpoints(config.aeClusterAddresses, config.aePortBase))
                    .egressChannel("aeron:udp?endpoint=" + config.localHost + ":0")
                    .egressListener(confirmation)
                    .errorHandler(t -> t.printStackTrace(System.err)));

            final long deadline = System.currentTimeMillis() + OVERALL_TIMEOUT_MS;
            final UnsafeBuffer buf = new UnsafeBuffer(new byte[64]);
            final MessageHeaderEncoder header = new MessageHeaderEncoder();

            final SetMoneyJournalEncoder set = new SetMoneyJournalEncoder();
            set.wrapAndApplyHeader(buf, 0, header)
                    .correlationId(confirmation.setCorr)
                    .enabled(enable ? BoolFlag.TRUE : BoolFlag.FALSE);
            offerBlocking(cluster, buf, MessageHeaderEncoder.ENCODED_LENGTH + set.encodedLength(), deadline);

            // Read back through the same session: the query is ordered behind the command in the log,
            // so a report that arrives can only reflect a state at or after it.
            final QueryFeedPositionEncoder query = new QueryFeedPositionEncoder();
            query.wrapAndApplyHeader(buf, 0, header).correlationId(confirmation.queryCorr);
            offerBlocking(cluster, buf, MessageHeaderEncoder.ENCODED_LENGTH + query.encodedLength(), deadline);

            final IdleStrategy idle = new BackoffIdleStrategy();
            while (!confirmation.answered) {
                if (System.currentTimeMillis() > deadline) {
                    System.err.println("set-money-journal: no FeedPositionReport within "
                            + OVERALL_TIMEOUT_MS + "ms — the setting is UNKNOWN, not unchanged");
                    return EXIT_TIMEOUT;
                }
                idle.idle(cluster.pollEgress());
            }

            if (confirmation.journalEnabled != enable) {
                System.err.println("set-money-journal: requested " + (enable ? "on" : "off")
                        + " but the engine reports " + (confirmation.journalEnabled ? "on" : "off")
                        + " — the command did not take (an engine older than schema v4 ignores it)");
                return EXIT_NOT_CONFIRMED;
            }
            System.out.println("money journal " + (enable ? "ARMED" : "DISARMED")
                    + " (confirmed; consumePosition=" + confirmation.consumePosition
                    + " lastAppliedTradeId=" + confirmation.lastAppliedTradeId + ")");
            return EXIT_OK;
        } catch (final io.aeron.exceptions.TimeoutException | IllegalStateException e) {
            System.err.println("set-money-journal: could not reach an AE leader: " + e.getMessage());
            return EXIT_TIMEOUT;
        } finally {
            CloseHelper.quietClose(cluster);
            CloseHelper.quietClose(driver);
        }
    }

    private static void offerBlocking(final AeronCluster cluster, final DirectBuffer buffer,
                                      final int length, final long deadline) {
        final IdleStrategy idle = new BackoffIdleStrategy();
        while (cluster.offer(buffer, 0, length) < 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("timed out offering to the AE cluster");
            }
            idle.idle();
            cluster.pollEgress();
        }
    }

    /** Collects the single FeedPositionReport answering our query. */
    private static final class Confirmation implements EgressListener {
        final long setCorr = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        final long queryCorr = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

        private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        private final FeedPositionReportDecoder reportDecoder = new FeedPositionReportDecoder();

        boolean answered;
        boolean journalEnabled;
        long consumePosition;
        long lastAppliedTradeId;

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp,
                              final DirectBuffer buffer, final int offset, final int length,
                              final Header header) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            headerDecoder.wrap(buffer, offset);
            if (headerDecoder.templateId() != FeedPositionReportDecoder.TEMPLATE_ID) {
                return;
            }
            reportDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            if (reportDecoder.correlationId() != queryCorr) {
                return;
            }
            consumePosition = reportDecoder.consumePosition();
            lastAppliedTradeId = reportDecoder.lastAppliedTradeId();
            journalEnabled = reportDecoder.journalEnabled() == BoolFlag.TRUE;
            answered = true;
        }
    }
}

// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;

import java.util.EnumSet;

/**
 * A member writer whose first payload chunk is held (the offer answers back pressure) for a fixed time, through the
 * kit writer's package-private fault seam (this class sits in the kit package in core TEST sources only). The header
 * is already recorded while it holds, so a kill in the hold leaves a header-bearing trailer-less recording, and an abort
 * requested in the hold is honoured at the next offer attempt.
 */
public final class SlowShadowWriters {

    private SlowShadowWriters() {
    }

    /** A writer like the production one whose message 1 is back-pressured for {@code holdMs} from its first attempt. */
    public static ShadowWriter slow(final Aeron aeron, final AeronArchive archive, final ShadowProducer producer,
                                    final ShadowSelfCheck selfCheck, final String ipcTermLength, final NanoClock clock,
                                    final long connectTimeoutNs, final long offerTimeoutNs, final long recordTimeoutNs,
                                    final long initialProducerSeq, final long initialLastWrittenPosition,
                                    final ShadowCounters counters, final long holdMs) {
        final ShadowWriter.Faults hold = new ShadowWriter.Faults() {
            private long firstAttemptNs = -1;

            @Override
            public long offer(final ExclusivePublication publication, final DirectBuffer buffer, final int offset,
                              final int length, final int messageIndex) {
                if (messageIndex == 0) {
                    firstAttemptNs = -1; // a new write
                }
                if (messageIndex == 1) {
                    final long now = System.nanoTime();
                    if (firstAttemptNs < 0) {
                        firstAttemptNs = now;
                        System.out.println("[SHADOW-PRODUCER] NC: holding the first payload chunk for " + holdMs
                                + " ms (slow writer, test only)");
                    }
                    if (now - firstAttemptNs < holdMs * 1_000_000L) {
                        return Publication.BACK_PRESSURED;
                    }
                }
                return publication.offer(buffer, offset, length);
            }
        };
        return new ShadowWriter(aeron, archive, producer, selfCheck, ipcTermLength, clock, connectTimeoutNs,
                Math.max(offerTimeoutNs, (holdMs + 30_000L) * 1_000_000L), recordTimeoutNs, initialProducerSeq,
                initialLastWrittenPosition, counters, hold, EnumSet.noneOf(NegativeControl.class));
    }
}

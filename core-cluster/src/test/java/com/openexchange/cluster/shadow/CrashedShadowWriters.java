// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.SystemNanoClock;

import java.util.EnumSet;

/**
 * The shape a {@code kill -9} mid-write leaves in an archive, written through the
 * kit writer's package-private fault seam (this class sits in the kit package in core TEST sources only). The
 * writer offers the header and every payload chunk, then "dies" before the trailer: the trailer offer throws and the
 * writer's own cleanup purge is refused, so the recording stays as a crash leaves it, trailer-less (NO_TRAILER), with
 * the producer's alias and header.
 */
public final class CrashedShadowWriters {

    private CrashedShadowWriters() {
    }

    /** A writer of {@code producer} whose write stops before the trailer and leaves the recording behind. */
    public static ShadowWriter crashingBeforeTrailer(final Aeron aeron, final AeronArchive archive,
                                                     final ShadowProducer producer, final ShadowSelfCheck selfCheck,
                                                     final String ipcTermLength, final long producerSeq) {
        final ShadowWriter.Faults crash = new ShadowWriter.Faults() {
            @Override
            public void onTrailerOffer(final long recordedPosition, final long publicationPosition) {
                throw new IllegalStateException("simulated kill -9 before the trailer (recorded " + recordedPosition
                        + ")");
            }

            @Override
            public void beforePurge(final long recordingId) {
                throw new IllegalStateException("simulated kill -9: no cleanup purge of " + recordingId);
            }
        };
        final long timeoutNs = 30_000_000_000L;
        return new ShadowWriter(aeron, archive, producer, selfCheck, ipcTermLength, SystemNanoClock.INSTANCE,
                timeoutNs, timeoutNs, timeoutNs, producerSeq, -1, new ShadowCounters(), crash,
                EnumSet.noneOf(NegativeControl.class));
    }
}

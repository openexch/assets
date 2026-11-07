// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/**
 * RCS S1 test helper: an Aeron {@link Header} whose {@code position()} (the frame-end position the service stamps)
 * equals a given log position. Term length 64 KiB ({@code positionBitsToShift = 16}), initial term id 0, one header-only
 * data frame ending exactly at the position. Self-checked: a position this cannot express throws.
 */
final class LogHeaders {

    private static final int BITS = 16;
    private static final int TERM_LENGTH = 1 << BITS;

    private LogHeaders() {
    }

    static Header at(final long position) {
        if (position < DataHeaderFlyweight.HEADER_LENGTH || (position & 31) != 0) {
            throw new IllegalArgumentException("position must be >= 32 and 32-aligned: " + position);
        }
        long termId = position >>> BITS;
        int end = (int) (position & (TERM_LENGTH - 1));
        if (end == 0) {
            termId--;
            end = TERM_LENGTH;
        }
        if (termId > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("position beyond the fixture's term range: " + position);
        }
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[DataHeaderFlyweight.HEADER_LENGTH]);
        frame.putInt(DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, DataHeaderFlyweight.HEADER_LENGTH, ByteOrder.LITTLE_ENDIAN);
        frame.putInt(DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, end - DataHeaderFlyweight.HEADER_LENGTH,
                ByteOrder.LITTLE_ENDIAN);
        frame.putInt(DataHeaderFlyweight.TERM_ID_FIELD_OFFSET, (int) termId, ByteOrder.LITTLE_ENDIAN);
        final Header header = new Header(0, BITS);
        header.buffer(frame);
        header.offset(0);
        if (header.position() != position) {
            throw new IllegalStateException("LogHeaders self-check: wanted " + position + " got " + header.position());
        }
        return header;
    }
}

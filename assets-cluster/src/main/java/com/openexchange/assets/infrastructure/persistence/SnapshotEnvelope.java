// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.zip.CRC32C;

/**
 * Integrity envelope around a snapshot payload (K5, matching engine {@code SnapshotCodec} pattern):
 * {@code [magic:int][version:int][payloadLength:int][reserved:int][payload...][crc32c:int]}.
 * The payload is written in place after the 16-byte header, so a 200 MB ledger is not copied; the
 * trailer is computed once over the finished payload. A loader verifies magic, length and CRC before
 * decoding a byte, and refuses to start on a mismatch: a truncated or foreign snapshot must never
 * become a ledger. Payloads without the magic (snapshots written before this envelope) are passed
 * through as legacy so an upgrade restores from its pre-envelope snapshot.
 */
public final class SnapshotEnvelope {

    public static final int MAGIC = 0x4145534E; // "AESN"
    public static final int VERSION = 1;
    public static final int HEADER_LENGTH = 16;
    public static final int TRAILER_LENGTH = 4;

    private final CRC32C crc = new CRC32C();

    /** Where a payload must start so the header fits before it. */
    public static int payloadOffset() {
        return HEADER_LENGTH;
    }

    /**
     * Seal a payload already written at {@link #payloadOffset()} with {@code payloadLength} bytes.
     *
     * @return the total envelope length to offer (header + payload + trailer)
     */
    public int seal(final MutableDirectBuffer buf, final int payloadLength) {
        buf.putInt(0, MAGIC);
        buf.putInt(4, VERSION);
        buf.putInt(8, payloadLength);
        buf.putInt(12, 0);
        buf.putInt(HEADER_LENGTH + payloadLength, crc32c(buf, HEADER_LENGTH, payloadLength));
        return HEADER_LENGTH + payloadLength + TRAILER_LENGTH;
    }

    /** True if {@code buf[0..length)} starts with this envelope's magic (else: a legacy bare payload). */
    public static boolean isEnveloped(final DirectBuffer buf, final int length) {
        return length >= HEADER_LENGTH + TRAILER_LENGTH && buf.getInt(0) == MAGIC;
    }

    /**
     * Verify an enveloped image and return the payload length (payload starts at {@link #payloadOffset()}).
     *
     * @throws IllegalStateException on a version, length or CRC mismatch
     */
    public int open(final DirectBuffer buf, final int length) {
        if (!isEnveloped(buf, length)) {
            throw new IllegalStateException("snapshot envelope magic missing (length=" + length + ")");
        }
        final int version = buf.getInt(4);
        if (version != VERSION) {
            throw new IllegalStateException("snapshot envelope version " + version + ", this build reads " + VERSION);
        }
        final int payloadLength = buf.getInt(8);
        if (payloadLength < 0 || HEADER_LENGTH + payloadLength + TRAILER_LENGTH != length) {
            throw new IllegalStateException("snapshot envelope length mismatch: header says " + payloadLength
                    + " payload bytes, image carries " + length + " total");
        }
        final int expected = buf.getInt(HEADER_LENGTH + payloadLength);
        final int actual = crc32c(buf, HEADER_LENGTH, payloadLength);
        if (expected != actual) {
            throw new IllegalStateException(String.format("snapshot CRC32C mismatch: trailer %08x, computed %08x over %d bytes",
                    expected, actual, payloadLength));
        }
        return payloadLength;
    }

    private int crc32c(final DirectBuffer buf, final int offset, final int length) {
        crc.reset();
        final byte[] array = buf.byteArray();
        if (array != null) {
            crc.update(array, buf.wrapAdjustment() + offset, length);
        } else {
            crc.update(buf.byteBuffer().duplicate().position(offset).limit(offset + length));
        }
        return (int) crc.getValue();
    }
}

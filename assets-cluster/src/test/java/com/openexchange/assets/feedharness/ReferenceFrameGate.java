// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import org.agrona.DirectBuffer;

import java.nio.ByteOrder;

/**
 * RC2 S9h 2.1 / S9a 4.4 rows 2-7: the Java formulation of the bridge's journal frame admission (row 1, the transport
 * flags, has no meaning outside a replay and is not modelled). The constants are literals on purpose (the C++
 * {@code FramePeek} and the projector's {@code journal.cpp} carry literals too); the test pins them against match's
 * generated schema 4 codecs. No business field is validated here: the core is the one semantic judge.
 */
public final class ReferenceFrameGate {

    public enum Reason { ADMIT, LENGTH, SCHEMA, TEMPLATE, BLOCK, FRAME_LENGTH, HEAD }

    public static final int MIN_LENGTH = 8;
    public static final int MAX_LENGTH = 1112;
    public static final int SCHEMA_ID = 4;
    public static final int SCHEMA_VERSION = 1;
    /** Root block length of templates 1..5 (index = templateId). */
    static final int[] BLOCK = {-1, 109, 62, 153, 44, 39};
    public static final int RELEASE_ENTRY_BLOCK = 22;
    public static final int RELEASE_CAP = 48;

    /** The peek of an admitted (or row-7-refused) frame. */
    public record Verdict(Reason reason, int templateId, long journalSeq, long meEpoch, long egressSeq) {
        public boolean admitted() {
            return reason == Reason.ADMIT;
        }
    }

    private ReferenceFrameGate() {
    }

    public static Verdict check(final byte[] frame) {
        return check(new org.agrona.concurrent.UnsafeBuffer(frame), 0, frame.length);
    }

    public static Verdict check(final DirectBuffer b, final int offset, final int length) {
        if (length < MIN_LENGTH || length > MAX_LENGTH) {
            return new Verdict(Reason.LENGTH, -1, 0, 0, 0);
        }
        final int block = u16(b, offset);
        final int templateId = u16(b, offset + 2);
        final int schemaId = u16(b, offset + 4);
        final int version = u16(b, offset + 6);
        if (schemaId != SCHEMA_ID || version != SCHEMA_VERSION) {
            return new Verdict(Reason.SCHEMA, templateId, 0, 0, 0);
        }
        if (templateId < 1 || templateId > 5) {
            return new Verdict(Reason.TEMPLATE, templateId, 0, 0, 0);
        }
        if (block != BLOCK[templateId]) {
            return new Verdict(Reason.BLOCK, templateId, 0, 0, 0);
        }
        if (templateId == 4) {
            if (length < 8 + 44 + 4) {
                return new Verdict(Reason.FRAME_LENGTH, templateId, 0, 0, 0);
            }
            final int entryBlock = u16(b, offset + 8 + 44);
            final int n = u16(b, offset + 8 + 44 + 2);
            if (entryBlock != RELEASE_ENTRY_BLOCK || n > RELEASE_CAP || length != 8 + 44 + 4 + RELEASE_ENTRY_BLOCK * n) {
                return new Verdict(Reason.FRAME_LENGTH, templateId, 0, 0, 0);
            }
        } else if (length != 8 + block) {
            return new Verdict(Reason.FRAME_LENGTH, templateId, 0, 0, 0);
        }
        final long journalSeq = b.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN);
        final long meEpoch = b.getLong(offset + 16, ByteOrder.LITTLE_ENDIAN);
        final long egressSeq = b.getLong(offset + 24, ByteOrder.LITTLE_ENDIAN);
        if (journalSeq < 1 || meEpoch < 1) {
            return new Verdict(Reason.HEAD, templateId, journalSeq, meEpoch, egressSeq);
        }
        return new Verdict(Reason.ADMIT, templateId, journalSeq, meEpoch, egressSeq);
    }

    private static int u16(final DirectBuffer b, final int at) {
        return b.getShort(at, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }
}

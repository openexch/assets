/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;

@SuppressWarnings("all")
public final class GateReasonsEncoder
{
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final int ENCODED_LENGTH = 4;
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private int offset;
    private MutableDirectBuffer buffer;

    public GateReasonsEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;

        return this;
    }

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public int encodedLength()
    {
        return ENCODED_LENGTH;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public GateReasonsEncoder clear()
    {
        buffer.putInt(offset, (int)0L, BYTE_ORDER);
        return this;
    }

    public GateReasonsEncoder admin(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 0) : bits & ~(1 << 0);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int admin(final int bits, final boolean value)
    {
        return value ? bits | (1 << 0) : bits & ~(1 << 0);
    }

    public GateReasonsEncoder retentionBudget(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 1) : bits & ~(1 << 1);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int retentionBudget(final int bits, final boolean value)
    {
        return value ? bits | (1 << 1) : bits & ~(1 << 1);
    }

    public GateReasonsEncoder bridgeHalt(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 2) : bits & ~(1 << 2);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int bridgeHalt(final int bits, final boolean value)
    {
        return value ? bits | (1 << 2) : bits & ~(1 << 2);
    }

    public GateReasonsEncoder recovery(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 3) : bits & ~(1 << 3);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int recovery(final int bits, final boolean value)
    {
        return value ? bits | (1 << 3) : bits & ~(1 << 3);
    }

    public GateReasonsEncoder meCapacity(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 4) : bits & ~(1 << 4);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int meCapacity(final int bits, final boolean value)
    {
        return value ? bits | (1 << 4) : bits & ~(1 << 4);
    }

    public GateReasonsEncoder genesis(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 5) : bits & ~(1 << 5);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int genesis(final int bits, final boolean value)
    {
        return value ? bits | (1 << 5) : bits & ~(1 << 5);
    }

    public GateReasonsEncoder applyFault(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 6) : bits & ~(1 << 6);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int applyFault(final int bits, final boolean value)
    {
        return value ? bits | (1 << 6) : bits & ~(1 << 6);
    }

    public GateReasonsEncoder meLegLive(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 7) : bits & ~(1 << 7);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int meLegLive(final int bits, final boolean value)
    {
        return value ? bits | (1 << 7) : bits & ~(1 << 7);
    }

    public GateReasonsEncoder meBehavior(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 8) : bits & ~(1 << 8);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int meBehavior(final int bits, final boolean value)
    {
        return value ? bits | (1 << 8) : bits & ~(1 << 8);
    }
}

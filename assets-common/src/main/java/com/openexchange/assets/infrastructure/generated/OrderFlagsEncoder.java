/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;

@SuppressWarnings("all")
public final class OrderFlagsEncoder
{
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final int ENCODED_LENGTH = 4;
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private int offset;
    private MutableDirectBuffer buffer;

    public OrderFlagsEncoder wrap(final MutableDirectBuffer buffer, final int offset)
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

    public OrderFlagsEncoder clear()
    {
        buffer.putInt(offset, (int)0L, BYTE_ORDER);
        return this;
    }
    /**
     * reserved; must be 0 in v6
     *
     * @param value true if reserved0 is set or false if not.
     * @return this for a fluent API.
     */

    public OrderFlagsEncoder reserved0(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 0) : bits & ~(1 << 0);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int reserved0(final int bits, final boolean value)
    {
        return value ? bits | (1 << 0) : bits & ~(1 << 0);
    }
}

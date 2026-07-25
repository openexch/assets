/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;

@SuppressWarnings("all")
public final class EgressChannelsEncoder
{
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 3;
    public static final String SEMANTIC_VERSION = "0.3";
    public static final int ENCODED_LENGTH = 4;
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private int offset;
    private MutableDirectBuffer buffer;

    public EgressChannelsEncoder wrap(final MutableDirectBuffer buffer, final int offset)
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

    public EgressChannelsEncoder clear()
    {
        buffer.putInt(offset, (int)0L, BYTE_ORDER);
        return this;
    }
    /**
     * responses to this client's own commands: hold/deposit/withdraw acks and rejects, feed position
     *
     * @param value true if acks is set or false if not.
     * @return this for a fluent API.
     */

    public EgressChannelsEncoder acks(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 0) : bits & ~(1 << 0);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int acks(final int bits, final boolean value)
    {
        return value ? bits | (1 << 0) : bits & ~(1 << 0);
    }
    /**
     * the BalanceUpdate firehose: one per changed (user, asset) line
     *
     * @param value true if balances is set or false if not.
     * @return this for a fluent API.
     */

    public EgressChannelsEncoder balances(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 1) : bits & ~(1 << 1);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int balances(final int bits, final boolean value)
    {
        return value ? bits | (1 << 1) : bits & ~(1 << 1);
    }
    /**
     * SettlementApplied and SettleFault
     *
     * @param value true if settlements is set or false if not.
     * @return this for a fluent API.
     */

    public EgressChannelsEncoder settlements(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 2) : bits & ~(1 << 2);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int settlements(final int bits, final boolean value)
    {
        return value ? bits | (1 << 2) : bits & ~(1 << 2);
    }
    /**
     * balance/hold snapshot streams and their terminators
     *
     * @param value true if snapshots is set or false if not.
     * @return this for a fluent API.
     */

    public EgressChannelsEncoder snapshots(final boolean value)
    {
        int bits = buffer.getInt(offset, BYTE_ORDER);
        bits = value ? bits | (1 << 3) : bits & ~(1 << 3);
        buffer.putInt(offset, bits, BYTE_ORDER);
        return this;
    }

    public static int snapshots(final int bits, final boolean value)
    {
        return value ? bits | (1 << 3) : bits & ~(1 << 3);
    }
}

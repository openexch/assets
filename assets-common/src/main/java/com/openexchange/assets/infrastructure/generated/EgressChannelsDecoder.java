/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;

@SuppressWarnings("all")
public final class EgressChannelsDecoder
{
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 3;
    public static final String SEMANTIC_VERSION = "0.3";
    public static final int ENCODED_LENGTH = 4;
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private int offset;
    private DirectBuffer buffer;

    public EgressChannelsDecoder wrap(final DirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;

        return this;
    }

    public DirectBuffer buffer()
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

    public boolean isEmpty()
    {
        return 0 == buffer.getInt(offset);
    }

    public long getRaw()
    {
        return (buffer.getInt(offset, BYTE_ORDER) & 0xFFFF_FFFFL);
    }
    /**
     * responses to this client's own commands: hold/deposit/withdraw acks and rejects, feed position
     *
     * @return true if acks set or false if not.
     */

    public boolean acks()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 0));
    }

    public static boolean acks(final int value)
    {
        return 0 != (value & (1 << 0));
    }
    /**
     * the BalanceUpdate firehose: one per changed (user, asset) line
     *
     * @return true if balances set or false if not.
     */

    public boolean balances()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 1));
    }

    public static boolean balances(final int value)
    {
        return 0 != (value & (1 << 1));
    }
    /**
     * SettlementApplied and SettleFault
     *
     * @return true if settlements set or false if not.
     */

    public boolean settlements()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 2));
    }

    public static boolean settlements(final int value)
    {
        return 0 != (value & (1 << 2));
    }
    /**
     * balance/hold snapshot streams and their terminators
     *
     * @return true if snapshots set or false if not.
     */

    public boolean snapshots()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 3));
    }

    public static boolean snapshots(final int value)
    {
        return 0 != (value & (1 << 3));
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        return appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        builder.append('{');
        boolean atLeastOne = false;
        if (acks())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("acks");
            atLeastOne = true;
        }
        if (balances())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("balances");
            atLeastOne = true;
        }
        if (settlements())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("settlements");
            atLeastOne = true;
        }
        if (snapshots())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("snapshots");
            atLeastOne = true;
        }
        builder.append('}');

        return builder;
    }
}

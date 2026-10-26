/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;

@SuppressWarnings("all")
public final class GateReasonsDecoder
{
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final int ENCODED_LENGTH = 4;
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private int offset;
    private DirectBuffer buffer;

    public GateReasonsDecoder wrap(final DirectBuffer buffer, final int offset)
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

    public boolean admin()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 0));
    }

    public static boolean admin(final int value)
    {
        return 0 != (value & (1 << 0));
    }

    public boolean retentionBudget()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 1));
    }

    public static boolean retentionBudget(final int value)
    {
        return 0 != (value & (1 << 1));
    }

    public boolean bridgeHalt()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 2));
    }

    public static boolean bridgeHalt(final int value)
    {
        return 0 != (value & (1 << 2));
    }

    public boolean recovery()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 3));
    }

    public static boolean recovery(final int value)
    {
        return 0 != (value & (1 << 3));
    }

    public boolean meCapacity()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 4));
    }

    public static boolean meCapacity(final int value)
    {
        return 0 != (value & (1 << 4));
    }

    public boolean genesis()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 5));
    }

    public static boolean genesis(final int value)
    {
        return 0 != (value & (1 << 5));
    }

    public boolean applyFault()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 6));
    }

    public static boolean applyFault(final int value)
    {
        return 0 != (value & (1 << 6));
    }

    public boolean meLegLive()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 7));
    }

    public static boolean meLegLive(final int value)
    {
        return 0 != (value & (1 << 7));
    }

    public boolean meBehavior()
    {
        return 0 != (buffer.getInt(offset, BYTE_ORDER) & (1 << 8));
    }

    public static boolean meBehavior(final int value)
    {
        return 0 != (value & (1 << 8));
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
        if (admin())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("admin");
            atLeastOne = true;
        }
        if (retentionBudget())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("retentionBudget");
            atLeastOne = true;
        }
        if (bridgeHalt())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("bridgeHalt");
            atLeastOne = true;
        }
        if (recovery())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("recovery");
            atLeastOne = true;
        }
        if (meCapacity())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("meCapacity");
            atLeastOne = true;
        }
        if (genesis())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("genesis");
            atLeastOne = true;
        }
        if (applyFault())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("applyFault");
            atLeastOne = true;
        }
        if (meLegLive())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("meLegLive");
            atLeastOne = true;
        }
        if (meBehavior())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("meBehavior");
            atLeastOne = true;
        }
        builder.append('}');

        return builder;
    }
}
